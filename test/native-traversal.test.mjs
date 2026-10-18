import assert from 'node:assert';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test, { after } from 'node:test';
import {
	openNativeFullTextIndex,
	openNativeFullTextReader,
	resetNativeFullTextIndex,
} from '@harperfast/fulltext/native';
import { encodeSearch } from '../dist/codec.js';
import { invoke } from '../dist/invoke.js';
import { loadAddon } from '../dist/load-addon.js';

const directories = [];
after(() => {
	for (const directory of directories) rmSync(directory, { recursive: true, force: true });
});

function options() {
	const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-traversal-'));
	directories.push(directory);
	return {
		path: directory,
		indexId: 'traversal',
		generation: 'one',
		fields: [{ name: 'title', weight: 1 }],
		filterFields: [{ name: 'category', type: 'string' }],
		analyzer: 'english@2',
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15000000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8388608,
			maxBatchBytes: 8388608,
		},
	};
}

async function pages(reader, query, expectedTotal, first) {
	const hits = [];
	let page = first ?? (await reader.searchPage(query));
	let pageCount = 0;
	const maxPages = Math.max(1, Math.ceil(expectedTotal / (query.limit ?? 20)));
	do {
		assert(++pageCount <= maxPages, 'continuation did not advance');
		assert.strictEqual(page.total, expectedTotal);
		assert.strictEqual(page.totalRelation, 'exact');
		hits.push(...page.hits);
		if (!page.continuation) break;
		page = await reader.searchPage({ ...query, continuation: page.continuation });
	} while (true);
	return hits;
}

const codeIs = (code) => (error) => error.code === code;

test('traverses more than 10,000 original matches while the writer publishes updates', async (context) => {
	const config = options();
	const writer = await openNativeFullTextIndex(config);
	context.after(() => writer.close({ mode: 'rollback' }));
	const ids = Array.from({ length: 10025 }, (_, i) => String(i).padStart(5, '0'));
	for (let start = 0; start < ids.length; start += 4000) {
		await writer.applyMutationBatch({
			upserts: ids.slice(start, start + 4000).map((id) => ({ id, version: 'original', fields: { title: 'widget' } })),
		});
		await writer.publish(String(start));
	}
	const reader = await openNativeFullTextReader(config);
	context.after(() => reader.close());
	const first = await reader.searchPage({ text: 'widget', limit: 997 });
	await reader.reload();
	await writer.applyMutationBatch({
		deletes: ids.slice(0, 100),
		upserts: [{ id: 'new', version: 'new', fields: { title: 'widget' } }],
	});
	await writer.publish('updated');
	await writer.close();
	const hits = await pages(reader, { text: 'widget', limit: 997 }, 10025, first);
	assert.deepStrictEqual(
		hits.map((hit) => hit.id),
		ids,
	);
	assert(hits.every((hit) => hit.version === 'original'));
	await assert.rejects(reader.search({ text: 'widget', offset: 10000, limit: 25 }), codeIs('E_INVALID_ARGUMENT'));
	await assert.rejects(resetNativeFullTextIndex({ path: config.path, indexId: config.indexId }), codeIs('E_LOCK_BUSY'));
	await reader.reload();
	await assert.rejects(
		reader.searchPage({ text: 'widget', limit: 997, continuation: first.continuation }),
		codeIs('E_CURSOR_EXPIRED'),
	);
	await reader.close();
	await assert.rejects(reader.searchPage({ text: 'widget', continuation: first.continuation }), codeIs('E_CLOSED'));
});

test('preserves mixed-score and UTF-8 tie order across pages, filters and Boolean queries', async (context) => {
	const config = options();
	const writer = await openNativeFullTextIndex(config);
	context.after(() => writer.close({ mode: 'rollback' }));
	const ids = Array.from({ length: 120 }, (_, i) => `${i % 2 ? 'é' : 'z'}-${String(i).padStart(3, '0')}`);
	for (let start = 0; start < 120; start += 30) {
		await writer.applyMutationBatch({
			upserts: ids.slice(start, start + 30).map((id, i) => ({
				id,
				fields: { title: i % 3 === 0 ? 'widget widget widget' : 'widget long catalog title' },
				filters: { category: i % 2 ? 'odd' : 'even' },
			})),
		});
		await writer.publish(String(start));
	}
	const reader = await openNativeFullTextReader(config);
	context.after(() => reader.close());
	for (const query of [
		{ text: 'widget' },
		{ text: 'widget', filter: { field: 'category', comparator: 'equals', value: 'even' } },
		{ text: 'widget', candidateIds: ids.slice(0, 80) },
		{ query: { operator: 'not', clause: { text: 'absent' } } },
		{ query: { operator: 'or', clauses: [{ text: 'widget' }, { text: 'catalog' }] } },
	]) {
		const expected = await reader.search({ ...query, limit: 120, exactTotal: true });
		const hits = await pages(reader, { ...query, limit: 17 }, expected.total);
		assert.deepStrictEqual(hits, expected.hits);
	}
	const empty = await reader.searchPage({ text: 'absent' });
	assert.deepStrictEqual(empty, { total: 0, totalRelation: 'exact', hits: [] });
	const exact = await pages(reader, { text: 'widget', limit: 30 }, 120);
	assert.strictEqual(exact.length, 120);
});

test('rejects mismatched and malformed continuations without poisoning the reader', async (context) => {
	const config = options();
	const writer = await openNativeFullTextIndex(config);
	context.after(() => writer.close({ mode: 'rollback' }));
	await writer.applyMutationBatch({ upserts: ['a', 'b', 'c'].map((id) => ({ id, fields: { title: 'widget' } })) });
	await writer.publish('one');
	const reader = await openNativeFullTextReader(config);
	const other = await openNativeFullTextReader(config);
	context.after(() => reader.close());
	context.after(() => other.close());
	const first = await reader.searchPage({ text: 'widget', limit: 1 });
	for (const defaults of [
		{ offset: undefined, exactTotal: undefined },
		{ offset: 0, exactTotal: false },
		{ offset: 0, exactTotal: true },
		{ offset: null, exactTotal: null },
	]) {
		assert.deepStrictEqual(await reader.searchPage({ text: 'widget', limit: 1, ...defaults }), first);
	}
	assert(first.continuation);
	await assert.rejects(
		other.searchPage({ text: 'widget', continuation: first.continuation }),
		codeIs('E_INVALID_ARGUMENT'),
	);
	await assert.rejects(
		reader.searchPage({ text: 'different', continuation: first.continuation }),
		codeIs('E_INVALID_ARGUMENT'),
	);
	const nan = Buffer.from(first.continuation, 'base64url');
	nan.writeFloatLE(NaN, 53);
	const badUtf8 = Buffer.from(first.continuation, 'base64url');
	badUtf8[57] = 255;
	for (const continuation of [
		'',
		'!',
		'a'.repeat(6000),
		first.continuation.slice(0, 70),
		nan.toString('base64url'),
		badUtf8.toString('base64url'),
	]) {
		await assert.rejects(reader.searchPage({ text: 'widget', limit: 1, continuation }), codeIs('E_INVALID_ARGUMENT'));
		assert.strictEqual(reader.status().state, 'open');
	}
	for (const query of [
		{ text: 'wid', mode: 'prefix' },
		{ text: 'widg', mode: 'fuzzy-prefix' },
		{ text: 'widget', limit: 0 },
		{ text: 'widget', limit: 10001 },
		{ text: 'widget', offset: 1 },
	]) {
		await assert.rejects(reader.searchPage(query), codeIs('E_INVALID_ARGUMENT'));
	}
	const continued = await reader.searchPage({ text: 'widget', limit: 2, continuation: first.continuation });
	assert.deepStrictEqual(
		continued.hits.map((hit) => hit.id),
		['b', 'c'],
	);
	assert.strictEqual(continued.continuation, undefined);
});

test('native decoding rejects damaged traversal frames without poisoning the handle', async (context) => {
	const config = options();
	const addon = loadAddon();
	const { encodeOpen, decodeResponse } = await import('../dist/codec.js');
	const packed = encodeOpen({
		...config,
		stopWords: true,
		positions: true,
		surfaceTerms: false,
		synonyms: [],
		filterFields: config.filterFields,
		fields: config.fields,
	});
	const cursor = await invoke((callback) => addon.__nativeOpen(packed, callback));
	const handle = cursor.u32();
	context.after(() => invoke((callback) => addon.__nativeClose(handle, true, callback)));
	const frame = encodeSearch({
		expression: { text: 'widget', mode: 'any', fields: [] },
		offset: 0,
		limit: 25,
		exactTotal: false,
		budgetMilliseconds: 30000,
		traversal: { after: { snapshot: '0000000000000000', score: 1, id: 'a' } },
	});
	const nonFinite = Buffer.from(frame);
	nonFinite.writeFloatLE(NaN, frame.length - 13);
	await assert.rejects(
		invoke((callback) => addon.__nativeSearch(handle, nonFinite, callback)),
		codeIs('E_INVALID_ARGUMENT'),
	);
	assert.strictEqual(
		typeof addon.__testDelayNextExpensiveSearch,
		'function',
		'build:test-native is required for budget coverage',
	);
	addon.__testDelayNextExpensiveSearch(handle, 50);
	const expired = encodeSearch({
		expression: { text: 'widget', mode: 'any', fields: [] },
		offset: 0,
		limit: 1,
		exactTotal: false,
		budgetMilliseconds: 1,
		traversal: {},
	});
	await assert.rejects(
		invoke((callback) => addon.__nativeSearch(handle, expired, callback)),
		codeIs('E_TIMEOUT'),
	);
	for (let length = 0; length < frame.length; length++) {
		await assert.rejects(invoke((callback) => addon.__nativeSearch(handle, frame.subarray(0, length), callback)));
		assert.strictEqual(decodeResponse(addon.__nativeStatus(handle)).u8(), 0);
	}
});

test('bounds continuation IDs and native response bytes at their advertised limits', async (context) => {
	const config = options();
	const writer = await openNativeFullTextIndex(config);
	context.after(() => writer.close({ mode: 'rollback' }));
	const suffix = 'x'.repeat(4095);
	await writer.applyMutationBatch({
		upserts: ['a', 'b'].map((prefix) => ({ id: prefix + suffix, fields: { title: 'widget' } })),
	});
	await writer.publish('large-ids');
	const first = await writer.searchPage({ text: 'widget', limit: 1 });
	assert.strictEqual(first.continuation.length, 5538);
	const next = await writer.searchPage({ text: 'widget', limit: 1, continuation: first.continuation });
	assert.strictEqual(next.hits[0].id, 'b' + suffix);
	await writer.applyMutationBatch({
		deletes: ['a' + suffix, 'b' + suffix],
		upserts: Array.from({ length: 1100 }, (_, i) => ({
			id: String(i).padStart(4, '0') + 'x'.repeat(4092),
			version: 'v'.repeat(4096),
			fields: { title: 'widget' },
		})),
	});
	await writer.publish('large-response');
	await assert.rejects(writer.searchPage({ text: 'widget', limit: 1100 }), codeIs('E_RESULT_TOO_LARGE'));
	assert.strictEqual(writer.status().state, 'open');
});

test('continuations reject replay in another process with the same native handle number', async (context) => {
	const config = options();
	const writer = await openNativeFullTextIndex(config);
	context.after(() => writer.close({ mode: 'rollback' }));
	await writer.applyMutationBatch({ upserts: ['a', 'b'].map((id) => ({ id, fields: { title: 'widget' } })) });
	await writer.publish('one');
	await writer.close();
	const child = `
		import { openNativeFullTextReader } from '@harperfast/fulltext/native';
		const reader = await openNativeFullTextReader(JSON.parse(process.argv[1]));
		try {
			const page = await reader.searchPage({ text: 'widget', limit: 1 });
			let code;
			if (process.argv[2]) {
				try { await reader.searchPage({ text: 'widget', limit: 1, continuation: process.argv[2] }); }
				catch (error) { code = error.code; }
			}
			console.log(JSON.stringify({ continuation: page.continuation, code }));
		} finally { await reader.close(); }
	`;
	const run = (continuation) =>
		JSON.parse(
			execFileSync(
				process.execPath,
				['--input-type=module', '--eval', child, JSON.stringify(config), ...(continuation ? [continuation] : [])],
				{ encoding: 'utf8', timeout: 60_000, env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' } },
			),
		);
	const first = run();
	const second = run(first.continuation);
	assert.strictEqual(Buffer.from(first.continuation, 'base64url').readUInt32LE(1), 1);
	assert.strictEqual(Buffer.from(second.continuation, 'base64url').readUInt32LE(1), 1);
	assert.strictEqual(second.code, 'E_INVALID_ARGUMENT');
	assert.notStrictEqual(first.continuation, second.continuation);
});
