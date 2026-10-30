import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import test from 'node:test';
import { NativeFullTextIndex, openNativeFullTextIndex } from '@harperfast/fulltext/native';

import { encodeOpen, encodeSearch, encodeTrace } from '../dist/codec.js';
import { invoke } from '../dist/invoke.js';
import { loadAddon } from '../dist/load-addon.js';

const invalid = (error) => error.code === 'E_INVALID_ARGUMENT';
const ids = (result) => result.hits.map(({ id }) => id).sort();

async function fixture(context, documents, extra = {}) {
	const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-controls-'));
	const options = {
		path: directory,
		indexId: 'query-controls',
		generation: 'one',
		fields: [
			{ name: 'title', weight: 1 },
			{ name: 'body', weight: 1 },
		],
		filterFields: [{ name: 'enabled', type: 'boolean' }],
		analyzer: 'english@2',
		stopWords: false,
		synonyms: [],
		positions: true,
		surfaceTerms: true,
		...extra,
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 4 * 1024 * 1024,
			maxBatchBytes: 1024 * 1024,
		},
	};
	const addon = loadAddon();
	const opened = await invoke((callback) => addon.__nativeOpen(encodeOpen(options), callback));
	const handle = opened.u32();
	assert.strictEqual(opened.u8(), 0);
	opened.finish();
	let index = new NativeFullTextIndex({
		handle,
		maxBatchBytes: options.limits.maxBatchBytes,
		fieldNames: options.fields.map(({ name }) => name),
		filterFields: options.filterFields,
	});
	context.after(async () => {
		await index.close({ mode: 'rollback' });
		await rm(directory, { recursive: true, force: true });
	});
	await index.applyMutationBatch({ upserts: documents });
	await index.commit();
	await index.reload();
	return {
		handle,
		addon,
		get index() {
			return index;
		},
		async reopen() {
			await index.close();
			index = await openNativeFullTextIndex(options);
		},
	};
}

test('fuzzy controls select edits and transpositions for full and prefix matching and tracing', async (context) => {
	const records = [
		{ id: 'exact', fields: { title: 'abcdef' } },
		{ id: 'one', fields: { title: 'abcxef' } },
		{ id: 'two', fields: { title: 'abxxef' } },
		{ id: 'swap', fields: { title: 'abdcef' } },
		{ id: 'suffix', fields: { title: 'abcdefzzz' } },
	];
	const fixture_ = await fixture(context, records);
	await fixture_.reopen();
	const index = fixture_.index;
	for (const mode of ['fuzzy', 'fuzzy-prefix']) {
		const base = { text: 'abcdef', mode, exactTotal: true };
		assert.deepStrictEqual(
			await index.search(base),
			await index.search({ ...base, fuzzyDistance: 1, fuzzyTranspositions: true }),
		);
		for (const fuzzyDistance of [0, 1, 2]) {
			for (const fuzzyTranspositions of [false, true]) {
				const request = { ...base, fuzzyDistance, fuzzyTranspositions };
				const result = await index.search(request);
				const expected = ['exact'];
				if (fuzzyDistance >= 1) expected.push('one');
				if (fuzzyDistance >= 2) expected.push('two');
				if (fuzzyDistance >= (fuzzyTranspositions ? 1 : 2)) expected.push('swap');
				if (mode === 'fuzzy-prefix') expected.push('suffix');
				assert.deepStrictEqual(ids(result), expected.sort(), JSON.stringify(request));
				assert.strictEqual(result.total, expected.length);
				assert.strictEqual(result.hits[0].id, 'exact');
				const trace = await index.traceMatches(request, records);
				assert.deepStrictEqual(
					trace.records
						.filter(({ values }) => values.length)
						.map(({ id }) => id)
						.sort(),
					expected,
				);
			}
		}
	}
});

test('invalid options reject consistently, including explicit defaults on unrelated modes, and recover', async (context) => {
	const { index } = await fixture(context, [{ id: 'saved', fields: { title: 'amber quartz' } }]);
	const requests = [
		...[NaN, Infinity, -1, 0.5, 3, '1', null].map((fuzzyDistance) => ({ text: 'amber', mode: 'fuzzy', fuzzyDistance })),
		...[0, 'true', null].map((fuzzyTranspositions) => ({ text: 'amber', mode: 'fuzzy', fuzzyTranspositions })),
		{ text: 'amber', fuzzyDistance: 1 },
		{ text: 'amber', fuzzyTranspositions: true },
		{ text: 'amber', mode: 'phrase', fuzzyDistance: 1 },
	];
	for (const request of requests) {
		await assert.rejects(index.search(request), invalid);
		await assert.rejects(index.searchPage(request), invalid);
		await assert.rejects(index.traceMatches(request, []), invalid);
		await assert.rejects(index.search({ query: request }), invalid);
	}
	for (const option of [{ fuzzyDistance: 1 }, { fuzzyTranspositions: true }]) {
		await assert.rejects(index.search({ query: { text: 'amber' }, ...option }), invalid);
	}
	assert.deepStrictEqual(ids(await index.search({ text: 'amber' })), ['saved']);
});

test('malformed packed query options reject at native decoding and leave the reader usable', async (context) => {
	const { handle, addon, index } = await fixture(context, [{ id: 'saved', fields: { title: 'amber quartz' } }]);
	for (const trace of [false, true]) {
		for (const [mode, field, value] of [
			['fuzzy', 'distance', 3],
			['fuzzy', 'transpositions', 2],
			['phrase', 'distance', 0],
			['any', 'transpositions', 0],
		]) {
			const text = 'amber quartz';
			const clause = { text, mode, fields: [] };
			const frame = trace
				? encodeTrace({ ...clause, records: [], budgetMilliseconds: 30_000 })
				: encodeSearch({ expression: clause, offset: 0, limit: 10, exactTotal: true, budgetMilliseconds: 30_000 });
			const optionsOffset = 6 + (trace ? 0 : 1) + 4 + Buffer.byteLength(text) + 1;
			frame.writeUInt8(value, optionsOffset + (field === 'distance' ? 0 : 1));
			await assert.rejects(
				invoke((callback) =>
					trace ? addon.__nativeTraceMatches(handle, frame, callback) : addon.__nativeSearch(handle, frame, callback),
				),
				invalid,
			);
		}
	}
	assert.deepStrictEqual(ids(await index.search({ text: 'amber' })), ['saved']);
});

test('controls retain field selection, Unicode tracing and independent fuzzy Boolean leaves', async (context) => {
	const records = [
		{ id: 'unicode', fields: { title: '😀 caféteria', body: 'abcdef' }, filters: { enabled: true } },
		{ id: 'other', fields: { title: 'abcxef', body: 'abxxef' } },
	];
	const { index } = await fixture(context, records);
	const query = {
		operator: 'and',
		clauses: [
			{ text: 'abcdef', mode: 'fuzzy', fuzzyDistance: 0, fields: ['body'] },
			{ text: 'caféterix', mode: 'fuzzy', fuzzyDistance: 1, fuzzyTranspositions: false, fields: ['title'] },
		],
	};
	const filtered = await index.search({
		query,
		exactTotal: true,
		filter: { field: 'enabled', comparator: 'equals', value: true },
	});
	assert.deepStrictEqual(ids(filtered), ['unicode']);
	assert.strictEqual(filtered.total, 1);
	const request = { text: 'caféterix', mode: 'fuzzy', fuzzyDistance: 1, fuzzyTranspositions: false, fields: ['title'] };
	const traced = await index.traceMatches(request, records);
	assert.deepStrictEqual(traced.records.find(({ id }) => id === 'unicode').values, [
		{ field: 'title', valueIndex: 0, spans: [{ start: 3, end: 12 }] },
	]);
	assert.deepStrictEqual(
		await index.search({ text: 'ab', mode: 'fuzzy' }),
		await index.search({ text: 'ab', mode: 'fuzzy', fuzzyDistance: 2 }),
	);
	for (const fuzzyDistance of [0, 1, 2]) {
		await assert.rejects(index.search({ text: 'ab', mode: 'fuzzy-prefix', fuzzyDistance }), invalid);
	}
});

test('fuzzy options are snapshotted once and continuation identity uses resolved defaults', async (context) => {
	const { index } = await fixture(context, [
		{ id: 'one', fields: { title: 'amber quartz' } },
		{ id: 'two', fields: { title: 'amber quartz' } },
	]);
	let reads = 0;
	const request = {
		text: 'amber',
		mode: 'fuzzy',
		get fuzzyDistance() {
			reads++;
			return reads === 1 ? 1 : 3;
		},
	};
	assert.strictEqual((await index.search(request)).total, 2);
	assert.strictEqual(reads, 1);
	reads = 0;
	await index.traceMatches(request, []);
	assert.strictEqual(reads, 1);
	for (const mode of ['fuzzy', 'fuzzy-prefix']) {
		const request = { text: 'amber', mode, limit: 1 };
		const page = await index.searchPage(request);
		assert(page.continuation);
		await assert.rejects(index.searchPage({ ...request, fuzzyDistance: 2, continuation: page.continuation }), invalid);
		await assert.rejects(
			index.searchPage({ ...request, fuzzyTranspositions: false, continuation: page.continuation }),
			invalid,
		);
		await assert.doesNotReject(
			index.searchPage({ ...request, fuzzyDistance: 1, fuzzyTranspositions: true, continuation: page.continuation }),
		);
	}
});
