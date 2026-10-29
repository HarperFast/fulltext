import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
	NativeFullTextIndex,
	openNativeFullTextIndex,
	openNativeFullTextReader,
	runtimeInfo,
} from '@harperfast/fulltext/native';
import { decodeResponse, encodeOpen, encodeSearch } from '../dist/codec.js';
import { invoke } from '../dist/invoke.js';
import { loadAddon } from '../dist/load-addon.js';

const words = (count) => Array.from({ length: count }, (_, i) => `word${i}`).join(' ');
const invalid = (error) => error.code === 'E_INVALID_ARGUMENT';
function options(directory, policy = {}, fields = [{ name: 'title' }, { name: 'body' }]) {
	return {
		path: directory,
		indexId: 'capacity',
		generation: 'one',
		analyzer: 'english@2',
		positions: true,
		surfaceTerms: true,
		stopWords: false,
		fields: fields.map((field) => ({ weight: 1, ...field })),
		filterFields: [{ name: 'category', type: 'string' }],
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8 << 20,
			maxBatchBytes: 8 << 20,
			...policy,
		},
	};
}
async function fixture(context, policy = {}, fields) {
	const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-query-capacity-'));
	const config = options(directory, policy, fields);
	const index = await openNativeFullTextIndex(config);
	context.after(async () => {
		const result = await index.close({ mode: 'rollback' });
		assert(!result.cleanupError);
		rmSync(directory, { recursive: true, force: true });
	});
	return { index, config };
}

test('default term boundary is preserved and repeated terms do not consume distinct capacity', async (context) => {
	const { index } = await fixture(context);
	assert.deepStrictEqual(index.queryLimits, { maxQueryTerms: 64, maxQueryClauses: 256 });
	await index.search({ text: words(64), fields: ['title'] });
	await assert.rejects(index.search({ text: words(65), fields: ['title'], maxQueryTerms: 1024 }), invalid);
	await index.search({ text: 'word0 '.repeat(10_000), fields: ['title'] });
	await assert.rejects(index.search({ text: 'word0 '.repeat(65), mode: 'phrase', fields: ['title'] }), invalid);
});

test('raised term and compiled-clause policies preserve results through readers and reopen', async (context) => {
	const { index, config } = await fixture(context, { maxQueryTerms: 256, maxQueryClauses: 512 });
	await index.applyMutationBatch({
		upserts: ['a', 'b'].map((id) => ({
			id,
			version: id,
			fields: { title: words(130), body: words(130) },
			filters: { category: 'sale' },
		})),
	});
	await index.publish('capacity:1');
	assert(Object.isFrozen(index.queryLimits));
	assert.throws(() => {
		index.queryLimits.maxQueryTerms = 64;
	}, TypeError);
	const defaultReader = await openNativeFullTextReader({
		...config,
		limits: { ...config.limits, maxQueryTerms: 64, maxQueryClauses: 256 },
	});
	try {
		const request = { text: words(64), exactTotal: true };
		assert.deepStrictEqual(await defaultReader.search(request), await index.search(request));
	} finally {
		assert(!(await defaultReader.close()).cleanupError);
	}
	const result = await index.search({ text: words(130), exactTotal: true });
	assert.strictEqual(result.total, 2);
	assert.deepStrictEqual(
		result.hits.map((hit) => hit.id),
		['a', 'b'],
	);
	const reader = await openNativeFullTextReader(config);
	try {
		assert.deepStrictEqual(reader.queryLimits, index.queryLimits);
		assert.deepStrictEqual(await reader.search({ text: words(130), exactTotal: true }), result);
		const first = await reader.searchPage({ text: words(130), limit: 1 });
		assert(first.continuation);
		const second = await reader.searchPage({ text: words(130), limit: 1, continuation: first.continuation });
		assert.deepStrictEqual([...first.hits, ...second.hits], result.hits);
		const trace = await reader.traceMatches({ text: words(130) }, [{ id: 'a', fields: { title: words(130) } }]);
		assert.strictEqual(trace.complete, true);
		assert.strictEqual(trace.records[0].values[0].spans.length, 130);
		const repeated = 'word0 '.repeat(1100);
		const allTrace = await reader.traceMatches({ text: words(130), mode: 'all' }, [
			{ id: 'complete', fields: { title: repeated + words(130) } },
			{ id: 'missing-last', fields: { title: repeated + words(129) } },
		]);
		assert.strictEqual(allTrace.complete, false);
		assert.deepStrictEqual(
			allTrace.records.map((record) => record.id),
			['complete'],
		);
		assert.strictEqual(allTrace.records[0].values[0].spans.length, 1024);
	} finally {
		assert(!(await reader.close()).cleanupError);
	}
	await assert.rejects(index.search({ text: words(257), fields: ['title'] }), invalid);
	await assert.rejects(index.traceMatches({ text: words(257) }, [{ id: 'a', fields: { title: 'word0' } }]), invalid);
	await index.close();
	const lower = await openNativeFullTextIndex({
		...config,
		limits: { ...config.limits, maxQueryTerms: 256, maxQueryClauses: 256 },
	});
	try {
		assert.strictEqual(lower.committedPayload, 'capacity:1');
		await lower.search({ text: words(128) });
		await assert.rejects(
			lower.search({ text: words(129) }),
			(error) => invalid(error) && /256 clauses/.test(error.message),
		);
		assert.strictEqual((await lower.search({ text: 'word0' })).total, 2);
	} finally {
		assert(!(await lower.close()).cleanupError);
	}
});

test('lowered policies apply to phrase, prefix, fuzzy and reader analysis while filter bounds stay fixed', async (context) => {
	const { index, config } = await fixture(context, { maxQueryTerms: 2, maxQueryClauses: 3 });
	for (const mode of ['any', 'all', 'phrase', 'prefix', 'fuzzy', 'fuzzy-prefix'])
		await assert.rejects(index.search({ text: 'alpha bravo charlie', mode, fields: ['title'] }), invalid);
	await assert.rejects(index.search({ text: 'alpha bravo' }), invalid);
	await index.search({ text: 'alpha', fields: ['title'] });
	await index.commit();
	const reader = await openNativeFullTextReader(config);
	try {
		await assert.rejects(reader.search({ text: 'alpha bravo charlie', fields: ['title'] }), invalid);
	} finally {
		assert(!(await reader.close()).cleanupError);
	}
	const { index: raised } = await fixture(context, { maxQueryTerms: 1024, maxQueryClauses: 4096 });
	await assert.rejects(
		raised.search({ text: 'word0', filter: { field: 'category', comparator: 'in', value: Array(257).fill('sale') } }),
		invalid,
	);
	await raised.search({ query: { operator: 'or', clauses: Array.from({ length: 257 }, () => ({ text: 'word0' })) } });
});

test('larger text trees follow compiled policy through writers, readers, paging and reopen', async (context) => {
	const { index, config } = await fixture(context, { maxQueryClauses: 1024 });
	await index.applyMutationBatch({
		upserts: [
			{ id: 'first', fields: { title: 'word0' } },
			{ id: 'last', fields: { title: 'word299' } },
			{ id: 'outside', fields: { title: 'word300' } },
		],
	});
	await index.publish('wide-tree:1');
	const query = {
		operator: 'or',
		clauses: Array.from({ length: 300 }, (_, i) => ({ text: `word${i}`, fields: ['title'] })),
	};
	const result = await index.search({ query, exactTotal: true });
	assert.strictEqual(result.total, 2);
	assert.deepStrictEqual(result.hits.map((hit) => hit.id).sort(), ['first', 'last']);
	assert(result.hits.every((hit) => Number.isFinite(hit.score)));
	for (const limit of [256, 599, 600, 1024]) {
		const reader = await openNativeFullTextReader({ ...config, limits: { ...config.limits, maxQueryClauses: limit } });
		try {
			if (limit < 600) await assert.rejects(reader.search({ query }), invalid);
			else {
				assert.deepStrictEqual(await reader.search({ query, exactTotal: true }), result);
				const first = await reader.searchPage({ query, limit: 1 });
				const next = await reader.searchPage({ query, limit: 1, continuation: first.continuation });
				assert.deepStrictEqual([...first.hits, ...next.hits], result.hits);
			}
		} finally {
			assert(!(await reader.close()).cleanupError);
		}
	}
	await index.close();
	const reopened = await openNativeFullTextIndex(config);
	try {
		assert.deepStrictEqual(await reopened.search({ query, exactTotal: true }), result);
	} finally {
		assert(!(await reopened.close()).cleanupError);
	}
});

test('zero-term trees charge structure globally, retain depth guards and reject sparse inputs', async (context) => {
	const { index, config } = await fixture(context, { maxQueryClauses: 4096 });
	await index.applyMutationBatch({ upserts: [{ id: 'one', fields: { title: 'sentinel' } }] });
	await index.commit();
	await index.reload();
	const empty = (count) => ({ operator: 'or', clauses: Array.from({ length: count }, () => ({ text: '' })) });
	assert.strictEqual((await index.search({ query: empty(4096) })).total, 0);
	await assert.rejects(
		index.search({ query: empty(4097) }),
		(error) => invalid(error) && /structure/.test(error.message),
	);
	await assert.rejects(
		index.search({ query: { operator: 'and', clauses: [empty(2048), empty(2048)] } }),
		(error) => invalid(error) && /structure/.test(error.message),
	);
	let nested = { text: '' };
	for (let i = 0; i < 8; i++) nested = { operator: 'not', clause: nested };
	await index.search({ query: nested });
	await assert.rejects(index.search({ query: { operator: 'not', clause: nested } }), invalid);
	await assert.rejects(index.search({ query: { operator: 'or', clauses: Array(3) } }), invalid);
	let touched = false;
	const clauses = Array(4097);
	Object.defineProperty(clauses, 0, {
		get() {
			touched = true;
			throw new Error('descendant must not be visited');
		},
	});
	await assert.rejects(
		index.search({ query: { operator: 'or', clauses } }),
		(error) => invalid(error) && /structure/.test(error.message),
	);
	assert.strictEqual(touched, false);
	for (const limit of [1, 2]) {
		const reader = await openNativeFullTextReader({ ...config, limits: { ...config.limits, maxQueryClauses: limit } });
		try {
			const search = reader.search({ query: { operator: 'not', clause: { text: '' } } });
			if (limit === 1) await assert.rejects(search, invalid);
			else assert.strictEqual((await search).total, 1);
		} finally {
			assert(!(await reader.close()).cleanupError);
		}
	}
});

test('text normalization snapshots array length and ignores caller iterators', async (context) => {
	const { index } = await fixture(context, { maxQueryClauses: 2 });
	await index.applyMutationBatch({ upserts: [{ id: 'one', fields: { title: 'sentinel' } }] });
	await index.commit();
	await index.reload();
	const leaf = { text: 'sentinel', fields: ['title'] };
	const overridden = [leaf];
	overridden[Symbol.iterator] = () => {
		throw new Error('caller iterator must not be used');
	};
	const growing = [leaf];
	Object.defineProperty(growing, 0, {
		get() {
			growing.push(...Array(4096).fill({ text: '' }));
			return leaf;
		},
	});
	for (const clauses of [overridden, growing]) {
		const result = await index.search({ query: { operator: 'or', clauses } });
		assert.deepStrictEqual(
			result.hits.map((hit) => hit.id),
			['one'],
		);
	}
	assert.strictEqual(growing.length, 4097);
	let reads = 0;
	const changingLength = new Proxy([leaf], {
		get(target, key, receiver) {
			return key === 'length' ? (++reads === 1 ? 1 : 4097) : Reflect.get(target, key, receiver);
		},
	});
	assert.strictEqual((await index.search({ query: { operator: 'or', clauses: changingLength } })).total, 1);
	assert.strictEqual(reads, 1);
	for (const length of [NaN, -1, 1.5, Infinity, '1', undefined]) {
		const lying = new Proxy([], {
			get: (target, key, receiver) => (key === 'length' ? length : Reflect.get(target, key, receiver)),
		});
		let touched = false;
		const later = Array(4097);
		Object.defineProperty(later, 0, {
			get() {
				touched = true;
				throw new Error('invalid count must reject before visiting the next sibling');
			},
		});
		await assert.rejects(
			index.search({
				query: {
					operator: 'or',
					clauses: [
						{ operator: 'or', clauses: lying },
						{ operator: 'or', clauses: later },
					],
				},
			}),
			(error) => invalid(error) && /at least one clause/.test(error.message),
		);
		assert.strictEqual(touched, false);
	}
});

test('raw native admission enforces lowered structure and schema-field bounds before callbacks', async () => {
	const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-raw-structure-'));
	const config = options(directory, { maxQueryClauses: 3 });
	const addon = loadAddon();
	let handle;
	try {
		const opened = await invoke((callback) => addon.__nativeOpen(encodeOpen(config), callback));
		handle = opened.u32();
		assert.strictEqual(opened.u8(), 0);
		opened.finish();
		const frame = (expression) =>
			encodeSearch({ expression, offset: 0, limit: 1, exactTotal: false, budgetMilliseconds: 5000 });
		const leaf = { text: '', mode: 'any', fields: [] };
		let callbacks = 0;
		for (const [expression, message] of [
			[{ operator: 'or', clauses: Array(4).fill(leaf) }, /structure exceeds 3/],
			[{ ...leaf, fields: ['title', 'body', 'unavailable'] }, /field count exceeds 2/],
		])
			assert.throws(
				() =>
					addon.__nativeSearch(handle, frame(expression), () => {
						callbacks++;
					}),
				(error) => invalid(error) && message.test(error.message),
			);
		assert.strictEqual(callbacks, 0);
		await invoke((callback) =>
			addon.__nativeSearch(handle, frame({ operator: 'or', clauses: Array(3).fill(leaf) }), callback),
		);
	} finally {
		if (handle !== undefined) await invoke((callback) => addon.__nativeClose(handle, true, callback));
		rmSync(directory, { recursive: true, force: true });
	}
});

test('query construction has fixed whole-tree phrase and fuzzy safeguards', async (context) => {
	const { index } = await fixture(
		context,
		{ maxQueryTerms: 1024, maxQueryClauses: 4096 },
		Array.from({ length: 128 }, (_, i) => ({ name: `field${i}` })),
	);
	await assert.rejects(
		index.search({ text: words(1024), mode: 'phrase' }),
		(error) => invalid(error) && /phrase construction/.test(error.message),
	);
	const text = 'alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa';
	await assert.rejects(
		index.search({
			query: {
				operator: 'or',
				clauses: Array.from({ length: 18 }, () => ({ text, mode: 'fuzzy', fields: ['field0'] })),
			},
		}),
		(error) => invalid(error) && /fuzzy construction/.test(error.message),
	);
	await assert.rejects(
		index.search({
			query: {
				operator: 'or',
				clauses: Array.from({ length: 129 }, () => ({
					text: 'alpha',
					mode: 'fuzzy-prefix',
					fields: ['field0', 'field1'],
				})),
			},
		}),
		(error) => invalid(error) && /fuzzy construction/.test(error.message),
	);
	await index.search({ text: 'alpha', fields: ['field0'] });
});

test('open rejects invalid policies in the facade and native decoder; capability metadata separates defaults and ceilings', async () => {
	const info = await runtimeInfo();
	assert.strictEqual(info.limits.maxQueryTerms, 64);
	assert.strictEqual(info.limits.maxQueryClauses, 256);
	assert.throws(
		() =>
			new NativeFullTextIndex({ handle: 0, fieldNames: [], maxBatchBytes: 1024, searchLimits: { maxQueryTerms: -1 } }),
		invalid,
	);
	for (const [name, ceiling] of [
		['maxQueryTerms', info.limits.maxConfiguredQueryTerms],
		['maxQueryClauses', info.limits.maxConfiguredQueryClauses],
	]) {
		for (const value of [0, -1, 1.5, NaN, Infinity, -Infinity, '128', ceiling + 1, 2 ** 32])
			await assert.rejects(
				openNativeFullTextIndex(options('unused', { [name]: value })),
				(error) => invalid(error) && error.message.includes(name),
			);
		for (const value of [0, ceiling + 1]) {
			const bytes = encodeOpen(options('unused'));
			bytes.writeUInt32LE(value, bytes.length - (name === 'maxQueryTerms' ? 8 : 4));
			assert.throws(() => decodeResponse(loadAddon().__nativeValidateOpen(bytes)), invalid);
		}
	}
	const bytes = encodeOpen(options('unused'));
	const legacy = bytes.subarray(0, -8);
	decodeResponse(loadAddon().__nativeValidateOpen(legacy)).finish();
	for (let length = 1; length < 8; length++) {
		assert.throws(
			() => decodeResponse(loadAddon().__nativeValidateOpen(bytes.subarray(0, legacy.length + length))),
			invalid,
		);
	}
	assert.throws(
		() => decodeResponse(loadAddon().__nativeValidateOpen(Buffer.concat([bytes, Buffer.from([0])]))),
		invalid,
	);
});

test('raised indexed queries retain late terms, later fields, and all/phrase constraints', async (context) => {
	const { index } = await fixture(context, { maxQueryTerms: 256, maxQueryClauses: 512 });
	const documents = [
		{ id: 'early', fields: { title: 'word0' } },
		{ id: 'late-title', fields: { title: 'word129' } },
		{ id: 'late-body', fields: { body: 'word129' } },
		{ id: 'all-body', fields: { body: words(130) } },
		{ id: 'missing-last', fields: { title: words(129) } },
		{ id: 'broken-phrase', fields: { body: `${words(129)} unrelated word129` } },
	];
	await index.applyMutationBatch({ upserts: documents });
	await index.commit();
	await index.reload();
	const ids = async (mode, fields = ['title', 'body']) =>
		(await index.search({ text: words(130), mode, fields, exactTotal: true })).hits.map((hit) => hit.id).sort();
	assert.deepStrictEqual(await ids('any'), documents.map((document) => document.id).sort());
	assert.deepStrictEqual(await ids('any', ['title']), ['early', 'late-title', 'missing-last']);
	assert.deepStrictEqual(await ids('all'), ['all-body', 'broken-phrase']);
	assert.deepStrictEqual(await ids('phrase'), ['all-body']);
});

test('prefix construction preserves empty expansions and remaining-clause errors', async (context) => {
	const { index } = await fixture(context, { maxQueryClauses: 2 });
	await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { title: 'alpha beta gamma' } }] });
	await index.commit();
	await index.reload();
	assert.strictEqual((await index.search({ text: 'alpha beta zzz', mode: 'prefix', fields: ['title'] })).total, 0);
	await assert.rejects(
		index.search({ text: 'alpha beta gam', mode: 'prefix', fields: ['title'] }),
		(error) => error.code === 'E_PREFIX_TOO_BROAD' && /2 clauses/.test(error.message),
	);
	const { index: wide } = await fixture(
		context,
		{},
		Array.from({ length: 257 }, (_, i) => ({ name: `field${i}` })),
	);
	assert.strictEqual((await wide.search({ text: 'nonexistent', mode: 'prefix' })).total, 0);
});

test('raised fuzzy tracing hashes exact-only terms and bounds fuzzy alternatives independently', async (context) => {
	const { index } = await fixture(context, { maxQueryTerms: 1024, maxQueryClauses: 4096 });
	const alphabetic =
		'alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa';
	const query = { text: `${words(1008)} ${alphabetic}`, mode: 'fuzzy', fields: ['title'] };
	const result = await index.traceMatches(query, [{ id: 'a', fields: { title: 'word1007 alphx wordx' } }]);
	assert.strictEqual(result.complete, true);
	assert.deepStrictEqual(result.records[0].values[0].spans, [
		{ start: 0, end: 8 },
		{ start: 9, end: 14 },
	]);
});
