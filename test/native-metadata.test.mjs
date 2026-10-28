import assert from 'node:assert';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { tmpdir } from 'node:os';
import test from 'node:test';
import {
	FulltextError,
	openNativeFullTextIndex,
	openNativeFullTextReader,
	inspectNativeFullTextIndex,
	resetNativeFullTextIndex,
} from '@harperfast/fulltext/native';
import { encodeSearch, encodeOpen, decodeResponse } from '../dist/codec.js';
import { loadAddon } from '../dist/load-addon.js';
import { decodeAggregations } from '../dist/catalog.js';

function config(
	context,
	filterFields = [
		{ name: 'brand', type: 'string' },
		{ name: 'price', type: 'number' },
		{ name: 'active', type: 'boolean' },
	],
) {
	const root = mkdtempSync(path.join(tmpdir(), 'fulltext-metadata-'));
	context.after(() => rmSync(root, { recursive: true, force: true }));
	return {
		path: path.join(root, 'index'),
		indexId: 'catalog',
		generation: '1',
		fields: [{ name: 'title' }],
		filterFields,
		analyzer: 'english@2',
		surfaceTerms: true,
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8 << 20,
			maxBatchBytes: 8 << 20,
		},
	};
}
const records = [
	{ id: 'a', price: 10, brand: 'beta', active: false },
	{ id: 'b', price: 20, brand: 'alpha', active: true },
	{ id: 'c', price: [30, 5], brand: ['zeta', 'alpha', 'zeta'] },
	{ id: 'd', price: 0, brand: '', active: false },
	{ id: 'e' },
	{ id: 'f', price: [], brand: [], active: [] },
];
const aggregations = [
	{ name: 'brands', type: 'terms', field: 'brand', size: 10 },
	{ name: 'prices', type: 'stats', field: 'price' },
	{ name: 'histogram', type: 'histogram', field: 'price', interval: 10 },
	{ name: 'ranges', type: 'range', field: 'price', ranges: [{ to: 10 }, { from: 10, to: 25 }, { from: 25 }] },
];
async function fixture(context) {
	const options = config(context);
	const index = await openNativeFullTextIndex(options);
	context.after(() => index.close({ mode: 'rollback' }));
	for (const batch of [records.slice(0, 3), records.slice(3)]) {
		await index.applyMutationBatch({
			upserts: batch.map(({ id, ...filters }) => ({
				id,
				version: `v-${id}`,
				fields: { title: 'running shoe' },
				filters,
			})),
		});
		await index.commit();
	}
	await index.reload();
	return { index, options };
}
const ids = (result) => result.hits.map((hit) => hit.id);
const sort = (field, direction = 'asc', missing = 'last') => ({ field, direction, missing });

test('metadata sorting is global, deterministic, preserves scores, and selects first array values', async (context) => {
	const { index } = await fixture(context);
	const baseline = await index.search({ text: 'shoe', limit: 10, exactTotal: true });
	const scores = new Map(baseline.hits.map((hit) => [hit.id, hit.score]));
	for (const [key, expected] of [
		[sort('price'), ['d', 'a', 'b', 'c', 'e', 'f']],
		[sort('price', 'desc'), ['c', 'b', 'a', 'd', 'e', 'f']],
		[sort('price', 'asc', 'first'), ['e', 'f', 'd', 'a', 'b', 'c']],
		[sort('price', 'desc', 'first'), ['e', 'f', 'c', 'b', 'a', 'd']],
		[sort('brand'), ['d', 'b', 'a', 'c', 'e', 'f']],
		[sort('brand', 'desc'), ['c', 'a', 'b', 'd', 'e', 'f']],
		[sort('brand', 'asc', 'first'), ['e', 'f', 'd', 'b', 'a', 'c']],
		[sort('brand', 'desc', 'first'), ['e', 'f', 'c', 'a', 'b', 'd']],
	]) {
		const result = await index.search({ text: 'shoe', sort: key, limit: 10 });
		assert.deepStrictEqual(ids(result), expected);
		assert.strictEqual(result.total, 6);
		assert.strictEqual(result.totalRelation, 'exact');
		for (const hit of result.hits) {
			assert.strictEqual(hit.score, scores.get(hit.id));
			assert.strictEqual(hit.version, `v-${hit.id}`);
		}
	}
	const paged = await index.search({ text: 'shoe', sort: sort('price'), offset: 2, limit: 2 });
	assert.deepStrictEqual(ids(paged), ['b', 'c']);
	assert.deepStrictEqual(ids(await index.search({ text: 'shoe', sort: sort('price'), offset: 8, limit: 2 })), []);
});

test('equal metadata uses ID order before distinct BM25 scores and exact aggregations span the ranked window', async (context) => {
	const options = config(context);
	const index = await openNativeFullTextIndex(options);
	context.after(() => index.close({ mode: 'rollback' }));
	await index.applyMutationBatch({
		upserts: [
			{ id: 'a', fields: { title: `shoe ${'padding '.repeat(100)}` }, filters: { price: 7, brand: 'same' } },
			{ id: 'm', fields: { title: `shoe ${'padding '.repeat(10)}` }, filters: { price: 7, brand: 'same' } },
			{ id: 'z', fields: { title: 'shoe' }, filters: { price: 7, brand: 'same' } },
			{ id: 'unmatched', fields: { title: 'boots' }, filters: { price: 999, brand: 'other' } },
		],
	});
	await index.commit();
	await index.reload();
	const ordinary = await index.search({ text: 'shoe', limit: 10, exactTotal: true });
	assert.deepStrictEqual(ids(ordinary), ['z', 'm', 'a']);
	const scores = new Map(ordinary.hits.map(({ id, score }) => [id, score]));
	assert(scores.get('z') > scores.get('m'));
	assert(scores.get('m') > scores.get('a'));
	for (const field of ['price', 'brand']) {
		for (const direction of ['asc', 'desc']) {
			const result = await index.search({ text: 'shoe', sort: sort(field, direction), limit: 10 });
			assert.deepStrictEqual(ids(result), ['a', 'm', 'z']);
			for (const hit of result.hits) assert.strictEqual(hit.score, scores.get(hit.id));
			const paged = [];
			for (let offset = 0; offset < 3; offset++) {
				const page = await index.search({ text: 'shoe', sort: sort(field, direction), offset, limit: 1 });
				assert.strictEqual(page.total, 3);
				assert.strictEqual(page.totalRelation, 'exact');
				assert.strictEqual(page.hits[0].score, scores.get(page.hits[0].id));
				paged.push(...ids(page));
			}
			assert.deepStrictEqual(paged, ['a', 'm', 'z']);
		}
	}
	const requests = [
		{ name: 'prices', field: 'price', type: 'stats' },
		{ name: 'brands', field: 'brand', type: 'terms', size: 10 },
	];
	const complete = await index.search({ text: 'shoe', limit: 10, exactTotal: true, aggregations: requests });
	const window = await index.search({ text: 'shoe', limit: 1, offset: 1, exactTotal: true, aggregations: requests });
	assert.deepStrictEqual(ids(window), ['m']);
	assert.strictEqual(window.hits[0].score, scores.get('m'));
	assert.strictEqual(window.total, 3);
	assert.strictEqual(window.totalRelation, 'exact');
	assert.deepStrictEqual(window.aggregations, complete.aggregations);
	assert.deepStrictEqual(window.aggregations, [
		{ name: 'prices', type: 'stats', count: 3, sum: 21, min: 7, max: 7, avg: 7 },
		{
			name: 'brands',
			type: 'terms',
			buckets: [{ key: 'same', count: 3 }],
			sumOtherDocCount: 0,
			docCountErrorUpperBound: 0,
		},
	]);
});

test('aggregations cover the full matching snapshot and deduplicate string values per document', async (context) => {
	const { index } = await fixture(context);
	const result = await index.search({ text: 'shoe', limit: 1, aggregations, exactTotal: true });
	assert.strictEqual(result.hits.length, 1);
	assert.strictEqual(result.total, 6);
	assert.strictEqual(result.totalRelation, 'exact');
	const approximate = await index.search({ text: 'shoe', limit: 1, aggregations });
	assert.strictEqual(approximate.totalRelation, 'lower-bound');
	assert.deepStrictEqual(approximate.aggregations, result.aggregations);
	const [terms, stats, histogram, ranges] = result.aggregations;
	assert.strictEqual(terms.type, 'terms');
	assert.deepStrictEqual(
		new Map(terms.buckets.map((b) => [b.key, b.count])),
		new Map([
			['alpha', 2],
			['beta', 1],
			['zeta', 1],
			['', 1],
		]),
	);
	assert.strictEqual(terms.sumOtherDocCount, 0);
	assert.strictEqual(terms.docCountErrorUpperBound, 0);
	assert.deepStrictEqual(stats, { name: 'prices', type: 'stats', count: 5, sum: 65, min: 0, max: 30, avg: 13 });
	assert.deepStrictEqual(histogram.buckets, [
		{ key: 0, count: 2 },
		{ key: 10, count: 1 },
		{ key: 20, count: 1 },
		{ key: 30, count: 1 },
	]);
	assert.deepStrictEqual(
		ranges.buckets.map(({ from, to, count }) => ({ from, to, count })),
		[
			{ from: undefined, to: 10, count: 2 },
			{ from: 10, to: 25, count: 2 },
			{ from: 25, to: undefined, count: 1 },
		],
	);
	const combined = await index.search({ text: 'shoe', sort: sort('price'), offset: 2, limit: 1, aggregations });
	assert.deepStrictEqual(ids(combined), ['b']);
	assert.deepStrictEqual(combined.aggregations, result.aggregations);
	const candidates = await index.search({
		text: 'shoe',
		candidateIds: ['b', 'c', 'e'],
		limit: 1,
		aggregations: [aggregations[1]],
		exactTotal: true,
	});
	assert.strictEqual(candidates.total, 3);
	assert.strictEqual(candidates.aggregations[0].sum, 55);
	const filtered = await index.search({
		text: 'shoe',
		filter: { operator: 'not', clause: { field: 'active', comparator: 'equals', value: true } },
		limit: 1,
		aggregations: [aggregations[1]],
		exactTotal: true,
	});
	assert.strictEqual(filtered.total, 5);
	assert.strictEqual(filtered.aggregations[0].sum, 45);
	const empty = await index.search({ text: 'unmatched', aggregations });
	assert.strictEqual(empty.total, 0);
	assert.deepStrictEqual(empty.aggregations[1], {
		name: 'prices',
		type: 'stats',
		count: 0,
		sum: 0,
		min: null,
		max: null,
		avg: null,
	});
});

test('retained readers keep sort and aggregates on one snapshot across deletes and commits', async (context) => {
	const { index, options } = await fixture(context);
	const reader = await openNativeFullTextReader(options);
	context.after(() => reader.close());
	const before = await reader.search({ text: 'shoe', sort: sort('price'), aggregations, limit: 10 });
	await index.applyMutationBatch({
		deletes: ['b'],
		upserts: [{ id: 'new', fields: { title: 'shoe' }, filters: { price: 100, brand: 'new' } }],
	});
	await index.commit();
	await index.reload();
	assert.deepStrictEqual(await reader.search({ text: 'shoe', sort: sort('price'), aggregations, limit: 10 }), before);
	const after = await index.search({ text: 'shoe', sort: sort('price'), aggregations, limit: 10 });
	assert(!ids(after).includes('b'));
	assert(ids(after).includes('new'));
	assert.strictEqual(after.aggregations[1].sum, 145);
	assert(!after.aggregations[0].buckets.some((bucket) => bucket.key === 'alpha' && bucket.count === 2));
});

test('sort ties use UTF-8 IDs across commits and reload', async (context) => {
	const options = config(context);
	const index = await openNativeFullTextIndex(options);
	context.after(() => index.close({ mode: 'rollback' }));
	const input = ['😀', 'z', 'é', 'a', '𐀀', '\uE000'];
	for (const id of input) {
		await index.applyMutationBatch({
			upserts: [{ id, fields: { title: 'shoe' }, filters: { price: 1, brand: 'same' } }],
		});
		await index.commit();
	}
	await index.reload();
	const expected = [...input].sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)));
	for (const field of ['price', 'brand']) {
		const actual = [];
		for (let offset = 0; offset < input.length; offset += 2) {
			actual.push(...ids(await index.search({ text: 'shoe', sort: sort(field), offset, limit: 2 })));
		}
		assert.deepStrictEqual(actual, expected);
	}
});

test('catalog errors are explicit and leave the handle usable', async (context) => {
	const { index } = await fixture(context);
	for (const request of [
		{ sort: sort('active') },
		{ sort: sort('__fulltext_id') },
		{ sort: sort('unknown') },
		{ aggregations: [{ name: 'bad', type: 'stats', field: 'brand' }] },
		{ aggregations: [{ name: 'bad', type: 'terms', field: 'price', size: 10 }] },
		{
			aggregations: [
				{ name: 'x', type: 'stats', field: 'price' },
				{ name: 'x', type: 'stats', field: 'price' },
			],
		},
		{ aggregations: [{ name: 'bad', type: 'histogram', field: 'price', interval: 0 }] },
		{ aggregations: [{ name: 'bad', type: 'range', field: 'price', ranges: [{ from: 2, to: 1 }] }] },
	])
		await assert.rejects(index.search({ text: 'shoe', ...request }), { code: 'E_INVALID_ARGUMENT' });
	await assert.rejects(index.search({ text: 'sh', mode: 'prefix', sort: sort('price'), offset: 100, limit: 100 }), {
		code: 'E_INVALID_ARGUMENT',
		message: /metadata continuation is not supported/,
	});
	await assert.rejects(index.search({ text: 'shoe', sort: sort('price'), offset: 10_000, limit: 1 }), {
		code: 'E_INVALID_ARGUMENT',
		message: /metadata continuation is not supported/,
	});
	await assert.rejects(index.searchPage({ text: 'shoe', sort: sort('price') }), { code: 'E_INVALID_ARGUMENT' });
	await assert.rejects(index.searchPage({ text: 'shoe', aggregations }), { code: 'E_INVALID_ARGUMENT' });
	assert.strictEqual((await index.search({ text: 'shoe', sort: sort('brand'), limit: 10_000 })).hits.length, 6);
	await assert.rejects(
		index.search({
			text: 'shoe',
			aggregations: [{ name: 'bad', type: 'histogram', field: 'price', interval: Number.MIN_VALUE }],
		}),
		{ code: 'E_RESOURCE_LIMIT' },
	);
	assert.strictEqual((await index.search({ text: 'shoe', exactTotal: true })).total, 6);
});

test('sort key memory rejection preserves smaller sorted windows and ordinary searches', async (context) => {
	const options = config(context, [{ name: 'brand', type: 'string' }]);
	const index = await openNativeFullTextIndex(options);
	context.after(() => index.close({ mode: 'rollback' }));
	const count = 1200;
	const padding = 'x'.repeat(60_000);
	for (let offset = 0; offset < count; offset += 100) {
		await index.applyMutationBatch({
			upserts: Array.from({ length: 100 }, (_, position) => {
				const id = String(offset + position).padStart(4, '0');
				return {
					id,
					fields: { title: 'shoe' },
					filters: { brand: offset + position === count - 1 ? `${id}${padding}` : id },
				};
			}),
		});
	}
	await index.commit();
	await index.reload();
	await assert.rejects(index.search({ text: 'shoe', sort: sort('brand'), limit: count }), (error) => {
		assert(error instanceof FulltextError);
		assert.strictEqual(error.code, 'E_RESOURCE_LIMIT');
		assert.strictEqual(error.message, 'Retained keys exceed materialization memory budget');
		return true;
	});
	const smaller = await index.search({ text: 'shoe', sort: sort('brand'), limit: 3 });
	assert.deepStrictEqual(ids(smaller), ['0000', '0001', '0002']);
	assert.strictEqual(smaller.total, count);
	assert.strictEqual(smaller.totalRelation, 'exact');
	const ordinary = await index.search({ text: 'shoe', limit: 3, exactTotal: true });
	assert.strictEqual(ordinary.hits.length, 3);
	assert.strictEqual(ordinary.total, count);
	assert.strictEqual(ordinary.totalRelation, 'exact');
});

test('raw packed catalog requests use the same native contract', async (context) => {
	const { index, options } = await fixture(context);
	const addon = loadAddon();
	const invoke = (call) =>
		new Promise((resolve, reject) => {
			try {
				call((value) => {
					try {
						resolve(decodeResponse(value));
					} catch (error) {
						reject(error);
					}
				});
			} catch (error) {
				reject(error);
			}
		});
	const opened = await invoke((callback) =>
		addon.__nativeOpenReader(
			encodeOpen({
				...options,
				fields: [{ name: 'title', weight: 1 }],
				positions: true,
				stopWords: true,
				surfaceTerms: true,
				synonyms: [],
			}),
			callback,
		),
	);
	const handle = opened.u32();
	context.after(() => invoke((callback) => addon.__nativeClose(handle, true, callback)));
	const request = {
		expression: { text: 'shoe', mode: 'any', fields: [] },
		offset: 0,
		limit: 1,
		exactTotal: true,
		budgetMilliseconds: 30_000,
		sort: sort('price'),
		aggregations: [aggregations[1]],
	};
	const result = await invoke((callback) => addon.__nativeSearch(handle, encodeSearch(request), callback));
	assert.strictEqual(result.u64(), 6n);
	assert.strictEqual(result.u8(), 0);
	assert.strictEqual(result.u32(), 1);
	const expected = await index.search({
		text: 'shoe',
		sort: sort('price'),
		limit: 1,
		exactTotal: true,
		aggregations: [aggregations[1]],
	});
	assert.strictEqual(result.f32(), expected.hits[0].score);
	assert.strictEqual(result.string(), 'd');
	assert.strictEqual(result.u8(), 1);
	assert.strictEqual(result.string(), 'v-d');
	assert.deepStrictEqual(decodeAggregations(result), expected.aggregations);
	result.finish();
	const packed = encodeSearch(request);
	packed[4] = 5;
	await assert.rejects(
		invoke((callback) => addon.__nativeSearch(handle, packed, callback)),
		{ code: 'E_INVALID_ARGUMENT' },
	);
	await assert.rejects(
		invoke((callback) => addon.__nativeSearch(handle, encodeSearch({ ...request, sort: sort('active') }), callback)),
		{ code: 'E_INVALID_ARGUMENT' },
	);
	assert.strictEqual((await index.search({ text: 'shoe', sort: sort('price'), limit: 1 })).hits[0].id, 'd');
});

test('identity migration preserves numeric/Boolean-only v7 and rejects string v7 before mutation', async (context) => {
	for (const fields of [
		[
			{ name: 'price', type: 'number' },
			{ name: 'active', type: 'boolean' },
		],
		[{ name: 'brand', type: 'string' }],
	]) {
		const options = config(context, fields);
		let index = await openNativeFullTextIndex(options);
		await index.commit();
		await index.close({ mode: 'rollback' });
		const file = path.join(options.path, '.harper-fulltext-identity');
		const bytes = readFileSync(file);
		const string = fields[0].type === 'string';
		assert.strictEqual(bytes[4], string ? 8 : 7);
		if (string) {
			bytes[4] = 7;
			writeFileSync(file, bytes);
			assert.deepStrictEqual(inspectNativeFullTextIndex(options), {
				state: 'incompatible',
				code: 'E_IDENTITY_MISMATCH',
			});
			await assert.rejects(openNativeFullTextIndex(options), { code: 'E_IDENTITY_MISMATCH' });
		} else {
			index = await openNativeFullTextIndex(options);
			await index.close({ mode: 'rollback' });
		}
		const reset = await resetNativeFullTextIndex({ path: options.path, indexId: options.indexId });
		assert.strictEqual(reset.state, 'reset');
	}
	const options = config(context);
	const index = await openNativeFullTextIndex(options);
	await index.commit();
	await index.close({ mode: 'rollback' });
	assert.strictEqual((await resetNativeFullTextIndex({ path: options.path, indexId: options.indexId })).state, 'reset');
});

test('native metadata sort preserves scorer bits for Boolean and expensive query modes', async (context) => {
	const { index } = await fixture(context);
	const queries = [
		{ text: 'running shoe', mode: 'any' },
		{ text: 'running shoe', mode: 'all' },
		{ text: 'running shoe', mode: 'phrase' },
		{ text: 'runn', mode: 'prefix' },
		{ text: 'shoo', mode: 'fuzzy' },
		{ text: 'runnn', mode: 'fuzzy-prefix' },
		{
			query: {
				operator: 'or',
				clauses: [
					{ text: 'running', fields: ['title'] },
					{ operator: 'and', clauses: [{ text: 'shoe' }, { operator: 'not', clause: { text: 'boots' } }] },
				],
			},
		},
	];
	for (const query of queries) {
		const ordinary = await index.search({ ...query, limit: 10, exactTotal: true });
		assert(ordinary.hits.length > 0, JSON.stringify(query));
		const sorted = await index.search({ ...query, limit: 10, sort: sort('price'), exactTotal: true });
		assert.strictEqual(sorted.total, ordinary.total);
		const expected = new Map(ordinary.hits.map((hit) => [hit.id, hit.score]));
		for (const hit of sorted.hits) assert.strictEqual(hit.score, expected.get(hit.id));
	}
});
