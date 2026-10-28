import assert from 'node:assert';
import test from 'node:test';
import { decodeAggregations, encodeCatalog, normalizeCatalog } from '../dist/catalog.js';

const invalid = (error) => error.code === 'E_INVALID_ARGUMENT';
const nativeFailure = (error) => error.code === 'E_NATIVE_FAILURE';

test('an absent aggregation payload needs only its count byte', () => {
	assert.strictEqual(decodeAggregations({ u8: () => 0 }), undefined);
});

test('catalog normalization snapshots all mutable request containers', () => {
	const sort = { field: 'price', direction: 'asc', missing: 'last' };
	const requests = [
		{ name: 'categories', field: 'category', type: 'terms', size: 10 },
		{ name: 'summary', field: 'price', type: 'stats' },
		{ name: 'prices', field: 'price', type: 'histogram', interval: 5 },
		{ name: 'bands', field: 'price', type: 'range', ranges: [{ to: 10 }, { from: 10 }] },
	];
	const expected = structuredClone({ sort, aggregations: requests });
	const normalized = normalizeCatalog(sort, requests);
	sort.field = 'changed';
	requests[0].size = 20;
	requests[3].ranges[0].to = 30;
	requests.push({ name: 'extra', field: 'price', type: 'stats' });
	assert.deepStrictEqual(normalized, expected);
	assert.deepStrictEqual(normalizeCatalog(undefined, undefined), { sort: undefined, aggregations: [] });
});

test('catalog normalization rejects malformed requests and respects UTF-8 bounds', () => {
	for (const sort of [
		null,
		[],
		{},
		{ field: 'price', direction: 'asc' },
		{ field: '', direction: 'asc', missing: 'first' },
	]) {
		assert.throws(() => normalizeCatalog(sort, undefined), invalid);
	}
	const base = { name: 'count', field: 'category', type: 'terms', size: 1 };
	for (const requests of [
		null,
		{},
		Array(1),
		Array.from({ length: 9 }, (_, index) => ({ ...base, name: String(index) })),
		[base, base],
		[{ ...base, name: 'é'.repeat(65) }],
		[{ ...base, name: '\ud800' }],
		[{ ...base, field: 'a'.repeat((1 << 20) + 1) }],
		[{ ...base, type: 'unknown' }],
		...[0, 101, 1.5, NaN, Infinity].map((size) => [{ ...base, size }]),
		...[0, -1, NaN, Infinity].map((interval) => [{ ...base, type: 'histogram', interval }]),
	]) {
		assert.throws(() => normalizeCatalog(undefined, requests), invalid);
	}
	assert.strictEqual(normalizeCatalog(undefined, [{ ...base, name: 'é'.repeat(64) }]).aggregations[0].name.length, 64);
});

test('range requests enforce finite contiguous non-overlapping bounds', () => {
	const request = (ranges) => [{ name: 'bands', field: 'price', type: 'range', ranges }];
	for (const ranges of [
		[],
		Array(1),
		[{ from: 1, to: 1 }],
		[{ from: 2, to: 1 }],
		[{ from: Infinity }],
		[{ to: null }],
		[{ to: 1 }, { from: 2 }],
		[{ to: 2 }, { from: 1 }],
		[{}, { from: 1 }],
		[{ to: 1 }, {}],
	]) {
		assert.throws(() => normalizeCatalog(undefined, request(ranges)), invalid);
	}
	assert.deepStrictEqual(normalizeCatalog(undefined, request([{}])).aggregations[0].ranges, [{}]);
	assert.deepStrictEqual(normalizeCatalog(undefined, request([{ from: -0, to: 1 }])).aggregations[0].ranges, [
		{ from: 0, to: 1 },
	]);
});

test('catalog encoding uses the agreed packed field order', () => {
	const calls = [];
	const writer = Object.fromEntries(
		['boolean', 'string', 'u8', 'u32', 'f64'].map((method) => [method, (value) => calls.push([method, value])]),
	);
	const normalized = normalizeCatalog({ field: 'price', direction: 'desc', missing: 'first' }, [
		{ name: 'terms', field: 'category', type: 'terms', size: 2 },
		{ name: 'stats', field: 'price', type: 'stats' },
		{ name: 'hist', field: 'price', type: 'histogram', interval: 2.5 },
		{ name: 'range', field: 'price', type: 'range', ranges: [{ to: 5 }, { from: 5 }] },
	]);
	encodeCatalog(writer, normalized.sort, normalized.aggregations);
	assert.deepStrictEqual(calls, [
		['boolean', true],
		['string', 'price'],
		['u8', 1],
		['u8', 0],
		['u8', 4],
		['string', 'terms'],
		['string', 'category'],
		['u8', 0],
		['u32', 2],
		['string', 'stats'],
		['string', 'price'],
		['u8', 1],
		['string', 'hist'],
		['string', 'price'],
		['u8', 2],
		['f64', 2.5],
		['string', 'range'],
		['string', 'price'],
		['u8', 3],
		['u8', 2],
		['boolean', false],
		['boolean', true],
		['f64', 5],
		['boolean', true],
		['f64', 5],
		['boolean', false],
	]);
});

function cursor(values) {
	let offset = 0;
	return Object.fromEntries(
		['u8', 'u16', 'u64', 'f64', 'string'].map((method) => [
			method,
			() => {
				if (offset === values.length) throw new Error('truncated fixture');
				return values[offset++];
			},
		]),
	);
}

test('catalog decoding preserves typed buckets, approximation metadata and empty stats', () => {
	assert.deepStrictEqual(
		decodeAggregations(
			cursor([
				5,
				'categories',
				0,
				1,
				'café',
				3n,
				2n,
				1,
				1n,
				'prices',
				1,
				3n,
				15,
				1,
				1,
				1,
				9,
				1,
				5,
				'empty',
				1,
				0n,
				0,
				0,
				0,
				0,
				'hist',
				2,
				1,
				10,
				2n,
				'bands',
				3,
				2,
				'*-5',
				0,
				1,
				5,
				1n,
				'5-*',
				1,
				5,
				0,
				2n,
			]),
		),
		[
			{
				name: 'categories',
				type: 'terms',
				buckets: [{ key: 'café', count: 3 }],
				sumOtherDocCount: 2,
				docCountErrorUpperBound: 1,
			},
			{ name: 'prices', type: 'stats', count: 3, sum: 15, min: 1, max: 9, avg: 5 },
			{ name: 'empty', type: 'stats', count: 0, sum: 0, min: null, max: null, avg: null },
			{ name: 'hist', type: 'histogram', buckets: [{ key: 10, count: 2 }] },
			{
				name: 'bands',
				type: 'range',
				buckets: [
					{ key: '*-5', to: 5, count: 1 },
					{ key: '5-*', from: 5, count: 2 },
				],
			},
		],
	);
	assert.deepStrictEqual(decodeAggregations(cursor([1, 'terms', 0, 0, 0n, 0])), [
		{ name: 'terms', type: 'terms', buckets: [], sumOtherDocCount: 0, docCountErrorUpperBound: null },
	]);
});

test('catalog decoding rejects invalid native tags, counts and numeric results', () => {
	for (const values of [
		[9],
		[1, '', 1],
		[1, 'x', 9],
		[1, 'x', 0, 1025],
		[1, 'x', 0, 0, 0n, 2],
		[1, 'x', 1, 9_007_199_254_740_992n],
		[1, 'x', 1, 0n, Infinity],
		[1, 'x', 1, 0n, 0, 2],
		[1, 'x', 2, 1, NaN],
		[2, 'x', 0, 0, 0n, 0, 'x', 0],
	]) {
		assert.throws(() => decodeAggregations(cursor(values)), nativeFailure);
	}
});
