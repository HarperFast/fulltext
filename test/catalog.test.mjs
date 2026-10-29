import assert from 'node:assert';
import test from 'node:test';
import { decodeAggregations, encodeCatalog, normalizeCatalog } from '../dist/catalog.js';

const invalid = (error) => error.code === 'E_INVALID_ARGUMENT';
const nativeFailure = (error) => error.code === 'E_NATIVE_FAILURE';

test('an absent aggregation payload needs only its count word', () => {
	assert.strictEqual(decodeAggregations({ u32: () => 0 }), undefined);
});

test('catalog normalization snapshots all mutable request containers', () => {
	const sort = { field: 'price', direction: 'asc', missing: 'last' };
	const requests = [
		{ name: 'categories', field: 'category', type: 'terms', size: 10 },
		{ name: 'summary', field: 'price', type: 'stats' },
		{ name: 'prices', field: 'price', type: 'histogram', interval: 5 },
		{ name: 'bands', field: 'price', type: 'range', ranges: [{ to: 10 }, { from: 10 }] },
	];
	const expected = structuredClone({ sort: [sort], aggregations: requests });
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
		[base, base],
		[{ ...base, name: 'é'.repeat(65) }],
		[{ ...base, name: '\ud800' }],
		[{ ...base, field: 'a'.repeat((1 << 20) + 1) }],
		[{ ...base, type: 'unknown' }],
		...[0, 0x100000000, 1.5, NaN, Infinity].map((size) => [{ ...base, size }]),
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
		['u32', 1],
		['string', 'price'],
		['u8', 1],
		['u8', 0],
		['u32', 4],
		['string', 'terms'],
		['string', 'category'],
		['u8', 0],
		['u32', 2],
		['u32', 0],
		['string', 'stats'],
		['string', 'price'],
		['u8', 1],
		['u32', 0],
		['string', 'hist'],
		['string', 'price'],
		['u8', 2],
		['f64', 2.5],
		['u32', 0],
		['string', 'range'],
		['string', 'price'],
		['u8', 3],
		['u32', 2],
		['boolean', false],
		['boolean', true],
		['f64', 5],
		['boolean', true],
		['f64', 5],
		['boolean', false],
		['u32', 0],
	]);
});

function cursor(values) {
	let offset = 0;
	return {
		remaining: () => 8 * 1024 * 1024,
		...Object.fromEntries(
			['u8', 'u32', 'u64', 'f64', 'string'].map((method) => [
				method,
				() => {
					if (offset === values.length) throw new Error('truncated fixture');
					return values[offset++];
				},
			]),
		),
	};
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
				0,
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
				0,
				'bands',
				3,
				2,
				'*-5',
				0,
				1,
				5,
				1n,
				0,
				'5-*',
				1,
				5,
				0,
				2n,
				0,
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
		[0xffffffff],
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

test('catalog supports ordered keys and nested trees above the old shape caps', () => {
	const sort = [
		{ field: 'brand', direction: 'asc', missing: 'last' },
		{ field: 'price', direction: 'desc', missing: 'first' },
	];
	const aggregations = Array.from({ length: 9 }, (_, index) => ({
		name: String(index),
		field: 'brand',
		type: 'terms',
		size: 101,
		aggregations: [{ name: 'summary', field: 'price', type: 'stats' }],
	}));
	const normalized = normalizeCatalog(sort, aggregations);
	assert.deepStrictEqual(normalized, { sort, aggregations });
	aggregations[0].aggregations[0].field = 'changed';
	assert.strictEqual(normalized.aggregations[0].aggregations[0].field, 'price');
	const ranges = Array.from({ length: 101 }, (_, index) => ({ from: index, to: index + 1 }));
	assert.strictEqual(
		normalizeCatalog(undefined, [{ name: 'ranges', field: 'price', type: 'range', ranges }]).aggregations[0].ranges
			.length,
		101,
	);
	assert.throws(() => normalizeCatalog([sort[0], sort[0]], undefined), invalid);
});

test('catalog rejects cycles, excessive depth and oversized requests before unbounded cloning', () => {
	const cyclic = { name: 'cycle', field: 'brand', type: 'terms', size: 1 };
	cyclic.aggregations = [cyclic];
	assert.throws(() => normalizeCatalog(undefined, [cyclic]), invalid);
	assert.throws(
		() => normalizeCatalog(undefined, [{ name: 'stats', field: 'price', type: 'stats', aggregations: [] }]),
		invalid,
	);
	let tree = { name: 'leaf', field: 'price', type: 'stats' };
	for (let index = 0; index < 31; index++)
		tree = { name: 'node', field: 'brand', type: 'terms', size: 1, aggregations: [tree] };
	assert.doesNotThrow(() => normalizeCatalog(undefined, [tree]));
	tree = { name: 'too-deep', field: 'brand', type: 'terms', size: 1, aggregations: [tree] };
	assert.throws(() => normalizeCatalog(undefined, [tree]), invalid);
	const large = Array.from({ length: 9 }, (_, index) => ({
		name: String(index),
		field: 'a'.repeat(1 << 20),
		type: 'stats',
	}));
	assert.throws(() => normalizeCatalog(undefined, large), invalid);
});

test('catalog decoding retains nested stats and shares the bucket budget across branches', () => {
	const result = decodeAggregations(
		cursor([1, 'brands', 0, 1, 'cedar', 2n, 1, 'price', 1, 2n, 10, 1, 4, 1, 6, 1, 5, 0n, 0]),
	);
	assert.deepStrictEqual(result[0].buckets[0], {
		key: 'cedar',
		count: 2,
		aggregations: [{ name: 'price', type: 'stats', count: 2, sum: 10, min: 4, max: 6, avg: 5 }],
	});
	assert.throws(
		() => decodeAggregations(cursor([1, 'brands', 0, 1, 'cedar', 2n, 1, 'nested', 2, 1024])),
		nativeFailure,
	);
	assert.throws(() => decodeAggregations({ u32: () => 100, remaining: () => 10 }), nativeFailure);
});

test('catalog encoding writes children after each request payload and decoding rejects deep responses', () => {
	const calls = [];
	const writer = Object.fromEntries(
		['boolean', 'string', 'u8', 'u32', 'f64'].map((method) => [method, (value) => calls.push([method, value])]),
	);
	const request = normalizeCatalog(undefined, [
		{
			name: 'brands',
			field: 'brand',
			type: 'terms',
			size: 101,
			aggregations: [{ name: 'summary', field: 'price', type: 'stats' }],
		},
	]);
	encodeCatalog(writer, request.sort, request.aggregations);
	assert.deepStrictEqual(calls, [
		['u32', 0],
		['u32', 1],
		['string', 'brands'],
		['string', 'brand'],
		['u8', 0],
		['u32', 101],
		['u32', 1],
		['string', 'summary'],
		['string', 'price'],
		['u8', 1],
		['u32', 0],
	]);
	const values = [];
	for (let index = 0; index < 33; index++) values.push(1, 'branch', 0, 1, 'key', 1n);
	assert.throws(() => decodeAggregations(cursor(values)), nativeFailure);
});
