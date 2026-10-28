import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import path from 'node:path';
import { tmpdir } from 'node:os';
import test from 'node:test';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const key = (field, direction = 'asc', missing = 'last') => ({ field, direction, missing });
const bounded = (promise) => {
	let timer;
	return Promise.race([
		promise,
		new Promise((_, reject) => {
			timer = setTimeout(() => reject(new Error('metadata operation exceeded 15 seconds')), 15_000);
		}),
	]).finally(() => clearTimeout(timer));
};
async function fixture(context, batches) {
	const root = mkdtempSync(path.join(tmpdir(), 'fulltext-expanded-metadata-'));
	context.after(() => rmSync(root, { recursive: true, force: true }));
	const index = await bounded(
		openNativeFullTextIndex({
			path: path.join(root, 'index'),
			indexId: 'expanded',
			generation: '1',
			fields: [{ name: 'title' }],
			filterFields: [
				{ name: 'category', type: 'string' },
				{ name: 'brand', type: 'string' },
				{ name: 'price', type: 'number' },
			],
			analyzer: 'english@2',
			limits: {
				indexingThreads: 1,
				searchThreads: 2,
				writerMemoryBytes: 15_000_000,
				maxQueuedCommands: 32,
				maxQueuedBytes: 8 << 20,
				maxBatchBytes: 8 << 20,
			},
		}),
	);
	context.after(() => index.close({ mode: 'rollback' }));
	for (const batch of batches) {
		await bounded(
			index.applyMutationBatch({
				upserts: batch.map(({ id, ...filters }) => ({ id, fields: { title: 'running shoe' }, filters })),
			}),
		);
		await bounded(index.commit());
	}
	await bounded(index.reload());
	return index;
}
const utf8 = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b));
const first = (value) => (Array.isArray(value) ? value[0] : value);
function oracle(records, sorts) {
	return [...records]
		.sort((a, b) => {
			for (const sort of sorts) {
				const left = first(a[sort.field]);
				const right = first(b[sort.field]);
				if (left === undefined || right === undefined) {
					if (left === right) continue;
					return (left === undefined ? -1 : 1) * (sort.missing === 'first' ? 1 : -1);
				}
				const comparison = typeof left === 'string' ? utf8(left, right) : left < right ? -1 : left > right ? 1 : 0;
				if (comparison) return comparison * (sort.direction === 'asc' ? 1 : -1);
			}
			return utf8(a.id, b.id);
		})
		.map((record) => record.id);
}

test(
	'multi-field sorting matches a complete oracle across sparse segments, deletes and paged ties',
	{ timeout: 60_000 },
	async (context) => {
		const batches = [
			[
				{ id: 'a', category: 'outdoor', brand: ['zeta', 'alpha'], price: [7, 1] },
				{ id: 'é', category: 'outdoor', brand: 'same', price: 3 },
				{ id: 'z', category: 'outdoor', brand: 'same', price: 3 },
			],
			[
				{ id: 'sparse-a', price: 9 },
				{ id: 'sparse-b', price: 1 },
			],
			[
				{ id: 'b', category: 'outdoor', brand: 'same', price: 3 },
				{ id: 'empty', category: [], brand: [], price: [] },
				{ id: 'delete', category: 'outdoor', brand: 'same', price: 99 },
			],
		];
		const index = await fixture(context, batches);
		await bounded(index.applyMutationBatch({ deletes: ['delete'] }));
		await bounded(index.commit());
		await bounded(index.reload());
		const records = batches.flat().filter((record) => record.id !== 'delete');
		const baseline = await bounded(index.search({ text: 'shoe', limit: 100, exactTotal: true }));
		const scores = new Map(baseline.hits.map((hit) => [hit.id, hit.score]));
		for (const sorts of [
			[key('brand'), key('price', 'desc', 'first')],
			[key('brand', 'desc', 'first'), key('price')],
			[key('price', 'asc', 'first'), key('category', 'desc'), key('brand')],
			[key('category'), key('brand'), key('price', 'desc')],
		]) {
			const expected = oracle(records, sorts);
			const actual = [];
			for (let offset = 0; offset < records.length; offset += 2) {
				const result = await bounded(index.search({ text: 'shoe', sort: sorts, offset, limit: 2, exactTotal: true }));
				assert.strictEqual(result.total, records.length);
				for (const hit of result.hits) {
					actual.push(hit.id);
					assert.strictEqual(hit.score, scores.get(hit.id));
				}
			}
			assert.deepStrictEqual(actual, expected);
		}
	},
);

test(
	'nested native aggregations use the complete filtered candidate snapshot and deduplicate memberships',
	{ timeout: 60_000 },
	async (context) => {
		const records = [
			{ id: 'a', category: ['outdoor', 'outdoor'], brand: ['cedar', 'cedar'], price: 5 },
			{ id: 'b', category: 'outdoor', brand: 'pine', price: 15 },
			{ id: 'c', category: 'indoor', brand: 'cedar', price: 25 },
			{ id: 'd', category: 'outdoor', brand: 'cedar', price: 35 },
		];
		const index = await fixture(
			context,
			records.map((record) => [record]),
		);
		const aggregations = [
			{
				name: 'categories',
				field: 'category',
				type: 'terms',
				size: 100,
				aggregations: [
					{
						name: 'brands',
						field: 'brand',
						type: 'terms',
						size: 100,
						aggregations: [{ name: 'prices', field: 'price', type: 'stats' }],
					},
					{
						name: 'hist',
						field: 'price',
						type: 'histogram',
						interval: 10,
						aggregations: [{ name: 'prices', field: 'price', type: 'stats' }],
					},
					{
						name: 'ranges',
						field: 'price',
						type: 'range',
						ranges: [{ to: 10 }, { from: 10 }],
						aggregations: [{ name: 'brands', field: 'brand', type: 'terms', size: 100 }],
					},
				],
			},
		];
		const result = await bounded(
			index.search({
				text: 'shoe',
				candidateIds: ['a', 'b', 'c'],
				filter: { field: 'category', comparator: 'equals', value: 'outdoor' },
				aggregations,
				limit: 1,
				exactTotal: true,
			}),
		);
		assert.strictEqual(result.total, 2);
		assert.strictEqual(result.hits.length, 1);
		const outdoor = result.aggregations[0].buckets;
		assert.strictEqual(outdoor.length, 1);
		assert.strictEqual(outdoor[0].key, 'outdoor');
		assert.strictEqual(outdoor[0].count, 2);
		const [brands, hist, ranges] = outdoor[0].aggregations;
		assert.deepStrictEqual(
			new Map(brands.buckets.map((bucket) => [bucket.key, bucket.count])),
			new Map([
				['cedar', 1],
				['pine', 1],
			]),
		);
		assert.deepStrictEqual(
			brands.buckets.map((bucket) => bucket.aggregations[0].sum).sort((a, b) => a - b),
			[5, 15],
		);
		assert.deepStrictEqual(
			hist.buckets.map((bucket) => [bucket.key, bucket.count, bucket.aggregations[0].sum]),
			[
				[0, 1, 5],
				[10, 1, 15],
			],
		);
		assert.deepStrictEqual(
			ranges.buckets.map((bucket) => [bucket.count, bucket.aggregations[0].buckets[0].count]),
			[
				[1, 1],
				[1, 1],
			],
		);
	},
);

test(
	'resource budgets replace the eight aggregation and hundred bucket request caps',
	{ timeout: 60_000 },
	async (context) => {
		const records = Array.from({ length: 120 }, (_, index) => ({
			id: String(index),
			brand: `brand-${String(index).padStart(3, '0')}`,
			price: index,
		}));
		const index = await fixture(context, [records]);
		const siblings = Array.from({ length: 12 }, (_, i) => ({ name: `stats-${i}`, field: 'price', type: 'stats' }));
		const result = await bounded(
			index.search({
				text: 'shoe',
				limit: 1,
				aggregations: [
					...siblings,
					{ name: 'brands', field: 'brand', type: 'terms', size: 120 },
					{
						name: 'ranges',
						field: 'price',
						type: 'range',
						ranges: Array.from({ length: 120 }, (_, i) => ({ from: i, to: i + 1 })),
					},
				],
			}),
		);
		assert.strictEqual(result.aggregations.length, 14);
		assert.strictEqual(result.aggregations[12].buckets.length, 120);
		assert.strictEqual(result.aggregations[13].buckets.filter((bucket) => bucket.count).length, 120);
		for (const stats of result.aggregations.slice(0, 12)) assert.strictEqual(stats.count, 120);
	},
);

test(
	'cyclic, deep and collector-heavy requests fail with typed errors and preserve handle usability',
	{ timeout: 60_000 },
	async (context) => {
		const index = await fixture(context, [
			[{ id: 'a', brand: 'cedar', price: 1 }],
			[{ id: 'b', price: 2 }],
			[{ id: 'c', brand: 'pine', price: 3 }],
		]);
		const cyclic = { name: 'cycle', field: 'brand', type: 'terms', size: 1 };
		cyclic.aggregations = [cyclic];
		let deep = { name: 'leaf', field: 'price', type: 'stats' };
		for (let i = 0; i < 32; i++)
			deep = { name: 'branch', field: 'brand', type: 'terms', size: 1, aggregations: [deep] };
		for (const aggregations of [
			[cyclic],
			[deep],
			Array.from({ length: 50_000 }, (_, i) => ({ name: String(i), field: 'price', type: 'stats' })),
		]) {
			await assert.rejects(bounded(index.search({ text: 'shoe', aggregations })), (error) =>
				['E_INVALID_ARGUMENT', 'E_RESOURCE_LIMIT'].includes(error.code),
			);
			assert.strictEqual((await bounded(index.search({ text: 'shoe', limit: 1, exactTotal: true }))).total, 3);
		}
	},
);
