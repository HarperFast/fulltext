import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import path from 'node:path';
import { tmpdir } from 'node:os';
import test from 'node:test';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const key = (field, direction = 'asc', missing = 'last') => ({
	field,
	direction,
	missing,
});
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
				{ name: 'rating', type: 'number' },
				{ name: 'stock', type: 'number' },
				{ name: 'weight', type: 'number' },
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
	context.after(async () => {
		const result = await index.close({ mode: 'rollback' });
		if (result.cleanupError) throw result.cleanupError;
		assert.strictEqual(index.status().state, 'closed', 'close must finish before removing the index directory');
		rmSync(root, { recursive: true, force: true });
	});
	for (const batch of batches) {
		await bounded(
			index.applyMutationBatch({
				upserts: batch.map(({ id, ...filters }) => ({
					id,
					fields: { title: 'running shoe' },
					filters,
				})),
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
				{
					id: 'a',
					category: 'outdoor',
					brand: ['zeta', 'alpha'],
					price: [7, 1],
				},
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
				const result = await bounded(
					index.search({
						text: 'shoe',
						sort: sorts,
						offset,
						limit: 2,
						exactTotal: true,
					}),
				);
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
			{
				id: 'a',
				category: ['outdoor', 'outdoor'],
				brand: ['cedar', 'cedar'],
				price: 5,
			},
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
		const siblings = Array.from({ length: 12 }, (_, i) => ({
			name: `stats-${i}`,
			field: 'price',
			type: 'stats',
		}));
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
						ranges: Array.from({ length: 120 }, (_, i) => ({
							from: i,
							to: i + 1,
						})),
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
			deep = {
				name: 'branch',
				field: 'brand',
				type: 'terms',
				size: 1,
				aggregations: [deep],
			};
		for (const aggregations of [
			[cyclic],
			[deep],
			Array.from({ length: 50_000 }, (_, i) => ({
				name: String(i),
				field: 'price',
				type: 'stats',
			})),
		]) {
			await assert.rejects(bounded(index.search({ text: 'shoe', aggregations })), (error) =>
				['E_INVALID_ARGUMENT', 'E_RESOURCE_LIMIT'].includes(error.code),
			);
			assert.strictEqual((await bounded(index.search({ text: 'shoe', limit: 1, exactTotal: true }))).total, 3);
		}
	},
);

test(
	'native tuple and runtime sort keys retain ordering after top-k thresholds engage',
	{ timeout: 60_000 },
	async (context) => {
		const records = Array.from({ length: 300 }, (_, index) => ({
			id: `record-${String(index).padStart(3, '0')}`,
			category: `category-${index % 3}`,
			brand: `brand-${index % 4}`,
			price: index % 5,
			rating: index % 2,
			...(index >= 100 && index < 200 ? {} : { stock: index % 7 }),
			weight: (index * 73) % 301,
		}));
		const zeros = [
			{
				id: 'zero-a',
				category: 'same',
				brand: 'same',
				price: 0,
				rating: 0,
				stock: 0,
				weight: +0,
			},
			{
				id: 'zero-z',
				category: 'same',
				brand: 'same',
				price: 0,
				rating: 0,
				stock: 0,
				weight: -0,
			},
		];
		records.push(...zeros);
		const index = await fixture(context, [records.slice(0, 100), records.slice(100, 200), records.slice(200)]);
		const baseline = await bounded(index.search({ text: 'shoe', limit: records.length, exactTotal: true }));
		const scores = new Map(baseline.hits.map((hit) => [hit.id, hit.score]));
		for (const sorts of [
			[
				key('category'),
				key('brand', 'desc'),
				key('price'),
				key('rating', 'desc'),
				key('stock', 'asc', 'first'),
				key('weight', 'desc'),
			],
			[
				key('stock', 'desc', 'last'),
				key('category', 'desc'),
				key('brand'),
				key('price', 'desc'),
				key('rating'),
				key('weight'),
			],
		].flatMap((sorts) => [sorts.slice(0, 3), sorts.slice(0, 4), sorts])) {
			const expected = oracle(records, sorts);
			for (const offset of [0, 2, 20, 100, 200, 300]) {
				const result = await bounded(
					index.search({
						text: 'shoe',
						sort: sorts,
						offset,
						limit: 2,
						exactTotal: true,
					}),
				);
				assert.strictEqual(result.total, records.length);
				assert.deepStrictEqual(
					result.hits.map((hit) => hit.id),
					expected.slice(offset, offset + 2),
				);
				for (const hit of result.hits) assert.strictEqual(hit.score, scores.get(hit.id));
			}
			const full = await bounded(index.search({ text: 'shoe', sort: sorts, limit: records.length }));
			assert.deepStrictEqual(
				full.hits.map((hit) => hit.id),
				expected,
			);
			assert.deepStrictEqual(
				full.hits.filter((hit) => hit.id.startsWith('zero-')).map((hit) => hit.id),
				['zero-a', 'zero-z'],
			);
		}
	},
);

test(
	'a large requested terms size remains bounded by actual low-cardinality results',
	{ timeout: 60_000 },
	async (context) => {
		const index = await fixture(context, [
			[{ id: 'a', brand: 'cedar', price: 1 }],
			[{ id: 'b', brand: 'pine', price: 2 }],
			[{ id: 'c', brand: 'birch', price: 3 }],
		]);
		const result = await bounded(
			index.search({
				text: 'shoe',
				limit: 1,
				aggregations: [{ name: 'brands', field: 'brand', type: 'terms', size: 1_000_000 }],
			}),
		);
		assert.strictEqual(result.aggregations[0].buckets.length, 3);
		assert.deepStrictEqual(
			new Set(result.aggregations[0].buckets.map((bucket) => bucket.key)),
			new Set(['cedar', 'pine', 'birch']),
		);
		assert.strictEqual(result.aggregations[0].sumOtherDocCount, 0);
		assert.strictEqual((await bounded(index.search({ text: 'shoe', limit: 1, exactTotal: true }))).total, 3);
	},
);

test(
	'actual aggregation output bytes reject oversized matches without imposing static request caps',
	{ timeout: 60_000 },
	async (context) => {
		const records = Array.from({ length: 900 }, (_, index) => ({
			id: String(index),
			brand: `${String(index).padStart(4, '0')}${'x'.repeat(11_000)}`,
		}));
		const index = await fixture(context, [records.slice(0, 300), records.slice(300, 600), records.slice(600)]);
		const aggregations = [{ name: 'brands', field: 'brand', type: 'terms', size: 900 }];
		await assert.rejects(bounded(index.search({ text: 'shoe', limit: 1, aggregations })), {
			code: 'E_RESULT_TOO_LARGE',
		});
		const empty = await bounded(index.search({ text: 'unmatched', limit: 1, aggregations }));
		assert.deepStrictEqual(empty.aggregations[0].buckets, []);
		assert.strictEqual((await bounded(index.search({ text: 'shoe', limit: 1, exactTotal: true }))).total, 900);
	},
);

test(
	'native global bucket limits depend on matching results and preserve handle recovery',
	{ timeout: 60_000 },
	async (context) => {
		const records = Array.from({ length: 1200 }, (_, index) => ({
			id: String(index),
			brand: `brand-${String(index).padStart(4, '0')}`,
		}));
		const index = await fixture(context, [records]);
		const aggregations = [{ name: 'brands', field: 'brand', type: 'terms', size: 1200 }];
		await assert.rejects(bounded(index.search({ text: 'shoe', limit: 1, aggregations })), { code: 'E_RESOURCE_LIMIT' });
		const empty = await bounded(index.search({ text: 'unmatched', limit: 1, aggregations }));
		assert.deepStrictEqual(empty.aggregations[0].buckets, []);
		const missing = await bounded(
			index.search({ text: 'shoe', filter: { field: 'price', comparator: 'missing' }, limit: 1, exactTotal: true }),
		);
		assert.strictEqual(missing.total, 1200);
	},
);
