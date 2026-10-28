import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-metadata-'));
let index;
try {
	index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'metadata-example',
		generation: 'v1',
		fields: [{ name: 'title' }],
		filterFields: [
			{ name: 'brand', type: 'string' },
			{ name: 'price', type: 'number' },
			{ name: 'active', type: 'boolean' },
		],
		analyzer: 'english@2',
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 4 * 1024 * 1024,
			maxBatchBytes: 1024 * 1024,
		},
	});
	const records = [
		{ id: 'a', filters: { brand: 'cedar', price: 20, active: true } },
		{ id: 'b', filters: { brand: 'ember', price: 10, active: true } },
		{ id: 'c', filters: { brand: 'cedar', price: 30, active: false } },
		{ id: 'd', filters: { price: 5, active: true } },
		{ id: 'e', filters: { brand: 'ember', active: true } },
	];
	const applied = await index.applyMutationBatch({
		upserts: records.map((record) => ({ ...record, fields: { title: 'Trail running shoe' } })),
	});
	assert.deepStrictEqual(applied.rejected, []);
	await index.commit();
	await index.reload();
	const result = await index.search({
		text: 'trail shoe',
		mode: 'all',
		limit: 1,
		exactTotal: true,
		sort: { field: 'price', direction: 'asc', missing: 'last' },
		filter: {
			operator: 'and',
			clauses: [
				{ field: 'active', comparator: 'equals', value: true },
				{ field: 'price', comparator: 'exists' },
				{ operator: 'not', clause: { field: 'brand', comparator: 'equals', value: 'ember' } },
			],
		},
		aggregations: [
			{ name: 'brands', field: 'brand', type: 'terms', size: 10 },
			{ name: 'prices', field: 'price', type: 'stats' },
			{ name: 'priceHistogram', field: 'price', type: 'histogram', interval: 10 },
			{ name: 'priceRanges', field: 'price', type: 'range', ranges: [{ to: 10 }, { from: 10, to: 25 }, { from: 25 }] },
		],
	});
	assert.strictEqual(result.total, 2);
	assert.deepStrictEqual(
		result.hits.map(({ id }) => id),
		['d'],
	);
	const byName = new Map(result.aggregations.map((aggregation) => [aggregation.name, aggregation]));
	assert.deepStrictEqual(byName.get('brands').buckets, [{ key: 'cedar', count: 1 }]);
	assert.deepStrictEqual(byName.get('prices'), {
		name: 'prices',
		type: 'stats',
		count: 2,
		sum: 25,
		min: 5,
		max: 20,
		avg: 12.5,
	});
	assert.deepStrictEqual(
		byName.get('priceHistogram').buckets.filter(({ count }) => count > 0),
		[
			{ key: 0, count: 1 },
			{ key: 20, count: 1 },
		],
	);
	assert.deepStrictEqual(
		byName.get('priceRanges').buckets.map(({ count }) => count),
		[1, 1, 0],
	);
	const missing = await index.search({ text: 'trail', filter: { field: 'brand', comparator: 'missing' } });
	assert.deepStrictEqual(
		missing.hits.map(({ id }) => id),
		['d'],
	);
	const byBrand = await index.search({
		text: 'trail',
		sort: [
			{ field: 'brand', direction: 'asc', missing: 'last' },
			{ field: 'price', direction: 'desc', missing: 'last' },
		],
		aggregations: [
			{
				name: 'brands',
				field: 'brand',
				type: 'terms',
				size: 150,
				aggregations: [{ name: 'prices', field: 'price', type: 'stats' }],
			},
		],
		limit: 10,
	});
	assert.deepStrictEqual(
		byBrand.hits.map(({ id }) => id),
		['c', 'a', 'b', 'e', 'd'],
	);
	assert.strictEqual(byBrand.aggregations[0].buckets.find(({ key }) => key === 'cedar').aggregations[0].sum, 50);
	console.log(
		JSON.stringify(
			{ result, missingBrandIds: missing.hits.map(({ id }) => id), sortedBrandIds: byBrand.hits.map(({ id }) => id) },
			null,
			2,
		),
	);
} finally {
	await index?.close({ mode: 'rollback' });
	await rm(directory, { recursive: true, force: true });
}
