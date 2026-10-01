import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const root = await mkdtemp(path.join(tmpdir(), 'fulltext-structured-filters-'));
let index;
let completed = false;
try {
	index = await openNativeFullTextIndex({
		path: path.join(root, 'products'),
		indexId: 'products',
		generation: 'v1',
		fields: [{ name: 'title', weight: 3 }, { name: 'description' }],
		filterFields: [
			{ name: 'category', type: 'string' },
			{ name: 'price', type: 'number' },
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
	await index.applyMutationBatch({
		upserts: [
			{
				id: 'shoe-1',
				fields: { title: 'Trail running shoe', description: 'Waterproof and lightweight' },
				filters: { category: 'outdoor', price: 89.99 },
			},
			{
				id: 'shoe-2',
				fields: { title: 'Trail running shoe', description: 'Waterproof and insulated' },
				filters: { category: 'winter', price: 149.99 },
			},
		],
	});
	await index.commit();
	await index.reload();
	const result = await index.search({
		text: 'waterproof running shoes',
		filter: {
			operator: 'and',
			clauses: [
				{ field: 'category', comparator: 'equals', value: 'outdoor' },
				{ field: 'price', comparator: 'between', value: [50, 100] },
			],
		},
	});
	assert.deepStrictEqual(
		result.hits.map(({ id }) => id),
		['shoe-1'],
	);
	console.log(result);
	completed = true;
} finally {
	let closeError;
	try {
		const closeResult = await index?.close();
		closeError = closeResult?.cleanupError;
	} catch (error) {
		closeError = error;
	}
	if (!closeError) {
		try {
			await rm(root, { recursive: true, force: true });
		} catch (error) {
			if (completed) throw error;
		}
	} else if (completed) throw closeError;
}
