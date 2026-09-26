import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { configureNativeFullTextRuntime, openNativeFullTextIndex } from '@harperfast/fulltext/native';

const root = await mkdtemp(path.join(tmpdir(), 'fulltext-multi-index-'));
configureNativeFullTextRuntime({
	maxResidentIndexes: 2,
	maxIndexingThreads: 2,
	maxSearchThreads: 4,
	maxWriterMemoryBytes: 30_000_000,
	maxQueuedBytes: 4 * 1024 * 1024,
	maxExpensiveSearches: 2,
});
const options = (name) => ({
	path: path.join(root, name),
	indexId: name,
	generation: 'v1',
	fields: [{ name: 'title' }],
	analyzer: 'english@2',
	limits: {
		indexingThreads: 1,
		searchThreads: 2,
		writerMemoryBytes: 15_000_000,
		maxQueuedCommands: 16,
		maxQueuedBytes: 1024 * 1024,
		maxBatchBytes: 1024 * 1024,
	},
});
let indexes = [];
try {
	indexes = await Promise.all([
		openNativeFullTextIndex(options('products')),
		openNativeFullTextIndex(options('articles')),
	]);
	await Promise.all(
		indexes.map(async (index, offset) => {
			await index.applyMutationBatch({
				upserts: [{ id: `record-${offset}`, fields: { title: `Concurrent searchable record ${offset}` } }],
			});
			await index.commit();
			await index.reload();
		}),
	);
	console.log(await Promise.all(indexes.map((index) => index.search({ text: 'searchable' }))));
} finally {
	await Promise.all(indexes.map((index) => index.close({ mode: 'rollback' })));
	await rm(root, { recursive: true, force: true });
}
