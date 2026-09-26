import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const root = await mkdtemp(path.join(tmpdir(), 'fulltext-query-modes-'));
let index;
try {
	index = await openNativeFullTextIndex({
		path: path.join(root, 'products'),
		indexId: 'products',
		generation: 'v1',
		fields: [{ name: 'title', weight: 3 }, { name: 'description' }],
		analyzer: 'english@2',
		positions: true,
		surfaceTerms: true,
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
			{ id: 'shoe-1', fields: { title: 'Waterproof trail running shoe', description: 'Lightweight grip' } },
			{ id: 'boot-1', fields: { title: 'Insulated hiking boot', description: 'Waterproof winter footwear' } },
		],
	});
	await index.commit();
	await index.reload();
	for (const request of [
		{ text: 'waterproof trail', mode: 'any' },
		{ text: 'waterproof trail', mode: 'all' },
		{ text: 'trail running', mode: 'phrase' },
		{ text: 'waterproof tra', mode: 'prefix' },
		{ text: 'waterprof', mode: 'fuzzy' },
		{ text: 'waterproof tral', mode: 'fuzzy-prefix' },
		{ text: 'waterproof', mode: 'any', candidateIds: ['boot-1'] },
	]) {
		console.log(request.mode, await index.search({ ...request, limit: 10 }));
	}
} finally {
	await index?.close({ mode: 'rollback' });
	await rm(root, { recursive: true, force: true });
}
