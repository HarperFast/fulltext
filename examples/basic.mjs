import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const root = await mkdtemp(path.join(tmpdir(), 'fulltext-basic-'));
let index;
let completed = false;
try {
	index = await openNativeFullTextIndex({
		path: path.join(root, 'products'),
		indexId: 'products',
		generation: 'v1',
		fields: [{ name: 'title', weight: 3 }, { name: 'description' }],
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
		upserts: [{ id: 'shoe-1', fields: { title: 'Trail running shoe', description: 'Waterproof and lightweight' } }],
	});
	await index.commit();
	await index.reload();
	const result = await index.search({ text: 'waterproof running shoes', limit: 10 });
	assert.strictEqual(result.total, 1);
	assert.strictEqual(result.hits[0]?.id, 'shoe-1');
	console.log(result);
	completed = true;
} finally {
	try {
		await index?.close({ mode: 'rollback' }).catch((error) => {
			if (completed) throw error;
		});
	} finally {
		await rm(root, { recursive: true, force: true });
	}
}
