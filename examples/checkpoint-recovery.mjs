import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { inspectNativeFullTextIndex, openNativeFullTextIndex } from '@harperfast/fulltext/native';

const root = await mkdtemp(path.join(tmpdir(), 'fulltext-checkpoint-'));
const options = {
	path: path.join(root, 'products'),
	indexId: 'products',
	generation: 'v1',
	fields: [{ name: 'title' }],
	analyzer: 'english@2',
	limits: {
		indexingThreads: 1,
		searchThreads: 1,
		writerMemoryBytes: 15_000_000,
		maxQueuedCommands: 16,
		maxQueuedBytes: 1024 * 1024,
		maxBatchBytes: 1024 * 1024,
	},
};
let index;
try {
	index = await openNativeFullTextIndex(options);
	await index.applyMutationBatch({ upserts: [{ id: 'shoe-1', fields: { title: 'Trail running shoe' } }] });
	await index.publish('source-checkpoint-42');
	await index.close();
	index = undefined;

	const inspection = inspectNativeFullTextIndex(options);
	assert.deepStrictEqual(inspection, { state: 'checkpointed', committedPayload: 'source-checkpoint-42' });

	index = await openNativeFullTextIndex(options);
	assert.strictEqual(index.committedPayload, 'source-checkpoint-42');
	console.log(await index.search({ text: 'running' }));
} finally {
	await index?.close({ mode: 'rollback' });
	await rm(root, { recursive: true, force: true });
}
