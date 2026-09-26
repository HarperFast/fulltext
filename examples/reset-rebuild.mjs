import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import {
	inspectNativeFullTextIndex,
	openNativeFullTextIndex,
	reclaimRetiredNativeFullTextIndexes,
	resetNativeFullTextIndex,
} from '@harperfast/fulltext/native';

const root = await mkdtemp(path.join(tmpdir(), 'fulltext-reset-'));
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
	await index.applyMutationBatch({ upserts: [{ id: 'old', fields: { title: 'Old derived content' } }] });
	await index.publish('checkpoint-1');
	await index.close();
	index = undefined;

	const retired = await resetNativeFullTextIndex({ path: options.path, indexId: options.indexId });
	assert.strictEqual(retired.state, 'reset');
	assert.deepStrictEqual(inspectNativeFullTextIndex(options), { state: 'missing' });
	console.log(await reclaimRetiredNativeFullTextIndexes({ path: options.path, retiredPath: retired.retiredPath }));

	index = await openNativeFullTextIndex({ ...options, generation: 'v2' });
	console.log(await index.search({ text: 'old', exactTotal: true }));
} finally {
	await index?.close({ mode: 'rollback' });
	await rm(root, { recursive: true, force: true });
}
