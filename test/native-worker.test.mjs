import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { Worker } from 'node:worker_threads';

import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

test('worker termination detaches completions and releases its writer', async (context) => {
	const indexPath = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-worker-'));
	context.after(() => rmSync(indexPath, { recursive: true, force: true }));
	const worker = new Worker(new URL('./fixtures/native-worker-child.mjs', import.meta.url), {
		workerData: {
			indexPath,
			moduleUrl: new URL('../dist/native.js', import.meta.url).href,
		},
	});
	await new Promise((resolve, reject) => {
		worker.once('error', reject);
		worker.on('message', (message) => message === 'applying' && resolve());
	});
	await worker.terminate();
	const index = await waitForOpen(indexPath);
	assert.strictEqual((await index.search({ text: 'worker owned product', exactTotal: true })).total, 0);
	await index.close();
});

async function waitForOpen(indexPath) {
	const deadline = performance.now() + 10_000;
	while (true) {
		try {
			return await openNativeFullTextIndex({
				path: indexPath,
				indexId: 'worker-products',
				generation: 'generation-1',
				fields: [{ name: 'title' }],
				analyzer: 'english@1',
				limits: {
					indexingThreads: 1,
					searchThreads: 1,
					writerMemoryBytes: 15_000_000,
					maxQueuedCommands: 8,
					maxQueuedBytes: 1024 * 1024,
					maxBatchBytes: 1024 * 1024,
				},
			});
		} catch (error) {
			if (!['E_DUPLICATE_OPEN', 'E_LOCK_BUSY'].includes(error.code) || performance.now() >= deadline) {
				throw error;
			}
			await new Promise((resolve) => setTimeout(resolve, 10));
		}
	}
}
