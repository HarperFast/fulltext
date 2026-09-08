import assert from 'node:assert';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

for (const [mode, expected] of [
	['committed', 1],
	['uncommitted', 0],
]) {
	test(`reopens ${mode} state after abrupt process exit`, async (context) => {
		const indexPath = mkdtempSync(path.join(tmpdir(), `harper-fulltext-${mode}-`));
		context.after(() => rmSync(indexPath, { recursive: true, force: true }));
		const child = spawnSync(
			process.execPath,
			[fileURLToPath(new URL('./fixtures/native-crash-child.mjs', import.meta.url)), indexPath, mode],
			{ encoding: 'utf8', timeout: 30_000 },
		);
		assert.strictEqual(child.status, 17, child.stderr);
		const index = await openNativeFullTextIndex({
			path: indexPath,
			indexId: 'crash-products',
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
		assert.strictEqual((await index.search({ text: 'durable product', exactTotal: true })).total, expected);
		await index.close();
	});
}
