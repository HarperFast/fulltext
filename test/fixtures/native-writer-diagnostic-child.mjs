import assert from 'node:assert';
import { closeSync, writeSync } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { openNativeFullTextIndex } from '../../dist/native.js';
import { closeIndex } from '../../benchmarks/metadata-cleanup.mjs';
import {
	startWriterDiagnostics,
	writeWriterDiagnostic,
	writerStatusSnapshot,
} from '../../benchmarks/writer-diagnostics.mjs';

const mode = process.argv[2];
if (mode === 'closed-stderr') {
	closeSync(2);
	writeWriterDiagnostic({ stage: 'closed-output' });
	writeSync(1, JSON.stringify({ complete: true }));
	process.exit(0);
} else if (mode === 'status-error') {
	await new Promise((resolve, reject) => {
		const timeout = setTimeout(() => reject(new Error('heartbeat did not run')), 10_000);
		const diagnostics = startWriterDiagnostics('status-error', () => {
			queueMicrotask(() => {
				diagnostics.stop();
				clearTimeout(timeout);
				resolve();
			});
			throw Object.assign(new Error('status unavailable'), { code: 'E_CLOSED' });
		});
	});
	console.log(JSON.stringify({ complete: true }));
} else {
	const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-writer-diagnostic-'));
	let index;
	const diagnostics = startWriterDiagnostics('native-writer-child', () => writerStatusSnapshot(index?.status()));
	try {
		index = await openNativeFullTextIndex({
			path: directory,
			indexId: 'diagnostic',
			generation: 'one',
			fields: [{ name: 'title' }],
			analyzer: 'english@2',
			limits: {
				indexingThreads: 1,
				searchThreads: 1,
				writerMemoryBytes: 15_000_000,
				maxQueuedCommands: 8,
				maxQueuedBytes: 1 << 20,
				maxBatchBytes: 1 << 20,
			},
		});
		await index.applyMutationBatch({ upserts: [{ id: 'one', fields: { title: 'amber' } }] });
		await index.commit();
		writeWriterDiagnostic({
			stage: 'js-commit-resolved',
			timestamp: Date.now(),
			native: writerStatusSnapshot({ ...index.status(), documentSource: 'must-not-be-recorded' }),
		});
		await index.reload();
		assert.strictEqual((await index.search({ text: 'amber', exactTotal: true })).total, 1);
	} finally {
		diagnostics.stop();
		await closeIndex(index, { mode: 'rollback' });
		if (index) await rm(directory, { recursive: true, force: true });
	}
	console.log(JSON.stringify({ complete: true }));
}
