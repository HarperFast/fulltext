import assert from 'node:assert';
import { spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';
import { loadAddon } from '../dist/load-addon.js';

function config(directory) {
	return {
		path: directory,
		indexId: 'telemetry-lifetime',
		generation: 'one',
		fields: [{ name: 'text' }],
		analyzer: 'english@2',
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8 << 20,
			maxBatchBytes: 1 << 20,
		},
	};
}

test('counter-only telemetry token is reclaimed after the closed wrapper is collected', { timeout: 30_000 }, () => {
	const result = spawnSync(
		process.execPath,
		['--expose-gc', fileURLToPath(new URL('./fixtures/telemetry-lifetime.mjs', import.meta.url))],
		{
			env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' },
			encoding: 'utf8',
			timeout: 25_000,
		},
	);
	assert.ifError(result.error);
	assert.strictEqual(result.status, 0, result.stderr || result.stdout);
	assert.match(result.stdout, /telemetry counters finalized/);
});

test(
	'a telemetry token allocation failure does not disable opening or querying the index',
	{ timeout: 20_000 },
	async () => {
		const addon = loadAddon();
		assert.strictEqual(typeof addon.__testFailNextTelemetryToken, 'function');
		const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-telemetry-token-'));
		let index;
		let closed = false;
		try {
			addon.__testFailNextTelemetryToken();
			index = await openNativeFullTextIndex(config(directory));
			assert.strictEqual(index.telemetry().available, false);
			assert.throws(() => index.configureTelemetry({ histograms: true }), { code: 'E_NATIVE_CAPABILITY_MISMATCH' });
			await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { text: 'hello' } }] });
			await index.publish('checkpoint');
			assert.strictEqual((await index.search({ text: 'hello' })).total, 1);
			const result = await index.close();
			assert.strictEqual(result.cleanupError, undefined);
			closed = true;
		} finally {
			if (!closed && index) {
				const result = await index.close({ mode: 'rollback' });
				assert.strictEqual(result.cleanupError, undefined);
				closed = true;
			}
			if (closed) rmSync(directory, { recursive: true, force: true });
		}
	},
);

test('worker termination releases native resources even while it owns telemetry', { timeout: 30_000 }, async () => {
	const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-telemetry-worker-'));
	const options = config(directory);
	const worker = new Worker(new URL('./fixtures/telemetry-worker.mjs', import.meta.url), { workerData: options });
	let terminated = false;
	let reopened;
	let closed = false;
	try {
		const [message] = await once(worker, 'message');
		assert.strictEqual(message.ready, true);
		assert.strictEqual(message.telemetry.available, true);
		assert.strictEqual(message.telemetry.native.reader.documents, 1n);
		await worker.terminate();
		terminated = true;
		reopened = await openNativeFullTextIndex(options);
		assert.strictEqual(reopened.committedPayload, 'worker-checkpoint');
		assert.strictEqual((await reopened.search({ text: 'hello' })).total, 1);
		const result = await reopened.close();
		assert.strictEqual(result.cleanupError, undefined);
		closed = true;
	} finally {
		if (!terminated) await worker.terminate();
		if (reopened && !closed) {
			const result = await reopened.close({ mode: 'rollback' });
			assert.strictEqual(result.cleanupError, undefined);
			closed = true;
		}
		if (closed) rmSync(directory, { recursive: true, force: true });
	}
});

for (const quiesced of [true, false]) {
	test(`terminal telemetry survives ${quiesced ? 'close' : 'quiescence'} failure`, { timeout: 30_000 }, () => {
		const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-telemetry-close-failure-'));
		const result = spawnSync(
			process.execPath,
			[fileURLToPath(new URL('./fixtures/telemetry-close-failure.mjs', import.meta.url)), directory, String(quiesced)],
			{
				env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' },
				encoding: 'utf8',
				timeout: 25_000,
			},
		);
		assert.ifError(result.error);
		assert.strictEqual(result.status, 0, result.stderr || result.stdout);
		assert.match(result.stdout, /terminal telemetry verified/);
		rmSync(directory, { recursive: true, force: true });
	});
}
