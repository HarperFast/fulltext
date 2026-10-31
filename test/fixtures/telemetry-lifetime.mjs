import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';
import { loadAddon } from '../../dist/load-addon.js';

const addon = loadAddon();
assert.strictEqual(typeof global.gc, 'function');
assert.strictEqual(typeof addon.__testLiveTelemetryMetrics, 'function');
const initial = addon.__testLiveTelemetryMetrics();
const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-telemetry-gc-'));
let index;
let closed = false;
try {
	index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'gc',
		generation: 'one',
		fields: [{ name: 'text' }],
		analyzer: 'english@2',
		limits: {
			indexingThreads: 1,
			searchThreads: 1,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8 << 20,
			maxBatchBytes: 1 << 20,
		},
	});
	await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { text: 'hello' } }] });
	await index.publish('saved');
	await index.close();
	closed = true;
	const telemetry = index.telemetry();
	assert.strictEqual(telemetry.available, true);
	assert.strictEqual(telemetry.native.operations.close.succeeded, 1n);
	assert(addon.__testLiveTelemetryMetrics() > initial);
	index = undefined;
	const deadline = performance.now() + 10_000;
	while (addon.__testLiveTelemetryMetrics() !== initial && performance.now() < deadline) {
		global.gc();
		await new Promise((resolve) => setImmediate(resolve));
	}
	assert.strictEqual(
		addon.__testLiveTelemetryMetrics(),
		initial,
		'telemetry counter storage survived wrapper collection',
	);
	console.log('telemetry counters finalized without retaining native index resources');
} finally {
	if (!closed && index) {
		const result = await index.close({ mode: 'rollback' });
		assert.strictEqual(result.cleanupError, undefined);
		closed = true;
	}
	if (closed) rmSync(directory, { recursive: true, force: true });
}
