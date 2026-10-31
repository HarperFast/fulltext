import assert from 'node:assert';
import { NativeFullTextIndex, encodeMutationBatch } from '@harperfast/fulltext/native';
import { encodeOpen } from '../../dist/codec.js';
import { invoke } from '../../dist/invoke.js';
import { loadAddon } from '../../dist/load-addon.js';

const config = {
	path: process.argv[2],
	indexId: 'close-failure',
	generation: 'one',
	fields: [{ name: 'text', weight: 1 }],
	analyzer: 'english@2',
	stopWords: true,
	positions: true,
	surfaceTerms: false,
	limits: {
		indexingThreads: 1,
		searchThreads: 2,
		writerMemoryBytes: 15_000_000,
		maxQueuedCommands: 32,
		maxQueuedBytes: 8 << 20,
		maxBatchBytes: 1 << 20,
	},
};
const addon = loadAddon();
const cursor = await invoke((callback) => addon.__nativeOpen(encodeOpen(config), callback));
const handle = cursor.u32();
assert.strictEqual(cursor.u8(), 0);
cursor.finish();
const index = new NativeFullTextIndex({ handle, maxBatchBytes: config.limits.maxBatchBytes, fieldNames: ['text'] });
await index.apply(encodeMutationBatch({ upserts: [{ id: 'a', fields: { text: 'hello' } }] }));
await index.publish('checkpoint');
const quiesced = process.argv[3] === 'true';
addon.__testFailNextClose(handle, quiesced);
if (quiesced) {
	const result = await index.close();
	assert.strictEqual(result.cleanupError?.code, 'E_CLOSE_FAILED');
} else {
	await assert.rejects(index.close(), { code: 'E_QUIESCENCE_FAILED' });
}
const telemetry = index.telemetry();
assert.strictEqual(telemetry.available, true);
assert.strictEqual(telemetry.native.state, quiesced ? 'closed' : 'poisoned');
const close = telemetry.native.operations.close;
assert.strictEqual(close.attempted, 1n);
assert.strictEqual(close.accepted, 1n);
assert.strictEqual(close.completed, 1n);
assert.strictEqual(close.failed, 1n);
assert.strictEqual(close.dispatched, 0n);
assert.strictEqual(close.errors[quiesced ? 'E_CLOSE_FAILED' : 'E_QUIESCENCE_FAILED'], 1n);
assert.strictEqual(close.execution.count, 1n);
assert.strictEqual(telemetry.native.phases.teardown.failed, 1n);
console.log(`terminal telemetry verified: ${quiesced ? 'close-failed' : 'quiescence-failed'}`);
