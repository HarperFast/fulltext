import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const root = await mkdtemp(path.join(tmpdir(), 'fulltext-observability-example-'));
let index;
let collectionEnabled = false;
const exportedSnapshots = [];
let primaryError;
let failed = false;
let closeAttempted = false;
const cleanupErrors = [];

function collectSnapshot() {
	if (!collectionEnabled) return;
	const snapshot = index.telemetry();
	assert(snapshot.available, 'The installed native addon should support telemetry');
	exportedSnapshots.push(snapshot);
	console.log(JSON.stringify(snapshot, (_key, value) => (typeof value === 'bigint' ? value.toString() : value)));
	return snapshot;
}

try {
	index = await openNativeFullTextIndex({
		path: path.join(root, 'products'),
		indexId: 'products',
		generation: 'v1',
		fields: [{ name: 'title' }],
		analyzer: 'english@2',
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 4 << 20,
			maxBatchBytes: 1 << 20,
		},
	});
	await index.applyMutationBatch({ upserts: [{ id: 'shoe-1', fields: { title: 'waterproof running shoe' } }] });
	await index.publish('catalog-1');
	const result = await index.search({ text: 'waterproof', limit: 10 });
	assert.strictEqual(result.hits[0]?.id, 'shoe-1');
	assert.strictEqual(collectSnapshot(), undefined);
	assert.strictEqual(exportedSnapshots.length, 0);
	collectionEnabled = true;
	const snapshot = collectSnapshot();
	assert.strictEqual(snapshot.logicalBatches.succeeded, 1n);
	assert.strictEqual(snapshot.native.reader.documents, 1n);
	assert.strictEqual(snapshot.native.operations.search.succeeded, 1n);
	assert.strictEqual(snapshot.native.histogramsEnabled, false);
	assert.strictEqual(snapshot.native.snapshotBytesEnabled, false);
	collectionEnabled = false;
	assert.strictEqual((await index.search({ text: 'waterproof', limit: 10 })).hits[0]?.id, 'shoe-1');
	assert.strictEqual(collectSnapshot(), undefined);
	assert.strictEqual(exportedSnapshots.length, 1);
	collectionEnabled = true;
	assert.strictEqual(collectSnapshot().native.operations.search.succeeded, 2n);
	assert.strictEqual(exportedSnapshots.length, 2);
	closeAttempted = true;
	const closed = await index.close();
	if (closed.cleanupError) throw closed.cleanupError;
	const finalSnapshot = collectSnapshot();
	assert.strictEqual(finalSnapshot.native.state, 'closed');
	collectionEnabled = false;
	assert.strictEqual(collectSnapshot(), undefined);
	assert.strictEqual(exportedSnapshots.length, 3);
} catch (error) {
	failed = true;
	primaryError = error;
} finally {
	if (!closeAttempted) {
		try {
			const result = await index?.close({ mode: 'rollback' });
			if (result?.cleanupError) cleanupErrors.push(result.cleanupError);
		} catch (error) {
			cleanupErrors.push(error);
		}
	}
	if (!failed && cleanupErrors.length === 0) {
		try {
			await rm(root, { recursive: true, force: true });
		} catch (error) {
			cleanupErrors.push(error);
		}
	}
}
if (failed || cleanupErrors.length > 0) console.error(`Observability example retained for inspection: ${root}`);
if (failed && cleanupErrors.length > 0)
	throw new AggregateError([primaryError, ...cleanupErrors], 'Example and cleanup failed', { cause: primaryError });
if (failed) throw primaryError;
if (cleanupErrors.length === 1) throw cleanupErrors[0];
if (cleanupErrors.length > 1) throw new AggregateError(cleanupErrors, 'Example cleanup failed');
