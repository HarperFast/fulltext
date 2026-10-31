import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const root = await mkdtemp(path.join(tmpdir(), 'fulltext-observability-example-'));
let index;
let completed = false;
let closeAttempted = false;
let closeError;

function exportSnapshot(snapshot) {
	return JSON.stringify(snapshot, (_key, value) => (typeof value === 'bigint' ? value.toString() : value));
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
	index.configureTelemetry({ histograms: true });
	await index.applyMutationBatch({ upserts: [{ id: 'shoe-1', fields: { title: 'waterproof running shoe' } }] });
	await index.publish('catalog-1');
	const result = await index.search({ text: 'waterproof', limit: 10 });
	assert.strictEqual(result.hits[0]?.id, 'shoe-1');
	const snapshot = index.telemetry();
	assert(snapshot.available, 'The installed native addon should support telemetry');
	assert.strictEqual(snapshot.logicalBatches.succeeded, 1n);
	assert.strictEqual(snapshot.native.reader.documents, 1n);
	console.log(exportSnapshot(snapshot));
	closeAttempted = true;
	try {
		const closed = await index.close();
		closeError = closed.cleanupError;
	} catch (error) {
		closeError = error;
	}
	if (closeError) throw closeError;
	const finalSnapshot = index.telemetry();
	assert(finalSnapshot.available);
	assert.strictEqual(finalSnapshot.native.state, 'closed');
	console.log(exportSnapshot(finalSnapshot));
	completed = true;
} finally {
	if (!closeAttempted) {
		try {
			const result = await index?.close();
			closeError = result?.cleanupError;
		} catch (error) {
			closeError = error;
		}
	}
	if (!closeError) await rm(root, { recursive: true, force: true });
	else if (completed) throw closeError;
}
