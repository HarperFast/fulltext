import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
	encodeMutationBatch,
	NativeFullTextIndex,
	FulltextError,
	openNativeFullTextIndex,
	openNativeFullTextReader,
	resetNativeFullTextIndex,
	runtimeInfo,
} from '@harperfast/fulltext/native';
import { loadAddon } from '../dist/load-addon.js';
import { encodeOpen } from '../dist/codec.js';
import { invoke } from '../dist/invoke.js';
import { setImmediate } from 'node:timers/promises';

function options(directory, overrides = {}) {
	return {
		path: directory,
		indexId: 'telemetry',
		generation: 'one',
		fields: [{ name: 'text', weight: 1 }],
		analyzer: 'english@2',
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8 << 20,
			maxBatchBytes: 8 << 20,
		},
		...overrides,
	};
}
async function fixture(context, overrides) {
	const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-telemetry-'));
	const config = options(directory, overrides);
	const index = await openNativeFullTextIndex(config);
	context.after(async () => {
		const result = await index.close({ mode: 'rollback' });
		assert.strictEqual(result.cleanupError, undefined);
		rmSync(directory, { recursive: true, force: true });
	});
	return { index, config };
}
function snapshot(index) {
	const value = index.telemetry();
	assert.strictEqual(value.available, true);
	assert.strictEqual(value.native.apiVersion, 1);
	return value;
}
function conserved(operation) {
	assert.strictEqual(operation.attempted, operation.accepted + operation.rejected);
	assert.strictEqual(operation.completed, operation.succeeded + operation.failed);
	assert.strictEqual(operation.accepted, operation.completed + operation.dispatched);
}

test('telemetry reports native operations separately from facade logical batches and leaves status unchanged', async (context) => {
	const { index } = await fixture(context);
	assert.strictEqual((await runtimeInfo()).telemetryApiVersion, 1);
	const originalStatus = Object.keys(index.status()).sort();
	const initial = snapshot(index);
	assert.strictEqual(initial.native.histogramsEnabled, false);
	assert.strictEqual(initial.native.snapshotBytesEnabled, false);
	assert.strictEqual(initial.native.reader.snapshotBytes, null);
	await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { text: 'hello world' } }] });
	await index.apply(encodeMutationBatch({ upserts: [{ id: 'b', fields: { text: 'hello earth' } }] }));
	await index.publish('checkpoint');
	assert.strictEqual((await index.search({ text: 'hello' })).total, 2);
	const telemetry = snapshot(index);
	assert.strictEqual(telemetry.logicalBatches.attempted, 1n);
	assert.strictEqual(telemetry.logicalBatches.succeeded, 1n);
	assert.strictEqual(telemetry.logicalBatches.processedMutations, 1n);
	assert.strictEqual(telemetry.logicalBatches.encodedFrames, 1n);
	assert.strictEqual(telemetry.logicalBatches.requestedMutationsDistribution.count, 0n);
	assert.strictEqual(telemetry.native.operations.applyFrame.succeeded, 2n);
	assert.strictEqual(telemetry.native.operations.publish.succeeded, 1n);
	assert.strictEqual(telemetry.native.operations.search.succeeded, 1n);
	assert.strictEqual(telemetry.native.reader.documents, 2n);
	assert(telemetry.native.reader.segments > 0n);
	assert.strictEqual(typeof telemetry.native.reader.generationId, 'bigint');
	for (const operation of Object.values(telemetry.native.operations)) conserved(operation);
	assert.deepStrictEqual(Object.keys(index.status()).sort(), originalStatus);
	assert.strictEqual(telemetry.cancellation.available, false);
	assert.strictEqual(typeof telemetry.native.unsupported.perIndexRss, 'string');
	telemetry.logicalBatches.attempted = 999n;
	assert.strictEqual(snapshot(index).logicalBatches.attempted, 1n);
});

test('telemetry configuration is strict, histogram samples freeze when disabled and omitted flags persist', async (context) => {
	const { index } = await fixture(context);
	for (const invalid of [null, [], true, { histograms: 1 }, { snapshotBytes: 'yes' }, { surprise: true }]) {
		assert.throws(() => index.configureTelemetry(invalid), { code: 'E_INVALID_ARGUMENT' });
	}
	index.configureTelemetry({ histograms: true });
	await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { text: 'hello' } }] });
	await index.publish('one');
	await index.search({ text: 'hello' });
	const enabled = snapshot(index);
	assert.strictEqual(enabled.native.histogramsEnabled, true);
	assert.strictEqual(enabled.native.operations.search.execution.histogram.count, 1n);
	assert.strictEqual(enabled.logicalBatches.requestedMutationsDistribution.count, 1n);
	assert.strictEqual(enabled.logicalBatches.frameBytesDistribution.count, 1n);
	index.configureTelemetry({ snapshotBytes: true });
	assert.strictEqual(snapshot(index).native.histogramsEnabled, true);
	index.configureTelemetry({ histograms: false });
	await index.search({ text: 'hello' });
	const disabled = snapshot(index);
	assert.strictEqual(disabled.native.operations.search.succeeded, 2n);
	assert.strictEqual(disabled.native.operations.search.execution.histogram.count, 1n);
	assert.strictEqual(disabled.native.snapshotBytesEnabled, true);
	index.configureTelemetry({ histograms: true });
	await index.search({ text: 'hello' });
	const resumed = snapshot(index).native.operations.search.execution.histogram;
	assert.strictEqual(resumed.count, 2n);
	assert.strictEqual(
		resumed.buckets.reduce((sum, count) => sum + count, 0n),
		resumed.count,
	);
});

test('facade preparation failures do not invent native search attempts or expose query text', async (context) => {
	const { index } = await fixture(context);
	await assert.rejects(index.search({ text: 'secret-query', fuzzyDistance: 2 }), { code: 'E_INVALID_ARGUMENT' });
	await assert.rejects(index.traceMatches({ text: 'secret-query' }, [{ id: '', fields: {} }]), {
		code: 'E_INVALID_ARGUMENT',
	});
	const telemetry = snapshot(index);
	assert.strictEqual(telemetry.preparationRejections.search.total, 1n);
	assert.strictEqual(telemetry.preparationRejections.search.errors.E_INVALID_ARGUMENT, 1n);
	assert.strictEqual(telemetry.preparationRejections.trace.total, 1n);
	assert.strictEqual(telemetry.native.operations.search.attempted, 0n);
	assert.strictEqual(telemetry.native.operations.trace.attempted, 0n);
	assert(
		!JSON.stringify(telemetry, (_key, value) => (typeof value === 'bigint' ? value.toString() : value)).includes(
			'secret-query',
		),
	);
});

test('failed logical batch preserves successful earlier frame progress and rejected record accounting', async (context) => {
	const { index } = await fixture(context, {
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8 << 20,
			maxBatchBytes: 256,
		},
	});
	await assert.rejects(
		index.applyMutationBatch({
			upserts: [
				{ id: 'a', fields: { text: 'a'.repeat(180) } },
				{ id: 'b', fields: { text: 'b'.repeat(180) } },
				{ id: 'bad', fields: { text: 'z'.repeat(500) } },
			],
		}),
		{ code: 'E_BATCH_TOO_LARGE' },
	);
	const telemetry = snapshot(index);
	assert.strictEqual(telemetry.logicalBatches.attempted, 1n);
	assert.strictEqual(telemetry.logicalBatches.completed, 1n);
	assert.strictEqual(telemetry.logicalBatches.failed, 1n);
	assert.strictEqual(telemetry.logicalBatches.requestedMutations, 3n);
	assert(telemetry.logicalBatches.processedMutations > 0n);
	assert.strictEqual(telemetry.logicalBatches.rejectedRecords, 1n);
	assert.strictEqual(telemetry.logicalBatches.encodedFrames, telemetry.native.operations.applyFrame.succeeded);
	assert(telemetry.logicalBatches.encodedFrameBytes > 0n);
});

test('replacement deletes and empty logical batches have explicit facade accounting', async (context) => {
	const { index } = await fixture(context);
	await index.applyMutationBatch({});
	await index.applyMutationBatch(
		{ upserts: [{ id: 'bad', fields: { text: 'x'.repeat((1 << 20) + 1) } }] },
		{ rejectedUpsert: 'delete' },
	);
	const telemetry = snapshot(index);
	assert.strictEqual(telemetry.logicalBatches.attempted, 2n);
	assert.strictEqual(telemetry.logicalBatches.succeeded, 2n);
	assert.strictEqual(telemetry.logicalBatches.requestedMutations, 1n);
	assert.strictEqual(telemetry.logicalBatches.processedMutations, 1n);
	assert.strictEqual(telemetry.logicalBatches.rejectedRecords, 1n);
	assert.strictEqual(telemetry.logicalBatches.encodedFrames, 1n);
});

test('closed telemetry tokens preserve final outcomes without blocking immediate index reset', async (context) => {
	const { index, config } = await fixture(context);
	index.configureTelemetry({ snapshotBytes: true, histograms: true });
	await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { text: 'hello' } }] });
	await index.publish('one');
	const reader = await openNativeFullTextReader(config);
	try {
		reader.configureTelemetry({ histograms: true });
		assert.strictEqual((await reader.search({ text: 'hello' })).total, 1);
		assert.strictEqual(snapshot(reader).native.operations.search.succeeded, 1n);
		assert.strictEqual(snapshot(index).native.operations.search.succeeded, 0n);
	} finally {
		await reader.close();
	}
	assert.throws(() => reader.configureTelemetry({}), { code: 'E_CLOSED' });
	await index.close();
	const final = snapshot(index);
	assert.strictEqual(final.native.state, 'closed');
	assert.strictEqual(final.native.operations.close.completed, 1n);
	assert.strictEqual(final.native.phases.teardown.count, 1n);
	assert.throws(() => index.configureTelemetry({}), { code: 'E_CLOSED' });
	const retired = await resetNativeFullTextIndex({ path: config.path, indexId: config.indexId });
	assert.strictEqual(retired.state, 'reset');
	assert.strictEqual(snapshot(index).native.operations.close.completed, 1n);
	if (retired.state === 'reset') rmSync(retired.retiredPath, { recursive: true, force: true });
});

test('telemetry token rejects ordinary JavaScript objects', () => {
	assert.throws(() => loadAddon().__nativeTelemetry({}));
	assert.throws(() => loadAddon().__nativeConfigureTelemetry({}, true, false));
});

async function rawFixture(context, limits = {}) {
	const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-telemetry-raw-'));
	const config = options(directory, { surfaceTerms: true, positions: true, stopWords: true });
	Object.assign(config.limits, limits);
	const addon = loadAddon();
	const opened = await invoke((callback) => addon.__nativeOpen(encodeOpen(config), callback));
	const handle = opened.u32();
	assert.strictEqual(opened.u8(), 0);
	opened.finish();
	const index = new NativeFullTextIndex({ handle, fieldNames: ['text'], maxBatchBytes: config.limits.maxBatchBytes });
	context.after(async () => {
		const result = await index.close({ mode: 'rollback' });
		assert.strictEqual(result.cleanupError, undefined);
		rmSync(directory, { recursive: true, force: true });
	});
	return { index, addon, handle };
}
async function waitFor(condition) {
	const deadline = performance.now() + 5_000;
	while (!condition()) {
		assert(performance.now() < deadline, 'condition did not become true');
		await setImmediate();
	}
}

test('native queue saturation and queued expiry preserve conservation and error distinctions', async (context) => {
	const { index, addon, handle } = await rawFixture(context, { maxQueuedCommands: 1 });
	index.configureTelemetry({ histograms: true });
	await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { text: 'waterproof trail' } }] });
	await index.publish('one');
	addon.__testDelayNextExpensiveSearch(handle, 200);
	const first = index.search({ text: 'trai', mode: 'fuzzy-prefix' }, { remainingBudgetMilliseconds: 1_000 });
	await waitFor(() => addon.__testExpensiveSearchState(handle)[0] === 0);
	const queued = index.search({ text: 'trai', mode: 'fuzzy-prefix' }, { remainingBudgetMilliseconds: 10 });
	const expired = assert.rejects(queued, { code: 'E_TIMEOUT' });
	await waitFor(() => snapshot(index).native.queues.expensiveSearch.commands === 1n);
	const waiting = snapshot(index).native.queues.expensiveSearch;
	assert(waiting.bytes > 0n);
	assert(waiting.oldestAgeNanoseconds >= 0n);
	await assert.rejects(index.search({ text: 'trai', mode: 'fuzzy-prefix' }), { code: 'E_QUEUE_FULL' });
	await Promise.all([first, expired]);
	const operation = snapshot(index).native.operations.expensiveSearch;
	conserved(operation);
	assert.strictEqual(operation.attempted, 3n);
	assert.strictEqual(operation.accepted, 2n);
	assert.strictEqual(operation.rejected, 1n);
	assert.strictEqual(operation.succeeded, 1n);
	assert.strictEqual(operation.failed, 1n);
	assert.strictEqual(operation.errors.E_QUEUE_FULL, 1n);
	assert.strictEqual(operation.errors.E_TIMEOUT, 1n);
	assert.strictEqual(snapshot(index).preparationRejections.search.total, 0n);
});

for (const afterCommit of [false, true]) {
	test(`failed publish ${afterCommit ? 'after' : 'before'} commit records phase and operation separately`, async (context) => {
		const { index, addon, handle } = await rawFixture(context);
		await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { text: 'hello' } }] });
		addon.__testFailNextPublish(handle, afterCommit);
		await assert.rejects(index.publish('one'), { code: 'E_STORAGE' });
		const telemetry = snapshot(index);
		assert.strictEqual(telemetry.native.operations.publish.failed, 1n);
		assert.strictEqual(telemetry.native.operations.publish.errors.E_STORAGE, 1n);
		assert.strictEqual(telemetry.native.reader.documents, 0n);
		assert.strictEqual(telemetry.native.phases.commit.succeeded, afterCommit ? 1n : 0n);
		assert.strictEqual(telemetry.native.phases.readerReload.count, 0n);
		conserved(telemetry.native.operations.publish);
	});
}

test('dirty close is rejected while a final rollback close is fully accounted', async (context) => {
	const { index } = await fixture(context);
	await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { text: 'hello' } }] });
	await assert.rejects(index.close(), { code: 'E_DIRTY_CLOSE' });
	const dirty = snapshot(index).native.operations.close;
	assert.strictEqual(dirty.errors.E_DIRTY_CLOSE, 1n);
	conserved(dirty);
	await index.close({ mode: 'rollback' });
	const final = snapshot(index).native.operations.close;
	assert.strictEqual(final.succeeded, 1n);
	conserved(final);
});

test('committing alone does not advance the reader observation; reload installs it', async (context) => {
	const { index } = await fixture(context);
	await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { text: 'hello' } }] });
	await index.commit();
	assert.strictEqual(snapshot(index).native.reader.documents, 0n);
	await index.reload();
	assert.strictEqual(snapshot(index).native.reader.documents, 1n);
	assert.strictEqual(snapshot(index).native.phases.commit.count, 1n);
	assert.strictEqual(snapshot(index).native.phases.readerReload.count, 1n);
});

test('snapshot-byte freshness survives unchanged reloads and resets on a changed snapshot', async (context) => {
	const { index, addon, handle } = await rawFixture(context);
	await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { text: 'hello' } }] });
	await index.publish('one');
	assert.strictEqual(snapshot(index).native.reader.snapshotBytes, null);
	assert.strictEqual(snapshot(index).native.reader.snapshotBytesAgeNanoseconds, null);
	index.configureTelemetry({ snapshotBytes: true });
	assert.strictEqual(snapshot(index).native.reader.snapshotBytes, null);
	assert.strictEqual(snapshot(index).native.reader.snapshotBytesAgeNanoseconds, null);
	await index.reload();
	await waitFor(() => {
		const reader = snapshot(index).native.reader;
		return !reader.snapshotBytesPending && reader.snapshotBytes !== null;
	});
	const measured = snapshot(index).native.reader;
	assert(measured.snapshotBytes > 0n);
	assert.strictEqual(measured.snapshotBytesError, null);
	assert.strictEqual(typeof measured.snapshotBytesAgeNanoseconds, 'bigint');
	assert(measured.snapshotBytesAgeNanoseconds >= 0n);

	addon.__testFailNextTelemetryObservation(handle);
	await index.reload();
	await waitFor(() => !snapshot(index).native.reader.snapshotBytesPending);
	const unchanged = snapshot(index).native.reader;
	assert.strictEqual(unchanged.snapshotBytes, measured.snapshotBytes);
	assert.strictEqual(unchanged.snapshotBytesError, null);
	assert(unchanged.snapshotBytesAgeNanoseconds >= measured.snapshotBytesAgeNanoseconds);
	assert(unchanged.snapshotBytesAgeNanoseconds > unchanged.ageNanoseconds);

	await index.applyMutationBatch({ upserts: [{ id: 'b', fields: { text: 'hello again' } }] });
	await index.publish('two');
	await waitFor(() => {
		const reader = snapshot(index).native.reader;
		return !reader.snapshotBytesPending && reader.snapshotBytesError === 'E_STORAGE';
	});
	const changed = snapshot(index).native.reader;
	assert.strictEqual(changed.documents, 2n);
	assert.strictEqual(typeof changed.ageNanoseconds, 'bigint');
	assert.strictEqual(changed.snapshotBytes, null);
	assert.strictEqual(changed.snapshotBytesAgeNanoseconds, null);

	await index.reload();
	await waitFor(() => {
		const reader = snapshot(index).native.reader;
		return !reader.snapshotBytesPending && reader.snapshotBytes !== null;
	});
	const recovered = snapshot(index).native.reader;
	assert.strictEqual(recovered.snapshotBytesError, null);
	assert.strictEqual(typeof recovered.snapshotBytesAgeNanoseconds, 'bigint');
});

test('snapshot-byte observer failure cannot change publication or search results', async (context) => {
	const { index, addon, handle } = await rawFixture(context);
	index.configureTelemetry({ snapshotBytes: true });
	addon.__testFailNextTelemetryObservation(handle);
	await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { text: 'hello' } }] });
	await index.publish('one');
	await waitFor(() => snapshot(index).native.reader.snapshotBytesError !== null);
	const failed = snapshot(index);
	assert.strictEqual(failed.native.reader.snapshotBytesError, 'E_STORAGE');
	assert.strictEqual(failed.native.operations.publish.succeeded, 1n);
	assert.strictEqual(failed.native.operations.publish.failed, 0n);
	assert.strictEqual((await index.search({ text: 'hello' })).total, 1);
	await index.reload();
	await waitFor(() => snapshot(index).native.reader.snapshotBytes !== null);
	assert.strictEqual(snapshot(index).native.reader.snapshotBytesError, null);
});

test('facade rejection labels whitelist codes and cannot replace a caller failure', async (context) => {
	const { index } = await fixture(context);
	const unknown = new FulltextError('secret-query-as-code', 'private-input');
	const malicious = new FulltextError('E_INVALID_ARGUMENT', 'private-input');
	Object.defineProperty(malicious, 'code', {
		get() {
			throw new Error('telemetry-must-not-leak');
		},
	});
	for (const original of [unknown, malicious]) {
		await assert.rejects(
			index.search({
				get query() {
					throw original;
				},
			}),
			(failure) => {
				assert.strictEqual(failure, original);
				return true;
			},
		);
	}
	const telemetry = snapshot(index);
	assert.deepStrictEqual(telemetry.preparationRejections.search, { total: 2n, errors: { other: 2n } });
	assert.strictEqual(telemetry.native.operations.search.attempted, 0n);
});
