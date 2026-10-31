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
// Call only after operation promises settle: this identity assumes no queued work.
function conservedAtQuiescence(operation) {
	assert.strictEqual(operation.dispatched, 0n);
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
	for (const operation of Object.values(telemetry.native.operations)) conservedAtQuiescence(operation);
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
	await assert.rejects(index.search({ text: 'hello' }), { code: 'E_CLOSED' });
	await assert.rejects(index.traceMatches({ text: 'hello' }, []), { code: 'E_CLOSED' });
	const rejectedAfterClose = snapshot(index);
	assert.strictEqual(rejectedAfterClose.preparationRejections.search.errors.E_CLOSED, 1n);
	assert.strictEqual(rejectedAfterClose.preparationRejections.trace.errors.E_CLOSED, 1n);
	assert.deepStrictEqual(rejectedAfterClose.native.operations, final.native.operations);
	const retired = await resetNativeFullTextIndex({ path: config.path, indexId: config.indexId });
	assert.strictEqual(retired.state, 'reset');
	assert.strictEqual(snapshot(index).native.operations.close.completed, 1n);
	if (retired.state === 'reset') rmSync(retired.retiredPath, { recursive: true, force: true });
});

test('accessor-triggered close rejects search and trace before native handoff', async (context) => {
	for (const operation of ['search', 'trace']) {
		const { index } = await fixture(context);
		let closing;
		const beginClose = () => (closing ??= index.close());
		const request =
			operation === 'search'
				? {
						get text() {
							beginClose();
							return 'hello';
						},
					}
				: {
						text: 'hello',
						get fields() {
							beginClose();
							return ['text'];
						},
					};
		const query = operation === 'search' ? index.search(request) : index.traceMatches(request, []);
		const [result, close] = await Promise.allSettled([query, closing ?? Promise.resolve()]);
		assert(closing);
		assert.strictEqual(result.status, 'rejected');
		assert.strictEqual(result.reason.code, 'E_CLOSED');
		assert.strictEqual(close.status, 'fulfilled');
		assert.strictEqual(close.value.cleanupError, undefined);
		const telemetry = snapshot(index);
		assert.strictEqual(telemetry.preparationRejections[operation].errors.E_CLOSED, 1n);
		assert.strictEqual(telemetry.native.operations[operation].attempted, 0n);
	}
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
	conservedAtQuiescence(operation);
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
		conservedAtQuiescence(telemetry.native.operations.publish);
	});
}

test('dirty close is rejected while a final rollback close is fully accounted', async (context) => {
	const { index } = await fixture(context);
	await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { text: 'hello' } }] });
	await assert.rejects(index.close(), { code: 'E_DIRTY_CLOSE' });
	const dirty = snapshot(index).native.operations.close;
	assert.strictEqual(dirty.errors.E_DIRTY_CLOSE, 1n);
	conservedAtQuiescence(dirty);
	await index.close({ mode: 'rollback' });
	const final = snapshot(index).native.operations.close;
	assert.strictEqual(final.succeeded, 1n);
	conservedAtQuiescence(final);
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

test('delete-only publication invalidates snapshot bytes without changing the segment count', async (context) => {
	const { index, addon, handle } = await rawFixture(context);
	await index.applyMutationBatch({
		upserts: [
			{ id: 'a', fields: { text: 'hello' } },
			{ id: 'b', fields: { text: 'hello again' } },
		],
	});
	await index.publish('before-delete');
	index.configureTelemetry({ snapshotBytes: true });
	await index.reload();
	await waitFor(() => {
		const reader = snapshot(index).native.reader;
		return !reader.snapshotBytesPending && reader.snapshotBytes !== null;
	});
	const before = snapshot(index).native.reader;
	assert.strictEqual(before.documents, 2n);
	assert.strictEqual(before.deletedDocuments, 0n);
	assert.strictEqual(before.segments, 1n);
	assert(before.snapshotBytes > 0n);
	assert.strictEqual(before.snapshotBytesError, null);

	addon.__testFailNextTelemetryObservation(handle);
	await index.applyMutationBatch({ deletes: ['a'] });
	await index.publish('after-delete');
	await waitFor(() => {
		const reader = snapshot(index).native.reader;
		return !reader.snapshotBytesPending && reader.snapshotBytesError === 'E_STORAGE';
	});
	const deleted = snapshot(index).native.reader;
	assert.strictEqual(deleted.documents, 1n);
	assert.strictEqual(deleted.deletedDocuments, 1n);
	assert.strictEqual(deleted.segments, before.segments);
	assert.strictEqual(deleted.snapshotBytes, null);
	assert.strictEqual(deleted.snapshotBytesAgeNanoseconds, null);

	await index.reload();
	await waitFor(() => {
		const reader = snapshot(index).native.reader;
		return !reader.snapshotBytesPending && reader.snapshotBytes !== null;
	});
	const recovered = snapshot(index).native.reader;
	assert.strictEqual(recovered.documents, 1n);
	assert.strictEqual(recovered.deletedDocuments, 1n);
	assert.strictEqual(recovered.segments, before.segments);
	assert(recovered.snapshotBytes > 0n);
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

test('trace encoding getters retain normalized API errors and count one preparation rejection', async (context) => {
	const { index } = await fixture(context);
	const original = new Error('caller encoding getter failed');
	const typed = new FulltextError('E_INVALID_ARGUMENT', 'caller encoding getter rejected');
	for (const failure of [original, typed]) {
		await assert.rejects(
			index.traceMatches(
				{
					text: 'private-query',
					get fields() {
						throw failure;
					},
				},
				[],
			),
			(error) => {
				assert(error instanceof FulltextError);
				if (failure === typed) assert.strictEqual(error, typed);
				else {
					assert.strictEqual(error.code, 'E_NATIVE_FAILURE');
					assert.strictEqual(error.cause, original);
				}
				return true;
			},
		);
	}
	const telemetry = snapshot(index);
	assert.deepStrictEqual(telemetry.preparationRejections.trace, {
		total: 2n,
		errors: { E_NATIVE_FAILURE: 1n, E_INVALID_ARGUMENT: 1n },
	});
	assert.strictEqual(telemetry.native.operations.trace.attempted, 0n);
});

test('an actual search actor panic records one failure and preserves final counters after close', async (context) => {
	const { index, addon, handle } = await rawFixture(context);
	addon.__testPanicNextSearch(handle);
	await assert.rejects(index.search({ text: 'hello' }), { code: 'E_NATIVE_PANIC' });
	await waitFor(() => snapshot(index).native.state === 'poisoned');
	const poisoned = snapshot(index).native.operations.search;
	assert.strictEqual(poisoned.attempted, 1n);
	assert.strictEqual(poisoned.accepted, 1n);
	assert.strictEqual(poisoned.rejected, 0n);
	assert.strictEqual(poisoned.completed, 1n);
	assert.strictEqual(poisoned.failed, 1n);
	assert.strictEqual(poisoned.succeeded, 0n);
	assert.strictEqual(poisoned.errors.E_NATIVE_PANIC, 1n);
	conservedAtQuiescence(poisoned);
	const result = await index.close({ mode: 'rollback' });
	assert.strictEqual(result.cleanupError, undefined);
	const final = snapshot(index).native;
	assert.strictEqual(final.state, 'closed');
	assert.deepStrictEqual(final.operations.search, poisoned);
	assert.strictEqual(final.operations.close.completed, 1n);
	assert.strictEqual(final.operations.close.succeeded, 1n);
	conservedAtQuiescence(final.operations.close);
});

test('worker panic settles its bound ticket and queued fallback tickets exactly once', async (context) => {
	const { index, addon, handle } = await rawFixture(context, { searchThreads: 1, maxQueuedCommands: 16 });
	await index.applyMutationBatch({ upserts: [{ id: 'one', fields: { text: 'catalog' } }] });
	await index.publish('one');
	index.configureTelemetry({ histograms: true });
	addon.__testDelayNextOrdinarySearch(handle, 200);
	addon.__testPanicNextSearch(handle);
	const queries = Array.from({ length: 8 }, () => index.search({ text: 'catalog', exactTotal: false }));
	const results = await Promise.allSettled(queries);
	assert(results.every((result) => result.status === 'rejected' && result.reason.code === 'E_NATIVE_PANIC'));
	const operation = snapshot(index).native.operations.search;
	assert.strictEqual(operation.attempted, 8n);
	assert.strictEqual(operation.accepted, 8n);
	assert.strictEqual(operation.completed, 8n);
	assert.strictEqual(operation.failed, 8n);
	assert.strictEqual(operation.errors.E_NATIVE_PANIC, 8n);
	assert.strictEqual(operation.succeeded, 0n);
	assert.strictEqual(operation.rejected, 0n);
	conservedAtQuiescence(operation);
	const closed = await index.close({ mode: 'rollback' });
	assert.strictEqual(closed.cleanupError, undefined);
	assert.deepStrictEqual(snapshot(index).native.operations.search, operation);
});

test('poison before native admission rejects one attempt without inventing a completed command', async (context) => {
	const { index, addon, handle } = await rawFixture(context);
	addon.__testPoisonBeforeNextAdmission(handle);
	await assert.rejects(index.commit(), { code: 'E_POISONED' });
	const poisoned = snapshot(index).native;
	assert.strictEqual(poisoned.state, 'poisoned');
	const rejected = poisoned.operations.commit;
	assert.strictEqual(rejected.attempted, 1n);
	assert.strictEqual(rejected.rejected, 1n);
	assert.strictEqual(rejected.accepted, 0n);
	assert.strictEqual(rejected.completed, 0n);
	assert.strictEqual(rejected.succeeded, 0n);
	assert.strictEqual(rejected.failed, 0n);
	assert.strictEqual(rejected.errors.E_POISONED, 1n);
	conservedAtQuiescence(rejected);
	const result = await index.close({ mode: 'rollback' });
	assert.strictEqual(result.cleanupError, undefined);
	const final = snapshot(index).native;
	assert.strictEqual(final.state, 'closed');
	assert.deepStrictEqual(final.operations.commit, rejected);
	assert.strictEqual(final.operations.close.completed, 1n);
	assert.strictEqual(final.operations.close.succeeded, 1n);
	conservedAtQuiescence(final.operations.close);
});

test('concurrent search callers remain correct while telemetry is repeatedly scraped', async (context) => {
	const { index, addon, handle } = await rawFixture(context, { searchThreads: 4 });
	const records = Array.from({ length: 64 }, (_, number) => ({
		id: `item-${number}`,
		fields: { text: 'shared catalog product' },
	}));
	const ids = new Set(records.map((record) => record.id));
	await index.applyMutationBatch({ upserts: records });
	await index.publish('one');
	index.configureTelemetry({ histograms: true });

	const callerCount = 4;
	const queriesPerCaller = 12;
	let scrapesWhileNativePending = 0;
	addon.__testDelayNextOrdinarySearch(handle, 200);
	const callers = Array.from({ length: callerCount }, async () => {
		for (let query = 0; query < queriesPerCaller; query++) {
			const result = await index.search(
				{ text: 'shared', mode: 'any', limit: records.length },
				{ remainingBudgetMilliseconds: 1_000 },
			);
			assert.strictEqual(result.total, records.length);
			assert.strictEqual(result.totalRelation, 'exact');
			assert.strictEqual(result.hits.length, records.length);
			assert.strictEqual(new Set(result.hits.map((hit) => hit.id)).size, records.length);
			assert(result.hits.every((hit) => ids.has(hit.id)));
			await setImmediate();
		}
	});
	const nativePending = (telemetry) =>
		telemetry.operations.search.dispatched > 0n || telemetry.queues.ordinarySearch.commands > 0n;
	const scraper = (async () => {
		await waitFor(() => {
			const telemetry = snapshot(index).native;
			if (!nativePending(telemetry)) return false;
			scrapesWhileNativePending++;
			return true;
		});
		for (let round = 0; round < callerCount * queriesPerCaller; round++) {
			for (let sample = 0; sample < 3; sample++) {
				const telemetry = snapshot(index).native;
				if (nativePending(telemetry)) scrapesWhileNativePending++;
				assert(telemetry.operations.search.dispatched >= 0n);
				assert(telemetry.operations.search.dispatched <= BigInt(callerCount));
				for (const queue of Object.values(telemetry.queues)) {
					assert.strictEqual(queue.available, true);
					assert(queue.commands >= 0n && queue.commands <= queue.maxCommands);
					assert(queue.bytes >= 0n && queue.bytes <= queue.maxBytes);
					if (queue.oldestAgeNanoseconds !== null) assert(queue.oldestAgeNanoseconds >= 0n);
				}
			}
			await setImmediate();
		}
	})();
	const results = await Promise.allSettled([...callers, scraper]);
	for (const result of results) {
		if (result.status === 'rejected') throw result.reason;
	}
	assert(scrapesWhileNativePending > 0);
	const final = snapshot(index).native;
	const searches = final.operations.search;
	assert.strictEqual(final.operations.expensiveSearch.attempted, 0n);
	const totalQueries = BigInt(callerCount * queriesPerCaller);
	assert.strictEqual(searches.attempted, totalQueries);
	assert.strictEqual(searches.accepted, totalQueries);
	assert.strictEqual(searches.succeeded, totalQueries);
	assert.strictEqual(searches.failed, 0n);
	assert.strictEqual(searches.rejected, 0n);
	assert.strictEqual(searches.execution.histogram.count, totalQueries);
	conservedAtQuiescence(searches);
	for (const queue of Object.values(final.queues)) {
		assert.strictEqual(queue.commands, 0n);
		assert.strictEqual(queue.bytes, 0n);
		assert.strictEqual(queue.oldestAgeNanoseconds, null);
	}
});
