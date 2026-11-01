import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

export async function mixedTelemetryCounterStress(context, histograms) {
	const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-counter-stress-'));
	const index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'counter-stress',
		generation: 'one',
		fields: [{ name: 'text' }],
		analyzer: 'english@2',
		positions: true,
		surfaceTerms: true,
		limits: {
			indexingThreads: 1,
			searchThreads: 4,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 128,
			maxQueuedBytes: 8 << 20,
			maxBatchBytes: 1 << 20,
		},
	});
	context.after(async () => {
		const closed = await index.close({ mode: 'rollback' });
		assert.strictEqual(closed.cleanupError, undefined);
		rmSync(directory, { recursive: true, force: true });
	});
	await index.applyMutationBatch({ upserts: [{ id: 'one', fields: { text: 'catalog product' } }] });
	await index.publish('one');
	index.configureTelemetry({ histograms });
	const total = 10_080;
	let next = 0;
	const results = await Promise.allSettled(
		Array.from({ length: 16 }, async () => {
			while (next < total) {
				const operation = next++ % 3;
				if (operation === 2) {
					const result = await index.traceMatches({ text: 'catalog' }, [
						{ id: 'one', fields: { text: 'catalog product' } },
					]);
					assert.strictEqual(result.complete, true);
					assert.deepStrictEqual(
						result.records.map((record) => record.id),
						['one'],
					);
				} else {
					const result = await index.search(
						operation === 0
							? { text: 'catalog', mode: 'any', exactTotal: false, limit: 1 }
							: { text: 'cata', mode: 'prefix', exactTotal: true, limit: 1 },
					);
					assert.strictEqual(result.total, 1);
					assert.deepStrictEqual(
						result.hits.map((hit) => hit.id),
						['one'],
					);
				}
			}
		}),
	);
	for (const result of results) if (result.status === 'rejected') throw result.reason;
	const telemetry = index.telemetry();
	assert.strictEqual(telemetry.available, true);
	for (const name of ['search', 'expensiveSearch', 'trace']) {
		const operation = telemetry.native.operations[name];
		assert.strictEqual(operation.attempted, BigInt(total / 3));
		assert.strictEqual(operation.accepted, BigInt(total / 3));
		assert.strictEqual(operation.completed, BigInt(total / 3));
		assert.strictEqual(operation.succeeded, BigInt(total / 3));
		assert.strictEqual(operation.failed, 0n);
		assert.strictEqual(operation.rejected, 0n);
		assert.strictEqual(operation.dispatched, 0n);
		assert.strictEqual(operation.queue.count, BigInt(total / 3));
		assert.strictEqual(operation.execution.count, BigInt(total / 3));
		assert.strictEqual(operation.execution.histogram.count, histograms ? BigInt(total / 3) : 0n);
		assert.strictEqual(operation.queue.histogram.count, histograms ? BigInt(total / 3) : 0n);
	}
	const closed = await index.close();
	assert.strictEqual(closed.cleanupError, undefined);
	const final = index.telemetry();
	assert.strictEqual(final.available, true);
	assert.strictEqual(final.native.state, 'closed');
	for (const name of ['search', 'expensiveSearch', 'trace']) {
		assert.deepStrictEqual(final.native.operations[name], telemetry.native.operations[name]);
	}
}
