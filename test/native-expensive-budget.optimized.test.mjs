import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { configureNativeFullTextRuntime, NativeFullTextIndex } from '@harperfast/fulltext/native';
import { encodeOpen } from '../dist/codec.js';
import { invoke } from '../dist/invoke.js';
import { loadAddon } from '../dist/load-addon.js';

test('optimized native searches preserve ordinary capacity and close queued expensive work', async (context) => {
	const indexPath = mkdtempSync(path.join(tmpdir(), 'fulltext-optimized-permit-'));
	context.after(() => rmSync(indexPath, { recursive: true, force: true }));
	configureNativeFullTextRuntime({
		maxResidentIndexes: 1,
		maxIndexingThreads: 1,
		maxSearchThreads: 3,
		maxWriterMemoryBytes: 15_000_000,
		maxQueuedBytes: 2 * 1024 * 1024,
		maxExpensiveSearches: 1,
	});
	const options = {
		path: indexPath,
		indexId: 'products',
		generation: 'v1',
		fields: [{ name: 'title', weight: 1 }],
		analyzer: 'english@2',
		stopWords: true,
		positions: true,
		surfaceTerms: true,
		synonyms: [],
		limits: {
			indexingThreads: 1,
			searchThreads: 3,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 16,
			maxQueuedBytes: 1024 * 1024,
			maxBatchBytes: 1024 * 1024,
		},
	};
	const addon = loadAddon();
	assert(
		addon.__testDelayNextExpensiveSearch && addon.__testDelayNextOrdinarySearch && addon.__testExpensiveSearchState,
	);
	const opened = await invoke((callback) => addon.__nativeOpen(encodeOpen(options), callback));
	const handle = opened.u32();
	assert.strictEqual(opened.u8(), 0);
	opened.finish();
	const index = new NativeFullTextIndex({
		handle,
		maxBatchBytes: options.limits.maxBatchBytes,
		fieldNames: options.fields.map((field) => field.name),
	});
	context.after(() => index.close({ mode: 'rollback' }).catch(() => undefined));
	await index.applyMutationBatch({ upserts: [{ id: 'one', fields: { title: 'waterproof trail running shoe' } }] });
	await index.commit();
	await index.reload();

	addon.__testDelayNextExpensiveSearch(handle, 150);
	const beforeContention = index.status();
	const first = index.search({ text: 'trail running', mode: 'phrase' }, { remainingBudgetMilliseconds: 1_000 });
	await waitFor(() => addon.__testExpensiveSearchState(handle)[0] === 0);
	await assert.rejects(
		index.search({ text: 'trail running', mode: 'phrase' }, { remainingBudgetMilliseconds: 10 }),
		(error) => error.code === 'E_TIMEOUT',
	);
	assert.strictEqual((await first).total, 1);
	const afterContention = index.status();
	assert(
		afterContention.metrics.searchQueueNanoseconds - beforeContention.metrics.searchQueueNanoseconds >= 5_000_000n,
	);

	addon.__testDelayNextOrdinarySearch(handle, 200);
	const ordinaryBlocker = index.search({ text: 'trail' });
	await waitFor(() => addon.__testExpensiveSearchState(handle)[2] === 0);
	addon.__testDelayNextExpensiveSearch(handle, 150);
	const expensiveHolder = index.search(
		{ text: 'trail running', mode: 'phrase' },
		{ remainingBudgetMilliseconds: 1_000 },
	);
	await waitFor(() => addon.__testExpensiveSearchState(handle)[0] === 0);
	const expensiveQueued = index.search(
		{ text: 'trail running', mode: 'phrase' },
		{ remainingBudgetMilliseconds: 1_000 },
	);
	await waitFor(() => index.status().searchQueuedCommands > 0n);
	assert.strictEqual((await index.search({ text: 'waterproof' })).total, 1);
	await Promise.all([ordinaryBlocker, expensiveHolder, expensiveQueued]);

	addon.__testDelayNextExpensiveSearch(handle, 150);
	const holding = index.search({ text: 'trail running', mode: 'phrase' }, { remainingBudgetMilliseconds: 1_000 });
	await waitFor(() => addon.__testExpensiveSearchState(handle)[0] === 0);
	const waiting = index.search({ text: 'trail running', mode: 'phrase' }, { remainingBudgetMilliseconds: 1_000 });
	await waitFor(() => index.status().searchQueuedCommands > 0n);
	const closing = index.close();
	await assert.rejects(waiting, (error) => error.code === 'E_CLOSED');
	await holding;
	await closing;
});

async function waitFor(condition) {
	const deadline = Date.now() + 5_000;
	while (!condition()) {
		if (Date.now() >= deadline) throw new Error('timed out waiting for native test state');
		await new Promise((resolve) => setTimeout(resolve, 2));
	}
}
