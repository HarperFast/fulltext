import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { configureNativeFullTextRuntime, NativeFullTextIndex } from '@harperfast/fulltext/native';
import { encodeOpen } from '../dist/codec.js';
import { invoke } from '../dist/invoke.js';
import { loadAddon } from '../dist/load-addon.js';

test(
	'raised query capacity preserves ordinary routing and shared expensive admission across handles',
	{ timeout: 30_000 },
	async () => {
		configureNativeFullTextRuntime({
			maxResidentIndexes: 2,
			maxIndexingThreads: 2,
			maxSearchThreads: 6,
			maxWriterMemoryBytes: 30_000_000,
			maxQueuedBytes: 4 << 20,
			maxExpensiveSearches: 1,
		});
		const root = mkdtempSync(path.join(tmpdir(), 'fulltext-capacity-admission-'));
		const addon = loadAddon();
		const opened = [];
		try {
			for (const raised of [true, false]) {
				const config = {
					path: path.join(root, String(raised)),
					indexId: 'capacity',
					generation: 'one',
					fields: [{ name: 'title', weight: 1 }],
					filterFields: [],
					analyzer: 'english@2',
					positions: true,
					surfaceTerms: true,
					stopWords: false,
					synonyms: [],
					limits: {
						indexingThreads: 1,
						searchThreads: 3,
						writerMemoryBytes: 15_000_000,
						maxQueuedCommands: 16,
						maxQueuedBytes: 1 << 20,
						maxBatchBytes: 1 << 20,
						maxQueryTerms: raised ? 1024 : 64,
						maxQueryClauses: raised ? 4096 : 256,
					},
				};
				const response = await invoke((callback) => addon.__nativeOpen(encodeOpen(config), callback));
				const handle = response.u32();
				assert.strictEqual(response.u8(), 0);
				response.finish();
				const index = new NativeFullTextIndex({
					handle,
					maxBatchBytes: config.limits.maxBatchBytes,
					searchLimits: config.limits,
					fieldNames: ['title'],
				});
				opened.push({ index, handle });
				await index.applyMutationBatch({ upserts: [{ id: 'one', fields: { title: 'word0 word1' } }] });
				await index.commit();
				await index.reload();
			}
			const [raised, ordinary] = opened;
			addon.__testDelayNextOrdinarySearch(raised.handle, 1500);
			addon.__testDelayNextExpensiveSearch(raised.handle, 1500);
			const small = raised.index.search({ text: 'word0' });
			try {
				await waitFor(() => raised.index.status().searchQueuedCommands === 0n);
				assert.strictEqual(
					(await ordinary.index.search({ text: 'word0 word1', mode: 'phrase' }, { remainingBudgetMilliseconds: 500 }))
						.total,
					1,
				);
			} finally {
				await small;
			}
			assert.strictEqual(addon.__testExpensiveSearchState(raised.handle)[0], 1500);
			const text = Array.from({ length: 65 }, (_, i) => `word${i}`).join(' ');
			const large = raised.index.search({ text });
			try {
				await waitFor(() => addon.__testExpensiveSearchState(raised.handle)[0] === 0);
				await assert.rejects(
					ordinary.index.search({ text: 'word0 word1', mode: 'phrase' }, { remainingBudgetMilliseconds: 10 }),
					(error) => error.code === 'E_TIMEOUT',
				);
				assert.strictEqual((await ordinary.index.search({ text: 'word0' })).total, 1);
			} finally {
				await large;
			}
			assert.strictEqual((await ordinary.index.search({ text: 'word0 word1', mode: 'phrase' })).total, 1);
		} finally {
			for (const { index } of opened) assert(!(await index.close({ mode: 'rollback' })).cleanupError);
			rmSync(root, { recursive: true, force: true });
		}
	},
);

async function waitFor(condition) {
	const deadline = performance.now() + 5000;
	while (!condition()) {
		assert(performance.now() < deadline, 'native admission state did not advance');
		await new Promise((resolve) => setTimeout(resolve, 1));
	}
}
