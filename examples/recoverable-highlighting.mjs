import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-recoverable-highlighting-'));
let index;
let failure;
try {
	index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'recoverable-highlights',
		generation: 'one',
		fields: [{ name: 'title' }],
		analyzer: 'english@2',
		surfaceTerms: true,
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 4 * 1024 * 1024,
			maxBatchBytes: 1024 * 1024,
		},
	});
	const matching = (id, count) => ({ id, fields: { title: 'needle '.repeat(count) } });
	const small = [
		matching('a', 600),
		{ id: 'nonmatch', fields: { title: 'haystack' } },
		matching('b', 600),
		matching('c', 600),
	];
	const oversized = [matching('large', 1100)];
	await index.applyMutationBatch({ upserts: [...small, ...oversized] });
	await index.commit();
	await index.reload();
	const request = { text: 'needle', mode: 'any' };
	const deadline = performance.now() + 30_000;
	async function collect(input) {
		const records = [];
		const states = [];
		const windowSize = 2; // Size windows for source bytes and expected match density.
		let offset = 0;
		let calls = 0;
		while (offset < input.length) {
			assert(++calls <= input.length, 'successful pages must advance');
			const remainingBudgetMilliseconds = Math.floor(deadline - performance.now());
			assert(remainingBudgetMilliseconds > 0, 'original recovery budget expired');
			const window = input.slice(offset, offset + windowSize);
			const page = await index.traceMatchesPage(request, window, { remainingBudgetMilliseconds });
			assert(page.nextRecord > 0 && page.nextRecord <= window.length);
			assert.strictEqual(page.states.length, page.nextRecord);
			assert.strictEqual(
				page.complete,
				page.nextRecord === window.length &&
					page.states.every(({ analysisComplete, spansComplete }) => analysisComplete && spansComplete),
			);
			assert.deepStrictEqual(
				page.states.map(({ id }) => id),
				input.slice(offset, offset + page.nextRecord).map(({ id }) => id),
			);
			records.push(...page.records);
			states.push(...page.states);
			offset += page.nextRecord;
			// Advance globally; refill a bounded window without retrying consumed partial states.
		}
		return { records, states, calls };
	}
	const recovered = await collect(small);
	assert(recovered.states.every(({ analysisComplete, spansComplete }) => analysisComplete && spansComplete));
	assert.strictEqual(recovered.states.find(({ id }) => id === 'nonmatch').membership, 'no-match');
	assert.deepStrictEqual(
		recovered.records.map(({ id }) => id),
		['a', 'b', 'c'],
	);
	assert.strictEqual(
		recovered.records.reduce(
			(total, record) => total + record.values.reduce((n, value) => n + value.spans.length, 0),
			0,
		),
		1800,
	);
	const partial = await collect(oversized);
	assert.strictEqual(partial.calls, 1);
	assert.deepStrictEqual(partial.states, [
		{ id: 'large', membership: 'match', analysisComplete: true, spansComplete: false },
	]);
	assert.strictEqual(partial.records[0].values[0].spans.length, 1024);
	console.log({ recoveredSpans: 1800, completedInputs: recovered.states.length, terminalPartialSpans: 1024 });
} catch (error) {
	failure = error;
} finally {
	try {
		if (index) {
			const closed = await index.close({ mode: 'rollback' });
			if (closed?.cleanupError) throw closed.cleanupError;
			assert.strictEqual(index.status().state, 'closed');
		}
		await rm(directory, { recursive: true, force: true });
	} catch (cleanupError) {
		console.error('Cleanup failed; preserved example data at', directory);
		failure = failure ? new AggregateError([failure, cleanupError], 'Example and cleanup failed') : cleanupError;
	}
}
if (failure) throw failure;
