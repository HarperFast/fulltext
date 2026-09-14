import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { encodeMutationBatch, NativeFullTextIndex, openNativeFullTextIndex } from '@harperfast/fulltext/native';
import { encodeOpen } from '../dist/codec.js';
import { invoke } from '../dist/invoke.js';
import { loadAddon } from '../dist/load-addon.js';

function config(context) {
	const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-publication-'));
	return {
		path: directory,
		indexId: 'publication',
		generation: 'one',
		fields: [{ name: 'title', weight: 1 }],
		analyzer: 'english@1',
		stopWords: true,
		positions: true,
		surfaceTerms: false,
		limits: {
			indexingThreads: 1,
			searchThreads: 1,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 1024 * 1024,
			maxBatchBytes: 1024 * 1024,
		},
	};
}

const batch = (id, title) => encodeMutationBatch({ upserts: [{ id, fields: { title } }] });
const hasCode = (code) => (error) => error.code === code;

function cleanup(context, options, currentIndex) {
	context.after(async () => {
		await currentIndex().close({ mode: 'rollback' });
		rmSync(options.path, { recursive: true, force: true });
	});
}

test('publishes visible mutations and opaque checkpoints, reopens, and replays by ID', async (context) => {
	const options = config(context);
	let index = await openNativeFullTextIndex(options);
	cleanup(context, options, () => index);
	assert.strictEqual(index.committedPayload, undefined);
	await index.apply(batch('one', 'original catalog'));
	await index.commit();
	assert.strictEqual((await index.search({ text: 'original', exactTotal: true })).total, 0);
	await index.reload();
	assert.strictEqual((await index.search({ text: 'original', exactTotal: true })).total, 1);
	assert.strictEqual(index.committedPayload, undefined);
	const payload = 'opaque\u0000:雪:🦀';
	await index.apply(batch('one', 'updated catalog'));
	const opstamp = await index.publish(payload);
	assert.strictEqual(typeof opstamp, 'bigint');
	assert.strictEqual(index.status().commitOpstamp, opstamp);
	assert.strictEqual(index.status().uncommittedMutations, 0n);
	assert.strictEqual(index.committedPayload, payload);
	assert.strictEqual((await index.search({ text: 'updated', exactTotal: true })).total, 1);
	await index.close();
	index = await openNativeFullTextIndex(options);
	assert.strictEqual(index.committedPayload, payload);
	await assert.rejects(index.commit(), hasCode('E_CHECKPOINT_REQUIRED'));
	await index.apply(batch('one', 'updated catalog'));
	await index.publish('replayed');
	assert.strictEqual((await index.search({ text: 'updated', exactTotal: true })).total, 1);
	await index.apply(encodeMutationBatch({ deletes: ['one'] }));
	await index.publish('deleted');
	assert.strictEqual((await index.search({ text: 'updated', exactTotal: true })).total, 0);
});

test('checks a queued commit after earlier publication and retains staged work on rejection', async (context) => {
	const options = config(context);
	let index = await openNativeFullTextIndex(options);
	cleanup(context, options, () => index);
	const published = index.publish('');
	const committed = assert.rejects(index.commit(), hasCode('E_CHECKPOINT_REQUIRED'));
	await Promise.all([published, committed]);
	assert.strictEqual(index.committedPayload, '');
	assert.strictEqual(index.status().state, 'open');
	await index.apply(batch('one', 'pending product'));
	await assert.rejects(index.commit(), hasCode('E_CHECKPOINT_REQUIRED'));
	assert.strictEqual(index.status().uncommittedMutations, 1n);
	await assert.rejects(index.close(), hasCode('E_DIRTY_CLOSE'));
	await index.publish('next');
	assert.strictEqual((await index.search({ text: 'pending', exactTotal: true })).total, 1);
	await index.publish('');
	await index.close();
	index = await openNativeFullTextIndex(options);
	assert.strictEqual(index.committedPayload, '');
	await assert.rejects(index.commit(), hasCode('E_CHECKPOINT_REQUIRED'));
	await index.apply(batch('one', 'rolled back'));
	await index.close({ mode: 'rollback' });
	index = await openNativeFullTextIndex(options);
	assert.strictEqual(index.committedPayload, '');
	assert.strictEqual((await index.search({ text: 'pending', exactTotal: true })).total, 1);
});

test('preserves the latest checkpoint for overlapping and cursor-only publications', async (context) => {
	const options = config(context);
	let index = await openNativeFullTextIndex(options);
	cleanup(context, options, () => index);
	const stamps = await Promise.all([index.publish('one'), index.publish('two'), index.publish('three')]);
	assert(stamps[0] < stamps[1] && stamps[1] < stamps[2]);
	assert.strictEqual(index.committedPayload, 'three');
	await index.close();
	index = await openNativeFullTextIndex(options);
	assert.strictEqual(index.committedPayload, 'three');
});

test('validates Unicode and the UTF-8 byte limit without poisoning readback', async (context) => {
	const options = config(context);
	let index = await openNativeFullTextIndex(options);
	cleanup(context, options, () => index);
	const boundary = '🦀'.repeat(16 * 1024);
	await index.publish(boundary);
	for (const payload of [undefined, 42, '\ud800', '\udfff', boundary + 'x']) {
		await assert.rejects(index.publish(payload), hasCode('E_INVALID_ARGUMENT'));
		assert.strictEqual(index.committedPayload, boundary);
	}
	assert.strictEqual(index.status().state, 'open');
	await index.close();
	index = await openNativeFullTextIndex(options);
	assert.strictEqual(index.committedPayload, boundary);
});

test('a synchronous queue rejection does not invalidate an earlier pending publication', async (context) => {
	const options = config(context);
	options.limits.maxQueuedBytes = options.limits.maxBatchBytes = 128;
	const index = await openNativeFullTextIndex(options);
	cleanup(context, options, () => index);
	await Promise.all([
		index.publish('accepted'),
		assert.rejects(index.publish('x'.repeat(129)), hasCode('E_QUEUE_FULL')),
	]);
	assert.strictEqual(index.committedPayload, 'accepted');
	assert.strictEqual(index.status().state, 'open');
	await index.publish('retry');
	assert.strictEqual(index.committedPayload, 'retry');
});

for (const afterCommit of [false, true]) {
	for (const prior of [undefined, 'previous']) {
		test(`recovers a ${afterCommit ? 'committed' : 'uncommitted'} failed publication after ${prior ?? 'no checkpoint'}`, async (context) => {
			const options = config(context);
			const addon = loadAddon();
			const opened = await invoke((callback) => addon.__nativeOpen(encodeOpen(options), callback));
			const handle = opened.u32();
			assert.strictEqual(opened.u8(), 0);
			opened.finish();
			let index = new NativeFullTextIndex(handle);
			cleanup(context, options, () => index);
			if (prior !== undefined) await index.publish(prior);
			await index.apply(batch('one', 'recoverable catalog'));
			addon.__testFailNextPublish(handle, afterCommit);
			await assert.rejects(index.publish('new checkpoint'), hasCode('E_STORAGE'));
			assert.throws(() => index.committedPayload, hasCode('E_POISONED'));
			await assert.rejects(index.apply(batch('two', 'rejected')), hasCode('E_POISONED'));
			await index.close({ mode: 'rollback' });
			assert.throws(() => index.committedPayload, hasCode('E_POISONED'));
			index = await openNativeFullTextIndex(options);
			assert.strictEqual(index.committedPayload, afterCommit ? 'new checkpoint' : prior);
			assert.strictEqual((await index.search({ text: 'recoverable', exactTotal: true })).total, afterCommit ? 1 : 0);
			if (!afterCommit && prior === undefined) await index.commit();
			else await assert.rejects(index.commit(), hasCode('E_CHECKPOINT_REQUIRED'));
		});
	}
}
