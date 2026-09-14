import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { encodeMutationBatch, NativeFullTextIndex, openNativeFullTextIndex } from '@harperfast/fulltext/native';
import { encodeOpen } from '../dist/codec.js';
import { invoke } from '../dist/invoke.js';
import { loadAddon } from '../dist/load-addon.js';
import { normalizeNativeError } from '../dist/errors.js';

const batch = (id) => encodeMutationBatch({ upserts: [{ id, fields: { title: 'catalog product' } }] });
const hasCode = (code) => (error) => error.code === code;

async function fixture(context) {
	const options = {
		path: mkdtempSync(path.join(tmpdir(), 'fulltext-admission-')),
		indexId: 'admission',
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
			maxQueuedCommands: 8,
			maxQueuedBytes: 1024 * 1024,
			maxBatchBytes: 1024 * 1024,
		},
	};
	const addon = loadAddon();
	assert(addon.__testPoisonBeforeNextAdmission);
	const opened = await invoke((callback) => addon.__nativeOpen(encodeOpen(options), callback));
	const handle = opened.u32();
	assert.strictEqual(opened.u8(), 0);
	opened.finish();
	const index = new NativeFullTextIndex(handle);
	context.after(async () => {
		await index.close({ mode: 'rollback' });
		rmSync(options.path, { recursive: true, force: true });
	});
	await index.apply(batch('saved'));
	await index.publish('saved checkpoint');
	return { options, addon, handle, index };
}

async function verifyReopen(options) {
	const reopened = await openNativeFullTextIndex(options);
	try {
		assert.strictEqual(reopened.committedPayload, 'saved checkpoint');
		assert.deepStrictEqual(
			(await reopened.search({ text: 'catalog', exactTotal: true })).hits.map((hit) => hit.id),
			['saved'],
		);
	} finally {
		await reopened.close({ mode: 'rollback' });
	}
}

for (const operation of ['apply', 'commit', 'publish', 'reload']) {
	test(
		`rejects ${operation} when poison drains between the early check and insertion`,
		{ timeout: 10_000 },
		async (context) => {
			const { options, addon, handle, index } = await fixture(context);
			await index.apply(batch('pending'));
			addon.__testPoisonBeforeNextAdmission(handle);
			const result =
				operation === 'apply'
					? index.apply(batch('late'))
					: operation === 'publish'
						? index.publish('late checkpoint')
						: index[operation]();
			await assert.rejects(result, hasCode('E_POISONED'));
			const status = index.status();
			assert.strictEqual(status.state, 'poisoned');
			assert.strictEqual(status.writerQueuedCommands, 0n);
			assert.strictEqual(status.writerQueuedBytes, 0n);
			assert.strictEqual(status.uncommittedMutations, 1n);
			assert.strictEqual(index.committedPayload, 'saved checkpoint');
			await assert.rejects(index.search({ text: 'catalog' }), hasCode('E_POISONED'));
			await index.close();
			assert.throws(
				() => addon.__nativeStatus(handle),
				(error) => normalizeNativeError(error).code === 'E_CLOSED',
			);
			await verifyReopen(options);
		},
	);
}

test(
	'a dirty close racing poison rolls back without reopening the terminal generation',
	{ timeout: 10_000 },
	async (context) => {
		const { options, addon, handle, index } = await fixture(context);
		await index.apply(batch('pending'));
		addon.__testPoisonBeforeNextAdmission(handle);
		await index.close();
		assert.throws(
			() => addon.__nativeStatus(handle),
			(error) => normalizeNativeError(error).code === 'E_CLOSED',
		);
		await assert.rejects(index.apply(batch('late')), hasCode('E_CLOSED'));
		await verifyReopen(options);
	},
);
