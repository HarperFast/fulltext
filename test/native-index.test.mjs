import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { encodeMutationBatch, openNativeFullTextIndex } from '@harperfast/fulltext/native';

function options(indexPath, overrides = {}) {
	return {
		path: indexPath,
		indexId: 'products',
		generation: 'generation-1',
		fields: [{ name: 'title', weight: 3 }, { name: 'description' }],
		analyzer: 'english@1',
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8 * 1024 * 1024,
			maxBatchBytes: 8 * 1024 * 1024,
		},
		...overrides,
	};
}

function temporaryIndex(context) {
	const directory = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-index-'));
	context.after(() => rmSync(directory, { recursive: true, force: true }));
	return directory;
}

test('runs the public create, mutate, BM25 search, close, and reopen route', async (context) => {
	const indexPath = temporaryIndex(context);
	const config = options(indexPath);
	let index = await openNativeFullTextIndex(config);
	assert.strictEqual(
		await index.apply(
			encodeMutationBatch({
				upserts: [
					{
						id: 'shoe-1',
						fields: { title: 'Trail Running Shoes', description: 'red outdoor footwear' },
					},
					{ id: 'rack-1', fields: { title: 'Wood Rack', description: 'shoe organizer' } },
				],
			}),
		),
		2,
	);
	assert.strictEqual(index.status().uncommittedMutations, 2n);
	await index.commit();
	await index.reload();
	const approximate = await index.search({ text: 'running shoes', limit: 1 });
	assert.strictEqual(approximate.total, 1);
	assert.strictEqual(approximate.totalRelation, 'lower-bound');
	const result = await index.search({ text: 'running shoes', exactTotal: true });
	assert.strictEqual(result.total, 2);
	assert.strictEqual(result.totalRelation, 'exact');
	assert.strictEqual(result.hits[0].id, 'shoe-1');
	assert(result.hits[0].score > result.hits[1].score);
	await Promise.all([index.close(), index.close()]);
	assert.strictEqual(index.status().state, 'closed');

	index = await openNativeFullTextIndex(config);
	assert.deepStrictEqual(
		(await index.search({ text: 'running shoes', exactTotal: true })).hits.map((hit) => hit.id),
		['shoe-1', 'rack-1'],
	);
	await index.apply(encodeMutationBatch({ deletes: ['shoe-1'] }));
	await index.commit();
	await index.reload();
	const afterDelete = await index.search({ text: 'running shoes', exactTotal: true });
	assert.deepStrictEqual(
		afterDelete.hits.map((hit) => hit.id),
		['rack-1'],
	);
	assert(afterDelete.hits[0].score > 0);
	await index.close();
});

test('preserves clean-close state after a rejected batch', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	await assert.rejects(
		index.apply(
			encodeMutationBatch({
				upserts: [
					{ id: 'partial', fields: { title: 'must not survive' } },
					{ id: 'bad', fields: { unknown: 'value' } },
				],
			}),
		),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.strictEqual(index.status().uncommittedMutations, 0n);
	await index.apply(encodeMutationBatch({ upserts: [{ id: 'valid', fields: { title: 'survives' } }] }));
	await index.commit();
	await index.reload();
	assert.strictEqual((await index.search({ text: 'must survive', exactTotal: true })).total, 1);
	await index.close();
});

test('requires an explicit rollback when close would discard mutations', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	await index.apply(encodeMutationBatch({ upserts: [{ id: 'one', fields: { title: 'one' } }] }));
	await assert.rejects(index.close(), (error) => error.code === 'E_DIRTY_CLOSE');
	assert.strictEqual(index.status().state, 'open');
	await index.close({ mode: 'rollback' });
});

test('rejects duplicate opens and persisted identity drift', async (context) => {
	const indexPath = temporaryIndex(context);
	const config = options(indexPath);
	const first = await openNativeFullTextIndex(config);
	await assert.rejects(openNativeFullTextIndex(config), (error) => error.code === 'E_DUPLICATE_OPEN');
	await first.close();
	await assert.rejects(
		openNativeFullTextIndex(options(indexPath, { generation: 'generation-2' })),
		(error) => error.code === 'E_IDENTITY_MISMATCH',
	);
});

test('keeps the JavaScript event loop responsive while indexing', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	const upserts = Array.from({ length: 20_000 }, (_, id) => ({
		id: String(id),
		fields: { title: `running shoe model ${id}`, description: 'lightweight outdoor product' },
	}));
	let applySettled = false;
	let heartbeatsWhilePending = 0;
	const timer = setInterval(() => {
		if (!applySettled) heartbeatsWhilePending++;
	}, 5);
	await index.apply(encodeMutationBatch({ upserts })).finally(() => {
		applySettled = true;
	});
	clearInterval(timer);
	assert(heartbeatsWhilePending > 0, 'indexing completed without yielding to the event loop');
	await index.close({ mode: 'rollback' });
});

test('search completes while the writer is processing a large batch', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = {
		...config.limits,
		maxQueuedBytes: 16 * 1024 * 1024,
		maxBatchBytes: 16 * 1024 * 1024,
	};
	const index = await openNativeFullTextIndex(config);
	await index.apply(encodeMutationBatch({ upserts: [{ id: 'visible', fields: { title: 'visible trail shoe' } }] }));
	await index.commit();
	await index.reload();
	const packed = encodeMutationBatch(
		{
			upserts: Array.from({ length: 50_000 }, (_, id) => ({
				id: `pending-${id}`,
				fields: { title: `pending catalog product ${id}`, description: 'large concurrent batch' },
			})),
		},
		config.limits.maxBatchBytes,
	);
	let applySettled = false;
	const apply = index.apply(packed).finally(() => {
		applySettled = true;
	});
	const result = await index.search({ text: 'visible trail shoe', exactTotal: true });
	assert.strictEqual(result.hits[0].id, 'visible');
	assert.strictEqual(applySettled, false, 'search waited for the writer batch to finish');
	await apply;
	await index.close({ mode: 'rollback' });
});

test('rejects overload instead of blocking the JavaScript thread', async (context) => {
	const indexPath = temporaryIndex(context);
	const config = options(indexPath);
	config.limits = {
		...config.limits,
		maxQueuedCommands: 1,
		maxQueuedBytes: 4 * 1024 * 1024,
		maxBatchBytes: 4 * 1024 * 1024,
	};
	const index = await openNativeFullTextIndex(config);
	const packed = encodeMutationBatch(
		{
			upserts: Array.from({ length: 15_000 }, (_, id) => ({
				id: String(id),
				fields: { title: `queued running product ${id}` },
			})),
		},
		config.limits.maxBatchBytes,
	);
	const settled = await Promise.allSettled(Array.from({ length: 12 }, () => index.apply(packed)));
	assert(settled.some((result) => result.status === 'rejected' && result.reason.code === 'E_QUEUE_FULL'));
	await index.close({ mode: 'rollback' });
});
