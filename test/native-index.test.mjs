import assert from 'node:assert';
import { fork } from 'node:child_process';
import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	realpathSync,
	renameSync,
	rmSync,
	statSync,
	symlinkSync,
	unlinkSync,
	writeFileSync,
} from 'node:fs';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
	encodeMutationBatch,
	inspectNativeFullTextIndex,
	openNativeFullTextIndex,
	resetNativeFullTextIndex,
} from '@harperfast/fulltext/native';

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

test('inspects missing storage without creating it', (context) => {
	const parent = temporaryIndex(context);
	const indexPath = path.join(parent, 'missing');
	const { limits: _, ...inspectionOptions } = options(indexPath);
	assert.deepStrictEqual(inspectNativeFullTextIndex(inspectionOptions), { state: 'missing' });
	assert.strictEqual(existsSync(indexPath), false);
});

test('retires a closed index, preserves its checkpoint, and permits a clean rebuild', async (context) => {
	const parent = temporaryIndex(context);
	const indexPath = path.join(parent, 'products');
	const config = options(indexPath);
	let index = await openNativeFullTextIndex(config);
	await index.apply(encodeMutationBatch({ upserts: [{ id: 'shoe-1', fields: { title: 'Trail running shoe' } }] }));
	await index.publish('source-checkpoint-42');
	await index.close();

	const retired = await resetNativeFullTextIndex({ path: indexPath, indexId: config.indexId });
	assert.strictEqual(retired.state, 'reset');
	assert.strictEqual(path.dirname(retired.retiredPath), path.join(realpathSync(parent), '.fulltext-retired'));
	assert.strictEqual(existsSync(indexPath), false);
	assert.deepStrictEqual(inspectNativeFullTextIndex(config), { state: 'missing' });

	renameSync(retired.retiredPath, indexPath);
	index = await openNativeFullTextIndex(config);
	assert.strictEqual(index.committedPayload, 'source-checkpoint-42');
	assert.strictEqual((await index.search({ text: 'trail running', exactTotal: true })).hits[0].id, 'shoe-1');
	await index.close();
	await resetNativeFullTextIndex({ path: indexPath, indexId: config.indexId });

	index = await openNativeFullTextIndex(config);
	assert.strictEqual((await index.search({ text: 'trail running', exactTotal: true })).total, 0);
	await index.close();
});

test('close waits for background merges before retiring native files', async (context) => {
	const parent = temporaryIndex(context);
	const indexPath = path.join(parent, 'merging');
	const config = options(indexPath);
	const index = await openNativeFullTextIndex(config);
	for (let batch = 0; batch < 20; batch++) {
		await index.apply(
			encodeMutationBatch({
				upserts: Array.from({ length: 100 }, (_, offset) => ({
					id: `${batch}-${offset}`,
					fields: { title: `Trail running shoe ${batch}-${offset}`, description: 'merge lifecycle test' },
				})),
			}),
		);
		await index.publish(`source-checkpoint-${batch}`);
	}
	await index.close();
	const retired = await resetNativeFullTextIndex({ path: indexPath, indexId: config.indexId });
	assert.strictEqual(retired.state, 'reset');
	rmSync(retired.retiredPath, { recursive: true });
});

test('reset is idempotent for a missing path and does not create it', async (context) => {
	const indexPath = path.join(temporaryIndex(context), 'missing');
	assert.deepStrictEqual(await resetNativeFullTextIndex({ path: indexPath, indexId: 'products' }), {
		state: 'missing',
	});
	assert.strictEqual(existsSync(indexPath), false);
});

test('reset rejects a live index without changing its data', async (context) => {
	const indexPath = temporaryIndex(context);
	const config = options(indexPath);
	const index = await openNativeFullTextIndex(config);
	await index.apply(encodeMutationBatch({ upserts: [{ id: 'shoe-1', fields: { title: 'Trail running shoe' } }] }));
	await index.publish('source-checkpoint-42');
	await assert.rejects(
		resetNativeFullTextIndex({ path: path.join(indexPath, '.'), indexId: config.indexId }),
		(error) => error.code === 'E_LOCK_BUSY',
	);
	assert.strictEqual((await index.search({ text: 'trail running', exactTotal: true })).hits[0].id, 'shoe-1');
	await index.close();
});

test('reset respects Tantivy ownership in another process', async (context) => {
	const parent = temporaryIndex(context);
	const indexPath = path.join(parent, 'products');
	const child = fork(
		fileURLToPath(new URL('./fixtures/native-lock-child.mjs', import.meta.url)),
		[new URL('../dist/native.js', import.meta.url).href, indexPath],
		{ stdio: ['ignore', 'ignore', 'inherit', 'ipc'] },
	);
	context.after(() => child.kill());
	await childMessage(child, 'ready');

	await assert.rejects(
		resetNativeFullTextIndex({ path: indexPath, indexId: 'products' }),
		(error) => error.code === 'E_LOCK_BUSY',
	);
	const exited = new Promise((resolve, reject) => {
		child.once('error', reject);
		child.once('exit', resolve);
	});
	child.send('close');
	await childMessage(child, 'closed');
	assert.strictEqual(child.exitCode, null);
	assert.strictEqual((await resetNativeFullTextIndex({ path: indexPath, indexId: 'products' })).state, 'reset');
	child.send('exit');
	await exited;
});

test('reset protects neighboring identities and unrelated directories', async (context) => {
	const parent = temporaryIndex(context);
	const indexPath = path.join(parent, 'products');
	const config = options(indexPath);
	const index = await openNativeFullTextIndex(config);
	await index.close();
	const beforeMismatch = fileSnapshot(indexPath);
	await assert.rejects(
		resetNativeFullTextIndex({ path: indexPath, indexId: 'orders' }),
		(error) => error.code === 'E_IDENTITY_MISMATCH',
	);
	assert.deepStrictEqual(fileSnapshot(indexPath), beforeMismatch);

	const unrelatedPath = path.join(parent, 'unrelated');
	mkdirSync(unrelatedPath);
	writeFileSync(path.join(unrelatedPath, 'important.txt'), 'keep');
	await assert.rejects(
		resetNativeFullTextIndex({ path: unrelatedPath, indexId: 'products' }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.strictEqual(readFileSync(path.join(unrelatedPath, 'important.txt'), 'utf8'), 'keep');
});

test('a rejected retirement destination releases the reset reservation', async (context) => {
	const parent = temporaryIndex(context);
	const indexPath = path.join(parent, 'products');
	const config = options(indexPath);
	let index = await openNativeFullTextIndex(config);
	await index.close();
	const retiredRoot = path.join(parent, '.fulltext-retired');
	writeFileSync(retiredRoot, 'not a directory');
	await assert.rejects(
		resetNativeFullTextIndex({ path: indexPath, indexId: config.indexId }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	unlinkSync(retiredRoot);
	index = await openNativeFullTextIndex(config);
	await index.close();
});

test('reset accepts an empty index directory', async (context) => {
	const parent = temporaryIndex(context);
	const indexPath = path.join(parent, 'empty');
	mkdirSync(indexPath);
	const result = await resetNativeFullTextIndex({ path: indexPath, indexId: 'products' });
	assert.strictEqual(result.state, 'reset');
	assert.strictEqual(existsSync(indexPath), false);

	const lockOnlyPath = path.join(parent, 'interrupted-open');
	mkdirSync(lockOnlyPath);
	writeFileSync(path.join(lockOnlyPath, '.tantivy-writer.lock'), '');
	assert.strictEqual((await resetNativeFullTextIndex({ path: lockOnlyPath, indexId: 'products' })).state, 'reset');
});

test('reset does not follow a symbolic-link path', async (context) => {
	if (process.platform === 'win32') {
		context.skip('creating directory symbolic links requires host privileges on Windows');
		return;
	}
	const parent = temporaryIndex(context);
	const target = path.join(parent, 'target');
	const alias = path.join(parent, 'alias');
	mkdirSync(target);
	symlinkSync(target, alias, 'dir');
	await assert.rejects(
		resetNativeFullTextIndex({ path: alias, indexId: 'products' }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.strictEqual(existsSync(target), true);

	const indexPath = path.join(parent, 'products');
	const config = options(indexPath);
	const index = await openNativeFullTextIndex(config);
	await index.close();
	symlinkSync(target, path.join(parent, '.fulltext-retired'), 'dir');
	await assert.rejects(
		resetNativeFullTextIndex({ path: indexPath, indexId: config.indexId }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.strictEqual(existsSync(indexPath), true);
});

test('inspects committed payloads without taking the writer', async (context) => {
	const indexPath = temporaryIndex(context);
	const config = options(indexPath);
	const index = await openNativeFullTextIndex(config);
	assert.deepStrictEqual(inspectNativeFullTextIndex(config), { state: 'cursorless' });
	await index.publish('cursor-1');
	assert.deepStrictEqual(inspectNativeFullTextIndex(config), {
		state: 'checkpointed',
		committedPayload: 'cursor-1',
	});
	await index.close();
	assert.deepStrictEqual(inspectNativeFullTextIndex(options(indexPath, { generation: 'generation-2' })), {
		state: 'incompatible',
		code: 'E_IDENTITY_MISMATCH',
	});
});

test('inspection does not modify native files', async (context) => {
	const indexPath = temporaryIndex(context);
	const config = options(indexPath);
	const index = await openNativeFullTextIndex(config);
	await index.publish('stable');
	const before = fileSnapshot(indexPath);
	assert.deepStrictEqual(inspectNativeFullTextIndex(config), {
		state: 'checkpointed',
		committedPayload: 'stable',
	});
	assert.deepStrictEqual(fileSnapshot(indexPath), before);
	await index.close();
	const closed = fileSnapshot(indexPath);
	inspectNativeFullTextIndex(config);
	assert.deepStrictEqual(fileSnapshot(indexPath), closed);
});

test('reports incomplete native storage as incompatible', (context) => {
	const indexPath = temporaryIndex(context);
	writeFileSync(path.join(indexPath, 'meta.json'), '{}');
	assert.deepStrictEqual(inspectNativeFullTextIndex(options(indexPath)), {
		state: 'incompatible',
		code: 'E_INCOMPLETE_CREATE',
	});
});

test('reports corrupt metadata as incompatible', async (context) => {
	const indexPath = temporaryIndex(context);
	const config = options(indexPath);
	const index = await openNativeFullTextIndex(config);
	await index.publish('stable');
	await index.close();
	writeFileSync(path.join(indexPath, 'meta.json'), '{');
	assert.deepStrictEqual(inspectNativeFullTextIndex(config), {
		state: 'incompatible',
		code: 'E_INDEX_CORRUPT',
	});
	await assert.rejects(openNativeFullTextIndex(config), (error) => error.code === 'E_INDEX_CORRUPT');
});

test('reports persisted schema drift as incompatible', async (context) => {
	const indexPath = temporaryIndex(context);
	const config = options(indexPath);
	const index = await openNativeFullTextIndex(config);
	await index.close();
	const metaPath = path.join(indexPath, 'meta.json');
	const meta = JSON.parse(readFileSync(metaPath, 'utf8'));
	assert(Array.isArray(meta.schema));
	meta.schema[0].name = 'unexpected';
	writeFileSync(metaPath, JSON.stringify(meta));
	assert.deepStrictEqual(inspectNativeFullTextIndex(config), {
		state: 'incompatible',
		code: 'E_SCHEMA_MISMATCH',
	});
});

test('bounds persisted commit payloads during inspection and open', async (context) => {
	const indexPath = temporaryIndex(context);
	const config = options(indexPath);
	const index = await openNativeFullTextIndex(config);
	await index.publish('stable');
	await index.close();
	const metaPath = path.join(indexPath, 'meta.json');
	const meta = JSON.parse(readFileSync(metaPath, 'utf8'));
	meta.payload = 'x'.repeat(64 * 1024 + 1);
	writeFileSync(metaPath, JSON.stringify(meta));
	assert.deepStrictEqual(inspectNativeFullTextIndex(config), {
		state: 'incompatible',
		code: 'E_INDEX_CORRUPT',
	});
	await assert.rejects(openNativeFullTextIndex(config), (error) => error.code === 'E_INDEX_CORRUPT');
});

test('inspection remains consistent while checkpoints publish', async (context) => {
	const indexPath = temporaryIndex(context);
	const config = options(indexPath);
	const index = await openNativeFullTextIndex(config);
	let observed = -1;
	for (let checkpoint = 0; checkpoint < 10; checkpoint++) {
		await index.apply(
			encodeMutationBatch({ upserts: [{ id: `product-${checkpoint}`, fields: { title: `Product ${checkpoint}` } }] }),
		);
		const publication = index.publish(`cursor-${checkpoint}`);
		const during = inspectNativeFullTextIndex(config);
		if (during.state === 'checkpointed') {
			const value = Number(during.committedPayload.slice('cursor-'.length));
			assert(value >= observed && value <= checkpoint);
			observed = value;
		} else {
			assert.strictEqual(during.state, 'cursorless');
			assert.strictEqual(checkpoint, 0);
		}
		await publication;
		assert.deepStrictEqual(inspectNativeFullTextIndex(config), {
			state: 'checkpointed',
			committedPayload: `cursor-${checkpoint}`,
		});
		observed = checkpoint;
	}
	await index.close();
});

test('classifies an unreadable committed segment footer as rebuildable when opening', async (context) => {
	const indexPath = temporaryIndex(context);
	const config = options(indexPath);
	const index = await openNativeFullTextIndex(config);
	await index.apply(encodeMutationBatch({ upserts: [{ id: 'one', fields: { title: 'running shoe' } }] }));
	await index.publish('cursor-1');
	await index.close();

	const termPath = committedSegmentPath(indexPath, '.term');
	const bytes = readFileSync(termPath);
	bytes[bytes.length - 1] ^= 0xff;
	writeFileSync(termPath, bytes);
	assert.deepStrictEqual(inspectNativeFullTextIndex(config), {
		state: 'checkpointed',
		committedPayload: 'cursor-1',
	});
	await assert.rejects(openNativeFullTextIndex(config), (error) => error.code === 'E_INDEX_CORRUPT');
});

test('classifies a missing committed segment as rebuildable when opening', async (context) => {
	const indexPath = temporaryIndex(context);
	const config = options(indexPath);
	const index = await openNativeFullTextIndex(config);
	await index.apply(encodeMutationBatch({ upserts: [{ id: 'one', fields: { title: 'running shoe' } }] }));
	await index.publish('cursor-1');
	await index.close();

	unlinkSync(committedSegmentPath(indexPath, '.term'));
	assert.deepStrictEqual(inspectNativeFullTextIndex(config), {
		state: 'checkpointed',
		committedPayload: 'cursor-1',
	});
	await assert.rejects(openNativeFullTextIndex(config), (error) => error.code === 'E_INDEX_CORRUPT');
});

test('reports accepted mutation count for absent deletes', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	assert.strictEqual(await index.apply(encodeMutationBatch({ deletes: ['not-indexed'] })), 1);
	assert.strictEqual(index.status().uncommittedMutations, 1n);
	await index.close({ mode: 'rollback' });
});

test('distinguishes oversized mutation batches from invalid input', async (context) => {
	assert.throws(
		() => encodeMutationBatch({ upserts: [{ id: 'large', fields: { title: 'x'.repeat(128) } }] }, 64),
		(error) => error.code === 'E_BATCH_TOO_LARGE',
	);

	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 64 };
	const index = await openNativeFullTextIndex(config);
	const packed = encodeMutationBatch(
		{ upserts: [{ id: 'large', fields: { title: 'x'.repeat(128) } }] },
		config.limits.maxQueuedBytes,
	);
	await assert.rejects(index.apply(packed), (error) => error.code === 'E_BATCH_TOO_LARGE');
	assert.strictEqual(index.status().uncommittedMutations, 0n);
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
	assert(heartbeatsWhilePending > 5, `indexing allowed only ${heartbeatsWhilePending} event-loop heartbeats`);
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

function fileSnapshot(directory, prefix = '') {
	const snapshot = [];
	for (const name of readdirSync(directory).sort()) {
		const entryPath = path.join(directory, name);
		const stat = statSync(entryPath);
		const relativePath = path.join(prefix, name);
		if (stat.isDirectory()) {
			snapshot.push({ name: relativePath, type: 'directory', mtimeMs: stat.mtimeMs });
			snapshot.push(...fileSnapshot(entryPath, relativePath));
		} else {
			let bytes;
			try {
				bytes = readFileSync(entryPath).toString('base64');
			} catch (error) {
				if (error.code !== 'EBUSY') throw error;
			}
			snapshot.push({
				name: relativePath,
				type: 'file',
				size: stat.size,
				mtimeMs: stat.mtimeMs,
				bytes,
			});
		}
	}
	return snapshot;
}

function childMessage(child, expected) {
	return new Promise((resolve, reject) => {
		const onMessage = (message) => {
			if (message !== expected) return;
			child.off('message', onMessage);
			child.off('error', reject);
			resolve();
		};
		child.once('error', reject);
		child.on('message', onMessage);
	});
}

function committedSegmentPath(directory, extension) {
	const name = readdirSync(directory).find((entry) => entry.endsWith(extension));
	assert(name, `expected a committed ${extension} segment`);
	return path.join(directory, name);
}
