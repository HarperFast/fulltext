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
	reclaimRetiredNativeFullTextIndexes,
	resetNativeFullTextIndex,
	validateNativeFullTextIndexOptions,
} from '@harperfast/fulltext/native';
import { loadAddon } from '../dist/load-addon.js';

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

test('runs every structured query mode and score-neutral candidate filtering', async (context) => {
	const index = await openNativeFullTextIndex(
		options(temporaryIndex(context), { positions: true, surfaceTerms: true }),
	);
	await index.applyMutationBatch({
		upserts: [
			{ id: 'one', fields: { title: 'Waterproof Trail Running Shoes' } },
			{ id: 'two', fields: { title: 'Waterproof Road Shoes' } },
			{ id: 'three', fields: { title: 'Wireless Headphones' } },
		],
	});
	await index.commit();
	await index.reload();

	assert.deepStrictEqual(
		(await index.search({ text: 'trail running', mode: 'phrase' })).hits.map((hit) => hit.id),
		['one'],
	);
	assert.deepStrictEqual(
		(await index.search({ text: 'waterproof trai', mode: 'prefix' })).hits.map((hit) => hit.id),
		['one'],
	);
	assert.deepStrictEqual(
		(await index.search({ text: 'waterprof', mode: 'fuzzy' })).hits.map((hit) => hit.id),
		['one', 'two'],
	);
	assert.deepStrictEqual(
		(await index.search({ text: 'waterproof tral', mode: 'fuzzy-prefix' })).hits.map((hit) => hit.id),
		['one'],
	);
	const unfilteredScore = (await index.search({ text: 'waterproof' })).hits.find((hit) => hit.id === 'two').score;
	const filtered = await index.search({ text: 'waterproof', candidateIds: ['two'] });
	assert.deepStrictEqual(
		filtered.hits.map((hit) => hit.id),
		['two'],
	);
	assert.strictEqual(filtered.hits[0].score, unfilteredScore);
	assert.deepStrictEqual(await index.search({ text: 'waterproof', candidateIds: [] }), {
		total: 0,
		totalRelation: 'exact',
		hits: [],
	});
	const traced = await index.traceMatches(
		{ text: 'trail running', mode: 'phrase' },
		[
			{ id: 'one', fields: { title: 'The Trail Running Shoes' } },
			{ id: 'two', fields: { title: 'Waterproof Road Shoes' } },
		],
		{ snippets: true, fragmentLength: 32 },
	);
	assert.deepStrictEqual(traced, {
		complete: true,
		records: [
			{
				id: 'one',
				values: [
					{
						field: 'title',
						valueIndex: 0,
						spans: [{ start: 4, end: 17 }],
						fragments: [
							{
								text: 'The Trail Running Shoes',
								start: 0,
								spans: [{ start: 4, end: 17 }],
							},
						],
					},
				],
			},
		],
	});
	const unicodeTrace = await index.traceMatches({ text: 'waterproof', mode: 'any' }, [
		{ id: 'one', fields: { title: '🥾 Waterproof Trail Shoes' } },
	]);
	assert.deepStrictEqual(unicodeTrace.records[0].values[0].spans, [{ start: 3, end: 13 }]);
	const boundedTrace = await index.traceMatches({ text: 'shoe' }, [
		{ id: 'one', fields: { title: 'shoe '.repeat(1_100) } },
	]);
	assert.strictEqual(boundedTrace.complete, false);
	assert.strictEqual(boundedTrace.records[0].values[0].spans.length, 1_024);
	const overlappingPrefixTrace = await index.traceMatches({ text: 'shoe sho', mode: 'prefix' }, [
		{ id: 'one', fields: { title: 'shoe '.repeat(513) } },
	]);
	assert.strictEqual(overlappingPrefixTrace.complete, true);
	assert.strictEqual(overlappingPrefixTrace.records[0].values[0].spans.length, 513);
	const exactEmptyTrace = await index.traceMatches({ text: 'shoe missing', mode: 'all' }, [
		{ id: 'one', fields: { title: 'shoe '.repeat(1_100) } },
	]);
	assert.deepStrictEqual(exactEmptyTrace, { complete: true, records: [] });
	await assert.rejects(
		index.search({ text: 'waterproof', mode: 'any', operator: 'all' }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(
		index.search({ text: 'x'.repeat(40), mode: 'prefix' }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.deepStrictEqual(
		(await index.search({ text: `${'x'.repeat(40)} waterproof`, mode: 'prefix' })).hits.map((hit) => hit.id).sort(),
		['one', 'two'],
	);
	await assert.rejects(index.search({ text: '\ud800' }), (error) => error.code === 'E_INVALID_ARGUMENT');
	await assert.rejects(
		index.search({ text: 'waterproof', limit: 10_001 }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(
		index.search({ text: 'waterproof', candidateIds: Array.from({ length: 1_025 }, (_, id) => `${id}`) }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(
		index.search({ text: 'waterproof', candidateIds: 'one' }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(
		index.search({ text: 'waterproof', candidateIds: ['x'.repeat(4_097)] }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(
		index.traceMatches({ text: 'waterproof' }, [{ id: 'x'.repeat(4_097), fields: { title: 'waterproof' } }]),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(
		index.apply(encodeMutationBatch({ deletes: ['x'.repeat(4_097)] })),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(
		index.search({ text: 'waterproof' }, { remainingBudgetMilliseconds: 0 }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.strictEqual((await index.search({ text: 'waterproof' }, { remainingBudgetMilliseconds: 60_000 })).total, 2);
	assert.strictEqual((await index.search({ text: 'waterproof' }, { remainingBudgetMilliseconds: 0.5 })).total, 2);
	await index.close();
});

test('uses the surface field for stop-word prefixes and preserves weighted fuzzy-prefix ranking', async (context) => {
	const index = await openNativeFullTextIndex(
		options(temporaryIndex(context), { positions: true, surfaceTerms: true }),
	);
	await index.applyMutationBatch({
		upserts: [
			{ id: 'title', fields: { title: 'Waterproof Therefore Catalogapple' } },
			{ id: 'description', fields: { description: 'Waterproof Thermal Catalogapple' } },
		],
	});
	await index.commit();
	await index.reload();
	assert.deepStrictEqual(
		(await index.search({ text: 'there', mode: 'prefix' })).hits.map((hit) => hit.id),
		['title'],
	);
	assert.deepStrictEqual(
		(await index.search({ text: 'waterproof the', mode: 'prefix' })).hits.map((hit) => hit.id),
		['title', 'description'],
	);
	assert.deepStrictEqual(
		(await index.search({ text: 'catalogapp', mode: 'fuzzy-prefix' })).hits.map((hit) => hit.id),
		['title', 'description'],
	);
	await index.close();
});

test('preserves phrase positions through stop words in search and tracing', async (context) => {
	const index = await openNativeFullTextIndex(
		options(temporaryIndex(context), { positions: true, surfaceTerms: true }),
	);
	await index.applyMutationBatch({
		upserts: [
			{ id: 'adjacent', fields: { title: 'trail running' } },
			{ id: 'gap', fields: { title: 'trail the running' } },
			{ id: 'substitute', fields: { title: 'trail blazing running' } },
		],
	});
	await index.commit();
	await index.reload();
	assert.deepStrictEqual(
		(await index.search({ text: 'trail running', mode: 'phrase' })).hits.map((hit) => hit.id),
		['adjacent'],
	);
	assert.deepStrictEqual(
		(await index.search({ text: 'trail the running', mode: 'phrase' })).hits.map((hit) => hit.id),
		['gap', 'substitute'],
	);
	const trace = await index.traceMatches({ text: 'trail the running', mode: 'phrase' }, [
		{ id: 'adjacent', fields: { title: 'trail running' } },
		{ id: 'gap', fields: { title: 'trail the running' } },
		{ id: 'substitute', fields: { title: 'trail blazing running' } },
	]);
	assert.deepStrictEqual(
		trace.records.map((record) => record.id),
		['gap', 'substitute'],
	);
	await index.close();
});

test('field weights can change without rebuilding native storage', async (context) => {
	const indexPath = temporaryIndex(context);
	let index = await openNativeFullTextIndex(options(indexPath));
	await index.applyMutationBatch({ upserts: [{ id: 'one', fields: { title: 'shoe', description: 'shoe' } }] });
	await index.publish('checkpoint-1');
	await index.close();

	const weighted = options(indexPath, {
		fields: [
			{ name: 'title', weight: 8 },
			{ name: 'description', weight: 0.5 },
		],
	});
	assert.deepStrictEqual(inspectNativeFullTextIndex(weighted), {
		state: 'checkpointed',
		committedPayload: 'checkpoint-1',
	});
	index = await openNativeFullTextIndex(weighted);
	assert.strictEqual((await index.search({ text: 'shoe' })).hits[0].id, 'one');
	await index.close();
});

test('times out bounded trace work inside the native execution lane', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context), { surfaceTerms: true }));
	await assert.rejects(
		index.traceMatches(
			{ text: 'waterprof', mode: 'fuzzy' },
			[{ id: 'one', fields: { title: 'waterproof '.repeat(80_000) } }],
			{ remainingBudgetMilliseconds: 1 },
		),
		(error) => error.code === 'E_TIMEOUT',
	);
	await index.close();
});

test('snapshots trace source arrays before asynchronous native execution', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context), { surfaceTerms: true }));
	const values = ['waterproof shoe'];
	const trace = index.traceMatches({ text: 'waterproof' }, [{ id: 'one', fields: { title: values } }], {
		snippets: true,
	});
	values[0] = 'changed';
	assert.strictEqual((await trace).records[0].values[0].fragments[0].text, 'waterproof shoe');
	await index.close();
});

test('rejects query modes when their schema capability is disabled', async (context) => {
	const index = await openNativeFullTextIndex(
		options(temporaryIndex(context), { positions: false, surfaceTerms: false }),
	);
	await index.applyMutationBatch({ upserts: [{ id: 'one', fields: { title: 'Waterproof shoe' } }] });
	await index.commit();
	await index.reload();
	assert.deepStrictEqual(
		(await index.search({ text: 'waterprof', mode: 'fuzzy' })).hits.map((hit) => hit.id),
		['one'],
	);
	await assert.rejects(
		index.search({ text: 'trail running', mode: 'phrase' }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(index.search({ text: 'trai', mode: 'prefix' }), (error) => error.code === 'E_INVALID_ARGUMENT');
	await assert.rejects(
		index.traceMatches({ text: 'trail' }, [{ id: 'one', fields: { title: 'trail' } }]),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await index.close();
});

test('inspects missing storage without creating it', (context) => {
	const parent = temporaryIndex(context);
	const indexPath = path.join(parent, 'missing');
	const { limits: _, ...inspectionOptions } = options(indexPath);
	assert.deepStrictEqual(inspectNativeFullTextIndex(inspectionOptions), { state: 'missing' });
	assert.strictEqual(existsSync(indexPath), false);
});

test('rejects a mutation frame limit that cannot hold one mutation', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 20 };
	await assert.rejects(openNativeFullTextIndex(config), (error) => error.code === 'E_INVALID_ARGUMENT');
});

test('validates native configuration without creating index storage', async (context) => {
	const indexPath = path.join(temporaryIndex(context), 'invalid');
	const config = options(indexPath);
	config.limits = { ...config.limits, writerMemoryBytes: 14_999_999 };
	assert.throws(
		() => validateNativeFullTextIndexOptions(config),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.throws(
		() => validateNativeFullTextIndexOptions({ ...config, fields: undefined }),
		(error) => error.name === 'FulltextError' && error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(
		openNativeFullTextIndex({ ...config, fields: undefined }),
		(error) => error.name === 'FulltextError' && error.code === 'E_INVALID_ARGUMENT',
	);
	assert.strictEqual(existsSync(indexPath), false);
});

test('reclaims only retired trees generated for the requested index', async (context) => {
	const parent = temporaryIndex(context);
	const productsPath = path.join(parent, 'products');
	const ordersPath = path.join(parent, 'orders');
	for (const [indexPath, indexId] of [
		[productsPath, 'products'],
		[ordersPath, 'orders'],
	]) {
		const index = await openNativeFullTextIndex(options(indexPath, { indexId }));
		await index.close();
	}
	const products = await resetNativeFullTextIndex({ path: productsPath, indexId: 'products' });
	const orders = await resetNativeFullTextIndex({ path: ordersPath, indexId: 'orders' });
	assert.strictEqual(products.state, 'reset');
	assert.strictEqual(orders.state, 'reset');
	const unrelated = path.join(parent, '.fulltext-retired', 'keep');
	mkdirSync(unrelated);

	assert.deepStrictEqual(
		await reclaimRetiredNativeFullTextIndexes({ path: productsPath, retiredPath: products.retiredPath }),
		{
			removed: 1,
			failed: 0,
		},
	);
	assert.strictEqual(existsSync(products.retiredPath), false);
	assert.strictEqual(existsSync(orders.retiredPath), true);
	assert.strictEqual(existsSync(unrelated), true);
	assert.deepStrictEqual(await reclaimRetiredNativeFullTextIndexes({ path: ordersPath }), {
		removed: 1,
		failed: 0,
	});
});

test('refuses a symbolic-link retirement root', async (context) => {
	const parent = temporaryIndex(context);
	const target = path.join(parent, 'target');
	const marker = path.join(target, 'must-remain');
	mkdirSync(target);
	writeFileSync(marker, 'retained');
	symlinkSync(target, path.join(parent, '.fulltext-retired'), process.platform === 'win32' ? 'junction' : 'dir');
	await assert.rejects(
		reclaimRetiredNativeFullTextIndexes({ path: path.join(parent, 'products') }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.strictEqual(readFileSync(marker, 'utf8'), 'retained');
});

test('removes a generated-name symbolic link without following it', async (context) => {
	const parent = temporaryIndex(context);
	const target = path.join(parent, 'target');
	const marker = path.join(target, 'must-remain');
	const retiredRoot = path.join(parent, '.fulltext-retired');
	const retiredLink = path.join(retiredRoot, 'products.1.2.3.4');
	mkdirSync(target);
	mkdirSync(retiredRoot);
	writeFileSync(marker, 'retained');
	symlinkSync(target, retiredLink, process.platform === 'win32' ? 'junction' : 'dir');
	assert.deepStrictEqual(await reclaimRetiredNativeFullTextIndexes({ path: path.join(parent, 'products') }), {
		removed: 1,
		failed: 0,
	});
	assert.strictEqual(existsSync(retiredLink), false);
	assert.strictEqual(readFileSync(marker, 'utf8'), 'retained');
});

test('rejects a reset result belonging to another index', async (context) => {
	const parent = temporaryIndex(context);
	const retiredRoot = path.join(parent, '.fulltext-retired');
	const retiredPath = path.join(retiredRoot, 'orders.1.2.3.4');
	mkdirSync(retiredPath, { recursive: true });
	await assert.rejects(
		reclaimRetiredNativeFullTextIndexes({
			path: path.join(parent, 'products'),
			retiredPath,
		}),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.strictEqual(existsSync(retiredPath), true);
});

test('validates a retirement hint even when the retirement root is absent', async (context) => {
	const parent = temporaryIndex(context);
	await assert.rejects(
		reclaimRetiredNativeFullTextIndexes({
			path: path.join(parent, 'products'),
			retiredPath: path.join(parent, '.fulltext-retired', 'orders.1.2.3.4'),
		}),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
});

test('does not broaden retirement cleanup through a symbolic-link index path', async (context) => {
	if (process.platform === 'win32') {
		context.skip('creating directory symbolic links requires host privileges on Windows');
		return;
	}
	const parent = temporaryIndex(context);
	const ordersPath = path.join(parent, 'orders');
	const productsPath = path.join(parent, 'products');
	const retiredPath = path.join(parent, '.fulltext-retired', 'orders.1.2.3.4');
	mkdirSync(ordersPath);
	mkdirSync(retiredPath, { recursive: true });
	symlinkSync(ordersPath, productsPath, 'dir');
	assert.deepStrictEqual(await reclaimRetiredNativeFullTextIndexes({ path: productsPath }), {
		removed: 0,
		failed: 0,
	});
	assert.strictEqual(existsSync(retiredPath), true);
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
	assert.strictEqual(
		realpathSync.native(path.dirname(retired.retiredPath)),
		realpathSync.native(path.join(parent, '.fulltext-retired')),
	);
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

test('serializes concurrent resets of one native path', async (context) => {
	const parent = temporaryIndex(context);
	const indexPath = path.join(parent, 'products');
	const config = options(indexPath);
	const index = await openNativeFullTextIndex(config);
	await index.close();
	const results = await Promise.allSettled([
		resetNativeFullTextIndex({ path: indexPath, indexId: config.indexId }),
		resetNativeFullTextIndex({ path: indexPath, indexId: config.indexId }),
	]);
	assert.strictEqual(
		results.filter((result) => result.status === 'fulfilled' && result.value.state === 'reset').length,
		1,
	);
	assert(
		results.some(
			(result) =>
				(result.status === 'fulfilled' && result.value.state === 'missing') ||
				(result.status === 'rejected' && result.reason.code === 'E_LOCK_BUSY'),
		),
	);
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
	writeFileSync(path.join(unrelatedPath, 'meta.json'), '{}');
	writeFileSync(path.join(unrelatedPath, 'important.txt'), 'keep');
	await assert.rejects(
		resetNativeFullTextIndex({ path: unrelatedPath, indexId: 'products' }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.strictEqual(readFileSync(path.join(unrelatedPath, 'important.txt'), 'utf8'), 'keep');

	writeFileSync(path.join(indexPath, '.harper-fulltext-identity'), 'invalid');
	await assert.rejects(
		resetNativeFullTextIndex({ path: indexPath, indexId: config.indexId }),
		(error) => error.code === 'E_INDEX_CORRUPT',
	);
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
	writeFileSync(path.join(lockOnlyPath, '.tantivy-meta.lock'), '');
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
	const lifecycleRoot = path.join(parent, '.fulltext-locks');
	rmSync(lifecycleRoot, { recursive: true });
	symlinkSync(target, lifecycleRoot, 'dir');
	await assert.rejects(
		resetNativeFullTextIndex({ path: indexPath, indexId: config.indexId }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	unlinkSync(lifecycleRoot);
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

test('partitions a Harper maximum-key delete workload into admissible native frames', async (context) => {
	const config = options(temporaryIndex(context));
	let index = await openNativeFullTextIndex(config);
	const id = `1.${Buffer.alloc(1978, 1).toString('base64url')}`;
	const deletes = Array.from({ length: 4096 }, (_, position) => `${position}.${id}`);
	const encoded = index.encodeMutationBatches({ deletes });
	assert.strictEqual(encoded.rejected.length, 0);
	assert(encoded.batches.length > 1);
	assert(encoded.batches.every((batch) => batch.bytes.byteLength <= config.limits.maxBatchBytes));
	let applied = 0;
	for (const batch of encoded.batches) {
		assert.strictEqual(await index.apply(batch.bytes), batch.mutationCount);
		applied += batch.mutationCount;
	}
	assert.strictEqual(applied, deletes.length);
	await index.publish('delete-boundary');
	await index.close();
	index = await openNativeFullTextIndex(config);
	assert.strictEqual(index.committedPayload, 'delete-boundary');
	await index.close();
});

test('preflights the exact replacement-delete frame boundary', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 256 };
	const index = await openNativeFullTextIndex(config);
	const result = await index.applyMutationBatch(
		{ upserts: [{ id: 'a'.repeat(238), fields: { title: 'x'.repeat(512) } }] },
		{ rejectedUpsert: 'delete' },
	);
	assert.deepStrictEqual(result.rejected, [{ operation: 'upsert', index: 0, code: 'E_BATCH_TOO_LARGE' }]);
	await index.publish('replacement-delete-boundary');
	await assert.rejects(
		index.applyMutationBatch(
			{ upserts: [{ id: 'a'.repeat(239), fields: { title: 'x'.repeat(512) } }] },
			{ rejectedUpsert: 'delete' },
		),
		(error) => error.code === 'E_BATCH_TOO_LARGE',
	);
	assert.strictEqual(index.status().uncommittedMutations, 0n);
	await index.close();
});

test('applies one frame containing both upserts and deletes', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	const encoded = index.encodeMutationBatches({
		upserts: [{ id: 'shoe-1', fields: { title: 'mixed frame trail shoe' } }],
		deletes: ['absent'],
	});
	assert.strictEqual(encoded.rejected.length, 0);
	assert.strictEqual(encoded.batches.length, 1);
	assert.strictEqual(encoded.batches[0].mutationCount, 2);
	assert.strictEqual(await index.apply(encoded.batches[0].bytes), 2);
	await index.publish('mixed-frame');
	assert.strictEqual((await index.search({ text: 'mixed frame', exactTotal: true })).hits[0].id, 'shoe-1');
	await index.close();
});

test('makes every upsert searchable after applying a multi-frame logical batch', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 512 };
	const index = await openNativeFullTextIndex(config);
	const upserts = Array.from({ length: 20 }, (_, id) => ({
		id: `product-${id}`,
		fields: { title: `partitioned catalog item ${id}`, description: 'shared searchable phrase' },
	}));
	const encoded = index.encodeMutationBatches({ upserts });
	assert.strictEqual(encoded.rejected.length, 0);
	assert(encoded.batches.length > 1);
	for (const batch of encoded.batches) assert.strictEqual(await index.apply(batch.bytes), batch.mutationCount);
	await index.publish('multi-frame-upserts');
	const result = await index.search({ text: 'shared searchable phrase', exactTotal: true, limit: upserts.length });
	assert.strictEqual(result.total, upserts.length);
	assert.strictEqual(result.hits.length, upserts.length);
	await index.close();
});

test('applies a logical mutation batch across native frames without rereading records', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 512 };
	const index = await openNativeFullTextIndex(config);
	const reads = Array.from({ length: 20 }, () => 0);
	const upserts = reads.map((_, id) => {
		const fields = new Proxy(
			{ title: `stateful catalog item ${id} ${'x'.repeat(100)}` },
			{
				get(target, property, receiver) {
					if (property === 'title') reads[id]++;
					return Reflect.get(target, property, receiver);
				},
			},
		);
		return { id: `stateful-${id}`, fields };
	});
	const result = await index.applyMutationBatch({ upserts });
	assert.strictEqual(result.processed, upserts.length);
	assert.deepStrictEqual(result.rejected, []);
	assert(result.frames > 1);
	assert(result.encodedBytes > 0);
	assert.deepStrictEqual(
		reads,
		Array.from({ length: upserts.length }, () => 1),
	);
	await index.publish('logical-multi-frame');
	assert.strictEqual((await index.search({ text: 'stateful catalog', exactTotal: true, limit: 20 })).total, 20);
	await index.close();
});

test('applies a dense delete-only logical batch through bounded frames', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 1024 };
	const index = await openNativeFullTextIndex(config);
	const deletes = Array.from({ length: 5_000 }, (_, id) => `absent-${id}`);
	const result = await index.applyMutationBatch({ deletes });
	assert.strictEqual(result.processed, deletes.length);
	assert.deepStrictEqual(result.rejected, []);
	assert(result.frames > 1);
	await index.close({ mode: 'rollback' });
});

test('deletes stale content when a replacement record is unindexable', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 1024 };
	const index = await openNativeFullTextIndex(config);
	await index.applyMutationBatch({ upserts: [{ id: 'product', fields: { title: 'stale searchable catalog' } }] });
	await index.publish('before-rejection');
	const result = await index.applyMutationBatch(
		{ upserts: [{ id: 'product', fields: { title: 'x'.repeat(2048) } }] },
		{ rejectedUpsert: 'delete' },
	);
	assert.deepStrictEqual(result.rejected, [{ operation: 'upsert', index: 0, code: 'E_BATCH_TOO_LARGE' }]);
	assert.strictEqual(result.processed, 1);
	assert.strictEqual(result.frames, 1);
	await index.publish('after-rejection');
	assert.strictEqual((await index.search({ text: 'stale searchable catalog', exactTotal: true })).total, 0);
	await index.close();
});

test('deletes stale content for an invalid replacement when requested', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	await index.applyMutationBatch({ upserts: [{ id: 'product', fields: { title: 'staleinvalidunique' } }] });
	await index.publish('before-invalid-replacement');
	const result = await index.applyMutationBatch(
		{ upserts: [{ id: 'product', fields: { title: 42 } }] },
		{ rejectedUpsert: 'delete' },
	);
	assert.deepStrictEqual(result.rejected, [{ operation: 'upsert', index: 0, code: 'E_INVALID_ARGUMENT' }]);
	await index.publish('after-invalid-replacement');
	assert.strictEqual((await index.search({ text: 'staleinvalidunique', exactTotal: true })).total, 0);
	await index.close();
});

test('uses the snapshotted upsert ID for a replacement delete', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 256 };
	const index = await openNativeFullTextIndex(config);
	await index.applyMutationBatch({
		upserts: [
			{ id: 'rejected', fields: { title: 'staleunique' } },
			{ id: 'victim', fields: { title: 'victimunique' } },
		],
	});
	await index.publish('before-snapshot-rejection');
	let rejectedId = 'rejected';
	let rejectedIdReads = 0;
	const rejected = {
		get id() {
			rejectedIdReads++;
			return rejectedId;
		},
		fields: { title: 'x'.repeat(512) },
	};
	const logical = {
		upserts: [{ id: 'valid', fields: { title: `valid partitioned product ${'x'.repeat(140)}` } }, rejected],
	};
	const applying = index.applyMutationBatch(logical, { rejectedUpsert: 'delete' });
	rejectedId = 'victim';
	logical.upserts[1] = { id: 'victim', fields: { title: 'caller replacement must not be observed' } };
	const result = await applying;
	assert.strictEqual(rejectedIdReads, 1);
	assert.deepStrictEqual(result.rejected, [{ operation: 'upsert', index: 1, code: 'E_BATCH_TOO_LARGE' }]);
	await index.publish('after-snapshot-rejection');
	assert.strictEqual((await index.search({ text: 'staleunique', exactTotal: true })).total, 0);
	assert.strictEqual((await index.search({ text: 'victimunique', exactTotal: true })).hits[0].id, 'victim');
	await index.close();
});

test('latches a partially applied logical batch until rollback close', async (context) => {
	const indexPath = temporaryIndex(context);
	const config = options(indexPath);
	config.limits = { ...config.limits, maxBatchBytes: 256 };
	let index = await openNativeFullTextIndex(config);
	await index.applyMutationBatch({ upserts: [{ id: 'stable', fields: { title: 'stable published catalog' } }] });
	await index.publish('stable-checkpoint');
	await assert.rejects(
		index.applyMutationBatch({
			upserts: [
				{ id: 'pending-one', fields: { title: `pending one ${'x'.repeat(170)}` } },
				{ id: 'pending-two', fields: { title: `pending two ${'x'.repeat(170)}` } },
				{ id: 'oversized', fields: { title: 'x'.repeat(512) } },
			],
		}),
		(error) => error.code === 'E_BATCH_TOO_LARGE',
	);
	assert(index.status().uncommittedMutations > 0n);
	await assert.rejects(index.publish('must-not-publish'), (error) => error.code === 'E_BATCH_INCOMPLETE');
	await assert.rejects(index.commit(), (error) => error.code === 'E_BATCH_INCOMPLETE');
	await assert.rejects(index.reload(), (error) => error.code === 'E_BATCH_INCOMPLETE');
	await assert.rejects(
		index.apply(encodeMutationBatch({ deletes: ['stable'] })),
		(error) => error.code === 'E_BATCH_INCOMPLETE',
	);
	await assert.rejects(
		index.applyMutationBatch({ deletes: ['stable'] }),
		(error) => error.code === 'E_BATCH_INCOMPLETE',
	);
	await assert.rejects(index.close(), (error) => error.code === 'E_BATCH_INCOMPLETE');
	await index.close({ mode: 'rollback' });

	index = await openNativeFullTextIndex(config);
	assert.strictEqual(index.committedPayload, 'stable-checkpoint');
	assert.strictEqual((await index.search({ text: 'stable published catalog', exactTotal: true })).total, 1);
	assert.strictEqual((await index.search({ text: 'pending', exactTotal: true })).total, 0);
	await index.close();
});

test('keeps staged work usable when the first logical frame is not admitted', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	await index.applyMutationBatch({ upserts: [{ id: 'staged', fields: { title: 'stagedunique' } }] });
	const addon = loadAddon();
	const nativeApply = addon.__nativeApply;
	try {
		addon.__nativeApply = () => {
			const error = new Error('writer queue is full');
			error.code = 'E_QUEUE_FULL';
			throw error;
		};
		await assert.rejects(
			index.applyMutationBatch({ upserts: [{ id: 'rejected', fields: { title: 'not admitted' } }] }),
			(error) => error.code === 'E_QUEUE_FULL',
		);
	} finally {
		addon.__nativeApply = nativeApply;
	}
	await index.publish('after-admission-rejection');
	assert.strictEqual((await index.search({ text: 'stagedunique', exactTotal: true })).hits[0].id, 'staged');
	await index.close();
});

test('rejects low-level writer interleaving during logical apply', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 4 * 1024 * 1024, maxQueuedBytes: 4 * 1024 * 1024 };
	const index = await openNativeFullTextIndex(config);
	await index.applyMutationBatch({ upserts: [{ id: 'visible', fields: { title: 'visible stable product' } }] });
	await index.publish('visible-checkpoint');
	const logical = {
		upserts: Array.from({ length: 15_000 }, (_, id) => ({
			id: `logical-${id}`,
			fields: { title: `logical catalog product ${id}` },
		})),
	};
	const applying = index.applyMutationBatch(logical);
	logical.upserts.push({ id: 'late-mutation', fields: { title: 'must not enter the active batch' } });
	await assert.rejects(
		index.apply(encodeMutationBatch({ deletes: ['other'] })),
		(error) => error.code === 'E_BATCH_ACTIVE',
	);
	await assert.rejects(index.reload(), (error) => error.code === 'E_BATCH_ACTIVE');
	assert.strictEqual((await index.search({ text: 'visible stable product', exactTotal: true })).hits[0].id, 'visible');
	assert.strictEqual((await applying).processed, 15_000);
	await index.close({ mode: 'rollback' });
});

test('latches before reading caller-controlled mutation getters', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	let nested;
	const upsert = {
		get id() {
			nested = index.applyMutationBatch({ deletes: ['nested'] });
			nested.catch(() => undefined);
			return 'outer';
		},
		fields: { title: 'outer unique product' },
	};
	assert.strictEqual((await index.applyMutationBatch({ upserts: [upsert] })).processed, 1);
	await assert.rejects(nested, (error) => error.code === 'E_BATCH_ACTIVE');
	await index.publish('after-reentrant-getter');
	assert.strictEqual((await index.search({ text: 'outer unique product', exactTotal: true })).hits[0].id, 'outer');
	await index.close();
});

test('rechecks the logical latch after reading a caller-owned typed array', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 256 };
	const index = await openNativeFullTextIndex(config);
	let logical;
	class ReentrantBatch extends Uint8Array {
		get buffer() {
			logical = index.applyMutationBatch({
				upserts: [
					{ id: 'logical-one', fields: { title: `logical one ${'x'.repeat(170)}` } },
					{ id: 'logical-two', fields: { title: `logical two ${'x'.repeat(170)}` } },
				],
			});
			logical.catch(() => undefined);
			return super.buffer;
		}
	}
	const lowLevel = new ReentrantBatch(
		encodeMutationBatch({ upserts: [{ id: 'interleaved', fields: { title: 'must not be admitted' } }] }),
	);
	await assert.rejects(index.apply(lowLevel), (error) => error.code === 'E_BATCH_ACTIVE');
	assert.strictEqual((await logical).processed, 2);
	await index.publish('after-typed-array-reentrancy');
	assert.strictEqual((await index.search({ text: 'logical', exactTotal: true })).total, 2);
	assert.strictEqual((await index.search({ text: 'must not be admitted', exactTotal: true })).total, 0);
	await index.close();
});

test('batches replacement deletes for many rejected upserts', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 1_024 };
	const index = await openNativeFullTextIndex(config);
	const upserts = Array.from({ length: 1_000 }, (_, id) => ({ id: `invalid-${id}`, fields: { title: 42 } }));
	const result = await index.applyMutationBatch({ upserts }, { rejectedUpsert: 'delete' });
	assert.strictEqual(result.processed, upserts.length);
	assert.strictEqual(result.rejected.length, upserts.length);
	assert(result.frames < 100, `expected bounded replacement-delete frames, got ${result.frames}`);
	await index.close({ mode: 'rollback' });
});

test('rejects oversized strings before copying them into buffers', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	const oversized = 'x'.repeat((1 << 20) + 1);
	const originalFrom = Buffer.from;
	let copiedOversizedValue = false;
	Buffer.from = function (value, ...args) {
		if (value === oversized) copiedOversizedValue = true;
		return originalFrom.call(this, value, ...args);
	};
	try {
		const oversizedField = index.encodeMutationBatches({
			upserts: [{ id: 'oversized-field', fields: { title: oversized } }],
		});
		assert.deepStrictEqual(oversizedField.rejected, [{ operation: 'upsert', index: 0, code: 'E_INVALID_ARGUMENT' }]);
		const oversizedId = index.encodeMutationBatches({ upserts: [{ id: oversized, fields: { title: 'value' } }] });
		assert.deepStrictEqual(oversizedId.rejected, [{ operation: 'upsert', index: 0, code: 'E_INVALID_ARGUMENT' }]);
	} finally {
		Buffer.from = originalFrom;
	}
	assert.strictEqual(copiedOversizedValue, false);
	await index.close();
});

test('rejects duplicate logical IDs before latching the writer', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	await assert.rejects(
		index.applyMutationBatch({
			upserts: [{ id: 'same', fields: { title: 'duplicate' } }],
			deletes: ['same'],
		}),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await index.applyMutationBatch({ upserts: [{ id: 'valid', fields: { title: 'writer remains usable' } }] });
	await index.publish('after-duplicate');
	await index.close();
});

test('keeps the writer usable when logical validation fails before native admission', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	for (const batch of [null, { upserts: 5 }, { deletes: {} }]) {
		await assert.rejects(index.applyMutationBatch(batch), (error) => error.code === 'E_INVALID_ARGUMENT');
		assert.throws(
			() => index.encodeMutationBatches(batch),
			(error) => error.code === 'E_INVALID_ARGUMENT',
		);
	}
	for (const fields of [new Map([['title', 'not a record']]), new Date()]) {
		await assert.rejects(
			index.applyMutationBatch({ upserts: [{ id: 'invalid-fields', fields }] }),
			(error) => error.code === 'E_INVALID_ARGUMENT',
		);
		assert.deepStrictEqual(index.encodeMutationBatches({ upserts: [{ id: 'invalid-fields', fields }] }).rejected, [
			{ operation: 'upsert', index: 0, code: 'E_INVALID_ARGUMENT' },
		]);
	}
	let laterFieldsRead = false;
	const laterFields = new Proxy(
		{ title: 'must not be read' },
		{
			ownKeys(target) {
				laterFieldsRead = true;
				return Reflect.ownKeys(target);
			},
		},
	);
	await assert.rejects(
		index.applyMutationBatch({
			upserts: [
				{ id: 'first-invalid', fields: { title: 42 } },
				{ id: 'later', fields: laterFields },
			],
		}),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.strictEqual(laterFieldsRead, false);
	await assert.rejects(
		index.applyMutationBatch({ upserts: [{ id: 'invalid', fields: { title: 42 } }] }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(
		index.applyMutationBatch(
			{
				upserts: [
					{ id: 'must-not-stage', fields: { title: 'mustnotstageunique' } },
					{ id: '\ud800', fields: { title: 'invalid id' } },
				],
			},
			{ rejectedUpsert: 'delete', assumeDistinctIds: true },
		),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(
		index.applyMutationBatch({ upserts: [null] }, { rejectedUpsert: 'delete' }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(
		index.applyMutationBatch({ upserts: [{ id: 'wrong-schema', fields: { missing: 'value' } }] }),
		(error) => error.code === 'E_SCHEMA_MISMATCH',
	);
	await index.applyMutationBatch({ upserts: [{ id: 'valid-after-errors', fields: { title: 'still usable' } }] });
	await index.publish('after-validation-errors');
	assert.strictEqual((await index.search({ text: 'mustnotstageunique', exactTotal: true })).total, 0);
	await index.close();
});

test('reports only explicit record validation failures during partitioning', async (context) => {
	const config = options(temporaryIndex(context));
	const index = await openNativeFullTextIndex(config);
	const boundary = 'x'.repeat(1 << 20);
	const encoded = index.encodeMutationBatches({
		upserts: [
			{ id: 'valid-boundary', fields: { title: boundary } },
			{ id: 'too-long', fields: { title: `${boundary}x` } },
			{ id: 'whole-record', fields: { title: boundary, description: boundary } },
			{ id: '\ud800', fields: { title: 'invalid id' } },
		],
	});
	assert.deepStrictEqual(encoded.rejected, [
		{ operation: 'upsert', index: 1, code: 'E_INVALID_ARGUMENT' },
		{ operation: 'upsert', index: 3, code: 'E_INVALID_ARGUMENT' },
	]);
	assert.strictEqual(
		encoded.batches.reduce((count, batch) => count + batch.mutationCount, 0),
		2,
	);
	for (const batch of encoded.batches) await index.apply(batch.bytes);
	await index.close({ mode: 'rollback' });
});

test('fails the logical call when a mutation field is outside the opened schema', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 256 };
	const index = await openNativeFullTextIndex(config);
	assert.throws(
		() => index.encodeMutationBatches({ upserts: [{ id: 'unknown', fields: { missing: 'value' } }] }),
		(error) => error.code === 'E_SCHEMA_MISMATCH',
	);
	assert.throws(
		() =>
			index.encodeMutationBatches({
				upserts: [{ id: 'oversized-and-unknown', fields: { title: 'x'.repeat(512), missing: 'value' } }],
			}),
		(error) => error.code === 'E_SCHEMA_MISMATCH',
	);
	await index.close();
});

test('returns a consumed prefix when total output reaches the caller ceiling', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 256 };
	const index = await openNativeFullTextIndex(config);
	const upserts = [
		{ id: 'one', fields: { title: 'catalog '.repeat(20) } },
		{ id: 'two', fields: { title: 'catalog '.repeat(20) } },
	];
	assert.throws(
		() => index.encodeMutationBatches({ upserts }, { maxTotalBytes: 300 }),
		(error) => error.code === 'E_BATCH_TOO_LARGE',
	);
	const first = index.encodeMutationBatches({ upserts }, { maxTotalBytes: 300, allowPartial: true });
	assert.strictEqual(first.consumedUpserts, 1);
	assert.strictEqual(first.consumedDeletes, 0);
	assert.strictEqual(
		first.batches.reduce((count, batch) => count + batch.mutationCount, 0),
		1,
	);
	const second = index.encodeMutationBatches(
		{ upserts: upserts.slice(first.consumedUpserts) },
		{ maxTotalBytes: 300, allowPartial: true },
	);
	assert.strictEqual(second.consumedUpserts, 1);
	assert.strictEqual(second.consumedDeletes, 0);
	for (const batch of [...first.batches, ...second.batches]) await index.apply(batch.bytes);
	await index.publish('prefixes');
	assert.strictEqual((await index.search({ text: 'catalog', exactTotal: true })).total, 2);
	await index.close();
});

test('requires a boolean to opt into partial mutation results', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 256 };
	const index = await openNativeFullTextIndex(config);
	const batch = {
		upserts: [
			{ id: 'one', fields: { title: 'catalog '.repeat(20) } },
			{ id: 'two', fields: { title: 'catalog '.repeat(20) } },
		],
	};
	assert.throws(
		() => index.encodeMutationBatches(batch, { maxTotalBytes: 300, allowPartial: 'false' }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.throws(
		() => index.encodeMutationBatches(batch, { maxTotalBytes: 300, allowPartial: 1 }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await index.close();
});

test('snapshots allowPartial before encoding a batch', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 256 };
	const index = await openNativeFullTextIndex(config);
	const batch = {
		upserts: [
			{ id: 'one', fields: { title: 'catalog '.repeat(20) } },
			{ id: 'two', fields: { title: 'catalog '.repeat(20) } },
		],
	};
	let reads = 0;
	assert.throws(
		() =>
			index.encodeMutationBatches(batch, {
				maxTotalBytes: 300,
				get allowPartial() {
					reads++;
					return reads > 1;
				},
			}),
		(error) => error.code === 'E_BATCH_TOO_LARGE',
	);
	assert.strictEqual(reads, 1);
	await index.close();
});

test('keeps a caller total ceiling below the configured frame ceiling', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 1024 };
	const index = await openNativeFullTextIndex(config);
	const one = index.encodeMutationBatches(
		{ upserts: [{ id: 'one', fields: { title: 'x'.repeat(128) } }] },
		{ maxTotalBytes: 256, allowPartial: true },
	);
	assert.strictEqual(one.rejected.length, 0);
	assert.strictEqual(one.consumedUpserts, 1);
	assert.strictEqual(one.consumedDeletes, 0);
	assert(one.batches.every((batch) => batch.bytes.byteLength <= 256));
	const two = index.encodeMutationBatches(
		{
			upserts: [
				{ id: 'one', fields: { title: 'x'.repeat(128) } },
				{ id: 'two', fields: { title: 'x'.repeat(128) } },
			],
		},
		{ maxTotalBytes: 256, allowPartial: true },
	);
	assert.strictEqual(two.consumedUpserts, 1);
	assert.strictEqual(two.consumedDeletes, 0);
	await index.close();
});

test('encodes a dense frame of small records without per-record frame chunks', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	const deletes = Array.from({ length: 100_000 }, (_, id) => `deleted-${id}`);
	const encoded = index.encodeMutationBatches({ deletes });
	assert.strictEqual(encoded.rejected.length, 0);
	assert.strictEqual(
		encoded.batches.reduce((count, batch) => count + batch.mutationCount, 0),
		deletes.length,
	);
	await index.close();
});

test('encodes a large multi-valued field without spreading codec chunks onto the stack', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	const encoded = index.encodeMutationBatches({
		upserts: [{ id: 'many', fields: { title: Array.from({ length: 60_000 }, () => 'x') } }],
	});
	assert.strictEqual(encoded.rejected.length, 0);
	assert.strictEqual(encoded.batches.length, 1);
	assert.strictEqual(await index.apply(encoded.batches[0].bytes), 1);
	await index.close({ mode: 'rollback' });
});

test('stops encoding one oversized multi-valued record at the frame bound', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 1024 };
	const index = await openNativeFullTextIndex(config);
	let valuesRead = 0;
	const values = new Proxy(Array(60_000).fill('x'), {
		get(target, property, receiver) {
			if (typeof property === 'string' && /^\d+$/u.test(property)) valuesRead++;
			return Reflect.get(target, property, receiver);
		},
	});
	const encoded = index.encodeMutationBatches({ upserts: [{ id: 'oversized', fields: { title: values } }] });
	assert.deepStrictEqual(encoded.rejected, [{ operation: 'upsert', index: 0, code: 'E_BATCH_TOO_LARGE' }]);
	assert.strictEqual(encoded.batches.length, 0);
	assert(valuesRead < 1000, `encoder read ${valuesRead} values after the record exceeded its frame`);
	await index.close();
});

test('reports a record that cannot fit one configured frame', async (context) => {
	const config = options(temporaryIndex(context));
	config.limits = { ...config.limits, maxBatchBytes: 1024, maxQueuedBytes: 8192 };
	const index = await openNativeFullTextIndex(config);
	const encoded = index.encodeMutationBatches({
		upserts: [
			{ id: 'valid', fields: { title: 'small' } },
			{ id: 'oversized', fields: { title: 'x'.repeat(2048) } },
		],
	});
	assert.deepStrictEqual(encoded.rejected, [{ operation: 'upsert', index: 1, code: 'E_BATCH_TOO_LARGE' }]);
	assert.strictEqual(encoded.batches.length, 1);
	assert.strictEqual(encoded.batches[0].mutationCount, 1);
	await index.close();
});

test('rejects duplicate encoded IDs before partitioning', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	assert.throws(
		() =>
			index.encodeMutationBatches({
				upserts: [{ id: 'same', fields: { title: 'value' } }],
				deletes: ['same'],
			}),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.throws(
		() =>
			index.encodeMutationBatches(
				{
					upserts: [
						{ id: 'same-prefix', fields: { title: 'x'.repeat(128) } },
						{ id: 'same-prefix', fields: { title: 'x'.repeat(128) } },
					],
				},
				{ maxTotalBytes: 256 },
			),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
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

test('snapshots the close mode before checking and executing it', async (context) => {
	const index = await openNativeFullTextIndex(options(temporaryIndex(context)));
	await index.apply(encodeMutationBatch({ upserts: [{ id: 'one', fields: { title: 'one' } }] }));
	let reads = 0;
	await assert.rejects(
		index.close({
			get mode() {
				reads++;
				return reads === 1 ? 'require-clean' : 'rollback';
			},
		}),
		(error) => error.code === 'E_DIRTY_CLOSE',
	);
	assert.strictEqual(reads, 1);
	assert.strictEqual(index.status().uncommittedMutations, 1n);
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

test('snapshots schema and limits before awaiting native open', async (context) => {
	const config = options(temporaryIndex(context));
	const opening = openNativeFullTextIndex(config);
	config.fields[0].name = 'mutated';
	config.limits.maxBatchBytes = 64;
	const index = await opening;
	const encoded = index.encodeMutationBatches({
		upserts: [{ id: 'original', fields: { title: 'x'.repeat(128) } }],
	});
	assert.strictEqual(encoded.rejected.length, 0);
	assert.strictEqual(encoded.batches.length, 1);
	assert(encoded.batches[0].bytes.byteLength > config.limits.maxBatchBytes);
	await index.close();
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
	if (child.exitCode !== null || child.signalCode !== null) {
		return Promise.reject(
			new Error(`child exited before ${expected}: code=${child.exitCode} signal=${child.signalCode}`),
		);
	}
	return new Promise((resolve, reject) => {
		const cleanup = () => {
			child.off('message', onMessage);
			child.off('error', onError);
			child.off('exit', onExit);
		};
		const onMessage = (message) => {
			if (message !== expected) return;
			cleanup();
			resolve();
		};
		const onError = (error) => {
			cleanup();
			reject(error);
		};
		const onExit = (code, signal) => {
			cleanup();
			reject(new Error(`child exited before ${expected}: code=${code} signal=${signal}`));
		};
		child.once('error', onError);
		child.once('exit', onExit);
		child.on('message', onMessage);
	});
}

function committedSegmentPath(directory, extension) {
	const name = readdirSync(directory).find((entry) => entry.endsWith(extension));
	assert(name, `expected a committed ${extension} segment`);
	return path.join(directory, name);
}
