import assert from 'node:assert';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
	NativeFullTextIndex,
	encodeMutationBatch,
	openNativeFullTextIndex,
	resetNativeFullTextIndex,
	runtimeInfo,
} from '@harperfast/fulltext/native';
import {
	decodeResponse,
	encodeOpen,
	encodeSearch,
	maxCommitPayloadBytes,
	maxSearchRequestBytes,
	maxSearchBudgetMilliseconds,
	maxTraceRecords,
	maxQueryClauses,
	maxFilterStringBytes,
	maxSynonymRules,
	maxSynonymReplacements,
	maxSynonymBytes,
	maxRecordIdBytes,
	maxRecordVersionBytes,
} from '../dist/codec.js';
import { normalizeNativeError } from '../dist/errors.js';
import { loadAddon, platformTriple } from '../dist/load-addon.js';

const packageManifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const cargoManifest = readFileSync(new URL('../Cargo.toml', import.meta.url), 'utf8');
const cargoPackageVersion = /^version\s*=\s*"([^"]+)"/m.exec(tomlSection(cargoManifest, 'package'))?.[1];
const tantivyVersion = /^tantivy\s*=\s*"=([^"]+)"/m.exec(tomlSection(cargoManifest, 'dependencies'))?.[1];

test('loads the artifact for the executing platform', async () => {
	assert.deepStrictEqual(loadAddon().runtimeInfo().storageBackends, ['native']);
	const info = await runtimeInfo();
	assert.deepStrictEqual(info, {
		packageVersion: packageManifest.version,
		tantivyVersion,
		nativeAbiVersion: 9,
		queryApiVersion: 4,
		queryClassIsolationMinimumSearchThreads: 2,
		lifecycleApiVersion: 1,
		mutationBatchApiVersion: 5,
		storageBackends: ['native'],
		limits: {
			maxCommitPayloadBytes,
			maxQueryTextBytes: 64 * 1024,
			maxQueryTerms: 64,
			maxQueryClauses,
			maxCandidateIds: 1_024,
			maxCandidateBytes: 1 << 20,
			maxRecordIdBytes,
			maxRecordVersionBytes,
			maxPrefixExpansions: 50,
			maxFuzzyTerms: 16,
			maxSearchWindow: 10_000,
			maxSearchPageSize: 10_000,
			maxFilterStringBytes,
			maxAutocompleteResults: 100,
			maxSearchRequestBytes,
			maxSearchResponseBytes: 8 << 20,
			maxSearchBudgetMilliseconds,
			maxTraceRecords,
			maxTraceSourceBytes: 1 << 20,
			maxTraceSpans: 1_024,
			maxSynonymRules,
			maxSynonymReplacements,
			maxSynonymBytes,
		},
	});
	assert.strictEqual(cargoPackageVersion, packageManifest.version);
	assert.match(platformTriple(), /^(darwin|linux|win32)-(arm64|x64)(-(gnu|musl|msvc))?$/);
});

test('turns a panic into a coded terminal error', async () => {
	const addon = loadAddon();
	assert(addon.__testCreateHandle && addon.__testPanic && addon.__testCheck);
	const firstHandle = addon.__testCreateHandle();
	const secondHandle = addon.__testCreateHandle();
	assert.throws(
		() => addon.__testCheck(0),
		(error) => normalizeNativeError(error).code === 'E_NATIVE_FAILURE',
	);
	assert.strictEqual(addon.__testCheck(secondHandle), true);
	assert.throws(
		() => addon.__testPanic(firstHandle),
		(error) => normalizeNativeError(error).code === 'E_NATIVE_PANIC',
	);
	assert.throws(
		() => addon.__testCheck(firstHandle),
		(error) => normalizeNativeError(error).code === 'E_POISONED',
	);
	assert.strictEqual(addon.__testCheck(secondHandle), true);
	await assert.doesNotReject(runtimeInfo());
});

test('default close tears down a poisoned native handle', async (context) => {
	const indexPath = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-poison-'));
	context.after(() => rmSync(indexPath, { recursive: true, force: true }));
	const addon = loadAddon();
	assert(addon.__testPoisonNativeHandle);
	const config = {
		path: indexPath,
		indexId: 'poison-close',
		generation: 'one',
		fields: [{ name: 'title', weight: 1 }],
		analyzer: 'english@2',
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
	const opened = await invoke((callback) => addon.__nativeOpen(encodeOpen(config), callback));
	const handle = opened.u32();
	assert.strictEqual(opened.u8(), 0);
	opened.finish();
	addon.__testPoisonNativeHandle(handle);
	const closed = await invoke((callback) => addon.__nativeClose(handle, false, callback));
	closed.finish();
	const reopened = await invoke((callback) => addon.__nativeOpen(encodeOpen(config), callback));
	const reopenedHandle = reopened.u32();
	assert.strictEqual(reopened.u8(), 0);
	reopened.finish();
	const reclosed = await invoke((callback) => addon.__nativeClose(reopenedHandle, false, callback));
	reclosed.finish();
});

test('a close failure after resource release remains resettable', async (context) => {
	const parent = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-close-failed-'));
	const indexPath = path.join(parent, 'index');
	context.after(() => rmSync(parent, { recursive: true, force: true }));
	const { addon, handle, index } = await testIndex(indexPath, 'close-failed');
	await assert.rejects(
		index.applyMutationBatch({
			upserts: [
				{ id: 'first', fields: { title: 'x'.repeat(700_000) } },
				{ id: 'second', fields: { title: 'x'.repeat(700_000) } },
				{ id: 'invalid', fields: { title: 'x'.repeat((1 << 20) + 1) } },
			],
		}),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert(addon.__testFailNextClose);
	addon.__testFailNextClose(handle, true);
	const result = await index.close({ mode: 'rollback' });
	assert.strictEqual(result.cleanupError?.code, 'E_CLOSE_FAILED');
	assert.strictEqual(index.status().state, 'closed');
	await assert.rejects(index.commit(), (error) => error.code === 'E_CLOSED');
	assert.strictEqual((await index.close()).cleanupError?.code, 'E_CLOSE_FAILED');
	assert.strictEqual((await resetNativeFullTextIndex({ path: indexPath, indexId: 'close-failed' })).state, 'reset');
});

test('post-close logical batches do not retain the active latch', async (context) => {
	const parent = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-post-close-batch-'));
	const indexPath = path.join(parent, 'index');
	context.after(() => rmSync(parent, { recursive: true, force: true }));
	const { index } = await testIndex(indexPath, 'post-close-batch');

	const closing = index.close();
	await assert.rejects(index.applyMutationBatch({ deletes: ['closing'] }), (error) => error.code === 'E_CLOSED');
	await closing;
	await assert.rejects(index.applyMutationBatch({}), (error) => error.code === 'E_CLOSED');
	const rejectedBatch = assert.rejects(
		index.applyMutationBatch({ deletes: ['closed'] }),
		(error) => error.code === 'E_CLOSED',
	);
	await assert.rejects(index.commit(), (error) => error.code === 'E_CLOSED');
	await rejectedBatch;
	await assert.rejects(index.applyMutationBatch({ deletes: ['closed-again'] }), (error) => error.code === 'E_CLOSED');
});

test('an unproven close releases environment tracking and quarantines the index', async (context) => {
	const parent = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-quiescence-failed-'));
	const indexPath = path.join(parent, 'index');
	context.after(() => rmSync(parent, { recursive: true, force: true }));
	const { addon, handle, index } = await testIndex(indexPath, 'quiescence-failed');
	assert(addon.__testFailNextClose);
	addon.__testFailNextClose(handle, false);
	await assert.rejects(index.close(), (error) => error.code === 'E_QUIESCENCE_FAILED');
	assert.strictEqual(index.status().state, 'poisoned');
	await assert.rejects(index.close(), (error) => error.code === 'E_QUIESCENCE_FAILED');
	await assert.rejects(
		resetNativeFullTextIndex({ path: indexPath, indexId: 'quiescence-failed' }),
		(error) => error.code === 'E_QUIESCENCE_FAILED',
	);
	const healthy = await openNativeFullTextIndex(nativeOptions(path.join(parent, 'healthy'), 'quiescence-failed'));
	await healthy.close();
});

async function testIndex(indexPath, indexId, options = nativeOptions(indexPath, indexId)) {
	const addon = loadAddon();
	const opened = await invoke((callback) => addon.__nativeOpen(encodeOpen(options), callback));
	const handle = opened.u32();
	assert.strictEqual(opened.u8(), 0);
	opened.finish();
	return {
		addon,
		handle,
		index: new NativeFullTextIndex({
			handle,
			maxBatchBytes: options.limits.maxBatchBytes,
			fieldNames: options.fields.map((field) => field.name),
			filterFields: options.filterFields,
		}),
	};
}

function nativeOptions(indexPath, indexId) {
	return {
		path: indexPath,
		indexId,
		generation: 'one',
		fields: [{ name: 'title', weight: 1 }],
		analyzer: 'english@2',
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
}

function invoke(start) {
	return new Promise((resolve, reject) => {
		try {
			start((response) => {
				try {
					resolve(decodeResponse(response));
				} catch (error) {
					reject(error);
				}
			});
		} catch (error) {
			reject(normalizeNativeError(error));
		}
	});
}

function tomlSection(manifest, name) {
	const sectionStart = manifest.indexOf(`[${name}]`);
	assert.notStrictEqual(sectionStart, -1, `Cargo.toml is missing [${name}]`);
	const bodyStart = sectionStart + name.length + 2;
	const nextSection = manifest.indexOf('\n[', bodyStart);
	return manifest.slice(bodyStart, nextSection === -1 ? undefined : nextSection);
}

test('enforces advertised page and filter bounds without truncation through both mutation APIs', async (context) => {
	const parent = mkdtempSync(path.join(tmpdir(), 'fulltext-limits-'));
	let index;
	context.after(async () => {
		if (index) await index.close({ mode: 'rollback' });
		rmSync(parent, { recursive: true, force: true });
	});
	const options = nativeOptions(path.join(parent, 'index'), 'limits');
	options.filterFields = [{ name: 'key', type: 'string' }];
	const opened = await testIndex(options.path, options.indexId, options);
	index = opened.index;
	const { limits } = await runtimeInfo();
	const value = 'x'.repeat(limits.maxFilterStringBytes);
	await index.applyMutationBatch({
		upserts: [{ id: 'logical', fields: { title: 'widget' }, filters: { key: value } }],
	});
	await index.apply(
		encodeMutationBatch({ upserts: [{ id: 'raw', fields: { title: 'widget' }, filters: { key: value } }] }),
	);
	await assert.rejects(
		index.apply(
			encodeMutationBatch({ upserts: [{ id: 'invalid', fields: { title: 'widget' }, filters: { key: `${value}x` } }] }),
		),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(
		index.applyMutationBatch({
			upserts: [{ id: 'invalid-logical', fields: { title: 'widget' }, filters: { key: `${value}x` } }],
		}),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await index.publish('limits');
	await assert.rejects(
		invoke((callback) =>
			opened.addon.__nativeSearch(
				opened.handle,
				encodeSearch({
					expression: { text: 'widget', mode: 'any', fields: [] },
					filter: { field: 'key', comparator: 'equals', values: [`${value}x`] },
					offset: 0,
					limit: 20,
					exactTotal: false,
					budgetMilliseconds: maxSearchBudgetMilliseconds,
				}),
				callback,
			),
		),
		(error) => error.code === 'E_INVALID_ARGUMENT' && error.message.includes(String(limits.maxFilterStringBytes)),
	);

	assert.strictEqual(
		(await index.search({ text: 'widget', filter: { field: 'key', comparator: 'equals', value }, exactTotal: true }))
			.total,
		2,
	);
	await assert.rejects(
		index.search({ text: 'widget', filter: { field: 'key', comparator: 'equals', value: `${value}x` } }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	await assert.rejects(
		index.search({ text: 'widget', offset: limits.maxSearchWindow, limit: 1 }),
		(error) => error.code === 'E_INVALID_ARGUMENT' && /searchPage/.test(error.message),
	);
	assert.strictEqual((await index.searchPage({ text: 'widget', limit: limits.maxSearchPageSize })).hits.length, 2);
	await assert.rejects(
		index.searchPage({ text: 'widget', limit: limits.maxSearchPageSize + 1 }),
		(error) => error.code === 'E_INVALID_ARGUMENT' && /reduce page size/.test(error.message),
	);
});

test('makes analyzer byte boundaries and incomplete trace analysis explicit', async (context) => {
	const parent = mkdtempSync(path.join(tmpdir(), 'fulltext-analysis-limits-'));
	let index;
	context.after(async () => {
		if (index) await index.close({ mode: 'rollback' });
		rmSync(parent, { recursive: true, force: true });
	});
	const options = nativeOptions(path.join(parent, 'index'), 'analysis-limits');
	options.surfaceTerms = true;
	index = await openNativeFullTextIndex(options);
	const terms = ['x'.repeat(39), 'x'.repeat(40), 'x'.repeat(41), '中'.repeat(13), '中'.repeat(14)];
	await index.applyMutationBatch({ upserts: terms.map((title, i) => ({ id: String(i), fields: { title } })) });
	await index.publish('analysis');
	for (const [i, text] of terms.entries()) {
		const result = await index.search({ text, exactTotal: true });
		assert.deepStrictEqual(
			result.hits.map((hit) => hit.id),
			Buffer.byteLength(text) < 40 ? [String(i)] : [],
		);
	}
	const exhausted = await index.traceMatches({ text: 'needle' }, [
		{ id: 'source', fields: { title: `${'x '.repeat(262_144)}needle` } },
	]);
	assert.deepStrictEqual(exhausted, { complete: false, records: [] });
	const bounded = await index.traceMatches({ text: 'needle' }, [
		{ id: 'source', fields: { title: 'needle '.repeat(1_025) } },
	]);
	assert.strictEqual(bounded.complete, false);
	assert.strictEqual(bounded.records[0].values[0].spans.length, (await runtimeInfo()).limits.maxTraceSpans);
});

test('rejects score overflow before returning hits or continuations and recovers with lower weights', async (context) => {
	const parent = mkdtempSync(path.join(tmpdir(), 'fulltext-score-limits-'));
	let index;
	context.after(async () => {
		if (index) await index.close({ mode: 'rollback' });
		rmSync(parent, { recursive: true, force: true });
	});
	const options = nativeOptions(path.join(parent, 'index'), 'score-limits');
	options.fields = [{ name: 'title', weight: 3e38 }];
	index = await openNativeFullTextIndex(options);
	await index.applyMutationBatch({
		upserts: Array.from({ length: 100 }, (_, i) => ({ id: String(i), fields: { title: i < 2 ? 'needle' : 'common' } })),
	});
	await index.publish('scores');
	const overflow = (error) => error.code === 'E_INVALID_ARGUMENT' && /lower field weights/.test(error.message);
	await assert.rejects(index.search({ text: 'needle' }), overflow);
	await assert.rejects(index.searchPage({ text: 'needle', limit: 1 }), overflow);
	await index.close();
	options.fields = [{ name: 'title', weight: 1 }];
	index = await openNativeFullTextIndex(options);
	const first = await index.searchPage({ text: 'needle', limit: 1 });
	const second = await index.searchPage({ text: 'needle', limit: 1, continuation: first.continuation });
	assert.deepStrictEqual(
		[...first.hits, ...second.hits].map((hit) => hit.id),
		['0', '1'],
	);
	assert([...first.hits, ...second.hits].every((hit) => Number.isFinite(hit.score)));
	assert.strictEqual(second.continuation, undefined);
});
