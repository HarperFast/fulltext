import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { openNativeFullTextIndex, openNativeFullTextReader, runtimeInfo } from '@harperfast/fulltext/native';
import { decodeResponse, encodeOpen } from '../dist/codec.js';
import { loadAddon } from '../dist/load-addon.js';

const words = (count) => Array.from({ length: count }, (_, i) => `word${i}`).join(' ');
const invalid = (error) => error.code === 'E_INVALID_ARGUMENT';
function options(directory, policy = {}, fields = [{ name: 'title' }, { name: 'body' }]) {
	return {
		path: directory,
		indexId: 'capacity',
		generation: 'one',
		analyzer: 'english@2',
		positions: true,
		surfaceTerms: true,
		stopWords: false,
		fields: fields.map((field) => ({ weight: 1, ...field })),
		filterFields: [{ name: 'category', type: 'string' }],
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8 << 20,
			maxBatchBytes: 8 << 20,
			...policy,
		},
	};
}
async function fixture(context, policy = {}, fields) {
	const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-query-capacity-'));
	const config = options(directory, policy, fields);
	const index = await openNativeFullTextIndex(config);
	context.after(async () => {
		const result = await index.close({ mode: 'rollback' });
		assert(!result.cleanupError);
		rmSync(directory, { recursive: true, force: true });
	});
	return { index, config };
}

test('default term boundary is preserved and repeated terms do not consume distinct capacity', async (context) => {
	const { index } = await fixture(context);
	assert.deepStrictEqual(index.queryLimits, { maxQueryTerms: 64, maxQueryClauses: 256 });
	await index.search({ text: words(64), fields: ['title'] });
	await assert.rejects(index.search({ text: words(65), fields: ['title'], maxQueryTerms: 1024 }), invalid);
	await index.search({ text: 'word0 '.repeat(10_000), fields: ['title'] });
	await assert.rejects(index.search({ text: 'word0 '.repeat(65), mode: 'phrase', fields: ['title'] }), invalid);
});

test('raised term and compiled-clause policies preserve results through readers and reopen', async (context) => {
	const { index, config } = await fixture(context, { maxQueryTerms: 256, maxQueryClauses: 512 });
	await index.applyMutationBatch({
		upserts: ['a', 'b'].map((id) => ({
			id,
			version: id,
			fields: { title: words(130), body: words(130) },
			filters: { category: 'sale' },
		})),
	});
	await index.publish('capacity:1');
	assert(Object.isFrozen(index.queryLimits));
	assert.throws(() => {
		index.queryLimits.maxQueryTerms = 64;
	}, TypeError);
	const result = await index.search({ text: words(130), exactTotal: true });
	assert.strictEqual(result.total, 2);
	assert.deepStrictEqual(
		result.hits.map((hit) => hit.id),
		['a', 'b'],
	);
	const reader = await openNativeFullTextReader(config);
	try {
		assert.deepStrictEqual(reader.queryLimits, index.queryLimits);
		assert.deepStrictEqual(await reader.search({ text: words(130), exactTotal: true }), result);
		const first = await reader.searchPage({ text: words(130), limit: 1 });
		assert(first.continuation);
		const second = await reader.searchPage({ text: words(130), limit: 1, continuation: first.continuation });
		assert.deepStrictEqual([...first.hits, ...second.hits], result.hits);
		const trace = await reader.traceMatches({ text: words(130) }, [{ id: 'a', fields: { title: words(130) } }]);
		assert.strictEqual(trace.complete, true);
		assert.strictEqual(trace.records[0].values[0].spans.length, 130);
	} finally {
		assert(!(await reader.close()).cleanupError);
	}
	await assert.rejects(index.search({ text: words(257), fields: ['title'] }), invalid);
	await assert.rejects(index.traceMatches({ text: words(257) }, [{ id: 'a', fields: { title: 'word0' } }]), invalid);
	await index.close();
	const lower = await openNativeFullTextIndex({
		...config,
		limits: { ...config.limits, maxQueryTerms: 256, maxQueryClauses: 256 },
	});
	try {
		assert.strictEqual(lower.committedPayload, 'capacity:1');
		await lower.search({ text: words(128) });
		await assert.rejects(
			lower.search({ text: words(129) }),
			(error) => invalid(error) && /256 clauses/.test(error.message),
		);
		assert.strictEqual((await lower.search({ text: 'word0' })).total, 2);
	} finally {
		assert(!(await lower.close()).cleanupError);
	}
});

test('lowered policies apply to phrase, prefix, fuzzy and reader analysis without changing structural bounds', async (context) => {
	const { index } = await fixture(context, { maxQueryTerms: 2, maxQueryClauses: 3 });
	for (const mode of ['any', 'all', 'phrase', 'prefix', 'fuzzy', 'fuzzy-prefix'])
		await assert.rejects(index.search({ text: 'alpha bravo charlie', mode, fields: ['title'] }), invalid);
	await assert.rejects(index.search({ text: 'alpha bravo' }), invalid);
	await index.search({ text: 'alpha', fields: ['title'] });
	const { index: raised } = await fixture(context, { maxQueryTerms: 1024, maxQueryClauses: 4096 });
	await assert.rejects(
		raised.search({ text: 'word0', filter: { field: 'category', comparator: 'in', value: Array(257).fill('sale') } }),
		invalid,
	);
	await assert.rejects(
		raised.search({ query: { operator: 'or', clauses: Array.from({ length: 257 }, () => ({ text: 'word0' })) } }),
		invalid,
	);
});

test('query construction has fixed whole-tree phrase and fuzzy safeguards', async (context) => {
	const { index } = await fixture(
		context,
		{ maxQueryTerms: 1024, maxQueryClauses: 4096 },
		Array.from({ length: 128 }, (_, i) => ({ name: `field${i}` })),
	);
	await assert.rejects(
		index.search({ text: words(1024), mode: 'phrase' }),
		(error) => invalid(error) && /phrase construction/.test(error.message),
	);
	const text = 'alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa';
	await assert.rejects(
		index.search({
			query: {
				operator: 'or',
				clauses: Array.from({ length: 18 }, () => ({ text, mode: 'fuzzy', fields: ['field0'] })),
			},
		}),
		(error) => invalid(error) && /fuzzy construction/.test(error.message),
	);
	await index.search({ text: 'alpha', fields: ['field0'] });
});

test('open rejects invalid policies in the facade and native decoder; capability metadata separates defaults and ceilings', async () => {
	const info = await runtimeInfo();
	assert.strictEqual(info.limits.maxQueryTerms, 64);
	assert.strictEqual(info.limits.maxQueryClauses, 256);
	for (const [name, ceiling] of [
		['maxQueryTerms', info.limits.maxConfiguredQueryTerms],
		['maxQueryClauses', info.limits.maxConfiguredQueryClauses],
	]) {
		for (const value of [0, -1, 1.5, NaN, Infinity, -Infinity, '128', ceiling + 1, 2 ** 32])
			await assert.rejects(openNativeFullTextIndex(options('unused', { [name]: value })), invalid);
		for (const value of [0, ceiling + 1]) {
			const bytes = encodeOpen(options('unused'));
			bytes.writeUInt32LE(value, bytes.length - (name === 'maxQueryTerms' ? 8 : 4));
			assert.throws(() => decodeResponse(loadAddon().__nativeValidateOpen(bytes)), invalid);
		}
	}
	const legacy = encodeOpen(options('unused')).subarray(0, -8);
	decodeResponse(loadAddon().__nativeValidateOpen(legacy)).finish();
});
