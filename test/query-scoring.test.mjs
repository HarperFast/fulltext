import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import test from 'node:test';
import { NativeFullTextIndex, openNativeFullTextIndex } from '@harperfast/fulltext/native';

import { encodeOpen, encodeSearch, encodeTrace } from '../dist/codec.js';
import { invoke } from '../dist/invoke.js';
import { loadAddon } from '../dist/load-addon.js';
import { closeIndex } from '../benchmarks/metadata-cleanup.mjs';

const invalid = (error) => error.code === 'E_INVALID_ARGUMENT';
const ids = (result) => result.hits.map(({ id }) => id).sort();

async function fixture(context, documents, extra = {}) {
	const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-scoring-'));
	const options = {
		path: directory,
		indexId: 'query-scoring',
		generation: 'one',
		fields: [
			{ name: 'title', weight: 1 },
			{ name: 'body', weight: 1 },
		],
		filterFields: [{ name: 'enabled', type: 'boolean' }],
		analyzer: 'english@2',
		stopWords: false,
		synonyms: [],
		positions: true,
		surfaceTerms: true,
		...extra,
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 4 * 1024 * 1024,
			maxBatchBytes: 1024 * 1024,
		},
	};
	const addon = loadAddon();
	const opened = await invoke((callback) => addon.__nativeOpen(encodeOpen(options), callback));
	const handle = opened.u32();
	assert.strictEqual(opened.u8(), 0);
	opened.finish();
	let index = new NativeFullTextIndex({
		handle,
		maxBatchBytes: options.limits.maxBatchBytes,
		fieldNames: options.fields.map(({ name }) => name),
		filterFields: options.filterFields,
	});
	context.after(async () => {
		await closeIndex(index, { mode: 'rollback' });
		await rm(directory, { recursive: true, force: true });
	});
	await index.applyMutationBatch({ upserts: documents });
	await index.commit();
	await index.reload();
	return {
		handle,
		addon,
		get index() {
			return index;
		},
		async reopen() {
			await closeIndex(index);
			index = await openNativeFullTextIndex(options);
		},
	};
}

const leaf = (text, fields = ['title']) => ({ text, fields });
const boost = (clause, value) => ({ operator: 'boost', boost: value, clause });
const optional = (required, ...clauses) => ({ operator: 'optional', required, optional: clauses });

const records = [
	{ id: 'required', fields: { title: 'amber stone' }, filters: { enabled: true } },
	{ id: 'bonus', fields: { title: 'amber quartz' }, filters: { enabled: true } },
	{ id: 'should-only', fields: { title: 'quartz stone' }, filters: { enabled: true } },
	{ id: 'disabled', fields: { title: 'amber quartz' }, filters: { enabled: false } },
];

test('optional scoring preserves required eligibility and totals across segments and restrictions', async (context) => {
	const { index } = await fixture(context, records);
	await index.applyMutationBatch({
		upserts: [
			{ id: 'later', fields: { title: 'amber stone' }, filters: { enabled: true } },
			{ id: 'later-bonus', fields: { title: 'amber quartz' }, filters: { enabled: true } },
		],
	});
	await index.commit();
	await index.reload();
	const required = leaf('amber');
	const query = optional(required, boost(leaf('quartz'), 10));
	for (const exactTotal of [false, true]) {
		for (const restrictions of [
			{},
			{ candidateIds: ['required', 'bonus', 'should-only', 'later'] },
			{ filter: { field: 'enabled', comparator: 'equals', value: true } },
			{ candidateIds: [] },
		]) {
			const request = { exactTotal, limit: 20, ...restrictions };
			const base = await index.search({ ...request, query: required });
			const scored = await index.search({ ...request, query });
			assert.deepStrictEqual(ids(scored), ids(base));
			assert.strictEqual(scored.total, base.total);
			assert(!ids(scored).includes('should-only'));
		}
	}
	const ranked = await index.search({ query, exactTotal: true });
	assert(['bonus', 'disabled', 'later-bonus'].includes(ranked.hits[0].id));
	assert(ranked.hits.find(({ id }) => id === 'bonus').score > ranked.hits.find(({ id }) => id === 'required').score);
});

test('boosts scale scores, preserve zero-score membership and compose without a cumulative cap', async (context) => {
	const { index } = await fixture(context, records);
	const query = leaf('amber');
	const base = await index.search({ query, exactTotal: true });
	for (const value of [0, 1, 0.5, 0.1, 100]) {
		const actual = await index.search({ query: boost(query, value), exactTotal: true });
		assert.deepStrictEqual(ids(actual), ids(base));
		for (const hit of actual.hits) {
			const expected = base.hits.find(({ id }) => id === hit.id).score * Math.fround(value);
			assert(Math.abs(hit.score - expected) <= Math.max(1e-6, Math.abs(expected) * 1e-6));
		}
	}
	assert.deepStrictEqual(await index.search({ query: boost(query, 1), exactTotal: true }), base);
	const nested = await index.search({ query: boost(boost(query, 100), 100), exactTotal: true });
	assert.strictEqual(nested.total, base.total);
	assert(nested.hits.every(({ score }) => Number.isFinite(score) && score > 0));
});

test('optional expressions compose with AND, OR and NOT while stopword-only required clauses stay empty', async (context) => {
	const { index } = await fixture(context, records, { stopWords: true });
	const scored = optional(leaf('amber'), leaf('quartz'));
	for (const [query, expected] of [
		[{ operator: 'and', clauses: [scored, { operator: 'not', clause: leaf('quartz') }] }, ['required']],
		[{ operator: 'or', clauses: [scored, leaf('stone')] }, ['bonus', 'disabled', 'required', 'should-only']],
		[{ operator: 'not', clause: scored }, ['should-only']],
		[optional(leaf('the'), leaf('quartz')), []],
	]) {
		const result = await index.search({ query, exactTotal: true });
		assert.deepStrictEqual(ids(result), expected);
		assert.strictEqual(result.total, expected.length);
	}
});

test('scored paging exactly reproduces search scores and ties, and binds continuations to scoring', async (context) => {
	const { index } = await fixture(context, records);
	for (const query of [optional(leaf('amber'), boost(leaf('quartz'), 3)), boost(leaf('amber'), 0)]) {
		const expected = await index.search({ query, exactTotal: true });
		const hits = [];
		let continuation;
		do {
			const page = await index.searchPage({ query, limit: 1, continuation });
			assert.strictEqual(page.total, expected.total);
			hits.push(...page.hits);
			continuation = page.continuation;
			if (continuation) {
				await assert.rejects(index.searchPage({ query: boost(query, 2), limit: 1, continuation }), invalid);
			}
			assert(hits.length <= expected.total);
		} while (continuation);
		assert.deepStrictEqual(hits, expected.hits);
	}
});

test('invalid scoring expressions reject in search and paging and leave the index usable', async (context) => {
	const { index } = await fixture(context, records);
	let tooDeep = leaf('amber');
	for (let i = 0; i < 10; i++) tooDeep = boost(tooDeep, 1);
	for (const query of [
		...[NaN, Infinity, -Infinity, -1, -0, 101, '2', null].map((value) => boost(leaf('amber'), value)),
		{ operator: 'optional', required: leaf('amber'), optional: [] },
		{ operator: 'optional', optional: [leaf('quartz')] },
		{ operator: 'optional', required: leaf('amber'), optional: null },
		tooDeep,
	]) {
		await assert.rejects(index.search({ query }), invalid);
		await assert.rejects(index.searchPage({ query }), invalid);
	}
	assert.deepStrictEqual(ids(await index.search({ text: 'amber' })), ['bonus', 'disabled', 'required']);
});

test('native decoding rejects malformed boost payloads and retains reader usability', async (context) => {
	const { index, addon, handle } = await fixture(context, records);
	for (const value of [NaN, Infinity, -Infinity, -1, -0, 101]) {
		const frame = encodeSearch({
			expression: boost({ text: 'amber', mode: 'any', fields: ['title'] }, 1),
			offset: 0,
			limit: 10,
			exactTotal: true,
			budgetMilliseconds: 30_000,
		});
		assert.strictEqual(frame[6], 4);
		frame.writeFloatLE(value, 7);
		await assert.rejects(
			invoke((callback) => addon.__nativeSearch(handle, frame, callback)),
			invalid,
		);
	}
	assert.strictEqual((await index.search({ text: 'amber' })).total, 3);
});

test('expression wire tags preserve existing Boolean encodings and append scoring variants', () => {
	const clause = { text: 'x', mode: 'any', fields: [] };
	const leafHex = '0001000000780001010000';
	for (const [expression, expected] of [
		[clause, leafHex],
		[{ operator: 'and', clauses: [clause] }, `010100${leafHex}`],
		[{ operator: 'or', clauses: [clause] }, `020100${leafHex}`],
		[{ operator: 'not', clause }, `03${leafHex}`],
		[boost(clause, 0.5), `040000003f${leafHex}`],
		[optional(clause, clause), `050100${leafHex}${leafHex}`],
	]) {
		const frame = encodeSearch({
			expression,
			offset: 0,
			limit: 10,
			exactTotal: true,
			budgetMilliseconds: 30_000,
		});
		assert.strictEqual(frame.subarray(6, 6 + expected.length / 2).toString('hex'), expected);
	}
});

test('optional exact counts remain complete when the hit window requires a separate count', async (context) => {
	const { index } = await fixture(context, records);
	const required = leaf('amber');
	const base = await index.search({ query: required, exactTotal: true, limit: 1 });
	const scored = await index.search({
		query: optional(required, boost(leaf('quartz'), 3)),
		exactTotal: true,
		limit: 1,
	});
	assert.strictEqual(scored.hits.length, 1);
	assert(base.total > scored.hits.length);
	assert.strictEqual(scored.total, base.total);
});

test('misplaced scoring controls and conflicting expression shapes fail closed', async (context) => {
	const { index } = await fixture(context, records);
	const clause = leaf('amber');
	for (const query of [
		{ ...clause, boost: 2 },
		{ ...clause, optional: [leaf('quartz')] },
		{ ...clause, required: leaf('quartz') },
		{ operator: 'and', clauses: [clause], optional: [leaf('quartz')] },
		{ operator: 'and', clauses: [clause], required: leaf('quartz') },
		{ ...boost(clause, 2), text: 'quartz' },
		{ ...optional(clause, leaf('quartz')), text: 'quartz' },
	]) {
		await context.test(JSON.stringify(query), async () => {
			await assert.rejects(index.search({ query }), invalid);
			await assert.rejects(index.searchPage({ query }), invalid);
		});
	}
	await assert.rejects(index.search({ query: clause, should: [leaf('quartz')] }), invalid);
	await assert.rejects(index.searchPage({ query: clause, should: [leaf('quartz')] }), invalid);
});

test('multi-term Any scoring has identical search and continuation score sequences', async (context) => {
	const { index } = await fixture(context, records);
	for (const query of [
		boost({ text: 'amber stone', mode: 'any', fields: ['title'] }, 0.7),
		optional({ text: 'amber stone', mode: 'any', fields: ['title'] }, boost(leaf('quartz'), 0.3)),
	]) {
		const expected = await index.search({ query, exactTotal: true, limit: 20 });
		const hits = [];
		let continuation;
		do {
			const page = await index.searchPage({ query, limit: 1, continuation });
			hits.push(...page.hits);
			continuation = page.continuation;
			assert(hits.length <= expected.total);
		} while (continuation);
		assert.deepStrictEqual(hits, expected.hits);
	}
});
