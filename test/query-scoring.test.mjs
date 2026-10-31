import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import { tmpdir } from 'node:os';
import test from 'node:test';
import { NativeFullTextIndex } from '@harperfast/fulltext/native';

import { encodeOpen, encodeSearch } from '../dist/codec.js';
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
	const index = new NativeFullTextIndex({
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
		index,
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

test('adds both optional contributions to an independently scored required match', async (context) => {
	const { index } = await fixture(context, [
		{ id: 'both', fields: { title: 'anchor', body: 'ruby sapphire' } },
		{ id: 'first', fields: { title: 'anchor', body: 'ruby' } },
		{ id: 'second', fields: { title: 'anchor', body: 'sapphire' } },
		{ id: 'neither', fields: { title: 'anchor', body: 'plain' } },
		{ id: 'optional-only', fields: { title: 'outside', body: 'ruby sapphire' } },
	]);
	const required = leaf('anchor');
	const first = leaf('ruby', ['body']);
	const second = leaf('sapphire', ['body']);
	const scores = async (query) =>
		new Map((await index.search({ query, exactTotal: true })).hits.map(({ id, score }) => [id, score]));
	const requiredScores = await scores(required);
	const firstScores = await scores(first);
	const secondScores = await scores(second);
	const actual = await index.search({
		query: optional(boost(required, 0.5), boost(first, 2), boost(second, 3.5)),
		exactTotal: true,
	});
	assert.deepStrictEqual(ids(actual), ['both', 'first', 'neither', 'second']);
	assert.strictEqual(actual.total, 4);
	for (const hit of actual.hits) {
		const contributions = [
			Math.fround(requiredScores.get(hit.id) * 0.5),
			Math.fround((firstScores.get(hit.id) ?? 0) * 2),
			Math.fround((secondScores.get(hit.id) ?? 0) * 3.5),
		];
		const expected = contributions.reduce((sum, score) => sum + score, 0);
		const tolerance = Math.max(1e-6, Math.abs(expected) * 1e-6);
		assert(Math.abs(hit.score - expected) <= tolerance, `additive score for ${hit.id}`);
		if (hit.id === 'both') {
			assert(hit.score > Math.max(...contributions) + tolerance, 'sum must exceed max-only scoring');
			for (const contribution of contributions)
				assert(contribution > tolerance, 'dropping any contribution must exceed the tolerance');
		}
	}
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
	const filter = { field: 'enabled', comparator: 'equals', value: true };
	const base = await index.search({ query: required, exactTotal: true, limit: 1, filter });
	const scored = await index.search({
		query: optional(required, boost(leaf('quartz'), 3)),
		filter,
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
		{ ...clause, operator: 'prefix' },
		{ ...clause, optional: [leaf('quartz')] },
		{ ...clause, required: leaf('quartz') },
		{ operator: 'and', clauses: [clause], optional: [leaf('quartz')] },
		{ operator: 'and', clauses: [clause], required: leaf('quartz') },
		{ ...boost(clause, 2), text: 'quartz' },
		{ ...optional(clause, leaf('quartz')), text: 'quartz' },
		{ ...boost(clause, 2), clauses: [leaf('quartz')] },
		{ ...optional(clause, leaf('quartz')), clause: leaf('stone') },
		{ ...optional(clause, leaf('quartz')), clauses: [leaf('stone')] },
		{ ...clause, clause: leaf('stone') },
		{ operator: 'and', clauses: [clause], clause: leaf('stone') },
		{ ...boost(clause, 2), fields: ['body'] },
		{ ...optional(clause, leaf('quartz')), mode: 'all' },
		{ operator: 'optional', required: clause, optional: new Array(1) },
		{ operator: 'optional', required: clause, optional: [, leaf('quartz')] },
	]) {
		await context.test(JSON.stringify(query), async () => {
			await assert.rejects(index.search({ query }), invalid);
			await assert.rejects(index.searchPage({ query }), invalid);
		});
	}
	assert.deepStrictEqual(
		await index.search({ text: 'amber', operator: 'prefix' }),
		await index.search({ text: 'amber', mode: 'prefix' }),
	);
	await assert.rejects(index.search({ query: clause, should: [leaf('quartz')] }), invalid);
	await assert.rejects(index.searchPage({ query: clause, should: [leaf('quartz')] }), invalid);
	await assert.rejects(index.search({ text: 'amber', required: clause }), invalid);
	const unsetControls = { boost: undefined, optional: undefined, required: undefined, should: undefined };
	const expected = await index.search({ query: clause });
	assert.deepStrictEqual(await index.search({ query: { ...clause, ...unsetControls } }), expected);
	assert.deepStrictEqual(await index.search({ query: clause, ...unsetControls }), expected);
	assert.deepStrictEqual(
		await index.search({
			query: { ...boost(clause, 2), clauses: undefined, optional: undefined, required: undefined },
		}),
		await index.search({ query: boost(clause, 2) }),
	);
});

test('multi-term Any scoring has identical search and continuation score sequences', async (context) => {
	const terms = ['amber', 'stone', 'quartz', 'ruby', 'sapphire', 'opal', 'silver', 'copper'];
	const documents = Array.from({ length: 127 }, (_, id) => ({
		id: String(id).padStart(2, '0'),
		fields: {
			title: terms
				.flatMap((term, index) =>
					Array.from({ length: (id + 1) & (1 << index) ? (((id + 3) * (index + 1)) % 11) + 1 : 0 }, () => term),
				)
				.join(' '),
			body: terms
				.flatMap((term, index) => Array.from({ length: (((id + 7) * (index + 3)) % 13) + 1 }, () => term))
				.join(' '),
		},
	}));
	const { index } = await fixture(context, documents, {
		fields: [
			{ name: 'title', weight: 0.7 },
			{ name: 'body', weight: 1.3 },
		],
	});
	const any = { text: terms.join(' '), mode: 'any', fields: ['title'] };
	for (const query of [boost(any, 0.7), optional(any, boost(leaf('quartz'), 0.3))]) {
		const expected = await index.search({ query, exactTotal: true, limit: documents.length });
		assert.strictEqual(expected.total, documents.length);
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

test('optional scoring snapshots Proxy array length before charging and reading children', async (context) => {
	const { index } = await fixture(context, records);
	for (const method of ['search', 'searchPage']) {
		let lengthReads = 0;
		let childReads = 0;
		const clauses = new Proxy([leaf('quartz')], {
			get(target, key, receiver) {
				if (key === 'length') return ++lengthReads <= 2 ? 1 : 100_000;
				if (typeof key === 'string' && /^\d+$/.test(key)) {
					childReads++;
					assert.strictEqual(key, '0', 'an uncharged child must never be read');
				}
				return Reflect.get(target, key, receiver);
			},
		});
		const query = { operator: 'optional', required: leaf('amber'), optional: clauses };
		assert.deepStrictEqual(
			await index[method]({ query }),
			await index[method]({ query: optional(leaf('amber'), leaf('quartz')) }),
		);
		assert.strictEqual(lengthReads, 1);
		assert.strictEqual(childReads, 1);

		lengthReads = 0;
		const overBudget = new Proxy([leaf('quartz')], {
			get(target, key, receiver) {
				if (key === 'length') {
					lengthReads++;
					return 100_000;
				}
				if (typeof key === 'string' && /^\d+$/.test(key)) assert.fail('over-budget children must not be read');
				return Reflect.get(target, key, receiver);
			},
		});
		await assert.rejects(index[method]({ query: { ...query, optional: overBudget } }), invalid);
		assert.strictEqual(lengthReads, 1);
	}
});

test('compiled term sums preserve small-window search and paging scores without boundary ties', async (context) => {
	const terms = ['amber', 'stone', 'quartz', 'ruby'];
	const documents = Array.from({ length: 300 }, (_, id) => ({
		id: `all-${id}`,
		fields: {
			title: [
				...terms.flatMap((term, position) => Array(1 + ((id * (7 + position * 4)) % (11 + position * 6))).fill(term)),
				...Array(1 + ((id * 37) % 173)).fill('filler'),
			].join(' '),
		},
	}));
	const all = { text: terms.join(' '), mode: 'all', fields: ['title'] };
	for (const [name, weight, query] of [
		['boosted All', 0.7, boost(all, 0.3)],
		[
			'prefix completed terms',
			Math.fround(Math.fround(0.7) * Math.fround(0.3)),
			{ ...all, mode: 'prefix', text: all.text + ' ' },
		],
		[
			'fuzzy-prefix completed terms',
			Math.fround(Math.fround(0.7) * Math.fround(0.3)),
			{ ...all, mode: 'fuzzy-prefix', text: all.text + ' ' },
		],
		['prefix singleton expansion', Math.fround(Math.fround(0.7) * Math.fround(0.3)), { ...all, mode: 'prefix' }],
		['bare All with folded weight', Math.fround(Math.fround(0.7) * Math.fround(0.3)), all],
		[
			'three-term All across segments',
			Math.fround(Math.fround(0.7) * Math.fround(0.3)),
			{ ...all, text: terms.slice(0, 3).join(' ') },
		],
		[
			'multi-field All with empty body',
			Math.fround(Math.fround(0.7) * Math.fround(0.3)),
			{ ...all, fields: ['title', 'body'] },
		],
		[
			'explicit AND with folded weight',
			Math.fround(Math.fround(0.7) * Math.fround(0.3)),
			{ operator: 'and', clauses: terms.map((text) => leaf(text)) },
		],
	]) {
		await context.test(name, async (context) => {
			const { index } = await fixture(
				context,
				[...documents.slice(0, 150), { id: 'distractor', fields: { title: 'amber stone' } }],
				{
					fields: [
						{ name: 'title', weight },
						{ name: 'body', weight: 1 },
					],
				},
			);
			await index.applyMutationBatch({ upserts: documents.slice(150) });
			await index.commit();
			await index.reload();
			const page = await index.searchPage({ query, limit: 6 });
			assert.strictEqual(page.total, documents.length);
			assert.strictEqual(page.hits.length, 6);
			assert.notStrictEqual(page.hits[4].score, page.hits[5].score, 'the search cutoff must not trigger a tie rerun');
			const searched = await index.search({ query, limit: 5, exactTotal: false });
			assert.deepStrictEqual(searched.hits, page.hits.slice(0, 5));
			assert(!searched.hits.some(({ id }) => id === 'distractor'));
			const first = await index.searchPage({ query, limit: 5 });
			assert.deepStrictEqual(first.hits, searched.hits);
			const next = await index.searchPage({ query, limit: 1, continuation: first.continuation });
			assert.deepStrictEqual(next.hits, page.hits.slice(5));
		});
	}
});
