import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';
import { Cursor, decodeTraceProgress } from '../dist/codec.js';
import { closeIndex } from '../benchmarks/metadata-cleanup.mjs';

const clause = (text = 'amber', fields = ['title']) => ({ text, fields, mode: 'any' });
const either = (...clauses) => ({ operator: 'or', clauses });
const not = (clause) => ({ operator: 'not', clause });
const row = (id, count) => ({ id, fields: { title: 'amber '.repeat(count) } });
const state = (id, membership = 'match', analysisComplete = true, spansComplete = true) => ({
	id,
	membership,
	analysisComplete,
	spansComplete,
});
const spanCount = (result) =>
	result.records.reduce((sum, record) => sum + record.values.reduce((sum, value) => sum + value.spans.length, 0), 0);

async function fixture(context) {
	const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-trace-recovery-'));
	const index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'trace-recovery',
		generation: 'one',
		analyzer: 'english@2',
		fields: [{ name: 'title' }, { name: 'body' }],
		positions: true,
		surfaceTerms: true,
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8 * 1024 * 1024,
			maxBatchBytes: 1024 * 1024,
		},
	});
	context.after(async () => {
		await closeIndex(index, { mode: 'rollback' });
		await rm(directory, { recursive: true, force: true });
	});
	return index;
}

async function recover(index, request, input, options = {}) {
	const records = [];
	const states = [];
	const pages = [];
	const deadline = performance.now() + 30_000;
	let offset = 0;
	for (let iteration = 0; offset < input.length; iteration++) {
		assert(iteration < input.length, 'recovery must terminate within one page per input');
		const remaining = deadline - performance.now();
		assert(remaining > 0, 'recovery exhausted its original deadline');
		const page = await index.traceMatchesPage(request, input.slice(offset), {
			...options,
			remainingBudgetMilliseconds: remaining,
		});
		assert(page.nextRecord > 0 && page.nextRecord <= input.length - offset);
		assert.strictEqual(page.states.length, page.nextRecord);
		assert.deepStrictEqual(
			page.states.map(({ id }) => id),
			input.slice(offset, offset + page.nextRecord).map(({ id }) => id),
		);
		records.push(...page.records);
		states.push(...page.states);
		pages.push(page);
		offset += page.nextRecord;
	}
	return { records, states, pages };
}

for (const request of [clause(), { query: either(clause(), clause('stone')) }]) {
	test(`recovery retains completed nonmatches and all 1800 spans for ${request.query ? 'Boolean' : 'simple'} clauses`, async (context) => {
		const index = await fixture(context);
		const input = [{ id: 'none', fields: { title: 'quartz' } }, row('a', 600), row('b', 600), row('c', 600)];
		const result = await recover(index, request, input);
		assert.strictEqual(result.pages.length, 3);
		assert.deepStrictEqual(
			result.pages.map(({ nextRecord }) => nextRecord),
			[2, 1, 1],
		);
		assert.deepStrictEqual(result.states, [state('none', 'no-match'), state('a'), state('b'), state('c')]);
		assert.strictEqual(spanCount(result), 1800);
		assert.deepStrictEqual(
			result.records.map(({ id }) => id),
			['a', 'b', 'c'],
		);
		for (const record of result.records)
			assert.deepStrictEqual(
				record.values[0].spans,
				Array.from({ length: 600 }, (_, i) => ({ start: i * 6, end: i * 6 + 5 })),
			);
	});

	test(`oversized singleton terminates once with explicit span truncation for ${request.query ? 'Boolean' : 'simple'} clauses`, async (context) => {
		const index = await fixture(context);
		const result = await recover(index, request, [row('large', 1100), row('small', 1)]);
		assert.strictEqual(result.pages.length, 2);
		assert.deepStrictEqual(result.states, [state('large', 'match', true, false), state('small')]);
		assert.strictEqual(result.pages[0].complete, false);
		assert.strictEqual(result.pages[0].nextRecord, 1);
		assert.strictEqual(spanCount(result.pages[0]), 1024);
		assert.strictEqual(result.pages[1].complete, true);
	});
}

test('exact-cap pages can consume subsequent nonmatches and candidate exclusions', async (context) => {
	const index = await fixture(context);
	const input = [row('full', 1024), { id: 'miss', fields: { title: 'quartz' } }, row('excluded', 10)];
	const page = await index.traceMatchesPage({ ...clause(), candidateIds: ['full', 'miss'] }, input);
	assert.strictEqual(page.complete, true);
	assert.strictEqual(page.nextRecord, 3);
	assert.deepStrictEqual(page.states, [state('full'), state('miss', 'no-match'), state('excluded', 'no-match')]);
	assert.strictEqual(spanCount(page), 1024);
	assert.deepStrictEqual(await index.traceMatchesPage(clause(), []), {
		complete: true,
		records: [],
		nextRecord: 0,
		states: [],
	});
});

test('analysis truncation stays unknown under NOT while a proven OR branch may return partial spans', async (context) => {
	const index = await fixture(context);
	const input = [{ id: 'limited', fields: { title: 'x '.repeat(262_144) + 'amber' } }];
	for (const request of [clause(), { query: not(clause()) }]) {
		const result = await index.traceMatchesPage(request, input);
		assert.strictEqual(result.nextRecord, 1);
		assert.strictEqual(result.complete, false);
		assert.deepStrictEqual(result.records, []);
		assert.deepStrictEqual(result.states, [state('limited', 'unknown', false, false)]);
	}
	const known = await index.traceMatchesPage({ query: either(not(clause()), clause('x')) }, input);
	assert.strictEqual(known.nextRecord, 1);
	assert.deepStrictEqual(known.states, [state('limited', 'match', false, false)]);
	assert.strictEqual(spanCount(known), 1024);
});

test('simple recovery retains legacy analysis beyond the Boolean cached-value limit', async (context) => {
	const index = await fixture(context);
	const input = [{ id: 'many', fields: { title: [...Array(4096).fill('quartz'), 'amber'] } }];
	const old = await index.traceMatches(clause(), input);
	assert.strictEqual(old.complete, true);
	const page = await index.traceMatchesPage(clause(), input);
	assert.deepStrictEqual({ complete: page.complete, records: page.records }, old);
	assert.deepStrictEqual(page.states, [state('many')]);
	assert.strictEqual(page.records[0].values[0].valueIndex, 4096);
});

test('simple prefix recovery preserves independent per-value analyzer budgets', async (context) => {
	const index = await fixture(context);
	const value = 'x '.repeat(262_140) + 'amber';
	const input = [{ id: 'many', fields: { title: [value, value] } }];
	const request = { text: 'amb', mode: 'prefix', fields: ['title'] };
	const old = await index.traceMatches(request, input);
	assert.strictEqual(old.complete, true);
	const page = await index.traceMatchesPage(request, input);
	assert.deepStrictEqual({ complete: page.complete, records: page.records }, old);
	assert.deepStrictEqual(page.states, [state('many')]);
	assert.strictEqual(spanCount(page), 2);
});

test('Boolean pages preserve zero-span negation matches, array values and Unicode snippet offsets', async (context) => {
	const index = await fixture(context);
	const input = [
		{ id: 'unicode', fields: { title: ['🦊 amber stone', 'stone amber'], body: 'quartz' } },
		{ id: 'negative', fields: { title: 'quartz', body: '' } },
	];
	const request = { query: either(clause(), not(clause('quartz'))) };
	const page = await index.traceMatchesPage(request, input, { snippets: true });
	assert.strictEqual(page.complete, true);
	assert.deepStrictEqual(page.states, [state('unicode'), state('negative', 'no-match')]);
	assert.deepStrictEqual(
		page.records[0].values.map(({ valueIndex, spans }) => ({ valueIndex, spans })),
		[
			{ valueIndex: 0, spans: [{ start: 3, end: 8 }] },
			{ valueIndex: 1, spans: [{ start: 6, end: 11 }] },
		],
	);
	for (const value of page.records[0].values) assert(value.fragments[0].text.includes('amber'));
	const negative = await index.traceMatchesPage({ query: not(clause('missing')) }, input);
	assert.deepStrictEqual(negative.states, [state('unicode'), state('negative')]);
	assert(negative.records.every(({ values }) => values.length === 0));
});

test('deterministic differential recovery agrees with every complete singleton', async (context) => {
	const index = await fixture(context);
	let seed = 123456789;
	const input = Array.from({ length: 24 }, (_, i) => {
		seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
		const count = [0, 1, 100, 600, 1024, 1100][seed % 6];
		return { id: `row${i}`, fields: { title: ['🦊 amber '.repeat(count), i % 2 ? 'stone' : 'quartz'], body: 'trail' } };
	});
	for (const request of [clause(), { query: either(clause(), not(clause('quartz'))) }]) {
		const result = await recover(index, request, input);
		for (const [i, record] of input.entries()) {
			const singleton = await index.traceMatches(request, [record]);
			if (!singleton.complete) continue;
			assert.strictEqual(result.states[i].membership, singleton.records.length ? 'match' : 'no-match');
			assert.strictEqual(result.states[i].analysisComplete, true);
			assert.strictEqual(result.states[i].spansComplete, true);
			assert.deepStrictEqual(
				result.records.filter(({ id }) => id === record.id),
				singleton.records,
			);
		}
	}
});

test('recovery preserves caller errors and stops on typed admission/closed errors', async (context) => {
	const index = await fixture(context);
	for (const failure of [new Error('caller failure'), 'primitive caller failure']) {
		await assert.rejects(
			index.traceMatchesPage(
				{
					get query() {
						throw failure;
					},
				},
				[],
			),
			(error) => error === failure,
		);
		await assert.rejects(
			index.traceMatchesPage(
				{
					...clause(),
					get candidateIds() {
						throw failure;
					},
				},
				[],
			),
			(error) => error === failure,
		);
		await assert.rejects(
			index.traceMatchesPage(clause(), [], {
				get remainingBudgetMilliseconds() {
					throw failure;
				},
			}),
			(error) => error === failure,
		);
	}
	await assert.rejects(index.traceMatchesPage(clause(), [row('same', 1), row('same', 1)]), {
		code: 'E_INVALID_ARGUMENT',
	});
	await assert.rejects(index.traceMatchesPage(clause(), [], { remainingBudgetMilliseconds: 0 }), {
		code: 'E_INVALID_ARGUMENT',
	});
	await closeIndex(index, { mode: 'rollback' });
	await assert.rejects(index.traceMatchesPage(clause(), []), { code: 'E_CLOSED' });
});

function progress(next, flags) {
	const bytes = Buffer.alloc(2 + flags.length);
	bytes.writeUInt16LE(next);
	Buffer.from(flags).copy(bytes, 2);
	return new Cursor(bytes);
}

test('recovery wire progress validates implicit input order and independent completeness flags', () => {
	assert.deepStrictEqual(decodeTraceProgress(progress(3, [12, 13, 1]), ['a', 'b', 'c'], ['b', 'c'], false), {
		nextRecord: 3,
		states: [state('a', 'no-match'), state('b'), state('c', 'match', false, false)],
	});
	assert.deepStrictEqual(decodeTraceProgress(progress(0, []), [], [], true), { nextRecord: 0, states: [] });
});

for (const [name, cursor, ids, matches, complete] of [
	['missing prefix', new Cursor(Buffer.from([1])), ['a'], [], false],
	['no progress', progress(0, []), ['a'], [], false],
	['prefix past input', progress(2, [12, 12]), ['a'], [], false],
	['missing status', progress(2, [12]), ['a', 'b'], [], false],
	['trailing byte', progress(1, [12, 12]), ['a'], [], false],
	['reserved membership', progress(1, [3]), ['a'], [], false],
	['reserved bits', progress(1, [28]), ['a'], [], false],
	['unknown complete analysis', progress(1, [6]), ['a'], [], false],
	['spans without complete analysis', progress(1, [9]), ['a'], ['a'], false],
	['missing matching record', progress(1, [13]), ['a'], [], true],
	['reordered matching record', progress(2, [13, 13]), ['a', 'b'], ['b', 'a'], true],
	['record without matching state', progress(1, [12]), ['a'], ['a'], true],
	['false complete prefix', progress(1, [13]), ['a', 'b'], ['a'], true],
	['false complete evidence', progress(1, [1]), ['a'], ['a'], true],
]) {
	test(`recovery wire rejects ${name}`, () =>
		assert.throws(() => decodeTraceProgress(cursor, ids, matches, complete), { code: 'E_NATIVE_FAILURE' }));
}
