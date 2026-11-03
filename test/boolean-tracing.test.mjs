import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const records = [
	{ id: 'a', fields: { title: 'amber quartz', body: 'trail running shoe' } },
	{ id: 'b', fields: { title: 'amber stone', body: 'laptop charges' } },
	{ id: 'c', fields: { title: 'quartz stone', body: 'laptop chargers' } },
	{ id: 'd', fields: { title: '', body: '' } },
];
const leaf = (text, fields = ['title'], mode = 'any') => ({ text, fields, mode });
const and = (...clauses) => ({ operator: 'and', clauses });
const or = (...clauses) => ({ operator: 'or', clauses });
const not = (clause) => ({ operator: 'not', clause });
const boost = (clause) => ({ operator: 'boost', boost: 2, clause });
const optional = (required, ...optional) => ({ operator: 'optional', required, optional });
async function fixture(context) {
	const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-boolean-trace-'));
	const index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'boolean-trace',
		generation: 'one',
		analyzer: 'english@2',
		fields: [{ name: 'title' }, { name: 'body' }],
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
		await index.close({ mode: 'rollback' });
		await rm(directory, { recursive: true, force: true });
	});
	await index.applyMutationBatch({ upserts: records });
	await index.commit();
	await index.reload();
	return index;
}
const cases = [
	and(leaf('amber'), not(leaf('quartz'))),
	or(and(leaf('amber'), leaf('missing')), leaf('quartz')),
	boost(leaf('amber')),
	optional(leaf('amber'), boost(leaf('quartz'))),
	not(leaf('quartz')),
	not(not(leaf('amber'))),
	and(leaf('laptop', ['body']), not(leaf('chargers', ['body'], 'fuzzy'))),
	and(leaf('amber'), leaf('running shoe', ['body'], 'phrase')),
	or(leaf('ambr', ['title'], 'fuzzy'), leaf('char', ['body'], 'prefix')),
];
for (const [number, query] of cases.entries()) {
	test(`Boolean trace ${number} agrees with fresh indexed membership and retains source spans`, async (context) => {
		const index = await fixture(context);
		const searched = await index.search({ query, limit: 100 });
		const traced = await index.traceMatches({ query }, records, { snippets: true });
		assert.strictEqual(traced.complete, true);
		assert.deepStrictEqual(traced.records.map(({ id }) => id).sort(), searched.hits.map(({ id }) => id).sort());
		for (const record of traced.records) {
			for (const value of record.values) {
				const source = records.find(({ id }) => id === record.id).fields[value.field];
				for (const span of value.spans) assert(source.slice(span.start, span.end).length > 0);
			}
		}
	});
}

test('false Boolean branches and negative clauses never contribute highlight spans', async (context) => {
	const index = await fixture(context);
	const query = or(and(leaf('amber'), leaf('missing')), leaf('quartz'));
	const result = await index.traceMatches({ query }, records);
	const first = result.records.find(({ id }) => id === 'a');
	assert.deepStrictEqual(first.values[0].spans, [{ start: 6, end: 12 }]);
	const negative = await index.traceMatches({ query: not(leaf('quartz')) }, records);
	assert(negative.records.every(({ values }) => values.length === 0));
	const optionalResult = await index.traceMatches({ query: optional(leaf('amber'), leaf('quartz')) }, records);
	assert.deepStrictEqual(optionalResult.records.find(({ id }) => id === 'b').values[0].spans, [{ start: 0, end: 5 }]);
});

test('truncated source cannot establish negated absence, while a known OR branch can still highlight', async (context) => {
	const index = await fixture(context);
	const source = [{ id: 'long', fields: { title: 'ox '.repeat(262_145) + 'forbidden' } }];
	const unknown = await index.traceMatches({ query: and(leaf('ox'), not(leaf('forbidden'))) }, source);
	assert.strictEqual(unknown.complete, false);
	assert.deepStrictEqual(unknown.records, []);
	const known = await index.traceMatches({ query: or(leaf('ox'), not(leaf('forbidden'))) }, source);
	assert.strictEqual(known.complete, false);
	assert.deepStrictEqual(
		known.records.map(({ id }) => id),
		['long'],
	);
	assert(known.records[0].values[0].spans.length > 0);
});

test('negative membership does not consume span capacity and candidates restrict source records', async (context) => {
	const index = await fixture(context);
	const source = [{ id: 'long', fields: { title: 'amber '.repeat(1100) + 'stone' } }];
	const result = await index.traceMatches({ query: or(not(leaf('amber')), leaf('stone')) }, source);
	assert.strictEqual(result.complete, true);
	assert.deepStrictEqual(result.records[0].values[0].spans, [{ start: 6600, end: 6605 }]);
	const candidate = await index.traceMatches(
		{ query: or(leaf('amber'), leaf('quartz')), candidateIds: ['b'] },
		records,
	);
	assert.deepStrictEqual(
		candidate.records.map(({ id }) => id),
		['b'],
	);
});

test('Boolean traces reject unsupported search options and invalid nested options', async (context) => {
	const index = await fixture(context);
	for (const key of ['filter', 'offset', 'limit', 'sort', 'aggregations', 'exactTotal', 'continuation']) {
		await assert.rejects(index.traceMatches({ query: boost(leaf('amber')), [key]: 1 }, records), {
			code: 'E_INVALID_ARGUMENT',
		});
	}
	await assert.rejects(
		index.traceMatches({ query: { operator: 'and', fields: ['title'], clauses: [leaf('amber')] } }, records),
		{ code: 'E_INVALID_ARGUMENT' },
	);
	assert.strictEqual((await index.traceMatches({ query: boost(leaf('amber')) }, records)).complete, true);
});

test('Boolean fuzzy and phrase preparation share the indexed construction budget', async (context) => {
	const index = await fixture(context);
	const text = Array.from({ length: 16 }, (_, i) => `catalog${String.fromCharCode(97 + i)}`).join(' ');
	const fuzzy = { ...leaf(text), mode: 'fuzzy', fuzzyDistance: 2 };
	const query = or(...Array.from({ length: 5 }, () => fuzzy));
	await assert.rejects(index.search({ query }), { code: 'E_INVALID_ARGUMENT' });
	await assert.rejects(index.traceMatches({ query }, records), { code: 'E_INVALID_ARGUMENT' });
	assert.strictEqual((await index.traceMatches({ query: boost(leaf('amber')) }, records)).complete, true);
});

test('surface analysis is retained only for fields that need it', async (context) => {
	const index = await fixture(context);
	const source = [{ id: 'large', fields: { body: 'marker ' + 'ox '.repeat(262_143), title: 'bar' } }];
	const query = and(leaf('marker', ['body']), not(leaf('foo', ['title'], 'prefix')));
	const result = await index.traceMatches({ query }, source);
	assert.strictEqual(result.complete, true);
	assert.deepStrictEqual(
		result.records.map(({ id }) => id),
		['large'],
	);
	assert.deepStrictEqual(result.records[0].values[0].spans, [{ start: 0, end: 6 }]);
});

test('per-record retained analysis and per-request matching work fail conservatively', async (context) => {
	const index = await fixture(context);
	const retained = await index.traceMatches({ query: boost(leaf('tail', ['body'], 'fuzzy')) }, [
		{
			id: 'large',
			fields: { body: ['ox '.repeat(150_000), 'ox '.repeat(150_000), 'tail'] },
		},
	]);
	assert.strictEqual(retained.complete, false);
	assert.deepStrictEqual(retained.records, []);
	const exhausted = await index.traceMatches(
		{ query: or(...Array.from({ length: 128 }, () => leaf('absent', ['body']))) },
		[
			{
				id: 'large',
				fields: { body: 'ox '.repeat(100_000) },
			},
		],
	);
	assert.strictEqual(exhausted.complete, false);
	assert.deepStrictEqual(exhausted.records, []);
});

test('legacy trace mode and operator remain mutually exclusive', async (context) => {
	const index = await fixture(context);
	await assert.rejects(index.traceMatches({ text: 'amber', mode: 'all', operator: 'any' }, records), {
		code: 'E_INVALID_ARGUMENT',
	});
});

test('Boolean cached-value capacity bounds zero-token metadata and preserves unknown absence', async (context) => {
	const index = await fixture(context);
	const empty = await index.traceMatches({ query: not(leaf('absent')) }, [
		{ id: 'empty', fields: { title: Array(5_000).fill(' ') } },
	]);
	assert.strictEqual(empty.complete, true);
	assert.deepStrictEqual(empty.records, [{ id: 'empty', values: [] }]);
	const source = [{ id: 'many', fields: { title: [...Array(4_096).fill('oak'), 'forbidden'] } }];
	const result = await index.traceMatches({ query: and(leaf('oak'), not(leaf('forbidden'))) }, source);
	assert.strictEqual(result.complete, false);
	assert.deepStrictEqual(result.records, []);
});
