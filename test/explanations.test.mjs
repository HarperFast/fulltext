import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { openNativeFullTextIndex, openNativeFullTextReader } from '@harperfast/fulltext/native';

const documents = [
	{ id: 'first', fields: { title: 'amber stone', body: 'trail running shoe' }, filters: { enabled: true } },
	{ id: 'second', fields: { title: 'amber quartz', body: 'waterproof running shoe' }, filters: { enabled: true } },
	{ id: 'third', fields: { title: 'quartz stone', body: 'waterproof boot' }, filters: { enabled: false } },
];
async function fixture(context, overrides = {}) {
	const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-explain-'));
	const options = {
		path: directory,
		indexId: 'explain',
		generation: 'one',
		analyzer: 'english@2',
		fields: [{ name: 'title', weight: 3 }, { name: 'body' }],
		filterFields: [{ name: 'enabled', type: 'boolean' }],
		surfaceTerms: true,
		...overrides,
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 4 * 1024 * 1024,
			maxBatchBytes: 1024 * 1024,
			...overrides.limits,
		},
	};
	const index = await openNativeFullTextIndex(options);
	context.after(async () => {
		await index.close({ mode: 'rollback' });
		await rm(directory, { force: true, recursive: true });
	});
	await index.applyMutationBatch({ upserts: documents });
	await index.commit();
	await index.reload();
	return { index, options };
}
const leaf = (text, fields = ['title'], mode = 'any') => ({ text, fields, mode });
const boost = (clause) => ({ operator: 'boost', boost: 2.5, clause });
const queries = [
	{ text: 'amber quartz stone' },
	{ text: 'amber stone', mode: 'all' },
	{ text: 'running shoe', fields: ['body'], mode: 'phrase' },
	{ text: 'waterp', fields: ['body'], mode: 'prefix' },
	{ text: 'ambr', mode: 'fuzzy' },
	{ query: boost(leaf('amber')) },
	{ query: boost({ operator: 'or', clauses: [leaf('amber'), leaf('stone')] }) },
	{ query: { operator: 'optional', required: leaf('amber'), optional: [boost(leaf('quartz'))] } },
	{ query: { operator: 'and', clauses: [leaf('stone'), { operator: 'not', clause: leaf('amber') }] } },
	{ text: 'amber stone', candidateIds: ['first'] },
	{ text: 'stone', filter: { field: 'enabled', comparator: 'equals', value: true } },
];
for (const [index, query] of queries.entries()) {
	test(`explain preserves search membership and exact score for query ${index}`, async (context) => {
		const { index } = await fixture(context);
		const ids = ['third', 'second', 'absent', 'first'];
		const searched = await index.search({ ...query, limit: 10, exactTotal: true });
		const page = await index.searchPage({ ...query, limit: 10 });
		assert.match(page.snapshot, /^[0-9a-f]{64}$/);
		const result = await index.explain(query, ids, { snapshot: page.snapshot });
		assert.strictEqual(result.snapshot, page.snapshot);
		assert.strictEqual(result.format, 'tantivy-0.27');
		assert.deepStrictEqual(
			result.records.map(({ id }) => id),
			ids,
		);
		for (const record of result.records) {
			const hit = searched.hits.find(({ id }) => id === record.id);
			assert.strictEqual(record.matched, !!hit);
			if (hit) {
				assert.strictEqual(record.score, hit.score);
				assert(Number.isFinite(record.explanation.value));
				assert.strictEqual(typeof record.explanation.description, 'string');
				assert.strictEqual(page.hits.find(({ id }) => id === record.id).score, record.score);
			} else assert.deepStrictEqual(record, { id: record.id, matched: false });
		}
	});
}

test('explain binds retained readers, rejects stale snapshots, and hides deleted IDs', async (context) => {
	const { index, options } = await fixture(context);
	const reader = await openNativeFullTextReader(options);
	try {
		const request = { text: 'amber' };
		const old = await reader.searchPage(request);
		await index.applyMutationBatch({ deletes: ['first'], upserts: [{ id: 'new', fields: { title: 'amber' } }] });
		await index.commit();
		await index.reload();
		const retained = await reader.explain(request, ['first'], { snapshot: old.snapshot });
		assert.strictEqual(retained.records[0].matched, true);
		await assert.rejects(index.explain(request, ['first'], { snapshot: old.snapshot }), { code: 'E_CURSOR_EXPIRED' });
		assert.deepStrictEqual((await index.explain(request, ['first'])).records, [{ id: 'first', matched: false }]);
		await reader.reload();
		await assert.rejects(reader.explain(request, ['first'], { snapshot: old.snapshot }), { code: 'E_CURSOR_EXPIRED' });
	} finally {
		await reader.close();
	}
});

test('explain validates bounded IDs, unsupported request options and snapshots without poisoning', async (context) => {
	const { index } = await fixture(context);
	for (const ids of [[], ['first', 'first'], Array(11).fill('first'), [''], ['x'.repeat(4097)]]) {
		await assert.rejects(index.explain({ text: 'amber' }, ids), { code: 'E_INVALID_ARGUMENT' });
	}
	for (const key of ['offset', 'limit', 'exactTotal', 'sort', 'aggregations', 'continuation']) {
		await assert.rejects(index.explain({ text: 'amber', [key]: 1 }, ['first']), { code: 'E_INVALID_ARGUMENT' });
	}
	await assert.rejects(index.explain({ text: 'amber' }, ['first'], { snapshot: 'bad' }), {
		code: 'E_INVALID_ARGUMENT',
	});
	assert.strictEqual((await index.explain({ text: 'amber' }, ['first'])).records[0].matched, true);
	await index.close();
	await assert.rejects(index.explain({ text: 'amber' }, ['first']), { code: 'E_CLOSED' });
});

for (const [name, request] of [
	[
		'a nested OR branch missing from every field',
		{ query: { operator: 'or', clauses: [{ text: 'amber' }, { text: 'absent' }] } },
	],
	['a nonmatching phrase field', { text: 'running shoe', mode: 'phrase' }],
]) {
	test(`explain handles ${name} without poisoning the handle`, async (context) => {
		const { index } = await fixture(context);
		const searched = await index.search(request);
		const result = await index.explain(
			request,
			searched.hits.map(({ id }) => id),
		);
		assert(result.records.every(({ matched }) => matched));
		assert.deepStrictEqual(
			result.records.map(({ score }) => score),
			searched.hits.map(({ score }) => score),
		);
	});
}

test('explain preserves request order across multiple segments and replacement snapshots', async (context) => {
	const { index, options } = await fixture(context);
	for (const id of ['segment-two', 'segment-three']) {
		await index.applyMutationBatch({ upserts: [{ id, fields: { title: 'amber amber' } }] });
		await index.commit();
		await index.reload();
	}
	const reader = await openNativeFullTextReader(options);
	try {
		assert(reader.telemetry().native.reader.segments >= 2n);
		const request = { text: 'amber', fields: ['title'] };
		const ids = ['segment-three', 'first', 'missing', 'segment-two', 'second'];
		const old = await reader.searchPage({ ...request, limit: 10 });
		const expected = new Map(old.hits.map(({ id, score }) => [id, score]));
		const explained = await reader.explain(request, ids, { snapshot: old.snapshot });
		assert.deepStrictEqual(
			explained.records.map(({ id }) => id),
			ids,
		);
		for (const record of explained.records) {
			assert.strictEqual(record.matched, expected.has(record.id));
			if (record.matched) assert.strictEqual(record.score, expected.get(record.id));
		}
		await index.applyMutationBatch({
			deletes: ['segment-two'],
			upserts: [{ id: 'first', fields: { title: 'quartz' } }],
		});
		await index.commit();
		await index.reload();
		assert.deepStrictEqual(await reader.explain(request, ids, { snapshot: old.snapshot }), explained);
		const current = await index.explain(request, ['first', 'segment-two']);
		assert(current.records.every(({ matched }) => !matched));
		await reader.reload();
		await assert.rejects(reader.explain(request, ids, { snapshot: old.snapshot }), { code: 'E_CURSOR_EXPIRED' });
	} finally {
		await reader.close();
	}
});

for (const outerBoost of [0, 1e-20]) {
	test(`explain preserves finite nested boost scores with outer factor ${outerBoost}`, async (context) => {
		const { index } = await fixture(context, { fields: [{ name: 'title', weight: 3e38 }, { name: 'body' }] });
		await index.applyMutationBatch({
			deletes: documents.map(({ id }) => id),
			upserts: Array.from({ length: 10 }, (_, i) => ({
				id: `overflow-${i}`,
				fields: { title: i === 0 ? 'amber' : 'quartz' },
			})),
		});
		await index.commit();
		await index.reload();
		const request = {
			query: { operator: 'boost', boost: outerBoost, clause: { operator: 'boost', boost: 2, clause: leaf('amber') } },
		};
		const searched = await index.search(request);
		assert(Number.isFinite(searched.hits[0].score));
		if (outerBoost === 0) assert.strictEqual(searched.hits[0].score, 0);
		else assert(searched.hits[0].score > 0);
		const explained = await index.explain(request, ['overflow-0']);
		assert.strictEqual(explained.records[0].score, searched.hits[0].score);
		const finite = (node) => {
			assert(Number.isFinite(node.value));
			for (const child of node.details ?? []) finite(child);
		};
		finite(explained.records[0].explanation);
	});
}

test('explain rejects non-object requests as invalid arguments', async (context) => {
	const { index } = await fixture(context);
	for (const request of [null, undefined, 1, 'amber', []]) {
		await assert.rejects(index.explain(request, ['first']), { code: 'E_INVALID_ARGUMENT' });
	}
});

test('explain rejects malformed options before native admission', async (context) => {
	const { index } = await fixture(context);
	for (const options of [null, 1, true, 'invalid', [], () => {}]) {
		await assert.rejects(index.explain({ text: 'amber' }, ['first'], options), { code: 'E_INVALID_ARGUMENT' });
	}
	assert.strictEqual((await index.explain({ text: 'amber' }, ['first'])).records[0].matched, true);
});

test('explain clamps raised structural policy before native admission', async (context) => {
	const { index } = await fixture(context, { limits: { maxQueryClauses: 4096 } });
	const request = { query: { operator: 'or', clauses: Array.from({ length: 257 }, () => leaf('amber')) } };
	assert((await index.search(request)).hits.length > 0);
	const before = index.telemetry().preparationRejections.search.total;
	await assert.rejects(index.explain(request, ['first']), {
		code: 'E_INVALID_ARGUMENT',
		message: /search structure exceeds 256 compiled clauses/,
	});
	assert.strictEqual(index.telemetry().preparationRejections.search.total, before + 1n);
	const admitted = { text: 'amber', fields: ['title'] };
	const searched = await index.search(admitted);
	const explained = await index.explain(
		admitted,
		searched.hits.map(({ id }) => id),
	);
	assert.deepStrictEqual(
		explained.records.map(({ id, score }) => ({ id, score })),
		searched.hits,
	);
	assert((await index.search(request)).hits.length > 0);
});
