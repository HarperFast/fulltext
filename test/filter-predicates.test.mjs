import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

test('presence and negation filters preserve missing, false, zero and multivalue semantics', async (context) => {
	const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-filter-predicates-'));
	const index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'predicates',
		generation: 'one',
		analyzer: 'english@2',
		fields: [{ name: 'title' }],
		filterFields: [
			{ name: 'category', type: 'string' },
			{ name: 'price', type: 'number' },
			{ name: 'active', type: 'boolean' },
		],
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15000000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8388608,
			maxBatchBytes: 8388608,
		},
	});
	context.after(async () => {
		await index.close({ mode: 'rollback' });
		rmSync(directory, { recursive: true, force: true });
	});
	await index.applyMutationBatch({
		upserts: [
			{ id: 'empty', fields: { title: 'catalog' }, filters: { category: '', price: 0, active: false } },
			{
				id: 'multi',
				fields: { title: 'catalog' },
				filters: { category: ['outdoor', 'sale'], price: [10, 20], active: true },
			},
			{ id: 'absent', fields: { title: 'catalog' } },
			{ id: 'array', fields: { title: 'catalog' }, filters: { category: [], price: [], active: [] } },
		],
	});
	await index.commit();
	await index.reload();
	const baseline = await index.search({ text: 'catalog' });
	const scores = new Map(baseline.hits.map(({ id, score }) => [id, score]));
	const ids = async (filter, extra = {}) => {
		const result = await index.search({ text: 'catalog', filter, ...extra });
		for (const { id, score } of result.hits) assert.strictEqual(score, scores.get(id));
		return result.hits.map(({ id }) => id).sort();
	};
	for (const field of ['category', 'price', 'active']) {
		assert.deepStrictEqual(await ids({ field, comparator: 'exists' }), ['empty', 'multi']);
		assert.deepStrictEqual(await ids({ field, comparator: 'missing' }), ['absent', 'array']);
	}
	const notSale = { operator: 'not', clause: { field: 'category', comparator: 'equals', value: 'sale' } };
	assert.deepStrictEqual(await ids(notSale), ['absent', 'array', 'empty']);
	assert.deepStrictEqual(await ids(notSale, { candidateIds: ['empty', 'multi'] }), ['empty']);
	assert.deepStrictEqual(
		await ids({ operator: 'and', clauses: [notSale, { field: 'active', comparator: 'exists' }] }),
		['empty'],
	);
	assert.deepStrictEqual(await ids({ operator: 'not', clause: { operator: 'not', clause: notSale } }), [
		'absent',
		'array',
		'empty',
	]);
	for (const filter of [
		{ field: 'category', comparator: 'exists', value: 'sale' },
		{ field: 'unknown', comparator: 'missing' },
		{ operator: 'not' },
		{
			operator: 'and',
			clauses: Array.from({ length: 2 }, () => ({
				operator: 'or',
				clauses: Array.from({ length: 129 }, () => notSale),
			})),
		},
	])
		await assert.rejects(index.search({ text: 'catalog', filter }), (error) => error.code === 'E_INVALID_ARGUMENT');
	let deep = notSale;
	for (let level = 0; level < 9; level++) deep = { operator: 'not', clause: deep };
	await assert.rejects(index.search({ text: 'catalog', filter: deep }), (error) => error.code === 'E_INVALID_ARGUMENT');
});
