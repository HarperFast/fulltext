import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';
import { loadAddon } from '../dist/load-addon.js';

const leaf = (text) => ({ text, fields: ['title'] });
const documents = [
	{ id: 'first', fields: { title: 'amber stone', body: 'trail running shoe' } },
	{ id: 'second', fields: { title: 'amber quartz', body: 'waterproof running shoe' } },
	{ id: 'third', fields: { title: 'quartz stone', body: 'waterproof boot' } },
];
async function fixture(context, fields = [{ name: 'title', weight: 3 }, { name: 'body' }]) {
	assert.strictEqual(loadAddon().__testDelayNextExpensiveSearch, undefined);
	const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-explain-release-'));
	const index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'explain-release',
		generation: 'one',
		analyzer: 'english@2',
		fields,
		surfaceTerms: true,
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 4 * 1024 * 1024,
			maxBatchBytes: 1024 * 1024,
		},
	});
	context.after(async () => {
		await index.close({ mode: 'rollback' });
		await rm(directory, { force: true, recursive: true });
	});
	return index;
}
for (const [name, request] of [
	['fuzzy', { text: 'ambr', mode: 'fuzzy' }],
	[
		'optional',
		{
			query: {
				operator: 'optional',
				required: leaf('amber'),
				optional: [{ operator: 'boost', boost: 2.5, clause: leaf('quartz') }],
			},
		},
	],
	['missing OR branch', { query: { operator: 'or', clauses: [{ text: 'amber' }, { text: 'absent' }] } }],
]) {
	test('production explanations preserve scores and pending writes for ' + name, async (context) => {
		const index = await fixture(context);
		await index.applyMutationBatch({ upserts: documents });
		await index.commit();
		await index.reload();
		const searched = await index.search(request);
		assert(searched.hits.length > 0);
		const pending = { id: 'pending', fields: { title: 'retained' } };
		await index.applyMutationBatch({ upserts: [pending] });
		const result = await index.explain(
			request,
			searched.hits.map(({ id }) => id),
		);
		assert.deepStrictEqual(
			result.records.map(({ id, matched, score }) => ({ id, matched, score })),
			searched.hits.map(({ id, score }) => ({ id, matched: true, score })),
		);
		const validate = (node) => {
			assert(Number.isFinite(node.value));
			assert.strictEqual(typeof node.description, 'string');
			for (const child of node.details ?? []) validate(child);
		};
		for (const record of result.records) validate(record.explanation);
		await index.commit();
		await index.reload();
		assert.deepStrictEqual(
			(await index.search({ text: 'retained' })).hits.map(({ id }) => id),
			['pending'],
		);
	});
}
for (const outerBoost of [0, 1e-20]) {
	test(`production explanations preserve a finite tree with outer boost ${outerBoost}`, async (context) => {
		const index = await fixture(context, [{ name: 'title', weight: 3e38 }]);
		await index.applyMutationBatch({
			upserts: Array.from({ length: 10 }, (_, i) => ({
				id: 'overflow-' + i,
				fields: { title: i === 0 ? 'amber' : 'quartz' },
			})),
		});
		await index.commit();
		await index.reload();
		const request = {
			query: { operator: 'boost', boost: outerBoost, clause: { operator: 'boost', boost: 2, clause: leaf('amber') } },
		};
		const score = (await index.search(request)).hits[0].score;
		assert(Number.isFinite(score));
		if (outerBoost === 0) assert.strictEqual(score, 0);
		else assert(score > 0);
		let result;
		let explainError;
		try {
			result = await index.explain(request, ['overflow-0']);
		} catch (error) {
			explainError = error;
		}
		assert.strictEqual((await index.search(request)).hits[0].score, score);
		if (explainError) throw explainError;
		assert.strictEqual(result.records[0].score, score);
		const finite = (node) => {
			assert(Number.isFinite(node.value));
			for (const child of node.details ?? []) finite(child);
		};
		finite(result.records[0].explanation);
	});
}
