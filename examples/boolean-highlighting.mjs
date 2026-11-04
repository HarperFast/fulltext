import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-boolean-highlighting-'));
let index;
try {
	index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'products',
		generation: 'v1',
		fields: [{ name: 'title', weight: 3 }, { name: 'description' }],
		analyzer: 'english@2',
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
	const records = [
		{ id: 'shoe', fields: { title: 'Waterproof trail shoe', description: 'Lightweight grip' } },
		{ id: 'boot', fields: { title: 'Waterproof boot', description: 'Insulated lining' } },
	];
	await index.applyMutationBatch({ upserts: records });
	await index.commit();
	await index.reload();
	const request = {
		query: {
			operator: 'optional',
			required: {
				operator: 'and',
				clauses: [{ text: 'waterproof' }, { operator: 'not', clause: { text: 'insulated' } }],
			},
			optional: [{ operator: 'boost', boost: 3, clause: { text: 'trail', fields: ['title'] } }],
		},
	};
	const searched = await index.search(request);
	const traced = await index.traceMatches(request, records, { snippets: true });
	assert.strictEqual(traced.complete, true);
	assert.deepStrictEqual(
		traced.records.map(({ id }) => id),
		searched.hits.map(({ id }) => id),
	);
	assert.deepStrictEqual(traced.records[0].values[0].spans, [
		{ start: 0, end: 10 },
		{ start: 11, end: 16 },
	]);
	console.log({ hits: searched.hits, currentSourceHighlights: traced.records });
} finally {
	await index?.close({ mode: 'rollback' });
	await rm(directory, { recursive: true, force: true });
}
