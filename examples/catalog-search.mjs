import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const catalog = JSON.parse(await readFile(new URL('./data/catalog.json', import.meta.url), 'utf8'));
const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-catalog-'));
let index;
try {
	index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'catalog',
		generation: 'v1',
		fields: catalog.fields,
		analyzer: 'english@2',
		positions: true,
		surfaceTerms: true,
		synonyms: [{ source: 'tv', replacements: ['television'] }],
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 4 * 1024 * 1024,
			maxBatchBytes: 1024 * 1024,
		},
	});
	await index.applyMutationBatch({ upserts: catalog.documents });
	await index.commit();
	await index.reload();

	for (const request of [
		{ text: 'trail shoes', mode: 'all' },
		{ text: 'waterproof', fields: ['title'] },
		{
			query: {
				operator: 'and',
				clauses: [
					{ text: 'waterproof trail', mode: 'all' },
					{ operator: 'not', clause: { text: 'used' } },
				],
			},
		},
		{ text: 'cafe resume', mode: 'all' },
		{ text: 'television', fields: ['title'] },
	]) {
		const result = await index.search({ ...request, exactTotal: true, limit: 10 });
		console.log({ request, total: result.total, hits: result.hits });
	}

	const request = { text: 'trail shoes', mode: 'all' };
	const { hits } = await index.search({ ...request, limit: 1 });
	const currentRecord = catalog.documents.find(({ id }) => id === hits[0]?.id);
	if (currentRecord) {
		const traced = await index.traceMatches(request, [{ id: currentRecord.id, fields: currentRecord.fields }], {
			snippets: true,
		});
		console.log(JSON.stringify(traced, null, 2));
	}
} finally {
	await index?.close({ mode: 'rollback' });
	await rm(directory, { recursive: true, force: true });
}
