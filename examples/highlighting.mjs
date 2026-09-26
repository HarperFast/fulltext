import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';

import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const root = await mkdtemp(path.join(tmpdir(), 'fulltext-highlighting-'));
let index;
try {
	index = await openNativeFullTextIndex({
		path: path.join(root, 'products'),
		indexId: 'products',
		generation: 'v1',
		fields: [{ name: 'title' }],
		analyzer: 'english@2',
		positions: true,
		surfaceTerms: true,
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 16,
			maxQueuedBytes: 1024 * 1024,
			maxBatchBytes: 1024 * 1024,
		},
	});
	const record = { id: 'shoe-1', fields: { title: 'Waterproof trail running shoe' } };
	await index.applyMutationBatch({ upserts: [record] });
	await index.commit();
	await index.reload();
	const search = { text: 'trail running', mode: 'phrase' };
	const result = await index.search(search);
	const traced = await index.traceMatches(search, [record], {
		snippets: true,
		fragmentLength: 80,
		maxFragmentsPerValue: 2,
	});
	console.log({ result, traced });
} finally {
	await index?.close({ mode: 'rollback' });
	await rm(root, { recursive: true, force: true });
}
