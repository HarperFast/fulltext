import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-score-explanation-'));
let index;
try {
	index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'diagnostics',
		generation: 'one',
		analyzer: 'english@2',
		fields: [{ name: 'title', weight: 3 }],
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 4 * 1024 * 1024,
			maxBatchBytes: 1024 * 1024,
		},
	});
	await index.applyMutationBatch({
		upserts: [
			{ id: 'first', fields: { title: 'amber quartz' } },
			{ id: 'second', fields: { title: 'amber amber quartz' } },
		],
	});
	await index.commit();
	await index.reload();
	const request = { text: 'amber', fields: ['title'] };
	const page = await index.searchPage({ ...request, limit: 10 });
	const result = await index.explain(request, ['second', 'missing', 'first'], { snapshot: page.snapshot });
	assert.strictEqual(result.snapshot, page.snapshot);
	assert.strictEqual(result.format, 'tantivy-0.27');
	assert.deepStrictEqual(
		result.records.map(({ id }) => id),
		['second', 'missing', 'first'],
	);
	for (const record of result.records) {
		const hit = page.hits.find(({ id }) => id === record.id);
		assert.strictEqual(record.matched, Boolean(hit));
		if (hit) {
			assert.strictEqual(record.score, hit.score);
			assert(Number.isFinite(record.explanation.value));
		}
	}
	console.log('Snapshot-bound BM25 explanation parity:', result.records);
} finally {
	if (index) {
		const closed = await index.close({ mode: 'rollback' });
		if (closed?.cleanupError) throw closed.cleanupError;
	}
	await rm(directory, { recursive: true, force: true });
}
