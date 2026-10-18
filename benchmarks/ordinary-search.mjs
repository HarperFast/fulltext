import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { performance } from 'node:perf_hooks';

const root = path.resolve(process.argv[2] ?? '.');
const label = process.argv[3] ?? 'current';
const { openNativeFullTextIndex } = await import(pathToFileURL(path.join(root, 'dist/native.js')));
const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-ordinary-search-'));
let index;
try {
	index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'ordinary-benchmark',
		generation: 'one',
		analyzer: 'english@2',
		fields: [{ name: 'title', weight: 1 }],
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15000000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8388608,
			maxBatchBytes: 8388608,
		},
	});
	await index.applyMutationBatch({
		upserts: Array.from({ length: 10000 }, (_, i) => ({
			id: String(i).padStart(5, '0'),
			fields: { title: i % 10 === 0 ? 'widget scarce' : 'widget common' },
		})),
	});
	await index.publish('one');
	for (const limit of [20, 1000]) {
		for (const [text, total] of [
			['absent', 0],
			['scarce', 1000],
			['widget', 10000],
		]) {
			const request = { text, limit, exactTotal: true };
			const check = await index.search(request);
			assert.equal(check.total, total);
			assert.equal(check.hits.length, Math.min(limit, total));
			for (let i = 0; i < 200; i++) await index.search(request);
			const started = performance.now();
			for (let i = 0; i < 1000; i++) await index.search(request);
			console.log(
				JSON.stringify({
					label,
					text,
					limit,
					iterations: 1000,
					milliseconds: performance.now() - started,
					node: process.version,
				}),
			);
		}
	}
} finally {
	if (index) await index.close({ mode: 'rollback' });
	await rm(directory, { recursive: true, force: true });
}
