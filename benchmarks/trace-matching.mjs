import { pathToFileURL } from 'node:url';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import assert from 'node:assert';
import { performance } from 'node:perf_hooks';
const root = path.resolve(process.argv[2] ?? '.'),
	label = process.argv[3] ?? 'current';
const { openNativeFullTextIndex } = await import(pathToFileURL(path.join(root, 'dist/native.js')));
const dir = await mkdtemp(path.join(tmpdir(), 'trace-reuse-'));
const index = await openNativeFullTextIndex({
	path: dir,
	indexId: 'probe',
	generation: 'one',
	analyzer: 'english@2',
	surfaceTerms: true,
	fields: [{ name: 'title' }],
	limits: {
		indexingThreads: 1,
		searchThreads: 2,
		writerMemoryBytes: 15000000,
		maxQueuedCommands: 32,
		maxQueuedBytes: 8388608,
		maxBatchBytes: 8388608,
	},
});
try {
	for (const mode of ['fuzzy', 'fuzzy-prefix'])
		for (const [words, queryTerms, matched] of [
			[1, 1, true],
			[1000, 1, true],
			[10000, 16, false],
		]) {
			const text =
				queryTerms === 1
					? mode === 'fuzzy'
						? 'waterprof'
						: 'waterpro'
					: Array.from({ length: 16 }, (_, i) => 'catalog' + String.fromCharCode(97 + i)).join(' ');
			const req = { text, mode };
			const records = [{ id: 'one', fields: { title: (matched ? 'waterproof ' : 'zzzzzzzz ').repeat(words) } }];
			for (let i = 0; i < 50; i++) await index.traceMatches(req, records);
			const times = [];
			for (let i = 0; i < 200; i++) {
				const start = performance.now();
				const r = await index.traceMatches(req, records);
				times.push(performance.now() - start);
				assert.equal(r.complete, true);
				assert.equal(r.records.length, Number(matched));
			}
			times.sort((a, b) => a - b);
			console.log(
				JSON.stringify({
					label,
					node: process.version,
					platform: process.platform,
					arch: process.arch,
					mode,
					words,
					queryTerms,
					iterations: 200,
					warmupIterations: 50,
					percentileMethod: 'nearest-rank',
					p50: times[99],
					p95: times[189],
					p99: times[197],
				}),
			);
		}
} finally {
	try {
		await index.close({ mode: 'rollback' });
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
}
