import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { performance } from 'node:perf_hooks';
import { cpus } from 'node:os';

const root = path.resolve(process.argv[2] ?? '.');
const label = process.argv[3] ?? 'current';
const { openNativeFullTextIndex } = await import(pathToFileURL(path.join(root, 'dist/native.js')));
const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-trace-matching-'));
let index;
try {
	index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'trace-benchmark',
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
	if (process.argv.includes('--boolean')) {
		for (const words of [1000, 10000, 100000]) {
			for (const matched of [false, true]) {
				const records = [
					{ id: 'one', fields: { title: (matched ? 'amber ' : 'zzzzzzzz ') + 'zzzzzzzz '.repeat(words - 1) } },
				];
				const leaf = { text: 'amber', fields: ['title'], mode: 'any' };
				for (const count of [0, 1, 8, 64]) {
					const request =
						count === 0
							? leaf
							: {
									query:
										count === 1
											? { operator: 'boost', boost: 2, clause: leaf }
											: { operator: 'or', clauses: Array.from({ length: count }, () => leaf) },
								};
					const samples = [];
					let complete;
					for (let i = 0; i < 25; i++) {
						const start = performance.now();
						const result = await index.traceMatches(request, records);
						assert.strictEqual(result.records.length, Number(matched));
						complete = result.complete;
						assert.strictEqual(complete, !(matched && count === 64 && words === 100000));
						if (matched) assert.deepStrictEqual(result.records[0].values[0].spans, [{ start: 0, end: 5 }]);
						if (i >= 5) samples.push(performance.now() - start);
					}
					samples.sort((a, b) => a - b);
					console.log(
						JSON.stringify({
							label,
							workload: 'boolean-source-tracing',
							path: count ? 'expression' : 'legacy',
							leaves: count || 1,
							words,
							matched,
							complete,
							iterations: 20,
							warmupIterations: 5,
							percentileMethod: 'nearest-rank',
							p50: samples[9],
							p95: samples[18],
							node: process.version,
							platform: process.platform,
							arch: process.arch,
							cpu: cpus()[0]?.model,
						}),
					);
				}
			}
		}
	} else
		for (const mode of ['fuzzy', 'fuzzy-prefix']) {
			for (const [words, queryTerms, inflected, matched] of [
				[1, 1, false, true],
				[1000, 1, false, true],
				[10000, 16, false, false],
				[20, 16, true, false],
			]) {
				const text =
					queryTerms === 1
						? mode === 'fuzzy'
							? 'waterprof'
							: 'watrepro'
						: Array.from(
								{ length: 16 },
								(_, i) => 'catalog' + String.fromCharCode(97 + i) + (inflected ? 'ing' : ''),
							).join(' ');
				const request = { text, mode };
				const source = matched ? 'waterproof ' : inflected ? 'zzazzzzzing ' : 'zzzzzzzz ';
				const records = [{ id: 'one', fields: { title: source.repeat(words) } }];
				const check = (result) => {
					assert.strictEqual(result.complete, true);
					assert.strictEqual(result.records.length, Number(matched));
				};
				const firstStarted = performance.now();
				const first = await index.traceMatches(request, records);
				const firstCallMilliseconds = performance.now() - firstStarted;
				check(first);
				for (let i = 0; i < 50; i++) await index.traceMatches(request, records);
				const samples = [];
				for (let i = 0; i < 200; i++) {
					const started = performance.now();
					const result = await index.traceMatches(request, records);
					samples.push(performance.now() - started);
					check(result);
				}
				samples.sort((a, b) => a - b);
				console.log(
					JSON.stringify({
						label,
						mode,
						words,
						queryTerms,
						inflected,
						firstCallMilliseconds,
						iterations: 200,
						warmupIterations: 50,
						percentileMethod: 'nearest-rank',
						p50: samples[99],
						p95: samples[189],
						p99: samples[197],
						node: process.version,
						platform: process.platform,
						arch: process.arch,
					}),
				);
			}
		}
} finally {
	try {
		if (index) await index.close({ mode: 'rollback' });
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
}
