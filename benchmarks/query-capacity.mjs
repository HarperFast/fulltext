import assert from 'node:assert';
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { openNativeFullTextIndex, openNativeFullTextReader, runtimeInfo } from '../dist/native.js';
import { closeIndex } from './metadata-cleanup.mjs';

const documents = 1_000;
const rounds = 12;
const gates = { maxP99Milliseconds: 5_000, maxRssGrowthBytes: 256 << 20 };
const text = Array.from({ length: 1024 }, (_, i) => `word${i}`).join(' ');
const fuzzyText = 'alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa';
const root = await mkdtemp(path.join(tmpdir(), 'fulltext-query-capacity-'));
const config = {
	path: path.join(root, 'index'),
	indexId: 'query-capacity',
	generation: 'one',
	analyzer: 'english@2',
	positions: true,
	surfaceTerms: true,
	fields: Array.from({ length: 16 }, (_, i) => ({ name: `field${i}` })),
	limits: {
		indexingThreads: 1,
		searchThreads: 4,
		writerMemoryBytes: 30_000_000,
		maxQueuedCommands: 128,
		maxQueuedBytes: 8 << 20,
		maxBatchBytes: 1 << 20,
		maxQueryTerms: 1024,
		maxQueryClauses: 4096,
	},
};
const expected = ['id0000', 'id0001', 'id0002'];
const cases = [
	{ name: 'small-exact-total', request: { text: 'word0', fields: ['field0'] } },
	{ name: 'max-terms', request: { text, fields: ['field0'] } },
	{ name: 'max-compiled-clauses', request: { text, fields: ['field0', 'field1', 'field2', 'field3'] } },
	{ name: 'max-phrase-positions', request: { text, mode: 'phrase' } },
	{ name: 'max-fuzzy-automata', request: { text: fuzzyText, mode: 'fuzzy' } },
];
let index;
let defaultReader;
let primaryError;
let report;
try {
	index = await openNativeFullTextIndex(config);
	await index.applyMutationBatch({
		upserts: Array.from({ length: documents }, (_, i) => ({
			id: `id${String(i).padStart(4, '0')}`,
			fields: { field0: `${text} ${fuzzyText}` },
		})),
	});
	await index.publish('qualification:1');
	defaultReader = await openNativeFullTextReader({
		...config,
		limits: {
			...config.limits,
			maxQueryTerms: 64,
			maxQueryClauses: 256,
		},
	});
	const baselineQuery = {
		text: text.split(' ').slice(0, 64).join(' '),
		fields: ['field0'],
		limit: 3,
		exactTotal: true,
	};
	assert.deepStrictEqual(await index.search(baselineQuery), await defaultReader.search(baselineQuery));
	const rssBaseline = process.memoryUsage().rss;
	let peakRss = rssBaseline;
	const rows = [];
	const run = async (request, budget) => {
		const result = await index.search({ ...request, limit: 3, exactTotal: true }, budget);
		assert.strictEqual(result.total, documents);
		assert.strictEqual(result.totalRelation, 'exact');
		assert.deepStrictEqual(
			result.hits.map((hit) => hit.id),
			expected,
		);
		assert(result.hits.every((hit) => Number.isFinite(hit.score)));
	};
	for (const entry of cases) {
		for (const concurrency of [1, 4, 8]) {
			await run(entry.request);
			const latencies = [];
			const cpuStart = process.cpuUsage();
			const started = performance.now();
			for (let round = 0; round < rounds; round++) {
				await Promise.all(
					Array.from({ length: concurrency }, async () => {
						const start = performance.now();
						await run(entry.request);
						latencies.push(performance.now() - start);
					}),
				);
				await run(cases[0].request);
				peakRss = Math.max(peakRss, process.memoryUsage().rss);
			}
			const elapsedMilliseconds = performance.now() - started;
			const cpu = process.cpuUsage(cpuStart);
			latencies.sort((a, b) => a - b);
			const percentile = (p) => latencies[Math.min(latencies.length - 1, Math.ceil(latencies.length * p) - 1)];
			assert(percentile(0.99) <= gates.maxP99Milliseconds);
			rows.push({
				case: entry.name,
				concurrency,
				requests: latencies.length,
				elapsedMilliseconds,
				p50: percentile(0.5),
				p95: percentile(0.95),
				p99: percentile(0.99),
				cpuMicroseconds: cpu.user + cpu.system,
			});
		}
	}
	let timeouts = 0;
	for (let i = 0; i < 24; i++) {
		try {
			await run(cases[2].request, { remainingBudgetMilliseconds: 1 });
		} catch (error) {
			assert.strictEqual(error.code, 'E_TIMEOUT');
			timeouts++;
		}
	}
	await Promise.all(Array.from({ length: 8 }, () => run(cases[2].request)));
	assert.strictEqual(index.status().searchQueuedCommands, 0n);
	assert.strictEqual(index.status().searchQueuedBytes, 0n);
	assert(peakRss - rssBaseline <= gates.maxRssGrowthBytes);
	report = {
		schemaVersion: 1,
		environment: {
			platform: process.platform,
			architecture: process.arch,
			node: process.version,
			observedAt: new Date().toISOString(),
		},
		runtime: await runtimeInfo(),
		documents,
		rounds,
		fields: 16,
		policy: index.queryLimits,
		gates,
		rssBaseline,
		peakRss,
		timeouts,
		rows,
		note: 'Local optimized synthetic qualification; sampled RSS is not native accounting or sustained-memory proof. CPU is process-wide. All cases request exactTotal and use expensive admission. Small-query controls are between rounds, not a fairness measurement.',
	};
} catch (error) {
	primaryError = error;
}
const cleanupErrors = [];
for (const handle of [defaultReader, index]) {
	try {
		await closeIndex(handle, { mode: 'rollback' });
	} catch (error) {
		cleanupErrors.push(error);
	}
}
if (cleanupErrors.length || primaryError) {
	throw new AggregateError(
		[...(primaryError ? [primaryError] : []), ...cleanupErrors],
		`Qualification failed; artifacts retained at ${root}`,
		{ cause: primaryError },
	);
}
const addonPaths = Object.keys(createRequire(import.meta.url).cache).filter(
	(file) => path.basename(file).startsWith('fulltext.') && file.endsWith('.node'),
);
assert.strictEqual(addonPaths.length, 1, 'qualification must identify one loaded native addon');
report.fingerprints = await Promise.all(
	[fileURLToPath(import.meta.url), ...addonPaths].map(async (file) => {
		const hash = createHash('sha256');
		for await (const chunk of createReadStream(file)) hash.update(chunk);
		return { file: path.basename(file), sha256: hash.digest('hex') };
	}),
);
await rm(root, { recursive: true });
console.log(JSON.stringify(report, null, 2));
