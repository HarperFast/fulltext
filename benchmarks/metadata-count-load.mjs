import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { pathToFileURL } from 'node:url';

const option = (name, fallback) => {
	const position = process.argv.indexOf(name);
	return position < 0 ? fallback : process.argv[position + 1];
};
const positive = (name, fallback) => {
	const value = Number(option(name, fallback));
	assert(Number.isSafeInteger(value) && value > 0, `${name} requires a positive integer`);
	return value;
};
const documents = positive('--documents', 100_000);
const queries = positive('--queries', 120);
const budget = positive('--budget-ms', 1);
const concurrency = String(option('--concurrency', '1,4,8')).split(',').map(Number);
assert(concurrency.every((value) => Number.isSafeInteger(value) && value > 0));
const modulePath = option('--module', path.resolve('dist/native.js'));
const { openNativeFullTextIndex, runtimeInfo } = await import(pathToFileURL(path.resolve(modulePath)).href);
const scratch = option('--scratch');
assert(scratch, '--scratch requires an existing directory for temporary index files');
const directory = await mkdtemp(path.join(scratch, 'fulltext-count-load-'));
const config = {
	path: directory,
	indexId: 'count-load',
	generation: 'one',
	analyzer: 'english@2',
	fields: [{ name: 'title' }, { name: 'description' }],
	filterFields: [{ name: 'active', type: 'boolean' }],
	limits: {
		indexingThreads: 1,
		searchThreads: 2,
		writerMemoryBytes: 15_000_000,
		maxQueuedCommands: 32,
		maxQueuedBytes: 4 * 1024 * 1024,
		maxBatchBytes: 1024 * 1024,
	},
};
let index;
const report = [];
try {
	index = await openNativeFullTextIndex(config);
	for (let start = 0; start < documents; start += 300) {
		const upserts = Array.from({ length: Math.min(300, documents - start) }, (_, offset) => {
			const ordinal = start + offset;
			return {
				id: `product-${String(ordinal).padStart(9, '0')}`,
				fields: {
					title: `Trail shoe ${'detail '.repeat(ordinal < 64 ? ordinal : 100)}`,
					description: `Outdoor running equipment hiking waterproof ${ordinal % 7}`,
				},
				filters: { active: ordinal % 3 !== 0 },
			};
		});
		const result = await index.applyMutationBatch({ upserts });
		assert.strictEqual(result.processed, upserts.length);
		assert.deepStrictEqual(result.rejected, []);
	}
	await index.commit();
	await index.reload();
	const cases = [
		{ name: 'realistic-and', text: 'trail shoe', mode: 'all', expected: documents },
		{
			name: 'dense-or-filtered',
			text: 'trail shoe outdoor running equipment hiking waterproof',
			mode: 'any',
			filter: { field: 'active', comparator: 'equals', value: true },
			expected: documents - Math.ceil(documents / 3),
		},
	];
	for (const { name, expected, ...expression } of cases) {
		const request = { ...expression, limit: 10, exactTotal: true };
		const boundary = await index.search({ ...request, limit: 11 });
		assert.strictEqual(boundary.total, expected);
		assert.strictEqual(boundary.hits.length, 11);
		assert.notStrictEqual(boundary.hits[9].score, boundary.hits[10].score);
		for (const parallel of concurrency) {
			await measure(name, request, expected, parallel, false);
			await measure(name, request, expected, parallel, true);
		}
	}
	console.log(
		JSON.stringify(
			{
				benchmark: 'metadata-count-load',
				revision: option('--revision', 'working-tree'),
				runtime: { node: process.version, platform: process.platform, arch: process.arch, native: await runtimeInfo() },
				config: { ...config, path: '<temporary-index>', documents, queries, concurrency, budgetMilliseconds: budget },
				method:
					'Closed-loop callers, three warmups; exact totals and distinct boundary scores asserted; filtered OR avoids stable-any path. CPU covers the Node process and native threads; RSS sampled every 5ms. Budget includes queue wait. Successful budgeted requests must also have exact totals. No production SLO is implied.',
				measurements: report,
			},
			null,
			2,
		),
	);
} finally {
	await index?.close({ mode: 'rollback' });
	await rm(directory, { recursive: true, force: true });
}

async function measure(name, request, expected, parallel, timed) {
	for (let warmup = 0; warmup < 3; warmup++) assert.strictEqual((await index.search(request)).total, expected);
	const latencies = [];
	const timeoutLatencies = [];
	const started = performance.now();
	const cpu = process.cpuUsage();
	const before = process.memoryUsage();
	let peakRss = before.rss;
	const sampler = setInterval(() => {
		peakRss = Math.max(peakRss, process.memoryUsage().rss);
	}, 5);
	let next = 0;
	try {
		await Promise.all(
			Array.from({ length: parallel }, async () => {
				while (next++ < queries) {
					const begin = performance.now();
					try {
						const result = await index.search(request, timed ? { remainingBudgetMilliseconds: budget } : undefined);
						assert.strictEqual(result.total, expected);
						assert.strictEqual(result.hits.length, 10);
						latencies.push(performance.now() - begin);
					} catch (error) {
						if (!timed || error.code !== 'E_TIMEOUT') throw error;
						timeoutLatencies.push(performance.now() - begin);
					}
				}
			}),
		);
	} finally {
		clearInterval(sampler);
	}
	const elapsedMilliseconds = performance.now() - started;
	const usedCpu = process.cpuUsage(cpu);
	const after = process.memoryUsage();
	peakRss = Math.max(peakRss, after.rss);
	const recoveryStarted = performance.now();
	await Promise.all(
		Array.from({ length: parallel }, async () => assert.strictEqual((await index.search(request)).total, expected)),
	);
	const recoveryMilliseconds = performance.now() - recoveryStarted;
	const recoveredStatus = index.status();
	assert.strictEqual(recoveredStatus.state, 'open');
	assert.strictEqual(recoveredStatus.searchQueuedCommands, 0n);
	assert.strictEqual(recoveredStatus.searchQueuedBytes, 0n);
	assert.strictEqual(latencies.length + timeoutLatencies.length, queries);
	const summary = (values) => {
		values.sort((a, b) => a - b);
		const percentile = (fraction) => (values.length ? values[Math.ceil(values.length * fraction) - 1] : null);
		return {
			count: values.length,
			p50Milliseconds: percentile(0.5),
			p95Milliseconds: percentile(0.95),
			p99Milliseconds: percentile(0.99),
		};
	};
	report.push({
		name,
		concurrency: parallel,
		budgetMilliseconds: timed ? budget : null,
		success: summary(latencies),
		timeouts: summary(timeoutLatencies),
		elapsedMilliseconds,
		attemptsPerSecond: (queries * 1000) / elapsedMilliseconds,
		successesPerSecond: (latencies.length * 1000) / elapsedMilliseconds,
		cpuMilliseconds: (usedCpu.user + usedCpu.system) / 1000,
		rssBeforeBytes: before.rss,
		rssAfterBytes: after.rss,
		peakRssBytes: peakRss,
		heapUsedDeltaBytes: after.heapUsed - before.heapUsed,
		recoveryMilliseconds,
	});
}
