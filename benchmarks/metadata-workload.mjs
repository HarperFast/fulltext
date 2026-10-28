import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath, pathToFileURL } from 'node:url';

const argument = (name, fallback) => {
	const at = process.argv.indexOf(name);
	return at < 0 ? fallback : process.argv[at + 1];
};
const number = (name, fallback) => {
	const value = Number(argument(name, fallback));
	assert(Number.isFinite(value) && value > 0, `${name} must be positive`);
	return value;
};
const integer = (name, fallback) => {
	const value = number(name, fallback);
	assert(Number.isSafeInteger(value), `${name} must be an integer`);
	return value;
};
const scratch = argument('--scratch');
assert(scratch, '--scratch requires an existing scratch directory');
const documents = integer('--documents', 50_000);
const rounds = integer('--rounds', 10);
const queries = integer('--queries', 12);
const duration = number('--duration', 0.001) * 1000;
const durationMode = process.argv.includes('--duration');
const concurrency = argument('--concurrency', '1,4,8').split(',').map(Number);
assert(concurrency.every((n) => Number.isSafeInteger(n) && n > 0));
const features = argument('--features', 'expanded');
assert(['flat', 'expanded'].includes(features));
const gates = {
	maxP95Milliseconds: number('--max-p95-ms', 250),
	maxP99Milliseconds: number('--max-p99-ms', 5000),
	minSuccessesPerSecond: number('--min-throughput', 10),
	maxPeakRssGrowthBytes: number('--max-rss-growth-mib', 256) * 1024 * 1024,
	maxTailRoundRssRangeBytes: number('--max-tail-rss-range-mib', 32) * 1024 * 1024,
	compatiblePairedAcceptance: {
		candidateP95AtMostBaselineMultiple: 1.2,
		candidateThroughputAtLeastBaselineMultiple: 0.8,
		method:
			'external comparison of three alternating genuine-baseline/candidate runs, per-case/concurrency pooled medians; no old comparison for expanded cases',
	},
	unexpectedErrors: 0,
	sourceOracle: 'exact totals, selected order, all returned aggregation trees',
	queues: 'zero queued commands and bytes after each round',
	resourcePressure: 'typed E_RESOURCE_LIMIT or E_RESULT_TOO_LARGE, followed by successful recovery',
};
console.error(
	JSON.stringify({
		benchmark: 'metadata-workload',
		phase: 'premeasurement-gates',
		gates,
		run: {
			documents,
			rounds: durationMode ? null : rounds,
			durationMillisecondsPerCaseConcurrency: durationMode ? duration : null,
			queriesPerRound: queries,
			concurrency,
			features,
		},
	}),
);
const modulePath = argument('--module', fileURLToPath(new URL('../dist/native.js', import.meta.url)));
const { openNativeFullTextIndex, runtimeInfo } = await import(pathToFileURL(path.resolve(modulePath)).href);
const directory = await mkdtemp(path.join(scratch, 'fulltext-metadata-workload-'));
const limits = {
	indexingThreads: 1,
	searchThreads: 2,
	writerMemoryBytes: 15_000_000,
	maxQueuedCommands: integer('--max-queued-commands', 32),
	maxQueuedBytes: integer('--max-queued-bytes', 4 << 20),
	maxBatchBytes: 1 << 20,
};
const budget = integer('--budget-ms', 5000);
const fields = [
	{ name: 'brand', type: 'string' },
	{ name: 'price', type: 'number' },
	{ name: 'sku', type: 'number' },
	{ name: 'active', type: 'boolean' },
];
const options = (indexPath) => ({
	path: indexPath,
	indexId: 'synthetic-catalog',
	generation: 'one',
	fields: [{ name: 'title' }, { name: 'description' }],
	filterFields: fields,
	analyzer: 'english@2',
	limits,
});
const rows = Array.from({ length: documents }, (_, n) => ({
	id: `product-${String(n).padStart(9, '0')}`,
	brand: `brand-${String(n % 8).padStart(2, '0')}`,
	price: n % 1000,
	sku: n,
	active: n % 3 !== 0,
}));
const priceSort = { field: 'price', direction: 'asc', missing: 'last' };
const stats = { name: 'prices', field: 'price', type: 'stats' };
const terms = { name: 'brands', field: 'brand', type: 'terms', size: 8 };
const candidateIds = rows.slice(0, Math.min(512, documents)).map((row) => row.id);
const candidates = new Set(candidateIds);
const cases = [
	{ name: 'ordinary-exact', request: {}, selected: rows },
	{ name: 'flat-price-sort', request: { sort: priceSort }, selected: rows },
	{
		name: 'flat-filter-candidates',
		request: {
			sort: priceSort,
			filter: { field: 'active', comparator: 'equals', value: true },
			candidateIds,
			aggregations: [stats, terms],
		},
		selected: rows.filter((row) => row.active && candidates.has(row.id)),
	},
];
if (features === 'expanded')
	cases.push({
		name: 'multi-sort-nested-tree',
		request: {
			sort: [{ field: 'brand', direction: 'desc', missing: 'last' }, priceSort],
			filter: { field: 'active', comparator: 'equals', value: true },
			aggregations: [
				{
					...terms,
					aggregations: [
						{
							name: 'bands',
							field: 'price',
							type: 'range',
							ranges: [{ to: 250 }, { from: 250, to: 750 }, { from: 750 }],
							aggregations: [stats],
						},
					],
				},
			],
		},
		selected: rows.filter((row) => row.active),
	});
let index;
let pressure;
const measurements = [];
const pressureResults = [];
let completedReport;
try {
	index = await openNativeFullTextIndex(options(path.join(directory, 'catalog')));
	for (let start = 0; start < documents; start += 300) {
		const batch = rows.slice(start, start + 300).map(({ id, ...filters }) => ({
			id,
			fields: { title: 'trail shoe', description: 'outdoor waterproof running equipment' },
			filters,
		}));
		const result = await index.applyMutationBatch({ upserts: batch });
		assert.strictEqual(result.processed, batch.length);
		assert.deepStrictEqual(result.rejected, []);
	}
	await index.commit();
	await index.reload();
	const postIndexBaseline = process.memoryUsage();
	for (const scenario of cases) {
		prepareOracle(scenario);
		const request = { text: 'shoe', exactTotal: true, limit: 10, ...scenario.request };
		for (let warmup = 0; warmup < 3; warmup++) validate(await index.search(request), scenario);
		for (const parallel of concurrency) {
			const until = performance.now() + duration;
			let round = 0;
			do {
				const latencies = [];
				const before = process.memoryUsage();
				const cpu = process.cpuUsage();
				const started = performance.now();
				let peakRss = before.rss;
				let next = 0;
				let maxQueueCommands = 0n;
				let maxQueueBytes = 0n;
				const sample = () => {
					peakRss = Math.max(peakRss, process.memoryUsage().rss);
					const status = index.status();
					maxQueueCommands =
						status.searchQueuedCommands > maxQueueCommands ? status.searchQueuedCommands : maxQueueCommands;
					maxQueueBytes = status.searchQueuedBytes > maxQueueBytes ? status.searchQueuedBytes : maxQueueBytes;
				};
				const timer = setInterval(sample, 5);
				try {
					await Promise.all(
						Array.from({ length: parallel }, async () => {
							while (next++ < queries) {
								const begin = performance.now();
								const result = await index.search(request, { remainingBudgetMilliseconds: budget });
								latencies.push(performance.now() - begin);
								validate(result, scenario);
							}
						}),
					);
				} finally {
					clearInterval(timer);
				}
				sample();
				const elapsed = performance.now() - started;
				const used = process.cpuUsage(cpu);
				const after = process.memoryUsage();
				const status = index.status();
				assert.strictEqual(status.searchQueuedCommands, 0n);
				assert.strictEqual(status.searchQueuedBytes, 0n);
				assert.strictEqual(status.state, 'open');
				const timing = percentiles(latencies);
				const throughput = (latencies.length * 1000) / elapsed;
				assert(timing.p95Milliseconds <= gates.maxP95Milliseconds, `${scenario.name}: p95 gate exceeded`);
				assert(timing.p99Milliseconds <= gates.maxP99Milliseconds, `${scenario.name}: p99 gate exceeded`);
				assert(throughput >= gates.minSuccessesPerSecond, `${scenario.name}: throughput gate failed`);
				assert(
					peakRss - postIndexBaseline.rss <= gates.maxPeakRssGrowthBytes,
					`${scenario.name}: RSS observation gate exceeded`,
				);
				measurements.push({
					name: scenario.name,
					concurrency: parallel,
					round: ++round,
					completed: latencies.length,
					latencyMilliseconds: latencies,
					...timing,
					elapsedMilliseconds: elapsed,
					successesPerSecond: throughput,
					cpuMilliseconds: (used.user + used.system) / 1000,
					rssBeforeBytes: before.rss,
					rssEndBytes: after.rss,
					sampledPeakRssBytes: peakRss,
					peakRssGrowthFromPostIndexBytes: peakRss - postIndexBaseline.rss,
					heapEndBytes: after.heapUsed,
					observedMaxQueuedCommands: String(maxQueueCommands),
					observedMaxQueuedBytes: String(maxQueueBytes),
				});
			} while (durationMode ? performance.now() < until : round < rounds);
		}
	}
	for (const scenario of cases)
		for (const parallel of concurrency) {
			const tail = measurements
				.filter((entry) => entry.name === scenario.name && entry.concurrency === parallel)
				.slice(-5)
				.map((entry) => entry.rssEndBytes);
			if (tail.length === 5)
				assert(
					Math.max(...tail) - Math.min(...tail) <= gates.maxTailRoundRssRangeBytes,
					`${scenario.name}: last-five-round RSS range gate exceeded`,
				);
		}
	if (documents > 1024)
		await expectPressure(
			'high-cardinality-histogram',
			index,
			{
				text: 'shoe',
				aggregations: [{ name: 'skus', field: 'sku', type: 'histogram', interval: 1 }],
			},
			documents,
			'product-000000000',
		);
	pressure = await openNativeFullTextIndex(options(path.join(directory, 'long-keys')));
	for (let start = 0; start < 300; start += 5) {
		const batch = Array.from({ length: 5 }, (_, offset) => ({
			id: `pressure-${start + offset}`,
			fields: { title: 'shoe' },
			filters: { brand: `${String(start + offset).padStart(4, '0')}${'x'.repeat(65_000)}` },
		}));
		const result = await pressure.applyMutationBatch({ upserts: batch });
		assert.strictEqual(result.processed, batch.length);
		assert.deepStrictEqual(result.rejected, []);
		if ((start + 5) % 75 === 0) await pressure.commit();
	}
	await pressure.commit();
	await pressure.reload();
	await expectPressure(
		'long-key-terms',
		pressure,
		{
			text: 'shoe',
			aggregations: [{ name: 'brands', field: 'brand', type: 'terms', size: 100 }],
		},
		300,
		'pressure-0',
	);
	completedReport = {
		benchmark: 'metadata-workload',
		revision: argument('--revision', 'working-tree'),
		features,
		runtime: { node: process.version, platform: process.platform, arch: process.arch, native: await runtimeInfo() },
		config: {
			documents,
			rounds: durationMode ? null : rounds,
			durationMilliseconds: durationMode ? duration : null,
			queriesPerRound: queries,
			concurrency,
			limits,
			budgetMilliseconds: budget,
		},
		gates,
		method:
			'Explicitly synthetic product catalog; no customer data. Closed-loop callers, three warmups, source oracles precomputed once per scenario before warmup; response comparisons outside search latency timing but included in round throughput/CPU. RSS sampled every5ms and at round ends relative to post-index baseline; native memory budgets are not RSS bounds. Short repeated rounds are not evidence of soak stability. Flat mode uses only pre-expansion API; expanded results have no old-API analogue.',
		postIndexBaseline,
		measurements,
		summaries: cases.flatMap((scenario) =>
			concurrency.map((parallel) => {
				const entries = measurements.filter((entry) => entry.name === scenario.name && entry.concurrency === parallel);
				const completed = entries.reduce((sum, entry) => sum + entry.completed, 0);
				const elapsed = entries.reduce((sum, entry) => sum + entry.elapsedMilliseconds, 0);
				return {
					name: scenario.name,
					concurrency: parallel,
					rounds: entries.length,
					completed,
					...percentiles(entries.flatMap((entry) => entry.latencyMilliseconds)),
					successesPerSecond: (completed * 1000) / elapsed,
					cpuMilliseconds: entries.reduce((sum, entry) => sum + entry.cpuMilliseconds, 0),
					sampledPeakRssBytes: Math.max(...entries.map((entry) => entry.sampledPeakRssBytes)),
					lastRoundRssBytes: entries.at(-1).rssEndBytes,
				};
			}),
		),
		pressureResults,
		qualification: 'pass',
	};
} finally {
	try {
		await pressure?.close(completedReport ? undefined : { mode: 'rollback' });
	} finally {
		try {
			await index?.close(completedReport ? undefined : { mode: 'rollback' });
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	}
}
console.log(JSON.stringify(completedReport, null, 2));

function percentiles(values) {
	const sorted = [...values].sort((a, b) => a - b);
	const percentile = (p) => sorted[Math.ceil(sorted.length * p) - 1];
	return { p50Milliseconds: percentile(0.5), p95Milliseconds: percentile(0.95), p99Milliseconds: percentile(0.99) };
}
function prepareOracle(scenario) {
	const sorts = scenario.request.sort
		? Array.isArray(scenario.request.sort)
			? scenario.request.sort
			: [scenario.request.sort]
		: [];
	const ordered = [...scenario.selected].sort((a, b) => {
		for (const sort of sorts) {
			const cmp =
				typeof a[sort.field] === 'number'
					? a[sort.field] - b[sort.field]
					: Buffer.compare(Buffer.from(a[sort.field]), Buffer.from(b[sort.field]));
			if (cmp) return sort.direction === 'asc' ? cmp : -cmp;
		}
		return Buffer.compare(Buffer.from(a.id), Buffer.from(b.id));
	});
	scenario.expected = {
		total: scenario.selected.length,
		ids: ordered.slice(0, 10).map((row) => row.id),
		aggregations: (scenario.request.aggregations ?? []).map((request) => oracleAggregation(request, scenario.selected)),
	};
}
function validate(result, scenario) {
	assert.strictEqual(result.total, scenario.expected.total);
	assert.strictEqual(result.totalRelation, 'exact');
	assert.deepStrictEqual(
		result.hits.map((hit) => hit.id),
		scenario.expected.ids,
	);
	assert.deepStrictEqual(canonical(result.aggregations ?? []), canonical(scenario.expected.aggregations));
}
function canonical(results) {
	return results.map((result) => {
		if (!result.buckets) return result;
		const buckets = result.buckets.map((bucket) => ({
			...bucket,
			...(bucket.aggregations ? { aggregations: canonical(bucket.aggregations) } : {}),
		}));
		if (result.type === 'range') for (const bucket of buckets) delete bucket.key;
		if (result.type === 'terms') buckets.sort((a, b) => Buffer.compare(Buffer.from(a.key), Buffer.from(b.key)));
		return { ...result, buckets };
	});
}
function oracleAggregation(request, selected) {
	const base = { name: request.name, type: request.type };
	if (request.type === 'stats') {
		const values = selected.map((row) => row[request.field]);
		const sum = values.reduce((a, b) => a + b, 0);
		return {
			...base,
			count: values.length,
			sum,
			min: values.length ? values.reduce((a, b) => Math.min(a, b), Infinity) : null,
			max: values.length ? values.reduce((a, b) => Math.max(a, b), -Infinity) : null,
			avg: values.length ? sum / values.length : null,
		};
	}
	const children = (rows) =>
		request.aggregations?.length
			? { aggregations: request.aggregations.map((child) => oracleAggregation(child, rows)) }
			: {};
	if (request.type === 'terms') {
		const groups = new Map();
		for (const row of selected) {
			const key = row[request.field];
			if (!groups.has(key)) groups.set(key, []);
			groups.get(key).push(row);
		}
		return {
			...base,
			buckets: [...groups].map(([key, rows]) => ({ key, count: rows.length, ...children(rows) })),
			sumOtherDocCount: 0,
			docCountErrorUpperBound: 0,
		};
	}
	return {
		...base,
		buckets: request.ranges.map((range) => {
			const rows = selected.filter(
				(row) =>
					(range.from === undefined || row[request.field] >= range.from) &&
					(range.to === undefined || row[request.field] < range.to),
			);
			return { key: `${range.from ?? '*'}-${range.to ?? '*'}`, ...range, count: rows.length, ...children(rows) };
		}),
	};
}
async function expectPressure(name, target, request, expectedTotal, expectedFirstId) {
	await assert.rejects(target.search(request, { remainingBudgetMilliseconds: budget }), (error) => {
		assert(['E_RESOURCE_LIMIT', 'E_RESULT_TOO_LARGE'].includes(error.code), `${name}: unexpected ${error.code}`);
		pressureResults.push({ name, code: error.code });
		return true;
	});
	const recovery = await target.search(
		{ text: 'shoe', exactTotal: true, limit: 1 },
		{ remainingBudgetMilliseconds: budget },
	);
	assert.strictEqual(recovery.total, expectedTotal);
	assert.strictEqual(recovery.totalRelation, 'exact');
	assert.strictEqual(recovery.hits[0].id, expectedFirstId);
	Object.assign(pressureResults.at(-1), {
		recovery: {
			total: recovery.total,
			totalRelation: recovery.totalRelation,
			firstId: recovery.hits[0].id,
			budgetMilliseconds: budget,
		},
	});
	const status = target.status();
	assert.strictEqual(status.searchQueuedCommands, 0n);
	assert.strictEqual(status.searchQueuedBytes, 0n);
}
