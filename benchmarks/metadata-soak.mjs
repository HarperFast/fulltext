import assert from 'node:assert';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { mkdir, open, readFile, readdir, rm, stat, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { prepareOracle, validate } from './metadata-oracle.mjs';

const argument = (name, fallback) => {
	const at = process.argv.indexOf(name);
	return at < 0 ? fallback : process.argv[at + 1];
};
const positive = (name, fallback, integer = false) => {
	const value = Number(argument(name, fallback));
	assert(Number.isFinite(value) && value > 0 && (!integer || Number.isSafeInteger(value)), `${name} must be positive`);
	return value;
};
const artifacts = argument('--artifacts');
assert(artifacts && path.isAbsolute(artifacts), '--artifacts requires a new absolute artifact directory');
const documents = positive('--documents', 50_000, true);
assert(documents >= 200, '--documents must be at least 200');
const mixedMilliseconds = positive('--mixed-seconds', 1800) * 1000;
const drainMilliseconds = positive('--drain-seconds', 600) * 1000;
const readers = positive('--readers', 8, true);
const writeInterval = positive('--write-interval-ms', 2000);
const sampleInterval = positive('--sample-ms', 1000, true);
assert(sampleInterval >= 100, '--sample-ms must be at least 100');
const budget = positive('--budget-ms', 5000, true);
const gates = {
	maxP95Milliseconds: positive('--max-p95-ms', 250),
	maxP99Milliseconds: positive('--max-p99-ms', 5000),
	minSuccessesPerSecond: positive('--min-throughput', 10),
	maxPeakRssGrowthBytes: positive('--max-rss-growth-mib', 256) * 1024 * 1024,
	maxTailRssSpanBytes: positive('--max-tail-rss-range-mib', 32) * 1024 * 1024,
	tailMilliseconds: 300_000,
	unexpectedErrors: 0,
};
const modulePath = path.resolve(argument('--module', fileURLToPath(new URL('../dist/native.js', import.meta.url))));
const api = await import(pathToFileURL(modulePath).href);
await mkdir(artifacts);
const indexPath = path.join(artifacts, 'index');
const telemetry = await open(path.join(artifacts, 'samples.ndjson'), 'wx');
const stringify = (value) => JSON.stringify(value, (_, entry) => (typeof entry === 'bigint' ? String(entry) : entry));
const log = (entry) => console.error(stringify(entry));
const hash = async (file) =>
	createHash('sha256')
		.update(await readFile(file))
		.digest('hex');
const runtime = await api.runtimeInfo();
const addonFiles = process.report
	.getReport()
	.sharedObjects.filter((file) => path.basename(file).startsWith('fulltext.') && file.endsWith('.node'));
const manifest = {
	kind: 'metadata-mixed-soak',
	startedAt: new Date().toISOString(),
	revision: execFileSync('git', ['rev-parse', 'HEAD'], {
		encoding: 'utf8',
		cwd: fileURLToPath(new URL('..', import.meta.url)),
	}).trim(),
	driverSha256: await hash(fileURLToPath(import.meta.url)),
	loadedAddons: await Promise.all(addonFiles.map(async (file) => ({ path: file, sha256: await hash(file) }))),
	oracleSha256: await hash(fileURLToPath(new URL('./metadata-oracle.mjs', import.meta.url))),
	modulePath,
	runtime,
	machine: {
		platform: process.platform,
		architecture: process.arch,
		node: process.version,
		cpus: os.cpus().length,
		totalMemory: os.totalmem(),
	},
	workload: {
		documents,
		readers,
		mixedMilliseconds,
		drainMilliseconds,
		writeInterval,
		sampleInterval,
		upsertsPerPublication: 100,
		deletesPerPublication: 10,
		restorePriorDeletes: true,
	},
	gates,
	measurement:
		'Public addon, fixed-generation reader leases and source-derived oracles. Fixed latency histograms (0.1 ms bins), bounded recent samples, all samples streamed to disk. No forced GC. Segment observations read committed meta.json; disk samples every 10 seconds. Throughput includes validation and oracle preparation overhead.',
};
await writeFile(path.join(artifacts, 'manifest.json'), stringify(manifest) + '\n', { flag: 'wx' });
log({ phase: 'premeasurement-gates', ...manifest });
const config = {
	path: indexPath,
	indexId: 'mixed-catalog-soak',
	generation: 'one',
	analyzer: 'english@2',
	fields: [{ name: 'title' }],
	filterFields: [
		{ name: 'brand', type: 'string' },
		{ name: 'price', type: 'number' },
		{ name: 'stock', type: 'number' },
		{ name: 'rating', type: 'number' },
		{ name: 'sku', type: 'number' },
		{ name: 'active', type: 'boolean' },
	],
	limits: {
		indexingThreads: 1,
		searchThreads: 2,
		writerMemoryBytes: 15_000_000,
		maxQueuedCommands: 32,
		maxQueuedBytes: 4 << 20,
		maxBatchBytes: 1 << 20,
	},
};
const idOf = (n) => `product-${String(n).padStart(9, '0')}`;
const rowOf = (n, epoch) => ({
	id: idOf(n),
	version: String(epoch),
	sku: n,
	brand: `brand-${(n + epoch) % 8}`,
	price: (n * 7 + epoch * 11) % 1000,
	stock: (n + epoch) % 101,
	rating: (n + epoch) % 5,
	active: (n + epoch) % 3 !== 0,
});
const upsert = ({ id, version, ...filters }) => ({ id, version, fields: { title: 'trail shoe' }, filters });
const truth = new Map(
	Array.from({ length: documents }, (_, n) => {
		const row = rowOf(n, 0);
		return [row.id, row];
	}),
);
let writer,
	writerClosed = false,
	current,
	firstError,
	stopping = false,
	samplerStopping = false;
let phase = 'setup',
	epoch = 0,
	publications = 0,
	mutations = 0,
	completed = 0;
let baseline,
	measuredStarted,
	phaseStarted,
	sampledPeak = 0,
	maxReaders = 0;
const leases = new Set(),
	closing = new Set(),
	recent = { mixed: [], drain: [] },
	summaries = new Map();
const controller = new AbortController();
let maximumSegments = 0,
	maximumDiskBytes = 0;
let disk = {},
	lastDiskAt = -Infinity,
	sampleTask,
	workers = [];
const fail = (error) => {
	firstError ??= error;
	stopping = true;
	controller.abort();
};
function summary(name, atPhase) {
	const key = `${atPhase}:${name}`;
	if (!summaries.has(key))
		summaries.set(key, { phase: atPhase, name, completed: 0, bins: new Uint32Array(50_002), maxMilliseconds: 0 });
	return summaries.get(key);
}
function recordLatency(entry, elapsed) {
	entry.completed++;
	entry.bins[Math.min(50_001, Math.ceil(elapsed * 10))]++;
	entry.maxMilliseconds = Math.max(entry.maxMilliseconds, elapsed);
}
function percentile(entry, fraction) {
	const target = Math.ceil(entry.completed * fraction);
	let count = 0;
	for (let i = 0; i < entry.bins.length; i++) {
		count += entry.bins[i];
		if (count >= target) return i === 50_001 ? entry.maxMilliseconds : i / 10;
	}
	return 0;
}
function reportSummaries() {
	return [...summaries.values()].map((entry) => ({
		phase: entry.phase,
		name: entry.name,
		completed: entry.completed,
		p95Milliseconds: percentile(entry, 0.95),
		p99Milliseconds: percentile(entry, 0.99),
		maxMilliseconds: entry.maxMilliseconds,
	}));
}
function scenarios() {
	const rows = [...truth.values()];
	const stats = { name: 'prices', field: 'price', type: 'stats' };
	const terms = { name: 'brands', field: 'brand', type: 'terms', size: 8 };
	const price = { field: 'price', direction: 'asc', missing: 'last' };
	const brand = { field: 'brand', direction: 'asc', missing: 'last' };
	return [
		{ name: 'ordinary-exact', request: {}, selected: rows },
		{ name: 'price-sort', request: { sort: price }, selected: rows },
		{
			name: 'nested-histogram',
			request: {
				sort: [brand, price],
				filter: { field: 'active', comparator: 'equals', value: true },
				aggregations: [
					{
						...terms,
						aggregations: [{ name: 'bands', field: 'price', type: 'histogram', interval: 250, aggregations: [stats] }],
					},
				],
			},
			selected: rows.filter((row) => row.active),
		},
		{
			name: 'runtime-five-sort',
			request: {
				sort: [
					brand,
					price,
					...['stock', 'rating', 'sku'].map((field) => ({ field, direction: 'desc', missing: 'last' })),
				],
				aggregations: [stats, terms],
			},
			selected: rows,
		},
	].map((scenario) => {
		prepareOracle(scenario);
		scenario.expectedVersions = scenario.expected.ids.map((id) => truth.get(id).version);
		delete scenario.selected;
		scenario.request = { text: 'shoe', exactTotal: true, limit: 10, ...scenario.request };
		return scenario;
	});
}
function verify(result, scenario) {
	validate(result, scenario);
	assert.deepStrictEqual(
		result.hits.map((hit) => hit.version),
		scenario.expectedVersions,
	);
}
async function bounded(operation, label) {
	let timer;
	try {
		return await Promise.race([
			operation,
			new Promise((_, reject) => {
				timer = setTimeout(() => reject(new Error(`${label} exceeded 30 seconds`)), 30_000);
			}),
		]);
	} finally {
		clearTimeout(timer);
	}
}
function retire(lease) {
	lease.retired = true;
	if (lease.active || lease.closing) return;
	lease.closing = true;
	const operation = bounded(lease.reader.close(), 'reader close').then(() => leases.delete(lease));
	closing.add(operation);
	operation.then(
		() => closing.delete(operation),
		(error) => {
			closing.delete(operation);
			fail(error);
		},
	);
}
async function installReader() {
	const expected = scenarios();
	const reader = await api.openNativeFullTextReader(config);
	const lease = { reader, scenarios: expected, epoch, active: 0, retired: false, closing: false };
	leases.add(lease);
	maxReaders = Math.max(maxReaders, leases.size);
	assert.strictEqual(reader.committedPayload, `soak:${epoch}`);
	for (const scenario of expected)
		verify(await reader.search(scenario.request, { remainingBudgetMilliseconds: budget }), scenario);
	const prior = current;
	current = lease;
	if (prior) retire(prior);
}
async function collectSample() {
	const now = performance.now();
	const memory = process.memoryUsage();
	sampledPeak = Math.max(sampledPeak, memory.rss);
	const statuses = [...leases].filter((lease) => !lease.closing).map((lease) => lease.reader.status());
	if (writer && !writerClosed) statuses.push(writer.status());
	const meta = JSON.parse(await readFile(path.join(indexPath, 'meta.json'), 'utf8'));
	if (now - lastDiskAt >= 10_000) {
		let bytes = 0,
			files = 0;
		const names = await readdir(indexPath);
		for (let start = 0; start < names.length; start += 32) {
			const sizes = await Promise.all(
				names.slice(start, start + 32).map(async (name) => {
					try {
						const value = await stat(path.join(indexPath, name));
						return value.isFile() ? value.size : undefined;
					} catch (error) {
						if (error.code === 'ENOENT') return;
						throw error;
					}
				}),
			);
			for (const size of sizes)
				if (size !== undefined) {
					bytes += size;
					files++;
				}
		}
		disk = { observedAtMilliseconds: now - measuredStarted, bytes, files };
		lastDiskAt = now;
	}
	const sample = {
		phase,
		elapsedMilliseconds: now - measuredStarted,
		phaseElapsedMilliseconds: now - phaseStarted,
		memory,
		completed,
		publications,
		mutations,
		epoch,
		publishedEpoch: current?.epoch,
		readers: leases.size,
		segments: meta.segments.length,
		segmentMaxDocs: meta.segments.reduce((sum, segment) => sum + segment.max_doc, 0),
		disk,
		searchQueuedCommands: statuses.reduce((sum, status) => sum + status.searchQueuedCommands, 0n),
		searchQueuedBytes: statuses.reduce((sum, status) => sum + status.searchQueuedBytes, 0n),
		writerQueuedCommands: statuses.reduce((sum, status) => sum + status.writerQueuedCommands, 0n),
		writerQueuedBytes: statuses.reduce((sum, status) => sum + status.writerQueuedBytes, 0n),
	};
	maximumSegments = Math.max(maximumSegments, sample.segments);
	maximumDiskBytes = Math.max(maximumDiskBytes, disk.bytes);
	await telemetry.write(stringify(sample) + '\n');
	if (recent[phase]) {
		recent[phase].push({ elapsedMilliseconds: now, rss: memory.rss, heap: memory.heapUsed });
		while (recent[phase].length > 1 && recent[phase][0].elapsedMilliseconds < now - gates.tailMilliseconds)
			recent[phase].shift();
	}
	assert(memory.rss - baseline.rss <= gates.maxPeakRssGrowthBytes, 'RSS peak-growth gate exceeded');
	return sample;
}
async function sampleLoop() {
	let lastProgress = 0;
	while (!samplerStopping) {
		const sample = await collectSample();
		if (sample.elapsedMilliseconds - lastProgress >= 60_000) {
			log({ phase: 'progress', sample, queries: reportSummaries() });
			lastProgress = sample.elapsedMilliseconds;
		}
		await delay(sampleInterval);
	}
}
async function readLoop(number) {
	let next = number;
	try {
		while (!stopping) {
			const lease = current,
				scenario = lease.scenarios[next++ % lease.scenarios.length],
				atPhase = phase;
			lease.active++;
			try {
				const start = performance.now();
				const result = await lease.reader.search(scenario.request, { remainingBudgetMilliseconds: budget });
				const elapsed = performance.now() - start;
				verify(result, scenario);
				recordLatency(summary(scenario.name, atPhase), elapsed);
				completed++;
			} finally {
				lease.active--;
				if (lease.retired) retire(lease);
			}
		}
	} catch (error) {
		fail(error);
	}
}
let finalReport;
try {
	writer = await api.openNativeFullTextIndex(config);
	const initial = [...truth.values()];
	for (let start = 0; start < initial.length; start += 200) {
		const result = await writer.applyMutationBatch({ upserts: initial.slice(start, start + 200).map(upsert) });
		assert.deepStrictEqual(result.rejected, []);
		assert.strictEqual(result.processed, Math.min(200, initial.length - start));
	}
	await writer.publish('soak:0');
	await writer.reload();
	await installReader();
	baseline = process.memoryUsage();
	sampledPeak = baseline.rss;
	measuredStarted = phaseStarted = performance.now();
	phase = 'mixed';
	sampleTask = sampleLoop().catch(fail);
	workers = Array.from({ length: readers }, (_, n) => readLoop(n));
	let deleted = [];
	while (!firstError && performance.now() - phaseStarted < mixedMilliseconds) {
		const nextWrite = performance.now() + writeInterval;
		epoch++;
		const changed = new Map();
		for (const n of deleted) changed.set(idOf(n), rowOf(n, epoch));
		const base = (epoch * 137) % documents;
		for (let n = 0; n < 100; n++) {
			const value = (base + n) % documents;
			changed.set(idOf(value), rowOf(value, epoch));
		}
		deleted = Array.from({ length: 10 }, (_, n) => (base + 100 + n) % documents);
		for (const n of deleted) changed.delete(idOf(n));
		const result = await writer.applyMutationBatch({
			upserts: [...changed.values()].map(upsert),
			deletes: deleted.map(idOf),
		});
		assert.deepStrictEqual(result.rejected, []);
		assert.strictEqual(result.processed, changed.size + deleted.length);
		mutations += changed.size + deleted.length;
		for (const [id, row] of changed) truth.set(id, row);
		for (const n of deleted) truth.delete(idOf(n));
		await writer.publish(`soak:${epoch}`);
		await writer.reload();
		await installReader();
		publications++;
		await delay(Math.max(0, nextWrite - performance.now()), undefined, { signal: controller.signal });
	}
	if (firstError) throw firstError;
	await bounded(writer.close(), 'writer drain');
	writerClosed = true;
	phase = 'drain';
	phaseStarted = performance.now();
	log({ phase: 'writer-drained', publications, mutations, completed });
	await delay(drainMilliseconds, undefined, { signal: controller.signal });
	if (firstError) throw firstError;
	stopping = true;
	await Promise.all(workers);
	samplerStopping = true;
	await sampleTask;
	await collectSample();
	const results = reportSummaries();
	for (const result of results) {
		assert(result.completed > 0);
		assert(result.p95Milliseconds <= gates.maxP95Milliseconds, `${result.phase}/${result.name}: p95 gate exceeded`);
		assert(result.p99Milliseconds <= gates.maxP99Milliseconds, `${result.phase}/${result.name}: p99 gate exceeded`);
	}
	const tails = Object.fromEntries(
		Object.entries(recent).map(([name, samples]) => {
			const rss = samples.map((sample) => sample.rss),
				minimum = Math.min(...rss),
				maximum = Math.max(...rss);
			assert(samples.length > 0, `${name}: missing samples`);
			assert(maximum - minimum <= gates.maxTailRssSpanBytes, `${name}: tail RSS span gate exceeded`);
			return [
				name,
				{
					samples: samples.length,
					milliseconds: samples.at(-1).elapsedMilliseconds - samples[0].elapsedMilliseconds,
					minimumRss: minimum,
					maximumRss: maximum,
					spanBytes: maximum - minimum,
					lastMinusFirstBytes: rss.at(-1) - rss[0],
				},
			];
		}),
	);
	const elapsed = performance.now() - measuredStarted;
	assert((completed * 1000) / elapsed >= gates.minSuccessesPerSecond, 'throughput gate failed');
	for (const lease of leases) {
		const status = lease.reader.status();
		assert.strictEqual(status.searchQueuedCommands, 0n);
		assert.strictEqual(status.searchQueuedBytes, 0n);
		retire(lease);
	}
	await Promise.all(closing);
	if (firstError) throw firstError;
	const reopened = await api.openNativeFullTextReader(config);
	try {
		assert.strictEqual(reopened.committedPayload, `soak:${epoch}`);
		for (const scenario of current.scenarios)
			verify(await reopened.search(scenario.request, { remainingBudgetMilliseconds: budget }), scenario);
	} finally {
		await bounded(reopened.close(), 'reopened reader close');
	}
	await rm(indexPath, { recursive: true });
	finalReport = {
		...manifest,
		finishedAt: new Date().toISOString(),
		accepted: true,
		elapsedMilliseconds: elapsed,
		completed,
		publications,
		mutations,
		sampledPeakRss: sampledPeak,
		peakGrowthBytes: sampledPeak - baseline.rss,
		baseline,
		maxReaders,
		maximumSegments,
		maximumDiskBytes,
		tails,
		queries: results,
		indexCleanup: 'closed, reopened and verified, then removed',
		telemetry: 'samples.ndjson',
	};
	await writeFile(path.join(artifacts, 'report.json'), stringify(finalReport) + '\n', { flag: 'wx' });
	log({ phase: 'accepted', completed, publications, mutations, tails, peakGrowthBytes: finalReport.peakGrowthBytes });
} catch (error) {
	fail(error);
	await Promise.allSettled(workers);
	samplerStopping = true;
	if (sampleTask) await sampleTask;
	const closeErrors = [];
	for (const lease of leases) {
		try {
			await bounded(lease.reader.close(), 'failure reader close');
		} catch (closeError) {
			closeErrors.push(String(closeError));
		}
	}
	if (writer && !writerClosed) {
		try {
			await bounded(writer.close({ mode: 'rollback' }), 'failure writer close');
		} catch (closeError) {
			closeErrors.push(String(closeError));
		}
	}
	const failure = {
		accepted: false,
		error: { message: firstError.message, stack: firstError.stack, code: firstError.code },
		phase,
		completed,
		publications,
		mutations,
		baseline,
		sampledPeakRss: sampledPeak,
		recent,
		queries: reportSummaries(),
		closeErrors,
		indexRetained: indexPath,
	};
	await writeFile(path.join(artifacts, 'failure.json'), stringify(failure) + '\n', { flag: 'wx' });
	log({ phase: 'failed', ...failure });
	process.exitCode = 1;
} finally {
	await telemetry.close();
}
if (finalReport) console.log(stringify(finalReport));
