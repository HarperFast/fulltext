import assert from 'node:assert';
import { mkdtemp, readdir, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

import { encodeMutationBatch, openNativeFullTextIndex, runtimeInfo } from '../dist/native.js';

const smoke = process.argv.includes('--smoke');
const documents = integerArgument('--documents', smoke ? 1_000 : 100_000);
const batchSize = integerArgument('--batch-size', smoke ? 250 : 1_000);
const queryCount = integerArgument('--queries', smoke ? 20 : 500);
const concurrency = integerArgument('--concurrency', 4);
const commitEvery = integerArgument('--commit-every', documents);
const mutationDriver = enumArgument('--mutation-driver', ['logical', 'low-level'], 'logical');
const revision = stringArgument('--revision', process.env.GITHUB_SHA ?? 'working-tree');
const outputPath = stringArgument('--output');
const indexPath = await mkdtemp(path.join(tmpdir(), 'harper-fulltext-benchmark-'));
const config = {
	path: indexPath,
	indexId: 'products-benchmark',
	generation: 'benchmark-v1',
	fields: [{ name: 'title', weight: 3 }, { name: 'description' }, { name: 'category', weight: 1.5 }],
	analyzer: 'english@1',
	positions: true,
	surfaceTerms: true,
	limits: {
		indexingThreads: Math.min(4, Math.max(1, concurrency)),
		searchThreads: Math.min(8, Math.max(1, concurrency)),
		writerMemoryBytes: 60_000_000,
		maxQueuedCommands: 128,
		maxQueuedBytes: 64 * 1024 * 1024,
		maxBatchBytes: 16 * 1024 * 1024,
	},
};

try {
	const info = await runtimeInfo();
	let index = await openNativeFullTextIndex(config);
	let packingMilliseconds = 0;
	let applyMilliseconds = 0;
	let packedBytes = 0;
	let uncommittedDocuments = 0;
	let frames = 0;
	let mutationDriverMilliseconds = 0;
	const logicalBatchLatencies = [];
	let peakRssBytes = process.memoryUsage.rss();
	const rssSampler = setInterval(() => {
		peakRssBytes = Math.max(peakRssBytes, process.memoryUsage.rss());
	}, 50);
	rssSampler.unref();
	const commitLatencies = [];
	const indexingStarted = performance.now();
	for (let start = 0; start < documents; start += batchSize) {
		const end = Math.min(start + batchSize, documents);
		const batch = Array.from({ length: end - start }, (_, offset) => product(start + offset));
		const logicalBatchStarted = performance.now();
		if (mutationDriver === 'low-level') {
			const packingStarted = performance.now();
			const packed = encodeMutationBatch({ upserts: batch }, config.limits.maxBatchBytes);
			packingMilliseconds += performance.now() - packingStarted;
			packedBytes += packed.byteLength;
			frames++;
			const applyStarted = performance.now();
			assert.strictEqual(await index.apply(packed), batch.length);
			applyMilliseconds += performance.now() - applyStarted;
		} else {
			const applyStarted = performance.now();
			const result = await index.applyMutationBatch({ upserts: batch });
			applyMilliseconds += performance.now() - applyStarted;
			assert.strictEqual(result.processed, batch.length);
			assert.deepStrictEqual(result.rejected, []);
			packedBytes += result.encodedBytes;
			frames += result.frames;
		}
		const logicalBatchMilliseconds = performance.now() - logicalBatchStarted;
		logicalBatchLatencies.push(logicalBatchMilliseconds);
		mutationDriverMilliseconds += logicalBatchMilliseconds;
		peakRssBytes = Math.max(peakRssBytes, process.memoryUsage.rss());
		uncommittedDocuments += batch.length;
		if (end === documents || uncommittedDocuments >= commitEvery) {
			const commitStarted = performance.now();
			await index.commit();
			commitLatencies.push(performance.now() - commitStarted);
			peakRssBytes = Math.max(peakRssBytes, process.memoryUsage.rss());
			uncommittedDocuments = 0;
		}
	}
	const durableIndexingMilliseconds = performance.now() - indexingStarted;
	const afterIndexing = index.status();
	const reloadStarted = performance.now();
	await index.reload();
	const reloadMilliseconds = performance.now() - reloadStarted;

	const correctness = await index.search({ text: 'waterproof trail shoes', exactTotal: true, limit: 10 });
	assert(correctness.total > 0);
	assert(correctness.hits.some((hit) => hit.id.startsWith('product-')));
	const queryMix = [
		{ name: 'any', text: 'waterproof trail shoes', mode: 'any' },
		{ name: 'all', text: 'waterproof trail shoes', mode: 'all' },
		{ name: 'phrase', text: 'trail running', mode: 'phrase' },
		{ name: 'prefix', text: 'waterproof trai', mode: 'prefix' },
		{ name: 'fuzzy', text: 'waterprof', mode: 'fuzzy' },
		{ name: 'fuzzy-prefix', text: 'waterproof tral', mode: 'fuzzy-prefix' },
		{ name: 'candidate-filter', text: 'waterproof', mode: 'any', candidateIds: ['product-0'] },
	];
	for (const query of queryMix) {
		await index.search({ ...query, limit: 10 });
	}
	const warm = await measureSearch(index, queryMix, queryCount, concurrency, false);
	const byMode = Object.fromEntries(
		await Promise.all(
			queryMix.map(async (query) => [
				query.name,
				await measureSearch(index, [query], Math.max(4, Math.floor(queryCount / queryMix.length)), concurrency, false),
			]),
		),
	);
	const exactComparisonCount = Math.max(4, Math.floor(queryCount / 10));
	const approximateSingle = await measureSearch(index, queryMix, exactComparisonCount, 1, false);
	const exact = await measureSearch(index, queryMix, exactComparisonCount, 1, true);
	const status = index.status();
	peakRssBytes = Math.max(peakRssBytes, process.memoryUsage.rss());
	clearInterval(rssSampler);
	await index.close();

	const reopenStarted = performance.now();
	index = await openNativeFullTextIndex(config);
	const reopenMilliseconds = performance.now() - reopenStarted;
	const cold = await measureSearch(index, queryMix, Math.min(20, queryCount), 1, false);
	await index.close();
	const sortedCommitLatencies = [...commitLatencies].sort((left, right) => left - right);
	logicalBatchLatencies.sort((left, right) => left - right);
	const output = {
		formatVersion: 2,
		revision,
		backend: 'tantivy-mmap',
		runtime: info,
		host: {
			platform: process.platform,
			arch: process.arch,
			node: process.version,
			cpus: navigator.hardwareConcurrency,
		},
		workload: {
			mutationDriver,
			documents,
			batchSize,
			packedBytes,
			averagePackedBytesPerDocument: packedBytes / documents,
			queryCount,
			concurrency,
			fields: config.fields.length,
			indexingThreads: config.limits.indexingThreads,
			searchThreads: config.limits.searchThreads,
			writerMemoryBytes: config.limits.writerMemoryBytes,
		},
		indexing: {
			packingMilliseconds,
			applyMilliseconds,
			mutationDriverMilliseconds,
			durableEndToEndMilliseconds: durableIndexingMilliseconds,
			durableDocumentsPerSecond: (documents * 1_000) / durableIndexingMilliseconds,
			durablePackedMiBPerSecond: (packedBytes / 1024 / 1024) * (1_000 / durableIndexingMilliseconds),
			frames,
			logicalBatchP50Milliseconds: percentile(logicalBatchLatencies, 0.5),
			logicalBatchP95Milliseconds: percentile(logicalBatchLatencies, 0.95),
			logicalBatchP99Milliseconds: percentile(logicalBatchLatencies, 0.99),
			writerQueueMilliseconds: Number(afterIndexing.metrics.writerQueueNanoseconds) / 1e6,
			writerExecutionMilliseconds: Number(afterIndexing.metrics.writerExecutionNanoseconds) / 1e6,
			commitEveryDocuments: commitEvery,
			commitCount: commitLatencies.length,
			commitMillisecondsTotal: commitLatencies.reduce((total, latency) => total + latency, 0),
			commitP50Milliseconds: percentile(sortedCommitLatencies, 0.5),
			commitP95Milliseconds: percentile(sortedCommitLatencies, 0.95),
			commitP99Milliseconds: percentile(sortedCommitLatencies, 0.99),
			reloadMilliseconds,
		},
		search: {
			warmApproximate: warm,
			byMode,
			warmApproximateSingle: approximateSingle,
			warmExactTotal: exact,
			coldAfterReopen: cold,
			reopenMilliseconds,
		},
		resources: {
			indexBytes: await directoryBytes(indexPath),
			peakRssBytes,
			postCloseRssBytes: process.memoryUsage.rss(),
		},
		metrics: {
			writerQueueNanoseconds: status.metrics.writerQueueNanoseconds.toString(),
			writerExecutionNanoseconds: status.metrics.writerExecutionNanoseconds.toString(),
			searchQueueNanoseconds: status.metrics.searchQueueNanoseconds.toString(),
			searchExecutionNanoseconds: status.metrics.searchExecutionNanoseconds.toString(),
		},
	};
	assert(output.indexing.durableDocumentsPerSecond > 0);
	assert(output.search.warmApproximate.p99Milliseconds > 0);
	assert(output.resources.indexBytes > 0);
	const serialized = `${JSON.stringify(output, null, 2)}\n`;
	if (outputPath) await writeFile(outputPath, serialized);
	console.log(serialized.trimEnd());
} finally {
	await rm(indexPath, { recursive: true, force: true });
}

function product(id) {
	const variants = [
		['Waterproof Trail Running Shoes', 'Lightweight outdoor footwear with durable grip', 'shoes'],
		['Wireless Noise Cancelling Headphones', 'Portable audio product with long battery life', 'electronics'],
		['Organic Cotton Blue Shirt', 'Comfortable everyday apparel in multiple sizes', 'clothing'],
		['Stainless Steel Water Bottle', 'Insulated outdoor product for hiking and travel', 'outdoors'],
	];
	const [title, description, category] = variants[id % variants.length];
	return { id: `product-${id}`, fields: { title: `${title} ${id}`, description, category } };
}

async function measureSearch(index, queries, count, parallelism, exactTotal) {
	const latencies = [];
	const started = performance.now();
	for (let offset = 0; offset < count; offset += parallelism) {
		await Promise.all(
			Array.from({ length: Math.min(parallelism, count - offset) }, async (_, lane) => {
				const queryStarted = performance.now();
				const result = await index.search({
					...queries[(offset + lane) % queries.length],
					limit: 10,
					exactTotal,
				});
				assert(result.hits.length > 0);
				latencies.push(performance.now() - queryStarted);
			}),
		);
	}
	const milliseconds = performance.now() - started;
	latencies.sort((left, right) => left - right);
	return {
		queries: count,
		concurrency: parallelism,
		queriesPerSecond: (count * 1_000) / milliseconds,
		p50Milliseconds: percentile(latencies, 0.5),
		p95Milliseconds: percentile(latencies, 0.95),
		p99Milliseconds: percentile(latencies, 0.99),
	};
}

function percentile(values, fraction) {
	return values[Math.min(values.length - 1, Math.ceil(values.length * fraction) - 1)];
}

function integerArgument(name, fallback) {
	const index = process.argv.indexOf(name);
	if (index === -1) {
		return fallback;
	}
	const value = Number(process.argv[index + 1]);
	if (!Number.isSafeInteger(value) || value <= 0) {
		throw new Error(`${name} must be a positive integer`);
	}
	return value;
}

function stringArgument(name, fallback) {
	const index = process.argv.indexOf(name);
	if (index === -1) return fallback;
	const value = process.argv[index + 1];
	if (!value || value.startsWith('--')) throw new Error(`${name} requires a value`);
	return value;
}

function enumArgument(name, values, fallback) {
	const index = process.argv.indexOf(name);
	if (index === -1) return fallback;
	const value = process.argv[index + 1];
	if (!values.includes(value)) throw new Error(`${name} must be one of ${values.join(', ')}`);
	return value;
}

async function directoryBytes(directory) {
	let bytes = 0;
	for (const entry of await readdir(directory, { withFileTypes: true })) {
		const entryPath = path.join(directory, entry.name);
		bytes += entry.isDirectory() ? await directoryBytes(entryPath) : (await stat(entryPath)).size;
	}
	return bytes;
}
