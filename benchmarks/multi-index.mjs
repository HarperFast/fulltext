import assert from 'node:assert';
import { mkdtemp, readdir, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

import { configureNativeFullTextRuntime, openNativeFullTextIndex, runtimeInfo } from '../dist/native.js';

const smoke = process.argv.includes('--smoke');
const indexCount = integerArgument('--indexes', smoke ? 2 : 8);
const documents = integerArgument('--documents', smoke ? 2_000 : 100_000);
const batchSize = integerArgument('--batch-size', smoke ? 250 : 1_000);
const queryCount = integerArgument('--queries', smoke ? 40 : 1_000);
const concurrency = integerArgument('--concurrency', smoke ? 4 : 16);
const revision = stringArgument('--revision', process.env.GITHUB_SHA ?? 'working-tree');
const outputPath = stringArgument('--output');
const root = await mkdtemp(path.join(tmpdir(), 'harper-fulltext-multi-index-'));
const perIndexWriterBytes = 15_000_000;
const perIndexQueueBytes = 8 * 1024 * 1024;

configureNativeFullTextRuntime({
	maxResidentIndexes: indexCount,
	maxIndexingThreads: indexCount,
	maxSearchThreads: indexCount * 2,
	maxWriterMemoryBytes: indexCount * perIndexWriterBytes,
	maxQueuedBytes: indexCount * perIndexQueueBytes * 2,
	maxExpensiveSearches: concurrency,
});

const configs = Array.from({ length: indexCount }, (_, index) => ({
	path: path.join(root, `catalog-${index}`),
	indexId: `catalog-${index}`,
	generation: 'benchmark-v1',
	fields: [{ name: 'title', weight: 3 }, { name: 'description' }, { name: 'category', weight: 1.5 }],
	analyzer: 'english@2',
	positions: true,
	surfaceTerms: true,
	synonyms: [{ source: 'tv', replacements: ['television'] }],
	limits: {
		indexingThreads: 1,
		searchThreads: 2,
		writerMemoryBytes: perIndexWriterBytes,
		maxQueuedCommands: 64,
		maxQueuedBytes: perIndexQueueBytes,
		maxBatchBytes: perIndexQueueBytes,
	},
}));

try {
	const info = await runtimeInfo();
	let indexes = await Promise.all(configs.map(openNativeFullTextIndex));
	await assert.rejects(
		openNativeFullTextIndex({
			...configs[0],
			path: path.join(root, 'over-budget'),
			indexId: 'over-budget',
		}),
		(error) => error.code === 'E_RESOURCE_LIMIT',
	);

	const ingestStarted = performance.now();
	await Promise.all(
		indexes.map(async (index, indexNumber) => {
			for (let start = indexNumber; start < documents; start += batchSize * indexCount) {
				const batch = [];
				for (let id = start; id < documents && batch.length < batchSize; id += indexCount) batch.push(product(id));
				const applied = await index.applyMutationBatch({ upserts: batch });
				assert.strictEqual(applied.processed, batch.length);
			}
			await index.commit();
			await index.reload();
		}),
	);
	const ingestMilliseconds = performance.now() - ingestStarted;

	const mutationStarted = performance.now();
	await Promise.all(
		indexes.map(async (index, indexNumber) => {
			const upserts = [];
			const deletes = [];
			for (let id = indexNumber; id < Math.min(documents, indexCount * 100); id += indexCount) {
				if (id % 3 === 0) deletes.push(`product-${id}`);
				else upserts.push(product(id, 'updated'));
			}
			await index.applyMutationBatch({ upserts, deletes });
			await index.commit();
			await index.reload();
		}),
	);
	const mutationMilliseconds = performance.now() - mutationStarted;

	const queryMix = [
		{ text: 'waterproof trail shoes', mode: 'any' },
		{ text: 'trail running', mode: 'phrase' },
		{ text: 'wireless hea', mode: 'prefix' },
		{ text: 'waterprof', mode: 'fuzzy' },
		{ text: 'television', mode: 'any' },
	];
	const warm = await measureSearch(indexes, queryMix, queryCount, concurrency);
	const statuses = indexes.map((index) => index.status());
	await Promise.all(indexes.map((index) => index.close()));

	const reopenStarted = performance.now();
	indexes = await Promise.all(configs.map(openNativeFullTextIndex));
	const reopenMilliseconds = performance.now() - reopenStarted;
	const cold = await measureSearch(indexes, queryMix, Math.min(queryCount, 100), 1);
	await Promise.all(indexes.map((index) => index.close()));

	const output = {
		formatVersion: 1,
		revision,
		backend: 'tantivy-mmap-multi-index',
		runtime: info,
		host: {
			platform: process.platform,
			arch: process.arch,
			node: process.version,
			cpus: navigator.hardwareConcurrency,
		},
		workload: { indexCount, documents, batchSize, queryCount, concurrency, heavyTail: true, synonyms: true },
		ingestion: {
			milliseconds: ingestMilliseconds,
			documentsPerSecond: (documents * 1_000) / ingestMilliseconds,
			updateDeleteMilliseconds: mutationMilliseconds,
		},
		search: { warm, coldAfterReopen: cold, reopenMilliseconds },
		resources: {
			indexBytes: await directoryBytes(root),
			rssBytes: process.memoryUsage.rss(),
			writerQueueNanoseconds: statuses
				.reduce((sum, status) => sum + status.metrics.writerQueueNanoseconds, 0n)
				.toString(),
			searchQueueNanoseconds: statuses
				.reduce((sum, status) => sum + status.metrics.searchQueueNanoseconds, 0n)
				.toString(),
		},
	};
	assert(output.ingestion.documentsPerSecond > 0);
	assert(output.search.warm.p99Milliseconds > 0);
	const serialized = `${JSON.stringify(output, null, 2)}\n`;
	if (outputPath) await writeFile(outputPath, serialized);
	console.log(serialized.trimEnd());
} finally {
	await rm(root, { recursive: true, force: true });
}

function product(id, suffix = '') {
	const variants = [
		['Waterproof Trail Running Shoes', 'Lightweight outdoor footwear with durable grip', 'shoes'],
		['Wireless TV Headphones', 'Portable television audio with long battery life', 'electronics'],
		['Organic Cotton Blue Shirt', 'Comfortable everyday apparel in multiple sizes', 'clothing'],
		['Stainless Steel Water Bottle', 'Insulated outdoor product for hiking and travel', 'outdoors'],
	];
	const [title, description, category] = variants[Math.floor(id / indexCount) % variants.length];
	const repeat = id % 100 === 0 ? 64 : id % 10 === 0 ? 8 : 1;
	return {
		id: `product-${id}`,
		fields: {
			title: `${title} ${id} ${suffix}`,
			description: `${description} `.repeat(repeat),
			category,
		},
	};
}

async function measureSearch(indexes, queries, count, parallelism) {
	const latencies = [];
	const started = performance.now();
	for (let offset = 0; offset < count; offset += parallelism) {
		await Promise.all(
			Array.from({ length: Math.min(parallelism, count - offset) }, async (_, lane) => {
				const sequence = offset + lane;
				const queryStarted = performance.now();
				let result;
				try {
					result = await indexes[sequence % indexes.length].search({
						...queries[sequence % queries.length],
						limit: 10,
					});
				} catch (error) {
					throw new Error(`query ${sequence} failed for index ${sequence % indexes.length}`, { cause: error });
				}
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
	if (index === -1) return fallback;
	const value = Number(process.argv[index + 1]);
	if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
	return value;
}

function stringArgument(name, fallback) {
	const index = process.argv.indexOf(name);
	if (index === -1) return fallback;
	const value = process.argv[index + 1];
	if (!value || value.startsWith('--')) throw new Error(`${name} requires a value`);
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
