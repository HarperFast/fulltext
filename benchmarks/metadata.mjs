import assert from 'node:assert';
import { mkdtemp, readdir, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { pathToFileURL } from 'node:url';

const smoke = process.argv.includes('--smoke');
const documentCount = integerArgument('--documents', smoke ? 300 : 10_000);
const queryCount = integerArgument('--queries', smoke ? 10 : 100);
const modulePath = stringArgument('--module');
const { openNativeFullTextIndex, runtimeInfo } = await import(
	modulePath ? pathToFileURL(path.resolve(modulePath)).href : new URL('../dist/native.js', import.meta.url).href
);
const native = await runtimeInfo();
const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-metadata-benchmark-'));
const config = {
	path: directory,
	indexId: 'metadata-benchmark',
	generation: 'v1',
	fields: [{ name: 'title' }, { name: 'description' }],
	filterFields: [
		{ name: 'brand', type: 'string' },
		{ name: 'price', type: 'number' },
		{ name: 'active', type: 'boolean' },
	],
	analyzer: 'english@2',
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
try {
	index = await openNativeFullTextIndex(config);
	const writeStarted = performance.now();
	for (let start = 0; start < documentCount; start += 300) {
		const upserts = Array.from({ length: Math.min(300, documentCount - start) }, (_, offset) => record(start + offset));
		const applied = await index.applyMutationBatch({ upserts });
		assert.strictEqual(applied.processed, upserts.length);
		assert.deepStrictEqual(applied.rejected, []);
	}
	const applyMilliseconds = performance.now() - writeStarted;
	const commitStarted = performance.now();
	await index.commit();
	const commitMilliseconds = performance.now() - commitStarted;
	const writeMilliseconds = performance.now() - writeStarted;
	await index.reload();
	const priceSort = { field: 'price', direction: 'asc', missing: 'last' };
	const aggregations = [
		{ name: 'brands', field: 'brand', type: 'terms', size: 10 },
		{ name: 'prices', field: 'price', type: 'stats' },
		{ name: 'histogram', field: 'price', type: 'histogram', interval: 10 },
	];
	const cases = [
		{ name: 'ordinary', request: {} },
		{ name: 'numeric-sort', request: { sort: priceSort } },
		{ name: 'string-sort', request: { sort: { field: 'brand', direction: 'asc', missing: 'last' } } },
		...aggregations.map((aggregation) => ({ name: aggregation.type, request: { aggregations: [aggregation] } })),
		{
			name: 'combined',
			request: { sort: priceSort, aggregations, filter: { field: 'active', comparator: 'equals', value: true } },
		},
	];
	const measurements = {};
	for (const { name, request } of cases) {
		if (native.queryApiVersion < 5 && name !== 'ordinary') {
			measurements[name] = { skipped: `query API ${native.queryApiVersion} does not support metadata collectors` };
			continue;
		}
		measurements[name] = await measure(index, name, request);
	}
	const closeResult = await index.close({ mode: 'rollback' });
	index = undefined;
	if (closeResult.cleanupError) throw closeResult.cleanupError;
	const footprint = await directoryFootprint(directory);
	console.log(
		JSON.stringify(
			{
				benchmark: 'metadata',
				revision: stringArgument('--revision') ?? process.env.GITHUB_SHA ?? 'working-tree',
				runtime: { node: process.version, platform: process.platform, arch: process.arch, native },
				config: {
					...config,
					path: '<temporary-index>',
					documents: documentCount,
					queriesPerCase: queryCount,
					batchSize: 300,
					concurrency: 1,
				},
				writes: {
					applyMilliseconds,
					commitMilliseconds,
					durableMilliseconds: writeMilliseconds,
					documentsPerSecond: (documentCount * 1000) / writeMilliseconds,
				},
				footprint: {
					...footprint,
					measurement: 'after close; total includes metadata and segment files',
					bytesPerDocument: footprint.totalBytes / documentCount,
				},
				queries: measurements,
			},
			null,
			2,
		),
	);
} finally {
	await index?.close({ mode: 'rollback' });
	await rm(directory, { recursive: true, force: true });
}

function record(index) {
	return {
		id: `product-${String(index).padStart(9, '0')}`,
		fields: { title: `Trail shoe model ${index % 100}`, description: `Outdoor running equipment ${index % 7}` },
		filters: {
			...(index % 17 ? { brand: `brand-${String(index % 32).padStart(2, '0')}` } : {}),
			...(index % 19 ? { price: index % 1000 } : {}),
			active: index % 3 !== 0,
		},
	};
}

async function measure(index, name, request) {
	const latencies = [];
	const rejections = [];
	let matched;
	for (let iteration = -3; iteration < queryCount; iteration++) {
		const started = performance.now();
		try {
			const result = await index.search({ text: 'trail shoe', mode: 'all', exactTotal: true, limit: 10, ...request });
			assert.strictEqual(
				result.total,
				name === 'combined' ? documentCount - Math.ceil(documentCount / 3) : documentCount,
			);
			matched = result.total;
			if (iteration >= 0) latencies.push(performance.now() - started);
		} catch (error) {
			if (!['E_RESOURCE_LIMIT', 'E_RESULT_TOO_LARGE', 'E_TIMEOUT'].includes(error.code)) throw error;
			const rejection = { iteration, code: error.code, message: error.message };
			rejections.push(rejection);
			console.error(JSON.stringify({ benchmark: 'metadata', case: name, rejection }));
		}
	}
	latencies.sort((left, right) => left - right);
	return {
		attempted: queryCount,
		succeeded: latencies.length,
		warmup: 3,
		matched,
		p50Milliseconds: percentile(latencies, 0.5),
		p95Milliseconds: percentile(latencies, 0.95),
		p99Milliseconds: percentile(latencies, 0.99),
		rejections,
	};
}

function percentile(values, fraction) {
	return values.length ? values[Math.ceil(values.length * fraction) - 1] : null;
}

function integerArgument(name, fallback) {
	const value = stringArgument(name);
	if (value === undefined) return fallback;
	const parsed = Number(value);
	if (!Number.isSafeInteger(parsed) || parsed < 1) throw new Error(`${name} must be a positive integer`);
	return parsed;
}

function stringArgument(name) {
	const index = process.argv.indexOf(name);
	if (index < 0) return undefined;
	const value = process.argv[index + 1];
	if (!value || value.startsWith('--')) throw new Error(`${name} requires a value`);
	return value;
}

async function directoryFootprint(directory) {
	let totalBytes = 0;
	let fileCount = 0;
	for (const entry of await readdir(directory, { withFileTypes: true })) {
		const file = path.join(directory, entry.name);
		if (entry.isDirectory()) {
			const nested = await directoryFootprint(file);
			totalBytes += nested.totalBytes;
			fileCount += nested.fileCount;
		} else {
			totalBytes += (await stat(file)).size;
			fileCount++;
		}
	}
	return { totalBytes, fileCount };
}
