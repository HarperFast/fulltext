import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { openNativeFullTextIndex, openNativeFullTextReader, runtimeInfo } from '@harperfast/fulltext/native';

const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-traversal-driver-'));
const count = 65_536;
const batchSize = 8_192;
const warmup = 10;
const samples = 60;
const config = {
	path: directory,
	indexId: 'traversal-driver',
	generation: 'one',
	analyzer: 'english@2',
	fields: [{ name: 'title', weight: 1 }],
	limits: {
		indexingThreads: 1,
		searchThreads: 2,
		writerMemoryBytes: 15_000_000,
		maxQueuedCommands: 32,
		maxQueuedBytes: 8_388_608,
		maxBatchBytes: 8_388_608,
	},
};
let writer, reader;
try {
	writer = await openNativeFullTextIndex(config);
	for (let start = 0; start < count; start += batchSize) {
		await writer.applyMutationBatch({
			upserts: Array.from({ length: batchSize }, (_, offset) => ({
				id: String(start + offset).padStart(6, '0'),
				fields: { title: 'widget catalog' },
			})),
		});
	}
	await writer.publish('one');
	reader = await openNativeFullTextReader(config);
	const request = { text: 'widget catalog', limit: 128, exactTotal: true };
	const ordinary = await reader.search(request);
	const page = await reader.searchPage(request);
	assert.deepStrictEqual(page.hits, ordinary.hits);
	assert.equal(page.total, ordinary.total);
	const times = { search: [], searchPage: [] };
	for (let i = 0; i < warmup + samples; i++) {
		for (const method of i % 2 ? ['search', 'searchPage'] : ['searchPage', 'search']) {
			const started = performance.now();
			const result = await reader[method](request);
			const elapsed = performance.now() - started;
			assert.equal(result.total, count);
			if (i >= warmup) times[method].push(elapsed);
		}
	}
	const median = (values) => {
		values.sort((a, b) => a - b);
		return (values[samples / 2 - 1] + values[samples / 2]) / 2;
	};
	const ordinaryMedianMs = median(times.search);
	const traversalMedianMs = median(times.searchPage);
	console.log(
		JSON.stringify({
			node: process.version,
			platform: process.platform,
			arch: process.arch,
			runtime: await runtimeInfo(),
			count,
			limit: request.limit,
			samplesPerMethod: samples,
			ordinaryMedianMs,
			traversalMedianMs,
			ratio: traversalMedianMs / ordinaryMedianMs,
		}),
	);
} finally {
	if (reader) await reader.close();
	if (writer) await writer.close({ mode: 'rollback' });
	await rm(directory, { recursive: true, force: true });
}
