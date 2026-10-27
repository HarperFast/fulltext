import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { performance } from 'node:perf_hooks';

const root = path.resolve(process.argv[2] ?? '.');
const label = process.argv[3] ?? 'current';
const { openNativeFullTextIndex } = await import(pathToFileURL(path.join(root, 'dist/native.js')));
if (process.argv.includes('--hydration')) {
	await hydrationCases();
} else {
	const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-ordinary-search-'));
	let index;
	try {
		index = await openNativeFullTextIndex(options(directory));
		await index.applyMutationBatch({
			upserts: Array.from({ length: 10000 }, (_, i) => ({
				id: String(i).padStart(5, '0'),
				fields: { title: i % 10 === 0 ? 'widget scarce' : 'widget common' },
			})),
		});
		await index.publish('one');
		for (const limit of [20, 1000]) {
			for (const [text, total] of [
				['absent', 0],
				['scarce', 1000],
				['widget', 10000],
			]) {
				const request = { text, limit, exactTotal: true };
				const check = await index.search(request);
				assert.equal(check.total, total);
				assert.equal(check.hits.length, Math.min(limit, total));
				for (let i = 0; i < 200; i++) await index.search(request);
				const started = performance.now();
				for (let i = 0; i < 1000; i++) await index.search(request);
				console.log(
					JSON.stringify({
						label,
						text,
						limit,
						iterations: 1000,
						milliseconds: performance.now() - started,
						node: process.version,
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
}

function options(directory) {
	return {
		path: directory,
		indexId: 'ordinary-benchmark',
		generation: 'one',
		analyzer: 'english@2',
		fields: [{ name: 'title', weight: 1 }],
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15000000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8388608,
			maxBatchBytes: 8388608,
		},
	};
}

async function hydrationCases() {
	const exactTotal = !process.argv.includes('--approximate-totals');
	for (const versionBytes of [0, 2048]) {
		const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-hydration-'));
		let index;
		try {
			index = await openNativeFullTextIndex(options(directory));
			const version = (i) => (versionBytes ? `version-${i}-` + 'x'.repeat(versionBytes) : undefined);
			await index.applyMutationBatch({
				upserts: Array.from({ length: 10000 }, (_, i) => ({
					id: String(i).padStart(5, '0'),
					version: version(i),
					fields: { title: i < 20 ? 'widget' : `widget ${'other '.repeat(100)}` },
				})),
			});
			await index.publish('one');
			for (const [offset, limit] of [
				[0, 20],
				[9990, 10],
			]) {
				const request = { text: 'widget', mode: 'all', offset, limit, exactTotal };
				const expectedIds = Array.from({ length: limit }, (_, i) => String(offset + i).padStart(5, '0'));
				const check = (result) => {
					assert.equal(result.total, !exactTotal && offset === 0 ? limit : 10000);
					assert.equal(result.totalRelation, !exactTotal && offset === 0 ? 'lower-bound' : 'exact');
					assert.deepStrictEqual(
						result.hits.map(({ id }) => id),
						expectedIds,
					);
					for (const hit of result.hits) assert.equal(hit.version, version(Number(hit.id)));
				};
				const expected = await index.search(request);
				check(expected);
				if (offset === 0) {
					const boundary = await index.search({ ...request, limit: 21 });
					assert(boundary.hits[19].score > boundary.hits[20].score);
				}
				for (let i = 0; i < 500; i++) await index.search(request);
				const samples = [];
				for (let i = 0; i < 1000; i++) {
					const start = performance.now();
					const result = await index.search(request);
					samples.push(performance.now() - start);
					assert.deepStrictEqual(result.hits, expected.hits);
				}
				samples.sort((a, b) => a - b);
				console.log(
					JSON.stringify({
						label,
						offset,
						limit,
						documents: 10000,
						versionPaddingBytes: versionBytes,
						exactTotal,
						iterations: samples.length,
						warmupIterations: 500,
						percentileMethod: 'nearest-rank',
						hits: expected.hits.map(({ id, score }) => ({ id, score })),
						modeledVersionDecodes: {
							eagerWindow: versionBytes ? Math.min(10000, offset + limit + 1) : 0,
							finalPage: versionBytes ? limit : 0,
						},
						p50: samples[499],
						p95: samples[949],
						p99: samples[989],
						node: process.version,
					}),
				);
			}
		} finally {
			try {
				if (index) await index.close({ mode: 'rollback' });
			} finally {
				await rm(directory, { recursive: true, force: true });
			}
		}
	}
}
