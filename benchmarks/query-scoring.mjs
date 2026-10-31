import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { openNativeFullTextIndex, runtimeInfo } from '../dist/native.js';
import { closeIndex } from './metadata-cleanup.mjs';

// Run against an optimized local addon: node scripts/run-local.mjs benchmarks/query-scoring.mjs.
const root = await mkdtemp(path.join(tmpdir(), 'fulltext-query-scoring-'));
const documents = 10_000;
const rounds = 20;
const required = { text: 'amber stone', mode: 'any', fields: ['title'] };
const queries = {
	baseline: required,
	boost: { operator: 'boost', boost: 2, clause: required },
	optional: {
		operator: 'optional',
		required,
		optional: [{ operator: 'boost', boost: 3, clause: { text: 'quartz', fields: ['title'] } }],
	},
};
let index;
try {
	index = await openNativeFullTextIndex({
		path: path.join(root, 'index'),
		indexId: 'query-scoring-qualification',
		generation: 'one',
		analyzer: 'english@2',
		stopWords: false,
		fields: [{ name: 'title' }],
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 30_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 4 << 20,
			maxBatchBytes: 1 << 20,
		},
	});
	for (let start = 0; start < documents; start += 500) {
		await index.applyMutationBatch({
			upserts: Array.from({ length: 500 }, (_, offset) => {
				const i = start + offset;
				return {
					id: String(i).padStart(5, '0'),
					fields: { title: i % 4 === 0 ? 'quartz pebble' : i % 4 === 1 ? 'amber quartz' : 'amber stone' },
				};
			}),
		});
		if (start % 2500 === 2000) await index.commit();
	}
	await index.commit();
	await index.reload();
	const baseline = await index.search({ query: required, exactTotal: true, limit: documents });
	const expectedIds = baseline.hits.map(({ id }) => id).sort();
	assert.strictEqual(baseline.total, documents * 0.75);
	const baselineScores = new Map(baseline.hits.map(({ id, score }) => [id, score]));
	for (const [name, query] of Object.entries(queries)) {
		const result = await index.search({ query, exactTotal: true, limit: documents });
		assert.strictEqual(result.total, baseline.total);
		assert.deepStrictEqual(result.hits.map(({ id }) => id).sort(), expectedIds);
		for (const hit of result.hits) {
			if (name === 'boost') assert.strictEqual(hit.score, baselineScores.get(hit.id) * 2);
			if (name === 'optional') assert(hit.score >= baselineScores.get(hit.id));
		}
		if (name === 'optional') assert.strictEqual(Number(result.hits[0].id) % 4, 1);
	}
	const rows = [];
	for (const exactTotal of [false, true]) {
		for (const [name, query] of Object.entries(queries)) {
			const request = { query, exactTotal, limit: 20 };
			await index.search(request);
			const times = [];
			for (let round = 0; round < rounds; round++) {
				const start = performance.now();
				const result = await index.search(request);
				times.push(performance.now() - start);
				assert.strictEqual(result.hits.length, 20);
				if (exactTotal) assert.strictEqual(result.total, baseline.total);
			}
			times.sort((a, b) => a - b);
			rows.push({
				name,
				exactTotal,
				rounds,
				p50Milliseconds: times[Math.floor(rounds / 2)],
				p95Milliseconds: times[Math.floor(rounds * 0.95)],
			});
		}
	}
	console.log(
		JSON.stringify(
			{
				node: process.version,
				platform: process.platform,
				architecture: process.arch,
				runtime: await runtimeInfo(),
				documents,
				requiredMatches: baseline.total,
				rows,
			},
			null,
			2,
		),
	);
} finally {
	await closeIndex(index, { mode: 'rollback' });
	await rm(root, { recursive: true, force: true });
}
