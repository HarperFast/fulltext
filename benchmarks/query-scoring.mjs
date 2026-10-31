import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { openNativeFullTextIndex, runtimeInfo } from '../dist/native.js';
import { closeIndex } from './metadata-cleanup.mjs';

const root = await mkdtemp(path.join(tmpdir(), 'fulltext-query-scoring-'));
const documents = 10_000;
const rounds = 20;
const required = { text: 'amber stone', mode: 'any', fields: ['title'] };
const all = { ...required, mode: 'all' };
const term = (text) => ({ text, fields: ['title'] });
const boosted = (clause) => ({ operator: 'boost', boost: 2, clause });
const rewarded = (required) => ({
	operator: 'optional',
	required,
	optional: [{ operator: 'boost', boost: 3, clause: term('quartz') }],
});
const or = { operator: 'or', clauses: [term('amber'), term('stone')] };
const and = { operator: 'and', clauses: [term('amber'), term('stone')] };
const queries = [
	{ name: 'any/baseline', query: required, baseline: 'any/baseline' },
	{ name: 'any/boost', query: boosted(required), baseline: 'any/baseline', scale: 2 },
	{ name: 'any/optional', query: rewarded(required), baseline: 'any/baseline', bonus: true },
	{ name: 'or/baseline', query: or, baseline: 'or/baseline' },
	{ name: 'or/root-boost', query: boosted(or), baseline: 'or/baseline', scale: 2 },
	{ name: 'and/baseline', query: and, baseline: 'and/baseline' },
	{ name: 'and/root-boost', query: boosted(and), baseline: 'and/baseline', scale: 2 },
	{ name: 'all/baseline', query: all, baseline: 'all/baseline' },
	{ name: 'all/optional', query: rewarded(all), baseline: 'all/baseline', bonus: true },
];
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
					fields: {
						title:
							i % 4 === 0
								? 'quartz pebble'
								: i % 4 === 1
									? 'amber quartz'
									: i % 4 === 2
										? 'amber stone quartz'
										: 'amber stone',
					},
				};
			}),
		});
		if (start % 2500 === 2000) await index.commit();
	}
	await index.commit();
	await index.reload();
	const results = new Map();
	for (const { name, query, baseline, scale, bonus } of queries) {
		const result = await index.search({ query, exactTotal: true, limit: documents });
		results.set(name, result);
		const expected = results.get(baseline);
		assert.strictEqual(
			result.total,
			name.startsWith('and/') || name.startsWith('all/') ? documents / 2 : documents * 0.75,
		);
		assert.deepStrictEqual(result.hits.map(({ id }) => id).sort(), expected.hits.map(({ id }) => id).sort());
		const scores = new Map(expected.hits.map(({ id, score }) => [id, score]));
		let improved = 0;
		for (const hit of result.hits) {
			if (scale) assert.strictEqual(hit.score, scores.get(hit.id) * scale);
			if (bonus) {
				assert(hit.score >= scores.get(hit.id));
				if (hit.score > scores.get(hit.id)) improved++;
			}
		}
		if (bonus) assert(improved > 0);
	}
	const rows = [];
	for (const exactTotal of [false, true]) {
		for (const { name, query, baseline } of queries) {
			const request = { query, exactTotal, limit: 20 };
			await index.search(request);
			const times = [];
			for (let round = 0; round < rounds; round++) {
				const start = performance.now();
				const result = await index.search(request);
				times.push(performance.now() - start);
				assert.strictEqual(result.hits.length, 20);
				if (exactTotal) assert.strictEqual(result.total, results.get(baseline).total);
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
				requiredMatches: Object.fromEntries(
					queries.filter(({ name, baseline }) => name === baseline).map(({ name }) => [name, results.get(name).total]),
				),
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
