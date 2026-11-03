import assert from 'node:assert';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { openNativeFullTextIndex, runtimeInfo } from '../dist/native.js';
import { closeIndex } from './metadata-cleanup.mjs';

const root = await mkdtemp(path.join(tmpdir(), 'fulltext-query-scoring-'));
const documents = 10_000;
const rounds = 20;
const variedScores = process.argv.includes('--varied-scores');
const routingShapes = process.argv.includes('--routing-shapes');
const corpus = variedScores ? 'varied-scores' : 'tied-scores';
const required = { text: 'amber stone', mode: 'any', fields: ['title'] };
const all = { ...required, mode: 'all' };
const all3 = { ...all, text: 'amber stone ruby' };
const all4 = { ...all, text: 'amber stone ruby sapphire' };
const term = (text) => ({ text, fields: ['title'] });
const boosted = (clause) => ({ operator: 'boost', boost: 2, clause });
const rewarded = (required) => ({
	operator: 'optional',
	required,
	optional: [{ operator: 'boost', boost: 3, clause: term('quartz') }],
});
const or = { operator: 'or', clauses: [term('amber'), term('stone')] };
const and = { operator: 'and', clauses: [term('amber'), term('stone')] };
const wrapped = (clause) => boosted({ operator: 'boost', boost: 1, clause });
const defaultQueries = [
	{ name: 'single/baseline', query: term('amber'), baseline: 'single/baseline' },
	{ name: 'single/lowered-root', query: boosted(term('amber')), baseline: 'single/baseline', scale: 2 },
	{
		name: 'single/native-wrapped',
		query: wrapped(term('amber')),
		baseline: 'single/baseline',
		scale: 2,
		equivalent: 'single/lowered-root',
	},
	{ name: 'any/baseline', query: required, baseline: 'any/baseline' },
	{ name: 'any/boost', query: boosted(required), baseline: 'any/baseline', scale: 2 },
	{ name: 'any/optional', query: rewarded(required), baseline: 'any/baseline', bonus: true },
	{ name: 'or/baseline', query: or, baseline: 'or/baseline' },
	{ name: 'or/root-boost', query: boosted(or), baseline: 'or/baseline', scale: 2 },
	{ name: 'and/baseline', query: and, baseline: 'and/baseline' },
	{ name: 'and/root-boost', query: boosted(and), baseline: 'and/baseline', scale: 2 },
	{ name: 'all/baseline', query: all, baseline: 'all/baseline' },
	{ name: 'all/lowered-root', query: boosted(all), baseline: 'all/baseline', scale: 2 },
	{
		name: 'all/native-wrapped',
		query: wrapped(all),
		baseline: 'all/baseline',
		scale: 2,
		equivalent: 'all/lowered-root',
	},
	{ name: 'all/optional', query: rewarded(all), baseline: 'all/baseline', bonus: true },
	{ name: 'all3/baseline', query: all3, baseline: 'all3/baseline' },
	{ name: 'all3/lowered-root', query: boosted(all3), baseline: 'all3/baseline', scale: 2 },
	{
		name: 'all3/native-wrapped',
		query: wrapped(all3),
		baseline: 'all3/baseline',
		scale: 2,
		equivalent: 'all3/lowered-root',
	},
	{ name: 'all4/baseline', query: all4, baseline: 'all4/baseline' },
	{ name: 'all4/lowered-root', query: boosted(all4), baseline: 'all4/baseline', scale: 2 },
	{
		name: 'all4/native-wrapped',
		query: wrapped(all4),
		baseline: 'all4/baseline',
		scale: 2,
		equivalent: 'all4/lowered-root',
	},
];
const routingQueries = [
	['or', or, documents * 0.75],
	['prefix', { text: 'amber stone ruby', mode: 'prefix', fields: ['title'] }, documents / 2],
	['short-fuzzy', { text: 'xy zz', mode: 'fuzzy', fields: ['title'] }, documents * 0.75],
	['phrase-two-fields', { text: 'amber', mode: 'phrase', fields: ['title', 'body'] }, documents * 0.75],
	['all3-two-fields', { ...all3, fields: ['title', 'body'] }, documents / 2],
].flatMap(([family, query, expectedTotal]) => [
	{ name: `${family}/baseline`, query, baseline: `${family}/baseline`, expectedTotal },
	{ name: `${family}/native-wrapped`, query: wrapped(query), baseline: `${family}/baseline`, expectedTotal, scale: 2 },
]);
const queries = routingShapes ? routingQueries : defaultQueries;

function documentTitle(i) {
	const words =
		i % 4 === 0
			? ['quartz', 'pebble']
			: i % 4 === 1
				? ['amber', 'quartz']
				: i % 4 === 2
					? ['amber', 'stone', 'quartz', 'ruby', 'sapphire']
					: ['amber', 'stone', 'ruby', 'sapphire'];
	if (routingShapes) {
		if (words.includes('amber')) words.push('xy');
		if (words.includes('stone')) words.push('zz');
	}
	if (!variedScores) return words.join(' ');
	return (
		words
			.map((word, field) =>
				Array(1 + ((i * (17 + field * 14)) % (23 + field * 6)))
					.fill(word)
					.join(' '),
			)
			.join(' ') +
		' ' +
		Array(1 + ((i * 37) % 173))
			.fill('filler')
			.join(' ')
	);
}

async function completeResult(index, query) {
	if (!routingShapes) return index.search({ query, exactTotal: true, limit: documents });
	const first = await index.searchPage({ query, limit: 100 });
	const hits = [...first.hits];
	let continuation = first.continuation;
	while (continuation) {
		const page = await index.searchPage({ query, limit: 100, continuation });
		hits.push(...page.hits);
		continuation = page.continuation;
	}
	assert.strictEqual(hits.length, first.total);
	return { ...first, hits };
}

let index;
try {
	index = await openNativeFullTextIndex({
		path: path.join(root, 'index'),
		indexId: 'query-scoring-qualification',
		generation: 'one',
		analyzer: 'english@2',
		stopWords: false,
		fields: routingShapes ? [{ name: 'title' }, { name: 'body' }] : [{ name: 'title' }],
		...(routingShapes ? { positions: true, surfaceTerms: true } : {}),
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 30_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 4 << 20,
			maxBatchBytes: 1 << 20,
		},
	});
	const batchSize = routingShapes ? 125 : 500;
	for (let start = 0; start < documents; start += batchSize) {
		await index.applyMutationBatch({
			upserts: Array.from({ length: batchSize }, (_, offset) => {
				const i = start + offset;
				return {
					id: String(i).padStart(5, '0'),
					fields: {
						title: documentTitle(i),
						...(routingShapes ? { body: documentTitle(i) } : {}),
					},
				};
			}),
		});
		if ((start + batchSize) % 2500 === 0) await index.commit();
	}
	await index.commit();
	await index.reload();
	const committedSegments = JSON.parse(await readFile(path.join(root, 'index', 'meta.json'), 'utf8')).segments.length;
	assert(committedSegments >= 2, 'qualification must span multiple committed segments');
	const results = new Map();
	for (const { name, query, baseline, scale, bonus, equivalent, expectedTotal } of queries) {
		const result = await completeResult(index, query);
		results.set(name, result);
		const expected = results.get(baseline);
		assert.strictEqual(
			result.total,
			expectedTotal ??
				(name.startsWith('and/') || name.startsWith('all/') || name.startsWith('all4/') || name.startsWith('all3/')
					? documents / 2
					: documents * 0.75),
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
		if (equivalent)
			assert.deepStrictEqual(
				result.hits,
				results.get(equivalent).hits,
				`${name}: native and root-lowered scores must agree`,
			);
	}
	const rows = [];
	for (const exactTotal of [false, true]) {
		for (const { name, query, baseline } of queries) {
			const boundary = results.get(name).hits;
			const distinctBoundary = boundary[19].score !== boundary[20].score;
			if (variedScores && !name.endsWith('/optional'))
				assert(distinctBoundary, `${name}: varied corpus must avoid a stable-collector boundary-tie rerun`);
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
				distinctBoundary,
				rounds,
				p50Milliseconds: times[Math.floor(rounds / 2)],
				p95Milliseconds: times[Math.ceil(rounds * 0.95) - 1],
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
				corpus,
				...(routingShapes ? { routingShapes: true } : {}),
				committedSegments,
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
