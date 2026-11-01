import assert from 'node:assert';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { parseArgs } from 'node:util';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { openNativeFullTextIndex, runtimeInfo } from '../dist/native.js';
import { closeIndex } from './metadata-cleanup.mjs';

const { values } = parseArgs({
	options: {
		documents: { type: 'string', default: '10000' },
		density: { type: 'string' },
		segments: { type: 'string', default: '4' },
		'writer-memory-mib': { type: 'string', default: '30' },
		rounds: { type: 'string', default: '100' },
		limit: { type: 'string', default: '20' },
		'deadline-ms': { type: 'string' },
		'deadline-rounds': { type: 'string', default: '20' },
		'varied-scores': { type: 'boolean', default: false },
		'routing-shapes': { type: 'boolean', default: false },
		restricted: { type: 'boolean', default: false },
	},
});
function integerOption(name, minimum, maximum) {
	const value = Number(values[name]);
	assert(
		Number.isSafeInteger(value) && value >= minimum && value <= maximum,
		`--${name}: integer ${minimum}..${maximum}`,
	);
	return value;
}
const documents = integerOption('documents', 100, 100000);
const requestedSegments = integerOption('segments', 1, Math.min(4, documents));
const writerMemoryMiB = integerOption('writer-memory-mib', 30, 1024);
const writerMemoryBytes = writerMemoryMiB * 1024 * 1024;
const rounds = integerOption('rounds', 1, 10000);
const limit = integerOption('limit', 20, 1000);
const deadlineRounds = integerOption('deadline-rounds', 1, 10000);
const deadlineBudgets =
	values['deadline-ms'] === undefined
		? []
		: values['deadline-ms'].split(',').map((value) => {
				assert(/^\d+$/.test(value), '--deadline-ms: comma-separated positive integers');
				const budget = Number(value);
				assert(Number.isSafeInteger(budget) && budget > 0, '--deadline-ms: positive safe integers');
				return budget;
			});
assert(!deadlineBudgets.length || values.restricted, '--deadline-ms requires --restricted');
const density = values.density === undefined ? 0.5 : Number(values.density);
assert(Number.isFinite(density) && density > 0 && density <= 1, '--density: number greater than 0 and at most 1');
const warmupRounds = 5;
const variedScores = values['varied-scores'];
const routingShapes = values['routing-shapes'];
assert(!routingShapes || limit <= 100, '--routing-shapes requires --limit <= 100: ordinary prefix search limit');
const corpus = variedScores ? 'varied-scores' : 'tied-scores';
const require = createRequire(import.meta.url);
const root = await mkdtemp(path.join(tmpdir(), 'fulltext-query-scoring-'));
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
		name: 'single-all/baseline',
		query: { ...term('amber'), mode: 'all' },
		baseline: 'single/baseline',
		equivalent: 'single/baseline',
	},
	{
		name: 'single-all/lowered-root',
		query: boosted({ ...term('amber'), mode: 'all' }),
		baseline: 'single/baseline',
		scale: 2,
		equivalent: 'single/lowered-root',
	},
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
	['or', or],
	['prefix', { text: 'amber stone ruby', mode: 'prefix', fields: ['title'] }],
	['short-fuzzy', { text: 'xy zz', mode: 'fuzzy', fields: ['title'] }],
	['phrase-two-fields', { text: 'amber', mode: 'phrase', fields: ['title', 'body'] }],
	['all3-two-fields', { ...all3, fields: ['title', 'body'] }],
].flatMap(([family, query]) => [
	{ name: `${family}/baseline`, query, baseline: `${family}/baseline` },
	{ name: `${family}/native-wrapped`, query: wrapped(query), baseline: `${family}/baseline`, scale: 2 },
]);
const candidateStride = Math.max(2, Math.ceil(documents / 1024));
const candidateIds = Array.from({ length: documents }, (_, i) => i)
	.filter((i) => i % candidateStride === 0)
	.map((i) => String(i).padStart(5, '0'));
const restrictedQueries = values.restricted
	? [
			['all4', all4],
			['or', or],
		].flatMap(([family, query]) =>
			[
				['filter', { filter: { field: 'enabled', comparator: 'equals', value: true } }],
				['candidates', { candidateIds }],
			].flatMap(([restriction, options]) => {
				const baseline = `${family}/${restriction}/baseline`;
				return [
					{ name: baseline, query, baseline, options, family, restricted: true },
					{
						name: `${family}/${restriction}/native-wrapped`,
						query: wrapped(query),
						baseline,
						options,
						family,
						restricted: true,
						scale: 2,
					},
				];
			}),
		)
	: [];
const queries = [...(routingShapes ? routingQueries : defaultQueries), ...restrictedQueries];
function documentFamily(i) {
	if (values.density === undefined) return i % 4;
	const bucket = ((i * 37) % 10000) / 10000;
	if (bucket < (1 - density) / 2) return 0;
	if (bucket < 1 - density) return 1;
	return bucket < 1 - density / 2 ? 2 : 3;
}
function expectedIds(descriptor) {
	const allTerms = /^(all|and|prefix)/.test(descriptor.family ?? descriptor.name);
	return Array.from({ length: documents }, (_, i) => i)
		.filter(
			(i) =>
				(!descriptor.restricted || i % (descriptor.options.candidateIds ? candidateStride : 2) === 0) &&
				(allTerms ? documentFamily(i) >= 2 : documentFamily(i) !== 0),
		)
		.map((i) => String(i).padStart(5, '0'));
}

function documentTitle(i) {
	const words =
		documentFamily(i) === 0
			? ['quartz', 'pebble']
			: documentFamily(i) === 1
				? ['amber', 'quartz']
				: documentFamily(i) === 2
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

async function completeResult(index, query, options = {}) {
	if (!routingShapes && documents <= 10000)
		return index.search({ ...options, query, exactTotal: true, limit: documents });
	const first = await index.searchPage({ ...options, query, limit: 1000 });
	const hits = [...first.hits];
	let continuation = first.continuation;
	while (continuation) {
		const page = await index.searchPage({ ...options, query, limit: 1000, continuation });
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
		...(values.restricted ? { filterFields: [{ name: 'enabled', type: 'boolean' }] } : {}),
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes,
			maxQueuedCommands: 32,
			maxQueuedBytes: 4 << 20,
			maxBatchBytes: 1 << 20,
		},
	});
	const batchSize = routingShapes ? 125 : 500;
	for (let segment = 0; segment < requestedSegments; segment++) {
		const end = Math.floor((documents * (segment + 1)) / requestedSegments);
		for (let start = Math.floor((documents * segment) / requestedSegments); start < end; start += batchSize) {
			await index.applyMutationBatch({
				upserts: Array.from({ length: Math.min(batchSize, end - start) }, (_, offset) => {
					const i = start + offset;
					return {
						id: String(i).padStart(5, '0'),
						fields: { title: documentTitle(i), ...(routingShapes ? { body: documentTitle(i) } : {}) },
						...(values.restricted ? { filters: { enabled: i % 2 === 0 } } : {}),
					};
				}),
			});
		}
		await index.commit();
	}
	await index.reload();
	const committedSegments = JSON.parse(await readFile(path.join(root, 'index', 'meta.json'), 'utf8')).segments.length;
	assert.strictEqual(
		committedSegments,
		requestedSegments,
		'observed segments must match --segments; automatic merging can invalidate this workload',
	);
	const results = new Map();
	for (const descriptor of queries) {
		const { name, query, baseline, scale, bonus, equivalent, options } = descriptor;
		const result = await completeResult(index, query, options);
		results.set(name, result);
		const expected = results.get(baseline);
		const sourceIds = expectedIds(descriptor);
		assert.strictEqual(result.total, sourceIds.length);
		assert.strictEqual(result.totalRelation, 'exact');
		assert.deepStrictEqual(result.hits.map(({ id }) => id).sort(), sourceIds.sort());
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
		const phase = queries.map((descriptor) => {
			const { name, query, options } = descriptor;
			const expected = results.get(name);
			const distinctBoundary =
				expected.hits.length > limit ? expected.hits[limit - 1].score !== expected.hits[limit].score : null;
			return { name, request: { ...options, query, exactTotal, limit }, expected, distinctBoundary, samples: [] };
		});
		for (let round = 0; round < warmupRounds + rounds; round++) {
			for (let position = 0; position < phase.length; position++) {
				const row = phase[(position + round) % phase.length];
				const start = performance.now();
				const result = await index.search(row.request);
				const elapsed = performance.now() - start;
				if (round >= warmupRounds) row.samples.push(elapsed);
				assert.deepStrictEqual(result.hits, row.expected.hits.slice(0, limit), `${row.name}: bounded score/ID parity`);
				assert.strictEqual(result.total, exactTotal ? row.expected.total : Math.min(row.expected.total, limit));
				assert.strictEqual(result.totalRelation, exactTotal || row.expected.total <= limit ? 'exact' : 'lower-bound');
			}
		}
		for (const row of phase) {
			const sorted = [...row.samples].sort((a, b) => a - b);
			rows.push({
				name: row.name,
				exactTotal,
				distinctBoundary: row.distinctBoundary,
				boundaryTie: row.distinctBoundary === false,
				rounds,
				warmupRounds,
				samplesMilliseconds: row.samples,
				p50Milliseconds: sorted[Math.ceil(rounds * 0.5) - 1],
				p95Milliseconds: sorted[Math.ceil(rounds * 0.95) - 1],
			});
		}
	}
	const deadlineRows = [];
	const deadlinePhase = restrictedQueries
		.filter(({ name, baseline }) => name === baseline)
		.flatMap((descriptor) =>
			deadlineBudgets.map((budgetMilliseconds) => ({ descriptor, budgetMilliseconds, attempts: [] })),
		);
	for (let round = 0; round < deadlineRounds && deadlinePhase.length; round++) {
		for (let position = 0; position < deadlinePhase.length; position++) {
			const row = deadlinePhase[(position + round) % deadlinePhase.length];
			const { name, query, options } = row.descriptor;
			const expected = results.get(name);
			const start = performance.now();
			let result;
			let outcome = 'success';
			try {
				result = await index.search(
					{ ...options, query, limit, exactTotal: false },
					{ remainingBudgetMilliseconds: row.budgetMilliseconds },
				);
			} catch (error) {
				assert.strictEqual(error.code, 'E_TIMEOUT', name);
				outcome = 'E_TIMEOUT';
			}
			const elapsedMilliseconds = performance.now() - start;
			row.attempts.push({
				budgetMilliseconds: row.budgetMilliseconds,
				elapsedMilliseconds,
				overshootMilliseconds: Math.max(0, elapsedMilliseconds - row.budgetMilliseconds),
				outcome,
			});
			if (result) {
				assert.deepStrictEqual(result.hits, expected.hits.slice(0, limit), `${name}: budgeted score/ID parity`);
				assert.strictEqual(result.total, Math.min(expected.total, limit));
				assert.strictEqual(result.totalRelation, expected.total <= limit ? 'exact' : 'lower-bound');
			}
		}
	}
	for (const descriptor of restrictedQueries.filter(({ name, baseline }) => name === baseline)) {
		if (!deadlineBudgets.length) break;
		const recovery = await index.search({ ...descriptor.options, query: descriptor.query, limit, exactTotal: false });
		const expected = results.get(descriptor.name);
		assert.deepStrictEqual(
			recovery.hits,
			expected.hits.slice(0, limit),
			`${descriptor.name}: recovery score/ID parity`,
		);
		assert.strictEqual(recovery.total, Math.min(expected.total, limit));
		assert.strictEqual(recovery.totalRelation, expected.total <= limit ? 'exact' : 'lower-bound');
		deadlineRows.push({
			name: descriptor.name,
			recovered: true,
			attempts: deadlinePhase
				.filter(({ descriptor: entry }) => entry.name === descriptor.name)
				.flatMap(({ attempts }) => attempts),
		});
	}
	const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');
	const nativeArtifacts = await Promise.all(
		Object.keys(require.cache)
			.filter((file) => file.endsWith('.node'))
			.map(async (file) => ({ path: file, sha256: sha256(await readFile(file)) })),
	);
	console.log(
		JSON.stringify(
			{
				node: process.version,
				platform: process.platform,
				architecture: process.arch,
				runtime: await runtimeInfo(),
				documents,
				limit,
				density,
				requestedSegments,
				writerMemoryMiB,
				writerMemoryBytes,
				...(values.restricted ? { candidateCount: candidateIds.length, candidateStride, filterDensity: 0.5 } : {}),
				rounds,
				warmupRounds,
				percentileMethod: 'nearest-rank',
				sampleOrder: 'rotating interleaved query rows; exact-total modes separate',
				driverSha256: sha256(await readFile(new URL(import.meta.url))),
				nativeArtifacts,
				corpus,
				...(routingShapes ? { routingShapes: true } : {}),
				committedSegments,
				requiredMatches: Object.fromEntries(
					queries.filter(({ name, baseline }) => name === baseline).map(({ name }) => [name, results.get(name).total]),
				),
				rows,
				...(deadlineBudgets.length
					? {
							deadlineRounds,
							deadlineBudgets,
							deadlineMeasurement:
								'end-to-end request elapsed time includes JavaScript queue and callback overhead; cooperative, not a hard deadline or native-loop proof',
							deadlineRows,
						}
					: {}),
			},
			null,
			2,
		),
	);
} finally {
	await closeIndex(index, { mode: 'rollback' });
	await rm(root, { recursive: true, force: true });
}
