import assert from 'node:assert';
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdtemp, rename, rm, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import { openNativeFullTextIndex, runtimeInfo } from '../dist/native.js';
import { closeIndex } from './metadata-cleanup.mjs';

const gates = { maxP99Milliseconds: 5_000, maxRssGrowthBytes: 256 << 20 };
const args = process.argv.slice(2);
assert(
	args.length <= 1 &&
		args.every((arg) => ['--profile=distance-1', '--profile=distance-2', '--profile=breadth'].includes(arg)),
	'usage: query-controls.mjs [--profile=distance-1|--profile=distance-2|--profile=breadth]',
);
const breadth = args[0] === '--profile=breadth';
const profileDistance = args.length && !breadth ? Number(args[0].slice(-1)) : undefined;
const documents = 1_000;
const rounds = 6;
const fields = Array.from({ length: profileDistance === undefined ? 5 : 16 }, (_, i) => `field${i}`);
const families = [
	{ name: 'ascii', query: 'abcdef', terms: ['abcdef', 'abcxef', 'abxxef', 'abdcef', 'abcdefzzz'] },
	{ name: 'unicode', query: 'caféqxyz', terms: ['caféqxyz', 'caféqxzz', 'caféqwzz', 'caféqyxz', 'caféqxyzzzz'] },
];
const kinds = ['exact', 'one', 'two', 'swap', 'suffix'];
const fixtures = families.flatMap((family) =>
	family.terms.map((term, i) => ({
		id: `${family.name}-${kinds[i]}`,
		fields: Object.fromEntries(fields.map((field) => [field, `😀 ${term}`])),
	})),
);
const dictionaryTerm = (i) => {
	let suffix = '';
	const letters = breadth ? 'abcdfghijklmnpqrstuvwxyz' : 'abcdefghijklmnopqrstuvwxyz';
	for (let position = 0; position < 8; position++) {
		suffix += letters[i % letters.length];
		i = Math.floor(i / letters.length);
	}
	return `${breadth ? 'shaa' : 'qrst'}${suffix}`;
};
const cases = [];
for (const mode of ['fuzzy', 'fuzzy-prefix']) {
	for (const fuzzyDistance of [0, 1, 2]) {
		for (const fuzzyTranspositions of [false, true]) {
			for (const family of families) {
				const expected = [`${family.name}-exact`];
				if (fuzzyDistance >= 1) expected.push(`${family.name}-one`);
				if (fuzzyDistance >= 2) expected.push(`${family.name}-two`);
				if (fuzzyDistance >= (fuzzyTranspositions ? 1 : 2)) expected.push(`${family.name}-swap`);
				if (mode === 'fuzzy-prefix') expected.push(`${family.name}-suffix`);
				cases.push({
					name: `${family.name}/${mode}/distance-${fuzzyDistance}/transpositions-${fuzzyTranspositions}`,
					request: { text: family.query, mode, fuzzyDistance, fuzzyTranspositions, fields: ['field0'] },
					expected: expected.sort(),
				});
			}
		}
	}
}
const boundaryRequest = (fieldCount) => ({
	query: {
		operator: 'or',
		clauses: Array.from({ length: 16 }, () => ({
			text: 'abcdef',
			mode: 'fuzzy',
			fuzzyDistance: 2,
			fuzzyTranspositions: true,
			fields: fields.slice(0, fieldCount),
		})),
	},
});
const boundaryCase = {
	name: 'distance-2/256-construction-work-units',
	request: boundaryRequest(4),
	expected: ['ascii-exact', 'ascii-one', 'ascii-swap', 'ascii-two'],
};
const alphabet = 'abcdefghijklmnopqrstuvwxyz';
const longTerms = Array.from({ length: 16 }, (_, i) => {
	const rotated = alphabet.slice(i) + alphabet.slice(0, i);
	return rotated.repeat(2).slice(0, 38) + 'q';
});
assert(longTerms.every((term) => term.length === 39 && /^[a-z]+$/.test(term)));
assert.strictEqual(new Set(longTerms).size, 16);
const constructionCase = {
	name: `long-distance-${profileDistance ?? 2}/256-construction-work-units`,
	request: {
		query: {
			operator: 'or',
			clauses: longTerms.map((text) => ({
				text,
				mode: 'fuzzy',
				fuzzyDistance: profileDistance ?? 2,
				fuzzyTranspositions: true,
				fields: fields.slice(0, profileDistance === 1 ? 16 : 4),
			})),
		},
	},
	// Every indexed term is at least 27 characters shorter; two edits cannot bridge that gap.
	expected: [],
};
const breadthIds = Array.from({ length: documents }, (_, i) => `dictionary-${i}`).sort();
const breadthCases = [
	{
		name: 'breadth/exact-prefix/zero-expansions',
		request: { text: 'shoe', mode: 'prefix', fields: ['field0'] },
		expected: [],
	},
	...['fuzzy', 'fuzzy-prefix'].flatMap((mode) =>
		[1, 2].map((fuzzyDistance) => {
			const matchesAll = mode === 'fuzzy-prefix' && fuzzyDistance === 2;
			return {
				name: `breadth/${mode}/distance-${fuzzyDistance}`,
				request: { text: 'shoe', mode, fuzzyDistance, fuzzyTranspositions: true, fields: ['field0'] },
				expected: matchesAll ? breadthIds.slice(0, 100) : [],
				expectedTotal: matchesAll ? documents : 0,
				verifyUniverse: matchesAll,
			};
		}),
	),
];
const workloads = breadth
	? breadthCases
	: profileDistance === undefined
		? [constructionCase, ...cases, boundaryCase]
		: [constructionCase];
const root = await mkdtemp(path.join(tmpdir(), 'fulltext-query-controls-'));
const telemetry = {
	schemaVersion: 1,
	profileDistance: profileDistance ?? null,
	profile: breadth ? 'breadth' : profileDistance === undefined ? 'matrix' : `distance-${profileDistance}`,
	gates,
	root,
	environment: {
		platform: process.platform,
		architecture: process.arch,
		node: process.version,
		observedAt: new Date().toISOString(),
	},
	documents,
	fields: fields.length,
	longConstructionTerms: breadth ? null : 16,
	longConstructionTermCharacters: breadth ? null : 39,
	...(breadth
		? {
				breadth: {
					query: 'shoe',
					dictionaryPrefix: 'shaa',
					uniqueTerms: documents * 32,
					suffixExcludes: ['o', 'e'],
					exactPrefixExpansions: 0,
				},
			}
		: {}),
	rows: [],
	cancellation: [],
};
const checkpoint = async (name) => {
	const target = path.join(root, name);
	await writeFile(`${target}.pending`, JSON.stringify(telemetry, null, 2));
	await rename(`${target}.pending`, target);
};
const fingerprints = async () => {
	const addonPaths = Object.keys(createRequire(import.meta.url).cache).filter(
		(file) => path.basename(file).startsWith('fulltext.') && file.endsWith('.node'),
	);
	assert.strictEqual(addonPaths.length, 1, 'qualification must identify one loaded native addon');
	return Promise.all(
		[
			fileURLToPath(import.meta.url),
			fileURLToPath(new URL('../dist/native.js', import.meta.url)),
			fileURLToPath(new URL('../dist/codec.js', import.meta.url)),
			...addonPaths,
		].map(async (file) => {
			const hash = createHash('sha256');
			for await (const chunk of createReadStream(file)) hash.update(chunk);
			return { file: path.basename(file), sha256: hash.digest('hex') };
		}),
	);
};
let index;
let report;
let primaryError;
let sampler;
try {
	index = await openNativeFullTextIndex({
		path: path.join(root, 'index'),
		indexId: 'query-controls-qualification',
		generation: 'one',
		analyzer: 'english@2',
		stopWords: false,
		surfaceTerms: true,
		positions: true,
		fields: fields.map((name) => ({ name })),
		limits: {
			indexingThreads: 1,
			searchThreads: 4,
			writerMemoryBytes: 30_000_000,
			maxQueuedCommands: 128,
			maxQueuedBytes: 8 << 20,
			maxBatchBytes: 1 << 20,
			...(profileDistance === undefined ? {} : { maxQueryClauses: 4096 }),
		},
	});
	const upserts = breadth ? [] : [...fixtures];
	for (let i = upserts.length; i < documents; i++) {
		const text = Array.from({ length: 32 }, (_, j) => dictionaryTerm(i * 32 + j)).join(' ');
		upserts.push({ id: `dictionary-${i}`, fields: Object.fromEntries(fields.map((field) => [field, text])) });
	}
	for (let start = 0; start < upserts.length; start += 100) {
		await index.applyMutationBatch({ upserts: upserts.slice(start, start + 100) });
	}
	await index.publish('qualification:1');
	telemetry.runtime = await runtimeInfo();
	telemetry.policy = index.queryLimits;
	telemetry.fingerprints = await fingerprints();
	const rssBaseline = process.memoryUsage().rss;
	let peakRss = rssBaseline;
	telemetry.rssBaseline = rssBaseline;
	telemetry.peakRss = peakRss;
	let currentGroup;
	const sampleRss = () => {
		const rss = process.memoryUsage().rss;
		peakRss = Math.max(peakRss, rss);
		telemetry.peakRss = peakRss;
		if (currentGroup) currentGroup.peakRss = Math.max(currentGroup.peakRss, rss);
	};
	sampler = setInterval(sampleRss, 25);
	const run = async (entry, options) => {
		const start = performance.now();
		const pending = index.search({ ...entry.request, limit: 100, exactTotal: true }, options);
		const preparationMilliseconds = performance.now() - start;
		const result = await pending;
		assert.strictEqual(result.total, entry.expectedTotal ?? entry.expected.length, entry.name);
		assert.strictEqual(result.totalRelation, 'exact', entry.name);
		assert.deepStrictEqual(result.hits.map(({ id }) => id).sort(), entry.expected, entry.name);
		assert(
			result.hits.every(({ score }) => Number.isFinite(score)),
			entry.name,
		);
		sampleRss();
		return { preparationMilliseconds, requestMilliseconds: performance.now() - start };
	};
	const percentile = (values, fraction) => {
		const sorted = [...values].sort((a, b) => a - b);
		return sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)];
	};
	const rows = telemetry.rows;
	for (const entry of workloads) {
		console.error(`query-controls: ${entry.name}`);
		if (entry.verifyUniverse) {
			const page = await index.searchPage({ ...entry.request, limit: documents });
			assert.deepStrictEqual(page.hits.map(({ id }) => id).sort(), breadthIds, `${entry.name}: complete universe`);
			assert.strictEqual(page.continuation, undefined);
			telemetry.breadth.completeUniverseVerified = true;
		}
		if (cases.includes(entry)) {
			const trace = await index.traceMatches(entry.request, fixtures);
			assert.strictEqual(trace.complete, true, entry.name);
			assert.deepStrictEqual(trace.records.map(({ id }) => id).sort(), entry.expected, `${entry.name}: trace`);
		}
		for (const concurrency of [1, 4, 8]) {
			const rssBefore = process.memoryUsage().rss;
			currentGroup = { peakRss: rssBefore };
			telemetry.activeGroup = { case: entry.name, concurrency, rssBefore };
			await run(entry);
			const measurements = [];
			const cpuStart = process.cpuUsage();
			const started = performance.now();
			const measuredRounds = cases.includes(entry) ? rounds : 3;
			for (let round = 0; round < measuredRounds; round++) {
				measurements.push(...(await Promise.all(Array.from({ length: concurrency }, () => run(entry)))));
			}
			const cpu = process.cpuUsage(cpuStart);
			const requestTimes = measurements.map(({ requestMilliseconds }) => requestMilliseconds);
			const preparationTimes = measurements.map(({ preparationMilliseconds }) => preparationMilliseconds);
			const p99 = percentile(requestTimes, 0.99);
			rows.push({
				case: entry.name,
				concurrency,
				requests: measurements.length,
				elapsedMilliseconds: performance.now() - started,
				p50: percentile(requestTimes, 0.5),
				p95: percentile(requestTimes, 0.95),
				p99,
				preparationP50: percentile(preparationTimes, 0.5),
				preparationP99: percentile(preparationTimes, 0.99),
				cpuMicroseconds: cpu.user + cpu.system,
				rssBefore,
				rssAfter: process.memoryUsage().rss,
				peakRss: currentGroup.peakRss,
				peakRssGrowthFromBaseline: peakRss - rssBaseline,
			});
			currentGroup = undefined;
			telemetry.activeGroup = null;
			await checkpoint('qualification-progress.json');
			assert(p99 <= gates.maxP99Milliseconds, `${entry.name}: p99 ${p99}ms exceeds gate`);
		}
	}
	if (profileDistance === undefined && !breadth) {
		await assert.rejects(
			index.search({ ...boundaryRequest(5), limit: 100 }),
			(error) => error.code === 'E_INVALID_ARGUMENT' && /construction work units/.test(error.message),
		);
	}
	const cancellation = telemetry.cancellation;
	for (const entry of workloads) {
		const outcomes = { case: entry.name, attempts: 0, timeouts: 0, successes: 0, recovered: false };
		cancellation.push(outcomes);
		for (let attempt = 0; attempt < 3; attempt++) {
			outcomes.attempts++;
			try {
				await run(entry, { remainingBudgetMilliseconds: 1 });
				outcomes.successes++;
			} catch (error) {
				assert.strictEqual(error.code, 'E_TIMEOUT', entry.name);
				outcomes.timeouts++;
			}
		}
		await run(entry);
		outcomes.recovered = true;
		await checkpoint('qualification-progress.json');
	}
	const cancellationObserved = cancellation.some(({ timeouts }) => timeouts > 0);
	telemetry.cancellationObserved = cancellationObserved;
	if (!cancellationObserved)
		console.error('query-controls: no timeout observed; recovery checked after successful budgeted requests only');
	const recoveryCase = breadth
		? breadthCases.find(({ verifyUniverse }) => verifyUniverse)
		: profileDistance === undefined
			? boundaryCase
			: constructionCase;
	await Promise.all(Array.from({ length: 8 }, () => run(recoveryCase)));
	assert.strictEqual(index.status().searchQueuedCommands, 0n);
	assert.strictEqual(index.status().searchQueuedBytes, 0n);
	clearInterval(sampler);
	sampleRss();
	telemetry.rssGrowthBytes = peakRss - rssBaseline;
	telemetry.gateResults = {
		latencyPassed: rows.every(({ p99 }) => p99 <= gates.maxP99Milliseconds),
		rssPassed: peakRss - rssBaseline <= gates.maxRssGrowthBytes,
	};
	await checkpoint('qualification-progress.json');
	assert(peakRss - rssBaseline <= gates.maxRssGrowthBytes, 'sampled RSS growth exceeds gate');
	report = {
		schemaVersion: 1,
		environment: {
			platform: process.platform,
			architecture: process.arch,
			node: process.version,
			observedAt: new Date().toISOString(),
		},
		runtime: await runtimeInfo(),
		documents,
		rounds,
		fields: fields.length,
		dictionaryTermsPerDocument: 32,
		policy: index.queryLimits,
		gates,
		rssBaseline,
		peakRss,
		rows,
		cancellation,
		cancellationObserved,
		profileDistance: profileDistance ?? null,
		profile: telemetry.profile,
		...(breadth ? { breadth: telemetry.breadth } : {}),
		longConstructionTerms: breadth ? null : longTerms.length,
		longConstructionTermCharacters: breadth ? null : 39,
		constructionBudget: {
			maximumWorkUnits: 256,
			distance2Charge: 4,
			boundaryAutomata: breadth ? null : profileDistance === 1 ? 256 : 64,
			excessRejected: profileDistance === undefined && !breadth ? true : null,
		},
		note: 'Finite local optimized synthetic qualification, not production evidence. No forced GC. Preparation measures synchronous facade dispatch/encoding, not native-only construction; request latency includes native work and queueing. Sampled RSS is not a hard peak or retained-memory proof. CPU is process-wide. One-millisecond attempts may succeed; all successful attempts must match the independent oracle. Recovery follows every cancellation group.',
	};
} catch (error) {
	primaryError = error;
	telemetry.failure = { name: error.name, code: error.code, message: error.message };
	try {
		await checkpoint('qualification-failure.json');
	} catch (reportError) {
		primaryError = new AggregateError([error, reportError], 'Qualification and diagnostic reporting failed', {
			cause: error,
		});
	}
} finally {
	clearInterval(sampler);
}
const cleanupErrors = [];
try {
	await closeIndex(index, { mode: 'rollback' });
} catch (error) {
	cleanupErrors.push(error);
}
if (cleanupErrors.length || primaryError) {
	telemetry.cleanupErrors = cleanupErrors.map(({ name, code, message }) => ({ name, code, message }));
	try {
		await checkpoint('qualification-failure.json');
	} catch (error) {
		cleanupErrors.push(error);
	}
	throw new AggregateError(
		[...(primaryError ? [primaryError] : []), ...cleanupErrors],
		`Qualification failed; artifacts retained at ${root}`,
		{ cause: primaryError },
	);
}
report.fingerprints = telemetry.fingerprints;
await rm(root, { recursive: true });
console.log(JSON.stringify(report, null, 2));
