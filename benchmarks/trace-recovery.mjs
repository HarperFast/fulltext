import assert from 'node:assert';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { cpus, release, tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import { openNativeFullTextIndex, runtimeInfo } from '../dist/native.js';
import { loadAddon, platformTriple } from '../dist/load-addon.js';
import { closeIndex } from './metadata-cleanup.mjs';

const args = process.argv.slice(2);
assert(args.length <= 1 && args.every((argument) => argument === '--smoke'), 'expected optional --smoke');
assert.strictEqual(process.env.FULLTEXT_PREFER_LOCAL_BUILD, '1', 'run through scripts/run-local.mjs');
assert.strictEqual(loadAddon().__testDelayNextExpensiveSearch, undefined, 'use a production native build');
const rounds = args.includes('--smoke') ? 1 : 5;
const warmupRounds = args.includes('--smoke') ? 0 : 1;
const sharedBudgetMilliseconds = 30_000;
const policies = ['pages', 'split-on-incomplete', 'singletons'];
const leaf = (text, fields = ['title']) => ({ text, fields });
const dense = (id, words) => ({ id, fields: { title: Array(words).fill('amber').join(' ') } });
const manyLeaves = { query: { operator: 'or', clauses: Array.from({ length: 64 }, () => leaf('amber')) } };
const datasets = [
	{
		name: 'easy-batch',
		request: leaf('amber'),
		records: Array.from({ length: 24 }, (_, index) => ({
			id: `easy-${index}`,
			fields: { title: 'trail amber stone' },
		})),
	},
	{
		name: 'late-nonmatches',
		request: leaf('amber'),
		records: Array.from({ length: 24 }, (_, index) => ({
			id: `late-${index}`,
			fields: { title: index < 3 ? 'amber' : 'zircon stone' },
		})),
	},
	{
		name: 'spans-600x3',
		request: leaf('amber'),
		records: [dense('dense-a', 600), dense('dense-b', 600), dense('dense-c', 600)],
	},
	{ name: 'singleton-1100', request: leaf('amber'), records: [dense('oversized', 1100)] },
	{
		name: 'multivalue-unicode-boolean',
		request: {
			query: {
				operator: 'and',
				clauses: [
					{ operator: 'or', clauses: [leaf('amber'), leaf('quartz', ['body'])] },
					{ operator: 'not', clause: leaf('insulated', ['body']) },
				],
			},
		},
		records: [
			{ id: 'unicode-a', fields: { title: ['🍂 Amber café', 'Ruby amber 😀'], body: ['QUARTZ trail', 'warm wool'] } },
			{ id: 'unicode-b', fields: { title: ['amber boot'], body: ['insulated'] } },
			{ id: 'unicode-c', fields: { title: ['quartz'], body: ['trail'] } },
			{ id: 'unicode-d', fields: { title: ['😀 amber'], body: ['trail quartz'] } },
		],
	},
	{
		name: 'work-bound-late',
		request: manyLeaves,
		records: Array.from({ length: 4 }, (_, index) => ({
			id: `work-${index}`,
			fields: { title: (index === 3 ? 'amber ' : 'zz ') + 'zz '.repeat(39_999) },
		})),
	},
];
const sourceBytes = (records) =>
	records.reduce(
		(total, record) =>
			total +
			Object.values(record.fields).reduce(
				(bytes, values) =>
					bytes + (Array.isArray(values) ? values : [values]).reduce((sum, value) => sum + Buffer.byteLength(value), 0),
				0,
			),
		0,
	);
for (const dataset of datasets) {
	assert(dataset.records.length > 0 && dataset.records.length <= 100);
	assert(sourceBytes(dataset.records) <= 1 << 20, `${dataset.name} exceeds source admission`);
	assert.strictEqual(new Set(dataset.records.map(({ id }) => id)).size, dataset.records.length);
}
const canonicalRecord = (record) =>
	record === undefined
		? undefined
		: {
				id: record.id,
				values: record.values
					.map((value) => ({
						field: value.field,
						valueIndex: value.valueIndex,
						spans: value.spans
							.map(({ start, end }) => ({ start, end }))
							.sort((a, b) => a.start - b.start || a.end - b.end),
					}))
					.sort((a, b) => a.field.localeCompare(b.field) || a.valueIndex - b.valueIndex),
			};
const digest = (value) => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const fingerprint = async (file) => {
	const hash = createHash('sha256');
	for await (const chunk of createReadStream(file)) hash.update(chunk);
	return { file: path.basename(file), sha256: hash.digest('hex') };
};
const repository = fileURLToPath(new URL('../', import.meta.url));
const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-trace-recovery-'));
let index;
let primaryFailure;
let observedTerminalError = false;

async function runPolicy(dataset, policy) {
	const started = performance.now();
	const deadline = started + sharedBudgetMilliseconds;
	const rssStart = process.memoryUsage().rss;
	let peakRss = rssStart;
	const sampleRss = () => {
		peakRss = Math.max(peakRss, process.memoryUsage().rss);
	};
	const timer = setInterval(sampleRss, 5);
	const accepted = [];
	const visits = new Map();
	const callLatenciesMilliseconds = [];
	const budgets = [];
	let submittedInputs = 0;
	let submittedSourceBytes = 0;
	let resubmittedInputs = 0;
	let resubmittedSourceBytes = 0;
	let pageConsumedInputs = 0;
	let callCount = 0;
	let failure;
	const maximumCalls = policy === 'split-on-incomplete' ? dataset.records.length * 2 - 1 : dataset.records.length;
	const accept = (records, states, matches) => {
		const byId = new Map(matches.map((record) => [record.id, canonicalRecord(record)]));
		assert.strictEqual(byId.size, matches.length);
		for (let position = 0; position < records.length; position++) {
			const record = records[position];
			assert.strictEqual(
				record.id,
				dataset.records[accepted.length].id,
				'consumer progress must remain an ordered prefix',
			);
			const state = states[position];
			assert.strictEqual(state.id, record.id);
			accepted.push({ ...state, record: byId.get(record.id) });
			byId.delete(record.id);
		}
		assert.strictEqual(byId.size, 0, 'output must not escape consumed inputs');
	};
	const submit = async (records, page) => {
		assert(++callCount <= maximumCalls, 'recovery iteration cap exceeded');
		const remaining = Math.min(Math.floor(deadline - performance.now()), (budgets.at(-1) ?? Infinity) - 1);
		if (remaining <= 0) {
			const error = new Error('shared consumer budget exhausted before submission');
			error.code = 'CONSUMER_BUDGET_EXHAUSTED';
			throw error;
		}
		assert(budgets.length === 0 || remaining < budgets.at(-1));
		budgets.push(remaining);
		submittedInputs += records.length;
		submittedSourceBytes += sourceBytes(records);
		for (const record of records) {
			if (visits.has(record.id)) {
				resubmittedInputs++;
				resubmittedSourceBytes += sourceBytes([record]);
			}
			visits.set(record.id, (visits.get(record.id) ?? 0) + 1);
		}
		const start = performance.now();
		try {
			return await index[page ? 'traceMatchesPage' : 'traceMatches'](dataset.request, records, {
				remainingBudgetMilliseconds: remaining,
			});
		} finally {
			callLatenciesMilliseconds.push(performance.now() - start);
			sampleRss();
		}
	};
	const acceptLegacy = (records, result) => {
		const ids = new Set(result.records.map(({ id }) => id));
		assert(result.complete || records.length === 1, 'only singleton incomplete output is terminally accepted');
		accept(
			records,
			records.map(({ id }) => ({
				id,
				membership: ids.has(id) ? 'match' : result.complete ? 'no-match' : 'unknown',
				analysisComplete: result.complete ? true : null,
				spansComplete: result.complete ? true : null,
				legacyComplete: result.complete,
			})),
			result.records,
		);
	};
	try {
		if (policy === 'pages') {
			while (accepted.length < dataset.records.length) {
				const remaining = dataset.records.slice(accepted.length);
				const result = await submit(remaining, true);
				assert(Number.isInteger(result.nextRecord) && result.nextRecord > 0 && result.nextRecord <= remaining.length);
				assert.strictEqual(result.states.length, result.nextRecord);
				for (const state of result.states) {
					assert(['match', 'no-match', 'unknown'].includes(state.membership));
					assert.strictEqual(typeof state.analysisComplete, 'boolean');
					assert.strictEqual(typeof state.spansComplete, 'boolean');
				}
				assert.deepStrictEqual(
					result.records.map(({ id }) => id),
					result.states.filter(({ membership }) => membership === 'match').map(({ id }) => id),
				);
				assert.strictEqual(
					result.complete,
					result.nextRecord === remaining.length &&
						result.states.every(({ analysisComplete, spansComplete }) => analysisComplete && spansComplete),
				);
				const before = accepted.length;
				accept(remaining.slice(0, result.nextRecord), result.states, result.records);
				assert(accepted.length > before);
				pageConsumedInputs += result.nextRecord;
			}
		} else if (policy === 'singletons') {
			for (const record of dataset.records) acceptLegacy([record], await submit([record], false));
		} else {
			const stack = [dataset.records];
			while (stack.length > 0) {
				const records = stack.pop();
				const result = await submit(records, false);
				if (result.complete || records.length === 1) acceptLegacy(records, result);
				else {
					const middle = Math.floor(records.length / 2);
					assert(middle > 0 && middle < records.length, 'split must strictly shrink both children');
					stack.push(records.slice(middle), records.slice(0, middle));
				}
			}
		}
	} catch (error) {
		if (error instanceof assert.AssertionError) throw error;
		failure = {
			name: error.name,
			code: error.code ?? null,
			message: error.message,
			acceptedBeforeError: accepted.length,
			retried: false,
		};
	} finally {
		clearInterval(timer);
		sampleRss();
	}
	return {
		accepted,
		row: {
			policy,
			calls: callLatenciesMilliseconds.length,
			consumerProcessedInputs: accepted.length,
			consumerFinished: !failure && accepted.length === dataset.records.length,
			evidenceComplete:
				!failure &&
				accepted.length === dataset.records.length &&
				accepted.every((state) =>
					policy === 'pages' ? state.analysisComplete && state.spansComplete : state.legacyComplete,
				),
			pageConsumedInputs: policy === 'pages' ? pageConsumedInputs : null,
			submittedInputs,
			resubmittedInputs,
			sourceBytes: sourceBytes(dataset.records),
			submittedSourceBytes,
			resubmittedSourceBytes,
			nativeProcessedInputs: null,
			nativeReprocessedInputs: null,
			callLatenciesMilliseconds,
			remainingBudgetsMilliseconds: budgets,
			consumerWallMilliseconds: performance.now() - started,
			rssStart,
			peakRss,
			retainedInputIdsBeforeTerminalError: failure ? accepted.map(({ id }) => id) : [],
			retainedRecordsBeforeTerminalError: failure ? accepted.flatMap(({ record }) => (record ? [record] : [])) : [],
			terminalError: failure ?? null,
			consumedIncompleteInputs: accepted
				.filter(
					(state) =>
						state.legacyComplete === false || state.analysisComplete === false || state.spansComplete === false,
				)
				.map(({ id, membership, analysisComplete, spansComplete }) => ({
					id,
					membership,
					analysisComplete,
					spansComplete,
				})),
			acceptedEvidenceSha256: digest(accepted.map(({ id, record }) => ({ id, record: record ?? null }))),
			states: accepted.map(({ record: _record, ...state }) => state),
		},
	};
}

try {
	index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'trace-recovery-qualification',
		generation: 'one',
		analyzer: 'english@2',
		surfaceTerms: true,
		fields: [{ name: 'title' }, { name: 'body' }],
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 4 << 20,
			maxBatchBytes: 1 << 20,
		},
	});
	assert.strictEqual(typeof index.traceMatchesPage, 'function', 'build the recovery API before qualification');
	const files = [
		'package-lock.json',
		'Cargo.lock',
		'src/engine.rs',
		'src/protocol.rs',
		'src/native.rs',
		'ts/native.ts',
		'ts/codec.ts',
		'dist/native.js',
		`fulltext.${platformTriple()}.node`,
	];
	console.log(
		JSON.stringify({
			type: 'environment',
			benchmark: 'trace-recovery',
			environment: {
				node: process.version,
				platform: process.platform,
				architecture: process.arch,
				osRelease: release(),
				cpu: cpus()[0]?.model,
				runtime: await runtimeInfo(),
			},
			gitHead: execFileSync('git', ['-C', repository, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
			fingerprints: await Promise.all(
				[fileURLToPath(import.meta.url), ...files.map((file) => path.join(repository, file))].map(fingerprint),
			),
			method: {
				rounds,
				warmupRounds,
				sharedBudgetMilliseconds,
				ordering:
					'rotate the first policy each round; each policy receives the same original records and a fresh shared deadline',
				index: 'empty native handle; supplied-source projection only, not catalog or index-size qualification',
				memory:
					'sampled whole-process RSS, including retained inputs and comparison outcomes; not isolated allocations or a per-request native peak',
				latency:
					'consumer wall time includes correctness checks and recovery bookkeeping; per-call latency covers the awaited public API',
				inputCounts:
					'submissions and repeated-ID submissions are observed; accepted consumer inputs and page statuses are progress, not native tokenization/provisional work',
				terminalPolicy:
					'consume incomplete singletons once; retain proven evidence and explicit unknown/partial state; never retry an error or expired shared budget; preserve prior accepted inputs',
				legacyState:
					'complete legacy singletons prove membership/evidence; incomplete legacy singletons cannot distinguish incomplete analysis from incomplete spans',
			},
		}),
	);
	for (const dataset of datasets) {
		console.log(
			JSON.stringify({
				type: 'dataset',
				name: dataset.name,
				inputs: dataset.records.length,
				sourceBytes: sourceBytes(dataset.records),
				inputSha256: digest(dataset.records),
				querySha256: digest(dataset.request),
			}),
		);
		for (let round = -warmupRounds; round < rounds; round++) {
			const outcomes = new Map();
			const first = (round + warmupRounds) % policies.length;
			for (let position = 0; position < policies.length; position++) {
				const policy = policies[(first + position) % policies.length];
				const outcome = await runPolicy(dataset, policy);
				if (outcome.row.terminalError) observedTerminalError = true;
				else assert.strictEqual(outcome.accepted.length, dataset.records.length);
				outcomes.set(policy, outcome);
			}
			const singleton = new Map(outcomes.get('singletons').accepted.map((state) => [state.id, state]));
			let completeSingletonComparisons = 0;
			for (const policy of ['pages', 'split-on-incomplete']) {
				for (const actual of outcomes.get(policy).accepted) {
					const expected = singleton.get(actual.id);
					if (!expected?.legacyComplete) continue;
					assert.strictEqual(
						actual.membership,
						expected.membership,
						`${dataset.name}/${policy}/${actual.id} membership`,
					);
					assert.deepStrictEqual(
						actual.record,
						expected.record,
						`${dataset.name}/${policy}/${actual.id} source evidence`,
					);
					if (policy === 'pages') {
						assert.strictEqual(actual.analysisComplete, true);
						assert.strictEqual(actual.spansComplete, true);
					}
					completeSingletonComparisons++;
				}
			}
			if (dataset.name === 'singleton-1100' && !outcomes.get('pages').row.terminalError) {
				const page = outcomes.get('pages');
				assert.strictEqual(page.row.calls, 1);
				assert.strictEqual(page.accepted[0].membership, 'match');
				assert.strictEqual(page.accepted[0].analysisComplete, true);
				assert.strictEqual(page.accepted[0].spansComplete, false);
				assert.strictEqual(
					page.accepted[0].record.values.reduce((count, value) => count + value.spans.length, 0),
					1024,
				);
			}
			if (round >= 0 || [...outcomes.values()].some(({ row }) => row.terminalError))
				for (const policy of policies)
					console.log(
						JSON.stringify({
							type: round < 0 ? 'warmup-terminal-sample' : 'sample',
							dataset: dataset.name,
							round,
							completeSingletonComparisons,
							...outcomes.get(policy).row,
						}),
					);
		}
	}
	if (observedTerminalError)
		throw new Error(
			'Qualification encountered terminal errors; prior accepted progress and evidence fingerprints are retained in sample rows',
		);
} catch (error) {
	primaryFailure = error;
	throw error;
} finally {
	let closeFailure;
	try {
		await closeIndex(index, { mode: 'rollback' });
	} catch (error) {
		closeFailure = error;
	}
	if (primaryFailure || closeFailure) console.error(`Retained trace recovery index after failure: ${directory}`);
	else await rm(directory, { recursive: true, force: true });
	if (!primaryFailure && closeFailure) throw closeFailure;
}
