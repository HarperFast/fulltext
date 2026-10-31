import assert from 'node:assert';
import { spawn } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath, pathToFileURL } from 'node:url';

const options = Object.fromEntries(
	process.argv.slice(2).map((arg) => {
		assert(arg.startsWith('--') && arg.includes('='), 'Use --name=value arguments');
		const separator = arg.indexOf('=');
		return [arg.slice(2, separator), arg.slice(separator + 1)];
	}),
);
const integer = (name, fallback) => {
	const value = Number(options[name] ?? fallback);
	assert(Number.isSafeInteger(value) && value > 0, `${name} must be a positive safe integer`);
	return value;
};
const percentile = (values, fraction) => values.toSorted((a, b) => a - b)[Math.ceil(values.length * fraction) - 1];
const median = (values) => percentile(values, 0.5);
const serialize = (value) => JSON.stringify(value, (_, item) => (typeof item === 'bigint' ? item.toString() : item), 2);

const supportedOperations = ['ordinary', 'ordinary-cheap', 'expensive'];
const measurementVersion = 2;
const validationWindow = integer('validation-window', 1_024);
assert(validationWindow <= 65_536, 'validation-window must not exceed 65536 results');
const pollIntervalMilliseconds = integer('poll-interval-ms', 1_000);
const minimumPollsPerHandle = integer('minimum-polls-per-handle', 10);
const baselineProfile = options['baseline-profile'] ?? 'none';
assert(['none', 'match'].includes(baselineProfile), 'baseline-profile must be none or match');

if (options.child) {
	assert(supportedOperations.includes(options.operation), 'Unknown operation');
	const cheapOrdinary = options.operation === 'ordinary-cheap';
	assert(!cheapOrdinary || integer('documents', 2_000) >= 8, 'ordinary-cheap requires at least eight documents');
	const { openNativeFullTextIndex, runtimeInfo } = await import(pathToFileURL(path.resolve(options.module)).href);
	const root = await mkdtemp(path.join(os.tmpdir(), 'fulltext-observability-'));
	const indexes = [];
	const documentCount = integer('documents', 2_000);
	const queryCount = integer('queries', 4_000);
	const callers = integer('callers', 1);
	const indexCount = integer('indexes', 1);
	const profile = options.profile;
	const pollTimerDelayMilliseconds = Math.max(1, Math.floor(pollIntervalMilliseconds / indexCount));
	const latencies = [];
	const publicationLatencies = [];
	const mutationPublicationLatencies = [];
	let pollingError;
	let poll;
	const polls = Array.from({ length: indexCount }, () => ({
		count: 0,
		durationsMilliseconds: [],
	}));
	let clean = false;
	const setupStart = performance.now();
	const setupCpuStart = process.cpuUsage();
	try {
		for (let i = 0; i < indexCount; i++) {
			const index = await openNativeFullTextIndex({
				path: path.join(root, `index-${i}`),
				indexId: `index-${i}`,
				generation: 'qualification-v1',
				fields: [{ name: 'text' }],
				analyzer: 'english@2',
				surfaceTerms: true,
				limits: {
					indexingThreads: 1,
					searchThreads: 4,
					writerMemoryBytes: 15_000_000,
					maxQueuedCommands: 128,
					maxQueuedBytes: 8 << 20,
					maxBatchBytes: 1 << 20,
				},
			});
			indexes.push(index);
			if (profile !== 'baseline')
				index.configureTelemetry({
					histograms: profile === 'on',
					snapshotBytes: profile === 'bytes',
				});
			await index.applyMutationBatch({
				upserts: Array.from({ length: documentCount }, (_, id) => ({
					id: String(id),
					fields: { text: `catalog common category${id % 20} product${id}` },
				})),
			});
			await index.publish('initial');
		}
		const indexingMilliseconds = performance.now() - setupStart;
		const indexingCpu = process.cpuUsage(setupCpuStart);
		const request = {
			text: cheapOrdinary ? 'product7' : 'catalog',
			mode: options.operation === 'expensive' ? 'prefix' : 'any',
			limit: 10,
			exactTotal: options.operation === 'expensive',
		};
		const queryClass = options.operation === 'expensive' ? 'expensiveSearch' : 'search';
		const otherQueryClass = queryClass === 'search' ? 'expensiveSearch' : 'search';
		const expectedIds = cheapOrdinary
			? ['7']
			: Array.from({ length: documentCount }, (_, id) => String(id))
					.sort()
					.slice(0, 10);
		const operationCounts = () =>
			indexes.map((index) => {
				const snapshot = index.telemetry();
				assert(snapshot.available, 'Candidate telemetry is required to verify native query classification');
				return snapshot.native.operations;
			});
		const beforeWarmup = profile === 'baseline' ? undefined : operationCounts();
		const validate = (result) => {
			assert.strictEqual(
				result.total,
				cheapOrdinary ? 1 : request.exactTotal ? documentCount : Math.min(documentCount, 10),
			);
			assert.strictEqual(
				result.totalRelation,
				cheapOrdinary || request.exactTotal || documentCount <= 10 ? 'exact' : 'lower-bound',
			);
			assert.strictEqual(result.hits.length, expectedIds.length);
			assert.strictEqual(new Set(result.hits.map((hit) => hit.id)).size, expectedIds.length);
			assert.deepStrictEqual(
				result.hits.map((hit) => hit.id),
				expectedIds,
			);
			for (const hit of result.hits) {
				assert(Number.isSafeInteger(Number(hit.id)) && Number(hit.id) >= 0 && Number(hit.id) < documentCount);
				assert(Number.isFinite(hit.score));
			}
		};
		const check = async (index) => validate(await index.search(request));
		for (let i = 0; i < 100; i++) await check(indexes[i % indexes.length]);
		if (beforeWarmup) {
			const afterWarmup = operationCounts();
			const selected = afterWarmup.reduce(
				(total, operations, i) => total + operations[queryClass].succeeded - beforeWarmup[i][queryClass].succeeded,
				0n,
			);
			const other = afterWarmup.reduce(
				(total, operations, i) =>
					total + operations[otherQueryClass].attempted - beforeWarmup[i][otherQueryClass].attempted,
				0n,
			);
			assert.strictEqual(selected, 100n, 'Warmup must use the selected native search class');
			assert.strictEqual(other, 0n, 'Warmup must not use the other native search class');
		}
		if (profile === 'polling') {
			let nextHandle = 0;
			const collect = () => {
				const handle = nextHandle++ % indexes.length;
				const before = performance.now();
				try {
					indexes[handle].telemetry();
				} catch (error) {
					pollingError ??= error;
				} finally {
					polls[handle].count++;
					polls[handle].durationsMilliseconds.push(performance.now() - before);
				}
				poll = setTimeout(collect, pollTimerDelayMilliseconds);
			};
			poll = setTimeout(collect, pollTimerDelayMilliseconds);
		}
		const cpuStart = process.cpuUsage();
		const rssStart = process.memoryUsage().rss;
		let peakRss = rssStart;
		const rssTimer = setInterval(() => {
			peakRss = Math.max(peakRss, process.memoryUsage().rss);
		}, 25);
		const start = performance.now();
		let queryMilliseconds = 0;
		const queryCpu = { user: 0, system: 0 };
		const queryEventLoop = { idle: 0, active: 0 };
		let queryWindows = 0;
		try {
			for (let first = 0; first < queryCount; first += validationWindow) {
				const end = Math.min(first + validationWindow, queryCount);
				const results = new Array(end - first);
				let next = first;
				const windowCpu = process.cpuUsage();
				const windowEventLoop = performance.eventLoopUtilization();
				const windowStart = performance.now();
				const completed = await Promise.allSettled(
					Array.from({ length: Math.min(callers, results.length) }, async () => {
						while (next < end) {
							const id = next++;
							const before = performance.now();
							const result = await indexes[id % indexes.length].search(request);
							const elapsed = performance.now() - before;
							results[id - first] = { result, elapsed };
						}
					}),
				);
				const failure = completed.find((result) => result.status === 'rejected');
				if (failure) throw failure.reason;
				queryMilliseconds += performance.now() - windowStart;
				const cpu = process.cpuUsage(windowCpu);
				const eventLoop = performance.eventLoopUtilization(windowEventLoop);
				queryCpu.user += cpu.user;
				queryCpu.system += cpu.system;
				queryEventLoop.idle += eventLoop.idle;
				queryEventLoop.active += eventLoop.active;
				queryWindows++;
				for (const { result, elapsed } of results) {
					validate(result);
					latencies.push(elapsed);
				}
			}
			for (let i = 0; i < 20; i++) {
				const index = indexes[i % indexes.length];
				const cycleStart = performance.now();
				await index.applyMutationBatch({
					upserts: [{ id: '0', fields: { text: `catalog common changed${i}` } }],
				});
				const before = performance.now();
				await index.publish(`publication-${i}`);
				publicationLatencies.push(performance.now() - before);
				mutationPublicationLatencies.push(performance.now() - cycleStart);
			}
		} finally {
			clearInterval(rssTimer);
		}
		clearTimeout(poll);
		const elapsedMilliseconds = performance.now() - start;
		const cpu = process.cpuUsage(cpuStart);
		if (pollingError) throw pollingError;
		for (const index of indexes) await check(index);
		console.log(
			serialize({
				measurementVersion,
				profile,
				operation: options.operation,
				collectorScenario: cheapOrdinary ? 'single-hit' : 'tie-heavy',
				queryClass,
				queryClassVerification: beforeWarmup ? 'native-operation-counters' : 'matching-request-classification',
				request,
				callers,
				indexCount,
				documentCount,
				queryCount,
				latencyScope: 'search-api-completion-before-validation',
				queryPhaseScope: 'sum-of-drained-query-windows-excluding-validation',
				validationWindow,
				queryWindows,
				queryCpuMicroseconds: queryCpu.user + queryCpu.system,
				queryEventLoopUtilization: {
					...queryEventLoop,
					utilization: queryEventLoop.active / (queryEventLoop.active + queryEventLoop.idle) || 0,
				},
				mixedPhaseScope: 'queries-validation-and-20-mutation-publication-cycles',
				polling: {
					enabled: profile === 'polling',
					intervalPerHandleMilliseconds: pollIntervalMilliseconds,
					timerTurnDelayMilliseconds: pollTimerDelayMilliseconds,
					minimumPollsPerHandle,
					samplesSufficient: profile === 'polling' ? polls.every(({ count }) => count >= minimumPollsPerHandle) : null,
					schedule: 'one-handle-per-timer-turn-staggered',
					histograms: profile === 'on',
					snapshotBytes: profile === 'bytes',
					handles: polls,
				},
				elapsedMilliseconds,
				queryMilliseconds,
				throughput: (queryCount * 1_000) / queryMilliseconds,
				p50Milliseconds: median(latencies),
				p99Milliseconds: percentile(latencies, 0.99),
				publicationP50Milliseconds: median(publicationLatencies),
				publicationP99Milliseconds: percentile(publicationLatencies, 0.99),
				mutationPublicationP50Milliseconds: median(mutationPublicationLatencies),
				mutationPublicationP99Milliseconds: percentile(mutationPublicationLatencies, 0.99),
				indexingMilliseconds,
				indexingCpuMicroseconds: indexingCpu.user + indexingCpu.system,
				cpuMicroseconds: cpu.user + cpu.system,
				rssStartBytes: rssStart,
				rssEndBytes: process.memoryUsage().rss,
				peakRssBytes: Math.max(peakRss, process.memoryUsage().rss),
				runtime: await runtimeInfo(),
				telemetry: profile === 'baseline' ? undefined : indexes.map((index) => index.telemetry()),
			}),
		);
		clean = true;
	} finally {
		clearTimeout(poll);
		let closeError;
		for (const index of indexes) {
			try {
				const result = await index.close();
				if (result.cleanupError) closeError ??= result.cleanupError;
			} catch (error) {
				closeError ??= error;
			}
		}
		if (!closeError) await rm(root, { recursive: true, force: true });
		else if (clean) throw closeError;
	}
} else {
	assert(
		options.baseline && options.candidate && options.output,
		'Required: --baseline=/absolute/dist/native.js --candidate=/absolute/dist/native.js --output=/absolute/results.json',
	);
	const pairs = integer('pairs', 7);
	const operations = (options.operations ?? supportedOperations.join(',')).split(',');
	assert(
		operations.length > 0 &&
			new Set(operations).size === operations.length &&
			operations.every((operation) => supportedOperations.includes(operation)),
		'operations must contain distinct known operation names',
	);
	assert(
		!operations.includes('ordinary-cheap') || integer('documents', 2_000) >= 8,
		'ordinary-cheap requires at least eight documents',
	);
	const profiles = (options.profiles ?? 'off,on,polling,bytes').split(',');
	assert(
		profiles.every((profile) => ['off', 'on', 'polling', 'bytes'].includes(profile)),
		'Unknown profile',
	);
	const samples = [];
	const run = (configuration) =>
		new Promise((resolve, reject) => {
			const child = spawn(
				process.execPath,
				[fileURLToPath(import.meta.url), ...Object.entries(configuration).map(([key, value]) => `--${key}=${value}`)],
				{
					env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' },
					stdio: ['ignore', 'pipe', 'pipe'],
					timeout: 180_000,
				},
			);
			let stdout = '';
			let stderr = '';
			child.stdout.on('data', (data) => {
				stdout += data;
			});
			child.stderr.on('data', (data) => {
				stderr += data;
			});
			child.on('error', reject);
			child.on('close', (code, signal) => {
				if (code !== 0) reject(new Error(`Qualification child failed (${code ?? signal}): ${stderr}`));
				else {
					try {
						resolve(JSON.parse(stdout));
					} catch (error) {
						reject(error);
					}
				}
			});
		});
	for (const callers of [1, 4])
		for (const indexes of [1, 2])
			for (const operation of operations)
				for (const profile of profiles) {
					const configuration = {
						child: 1,
						'validation-window': validationWindow,
						'poll-interval-ms': pollIntervalMilliseconds,
						'minimum-polls-per-handle': minimumPollsPerHandle,
						callers,
						indexes,
						operation,
						queries: integer('queries', 4_000),
						documents: integer('documents', 2_000),
					};
					const paired = [];
					for (let pair = 0; pair < pairs; pair++) {
						const entries = pair % 2 === 0 ? ['baseline', profile] : [profile, 'baseline'];
						const results = {};
						try {
							for (const entry of entries)
								results[entry] = await run({
									...configuration,
									profile: entry === 'baseline' && baselineProfile === 'match' ? profile : entry,
									module: path.resolve(entry === 'baseline' ? options.baseline : options.candidate),
								});
						} catch (error) {
							await writeFile(
								path.resolve(options.output),
								serialize({
									status: 'failed',
									measurementVersion,
									node: process.version,
									samples,
									incomplete: { profile, configuration, pair, results },
									error: { name: error.name, message: error.message },
								}),
							);
							throw error;
						}
						paired.push({
							pair,
							baseline: results.baseline,
							candidate: results[profile],
						});
					}
					const throughputLoss = paired.map(
						({ baseline, candidate }) => 1 - candidate.throughput / baseline.throughput,
					);
					const p99Increase = paired.map(
						({ baseline, candidate }) => candidate.p99Milliseconds / baseline.p99Milliseconds - 1,
					);
					const target = baselineProfile === 'match' ? null : profile === 'off' ? 0.01 : profile === 'on' ? 0.03 : null;
					const pollingSufficient =
						profile !== 'polling' ||
						paired.every(
							({ baseline, candidate }) =>
								candidate.polling.samplesSufficient &&
								(baselineProfile !== 'match' || baseline.polling.samplesSufficient),
						);
					const observedThroughputLoss = median(throughputLoss);
					const observedP99Increase = median(p99Increase);
					// A range of paired observations is deliberately conservative; it is not a confidence interval.
					const status = !pollingSufficient
						? 'insufficient-polling-samples'
						: target === null
							? baselineProfile === 'match'
								? 'matched-profile-comparison-only'
								: 'cost-observation-only'
							: throughputLoss.every((value) => value <= target) &&
								  (profile === 'off' || p99Increase.every((value) => value <= 0.05))
								? 'within-target-in-every-pair'
								: throughputLoss.every((value) => value > target) ||
									  (profile !== 'off' && p99Increase.every((value) => value > 0.05))
									? 'outside-target-in-every-pair'
									: 'inconclusive';
					const result = {
						profile,
						baselineProfile,
						comparisonScope:
							baselineProfile === 'match'
								? 'optimization-only-matching-telemetry-profiles'
								: 'feature-cost-versus-no-telemetry-baseline',
						callers,
						indexes,
						operation,
						targetMedianThroughputLoss: target,
						targetMedianP99Increase: baselineProfile === 'none' && profile === 'on' ? 0.05 : null,
						observedThroughputLoss,
						observedP99Increase,
						throughputLossRange: [Math.min(...throughputLoss), Math.max(...throughputLoss)],
						p99IncreaseRange: [Math.min(...p99Increase), Math.max(...p99Increase)],
						status,
						paired,
					};
					samples.push(result);
					console.error(`${profile}/${callers}/${indexes}/${operation}: ${status}`);
					await writeFile(
						path.resolve(options.output),
						serialize({
							scope: 'finite local synthetic qualification; no production or sustained-memory claim',
							measurementVersion,
							validationWindow,
							pollIntervalMilliseconds,
							minimumPollsPerHandle,
							baselineProfile,
							baselineModule: path.resolve(options.baseline),
							candidateModule: path.resolve(options.candidate),
							baselineRevision: options['baseline-revision'] ?? 'unspecified',
							candidateRevision: options['candidate-revision'] ?? 'unspecified',
							node: process.version,
							platform: process.platform,
							architecture: process.arch,
							cpu: os.cpus()[0]?.model,
							pairs,
							samples,
						}),
					);
				}
}
