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

if (options.child) {
	const { openNativeFullTextIndex, runtimeInfo } = await import(pathToFileURL(path.resolve(options.module)).href);
	const root = await mkdtemp(path.join(os.tmpdir(), 'fulltext-observability-'));
	const indexes = [];
	const documentCount = integer('documents', 2_000);
	const queryCount = integer('queries', 4_000);
	const callers = integer('callers', 1);
	const indexCount = integer('indexes', 1);
	const profile = options.profile;
	const latencies = [];
	const publicationLatencies = [];
	const mutationPublicationLatencies = [];
	let pollingError;
	let poll;
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
			text: 'catalog',
			mode: options.operation === 'expensive' ? 'prefix' : 'any',
			limit: 10,
			exactTotal: true,
		};
		const check = async (index) => {
			const result = await index.search(request);
			assert.strictEqual(result.total, documentCount);
			assert.strictEqual(result.totalRelation, 'exact');
			assert.strictEqual(result.hits.length, 10);
			assert.strictEqual(new Set(result.hits.map((hit) => hit.id)).size, 10);
			for (const hit of result.hits) {
				assert(Number.isSafeInteger(Number(hit.id)) && Number(hit.id) >= 0 && Number(hit.id) < documentCount);
				assert(Number.isFinite(hit.score));
			}
		};
		for (let i = 0; i < 100; i++) await check(indexes[i % indexes.length]);
		if (profile === 'polling')
			poll = setInterval(() => {
				try {
					for (const index of indexes) index.telemetry();
				} catch (error) {
					pollingError ??= error;
				}
			}, 10);
		const cpuStart = process.cpuUsage();
		const rssStart = process.memoryUsage().rss;
		let peakRss = rssStart;
		const rssTimer = setInterval(() => {
			peakRss = Math.max(peakRss, process.memoryUsage().rss);
		}, 25);
		const start = performance.now();
		let queryMilliseconds;
		try {
			let next = 0;
			await Promise.all(
				Array.from({ length: callers }, async () => {
					while (next < queryCount) {
						const id = next++;
						const before = performance.now();
						await check(indexes[id % indexes.length]);
						latencies.push(performance.now() - before);
					}
				}),
			);
			queryMilliseconds = performance.now() - start;
			for (let i = 0; i < 20; i++) {
				const index = indexes[i % indexes.length];
				const cycleStart = performance.now();
				await index.applyMutationBatch({ upserts: [{ id: '0', fields: { text: `catalog common changed${i}` } }] });
				const before = performance.now();
				await index.publish(`publication-${i}`);
				publicationLatencies.push(performance.now() - before);
				mutationPublicationLatencies.push(performance.now() - cycleStart);
			}
		} finally {
			clearInterval(rssTimer);
		}
		const elapsedMilliseconds = performance.now() - start;
		const cpu = process.cpuUsage(cpuStart);
		if (pollingError) throw pollingError;
		for (const index of indexes) await check(index);
		console.log(
			serialize({
				profile,
				operation: options.operation,
				callers,
				indexCount,
				documentCount,
				queryCount,
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
		clearInterval(poll);
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
			for (const operation of ['ordinary', 'expensive'])
				for (const profile of profiles) {
					const configuration = {
						child: 1,
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
									profile: entry,
									module: path.resolve(entry === 'baseline' ? options.baseline : options.candidate),
								});
						} catch (error) {
							await writeFile(
								path.resolve(options.output),
								serialize({
									status: 'failed',
									node: process.version,
									samples,
									incomplete: { profile, configuration, pair, results },
									error: { name: error.name, message: error.message },
								}),
							);
							throw error;
						}
						paired.push({ pair, baseline: results.baseline, candidate: results[profile] });
					}
					const throughputLoss = paired.map(
						({ baseline, candidate }) => 1 - candidate.throughput / baseline.throughput,
					);
					const p99Increase = paired.map(
						({ baseline, candidate }) => candidate.p99Milliseconds / baseline.p99Milliseconds - 1,
					);
					const target = profile === 'off' ? 0.01 : profile === 'on' ? 0.03 : null;
					const observedThroughputLoss = median(throughputLoss);
					const observedP99Increase = median(p99Increase);
					// A range of paired observations is deliberately conservative; it is not a confidence interval.
					const status =
						target === null
							? 'cost-observation-only'
							: throughputLoss.every((value) => value <= target) &&
								  (profile === 'off' || p99Increase.every((value) => value <= 0.05))
								? 'within-target-in-every-pair'
								: throughputLoss.every((value) => value > target) ||
									  (profile !== 'off' && p99Increase.every((value) => value > 0.05))
									? 'outside-target-in-every-pair'
									: 'inconclusive';
					const result = {
						profile,
						callers,
						indexes,
						operation,
						targetMedianThroughputLoss: target,
						targetMedianP99Increase: profile === 'on' ? 0.05 : null,
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
