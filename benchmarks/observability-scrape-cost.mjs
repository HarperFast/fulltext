import assert from 'node:assert';
import { spawnSync } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath, pathToFileURL } from 'node:url';

const options = Object.fromEntries(
	process.argv.slice(2).map((argument) => {
		assert(argument.startsWith('--') && argument.includes('='), 'Use --name=value arguments');
		const separator = argument.indexOf('=');
		return [argument.slice(2, separator), argument.slice(separator + 1)];
	}),
);
const integer = (name, fallback, maximum) => {
	const value = Number(options[name] ?? fallback);
	assert(
		Number.isSafeInteger(value) && value > 0 && value <= maximum,
		`${name} must be a positive integer no greater than ${maximum}`,
	);
	return value;
};
const warmupRounds = integer('warmup-rounds', 25, 1_000);
const sampleRounds = integer('sample-rounds', 120, 10_000);
const supportedTopologies = ['1x1', '4x1', '64x1', '4x4', '4x16'];
const percentile = (values, fraction) => values.toSorted((a, b) => a - b)[Math.ceil(values.length * fraction) - 1];

if (options.child) {
	assert(supportedTopologies.includes(options.topology), 'Unknown worker-by-handle topology');
	assert(['false', 'true'].includes(options.histograms), 'histograms must be false or true');
	const [workers, handles] = options.topology.split('x').map(Number);
	const histograms = options.histograms === 'true';
	const module = path.resolve(options.module);
	const { openNativeFullTextIndex, configureNativeFullTextRuntime, runtimeInfo } = await import(pathToFileURL(module));
	configureNativeFullTextRuntime({
		maxResidentIndexes: handles,
		maxIndexingThreads: handles,
		maxSearchThreads: workers * handles,
		maxWriterMemoryBytes: handles * 15_000_000,
		maxQueuedBytes: 3 * handles * (8 << 20),
		maxExpensiveSearches: 1,
	});
	const root = await mkdtemp(path.join(tmpdir(), 'fulltext-scrape-'));
	const indexes = [];
	const yieldTurn = () => new Promise((resolve) => setImmediate(resolve));
	let report;
	let primaryError;
	let failed = false;
	const cleanupErrors = [];
	try {
		for (let i = 0; i < handles; i++) {
			const index = await openNativeFullTextIndex({
				path: path.join(root, String(i)),
				indexId: String(i),
				generation: 'scrape-cost-v1',
				fields: [{ name: 'text' }],
				analyzer: 'english@2',
				limits: {
					indexingThreads: 1,
					searchThreads: workers,
					writerMemoryBytes: 15_000_000,
					maxQueuedCommands: 128,
					maxQueuedBytes: 8 << 20,
					maxBatchBytes: 1 << 20,
				},
			});
			indexes.push(index);
			index.configureTelemetry({ histograms, snapshotBytes: false });
			await index.applyMutationBatch({
				upserts: [{ id: 'one', fields: { text: 'catalog' } }],
			});
			await index.publish('one');
			for (let j = 0; j < 100; j++)
				assert.strictEqual(
					(
						await index.search({
							text: 'catalog',
							mode: 'any',
							exactTotal: false,
							limit: 1,
						})
					).total,
					1,
				);
		}
		for (let round = 0; round < warmupRounds; round++)
			for (const index of indexes) {
				assert(index.telemetry().available);
				await yieldTurn();
			}
		const durations = Array.from({ length: handles }, () => []);
		for (let round = 0; round < sampleRounds; round++)
			for (let i = 0; i < handles; i++) {
				const start = performance.now();
				const snapshot = indexes[i].telemetry();
				const elapsed = performance.now() - start;
				assert(snapshot.available);
				assert.strictEqual(snapshot.native.operations.search.succeeded, 100n);
				durations[i].push(elapsed);
				await yieldTurn();
			}
		const all = durations.flat();
		report = {
			measurementVersion: 1,
			module,
			workers,
			handles,
			histograms,
			snapshotBytes: false,
			scope: 'idle-populated-handle-synchronous-telemetry-call',
			schedule: 'serial-one-handle-per-event-loop-turn-no-interval-target',
			searchWarmupPerHandle: 100,
			warmupRounds,
			samplesPerHandle: sampleRounds,
			durationsMilliseconds: durations,
			p50Milliseconds: percentile(all, 0.5),
			p99Milliseconds: percentile(all, 0.99),
			maxMilliseconds: all.reduce((maximum, duration) => Math.max(maximum, duration), 0),
			runtime: await runtimeInfo(),
		};
	} catch (error) {
		failed = true;
		primaryError = error;
	} finally {
		for (const index of indexes)
			try {
				const result = await index.close({ mode: 'rollback' });
				if (result.cleanupError) cleanupErrors.push(result.cleanupError);
			} catch (error) {
				cleanupErrors.push(error);
			}
		if (!failed && cleanupErrors.length === 0)
			try {
				await rm(root, { recursive: true, force: true });
			} catch (error) {
				cleanupErrors.push(error);
			}
	}
	if (failed || cleanupErrors.length > 0) console.error(`Scrape fixture retained for inspection: ${root}`);
	if (failed && cleanupErrors.length > 0)
		throw new AggregateError([primaryError, ...cleanupErrors], 'Scrape measurement and cleanup failed', {
			cause: primaryError,
		});
	if (failed) throw primaryError;
	if (cleanupErrors.length === 1) throw cleanupErrors[0];
	if (cleanupErrors.length > 1) throw new AggregateError(cleanupErrors, 'Scrape cleanup failed');
	console.log(JSON.stringify(report));
} else {
	assert(
		options.baseline && options.candidate && options.output,
		'Required: --baseline=/absolute/dist/native.js --candidate=/absolute/dist/native.js --output=/absolute/results.json',
	);
	const topologies = (options.topologies ?? supportedTopologies.join(',')).split(',');
	assert(
		topologies.length > 0 &&
			new Set(topologies).size === topologies.length &&
			topologies.every((topology) => supportedTopologies.includes(topology)),
		'topologies must contain distinct supported worker-by-handle combinations',
	);
	const results = [];
	for (const topology of topologies)
		for (const histograms of [false, true])
			for (const version of ['baseline', 'candidate']) {
				const run = spawnSync(
					process.execPath,
					[
						fileURLToPath(import.meta.url),
						'--child=1',
						`--module=${path.resolve(options[version])}`,
						`--topology=${topology}`,
						`--histograms=${histograms}`,
						`--warmup-rounds=${warmupRounds}`,
						`--sample-rounds=${sampleRounds}`,
					],
					{
						encoding: 'utf8',
						timeout: 180_000,
						maxBuffer: 8 << 20,
						env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' },
					},
				);
				try {
					assert.ifError(run.error);
					assert.strictEqual(run.status, 0, run.stderr);
					results.push({ version, ...JSON.parse(run.stdout) });
				} catch (error) {
					await writeFile(
						path.resolve(options.output),
						JSON.stringify(
							{
								status: 'failed',
								results,
								incomplete: { topology, histograms, version },
								error: { name: error.name, message: error.message },
							},
							null,
							2,
						),
					);
					throw error;
				}
				await writeFile(
					path.resolve(options.output),
					JSON.stringify(
						{
							scope:
								'finite idle scrape cost observations; no production, throughput or sustained-memory qualification',
							node: process.version,
							platform: process.platform,
							architecture: process.arch,
							warmupRounds,
							sampleRounds,
							results,
						},
						null,
						2,
					),
				);
				console.error(`${topology}/${histograms}/${version}: complete`);
			}
}
