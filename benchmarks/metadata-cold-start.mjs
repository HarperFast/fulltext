import { closeIndex } from './metadata-cleanup.mjs';
import assert from 'node:assert';
import { execFileSync } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

const { values } = parseArgs({
	options: {
		module: { type: 'string', default: fileURLToPath(new URL('../dist/native.js', import.meta.url)) },
		scratch: { type: 'string' },
		documents: { type: 'string', default: '65536' },
		rounds: { type: 'string', default: '3' },
		concurrency: { type: 'string', default: '1,4,8' },
		action: { type: 'string' },
		config: { type: 'string' },
	},
});
const positive = (value) => {
	const number = Number(value);
	assert(Number.isSafeInteger(number) && number > 0);
	return number;
};
const documents = positive(values.documents);
assert(documents >= 1000, 'At least 1000 distinct keys are needed to force dictionary qualification');
const rounds = positive(values.rounds);
const concurrency = values.concurrency.split(',').map(positive);
const api = await import(pathToFileURL(path.resolve(values.module)).href);

if (values.action === 'measure') {
	const { configs, parallel, shared, budget } = JSON.parse(values.config);
	const readers = await Promise.all(configs.slice(0, shared ? 1 : parallel).map(api.openNativeFullTextReader));
	const request = {
		text: 'shoe',
		limit: 5,
		exactTotal: true,
		candidateIds: Array.from({ length: 16 }, (_, n) => `item-${String(n).padStart(8, '0')}`),
		aggregations: [{ name: 'brands', field: 'brand', type: 'terms', size: 100 }],
	};
	const run = async (timed) => {
		const latencies = [];
		const timeouts = [];
		const start = performance.now();
		const cpu = process.cpuUsage();
		const before = process.memoryUsage().rss;
		let peak = before;
		const sampler = setInterval(() => {
			peak = Math.max(peak, process.memoryUsage().rss);
		}, 1);
		try {
			await Promise.all(
				Array.from({ length: parallel }, async (_, n) => {
					const begin = performance.now();
					try {
						const result = await readers[shared ? 0 : n].search(
							request,
							timed ? { remainingBudgetMilliseconds: budget } : undefined,
						);
						assert.strictEqual(result.total, 16);
						assert.strictEqual(result.aggregations[0].buckets.length, 16);
						assert(result.aggregations[0].buckets.every(({ count }) => count === 1));
						latencies.push(performance.now() - begin);
					} catch (error) {
						if (!timed || error.code !== 'E_TIMEOUT') throw error;
						timeouts.push(performance.now() - begin);
					}
				}),
			);
		} finally {
			clearInterval(sampler);
		}
		const elapsedMilliseconds = performance.now() - start;
		const used = process.cpuUsage(cpu);
		const after = process.memoryUsage().rss;
		return {
			latencies,
			timeouts,
			elapsedMilliseconds,
			cpuMilliseconds: (used.user + used.system) / 1000,
			rssBeforeBytes: before,
			rssAfterBytes: after,
			peakRssBytes: Math.max(peak, after),
		};
	};
	let report;
	let primaryError;
	try {
		const cold = await run(budget !== undefined);
		const recovery = await run(false);
		const warm = [];
		for (let n = 0; n < 20; n++) warm.push(await run(false));
		for (const reader of readers) {
			const status = reader.status();
			assert.strictEqual(status.searchQueuedCommands, 0n);
			assert.strictEqual(status.searchQueuedBytes, 0n);
		}
		report = { parallel, shared, budgetMilliseconds: budget ?? null, cold, recovery, warm };
	} catch (error) {
		primaryError = error;
		throw error;
	} finally {
		const outcomes = await Promise.allSettled(readers.map((reader) => closeIndex(reader)));
		const errors = outcomes.filter(({ status }) => status === 'rejected').map(({ reason }) => reason);
		if (errors.length) throw new AggregateError(errors, 'cold-start reader cleanup failed', { cause: primaryError });
	}
	console.log(JSON.stringify(report));
} else {
	assert(values.scratch, 'Pass --scratch pointing to an existing scratch parent');
	const directory = await mkdtemp(path.join(path.resolve(values.scratch), 'fulltext-cold-start-'));
	const configs = Array.from({ length: Math.max(...concurrency) }, (_, n) => ({
		path: path.join(directory, `index-${n}`),
		indexId: `cold-${n}`,
		generation: 'one',
		analyzer: 'english@2',
		fields: [{ name: 'title' }],
		filterFields: [{ name: 'brand', type: 'string' }],
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15000000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8388608,
			maxBatchBytes: 1048576,
		},
	}));
	let completedReport;
	try {
		for (const config of configs) {
			const writer = await api.openNativeFullTextIndex(config);
			let primaryError;
			try {
				for (let start = 0; start < documents; start += 256) {
					const upserts = Array.from({ length: Math.min(256, documents - start) }, (_, offset) => {
						const id = String(start + offset).padStart(8, '0');
						return { id: `item-${id}`, fields: { title: 'shoe' }, filters: { brand: `brand-${id}` } };
					});
					const result = await writer.applyMutationBatch({ upserts });
					assert.strictEqual(result.processed, upserts.length);
					assert.deepStrictEqual(result.rejected, []);
				}
				await writer.publish('complete');
			} catch (error) {
				primaryError = error;
				throw error;
			} finally {
				try {
					await closeIndex(writer, { mode: 'rollback' });
				} catch (error) {
					throw new AggregateError([error], 'cold-start writer cleanup failed', { cause: primaryError });
				}
			}
		}
		const measurements = [];
		for (let round = 0; round < rounds; round++)
			for (const parallel of concurrency)
				for (const shared of [true, false]) {
					for (const budget of [undefined, 1]) {
						const result = execFileSync(
							process.execPath,
							[
								fileURLToPath(import.meta.url),
								'--module',
								path.resolve(values.module),
								'--documents',
								String(documents),
								'--action',
								'measure',
								'--config',
								JSON.stringify({ configs, parallel, shared, budget }),
							],
							{ encoding: 'utf8', timeout: 120000, env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' } },
						);
						measurements.push({ round, ...JSON.parse(result) });
					}
				}
		completedReport = JSON.stringify(
			{
				benchmark: 'metadata-cold-start',
				runtime: {
					node: process.version,
					platform: process.platform,
					arch: process.arch,
					native: await api.runtimeInfo(),
				},
				documentsPerIndex: documents,
				indexes: configs.length,
				rounds,
				concurrency,
				method:
					'Fresh child process per cohort; dictionary maximum cache cold, filesystem cache uncontrolled and likely warm. Candidate filter matches 16 of the unique string keys; size100 forces retained-key preflight even for this selective query. Shared cohorts target one index; independent cohorts target distinct indexes. Cold then recovery then20warm waves. CPU includes native threads; RSS sampled1ms. No production SLO implied.',
				measurements,
			},
			null,
			2,
		);
	} finally {
		if (completedReport) await rm(directory, { recursive: true, force: true });
	}
	console.log(completedReport);
}
