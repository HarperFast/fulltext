import assert from 'node:assert';
import { execFile } from 'node:child_process';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const execute = promisify(execFile);
const driver = fileURLToPath(new URL('../benchmarks/metadata-workload.mjs', import.meta.url));

test(
	'public-addon metadata workload validates nested results, concurrent rounds and pressure recovery',
	{ timeout: 65_000 },
	async (context) => {
		const scratch = await mkdtemp(path.join(tmpdir(), 'fulltext-workload-test-'));
		context.after(() => rm(scratch, { recursive: true, force: true }));
		const { stdout, stderr } = await execute(
			process.execPath,
			[
				driver,
				'--scratch',
				scratch,
				'--documents',
				'1100',
				'--rounds',
				'5',
				'--queries',
				'2',
				'--concurrency',
				'1,2',
				'--features',
				'expanded',
				'--revision',
				'ci-report-contract',
				'--max-p95-ms',
				'30000',
				'--max-p99-ms',
				'30000',
				'--min-throughput',
				'0.01',
				'--max-rss-growth-mib',
				'4096',
				'--max-tail-rss-range-mib',
				'4096',
				'--budget-ms',
				'30000',
			],
			{ timeout: 60_000, maxBuffer: 4 * 1024 * 1024, env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' } },
		);
		const report = JSON.parse(stdout);
		const premeasurement = JSON.parse(stderr.trim());
		assert.strictEqual(premeasurement.phase, 'premeasurement-gates');
		assert.deepStrictEqual(premeasurement.gates, report.gates);
		assert.strictEqual(report.qualification, 'pass');
		assert.strictEqual(report.revision, 'ci-report-contract');
		assert.strictEqual(report.features, 'expanded');
		assert.strictEqual(report.config.documents, 1100);
		assert.strictEqual(report.config.rounds, 5);
		assert.strictEqual(report.config.queriesPerRound, 2);
		assert.deepStrictEqual(report.config.concurrency, [1, 2]);
		assert.strictEqual(report.measurements.length, 4 * 2 * 5);
		assert.strictEqual(report.summaries.length, 4 * 2);
		assert.deepStrictEqual(
			new Set(report.measurements.map(({ name }) => name)),
			new Set(['ordinary-exact', 'flat-price-sort', 'flat-filter-candidates', 'multi-sort-nested-tree']),
		);
		for (const measurement of report.measurements) {
			assert.strictEqual(measurement.completed, 2);
			assert.strictEqual(measurement.latencyMilliseconds.length, 2);
			assert(measurement.p99Milliseconds >= measurement.p50Milliseconds);
			assert(measurement.sampledPeakRssBytes >= measurement.rssBeforeBytes);
			assert(measurement.successesPerSecond > 0);
		}
		for (const summary of report.summaries) {
			assert.strictEqual(summary.rounds, 5);
			assert.strictEqual(summary.completed, 10);
		}
		assert.deepStrictEqual(report.pressureResults.map(({ name }) => name).sort(), [
			'high-cardinality-histogram',
			'long-key-terms',
		]);
		for (const pressure of report.pressureResults) {
			assert(['E_RESOURCE_LIMIT', 'E_RESULT_TOO_LARGE'].includes(pressure.code));
			assert.deepStrictEqual(pressure.recovery, {
				total: pressure.name === 'long-key-terms' ? 300 : 1100,
				totalRelation: 'exact',
				firstId: pressure.name === 'long-key-terms' ? 'pressure-0' : 'product-000000000',
				budgetMilliseconds: 30000,
			});
		}
		assert.deepStrictEqual(
			await readdir(scratch),
			[],
			'driver must remove temporary indexes after successful recovery',
		);
	},
);

test('metadata workload requires an explicit scratch directory', async () => {
	await assert.rejects(execute(process.execPath, [driver], { timeout: 10_000 }), (error) => {
		assert.match(error.stderr, /--scratch requires an existing scratch directory/);
		return true;
	});
});
