import assert from 'node:assert';
import { execFile } from 'node:child_process';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const execute = promisify(execFile);
import { executeDriver } from './fixtures/metadata-driver-process.mjs';
import { prepareOracle } from '../benchmarks/metadata-oracle.mjs';
const driver = fileURLToPath(new URL('../benchmarks/metadata-workload.mjs', import.meta.url));

test(
	'public-addon metadata workload validates nested results, concurrent rounds and pressure recovery',
	{ timeout: 65_000 },
	async (context) => {
		const scratch = await mkdtemp(path.join(tmpdir(), 'fulltext-workload-test-'));
		context.after(() => rm(scratch, { recursive: true, force: true }));
		const { stdout, stderr } = await executeDriver(
			context,
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
		context.diagnostic(stderr.trim());
		const report = JSON.parse(stdout);
		const diagnostics = stderr
			.trim()
			.split('\n')
			.filter((line) => line.startsWith('{'))
			.map((line) => JSON.parse(line));
		const premeasurement = diagnostics.find(({ phase }) => phase === 'premeasurement-gates');
		const stages = diagnostics.filter(({ phase }) => phase === 'progress');
		assert.strictEqual(stages[0].stage, 'addon-import');
		assert.strictEqual(stages.at(-1).stage, 'cleanup-complete');
		for (let i = 1; i < stages.length; i++) assert(stages[i].elapsedMilliseconds >= stages[i - 1].elapsedMilliseconds);
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

test(
	'metadata workload retains the failed round and cleans up before reporting failure',
	{ timeout: 35_000 },
	async (context) => {
		const scratch = await mkdtemp(path.join(tmpdir(), 'fulltext-workload-gate-test-'));
		context.after(() => rm(scratch, { recursive: true, force: true }));
		await assert.rejects(
			execute(
				process.execPath,
				[
					driver,
					'--scratch',
					scratch,
					'--documents',
					'20',
					'--rounds',
					'1',
					'--queries',
					'1',
					'--concurrency',
					'1',
					'--features',
					'flat',
					'--min-throughput',
					'1000000000000',
					'--max-p95-ms',
					'30000',
					'--max-p99-ms',
					'30000',
				],
				{ timeout: 30_000, env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' } },
			),
			(error) => {
				assert.strictEqual(error.stdout, '', 'a failed gate must not emit a qualification pass');
				const diagnostics = error.stderr
					.split('\n')
					.filter((line) => line.startsWith('{'))
					.map((line) => JSON.parse(line));
				const failure = diagnostics.find(({ phase }) => phase === 'round-gate-failure');
				assert(failure, 'structured failed-round evidence must be retained');
				assert.strictEqual(failure.gate, 'throughput');
				assert.strictEqual(failure.name, 'ordinary-exact');
				assert.strictEqual(failure.concurrency, 1);
				assert.strictEqual(failure.lastTenRounds.length, 1);
				assert.strictEqual(failure.lastTenRounds[0].round, 1);
				assert.strictEqual(failure.lastTenRounds[0].completed, 1);
				assert.strictEqual(failure.lastTenRounds[0].latencyMilliseconds.length, 1);
				assert(failure.postIndexBaseline.rss > 0);
				return true;
			},
		);
		assert.deepStrictEqual(await readdir(scratch), [], 'failure must still close and remove generated indexes');
	},
);

test(
	'metadata workload retains its named stage in an execFile termination error',
	{ timeout: 15_000 },
	async (context) => {
		const scratch = await mkdtemp(path.join(tmpdir(), 'fulltext-workload-kill-test-'));
		let child;
		context.after(async () => {
			if (child && child.exitCode === null && child.signalCode === null) child.kill();
			await rm(scratch, { recursive: true, force: true });
		});
		const pending = execute(
			process.execPath,
			[
				driver,
				'--scratch',
				scratch,
				'--documents',
				'20',
				'--rounds',
				'1',
				'--queries',
				'1',
				'--concurrency',
				'1',
				'--features',
				'flat',
			],
			{ timeout: 10_000, env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' } },
		);
		child = pending.child;
		let observed = '',
			killedAtStage = false;
		child.stderr.on('data', (chunk) => {
			observed += chunk;
			if (!killedAtStage && observed.includes('"stage":"catalog-open"')) {
				killedAtStage = child.kill();
			}
		});
		await assert.rejects(pending, (error) => {
			assert(killedAtStage);
			assert(error.killed);
			assert.strictEqual(error.stdout, '');
			const events = error.stderr
				.split('\n')
				.filter((line) => line.startsWith('{') && line.endsWith('}'))
				.map((line) => JSON.parse(line));
			assert.strictEqual(events[0].phase, 'premeasurement-gates');
			const stages = events.filter(({ phase }) => phase === 'progress');
			assert(stages.some(({ stage }) => stage === 'addon-import'));
			for (let i = 1; i < stages.length; i++)
				assert(stages[i].elapsedMilliseconds >= stages[i - 1].elapsedMilliseconds);
			assert(!events.some(({ stage }) => stage === 'cleanup-complete'));
			return true;
		});
	},
);

test('workload diagnostics retain actual execFile timeout stderr', { timeout: 10_000 }, async () => {
	const diagnostics = [];
	await assert.rejects(
		executeDriver(
			{ diagnostic: (text) => diagnostics.push(text) },
			process.execPath,
			['--input-type=module', '-e', "console.error(JSON.stringify({ stage: 'waiting' })); setInterval(() => {}, 1000)"],
			{ timeout: 1000 },
		),
		(error) => {
			assert(error.killed);
			assert.strictEqual(diagnostics.length, 1);
			assert.strictEqual(diagnostics[0], error.stderr);
			assert.strictEqual(JSON.parse(diagnostics[0]).stage, 'waiting');
			return true;
		},
	);
});

test('workload retains indexes and reports resolved cleanup errors', { timeout: 30_000 }, async (context) => {
	const scratch = await mkdtemp(path.join(tmpdir(), 'fulltext-workload-cleanup-test-'));
	context.after(() => rm(scratch, { recursive: true, force: true }));
	await assert.rejects(
		execute(
			process.execPath,
			[
				driver,
				'--scratch',
				scratch,
				'--documents',
				'20',
				'--rounds',
				'1',
				'--queries',
				'1',
				'--concurrency',
				'1',
				'--features',
				'flat',
				'--min-throughput',
				'1000000000000',
				'--module',
				fileURLToPath(new URL('./fixtures/metadata-close-failure.mjs', import.meta.url)),
			],
			{
				timeout: 25_000,
				env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1', FULLTEXT_TEST_CLOSE_KIND: 'writer' },
			},
		),
		(error) => {
			assert.strictEqual(error.stdout, '');
			assert.match(error.stderr, /native cleanup failed/);
			assert.match(error.stderr, /E_CLOSE_FAILED/);
			return true;
		},
	);
	assert((await readdir(scratch)).length > 0, 'cleanup failure must retain index evidence');
});

test('source oracle prepares string keys once while retaining raw UTF-8 ordering', () => {
	let reads = 0;
	const selected = Array.from({ length: 200 }, (_, i) => ({
		id: `id-${i}`,
		get brand() {
			reads++;
			return ['é', '\uE000', '\u{10000}', 'a'][i % 4];
		},
	}));
	const scenario = { selected, request: { sort: { field: 'brand', direction: 'asc' } } };
	prepareOracle(scenario);
	assert(reads <= selected.length * 2, 'sort comparisons must reuse prepared keys');
	const expected = [...selected].sort(
		(a, b) =>
			Buffer.compare(Buffer.from(a.brand), Buffer.from(b.brand)) ||
			Buffer.compare(Buffer.from(a.id), Buffer.from(b.id)),
	);
	assert.deepStrictEqual(
		scenario.expected.ids,
		expected.slice(0, 10).map(({ id }) => id),
	);
	assert.strictEqual(scenario.expected.total, selected.length);
});
