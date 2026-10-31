import assert from 'node:assert';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

function run(arguments_ = []) {
	const result = spawnSync(
		process.execPath,
		[
			fileURLToPath(new URL('../benchmarks/observability.mjs', import.meta.url)),
			'--child=1',
			'--operation=ordinary',
			'--profile=baseline',
			`--module=${fileURLToPath(new URL('./fixtures/observability-benchmark-clock.mjs', import.meta.url))}`,
			'--documents=8',
			'--queries=3',
			'--callers=1',
			'--indexes=1',
			...arguments_,
		],
		{ encoding: 'utf8', timeout: 20_000 },
	);
	assert.ifError(result.error);
	assert.strictEqual(result.status, 0, result.stderr);
	return JSON.parse(result.stdout);
}

test('observability benchmark excludes validation from API latency and query windows', () => {
	const report = run();
	assert.strictEqual(report.p50Milliseconds, 5);
	assert.strictEqual(report.p99Milliseconds, 5);
	assert.strictEqual(report.queryMilliseconds, 15);
	assert.strictEqual(report.latencyScope, 'search-api-completion-before-validation');
	assert.strictEqual(report.queryPhaseScope, 'sum-of-drained-query-windows-excluding-validation');
});

test('observability benchmark drains partial concurrent windows and validates every result once', () => {
	const report = run(['--queries=5', '--validation-window=2', '--callers=2']);
	assert.strictEqual(report.queryMilliseconds, 25);
	assert.strictEqual(report.queryWindows, 3);
	assert.strictEqual(report.validationWindow, 2);
	assert.strictEqual(report.runtime.searchCalls, 106);
	assert.deepStrictEqual(
		report.runtime.validatedCalls,
		Array.from({ length: 106 }, (_, i) => i + 1),
	);
	assert.strictEqual(report.measurementVersion, 2);
	assert.strictEqual(report.polling.enabled, false);
	assert.strictEqual(report.polling.intervalPerHandleMilliseconds, 1_000);
	assert.strictEqual(report.polling.handles[0].count, 0);
});

for (const argument of ['--validation-window=65537', '--validation-window=1.5', '--poll-interval-ms=0'])
	test(`observability benchmark rejects invalid bounded scheduling option ${argument}`, () => {
		const result = spawnSync(
			process.execPath,
			[fileURLToPath(new URL('../benchmarks/observability.mjs', import.meta.url)), argument],
			{
				encoding: 'utf8',
				timeout: 20_000,
			},
		);
		assert.ifError(result.error);
		assert.notStrictEqual(result.status, 0);
		assert.match(result.stderr, /validation-window|poll-interval-ms/);
	});

function paired(arguments_) {
	const root = mkdtempSync(path.join(tmpdir(), 'observability-driver-test-'));
	try {
		const module = fileURLToPath(new URL('./fixtures/observability-benchmark-clock.mjs', import.meta.url));
		const output = path.join(root, 'results.json');
		const result = spawnSync(
			process.execPath,
			[
				fileURLToPath(new URL('../benchmarks/observability.mjs', import.meta.url)),
				`--baseline=${module}`,
				`--candidate=${module}`,
				`--output=${output}`,
				'--pairs=1',
				'--operations=ordinary',
				'--documents=8',
				'--queries=3',
				...arguments_,
			],
			{ encoding: 'utf8', timeout: 20_000 },
		);
		assert.ifError(result.error);
		assert.strictEqual(result.status, 0, result.stderr);
		return JSON.parse(readFileSync(output, 'utf8'));
	} finally {
		rmSync(root, { recursive: true, force: true });
	}
}

test('matched telemetry profiles compare optimization only and preserve histogram configuration', () => {
	const report = paired(['--profiles=on', '--baseline-profile=match']);
	assert.strictEqual(report.baselineProfile, 'match');
	for (const sample of report.samples) {
		assert.strictEqual(sample.status, 'matched-profile-comparison-only');
		assert.strictEqual(sample.targetMedianThroughputLoss, null);
		assert.strictEqual(sample.targetMedianP99Increase, null);
		for (const side of ['baseline', 'candidate']) {
			assert.strictEqual(sample.paired[0][side].profile, 'on');
			assert.deepStrictEqual(
				sample.paired[0][side].runtime.configurations,
				Array.from({ length: sample.indexes }, () => ({
					histograms: true,
					snapshotBytes: false,
				})),
			);
		}
	}
});

test('default paired baseline never configures telemetry and retains feature-cost targets', () => {
	const report = paired(['--profiles=off']);
	assert.strictEqual(report.baselineProfile, 'none');
	for (const sample of report.samples) {
		assert.deepStrictEqual(sample.paired[0].baseline.runtime.configurations, []);
		assert.strictEqual(sample.paired[0].baseline.profile, 'baseline');
		assert.strictEqual(sample.targetMedianThroughputLoss, 0.01);
	}
});

test('polling with too few observed scrapes is explicitly insufficient', () => {
	const report = paired(['--profiles=polling', '--minimum-polls-per-handle=10']);
	for (const sample of report.samples) {
		assert.strictEqual(sample.status, 'insufficient-polling-samples');
		const polling = sample.paired[0].candidate.polling;
		assert.strictEqual(polling.intervalPerHandleMilliseconds, 1_000);
		assert.strictEqual(polling.minimumPollsPerHandle, 10);
		assert.strictEqual(polling.samplesSufficient, false);
		assert(polling.handles.every(({ count }) => count === 0));
	}
});

for (const topology of ['4x4', '64x1'])
	test(`idle scrape driver measures exact bounded per-handle samples at ${topology}`, () => {
		const result = spawnSync(
			process.execPath,
			[
				fileURLToPath(new URL('../benchmarks/observability-scrape-cost.mjs', import.meta.url)),
				'--child=1',
				`--module=${fileURLToPath(new URL('./fixtures/observability-scrape-clock.mjs', import.meta.url))}`,
				`--topology=${topology}`,
				'--histograms=true',
				'--warmup-rounds=2',
				'--sample-rounds=3',
			],
			{ encoding: 'utf8', timeout: 20_000 },
		);
		assert.ifError(result.error);
		assert.strictEqual(result.status, 0, result.stderr);
		const report = JSON.parse(result.stdout);
		assert.strictEqual(report.samplesPerHandle, 3);
		assert.strictEqual(report.warmupRounds, 2);
		assert.strictEqual(report.p50Milliseconds, 2);
		assert.strictEqual(report.p99Milliseconds, 2);
		assert.deepStrictEqual(
			report.durationsMilliseconds,
			Array.from({ length: report.handles }, () => [2, 2, 2]),
		);
		assert.strictEqual(report.runtime.runtimeConfiguration.maxSearchThreads, report.workers * report.handles);
		for (const handle of report.runtime.handles) {
			assert.strictEqual(handle.queries, 100);
			assert.strictEqual(handle.scrapes, 5);
			assert.strictEqual(handle.limits.searchThreads, report.workers);
			assert.deepStrictEqual(handle.configuration, {
				histograms: true,
				snapshotBytes: false,
			});
		}
	});

for (const argument of ['--sample-rounds=10001', '--warmup-rounds=1001'])
	test(`idle scrape driver rejects unbounded option ${argument}`, () => {
		const result = spawnSync(
			process.execPath,
			[fileURLToPath(new URL('../benchmarks/observability-scrape-cost.mjs', import.meta.url)), argument],
			{ encoding: 'utf8', timeout: 20_000 },
		);
		assert.ifError(result.error);
		assert.notStrictEqual(result.status, 0);
		assert.match(result.stderr, /must be a positive integer/);
	});

for (const failure of ['primary-and-close', 'close', 'partial-open'])
	test(`idle scrape driver preserves ${failure} failure and publishes no success report`, () => {
		const root = mkdtempSync(path.join(tmpdir(), 'observability-cleanup-test-'));
		const marker = path.join(root, 'marker.json');
		let fixtureRoot;
		try {
			const result = spawnSync(
				process.execPath,
				[
					fileURLToPath(new URL('../benchmarks/observability-scrape-cost.mjs', import.meta.url)),
					'--child=1',
					`--module=${fileURLToPath(new URL('./fixtures/observability-scrape-clock.mjs', import.meta.url))}`,
					'--topology=4x4',
					'--histograms=false',
					'--warmup-rounds=1',
					'--sample-rounds=1',
				],
				{
					encoding: 'utf8',
					timeout: 20_000,
					env: { ...process.env, FULLTEXT_SCRAPE_TEST_FAILURE: failure, FULLTEXT_SCRAPE_TEST_MARKER: marker },
				},
			);
			assert.ifError(result.error);
			assert.notStrictEqual(result.status, 0);
			assert.strictEqual(
				result.stdout,
				'',
				'A failed measurement or cleanup must not publish a successful child report',
			);
			const state = JSON.parse(readFileSync(marker, 'utf8'));
			fixtureRoot = state.root;
			assert.strictEqual(
				state.closeAttempts,
				failure === 'partial-open' ? 1 : 4,
				'All returned handles must get a close attempt after failure',
			);
			assert(existsSync(fixtureRoot), 'An index root must be retained after a measurement or cleanup failure');
			if (failure === 'partial-open') {
				assert.match(result.stderr, /E_SCRAPE_OPEN/);
				assert(result.stderr.includes(fixtureRoot), 'A retained fixture path must be reported');
			} else assert.match(result.stderr, /E_SCRAPE_CLOSE/);
			if (failure === 'primary-and-close') {
				assert.match(result.stderr, /AggregateError: Scrape measurement and cleanup failed/);
				assert.match(result.stderr, /E_SCRAPE_PRIMARY/);
				assert.match(result.stderr, /\[cause\]/);
			} else {
				assert.doesNotMatch(result.stderr, /AggregateError/);
				assert.doesNotMatch(result.stderr, /E_SCRAPE_PRIMARY/);
			}
		} finally {
			if (fixtureRoot) rmSync(fixtureRoot, { recursive: true, force: true });
			rmSync(root, { recursive: true, force: true });
		}
	});
