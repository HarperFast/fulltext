import assert from 'node:assert';
import { spawnSync } from 'node:child_process';
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
