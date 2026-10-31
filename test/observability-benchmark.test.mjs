import assert from 'node:assert';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

test('observability benchmark samples API completion before validating the result', () => {
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
		],
		{ encoding: 'utf8', timeout: 20_000 },
	);
	assert.ifError(result.error);
	assert.strictEqual(result.status, 0, result.stderr);
	const report = JSON.parse(result.stdout);
	assert.strictEqual(report.p50Milliseconds, 5);
	assert.strictEqual(report.p99Milliseconds, 5);
	assert.strictEqual(report.queryMilliseconds, 315);
	assert.strictEqual(report.latencyScope, 'search-api-completion-before-validation');
	assert.strictEqual(report.queryPhaseScope, 'search-and-validation');
});
