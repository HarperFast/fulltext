import assert from 'node:assert';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const scenarioPath = fileURLToPath(new URL('./fixtures/native-worker-scenario.mjs', import.meta.url));

const scenarios = [
	['apply', 'abrupt worker termination detaches completions and releases its writer'],
	['opening', 'worker termination during open releases its native cleanup hook'],
	['multiple', 'worker termination releases every index in one Node environment'],
	['foreign', 'native handles reject use from another Node environment'],
	['sequence', 'repeated worker termination remains safe in one Node process'],
];
if (process.platform === 'win32' && process.env.FULLTEXT_TEST_PHASE === 'test') {
	scenarios.push(['apply-stress', 'repeated abrupt worker termination remains safe on Windows']);
}

for (const [scenario, name] of scenarios) {
	test(name, async () => {
		const result = await runScenario(scenario);
		assert.strictEqual(result.timedOut, false, `${scenario} scenario timed out\n${result.stderr}${result.stdout}`);
		assert.strictEqual(result.code, 0, `${scenario} scenario exited ${result.code}\n${result.stderr}${result.stdout}`);
	});
}

function runScenario(scenario) {
	return new Promise((resolve, reject) => {
		let timedOut = false;
		const timeoutMilliseconds = scenario === 'apply-stress' ? 480_000 : scenario === 'sequence' ? 240_000 : 45_000;
		const child = spawn(process.execPath, [scenarioPath, scenario], {
			stdio: ['ignore', 'pipe', 'pipe'],
		});
		const timeout = setTimeout(() => {
			timedOut = true;
			child.kill();
		}, timeoutMilliseconds);
		let stdout = '';
		let stderr = '';
		child.stdout.setEncoding('utf8').on('data', (chunk) => (stdout += chunk));
		child.stderr.setEncoding('utf8').on('data', (chunk) => (stderr += chunk));
		child.once('error', (error) => {
			clearTimeout(timeout);
			reject(error);
		});
		child.once('close', (code, signal) => {
			clearTimeout(timeout);
			resolve({ code: code ?? signal, stderr, stdout, timedOut });
		});
	});
}
