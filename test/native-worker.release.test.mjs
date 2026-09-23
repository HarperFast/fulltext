import assert from 'node:assert';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const scenarioPath = fileURLToPath(new URL('./fixtures/native-worker-scenario.mjs', import.meta.url));

for (const [scenario, name] of [
	['apply', 'abrupt worker termination detaches completions and releases its writer'],
	['opening', 'worker termination during open releases its native cleanup hook'],
	['multiple', 'worker termination releases every index in one Node environment'],
]) {
	test(name, async () => {
		const result = await runScenario(scenario);
		assert.strictEqual(result.code, 0, `${scenario} scenario exited ${result.code}\n${result.stderr}${result.stdout}`);
	});
}

function runScenario(scenario) {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, [scenarioPath, scenario], {
			stdio: ['ignore', 'pipe', 'pipe'],
		});
		let stdout = '';
		let stderr = '';
		child.stdout.setEncoding('utf8').on('data', (chunk) => (stdout += chunk));
		child.stderr.setEncoding('utf8').on('data', (chunk) => (stderr += chunk));
		child.once('error', reject);
		child.once('exit', (code, signal) => resolve({ code: code ?? signal, stderr, stdout }));
	});
}
