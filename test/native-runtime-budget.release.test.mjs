import assert from 'node:assert';
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const fixture = fileURLToPath(new URL('./fixtures/native-runtime-budget-child.mjs', import.meta.url));
const nativeModule = fileURLToPath(new URL('../dist/native.js', import.meta.url));

test('optimized builds release process-wide native reservations', async (context) => {
	const child = fork(fixture, [nativeModule], { stdio: ['ignore', 'ignore', 'inherit', 'ipc'] });
	context.after(() => child.kill());
	assert.deepStrictEqual(await childMessage(child), {
		conflict: 'E_RESOURCE_LIMIT',
		saturated: 'E_RESOURCE_LIMIT',
	});
});

function childMessage(child) {
	return new Promise((resolve, reject) => {
		child.once('message', resolve);
		child.once('error', reject);
		child.once('exit', (code) => {
			if (code && code !== 0) reject(new Error(`runtime budget child exited with ${code}`));
		});
	});
}
