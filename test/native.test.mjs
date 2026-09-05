import assert from 'node:assert';
import test from 'node:test';

import { runtimeInfo } from '@harperfast/fulltext/native';
import { normalizeNativeError } from '../dist/errors.js';
import { loadAddon, platformTriple } from '../dist/load-addon.js';

test('loads the artifact for the executing platform', async () => {
	const info = await runtimeInfo();
	assert.deepStrictEqual(info, {
		packageVersion: '0.0.0',
		tantivyVersion: '0.26.1',
		nativeAbiVersion: 1,
		storageBackends: ['native'],
	});
	assert.match(platformTriple(), /^(darwin|linux|win32)-(arm64|x64)(-(gnu|musl|msvc))?$/);
});

test('turns a panic into a coded terminal error', async () => {
	const firstHandle = new (loadAddon().TestHandle)();
	const secondHandle = new (loadAddon().TestHandle)();
	assert.throws(
		() => firstHandle.panic(),
		(error) => normalizeNativeError(error).code === 'E_NATIVE_PANIC',
	);
	assert.throws(
		() => firstHandle.check(),
		(error) => normalizeNativeError(error).code === 'E_POISONED',
	);
	assert.strictEqual(secondHandle.check(), true);
	await assert.doesNotReject(runtimeInfo());
});
