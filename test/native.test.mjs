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
	const addon = loadAddon();
	assert(addon.__testCreateHandle && addon.__testPanic && addon.__testCheck);
	const firstHandle = addon.__testCreateHandle();
	const secondHandle = addon.__testCreateHandle();
	assert.throws(
		() => addon.__testPanic(firstHandle),
		(error) => normalizeNativeError(error).code === 'E_NATIVE_PANIC',
	);
	assert.throws(
		() => addon.__testCheck(firstHandle),
		(error) => normalizeNativeError(error).code === 'E_POISONED',
	);
	assert.strictEqual(addon.__testCheck(secondHandle), true);
	await assert.doesNotReject(runtimeInfo());
});
