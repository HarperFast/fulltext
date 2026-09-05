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
	assert.throws(
		() => loadAddon().__testPanic(),
		(error) => normalizeNativeError(error).code === 'E_NATIVE_PANIC',
	);
	await assert.rejects(runtimeInfo(), { code: 'E_POISONED' });
});
