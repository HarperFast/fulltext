import assert from 'node:assert';
import test from 'node:test';

import { verifyPackageArtifacts } from '../scripts/verify-package-artifacts.mjs';

test('package verification rejects missing and additional native artifacts', () => {
	const expected = 'fulltext.linux-x64-gnu.node';
	assert.doesNotThrow(() => verifyPackageArtifacts([expected], expected));
	assert.throws(() => verifyPackageArtifacts([], expected), /found none/);
	assert.throws(
		() => verifyPackageArtifacts([expected, 'fulltext.win32-x64-msvc.node'], expected),
		/fulltext\.win32-x64-msvc\.node/,
	);
});
