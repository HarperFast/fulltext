import assert from 'node:assert';
import test from 'node:test';

import { verifyPackageArtifacts } from '../scripts/verify-package-artifacts.mjs';

const validManifest = {
	name: '@harperfast/fulltext',
	version: '0.1.0',
	files: ['dist/', 'README.md'],
	optionalDependencies: {
		'@harperfast/fulltext-darwin-arm64': '0.1.0',
		'@harperfast/fulltext-linux-arm64-gnu': '0.1.0',
		'@harperfast/fulltext-linux-x64-gnu': '0.1.0',
		'@harperfast/fulltext-win32-x64-msvc': '0.1.0',
	},
};

test('root package verification enforces exact platform packages without a bundled binary', () => {
	assert.doesNotThrow(() => verifyPackageArtifacts(validManifest));
	assert.throws(
		() => verifyPackageArtifacts({ ...validManifest, files: [...validManifest.files, 'fulltext.*.node'] }),
		/must not include a native artifact/,
	);
	assert.throws(
		() =>
			verifyPackageArtifacts({
				...validManifest,
				optionalDependencies: {
					...validManifest.optionalDependencies,
					'@harperfast/fulltext-linux-x64-gnu': '0.1.1',
				},
			}),
		/pinned to 0\.1\.0/,
	);
});
