import assert from 'node:assert';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { integrityForTarball, validateReleaseManifests } from '../scripts/publish-release-packages.mjs';
import { platformPackageName, stagePlatformPackage, supportedPlatformPackages } from '../scripts/platform-packages.mjs';

const rootManifest = {
	name: '@harperfast/fulltext',
	version: '0.1.0',
	license: 'Apache-2.0',
	repository: { type: 'git', url: 'git+https://github.com/HarperFast/fulltext.git' },
	engines: { node: '^22.18.0 || >=24.0.0' },
	files: ['dist/'],
	optionalDependencies: Object.fromEntries(
		supportedPlatformPackages.map(({ triple }) => [`@harperfast/fulltext-${triple}`, '0.1.0']),
	),
};

test('stages a constrained native package for a supported target', (context) => {
	const temporaryDirectory = mkdtempSync(path.join(tmpdir(), 'fulltext-platform-package-'));
	context.after(() => rmSync(temporaryDirectory, { recursive: true, force: true }));
	const manifestPath = path.join(temporaryDirectory, 'package.json');
	const artifactPath = path.join(temporaryDirectory, 'fulltext.linux-x64-gnu.node');
	const outputDirectory = path.join(temporaryDirectory, 'output');
	writeFileSync(manifestPath, JSON.stringify(rootManifest));
	writeFileSync(artifactPath, 'native artifact');

	const manifest = stagePlatformPackage({
		rootManifestPath: manifestPath,
		triple: 'linux-x64-gnu',
		artifactPath,
		outputDirectory,
	});
	assert.strictEqual(manifest.name, '@harperfast/fulltext-linux-x64-gnu');
	assert.deepStrictEqual(manifest.os, ['linux']);
	assert.deepStrictEqual(manifest.cpu, ['x64']);
	assert.deepStrictEqual(manifest.libc, ['glibc']);
	assert.strictEqual(
		readFileSync(path.join(outputDirectory, 'fulltext.linux-x64-gnu.node'), 'utf8'),
		'native artifact',
	);
});

test('release validation requires every exact-version platform package', () => {
	const nativeManifests = supportedPlatformPackages.map((platform) => ({
		name: platformPackageName(rootManifest.name, platform.triple),
		version: rootManifest.version,
		os: [platform.os],
		cpu: [platform.cpu],
		...(platform.libc ? { libc: [platform.libc] } : {}),
	}));
	assert.strictEqual(validateReleaseManifests([rootManifest, ...nativeManifests]), rootManifest);
	assert.throws(
		() => validateReleaseManifests([rootManifest, ...nativeManifests.slice(1)]),
		/Missing release packages/,
	);
	assert.throws(
		() => validateReleaseManifests([rootManifest, ...nativeManifests, { name: '@harperfast/fulltext-other' }]),
		/Unexpected release package/,
	);
	assert.throws(
		() =>
			validateReleaseManifests([
				rootManifest,
				{ ...nativeManifests[0], version: '0.1.1' },
				...nativeManifests.slice(1),
			]),
		/expected 0\.1\.0/,
	);
});

test('tarball integrity is stable and content-sensitive', (context) => {
	const temporaryDirectory = mkdtempSync(path.join(tmpdir(), 'fulltext-integrity-'));
	context.after(() => rmSync(temporaryDirectory, { recursive: true, force: true }));
	const tarball = path.join(temporaryDirectory, 'package.tgz');
	writeFileSync(tarball, 'first');
	const first = integrityForTarball(tarball);
	assert.match(first, /^sha512-/);
	assert.strictEqual(integrityForTarball(tarball), first);
	writeFileSync(tarball, 'second');
	assert.notStrictEqual(integrityForTarball(tarball), first);
});
