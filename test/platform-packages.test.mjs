import assert from 'node:assert';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
	integrityForTarball,
	isUnpublishedVersionError,
	parsePublishedIntegrity,
	tarballsIn,
	validateReleaseManifests,
} from '../scripts/publish-release-packages.mjs';
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

test('stages constrained native packages for every supported target', (context) => {
	const temporaryDirectory = mkdtempSync(path.join(tmpdir(), 'fulltext-platform-package-'));
	context.after(() => rmSync(temporaryDirectory, { recursive: true, force: true }));
	const manifestPath = path.join(temporaryDirectory, 'package.json');
	writeFileSync(manifestPath, JSON.stringify(rootManifest));
	assert.deepStrictEqual(supportedPlatformPackages, [
		{ triple: 'darwin-arm64', os: 'darwin', cpu: 'arm64' },
		{ triple: 'linux-arm64-gnu', os: 'linux', cpu: 'arm64', libc: 'glibc' },
		{ triple: 'linux-x64-gnu', os: 'linux', cpu: 'x64', libc: 'glibc' },
		{ triple: 'win32-x64-msvc', os: 'win32', cpu: 'x64' },
	]);

	for (const platform of supportedPlatformPackages) {
		const artifactName = `fulltext.${platform.triple}.node`;
		const artifactPath = path.join(temporaryDirectory, artifactName);
		const outputDirectory = path.join(temporaryDirectory, platform.triple);
		writeFileSync(artifactPath, 'native artifact');

		const manifest = stagePlatformPackage({
			rootManifestPath: manifestPath,
			triple: platform.triple,
			artifactPath,
			outputDirectory,
		});
		assert.strictEqual(manifest.name, `@harperfast/fulltext-${platform.triple}`);
		assert.deepStrictEqual(manifest.os, [platform.os]);
		assert.deepStrictEqual(manifest.cpu, [platform.cpu]);
		assert.deepStrictEqual(manifest.libc, platform.libc ? [platform.libc] : undefined);
		assert.strictEqual(readFileSync(path.join(outputDirectory, artifactName), 'utf8'), 'native artifact');
	}
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

test('release tarballs use unambiguous absolute paths for npm publish', (context) => {
	const temporaryDirectory = mkdtempSync(path.join(tmpdir(), 'fulltext-release-tarballs-'));
	context.after(() => rmSync(temporaryDirectory, { recursive: true, force: true }));
	const tarball = path.join(temporaryDirectory, 'package.tgz');
	writeFileSync(tarball, 'package');
	assert.deepStrictEqual(tarballsIn(path.relative(process.cwd(), temporaryDirectory)), [path.resolve(tarball)]);
});

test('release workflow marks staged platform packages as local npm inputs', () => {
	const workflow = readFileSync(new URL('../.github/workflows/publish.yml', import.meta.url), 'utf8');
	assert.match(workflow, /npm pack "\.\/release\/\$\{\{ matrix\.target \}\}"/);
	assert.doesNotMatch(workflow, /npm pack "release\/\$\{\{ matrix\.target \}\}"/);
	assert.match(workflow, /openNativeFullTextIndex/);
	assert.match(workflow, /applyMutationBatch/);
	assert.match(workflow, /index\.search/);
	assert.match(workflow, /index\.close/);
});

test('Linux arm64 is built, installed, and benchmarked on a native runner', () => {
	const publishWorkflow = readFileSync(new URL('../.github/workflows/publish.yml', import.meta.url), 'utf8');
	const ciWorkflow = readFileSync(new URL('../.github/workflows/ci.yml', import.meta.url), 'utf8');
	const benchmarkWorkflow = readFileSync(new URL('../.github/workflows/benchmark.yml', import.meta.url), 'utf8');
	assert.strictEqual(publishWorkflow.match(/target: linux-arm64-gnu/g)?.length, 2);
	assert.match(ciWorkflow, /target: linux-arm64-gnu/);
	assert.match(ciWorkflow, /os: ubuntu-22\.04-arm\s+node: '22\.18\.0'/);
	assert.match(ciWorkflow, /os: ubuntu-22\.04-arm\s+node: '24'/);
	assert.match(benchmarkWorkflow, /benchmark-native-linux-arm64-gnu\.json/);
	for (const workflow of [publishWorkflow, ciWorkflow, benchmarkWorkflow]) {
		assert.match(workflow, /ubuntu-22\.04-arm/);
	}
});

test('npm registry responses distinguish unpublished versions from malformed metadata', () => {
	assert.strictEqual(parsePublishedIntegrity(''), undefined);
	assert.strictEqual(parsePublishedIntegrity('null\n'), undefined);
	assert.strictEqual(parsePublishedIntegrity('"sha512-example"\n'), 'sha512-example');
	assert.throws(() => parsePublishedIntegrity('{}'), /invalid dist\.integrity/);
	assert.strictEqual(isUnpublishedVersionError({ stderr: 'npm error code E404' }), true);
	assert.strictEqual(isUnpublishedVersionError({ stderr: 'npm error code ETARGET' }), true);
	assert.strictEqual(isUnpublishedVersionError({ stderr: 'No matching version found for example@2.0.0' }), true);
	assert.strictEqual(isUnpublishedVersionError({ stderr: 'npm error code E401' }), false);
});
