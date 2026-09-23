import assert from 'node:assert';
import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { platformPackageName, stagePlatformPackage } from '../scripts/platform-packages.mjs';
import { platformTriple } from '../dist/load-addon.js';

const npmCommand = process.env.npm_execpath
	? { executable: process.execPath, prefix: [process.env.npm_execpath] }
	: { executable: process.platform === 'win32' ? process.env.ComSpec || 'cmd.exe' : 'npm', prefix: [] };

function runNpm(arguments_, options) {
	const argumentsWithPrefix = [...npmCommand.prefix];
	if (process.platform === 'win32' && npmCommand.prefix.length === 0) {
		argumentsWithPrefix.push('/d', '/s', '/c', 'npm.cmd');
	}
	argumentsWithPrefix.push(...arguments_);
	return execFileSync(npmCommand.executable, argumentsWithPrefix, options);
}

test('the packed package loads without consumer lifecycle scripts', (context) => {
	const temporaryDirectory = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-'));
	context.after(() => rmSync(temporaryDirectory, { force: true, recursive: true }));
	const packOutput = runNpm(
		['pack', '--json', '--foreground-scripts=false', '--pack-destination', temporaryDirectory],
		{
			encoding: 'utf8',
		},
	);
	const parsedOutput = JSON.parse(packOutput);
	const pack = Array.isArray(parsedOutput) ? parsedOutput[0] : parsedOutput['@harperfast/fulltext'];
	const { filename, files } = pack;
	const includedPaths = files.map((file) => file.path);
	assert(includedPaths.includes('dist/native.js'));
	assert(includedPaths.includes('dist/native.d.ts'));
	assert(!includedPaths.includes('dist/harper.js'));
	assert(!includedPaths.includes('dist/host-storage.js'));
	assert(!includedPaths.some((file) => /^fulltext\..+\.node$/.test(file)));
	assert(!includedPaths.some((file) => file.startsWith('src/') || file === 'ts/addon.d.ts'));
	assert.doesNotMatch(readFileSync(new URL('../ts/addon.d.ts', import.meta.url), 'utf8'), /__(?:test|phase0)/);
	const rootManifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
	const triple = platformTriple();
	const nativePackageName = platformPackageName(rootManifest.name, triple);
	const nativePackageDirectory = path.join(temporaryDirectory, 'native-package');
	stagePlatformPackage({
		rootManifestPath: new URL('../package.json', import.meta.url),
		triple,
		artifactPath: new URL(`../fulltext.${triple}.node`, import.meta.url),
		outputDirectory: nativePackageDirectory,
	});
	const nativePackOutput = runNpm(
		['pack', '--json', '--foreground-scripts=false', '--pack-destination', temporaryDirectory],
		{ cwd: nativePackageDirectory, encoding: 'utf8' },
	);
	const nativeParsedOutput = JSON.parse(nativePackOutput);
	const nativePack = Array.isArray(nativeParsedOutput) ? nativeParsedOutput[0] : nativeParsedOutput[nativePackageName];
	assert.deepStrictEqual(nativePack.files.map((file) => file.path).sort(), [
		'README.md',
		`fulltext.${triple}.node`,
		'package.json',
	]);

	const projectDirectory = path.join(temporaryDirectory, 'consumer');
	mkdirSync(projectDirectory);
	writeFileSync(
		path.join(projectDirectory, 'package.json'),
		JSON.stringify({
			type: 'module',
			private: true,
			dependencies: {
				'@harperfast/fulltext': `file:${path.join(temporaryDirectory, filename)}`,
				[nativePackageName]: `file:${path.join(temporaryDirectory, nativePack.filename)}`,
			},
		}),
	);
	runNpm(['install', '--offline', '--ignore-scripts', '--no-audit', '--no-fund'], {
		cwd: projectDirectory,
		stdio: 'pipe',
	});
	const installedManifest = JSON.parse(
		readFileSync(path.join(projectDirectory, 'node_modules/@harperfast/fulltext/package.json'), 'utf8'),
	);
	for (const lifecycle of ['preinstall', 'install', 'postinstall', 'prepare']) {
		assert(!installedManifest.scripts?.[lifecycle], `${lifecycle} must not run in a consumer installation`);
	}
	assert.deepStrictEqual(Object.keys(installedManifest.exports), ['./native']);
	assert.strictEqual(installedManifest.optionalDependencies[nativePackageName], installedManifest.version);
	const installedNativeManifest = JSON.parse(
		readFileSync(path.join(projectDirectory, 'node_modules', ...nativePackageName.split('/'), 'package.json'), 'utf8'),
	);
	assert.strictEqual(installedNativeManifest.version, installedManifest.version);
	const consumerEnvironment = { ...process.env };
	delete consumerEnvironment.FULLTEXT_PREFER_LOCAL_BUILD;
	const output = execFileSync(
		process.execPath,
		[
			'--input-type=module',
			'--eval',
			"import { createRequire } from 'node:module'; const addon = createRequire(import.meta.url)(process.argv[1]); if (Object.keys(addon).some(key => key.startsWith('__test') || key.startsWith('__phase0') || key.startsWith('__harper') || key.startsWith('__hostStorage'))) process.exit(1); try { await import('@harperfast/fulltext/harper'); process.exit(2); } catch (error) { if (error.code !== 'ERR_PACKAGE_PATH_NOT_EXPORTED') throw error; } const info = await import('@harperfast/fulltext/native').then(x => x.runtimeInfo()); if (info.storageBackends.join(',') !== 'native') process.exit(3); console.log(info);",
			nativePackageName,
		],
		{ cwd: projectDirectory, encoding: 'utf8', env: consumerEnvironment },
	);
	assert.match(output, /tantivyVersion: '0\.26\.1'/);
});
