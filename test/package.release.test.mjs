import assert from 'node:assert';
import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

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
	assert(includedPaths.includes('dist/harper.js'));
	assert(includedPaths.some((file) => /^fulltext\..+\.node$/.test(file)));
	assert(!includedPaths.some((file) => file.startsWith('src/') || file === 'ts/addon.d.ts'));
	assert.doesNotMatch(readFileSync(new URL('../ts/addon.d.ts', import.meta.url), 'utf8'), /__(?:test|phase0)/);

	const projectDirectory = path.join(temporaryDirectory, 'consumer');
	mkdirSync(projectDirectory);
	writeFileSync(
		path.join(projectDirectory, 'package.json'),
		JSON.stringify({
			type: 'module',
			private: true,
			dependencies: { '@harperfast/fulltext': `file:${path.join(temporaryDirectory, filename)}` },
		}),
	);
	runNpm(['install', '--package-lock-only', '--offline', '--ignore-scripts', '--no-audit', '--no-fund'], {
		cwd: projectDirectory,
		stdio: 'pipe',
	});
	runNpm(['ci', '--offline', '--ignore-scripts', '--no-audit', '--no-fund'], {
		cwd: projectDirectory,
		stdio: 'pipe',
	});
	const installedManifest = JSON.parse(
		readFileSync(path.join(projectDirectory, 'node_modules/@harperfast/fulltext/package.json'), 'utf8'),
	);
	for (const lifecycle of ['preinstall', 'install', 'postinstall', 'prepare']) {
		assert(!installedManifest.scripts?.[lifecycle], `${lifecycle} must not run in a consumer installation`);
	}
	const artifact = includedPaths.find((file) => /^fulltext\..+\.node$/.test(file));
	const installedAddonPath = path.join(projectDirectory, 'node_modules/@harperfast/fulltext', artifact);
	const output = execFileSync(
		process.execPath,
		[
			'--input-type=module',
			'--eval',
			"import { createRequire } from 'node:module'; const addon = createRequire(import.meta.url)(process.argv[1]); if (Object.keys(addon).some(key => key.startsWith('__test') || key.startsWith('__phase0'))) process.exit(1); await import('@harperfast/fulltext/harper'); console.log(await import('@harperfast/fulltext/native').then(x => x.runtimeInfo()));",
			installedAddonPath,
		],
		{ cwd: projectDirectory, encoding: 'utf8' },
	);
	assert.match(output, /tantivyVersion: '0\.26\.1'/);
});
