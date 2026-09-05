import assert from 'node:assert';
import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

const npmInvocation = process.env.npm_execpath
	? { executable: process.execPath, prefix: [process.env.npm_execpath] }
	: { executable: process.platform === 'win32' ? process.env.ComSpec || 'cmd.exe' : 'npm', prefix: [] };

function runNpm(arguments_, options) {
	const argumentsWithPrefix = [...npmInvocation.prefix];
	if (process.platform === 'win32' && npmInvocation.prefix.length === 0) {
		argumentsWithPrefix.push('/d', '/s', '/c', 'npm.cmd');
	}
	argumentsWithPrefix.push(...arguments_);
	return execFileSync(npmInvocation.executable, argumentsWithPrefix, options);
}

test('the packed package loads without consumer lifecycle scripts', (context) => {
	const temporaryDirectory = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-'));
	context.after(() => rmSync(temporaryDirectory, { force: true, recursive: true }));
	const packOutput = runNpm(['pack', '--json', '--pack-destination', temporaryDirectory], {
		encoding: 'utf8',
	});
	const parsedOutput = JSON.parse(packOutput);
	const pack = Array.isArray(parsedOutput) ? parsedOutput[0] : parsedOutput['@harperfast/fulltext'];
	const { filename, files } = pack;
	const includedPaths = files.map((file) => file.path);
	assert(includedPaths.includes('dist/native.js'));
	assert(includedPaths.some((file) => /^fulltext\..+\.node$/.test(file)));
	assert(!includedPaths.some((file) => file.startsWith('src/') || file === 'ts/addon.d.ts'));

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
	const require = createRequire(import.meta.url);
	const installedAddon = require(path.join(projectDirectory, 'node_modules/@harperfast/fulltext', artifact));
	assert(!('TestHandle' in installedAddon));
	const output = execFileSync(
		process.execPath,
		[
			'--input-type=module',
			'--eval',
			"import('@harperfast/fulltext/native').then(x => x.runtimeInfo()).then(console.log)",
		],
		{ cwd: projectDirectory, encoding: 'utf8' },
	);
	assert.match(output, /tantivyVersion: '0\.26\.1'/);
});
