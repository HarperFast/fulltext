import assert from 'node:assert';
import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { platformPackageName, platformTriple } from '../dist/load-addon.js';

test('platform package wins over an adjacent development artifact', (context) => {
	const fixture = createFixture(context, { nativeVersion: '0.1.0' });
	writeFileSync(path.join(fixture.root, `fulltext.${fixture.triple}.node`), 'not a native binary');
	assert.strictEqual(runFixture(fixture.root, 'console.log(loadAddon().source)'), 'platform');
});

test('native package version skew is rejected', (context) => {
	const fixture = createFixture(context, { nativeVersion: '0.1.1' });
	assert.strictEqual(
		runFixture(fixture.root, 'try { loadAddon(); } catch (error) { console.log(`${error.code}:${error.message}`); }'),
		'E_NATIVE_CAPABILITY_MISMATCH:Fulltext package 0.1.0 cannot load native package 0.1.1 from ' + fixture.packageName,
	);
});

test('a present but unloadable native package produces a typed error with its cause', (context) => {
	const fixture = createFixture(context, { loadFailure: true });
	assert.strictEqual(
		runFixture(
			fixture.root,
			'try { loadAddon(); } catch (error) { console.log(`${error.code}:${error.cause?.message}`); }',
		),
		'E_NATIVE_LOAD_FAILED:fixture load failure',
	);
});

test('missing packages and unsupported musl targets have actionable names', (context) => {
	const fixture = createFixture(context, { omitPackage: true });
	const missing = runFixture(
		fixture.root,
		'try { loadAddon(); } catch (error) { console.log(`${error.code}:${error.message}`); }',
	);
	assert.match(missing, new RegExp(`^E_NATIVE_ADDON_NOT_FOUND:.*${fixture.packageName.replaceAll('/', '\\/')}$`));
	const muslTriple = platformTriple({ platform: 'linux', architecture: 'x64', glibc: false });
	assert.strictEqual(muslTriple, 'linux-x64-musl');
	assert.strictEqual(platformPackageName(muslTriple), '@harperfast/fulltext-linux-x64-musl');
});

function createFixture(context, options) {
	const root = mkdtempSync(path.join(tmpdir(), 'fulltext-loader-'));
	context.after(() => rmSync(root, { recursive: true, force: true }));
	mkdirSync(path.join(root, 'dist'), { recursive: true });
	copyFileSync(new URL('../dist/load-addon.js', import.meta.url), path.join(root, 'dist/load-addon.js'));
	copyFileSync(new URL('../dist/errors.js', import.meta.url), path.join(root, 'dist/errors.js'));
	writeFileSync(
		path.join(root, 'package.json'),
		JSON.stringify({ name: '@harperfast/fulltext', version: '0.1.0', type: 'module' }),
	);
	const triple = platformTriple();
	const packageName = platformPackageName(triple);
	if (!options.omitPackage) {
		const packageDirectory = path.join(root, 'node_modules', ...packageName.split('/'));
		mkdirSync(packageDirectory, { recursive: true });
		writeFileSync(
			path.join(packageDirectory, 'package.json'),
			JSON.stringify({ name: packageName, version: options.nativeVersion ?? '0.1.0', main: 'index.cjs' }),
		);
		writeFileSync(
			path.join(packageDirectory, 'index.cjs'),
			options.loadFailure
				? "throw new Error('fixture load failure');\n"
				: `module.exports = {
					source: 'platform',
					runtimeInfo() { return { packageVersion: ${JSON.stringify(options.nativeVersion)}, nativeAbiVersion: 6, queryApiVersion: 1, storageBackends: ['native'] }; },
					__nativeInspect() {}, __nativeValidateOpen() {}, __nativeReset() {}
				};\n`,
		);
	}
	return { root, triple, packageName };
}

function runFixture(root, statement) {
	const environment = { ...process.env };
	delete environment.FULLTEXT_PREFER_LOCAL_BUILD;
	return execFileSync(
		process.execPath,
		['--input-type=module', '--eval', `import { loadAddon } from './dist/load-addon.js'; ${statement}`],
		{ cwd: root, encoding: 'utf8', env: environment },
	).trim();
}
