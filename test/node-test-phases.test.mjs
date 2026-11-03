import assert from 'node:assert';
import { spawnSync } from 'node:child_process';
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

const manifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const npmCommand = process.env.npm_execpath
	? { executable: process.execPath, prefix: [process.env.npm_execpath] }
	: process.platform === 'win32'
		? { executable: process.env.ComSpec || 'cmd.exe', prefix: ['/d', '/s', '/c', 'npm.cmd'] }
		: { executable: 'npm', prefix: [] };

function runFixture(context, exits = {}, signalStep, runnerKind) {
	const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-node-phases-'));
	context.after(() => rmSync(directory, { recursive: true, force: true }));
	mkdirSync(path.join(directory, 'scripts'));
	copyFileSync(
		new URL('../scripts/run-node-suite.mjs', import.meta.url),
		path.join(directory, 'scripts/run-node-suite.mjs'),
	);
	writeFileSync(
		path.join(directory, 'package.json'),
		JSON.stringify({
			private: true,
			type: 'module',
			scripts: {
				'test:node': manifest.scripts['test:node'],
				'build:test-native': 'node step.mjs debug-build',
				'build:typescript': 'node step.mjs typescript-build',
				'build:native': 'node step.mjs release-build',
			},
		}),
	);
	writeFileSync(
		path.join(directory, 'step.mjs'),
		String.raw`import { appendFileSync, writeFileSync } from 'node:fs';
const step = process.argv[2];
appendFileSync('events.jsonl', JSON.stringify(step) + '\n');
if (step === process.env.FIXTURE_SIGNAL_STEP) process.kill(process.pid, 'SIGTERM');
const status = JSON.parse(process.env.FIXTURE_EXITS)[step] ?? 0;
if (status === 0 && step.endsWith('-build') && step !== 'typescript-build') writeFileSync('addon-kind', step);
process.exitCode = status;
`,
	);
	writeFileSync(path.join(directory, 'scripts/run-node-tests.mjs'), "import '../step.mjs';\n");
	if (runnerKind) {
		copyFileSync(
			new URL('../scripts/run-node-tests.mjs', import.meta.url),
			path.join(directory, 'scripts/run-node-tests.mjs'),
		);
		if (runnerKind === 'signal' || runnerKind === 'assertion') {
			mkdirSync(path.join(directory, 'test'));
			for (const name of [
				'ordinary',
				'metadata-workload',
				'metadata-soak',
				'native-worker.fixture',
				'production.release',
			]) {
				writeFileSync(
					path.join(directory, 'test', `${name}.test.mjs`),
					`import { appendFileSync } from 'node:fs';
appendFileSync('events.jsonl', JSON.stringify('${name}') + '\\n');
${name === 'ordinary' ? (runnerKind === 'signal' ? "process.kill(process.ppid, 'SIGKILL');" : "import assert from 'node:assert'; assert.fail('fixture assertion verdict');") : ''}
`,
				);
			}
		}
	}
	const fixtureEnvironment = {
		...process.env,
		FIXTURE_EXITS: JSON.stringify(exits),
		FIXTURE_SIGNAL_STEP: signalStep ?? '',
	};
	delete fixtureEnvironment.NODE_TEST_CONTEXT;
	const result = spawnSync(npmCommand.executable, [...npmCommand.prefix, 'run', 'test:node'], {
		cwd: directory,
		env: fixtureEnvironment,
		encoding: 'utf8',
		timeout: 60_000,
	});
	assert.ifError(result.error);
	assert(existsSync(path.join(directory, 'events.jsonl')), result.stdout + result.stderr);
	const events = readFileSync(path.join(directory, 'events.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
	return { ...result, directory, events };
}

const allSteps = ['debug-build', 'typescript-build', 'test', 'release-build', 'release'];
for (const [debugStatus, releaseStatus] of [
	[0, 0],
	[1, 0],
	[0, 1],
	[1, 1],
]) {
	test(`Node suite runs both test phases with verdicts ${debugStatus} and ${releaseStatus}`, (context) => {
		const result = runFixture(context, { test: debugStatus, release: releaseStatus });
		assert.deepStrictEqual(result.events, allSteps, result.stdout + result.stderr);
		assert.strictEqual(result.status, debugStatus || releaseStatus, result.stdout + result.stderr);
		assert.match(
			result.stdout,
			new RegExp(`Node debug test phase: ${debugStatus ? 'failed' : 'passed'} \\(status ${debugStatus}\\)`),
		);
		assert.match(
			result.stdout,
			new RegExp(`Node production test phase: ${releaseStatus ? 'failed' : 'passed'} \\(status ${releaseStatus}\\)`),
		);
		if (debugStatus) assert.match(result.stdout, /Continuing to production qualification after debug test failures/);
		assert.match(
			result.stdout,
			new RegExp(
				`Node suite: ${debugStatus || releaseStatus ? 'failed' : 'passed'} \\(status ${debugStatus || releaseStatus}\\)`,
			),
		);
		assert.strictEqual(readFileSync(path.join(result.directory, 'addon-kind'), 'utf8'), 'release-build');
	});
}

for (const [step, expected] of [
	['debug-build', ['debug-build']],
	['typescript-build', ['debug-build', 'typescript-build']],
	['release-build', ['debug-build', 'typescript-build', 'test', 'release-build']],
]) {
	test(`Node suite aborts after a failed ${step}`, (context) => {
		const result = runFixture(context, { [step]: 7 });
		assert.deepStrictEqual(result.events, expected, result.stdout + result.stderr);
		assert.strictEqual(result.status, 7, result.stdout + result.stderr);
		assert.match(result.stderr, /Node suite stopping after .* failed \(status 7\)/);
	});
}

test('Node suite aborts when its test process receives a signal', (context) => {
	const result = runFixture(context, {}, 'test');
	assert.deepStrictEqual(result.events, ['debug-build', 'typescript-build', 'test'], result.stdout + result.stderr);
	assert.notStrictEqual(result.status, 0, result.stdout + result.stderr);
	assert.match(result.stderr, /terminated by SIGTERM/);
});

test('Node suite aborts and preserves abnormal test process status', (context) => {
	const result = runFixture(context, { test: 7 });
	assert.deepStrictEqual(result.events, ['debug-build', 'typescript-build', 'test'], result.stdout + result.stderr);
	assert.strictEqual(result.status, 7, result.stdout + result.stderr);
});

test('Node suite preserves a production build failure after a debug assertion failure', (context) => {
	const result = runFixture(context, { test: 1, 'release-build': 7 });
	assert.deepStrictEqual(
		result.events,
		['debug-build', 'typescript-build', 'test', 'release-build'],
		result.stdout + result.stderr,
	);
	assert.strictEqual(result.status, 7, result.stdout + result.stderr);
});

test('Node suite aborts after real test discovery setup fails', (context) => {
	const result = runFixture(context, {}, undefined, 'missing-directory');
	assert.deepStrictEqual(result.events, ['debug-build', 'typescript-build'], result.stdout + result.stderr);
	assert.strictEqual(result.status, 2, result.stdout + result.stderr);
	assert.match(result.stderr, /ENOENT/);
});

test('Node suite aborts remaining batches and release after a nested test batch signal', (context) => {
	const result = runFixture(context, {}, undefined, 'signal');
	assert.deepStrictEqual(result.events, ['debug-build', 'typescript-build', 'ordinary'], result.stdout + result.stderr);
	assert.strictEqual(result.status, 2, result.stdout + result.stderr);
	assert.match(result.stderr, /signal SIGKILL/);
});

test('Node suite continues all real debug batches and production after an assertion verdict', (context) => {
	const result = runFixture(context, {}, undefined, 'assertion');
	assert.strictEqual(result.status, 1, result.stdout + result.stderr);
	assert.deepStrictEqual(result.events.slice(0, 3), ['debug-build', 'typescript-build', 'ordinary']);
	assert.deepStrictEqual(result.events.slice(3, 5).sort(), ['metadata-soak', 'metadata-workload']);
	assert.deepStrictEqual(result.events.slice(5), ['native-worker.fixture', 'release-build', 'production.release']);
	assert.strictEqual(readFileSync(path.join(result.directory, 'addon-kind'), 'utf8'), 'release-build');
});
