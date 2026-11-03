import assert from 'node:assert';
import { spawnSync } from 'node:child_process';
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

const manifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const npmCommand = process.env.npm_execpath
	? { executable: process.execPath, prefix: [process.env.npm_execpath] }
	: process.platform === 'win32'
		? { executable: process.env.ComSpec || 'cmd.exe', prefix: ['/d', '/s', '/c', 'npm.cmd'] }
		: { executable: 'npm', prefix: [] };

function runFixture(context, { exits = {}, termination, missingDirectory = false } = {}) {
	const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-node-phases-'));
	context.after(() => rmSync(directory, { recursive: true, force: true }));
	mkdirSync(path.join(directory, 'scripts'));
	for (const name of ['run-node-tests.mjs', 'node-test-completion-reporter.mjs']) {
		copyFileSync(new URL(`../scripts/${name}`, import.meta.url), path.join(directory, 'scripts', name));
	}
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
const status = JSON.parse(process.env.FIXTURE_EXITS)[step] ?? 0;
if (status === 0 && step !== 'typescript-build') writeFileSync('addon-kind', step);
process.exitCode = status;
`,
	);
	if (!missingDirectory) {
		mkdirSync(path.join(directory, 'test'));
		for (const name of [
			'ordinary',
			'metadata-workload',
			'metadata-soak',
			'native-worker.fixture',
			'production.release',
		]) {
			const action =
				name === 'ordinary' && termination === 'nested-kill'
					? "process.kill(process.ppid, 'SIGKILL');"
					: name === 'ordinary' && termination === 'interrupted'
						? "process.kill(process.ppid, 'SIGTERM');"
						: name === 'native-worker.fixture' && termination === 'exit-one'
							? 'process.exit(1);'
							: name === 'native-worker.fixture' && termination === 'exit-seven'
								? 'process.exit(7);'
								: name === 'native-worker.fixture' && termination === 'mismatch'
									? "process.on('exit', () => { process.exitCode = 1; });"
									: '';
			writeFileSync(
				path.join(directory, 'test', `${name}.test.mjs`),
				`import assert from 'node:assert';
import { appendFileSync } from 'node:fs';
import test from 'node:test';
appendFileSync('events.jsonl', JSON.stringify('${name}') + '\\n');
${action}
test('fixture ${name}', () => assert.strictEqual(JSON.parse(process.env.FIXTURE_EXITS)['${name}'] ?? 0, 0));
`,
			);
		}
	}
	const environment = { ...process.env, FIXTURE_EXITS: JSON.stringify(exits) };
	delete environment.NODE_TEST_CONTEXT;
	delete environment.FULLTEXT_TEST_COVERAGE;
	const result = spawnSync(npmCommand.executable, [...npmCommand.prefix, 'run', 'test:node'], {
		cwd: directory,
		env: environment,
		encoding: 'utf8',
		timeout: 60_000,
	});
	assert.ifError(result.error);
	const events = readFileSync(path.join(directory, 'events.jsonl'), 'utf8').trim().split('\n').map(JSON.parse);
	return { ...result, directory, events };
}

function assertDebugBatches(events, diagnostics) {
	assert.deepStrictEqual(events.slice(0, 3), ['debug-build', 'typescript-build', 'ordinary'], diagnostics);
	assert.deepStrictEqual(events.slice(3, 5).sort(), ['metadata-soak', 'metadata-workload'], diagnostics);
	assert.strictEqual(events[5], 'native-worker.fixture', diagnostics);
}

for (const [debugStatus, releaseStatus] of [
	[0, 0],
	[1, 0],
	[0, 1],
	[1, 1],
]) {
	test(`Node suite completes both real test phases with verdicts ${debugStatus} and ${releaseStatus}`, (context) => {
		const result = runFixture(context, { exits: { ordinary: debugStatus, 'production.release': releaseStatus } });
		const diagnostics = result.stdout + result.stderr;
		assertDebugBatches(result.events, diagnostics);
		assert.deepStrictEqual(result.events.slice(6), ['release-build', 'production.release'], diagnostics);
		assert.strictEqual(result.status, debugStatus || releaseStatus, diagnostics);
		assert.match(result.stdout, new RegExp(`Node debug test phase: ${debugStatus ? 'failed' : 'passed'}`));
		assert.match(result.stdout, new RegExp(`Node production test phase: ${releaseStatus ? 'failed' : 'passed'}`));
		if (debugStatus) assert.match(result.stdout, /Continuing to production qualification after debug test failures/);
		assert.match(result.stdout, new RegExp(`Node suite: ${debugStatus || releaseStatus ? 'failed' : 'passed'}`));
		assert.strictEqual(readFileSync(path.join(result.directory, 'addon-kind'), 'utf8'), 'release-build');
	});
}

for (const step of ['debug-build', 'typescript-build', 'release-build']) {
	test(`Node suite stops after a failed ${step}`, (context) => {
		const result = runFixture(context, { exits: { [step]: 7 } });
		const diagnostics = result.stdout + result.stderr;
		assert.strictEqual(result.status, 7, diagnostics);
		assert.strictEqual(result.events.at(-1), step, diagnostics);
		assert.match(result.stderr, /Node suite stopping after .* failed \(status 7\)/);
	});
}

test('a production build failure replaces an earlier assertion verdict', (context) => {
	const result = runFixture(context, { exits: { ordinary: 1, 'release-build': 7 } });
	assert.strictEqual(result.status, 7, result.stdout + result.stderr);
	assert.strictEqual(result.events.at(-1), 'release-build');
});

test('test discovery setup failure stops before production', (context) => {
	const result = runFixture(context, { missingDirectory: true });
	assert.strictEqual(result.status, 2, result.stdout + result.stderr);
	assert.deepStrictEqual(result.events, ['debug-build', 'typescript-build']);
	assert.match(result.stderr, /ENOENT/);
});

test('an actual harness exit 1 without completion attestation stops before production', (context) => {
	const result = runFixture(context, { termination: 'exit-one' });
	assertDebugBatches(result.events, result.stdout + result.stderr);
	assert.strictEqual(result.events.length, 6);
	assert.strictEqual(result.status, 2, result.stdout + result.stderr);
	assert.match(result.stderr, /did not attest normal harness completion \(status 1\)/);
});

test('an abnormal actual harness exit preserves its status and stops before production', (context) => {
	const result = runFixture(context, { termination: 'exit-seven' });
	assertDebugBatches(result.events, result.stdout + result.stderr);
	assert.strictEqual(result.events.length, 6);
	assert.strictEqual(result.status, 7, result.stdout + result.stderr);
});

for (const termination of ['nested-kill', 'interrupted']) {
	test(`${termination} stops remaining batches and production on every platform`, (context) => {
		const result = runFixture(context, { termination });
		assert.strictEqual(result.status, 2, result.stdout + result.stderr);
		assert.deepStrictEqual(result.events, ['debug-build', 'typescript-build', 'ordinary']);
		assert.match(result.stderr, /terminated abnormally|did not attest normal harness completion/);
	});
}

test('a completed summary inconsistent with the process exit status is rejected', (context) => {
	const result = runFixture(context, { termination: 'mismatch' });
	assertDebugBatches(result.events, result.stdout + result.stderr);
	assert.strictEqual(result.events.length, 6);
	assert.strictEqual(result.status, 2, result.stdout + result.stderr);
	assert.match(result.stderr, /did not attest normal harness completion/);
});
