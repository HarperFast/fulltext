import { spawnSync } from 'node:child_process';
import { readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

function runPhase(phase) {
	let status = 0;
	if (phase !== 'test' && phase !== 'optimized' && phase !== 'release') {
		throw new Error('Expected the test phase to be "test", "optimized", or "release"');
	}

	const testDirectory = fileURLToPath(new URL('../test/', import.meta.url));
	const files = readdirSync(testDirectory, { recursive: true })
		.filter((entry) => typeof entry === 'string' && entry.endsWith('.test.mjs'))
		.filter((entry) => {
			if (entry.endsWith('.release.test.mjs')) return phase === 'release';
			if (entry.endsWith('.optimized.test.mjs')) return phase === 'optimized';
			return phase === 'test';
		})
		.map((entry) => path.join(testDirectory, entry));

	if (files.length === 0) {
		throw new Error(`No ${phase} test files were discovered`);
	}

	const isolatedFiles = files.filter((file) => path.basename(file).startsWith('native-worker.'));
	const qualificationNames = new Set(['metadata-workload.test.mjs', 'metadata-soak.test.mjs']);
	const qualificationFiles = files.filter((file) => qualificationNames.has(path.basename(file)));
	if (phase === 'test' && qualificationFiles.length !== qualificationNames.size) {
		throw new Error('Expected both metadata qualification test files in the serial batch');
	}
	const concurrentFiles = files.filter((file) => !isolatedFiles.includes(file) && !qualificationFiles.includes(file));
	const nodeMajorVersion = Number.parseInt(process.versions.node, 10);
	const isolatedTestTimeout = 900_000;
	for (const batch of [concurrentFiles, qualificationFiles, isolatedFiles]) {
		if (batch.length === 0) {
			continue;
		}
		const arguments_ = [
			'--test',
			'--test-reporter=spec',
			`--test-reporter=${new URL('./node-test-completion-reporter.mjs', import.meta.url).href}`,
			'--test-reporter-destination=stdout',
			'--test-reporter-destination=stdout',
		];
		if (batch === qualificationFiles) {
			console.log('Running metadata qualification files serially');
			arguments_.push('--test-concurrency=1');
		}
		if (process.env.FULLTEXT_TEST_COVERAGE === '1') {
			arguments_.push('--experimental-test-coverage', '--test-coverage-include=dist/**/*.js');
		}
		if (batch === isolatedFiles) {
			arguments_.push(nodeMajorVersion < 24 ? '--experimental-test-isolation=none' : '--test-isolation=none');
		}
		arguments_.push(...batch);
		const result = spawnSync(process.execPath, arguments_, {
			env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' },
			stdio: ['inherit', 'inherit', 'inherit', 'pipe'],
			maxBuffer: 4096,
			timeout: batch === isolatedFiles ? isolatedTestTimeout : undefined,
		});
		const relativeBatch = () => batch.map((file) => path.relative(process.cwd(), file)).join(', ');
		if (result.error) {
			console.error(
				result.error.code === 'ETIMEDOUT'
					? `Node test process timed out after ${isolatedTestTimeout / 1000}s: ${relativeBatch()}`
					: `Node test process failed to start: ${relativeBatch()}`,
			);
			throw result.error;
		}
		if (result.signal || (result.status !== 0 && result.status !== 1)) {
			console.error(
				`Node test process terminated abnormally (status ${result.status ?? 'unknown'}${result.signal ? `, signal ${result.signal}` : ''}): ${relativeBatch()}`,
			);
			return result.signal ? 2 : (result.status ?? 2);
		}
		let completion;
		try {
			completion = JSON.parse(result.output[3].toString());
		} catch {}
		if (
			completion?.completed !== true ||
			typeof completion.success !== 'boolean' ||
			Number(!completion.success) !== result.status
		) {
			console.error(
				`Node test process did not attest normal harness completion (status ${result.status}): ${relativeBatch()}`,
			);
			return 2;
		}
		status ||= result.status;
	}
	return status;
}

function runAll() {
	if (!process.env.npm_execpath) throw new Error('Run the Node test suite with npm run test:node');
	let status = 0;
	for (const step of [
		{ build: 'build:test-native' },
		{ build: 'build:typescript' },
		{ phase: 'test' },
		{ build: 'build:native' },
		{ phase: 'release' },
	]) {
		let result;
		if (step.build) {
			const child = spawnSync(process.execPath, [process.env.npm_execpath, 'run', step.build], { stdio: 'inherit' });
			if (child.error) throw child.error;
			result = child.signal ? 2 : (child.status ?? 2);
		} else {
			result = runPhase(step.phase);
			console.log(
				`Node ${step.phase === 'test' ? 'debug' : 'production'} test phase: ${result === 0 ? 'passed' : 'failed'} (status ${result})`,
			);
		}
		if (result !== 0) {
			if (step.build || result !== 1) {
				console.error(`Node suite stopping after ${step.build ?? step.phase} failed (status ${result})`);
				return result;
			}
			status = 1;
			if (step.phase === 'test') console.log('Continuing to production qualification after debug test failures');
		}
	}
	return status;
}

try {
	process.exitCode = process.argv[2] === 'all' ? runAll() : runPhase(process.argv[2]);
	if (process.argv[2] === 'all')
		console.log(`Node suite: ${process.exitCode ? 'failed' : 'passed'} (status ${process.exitCode})`);
} catch (error) {
	console.error(error);
	process.exitCode = 2;
}
