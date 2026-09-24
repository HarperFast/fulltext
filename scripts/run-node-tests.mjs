import { spawnSync } from 'node:child_process';
import { readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const phase = process.argv[2];
if (phase !== 'test' && phase !== 'release') {
	throw new Error('Expected the test phase to be "test" or "release"');
}

const testDirectory = fileURLToPath(new URL('../test/', import.meta.url));
const files = readdirSync(testDirectory, { recursive: true })
	.filter((entry) => typeof entry === 'string' && entry.endsWith('.test.mjs'))
	.filter((entry) => (phase === 'release') === entry.endsWith('.release.test.mjs'))
	.map((entry) => path.join(testDirectory, entry));

if (files.length === 0) {
	throw new Error(`No ${phase} test files were discovered`);
}

const isolatedFiles = files.filter((file) => path.basename(file).startsWith('native-worker.'));
const concurrentFiles = files.filter((file) => !isolatedFiles.includes(file));
const nodeMajorVersion = Number.parseInt(process.versions.node, 10);
const isolatedTestTimeout = 1_800_000;
for (const batch of [concurrentFiles, isolatedFiles]) {
	if (batch.length === 0) {
		continue;
	}
	const arguments_ = ['--test'];
	if (batch === isolatedFiles) {
		arguments_.push(nodeMajorVersion < 24 ? '--experimental-test-isolation=none' : '--test-isolation=none');
	}
	arguments_.push(...batch);
	const result = spawnSync(process.execPath, arguments_, {
		env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' },
		stdio: 'inherit',
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
	if (result.status !== 0) {
		if (result.status !== 1 || result.signal) {
			console.error(
				`Node test process failed with status ${result.status ?? 'unknown'}${result.signal ? ` and signal ${result.signal}` : ''}: ${relativeBatch()}`,
			);
		}
		const status = result.status ?? 1;
		if (!process.exitCode || (process.exitCode === 1 && status !== 1)) {
			process.exitCode = status;
		}
	}
}
