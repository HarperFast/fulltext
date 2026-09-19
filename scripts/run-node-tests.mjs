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

const isolatedFiles = files.filter((file) => path.basename(file) === 'native-worker.test.mjs');
const concurrentFiles = files.filter((file) => !isolatedFiles.includes(file));
const nodeMajorVersion = Number.parseInt(process.versions.node, 10);
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
		stdio: 'inherit',
		timeout: batch === isolatedFiles ? 120_000 : undefined,
	});
	const relativeBatch = () => batch.map((file) => path.relative(process.cwd(), file)).join(', ');
	if (result.error) {
		console.error(`Node test process failed to complete: ${relativeBatch()}`);
		throw result.error;
	}
	if (result.status !== 0) {
		if (result.status !== 1 || result.signal) {
			console.error(
				`Node test process failed with status ${result.status ?? 'unknown'}${result.signal ? ` and signal ${result.signal}` : ''}: ${relativeBatch()}`,
			);
		}
		process.exitCode ||= result.status ?? 1;
	}
}
