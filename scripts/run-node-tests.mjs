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

const result = spawnSync(process.execPath, ['--test', ...files], { stdio: 'inherit' });
if (result.error) {
	throw result.error;
}
process.exitCode = result.status ?? 1;
