import assert from 'node:assert';
import { spawnSync } from 'node:child_process';
import { readdirSync } from 'node:fs';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const examplesDirectory = new URL('../examples/', import.meta.url);
for (const example of readdirSync(examplesDirectory)
	.filter((entry) => entry.endsWith('.mjs'))
	.sort()) {
	test(`runs examples/${example}`, () => {
		const result = spawnSync(process.execPath, [fileURLToPath(new URL(example, examplesDirectory))], {
			env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' },
			encoding: 'utf8',
			timeout: 30_000,
		});
		assert.ifError(result.error);
		assert.strictEqual(result.status, 0, result.stderr || result.stdout);
		assert(result.stdout.trim().length > 0);
	});
}
