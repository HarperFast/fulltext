import assert from 'node:assert';
import { execFile } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { platformTriple } from '../dist/load-addon.js';

const execute = promisify(execFile);
test(
	'production addon omits writer trace code even when the test environment flag is enabled',
	{ timeout: 35_000 },
	async () => {
		const result = await execute(
			process.execPath,
			[fileURLToPath(new URL('./fixtures/native-writer-diagnostic-child.mjs', import.meta.url))],
			{
				timeout: 30_000,
				env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1', FULLTEXT_TEST_WRITER_DIAGNOSTICS: '1' },
			},
		);
		assert.deepStrictEqual(JSON.parse(result.stdout), { complete: true });
		assert(!result.stderr.includes('fulltext-writer-diagnostic '));
		const binary = readFileSync(new URL(`../fulltext.${platformTriple()}.node`, import.meta.url));
		assert.strictEqual(binary.includes(Buffer.from('fulltext-writer-diagnostic ')), false);
	},
);
