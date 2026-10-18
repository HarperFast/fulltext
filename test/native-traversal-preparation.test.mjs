import assert from 'node:assert';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

for (const scenario of ['commands', 'bytes', 'concurrent', 'timeout', 'close']) {
	test(`traversal preparation handles ${scenario} and releases capacity`, () => {
		const output = execFileSync(
			process.execPath,
			[fileURLToPath(new URL('./fixtures/traversal-preparation-child.mjs', import.meta.url)), scenario],
			{
				env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1', UV_THREADPOOL_SIZE: '1' },
				encoding: 'utf8',
				timeout: 30000,
			},
		);
		assert.deepStrictEqual(JSON.parse(output), { scenario, recovered: true });
	});
}
