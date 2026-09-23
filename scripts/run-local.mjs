import { spawnSync } from 'node:child_process';

const [script, ...arguments_] = process.argv.slice(2);
if (!script) {
	throw new Error('A script path is required');
}
const result = spawnSync(process.execPath, [script, ...arguments_], {
	env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' },
	stdio: 'inherit',
});
if (result.error) {
	throw result.error;
}
process.exitCode = result.status ?? 1;
