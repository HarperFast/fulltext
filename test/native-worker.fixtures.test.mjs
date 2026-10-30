import assert from 'node:assert';
import { execFile } from 'node:child_process';
import { existsSync } from 'node:fs';
import { rm } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import test from 'node:test';

const execute = promisify(execFile);
async function observe(context, args, options) {
	try {
		return await execute(process.execPath, args, { ...options, signal: context.signal });
	} catch (error) {
		if (error.stderr) context.diagnostic(error.stderr);
		throw error;
	}
}
const scenario = fileURLToPath(new URL('./fixtures/native-worker-scenario.mjs', import.meta.url));

test('worker fixture preserves a real reopen error and its index evidence', { timeout: 65_000 }, async () => {
	const result = await execute(process.execPath, [scenario, 'apply'], {
		timeout: 60_000,
		env: {
			...process.env,
			FULLTEXT_PREFER_LOCAL_BUILD: '1',
			FULLTEXT_TEST_WORKER_REOPEN_ID: 'different-index-identity',
		},
	}).then(
		() => assert.fail('the real native reopen must reject the incompatible identity'),
		(error) => error,
	);
	assert.strictEqual(result.code, 1);
	assert.match(result.stderr, /E_IDENTITY_MISMATCH/);
	const directory = /^apply:path:(.+)$/m.exec(result.stderr)?.[1]?.trim();
	assert(directory, result.stderr);
	try {
		assert(existsSync(directory), 'a failed reopen must retain the index rather than delete its evidence');
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

const diagnosticChild = fileURLToPath(new URL('./fixtures/native-writer-diagnostic-child.mjs', import.meta.url));
const nativeEvents = (stderr) =>
	stderr
		.split('\n')
		.filter((line) => line.startsWith('fulltext-writer-diagnostic '))
		.map((line) => JSON.parse(line.slice('fulltext-writer-diagnostic '.length)));

test('test-addon diagnostics correlate writer work without changing results', { timeout: 35_000 }, async (context) => {
	const env = { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1', FULLTEXT_TEST_WRITER_DIAGNOSTICS: '1' };
	const result = await observe(context, [diagnosticChild], { timeout: 30_000, env });
	assert.deepStrictEqual(JSON.parse(result.stdout), { complete: true });
	assert(result.stderr.length < 64 * 1024);
	const records = nativeEvents(result.stderr);
	assert(records.length > 0);
	assert.strictEqual(new Set(records.map(({ handle }) => handle)).size, 1);
	const commit = records.filter(({ operation }) => operation === 'commit');
	assert.deepStrictEqual(
		commit.map(({ stage }) => stage),
		['writer-start', 'writer-end'],
	);
	assert(commit[1].durationNanoseconds > 0);
	assert(commit.every(({ timestamp, pid }) => Number.isSafeInteger(timestamp) && pid > 0));
	const resolved = result.stderr
		.split('\n')
		.filter((line) => line.startsWith('fulltext-process-diagnostic '))
		.map((line) => JSON.parse(line.slice('fulltext-process-diagnostic '.length)))
		.find(({ stage }) => stage === 'js-commit-resolved');
	assert.strictEqual(resolved.native.uncommittedMutations, '0');
	assert.strictEqual(resolved.native.documentSource, undefined);
	assert(!result.stderr.includes('must-not-be-recorded'));
	assert(BigInt(resolved.native.commitOpstamp) > 0n);
});

test('test-addon diagnostics preserve worker teardown evidence', { timeout: 65_000 }, async (context) => {
	const env = { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1', FULLTEXT_TEST_WRITER_DIAGNOSTICS: '1' };
	const worker = await observe(context, [scenario, 'apply'], { timeout: 60_000, env });
	const cleanup = nativeEvents(worker.stderr);
	const wait = cleanup.find(({ stage }) => stage === 'cleanup-wait-start');
	assert(wait, worker.stderr);
	for (const stage of ['close-start', 'close-end', 'runtime-released', 'cleanup-wait-end'])
		assert(cleanup.some((record) => record.stage === stage && record.handle === wait.handle));
	assert(!worker.stderr.includes('retained-index'));
	const directory = /^apply:path:(.+)$/m.exec(worker.stderr)?.[1]?.trim();
	assert(directory);
	assert.strictEqual(existsSync(directory), false, 'successful close must remove the fixture');
});

for (const mode of ['closed-stderr', 'status-error', 'detached-status', 'disabled']) {
	test(`writer diagnostics contain ${mode}`, { timeout: 20_000 }, async (context) => {
		const env = {
			...process.env,
			FULLTEXT_PREFER_LOCAL_BUILD: '1',
			FULLTEXT_TEST_WRITER_DIAGNOSTICS: mode === 'disabled' ? '0' : '1',
		};
		const result = await observe(context, [diagnosticChild, mode], { timeout: 15_000, env });
		assert.deepStrictEqual(JSON.parse(result.stdout), { complete: true });
		if (mode === 'status-error') assert.match(result.stderr, /E_CLOSED/);
		if (mode === 'disabled') assert.deepStrictEqual(nativeEvents(result.stderr), []);
		if (mode === 'detached-status') {
			assert.match(result.stderr, /fulltext-process-diagnostic /);
			assert(!result.stderr.includes('must-not-be-polled'));
		}
	});
}
