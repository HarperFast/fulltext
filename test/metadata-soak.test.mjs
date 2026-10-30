import assert from 'node:assert';
import { execFile } from 'node:child_process';
import { access, mkdir, mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';

const execute = promisify(execFile);
const driver = fileURLToPath(new URL('../benchmarks/metadata-soak.mjs', import.meta.url));
const cases = ['ordinary-exact', 'price-sort', 'nested-histogram', 'runtime-five-sort'];
const readJson = async (file) => JSON.parse(await readFile(file, 'utf8'));
async function fixture(context) {
	const artifactRoot = process.env.FULLTEXT_TEST_ARTIFACT_ROOT;
	if (artifactRoot) await mkdir(artifactRoot, { recursive: true });
	const root = await mkdtemp(path.join(artifactRoot ?? tmpdir(), 'fulltext-soak-test-'));
	if (!artifactRoot) context.after(() => rm(root, { recursive: true, force: true }));
	const artifacts = path.join(root, 'artifacts');
	const args = [
		driver,
		'--artifacts',
		artifacts,
		'--documents',
		'400',
		'--mixed-seconds',
		'1',
		'--drain-seconds',
		'1',
		'--write-interval-ms',
		'300',
		'--sample-ms',
		'100',
		'--max-p95-ms',
		'30000',
		'--max-p99-ms',
		'30000',
		'--budget-ms',
		'30000',
		'--max-rss-growth-mib',
		'4096',
		'--max-tail-rss-range-mib',
		'4096',
	];
	return { artifacts, args };
}
async function verifyRetainedSamples(artifacts) {
	const manifest = await readJson(path.join(artifacts, 'manifest.json'));
	assert.strictEqual(manifest.workload.documents, 400);
	assert.strictEqual(manifest.workload.mixedMilliseconds, 1000);
	assert.strictEqual(manifest.workload.drainMilliseconds, 1000);
	const samples = (await readFile(path.join(artifacts, 'samples.ndjson'), 'utf8')).trim().split('\n').map(JSON.parse);
	assert(samples.length > 1);
	assert(samples.some(({ phase }) => phase === 'mixed'));
	assert(samples.some(({ phase }) => phase === 'drain'));
	for (const sample of samples) {
		assert(sample.phaseElapsedMilliseconds >= 0, 'sample phase must match its measurement time');
		assert(sample.memory.rss > 0);
		assert(sample.memory.heapUsed > 0);
		assert(Number.isInteger(sample.segments) && sample.segments > 0);
		assert(BigInt(sample.searchQueuedCommands) >= 0n);
		assert(BigInt(sample.searchQueuedBytes) >= 0n);
		assert(BigInt(sample.writerQueuedCommands) >= 0n);
		assert(BigInt(sample.writerQueuedBytes) >= 0n);
	}
}
function verifyQueries(report) {
	for (const phase of ['mixed', 'drain']) {
		const queries = report.queries.filter((entry) => entry.phase === phase);
		assert.deepStrictEqual(queries.map(({ name }) => name).sort(), [...cases].sort());
		for (const query of queries) assert(query.completed > 0, `${phase}/${query.name} must complete and verify queries`);
	}
}

test(
	'short mixed metadata soak verifies published snapshots and retains telemetry after successful cleanup',
	{ timeout: 45_000 },
	async (context) => {
		const { artifacts, args } = await fixture(context);
		const { stdout } = await execute(process.execPath, args, {
			timeout: 40_000,
			maxBuffer: 4 << 20,
			env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' },
		});
		const report = JSON.parse(stdout);
		assert.strictEqual(report.accepted, true);
		assert(report.publications >= 2);
		assert(report.mutations > 0);
		assert.strictEqual(report.indexCleanup, 'closed, reopened and verified, then removed');
		assert.deepStrictEqual(await readJson(path.join(artifacts, 'report.json')), report);
		verifyQueries(report);
		await verifyRetainedSamples(artifacts);
		await assert.rejects(access(path.join(artifacts, 'index')), { code: 'ENOENT' });
	},
);

test(
	'failed mixed metadata soak keeps diagnostics and the native index without reporting acceptance',
	{ timeout: 45_000 },
	async (context) => {
		const { artifacts, args } = await fixture(context);
		await assert.rejects(
			execute(process.execPath, [...args, '--min-throughput', '1000000000000'], {
				timeout: 40_000,
				maxBuffer: 4 << 20,
				env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' },
			}),
			(error) => {
				assert.strictEqual(error.stdout, '');
				assert.match(error.stderr, /throughput gate failed/);
				return true;
			},
		);
		const failure = await readJson(path.join(artifacts, 'failure.json'));
		assert.strictEqual(failure.accepted, false);
		assert.deepStrictEqual(failure.closeErrors, []);
		assert.strictEqual(failure.indexRetained, path.join(artifacts, 'index'));
		assert(failure.publications >= 2);
		assert(failure.mutations > 0);
		verifyQueries(failure);
		await verifyRetainedSamples(artifacts);
		await access(path.join(artifacts, 'index', 'meta.json'));
		await assert.rejects(access(path.join(artifacts, 'report.json')), { code: 'ENOENT' });
	},
);

test('report artifact write failure cannot emit an accepted soak result', { timeout: 45_000 }, async (context) => {
	const { artifacts, args } = await fixture(context);
	const child = execute(process.execPath, args, {
		timeout: 40_000,
		maxBuffer: 4 << 20,
		env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1' },
	}).then(
		(result) => ({ result }),
		(error) => ({ error }),
	);
	const until = Date.now() + 5000;
	while (true) {
		try {
			await access(path.join(artifacts, 'manifest.json'));
			break;
		} catch (error) {
			if (error.code !== 'ENOENT' || Date.now() >= until) throw error;
			await delay(5);
		}
	}
	await mkdir(path.join(artifacts, 'report.json'));
	const completed = await child;
	assert(completed.error, 'report write failure must reject the child process');
	assert.strictEqual(completed.error.stdout, '', 'artifact failure must not emit accepted output');
	const failure = await readJson(path.join(artifacts, 'failure.json'));
	assert.strictEqual(failure.accepted, false);
	assert.strictEqual(failure.indexRetained, null, 'the successfully removed index must not be reported retained');
	assert.deepStrictEqual(failure.closeErrors, []);
	assert.match(failure.error.message, /EISDIR|EEXIST/);
	await verifyRetainedSamples(artifacts);
	await assert.rejects(access(path.join(artifacts, 'index')), { code: 'ENOENT' });
});

for (const kind of ['writer', 'reader'])
	test(`soak rejects resolved ${kind} cleanup errors and retains evidence`, { timeout: 45_000 }, async (context) => {
		const { artifacts, args } = await fixture(context);
		await assert.rejects(
			execute(
				process.execPath,
				[...args, '--module', fileURLToPath(new URL('./fixtures/metadata-close-failure.mjs', import.meta.url))],
				{
					timeout: 40_000,
					maxBuffer: 4 << 20,
					env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1', FULLTEXT_TEST_CLOSE_KIND: kind },
				},
			),
			(error) => {
				assert.strictEqual(error.stdout, '');
				assert.match(error.stderr, /E_CLOSE_FAILED/);
				return true;
			},
		);
		const failure = await readJson(path.join(artifacts, 'failure.json'));
		assert.strictEqual(failure.accepted, false);
		assert.strictEqual(failure.error.code, 'E_CLOSE_FAILED');
		assert.strictEqual(failure.indexRetained, path.join(artifacts, 'index'));
		assert(Array.isArray(failure.closeErrors));
		await assert.rejects(access(path.join(artifacts, 'report.json')), { code: 'ENOENT' });
	});

test('soak records publication failure before any completed publication', { timeout: 45_000 }, async (context) => {
	const { artifacts, args } = await fixture(context);
	await assert.rejects(
		execute(
			process.execPath,
			[...args, '--module', fileURLToPath(new URL('./fixtures/metadata-close-failure.mjs', import.meta.url))],
			{
				timeout: 40_000,
				maxBuffer: 4 << 20,
				env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1', FULLTEXT_TEST_CLOSE_KIND: 'publish' },
			},
		),
		(error) => /injected publication failure/.test(error.stderr),
	);
	const failure = await readJson(path.join(artifacts, 'failure.json'));
	assert.strictEqual(failure.accepted, false);
	assert.strictEqual(failure.error.code, 'E_STORAGE');
	assert.strictEqual(failure.operation, 'publish');
	assert.strictEqual(failure.publications, 0);
	assert.strictEqual(failure.mutations, 110);
	assert.deepStrictEqual(failure.closeErrors, []);
	await access(path.join(artifacts, 'index', 'meta.json'));
});

test('soak retains validation error when final reader close also fails', { timeout: 45_000 }, async (context) => {
	const { artifacts, args } = await fixture(context);
	await assert.rejects(
		execute(
			process.execPath,
			[...args, '--module', fileURLToPath(new URL('./fixtures/metadata-close-failure.mjs', import.meta.url))],
			{
				timeout: 40_000,
				maxBuffer: 4 << 20,
				env: { ...process.env, FULLTEXT_PREFER_LOCAL_BUILD: '1', FULLTEXT_TEST_CLOSE_KIND: 'final-reader' },
			},
		),
		(error) => /injected final reader validation failure/.test(error.stderr),
	);
	const failure = await readJson(path.join(artifacts, 'failure.json'));
	assert.strictEqual(failure.accepted, false);
	assert.strictEqual(failure.operation, 'final-reader-validation');
	assert.match(failure.error.message, /injected final reader validation failure/);
	assert.strictEqual(failure.indexRetained, path.join(artifacts, 'index'));
	await assert.rejects(access(path.join(artifacts, 'report.json')), { code: 'ENOENT' });
});
