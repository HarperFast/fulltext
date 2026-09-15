import assert from 'node:assert';
import { cp, mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';

import {
	encodeMutationBatch,
	inspectNativeFullTextIndex,
	openNativeFullTextIndex,
	runtimeInfo,
} from '../dist/native.js';

const smoke = process.argv.includes('--smoke');
const sizes = integerListArgument('--indexes', smoke ? [1, 10] : [1, 10, 100, 1_000]);
if (2 * sizes.reduce((total, value) => total + value, 0) > 10_000)
	throw new Error('--indexes must create no more than 10,000 total benchmark directories');
const commits = integerArgument('--commits', smoke ? 2 : 64);
const warmRounds = integerArgument('--warm-rounds', smoke ? 3 : 10);
const root = await mkdtemp(path.join(tmpdir(), 'harper-fulltext-inspect-'));
const seedPath = path.join(root, 'seed');
const base = {
	indexId: 'inspection-benchmark',
	generation: 'benchmark-v1',
	fields: [{ name: 'title' }, { name: 'description' }],
	analyzer: 'english@1',
	limits: {
		indexingThreads: 1,
		searchThreads: 1,
		writerMemoryBytes: 15_000_000,
		maxQueuedCommands: 8,
		maxQueuedBytes: 8 * 1024 * 1024,
		maxBatchBytes: 8 * 1024 * 1024,
	},
};

try {
	const seedConfig = { ...base, path: seedPath };
	const seed = await openNativeFullTextIndex(seedConfig);
	for (let commit = 0; commit < commits; commit++) {
		await seed.apply(
			encodeMutationBatch({
				upserts: [
					{
						id: `product-${commit}`,
						fields: {
							title: `inspection benchmark product ${commit}`,
							description: 'catalog text for native metadata inspection',
						},
					},
				],
			}),
		);
		await seed.publish(`cursor-${commit}`);
	}
	await seed.close();

	const meta = JSON.parse(await readFile(path.join(seedPath, 'meta.json'), 'utf8'));
	const results = [];
	for (const count of sizes) {
		const inspectionPaths = [];
		const reopenPaths = [];
		for (let index = 0; index < count; index++) {
			const inspectionPath = path.join(root, `${count}-inspection-${index}`);
			const reopenPath = path.join(root, `${count}-reopen-${index}`);
			await cp(seedPath, inspectionPath, { recursive: true, preserveTimestamps: true });
			await cp(seedPath, reopenPath, { recursive: true, preserveTimestamps: true });
			inspectionPaths.push(inspectionPath);
			reopenPaths.push(reopenPath);
		}
		const inspection = {
			firstPass: measureInspections(inspectionPaths, base, 1),
			warm: measureInspections(inspectionPaths, base, warmRounds),
		};
		const reopen = {
			firstPass: await measureReopens(reopenPaths, base, 1),
			warm: await measureReopens(reopenPaths, base, warmRounds),
		};
		results.push({ count, inspection, reopen });
		for (const indexPath of [...inspectionPaths, ...reopenPaths]) await rm(indexPath, { recursive: true, force: true });
	}

	console.log(
		JSON.stringify(
			{
				formatVersion: 1,
				benchmark: 'native-index-inspection',
				runtime: await runtimeInfo(),
				host: { platform: process.platform, arch: process.arch, node: process.version },
				workload: {
					indexes: sizes,
					commits,
					segments: meta.segments.length,
					metaBytes: Buffer.byteLength(JSON.stringify(meta)),
					warmRounds,
				},
				results,
			},
			null,
			2,
		),
	);
} finally {
	await rm(root, { recursive: true, force: true });
}

function measureInspections(paths, options, rounds) {
	const latencies = [];
	const started = performance.now();
	for (let round = 0; round < rounds; round++) {
		for (const indexPath of paths) {
			const inspectionStarted = performance.now();
			const result = inspectNativeFullTextIndex({ ...options, path: indexPath });
			latencies.push(performance.now() - inspectionStarted);
			assert.deepStrictEqual(result, { state: 'checkpointed', committedPayload: `cursor-${commits - 1}` });
		}
	}
	const totalMilliseconds = performance.now() - started;
	return distribution(latencies, totalMilliseconds);
}

async function measureReopens(paths, options, rounds) {
	const latencies = [];
	const started = performance.now();
	for (let round = 0; round < rounds; round++) {
		for (const indexPath of paths) {
			const reopenStarted = performance.now();
			const index = await openNativeFullTextIndex({ ...options, path: indexPath });
			const committedPayload = index.committedPayload;
			await index.close();
			latencies.push(performance.now() - reopenStarted);
			assert.strictEqual(committedPayload, `cursor-${commits - 1}`);
		}
	}
	return distribution(latencies, performance.now() - started);
}

function distribution(latencies, totalMilliseconds) {
	latencies.sort((left, right) => left - right);
	return {
		operations: latencies.length,
		totalMilliseconds,
		operationsPerSecond: (latencies.length * 1_000) / totalMilliseconds,
		p50Milliseconds: percentile(latencies, 0.5),
		p95Milliseconds: percentile(latencies, 0.95),
		p99Milliseconds: percentile(latencies, 0.99),
		maxMilliseconds: latencies.at(-1),
	};
}

function percentile(values, fraction) {
	return values[Math.min(values.length - 1, Math.ceil(values.length * fraction) - 1)];
}

function integerArgument(name, fallback) {
	const index = process.argv.indexOf(name);
	if (index === -1) return fallback;
	const value = Number(process.argv[index + 1]);
	if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
	return value;
}

function integerListArgument(name, fallback) {
	const index = process.argv.indexOf(name);
	if (index === -1) return fallback;
	const argument = process.argv[index + 1];
	if (argument === undefined) throw new Error(`${name} requires a comma-separated value`);
	const values = argument.split(',').map(Number);
	if (values.some((value) => !Number.isSafeInteger(value) || value <= 0))
		throw new Error(`${name} must be a comma-separated list of positive integers`);
	return values;
}
