import assert from 'node:assert';
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import { openNativeFullTextIndex, runtimeInfo } from '../dist/native.js';
import { loadAddon } from '../dist/load-addon.js';

const args = process.argv.slice(2);
assert(args.length <= 1 && args.every((arg) => ['--smoke', '--stress'].includes(arg)));
assert.strictEqual(process.env.FULLTEXT_PREFER_LOCAL_BUILD, '1', 'run through scripts/run-local.mjs');
assert.strictEqual(loadAddon().__testDelayNextExpensiveSearch, undefined, 'use a production native build');
const documents = args.includes('--smoke') ? 1024 : args.includes('--stress') ? 131_072 : 16_384;
const rounds = args.includes('--smoke') ? 3 : 20;
const root = await mkdtemp(path.join(tmpdir(), 'fulltext-explanations-'));
const leaf = (text, extra = {}) => ({ text, fields: ['title'], ...extra });
const fields = ['title', 'empty1', 'empty2', 'empty3'];
const queries = [
	['term', leaf('amber')],
	['fuzzy', leaf('ambr', { mode: 'fuzzy' })],
	['prefix', leaf('stone', { mode: 'prefix' })],
	['optional', { query: { operator: 'optional', required: leaf('amber'), optional: [leaf('quartz')] } }],
	['or', { query: { operator: 'or', clauses: [leaf('amber'), leaf('absent')] } }],
	[
		'fuzzy-construction-cap',
		{
			query: {
				operator: 'or',
				clauses: Array.from({ length: 16 }, (_, i) => ({
					text: 'ambr' + String.fromCharCode(97 + i),
					mode: 'fuzzy',
					fuzzyDistance: 2,
					fields,
				})),
			},
		},
	],
];
const fingerprint = async (url) => {
	const hash = createHash('sha256');
	for await (const chunk of createReadStream(url)) hash.update(chunk);
	return { file: path.basename(fileURLToPath(url)), sha256: hash.digest('hex') };
};
const telemetry = {
	environment: {
		observedAt: new Date().toISOString(),
		platform: process.platform,
		architecture: process.arch,
		node: process.version,
		runtime: await runtimeInfo(),
	},
	documents,
	populatedFields: ['title'],
	emptyFields: fields.slice(1),
	fingerprints: await Promise.all([
		fingerprint(new URL(import.meta.url)),
		fingerprint(new URL('../dist/native.js', import.meta.url)),
		fingerprint(
			new URL(
				`../fulltext.${process.platform}-${process.arch}${process.platform === 'linux' ? '-gnu' : process.platform === 'win32' ? '-msvc' : ''}.node`,
				import.meta.url,
			),
		),
	]),
	rows: [],
};
let failure;
try {
	for (const commits of [1, 16]) {
		const indexPath = path.join(root, String(commits));
		const index = await openNativeFullTextIndex({
			path: indexPath,
			indexId: 'explanation-qualification',
			generation: 'one',
			analyzer: 'english@2',
			surfaceTerms: true,
			fields: fields.map((name) => ({ name })),
			limits: {
				indexingThreads: 1,
				searchThreads: 2,
				writerMemoryBytes: 15_000_000,
				maxQueuedCommands: 32,
				maxQueuedBytes: 4 << 20,
				maxBatchBytes: 1 << 20,
			},
		});
		let indexFailure;
		try {
			const batchSize = Math.min(256, documents / 16);
			for (let start = 0; start < documents; start += batchSize) {
				await index.applyMutationBatch({
					upserts: Array.from({ length: batchSize }, (_, j) => ({
						id: `doc-${String(start + j).padStart(6, '0')}`,
						fields: {
							title: `amber stone ${j % 2 ? 'quartz' : 'ruby'} ${Array.from({ length: 16 }, (_, k) => `amber${String((start + j) * 16 + k).padStart(7, '0')}`).join(' ')}`,
						},
					})),
				});
				if ((start + batchSize) % (documents / commits) === 0) await index.commit();
			}
			await index.reload();
			const segments = Number(index.telemetry().native.reader.segments);
			if (!args.includes('--smoke') && commits === 16) assert(segments > 1);
			for (const [name, request] of queries) {
				const distributedIds = Array.from(
					{ length: 10 },
					(_, i) => `doc-${String(Math.floor((i * (documents - 1)) / 9)).padStart(6, '0')}`,
				);
				const hits = (await index.search({ ...request, candidateIds: distributedIds, limit: 10 })).hits;
				assert.strictEqual(hits.length, 10);
				const byId = new Map(hits.map((hit) => [hit.id, hit]));
				for (const count of [1, 10]) {
					const ids = distributedIds.slice(0, count).reverse();
					const expected = ids.map((id) => byId.get(id));
					const samples = [];
					const cpuStart = process.cpuUsage();
					const rssStart = process.memoryUsage().rss;
					let peakRss = rssStart;
					const sampleRss = () => {
						peakRss = Math.max(peakRss, process.memoryUsage().rss);
					};
					const timer = setInterval(sampleRss, 5);
					try {
						for (let i = 0; i < rounds + 2; i++) {
							const start = performance.now();
							const result = await index.explain(request, ids);
							const milliseconds = performance.now() - start;
							sampleRss();
							assert.deepStrictEqual(
								result.records.map(({ id, score }) => ({ id, score })),
								expected,
							);
							if (i >= 2) samples.push(milliseconds);
						}
					} finally {
						clearInterval(timer);
					}
					const cpu = process.cpuUsage(cpuStart);
					samples.sort((a, b) => a - b);
					const row = {
						commits,
						segments,
						name,
						ids: count,
						rounds,
						p50Ms: samples[Math.ceil(rounds * 0.5) - 1],
						p95Ms: samples[Math.ceil(rounds * 0.95) - 1],
						maxMs: samples.at(-1),
						cpuWithWarmupsAndChecksMs: (cpu.user + cpu.system) / 1000,
						rssStart,
						peakRss,
					};
					telemetry.rows.push(row);
					console.log(JSON.stringify(row));
				}
			}
		} catch (error) {
			indexFailure = error;
			throw error;
		} finally {
			try {
				await index.close({ mode: 'rollback' });
			} catch (error) {
				if (!indexFailure) throw error;
			}
		}
	}
	console.log(JSON.stringify(telemetry));
} catch (error) {
	failure = error;
	throw error;
} finally {
	if (!failure) await rm(root, { recursive: true, force: true });
}
