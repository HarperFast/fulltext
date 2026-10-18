import assert from 'node:assert';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import {
	configureNativeFullTextRuntime,
	openNativeFullTextIndex,
	openNativeFullTextReader,
	runtimeInfo,
} from '@harperfast/fulltext/native';

const smoke = process.argv.includes('--smoke');
const count = smoke ? 2048 : 16384;
const commits = smoke ? 4 : 32;
const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-traversal-qualification-'));
const config = {
	path: directory,
	indexId: 'traversal-qualification',
	generation: 'one',
	analyzer: 'english@2',
	surfaceTerms: true,
	fields: [{ name: 'title', weight: 1 }],
	limits: {
		indexingThreads: 1,
		searchThreads: 3,
		writerMemoryBytes: 15000000,
		maxQueuedCommands: 32,
		maxQueuedBytes: 8388608,
		maxBatchBytes: 8388608,
	},
};
configureNativeFullTextRuntime({
	maxResidentIndexes: 2,
	maxIndexingThreads: 1,
	maxSearchThreads: 6,
	maxWriterMemoryBytes: 15000000,
	maxQueuedBytes: 25165824,
	maxExpensiveSearches: 1,
});
const words = new Set(['widget']);
for (let position = 0; position < 3; position++)
	for (let letter = 97; letter <= 122; letter++) {
		words.add('widget'.slice(0, position) + String.fromCharCode(letter) + 'widget'.slice(position + 1));
	}
const vocabulary = [...words];
const id = (i) => String(i).padStart(6, '0') + 'x'.repeat(3900);
let writer, reader;
try {
	writer = await openNativeFullTextIndex(config);
	for (let start = 0; start < count; start += count / commits) {
		await writer.applyMutationBatch({
			upserts: Array.from({ length: count / commits }, (_, offset) => {
				const i = start + offset;
				return {
					id: id(i),
					version: 'v'.repeat(4096),
					fields: { title: `catalog ${vocabulary[i % vocabulary.length]}` },
				};
			}),
		});
		await writer.publish(String(start));
	}
	reader = await openNativeFullTextReader(config);
	const metadata = JSON.parse(await readFile(path.join(directory, 'meta.json'), 'utf8'));
	assert(
		metadata !== null && typeof metadata === 'object' && Array.isArray(metadata.segments),
		'Tantivy meta.json no longer exposes a segments array; update traversal qualification for the pinned layout',
	);
	if (!smoke) assert(metadata.segments.length > 1, 'full qualification requires multiple actual segments');
	console.log(
		JSON.stringify({
			runtime: await runtimeInfo(),
			count,
			commits,
			segments: metadata.segments.length,
			vocabulary: vocabulary.length,
		}),
	);
	for (const mode of ['any', 'prefix', 'fuzzy-prefix']) {
		const text = mode === 'any' ? 'catalog' : 'widg';
		const expected = mode === 'prefix' ? Math.ceil(count / vocabulary.length) : count;
		let previous,
			continuation,
			seen = new Set(),
			calls = 0,
			milliseconds = 0,
			ordinaryCompleted = 0;
		const started = performance.now();
		const updates = (async () => {
			for (let i = 0; i < 4; i++) {
				await writer.applyMutationBatch({
					upserts: [{ id: `new-${mode}-${i}`, fields: { title: 'unrelated addition' } }],
				});
				await writer.publish(`${mode}-${i}`);
			}
		})().then(
			() => undefined,
			(error) => error,
		);
		do {
			assert(++calls <= Math.ceil(expected / 128));
			const before = performance.now();
			const [page, ordinary] = await Promise.all([
				reader.searchPage({ text, mode, limit: 128, continuation }, { remainingBudgetMilliseconds: 5000 }),
				reader.search({ text: 'catalog', limit: 1 }),
			]);
			milliseconds += performance.now() - before;
			assert.equal(ordinary.hits.length, 1);
			ordinaryCompleted++;
			assert.equal(page.total, expected);
			assert.equal(page.totalRelation, 'exact');
			for (const hit of page.hits) {
				const number = Number(hit.id.slice(0, 6));
				assert(number < count && hit.id === id(number));
				if (mode === 'prefix') assert.equal(number % vocabulary.length, 0);
				if (previous)
					assert(
						previous.score > hit.score ||
							(previous.score === hit.score && Buffer.compare(Buffer.from(previous.id), Buffer.from(hit.id)) < 0),
					);
				previous = hit;
				assert(!seen.has(hit.id));
				seen.add(hit.id);
				assert.equal(hit.version, 'v'.repeat(4096));
			}
			continuation = page.continuation;
		} while (continuation);
		const updateError = await updates;
		if (updateError) throw updateError;
		assert.equal(seen.size, expected);
		console.log(
			JSON.stringify({
				mode,
				hits: seen.size,
				calls,
				ordinaryCompleted,
				milliseconds,
				elapsedMilliseconds: performance.now() - started,
				rss: process.memoryUsage().rss,
			}),
		);
	}
	await assert.rejects(
		reader.searchPage({ text: 'catalog', limit: 2000 }),
		(error) => error.code === 'E_RESULT_TOO_LARGE',
	);
	assert.equal((await reader.searchPage({ text: 'catalog', limit: 128 })).hits.length, 128);
} finally {
	if (reader) await reader.close();
	if (writer) await writer.close({ mode: 'rollback' });
	await rm(directory, { recursive: true, force: true });
}
