import assert from 'node:assert';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const catalog = JSON.parse(await readFile(new URL('../examples/data/catalog.json', import.meta.url), 'utf8'));
const fixture = JSON.parse(await readFile(new URL('./fixtures/relevance.json', import.meta.url), 'utf8'));
const profiles = fixture.catalog.profiles.map(({ id, options, expectedIds }) => ({
	id,
	options,
	documents: catalog.documents,
	cases: fixture.catalog.cases.map((query) => ({ ...query, expectedIds: expectedIds[query.id] })),
}));
profiles.push({
	id: 'tantivy-027-stemming',
	options: { fields: [{ name: 'title' }] },
	documents: fixture.stemming.documents,
	cases: fixture.stemming.cases,
});

for (const profile of profiles) {
	for (const reverse of [false, true]) {
		test(`${profile.id}: ${reverse ? 'reverse' : 'forward'} insertion`, async (context) => {
			const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-relevance-'));
			let index;
			try {
				index = await openNativeFullTextIndex({
					path: directory,
					indexId: profile.id,
					generation: 'v1',
					analyzer: 'english@2',
					positions: true,
					surfaceTerms: true,
					...profile.options,
					limits: {
						indexingThreads: 1,
						searchThreads: 2,
						writerMemoryBytes: 15_000_000,
						maxQueuedCommands: 32,
						maxQueuedBytes: 4 * 1024 * 1024,
						maxBatchBytes: 1024 * 1024,
					},
				});
				await index.applyMutationBatch({
					upserts: reverse ? [...profile.documents].reverse() : profile.documents,
				});
				await index.commit();
				await index.reload();
				for (const query of profile.cases) {
					await context.test(query.id, async () => {
						const result = await index.search({ ...query.request, exactTotal: true, limit: 100 });
						assert.deepStrictEqual(
							result.hits.map(({ id }) => id),
							query.expectedIds,
						);
						assert.strictEqual(result.total, query.expectedIds.length);
						assert.strictEqual(result.totalRelation, 'exact');
						for (let i = 0; i < result.hits.length; i++) {
							const hit = result.hits[i];
							assert.strictEqual(hit.version, '1');
							assert(Number.isFinite(hit.score));
							if (i > 0) {
								const previous = result.hits[i - 1];
								assert(previous.score >= hit.score);
								if (previous.score === hit.score)
									assert(Buffer.compare(Buffer.from(previous.id), Buffer.from(hit.id)) < 0);
							}
						}
						if (['catalog-any', 'prefix', 'equal-ties', 'added-fuzzy'].includes(query.id)) {
							const hits = [];
							let continuation;
							let pages = 0;
							do {
								const page = await index.searchPage({ ...query.request, limit: 2, continuation });
								assert.strictEqual(page.total, result.total);
								assert.strictEqual(page.totalRelation, 'exact');
								hits.push(...page.hits);
								continuation = page.continuation;
								assert(++pages <= profile.documents.length + 1);
							} while (continuation);
							assert.deepStrictEqual(hits, result.hits);
						}
					});
				}
			} finally {
				await index?.close({ mode: 'rollback' });
				await rm(directory, { recursive: true, force: true });
			}
		});
	}
}
