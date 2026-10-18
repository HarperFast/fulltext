import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { openNativeFullTextIndex, openNativeFullTextReader } from '@harperfast/fulltext/native';

async function fixture(context, overrides = {}) {
	const directory = mkdtempSync(path.join(tmpdir(), 'fulltext-autocomplete-traversal-'));
	let writer;
	let reader;
	context.after(async () => {
		if (reader) await reader.close();
		if (writer) await writer.close({ mode: 'rollback' });
		rmSync(directory, { recursive: true, force: true });
	});
	const config = {
		path: directory,
		indexId: 'autocomplete-traversal',
		generation: 'one',
		fields: [
			{ name: 'title', weight: 2 },
			{ name: 'description', weight: 1 },
		],
		filterFields: [{ name: 'category', type: 'string' }],
		analyzer: 'english@2',
		surfaceTerms: true,
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15000000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8388608,
			maxBatchBytes: 8388608,
		},
		...overrides,
	};
	writer = await openNativeFullTextIndex(config);
	return { config, writer, openReader: async () => (reader = await openNativeFullTextReader(config)) };
}

async function pages(reader, query, expectedTotal, first) {
	const hits = [];
	let page = first ?? (await reader.searchPage(query));
	let count = 0;
	const maxPages = Math.max(1, Math.ceil(expectedTotal / (query.limit ?? 20)));
	do {
		assert(++count <= maxPages, 'autocomplete continuation did not advance');
		assert.strictEqual(page.total, expectedTotal);
		assert.strictEqual(page.totalRelation, 'exact');
		hits.push(...page.hits);
		if (!page.continuation) break;
		page = await reader.searchPage({ ...query, continuation: page.continuation });
	} while (true);
	return hits;
}

const codeIs = (code) => (error) => error.code === code;

test('traverses prefix matches beyond the first 100 raw hits without losing later eligible results', async (context) => {
	const { writer, openReader } = await fixture(context);
	const ids = Array.from({ length: 250 }, (_, i) => String(i).padStart(3, '0'));
	await writer.applyMutationBatch({
		upserts: ids.map((id) => ({ id, version: 'original', fields: { title: 'widget' } })),
	});
	await writer.publish('original');
	const reader = await openReader();
	const query = { text: 'wid', mode: 'prefix', fields: ['title'] };
	const legacy = await reader.search({ ...query, limit: 100, exactTotal: true });
	assert.strictEqual(legacy.total, 250);
	assert.strictEqual(legacy.hits.length, 100);
	const whole = await reader.searchPage({ ...query, limit: 1000 });
	const hits = await pages(reader, { ...query, limit: 37 }, 250);
	assert.deepStrictEqual(hits, whole.hits);
	assert.deepStrictEqual(
		hits.map((hit) => hit.id),
		ids,
	);
	assert.deepStrictEqual(hits.slice(0, 100), legacy.hits);
	assert.strictEqual(legacy.hits.filter((hit) => Number(hit.id) >= 100).length, 0);
	assert.deepStrictEqual(
		hits
			.filter((hit) => Number(hit.id) >= 100)
			.slice(0, 10)
			.map((hit) => hit.id),
		ids.slice(100, 110),
	);
	await assert.rejects(
		reader.search({ ...query, offset: 100, limit: 10 }),
		(error) => error.code === 'E_INVALID_ARGUMENT' && /searchPage/.test(error.message),
	);
	await assert.rejects(reader.search({ ...query, limit: 101 }), codeIs('E_INVALID_ARGUMENT'));
	await assert.rejects(reader.searchPage({ ...query, limit: 10001 }), codeIs('E_INVALID_ARGUMENT'));
	const ordinary = await reader.searchPage({ text: 'widget', fields: ['title'], limit: 1 });
	await assert.rejects(
		reader.searchPage({ text: 'widget', mode: 'prefix', fields: ['title'], continuation: ordinary.continuation }),
		codeIs('E_INVALID_ARGUMENT'),
	);
});

test('preserves scored autocomplete sequences across modes, Boolean trees and native filters', async (context) => {
	const { writer, openReader } = await fixture(context);
	const ids = Array.from({ length: 300 }, (_, i) => `${['z', 'é', '中', '😀'][i % 4]}-${String(i).padStart(3, '0')}`);
	for (let start = 0; start < ids.length; start += 75) {
		await writer.applyMutationBatch({
			upserts: ids.slice(start, start + 75).map((id, offset) => {
				const i = start + offset;
				return {
					id,
					version: 'original',
					fields: {
						title: i % 4 === 0 ? 'wodget catalog' : `${'widget '.repeat((i % 4) + 1)}catalog`,
						description: i % 5 === 0 ? 'widget' : 'catalog',
					},
					filters: { category: i % 2 ? 'odd' : 'even' },
				};
			}),
		});
		await writer.publish(String(start));
	}
	const reader = await openReader();
	for (const query of [
		{ text: 'widg', mode: 'prefix' },
		{ text: 'widg', mode: 'fuzzy-prefix' },
		{ text: 'catalog widg', mode: 'prefix' },
		{ text: 'widget ', mode: 'prefix' },
		{
			text: 'widg',
			mode: 'prefix',
			fields: ['title'],
			filter: { field: 'category', comparator: 'equals', value: 'even' },
		},
		{ text: 'widg', mode: 'fuzzy-prefix', candidateIds: ids.slice(100) },
		{ query: { operator: 'and', clauses: [{ text: 'widg', mode: 'prefix' }, { text: 'catalog' }] } },
		{ query: { operator: 'or', clauses: [{ text: 'widg', mode: 'prefix' }, { text: 'wodget' }] } },
		{ query: { operator: 'not', clause: { text: 'widg', mode: 'prefix' } } },
		{
			query: {
				operator: 'and',
				clauses: [
					{ text: 'widg', mode: 'fuzzy-prefix' },
					{ operator: 'not', clause: { text: 'absent' } },
				],
			},
		},
	]) {
		const expected = await reader.searchPage({ ...query, limit: 1000 });
		const legacy = await reader.search({ ...query, limit: 100, exactTotal: true });
		assert.strictEqual(expected.total, legacy.total);
		assert.deepStrictEqual(expected.hits.slice(0, 100), legacy.hits);
		const actual = await pages(reader, { ...query, limit: 23 }, expected.total);
		assert.deepStrictEqual(actual, expected.hits);
		assert.strictEqual(new Set(actual.map((hit) => hit.id)).size, expected.total);
		for (let i = 1; i < actual.length; i++) {
			assert(
				actual[i - 1].score > actual[i].score ||
					(actual[i - 1].score === actual[i].score &&
						Buffer.compare(Buffer.from(actual[i - 1].id), Buffer.from(actual[i].id)) < 0),
			);
		}
		for (const hit of expected.hits.filter((_, i) => i >= 100 && i % 29 === 0)) {
			const point = await reader.search({ ...query, candidateIds: [hit.id], limit: 1 });
			assert.deepStrictEqual(point.hits, [hit]);
		}
	}
});

test('retains original exact and fuzzy-prefix snapshots while writer updates arrive', async (context) => {
	const { writer, openReader } = await fixture(context);
	const ids = Array.from({ length: 250 }, (_, i) => String(i).padStart(3, '0'));
	await writer.applyMutationBatch({
		upserts: ids.map((id) => ({ id, version: 'original', fields: { title: 'widget' } })),
	});
	await writer.publish('original');
	const reader = await openReader();
	const queries = ['prefix', 'fuzzy-prefix'].map((mode) => ({ text: 'widget', mode, limit: 31 }));
	const initial = await Promise.all(queries.map((query) => reader.searchPage(query)));
	await writer.applyMutationBatch({
		deletes: ids.slice(0, 25),
		upserts: [
			{ id: '030', version: 'new', fields: { title: 'gadget' } },
			{ id: 'new', version: 'new', fields: { title: 'widget' } },
		],
	});
	await writer.publish('updated');
	for (let i = 0; i < queries.length; i++) {
		const hits = await pages(reader, queries[i], 250, initial[i]);
		assert.deepStrictEqual(
			hits.map((hit) => hit.id),
			ids,
		);
		assert(hits.every((hit) => hit.version === 'original'));
	}
	await reader.reload();
	for (let i = 0; i < queries.length; i++) {
		await assert.rejects(
			reader.searchPage({ ...queries[i], continuation: initial[i].continuation }),
			codeIs('E_CURSOR_EXPIRED'),
		);
	}
});

test('keeps autocomplete complexity bounds explicit and leaves rejected handles usable', async (context) => {
	const { writer, openReader } = await fixture(context);
	const words = Array.from(
		{ length: 50 },
		(_, i) => `catalog${String.fromCharCode(97 + Math.floor(i / 26))}${String.fromCharCode(97 + (i % 26))}`,
	);
	await writer.applyMutationBatch({
		upserts: words.map((title, i) => ({ id: String(i), fields: { title }, filters: { category: 'included' } })),
	});
	await writer.publish('fifty');
	const reader = await openReader();
	assert.strictEqual(
		(await pages(reader, { text: 'catalog', mode: 'prefix', fields: ['title'], limit: 11 }, 50)).length,
		50,
	);
	await writer.applyMutationBatch({
		upserts: [{ id: 'fifty-one', fields: { title: 'catalogzz' }, filters: { category: 'excluded' } }],
	});
	await writer.publish('fifty-one');
	await reader.reload();
	for (const mode of ['prefix', 'fuzzy-prefix']) {
		for (const extra of [
			{},
			{ candidateIds: ['0'] },
			{ filter: { field: 'category', comparator: 'equals', value: 'included' } },
		]) {
			await assert.rejects(
				reader.searchPage({ text: 'catalog', mode, fields: ['title'], ...extra }),
				codeIs('E_PREFIX_TOO_BROAD'),
			);
			assert.strictEqual(reader.status().state, 'open');
		}
	}
	assert.strictEqual((await reader.search({ text: words[0] })).total, 1);
});

test('requires surface terms for autocomplete traversal without poisoning the handle', async (context) => {
	const { writer } = await fixture(context, { surfaceTerms: false });
	await writer.applyMutationBatch({ upserts: [{ id: 'one', fields: { title: 'widget' } }] });
	await writer.publish('one');
	for (const mode of ['prefix', 'fuzzy-prefix']) {
		await assert.rejects(
			writer.searchPage({ text: 'widget', mode }),
			(error) => error.code === 'E_INVALID_ARGUMENT' && /surfaceTerms/.test(error.message),
		);
		assert.strictEqual(writer.status().state, 'open');
	}
	assert.strictEqual((await writer.search({ text: 'widget' })).total, 1);
});
