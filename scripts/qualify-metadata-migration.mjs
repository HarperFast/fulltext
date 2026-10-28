import assert from 'node:assert';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readdirSync } from 'node:fs';
import { mkdtemp, readdir, readFile, rm } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

const { values } = parseArgs({
	options: {
		'old-module': { type: 'string' },
		module: { type: 'string', default: fileURLToPath(new URL('../dist/native.js', import.meta.url)) },
		'scratch-dir': { type: 'string' },
		action: { type: 'string' },
		config: { type: 'string' },
		checkpoint: { type: 'string', default: 'source-checkpoint-3' },
	},
});
const records = [
	{ id: 'a', fields: { title: 'Trail shoe' }, filters: { brand: 'cedar', price: 20, active: true } },
	{ id: 'b', fields: { title: 'Trail shoe' }, filters: { brand: 'ember', price: 10, active: true } },
	{ id: 'c', fields: { title: 'Trail shoe' }, filters: { brand: 'cedar', price: 30, active: false } },
];

if (values.action) {
	const api = await import(pathToFileURL(path.resolve(values.module)).href);
	const config = JSON.parse(values.config);
	let index;
	try {
		if (values.action === 'write') {
			index = await api.openNativeFullTextIndex(config);
			const upserts = records.map((record) => ({
				...record,
				filters: Object.fromEntries(config.filterFields.map(({ name }) => [name, record.filters[name]])),
			}));
			assert.deepStrictEqual((await index.applyMutationBatch({ upserts })).rejected, []);
			await index.publish(values.checkpoint);
			console.log(JSON.stringify({ written: upserts.length }));
		} else if (values.action === 'reject') {
			assert.deepStrictEqual(api.inspectNativeFullTextIndex(config), {
				state: 'incompatible',
				code: 'E_IDENTITY_MISMATCH',
			});
			for (const open of [api.openNativeFullTextIndex, api.openNativeFullTextReader]) {
				await assert.rejects(open(config), (error) => error.code === 'E_IDENTITY_MISMATCH');
			}
			console.log(
				JSON.stringify({
					inspect: 'E_IDENTITY_MISMATCH',
					writer: 'E_IDENTITY_MISMATCH',
					reader: 'E_IDENTITY_MISMATCH',
				}),
			);
		} else if (values.action === 'reject-old') {
			const inspection = api.inspectNativeFullTextIndex(config);
			assert.strictEqual(inspection.state, 'incompatible');
			assert(['E_INDEX_CORRUPT', 'E_IDENTITY_MISMATCH'].includes(inspection.code));
			const rejected = { inspect: inspection.code };
			for (const [name, open] of [
				['writer', api.openNativeFullTextIndex],
				['reader', api.openNativeFullTextReader],
			]) {
				await assert.rejects(open(config), (error) => {
					assert(['E_INDEX_CORRUPT', 'E_IDENTITY_MISMATCH'].includes(error.code));
					rejected[name] = error.code;
					return true;
				});
			}
			console.log(JSON.stringify(rejected));
		} else if (values.action === 'reset') {
			const retired = await api.resetNativeFullTextIndex({ path: config.path, indexId: config.indexId });
			assert.strictEqual(retired.state, 'reset');
			assert.deepStrictEqual(api.inspectNativeFullTextIndex(config), { state: 'missing' });
			const reclaimed = await api.reclaimRetiredNativeFullTextIndexes({
				path: config.path,
				retiredPath: retired.retiredPath,
			});
			assert.strictEqual(reclaimed.failed, 0);
			console.log(JSON.stringify({ reset: retired.state }));
		} else if (values.action === 'verify-old') {
			assert.deepStrictEqual(api.inspectNativeFullTextIndex(config), {
				state: 'checkpointed',
				committedPayload: values.checkpoint,
			});
			index = await api.openNativeFullTextReader(config);
			const text = await index.search({ text: 'trail', exactTotal: true, limit: 10 });
			assert.strictEqual(text.total, 3);
			assert.deepStrictEqual(text.hits.map(({ id }) => id).sort(), ['a', 'b', 'c']);
			const active = await index.search({
				text: 'trail',
				exactTotal: true,
				limit: 10,
				filter: { field: 'active', comparator: 'equals', value: true },
			});
			assert.strictEqual(active.total, 2);
			assert.deepStrictEqual(active.hits.map(({ id }) => id).sort(), ['a', 'b']);
			let brandIds;
			if (config.filterFields.some(({ type }) => type === 'string')) {
				const strings = await index.search({
					text: 'shoe',
					exactTotal: true,
					limit: 10,
					filter: { field: 'brand', comparator: 'equals', value: 'cedar' },
				});
				assert.strictEqual(strings.total, 2);
				assert.deepStrictEqual(strings.hits.map(({ id }) => id).sort(), ['a', 'c']);
				brandIds = strings.hits.map(({ id }) => id).sort();
			}
			console.log(
				JSON.stringify({
					reopened: true,
					checkpoint: values.checkpoint,
					textTotal: text.total,
					activeIds: active.hits.map(({ id }) => id).sort(),
					...(brandIds ? { brandIds } : {}),
				}),
			);
		} else if (values.action === 'verify') {
			assert.deepStrictEqual(api.inspectNativeFullTextIndex(config), {
				state: 'checkpointed',
				committedPayload: values.checkpoint,
			});
			index = await api.openNativeFullTextReader(config);
			const result = await index.search({
				text: 'trail',
				exactTotal: true,
				limit: 10,
				sort: { field: 'price', direction: 'asc', missing: 'last' },
				filter: { field: 'active', comparator: 'equals', value: true },
				aggregations: [{ name: 'prices', field: 'price', type: 'stats' }],
			});
			assert.strictEqual(result.total, 2);
			assert.deepStrictEqual(
				result.hits.map(({ id }) => id),
				['b', 'a'],
			);
			assert.deepStrictEqual(result.aggregations, [
				{ name: 'prices', type: 'stats', count: 2, sum: 30, min: 10, max: 20, avg: 15 },
			]);
			if (config.filterFields.some(({ type }) => type === 'string')) {
				const strings = await index.search({
					text: 'shoe',
					filter: { field: 'brand', comparator: 'equals', value: 'cedar' },
					sort: { field: 'brand', direction: 'asc', missing: 'last' },
					aggregations: [{ name: 'brands', field: 'brand', type: 'terms', size: 10 }],
				});
				assert.deepStrictEqual(
					strings.hits.map(({ id }) => id),
					['a', 'c'],
				);
				assert.deepStrictEqual(strings.aggregations[0].buckets, [{ key: 'cedar', count: 2 }]);
			}
			console.log(
				JSON.stringify({
					reopened: true,
					checkpoint: values.checkpoint,
					filteredTotal: result.total,
					sortedIds: result.hits.map(({ id }) => id),
					priceSum: 30,
				}),
			);
		} else if (values.action === 'runtime') {
			const info = await api.runtimeInfo();
			console.log(
				JSON.stringify({
					packageVersion: info.packageVersion,
					tantivyVersion: info.tantivyVersion,
					nativeAbiVersion: info.nativeAbiVersion,
					queryApiVersion: info.queryApiVersion,
				}),
			);
		} else throw new Error('Unknown qualification action');
	} finally {
		await index?.close({ mode: 'rollback' });
	}
} else {
	assert(values['old-module'], 'Pass --old-module pointing to a genuinely previous built dist/native.js');
	assert(values['scratch-dir'], 'Pass --scratch-dir pointing to an existing scratch parent');
	assert.notStrictEqual(path.resolve(values['old-module']), path.resolve(values.module));
	const directory = await mkdtemp(path.join(path.resolve(values['scratch-dir']), 'fulltext-migration-'));
	const execute = (module, action, config = {}, checkpoint = 'source-checkpoint-3') =>
		JSON.parse(
			execFileSync(
				process.execPath,
				[
					fileURLToPath(import.meta.url),
					'--module',
					path.resolve(module),
					'--action',
					action,
					'--config',
					JSON.stringify(config),
					'--checkpoint',
					checkpoint,
				],
				{
					encoding: 'utf8',
					timeout: 60_000,
					env: {
						...process.env,
						FULLTEXT_PREFER_LOCAL_BUILD: readdirSync(path.dirname(path.dirname(path.resolve(module)))).some((name) =>
							/^fulltext\..+\.node$/.test(name),
						)
							? '1'
							: '0',
					},
				},
			),
		);
	const snapshot = async (directory) => {
		const entries = [];
		for (const entry of (await readdir(directory, { withFileTypes: true })).sort((a, b) =>
			a.name.localeCompare(b.name),
		)) {
			const child = path.join(directory, entry.name);
			assert(!entry.isSymbolicLink());
			entries.push([
				entry.name,
				entry.isDirectory()
					? await snapshot(child)
					: createHash('sha256')
							.update(await readFile(child))
							.digest('hex'),
			]);
		}
		return entries;
	};
	const configuration = (name, strings) => ({
		path: path.join(directory, name),
		indexId: name,
		generation: 'v1',
		analyzer: 'english@2',
		fields: [{ name: 'title' }],
		filterFields: [
			...(strings ? [{ name: 'brand', type: 'string' }] : []),
			{ name: 'price', type: 'number' },
			{ name: 'active', type: 'boolean' },
		],
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 16,
			maxQueuedBytes: 1_048_576,
			maxBatchBytes: 1_048_576,
		},
	});
	try {
		const oldRuntime = execute(values['old-module'], 'runtime');
		const runtime = execute(values.module, 'runtime');
		assert(
			oldRuntime.queryApiVersion < runtime.queryApiVersion,
			'The old facade/addon must precede the metadata query API',
		);
		const numeric = configuration('numeric-boolean', false);
		execute(values['old-module'], 'write', numeric);
		const numericBooleanCompatibility = execute(values.module, 'verify', numeric);
		execute(values.module, 'write', numeric, 'source-checkpoint-4');
		const numericBooleanOldReaderAfterNewWrite = execute(
			values['old-module'],
			'verify-old',
			numeric,
			'source-checkpoint-4',
		);
		const strings = configuration('string-metadata', true);
		execute(values['old-module'], 'write', strings);
		const before = await snapshot(strings.path);
		const rejected = execute(values.module, 'reject', strings);
		assert.deepStrictEqual(
			await snapshot(strings.path),
			before,
			'Rejected inspection/open must not rewrite any index file',
		);
		execute(values.module, 'reset', strings);
		const rebuilt = { ...strings, generation: 'v2' };
		execute(values.module, 'write', rebuilt);
		const rebuild = execute(values.module, 'verify', rebuilt);
		const beforeOldRejection = await snapshot(rebuilt.path);
		const oldRejected = execute(values['old-module'], 'reject-old', rebuilt);
		assert.deepStrictEqual(
			await snapshot(rebuilt.path),
			beforeOldRejection,
			'Rejected old inspection/open must not rewrite any rebuilt index file',
		);
		execute(values.module, 'reset', rebuilt);
		const downgraded = { ...strings, generation: 'v3' };
		execute(values['old-module'], 'write', downgraded);
		const downgrade = execute(values['old-module'], 'verify-old', downgraded);
		console.log(
			JSON.stringify(
				{
					oldRuntime,
					runtime,
					numericBooleanCompatibility,
					numericBooleanOldReaderAfterNewWrite,
					stringMigration: { rejected, indexContentsUnchanged: true, newGeneration: rebuilt.generation, ...rebuild },
					downgrade: {
						oldRejected,
						rebuiltIndexContentsUnchanged: true,
						newGeneration: downgraded.generation,
						...downgrade,
					},
				},
				null,
				2,
			),
		);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
}
