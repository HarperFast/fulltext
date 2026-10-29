import assert from 'node:assert';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

const { values } = parseArgs({
	options: {
		'harper-root': { type: 'string' },
		module: { type: 'string' },
		'old-module': { type: 'string' },
		'scratch-dir': { type: 'string' },
		phase: { type: 'string' },
		root: { type: 'string' },
	},
});
const filename = fileURLToPath(import.meta.url);
const marker = 'HARPER_METADATA_QUALIFICATION:';
const initial = [
	{ id: 'a', title: 'Trail shoe', brand: ['zeta', 'alpha'], price: [20, 1] },
	{ id: 'b', title: 'Trail shoe', brand: 'alpha', price: 20 },
	{ id: 'c', title: 'Trail shoe', brand: 'alpha', price: 10 },
	{ id: 'd', title: 'Trail shoe' },
	{ id: 'e', title: 'Trail shoe', brand: 'alpha', price: 20 },
	{ id: 'é', title: 'Trail shoe', brand: 'alpha', price: 20 },
	{ id: 'Ω', title: 'Trail shoe', brand: 'alpha', price: 20 },
].map((record) => ({ ...record, tenant: record.id === 'e' ? 'other' : 'catalog' }));
const array = (value) => (value === undefined ? [] : Array.isArray(value) ? value : [value]);
const utf8 = (left, right) => Buffer.compare(Buffer.from(left), Buffer.from(right));
function config(root, generation) {
	return {
		path: path.join(root, 'index'),
		indexId: 'harper-consumer',
		generation,
		analyzer: 'english@2',
		fields: [{ name: 'title' }],
		filterFields: [
			{ name: 'brand', type: 'string' },
			{ name: 'price', type: 'number' },
		],
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 16,
			maxQueuedBytes: 1_048_576,
			maxBatchBytes: 1_048_576,
		},
	};
}
function upsert(record) {
	return {
		id: record.id,
		fields: { title: record.title },
		filters: Object.fromEntries(
			['brand', 'price'].filter((key) => record[key] !== undefined).map((key) => [key, record[key]]),
		),
	};
}
const checkpointPayload = (sequence) => `harper-source-journal:${sequence}`;
function sortedOracle(records, sorts) {
	return [...records]
		.sort((left, right) => {
			for (const sort of sorts) {
				const a = array(left[sort.field])[0],
					b = array(right[sort.field])[0];
				if (a === undefined || b === undefined) {
					if (a === b) continue;
					return (a === undefined ? -1 : 1) * (sort.missing === 'first' ? 1 : -1);
				}
				const order = typeof a === 'string' ? utf8(a, b) : a - b;
				if (order) return order * (sort.direction === 'asc' ? 1 : -1);
			}
			return utf8(left.id, right.id);
		})
		.map(({ id }) => id);
}
function stats(records) {
	const numbers = records.flatMap((record) => array(record.price));
	return {
		count: numbers.length,
		sum: numbers.reduce((sum, number) => sum + number, 0),
		min: numbers.length ? Math.min(...numbers) : null,
		max: numbers.length ? Math.max(...numbers) : null,
		avg: numbers.length ? numbers.reduce((sum, number) => sum + number, 0) / numbers.length : null,
	};
}
async function bootstrapHarper(root) {
	assert.strictEqual(process.env.ROOTPATH, path.join(root, 'harper'));
	const requireHarper = createRequire(path.join(path.resolve(values['harper-root']), 'package.json'));
	const load = (relative) => requireHarper(path.join(path.resolve(values['harper-root']), 'dist', relative));
	const env = load('utility/environment/environmentManager.js');
	env.initSync();
	load('server/threads/manageThreads.js').setMainIsWorker(true);
	const databases = load('resources/databases.js');
	const { Resource } = load('resources/Resource.js');
	const { transaction } = load('resources/transaction.js');
	const definition = (table, names) => ({
		database: 'metadata_qualification',
		table,
		audit: false,
		attributes: [{ name: 'id', isPrimaryKey: true }, ...names.map((name) => ({ name }))],
	});
	const Source = databases.table(definition('Catalog', ['title', 'brand', 'price', 'tenant']));
	const Journal = databases.table(definition('SourceJournal', ['sequence', 'operation', 'record']));
	const Checkpoint = databases.table(definition('SourceCheckpoint', ['sequence', 'generation']));
	assert(Resource.prototype.isPrototypeOf(Source.prototype));
	assert.strictEqual(Source.put, Resource.put, 'writes must use the real transactional Resource entry point');
	assert.strictEqual(Source.delete, Resource.delete);
	assert.strictEqual(Source.search, Resource.search);
	assert(path.resolve(Source.primaryStore.rootStore.path).startsWith(path.join(root, 'harper', 'database') + path.sep));
	const sequence = async () => (await Checkpoint.get('source'))?.sequence ?? 0;
	const rows = async (Table) => {
		const result = [];
		for await (const record of await Table.search([])) result.push(JSON.parse(JSON.stringify(record)));
		return result;
	};
	const mutate = async (operation, record) => {
		const next = (await sequence()) + 1;
		const context = {};
		await transaction(context, async () => {
			if (operation === 'delete') await Source.delete(record.id, context);
			else await Source.put(record.id, record, context);
			await Journal.put(String(next).padStart(10, '0'), { sequence: next, operation, record }, context);
			await Checkpoint.put('source', { sequence: next }, context);
		});
		return next;
	};
	return { Source, Journal, Checkpoint, rows, sequence, mutate, close: () => databases.closeLoadedDatabases() };
}
async function verifyCurrent(index, records, api) {
	const sorts = [
		[
			{ field: 'brand', direction: 'asc', missing: 'last' },
			{ field: 'price', direction: 'asc', missing: 'last' },
		],
		[
			{ field: 'price', direction: 'asc', missing: 'first' },
			{ field: 'brand', direction: 'desc', missing: 'last' },
		],
	];
	for (const sort of sorts) {
		const result = await index.search({ text: 'trail', exactTotal: true, limit: 100, sort });
		assert.strictEqual(result.total, records.length);
		assert.deepStrictEqual(
			result.hits.map(({ id }) => id),
			sortedOracle(records, sort),
		);
	}
	const allowed = records.filter(({ tenant }) => tenant === 'catalog');
	for (const [candidateIds, eligible] of [
		[undefined, records],
		[allowed.map(({ id }) => id), allowed],
	]) {
		const result = await index.search({
			text: 'trail',
			candidateIds,
			exactTotal: true,
			limit: 1,
			aggregations: [
				{
					name: 'brands',
					field: 'brand',
					type: 'terms',
					size: 100,
					aggregations: [{ name: 'prices', field: 'price', type: 'stats' }],
				},
			],
		});
		const groups = new Map();
		for (const record of eligible)
			for (const brand of new Set(array(record.brand))) groups.set(brand, [...(groups.get(brand) ?? []), record]);
		assert.strictEqual(result.aggregations[0].buckets.length, groups.size);
		for (const bucket of result.aggregations[0].buckets) {
			const members = groups.get(bucket.key);
			assert(members);
			assert.strictEqual(bucket.count, members.length);
			assert.deepStrictEqual(bucket.aggregations, [{ name: 'prices', type: 'stats', ...stats(members) }]);
		}
		assert.strictEqual(result.total, eligible.length);
	}

	await assert.rejects(
		index.search({
			text: 'trail',
			aggregations: [{ name: 'bad', field: 'price', type: 'histogram', interval: Number.MIN_VALUE }],
		}),
		(error) => error.code === 'E_RESOURCE_LIMIT',
	);
	const ranges = Array.from({ length: 100 }, (_, from) => ({ from, to: from + 1 }));
	await assert.rejects(
		index.search({
			text: 'trail',
			aggregations: [
				{
					name: 'outer',
					field: 'price',
					type: 'range',
					ranges,
					aggregations: [{ name: 'inner', field: 'price', type: 'range', ranges }],
				},
			],
		}),
		(error) => error instanceof api.FulltextError && error.code === 'E_RESOURCE_LIMIT',
	);
	await assert.rejects(
		index.searchPage({ text: 'trail', sort: { field: 'price', direction: 'asc', missing: 'last' } }),
		(error) => error instanceof api.FulltextError && error.code === 'E_INVALID_ARGUMENT',
	);
	assert.strictEqual((await index.search({ text: 'trail', exactTotal: true, limit: 100 })).total, records.length);
}
async function child() {
	const root = path.resolve(values.root);
	const owner = JSON.parse(await readFile(path.join(root, 'qualification-owner.json'), 'utf8'));
	assert.strictEqual(owner.root, root);
	assert.strictEqual(owner.harperRoot, path.resolve(values['harper-root']));
	const source = await bootstrapHarper(root);
	const api = await import(pathToFileURL(path.resolve(values.module)).href);
	const runtime = await api.runtimeInfo();
	const runtimeSummary = Object.fromEntries(
		['packageVersion', 'tantivyVersion', 'nativeAbiVersion', 'queryApiVersion'].map((key) => [key, runtime[key]]),
	);
	const apply = async (batch) => assert.deepStrictEqual((await index.applyMutationBatch(batch)).rejected, []);
	let index;
	let result;
	try {
		if (values.phase === 'seed-old') {
			assert.strictEqual(await source.sequence(), 0);
			for (const record of initial) await source.mutate('upsert', record);
			index = await api.openNativeFullTextIndex(config(root, 'old'));
			const checkpoint = await source.sequence();
			const records = await source.rows(source.Source);
			assert.deepStrictEqual((await index.applyMutationBatch({ upserts: records.map(upsert) })).rejected, []);
			await index.publish(checkpointPayload(checkpoint));
			assert.deepStrictEqual(
				(await index.search({ text: 'trail', exactTotal: true, limit: 100 })).hits.map(({ id }) => id).sort(utf8),
				records.map(({ id }) => id).sort(utf8),
			);
			await source.Checkpoint.put('active', { sequence: checkpoint, generation: 'old' });
			result = {
				phase: values.phase,
				records: records.length,
				checkpoint,
				runtime: runtimeSummary,
			};
		} else if (values.phase === 'rebuild-new') {
			assert.strictEqual(
				await source.sequence(),
				initial.length,
				'separate child must reopen durable Harper checkpoint',
			);
			assert.strictEqual((await source.rows(source.Source)).length, initial.length);
			assert.strictEqual(api.inspectNativeFullTextIndex(config(root, 'old')).code, 'E_IDENTITY_MISMATCH');
			for (const open of [api.openNativeFullTextIndex, api.openNativeFullTextReader])
				await assert.rejects(
					open(config(root, 'old')),
					(error) => error instanceof api.FulltextError && error.code === 'E_IDENTITY_MISMATCH',
				);
			const retired = await api.resetNativeFullTextIndex({
				path: config(root, 'old').path,
				indexId: 'harper-consumer',
			});
			assert.strictEqual(retired.state, 'reset');
			await writeFile(path.join(root, 'retired-old.json'), JSON.stringify(retired));
			const barrier = await source.sequence();
			await source.mutate('upsert', {
				id: 'a',
				tenant: 'catalog',
				title: 'Trail shoe updated',
				brand: ['beta', 'alpha'],
				price: [15, 2],
			});
			const snapshot = await source.rows(source.Source);
			assert.deepStrictEqual(snapshot.find(({ id }) => id === 'a').price, [15, 2]);
			index = await api.openNativeFullTextIndex(config(root, 'new'));
			await apply({ upserts: snapshot.slice(0, 1).map(upsert) });
			await source.mutate('delete', { id: 'c' });
			await source.mutate('upsert', {
				id: 'f',
				tenant: 'other',
				title: 'Trail shoe added',
				brand: 'beta',
				price: [50, 3],
			});
			await apply({ upserts: snapshot.slice(1).map(upsert) });
			const tail = (await source.rows(source.Journal))
				.filter(({ sequence }) => sequence > barrier)
				.sort((a, b) => a.sequence - b.sequence);
			assert.deepStrictEqual(
				tail.map(({ operation }) => operation),
				['upsert', 'delete', 'upsert'],
			);
			assert.deepStrictEqual(
				tail.map(({ sequence }) => sequence),
				[barrier + 1, barrier + 2, barrier + 3],
			);
			for (const event of tail)
				await apply(
					event.operation === 'delete' ? { deletes: [event.record.id] } : { upserts: [upsert(event.record)] },
				);
			const checkpoint = await source.sequence();
			assert.strictEqual(tail.at(-1).sequence, checkpoint, 'catch-up must reach the quiesced source checkpoint');
			await index.publish(checkpointPayload(checkpoint));
			await verifyCurrent(index, await source.rows(source.Source), api);
			await source.Checkpoint.put('active', { sequence: checkpoint, generation: 'new' });
			await stat(retired.retiredPath);
			result = {
				phase: values.phase,
				snapshotCheckpoint: barrier,
				snapshotIncludesReplayedUpdate: true,
				replayed: tail.length,
				checkpoint,
				retiredOldPreservedUntilPublish: true,
				runtime: runtimeSummary,
			};
		} else if (values.phase === 'cutover-reopen') {
			const checkpoint = await source.sequence();
			assert.strictEqual(
				api.inspectNativeFullTextIndex(config(root, 'new')).committedPayload,
				checkpointPayload(checkpoint),
			);
			index = await api.openNativeFullTextReader(config(root, 'new'));
			const records = await source.rows(source.Source);
			await verifyCurrent(index, records, api);
			const active = await source.Checkpoint.get('active');
			assert.strictEqual(active.sequence, checkpoint);
			assert.strictEqual(active.generation, 'new');
			result = { phase: values.phase, checkpoint, sourceReopened: true, readerReopened: true, records: records.length };
		} else if (values.phase === 'resume-current') {
			index = await api.openNativeFullTextIndex(config(root, 'new'));
			await source.mutate('upsert', {
				id: 'b',
				tenant: 'catalog',
				title: 'Trail shoe resumed',
				brand: 'alpha',
				price: [25, 4],
			});
			const checkpoint = await source.sequence();
			const event = (await source.rows(source.Journal)).find((event) => event.sequence === checkpoint);
			assert(event);
			await apply({ upserts: [upsert(event.record)] });
			await index.publish(checkpointPayload(checkpoint));
			await verifyCurrent(index, await source.rows(source.Source), api);
			await source.Checkpoint.put('active', { sequence: checkpoint, generation: 'new' });
			result = { phase: values.phase, checkpoint, sourceWritesResumed: true };
		} else if (values.phase === 'reset-for-rollback') {
			const retired = await api.resetNativeFullTextIndex({
				path: config(root, 'new').path,
				indexId: 'harper-consumer',
			});
			assert.strictEqual(retired.state, 'reset');
			result = { phase: values.phase, reset: true };
		} else if (values.phase === 'rollback-old') {
			index = await api.openNativeFullTextIndex(config(root, 'rollback'));
			const checkpoint = await source.sequence();
			const records = await source.rows(source.Source);
			assert.deepStrictEqual((await index.applyMutationBatch({ upserts: records.map(upsert) })).rejected, []);
			await index.publish(checkpointPayload(checkpoint));
			assert.deepStrictEqual(await index.close(), {});
			index = await api.openNativeFullTextReader(config(root, 'rollback'));
			assert.strictEqual(
				api.inspectNativeFullTextIndex(config(root, 'rollback')).committedPayload,
				checkpointPayload(checkpoint),
			);
			assert.deepStrictEqual(
				(await index.search({ text: 'trail', exactTotal: true, limit: 100 })).hits.map(({ id }) => id).sort(utf8),
				records.map(({ id }) => id).sort(utf8),
			);
			const beta = await index.search({
				text: 'trail',
				exactTotal: true,
				limit: 100,
				filter: { field: 'brand', comparator: 'equals', value: 'beta' },
			});
			assert.deepStrictEqual(
				beta.hits.map(({ id }) => id).sort(utf8),
				records
					.filter((record) => array(record.brand).includes('beta'))
					.map(({ id }) => id)
					.sort(utf8),
			);
			const updated = await index.search({
				text: 'trail',
				exactTotal: true,
				limit: 100,
				filter: { field: 'price', comparator: 'equals', value: 25 },
			});
			assert.deepStrictEqual(
				updated.hits.map(({ id }) => id),
				['b'],
			);
			await source.Checkpoint.put('active', { sequence: checkpoint, generation: 'rollback' });
			result = {
				phase: values.phase,
				checkpoint,
				records: records.length,
				oldArtifactReaderReopened: true,
				latestSourceMetadataVerified: true,
			};
		} else throw new Error('Unknown child phase');
	} finally {
		if (index) assert.deepStrictEqual(await index.close({ mode: 'rollback' }), {});
		source.close();
	}
	await new Promise((resolve) => process.stdout.write(marker + JSON.stringify(result) + '\n', resolve));
	process.exit(0);
}
if (values.phase) {
	try {
		await child();
	} catch (error) {
		console.error(error);
		process.exit(1);
	}
} else {
	for (const option of ['harper-root', 'module', 'old-module', 'scratch-dir'])
		assert(values[option], `Pass --${option}`);
	assert.notStrictEqual(path.resolve(values.module), path.resolve(values['old-module']));
	const harperRoot = path.resolve(values['harper-root']),
		requireHarper = createRequire(path.join(harperRoot, 'package.json'));
	const root = await mkdtemp(path.join(path.resolve(values['scratch-dir']), 'fulltext-harper-consumer-'));
	let report;
	try {
		await writeFile(path.join(root, 'qualification-owner.json'), JSON.stringify({ root, harperRoot }));
		for (const directory of ['database', 'log', 'components', 'keys'])
			await mkdir(path.join(root, 'harper', directory), { recursive: true });
		const yaml = requireHarper('yaml');
		const document = yaml.parseDocument(await readFile(path.join(harperRoot, 'static/defaultConfig.yaml'), 'utf8'));
		for (const [keys, value] of [
			[['rootPath'], path.join(root, 'harper')],
			[['storage', 'path'], path.join(root, 'harper', 'database')],
			[['componentsRoot'], 'components'],
			[['logging', 'root'], 'log'],
			[['logging', 'rotation', 'path'], 'log'],
			[['logging', 'file'], false],
		])
			document.setIn(keys, value);
		await writeFile(path.join(root, 'harper', 'harper-config.yaml'), document.toString());
		const execute = (phase, module) => {
			const output = execFileSync(
				process.execPath,
				[filename, '--phase', phase, '--root', root, '--harper-root', harperRoot, '--module', path.resolve(module)],
				{
					encoding: 'utf8',
					timeout: 120_000,
					maxBuffer: 8 * 1024 * 1024,
					env: {
						...Object.fromEntries(
							['PATH', 'HOME', 'USER', 'LOGNAME', 'TMPDIR', 'LANG', 'LC_ALL']
								.filter((key) => process.env[key] !== undefined)
								.map((key) => [key, process.env[key]]),
						),
						ROOTPATH: path.join(root, 'harper'),
						HARPER_STORAGE_ENGINE: 'rocksdb',
						FULLTEXT_PREFER_LOCAL_BUILD: '1',
					},
				},
			);
			const lines = output.split('\n').filter((line) => line.startsWith(marker));
			assert.strictEqual(lines.length, 1, 'child must emit exactly one completed qualification result');
			return JSON.parse(lines[0].slice(marker.length));
		};
		const seed = execute('seed-old', values['old-module']);
		const rebuilt = execute('rebuild-new', values.module);
		assert(
			seed.runtime.queryApiVersion < rebuilt.runtime.queryApiVersion,
			'old module must be a genuinely earlier built artifact',
		);
		const cutover = execute('cutover-reopen', values.module);
		const resumed = execute('resume-current', values.module);
		const caughtUp = execute('cutover-reopen', values.module);
		const reset = execute('reset-for-rollback', values.module);
		const rollback = execute('rollback-old', values['old-module']);
		report = {
			kind: 'local-harper-table-consumer',
			harperRevision: execFileSync('git', ['-C', harperRoot, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
			harperVersion: requireHarper('./package.json').version,
			harperDistDigestFiles: ['resources/Table.js', 'resources/Resource.js', 'resources/databases.js'],
			harperDistDigest: createHash('sha256')
				.update(await readFile(path.join(harperRoot, 'dist/resources/Table.js')))
				.update(await readFile(path.join(harperRoot, 'dist/resources/Resource.js')))
				.update(await readFile(path.join(harperRoot, 'dist/resources/databases.js')))
				.digest('hex'),
			phases: [seed, rebuilt, cutover, resumed, caughtUp, reset, rollback],
			cleanup: 'isolated scratch tree removed',
		};
	} finally {
		await rm(root, { recursive: true, force: true });
	}
	console.log(JSON.stringify(report, null, 2));
}
