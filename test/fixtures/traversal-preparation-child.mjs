import assert from 'node:assert';
import { pbkdf2 } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';
import { openNativeFullTextIndex, openNativeFullTextReader } from '@harperfast/fulltext/native';

const scenario = process.argv[2];
const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-preparation-'));
const config = {
	path: directory,
	indexId: 'preparation',
	generation: 'one',
	analyzer: 'english@2',
	fields: [{ name: 'title', weight: 1 }],
	filterFields: [{ name: 'category', type: 'string' }],
	limits: {
		indexingThreads: 1,
		searchThreads: 2,
		writerMemoryBytes: 15000000,
		maxQueuedCommands: scenario === 'bytes' || scenario === 'concurrent' ? 2 : 1,
		maxQueuedBytes: 8388608,
		maxBatchBytes: 8388608,
	},
};
let writer, reader, blocker;
try {
	writer = await openNativeFullTextIndex(config);
	await writer.applyMutationBatch({ upserts: ['a', 'b', 'c'].map((id) => ({ id, fields: { title: 'widget' } })) });
	await writer.publish('one');
	reader = await openNativeFullTextReader(config);
	const query = {
		text: 'widget',
		candidateIds: Array.from({ length: 256 }, (_, i) => String(i).padStart(4, '0') + 'x'.repeat(4092)),
		filter: {
			field: 'category',
			comparator: 'in',
			value: Array.from({ length: 120 }, (_, i) => String(i).padStart(4, '0') + 'v'.repeat(59996)),
		},
	};
	if (scenario === 'commands' || scenario === 'bytes') {
		const pending = reader.searchPage(query).then(
			(result) => ({ result }),
			(error) => ({ error }),
		);
		await assert.rejects(
			reader.searchPage(query),
			(error) => error.code === 'E_QUEUE_FULL' && /preparation/.test(error.message),
		);
		assert.equal((await reader.searchPage({ text: 'widget' })).total, 3);
		assert.equal((await reader.search({ text: 'widget' })).total, 3);
		const completed = await pending;
		if (completed.error) {
			assert.equal(completed.error.code, 'E_QUEUE_FULL');
			assert(!/preparation/.test(completed.error.message));
			assert.equal((await reader.searchPage(query)).total, 0);
		} else assert.equal(completed.result.total, 0);
	} else if (scenario === 'reentrant') {
		const shared = { text: 'widget', candidateIds: ['a', 'b', 'c', ...query.candidateIds.slice(0, 20)] };
		const ids = shared.candidateIds.slice();
		let nested;
		Object.defineProperty(ids, 0, {
			get() {
				nested ??= reader.searchPage(shared);
				return 'a';
			},
		});
		await assert.rejects(
			reader.searchPage({ ...shared, candidateIds: ids }),
			(error) => error.code === 'E_QUEUE_FULL' && /preparation/.test(error.message),
		);
		assert.equal((await nested).total, 3);
	} else if (scenario === 'concurrent') {
		const shared = { text: 'widget', candidateIds: ['a', 'b', 'c', ...query.candidateIds.slice(0, 20)] };
		const [first, second] = await Promise.all([
			reader.searchPage({ ...shared, limit: 1 }),
			reader.searchPage({ ...shared, limit: 2 }),
		]);
		const changed = Buffer.from(first.continuation, 'base64url');
		changed.writeFloatLE(123, 53);
		await assert.rejects(
			reader.searchPage({ ...shared, continuation: changed.toString('base64url') }),
			(error) => error.code === 'E_INVALID_ARGUMENT',
		);
		assert.deepStrictEqual(
			(await reader.searchPage({ ...shared, continuation: first.continuation })).hits.map((hit) => hit.id),
			['b', 'c'],
		);
		assert.deepStrictEqual(
			(await reader.searchPage({ ...shared, continuation: second.continuation })).hits.map((hit) => hit.id),
			['c'],
		);
	} else if (scenario === 'timeout') {
		blocker = promisify(pbkdf2)('preparation', 'test', 1000000, 32, 'sha256');
		await assert.rejects(
			reader.searchPage(query, { remainingBudgetMilliseconds: 50 }),
			(error) => error.code === 'E_TIMEOUT' && /preparation/.test(error.message),
		);
		await blocker;
	} else if (scenario === 'close') {
		const pending = reader.searchPage(query);
		const rejected = assert.rejects(pending, (error) => error.code === 'E_CLOSED');
		await reader.close();
		await rejected;
		reader = await openNativeFullTextReader(config);
	} else throw new Error(`unknown scenario ${scenario}`);
	assert.equal((await reader.searchPage({ text: 'widget', limit: 1 })).hits[0].id, 'a');
	const first = await reader.searchPage({ text: 'widget', limit: 1 });
	const edited = Buffer.from(first.continuation, 'base64url');
	edited.writeFloatLE(123, 53);
	await assert.rejects(
		reader.searchPage({ text: 'widget', continuation: edited.toString('base64url') }),
		(error) => error.code === 'E_INVALID_ARGUMENT',
	);
	assert.equal((await reader.searchPage({ text: 'widget', continuation: first.continuation })).hits[0].id, 'b');
	console.log(JSON.stringify({ scenario, recovered: true }));
} finally {
	if (blocker) await blocker;
	if (reader) await reader.close();
	if (writer) await writer.close({ mode: 'rollback' });
	await rm(directory, { recursive: true, force: true });
}
