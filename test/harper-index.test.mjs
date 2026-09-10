import assert from 'node:assert';
import test from 'node:test';
import { Worker } from 'node:worker_threads';

import { encodeMutationBatch, openHarperFullTextIndex } from '@harperfast/fulltext/harper';

const readResponseBytes = 1024 * 1024;
const controlResponseBytes = 1024 * 1024;

function createStorage() {
	const entries = new Map();
	const calls = [];
	return {
		calls,
		storage: {
			read(key) {
				calls.push('read');
				const value = entries.get(key.toString('hex'));
				return value && Buffer.from(value);
			},
			write(mutations, policy) {
				calls.push(`write:${policy}`);
				const next = new Map(entries);
				for (const mutation of mutations) {
					const key = mutation.key.toString('hex');
					if (mutation.type === 'put') next.set(key, Buffer.from(mutation.value));
					else next.delete(key);
				}
				entries.clear();
				for (const [key, value] of next) entries.set(key, value);
			},
			sync() {
				calls.push('sync');
			},
		},
	};
}

function options(storage, overrides = {}) {
	return {
		storage,
		storeIdentity: [11n, 22n, 33n],
		namespace: Buffer.from('products-title'),
		indexId: 'products-title',
		generation: 'generation-1',
		fields: [{ name: 'title', weight: 3 }, { name: 'description' }],
		analyzer: 'english@1',
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15_000_000,
			maxQueuedCommands: 32,
			maxQueuedBytes: 8 * 1024 * 1024,
			maxBatchBytes: 8 * 1024 * 1024,
		},
		transport: {
			maxOperations: 32,
			maxBytes: 40 * 1024 * 1024,
			readTimeoutMs: 5_000,
			maxMutations: 4_096,
			maxReadResponseBytes: readResponseBytes,
			maxControlResponseBytes: controlResponseBytes,
			maxErrorBytes: 64 * 1024,
		},
		...overrides,
	};
}

test('publishes, searches, closes, and reopens entirely through host storage', async () => {
	const host = createStorage();
	const config = options(host.storage);
	let index = await openHarperFullTextIndex(config);
	assert.strictEqual(index.committedPayload, undefined);
	await index.apply(
		encodeMutationBatch({
			upserts: [
				{ id: 'shoe-1', fields: { title: 'Trail Running Shoes', description: 'red outdoor footwear' } },
				{ id: 'rack-1', fields: { title: 'Wood Rack', description: 'shoe organizer' } },
			],
		}),
	);
	await index.publish('cursor-v1:42');
	assert.strictEqual(index.committedPayload, 'cursor-v1:42');
	assert.deepStrictEqual(
		(await index.search({ text: 'running shoes', exactTotal: true })).hits.map((hit) => hit.id),
		['shoe-1', 'rack-1'],
	);
	await index.close();

	const callsAfterClose = host.calls.length;
	await new Promise((resolve) => setImmediate(resolve));
	assert.strictEqual(host.calls.length, callsAfterClose, 'storage was entered after close resolved');

	index = await openHarperFullTextIndex(config);
	assert.strictEqual(index.committedPayload, 'cursor-v1:42');
	assert.deepStrictEqual(
		(await index.search({ text: 'running shoes', exactTotal: true })).hits.map((hit) => hit.id),
		['shoe-1', 'rack-1'],
	);
	await index.close();
	assert(host.calls.includes('write:wal'));
	assert(host.calls.includes('sync'));
});

test('rejects duplicate owner-writer opens for one process identity and namespace', async () => {
	const host = createStorage();
	const config = options(host.storage);
	const first = await openHarperFullTextIndex(config);
	await assert.rejects(openHarperFullTextIndex(config), (error) => error.code === 'E_DUPLICATE_OPEN');
	await first.close();
});

test('rejects an oversized cursor without changing or poisoning the generation', async () => {
	const host = createStorage();
	const index = await openHarperFullTextIndex(options(host.storage));
	await assert.rejects(index.publish('x'.repeat(64 * 1024 + 1)), (error) => error.code === 'E_INVALID_ARGUMENT');
	assert.strictEqual(index.status().state, 'open');
	await index.close();
});

for (const stage of ['open', 'read', 'apply', 'publish']) {
	test(`worker termination safely drains a hosted runtime during ${stage}`, async () => {
		const control = new SharedArrayBuffer(4);
		const worker = new Worker(new URL('./fixtures/harper-worker-child.mjs', import.meta.url), {
			workerData: { stage, control },
		});
		await new Promise((resolve, reject) => {
			const timeout = setTimeout(() => reject(new Error(`${stage} did not enter host storage`)), 10_000);
			worker.once('error', reject);
			worker.on('message', (message) => {
				if (message === 'blocked') {
					clearTimeout(timeout);
					resolve();
				} else if (message?.error) {
					clearTimeout(timeout);
					reject(new Error(message.error));
				}
			});
		});
		const started = performance.now();
		Atomics.store(new Int32Array(control), 0, 1);
		await worker.terminate();
		assert(performance.now() - started < 5_000, `${stage} teardown did not drain bounded storage work promptly`);
	});
}
