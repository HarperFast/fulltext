import { parentPort, workerData } from 'node:worker_threads';

import { encodeMutationBatch, openHarperFullTextIndex } from '../../dist/harper.js';

const entries = new Map();
const wait = new Int32Array(workerData.control);
let blocked = workerData.stage === 'open';
let reported = false;

function maybeBlock(operation) {
	if (!blocked || (workerData.stage === 'read' && operation !== 'read')) return;
	if (!reported) {
		reported = true;
		parentPort.postMessage('blocked');
	}
	blocked = false;
	Atomics.wait(wait, 0, 0);
	if (Atomics.load(wait, 0) === 1) throw new Error('host generation was revoked');
}

const storage = {
	read(key) {
		maybeBlock('read');
		const value = entries.get(key.toString('hex'));
		return value && Buffer.from(value);
	},
	write(mutations) {
		maybeBlock('write');
		for (const mutation of mutations) {
			const key = mutation.key.toString('hex');
			if (mutation.type === 'put') entries.set(key, Buffer.from(mutation.value));
			else entries.delete(key);
		}
	},
	sync() {
		maybeBlock('sync');
	},
};

const config = {
	storage,
	storeIdentity: [101n, 202n, 303n],
	namespace: Buffer.from(`worker-${workerData.stage}`),
	indexId: `worker-${workerData.stage}`,
	generation: 'one',
	fields: [{ name: 'title' }],
	analyzer: 'english@1',
	limits: {
		indexingThreads: 1,
		searchThreads: 1,
		writerMemoryBytes: 15_000_000,
		maxQueuedCommands: 8,
		maxQueuedBytes: 32 * 1024 * 1024,
		maxBatchBytes: 32 * 1024 * 1024,
	},
	transport: {
		maxOperations: 8,
		maxBytes: 40 * 1024 * 1024,
		readTimeoutMs: 30_000,
		maxMutations: 4_096,
		maxReadResponseBytes: 1024 * 1024,
		maxControlResponseBytes: 1024 * 1024,
		maxErrorBytes: 64 * 1024,
	},
};

try {
	const index = await openHarperFullTextIndex(config);
	if (workerData.stage === 'open') throw new Error('open unexpectedly passed the blocked storage call');
	const seed = encodeMutationBatch({ upserts: [{ id: 'seed', fields: { title: 'running shoe' } }] });
	if (workerData.stage === 'publish') {
		await index.apply(seed);
		blocked = true;
		await index.publish('cursor');
	} else if (workerData.stage === 'read') {
		await index.apply(seed);
		await index.publish('cursor');
		blocked = true;
		await index.search({ text: 'running' });
	} else if (workerData.stage === 'apply') {
		blocked = false;
		reported = true;
		const batch = encodeMutationBatch(
			{
				upserts: Array.from({ length: 50_000 }, (_, id) => ({
					id: String(id),
					fields: { title: `worker-owned running product ${id}` },
				})),
			},
			config.limits.maxBatchBytes,
		);
		const applying = index.apply(batch);
		parentPort.postMessage('blocked');
		await applying;
	} else {
		throw new Error(`unknown worker stage ${workerData.stage}`);
	}
	parentPort.postMessage('unexpected-completion');
} catch (error) {
	parentPort.postMessage({ error: error instanceof Error ? error.message : String(error) });
}
