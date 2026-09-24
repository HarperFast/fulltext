import { parentPort, workerData } from 'node:worker_threads';

const { encodeMutationBatch, openNativeFullTextIndex } = await import(workerData.moduleUrl);
const openIndex = (indexPath) =>
	openNativeFullTextIndex({
		path: indexPath,
		indexId: 'worker-products',
		generation: 'generation-1',
		fields: [{ name: 'title' }],
		analyzer: 'english@1',
		limits: {
			indexingThreads: 2,
			searchThreads: 2,
			writerMemoryBytes: 30_000_000,
			maxQueuedCommands: 8,
			maxQueuedBytes: 16 * 1024 * 1024,
			maxBatchBytes: 16 * 1024 * 1024,
		},
	});

if (workerData.mode === 'owner') {
	await runOwner();
} else if (workerData.mode === 'foreign') {
	await runForeignCall();
} else if (workerData.mode === 'multiple') {
	await Promise.all(workerData.indexPaths.map(openIndex));
	parentPort.postMessage('multiple-open');
	await stayAlive();
} else {
	const opening = openIndex(workerData.indexPath);
	if (workerData.mode === 'opening') {
		parentPort.postMessage('opening');
	}
	const index = await opening;
	if (workerData.mode === 'opening') {
		parentPort.postMessage('opened');
		await stayAlive();
	}
	const upserts = Array.from({ length: 50_000 }, (_, id) => ({
		id: String(id),
		fields: { title: `worker-owned running product ${id}` },
	}));
	const packed = encodeMutationBatch({ upserts }, 16 * 1024 * 1024);
	parentPort.postMessage('applying');
	await index.apply(packed);
	parentPort.postMessage('finished');
}

async function runOwner() {
	const { encodeOpen } = await import(new URL('./codec.js', workerData.moduleUrl));
	const { invoke } = await import(new URL('./invoke.js', workerData.moduleUrl));
	const { loadAddon } = await import(new URL('./load-addon.js', workerData.moduleUrl));
	const cursor = await invoke((callback) =>
		loadAddon().__nativeOpen(
			encodeOpen({
				path: workerData.indexPath,
				indexId: 'worker-products',
				generation: 'generation-1',
				fields: [{ name: 'title', weight: 1 }],
				analyzer: 'english@1',
				stopWords: true,
				positions: true,
				surfaceTerms: false,
				limits: {
					indexingThreads: 1,
					searchThreads: 1,
					writerMemoryBytes: 15_000_000,
					maxQueuedCommands: 8,
					maxQueuedBytes: 1024 * 1024,
					maxBatchBytes: 1024 * 1024,
				},
			}),
			callback,
		),
	);
	const handle = cursor.u32();
	assertNoCommittedPayload(cursor);
	parentPort.postMessage({ type: 'owner-open', handle });
	await new Promise((resolve) => parentPort.once('message', resolve));
	await invoke((callback) => loadAddon().__nativeClose(handle, true, callback));
}

async function runForeignCall() {
	const { invoke } = await import(new URL('./invoke.js', workerData.moduleUrl));
	const { loadAddon } = await import(new URL('./load-addon.js', workerData.moduleUrl));
	const ownIndex = await openIndex(workerData.indexPath);
	try {
		await invoke((callback) => loadAddon().__nativeCommit(workerData.handle, callback));
		parentPort.postMessage({ type: 'foreign-result' });
	} catch (error) {
		parentPort.postMessage({ type: 'foreign-result', code: error.code, message: error.message });
	} finally {
		await ownIndex.close();
	}
}

function assertNoCommittedPayload(cursor) {
	if (cursor.u8() !== 0) throw new Error('new worker-owned index unexpectedly has a committed payload');
	cursor.finish();
}

function stayAlive() {
	return new Promise(() => setInterval(() => {}, 1_000));
}
