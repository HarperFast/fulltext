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
if (workerData.mode === 'multiple') {
	await Promise.all(workerData.indexPaths.map(openIndex));
	parentPort.postMessage('multiple-open');
	await stayAlive();
}
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

function stayAlive() {
	return new Promise(() => setInterval(() => {}, 1_000));
}
