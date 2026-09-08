import { encodeMutationBatch, openNativeFullTextIndex } from '../../dist/native.js';

const [indexPath, mode] = process.argv.slice(2);
const index = await openNativeFullTextIndex({
	path: indexPath,
	indexId: 'crash-products',
	generation: 'generation-1',
	fields: [{ name: 'title' }],
	analyzer: 'english@1',
	limits: {
		indexingThreads: 1,
		searchThreads: 1,
		writerMemoryBytes: 15_000_000,
		maxQueuedCommands: 8,
		maxQueuedBytes: 1024 * 1024,
		maxBatchBytes: 1024 * 1024,
	},
});
await index.apply(encodeMutationBatch({ upserts: [{ id: 'product-1', fields: { title: 'durable product' } }] }));
if (mode === 'committed') {
	await index.commit();
}
process.exit(17);
