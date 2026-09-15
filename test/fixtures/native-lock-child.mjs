const [moduleUrl, indexPath] = process.argv.slice(2);
const { openNativeFullTextIndex } = await import(moduleUrl);

const index = await openNativeFullTextIndex({
	path: indexPath,
	indexId: 'products',
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

process.send('ready');
process.on('message', async (message) => {
	if (message === 'close') {
		await index.close();
		process.send('closed');
	} else if (message === 'exit') {
		process.disconnect();
	}
});
