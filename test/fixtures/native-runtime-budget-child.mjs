import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const [{ configureNativeFullTextRuntime, openNativeFullTextIndex }] = await Promise.all([
	import(pathToFileURL(process.argv[2]).href),
]);

const root = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-runtime-budget-'));
const limits = {
	maxResidentIndexes: 1,
	maxIndexingThreads: 1,
	maxSearchThreads: 1,
	maxWriterMemoryBytes: 15_000_000,
	maxQueuedBytes: 2 * 1024 * 1024,
	maxExpensiveSearches: 1,
};
const options = (name) => ({
	path: path.join(root, name),
	indexId: name,
	generation: 'one',
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

try {
	configureNativeFullTextRuntime(limits);
	configureNativeFullTextRuntime({ ...limits });
	let conflict;
	try {
		configureNativeFullTextRuntime({ ...limits, maxResidentIndexes: 2 });
	} catch (error) {
		conflict = error.code;
	}
	const first = await openNativeFullTextIndex(options('first'));
	let saturated;
	try {
		await openNativeFullTextIndex(options('second'));
	} catch (error) {
		saturated = error.code;
	}
	await first.close();
	const second = await openNativeFullTextIndex(options('second'));
	await second.close();
	process.send?.({ conflict, saturated });
} finally {
	rmSync(root, { recursive: true, force: true });
}
