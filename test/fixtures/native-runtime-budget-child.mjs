import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { loadAddon } from '../../dist/load-addon.js';

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
	analyzer: 'english@2',
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
	if (process.argv[3] === 'unproven-first') {
		const addon = loadAddon();
		addon.__testFailNextOpenCleanup();
		let failedOpen;
		try {
			await openNativeFullTextIndex(options('unproven'));
		} catch (error) {
			failedOpen = error.code;
		}
		let lateConfiguration;
		try {
			configureNativeFullTextRuntime(limits);
		} catch (error) {
			lateConfiguration = error.code;
		}
		if (process.send) await new Promise((resolve) => process.send({ failedOpen, lateConfiguration }, resolve));
		rmSync(root, { recursive: true, force: true });
		process.exit(0);
	}
	if (process.argv[3] === 'failed-first') {
		const failedPath = path.join(root, 'failed');
		mkdirSync(failedPath);
		writeFileSync(path.join(failedPath, 'meta.json'), '{}');
		let failedOpen;
		try {
			await openNativeFullTextIndex(options('failed'));
		} catch (error) {
			failedOpen = error.code;
		}
		configureNativeFullTextRuntime(limits);
		const recovered = await openNativeFullTextIndex(options('recovered'));
		await recovered.close();
		if (process.send) await new Promise((resolve) => process.send({ failedOpen }, resolve));
		rmSync(root, { recursive: true, force: true });
		process.exit(0);
	}
	if (process.argv[3] === 'late') {
		const unbounded = await openNativeFullTextIndex(options('unbounded'));
		let lateConfiguration;
		try {
			configureNativeFullTextRuntime(limits);
		} catch (error) {
			lateConfiguration = error.code;
		}
		await unbounded.close();
		if (process.send) await new Promise((resolve) => process.send({ lateConfiguration }, resolve));
		rmSync(root, { recursive: true, force: true });
		process.exit(0);
	}
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
