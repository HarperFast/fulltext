import assert from 'node:assert';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Worker } from 'node:worker_threads';

import { openNativeFullTextIndex } from '@harperfast/fulltext/native';
import { closeIndex } from '../../benchmarks/metadata-cleanup.mjs';
import { startWriterDiagnostics, writeWriterDiagnostic } from '../../benchmarks/writer-diagnostics.mjs';

const scenario = process.argv[2];
const moduleUrl = new URL('../../dist/native.js', import.meta.url).href;

const diagnostics = startWriterDiagnostics('native-worker-scenario');
try {
	if (scenario === 'apply') {
		await runApply();
	} else if (scenario === 'opening') {
		await runOpening();
	} else if (scenario === 'multiple') {
		await runMultiple();
	} else if (scenario === 'foreign') {
		await runForeignEnvironment();
	} else if (scenario === 'sequence') {
		await runApply();
		await runOpening();
		await runMultiple();
		await runForeignEnvironment();
	} else if (scenario === 'apply-stress') {
		for (let attempt = 1; attempt <= 5; attempt++) {
			mark(`apply:${attempt}:starting`);
			await runApply();
		}
	} else {
		throw new Error(`unknown native worker scenario: ${scenario}`);
	}
} finally {
	diagnostics.stop();
}

async function runApply() {
	const indexPath = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-worker-'));
	mark(`apply:path:${indexPath}`);
	let quiescent = false;
	try {
		const worker = startWorker({ indexPath });
		await waitForMessage(worker, 'applying');
		mark('apply:terminating');
		await worker.terminate();
		mark('apply:reopening');
		const index = await waitForOpen(indexPath);
		assert.strictEqual((await index.search({ text: 'worker owned product', exactTotal: true })).total, 0);
		await closeIndex(index);
		quiescent = true;
		mark('apply:closed');
	} finally {
		removeQuiescentFixtures([indexPath], quiescent);
	}
}

async function runOpening() {
	const indexPath = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-worker-open-'));
	let quiescent = false;
	try {
		const worker = startWorker({ indexPath, mode: 'opening' });
		await waitForMessage(worker, 'opening');
		mark('opening:terminating');
		await worker.terminate();
		mark('opening:reopening');
		const index = await waitForOpen(indexPath);
		await closeIndex(index);
		quiescent = true;
		mark('opening:closed');
	} finally {
		removeQuiescentFixtures([indexPath], quiescent);
	}
}

async function runMultiple() {
	const indexPaths = Array.from({ length: 3 }, () => mkdtempSync(path.join(tmpdir(), 'harper-fulltext-worker-many-')));
	let quiescent = false;
	try {
		const worker = startWorker({ indexPaths, mode: 'multiple' });
		await waitForMessage(worker, 'multiple-open');
		mark('multiple:terminating');
		await worker.terminate();
		mark('multiple:reopening');
		const opened = await Promise.allSettled(indexPaths.map(waitForOpen));
		const closed = await Promise.allSettled(
			opened.filter(({ status }) => status === 'fulfilled').map(({ value }) => closeIndex(value)),
		);
		const errors = [...opened, ...closed].filter(({ status }) => status === 'rejected').map(({ reason }) => reason);
		if (errors.length) throw new AggregateError(errors, 'multiple-index reopen or close failed', { cause: errors[0] });
		quiescent = true;
		mark('multiple:closed');
	} finally {
		removeQuiescentFixtures(indexPaths, quiescent);
	}
}

async function runForeignEnvironment() {
	const indexPath = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-worker-owner-'));
	const callerIndexPath = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-worker-caller-'));
	let quiescent = false;
	try {
		const owner = startWorker({ indexPath, mode: 'owner' });
		const ownerExit = waitForExit(owner);
		const opened = await waitForMessage(owner, (message) => message?.type === 'owner-open');
		const caller = startWorker({ handle: opened.handle, indexPath: callerIndexPath, mode: 'foreign' });
		const callerExit = waitForExit(caller);
		const result = await waitForMessage(caller, (message) => message?.type === 'foreign-result');
		assert.strictEqual(result.code, 'E_NATIVE_FAILURE');
		assert.match(result.message, /another Node environment/);
		assert.strictEqual(await callerExit, 0);
		owner.postMessage('close');
		assert.strictEqual(await ownerExit, 0);
		const index = await waitForOpen(indexPath);
		await closeIndex(index);
		quiescent = true;
		mark('foreign:closed');
	} finally {
		removeQuiescentFixtures([indexPath, callerIndexPath], quiescent);
	}
}

function startWorker(workerData) {
	return new Worker(new URL('./native-worker-child.mjs', import.meta.url), {
		workerData: { ...workerData, moduleUrl },
	});
}

function waitForMessage(worker, expected) {
	return new Promise((resolve, reject) => {
		const matches = typeof expected === 'function' ? expected : (message) => message === expected;
		const onError = (error) => {
			worker.off('message', onMessage);
			reject(error);
		};
		const onMessage = (message) => {
			if (!matches(message)) return;
			worker.off('error', onError);
			worker.off('message', onMessage);
			resolve(message);
		};
		worker.once('error', onError);
		worker.on('message', onMessage);
	});
}

function waitForExit(worker) {
	return new Promise((resolve, reject) => {
		worker.once('error', reject);
		worker.once('exit', resolve);
	});
}

function mark(stage) {
	diagnostics.stage(stage);
	process.stderr.write(`${stage}\n`);
}

function removeQuiescentFixtures(paths, quiescent) {
	for (const directory of paths) {
		if (quiescent) rmSync(directory, { recursive: true, force: true });
		else writeWriterDiagnostic({ name: 'native-worker-scenario', stage: 'retained-index', directory });
	}
}

async function waitForOpen(indexPath) {
	const deadline = performance.now() + 10_000;
	while (true) {
		try {
			return await openNativeFullTextIndex({
				path: indexPath,
				indexId: process.env.FULLTEXT_TEST_WORKER_REOPEN_ID ?? 'worker-products',
				generation: 'generation-1',
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
		} catch (error) {
			if (!['E_DUPLICATE_OPEN', 'E_LOCK_BUSY'].includes(error.code) || performance.now() >= deadline) {
				throw error;
			}
			await new Promise((resolve) => setTimeout(resolve, 10));
		}
	}
}
