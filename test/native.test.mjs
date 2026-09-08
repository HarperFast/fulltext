import assert from 'node:assert';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { runtimeInfo } from '@harperfast/fulltext/native';
import { decodeResponse, encodeOpen } from '../dist/codec.js';
import { normalizeNativeError } from '../dist/errors.js';
import { loadAddon, platformTriple } from '../dist/load-addon.js';

const packageManifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const cargoManifest = readFileSync(new URL('../Cargo.toml', import.meta.url), 'utf8');
const cargoPackageVersion = /^version\s*=\s*"([^"]+)"/m.exec(tomlSection(cargoManifest, 'package'))?.[1];
const tantivyVersion = /^tantivy\s*=\s*"=([^"]+)"/m.exec(tomlSection(cargoManifest, 'dependencies'))?.[1];

test('loads the artifact for the executing platform', async () => {
	const info = await runtimeInfo();
	assert.deepStrictEqual(info, {
		packageVersion: packageManifest.version,
		tantivyVersion,
		nativeAbiVersion: 1,
		storageBackends: ['native'],
	});
	assert.strictEqual(cargoPackageVersion, packageManifest.version);
	assert.match(platformTriple(), /^(darwin|linux|win32)-(arm64|x64)(-(gnu|musl|msvc))?$/);
});

test('turns a panic into a coded terminal error', async () => {
	const addon = loadAddon();
	assert(addon.__testCreateHandle && addon.__testPanic && addon.__testCheck);
	const firstHandle = addon.__testCreateHandle();
	const secondHandle = addon.__testCreateHandle();
	assert.throws(
		() => addon.__testCheck(0),
		(error) => normalizeNativeError(error).code === 'E_NATIVE_FAILURE',
	);
	assert.strictEqual(addon.__testCheck(secondHandle), true);
	assert.throws(
		() => addon.__testPanic(firstHandle),
		(error) => normalizeNativeError(error).code === 'E_NATIVE_PANIC',
	);
	assert.throws(
		() => addon.__testCheck(firstHandle),
		(error) => normalizeNativeError(error).code === 'E_POISONED',
	);
	assert.strictEqual(addon.__testCheck(secondHandle), true);
	await assert.doesNotReject(runtimeInfo());
});

test('default close tears down a poisoned native handle', async (context) => {
	const indexPath = mkdtempSync(path.join(tmpdir(), 'harper-fulltext-poison-'));
	context.after(() => rmSync(indexPath, { recursive: true, force: true }));
	const addon = loadAddon();
	assert(addon.__testPoisonNativeHandle);
	const config = {
		path: indexPath,
		indexId: 'poison-close',
		generation: 'one',
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
	};
	const opened = await invoke((callback) => addon.__nativeOpen(encodeOpen(config), callback));
	const handle = opened.u32();
	opened.finish();
	addon.__testPoisonNativeHandle(handle);
	const closed = await invoke((callback) => addon.__nativeClose(handle, false, callback));
	closed.finish();
	const reopened = await invoke((callback) => addon.__nativeOpen(encodeOpen(config), callback));
	const reopenedHandle = reopened.u32();
	reopened.finish();
	const reclosed = await invoke((callback) => addon.__nativeClose(reopenedHandle, false, callback));
	reclosed.finish();
});

function invoke(start) {
	return new Promise((resolve, reject) => {
		try {
			start((response) => {
				try {
					resolve(decodeResponse(response));
				} catch (error) {
					reject(error);
				}
			});
		} catch (error) {
			reject(normalizeNativeError(error));
		}
	});
}

function tomlSection(manifest, name) {
	const sectionStart = manifest.indexOf(`[${name}]`);
	assert.notStrictEqual(sectionStart, -1, `Cargo.toml is missing [${name}]`);
	const bodyStart = sectionStart + name.length + 2;
	const nextSection = manifest.indexOf('\n[', bodyStart);
	return manifest.slice(bodyStart, nextSection === -1 ? undefined : nextSection);
}
