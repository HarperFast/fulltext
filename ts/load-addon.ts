import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

import { FulltextError } from './errors.js';

interface NativeRuntimeInfo {
	packageVersion: string;
	tantivyVersion: string;
	nativeAbiVersion: number;
	storageBackends: Array<string>;
}

interface NativeAddonApi {
	runtimeInfo(): NativeRuntimeInfo;
	__nativeOpen(config: Buffer, callback: NativeCallback): void;
	__nativeApply(handle: number, batch: Buffer, callback: NativeCallback): void;
	__nativeCommit(handle: number, callback: NativeCallback): void;
	__nativeReload(handle: number, callback: NativeCallback): void;
	__nativeSearch(handle: number, request: Buffer, callback: NativeCallback): void;
	__nativeClose(handle: number, rollback: boolean, callback: NativeCallback): void;
	__nativeStatus(handle: number): Buffer;
	__testCreateHandle?(): number;
	__testPanic?(id: number): void;
	__testCheck?(id: number): boolean;
	__testPoisonNativeHandle?(handle: number): void;
	__testOpenHostTransport?(
		handler: (request: Buffer) => Buffer,
		maxOperations: number,
		maxBytes: number,
		timeoutMs: number,
	): number;
	__testHostRoundTrip?(
		handle: number,
		request: Buffer,
		responseBudget: number,
		useTimeout: boolean,
		callback: NativeCallback,
	): void;
	__testVerifyTantivyOnHostTransport?(
		handle: number,
		maxReadResponseBytes: number,
		maxControlResponseBytes: number,
		callback: NativeCallback,
	): void;
	__testCloseHostTransport?(handle: number): boolean;
}

export type NativeCallback = (response: Buffer) => void;

const require = createRequire(import.meta.url);
const expectedNativeAbiVersion = 1;
let loadedAddon: NativeAddonApi | undefined;

export function loadAddon(): NativeAddonApi {
	if (loadedAddon) {
		return loadedAddon;
	}
	const triple = platformTriple();
	const artifact = `fulltext.${triple}.node`;
	const localPath = fileURLToPath(new URL(`../${artifact}`, import.meta.url));
	if (existsSync(localPath)) {
		const addon = require(localPath) as NativeAddonApi;
		validateAddon(addon, localPath);
		loadedAddon = addon;
		return loadedAddon;
	}
	throw new FulltextError('E_NATIVE_ADDON_NOT_FOUND', `No fulltext native artifact is installed for ${triple}`);
}

export function platformTriple(): string {
	if (process.platform === 'linux') {
		return `linux-${process.arch}-${usesGlibc() ? 'gnu' : 'musl'}`;
	}
	if (process.platform === 'darwin') {
		return `darwin-${process.arch}`;
	}
	if (process.platform === 'win32') {
		return `win32-${process.arch}-msvc`;
	}
	throw new FulltextError(
		'E_NATIVE_ADDON_NOT_FOUND',
		`Fulltext does not provide a native artifact for ${process.platform}-${process.arch}`,
	);
}

function usesGlibc(): boolean {
	const report = process.report?.getReport() as { header?: { glibcVersionRuntime?: string } } | undefined;
	return Boolean(report?.header?.glibcVersionRuntime);
}

function validateAddon(addon: NativeAddonApi, artifactPath: string): void {
	const info = addon.runtimeInfo();
	if (info.nativeAbiVersion !== expectedNativeAbiVersion) {
		throw new FulltextError(
			'E_NATIVE_ABI_MISMATCH',
			`Fulltext native ABI ${info.nativeAbiVersion} from ${artifactPath} does not match ${expectedNativeAbiVersion}`,
		);
	}
	if (info.storageBackends.length !== 1 || info.storageBackends[0] !== 'native') {
		throw new FulltextError(
			'E_NATIVE_CAPABILITY_MISMATCH',
			`Unexpected storage capabilities from ${artifactPath}: ${info.storageBackends.join(', ')}`,
		);
	}
}
