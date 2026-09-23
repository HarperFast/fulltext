import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

import { FulltextError } from './errors.js';

interface NativeRuntimeInfo {
	packageVersion: string;
	tantivyVersion: string;
	nativeAbiVersion: number;
	queryApiVersion: number;
	queryClassIsolationMinimumSearchThreads: number;
	storageBackends: Array<string>;
	limits: {
		maxCommitPayloadBytes: number;
		maxQueryTextBytes: number;
		maxQueryTerms: number;
		maxQueryClauses: number;
		maxCandidateIds: number;
		maxCandidateBytes: number;
		maxRecordIdBytes: number;
		maxPrefixExpansions: number;
		maxFuzzyTerms: number;
		maxSearchWindow: number;
		maxAutocompleteResults: number;
		maxSearchRequestBytes: number;
		maxSearchResponseBytes: number;
		maxSearchBudgetMilliseconds: number;
		maxTraceRecords: number;
		maxTraceSourceBytes: number;
		maxTraceSpans: number;
	};
}

interface NativeAddonApi {
	runtimeInfo(): NativeRuntimeInfo;
	__nativeValidateOpen(config: Buffer): Buffer;
	__nativeInspect(config: Buffer): Buffer;
	__nativeReset(config: Buffer, callback: NativeCallback): void;
	__nativeOpen(config: Buffer, callback: NativeCallback): void;
	__nativeApply(handle: number, batch: Buffer, callback: NativeCallback): void;
	__nativeCommit(handle: number, callback: NativeCallback): void;
	__nativePublish(handle: number, payload: string, callback: NativeCallback): void;
	__nativeReload(handle: number, callback: NativeCallback): void;
	__nativeSearch(handle: number, request: Buffer, callback: NativeCallback): void;
	__nativeTraceMatches(handle: number, request: Buffer, callback: NativeCallback): void;
	__nativeClose(handle: number, rollback: boolean, callback: NativeCallback): void;
	__nativeStatus(handle: number): Buffer;
	__testCreateHandle?(): number;
	__testPanic?(id: number): void;
	__testCheck?(id: number): boolean;
	__testPoisonNativeHandle?(handle: number): void;
	__testPoisonBeforeNextAdmission?(handle: number): void;
	__testFailNextPublish?(handle: number, afterCommit: boolean): void;
	__testFailNextClose?(handle: number, quiesced: boolean): void;
}

export type NativeCallback = (response: Buffer) => void;

const require = createRequire(import.meta.url);
const expectedNativeAbiVersion = 6;
const packageManifest = require('../package.json') as { name: string; version: string };
let loadedAddon: NativeAddonApi | undefined;
let runtimeUsesGlibc: boolean | undefined;

export function loadAddon(): NativeAddonApi {
	if (loadedAddon) {
		return loadedAddon;
	}
	const triple = platformTriple();
	const packageName = platformPackageName(triple);
	if (process.env.FULLTEXT_PREFER_LOCAL_BUILD === '1') {
		loadedAddon = loadLocalAddon(triple, true);
		return loadedAddon;
	}
	const packagedAddon = loadPlatformPackage(packageName);
	if (packagedAddon) {
		validateAddon(packagedAddon, packageName);
		loadedAddon = packagedAddon;
		return loadedAddon;
	}
	const localAddon = loadLocalAddon(triple, false);
	if (localAddon) {
		loadedAddon = localAddon;
		return loadedAddon;
	}
	throw new FulltextError(
		'E_NATIVE_ADDON_NOT_FOUND',
		`No fulltext native artifact is installed for ${triple}; expected optional package ${packageName}`,
	);
}

export function platformTriple(
	runtime: { platform?: NodeJS.Platform; architecture?: string; glibc?: boolean } = {},
): string {
	const platform = runtime.platform ?? process.platform;
	const architecture = runtime.architecture ?? process.arch;
	if (platform === 'linux') {
		return `linux-${architecture}-${(runtime.glibc ?? usesGlibc()) ? 'gnu' : 'musl'}`;
	}
	if (platform === 'darwin') {
		return `darwin-${architecture}`;
	}
	if (platform === 'win32') {
		return `win32-${architecture}-msvc`;
	}
	throw new FulltextError(
		'E_NATIVE_ADDON_NOT_FOUND',
		`Fulltext does not provide a native artifact for ${platform}-${architecture}`,
	);
}

export function platformPackageName(triple: string): string {
	return `${packageManifest.name}-${triple}`;
}

function usesGlibc(): boolean {
	if (runtimeUsesGlibc !== undefined) {
		return runtimeUsesGlibc;
	}
	const report = process.report?.getReport() as { header?: { glibcVersionRuntime?: string } } | undefined;
	runtimeUsesGlibc = Boolean(report?.header?.glibcVersionRuntime);
	return runtimeUsesGlibc;
}

function loadLocalAddon(triple: string, required: true): NativeAddonApi;
function loadLocalAddon(triple: string, required: false): NativeAddonApi | undefined;
function loadLocalAddon(triple: string, required: boolean): NativeAddonApi | undefined {
	const localPath = fileURLToPath(new URL(`../fulltext.${triple}.node`, import.meta.url));
	if (!existsSync(localPath)) {
		if (required) {
			throw new FulltextError(
				'E_NATIVE_ADDON_NOT_FOUND',
				`FULLTEXT_PREFER_LOCAL_BUILD is set but ${localPath} does not exist`,
			);
		}
		return undefined;
	}
	let addon: NativeAddonApi;
	try {
		addon = require(localPath) as NativeAddonApi;
	} catch (error) {
		throw new FulltextError('E_NATIVE_LOAD_FAILED', `Failed to load fulltext native artifact ${localPath}`, error);
	}
	validateAddon(addon, localPath);
	return addon;
}

function loadPlatformPackage(packageName: string): NativeAddonApi | undefined {
	try {
		require.resolve(packageName);
	} catch (error) {
		if (isMissingModule(error, packageName)) {
			return undefined;
		}
		throw new FulltextError('E_NATIVE_LOAD_FAILED', `Failed to resolve fulltext native package ${packageName}`, error);
	}
	try {
		return require(packageName) as NativeAddonApi;
	} catch (error) {
		throw new FulltextError('E_NATIVE_LOAD_FAILED', `Failed to load fulltext native package ${packageName}`, error);
	}
}

function isMissingModule(error: unknown, packageName: string): boolean {
	return Boolean(
		error &&
			typeof error === 'object' &&
			'code' in error &&
			error.code === 'MODULE_NOT_FOUND' &&
			'message' in error &&
			typeof error.message === 'string' &&
			error.message.includes(`'${packageName}'`),
	);
}

function validateAddon(addon: NativeAddonApi, artifactPath: string): void {
	const info = addon.runtimeInfo();
	if (info.packageVersion !== packageManifest.version) {
		throw new FulltextError(
			'E_NATIVE_CAPABILITY_MISMATCH',
			`Fulltext package ${packageManifest.version} cannot load native package ${info.packageVersion} from ${artifactPath}`,
		);
	}
	if (info.nativeAbiVersion !== expectedNativeAbiVersion) {
		throw new FulltextError(
			'E_NATIVE_ABI_MISMATCH',
			`Fulltext native ABI ${info.nativeAbiVersion} from ${artifactPath} does not match ${expectedNativeAbiVersion}`,
		);
	}
	if (info.queryApiVersion !== 1) {
		throw new FulltextError(
			'E_NATIVE_CAPABILITY_MISMATCH',
			`Fulltext query API ${info.queryApiVersion} from ${artifactPath} is not supported`,
		);
	}
	if (typeof addon.__nativeInspect !== 'function') {
		throw new FulltextError(
			'E_NATIVE_CAPABILITY_MISMATCH',
			`Fulltext native artifact ${artifactPath} does not provide index inspection`,
		);
	}
	if (typeof addon.__nativeValidateOpen !== 'function') {
		throw new FulltextError(
			'E_NATIVE_CAPABILITY_MISMATCH',
			`Fulltext native artifact ${artifactPath} does not provide open configuration validation`,
		);
	}
	if (typeof addon.__nativeReset !== 'function') {
		throw new FulltextError(
			'E_NATIVE_CAPABILITY_MISMATCH',
			`Fulltext native artifact ${artifactPath} does not provide index reset`,
		);
	}
	if (info.storageBackends.length !== 1 || info.storageBackends[0] !== 'native') {
		throw new FulltextError(
			'E_NATIVE_CAPABILITY_MISMATCH',
			`Unexpected storage capabilities from ${artifactPath}: ${info.storageBackends.join(', ')}`,
		);
	}
}
