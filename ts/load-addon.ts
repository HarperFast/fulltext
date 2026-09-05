import { existsSync } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';

import type * as NativeAddon from './addon.js';
import { FulltextError } from './errors.js';

const require = createRequire(import.meta.url);
let loadedAddon: typeof NativeAddon | undefined;

export function loadAddon(): typeof NativeAddon {
	if (loadedAddon) {
		return loadedAddon;
	}
	const triple = platformTriple();
	const artifact = `fulltext.${triple}.node`;
	const localPath = fileURLToPath(new URL(`../${artifact}`, import.meta.url));
	if (existsSync(localPath)) {
		loadedAddon = require(localPath) as typeof NativeAddon;
		return loadedAddon;
	}

	const platformPackage = `@harperfast/fulltext-${triple}`;
	let packagePath: string;
	try {
		packagePath = require.resolve(platformPackage);
	} catch (error) {
		throw new FulltextError(
			'E_NATIVE_ADDON_NOT_FOUND',
			`No fulltext native artifact is installed for ${triple}`,
			error,
		);
	}
	loadedAddon = require(packagePath) as typeof NativeAddon;
	return loadedAddon;
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
