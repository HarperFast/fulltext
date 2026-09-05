import type { RuntimeInfo as NativeRuntimeInfo } from './addon.js';
import { normalizeNativeError } from './errors.js';
import { loadAddon } from './load-addon.js';

export { FulltextError } from './errors.js';
export type { FulltextErrorCode } from './errors.js';

export interface RuntimeInfo {
	packageVersion: string;
	tantivyVersion: string;
	nativeAbiVersion: number;
	storageBackends: ReadonlyArray<'native'>;
}

export async function runtimeInfo(): Promise<RuntimeInfo> {
	try {
		return toRuntimeInfo(loadAddon().runtimeInfo());
	} catch (error) {
		throw normalizeNativeError(error);
	}
}

function toRuntimeInfo(info: NativeRuntimeInfo): RuntimeInfo {
	if (info.storageBackends.length !== 1 || info.storageBackends[0] !== 'native') {
		throw new Error(`Unexpected storage capabilities: ${info.storageBackends.join(', ')}`);
	}
	return {
		packageVersion: info.packageVersion,
		tantivyVersion: info.tantivyVersion,
		nativeAbiVersion: info.nativeAbiVersion,
		storageBackends: ['native'],
	};
}
