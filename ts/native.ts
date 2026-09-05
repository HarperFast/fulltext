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
		const info = loadAddon().runtimeInfo();
		return {
			packageVersion: info.packageVersion,
			tantivyVersion: info.tantivyVersion,
			nativeAbiVersion: info.nativeAbiVersion,
			storageBackends: ['native'],
		};
	} catch (error) {
		throw normalizeNativeError(error);
	}
}
