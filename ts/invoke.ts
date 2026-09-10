import { Cursor, decodeResponse } from './codec.js';
import { normalizeNativeError } from './errors.js';
import type { NativeCallback } from './load-addon.js';

export function invoke(start: (callback: NativeCallback) => void): Promise<Cursor> {
	return new Promise((resolve, reject) => {
		try {
			start((response) => {
				try {
					resolve(decodeResponse(response));
				} catch (error) {
					reject(normalizeNativeError(error));
				}
			});
		} catch (error) {
			reject(normalizeNativeError(error));
		}
	});
}
