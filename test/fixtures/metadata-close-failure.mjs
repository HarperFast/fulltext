import { NativeFullTextIndex, NativeFullTextReader, runtimeInfo } from '../../dist/native.js';
import { encodeOpen, decodeResponse } from '../../dist/codec.js';
import { loadAddon } from '../../dist/load-addon.js';
export { runtimeInfo };

async function open(options, readOnly) {
	const addon = loadAddon();
	const config = {
		...options,
		stopWords: true,
		positions: true,
		surfaceTerms: false,
		fields: options.fields.map((field) => ({ weight: 1, ...field })),
	};
	const cursor = await new Promise((resolve, reject) => {
		const callback = (response) => {
			try {
				resolve(decodeResponse(response));
			} catch (error) {
				reject(error);
			}
		};
		(readOnly ? addon.__nativeOpenReader : addon.__nativeOpen)(encodeOpen(config), callback);
	});
	const handle = cursor.u32();
	const payload = cursor.u8() ? cursor.string() : undefined;
	cursor.finish();
	if ((process.env.FULLTEXT_TEST_CLOSE_KIND === 'reader') === readOnly) addon.__testFailNextClose(handle, true);
	const index = new NativeFullTextIndex({
		handle,
		committedPayload: payload,
		maxBatchBytes: options.limits.maxBatchBytes,
		searchLimits: options.limits,
		fieldNames: options.fields.map(({ name }) => name),
		filterFields: options.filterFields,
	});
	return readOnly ? new NativeFullTextReader(index) : index;
}
export const openNativeFullTextIndex = (options) => open(options, false);
export const openNativeFullTextReader = (options) => open(options, true);
