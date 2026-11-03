import { setTimeout as delay } from 'node:timers/promises';
import * as native from '../../dist/native.js';
export * from '../../dist/native.js';

export async function openNativeFullTextIndex(options) {
	const index = await native.openNativeFullTextIndex(options);
	const publish = index.publish.bind(index);
	let publications = 0;
	index.publish = async (...args) => {
		const result = await publish(...args);
		// Cross the short fixture's mixed-phase duration after its first real update.
		if (++publications === 2) await delay(1100);
		return result;
	};
	return index;
}
