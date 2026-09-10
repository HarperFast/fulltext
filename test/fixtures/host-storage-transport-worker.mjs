import { parentPort } from 'node:worker_threads';

import { loadAddon } from '../../dist/load-addon.js';

const addon = loadAddon();
const handle = addon.__testOpenHostTransport(
	(dispatchId, request) => {
		addon.__hostStorageComplete(dispatchId, request);
	},
	2,
	1_024,
	1_000,
);
addon.__testConfigureHostTransportCleanup(handle, 256, 512);
addon.__testHoldHostTransportCapacity(handle, 8, 128, true);
addon.__testHoldHostTransportCapacity(handle, 10, 128, false);
parentPort.postMessage({ handle, stats: addon.__testHostTransportStats(handle) });
setInterval(() => {}, 1_000);
