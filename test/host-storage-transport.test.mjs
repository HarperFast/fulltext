import assert from 'node:assert';
import test from 'node:test';

import { createHostStorageHandler } from '../dist/host-storage.js';
import { loadAddon } from '../dist/load-addon.js';

const addon = loadAddon();

test('host storage transport round-trips bytes without blocking JavaScript', async (context) => {
	const requests = [];
	const handle = addon.__testOpenHostTransport(
		(request) => {
			requests.push(Buffer.from(request));
			return Buffer.concat([Buffer.from('response:'), request]);
		},
		4,
		1024,
		1_000,
	);
	context.after(() => addon.__testCloseHostTransport(handle));

	const response = await roundTrip(handle, Buffer.from('read:key'));
	assert.deepStrictEqual(requests, [Buffer.from('read:key')]);
	assert.deepStrictEqual(response, Buffer.from('response:read:key'));
});

test('host storage transport rejects work beyond its operation budget', async (context) => {
	const handle = addon.__testOpenHostTransport(
		(request) => {
			Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30);
			return request;
		},
		1,
		1024,
		1_000,
	);
	context.after(() => addon.__testCloseHostTransport(handle));

	const first = roundTrip(handle, Buffer.from('first'));
	const second = roundTrip(handle, Buffer.from('second'));
	const results = await Promise.allSettled([first, second]);
	assert.strictEqual(results.filter(({ status }) => status === 'fulfilled').length, 1);
	assert.strictEqual(results.filter(({ status }) => status === 'rejected').length, 1);
	assert.match(results.find(({ status }) => status === 'rejected').reason.message, /at capacity/);
});

test('host storage transport reserves response capacity before dispatch', async (context) => {
	let calls = 0;
	const handle = addon.__testOpenHostTransport(
		(request) => {
			calls++;
			return request;
		},
		1,
		16,
		1_000,
	);
	context.after(() => addon.__testCloseHostTransport(handle));

	await assert.rejects(roundTrip(handle, Buffer.alloc(8), 9), /reservation exceeds/);
	assert.strictEqual(calls, 0);
});

test('a timed-out host request closes the transport and fences late completion', async (context) => {
	const handle = addon.__testOpenHostTransport(
		(request) => {
			Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 40);
			return request;
		},
		1,
		1024,
		5,
	);
	context.after(() => addon.__testCloseHostTransport(handle));

	await assert.rejects(roundTrip(handle, Buffer.from('slow')), /timed out/);
	await assert.rejects(roundTrip(handle, Buffer.from('later')), /closed|timed out/);
});

test('invalid callback responses fail the request without terminating the process', async (context) => {
	const handle = addon.__testOpenHostTransport(() => 'not a buffer', 1, 1024, 1_000);
	context.after(() => addon.__testCloseHostTransport(handle));

	await assert.rejects(roundTrip(handle, Buffer.from('read')), /must return a Buffer/);
});

test('KvDirectory and Tantivy operate through the host storage transport', async (context) => {
	const entries = new Map();
	const requests = [];
	const storage = {
		read(key) {
			requests.push('read');
			return entries.get(key.toString('hex'));
		},
		write(mutations, policy) {
			requests.push(`write:${policy}`);
			for (const mutation of mutations) {
				const key = mutation.key.toString('hex');
				if (mutation.type === 'put') entries.set(key, Buffer.from(mutation.value));
				else entries.delete(key);
			}
		},
		sync() {
			requests.push('sync');
		},
	};
	const handler = createHostStorageHandler(storage, {
		maxMutations: 1_024,
		maxReadResponseBytes: 32 * 1024 * 1024,
		maxErrorBytes: 64 * 1024,
	});
	const handle = addon.__testOpenHostTransport(handler, 32, 64 * 1024 * 1024, 5_000);
	context.after(() => addon.__testCloseHostTransport(handle));

	await verifyTantivy(handle);
	assert.ok(requests.includes('read'), 'read requests were issued');
	assert.ok(
		requests.some((request) => request.startsWith('write:')),
		'atomic write batches were issued',
	);
	assert.ok(requests.includes('sync'), 'directory sync requests were issued');
	assert.ok(entries.size > 0, 'Tantivy state remains in host storage for reopen');
});

test('host storage failures cross the native boundary without escaping JavaScript', async (context) => {
	const storage = {
		read() {
			throw new Error('injected host read failure');
		},
		write() {},
		sync() {},
	};
	const handler = createHostStorageHandler(storage, {
		maxMutations: 16,
		maxReadResponseBytes: 1_024,
		maxErrorBytes: 1_024,
	});
	const handle = addon.__testOpenHostTransport(handler, 4, 64 * 1024 * 1024, 1_000);
	context.after(() => addon.__testCloseHostTransport(handle));

	await assert.rejects(verifyTantivy(handle), /injected host read failure/);
});

function roundTrip(handle, request, responseBudget = 128) {
	return new Promise((resolve, reject) => {
		addon.__testHostRoundTrip(handle, request, responseBudget, (encoded) => {
			if (encoded[0] === 0) {
				resolve(encoded.subarray(1));
			} else {
				reject(new Error(encoded.subarray(1).toString()));
			}
		});
	});
}

function verifyTantivy(handle) {
	return new Promise((resolve, reject) => {
		addon.__testVerifyTantivyOnHostTransport(handle, (encoded) => {
			if (encoded[0] === 0) {
				resolve();
			} else {
				reject(new Error(encoded.subarray(1).toString()));
			}
		});
	});
}
