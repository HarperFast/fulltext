import assert from 'node:assert';
import test from 'node:test';

import { createHostStorageHandler } from '../dist/host-storage.js';
import { loadAddon } from '../dist/load-addon.js';

const addon = loadAddon();
const readResponseBytes = 1024 * 1024;
const controlResponseBytes = 64 * 1024;

test('host storage transport round-trips bytes intact', async (context) => {
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

test('host storage transport backpressures work beyond its operation budget', async (context) => {
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
	assert.deepStrictEqual(await Promise.all([first, second]), [Buffer.from('first'), Buffer.from('second')]);
});

test('host storage transport backpressures work beyond its byte budget', async (context) => {
	const handle = addon.__testOpenHostTransport(
		(request) => {
			Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 30);
			return request;
		},
		2,
		136,
		1_000,
	);
	context.after(() => addon.__testCloseHostTransport(handle));

	const first = roundTrip(handle, Buffer.from('first'));
	const second = roundTrip(handle, Buffer.from('second'));
	assert.deepStrictEqual(await Promise.all([first, second]), [Buffer.from('first'), Buffer.from('second')]);
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

test('a host request can wait for a definitive result without a deadline', async (context) => {
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

	assert.deepStrictEqual(await roundTrip(handle, Buffer.from('slow'), 128, false), Buffer.from('slow'));
});

test('a read timeout fences only that request', async (context) => {
	let calls = 0;
	const handle = addon.__testOpenHostTransport(
		(request) => {
			if (calls++ === 0) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 40);
			return request;
		},
		1,
		1_024,
		5,
	);
	context.after(() => addon.__testCloseHostTransport(handle));

	await assert.rejects(roundTrip(handle, Buffer.from('slow')), /timed out/);
	assert.deepStrictEqual(await roundTrip(handle, Buffer.from('recovered')), Buffer.from('recovered'));
});

test('invalid callback responses fail the request without terminating the process', async (context) => {
	for (const invalid of ['not a buffer', {}]) {
		const handle = addon.__testOpenHostTransport(() => invalid, 1, 1024, 1_000);
		context.after(() => addon.__testCloseHostTransport(handle));
		await assert.rejects(roundTrip(handle, Buffer.from('read')), /must return a Buffer/);
	}
});

test('malformed host protocol responses fail the Tantivy operation', async (context) => {
	for (const invalid of [Buffer.from([2, 0]), Buffer.from([1])]) {
		const handle = addon.__testOpenHostTransport(() => invalid, 4, 64 * 1024 * 1024, 1_000);
		context.after(() => addon.__testCloseHostTransport(handle));
		await assert.rejects(verifyTantivy(handle), /unsupported protocol version|truncated/);
	}
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
		maxReadResponseBytes: readResponseBytes,
		maxControlResponseBytes: controlResponseBytes,
		maxErrorBytes: controlResponseBytes,
	});
	const handle = addon.__testOpenHostTransport(handler, 32, 40 * 1024 * 1024, 5_000);
	context.after(() => addon.__testCloseHostTransport(handle));

	await verifyTantivy(handle);
	assert.ok(requests.includes('read'), 'read requests were issued');
	assert.ok(requests.includes('write:wal'), 'ordinary objects use WAL writes');
	assert.ok(!requests.includes('write:wal-sync'), 'the host is not asked for an unsupported per-write sync primitive');
	assert.ok(requests.includes('sync'), 'metadata publication and directory sync use explicit durability barriers');
	assert.ok(entries.size > 0, 'Tantivy state remains in host storage for reopen');
});

test('a failed durability barrier does not pretend the preceding atomic write rolled back', async (context) => {
	const entries = new Map();
	let writes = 0;
	let syncs = 0;
	const storage = {
		read(key) {
			return entries.get(key.toString('hex'));
		},
		write(mutations, policy) {
			assert.strictEqual(policy, 'wal');
			writes++;
			for (const mutation of mutations) {
				const key = mutation.key.toString('hex');
				if (mutation.type === 'put') entries.set(key, Buffer.from(mutation.value));
				else entries.delete(key);
			}
		},
		sync() {
			if (++syncs === 1) throw new Error('injected durability failure');
		},
	};
	const handler = createHostStorageHandler(storage, {
		maxMutations: 1_024,
		maxReadResponseBytes: readResponseBytes,
		maxControlResponseBytes: controlResponseBytes,
		maxErrorBytes: controlResponseBytes,
	});
	const handle = addon.__testOpenHostTransport(handler, 32, 40 * 1024 * 1024, 5_000);
	context.after(() => addon.__testCloseHostTransport(handle));

	await assert.rejects(verifyTantivy(handle), /injected durability failure/);
	assert.ok(writes > 0, 'the atomic WAL write completed before its durability barrier failed');
	assert.ok(entries.size > 0, 'the transport does not report that applied writes were rolled back');
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
		maxControlResponseBytes: 1_024,
		maxErrorBytes: 1_024,
	});
	const handle = addon.__testOpenHostTransport(handler, 4, 64 * 1024 * 1024, 1_000);
	context.after(() => addon.__testCloseHostTransport(handle));

	await assert.rejects(verifyTantivy(handle), /injected host read failure/);
});

test('host mutation failures return a definitive result without timing out', async (context) => {
	const storage = {
		read() {},
		write() {
			throw new Error('injected host write failure');
		},
		sync() {},
	};
	const handler = createHostStorageHandler(storage, {
		maxMutations: 16,
		maxReadResponseBytes: 1_024,
		maxControlResponseBytes: 1_024,
		maxErrorBytes: 1_024,
	});
	const handle = addon.__testOpenHostTransport(handler, 4, 64 * 1024 * 1024, 5);
	context.after(() => addon.__testCloseHostTransport(handle));

	await assert.rejects(verifyTantivy(handle), /injected host write failure/);
});

test('host storage handler preserves no-WAL policy and rejects malformed frames', () => {
	const policies = [];
	const handler = createHostStorageHandler(
		{
			read() {},
			write(_mutations, policy) {
				policies.push(policy);
			},
			sync() {},
		},
		{
			maxMutations: 4,
			maxReadResponseBytes: 64,
			maxControlResponseBytes: 64,
			maxErrorBytes: 64,
		},
	);

	assert.deepStrictEqual(handler(Buffer.from([1, 2, 3, 1, 0, 0, 0, 2, 1, 0, 0, 0, 97])), Buffer.from([1, 0]));
	assert.deepStrictEqual(policies, ['no-wal']);
	assert.match(decodeHandlerError(handler(Buffer.from([1, 2, 2, 0, 0, 0, 0]))), /unknown.*write policy/);
	assert.match(decodeHandlerError(handler(Buffer.from([2, 3]))), /unsupported.*protocol version/);
	assert.match(decodeHandlerError(handler(Buffer.from([1, 1, 4, 0, 0]))), /truncated/);
});

function roundTrip(handle, request, responseBudget = 128, useTimeout = true) {
	return new Promise((resolve, reject) => {
		addon.__testHostRoundTrip(handle, request, responseBudget, useTimeout, (encoded) => {
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
		addon.__testVerifyTantivyOnHostTransport(handle, readResponseBytes, controlResponseBytes, (encoded) => {
			if (encoded[0] === 0) {
				resolve();
			} else {
				reject(new Error(encoded.subarray(1).toString()));
			}
		});
	});
}

function decodeHandlerError(response) {
	assert.strictEqual(response[0], 1);
	assert.strictEqual(response[1], 1);
	const length = response.readUInt32LE(2);
	return response.subarray(6, 6 + length).toString();
}
