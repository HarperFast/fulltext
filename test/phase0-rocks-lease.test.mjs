import assert from 'node:assert';
import { randomBytes } from 'node:crypto';
import { rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { pathToFileURL } from 'node:url';

import { loadAddon } from '../dist/load-addon.js';

const rocksdbRoot = process.env.FULLTEXT_PHASE0_ROCKSDB_JS_ROOT;

test(
	'runs Tantivy through the rocksdb-js storage lease and revokes it on close',
	{ skip: rocksdbRoot ? false : 'set FULLTEXT_PHASE0_ROCKSDB_JS_ROOT to a feature-enabled rocksdb-js checkout' },
	async () => {
		const { RocksDatabase } = await import(pathToFileURL(path.join(rocksdbRoot, 'src/index.ts')).href);
		const dbPath = path.join(tmpdir(), `fulltext-phase0-${randomBytes(8).toString('hex')}`);
		const database = new RocksDatabase(dbPath, { encoding: false, name: 'fulltext-phase0' }).open();
		const addon = loadAddon();
		let leaseId;

		try {
			const external = database.store.db.__nativeStorageLease();
			leaseId = addon.__phase0OpenStorageLease(external);
			const [databaseIncarnation, columnIncarnation, provider, state] = addon.__phase0StorageLeaseInfo(leaseId);
			assert.match(databaseIncarnation, /^[1-9]\d*$/);
			assert.match(columnIncarnation, /^[1-9]\d*$/);
			assert.strictEqual(provider, 'rocksdb-js/2.8.0-phase0');
			assert.strictEqual(state, 'active');

			addon.__phase0StoragePut(leaseId, Buffer.from('test/a'), Buffer.from('alpha'), true);
			addon.__phase0StoragePut(leaseId, Buffer.from('test/b'), Buffer.from('beta'), true);
			assert.deepStrictEqual(addon.__phase0StorageGet(leaseId, Buffer.from('test/a')), Buffer.from('alpha'));
			assert.deepStrictEqual(addon.__phase0StorageScan(leaseId, Buffer.from('test/'), Buffer.alloc(0)), [
				Buffer.from('test/a'),
				Buffer.from('alpha'),
				Buffer.from('test/b'),
				Buffer.from('beta'),
			]);
			addon.__phase0StorageDelete(leaseId, Buffer.from('test/a'), true);
			assert.strictEqual(addon.__phase0StorageGet(leaseId, Buffer.from('test/a')), null);

			addon.__phase0VerifyTantivyOnStorageLease(leaseId);
			const [gets, scans, batches, , returnedBytes, copiedBytes, liveBuffers, providerErrors] =
				addon.__phase0StorageStats(leaseId);
			assert(BigInt(gets) > 0n);
			assert(BigInt(scans) > 0n);
			assert(BigInt(batches) > 0n);
			assert(BigInt(returnedBytes) > 0n);
			assert(BigInt(copiedBytes) > 0n);
			assert.strictEqual(liveBuffers, '0');
			assert.strictEqual(providerErrors, '0');

			database.close();
			assert.strictEqual(addon.__phase0StorageLeaseState(leaseId), 'revoked');
			assert.throws(
				() => addon.__phase0StorageGet(leaseId, Buffer.from('test/b')),
				(error) => error?.code === 'E_STORAGE' && /closed|revoked/.test(error.message),
			);
		} finally {
			if (leaseId !== undefined) {
				addon.__phase0CloseStorageLease(leaseId);
			}
			database.close();
			rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 100 });
		}
	},
);

test(
	'rejects VerificationTable overlap and revokes a dropped column-family incarnation',
	{ skip: rocksdbRoot ? false : 'set FULLTEXT_PHASE0_ROCKSDB_JS_ROOT to a feature-enabled rocksdb-js checkout' },
	async () => {
		const { RocksDatabase } = await import(pathToFileURL(path.join(rocksdbRoot, 'src/index.ts')).href);
		const dbPath = path.join(tmpdir(), `fulltext-phase0-lifecycle-${randomBytes(8).toString('hex')}`);
		const addon = loadAddon();
		let database;
		let verificationDatabase;
		let replacement;
		let oldLeaseId;
		let newLeaseId;

		try {
			verificationDatabase = new RocksDatabase(dbPath, {
				encoding: false,
				name: 'fulltext-phase0',
				verificationTable: true,
			}).open();
			assert.throws(() => verificationDatabase.store.db.__nativeStorageLease(), /uses the verification table/);
			verificationDatabase.close();

			database = new RocksDatabase(dbPath, { encoding: false, name: 'fulltext-phase0' }).open();
			oldLeaseId = addon.__phase0OpenStorageLease(database.store.db.__nativeStorageLease());
			const oldIncarnation = addon.__phase0StorageLeaseInfo(oldLeaseId)[1];
			verificationDatabase = new RocksDatabase(dbPath, {
				encoding: false,
				name: 'fulltext-phase0',
				verificationTable: true,
			});
			assert.throws(() => verificationDatabase.open(), /active native storage lease/);

			database.dropSync();
			assert.strictEqual(addon.__phase0StorageLeaseState(oldLeaseId), 'revoked');
			assert.throws(
				() => addon.__phase0StorageGet(oldLeaseId, Buffer.from('missing')),
				(error) => error?.code === 'E_STORAGE' && /stale/.test(error.message),
			);

			replacement = new RocksDatabase(dbPath, { encoding: false, name: 'fulltext-phase0' }).open();
			newLeaseId = addon.__phase0OpenStorageLease(replacement.store.db.__nativeStorageLease());
			const newIncarnation = addon.__phase0StorageLeaseInfo(newLeaseId)[1];
			assert.notStrictEqual(newIncarnation, oldIncarnation);
		} finally {
			if (oldLeaseId !== undefined) addon.__phase0CloseStorageLease(oldLeaseId);
			if (newLeaseId !== undefined) addon.__phase0CloseStorageLease(newLeaseId);
			verificationDatabase?.close();
			replacement?.close();
			database?.close();
			rmSync(dbPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 100 });
		}
	},
);

test(
	'rejects storage leases whose database mode cannot provide bounded writes',
	{ skip: rocksdbRoot ? false : 'set FULLTEXT_PHASE0_ROCKSDB_JS_ROOT to a feature-enabled rocksdb-js checkout' },
	async () => {
		const { RocksDatabase } = await import(pathToFileURL(path.join(rocksdbRoot, 'src/index.ts')).href);
		const pessimisticPath = path.join(tmpdir(), `fulltext-phase0-pessimistic-${randomBytes(8).toString('hex')}`);
		const readOnlyPath = path.join(tmpdir(), `fulltext-phase0-readonly-${randomBytes(8).toString('hex')}`);
		let pessimistic;
		let writable;
		let readOnly;

		try {
			pessimistic = new RocksDatabase(pessimisticPath, {
				encoding: false,
				name: 'fulltext-phase0',
				pessimistic: true,
			}).open();
			assert.throws(() => pessimistic.store.db.__nativeStorageLease(), /do not support pessimistic/);

			writable = new RocksDatabase(readOnlyPath, { encoding: false, name: 'fulltext-phase0' }).open();
			writable.close();
			readOnly = new RocksDatabase(readOnlyPath, {
				encoding: false,
				name: 'fulltext-phase0',
				readOnly: true,
			}).open();
			assert.throws(() => readOnly.store.db.__nativeStorageLease(), /require a writable database/);
		} finally {
			pessimistic?.close();
			writable?.close();
			readOnly?.close();
			rmSync(pessimisticPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 100 });
			rmSync(readOnlyPath, { force: true, recursive: true, maxRetries: 3, retryDelay: 100 });
		}
	},
);
