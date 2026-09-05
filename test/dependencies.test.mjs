import assert from 'node:assert';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const cargoManifest = readFileSync(new URL('../Cargo.toml', import.meta.url), 'utf8');
const packageManifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const dependencyLedger = readFileSync(new URL('../dependencies.md', import.meta.url), 'utf8');

test('every direct dependency is documented', () => {
	for (const dependency of ['tantivy', 'napi', 'napi-derive', 'napi-build']) {
		assert.match(cargoManifest, new RegExp(`^${dependency.replace('-', '\\-')}\\s*=`, 'm'));
		assert(dependencyLedger.includes(`\`${dependency}\``), `${dependency} is absent from dependencies.md`);
	}
	for (const dependency of Object.keys(packageManifest.devDependencies)) {
		assert(dependencyLedger.includes(`\`${dependency}\``), `${dependency} is absent from dependencies.md`);
	}
});

test('the native dependency graph does not declare RocksDB', () => {
	assert(!/rocksdb/i.test(cargoManifest));
});
