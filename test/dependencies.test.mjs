import assert from 'node:assert';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const cargoManifest = readFileSync(new URL('../Cargo.toml', import.meta.url), 'utf8');
const cargoLock = readFileSync(new URL('../Cargo.lock', import.meta.url), 'utf8');
const packageManifest = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
const dependencyLedger = readFileSync(new URL('../dependencies.md', import.meta.url), 'utf8');

test('every direct dependency is documented', () => {
	for (const dependency of cargoDependencies(cargoManifest)) {
		assert(dependencyLedger.includes(`\`${dependency}\``), `${dependency} is absent from dependencies.md`);
	}
	for (const section of ['dependencies', 'optionalDependencies', 'peerDependencies', 'devDependencies']) {
		for (const dependency of Object.keys(packageManifest[section] ?? {})) {
			assert(dependencyLedger.includes(`\`${dependency}\``), `${dependency} is absent from dependencies.md`);
		}
	}
});

test('the native dependency graph does not declare RocksDB', () => {
	assert(!/rocksdb/i.test(cargoManifest));
	assert(!/^name\s*=\s*"[^"]*rocksdb[^"]*"/im.test(cargoLock));
});

function cargoDependencies(manifest) {
	const dependencySections = new Set(['dependencies', 'build-dependencies', 'dev-dependencies']);
	let currentSection;
	const dependencies = new Set();
	for (const line of manifest.split('\n')) {
		const section = /^\[([^\]]+)]$/.exec(line.trim());
		if (section) {
			currentSection = section[1];
			continue;
		}
		const isDependencySection =
			dependencySections.has(currentSection) ||
			currentSection?.endsWith('.dependencies') ||
			currentSection?.endsWith('.build-dependencies') ||
			currentSection?.endsWith('.dev-dependencies');
		if (!isDependencySection) {
			continue;
		}
		const dependency = /^([A-Za-z0-9_-]+)\s*=/.exec(line);
		if (dependency) {
			dependencies.add(dependency[1]);
		}
	}
	return dependencies;
}
