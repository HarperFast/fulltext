import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';

import { platformPackageName, supportedPlatformPackages, validateRootManifest } from './platform-packages.mjs';

export function validateReleaseManifests(manifests) {
	const root = manifests.find((manifest) => manifest.name === '@harperfast/fulltext');
	if (!root) {
		throw new Error('Missing @harperfast/fulltext root package');
	}
	validateRootManifest(root);
	const expected = new Map(
		supportedPlatformPackages.map((platform) => [platformPackageName(root.name, platform.triple), platform]),
	);
	for (const manifest of manifests) {
		if (manifest === root) {
			continue;
		}
		const platform = expected.get(manifest.name);
		if (!platform) {
			throw new Error(`Unexpected release package ${manifest.name}`);
		}
		if (manifest.version !== root.version) {
			throw new Error(`${manifest.name} is ${manifest.version}, expected ${root.version}`);
		}
		if (
			manifest.os?.[0] !== platform.os ||
			manifest.cpu?.[0] !== platform.cpu ||
			(platform.libc ? manifest.libc?.[0] !== platform.libc : manifest.libc !== undefined)
		) {
			throw new Error(`${manifest.name} has incorrect platform constraints`);
		}
		expected.delete(manifest.name);
	}
	if (expected.size > 0) {
		throw new Error(`Missing release packages: ${[...expected.keys()].join(', ')}`);
	}
	return root;
}

export function integrityForTarball(tarballPath) {
	return `sha512-${createHash('sha512').update(readFileSync(tarballPath)).digest('base64')}`;
}

export function readTarballManifest(tarballPath) {
	return JSON.parse(execFileSync('tar', ['-xOf', tarballPath, 'package/package.json'], { encoding: 'utf8' }));
}

export function parsePublishedIntegrity(output) {
	const trimmed = output.trim();
	if (!trimmed) {
		return undefined;
	}
	const integrity = JSON.parse(trimmed);
	if (integrity === null) {
		return undefined;
	}
	if (typeof integrity !== 'string') {
		throw new Error(`npm returned an invalid dist.integrity value: ${trimmed}`);
	}
	return integrity;
}

export function isUnpublishedVersionError(error) {
	const detail = `${error.stderr ?? ''}\n${error.stdout ?? ''}`;
	return /E404|ETARGET|404 Not Found|No matching version found/i.test(detail);
}

export function tarballsIn(directory) {
	const tarballs = [];
	for (const entry of readdirSync(directory, { withFileTypes: true })) {
		const entryPath = path.resolve(directory, entry.name);
		if (entry.isDirectory()) {
			tarballs.push(...tarballsIn(entryPath));
		} else if (entry.isFile() && entry.name.endsWith('.tgz')) {
			tarballs.push(entryPath);
		}
	}
	return tarballs;
}

function publishedIntegrity(name, version) {
	try {
		const output = execFileSync('npm', ['view', `${name}@${version}`, 'dist.integrity', '--json'], {
			encoding: 'utf8',
			stdio: ['ignore', 'pipe', 'pipe'],
		});
		return parsePublishedIntegrity(output);
	} catch (error) {
		if (isUnpublishedVersionError(error)) {
			return undefined;
		}
		throw error;
	}
}

function publishTarball({ tarballPath, manifest, tag }) {
	const localIntegrity = integrityForTarball(tarballPath);
	const remoteIntegrity = publishedIntegrity(manifest.name, manifest.version);
	if (remoteIntegrity !== undefined) {
		if (remoteIntegrity !== localIntegrity) {
			throw new Error(
				`${manifest.name}@${manifest.version} is already published with different contents (${remoteIntegrity})`,
			);
		}
		console.log(`Skipping identical ${manifest.name}@${manifest.version}`);
		execFileSync('npm', ['dist-tag', 'add', `${manifest.name}@${manifest.version}`, tag], { stdio: 'inherit' });
		return;
	}
	execFileSync('npm', ['publish', tarballPath, '--access', 'public', '--tag', tag, '--provenance'], {
		stdio: 'inherit',
	});
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
	const { values } = parseArgs({
		options: { directory: { type: 'string' }, tag: { type: 'string', default: 'latest' } },
	});
	if (!values.directory) {
		throw new Error('--directory is required');
	}
	if (!process.env.NODE_AUTH_TOKEN) {
		throw new Error('NODE_AUTH_TOKEN is required');
	}
	const packages = tarballsIn(values.directory).map((tarballPath) => ({
		tarballPath,
		manifest: readTarballManifest(tarballPath),
	}));
	const root = validateReleaseManifests(packages.map(({ manifest }) => manifest));
	for (const item of packages.filter(({ manifest }) => manifest !== root)) {
		publishTarball({ ...item, tag: values.tag });
	}
	const rootPackage = packages.find(({ manifest }) => manifest === root);
	publishTarball({ ...rootPackage, tag: values.tag });
}
