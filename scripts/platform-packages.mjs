import { copyFileSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const supportedPlatformPackages = Object.freeze([
	Object.freeze({ triple: 'darwin-arm64', os: 'darwin', cpu: 'arm64' }),
	Object.freeze({ triple: 'linux-arm64-gnu', os: 'linux', cpu: 'arm64', libc: 'glibc' }),
	Object.freeze({ triple: 'linux-x64-gnu', os: 'linux', cpu: 'x64', libc: 'glibc' }),
	Object.freeze({ triple: 'win32-x64-msvc', os: 'win32', cpu: 'x64' }),
]);

export function platformPackageName(rootName, triple) {
	return `${rootName}-${triple}`;
}

export function validateRootManifest(manifest) {
	if (manifest.version === '0.0.0') {
		throw new Error('The release package version must not be 0.0.0');
	}
	if (manifest.files?.some((entry) => /^fulltext\.\*.*\.node$/.test(entry) || /^fulltext\..+\.node$/.test(entry))) {
		throw new Error('The root package must not include a native artifact');
	}
	for (const platform of supportedPlatformPackages) {
		const name = platformPackageName(manifest.name, platform.triple);
		if (manifest.optionalDependencies?.[name] !== manifest.version) {
			throw new Error(`${name} must be pinned to ${manifest.version}`);
		}
	}
	const expected = new Set(
		supportedPlatformPackages.map((platform) => platformPackageName(manifest.name, platform.triple)),
	);
	for (const name of Object.keys(manifest.optionalDependencies ?? {})) {
		if (!expected.delete(name)) {
			throw new Error(`Unexpected optional dependency ${name}`);
		}
	}
	if (expected.size > 0) {
		throw new Error(`Missing optional dependencies: ${[...expected].join(', ')}`);
	}
}

export function stagePlatformPackage({ rootManifestPath, triple, artifactPath, outputDirectory }) {
	const manifest = JSON.parse(readFileSync(rootManifestPath, 'utf8'));
	validateRootManifest(manifest);
	const platform = supportedPlatformPackages.find((candidate) => candidate.triple === triple);
	if (!platform) {
		throw new Error(`Unsupported platform package ${triple}`);
	}
	const artifactName = `fulltext.${triple}.node`;
	const resolvedArtifactPath = artifactPath instanceof URL ? fileURLToPath(artifactPath) : artifactPath;
	if (path.basename(resolvedArtifactPath) !== artifactName) {
		throw new Error(`Expected artifact ${artifactName}, received ${path.basename(resolvedArtifactPath)}`);
	}
	const packageManifest = {
		name: platformPackageName(manifest.name, triple),
		version: manifest.version,
		description: `${triple} native binding for ${manifest.name}`,
		license: manifest.license,
		repository: manifest.repository,
		main: `./${artifactName}`,
		exports: { '.': `./${artifactName}` },
		files: [artifactName],
		preferUnplugged: true,
		engines: manifest.engines,
		os: [platform.os],
		cpu: [platform.cpu],
		...(platform.libc ? { libc: [platform.libc] } : {}),
	};
	rmSync(outputDirectory, { recursive: true, force: true });
	mkdirSync(outputDirectory, { recursive: true });
	copyFileSync(resolvedArtifactPath, path.join(outputDirectory, artifactName));
	writeFileSync(path.join(outputDirectory, 'package.json'), `${JSON.stringify(packageManifest, null, '\t')}\n`);
	writeFileSync(
		path.join(outputDirectory, 'README.md'),
		`# ${packageManifest.name}\n\nNative binding for [${manifest.name}](https://www.npmjs.com/package/${manifest.name}).\n`,
	);
	return packageManifest;
}
