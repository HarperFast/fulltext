import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

import { validateRootManifest } from './platform-packages.mjs';

export function verifyPackageArtifacts(manifest) {
	validateRootManifest(manifest);
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
	verifyPackageArtifacts(JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8')));
}
