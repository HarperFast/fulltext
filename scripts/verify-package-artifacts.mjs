import { readdirSync } from 'node:fs';

import { platformTriple } from '../dist/load-addon.js';

export function verifyPackageArtifacts(entries, expectedArtifact) {
	const artifacts = entries.filter((entry) => /^fulltext\..+\.node$/.test(entry));
	if (artifacts.length !== 1 || artifacts[0] !== expectedArtifact) {
		throw new Error(
			`Expected only ${expectedArtifact} before packing; found ${artifacts.length === 0 ? 'none' : artifacts.join(', ')}`,
		);
	}
}

verifyPackageArtifacts(readdirSync(new URL('../', import.meta.url)), `fulltext.${platformTriple()}.node`);
