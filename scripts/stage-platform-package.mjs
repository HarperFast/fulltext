import { parseArgs } from 'node:util';

import { stagePlatformPackage } from './platform-packages.mjs';

const { values } = parseArgs({
	options: {
		artifact: { type: 'string' },
		output: { type: 'string' },
		root: { type: 'string', default: 'package.json' },
		target: { type: 'string' },
	},
});

for (const required of ['artifact', 'output', 'target']) {
	if (!values[required]) {
		throw new Error(`--${required} is required`);
	}
}

const manifest = stagePlatformPackage({
	rootManifestPath: values.root,
	triple: values.target,
	artifactPath: values.artifact,
	outputDirectory: values.output,
});
console.log(`${manifest.name}@${manifest.version}`);
