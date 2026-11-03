import { spawnSync } from 'node:child_process';

if (!process.env.npm_execpath) {
	throw new Error('Run the Node test suite with npm run test:node');
}

const steps = [
	{ kind: 'build', arguments: [process.env.npm_execpath, 'run', 'build:test-native'] },
	{ kind: 'build', arguments: [process.env.npm_execpath, 'run', 'build:typescript'] },
	{ kind: 'test', arguments: ['scripts/run-node-tests.mjs', 'test'] },
	{ kind: 'build', arguments: [process.env.npm_execpath, 'run', 'build:native'] },
	{ kind: 'test', arguments: ['scripts/run-node-tests.mjs', 'release'] },
];

for (const step of steps) {
	const result = spawnSync(process.execPath, step.arguments, { stdio: 'inherit' });
	if (result.error) throw result.error;
	if (result.signal) {
		console.error(`Node suite ${step.kind} process terminated by ${result.signal}`);
		process.exitCode = result.status ?? 1;
		break;
	}
	if (step.kind === 'test') {
		console.log(
			`Node ${step.arguments.at(-1) === 'test' ? 'debug' : 'production'} test phase: ${result.status === 0 ? 'passed' : 'failed'} (status ${result.status ?? 'unknown'})`,
		);
	}
	if (result.status !== 0) {
		if (!process.exitCode || result.status !== 1) process.exitCode = result.status ?? 2;
		if (step.kind === 'build' || result.status !== 1) {
			console.error(`Node suite stopping after ${step.arguments.at(-1)} failed (status ${result.status ?? 'unknown'})`);
			break;
		}
		if (step.arguments.at(-1) === 'test')
			console.log('Continuing to production qualification after debug test failures');
	}
}

console.log(`Node suite: ${process.exitCode ? 'failed' : 'passed'} (status ${process.exitCode ?? 0})`);
