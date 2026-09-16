import assert from 'node:assert';
import { readFileSync } from 'node:fs';
import test from 'node:test';

test('keeps Rust and TypeScript error codes in parity', () => {
	const rust = readFileSync(new URL('../src/error.rs', import.meta.url), 'utf8');
	const typescript = readFileSync(new URL('../ts/errors.ts', import.meta.url), 'utf8');
	const rustCodes = codesIn(constantBlock(rust, 'pub const ERROR_CODES', '];'));
	const typescriptCodes = codesIn(constantBlock(typescript, 'const errorCodes', '] as const'));
	assert.deepStrictEqual(rustCodes, typescriptCodes);
});

function codesIn(source) {
	return [...source.matchAll(/["'](E_[A-Z_]+)["']/g)].map((match) => match[1]).sort();
}

test('keeps the checkpoint payload byte limit aligned across Rust and TypeScript', () => {
	const rust = readFileSync(new URL('../src/engine.rs', import.meta.url), 'utf8');
	const typescript = readFileSync(new URL('../ts/native.ts', import.meta.url), 'utf8');
	const rustLimit = /MAX_COMMIT_PAYLOAD_BYTES: usize = ([\d\s*]+);/.exec(rust)?.[1];
	const tsLimit = /maxCommitPayloadBytes = ([\d\s*]+);/.exec(typescript)?.[1];
	assert(rustLimit && tsLimit);
	const product = (expression) => expression.split('*').reduce((value, factor) => value * Number(factor.trim()), 1);
	assert.strictEqual(product(rustLimit), product(tsLimit));
});

test('keeps mutation batch size constants aligned across Rust and TypeScript', () => {
	const rust = readFileSync(new URL('../src/protocol.rs', import.meta.url), 'utf8');
	const typescript = readFileSync(new URL('../ts/codec.ts', import.meta.url), 'utf8');
	const rustHeader = Number(/MUTATION_BATCH_HEADER_BYTES: usize = (\d+);/.exec(rust)?.[1]);
	const rustMinimumIncrement = Number(
		/MIN_MUTATION_BATCH_BYTES: usize = MUTATION_BATCH_HEADER_BYTES \+ (\d+);/.exec(rust)?.[1],
	);
	const typescriptHeader = Number(/mutationBatchHeaderBytes = (\d+);/.exec(typescript)?.[1]);
	const typescriptMinimumIncrement = Number(
		/minimumMutationBatchBytes = mutationBatchHeaderBytes \+ (\d+);/.exec(typescript)?.[1],
	);
	assert.strictEqual(rustHeader, typescriptHeader);
	assert.strictEqual(rustMinimumIncrement, typescriptMinimumIncrement);
});

function constantBlock(source, marker, terminator) {
	const start = source.indexOf(marker);
	assert.notStrictEqual(start, -1);
	const end = source.indexOf(terminator, start);
	assert.notStrictEqual(end, -1);
	return source.slice(start, end);
}
