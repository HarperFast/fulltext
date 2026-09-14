import assert from 'node:assert';
import test from 'node:test';
import { PublicationState } from '../dist/publication.js';

test('out-of-order successful callbacks cannot move checkpoint readback backward', () => {
	const state = new PublicationState('opened');
	const first = state.begin();
	const second = state.begin();
	state.succeed(second, 'newer');
	state.succeed(first, 'older');
	assert.strictEqual(state.committedPayload, 'newer');
});

test('an older success cannot mask a newer ambiguous outcome', () => {
	const state = new PublicationState();
	assert.strictEqual(state.committedPayload, undefined);
	const first = state.begin();
	const second = state.begin();
	state.fail(second);
	state.succeed(first, 'older');
	assert.throws(
		() => state.committedPayload,
		(error) => error.code === 'E_POISONED',
	);
	state.fail(first);
	assert.throws(
		() => state.committedPayload,
		(error) => error.code === 'E_POISONED',
	);
});

test('a newer known success supersedes an older uncertain completion', () => {
	const state = new PublicationState();
	const first = state.begin();
	const second = state.begin();
	state.succeed(second, '');
	state.fail(first);
	assert.strictEqual(state.committedPayload, '');
});
