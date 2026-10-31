import assert from 'node:assert';
import { performance } from 'node:perf_hooks';
import { writeFileSync } from 'node:fs';
import path from 'node:path';

let milliseconds = 0;
let searchCalls = 0;
let pending = 0;
const validatedCalls = [];
const configurations = [];
const failure = process.env.FULLTEXT_PAIRED_TEST_FAILURE;
const marker = process.env.FULLTEXT_PAIRED_TEST_MARKER;
let opens = 0;
let closeAttempts = 0;
let fixtureRoot;
const closeModes = [];
const record = () => {
	if (marker) writeFileSync(marker, JSON.stringify({ root: fixtureRoot, closeAttempts, closeModes }));
};
Object.defineProperty(performance, 'now', { value: () => milliseconds });

export async function openNativeFullTextIndex(options) {
	opens++;
	fixtureRoot = path.dirname(options.path);
	record();
	if (failure === 'partial-open' && opens === 2) throw new Error('E_PAIRED_OPEN');
	let localSearchCalls = 0;
	let publications = 0;
	return {
		configureTelemetry(options) {
			configurations.push(options);
		},
		telemetry() {
			return {
				available: true,
				native: {
					operations: {
						search: {
							succeeded: BigInt(localSearchCalls),
							attempted: BigInt(localSearchCalls),
						},
						expensiveSearch: { succeeded: 0n, attempted: 0n },
					},
				},
			};
		},
		async applyMutationBatch() {},
		async publish() {
			if (failure === 'dirty-publish' && publications++ > 0) throw new Error('E_PAIRED_PUBLISH');
			milliseconds++;
		},
		async close(options) {
			closeAttempts++;
			closeModes.push(options?.mode);
			record();
			if (failure === 'dirty-publish' && options?.mode !== 'rollback') throw new Error('E_DIRTY_CLOSE');
			if (failure === 'primary-and-close') throw new Error(`E_PAIRED_CLOSE_${closeAttempts}`);
			if (failure === 'close' && closeAttempts === 1) return { cleanupError: new Error('E_PAIRED_CLOSE') };
			return {};
		},
		async search() {
			const call = ++searchCalls;
			if ((failure === 'primary' || failure === 'primary-and-close') && call === 101)
				throw new Error('E_PAIRED_PRIMARY');
			localSearchCalls++;
			pending++;
			milliseconds += 5;
			await Promise.resolve();
			pending--;
			let observed = false;
			return {
				total: 8,
				totalRelation: 'exact',
				get hits() {
					assert.strictEqual(pending, 0, 'Validation must wait for every query in the window');
					if (!observed) {
						milliseconds += 100;
						observed = true;
						validatedCalls.push(call);
					}
					return Array.from({ length: 8 }, (_, id) => ({
						id: String(id),
						score: 1,
					}));
				},
			};
		},
	};
}
export async function runtimeInfo() {
	return {
		fixture: 'deterministic-api-and-oracle-clock',
		searchCalls,
		validatedCalls,
		configurations,
	};
}
