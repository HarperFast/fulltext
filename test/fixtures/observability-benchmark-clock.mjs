import assert from 'node:assert';
import { performance } from 'node:perf_hooks';

let milliseconds = 0;
let searchCalls = 0;
let pending = 0;
const validatedCalls = [];
const configurations = [];
Object.defineProperty(performance, 'now', { value: () => milliseconds });

export async function openNativeFullTextIndex() {
	let localSearchCalls = 0;
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
			milliseconds++;
		},
		async close() {
			return {};
		},
		async search() {
			const call = ++searchCalls;
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
