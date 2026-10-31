import { performance } from 'node:perf_hooks';

let milliseconds = 0;
Object.defineProperty(performance, 'now', { value: () => milliseconds });

export async function openNativeFullTextIndex() {
	return {
		async applyMutationBatch() {},
		async publish() {
			milliseconds++;
		},
		async close() {
			return {};
		},
		async search() {
			milliseconds += 5;
			let observed = false;
			return {
				total: 8,
				totalRelation: 'exact',
				get hits() {
					if (!observed) {
						milliseconds += 100;
						observed = true;
					}
					return Array.from({ length: 8 }, (_, id) => ({ id: String(id), score: 1 }));
				},
			};
		},
	};
}
export async function runtimeInfo() {
	return { fixture: 'deterministic-api-and-oracle-clock' };
}
