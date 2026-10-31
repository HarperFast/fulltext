import assert from 'node:assert';
import { performance } from 'node:perf_hooks';
let milliseconds = 0;
let runtimeConfiguration;
const handles = [];
Object.defineProperty(performance, 'now', { value: () => milliseconds });
export function configureNativeFullTextRuntime(options) {
	runtimeConfiguration = options;
}
export async function openNativeFullTextIndex(options) {
	const counts = { queries: 0, scrapes: 0, limits: options.limits };
	handles.push(counts);
	return {
		configureTelemetry(options) {
			counts.configuration = options;
		},
		async applyMutationBatch() {},
		async publish() {},
		async close() {
			return {};
		},
		async search() {
			counts.queries++;
			return { total: 1 };
		},
		telemetry() {
			assert.strictEqual(counts.queries, 100);
			counts.scrapes++;
			milliseconds += 2;
			return {
				available: true,
				native: { operations: { search: { succeeded: 100n } } },
			};
		},
	};
}
export async function runtimeInfo() {
	return { runtimeConfiguration, handles };
}
