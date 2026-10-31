import assert from 'node:assert';
import { performance } from 'node:perf_hooks';
import { writeFileSync } from 'node:fs';
import path from 'node:path';
let milliseconds = 0;
let runtimeConfiguration;
const handles = [];
const failure = process.env.FULLTEXT_SCRAPE_TEST_FAILURE;
const marker = process.env.FULLTEXT_SCRAPE_TEST_MARKER;
let fixtureRoot;
let closeAttempts = 0;
const record = () => {
	if (marker) writeFileSync(marker, JSON.stringify({ root: fixtureRoot, closeAttempts }));
};
Object.defineProperty(performance, 'now', { value: () => milliseconds });
export function configureNativeFullTextRuntime(options) {
	runtimeConfiguration = options;
}
export async function openNativeFullTextIndex(options) {
	const counts = { queries: 0, scrapes: 0, limits: options.limits };
	handles.push(counts);
	fixtureRoot = path.dirname(options.path);
	record();
	return {
		configureTelemetry(options) {
			counts.configuration = options;
		},
		async applyMutationBatch() {},
		async publish() {},
		async close() {
			closeAttempts++;
			record();
			if (closeAttempts === 1 && failure === 'primary-and-close') throw new Error('E_SCRAPE_CLOSE');
			if (closeAttempts === 1 && failure === 'close') return { cleanupError: new Error('E_SCRAPE_CLOSE') };
			return {};
		},
		async search() {
			counts.queries++;
			return { total: 1 };
		},
		telemetry() {
			if (failure === 'primary-and-close') throw new Error('E_SCRAPE_PRIMARY');
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
