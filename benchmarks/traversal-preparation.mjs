import assert from 'node:assert';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { monitorEventLoopDelay, performance } from 'node:perf_hooks';
import { setImmediate } from 'node:timers/promises';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const directory = await mkdtemp(path.join(tmpdir(), 'fulltext-preparation-benchmark-'));
const query = {
	text: 'widget',
	candidateIds: Array.from({ length: 256 }, (_, i) => String(i).padStart(4, '0') + 'x'.repeat(4092)),
	filter: {
		field: 'category',
		comparator: 'in',
		value: Array.from({ length: 120 }, (_, i) => String(i).padStart(4, '0') + 'v'.repeat(59996)),
	},
};
let index;
try {
	index = await openNativeFullTextIndex({
		path: directory,
		indexId: 'preparation-benchmark',
		generation: 'one',
		analyzer: 'english@2',
		fields: [{ name: 'title', weight: 1 }],
		filterFields: [{ name: 'category', type: 'string' }],
		limits: {
			indexingThreads: 1,
			searchThreads: 2,
			writerMemoryBytes: 15000000,
			maxQueuedCommands: 8,
			maxQueuedBytes: 33554432,
			maxBatchBytes: 8388608,
		},
	});
	await index.applyMutationBatch({ upserts: [{ id: 'one', fields: { title: 'widget' } }] });
	await index.publish('one');
	for (let i = 0; i < 5; i++) await index.searchPage(query);
	const delay = monitorEventLoopDelay({ resolution: 1 });
	const submission = [],
		total = [];
	delay.enable();
	for (let i = 0; i < 30; i++) {
		await setImmediate();
		const started = performance.now();
		const pending = index.searchPage(query);
		submission.push(performance.now() - started);
		assert.equal((await pending).total, 0);
		total.push(performance.now() - started);
	}
	delay.disable();
	submission.sort((a, b) => a - b);
	total.sort((a, b) => a - b);
	console.log(
		JSON.stringify({
			label: process.argv[2] ?? 'current',
			node: process.version,
			calls: 30,
			submissionMilliseconds: { median: submission[15], p95: submission[28], max: submission[29] },
			totalMilliseconds: { median: total[15], p95: total[28] },
			eventLoopDelayMilliseconds: { p95: delay.percentile(95) / 1e6, max: delay.max / 1e6 },
			rss: process.memoryUsage().rss,
		}),
	);
} finally {
	if (index) await index.close({ mode: 'rollback' });
	await rm(directory, { recursive: true, force: true });
}
