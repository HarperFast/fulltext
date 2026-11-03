import { writeSync } from 'node:fs';

export default async function* completionReporter(source) {
	let summary;
	let interrupted = false;
	for await (const event of source) {
		if (event.type === 'test:interrupted') interrupted = true;
		if (event.type === 'test:summary' && event.data.file === undefined) summary = event.data;
	}
	if (!interrupted && typeof summary?.success === 'boolean') {
		writeSync(3, JSON.stringify({ completed: true, success: summary.success }));
	}
}
