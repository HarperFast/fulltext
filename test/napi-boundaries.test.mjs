import assert from 'node:assert';
import { readdirSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import test from 'node:test';

test('every Node-API function has an outer unwind boundary', () => {
	const sourceDirectory = fileURLToPath(new URL('../src/', import.meta.url));
	for (const entry of readdirSync(sourceDirectory, { recursive: true })) {
		if (typeof entry !== 'string' || !entry.endsWith('.rs')) {
			continue;
		}
		const source = readFileSync(path.join(sourceDirectory, entry), 'utf8');
		for (const match of source.matchAll(/#\[napi(?:\(([^)]*)\))?]\s+pub\s+(?:async\s+)?fn\s+(\w+)/g)) {
			assert.match(match[1] ?? '', /(?:^|,)\s*catch_unwind\s*(?:,|$)/, `${entry}:${match[2]} lacks catch_unwind`);
		}
	}
});
