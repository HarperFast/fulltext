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
		verifyNapiBoundaries(readFileSync(path.join(sourceDirectory, entry), 'utf8'), entry);
	}
});

test('the Node-API boundary check rejects syntax that previously failed open', () => {
	assert.throws(
		() => verifyNapiBoundaries('#[napi]\n#[allow(dead_code)]\npub fn missing() {}', 'extra-attribute.rs'),
		/lacks catch_unwind/,
	);
	assert.throws(
		() => verifyNapiBoundaries('#[napi(ts_args_type = "Array<(string)>")]\npub fn nested() {}', 'nested.rs'),
		/lacks catch_unwind/,
	);
	assert.throws(
		() => verifyNapiBoundaries('#[napi(\ncatch_unwind\n)]\npub fn multiline() {}', 'multiline.rs'),
		/must be written on one line/,
	);
	assert.doesNotThrow(() =>
		verifyNapiBoundaries(
			'#[napi(ts_args_type = "Array<(string)>", catch_unwind)]\n#[allow(dead_code)]\npub fn guarded() {}',
			'guarded.rs',
		),
	);
});

function verifyNapiBoundaries(source, entry) {
	const lines = source.split('\n');
	for (let index = 0; index < lines.length; index++) {
		const attribute = lines[index].trim();
		if (!attribute.startsWith('#[napi')) {
			continue;
		}
		assert.match(
			attribute,
			/^#\[napi(?:\(.*\))?]$/,
			`${entry}:${index + 1} napi attributes must be written on one line`,
		);

		let itemIndex = index + 1;
		while (itemIndex < lines.length) {
			const line = lines[itemIndex].trim();
			if (line === '' || line.startsWith('//')) {
				itemIndex++;
				continue;
			}
			if (line.startsWith('#[')) {
				assert.match(line, /^#\[.*]$/, `${entry}:${itemIndex + 1} item attributes must be written on one line`);
				itemIndex++;
				continue;
			}
			const functionName = /(?:^|\s)fn\s+(\w+)/.exec(line)?.[1];
			if (functionName) {
				assert.match(attribute, /(?:\(|,)\s*catch_unwind\s*(?:,|\))/, `${entry}:${functionName} lacks catch_unwind`);
			}
			break;
		}
	}
}
