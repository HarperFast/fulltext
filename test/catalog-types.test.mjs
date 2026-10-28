import assert from 'node:assert';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';

test('public metadata request types accept supported shapes and reject paging and presence misuse', () => {
	const root = fileURLToPath(new URL('../', import.meta.url));
	const virtualPath = path.join(root, 'test', 'virtual-catalog-types.ts');
	const source = `
import type { SearchRequest, SearchPageRequest, FilterExpression } from '../ts/native.js';

const search = {
	text: 'trail shoes',
	sort: { field: 'price', direction: 'asc', missing: 'last' },
	aggregations: [
		{ name: 'brands', field: 'brand', type: 'terms', size: 10 },
		{ name: 'prices', field: 'price', type: 'stats' },
		{ name: 'histogram', field: 'price', type: 'histogram', interval: 10 },
		{ name: 'ranges', field: 'price', type: 'range', ranges: [{ to: 10 }, { from: 10 }] },
	],
	filter: { operator: 'not', clause: { field: 'brand', comparator: 'missing' } },
} satisfies SearchRequest;
const page = { text: 'trail shoes', continuation: 'token', filter: { field: 'brand', comparator: 'exists' } } satisfies SearchPageRequest;
const filter = { operator: 'not', clause: { operator: 'and', clauses: [
	{ field: 'brand', comparator: 'exists' },
	{ field: 'price', comparator: 'missing' },
	{ field: 'active', comparator: 'equals', value: true },
] } } satisfies FilterExpression;

// @ts-expect-error Native field sorting is excluded from ranked continuation requests.
const sortedPage = { text: 'trail', sort: search.sort } satisfies SearchPageRequest;
// @ts-expect-error Aggregations are excluded from ranked continuation requests.
const aggregatedPage = { text: 'trail', aggregations: search.aggregations } satisfies SearchPageRequest;
// @ts-expect-error Presence predicates have no value.
const existsWithValue = { field: 'brand', comparator: 'exists', value: 'cedar' } satisfies FilterExpression;
// @ts-expect-error Missing predicates have no value.
const missingWithValue = { field: 'price', comparator: 'missing', value: 1 } satisfies FilterExpression;
// @ts-expect-error Comparisons still require a value.
const equalsWithoutValue = { field: 'active', comparator: 'equals' } satisfies FilterExpression;
// @ts-expect-error Negation requires one clause rather than an array of clauses.
const invalidNot = { operator: 'not', clauses: [filter] } satisfies FilterExpression;
// @ts-expect-error Missing placement must be explicit.
const implicitMissing = { sort: { field: 'price', direction: 'asc' } } satisfies SearchRequest;
// @ts-expect-error Sort direction must be explicit.
const implicitDirection = { sort: { field: 'price', missing: 'first' } } satisfies SearchRequest;
`;
	const configFile = ts.readConfigFile(path.join(root, 'tsconfig.json'), ts.sys.readFile);
	assert.strictEqual(configFile.error, undefined);
	const parsed = ts.parseJsonConfigFileContent(configFile.config, ts.sys, root);
	assert.deepStrictEqual(parsed.errors, []);
	const options = { ...parsed.options, noEmit: true, rootDir: undefined };
	const host = ts.createCompilerHost(options);
	const readFile = host.readFile.bind(host);
	const fileExists = host.fileExists.bind(host);
	const getSourceFile = host.getSourceFile.bind(host);
	host.readFile = (filename) => (path.resolve(filename) === virtualPath ? source : readFile(filename));
	host.fileExists = (filename) => path.resolve(filename) === virtualPath || fileExists(filename);
	host.getSourceFile = (filename, languageVersion, onError, shouldCreateNewSourceFile) =>
		path.resolve(filename) === virtualPath
			? ts.createSourceFile(filename, source, languageVersion, true)
			: getSourceFile(filename, languageVersion, onError, shouldCreateNewSourceFile);
	const program = ts.createProgram([virtualPath], options, host);
	const diagnostics = ts.getPreEmitDiagnostics(program);
	assert.strictEqual(
		diagnostics.length,
		0,
		ts.formatDiagnosticsWithColorAndContext(diagnostics, {
			getCurrentDirectory: () => root,
			getCanonicalFileName: (filename) => filename,
			getNewLine: () => '\n',
		}),
	);
});
