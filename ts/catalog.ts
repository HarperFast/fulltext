import { FulltextError } from './errors.js';

const maxAggregationDepth = 32;
const maxCatalogBytes = 8 << 20;
const maxAggregationNameBytes = 128;
const maxTermsSize = 0xffffffff;
const maxAggregationResponseBuckets = 1_024;
const maxCatalogFieldBytes = 1 << 20;
const invalidSurrogate = /[\uD800-\uDFFF]/u;

export interface SortRequest {
	field: string;
	direction: 'asc' | 'desc';
	missing: 'first' | 'last';
}

export interface AggregationRange {
	from?: number;
	to?: number;
}

type AggregationBase = { name: string; field: string };
export type AggregationRequest = AggregationBase &
	(
		| { type: 'terms'; size: number; aggregations?: AggregationRequest[] }
		| { type: 'stats'; aggregations?: never }
		| { type: 'histogram'; interval: number; aggregations?: AggregationRequest[] }
		| { type: 'range'; ranges: AggregationRange[]; aggregations?: AggregationRequest[] }
	);

export type AggregationResult =
	| {
			name: string;
			type: 'terms';
			buckets: Array<{ key: string; count: number; aggregations?: AggregationResult[] }>;
			sumOtherDocCount: number;
			docCountErrorUpperBound: number | null;
	  }
	| {
			name: string;
			type: 'stats';
			count: number;
			sum: number;
			min: number | null;
			max: number | null;
			avg: number | null;
	  }
	| {
			name: string;
			type: 'histogram';
			buckets: Array<{ key: number; count: number; aggregations?: AggregationResult[] }>;
	  }
	| {
			name: string;
			type: 'range';
			buckets: Array<{ key: string; from?: number; to?: number; count: number; aggregations?: AggregationResult[] }>;
	  };

export interface CatalogWriter {
	boolean(value: boolean): void;
	string(value: string): void;
	u8(value: number, label: string): void;
	u32(value: number, label: string): void;
	f64(value: number, label: string): void;
}

export interface CatalogCursor {
	u8(): number;
	u32(): number;
	remaining(): number;
	u64(): bigint;
	f64(): number;
	string(): string;
}

export function normalizeCatalog(
	sort: unknown,
	aggregations: unknown,
): { sort?: SortRequest[]; aggregations: AggregationRequest[] } {
	let bytes = 8;
	let resultBytes = 4;
	const charge = (amount: number) => {
		bytes += amount;
		if (bytes > maxCatalogBytes) invalid('catalog exceeds the search request byte budget');
	};
	const readText = (value: unknown, label: string, limit: number) => {
		const result = text(value, label, limit);
		charge(4 + Buffer.byteLength(result));
		return result;
	};
	let normalizedSort: SortRequest[] | undefined;
	if (sort !== undefined) {
		const input = Array.isArray(sort) ? sort : [sort];
		if (!input.length) invalid('sort must contain at least one field');
		const fields = new Set<string>();
		normalizedSort = [];
		for (const entry of input) {
			charge(2);
			const value = object(entry, 'sort');
			const field = readText(value.field, 'sort.field', maxCatalogFieldBytes);
			if (fields.has(field)) invalid('sort fields must be distinct');
			fields.add(field);
			const direction = value.direction;
			const missing = value.missing;
			if (direction !== 'asc' && direction !== 'desc') invalid('sort.direction must be asc or desc');
			if (missing !== 'first' && missing !== 'last') invalid('sort.missing must be first or last');
			normalizedSort.push({ field, direction, missing });
		}
	}
	const normalize = (input: unknown, depth: number): AggregationRequest[] => {
		if (input === undefined) return [];
		if (!Array.isArray(input)) invalid('aggregations must be an array');
		if (input.length && depth > maxAggregationDepth) invalid('aggregation depth exceeds its stack safety limit');
		const names = new Set<string>();
		const normalized: AggregationRequest[] = [];
		for (const entry of input) {
			charge(5);
			const value = object(entry, 'aggregation');
			const name = readText(value.name, 'aggregation.name', maxAggregationNameBytes);
			if (names.has(name)) invalid('aggregation names must be distinct');
			names.add(name);
			const field = readText(value.field, 'aggregation.field', maxCatalogFieldBytes);
			resultBytes += 5 + Buffer.byteLength(name) + (value.type === 'stats' ? 19 : value.type === 'terms' ? 13 : 4);
			if (resultBytes > maxCatalogBytes) invalid('aggregations exceed the minimum result byte budget');
			switch (value.type) {
				case 'terms': {
					charge(4);
					const size = value.size;
					if (typeof size !== 'number' || !Number.isInteger(size) || size < 1 || size > maxTermsSize)
						invalid('terms size must be a positive u32 integer');
					normalized.push({ name, field, type: 'terms', size, ...children(value, depth) });
					break;
				}
				case 'stats':
					if (value.aggregations !== undefined) invalid('stats aggregations cannot have children');
					normalized.push({ name, field, type: 'stats' });
					break;
				case 'histogram': {
					charge(8);
					const interval = finite(value.interval, 'histogram interval');
					if (interval <= 0) invalid('histogram interval must be greater than zero');
					normalized.push({ name, field, type: 'histogram', interval, ...children(value, depth) });
					break;
				}
				case 'range': {
					charge(4);
					const input = value.ranges;
					if (!Array.isArray(input) || !input.length) invalid('ranges must contain at least one entry');
					const ranges: AggregationRange[] = [];
					for (let index = 0; index < input.length; index++) {
						charge(2);
						const range = object(input[index], 'range');
						const from = range.from === undefined ? undefined : finite(range.from, 'range.from');
						const to = range.to === undefined ? undefined : finite(range.to, 'range.to');
						charge((from === undefined ? 0 : 8) + (to === undefined ? 0 : 8));
						if (from === undefined && index !== 0) invalid('only the first range may omit from');
						if (to === undefined && index !== input.length - 1) invalid('only the last range may omit to');
						if (from !== undefined && to !== undefined && from >= to) invalid('range.from must be less than range.to');
						if (index && ranges[index - 1].to !== from) invalid('ranges must be contiguous and non-overlapping');
						ranges.push({ ...(from === undefined ? {} : { from }), ...(to === undefined ? {} : { to }) });
					}
					normalized.push({ name, field, type: 'range', ranges, ...children(value, depth) });
					break;
				}
				default:
					invalid('aggregation.type must be terms, stats, histogram, or range');
			}
		}
		return normalized;
	};
	const children = (value: Record<string, unknown>, depth: number) => {
		const aggregations = normalize(value.aggregations, depth + 1);
		return aggregations.length ? { aggregations } : {};
	};
	return { sort: normalizedSort, aggregations: normalize(aggregations, 1) };
}

export function encodeCatalog(
	writer: CatalogWriter,
	sort: SortRequest | SortRequest[] | undefined,
	aggregations: AggregationRequest[] | undefined,
): void {
	const sorts = sort === undefined ? [] : Array.isArray(sort) ? sort : [sort];
	writer.u32(sorts.length, 'sort count');
	for (const entry of sorts) {
		writer.string(entry.field);
		writer.u8(entry.direction === 'asc' ? 0 : 1, 'sort direction');
		writer.u8(entry.missing === 'first' ? 0 : 1, 'sort missing');
	}
	encodeAggregations(writer, aggregations, 1);
}

function encodeAggregations(
	writer: CatalogWriter,
	aggregations: AggregationRequest[] | undefined,
	depth: number,
): void {
	if (aggregations?.length && depth > maxAggregationDepth) invalid('aggregation depth exceeds its stack safety limit');
	writer.u32(aggregations?.length ?? 0, 'aggregation count');
	if (!aggregations) return;
	for (const aggregation of aggregations) {
		writer.string(aggregation.name);
		writer.string(aggregation.field);
		switch (aggregation.type) {
			case 'terms':
				writer.u8(0, 'aggregation type');
				writer.u32(aggregation.size, 'terms size');
				break;
			case 'stats':
				writer.u8(1, 'aggregation type');
				break;
			case 'histogram':
				writer.u8(2, 'aggregation type');
				writer.f64(aggregation.interval, 'histogram interval');
				break;
			case 'range':
				writer.u8(3, 'aggregation type');
				writer.u32(aggregation.ranges.length, 'range count');
				for (const range of aggregation.ranges) {
					writer.boolean(range.from !== undefined);
					if (range.from !== undefined) writer.f64(range.from, 'range.from');
					writer.boolean(range.to !== undefined);
					if (range.to !== undefined) writer.f64(range.to, 'range.to');
				}
		}
		encodeAggregations(writer, aggregation.aggregations, depth + 1);
	}
}

export function decodeAggregations(cursor: CatalogCursor): AggregationResult[] | undefined {
	let totalBuckets = 0;
	const decode = (depth: number): AggregationResult[] | undefined => {
		const count = cursor.u32();
		if (!count) return;
		if (depth > maxAggregationDepth) nativeFailure('Native aggregation depth exceeds its stack safety limit');
		if (count > Math.floor(cursor.remaining() / 10)) nativeFailure('Native aggregation count exceeds remaining bytes');
		const results: AggregationResult[] = [];
		const names = new Set<string>();
		const buckets = () => {
			const count = cursor.u32();
			totalBuckets += count;
			if (totalBuckets > maxAggregationResponseBuckets) nativeFailure('Native aggregation buckets exceed their limit');
			if (count > Math.floor(cursor.remaining() / 16)) nativeFailure('Native bucket count exceeds remaining bytes');
			return count;
		};
		const children = () => {
			const aggregations = decode(depth + 1);
			return aggregations ? { aggregations } : {};
		};
		for (let index = 0; index < count; index++) {
			const name = cursor.string();
			if (!name || Buffer.byteLength(name) > maxAggregationNameBytes || names.has(name)) {
				nativeFailure('Native aggregation names are invalid');
			}
			names.add(name);
			switch (cursor.u8()) {
				case 0: {
					const entries = Array.from({ length: buckets() }, () => ({
						key: cursor.string(),
						count: safeNumber(cursor.u64()),
						...children(),
					}));
					const sumOtherDocCount = safeNumber(cursor.u64());
					const docCountErrorUpperBound = flag(cursor) ? safeNumber(cursor.u64()) : null;
					results.push({ name, type: 'terms', buckets: entries, sumOtherDocCount, docCountErrorUpperBound });
					break;
				}
				case 1:
					results.push({
						name,
						type: 'stats',
						count: safeNumber(cursor.u64()),
						sum: nativeFloat(cursor),
						min: optionalFloat(cursor),
						max: optionalFloat(cursor),
						avg: optionalFloat(cursor),
					});
					break;
				case 2:
					results.push({
						name,
						type: 'histogram',
						buckets: Array.from({ length: buckets() }, () => ({
							key: nativeFloat(cursor),
							count: safeNumber(cursor.u64()),
							...children(),
						})),
					});
					break;
				case 3:
					results.push({
						name,
						type: 'range',
						buckets: Array.from({ length: buckets() }, () => {
							const key = cursor.string();
							const from = optionalFloat(cursor);
							const to = optionalFloat(cursor);
							return {
								key,
								...(from === null ? {} : { from }),
								...(to === null ? {} : { to }),
								count: safeNumber(cursor.u64()),
								...children(),
							};
						}),
					});
					break;
				default:
					nativeFailure('Native aggregation type is invalid');
			}
		}
		return results;
	};
	return decode(1);
}

function object(value: unknown, label: string): Record<string, unknown> {
	if (!value || typeof value !== 'object' || Array.isArray(value)) invalid(`${label} must be an object`);
	return value as Record<string, unknown>;
}

function text(value: unknown, label: string, maxBytes: number): string {
	if (
		typeof value !== 'string' ||
		!value.length ||
		invalidSurrogate.test(value) ||
		Buffer.byteLength(value) > maxBytes
	) {
		invalid(`${label} must be nonempty valid UTF-8 text of at most ${maxBytes} bytes`);
	}
	return value;
}

function finite(value: unknown, label: string): number {
	if (typeof value !== 'number' || !Number.isFinite(value)) invalid(`${label} must be finite`);
	return Object.is(value, -0) ? 0 : value;
}

function flag(cursor: CatalogCursor): boolean {
	const value = cursor.u8();
	if (value !== 0 && value !== 1) nativeFailure('Native aggregation presence flag is invalid');
	return value === 1;
}

function nativeFloat(cursor: CatalogCursor): number {
	const value = cursor.f64();
	if (!Number.isFinite(value)) nativeFailure('Native aggregation value is not finite');
	return value;
}

function optionalFloat(cursor: CatalogCursor): number | null {
	return flag(cursor) ? nativeFloat(cursor) : null;
}

function safeNumber(value: bigint): number {
	if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER))
		nativeFailure('Native aggregation count exceeds JavaScript safe integers');
	return Number(value);
}

function invalid(message: string): never {
	throw new FulltextError('E_INVALID_ARGUMENT', message);
}

function nativeFailure(message: string): never {
	throw new FulltextError('E_NATIVE_FAILURE', message);
}
