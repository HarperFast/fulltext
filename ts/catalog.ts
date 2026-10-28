import { FulltextError } from './errors.js';

export const maxAggregations = 8;
export const maxAggregationNameBytes = 128;
export const maxAggregationBuckets = 100;
export const maxAggregationResponseBuckets = 1_024;
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
		| { type: 'terms'; size: number }
		| { type: 'stats' }
		| { type: 'histogram'; interval: number }
		| { type: 'range'; ranges: AggregationRange[] }
	);

export type AggregationResult =
	| {
			name: string;
			type: 'terms';
			buckets: Array<{ key: string; count: number }>;
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
	| { name: string; type: 'histogram'; buckets: Array<{ key: number; count: number }> }
	| { name: string; type: 'range'; buckets: Array<{ key: string; from?: number; to?: number; count: number }> };

export interface CatalogWriter {
	boolean(value: boolean): void;
	string(value: string): void;
	u8(value: number, label: string): void;
	u32(value: number, label: string): void;
	f64(value: number, label: string): void;
}

export interface CatalogCursor {
	u8(): number;
	u16(): number;
	u64(): bigint;
	f64(): number;
	string(): string;
}

export function normalizeCatalog(
	sort: unknown,
	aggregations: unknown,
): { sort?: SortRequest; aggregations: AggregationRequest[] } {
	let normalizedSort: SortRequest | undefined;
	if (sort !== undefined) {
		const value = object(sort, 'sort');
		const field = text(value.field, 'sort.field', maxCatalogFieldBytes);
		const direction = value.direction;
		const missing = value.missing;
		if (direction !== 'asc' && direction !== 'desc') invalid('sort.direction must be asc or desc');
		if (missing !== 'first' && missing !== 'last') invalid('sort.missing must be first or last');
		normalizedSort = { field, direction, missing };
	}
	if (aggregations === undefined) return { sort: normalizedSort, aggregations: [] };
	if (!Array.isArray(aggregations) || aggregations.length > maxAggregations) {
		invalid(`aggregations must be an array with at most ${maxAggregations} entries`);
	}
	const names = new Set<string>();
	const normalized = Array.from(aggregations, (entry): AggregationRequest => {
		const value = object(entry, 'aggregation');
		const name = text(value.name, 'aggregation.name', maxAggregationNameBytes);
		if (names.has(name)) invalid('aggregation names must be distinct');
		names.add(name);
		const field = text(value.field, 'aggregation.field', maxCatalogFieldBytes);
		switch (value.type) {
			case 'terms': {
				const size = value.size;
				if (typeof size !== 'number' || !Number.isInteger(size) || size < 1 || size > maxAggregationBuckets) {
					invalid(`terms size must be an integer from 1 to ${maxAggregationBuckets}`);
				}
				return { name, field, type: 'terms', size };
			}
			case 'stats':
				return { name, field, type: 'stats' };
			case 'histogram': {
				const interval = finite(value.interval, 'histogram interval');
				if (interval <= 0) invalid('histogram interval must be greater than zero');
				return { name, field, type: 'histogram', interval };
			}
			case 'range': {
				const input = value.ranges;
				if (!Array.isArray(input) || input.length < 1 || input.length > maxAggregationBuckets) {
					invalid(`ranges must contain between 1 and ${maxAggregationBuckets} entries`);
				}
				const ranges = Array.from(input, (entry, index): AggregationRange => {
					const range = object(entry, 'range');
					const from = range.from === undefined ? undefined : finite(range.from, 'range.from');
					const to = range.to === undefined ? undefined : finite(range.to, 'range.to');
					if (from === undefined && index !== 0) invalid('only the first range may omit from');
					if (to === undefined && index !== input.length - 1) invalid('only the last range may omit to');
					if (from !== undefined && to !== undefined && from >= to) invalid('range.from must be less than range.to');
					return { ...(from === undefined ? {} : { from }), ...(to === undefined ? {} : { to }) };
				});
				for (let index = 1; index < ranges.length; index++) {
					if (ranges[index - 1].to !== ranges[index].from) invalid('ranges must be contiguous and non-overlapping');
				}
				return { name, field, type: 'range', ranges };
			}
			default:
				invalid('aggregation.type must be terms, stats, histogram, or range');
		}
	});
	return { sort: normalizedSort, aggregations: normalized };
}

export function encodeCatalog(
	writer: CatalogWriter,
	sort: SortRequest | undefined,
	aggregations: AggregationRequest[],
): void {
	writer.boolean(sort !== undefined);
	if (sort) {
		writer.string(sort.field);
		writer.u8(sort.direction === 'asc' ? 0 : 1, 'sort direction');
		writer.u8(sort.missing === 'first' ? 0 : 1, 'sort missing');
	}
	writer.u8(aggregations.length, 'aggregation count');
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
				writer.u8(aggregation.ranges.length, 'range count');
				for (const range of aggregation.ranges) {
					writer.boolean(range.from !== undefined);
					if (range.from !== undefined) writer.f64(range.from, 'range.from');
					writer.boolean(range.to !== undefined);
					if (range.to !== undefined) writer.f64(range.to, 'range.to');
				}
		}
	}
}

export function decodeAggregations(cursor: CatalogCursor): AggregationResult[] {
	const count = cursor.u8();
	if (count > maxAggregations) nativeFailure('Native aggregation count exceeds its limit');
	const results: AggregationResult[] = [];
	const names = new Set<string>();
	let totalBuckets = 0;
	const buckets = () => {
		const count = cursor.u16();
		totalBuckets += count;
		if (totalBuckets > maxAggregationResponseBuckets) nativeFailure('Native aggregation buckets exceed their limit');
		return count;
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
						};
					}),
				});
				break;
			default:
				nativeFailure('Native aggregation type is invalid');
		}
	}
	return results;
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
