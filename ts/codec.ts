import { FulltextError } from './errors.js';

const protocolVersion = 1;

export interface PackedFieldConfig {
	name: string;
	weight: number;
}

export interface PackedOpenConfig {
	path: string;
	indexId: string;
	generation: string;
	fields: PackedFieldConfig[];
	analyzer: string;
	stopWords: boolean;
	positions: boolean;
	surfaceTerms: boolean;
	limits: {
		indexingThreads: number;
		searchThreads: number;
		writerMemoryBytes: number;
		maxQueuedCommands: number;
		maxQueuedBytes: number;
		maxBatchBytes: number;
	};
}

export interface PackedMutationBatch {
	upserts: Array<{ id: string; fields: Record<string, string | string[]> }>;
	deletes: string[];
}

export interface PackedSearchRequest {
	text: string;
	operator: 'any' | 'all';
	fields: string[];
	offset: number;
	limit: number;
	exactTotal: boolean;
}

export function encodeOpen(config: PackedOpenConfig): Buffer {
	const writer = new ByteWriter(Number.MAX_SAFE_INTEGER);
	writer.header('FTOP');
	writer.string(config.path);
	writer.string(config.indexId);
	writer.string(config.generation);
	writer.string(config.analyzer);
	writer.boolean(config.stopWords);
	writer.boolean(config.positions);
	writer.boolean(config.surfaceTerms);
	writer.u16(config.fields.length, 'fields.length');
	for (const field of config.fields) {
		writer.string(field.name);
		writer.f32(field.weight, 'field.weight');
	}
	writer.u16(config.limits.indexingThreads, 'limits.indexingThreads');
	writer.u16(config.limits.searchThreads, 'limits.searchThreads');
	writer.u64(config.limits.writerMemoryBytes, 'limits.writerMemoryBytes');
	writer.u32(config.limits.maxQueuedCommands, 'limits.maxQueuedCommands');
	writer.u64(config.limits.maxQueuedBytes, 'limits.maxQueuedBytes');
	writer.u64(config.limits.maxBatchBytes, 'limits.maxBatchBytes');
	return writer.finish();
}

export function encodeBatch(batch: PackedMutationBatch, maxBytes: number): Buffer {
	const writer = new ByteWriter(maxBytes);
	writer.header('FTMB');
	writer.u32(batch.upserts.length, 'upserts.length');
	writer.u32(batch.deletes.length, 'deletes.length');
	for (const upsert of batch.upserts) {
		writer.string(upsert.id);
		const fields = Object.entries(upsert.fields);
		writer.u16(fields.length, 'upsert field count');
		for (const [name, value] of fields) {
			writer.string(name);
			const values = Array.isArray(value) ? value : [value];
			writer.u16(values.length, 'field value count');
			for (const entry of values) {
				writer.string(entry);
			}
		}
	}
	for (const id of batch.deletes) {
		writer.string(id);
	}
	return writer.finish();
}

export function encodeSearch(request: PackedSearchRequest): Buffer {
	const writer = new ByteWriter(2 * 1024 * 1024);
	writer.header('FTSQ');
	writer.string(request.text);
	writer.u8(request.operator === 'all' ? 1 : 0, 'operator');
	writer.u16(request.fields.length, 'fields.length');
	for (const field of request.fields) {
		writer.string(field);
	}
	writer.u32(request.offset, 'offset');
	writer.u32(request.limit, 'limit');
	writer.boolean(request.exactTotal);
	return writer.finish();
}

export function decodeResponse(value: Buffer): Cursor {
	const cursor = new Cursor(value);
	if (cursor.text(4) !== 'FTRP' || cursor.u16() !== protocolVersion) {
		throw new FulltextError('E_NATIVE_ABI_MISMATCH', 'Invalid native response envelope');
	}
	const status = cursor.u8();
	if (status === 1) {
		const code = cursor.string();
		const message = cursor.string();
		throw new FulltextError(
			FulltextError.isCode(code) ? code : 'E_NATIVE_FAILURE',
			FulltextError.isCode(code) ? message : `${code}: ${message}`,
		);
	}
	if (status !== 0) {
		throw new FulltextError('E_NATIVE_FAILURE', `Unknown native response status ${status}`);
	}
	return cursor;
}

export class Cursor {
	readonly #buffer: Buffer;
	#offset = 0;

	constructor(buffer: Buffer) {
		this.#buffer = buffer;
	}

	u8(): number {
		return this.take(1)[0];
	}

	u16(): number {
		const value = this.#buffer.readUInt16LE(this.#offset);
		this.take(2);
		return value;
	}

	u32(): number {
		const value = this.#buffer.readUInt32LE(this.#offset);
		this.take(4);
		return value;
	}

	u64(): bigint {
		const value = this.#buffer.readBigUInt64LE(this.#offset);
		this.take(8);
		return value;
	}

	f32(): number {
		const value = this.#buffer.readFloatLE(this.#offset);
		this.take(4);
		return value;
	}

	string(): string {
		return this.text(this.u32());
	}

	text(length: number): string {
		return this.take(length).toString('utf8');
	}

	finish(): void {
		if (this.#offset !== this.#buffer.length) {
			throw new FulltextError('E_NATIVE_FAILURE', 'Native response has trailing bytes');
		}
	}

	private take(length: number): Buffer {
		const end = this.#offset + length;
		if (!Number.isSafeInteger(end) || end > this.#buffer.length) {
			throw new FulltextError('E_NATIVE_FAILURE', 'Native response is truncated');
		}
		const value = this.#buffer.subarray(this.#offset, end);
		this.#offset = end;
		return value;
	}
}

class ByteWriter {
	readonly #chunks: Buffer[] = [];
	readonly #maxBytes: number;
	#length = 0;

	constructor(maxBytes: number) {
		if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'maxBytes must be a positive safe integer');
		}
		this.#maxBytes = maxBytes;
	}

	header(magic: string): void {
		this.bytes(Buffer.from(magic, 'ascii'));
		this.u16(protocolVersion, 'protocolVersion');
	}

	boolean(value: boolean): void {
		this.u8(value ? 1 : 0, 'boolean');
	}

	u8(value: number, name: string): void {
		this.integer(value, 0xff, name, 1, 'writeUInt8');
	}

	u16(value: number, name: string): void {
		this.integer(value, 0xffff, name, 2, 'writeUInt16LE');
	}

	u32(value: number, name: string): void {
		this.integer(value, 0xffffffff, name, 4, 'writeUInt32LE');
	}

	u64(value: number, name: string): void {
		if (!Number.isSafeInteger(value) || value < 0) {
			throw new FulltextError('E_INVALID_ARGUMENT', `${name} must be a non-negative safe integer`);
		}
		const buffer = Buffer.allocUnsafe(8);
		buffer.writeBigUInt64LE(BigInt(value));
		this.bytes(buffer);
	}

	f32(value: number, name: string): void {
		if (!Number.isFinite(value) || value <= 0) {
			throw new FulltextError('E_INVALID_ARGUMENT', `${name} must be finite and greater than zero`);
		}
		const buffer = Buffer.allocUnsafe(4);
		buffer.writeFloatLE(value);
		this.bytes(buffer);
	}

	string(value: string): void {
		if (typeof value !== 'string') {
			throw new FulltextError('E_INVALID_ARGUMENT', 'packed string values must be strings');
		}
		const bytes = Buffer.from(value, 'utf8');
		this.u32(bytes.length, 'string byte length');
		this.bytes(bytes);
	}

	finish(): Buffer {
		return Buffer.concat(this.#chunks, this.#length);
	}

	private integer(
		value: number,
		maximum: number,
		name: string,
		width: number,
		method: 'writeUInt8' | 'writeUInt16LE' | 'writeUInt32LE',
	): void {
		if (!Number.isInteger(value) || value < 0 || value > maximum) {
			throw new FulltextError('E_INVALID_ARGUMENT', `${name} is outside its packed integer range`);
		}
		const buffer = Buffer.allocUnsafe(width);
		buffer[method](value, 0);
		this.bytes(buffer);
	}

	private bytes(value: Buffer): void {
		const nextLength = this.#length + value.length;
		if (!Number.isSafeInteger(nextLength) || nextLength > this.#maxBytes) {
			throw new FulltextError('E_INVALID_ARGUMENT', `packed value exceeds ${this.#maxBytes} bytes`);
		}
		this.#length = nextLength;
		this.#chunks.push(value);
	}
}
