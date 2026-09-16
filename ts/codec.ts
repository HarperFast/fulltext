import { FulltextError, type FulltextErrorCode } from './errors.js';

const protocolVersion = 1;
const maxStringBytes = 1 << 20;
const maxFields = 1_024;
const mutationBatchHeaderBytes = 14;
const maxPendingWriterChunks = 1_024;

export interface PackedFieldConfig {
	name: string;
	weight: number;
}

export interface PackedIndexIdentityConfig {
	indexId: string;
	generation: string;
	fields: PackedFieldConfig[];
	analyzer: string;
	stopWords: boolean;
	positions: boolean;
	surfaceTerms: boolean;
}

export interface PackedEngineConfig extends PackedIndexIdentityConfig {
	limits: {
		indexingThreads: number;
		searchThreads: number;
		writerMemoryBytes: number;
		maxQueuedCommands: number;
		maxQueuedBytes: number;
		maxBatchBytes: number;
	};
}

export interface PackedOpenConfig extends PackedEngineConfig {
	path: string;
}

export interface PackedInspectConfig extends PackedIndexIdentityConfig {
	path: string;
}

export interface PackedResetConfig {
	path: string;
	indexId: string;
}

export interface PackedMutationBatch {
	upserts: Array<{ id: string; fields: Record<string, string | string[]> }>;
	deletes: string[];
}

export interface PackedMutationBatchRejection {
	operation: 'upsert' | 'delete';
	index: number;
	code: 'E_INVALID_ARGUMENT' | 'E_BATCH_TOO_LARGE';
}

export interface PackedMutationBatchPartition {
	bytes: Buffer;
	mutationCount: number;
}

export interface PackedMutationBatchPartitions {
	batches: PackedMutationBatchPartition[];
	rejected: PackedMutationBatchRejection[];
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
	const writer = new ByteWriter(Number.MAX_SAFE_INTEGER, 'E_INVALID_ARGUMENT');
	writer.header('FTOP');
	writer.string(config.path);
	encodeEngine(writer, config);
	return writer.finish();
}

export function encodeInspect(config: PackedInspectConfig): Buffer {
	const writer = new ByteWriter(Number.MAX_SAFE_INTEGER, 'E_INVALID_ARGUMENT');
	writer.header('FTIP');
	writer.string(config.path);
	encodeIndexIdentity(writer, config);
	return writer.finish();
}

export function encodeReset(config: PackedResetConfig): Buffer {
	const writer = new ByteWriter(Number.MAX_SAFE_INTEGER, 'E_INVALID_ARGUMENT');
	writer.header('FTRX');
	writer.string(config.path);
	writer.string(config.indexId);
	return writer.finish();
}

function encodeEngine(writer: ByteWriter, config: PackedEngineConfig): void {
	encodeIndexIdentity(writer, config);
	writer.u16(config.limits.indexingThreads, 'limits.indexingThreads');
	writer.u16(config.limits.searchThreads, 'limits.searchThreads');
	writer.u64(config.limits.writerMemoryBytes, 'limits.writerMemoryBytes');
	writer.u32(config.limits.maxQueuedCommands, 'limits.maxQueuedCommands');
	writer.u64(config.limits.maxQueuedBytes, 'limits.maxQueuedBytes');
	writer.u64(config.limits.maxBatchBytes, 'limits.maxBatchBytes');
}

function encodeIndexIdentity(writer: ByteWriter, config: PackedIndexIdentityConfig): void {
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
}

export function encodeBatch(batch: PackedMutationBatch, maxBytes: number): Buffer {
	const writer = new ByteWriter(maxBytes, 'E_BATCH_TOO_LARGE');
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

export function encodeBatchPartitions(
	batch: PackedMutationBatch,
	maxBytes: number,
	maxTotalBytes: number,
	fieldNames: ReadonlySet<string>,
): PackedMutationBatchPartitions {
	if (!Array.isArray(batch.upserts) || !Array.isArray(batch.deletes)) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'mutation batch arrays are required');
	}
	if (!Number.isSafeInteger(maxBytes) || maxBytes <= mutationBatchHeaderBytes) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'maxBytes is too small for a mutation batch');
	}
	if (!Number.isSafeInteger(maxTotalBytes) || maxTotalBytes < maxBytes) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'maxTotalBytes must be a safe integer no smaller than maxBytes');
	}

	type EncodedRecord = {
		operation: 'upsert' | 'delete';
		index: number;
		chunks: Buffer[];
		byteLength: number;
	};
	const rejected: PackedMutationBatchRejection[] = [];
	const ids = new Set<string>();
	let totalBytes = 0;
	const batches: PackedMutationBatchPartition[] = [];
	let chunks: Buffer[] = [];
	let byteLength = mutationBatchHeaderBytes;
	let upsertCount = 0;
	let deleteCount = 0;
	const finish = () => {
		if (upsertCount + deleteCount === 0) return;
		if (totalBytes + byteLength > maxTotalBytes) {
			throw new FulltextError('E_BATCH_TOO_LARGE', 'logical mutation batch exceeds its total encoding limit');
		}
		const header = encodeBatchHeader(upsertCount, deleteCount);
		const bytes = Buffer.concat([header, ...chunks], byteLength);
		batches.push({ bytes, mutationCount: upsertCount + deleteCount });
		totalBytes += byteLength;
		chunks = [];
		byteLength = mutationBatchHeaderBytes;
		upsertCount = 0;
		deleteCount = 0;
	};
	const append = (record: EncodedRecord) => {
		if (byteLength + record.byteLength > maxBytes) finish();
		if (totalBytes + byteLength + record.byteLength > maxTotalBytes) {
			throw new FulltextError('E_BATCH_TOO_LARGE', 'logical mutation batch exceeds its total encoding limit');
		}
		for (const chunk of record.chunks) chunks.push(chunk);
		byteLength += record.byteLength;
		if (record.operation === 'upsert') upsertCount++;
		else deleteCount++;
	};

	const validateId = (id: unknown, operation: 'upsert' | 'delete', index: number): Buffer | undefined => {
		if (typeof id !== 'string' || id.length === 0 || /[\uD800-\uDFFF]/u.test(id)) {
			rejected.push({ operation, index, code: 'E_INVALID_ARGUMENT' });
			return;
		}
		const bytes = Buffer.from(id, 'utf8');
		if (bytes.length > maxStringBytes) {
			rejected.push({ operation, index, code: 'E_INVALID_ARGUMENT' });
			return;
		}
		if (ids.has(id)) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'mutation batch IDs must be distinct');
		}
		ids.add(id);
		return bytes;
	};

	for (let index = 0; index < batch.upserts.length; index++) {
		const upsert = batch.upserts[index];
		const id = validateId(upsert?.id, 'upsert', index);
		if (!id) continue;
		let encoded: { chunks: Buffer[]; byteLength: number };
		try {
			if (!upsert.fields || typeof upsert.fields !== 'object' || Array.isArray(upsert.fields)) {
				throw new FulltextError('E_INVALID_ARGUMENT', 'upsert fields must be an object');
			}
			const names = Object.keys(upsert.fields);
			if (names.length > maxFields) {
				throw new FulltextError('E_INVALID_ARGUMENT', 'upsert field count exceeds the supported limit');
			}
			for (const name of names) {
				if (!fieldNames.has(name)) {
					throw new FulltextError('E_SCHEMA_MISMATCH', 'upsert contains a field outside the opened schema');
				}
			}
			const writer = new ByteWriter(maxBytes - mutationBatchHeaderBytes, 'E_BATCH_TOO_LARGE');
			writer.encodedString(id);
			writer.u16(names.length, 'upsert field count');
			for (const name of names) {
				writer.string(name);
				const value = upsert.fields[name];
				const values = Array.isArray(value) ? value : [value];
				writer.u16(values.length, 'field value count');
				for (const entry of values) writer.string(entry);
			}
			encoded = writer.take();
		} catch (error) {
			if (!(error instanceof FulltextError)) throw error;
			if (error.code === 'E_BATCH_TOO_LARGE') {
				rejected.push({ operation: 'upsert', index, code: 'E_BATCH_TOO_LARGE' });
				continue;
			}
			if (error.code !== 'E_INVALID_ARGUMENT') throw error;
			rejected.push({ operation: 'upsert', index, code: error.code });
			continue;
		}
		append({ operation: 'upsert', index, ...encoded });
	}

	for (let index = 0; index < batch.deletes.length; index++) {
		const id = validateId(batch.deletes[index], 'delete', index);
		if (!id) continue;
		const writer = new ByteWriter(Number.MAX_SAFE_INTEGER, 'E_INVALID_ARGUMENT');
		writer.encodedString(id);
		const encoded = writer.take();
		if (mutationBatchHeaderBytes + encoded.byteLength > maxBytes) {
			rejected.push({ operation: 'delete', index, code: 'E_BATCH_TOO_LARGE' });
			continue;
		}
		append({ operation: 'delete', index, ...encoded });
	}
	finish();
	return { batches, rejected };
}

function encodeBatchHeader(upserts: number, deletes: number): Buffer {
	const header = Buffer.allocUnsafe(mutationBatchHeaderBytes);
	let offset = header.write('FTMB', 0, 'ascii');
	offset = header.writeUInt16LE(protocolVersion, offset);
	offset = header.writeUInt32LE(upserts, offset);
	offset = header.writeUInt32LE(deletes, offset);
	if (offset !== header.length) throw new FulltextError('E_NATIVE_FAILURE', 'mutation batch header length mismatch');
	return header;
}

export function encodeSearch(request: PackedSearchRequest): Buffer {
	const writer = new ByteWriter(2 * 1024 * 1024, 'E_INVALID_ARGUMENT');
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
	readonly #pendingChunks: Buffer[] = [];
	readonly #maxBytes: number;
	readonly #sizeErrorCode: FulltextErrorCode;
	#length = 0;
	#pendingLength = 0;

	constructor(maxBytes: number, sizeErrorCode: FulltextErrorCode) {
		if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'maxBytes must be a positive safe integer');
		}
		this.#maxBytes = maxBytes;
		this.#sizeErrorCode = sizeErrorCode;
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
		if (bytes.length > maxStringBytes) {
			throw new FulltextError('E_INVALID_ARGUMENT', `packed string exceeds ${maxStringBytes} UTF-8 bytes`);
		}
		this.encodedString(bytes);
	}

	encodedString(bytes: Buffer): void {
		this.u32(bytes.length, 'string byte length');
		this.bytes(bytes);
	}

	finish(): Buffer {
		return Buffer.concat([...this.#chunks, ...this.#pendingChunks], this.#length);
	}

	take(): { chunks: Buffer[]; byteLength: number } {
		return { chunks: [...this.#chunks, ...this.#pendingChunks], byteLength: this.#length };
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
			throw new FulltextError(this.#sizeErrorCode, `packed value exceeds ${this.#maxBytes} bytes`);
		}
		this.#length = nextLength;
		this.#pendingChunks.push(value);
		this.#pendingLength += value.length;
		if (this.#pendingChunks.length >= maxPendingWriterChunks) {
			this.#chunks.push(Buffer.concat(this.#pendingChunks, this.#pendingLength));
			this.#pendingChunks.length = 0;
			this.#pendingLength = 0;
		}
	}
}
