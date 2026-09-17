import { FulltextError, type FulltextErrorCode } from './errors.js';

const protocolVersion = 1;
const maxStringBytes = 1 << 20;
export const maxFields = 1_024;
export const mutationBatchHeaderBytes = 14;
export const minimumMutationBatchBytes = mutationBatchHeaderBytes + 7;
const maxPendingWriterChunks = 1_024;
const invalidSurrogate = /[\uD800-\uDFFF]/u;

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
	consumedUpserts: number;
	consumedDeletes: number;
}

export function validateMutationBatch(
	batch: PackedMutationBatch,
	validateDistinctIds: boolean,
	replacementDeleteMaxBytes?: number,
): void {
	if (!Array.isArray(batch.upserts) || !Array.isArray(batch.deletes)) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'mutation batch arrays are required');
	}
	if (!validateDistinctIds && replacementDeleteMaxBytes === undefined) return;
	const ids = validateDistinctIds ? new Set<string>() : undefined;
	const check = (id: unknown, operation: 'upsert' | 'delete', index: number) => {
		const rejection = mutationIdRejection(
			id,
			replacementDeleteMaxBytes === undefined
				? maxStringBytes
				: replacementDeleteMaxBytes - mutationBatchHeaderBytes - 4,
		);
		if (replacementDeleteMaxBytes !== undefined) {
			if (rejection === 'E_INVALID_ARGUMENT') {
				throw new FulltextError(
					'E_INVALID_ARGUMENT',
					`mutation batch ${operation} at index ${index} has no usable ID for a ${operation === 'upsert' ? 'replacement delete' : 'delete'}`,
				);
			}
			if (rejection === 'E_BATCH_TOO_LARGE') {
				throw new FulltextError(
					'E_BATCH_TOO_LARGE',
					`mutation batch ${operation} at index ${index} cannot fit in a ${operation === 'upsert' ? 'replacement-delete' : 'delete'} frame`,
				);
			}
		}
		if (rejection || !ids) return;
		const validId = id as string;
		if (ids.has(validId)) throw new FulltextError('E_INVALID_ARGUMENT', 'mutation batch IDs must be distinct');
		ids.add(validId);
	};
	for (let index = 0; index < batch.upserts.length; index++) check(batch.upserts[index]?.id, 'upsert', index);
	for (let index = 0; index < batch.deletes.length; index++) check(batch.deletes[index], 'delete', index);
}

export interface PackedMutationBatchFrame {
	batch?: PackedMutationBatchPartition;
	rejected: PackedMutationBatchRejection[];
	consumedUpserts: number;
	consumedDeletes: number;
	done: boolean;
}

type EncodedMutationRecord = {
	operation: 'upsert' | 'delete';
	index: number;
	chunks: Buffer[];
	byteLength: number;
};

export class MutationBatchFrameCursor {
	readonly #batch: PackedMutationBatch;
	readonly #maxBytes: number;
	readonly #fieldNames: ReadonlySet<string>;
	readonly #stopAfterFirstRejection: boolean;
	#upsertIndex = 0;
	#deleteIndex = 0;
	#pending?: EncodedMutationRecord;

	constructor(
		batch: PackedMutationBatch,
		maxBytes: number,
		fieldNames: ReadonlySet<string>,
		options: {
			validateDistinctIds: boolean;
			stopAfterFirstRejection?: boolean;
			requireReplacementDeletes?: boolean;
		},
	) {
		if (!Number.isSafeInteger(maxBytes) || maxBytes < minimumMutationBatchBytes) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'maxBytes is too small for a mutation batch');
		}
		validateMutationBatch(batch, options.validateDistinctIds, options.requireReplacementDeletes ? maxBytes : undefined);
		this.#batch = batch;
		this.#maxBytes = maxBytes;
		this.#fieldNames = fieldNames;
		this.#stopAfterFirstRejection = options.stopAfterFirstRejection ?? false;
	}

	next(): PackedMutationBatchFrame {
		const startUpsert = this.#upsertIndex;
		const startDelete = this.#deleteIndex;
		const rejected: PackedMutationBatchRejection[] = [];
		const frameWriter = new ByteWriter(this.#maxBytes - mutationBatchHeaderBytes, 'E_BATCH_TOO_LARGE');
		let byteLength = mutationBatchHeaderBytes;
		let upserts = 0;
		let deletes = 0;

		while (this.#upsertIndex < this.#batch.upserts.length || this.#deleteIndex < this.#batch.deletes.length) {
			const encoded = this.#pending ?? this.#encodeCurrent(rejected);
			if (!encoded) {
				if (rejected.length > 0 && this.#stopAfterFirstRejection) break;
				continue;
			}
			if (upserts + deletes > 0 && byteLength + encoded.byteLength > this.#maxBytes) {
				this.#pending = encoded;
				break;
			}
			this.#pending = undefined;
			frameWriter.encodedBytes(encoded.chunks);
			byteLength += encoded.byteLength;
			if (encoded.operation === 'upsert') {
				upserts++;
				this.#upsertIndex++;
			} else {
				deletes++;
				this.#deleteIndex++;
			}
		}

		let batch: PackedMutationBatchPartition | undefined;
		if (upserts + deletes > 0) {
			const header = encodeBatchHeader(upserts, deletes);
			const frame = frameWriter.take();
			batch = {
				bytes: Buffer.concat([header, ...frame.chunks], byteLength),
				mutationCount: upserts + deletes,
			};
		}
		return {
			batch,
			rejected,
			consumedUpserts: this.#upsertIndex - startUpsert,
			consumedDeletes: this.#deleteIndex - startDelete,
			done: this.#upsertIndex === this.#batch.upserts.length && this.#deleteIndex === this.#batch.deletes.length,
		};
	}

	#encodeCurrent(rejected: PackedMutationBatchRejection[]): EncodedMutationRecord | undefined {
		if (this.#upsertIndex < this.#batch.upserts.length) {
			const index = this.#upsertIndex;
			const upsert = this.#batch.upserts[index];
			const id = encodeMutationId(upsert?.id);
			if (!id) {
				rejected.push({ operation: 'upsert', index, code: 'E_INVALID_ARGUMENT' });
				this.#upsertIndex++;
				return;
			}
			try {
				return encodeUpsertRecord(upsert, id, index, this.#maxBytes - mutationBatchHeaderBytes, this.#fieldNames);
			} catch (error) {
				if (!(error instanceof FulltextError)) throw error;
				if (error.code !== 'E_INVALID_ARGUMENT' && error.code !== 'E_BATCH_TOO_LARGE') throw error;
				rejected.push({ operation: 'upsert', index, code: error.code });
				this.#upsertIndex++;
				return;
			}
		}

		const index = this.#deleteIndex;
		const id = encodeMutationId(this.#batch.deletes[index]);
		if (!id) {
			rejected.push({ operation: 'delete', index, code: 'E_INVALID_ARGUMENT' });
			this.#deleteIndex++;
			return;
		}
		const encoded = encodeDeleteRecord(id, index);
		if (mutationBatchHeaderBytes + encoded.byteLength > this.#maxBytes) {
			rejected.push({ operation: 'delete', index, code: 'E_BATCH_TOO_LARGE' });
			this.#deleteIndex++;
			return;
		}
		return encoded;
	}
}

function encodeMutationId(id: unknown): Buffer | undefined {
	if (mutationIdRejection(id)) return;
	return Buffer.from(id as string, 'utf8');
}

function mutationIdRejection(
	id: unknown,
	maximumBytes = maxStringBytes,
): 'E_INVALID_ARGUMENT' | 'E_BATCH_TOO_LARGE' | undefined {
	if (typeof id !== 'string' || id.length === 0 || id.length > maxStringBytes || invalidSurrogate.test(id))
		return 'E_INVALID_ARGUMENT';
	if (id.length * 3 <= Math.min(maxStringBytes, maximumBytes)) return;
	const byteLength = Buffer.byteLength(id, 'utf8');
	if (byteLength > maxStringBytes) return 'E_INVALID_ARGUMENT';
	if (byteLength > maximumBytes) return 'E_BATCH_TOO_LARGE';
}

function encodeUpsertRecord(
	upsert: PackedMutationBatch['upserts'][number],
	id: Buffer,
	index: number,
	maxBytes: number,
	fieldNames: ReadonlySet<string>,
): EncodedMutationRecord {
	if (!upsert.fields || typeof upsert.fields !== 'object' || Array.isArray(upsert.fields)) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'upsert fields must be an object');
	}
	const prototype = Object.getPrototypeOf(upsert.fields);
	if (prototype !== Object.prototype && prototype !== null) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'upsert fields must be a plain object');
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
	const writer = new ByteWriter(maxBytes, 'E_BATCH_TOO_LARGE');
	writer.encodedString(id);
	writer.u16(names.length, 'upsert field count');
	for (const name of names) {
		writer.string(name);
		const value = upsert.fields[name];
		const values = Array.isArray(value) ? value : [value];
		writer.u16(values.length, 'field value count');
		for (const entry of values) writer.string(entry);
	}
	return { operation: 'upsert', index, ...writer.take() };
}

function encodeDeleteRecord(id: Buffer, index: number): EncodedMutationRecord {
	const writer = new ByteWriter(Number.MAX_SAFE_INTEGER, 'E_INVALID_ARGUMENT');
	writer.encodedString(id);
	return { operation: 'delete', index, ...writer.take() };
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
	if (!Number.isSafeInteger(maxBytes) || maxBytes < minimumMutationBatchBytes) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'maxBytes is too small for a mutation batch');
	}
	if (!Number.isSafeInteger(maxTotalBytes) || maxTotalBytes < minimumMutationBatchBytes) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'maxTotalBytes is too small for a mutation batch');
	}
	const maxFrameBytes = Math.min(maxBytes, maxTotalBytes);
	const encodedIds = new Set<string>();
	const checkDuplicate = (id: unknown) => {
		if (typeof id !== 'string' || id.length === 0 || id.length > maxStringBytes || invalidSurrogate.test(id)) return;
		if (Buffer.byteLength(id) > maxStringBytes) return;
		if (encodedIds.has(id)) throw new FulltextError('E_INVALID_ARGUMENT', 'mutation batch IDs must be distinct');
		encodedIds.add(id);
	};
	for (const upsert of batch.upserts) checkDuplicate(upsert?.id);
	for (const id of batch.deletes) checkDuplicate(id);

	const rejected: PackedMutationBatchRejection[] = [];
	let totalBytes = 0;
	const batches: PackedMutationBatchPartition[] = [];
	let frameWriter = new ByteWriter(maxFrameBytes - mutationBatchHeaderBytes, 'E_BATCH_TOO_LARGE');
	let byteLength = mutationBatchHeaderBytes;
	let upsertCount = 0;
	let deleteCount = 0;
	const finish = () => {
		if (upsertCount + deleteCount === 0) return;
		const header = encodeBatchHeader(upsertCount, deleteCount);
		const frame = frameWriter.take();
		const bytes = Buffer.concat([header, ...frame.chunks], byteLength);
		batches.push({ bytes, mutationCount: upsertCount + deleteCount });
		totalBytes += byteLength;
		frameWriter = new ByteWriter(maxFrameBytes - mutationBatchHeaderBytes, 'E_BATCH_TOO_LARGE');
		byteLength = mutationBatchHeaderBytes;
		upsertCount = 0;
		deleteCount = 0;
	};
	const append = (record: EncodedMutationRecord): boolean => {
		if (byteLength + record.byteLength > maxFrameBytes) finish();
		if (totalBytes + byteLength + record.byteLength > maxTotalBytes) return false;
		frameWriter.encodedBytes(record.chunks);
		byteLength += record.byteLength;
		if (record.operation === 'upsert') upsertCount++;
		else deleteCount++;
		return true;
	};

	const validateId = (id: unknown, operation: 'upsert' | 'delete', index: number): Buffer | undefined => {
		const bytes = typeof id === 'string' && encodedIds.has(id) ? encodeMutationId(id) : undefined;
		if (!bytes) {
			rejected.push({ operation, index, code: 'E_INVALID_ARGUMENT' });
			return;
		}
		return bytes;
	};

	let consumedUpserts = 0;
	for (let index = 0; index < batch.upserts.length; index++) {
		const upsert = batch.upserts[index];
		const id = validateId(upsert?.id, 'upsert', index);
		if (!id) {
			consumedUpserts++;
			continue;
		}
		let encoded: EncodedMutationRecord;
		try {
			encoded = encodeUpsertRecord(upsert, id, index, maxFrameBytes - mutationBatchHeaderBytes, fieldNames);
		} catch (error) {
			if (!(error instanceof FulltextError)) throw error;
			if (error.code === 'E_BATCH_TOO_LARGE') {
				rejected.push({ operation: 'upsert', index, code: 'E_BATCH_TOO_LARGE' });
				consumedUpserts++;
				continue;
			}
			if (error.code !== 'E_INVALID_ARGUMENT') throw error;
			rejected.push({ operation: 'upsert', index, code: error.code });
			consumedUpserts++;
			continue;
		}
		if (!append(encoded)) break;
		consumedUpserts++;
	}

	let consumedDeletes = 0;
	if (consumedUpserts === batch.upserts.length)
		for (let index = 0; index < batch.deletes.length; index++) {
			const id = validateId(batch.deletes[index], 'delete', index);
			if (!id) {
				consumedDeletes++;
				continue;
			}
			const encoded = encodeDeleteRecord(id, index);
			if (mutationBatchHeaderBytes + encoded.byteLength > maxFrameBytes) {
				rejected.push({ operation: 'delete', index, code: 'E_BATCH_TOO_LARGE' });
				consumedDeletes++;
				continue;
			}
			if (!append(encoded)) break;
			consumedDeletes++;
		}
	finish();
	return { batches, rejected, consumedUpserts, consumedDeletes };
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
		if (value.length > maxStringBytes || Buffer.byteLength(value, 'utf8') > maxStringBytes) {
			throw new FulltextError('E_INVALID_ARGUMENT', `packed string exceeds ${maxStringBytes} UTF-8 bytes`);
		}
		const bytes = Buffer.from(value, 'utf8');
		this.encodedString(bytes);
	}

	encodedString(bytes: Buffer): void {
		this.u32(bytes.length, 'string byte length');
		this.bytes(bytes);
	}

	encodedBytes(chunks: readonly Buffer[]): void {
		for (const chunk of chunks) this.bytes(chunk);
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
