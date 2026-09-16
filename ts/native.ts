import { lstat, readdir, realpath, rm } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';

import { FulltextError, normalizeNativeError } from './errors.js';
import {
	decodeResponse,
	encodeBatch,
	encodeBatchPartitions,
	encodeInspect,
	encodeOpen,
	encodeReset,
	encodeSearch,
} from './codec.js';
import { invoke } from './invoke.js';
import { loadAddon } from './load-addon.js';
import { PublicationState } from './publication.js';

const maxCommitPayloadBytes = 64 * 1024;

export { FulltextError } from './errors.js';
export type { FulltextErrorCode } from './errors.js';

export interface RuntimeInfo {
	packageVersion: string;
	tantivyVersion: string;
	nativeAbiVersion: number;
	lifecycleApiVersion: 1;
	mutationBatchApiVersion: 2;
	storageBackends: ReadonlyArray<'native'>;
	limits: { maxCommitPayloadBytes: number };
}

export interface NativeFullTextIndexOptions {
	path: string;
	indexId: string;
	generation: string;
	fields: Array<{ name: string; weight?: number }>;
	analyzer: 'english@1';
	stopWords?: boolean;
	positions?: boolean;
	surfaceTerms?: boolean;
	limits: NativeFullTextIndexLimits;
}

export interface NativeFullTextIndexLimits {
	indexingThreads: number;
	searchThreads: number;
	writerMemoryBytes: number;
	maxQueuedCommands: number;
	maxQueuedBytes: number;
	maxBatchBytes: number;
}

export type NativeFullTextIndexInspectionOptions = Omit<NativeFullTextIndexOptions, 'limits'>;

export type NativeFullTextIndexInspection =
	| { state: 'missing' }
	| { state: 'cursorless' }
	| { state: 'checkpointed'; committedPayload: string }
	| {
			state: 'incompatible';
			code:
				| 'E_IDENTITY_MISMATCH'
				| 'E_INCOMPLETE_CREATE'
				| 'E_INDEX_CORRUPT'
				| 'E_INDEX_FORMAT_INCOMPATIBLE'
				| 'E_SCHEMA_MISMATCH';
	  };

export interface NativeFullTextIndexResetOptions {
	path: string;
	indexId: string;
}

export type NativeFullTextIndexResetResult = { state: 'missing' } | { state: 'reset'; retiredPath: string };

export type NativeFullTextReclaimResult = { removed: number; failed: number };

export interface FullTextMutationBatch {
	upserts?: Array<{ id: string; fields: Record<string, string | string[]> }>;
	deletes?: string[];
}

export interface FullTextMutationBatchRejection {
	operation: 'upsert' | 'delete';
	/** Zero-based index in the corresponding `upserts` or `deletes` input array. */
	index: number;
	code: 'E_INVALID_ARGUMENT' | 'E_BATCH_TOO_LARGE';
}

export interface EncodedFullTextMutationBatch {
	bytes: Uint8Array;
	mutationCount: number;
}

export interface EncodedFullTextMutationBatches {
	batches: EncodedFullTextMutationBatch[];
	rejected: FullTextMutationBatchRejection[];
	consumedUpserts: number;
	consumedDeletes: number;
}

export interface EncodeFullTextMutationBatchesOptions {
	maxTotalBytes?: number;
	allowPartial?: boolean;
}

export interface SearchRequest {
	text: string;
	operator?: 'any' | 'all';
	fields?: string[];
	offset?: number;
	limit?: number;
	exactTotal?: boolean;
}

export interface SearchResult {
	total: number;
	totalRelation: 'exact' | 'lower-bound';
	hits: Array<{ id: string; score: number }>;
}

export interface FullTextStatus {
	state: 'open' | 'closing' | 'closed' | 'poisoned';
	uncommittedMutations: bigint;
	writerQueuedCommands: bigint;
	writerQueuedBytes: bigint;
	searchQueuedCommands: bigint;
	searchQueuedBytes: bigint;
	commitOpstamp: bigint;
	metrics: {
		writerQueueNanoseconds: bigint;
		writerExecutionNanoseconds: bigint;
		searchQueueNanoseconds: bigint;
		searchExecutionNanoseconds: bigint;
	};
}

export interface CloseOptions {
	mode?: 'require-clean' | 'rollback';
}

export type CloseResult = { cleanupError?: FulltextError };

export class NativeFullTextIndex {
	readonly #handle: number;
	readonly #publication: PublicationState;
	readonly #maxBatchBytes: number;
	readonly #fieldNames: ReadonlySet<string>;
	#closed = false;
	#closedStatus?: FullTextStatus;
	#closePromise?: Promise<CloseResult>;

	constructor(options: {
		handle: number;
		committedPayload?: string;
		maxBatchBytes: number;
		fieldNames: Iterable<string>;
	}) {
		this.#handle = options.handle;
		this.#publication = new PublicationState(options.committedPayload);
		this.#maxBatchBytes = options.maxBatchBytes;
		this.#fieldNames = new Set(options.fieldNames);
	}

	get committedPayload(): string | undefined {
		return this.#publication.committedPayload;
	}

	async apply(packedBatch: Uint8Array): Promise<number> {
		const cursor = await invoke((callback) => loadAddon().__nativeApply(this.#handle, asBuffer(packedBatch), callback));
		const count = safeNumber(cursor.u64(), 'mutation count');
		cursor.finish();
		return count;
	}

	encodeMutationBatches(
		batch: FullTextMutationBatch,
		options: EncodeFullTextMutationBatchesOptions = {},
	): EncodedFullTextMutationBatches {
		try {
			if (options.allowPartial !== undefined && typeof options.allowPartial !== 'boolean') {
				throw new FulltextError('E_INVALID_ARGUMENT', 'allowPartial must be a boolean');
			}
			const logical = { upserts: batch.upserts ?? [], deletes: batch.deletes ?? [] };
			const encoded = encodeBatchPartitions(
				logical,
				this.#maxBatchBytes,
				options.maxTotalBytes ?? 64 * 1024 * 1024,
				this.#fieldNames,
			);
			if (
				options.allowPartial !== true &&
				(encoded.consumedUpserts < logical.upserts.length || encoded.consumedDeletes < logical.deletes.length)
			) {
				throw new FulltextError('E_BATCH_TOO_LARGE', 'logical mutation batch exceeds its total encoding limit');
			}
			return encoded;
		} catch (error) {
			if (error instanceof FulltextError) throw error;
			throw normalizeNativeError(error);
		}
	}

	async commit(): Promise<bigint> {
		const cursor = await invoke((callback) => loadAddon().__nativeCommit(this.#handle, callback));
		const opstamp = cursor.u64();
		cursor.finish();
		return opstamp;
	}

	async publish(payload: string): Promise<bigint> {
		if (typeof payload !== 'string') {
			throw new FulltextError('E_INVALID_ARGUMENT', 'commit payload must be a string');
		}
		if (payload.length > maxCommitPayloadBytes || Buffer.byteLength(payload) > maxCommitPayloadBytes) {
			throw new FulltextError('E_INVALID_ARGUMENT', `commit payload exceeds ${maxCommitPayloadBytes} UTF-8 bytes`);
		}
		if (/[\uD800-\uDFFF]/u.test(payload)) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'commit payload must be a well-formed Unicode string');
		}
		const sequence = this.#publication.begin();
		let admitted = false;
		try {
			const cursor = await invoke((callback) => {
				loadAddon().__nativePublish(this.#handle, payload, callback);
				admitted = true;
			});
			const opstamp = cursor.u64();
			cursor.finish();
			this.#publication.succeed(sequence, payload);
			return opstamp;
		} catch (error) {
			if (admitted) this.#publication.fail(sequence);
			throw error;
		}
	}

	async reload(): Promise<void> {
		const cursor = await invoke((callback) => loadAddon().__nativeReload(this.#handle, callback));
		cursor.finish();
	}

	async search(request: SearchRequest): Promise<SearchResult> {
		const cursor = await invoke((callback) =>
			loadAddon().__nativeSearch(
				this.#handle,
				encodeSearch({
					text: request.text,
					operator: request.operator ?? 'any',
					fields: request.fields ?? [],
					offset: request.offset ?? 0,
					limit: request.limit ?? 20,
					exactTotal: request.exactTotal ?? false,
				}),
				callback,
			),
		);
		const total = safeNumber(cursor.u64(), 'search total');
		const relation = cursor.u8();
		const hitCount = cursor.u32();
		const hits = Array.from({ length: hitCount }, () => ({ score: cursor.f32(), id: cursor.string() }));
		cursor.finish();
		if (relation !== 0 && relation !== 1) {
			throw new FulltextError('E_NATIVE_FAILURE', `Unknown total relation ${relation}`);
		}
		return {
			total,
			totalRelation: relation === 0 ? 'exact' : 'lower-bound',
			hits,
		};
	}

	status(): FullTextStatus {
		if (this.#closedStatus) {
			return this.#closedStatus;
		}
		try {
			const cursor = decodeResponse(loadAddon().__nativeStatus(this.#handle));
			const state = cursor.u8();
			const status: FullTextStatus = {
				state: ['open', 'closing', 'closed', 'poisoned'][state] as FullTextStatus['state'],
				uncommittedMutations: cursor.u64(),
				writerQueuedCommands: cursor.u64(),
				writerQueuedBytes: cursor.u64(),
				searchQueuedCommands: cursor.u64(),
				searchQueuedBytes: cursor.u64(),
				commitOpstamp: cursor.u64(),
				metrics: {
					writerQueueNanoseconds: cursor.u64(),
					writerExecutionNanoseconds: cursor.u64(),
					searchQueueNanoseconds: cursor.u64(),
					searchExecutionNanoseconds: cursor.u64(),
				},
			};
			cursor.finish();
			if (state > 3) {
				throw new FulltextError('E_NATIVE_FAILURE', `Unknown native lifecycle state ${state}`);
			}
			return status;
		} catch (error) {
			throw normalizeNativeError(error);
		}
	}

	async close(options: CloseOptions = {}): Promise<CloseResult> {
		if (this.#closePromise) {
			return this.#closePromise;
		}
		if (this.#closed) {
			return {};
		}
		const openStatus = this.status();
		this.#closePromise = (async () => {
			try {
				const cursor = await invoke((callback) =>
					loadAddon().__nativeClose(this.#handle, options.mode === 'rollback', callback),
				);
				cursor.finish();
				this.#closed = true;
				this.#closedStatus = { ...openStatus, state: 'closed' };
				return {};
			} catch (error) {
				const nativeError = normalizeNativeError(error);
				if (nativeError.code === 'E_CLOSE_FAILED') {
					this.#closed = true;
					this.#closedStatus = { ...openStatus, state: 'closed' };
					return { cleanupError: nativeError };
				} else if (nativeError.code === 'E_QUIESCENCE_FAILED') {
					this.#closed = true;
					this.#closedStatus = { ...openStatus, state: 'poisoned' };
				}
				throw nativeError;
			}
		})();
		try {
			return await this.#closePromise;
		} finally {
			if (!this.#closed) {
				this.#closePromise = undefined;
			}
		}
	}
}

export function encodeMutationBatch(batch: FullTextMutationBatch, maxBytes = 8 * 1024 * 1024): Uint8Array {
	return encodeBatch({ upserts: batch.upserts ?? [], deletes: batch.deletes ?? [] }, maxBytes);
}

export function inspectNativeFullTextIndex(
	options: NativeFullTextIndexInspectionOptions,
): NativeFullTextIndexInspection {
	try {
		const cursor = decodeResponse(loadAddon().__nativeInspect(encodeInspect(packedInspectionOptions(options))));
		const state = cursor.u8();
		if (state === 0) {
			cursor.finish();
			return { state: 'missing' };
		}
		if (state === 1) {
			cursor.finish();
			return { state: 'cursorless' };
		}
		if (state === 2) {
			const committedPayload = cursor.string();
			cursor.finish();
			return { state: 'checkpointed', committedPayload };
		}
		throw new FulltextError('E_NATIVE_FAILURE', `Unknown native inspection state ${state}`);
	} catch (error) {
		const nativeError = normalizeNativeError(error);
		if (
			nativeError.code === 'E_IDENTITY_MISMATCH' ||
			nativeError.code === 'E_INCOMPLETE_CREATE' ||
			nativeError.code === 'E_INDEX_CORRUPT' ||
			nativeError.code === 'E_INDEX_FORMAT_INCOMPATIBLE' ||
			nativeError.code === 'E_SCHEMA_MISMATCH'
		) {
			return { state: 'incompatible', code: nativeError.code };
		}
		throw nativeError;
	}
}

export async function resetNativeFullTextIndex(
	options: NativeFullTextIndexResetOptions,
): Promise<NativeFullTextIndexResetResult> {
	const cursor = await invoke((callback) => loadAddon().__nativeReset(encodeReset(options), callback));
	const state = cursor.u8();
	if (state === 0) {
		cursor.finish();
		return { state: 'missing' };
	}
	if (state === 1) {
		const retiredPath = cursor.string();
		cursor.finish();
		return { state: 'reset', retiredPath };
	}
	throw new FulltextError('E_NATIVE_FAILURE', `Unknown native reset state ${state}`);
}

export async function reclaimRetiredNativeFullTextIndexes(options: {
	path: string;
}): Promise<NativeFullTextReclaimResult> {
	if (!options || typeof options.path !== 'string' || options.path.length === 0) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'path must not be empty');
	}
	const livePath = resolve(options.path);
	const retiredRoot = join(dirname(livePath), '.fulltext-retired');
	let canonicalRoot: string;
	let entries;
	try {
		const stats = await lstat(retiredRoot);
		if (!stats.isDirectory() || stats.isSymbolicLink()) {
			throw new FulltextError(
				'E_INVALID_ARGUMENT',
				'the .fulltext-retired path must be a directory and must not be a symbolic link',
			);
		}
		canonicalRoot = await realpath(retiredRoot);
		entries = await readdir(canonicalRoot, { withFileTypes: true });
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === 'ENOENT') return { removed: 0, failed: 0 };
		if (error instanceof FulltextError) throw error;
		throw new FulltextError('E_STORAGE', `could not inspect retired full-text indexes for ${livePath}`, error);
	}
	const generatedName = new RegExp(`^${escapeRegExp(basename(livePath))}\\.\\d+\\.\\d+\\.\\d+\\.\\d+$`);
	let removed = 0;
	let failed = 0;
	for (const entry of entries) {
		if (!generatedName.test(entry.name)) continue;
		try {
			await rm(join(canonicalRoot, entry.name), { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
			removed++;
		} catch {
			failed++;
		}
	}
	return { removed, failed };
}

export function validateNativeFullTextIndexOptions(options: NativeFullTextIndexOptions): void {
	const cursor = decodeResponse(loadAddon().__nativeValidateOpen(packedOpenOptions(options)));
	cursor.finish();
}

export async function openNativeFullTextIndex(options: NativeFullTextIndexOptions): Promise<NativeFullTextIndex> {
	const config = packedOptions(options);
	config.limits = { ...config.limits };
	const packed = encodeOpen(config);
	const validation = decodeResponse(loadAddon().__nativeValidateOpen(packed));
	validation.finish();
	const cursor = await invoke((callback) => loadAddon().__nativeOpen(packed, callback));
	const handle = cursor.u32();
	try {
		const hasPayload = cursor.u8();
		if (hasPayload !== 0 && hasPayload !== 1) {
			throw new FulltextError('E_NATIVE_FAILURE', `Unknown committed payload status ${hasPayload}`);
		}
		const payload = hasPayload === 1 ? cursor.string() : undefined;
		cursor.finish();
		return new NativeFullTextIndex({
			handle,
			committedPayload: payload,
			maxBatchBytes: config.limits.maxBatchBytes,
			fieldNames: config.fields.map((field) => field.name),
		});
	} catch (error) {
		await invoke((callback) => loadAddon().__nativeClose(handle, true, callback)).catch(() => undefined);
		throw error;
	}
}

function packedOpenOptions(options: NativeFullTextIndexOptions): Buffer {
	const config = packedOptions(options);
	config.limits = { ...config.limits };
	return encodeOpen(config);
}

function packedOptions(options: NativeFullTextIndexOptions) {
	return {
		...packedIndexIdentity(options),
		limits: options.limits,
	};
}

function packedInspectionOptions(options: NativeFullTextIndexInspectionOptions) {
	return packedIndexIdentity(options);
}

function packedIndexIdentity(options: NativeFullTextIndexInspectionOptions) {
	return {
		...options,
		fields: options.fields.map((field) => ({ name: field.name, weight: field.weight ?? 1 })),
		stopWords: options.stopWords ?? true,
		positions: options.positions ?? true,
		surfaceTerms: options.surfaceTerms ?? false,
	};
}

export async function runtimeInfo(): Promise<RuntimeInfo> {
	try {
		const info = loadAddon().runtimeInfo();
		return {
			packageVersion: info.packageVersion,
			tantivyVersion: info.tantivyVersion,
			nativeAbiVersion: info.nativeAbiVersion,
			lifecycleApiVersion: 1,
			mutationBatchApiVersion: 2,
			storageBackends: ['native'],
			limits: { maxCommitPayloadBytes: info.limits.maxCommitPayloadBytes },
		};
	} catch (error) {
		throw normalizeNativeError(error);
	}
}

function asBuffer(value: Uint8Array): Buffer {
	return Buffer.isBuffer(value) ? value : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
}

function safeNumber(value: bigint, name: string): number {
	if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
		throw new FulltextError('E_NATIVE_FAILURE', `${name} exceeds JavaScript's safe integer range`);
	}
	return Number(value);
}

function escapeRegExp(value: string): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
