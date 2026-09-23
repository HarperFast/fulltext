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
	encodeTrace,
	MutationBatchFrameCursor,
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
	queryApiVersion: 1;
	queryClassIsolationMinimumSearchThreads: 2;
	lifecycleApiVersion: 1;
	mutationBatchApiVersion: 3;
	storageBackends: ReadonlyArray<'native'>;
	limits: {
		maxCommitPayloadBytes: number;
		maxQueryTextBytes: number;
		maxQueryTerms: number;
		maxQueryClauses: number;
		maxCandidateIds: number;
		maxCandidateBytes: number;
		maxPrefixExpansions: number;
		maxFuzzyTerms: number;
		maxSearchWindow: number;
		maxAutocompleteResults: number;
		maxSearchRequestBytes: number;
		maxSearchResponseBytes: number;
		maxSearchBudgetMilliseconds: number;
		maxTraceRecords: number;
		maxTraceSourceBytes: number;
		maxTraceSpans: number;
	};
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

export interface ApplyFullTextMutationBatchOptions {
	/** The caller guarantees that IDs are distinct; behavior is undefined if that precondition is false. */
	assumeDistinctIds?: boolean;
	rejectedUpsert?: 'reject' | 'delete';
}

export interface AppliedFullTextMutationBatch {
	/** Original logical mutations handled, including rejected upserts replaced by deletes. */
	processed: number;
	rejected: FullTextMutationBatchRejection[];
	encodedBytes: number;
	frames: number;
}

export interface SearchRequest {
	text: string;
	mode?: 'any' | 'all' | 'phrase' | 'prefix' | 'fuzzy' | 'fuzzy-prefix';
	operator?: 'any' | 'all';
	fields?: string[];
	candidateIds?: string[];
	offset?: number;
	limit?: number;
	exactTotal?: boolean;
}

export interface SearchResult {
	total: number;
	totalRelation: 'exact' | 'lower-bound';
	hits: Array<{ id: string; score: number }>;
}

export interface SearchExecutionOptions {
	/** Queue wait and native execution share this budget. The native ceiling is 30 seconds. */
	remainingBudgetMilliseconds?: number;
}

export interface TraceRecord {
	id: string;
	fields: Record<string, string | string[]>;
}

export interface TraceSpan {
	start: number;
	end: number;
}

export interface TraceFragment {
	text: string;
	start: number;
	spans: TraceSpan[];
}

export interface TraceValueMatch {
	field: string;
	valueIndex: number;
	spans: TraceSpan[];
	fragments?: TraceFragment[];
}

export interface TraceMatchesResult {
	complete: boolean;
	records: Array<{ id: string; values: TraceValueMatch[] }>;
}

export interface TraceMatchesOptions extends SearchExecutionOptions {
	/** Snippet generation is opt-in; match offsets are always returned. */
	snippets?: boolean;
	fragmentLength?: number;
	maxFragmentsPerValue?: number;
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
	#logicalMutationState: 'idle' | 'active' | 'incomplete' = 'idle';

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
		this.#assertLogicalMutationIdle();
		const buffer = asBuffer(packedBatch);
		this.#assertLogicalMutationIdle();
		return this.#applyPacked(buffer);
	}

	async #applyPacked(packedBatch: Uint8Array, onAdmitted?: () => void): Promise<number> {
		const cursor = await invoke((callback) => {
			loadAddon().__nativeApply(this.#handle, asBuffer(packedBatch), callback);
			onAdmitted?.();
		});
		const count = safeNumber(cursor.u64(), 'mutation count');
		cursor.finish();
		return count;
	}

	async applyMutationBatch(
		batch: FullTextMutationBatch,
		options: ApplyFullTextMutationBatchOptions = {},
	): Promise<AppliedFullTextMutationBatch> {
		this.#assertOpen();
		this.#assertLogicalMutationIdle();
		this.#logicalMutationState = 'active';
		let nativeAttempted = false;
		try {
			if (!options || typeof options !== 'object' || Array.isArray(options)) {
				throw new FulltextError('E_INVALID_ARGUMENT', 'mutation batch options must be an object');
			}
			const assumeDistinctIds = options.assumeDistinctIds;
			const rejectedUpsertOption = options.rejectedUpsert;
			if (assumeDistinctIds !== undefined && typeof assumeDistinctIds !== 'boolean') {
				throw new FulltextError('E_INVALID_ARGUMENT', 'assumeDistinctIds must be a boolean');
			}
			if (
				rejectedUpsertOption !== undefined &&
				rejectedUpsertOption !== 'reject' &&
				rejectedUpsertOption !== 'delete'
			) {
				throw new FulltextError('E_INVALID_ARGUMENT', "rejectedUpsert must be 'reject' or 'delete'");
			}
			const logical = snapshotMutationBatch(batch);
			const rejectedUpsert = rejectedUpsertOption ?? 'reject';
			const cursor = new MutationBatchFrameCursor(logical, this.#maxBatchBytes, this.#fieldNames, {
				validateDistinctIds: assumeDistinctIds !== true,
				stopAfterFirstRejection: rejectedUpsert === 'reject',
				requireReplacementDeletes: rejectedUpsert === 'delete',
			});
			if (logical.upserts.length + logical.deletes.length === 0) {
				this.#logicalMutationState = 'idle';
				return { processed: 0, rejected: [], encodedBytes: 0, frames: 0 };
			}

			let processed = 0;
			let encodedBytes = 0;
			let frames = 0;
			const rejected: FullTextMutationBatchRejection[] = [];
			let done = false;
			while (!done) {
				const encoded = cursor.next();
				const consumed = encoded.consumedUpserts + encoded.consumedDeletes;
				if (consumed === 0) {
					throw new FulltextError('E_NATIVE_FAILURE', 'mutation batch partitioner made no progress');
				}
				if (encoded.rejected.length > 0) {
					if (rejectedUpsert === 'reject') {
						const rejection = encoded.rejected[0];
						throw new FulltextError(
							rejection.code,
							`mutation batch ${rejection.operation} at index ${rejection.index} was rejected`,
						);
					}
					for (const rejection of encoded.rejected) rejected.push(rejection);
				}
				if (encoded.batch) {
					const count = await this.#applyPacked(encoded.batch.bytes, () => {
						nativeAttempted = true;
					});
					if (count !== encoded.batch.mutationCount) {
						throw new FulltextError(
							'E_NATIVE_FAILURE',
							`native writer applied ${count} of ${encoded.batch.mutationCount} frame mutations`,
						);
					}
					encodedBytes += encoded.batch.bytes.byteLength;
					frames++;
				}
				if (encoded.rejected.length > 0) {
					const replacementDeletes = encoded.rejected.map((rejection) => {
						if (rejection.operation !== 'upsert') {
							throw new FulltextError(rejection.code, 'a rejected delete cannot be replaced safely');
						}
						const id = logical.upserts[rejection.index]?.id;
						if (typeof id !== 'string') {
							throw new FulltextError(rejection.code, `rejected upsert at index ${rejection.index} has no usable ID`);
						}
						return id;
					});
					const replacement = await this.#applyReplacementDeletes(replacementDeletes, () => {
						nativeAttempted = true;
					});
					encodedBytes += replacement.encodedBytes;
					frames += replacement.frames;
				}
				processed += consumed;
				done = encoded.done;
			}
			this.#logicalMutationState = 'idle';
			return { processed, rejected, encodedBytes, frames };
		} catch (error) {
			this.#logicalMutationState = nativeAttempted && !this.#closed ? 'incomplete' : 'idle';
			throw normalizeNativeError(error);
		}
	}

	async #applyReplacementDeletes(
		deletes: string[],
		onNativeAttempt: () => void,
	): Promise<{ encodedBytes: number; frames: number }> {
		let encodedBytes = 0;
		let frames = 0;
		const cursor = new MutationBatchFrameCursor({ upserts: [], deletes }, this.#maxBatchBytes, this.#fieldNames, {
			validateDistinctIds: false,
		});
		let done = false;
		while (!done) {
			const encoded = cursor.next();
			if (encoded.rejected.length > 0 || encoded.consumedDeletes === 0 || !encoded.batch) {
				throw new FulltextError(
					encoded.rejected[0]?.code ?? 'E_NATIVE_FAILURE',
					'rejected upsert IDs could not be encoded as replacement deletes',
				);
			}
			const frame = encoded.batch;
			const count = await this.#applyPacked(frame.bytes, onNativeAttempt);
			if (count !== frame.mutationCount) {
				throw new FulltextError(
					'E_NATIVE_FAILURE',
					`native writer applied ${count} of ${frame.mutationCount} replacement deletes`,
				);
			}
			encodedBytes += frame.bytes.byteLength;
			frames++;
			done = encoded.done;
		}
		return { encodedBytes, frames };
	}

	encodeMutationBatches(
		batch: FullTextMutationBatch,
		options: EncodeFullTextMutationBatchesOptions = {},
	): EncodedFullTextMutationBatches {
		try {
			if (!options || typeof options !== 'object' || Array.isArray(options)) {
				throw new FulltextError('E_INVALID_ARGUMENT', 'mutation batch options must be an object');
			}
			const allowPartial = options.allowPartial;
			const maxTotalBytes = options.maxTotalBytes ?? 64 * 1024 * 1024;
			if (allowPartial !== undefined && typeof allowPartial !== 'boolean') {
				throw new FulltextError('E_INVALID_ARGUMENT', 'allowPartial must be a boolean');
			}
			const logical = snapshotMutationBatch(batch);
			const encoded = encodeBatchPartitions(logical, this.#maxBatchBytes, maxTotalBytes, this.#fieldNames);
			if (
				allowPartial !== true &&
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
		this.#assertLogicalMutationIdle();
		const cursor = await invoke((callback) => loadAddon().__nativeCommit(this.#handle, callback));
		const opstamp = cursor.u64();
		cursor.finish();
		return opstamp;
	}

	async publish(payload: string): Promise<bigint> {
		this.#assertLogicalMutationIdle();
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
		this.#assertLogicalMutationIdle();
		const cursor = await invoke((callback) => loadAddon().__nativeReload(this.#handle, callback));
		cursor.finish();
	}

	async search(request: SearchRequest, options: SearchExecutionOptions = {}): Promise<SearchResult> {
		if (request.mode !== undefined && request.operator !== undefined) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'mode and operator are mutually exclusive');
		}
		const cursor = await invoke((callback) =>
			loadAddon().__nativeSearch(
				this.#handle,
				encodeSearch({
					text: request.text,
					mode: request.mode ?? request.operator ?? 'any',
					fields: request.fields ?? [],
					candidateIds: request.candidateIds,
					offset: request.offset ?? 0,
					limit: request.limit ?? 20,
					exactTotal: request.exactTotal ?? false,
					budgetMilliseconds: searchBudget(options.remainingBudgetMilliseconds),
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

	async traceMatches(
		request: SearchRequest,
		records: TraceRecord[],
		options: TraceMatchesOptions = {},
	): Promise<TraceMatchesResult> {
		if (request.mode !== undefined && request.operator !== undefined) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'mode and operator are mutually exclusive');
		}
		if (!Array.isArray(records) || records.length > 100) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'trace records must be an array with at most 100 entries');
		}
		const seenIds = new Set<string>();
		const sourceById = new Map<string, Record<string, string[]>>();
		const packedRecords = records.map((record) => {
			if (!record || typeof record !== 'object' || typeof record.id !== 'string' || record.id.length === 0) {
				throw new FulltextError('E_INVALID_ARGUMENT', 'trace records require a non-empty string id');
			}
			if (seenIds.has(record.id)) {
				throw new FulltextError('E_INVALID_ARGUMENT', `duplicate trace record ${record.id}`);
			}
			seenIds.add(record.id);
			if (!record.fields || typeof record.fields !== 'object' || Array.isArray(record.fields)) {
				throw new FulltextError('E_INVALID_ARGUMENT', 'trace record fields must be an object');
			}
			const normalized: Record<string, string[]> = {};
			const fields = Object.entries(record.fields).map(([name, input]) => {
				const values = typeof input === 'string' ? [input] : input;
				if (!Array.isArray(values) || values.some((value) => typeof value !== 'string')) {
					throw new FulltextError('E_INVALID_ARGUMENT', `trace field ${name} must contain strings`);
				}
				const snapshot = values.slice();
				normalized[name] = snapshot;
				return { name, values: snapshot };
			});
			sourceById.set(record.id, normalized);
			return { id: record.id, fields };
		});
		const snippets = options.snippets ?? false;
		const fragmentLength = options.fragmentLength ?? 160;
		const maxFragments = options.maxFragmentsPerValue ?? 3;
		if (!Number.isInteger(fragmentLength) || fragmentLength < 32 || fragmentLength > 512) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'fragmentLength must be an integer between 32 and 512');
		}
		if (!Number.isInteger(maxFragments) || maxFragments < 1 || maxFragments > 5) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'maxFragmentsPerValue must be an integer between 1 and 5');
		}
		const cursor = await invoke((callback) =>
			loadAddon().__nativeTraceMatches(
				this.#handle,
				encodeTrace({
					text: request.text,
					mode: request.mode ?? request.operator ?? 'any',
					fields: request.fields ?? [],
					candidateIds: request.candidateIds,
					records: packedRecords,
					budgetMilliseconds: searchBudget(options.remainingBudgetMilliseconds),
				}),
				callback,
			),
		);
		const complete = cursor.u8() === 1;
		const recordCount = cursor.u16();
		const matched = Array.from({ length: recordCount }, () => {
			const id = cursor.string();
			const valueCount = cursor.u16();
			const values = Array.from({ length: valueCount }, () => {
				const field = cursor.string();
				const valueIndex = cursor.u32();
				const spanCount = cursor.u16();
				const spans = Array.from({ length: spanCount }, () => ({ start: cursor.u32(), end: cursor.u32() }));
				const value = sourceById.get(id)?.[field]?.[valueIndex];
				return {
					field,
					valueIndex,
					spans,
					...(snippets && value !== undefined
						? { fragments: traceFragments(value, spans, fragmentLength, maxFragments) }
						: {}),
				};
			});
			return { id, values };
		});
		cursor.finish();
		return { complete, records: matched };
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
		if (!options || typeof options !== 'object' || Array.isArray(options)) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'close options must be an object');
		}
		const mode = options.mode;
		if (mode !== undefined && mode !== 'require-clean' && mode !== 'rollback') {
			throw new FulltextError('E_INVALID_ARGUMENT', 'close mode must be require-clean or rollback');
		}
		if (this.#closePromise) return this.#closePromise;
		if (this.#closed) return {};
		const rollback = mode === 'rollback';
		if (!rollback) this.#assertLogicalMutationIdle();
		const openStatus = this.status();
		this.#closePromise = (async () => {
			try {
				const cursor = await invoke((callback) => loadAddon().__nativeClose(this.#handle, rollback, callback));
				cursor.finish();
				this.#closed = true;
				this.#logicalMutationState = 'idle';
				this.#closedStatus = { ...openStatus, state: 'closed' };
				return {};
			} catch (error) {
				const nativeError = normalizeNativeError(error);
				if (nativeError.code === 'E_CLOSE_FAILED') {
					this.#closed = true;
					this.#logicalMutationState = 'idle';
					this.#closedStatus = { ...openStatus, state: 'closed' };
					return { cleanupError: nativeError };
				} else if (nativeError.code === 'E_QUIESCENCE_FAILED') {
					this.#closed = true;
					this.#logicalMutationState = 'idle';
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

	#assertLogicalMutationIdle(): void {
		if (this.#logicalMutationState !== 'idle') {
			throw new FulltextError(
				this.#logicalMutationState === 'active' ? 'E_BATCH_ACTIVE' : 'E_BATCH_INCOMPLETE',
				this.#logicalMutationState === 'active'
					? 'a logical mutation batch is active'
					: 'a logical mutation batch failed and the index must be rollback-closed',
			);
		}
	}

	#assertOpen(): void {
		if (this.#closed || this.#closePromise) {
			throw new FulltextError('E_CLOSED', 'index is closing or closed');
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
	retiredPath?: string;
}): Promise<NativeFullTextReclaimResult> {
	if (!options || typeof options.path !== 'string' || options.path.length === 0) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'path must not be empty');
	}
	const livePath = resolve(options.path);
	const retiredRoot = join(dirname(livePath), '.fulltext-retired');
	const sourceNames = new Set([basename(livePath)]);
	let retiredPath: string | undefined;
	if (options.retiredPath !== undefined) {
		if (typeof options.retiredPath !== 'string' || options.retiredPath.length === 0) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'retiredPath must not be empty');
		}
		retiredPath = resolve(options.retiredPath);
		let expectedRetiredRoot: string;
		let suppliedRetiredRoot: string;
		try {
			[expectedRetiredRoot, suppliedRetiredRoot] = await Promise.all([
				canonicalSiblingPath(retiredRoot),
				canonicalSiblingPath(dirname(retiredPath)),
			]);
		} catch (error) {
			throw new FulltextError('E_STORAGE', `could not resolve retired full-text path ${retiredPath}`, error);
		}
		if (suppliedRetiredRoot !== expectedRetiredRoot) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'retiredPath must be inside the index retirement directory');
		}
		const match = /^(.*)\.\d+\.\d+\.\d+\.\d+$/.exec(basename(retiredPath));
		if (!match || match[1].length === 0) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'retiredPath is not a generated full-text retirement path');
		}
		if (!sourceNames.has(match[1])) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'retiredPath does not belong to the requested full-text index');
		}
	}
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
	if (retiredPath !== undefined) {
		let retiredParent: string;
		try {
			retiredParent = await realpath(dirname(retiredPath));
		} catch (error) {
			throw new FulltextError('E_STORAGE', `could not resolve retired full-text path ${retiredPath}`, error);
		}
		if (retiredParent !== canonicalRoot) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'retiredPath must be inside the index retirement directory');
		}
	}
	const generatedName = new RegExp(`^(?:${[...sourceNames].map(escapeRegExp).join('|')})\\.\\d+\\.\\d+\\.\\d+\\.\\d+$`);
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
	try {
		const cursor = decodeResponse(loadAddon().__nativeValidateOpen(packedOpenOptions(options)));
		cursor.finish();
	} catch (error) {
		throw normalizeOptionsError(error);
	}
}

export async function openNativeFullTextIndex(options: NativeFullTextIndexOptions): Promise<NativeFullTextIndex> {
	let config: ReturnType<typeof packedOptions>;
	let packed: Buffer;
	try {
		config = packedOptions(options);
		config.limits = { ...config.limits };
		packed = encodeOpen(config);
		const validation = decodeResponse(loadAddon().__nativeValidateOpen(packed));
		validation.finish();
	} catch (error) {
		throw normalizeOptionsError(error);
	}
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

function normalizeOptionsError(error: unknown): FulltextError {
	return error instanceof TypeError
		? new FulltextError('E_INVALID_ARGUMENT', error.message, error)
		: normalizeNativeError(error);
}

export async function runtimeInfo(): Promise<RuntimeInfo> {
	try {
		const info = loadAddon().runtimeInfo();
		return {
			packageVersion: info.packageVersion,
			tantivyVersion: info.tantivyVersion,
			nativeAbiVersion: info.nativeAbiVersion,
			queryApiVersion: info.queryApiVersion as 1,
			queryClassIsolationMinimumSearchThreads: info.queryClassIsolationMinimumSearchThreads as 2,
			lifecycleApiVersion: 1,
			mutationBatchApiVersion: 3,
			storageBackends: ['native'],
			limits: { ...info.limits },
		};
	} catch (error) {
		throw normalizeNativeError(error);
	}
}

function asBuffer(value: Uint8Array): Buffer {
	return Buffer.isBuffer(value) ? value : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
}

function snapshotMutationBatch(batch: FullTextMutationBatch): Required<FullTextMutationBatch> {
	if (!batch || typeof batch !== 'object' || Array.isArray(batch)) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'mutation batch must be an object');
	}
	if (batch.upserts !== undefined && !Array.isArray(batch.upserts)) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'mutation batch upserts must be an array');
	}
	if (batch.deletes !== undefined && !Array.isArray(batch.deletes)) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'mutation batch deletes must be an array');
	}
	return {
		upserts:
			batch.upserts?.map((upsert) =>
				upsert && typeof upsert === 'object' ? { id: upsert.id, fields: upsert.fields } : upsert,
			) ?? [],
		deletes: batch.deletes?.slice() ?? [],
	};
}

function safeNumber(value: bigint, name: string): number {
	if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
		throw new FulltextError('E_NATIVE_FAILURE', `${name} exceeds JavaScript's safe integer range`);
	}
	return Number(value);
}

function searchBudget(value: number | undefined): number {
	if (value === undefined) return 30_000;
	if (!Number.isFinite(value) || value <= 0) {
		throw new FulltextError('E_INVALID_ARGUMENT', 'remainingBudgetMilliseconds must be greater than zero');
	}
	return Math.min(30_000, Math.floor(value));
}

function traceFragments(
	value: string,
	spans: TraceSpan[],
	fragmentLength: number,
	maxFragments: number,
): TraceFragment[] {
	const fragments: TraceFragment[] = [];
	for (const span of spans) {
		if (fragments.length >= maxFragments) break;
		let start = Math.max(0, span.start - Math.floor((fragmentLength - (span.end - span.start)) / 2));
		let end = Math.min(value.length, start + fragmentLength);
		start = Math.max(0, end - fragmentLength);
		if (start > 0 && isLowSurrogate(value.charCodeAt(start))) start--;
		if (end < value.length && isHighSurrogate(value.charCodeAt(end - 1))) end++;
		if (fragments.some((fragment) => start >= fragment.start && end <= fragment.start + fragment.text.length)) {
			continue;
		}
		const included = spans
			.filter((candidate) => candidate.start < end && candidate.end > start)
			.map((candidate) => ({
				start: Math.max(candidate.start, start) - start,
				end: Math.min(candidate.end, end) - start,
			}));
		fragments.push({ text: value.slice(start, end), start, spans: included });
	}
	return fragments;
}

function isHighSurrogate(value: number): boolean {
	return value >= 0xd800 && value <= 0xdbff;
}

function isLowSurrogate(value: number): boolean {
	return value >= 0xdc00 && value <= 0xdfff;
}

function escapeRegExp(value: string): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

async function canonicalSiblingPath(value: string): Promise<string> {
	try {
		return join(await realpath(dirname(value)), basename(value));
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === 'ENOENT') return value;
		throw error;
	}
}
