import { FulltextError, normalizeNativeError } from './errors.js';
import { decodeResponse, encodeBatch, encodeOpen, encodeSearch } from './codec.js';
import { invoke } from './invoke.js';
import { loadAddon } from './load-addon.js';

export { FulltextError } from './errors.js';
export type { FulltextErrorCode } from './errors.js';

export interface RuntimeInfo {
	packageVersion: string;
	tantivyVersion: string;
	nativeAbiVersion: number;
	storageBackends: ReadonlyArray<'native' | 'harper'>;
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
	limits: {
		indexingThreads: number;
		searchThreads: number;
		writerMemoryBytes: number;
		maxQueuedCommands: number;
		maxQueuedBytes: number;
		maxBatchBytes: number;
	};
}

export interface FullTextMutationBatch {
	upserts?: Array<{ id: string; fields: Record<string, string | string[]> }>;
	deletes?: string[];
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

export class NativeFullTextIndex {
	readonly #handle: number;
	#closed = false;
	#closedStatus?: FullTextStatus;
	#closePromise?: Promise<void>;

	constructor(handle: number) {
		this.#handle = handle;
	}

	async apply(packedBatch: Uint8Array): Promise<number> {
		const cursor = await invoke((callback) => loadAddon().__nativeApply(this.#handle, asBuffer(packedBatch), callback));
		const count = safeNumber(cursor.u64(), 'mutation count');
		cursor.finish();
		return count;
	}

	async commit(): Promise<bigint> {
		const cursor = await invoke((callback) => loadAddon().__nativeCommit(this.#handle, callback));
		const opstamp = cursor.u64();
		cursor.finish();
		return opstamp;
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

	async close(options: CloseOptions = {}): Promise<void> {
		if (this.#closed) {
			return;
		}
		if (this.#closePromise) {
			return this.#closePromise;
		}
		const openStatus = this.status();
		this.#closePromise = (async () => {
			const cursor = await invoke((callback) =>
				loadAddon().__nativeClose(this.#handle, options.mode === 'rollback', callback),
			);
			cursor.finish();
			this.#closed = true;
			this.#closedStatus = { ...openStatus, state: 'closed' };
		})();
		try {
			await this.#closePromise;
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

export async function openNativeFullTextIndex(options: NativeFullTextIndexOptions): Promise<NativeFullTextIndex> {
	const cursor = await invoke((callback) =>
		loadAddon().__nativeOpen(
			encodeOpen({
				...options,
				fields: options.fields.map((field) => ({ name: field.name, weight: field.weight ?? 1 })),
				stopWords: options.stopWords ?? true,
				positions: options.positions ?? true,
				surfaceTerms: options.surfaceTerms ?? false,
			}),
			callback,
		),
	);
	const handle = cursor.u32();
	cursor.finish();
	return new NativeFullTextIndex(handle);
}

export async function runtimeInfo(): Promise<RuntimeInfo> {
	try {
		const info = loadAddon().runtimeInfo();
		return {
			packageVersion: info.packageVersion,
			tantivyVersion: info.tantivyVersion,
			nativeAbiVersion: info.nativeAbiVersion,
			storageBackends: ['native', 'harper'],
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
