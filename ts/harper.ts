import { encodeHostOpen } from './codec.js';
import { FulltextError } from './errors.js';
import { createHostStorageHandler } from './host-storage.js';
import type { HostStorage } from './host-storage.js';
import { invoke } from './invoke.js';
import { loadAddon } from './load-addon.js';
import { NativeFullTextIndex } from './native.js';
import type {
	CloseOptions,
	FullTextStatus,
	NativeFullTextIndexOptions,
	SearchRequest,
	SearchResult,
} from './native.js';

export { encodeMutationBatch, FulltextError, runtimeInfo } from './native.js';
export type {
	CloseOptions,
	FullTextMutationBatch,
	FullTextStatus,
	RuntimeInfo,
	SearchRequest,
	SearchResult,
} from './native.js';
export type { HostStorage, HostStorageMutation, HostWritePolicy } from './host-storage.js';

const maxCommitPayloadBytes = 64 * 1024;

export interface HarperFullTextIndexOptions extends Omit<NativeFullTextIndexOptions, 'path'> {
	storage: HostStorage;
	storeIdentity: readonly [bigint, bigint, bigint];
	namespace: Uint8Array;
	transport: {
		maxOperations: number;
		maxBytes: number;
		readTimeoutMs: number;
		maxMutations: number;
		maxReadResponseBytes: number;
		maxControlResponseBytes: number;
		maxErrorBytes: number;
	};
}

class StorageGate implements HostStorage {
	readonly #storage: HostStorage;
	#active = true;

	constructor(storage: HostStorage) {
		this.#storage = storage;
	}

	read(key: Buffer): Buffer | undefined {
		this.#requireActive();
		return this.#storage.read(key);
	}

	write(mutations: Parameters<HostStorage['write']>[0], policy: Parameters<HostStorage['write']>[1]): undefined {
		this.#requireActive();
		return this.#storage.write(mutations, policy);
	}

	sync(): undefined {
		this.#requireActive();
		return this.#storage.sync();
	}

	revoke(): void {
		this.#active = false;
	}

	#requireActive(): void {
		if (!this.#active) throw new FulltextError('E_CLOSED', 'Harper storage generation is closed');
	}
}

export class HarperFullTextIndex {
	readonly #handle: number;
	readonly #index: NativeFullTextIndex;
	readonly #storageGate: StorageGate;
	#committedPayload?: string;
	#payloadKnown = true;
	#nextPublishSequence = 0n;
	#publishedSequence = 0n;

	constructor(handle: number, committedPayload: string | undefined, storageGate: StorageGate) {
		this.#handle = handle;
		this.#index = new NativeFullTextIndex(handle);
		this.#committedPayload = committedPayload;
		this.#storageGate = storageGate;
	}

	get committedPayload(): string | undefined {
		if (!this.#payloadKnown) {
			throw new FulltextError('E_POISONED', 'committed payload is unknown until the index is reopened');
		}
		return this.#committedPayload;
	}

	apply(packedBatch: Uint8Array): Promise<number> {
		return this.#index.apply(packedBatch);
	}

	async publish(payload: string): Promise<bigint> {
		if (typeof payload !== 'string') throw new FulltextError('E_INVALID_ARGUMENT', 'commit payload must be a string');
		if (Buffer.byteLength(payload) > maxCommitPayloadBytes) {
			throw new FulltextError('E_INVALID_ARGUMENT', `commit payload exceeds ${maxCommitPayloadBytes} UTF-8 bytes`);
		}
		const sequence = ++this.#nextPublishSequence;
		let cursor;
		try {
			cursor = await invoke((callback) => loadAddon().__harperPublish(this.#handle, payload, callback));
		} catch (error) {
			try {
				this.#payloadKnown = this.#index.status().state !== 'poisoned';
			} catch {
				this.#payloadKnown = false;
			}
			throw error;
		}
		const opstamp = cursor.u64();
		cursor.finish();
		if (sequence > this.#publishedSequence) {
			this.#publishedSequence = sequence;
			this.#committedPayload = payload;
		}
		return opstamp;
	}

	search(request: SearchRequest): Promise<SearchResult> {
		return this.#index.search(request);
	}

	status(): FullTextStatus {
		return this.#index.status();
	}

	async close(options: CloseOptions = {}): Promise<void> {
		await this.#index.close(options);
		this.#storageGate.revoke();
	}
}

export async function openHarperFullTextIndex(options: HarperFullTextIndexOptions): Promise<HarperFullTextIndex> {
	validateSynchronousStorage(options.storage);
	const gate = new StorageGate(options.storage);
	const handler = createHostStorageHandler(gate, {
		maxMutations: options.transport.maxMutations,
		maxReadResponseBytes: options.transport.maxReadResponseBytes,
		maxControlResponseBytes: options.transport.maxControlResponseBytes,
		maxErrorBytes: options.transport.maxErrorBytes,
	});
	const dispatch = createStorageDispatcher(handler);
	let openedHandle: number | undefined;
	try {
		const cursor = await invoke((callback) =>
			loadAddon().__harperOpen(
				encodeHostOpen({
					storeIdentity: options.storeIdentity,
					namespace: options.namespace,
					indexId: options.indexId,
					generation: options.generation,
					fields: options.fields.map((field) => ({ name: field.name, weight: field.weight ?? 1 })),
					analyzer: options.analyzer,
					stopWords: options.stopWords ?? true,
					positions: options.positions ?? true,
					surfaceTerms: options.surfaceTerms ?? false,
					limits: options.limits,
					transport: options.transport,
				}),
				dispatch,
				callback,
			),
		);
		const handle = cursor.u32();
		openedHandle = handle;
		const hasPayload = cursor.u8();
		if (hasPayload !== 0 && hasPayload !== 1) {
			throw new FulltextError('E_NATIVE_FAILURE', `Unknown committed payload status ${hasPayload}`);
		}
		const committedPayload = hasPayload === 1 ? cursor.string() : undefined;
		cursor.finish();
		return new HarperFullTextIndex(handle, committedPayload, gate);
	} catch (error) {
		const handle = openedHandle;
		if (handle !== undefined) {
			await invoke((callback) => loadAddon().__nativeClose(handle, true, callback)).catch(() => undefined);
		}
		gate.revoke();
		throw error;
	}
}

function validateSynchronousStorage(storage: HostStorage): void {
	for (const name of ['read', 'write', 'sync'] as const) {
		const method = storage?.[name];
		if (typeof method !== 'function') {
			throw new FulltextError('E_INVALID_ARGUMENT', `host storage ${name} must be a function`);
		}
		if (method.constructor?.name === 'AsyncFunction') {
			throw new FulltextError('E_INVALID_ARGUMENT', `host storage ${name} must be synchronous`);
		}
	}
}

function createStorageDispatcher(handler: (request: Buffer) => Buffer): (dispatchId: Buffer, request: Buffer) => void {
	const addon = loadAddon();
	return (dispatchId, request) => {
		try {
			addon.__hostStorageComplete(dispatchId, handler(request));
		} catch (error) {
			try {
				addon.__hostStorageFail(dispatchId, error instanceof Error ? error.message : String(error));
			} catch {
				// Transport teardown resolves any request that cannot be failed here.
			}
		}
	};
}
