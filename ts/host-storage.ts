const protocolVersion = 1;
const operationRead = 1;
const operationWrite = 2;
const operationSync = 3;
const responseOk = 0;
const responseError = 1;
const valueMissing = 0;
const valuePresent = 1;
const mutationPut = 1;
const mutationDelete = 2;
const minimumMutationBytes = 5;
const fallbackError = Buffer.from([protocolVersion, responseError, 0, 0, 0, 0]);

export type HostWritePolicy = 'wal' | 'no-wal';

export type HostStorageMutation = { type: 'put'; key: Buffer; value: Buffer } | { type: 'delete'; key: Buffer };

export interface HostStorage {
	read(key: Buffer): Buffer | undefined;
	/** Apply all mutations or none, satisfy the policy, and return undefined synchronously. */
	write(mutations: Array<HostStorageMutation>, policy: HostWritePolicy): undefined;
	/** Make prior writes durable and return undefined synchronously. */
	sync(): undefined;
}

export interface HostStorageHandlerOptions {
	maxMutations: number;
	maxReadResponseBytes: number;
	maxControlResponseBytes: number;
	maxErrorBytes: number;
}

/** Creates the total callback required by the native transport; storage exceptions become protocol errors. */
export function createHostStorageHandler(
	storage: HostStorage,
	{ maxMutations, maxReadResponseBytes, maxControlResponseBytes, maxErrorBytes }: HostStorageHandlerOptions,
): (request: Buffer) => Buffer {
	validateLimit(maxMutations, 'maxMutations');
	validateLimit(maxReadResponseBytes, 'maxReadResponseBytes');
	validateLimit(maxControlResponseBytes, 'maxControlResponseBytes');
	validateLimit(maxErrorBytes, 'maxErrorBytes');
	if (
		maxReadResponseBytes < 7 ||
		maxControlResponseBytes < 2 ||
		maxErrorBytes > Math.min(maxReadResponseBytes, maxControlResponseBytes)
	) {
		throw new TypeError('response byte limits are inconsistent');
	}
	return (request) => {
		try {
			const decoder = new RequestDecoder(request);
			if (decoder.u8() !== protocolVersion) throw new Error('unsupported host storage protocol version');
			const operation = decoder.u8();
			if (operation === operationRead) {
				const key = decoder.bytes();
				decoder.finish();
				const value = storage.read(key);
				if (value === undefined) return Buffer.from([protocolVersion, responseOk, valueMissing]);
				if (!Buffer.isBuffer(value)) throw new Error('host storage read must return a Buffer or undefined');
				const responseBytes = 7 + value.length;
				if (responseBytes > maxReadResponseBytes) throw new Error('host storage read response exceeds its byte limit');
				const response = Buffer.allocUnsafe(responseBytes);
				response[0] = protocolVersion;
				response[1] = responseOk;
				response[2] = valuePresent;
				response.writeUInt32LE(value.length, 3);
				value.copy(response, 7);
				return response;
			}
			if (operation === operationWrite) {
				const policy = decodeWritePolicy(decoder.u8());
				const count = decoder.u32();
				if (count > maxMutations || count > Math.floor(decoder.remaining / minimumMutationBytes)) {
					throw new Error('host storage mutation count exceeds its limit');
				}
				const mutations = new Array<HostStorageMutation>(count);
				for (let index = 0; index < count; index++) {
					const type = decoder.u8();
					const key = decoder.bytes();
					if (type === mutationPut) {
						mutations[index] = { type: 'put', key, value: decoder.bytes() };
					} else if (type === mutationDelete) {
						mutations[index] = { type: 'delete', key };
					} else {
						throw new Error(`unknown host storage mutation type ${type}`);
					}
				}
				decoder.finish();
				if (storage.write(mutations, policy) !== undefined) {
					throw new Error('host storage write must return undefined synchronously');
				}
				return Buffer.from([protocolVersion, responseOk]);
			}
			if (operation === operationSync) {
				decoder.finish();
				if (storage.sync() !== undefined) {
					throw new Error('host storage sync must return undefined synchronously');
				}
				return Buffer.from([protocolVersion, responseOk]);
			}
			throw new Error(`unknown host storage operation ${operation}`);
		} catch (error) {
			try {
				return encodeError(error, maxErrorBytes);
			} catch {
				return fallbackError;
			}
		}
	};
}

function decodeWritePolicy(encoded: number): HostWritePolicy {
	if (encoded === 1) return 'wal';
	if (encoded === 3) return 'no-wal';
	throw new Error(`unknown host storage write policy ${encoded}`);
}

function encodeError(error: unknown, maxErrorBytes: number): Buffer {
	const encoded = Buffer.from(error instanceof Error ? error.message : String(error));
	const length = Math.min(encoded.length, maxErrorBytes - 6);
	const response = Buffer.allocUnsafe(6 + length);
	response[0] = protocolVersion;
	response[1] = responseError;
	response.writeUInt32LE(length, 2);
	encoded.copy(response, 6, 0, length);
	return response;
}

function validateLimit(value: number, name: string): void {
	if (!Number.isSafeInteger(value) || value <= 0 || (name === 'maxErrorBytes' && value < 6)) {
		throw new TypeError(`${name} must be a positive safe integer${name === 'maxErrorBytes' ? ' of at least 6' : ''}`);
	}
}

class RequestDecoder {
	readonly #bytes: Buffer;
	#offset = 0;

	constructor(bytes: Buffer) {
		if (!Buffer.isBuffer(bytes)) throw new TypeError('host storage request must be a Buffer');
		this.#bytes = bytes;
	}

	get remaining(): number {
		return this.#bytes.length - this.#offset;
	}

	u8(): number {
		if (this.remaining < 1) throw new Error('host storage request is truncated');
		return this.#bytes[this.#offset++];
	}

	u32(): number {
		if (this.remaining < 4) throw new Error('host storage request is truncated');
		const value = this.#bytes.readUInt32LE(this.#offset);
		this.#offset += 4;
		return value;
	}

	bytes(): Buffer {
		const length = this.u32();
		if (length > this.remaining) throw new Error('host storage request is truncated');
		const start = this.#offset;
		this.#offset += length;
		return this.#bytes.subarray(start, this.#offset);
	}

	finish(): void {
		if (this.remaining !== 0) throw new Error('host storage request has trailing bytes');
	}
}
