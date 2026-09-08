const errorCodes = [
	'E_NATIVE_ADDON_NOT_FOUND',
	'E_NATIVE_ABI_MISMATCH',
	'E_NATIVE_CAPABILITY_MISMATCH',
	'E_NATIVE_PANIC',
	'E_POISONED',
	'E_NATIVE_FAILURE',
	'E_CLOSED',
	'E_DIRTY_CLOSE',
	'E_DUPLICATE_OPEN',
	'E_IDENTITY_MISMATCH',
	'E_INCOMPLETE_CREATE',
	'E_INVALID_ARGUMENT',
	'E_LOCK_BUSY',
	'E_QUEUE_FULL',
	'E_SCHEMA_MISMATCH',
	'E_STORAGE',
] as const;

export type FulltextErrorCode = (typeof errorCodes)[number];

export class FulltextError extends Error {
	readonly code: FulltextErrorCode;

	constructor(code: FulltextErrorCode, message: string, cause?: unknown) {
		super(message, { cause });
		this.name = 'FulltextError';
		this.code = code;
	}

	static isCode(value: string): value is FulltextErrorCode {
		return errorCodes.includes(value as FulltextErrorCode);
	}
}

export function normalizeNativeError(error: unknown): FulltextError {
	if (error instanceof FulltextError) {
		return error;
	}
	const message = error instanceof Error ? error.message : String(error);
	const code = readErrorCode(error);
	if (code) {
		return new FulltextError(code, message || code, error);
	}
	return new FulltextError('E_NATIVE_FAILURE', message, error);
}

function readErrorCode(error: unknown): FulltextErrorCode | undefined {
	if (typeof error !== 'object' || error === null || !('code' in error)) {
		return undefined;
	}
	const code = error.code;
	return typeof code === 'string' && FulltextError.isCode(code) ? (code as FulltextErrorCode) : undefined;
}
