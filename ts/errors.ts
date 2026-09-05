const errorCodes = [
	'E_NATIVE_ADDON_NOT_FOUND',
	'E_NATIVE_ABI_MISMATCH',
	'E_NATIVE_CAPABILITY_MISMATCH',
	'E_NATIVE_PANIC',
	'E_POISONED',
	'E_NATIVE_FAILURE',
] as const;

export type FulltextErrorCode = (typeof errorCodes)[number];

export class FulltextError extends Error {
	readonly code: FulltextErrorCode;

	constructor(code: FulltextErrorCode, message: string, cause?: unknown) {
		super(message, { cause });
		this.name = 'FulltextError';
		this.code = code;
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
	return typeof code === 'string' && errorCodes.includes(code as FulltextErrorCode)
		? (code as FulltextErrorCode)
		: undefined;
}
