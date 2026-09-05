export type FulltextErrorCode = 'E_NATIVE_ADDON_NOT_FOUND' | 'E_NATIVE_PANIC' | 'E_POISONED' | 'E_NATIVE_FAILURE';

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
	const match = /^\[(E_[A-Z_]+)]\s*(.*)$/.exec(message);
	if (match && isErrorCode(match[1])) {
		return new FulltextError(match[1], match[2] || match[1], error);
	}
	return new FulltextError('E_NATIVE_FAILURE', message, error);
}

function isErrorCode(value: string): value is FulltextErrorCode {
	return value === 'E_NATIVE_ADDON_NOT_FOUND' || value === 'E_NATIVE_PANIC' || value === 'E_POISONED';
}
