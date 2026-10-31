import { FulltextError, normalizeNativeError } from './errors.js';
import { loadAddon } from './load-addon.js';

export interface ConfigureTelemetryOptions {
	histograms?: boolean;
	snapshotBytes?: boolean;
}
export interface TelemetryHistogram {
	count: bigint;
	sumNanoseconds: bigint;
	buckets: bigint[];
	upperBoundsNanoseconds: Array<bigint | null>;
}
export interface TelemetryLatency {
	count: bigint;
	sumNanoseconds: bigint;
	histogram: TelemetryHistogram;
}
export interface TelemetryPhase extends TelemetryLatency {
	succeeded: bigint;
	failed: bigint;
}
export interface TelemetryOperation {
	attempted: bigint;
	accepted: bigint;
	rejected: bigint;
	dispatched: bigint;
	completed: bigint;
	succeeded: bigint;
	failed: bigint;
	requestBytes: bigint;
	mutations: bigint;
	errors: Record<string, bigint>;
	queue: TelemetryLatency;
	execution: TelemetryLatency;
}
export interface TelemetryQueue {
	available: boolean;
	commands: bigint;
	bytes: bigint;
	oldestAgeNanoseconds: bigint | null;
	maxCommands: bigint;
	maxBytes: bigint;
	sharedCapacity: boolean;
}
export interface NativeTelemetry {
	apiVersion: 1;
	histogramsEnabled: boolean;
	snapshotBytesEnabled: boolean;
	state: 'open' | 'closing' | 'closed' | 'poisoned';
	operations: Record<
		'applyFrame' | 'commit' | 'publish' | 'reload' | 'close' | 'search' | 'expensiveSearch' | 'trace',
		TelemetryOperation
	>;
	phases: Record<'commit' | 'readerReload' | 'teardown', TelemetryPhase>;
	reader: {
		available: boolean;
		documents: bigint;
		deletedDocuments: bigint;
		segments: bigint;
		generationId: bigint;
		ageNanoseconds: bigint | null;
		durationNanoseconds: bigint;
		error: string | null;
		snapshotBytes: bigint | null;
		snapshotBytesPending: boolean;
		snapshotBytesError: string | null;
		snapshotBytesDurationNanoseconds: bigint;
		snapshotBytesAgeNanoseconds: bigint | null;
	};
	queues: Record<'writer' | 'ordinarySearch' | 'expensiveSearch', TelemetryQueue>;
	unsupported: Record<'directoryTotalBytes' | 'automaticMergeStats' | 'perIndexRss' | 'nativeCancellation', string>;
}
export interface BatchSizeDistribution {
	count: bigint;
	sum: bigint;
	buckets: bigint[];
	upperBounds: Array<bigint | null>;
}
export interface FacadeTelemetrySnapshot {
	logicalBatches: {
		attempted: bigint;
		completed: bigint;
		succeeded: bigint;
		failed: bigint;
		requestedMutations: bigint;
		processedMutations: bigint;
		rejectedRecords: bigint;
		encodedFrameBytes: bigint;
		encodedFrames: bigint;
		requestedMutationsDistribution: BatchSizeDistribution;
		frameBytesDistribution: BatchSizeDistribution;
	};
	preparationRejections: Record<'search' | 'trace', { total: bigint; errors: Record<string, bigint> }>;
	cancellation: { available: false; reason: string };
}
export type FullTextTelemetry = FacadeTelemetrySnapshot &
	({ available: true; native: NativeTelemetry } | { available: false; reason: string });

const mutationBounds = [0n, 1n, 10n, 100n, 1_000n, 10_000n];
const byteBounds = [1_024n, 16_384n, 65_536n, 1_048_576n, 8_388_608n];
function distribution(bounds: bigint[]): BatchSizeDistribution {
	return { count: 0n, sum: 0n, buckets: bounds.map(() => 0n).concat(0n), upperBounds: [...bounds, null] };
}
function observe(distribution: BatchSizeDistribution, value: bigint): void {
	distribution.count++;
	distribution.sum += value;
	for (let index = 0; index < distribution.upperBounds.length; index++) {
		const bound = distribution.upperBounds[index];
		if (bound === null || value <= bound) {
			distribution.buckets[index]++;
			break;
		}
	}
}

export class FacadeTelemetry {
	readonly #token: unknown;
	readonly #batches = {
		attempted: 0n,
		completed: 0n,
		succeeded: 0n,
		failed: 0n,
		requestedMutations: 0n,
		processedMutations: 0n,
		rejectedRecords: 0n,
		encodedFrameBytes: 0n,
		encodedFrames: 0n,
		requestedMutationsDistribution: distribution(mutationBounds),
		frameBytesDistribution: distribution(byteBounds),
	};
	readonly #preparation = {
		search: { total: 0n, errors: {} as Record<string, bigint> },
		trace: { total: 0n, errors: {} as Record<string, bigint> },
	};
	#histograms = false;
	#snapshotBytes = false;
	constructor(handle: number) {
		try {
			this.#token = loadAddon().__nativeCreateTelemetry(handle);
		} catch {}
	}
	configure(options: ConfigureTelemetryOptions): void {
		if (!options || typeof options !== 'object' || Array.isArray(options)) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'telemetry options must be an object');
		}
		const keys = Object.keys(options);
		if (keys.some((key) => key !== 'histograms' && key !== 'snapshotBytes')) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'unknown telemetry option');
		}
		const { histograms, snapshotBytes } = options;
		if (
			(histograms !== undefined && typeof histograms !== 'boolean') ||
			(snapshotBytes !== undefined && typeof snapshotBytes !== 'boolean')
		) {
			throw new FulltextError('E_INVALID_ARGUMENT', 'telemetry options must be booleans');
		}
		if (this.#token === undefined)
			throw new FulltextError('E_NATIVE_CAPABILITY_MISMATCH', 'telemetry token unavailable');
		const nextHistograms = histograms ?? this.#histograms;
		const nextBytes = snapshotBytes ?? this.#snapshotBytes;
		try {
			loadAddon().__nativeConfigureTelemetry(this.#token, nextHistograms, nextBytes);
		} catch (error) {
			throw normalizeNativeError(error);
		}
		this.#histograms = nextHistograms;
		this.#snapshotBytes = nextBytes;
	}
	batchAttempt(): void {
		this.#batches.attempted++;
	}
	batchRequested(count: number): void {
		this.#batches.requestedMutations += BigInt(count);
		if (this.#histograms) observe(this.#batches.requestedMutationsDistribution, BigInt(count));
	}
	batchRejected(count: number): void {
		this.#batches.rejectedRecords += BigInt(count);
	}
	batchFrame(bytes: number, mutations: number): void {
		this.#batches.encodedFrames++;
		this.#batches.encodedFrameBytes += BigInt(bytes);
		this.#batches.processedMutations += BigInt(mutations);
		if (this.#histograms) observe(this.#batches.frameBytesDistribution, BigInt(bytes));
	}
	batchComplete(): void {
		this.#batches.completed++;
		this.#batches.succeeded++;
	}
	batchFail(): void {
		this.#batches.completed++;
		this.#batches.failed++;
	}
	preparationRejected(operation: 'search' | 'trace', error: unknown): void {
		const metric = this.#preparation[operation];
		let code = 'other';
		try {
			const candidate = error instanceof FulltextError ? error.code : undefined;
			if (typeof candidate === 'string' && FulltextError.isCode(candidate)) code = candidate;
		} catch {}
		metric.total++;
		metric.errors[code] = (metric.errors[code] ?? 0n) + 1n;
	}
	snapshot(): FullTextTelemetry {
		const facade: FacadeTelemetrySnapshot = {
			logicalBatches: structuredClone(this.#batches),
			preparationRejections: structuredClone(this.#preparation),
			cancellation: { available: false, reason: 'The facade exposes no cancellation API; deadlines are cooperative.' },
		};
		if (this.#token === undefined) return { ...facade, available: false, reason: 'token-unavailable' };
		try {
			const native = JSON.parse(loadAddon().__nativeTelemetry(this.#token), (_key, value) =>
				typeof value === 'string' && /^\d+$/.test(value) ? BigInt(value) : value,
			) as NativeTelemetry;
			return { ...facade, available: true, native };
		} catch {
			return { ...facade, available: false, reason: 'snapshot-unavailable' };
		}
	}
}
