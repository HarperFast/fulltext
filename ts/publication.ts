import { FulltextError } from './errors.js';

export class PublicationState {
	#payload?: string;
	#nextSequence = 0n;
	#publishedSequence = 0n;
	#uncertainSequence = 0n;

	constructor(payload?: string) {
		this.#payload = payload;
	}

	get committedPayload(): string | undefined {
		if (this.#uncertainSequence > this.#publishedSequence) {
			throw new FulltextError('E_POISONED', 'committed payload is unknown until the index is reopened');
		}
		return this.#payload;
	}

	begin(): bigint {
		return ++this.#nextSequence;
	}

	succeed(sequence: bigint, payload: string): void {
		if (sequence > this.#publishedSequence) {
			this.#publishedSequence = sequence;
			this.#payload = payload;
		}
	}

	fail(sequence: bigint): void {
		if (sequence > this.#uncertainSequence) this.#uncertainSequence = sequence;
	}
}
