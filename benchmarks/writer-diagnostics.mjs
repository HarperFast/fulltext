import { writeSync } from 'node:fs';

export function writeWriterDiagnostic(record) {
	try {
		writeSync(
			2,
			`fulltext-process-diagnostic ${JSON.stringify(record, (_key, value) => (typeof value === 'bigint' ? String(value) : value))}\n`,
		);
	} catch {}
}

export function writerStatusSnapshot(status) {
	if (!status) return undefined;
	return {
		state: status.state,
		uncommittedMutations: status.uncommittedMutations,
		writerQueuedCommands: status.writerQueuedCommands,
		writerQueuedBytes: status.writerQueuedBytes,
		commitOpstamp: status.commitOpstamp,
		metrics: {
			writerQueueNanoseconds: status.metrics.writerQueueNanoseconds,
			writerExecutionNanoseconds: status.metrics.writerExecutionNanoseconds,
		},
	};
}

export function startWriterDiagnostics(name, status = () => undefined) {
	let stage;
	let timer;
	if (process.env.FULLTEXT_TEST_WRITER_DIAGNOSTICS === '1') {
		timer = setInterval(() => {
			try {
				const cpu = process.cpuUsage();
				let native;
				try {
					native = status();
				} catch (error) {
					native = { error: error.code ?? error.name };
				}
				writeWriterDiagnostic({
					name,
					pid: process.pid,
					timestamp: Date.now(),
					stage,
					cpu,
					rss: process.memoryUsage().rss,
					native,
				});
			} catch {}
		}, 5_000);
		timer.unref();
	}
	return {
		stage(value) {
			stage = value;
		},
		detachStatus() {
			status = () => undefined;
		},
		stop() {
			clearInterval(timer);
		},
	};
}
