import { parentPort, workerData } from 'node:worker_threads';
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const index = await openNativeFullTextIndex(workerData);
index.configureTelemetry({ histograms: true, snapshotBytes: true });
await index.applyMutationBatch({ upserts: [{ id: 'a', fields: { text: 'worker hello' } }] });
await index.publish('worker-checkpoint');
parentPort.on('message', () => {});
parentPort.postMessage({ ready: true, telemetry: index.telemetry() });
