# @harperfast/fulltext

Native Tantivy full-text indexing for Node.js, with a standalone filesystem backend and an
experimental Harper-owned storage integration.

This repository is under active development. The native entry point provides a standalone Tantivy
index backed by `MmapDirectory`. Harper releases will use only the Harper integration backed by
Harper's existing RocksDB lifecycle; they will not use Tantivy's filesystem storage.

## Requirements

- Node.js 22.18 or newer, or Node.js 24 or newer
- Rust 1.90 when building from source

The package never compiles or downloads native code during installation. A supported prebuilt
artifact must be present for the executing platform.

The initial CI-qualified targets are Linux x64 glibc, macOS arm64, and Windows x64. Additional
targets are added only after their artifacts are loaded and tested on the target runtime.

## Native usage

```js
import { encodeMutationBatch, openNativeFullTextIndex } from '@harperfast/fulltext/native';

const index = await openNativeFullTextIndex({
	path: './search/products',
	indexId: 'products',
	generation: 'v1',
	fields: [{ name: 'title', weight: 3 }, { name: 'description' }],
	analyzer: 'english@1',
	limits: {
		indexingThreads: 2,
		searchThreads: 4,
		writerMemoryBytes: 60_000_000,
		maxQueuedCommands: 128,
		maxQueuedBytes: 64 * 1024 * 1024,
		maxBatchBytes: 8 * 1024 * 1024,
	},
});

await index.apply(
	encodeMutationBatch({
		upserts: [{ id: 'shoe-1', fields: { title: 'Trail running shoe', description: 'Waterproof' } }],
	}),
);
await index.commit();
await index.reload();
console.log(await index.search({ text: 'waterproof running shoes', limit: 10 }));
await index.close();
```

Mutation batches are versioned packed values, so indexing crosses Node-API once per batch rather
than once per document. One dedicated actor owns Tantivy's single writer for each index. A bounded
search pool shares immutable searchers and can execute reads while indexing or commit work is in
progress. Queue limits reject overload with `E_QUEUE_FULL` rather than blocking the JavaScript
thread.

Search uses BM25. `total` is a bounded result by default so Tantivy can retain block-max WAND
pruning. Set `exactTotal: true` only when an exact match count is worth a second full-match
traversal. `positions` defaults on for future phrase queries; `surfaceTerms` defaults off because it
stores source text for future highlighting and suggestions. Both settings are persisted and must
match when the index is reopened.

`close()` rejects uncommitted data by default. Use `close({ mode: 'rollback' })` to discard it
explicitly. `commit()` publishes mutations, and `reload()` makes the latest commit visible to this
handle's searches.

### Checkpointed publication

Use `publish(payload)` when a consumer needs to resume from a durable checkpoint:

```js
console.log(index.committedPayload); // undefined on a new index; recovered from files on reopen
await index.apply(encodeMutationBatch({ upserts: [{ id: 'shoe-1', fields: { title: 'Trail shoes' } }] }));
await index.publish('source-checkpoint-42');
// Both the mutations and the checkpoint are committed; searches now see that commit.
```

The payload is an opaque, well-formed Unicode string, limited to 64 KiB in UTF-8. Fulltext does not
interpret it or verify that it describes the mutations supplied by the caller. Empty strings are
valid checkpoints; `undefined` means the index has never committed one. Publication uses the same
bounded writer queue as mutations and returns a Tantivy opstamp, not an application cursor.

Once a generation has a checkpoint, plain `commit()` rejects with `E_CHECKPOINT_REQUIRED`, including
after reopen. Pending mutations remain available to a subsequent `publish()`, or can be discarded
with rollback close. A publication without mutations can advance the checkpoint. Ordinary indexes
that never publish retain the separate `commit()` and `reload()` API.

An accepted publication that fails can have an ambiguous durable outcome: the commit may have
succeeded before reader reload failed. The handle becomes terminal and `committedPayload` throws
`E_POISONED`; close it, reopen the same files and identity, and recover the checkpoint from that new
handle. Do not infer recovery progress from `status()` counters. Validation and queue admission
failures do not themselves poison the handle or invalidate a known checkpoint.

This API uses native ABI 2. The loader rejects older addon binaries; persisted index identity and
Tantivy file formats are unchanged by the ABI update.

## Storage boundaries

The package has two explicit entry points:

- `@harperfast/fulltext/native` uses Tantivy's native directory implementation and has no
  rocksdb-js dependency.
- `@harperfast/fulltext/harper` is an experimental integration surface that stores Tantivy objects
  through a synchronous Harper-owned key-value view. It exists to prove and measure the real
  derived-index path before Harper enables a customer-facing feature.

The hosted entry point remains temporarily for migration and regression coverage. It is not the
planned Harper integration: Harper will use native Tantivy files as a locally rebuildable derived
index, with authoritative records and replication owned by Harper. Native checkpoint publication
is implemented here; wiring that lifecycle into Harper and removing the hosted backend are separate
work. There is no automatic storage fallback. The fulltext addon does not link RocksDB or depend
on rocksdb-js.

The Harper opener requires a process-lifetime store identity, persistent generation, byte namespace,
bounded transport limits, and a `HostStorage` implementation. `publish(payload)` commits the index
and opaque payload into one Tantivy `meta.json` generation, then reloads the local reader before it
resolves. Harper uses that payload for its derived-index cursor. `committedPayload` exposes the
payload recovered at open or the newest successful publish. If a publish poisons the generation,
its durable outcome can be ambiguous, so the getter throws until the index is reopened. Host storage
methods are strictly synchronous; `write` and `sync` must return `undefined`, and Promise-returning
implementations are rejected rather than acknowledged.

The current Harper path is owner-worker-only. It does not yet provide non-owner read handles,
cross-worker refresh, generation retirement, or scheduled physical reclamation. Those lifecycle
pieces and representative performance results are required before release enablement.

## Development

```bash
npm ci --ignore-scripts
npm run build:debug
npm test
npm run lint
npm run format:check
npm run benchmark:native -- --documents 100000 --concurrency 4 --commit-every 25000
npm run benchmark:kv-directory -- --revision candidate
```

The benchmark generates a deterministic, high-cardinality product catalog and emits one versioned
JSON record. It reports packing, apply, durable end-to-end ingestion, actor queue and execution
time, commit distributions, reload cost, warm and cold BM25 p50/p95/p99, exact-total overhead,
index bytes, and periodically sampled process RSS. `--commit-every` sets the target number of mutations between
durability points; it materially affects throughput and peak memory because replacement-safe
upserts include delete terms. CI runs only the correctness smoke profile; timing comparisons
require controlled hardware.

The `kv-directory` benchmark measures the caller-visible buffered write path, empty and dirty
flushes, 256 KiB chunk publication, closed- and active-writer deletion, retained and churned read
handle opens, distinct-file read-open and write concurrency at one, two, four, and eight threads,
and shared-file read-open concurrency at two, four, and eight threads. It reports percentiles across
per-sample mean latencies and uses a deterministic in-memory store to isolate directory coordination
from RocksDB and Node transport costs. The store permits concurrent point reads but serializes
writes, so read-open cases isolate registration contention while write cases detect coordination
regressions; neither predicts RocksDB scaling. The `-rw-` read-open case names establish a new
comparison series and must not be compared with results from the earlier serialized-store cases.
Compare two optimized builds on the same quiet host; records include the Git revision and dirty
state, while `--revision` can add a run label and `--samples` and `--warmup` control the run. CI
executes only `--smoke`, whose timings are not comparable to a full run, and applies no timing
threshold.

Generated Node-API declarations in `ts/addon.d.ts` are private implementation types. Consumers use
only the types exported from a package entry point.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow and
[docs/scaffold-design.md](docs/scaffold-design.md) for the architecture behind the initial package.

## License

Apache-2.0
