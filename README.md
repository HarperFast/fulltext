# @harperfast/fulltext

Native Tantivy full-text indexing for Node.js and Harper.

This repository is under active development. The native entry point provides a Tantivy index
backed by `MmapDirectory`, for standalone use and the planned Harper derived-index integration.
Harper remains the source of truth; each node maintains its own rebuildable Tantivy files.

## Requirements

- Node.js 22.18 or newer, or Node.js 24 or newer
- Rust 1.90 when building from source

The package never compiles or downloads native code during installation. A supported prebuilt
artifact must be present for the executing platform.

The initial CI-qualified targets are Linux x64 glibc, macOS arm64, and Windows x64. Additional
targets are added only after their artifacts are loaded and tested on the target runtime.

## Native usage

```js
import {
	inspectNativeFullTextIndex,
	openNativeFullTextIndex,
	reclaimRetiredNativeFullTextIndexes,
	resetNativeFullTextIndex,
	validateNativeFullTextIndexOptions,
} from '@harperfast/fulltext/native';

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

const applied = await index.applyMutationBatch(
	{
		upserts: [{ id: 'shoe-1', fields: { title: 'Trail running shoe', description: 'Waterproof' } }],
	},
	{ rejectedUpsert: 'delete' },
);
if (applied.rejected.length) console.warn(`${applied.rejected.length} products were removed from the index`);
await index.commit();
await index.reload();
console.log(await index.search({ text: 'waterproof running shoes', limit: 10 }));
await index.publish('source-checkpoint-42');
await index.close();

console.log(
	inspectNativeFullTextIndex({
		path: './search/products',
		indexId: 'products',
		generation: 'v1',
		fields: [{ name: 'title', weight: 3 }, { name: 'description' }],
		analyzer: 'english@1',
	}),
); // { state: 'checkpointed', committedPayload: 'source-checkpoint-42' }
```

Mutation batches are versioned packed values, so indexing crosses Node-API once per batch rather
than once per document. One dedicated actor owns Tantivy's single writer for each index. A bounded
search pool shares immutable searchers and can execute reads while indexing or commit work is in
progress. Queue limits reject overload with `E_QUEUE_FULL` rather than blocking the JavaScript
thread. `applyMutationBatch()` is the normal logical mutation API. It partitions one logical batch
into bounded native frames, applies them sequentially, validates native counts, and returns
`{ processed, rejected, encodedBytes, frames }`. IDs must be distinct across the logical batch.
By default, any rejected record fails the operation. Pass `{ rejectedUpsert: 'delete' }` when an
unindexable replacement must delete previously searchable content for the same ID. A failure after
native application is attempted leaves the handle incomplete: writer operations reject
`E_BATCH_INCOMPLETE` until the handle is closed with `{ mode: 'rollback' }`. This prevents a later
publish from exposing part of a logical batch.

Schema mismatches fail the whole call even with `{ rejectedUpsert: 'delete' }`; treating schema drift
as record-local rejection could remove many documents under the wrong schema. The option applies
only to record-local `E_INVALID_ARGUMENT` and `E_BATCH_TOO_LARGE` rejections with usable IDs.

Trusted callers that already enforce distinct IDs may pass `assumeDistinctIds: true` to skip the
whole-batch duplicate prepass. Supplying duplicates with that option violates the API contract.
The wrapper snapshots the two mutation arrays, but callers must not mutate record objects, field
maps, or nested field-value arrays until the returned promise settles.

`encodeMutationBatch(batch, maxBytes)` rejects output beyond its encoding bound with
`E_BATCH_TOO_LARGE`; `maxBytes` defaults to 8 MiB and is intended for low-level callers producing a
single native frame. Low-level callers may use `index.encodeMutationBatches()`. It uses the limit from
the index's open configuration, validates IDs and fields against that handle, and greedily emits
admissible frames. IDs must be distinct across the logical batch. Aggregate size creates more
frames; a single invalid or unencodable mutation is returned in `rejected` with its operation and
zero-based index in the corresponding input array. It is never dropped automatically.

Encoding is synchronous and runs on the JavaScript thread. The total encoded output of one logical
call defaults to 64 MiB and can be lowered with `encodeMutationBatches(batch, { maxTotalBytes })`.
The default behavior throws `E_BATCH_TOO_LARGE` when the complete logical batch exceeds that
ceiling. Callers that pass `allowPartial: true` instead receive a leading prefix;
`consumedUpserts` and `consumedDeletes` identify the mutations represented by the returned frames
and rejections so they can continue with each array's remaining suffix. The low-level API validates
distinct IDs across the full input before applying the output ceiling, so repeated suffix calls
repeat that validation scan. Prefer `applyMutationBatch()` for large logical batches.
Producers using the low-level API should keep logical batches comfortably below that limit.
Applying multiple frames stages them in one Tantivy writer. Commit or publish only after every frame
succeeds; on a later failure, close with rollback rather than publishing the partial logical batch.
Concurrent low-level callers should leave queue-byte headroom for commit or publish.
A successful `apply()` resolves to the number of accepted mutation commands, including deletes for
IDs that are not currently indexed.

One caller must own a handle's complete apply-and-publish sequence at a time. The high-level method
blocks low-level writer interleaving while its logical batch is active. The low-level frame API does
not infer logical batch boundaries, so callers using it remain responsible for excluding another
publisher between frames. Searches may still run concurrently with either writer sequence.

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
await index.applyMutationBatch({
	upserts: [{ id: 'shoe-1', fields: { title: 'Trail shoes' } }],
});
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

`inspectNativeFullTextIndex(options)` synchronously validates an existing index's identity, schema,
metadata, and committed payload without creating files, reserving a handle, starting actors, or
acquiring the Tantivy writer. It returns `missing`, `cursorless`, `checkpointed` with the committed
payload, or `incompatible` with a stable identity/schema or corrupt-format code. `checkpointed`
means the committed metadata is compatible; callers must still open the index successfully before
serving queries. Opening maps missing segment files and unreadable segment metadata or footers to
rebuildable error codes.
Operational storage failures throw. Inspection is intended for short lifecycle checks such as
derived-index election, not request hot paths. Its options intentionally omit writer, queue, and
search limits because inspection creates none of those resources.

`validateNativeFullTextIndexOptions(options)` runs the same native configuration decoder as open
without creating files or starting actors. This is intended for activation-time validation.

`close()` is also the native quiescence barrier. A resolved result means the writer, search actors,
readers, merge threads, and memory mappings no longer use the index path. After closing an
incompatible or corrupt local index, retire it atomically before rebuilding:

```js
const result = await resetNativeFullTextIndex({ path: './search/products', indexId: 'products' });
if (result.state === 'reset') {
	console.log(`retired native files at ${result.retiredPath}`);
}
```

The resolved close result is `{}` normally. If native resources were released but shutdown also
reported an operational cleanup error, it is `{ cleanupError }` and that error has code
`E_CLOSE_FAILED`; the path is still safe to reset. `E_QUIESCENCE_FAILED` rejects because the wrapper
could not prove all native work stopped. Do not reset, remove, or rename that path until the process
restarts.

Reset returns `missing` without creating the path. It rejects a live owner with `E_LOCK_BUSY`, a
different persisted logical index with `E_IDENTITY_MISMATCH`, and unrelated nonempty directories
with `E_INVALID_ARGUMENT`. A malformed identity sidecar fails closed with `E_INDEX_CORRUPT`. On
success, reset renames the live directory into a unique path below the parent's `.fulltext-retired`
directory. The wrapper does not delete it automatically. Call
`reclaimRetiredNativeFullTextIndexes({ path, retiredPath: result.retiredPath })` at a lifecycle point
chosen by the application. Passing the opaque reset result preserves the canonical source basename
and verifies that the hint belongs to the requested index. The reclaimer removes only retired trees
generated for that index path, ignores unrelated entries and other indices, and returns
`{ removed, failed }`. Harper invokes it during derived-index initialization and after reset.

An established duplicate open returns `E_DUPLICATE_OPEN`. An open racing another open or reset can
return `E_LOCK_BUSY` while the shared lifecycle lock is held; callers may retry that acquisition.
Lifecycle lock files are stored in the index parent's `.fulltext-locks` directory so reset can keep
the handoff lock while renaming the native directory on Windows. The wrapper does not remove this
lock directory. The parent must permit creating this directory, and `.fulltext-locks` must remain
writable while indices are opened or reset; failures name the lock-directory path.

This API uses native ABI 5. The loader rejects older addon binaries; persisted index identity and
Tantivy file formats are unchanged by the ABI update.

## Storage boundary

The package has one public entry point: `@harperfast/fulltext/native`. It uses Tantivy's native
filesystem directory and has no rocksdb-js dependency. There is no hosted key-value storage entry
point and no automatic storage fallback.

Harper uses the same native entry point for its locally rebuildable derived index. Harper records
and transaction logs remain the source of truth; the local Tantivy files and their committed replay
payload can be reused on restart or rebuilt from Harper data when necessary. The addon does not link
RocksDB or depend on rocksdb-js.

## Development

```bash
npm ci --ignore-scripts
npm run build:debug
npm test
npm run lint
npm run format:check
npm run benchmark:native -- --documents 100000 --concurrency 4 --commit-every 25000
npm run benchmark:native -- --documents 100000 --concurrency 4 --commit-every 25000 --mutation-driver low-level
npm run benchmark:inspect -- --indexes 1,10,100,1000 --commits 64 --warm-rounds 10
```

The benchmark generates a deterministic, high-cardinality product catalog and emits one versioned
JSON record. It reports packing, apply, durable end-to-end ingestion, actor queue and execution
time, logical-batch p50/p95/p99, frame count, commit distributions, reload cost, warm and cold BM25
p50/p95/p99, exact-total overhead, index bytes, and periodically sampled process RSS. The default
`--mutation-driver logical` exercises `applyMutationBatch()`; rerun the identical command with
`--mutation-driver low-level` for the prior encode-plus-apply path. Compare
`mutationDriverMilliseconds`, durable end-to-end throughput, logical-batch percentiles, and peak
RSS. The component `packingMilliseconds` and `applyMilliseconds` fields are not comparable because
the logical API performs both inside one call. `--commit-every` sets the target number of mutations
between durability points; it materially affects throughput and peak memory because
replacement-safe upserts include delete terms. CI runs only the correctness smoke profile; timing
comparisons require controlled hardware.

The inspection benchmark compares synchronous read-only inspection with full writer reopen across
multiple index counts. It reports equivalent first-pass and warm p50/p95/p99/max latency,
synchronous wall time per inspection sweep, metadata size, and the actual segment count produced by
the seed workload. It does not claim to model OS-cold storage. `--indexes` accepts configurations
that create at most 10,000 temporary index directories across both benchmark paths.

Generated Node-API declarations in `ts/addon.d.ts` are private implementation types. Consumers use
only the types exported from a package entry point.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow and
[the native backend design](docs/native-backend-implementation.md) for implementation details.

## License

Apache-2.0
