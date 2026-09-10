# @harperfast/fulltext

Native Tantivy full-text indexing for Node.js, with a native filesystem backend and a planned
caller-owned rocksdb-js backend for Harper.

This repository is under active development. The native entry point provides a standalone Tantivy
index backed by `MmapDirectory`. Harper releases will use only the planned RocksDB entry point.

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

## Storage boundaries

The package is designed around two explicit entry points:

- `@harperfast/fulltext/native` uses Tantivy's native directory implementation and has no
  rocksdb-js dependency.
- `@harperfast/fulltext/rocks` will use a caller-owned rocksdb-js database through a versioned
  native capability lease. It is not exported until that contract is implemented and tested.

There is no generic storage selector and no fallback between backends. Harper will consume only the
Rocks entry point. The fulltext addon will not link its own copy of RocksDB.

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
flushes, 256 KiB chunk publication, closed- and active-writer deletion, and distinct-file
concurrency at one, two, four, and eight threads. It reports percentiles across per-sample mean
latencies and uses the deterministic in-memory Phase 0 store to isolate directory coordination from
RocksDB and Node transport costs. That store serializes access, so the concurrency cases detect
coordination regressions but do not predict RocksDB scaling. Compare two optimized builds on the
same quiet host; records include the Git revision and dirty state, while `--revision` can add a run
label and `--samples` and `--warmup` control the run. CI executes only `--smoke`, whose timings are
not comparable to a full run, and applies no timing threshold.

Generated Node-API declarations in `ts/addon.d.ts` are private implementation types. Consumers use
only the types exported from a package entry point.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow and
[docs/scaffold-design.md](docs/scaffold-design.md) for the architecture behind the initial package.

## License

Apache-2.0
