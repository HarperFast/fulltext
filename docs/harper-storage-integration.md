# Tantivy storage through Harper

## Scope and release boundary

The library has two delivery targets: the existing standalone native Tantivy backend and a Harper
integration that stores all persistent Tantivy index state in Harper's existing RocksDB database.
The planned Harper entry point is `@harperfast/fulltext/harper`; it is not an implemented export yet.
The native entry point remains `@harperfast/fulltext/native`.

There is no supported standalone rocksdb-js backend, optional rocksdb-js peer dependency, or required
native addon-to-addon lease. Engineering will not add the proposed fulltext storage functionality to
base rocksdb-js. The experimental branch `codex/issue-831-phase0-lease` is retained unmerged. Existing
experimental fulltext code may supply reusable Directory tests and mapping logic, but its lease
consumer is not the production integration or a release prerequisite. A three-backend benchmark is
optional investigation if a measured question warrants it, not a planned delivery gate.

Harper has no native-filesystem fallback. A native-only library release is possible; a Harper
fulltext release requires the qualified RocksDB path. No second RocksDB runtime, database opener,
private patched rocksdb-js distribution, or external Tantivy service is introduced.

This is the current storage and integration plan. The earlier native-lease experiment remains
historical evidence. Storage-independent schema, English analysis, query, and result contracts
remain in the full product and wrapper designs.

## Architecture and ownership

```mermaid
flowchart TD
    W[Local or replicated record commit] --> H[Harper shared DerivedIndexRuntime]
    H -->|bounded projection and nonblocking admission| E[Shared fulltext engine and per-index runtime]
    H -->|replay and rebuild| E
    Q[Table.search and REST] --> A[Harper authorization and query planning]
    A --> E
    E --> T[Tantivy IndexWriter and IndexReader]
    T --> D[Harper-backed Tantivy Directory]
    D --> B[Bounded storage transport inside the integration]
    B --> S[Harper-owned derived store using existing storage APIs]
    S --> R[Harper's existing RocksDB instance]
    N[Standalone native factory] --> E
    T -->|native mode only| M[Tantivy MmapDirectory]
```

| Concern                                                                               | Owner                                      |
| ------------------------------------------------------------------------------------- | ------------------------------------------ |
| Schema, records, authorization, REST and Table.search                                 | Harper                                     |
| Post-commit delivery, replay positions, retention policy, rebuild scan and activation | Harper shared derived-index runtime        |
| Analysis, BM25, query execution, postings, segments and merges                        | Tantivy through the shared fulltext engine |
| Bounded native admission, writer/search scheduling, errors and result buffers         | fulltext                                   |
| Tantivy logical-file mapping and storage transport                                    | fulltext Harper integration                |
| Derived-store registration, supported storage operations, backup/drop/close ordering  | Harper                                     |
| Database handles, transactions, WAL, flush, compaction and block cache                | Existing rocksdb-js/RocksDB implementation |

`DerivedIndexBackend` is the delivery/recovery protocol proposed in
[Derived-index delivery protocol (DerivedIndexBackend): shared post-commit delivery, watermark/replay, and blob-content contract for HNSW and full-text indexes](https://github.com/HarperFast/harper/issues/2489).
It is not itself a binary storage API. Its implementation status and exact replay contract must be
verified in Harper before freezing the integration. The fulltext wrapper does not parse Harper log
files, invent cursor ordering, or implement a second retention service.

Harper supplies an internal store integration using the database it already owns. This means a
Harper module calling supported storage APIs; it does not mean Harper can manufacture access to
rocksdb-js's private native handles. The exact store interface and transport are proof-stage work,
not a new customer-extensible provider API.

The engine, query builders, batch codec, commit actor, reader reload and error/lifecycle behavior
remain shared with native mode. The Harper integration supplies storage and derived-delivery
adaptation. It does not contain a second fulltext engine.

## First milestone: a real Harper vertical slice

Build the smallest integrated path before broad storage implementation:

1. Register a private derived index store within Harper's normal database lifecycle.
2. Route a bounded projected batch from Harper's post-commit runtime into the existing Tantivy
   engine. All write-capable workers, including replication applies, use the same delivery contract.
3. Write Tantivy objects and publication metadata through existing Harper/rocksdb-js APIs.
4. Commit, reload, query, close, restart Harper and reopen the same persistent index.
5. Force a process crash between source commit and delivery, and at storage-publication boundaries.
   Recover the previous complete index or the new complete index, then replay without missing data.
6. Repeat with concurrent record writes and multiple fulltext indexes. Record event-loop delay,
   queue occupancy, copied bytes, storage wait, ingestion throughput and search latency.

A mocked store or the experimental native lease does not clear this milestone. Native mode remains
the behavior reference. The first slice may use a narrow term query, but its storage contract must
exercise actual Tantivy flush, immutable slices and metadata publication.

## Storage transport: establish feasibility, then measure

Tantivy issues synchronous Directory operations from native indexing, merge and search threads.
Existing JavaScript storage methods cannot simply be invoked from those threads. The integration
must provide a legal, bounded handoff to a JavaScript environment that owns a supported store view,
or a bounded staging/read strategy using those APIs. This is a threading problem to implement and
measure, not evidence that rocksdb-js needs a new native ABI.

The first proof should test a request/response transport with batched chunk operations and explicit
buffer ownership. Select the storage worker arrangement from Harper's existing worker facilities.
Verify that those workers can obtain the intended store view and participate in Harper close/drop.
Do not assign all storage traffic permanently to worker 0 or promise zero JavaScript storage
callbacks. Batching may reduce crossings; random query misses still need measured service capacity.

Required transport properties:

- Record post-commit hooks only project/enqueue and return; they never wait for storage or queue space.
- Only package-owned native workers may wait for storage completion. A JS thread servicing storage
  must not synchronously wait for a Tantivy task that needs that same JS thread.
- Bound queued request count, bytes, response buffers and outstanding operations across all indexes.
  Give query work and control/shutdown operations explicit progress under sustained ingestion.
- Define response ownership, cancellation, environment exit and late-completion behavior. Stopping
  the storage worker must wake native waiters with errors; closing the database must not strand them.
- Reject new work and drain or cancel dependencies before releasing the Harper store. Do not hold
  database locks across a round trip that requires another JS callback to finish.
- A runtime identifies the actual Harper database, index and generation. Reopening or recreating
  a store cannot make an old handle valid again.

A bounded staging or read cache may be considered only after measurements show why it is needed;
its memory, invalidation and recovery obligations must then be specified. Whole-index RAM residency
is unsuitable for the target catalog scale. No local Tantivy files are used as a fallback.
If supported APIs cannot satisfy correctness, record the exact missing guarantee and stop that
milestone. Do not silently substitute the unmerged bridge or declare the Harper backend complete.

## Logical files and Directory conformance

Tantivy keeps its own segment formats. The derived store contains chunked segment objects (term
dictionaries, postings, optional positions, field norms, fast fields and configured stored content),
logical file bindings, delete metadata, `meta.json`, `.managed.json` and generation/format metadata.
It contains no duplicate authoritative records or independent mutation journal.

A per-index store with separate generation prefixes is the initial layout to test against Harper's
existing index-store lifecycle. Confirm allocation and cleanup APIs before fixing a physical
column-family layout; qualify column-family and memtable overhead with multiple indexes.

| Operation                   | Required behavior                                                                                                 |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Open/write/flush            | Newly created logical files are readable; repeated flush permits continued append and preserves visible prefixes. |
| Open slice/read             | Freeze object identity and visible length; existing slices never observe changed bytes.                           |
| Atomic metadata replacement | Readers see one complete old or new value, never a partial value.                                                 |
| Delete                      | Remove the logical binding while keeping already-open slices valid.                                               |
| Termination and sync        | Honor the pinned Tantivy contract, with visibility and durable completion distinguished explicitly.               |
| Locks and watch             | Preserve Tantivy writer exclusion and notification lifetime without inventing a distributed lock service.         |

Reuse the existing backend-parameterized Directory harness and experimental immutable-object tests
where they describe Tantivy semantics rather than native-lease mechanics. Chunk sizing, batched
reads and copy strategy are measurement choices. Pinned native reads, MultiGet, target-CF flush,
native lock tokens and external SST ingestion are not required APIs in this plan.

## Publication, recovery and lifecycle

```mermaid
sequenceDiagram
    participant H as Harper runtime
    participant F as fulltext writer
    participant S as Harper derived store
    participant Q as shared searcher
    H->>F: bounded mutations and progress context
    F->>S: write segment objects through supported APIs
    F->>S: establish required object durability
    F->>S: atomically publish Tantivy metadata and checkpoint payload
    S-->>F: publication durability established
    F-->>H: acknowledge durable contiguous progress
    F->>Q: reload immutable searchable snapshot
```

The checkpoint names only a contiguous set of completed source work represented in the published
index. Durable progress never outruns durable referenced objects. Searcher visibility follows
reload and is reported separately from storage durability.

The proof must name the exact current APIs and write options providing atomicity and durability.
It must evaluate a WAL-enabled derived-write path using existing APIs before assuming specialized
flush support. If WAL-disabled objects are used, their supported durability barrier must complete
before the metadata that references them can be acknowledged durable. An ordinary write promise,
a visibility flush or a process-kill test alone does not prove power-loss durability.

Do not change Harper's global WAL, flush or transaction-log truncation policy to make this adapter
work. Measure database-wide flush/write-stall coupling if the existing barrier has that scope.
Backup/checkpoint tests must prove referenced objects and metadata survive restore together, and
that a derived-only operation never falsely advances authoritative-record durability or log purge.

Harper coordinates rebuild scans, retention gaps, schema activation, cache residency, replicated
content retries and generation swaps through the shared protocol. The wrapper reports completion
and failure; it never scans Harper's primary store or decides that missing replay history is safe
to skip. Precise cursor/resume and retention gaps in the proposed protocol remain Harper-owned
integration blockers. They are separate from the rejected native storage bridge and must be
resolved explicitly using the supported stack before qualification.

## Writer topology and throughput

Tantivy permits one IndexWriter for a physical index generation because segment publication,
deletion state and merge coordination share one authority. That writer can use Tantivy's indexing
threads; it does not imply one JavaScript worker, one writer per database or one writer per cluster.

Independent fulltext indexes may ingest, merge and search concurrently. Each has its own writer,
reader and generation state; shared native and storage-transport budgets bound aggregate work.
Storage workers and queues must allow useful progress across indexes without recreating a global
serial write lane. Saturation defers derived delivery to Harper replay while preserving the source
commit. Benchmarks must measure both throughput and backlog growth: a fast enqueue rate with
unbounded indexing lag is not throughput success.

## Performance and CI

The primary comparison is the existing native backend versus the actual Harper RocksDB-backed
integration. Use the same Tantivy/fulltext build, corpus, analyzer, mutation sequence, durability
cadence, query set, result validation and resource budgets wherever the layers permit.

Report two scopes separately:

- Engine/storage measurements through native and Harper-backed Directories, when the real Harper
  harness can exercise equivalent operations.
- Harper product measurements through Table.search and REST, including authorization, filtering,
  record materialization and protocol overhead.

An end-to-end Harper/native ratio is not a pure RocksDB overhead measurement. Attribute differences
with spans/counters for projection, packing, transport queue wait, copies, storage calls, commit,
reload, Tantivy execution and record retrieval. Run focused experiments only when those measurements
identify a question. Direct rocksdb-js benchmark results from the retained branch are experimental,
not a third supported backend and not a release blocker.

Measure ingest and update throughput, commit-to-searchable lag, search p50/p95/p99, errors/timeouts,
recovery/rebuild time, CPU/RSS, storage size and write amplification where available. Include warm
and cold reads, concurrent indexes, heavy-tail document sizes, sustained updates, delete/eviction,
replication, shared-database writes and increasing dataset sizes up to the target catalog scale.

The Harper goal remains p99 below 50 ms for the agreed workload at hundreds-of-millions scale.
Record sizes, query mix, concurrency, hardware and acceptable lag must accompany any claim.
The existing small native benchmark does not establish that product goal.

PR CI runs correctness, Directory parity, packed-package tests, schema-valid benchmark output and
small smoke workloads. Stable hardware runs scheduled and release profiles with reviewed thresholds;
no noisy hosted-runner latency ratio becomes a release gate. GitHub stores versioned JSON summaries,
environment/build/workload fingerprints and immutable release evidence so results compare across
releases. Experimental results have a distinct cohort and cannot replace production baselines.
A changed Harper/storage stack requires compatibility and performance requalification, not a new
lease-ABI pairing.

## Packaging and documentation

The native import remains independently usable. Add the Harper import only with its tested
integration; do not export a placeholder or advertise it as available before then. Harper owns its
rocksdb-js version, database configuration and dependency updates. fulltext neither declares the
proposed rocksdb-js peer nor bundles RocksDB into its Rust addon.

Release documentation includes native quick start, Harper setup and schema/query examples, supported
versions/platforms, ownership and shutdown, durability versus visibility, recovery/backup, errors,
limits, performance methodology and troubleshooting. Examples run against packed artifacts and the
qualified Harper checkout. Apache-2.0 applies to source and published artifacts.

## Approaches considered

| Axis            | Approach and disposition                                                                                                                                                                 |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Different layer | A native capability table in base rocksdb-js would serve direct native I/O, but engineering has rejected that addition. It is not a production dependency.                               |
| Deeper cause    | Prove how native Directory calls reach supported host storage without a deadlock or premature publication. Merely naming a private provider does not establish either invariant.         |
| Do less         | Skip the standalone Rocks backend and mandatory third benchmark arm. The existing native reference plus Harper instrumentation answers the immediate delivery question.                  |
| Chosen          | Implement and qualify the Harper path first, reusing its derived runtime and storage APIs and the existing fulltext engine. Measure any transport cost before designing an optimization. |

## Execution and decisions still to resolve

1. Prove storage worker/view ownership, bounded request/response transport and Directory semantics
   on the supported Harper stack; record exact source revisions and APIs.
2. Prove object/metadata durability, close/drop/backup ordering and crash/reopen behavior.
3. Integrate the shared derived protocol, including exact replay, retention-gap detection,
   replication, eviction and rebuild. Do not represent the issue sketch as shipped code.
4. Reuse the shared engine for full query behavior and multi-index operation.
5. Qualify performance, documentation and packages; investigate measured differences as needed.

Before the Harper factory is frozen, engineering must resolve transport topology, memory/queue
budgets, durability options, store registration, and the shared protocol's exact progress/resume
contract. These are implementation proof obligations, not authorization to add native capabilities
to rocksdb-js. The schema and query design do not expose these internal choices to customers.
