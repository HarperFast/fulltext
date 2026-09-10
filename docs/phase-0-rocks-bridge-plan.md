# Phase 0: Tantivy Directory semantics and the rocksdb-js bridge

- **Issue:** [HarperFast/fulltext#7](https://github.com/HarperFast/fulltext/issues/7)
- **Related rocksdb-js issue:** [HarperFast/rocksdb-js#831](https://github.com/HarperFast/rocksdb-js/issues/831)
- **Status:** Phase 0 implementation and qualification plan
- **Base:** `origin/main` at `89df005`
- **Tantivy:** 0.26.1 at `d8f4c0b703120ed98f06297724dc1522df6019b9`
- **rocksdb-js:** `origin/main` at `7ab102ca3e9600343bcefe6f19204b111836ec52`

## Objective

Retire the storage risks that would otherwise be embedded in `RocksDbDirectory`. The phase ends
with measured evidence, a go/no-go decision, and the smallest generic addon-to-addon capability
table that rocksdb-js must own. It does not export `@harperfast/fulltext/rocks`, freeze the final
storage format, or implement the production adapter.

The phase protects two invariants:

1. **Publication:** a published Tantivy commit may lag its source, but it must always reopen as the
   previous complete commit or the new complete commit and must never reference missing or partial
   objects.
2. **Lifetime:** every RocksDB access occurs while rocksdb-js retains the database and column-family
   incarnation; close revokes future access and waits for admitted calls, while retained buffers
   remain provider-owned until explicitly released.

## Design assessment

This work crosses the Tantivy `Directory`, Rust/Node-API, and rocksdb-js native-lifecycle boundaries.
It also determines which durability and ownership guarantees become public native ABI contracts.
The experiments therefore precede implementation of the Rocks package entry point and treat every
unmeasured capability as optional.

The end-to-end route combines two tests. A real Tantivy index test creates a schema, indexes
documents, commits, reloads, searches, closes, and reopens through the candidate Directory. A
child-process crash and reopen suite terminates at named durability points, reopens the caller-owned
database, and verifies the last published commit and every referenced object. The Phase 0 seam is
excluded from published package artifacts.

## Source-grounded baseline

### Tantivy 0.26.1

The pinned `Directory` contract requires behavior that a key/value adapter must preserve:

- `open_write()` creates a previously absent logical file immediately. A subsequent read can open
  it before termination.
- Writer `flush()` makes preceding bytes readable and persistent. A writer may continue appending
  after a flush.
- An opened `FileSlice` is immutable. Later writes, flushes, replacement, or deletion must not
  change its bytes or visible length.
- `delete()` removes the logical name without invalidating an already-open slice.
- `atomic_write()` replaces a small logical file without exposing partial contents.
- `sync_directory()` makes newly created files durable.
- locks provide blocking and nonblocking exclusion; `watch()` notifies retained callbacks after
  `meta.json` changes made through `atomic_write()`.

These claims are traced to the `Directory` trait, its shared tests, and `MmapDirectory` in Tantivy
0.26.1. The existing fulltext harness already covers the basic contract against `MmapDirectory`.
Phase 0 extends it for repeated flush, writer continuation, frozen prefixes, termination failure,
and the crash cases that an in-memory oracle cannot exercise.

Pinned `MmapDirectory` source also exposes a reference limitation: its path cache may return the
same older mmap to a new `open_read()` while a handle to that mmap remains alive, even after the
writer flushes more bytes. The shared reference test therefore proves writer continuation, old-handle
immutability, and later visibility after the old handle is released. The Rocks prototype separately
proves simultaneous old and new binding revisions because its write-once chunks and tail revisions
support that stronger behavior. The wrapper does not claim Mmap and Rocks have identical raw
Directory cache semantics; it requires identical observable index, commit, reopen, and search
behavior.

### rocksdb-js 2.8.0

Current rocksdb-js already owns the primitives that the bridge must participate in:

- one process-global `DBDescriptor` per open database identity;
- shared column-family descriptors and `DBHandle` views;
- the descriptor operation count and close fence used by `OperationGuard`;
- the `Closable` graph used to revoke and drain attached resources;
- per-database lock coordination;
- point reads, writes, deletes, iteration, transactions, checkpoints, backup, statistics, background
  errors, and write-stall reporting; and
- explicit per-handle WAL policy.

It does not expose an addon-to-addon function table. Its JavaScript `flush()` calls
`DBDescriptor::flush()`, which snapshots and flushes every column family. JavaScript CRUD also
crosses N-API per operation, and asynchronous calls use libuv workers. Those are valid application
APIs, but they cannot be the sustained Tantivy I/O path: the Directory runs on package-owned native
threads, requires native lifetime fencing, and may issue many reads while answering one search.

The bridge reuses `DBDescriptor`, `ColumnFamilyDescriptor`, and `Closable`. The descriptor's existing
operation-count pattern is factored into a Node-free `OperationGate`: ordinary JavaScript calls use
borrowed claims while a native lease uses a shared claim that keeps only the gate—not the descriptor
or database—alive. This lets foreign native threads participate in the same close fence without
linking Node headers or inventing a second lifecycle. The bridge does not expose `rocksdb::DB*`, open
the database again, or link another copy of RocksDB.

Tantivy writer locking remains in fulltext's shared in-process index runtime. `KvDirectory`
overrides Tantivy's file-backed default so `.tantivy-writer.lock` and `.tantivy-meta.lock` never
enter RocksDB or survive a process crash. It is not a RocksDB data lock and does not need to join
rocksdb-js's key-lock registry. The Phase 0 ABI therefore has no native lock callbacks.

## Experiment architecture

```text
Node test coordinator
        │ owns one rocksdb-js Database and selected column family
        │ requests a test-only native lease
        ▼
@harperfast/fulltext test addon
        │ validates ABI, identity, capabilities, and lifetime
        │ runs Tantivy Directory calls on native test threads
        ▼
Rocks bridge candidate
        │ function-table calls; no JavaScript callback per I/O
        ▼
rocksdb-js DBDescriptor ──► existing RocksDB instance / column family

child process kill points ──► parent reopens rocksdb-js ──► validates published head
```

The proof separates mapping correctness from native transport:

```text
pure Rust FaultingKv oracle ──► logical mapping + deterministic visibility/durability faults

test-only rocksdb-js lease ──► byte transport + ownership + real durability/crash behavior
```

`FaultingKv` keeps distinct visible and durable states and exposes deterministic failure points. It
proves the mapping and publication state machine without linking a RocksDB crate or depending on
process-kill timing. The real lease then runs the same state-machine trace against rocksdb-js. A
failure can therefore be classified as mapping, transport, or RocksDB durability rather than being
reported as an undifferentiated adapter failure.

The prototype is intentionally split across coordinated branches:

1. fulltext owns the backend-neutral Directory harness, logical object prototype, fault schedule,
   measurements, and decision report;
2. rocksdb-js owns the test-only lease producer and any generic native operations under evaluation;
3. fulltext consumes the lease without importing RocksDB headers or libraries; and
4. neither package publishes the test seam. The selected ABI receives its own reviewed production
   implementation after Phase 0.

This split is part of the proof. A prototype that works only by reaching into rocksdb-js internals
from fulltext has failed the ownership requirement.

The coordinated Node test is opt-in because the repositories remain independently buildable. Build
rocksdb-js with `ROCKSDB_JS_NATIVE_STORAGE_LEASE=1`, then run fulltext's Node suite with
`FULLTEXT_PHASE0_ROCKSDB_JS_ROOT` set to that checkout. The test consumes the hidden tagged
`External`, exercises owned reads, batches, scans, statistics, and a real Tantivy lifecycle, rejects
VerificationTable overlap, verifies drop/recreate incarnation changes, and proves close revocation.
Without the environment variable, the cross-repository case is reported as skipped; all pure Rust
and package-isolation cases still run.

## Logical object prototype

The first Harper mapping uses fixed-size data chunks and a versioned tail. The chunk size is an
internal format choice, not a schema or factory option. It remains subject to the benchmark sweep
before the persisted format is declared stable.

```text
namespace / index generation
  format marker kind                   -> directory key-format version
  chunk kind: object-id, ordinal       -> immutable 256 KiB data chunk
  tail kind: object-id, revision       -> immutable final partial chunk
  binding kind: logical path           -> v3 object-id, published state and physical-key high-waters
  atomic kind: logical path            -> complete small-file bytes
  reclaim tail/head kinds: shard       -> next enqueue sequence / oldest retained sequence
  reclaim entry kind: shard, sequence  -> v2 retired object identity, published chunks and high-waters
  reclaim progress: shard, sequence    -> next chunk ordinal and tail revision
```

The keyspace uses a fixed magic, format version, length-prefixed namespace, and one-byte key-kind
tag before kind-specific bytes. Namespace and path delimiters therefore cannot alias another
index's keys. An unversioned, length-prefixed format-marker key records the active key version so a
future implementation detects an unsupported format instead of opening an empty parallel keyspace.
The unreleased delimiter-based prototype is rejected when its counter or known atomic metadata
sentinels exist; it is rebuilt rather than migrated. Read-only access validates but does not create
the marker, while the first write creates it durably before any payload. The sentinel check is a
prototype guard, not a general mixed-keyspace detector: backup restore replaces a closed generation
and its storage incarnation rather than merging bytes into a live namespace. After one clean
marker-less probe, read-only calls continue checking the marker but do not repeat the prototype
sentinel reads.

A key-format version change requires dropping the old namespace storage and rebuilding the derived
generation from Harper source data; changing the version prefix alone would strand old payload.
The consumer advances the unreleased directory key format to v3 because reclaim entry v2 adds the
published chunk count and introduces sequence-keyed progress. The former v2 prototype is rejected
rather than partially interpreted. New logical key kinds receive distinct kind tags under that
version. A storage provider must also change `KvStoreIdentity` whenever close, restore, or column-
family replacement can change the bytes behind an identity.

`open_write()` creates a new object identity and a zero-length binding. Before writing the first
chunk outside the binding's exclusive chunk high-water, the writer reserves 64 ordinals with one
WAL binding update. Covered chunks keep the one-put path, so a 1 GiB file adds 64 reservation reads
and writes rather than one binding round trip per 256 KiB chunk. The reservation is visible before
its payloads, and missing keys inside it are valid. The writer retains at most one partial chunk.
`flush()` atomically publishes a new binding and an immutable, revisioned tail while advancing the
tail high-water, including for an empty tail. Filling a previously published tail creates a full
chunk and a later binding revision; it never overwrites bytes visible to an existing file handle.
Applied-but-reported-failed reservations, chunk puts, and publications are retried idempotently.

Every flush that replaces a partial tail leaves its previous immutable tail revision unreachable.
A file appended across K such flushes can therefore leave K-1 tails of up to `CHUNK_SIZE - 1` bytes,
while a failed publication may additionally leave an unpublished tail or full chunks. Binding
decode enforces that the published chunk count and tail revision stay within the high-waters and
that their maximum possible physical extent is no more than the 1 TiB format safety bound. These
values remain until the derived-index reclaimer is implemented.

A file handle captures one binding value. It computes a full-chunk key directly from the requested
offset and reads the versioned tail only when the range intersects it. A range contained in one
chunk therefore performs one payload lookup regardless of file size; ranges spanning boundaries
perform one lookup per intersecting chunk. No read walks or fetches preceding payloads.

A full-chunk read response requires at least `CHUNK_SIZE + 7` bytes for the protocol envelope, so
the host store rejects a smaller response budget at construction. Transport admission separately
accounts for the exact encoded request and reserved response before dispatch. The prototype does
not claim a universal construction-time transport minimum: namespace and path lengths vary, and
`atomic_write()` does not yet enforce a metadata size bound. The production factory must bound
those inputs and validate its aggregate byte budget before this backend is exposed. The Phase 0
sweep records small-read amplification and retained `OwnedBytes` because a slice keeps its complete
chunk allocation alive; a bounded chunk cache is considered only if those measurements justify it.

Termination flushes the remaining tail. Deleting removes the binding but leaves every immutable
chunk and tail behind. They remain readable through already-open handles and become eligible for
reclamation after those handles drain.

`atomic_write()` stores the complete small value in one RocksDB write batch. It is used for metadata
such as `meta.json` and `.managed.json`, not large segment output. Issue #11 still owns format
qualification, bounded orphan reclamation and the chunk-size performance sweep before release.

## Experiment matrix

### 1. Complete the Directory oracle

Run identical backend-parameterized Rust tests against `MmapDirectory` and the Rocks candidate:

| Contract           | Required observation                                                                    |
| ------------------ | --------------------------------------------------------------------------------------- |
| Immediate creation | `exists()` and `open_read()` succeed after `open_write()` and before bytes              |
| Repeated flush     | each flush exposes exactly its prefix; a following append and flush succeeds            |
| Frozen handle      | an earlier handle retains its original object identity, bytes, and length               |
| Termination        | successful termination seals all flushed bytes; injected failure publishes no new bytes |
| Delete while open  | delete succeeds, new opens fail, and the retained handle remains readable               |
| Missing paths      | open and delete preserve Tantivy's typed missing-file distinctions                      |
| Atomic replacement | concurrent readers observe only the old or new complete value                           |
| Failed replacement | the old complete value remains visible                                                  |
| Locks              | nonblocking contention fails; blocking contention wakes in FIFO order after release     |
| Watch              | retained callbacks observe the committed `meta.json` revision, not an attempted write   |
| Durability         | a completed barrier survives process termination and reopen                             |

Each contract case gets a fresh Directory or unique path namespace and reports its own metric delta;
the suite does not rely on order-coupled fixed paths or one aggregate read-count constant. The
harness records logical reads and requested bytes. The Rocks candidate additionally records
physical gets, bytes fetched, bytes copied, batches, WAL bytes, requested and automatic flushes,
participating column families, stalls, and block-cache behavior.

### 2. Measure the existing JavaScript path

A Node benchmark implements the same logical operations using current public rocksdb-js APIs. It is
a measured baseline, not an adapter candidate. Workloads cover:

- point get, put, delete, prefix iteration, and multi-key publication;
- payloads from 0 bytes through the Phase 0 candidate chunk sizes;
- warm and cold reads, sequential and random ranges, and concurrent readers;
- one through many logical files and one through many indexes;
- callback/copy cost and event-loop/libuv delay; and
- database-wide flush with an unrelated foreground column-family workload.

The result establishes the cost of crossing JavaScript per operation and the blast radius of the
only current public durability barrier. It also provides the control against which the native lease
must improve.

### 3. Compare publication durability

Use the same object and metadata sequence for three candidates supported by existing rocksdb-js
primitives:

| Candidate | Object writes          | Object barrier                     | Metadata publication                                | Purpose                                              |
| --------- | ---------------------- | ---------------------------------- | --------------------------------------------------- | ---------------------------------------------------- |
| A         | WAL enabled, unsynced  | none                               | WAL enabled, synchronous batch in the same database | prevent the unsafe state through shared-WAL ordering |
| B         | WAL disabled           | current database-wide atomic flush | WAL enabled, synchronous batch                      | measure the existing flush alternative               |
| C         | WAL enabled and synced | none                               | WAL enabled, synchronous batch                      | upper-cost correctness control                       |

Candidate A tests the deeper prevention strategy: when RocksDB uses one ordered WAL for the database,
a synchronous metadata batch should make preceding WAL-enabled object writes durable without a
separate object barrier. Phase 0 must verify this from the pinned RocksDB behavior and live
rocksdb-js options. `unordered_write`, manual WAL flushing, a different database, or another option
that breaks this ordering disqualifies the candidate; the plan does not assume those settings.

For every candidate, kill the child process before, during, and after object writes, writer flush,
object barrier, metadata batch, reader reload, delete, and generation swap. Barrier candidates also
admit appends for the next publication while the barrier is in progress, then kill during the
barrier. The commit actor freezes the exact object and fragment set being published; later appends
cannot enter that head. Reopen verifies every fragment and length for every object referenced by a
visible head. The test must distinguish an expected older commit from corruption; merely reopening
or parsing the head without error is insufficient.

Candidate A is preferred if its ordering premise is proven and its WAL cost passes the fixed gates.
Candidate B is a control using rocksdb-js's existing database-wide flush; it does not justify a new
target-column-family primitive. It cannot ship if repeated publication causes foreground stalls.
Candidate C is not expected to ship; it establishes the cost of synchronizing every object write.

### 4. Prove lifecycle and identity

The candidate lease is exercised across worker environments, column-family views, close, drop, and
reopen:

- the same live database and column family resolve to one stable incarnation identity;
- a dropped and recreated column family has a new identity even when its name is reused;
- a closed and reopened database has a new identity;
- every admitted operation owns an `OperationGate` claim until completion;
- close rejects new lease operations, waits for admitted operations, and revokes attached state;
- a stale lease operation after close fails without dereferencing RocksDB, while a retained
  provider buffer can still be released through its provider-owned release context; and
- a lease from another rocksdb-js addon image or incompatible ABI fails before fulltext stores it.

The database uses its existing descriptor epoch as its incarnation. A process-lifetime monotonic
identifier is added to `ColumnFamilyDescriptor`, because a reused name or native address cannot
distinguish a dropped column family from its replacement. Successful drop revokes the old
descriptor before a replacement may be leased. A column family participating in VerificationTable
coordination rejects native leases, and a leased column family rejects VerificationTable
participation; the exclusion is column-family-wide rather than dependent on which JavaScript view
made the request.

### 5. Prove locks and watch stay in fulltext

All clones and independently constructed `KvDirectory` values for the same store identity and index
namespace share one fulltext-owned `DirectoryState`, including fair Tantivy locks, object-id
allocation, mutations, and watch callbacks. The production runtime owns the lifecycle of this
canonical state so independently constructed JavaScript wrapper objects cannot create multiple
Tantivy writers for the same index. RocksDB key locks protect a different concern and are not reused.

Watch is storage-local notification after successful atomic publication, not a RocksDB filesystem
watch. Phase 0 first proves that the shared fulltext runtime can notify all handles for the same
canonical storage identity. A new rocksdb-js watch primitive is justified only if worker/addon
identity tests show the wrapper cannot provide that notification without duplication or missed
commits.

## Candidate lease capabilities

The initial prototype header contains only the framing needed to reject an unsafe lease plus the
operations under test. The decision report classifies each operation as required, optional, or
rejected.

### Required framing

- fixed magic, ABI major/minor, structure byte length, and capability bits;
- provider package/build identity and embedded RocksDB version;
- process-addon image token;
- database and column-family incarnation identities;
- opaque lease context with retain/release;
- operation admission/release using the existing close fence; and
- caller-owned status buffers filled in place, plus provider-owned result-buffer release.

Validation is ordered. The Node value must be an External with the shared N-API type tag before the
consumer obtains its pointer. The pointed-to prefix contains only the fixed magic, ABI version, and
structure length; the consumer validates that prefix and the minimum length before reading
capabilities, identities, or any function pointer. Type tagging prevents accidental cross-casting;
the ABI checks remain mandatory because a type tag is not a security boundary against another
native addon. ABI v1 accepts only supported minor versions and the exact v1 caller-owned status
layout. After validation, fulltext copies the plain-data function table and build identity into
Rust-owned memory, then retains only the opaque provider context; the JavaScript External need not
remain reachable.

### Phase 0 operation table

- provider-owned point reads;
- one atomic ordered batch containing puts and deletes, with an explicit element stride and
  explicit `WAL`, `WAL_SYNC`, or `NO_WAL` policy;
- bounded prefix scan returning a provider-owned page;
- lease retain/release and state polling; and
- bounded transport statistics needed by the experiment.

The initial table deliberately excludes provider-pinned reads, `MultiGet`, target-column-family
flush, lock callbacks, and external SST ingestion. Owned results already avoid a second copy between
rocksdb-js and Tantivy: fulltext wraps the provider allocation directly in Tantivy `OwnedBytes`, and
its destructor invokes the provider's release callback. Pinned RocksDB blocks would introduce cache
residency and close-pressure contracts before measurements prove a need. Any excluded operation may
be proposed later only with an access trace and benchmark showing that the existing table cannot
meet a concrete gate.

The scan page encoding is versioned by ABI v1: a little-endian `u32` entry count, followed by each
entry's little-endian `u64` key length, little-endian `u64` value length, key bytes, and value bytes.
The provider limits a page to 4,096 entries and 64 MiB. The consumer validates every boundary,
integer conversion, and trailing byte before exposing slices.

`sync_directory()` does not call RocksDB flush. Under Candidate A, ordinary immutable-object writes
use `WAL`, and the atomic metadata publication uses `WAL_SYNC` in the same database. That synchronous
publication is the durability barrier for all preceding ordered WAL records. The Phase 0 crash
matrix must prove the ordering under the accepted RocksDB options; if it fails, Candidate A is
rejected rather than silently weakening `sync_directory()`.

The function table remains a C ABI owned by rocksdb-js. Every rocksdb-js entry point is `noexcept`,
catches all C++ exceptions, and converts them to a stable status plus bounded provider context.
Every returned allocation includes a provider-owned release operation; fulltext never assumes the
provider allocator. No Rust panic may cross a provider callback frame. Phase 0 uses fallible state
access and the Node export boundary to contain prototype failures. The production Directory adds
per-index panic containment and poisoning before the Rocks backend becomes public, so every later
operation on an affected Directory fails consistently. Fault tests inject C++ exceptions and Rust
panics immediately around provider calls on writer and merge threads and prove the Node process
remains alive.

The lease is deliberately not transaction-joinable. Harper's
[derived-index protocol](https://github.com/HarperFast/harper/issues/2489) is post-commit,
non-transactional, and recovers from a durable transaction-log watermark; joining the record
transaction would add request latency and rollback coupling while conflicting with that contract.
Standalone callers likewise receive lag-and-replay semantics rather than an API for passing
rocksdb-js transactions into Tantivy. Phase 0 verifies that a crash after a source commit but before
metadata publication leaves the older searchable head and that replay can advance it; it does not
attempt atomic source/index commit.

## Measurements and decision gates

Every result records OS, architecture, Node version, Rust version, Tantivy commit, fulltext commit,
rocksdb-js commit, embedded RocksDB version, database options, cache and write-buffer budgets, CPU,
memory, storage device, corpus seed, and workload parameters.

The Phase 0 report includes:

- operation throughput and p50/p95/p99 latency;
- event-loop delay and libuv occupancy for the JavaScript control;
- native calls, point reads, physical/requested/copied bytes, and allocations per logical read;
- ingest throughput, writer-flush latency, durability-barrier latency, and reload latency;
- WAL bytes, compaction bytes, write amplification, storage size, and cache hit behavior;
- requested and automatic flush counts with participating column families;
- foreground write p50/p95/p99 and stall time with no full text, JavaScript control, and each
  native durability candidate; and
- crash-matrix pass/fail results for every named kill point.

The following gates are fixed before measurements begin. Hardware qualification may add a stricter
release profile, but changing these values after seeing a result requires a reviewed plan amendment:

- zero contract, crash-matrix, sanitizer, or stale-lease failures;
- zero fulltext-attributable RocksDB write-stop events during the steady-state mixed workload;
- foreground point-write p99 regression no greater than 10% and 1 ms absolute versus the same
  offered load without full text;
- publication durability-barrier p99 no greater than 250 ms, reserving the rest of the one-second
  commit-to-searchable objective for indexing, commit, and reload;
- no unbounded queue, buffer retention, or RSS growth across a 30-minute soak;
- the native lease path must reduce Directory-operation p99 by at least 50% versus the JavaScript
  control at the same payload and concurrency, otherwise the extra ABI is not justified; and
- a new read primitive is considered only if profiles attribute at least 10% of real Tantivy query
  p99 to the current provider-copy/read path and the proposed primitive remains memory-bounded.

The mixed workload sweeps increasing full-text offered load. The report identifies the maximum safe
ingest rate that still passes the foreground gates rather than assuming an unprovided production
write rate. The catalog-scale product benchmark later decides whether that envelope is sufficient.

The adapter receives a **go** only when:

1. both backends pass the shared contract and all supported-platform crash cases;
2. no Directory operation invokes JavaScript on the sustained I/O path;
3. close, drop, and incarnation reuse cannot produce use-after-close or cross-generation access;
4. a durable published head never references missing or partial objects;
5. one chosen durability path has bounded completion and does not make foreground RocksDB behavior
   unsuitable for Harper's catalog workload; and
6. the measured capability table is small enough to remain a generic rocksdb-js storage lease
   rather than a Tantivy API embedded in rocksdb-js.

Phase 0 does not claim the product's sub-50 ms search p99; there is no search engine on this adapter
yet. It must preserve a credible latency budget by avoiding per-I/O JavaScript callbacks, unbounded
copies, libuv dependency, and routine database-wide flushes. Issue #15 owns the end-to-end target
once indexing and querying exist.

## Deliverables

- expanded backend-neutral Directory contract tests;
- test-only rocksdb-js lease producer and fulltext consumer;
- child-process crash/reopen runner with named fault points;
- real Tantivy create/commit/reload/search/reopen coverage;
- JavaScript-path and native-path microbenchmarks;
- machine-readable raw results and a concise decision report;
- final required/optional/rejected capability table for rocksdb-js issue #831; and
- follow-up issues for any proven rocksdb-js production primitives, each with its measurement or
  correctness evidence.

The report must state the selected object durability path, the frozen initial fragment-size range,
and any platform that failed qualification. If no candidate passes, the result is a documented
no-go; the wrapper does not fall back to filesystem storage in Harper.

## Approaches considered

### Different layer: implement Tantivy storage inside rocksdb-js

Rejected because rocksdb-js would then own Tantivy logical files, commit behavior, and release
coupling. Those are fulltext responsibilities and would prevent rocksdb-js from exposing a small
generic native-storage lease to other addon consumers.

### Deeper cause: eliminate a separate object barrier through ordered publication

Included as Candidate A rather than rejected. Write-once object fragments enter the database's
ordered WAL before a synchronous metadata batch. If Phase 0 proves the live RocksDB options preserve
that order through recovery, the metadata sync establishes object durability by construction. If
it does not, the existing database-wide flush is measured as Candidate B; Phase 0 does not add a
target-column-family flush.

### Do less: build the Directory over existing JavaScript CRUD and flush

Kept only as the benchmark control. It requires a JavaScript/N-API/libuv round trip for sustained
native Directory operations and currently offers only a database-wide flush, so it cannot meet the
execution and isolation requirements without evidence that contradicts the expected cost.

### Transport alternative: expose rocksdb-js C++ objects directly

Rejected because the real ownership problem is lifecycle and ABI, not pointer discovery. A raw
`rocksdb::DB*` or `ColumnFamilyHandle*` cannot preserve `OperationGate`, `Closable`, allocator,
incarnation, or provider-version invariants and would bind fulltext to rocksdb-js's C++ build.

### Chosen: a measured, versioned native lease over caller-owned rocksdb-js

This preserves one RocksDB owner and reuses its close, column-family, durability, and error
primitives. Phase 0 starts with a test-only table, measures each proposed operation, and promotes
only the minimum proven surface. fulltext continues to own the Tantivy mapping and all search
behavior; rocksdb-js continues to own RocksDB.
