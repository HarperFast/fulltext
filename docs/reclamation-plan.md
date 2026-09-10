# Bounded Directory reclamation

Issue: [Implement Harper-backed Tantivy Directory mapping and atomic publication #11](https://github.com/HarperFast/fulltext/issues/11).

Reclamation may delete a chunk or tail only when no current logical binding, open file handle, or
active writer can reference it. It must also discover objects abandoned before their first
publication, remain bounded at hundreds of millions of source records, and never make search
readiness wait for a complete sweep.

## Grounding

This plan is written against fulltext main at `36d96e3`, the rebase-merged result of
[Enqueue deleted directory objects for reclamation #26](https://github.com/HarperFast/fulltext/pull/26).
`KvDirectory` stores immutable 256 KiB chunks and revisioned tails. `delete()` atomically removes a
logical binding and enqueues the object's derivable physical extent, but no consumer deletes those
payloads yet. Each successful `flush()` can leave the previous tail revision unreachable. `KvStore`
supplies point read, atomic batch write, and sync, but no key enumeration. Harper PR #2535 supplies
the same narrow storage shape from Harper-owned RocksDB.

Tantivy's `ManagedDirectory` decides when a logical file name is retired. `KvDirectory` owns the
physical object behind that name and the lifetime of opened handles. Harper's derived-index runtime
owns source replay and exclusive index-writer lifecycle; it does not infer Tantivy object
reachability.

## Design

### Make every physical key derivable

Extend the binding with monotonic chunk and tail high-waters. Before staging a chunk outside the
current bound, the writer durably reserves a fixed stride of chunk ordinals in the binding. The
reservation write completes before any covered payload write is issued; RocksDB WAL prefix ordering
and the later publication barrier therefore cannot recover or publish a chunk without its earlier
bound. Tail payload and tail high-water advance together in the existing publication batch. The
complete possible physical key set is arithmetic—chunk ordinals `0..chunk_high_water` and tail
revisions `1..=tail_high_water`—even when a writer crashes before publication. Bounds may include
keys that were never written; missing payload keys are normal during reclamation.

Slice 2 encodes these fields in binding format v3 and advances the directory key-format marker so
every storage entry point rejects a slice-1 namespace at one choke point. The chunk high-water is an
exclusive ordinal and must be at least the published full-chunk count. The tail high-water is the
greatest possible revision, including revisions advanced by an empty-tail flush, so the published
tail revision must never exceed it. Decoding rejects malformed lengths, inconsistent published
lengths, either violated bound, and a possible physical extent above the format's 1 TiB hard limit
before any cleanup work can use the values. Harper may impose a lower operational limit, but the
library does not expose a customer setting for the on-disk safety bound. The prior unreleased key and
binding formats are rebuilt rather than migrated.

The first chunk in each 64-ordinal reservation takes the existing directory mutation gate, rereads
the binding, and verifies that the object id and published file state still match the writer. A
stored reservation ahead of the writer is adopted as the result of an applied-but-reported-failed
attempt; a lower stored bound is corruption. Otherwise one binding mutation reserves the next
stride. Covered chunks keep the current single-put path without a binding read or directory gate.
An applied-but-reported-failed chunk put is retried idempotently with the same bytes and ordinal.
Slice 3 installs per-path writer retirement before enabling cleanup, so delete cannot drain a
captured reservation and then allow its old writer to stage into it. Slice 2 alone does not claim to
reclaim a deleted binding and remains unavailable to production.

`flush()` treats object identity and published fields—not bookkeeping-only high-waters—as the file
replacement guard. It preserves the greatest stored bounds and allocates every revision above both
the published revision and tail high-water, even when the new tail is empty; this makes the decode
invariant unconditional and prevents later payload reuse. Publication writes any tail payload,
updated bounds, and published binding in one WAL batch. A retry recognizes an already-published
intended binding as success before accepting later bytes; a definitely unapplied attempt is cleared
and recomputed from the preserved writer buffer. Any other published-state change is replacement.
All mutation gates touched by this slice recover poisoned mutex state and return storage errors
rather than creating a repeatable actor panic.

The 64-chunk stride bounds overstatement to less than 16 MiB per reservation while reducing a 1 GiB
file from 4,096 binding reads and updates to 64. A `CountingKv` test pins reservation reads, write
calls, mutation count, and overstatement so the stride remains an explicit performance choice rather
than an accidental constant. The existing host-transport large-file test exercises reservation,
chunk staging, and publication through the Node boundary; host operation counts remain a production
integration measurement.

`delete()` atomically removes the binding and appends a reclaim entry containing the object id and
its high-waters to a durable FIFO. Tantivy 0.26.1's `ManagedDirectory` makes this transition complete:
it persists a managed path before calling `open_write()`, and garbage collection calls the
underlying `delete()` before removing that path from `.managed.json`. A crash-abandoned writer
therefore remains named by its binding and managed path until it is placed on the reclaim FIFO.

Whole-object deletion already covers every tail revision through `tail_high_water`. Tail-only
enqueue is deferred to slice 4 and retained only if measurement shows that repeated non-empty tail
publication materially increases live-index storage before final file deletion. If retained, the
publication batch appends the exact old revision so bytes cannot be deleted while an earlier handle
still references them. Queue entries are the durable consequence of the transition that made bytes
unreachable, not a second source-data journal.

The FIFO is split into a fixed number of shards selected by object id. Each shard has its own head,
tail, sequence-addressed entries, and enqueue mutex; unrelated publishers do not wait on one global
host mutation. Enqueue and tail advance share the binding publication or deletion batch. Dequeue
progress and payload deletes share one WAL-only batch; final entry deletion and head advance share
one WAL-only batch. Missing payload keys are normal, and all operations are idempotent after crash.
Reclamation uses only existing point reads and atomic batch writes, so `KvStore`, the host protocol,
the TypeScript handler, and rocksdb-js gain no new primitive.

### Protect handles and writers

Replace the directory-wide mutation mutex with per-path lifecycle state, a strided object-id
allocator, and per-shard FIFO-enqueue mutexes. The allocator durably reserves a fixed range under its
mutex and hands out ids from that range without storage reads; gaps after failure or restart are safe
in the `u64` identity space. The path registry
performs an O(1) lookup and never runs a whole-map retain on the indexing or reload path. In slice 3,
only `open_write()`, `atomic_write()`, and `delete()` take the path's exclusive lifecycle gate;
file-handle opens remain unchanged until pin registration lands. Atomic-file writes use that gate to
preserve completion order against deletion; their short-lived registry allocation is accepted next
to the required synchronous host write. A writer owns an atomic retirement and in-flight-operation
fence. Each logical `Write::write()` or `Write::flush()` takes one claim and holds it across all
dependent storage operations, rechecking retirement after registering in flight with sequentially
consistent ordering. Delete
retires the writer, waits for existing claims to reach their definitive result, then rereads the
binding before enqueueing its final high-waters. Host mutations are deliberately uncancellable once
dispatched, so writer retirement uses the same unbounded completion contract instead of inventing
an unrelated timeout. Closing the host wakes pending operations and releases their claims. No path
gate is held by chunk staging or publication across a host round trip.

Pins are indexed by object id, not logical path, so delete/recreate cannot hide handles to the prior
object. Each object has counted pins by tail revision. A handle owns one pin and retains the shared
directory and path state; dropping it decrements the revision and object counts, and dropping the
last pin removes the object's weak registry entry. Registry shards use the same object-id partition
as reclamation, so unrelated objects do not share one pin mutex. This bounds retained control state
by live handles during repeated object, revision, and open/drop churn. Writer state is likewise
retained through failed and unterminated writes. Gates recover poisoned state rather than panicking
the writer actor. Pins land in the first slice 4 unit before the FIFO consumer. A handle reads its
binding and registers the pin under the shared side of a fixed hash-sharded registration fence.
Deletion takes the exclusive side of the same shard only for its final binding-removal batch, so
concurrent opens remain parallel except for hash collisions and writer retirement does not block
them. This preserves one storage read per handle open and adds no directory-wide coordination. The
consumer follows only after these lifetime rules are independently covered. Harper may enable
cleanup only after a two-worker test proves that every local search
handle for one RocksDB-backed generation reaches the same native `DirectoryState`. A process that
cannot establish that invariant cannot obtain the cleanup owner lease.

Cleanup never holds a path gate or pin-registry lock across storage I/O. A retired object cannot
gain a new pin because its binding is gone. Before tail-only reclamation is enabled, publication of
the newer binding and its superseded-tail entry must take the exclusive side of the same
registration fence used by deletion. That prevents a reader from registering the old revision
after publication. Whole-object entries match any pin for that object, while tail-only entries
match only the exact object id and tail revision. If a matching pin remains, cleanup moves the entry
to a cleanup-only deferred queue with bounded backoff rather than blocking later garbage.
Foreground publishers only mutate the ingress tail; cleanup only mutates the ingress head and
deferred queue. A low-priority cleanup host operation therefore never owns the mutex or counter
needed by a foreground enqueue.

This process-local pin model is valid only while Harper's derived-index lifecycle guarantees one
active generation owner. Cleanup requires an explicit owner lease supplied by that lifecycle; the
production host cannot invoke it without one. Admission checks the lease before work, and worker
loss or close revokes admission, fails the transport before joining, waits for any definitive
mutation result, and drains the cleanup task. The Directory does not invent a second cross-process
lock or compare-and-set protocol that the supported storage contract cannot enforce.

### Slice 3 transition rules

Slice 3 adds durable enqueue and writer-lifetime protection, but deliberately does not read or drain
the reclaim FIFO and does not install unused reader pins. The invariant for this slice is: once a binding transition makes bytes
unreachable, the same atomic WAL batch records exactly one reclaim entry, and no prior writer can
stage more bytes for the retired object after that transition begins.

The path registry stores weak references and removes only the requested stale entry during lookup.
The final path-state drop compares its weak identity under the registry lock before removing its own
entry, so replacement cannot trigger an ABA removal. The registry is touched once per writer
lifecycle, atomic-file transition, and deletion, not by chunk staging or file-handle reads. Each
writer retains the shared `DirectoryState` as well as its path state, so reconstructing a Directory
while a writer remains open converges on the registry that contains its fence. Each path state
contains an exclusive lifecycle gate and a weak reference to the active writer, whose object id is
checked before retirement. Each writer call uses an RAII in-flight claim spanning its dependent
storage operations and rechecks retirement with `SeqCst` ordering after registering in flight. Delete
retires only the writer whose object id matches the binding, waits for its in-flight claims to reach
zero, and rereads the binding before constructing the retirement entry. Different paths therefore proceed
independently except when replenishing the strided object-id allocator or enqueueing onto the same
reclaim shard.

Each reclaim shard is a zero-based, monotonic FIFO. Its persisted tail is the next unused sequence;
an absent tail means zero. Foreground enqueue rereads that durable tail under the shard mutex for
every deletion; it never allocates from a cached value. It also reads the current slot and fails
`InvalidData` if occupied, exposing pre-existing queue corruption instead of overwriting an entry.
That point read cannot detect two incorrectly separated `DirectoryState` instances that both observe
an empty slot before either writes; the canonical-identity and single-owner gates are therefore
load-bearing correctness requirements, not redundant checks. Enqueue writes the sequence-addressed entry and advances the tail in the same
deletion batch. Slice 3 entries contain the object id plus both high-waters. The shard count and later
pin-pruning cadence are internal hard bounds, not customer configuration.

Delete retires a matching writer before issuing its batch because a returned I/O error may still
mean the batch committed.
Tantivy 0.26.1 treats `FileDoesNotExist` as a successful garbage-collection retry, so a committed
delete whose result was lost does not require a path tombstone or another storage primitive.
An undecodable binding fails deletion with `InvalidData`; Harper marks that derived generation for
rebuild rather than guessing an object extent or allowing Tantivy to retry it indefinitely.
A definitively failed delete rereads the binding while it still owns the path lifecycle gate. If the
same binding remains, the writer fence is reopened and the error is returned; if the binding is gone,
the atomic enqueue also committed and delete succeeds. An unreadable or unexpected binding keeps the
writer retired and fails closed. Invalid persisted directory or queue data is a
generation-corruption signal; slice 4 must surface that terminal health state to Harper before
production is enabled rather than relying on Tantivy's repeated GC attempt.

Queue writers for one namespace must share one `DirectoryState`. This follows the existing
`KvStoreIdentity` provider contract, not the transport handle: the identity must represent the live
RocksDB store incarnation and derived column family. The production host stays disabled until the
Harper integration proves that two transports for the same generation converge on that identity and
that only the derived-index owner can write. A test-only transport handle may identify a store only
when that handle is its sole access path.

### Reclaim with bounded point operations

The worker point-reads only the FIFO head, its current entry, and bounded progress. A whole-object
entry deletes derived chunk and tail ranges over as many batches as necessary. A tail-only entry
deletes one derived key. A pinned entry rotates behind other work; repeated rotations are rate-limited
and surface a blocked-reclamation health state rather than consuming the JavaScript service thread.
Cleanup cost is proportional to garbage queued, never to object ids or records ever created.

Cleanup runs on a dedicated native task, not the JavaScript service thread or writer actor. Host
callbacks still execute on JavaScript, so cleanup has a low-priority admission class that cannot
take the last foreground transport slot. Each admission bounds point reads, delete mutations,
request bytes, and elapsed time checked between storage operations. One admitted synchronous host
operation cannot be canceled and may exceed the elapsed budget. Panics are caught at the task
boundary; terminal failure, queue depth, pinned rotations, and no-progress state are observable by
Harper instead of silently disabling reclamation.

Binding, queue-entry, and progress decoding validates versions, lengths, numeric ranges, and a
configured maximum total extent before allocating or scheduling work. An undecodable binding is not
guessed from point misses: this is a derived index, so the generation is marked corrupt and rebuilt
from Harper source data. A caught cleanup panic enters the same terminal health state. Neither case
silently retries forever or advances past unknown data.

## Alternatives

| Axis                     | Candidate and disposition                                                                                                                                                                                                                                                                                                               |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Different layer          | RocksDB compaction and Harper log retention cannot see Tantivy handles. `ManagedDirectory` owns logical retirement, while `KvDirectory` owns physical retirement.                                                                                                                                                                       |
| Discovery                | Prefix enumeration or a dense object-id sweep. `KvStore` exposes no enumeration primitive, and either scan would do work proportional to stored history rather than garbage.                                                                                                                                                            |
| Managed paths            | Treat Tantivy's `.managed.json` as the discovery source. It names logical paths, not object ids, chunk ordinals, or tail revisions, so delete/recreate cannot recover the retired object's extent.                                                                                                                                      |
| Bound placement          | Update the binding with every payload, reserve bounded strides in the binding, or add a separate per-object extent key. Strided binding reservation is chosen: it keeps deletion capture atomic in slice 3 without per-chunk host reads.                                                                                                |
| Different timing         | Delete up to a fixed number of chunks synchronously in `delete()` and enqueue only the remainder. This may help small objects, but it lengthens Tantivy metadata GC and is deferred until measurement shows a net win.                                                                                                                  |
| Lower-layer range delete | Add a range-tombstone primitive. This expands the frozen Harper storage surface and makes foreground reads pay tombstone checks until compaction in a shared column family, so it is rejected for the first release.                                                                                                                    |
| Deeper cause             | Record existence in the batch that creates each key and enqueue retirement in the batch that makes it unreachable. This is the chosen foundation.                                                                                                                                                                                       |
| Do less                  | Slice 3 enqueues only whole-object deletion. It leaves superseded tails within the object's high-water until final deletion; tail-only enqueue moves to slice 4 and ships only if measured tail accumulation justifies publication-path cost.                                                                                           |
| Higher-layer rotation    | Rebuild into a fresh Harper generation and drop the old column family. This remains the corruption-recovery path, but routine reclamation would require replaying hundreds of millions of records and would move cleanup outside the standalone library.                                                                                |
| Reader lifetime          | Process-wide object/revision `Arc` pins are chosen because they exactly model Tantivy handle lifetime without a read-time storage call. A Harper two-worker shared-state test is an enablement gate. A searcher epoch would require a new cross-layer reader API, and a wall-clock grace cannot prove that a reader released the bytes. |
| Enqueue naming           | An object-id-addressed record requires scanning every allocated id because `KvStore` has no enumeration. Rewriting a tombstone binding under the logical path loses the old object on path reuse. A persisted sharded sequence FIFO is chosen because its point-read cost is proportional to garbage transitions.                       |
| Writer fence             | Holding a path lock across each chunk write would add lock convoying around an uncancellable host round trip. A per-writer atomic retired/in-flight fence is chosen: delete closes admission and waits before its final binding read, while chunk staging adds no mutex or allocation.                                                  |
| Object-id allocation     | Updating the counter with every binding creation serializes file creation across a host round trip. The allocator reserves fixed durable strides, reducing that shared operation to one per stride; unused ids after failure or restart are harmless.                                                                                   |
| Queue-tail reads         | Cache the next sequence and reread only after errors, or point-read it before every enqueue. The durable point read is chosen because slice 3 enqueues only file deletion rather than indexed documents, removes cache recovery state, and is measured explicitly.                                                                      |
| Cleanup priority         | Cleanup does not rotate through the foreground ingress tail. A cleanup-only deferred queue lets the consumer advance past pinned entries without holding a foreground enqueue mutex or counter across low-priority host I/O.                                                                                                            |
| Chosen                   | Binding high-waters plus a transition-fed durable FIFO, with object-id pins and bounded low-priority draining.                                                                                                                                                                                                                          |

## Persistence format and delivery sequence

Namespace keys require a versioned, length-prefixed encoding so delimiter-containing namespaces
cannot alias. Because that changes the prototype format, land it as a focused prerequisite change
with an explicit format marker and rejection of old prototype data; no migration is promised for
the unreleased prototype. Do not silently create an empty new-format index beside old keys.

Then deliver reclamation in reviewable slices:

1. versioned namespace encoding and format validation;
2. binding high-waters and atomic updates at chunk staging and flush; update the writer replacement
   guard and in-memory binding together so high-water-only changes cannot look like path replacement;
3. per-path lifecycle state, a strided object-id allocator, an atomic writer fence, and sharded-FIFO
   enqueue at object deletion; key writer retirement by object id so path reuse cannot retire the
   replacement writer;
4. add object-id/revision reader pins and coordinate registration with deletion through the
   hash-sharded registration fence;
5. measure and, if justified, add tail-supersession entries; then add bounded FIFO draining and
   deferred queues, low-priority admission, failure observability, close fencing, crash recovery, and
   Harper's exclusive-owner integration.

The production Harper package remains disabled until the cleanup lifecycle fence and Harper host-
storage integration both pass. No native RocksDB storage provider is added to the public library.

## Verification

Slice 2 adds an applied-then-report-failure mode to `FaultingKv` and an invariant audit over its
physical keys. Before deletion is introduced in slice 3, every visible or crash-recovered chunk and
tail must fall within its surviving binding's bounds. Tests cover reservation failure and adoption,
idempotent chunk retry, already-published flush retry, empty-tail bounds, former-format rejection at
every entry point, decode-time extent limits, and exact reservation I/O counts.

Slice 3 tests the deletion batch directly: object deletion produces one whole-object entry, and a
confirmed committed delete cannot duplicate or overwrite it on retry. Every enqueue reads the
persisted shard tail and the target slot, including after reopen. A deterministic admission test
pins the post-registration retirement recheck; barriered writer/delete races prove that deletion
waits for both publication and chunk-storage claims and that queued bounds cover the result.
Different-path deletion and different-shard enqueue remain concurrent. A definitively failed delete restores a matching writer,
while a confirmed committed delete leaves it retired. `CountingKv` fixes the active-writer and
closed-writer point-read, write-call, and mutation deltas for delete and proves that a covered chunk write adds no read or mutex-backed
storage operation. Atomic-only deletion neither decodes nor enqueues a binding. Reclaim entries
round-trip their exact extents, and malformed bindings, entries, and queue tails fail as
`InvalidData` before they can schedule work. A whole-store audit rejects an entry that names a still
live object and verifies that every deleted object's physical keys fit its queued high-waters after
real Tantivy merge and garbage-collection cycles and after crash/reopen. `FaultingKv` models WAL
recovery at atomic batch boundaries; the Harper integration separately proves that one host write
request commits as one RocksDB batch.

The dependency-free `kv_directory` release benchmark compares adjacent merged slices through
Tantivy's public directory interfaces. It reports per-sample-mean p50/p95/p99 and aggregate
throughput for caller write sizes, empty and dirty flushes, chunk publication, deletion with closed
and active writers, retained and churned read-handle opens, and distinct-file read-open and write
concurrency. The empty-flush case isolates the per-call retirement-fence cost; the buffered cases
show how Tantivy's writer amortizes it in practice. Retained handles measure registration growth,
while churned handles exercise drop-time pin removal. Results are versioned JSON labeled by
revision. Shared CI runs a correctness smoke with no timing threshold; performance decisions use
alternating runs on one fixed host. The deterministic Phase 0 store removes RocksDB and Node
transport variance but serializes access, so its concurrency results detect directory-coordination
regressions rather than predicting RocksDB scaling.

The completed mapping tests cover repeated flush, delete/recreate with a retained old reader,
abandoned writers, partial object cleanup, restart during a range and between FIFO entries, namespace
isolation, concurrent `open_write()` on distinct paths, and cleanup concurrent with Tantivy merge
completion. A deterministic hook forces the binding-read/pin-register race. Failure injection
covers staging and high-water updates, publication and tail enqueue, object retirement and enqueue,
payload deletion, progress updates, rotation, and head advance; every crash/reopen result must expose
the old complete binding, the new complete binding, or logical absence—never a reference to missing
bytes.

Efficacy is measured as well as safety: after repeated real Tantivy merge/delete cycles, physical
key count and payload bytes must return to a bound proportional to the live index rather than bytes
ever written. `CountingKv` asserts per-entry point-read and mutation cost does not grow with object
ids ever allocated, plus request-byte and time-admission bounds. Measure real Tantivy tail revisions
per file before retaining the tail-only path. Add a hot-path regression test showing a foreground
read still gains transport capacity while cleanup waits, plus open/drop churn that keeps weak-pin
memory bounded. The same reclamation harness runs through host transport; Harper separately verifies
owner loss with a second worker, close drain, cleanup health reporting, backup/restore, and
derived-index replay coordination.

The FIFO tests include parallel enqueue on different shards, sequence/batch failure, pinned-entry
rotation cost, corrupt binding and entry handling, independent namespaces on one store, and a
numeric post-drain bound. Host tests drain to quiescence, assert at least one entry was reclaimed,
and run foreground reads with cleanup occupying every cleanup-eligible transport slot.

Run formatting, lint, all-feature Rust tests, Node tests, packed-package checks, and local Claude and
Gemini reviews before opening each implementation PR.
