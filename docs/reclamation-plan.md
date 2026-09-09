# Bounded Directory reclamation

Issue: [Implement Harper-backed Tantivy Directory mapping and atomic publication #11](https://github.com/HarperFast/fulltext/issues/11).

Reclamation may delete a chunk or tail only when no current logical binding, open file handle, or
active writer can reference it. It must also discover objects abandoned before their first
publication, remain bounded at hundreds of millions of source records, and never make search
readiness wait for a complete sweep.

## Grounding

The merged fulltext baseline is `2e853d4`. `KvDirectory` stores immutable 256 KiB chunks and
revisioned tails. `delete()` currently removes only logical bindings, each successful `flush()` can
leave the previous tail revision unreachable, and a crashed writer can leave staged chunks that no
binding ever named. `KvStore` supplies point read, atomic batch write, and sync, but no key
enumeration. Harper PR #2535 supplies the same narrow storage shape from Harper-owned RocksDB.

Tantivy's `ManagedDirectory` decides when a logical file name is retired. `KvDirectory` owns the
physical object behind that name and the lifetime of opened handles. Harper's derived-index runtime
owns source replay and exclusive index-writer lifecycle; it does not infer Tantivy object
reachability.

## Design

### Make every physical key derivable

Extend the binding with monotonic chunk and tail high-waters. Every payload write advances its bound
in the same atomic batch: staged chunk `n` records at least `n + 1`, and published tail revision `r`
records at least `r`. `flush()` updates the published fields and binding in that batch as well. The
complete physical key set is therefore arithmetic—chunk ordinals `0..chunk_high_water` and tail
revisions `1..=tail_high_water`—even when a writer crashes before publication. A payload key can
never become durable without a durable record that bounds its existence.

`delete()` atomically removes the binding and appends a reclaim entry containing the object id and
its high-waters to a durable FIFO. Tantivy 0.26.1's `ManagedDirectory` makes this transition complete:
it persists a managed path before calling `open_write()`, and garbage collection calls the
underlying `delete()` before removing that path from `.managed.json`. A crash-abandoned writer
therefore remains named by its binding and managed path until it is placed on the reclaim FIFO.

When `flush()` supersedes a non-empty tail, its publication batch appends a tail-only reclaim entry
for the old revision. This avoids deleting bytes needed by an open handle and prevents an active,
long-lived logical file from accumulating tail revisions until final deletion. Queue entries are
the durable consequence of the transition that made the bytes unreachable, not a second source-data
journal.

The FIFO is split into a fixed number of shards selected by object id. Each shard has its own head,
tail, sequence-addressed entries, and enqueue mutex; unrelated publishers do not wait on one global
host mutation. Enqueue and tail advance share the binding publication or deletion batch. Dequeue
progress and payload deletes share one WAL-only batch; final entry deletion and head advance share
one WAL-only batch. Missing payload keys are normal, and all operations are idempotent after crash.
Reclamation uses only existing point reads and atomic batch writes, so `KvStore`, the host protocol,
the TypeScript handler, and rocksdb-js gain no new primitive.

### Protect handles and writers

Replace the directory-wide mutation mutex with per-path shared/exclusive state plus a dedicated
allocator mutex for the read-modify-write object counter and per-shard FIFO-enqueue mutexes. The
path registry performs an O(1) lookup and never runs a whole-map retain on the indexing or reload
path. File-handle opens take a shared path gate while reading and registering the immutable binding;
opens on the same file remain concurrent. Chunk staging, flush, delete, and replacement take the
exclusive side, preventing delete from capturing a high-water while the old writer creates another
chunk. A deleted writer is marked retired so a later write cannot stage new payload.

Pins are indexed by object id, not logical path, so delete/recreate cannot hide handles to the prior
object. A handle owns an `Arc` pin, and drop only decrements the `Arc`. Pin insertion prunes dead weak
entries above a small fixed threshold, and FIFO visits prune them again. This bounds control-block
retention during repeated open/drop churn. Writer state is likewise retained through failed and
unterminated writes. Gates recover poisoned state rather than panicking the writer actor.

Cleanup never holds a path gate or pin-registry lock across storage I/O. A retired object cannot gain
a new pin because its binding is gone; a tail-only entry can gain no new pin for its old revision
after the newer binding is published. Whole-object entries match any pin for that object, while
tail-only entries match only the exact object id and tail revision. If a matching pin remains, the
entry is rotated to its shard's tail with bounded backoff rather than blocking later garbage.

This process-local pin model is valid only while Harper's derived-index lifecycle guarantees one
active generation owner. Cleanup requires an explicit owner lease supplied by that lifecycle; the
production host cannot invoke it without one. Admission checks the lease before work, and worker
loss or close revokes admission, fails the transport before joining, waits for any definitive
mutation result, and drains the cleanup task. The Directory does not invent a second cross-process
lock or compare-and-set protocol that the supported storage contract cannot enforce.

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

| Axis                     | Candidate and disposition                                                                                                                                                                                              |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Different layer          | RocksDB compaction and Harper log retention cannot see Tantivy handles. `ManagedDirectory` owns logical retirement, while `KvDirectory` owns physical retirement.                                                      |
| Discovery                | Prefix enumeration or a dense object-id sweep. Both do work proportional to stored history rather than garbage and add machinery that transition records avoid.                                                        |
| Different timing         | Delete up to a fixed number of chunks synchronously in `delete()` and enqueue only the remainder. This may help small objects, but it lengthens Tantivy metadata GC and is deferred until measurement shows a net win. |
| Lower-layer range delete | Add a range-tombstone primitive. This expands the frozen Harper storage surface and makes foreground reads pay tombstone checks until compaction in a shared column family, so it is rejected for the first release.   |
| Deeper cause             | Record existence in the batch that creates each key and enqueue retirement in the batch that makes it unreachable. This is the chosen foundation.                                                                      |
| Do less                  | Reclaim only final object deletion and leave superseded tails until then. Real Tantivy flush-count measurement remains a gate, but tail-only FIFO entries cheaply bound the general Directory contract.                |
| Chosen                   | Binding high-waters plus a transition-fed durable FIFO, with object-id pins and bounded low-priority draining.                                                                                                         |

## Persistence format and delivery sequence

Namespace keys require a versioned, length-prefixed encoding so delimiter-containing namespaces
cannot alias. Because that changes the prototype format, land it as a focused prerequisite change
with an explicit format marker and rejection of old prototype data; no migration is promised for
the unreleased prototype. Do not silently create an empty new-format index beside old keys.

Then deliver reclamation in reviewable slices:

1. versioned namespace encoding and format validation;
2. binding high-waters and atomic updates at chunk staging and flush; update the writer replacement
   guard and in-memory binding together so high-water-only changes cannot look like path replacement;
3. per-path shared/exclusive state, object-id/revision pins, and atomic sharded-FIFO enqueue at tail
   supersession and object deletion; key writer retirement by object id so path reuse cannot retire
   the replacement writer;
4. bounded FIFO draining, low-priority admission, failure observability, close fencing, crash
   recovery, and Harper's
   exclusive-owner integration.

The production Harper package remains disabled until slice 4's lifecycle fence and Harper host-
storage integration both pass. No native RocksDB storage provider is added to the public library.

## Verification

The mapping tests cover repeated flush, delete/recreate with a retained old reader, abandoned
writers, partial object cleanup, restart during a range and between FIFO entries, namespace
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
