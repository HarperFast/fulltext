# Native checkpoint publication

Native checkpoint publication supplies the persistence boundary used by standalone callers and
Harper's derived-index integration. Fulltext stores the opaque checkpoint in Tantivy's commit
metadata; it does not add another checkpoint file, journal, storage provider, or RocksDB dependency.
Harper owns checkpoint contents, validation, replay, replication, and source-log retention.

## Contract

`NativeFullTextIndex` exposes `publish(payload: string): Promise<bigint>` and read-only
`committedPayload: string | undefined`. Payloads are opaque strings, bounded by the existing
64 KiB UTF-8 limit. An empty string is a checkpoint; absence is not.

Publication enters the existing bounded writer queue alongside mutations. The writer commits
the documents and payload using Tantivy's prepared commit, reloads the shared reader, then
acknowledges success.

The invariant is that a generation which has committed a checkpoint cannot subsequently commit
without one. Enforce this on the writer when the operation executes, including after reopen.
A rejected plain `commit()` returns `E_CHECKPOINT_REQUIRED` without poisoning the handle or
discarding staged mutations. Standalone indexes that never publish retain separate `commit()`
and `reload()` behavior. A caller may switch such an index to checkpointed publication once.

Readback comes from Tantivy commit metadata on open, not inferred queue counters. The native open
response carries the optional payload, and the TypeScript loader rejects an incompatible ABI before
decoding it.

Publication failure after entering the writer makes the handle terminal under the existing
poison/close path. The payload getter must not claim an older checkpoint after an ambiguous
failure. Recovery is close, reopen, read the durable payload, then replay from it. A failed
reader reload can follow a successful commit; reopening may therefore expose the new payload.
Argument validation and queue admission failures do not change native state and must not by
themselves make readback uncertain. Overlapping publications must not move the getter backward
if JavaScript completions arrive out of order.

The getter throws `E_POISONED` on uncertain publication. Reuse
monotonic local publication sequence numbers: calls synchronously enqueue in that order, while
their callbacks may settle in another order. Track whether native admission succeeded so a
definite queue rejection does not invalidate a known checkpoint. A native opstamp is not a
Harper generation or cursor.

Readback is local sequence state, not a synchronous native status call. Once payload-bearing
commit execution begins, the writer keeps its checkpoint guard even if the commit reports an
error: metadata may already have changed. Reopen determines whether anything persisted.

## Ownership boundaries

- Put checkpoint-preservation state with the engine writer, initialized from committed metadata
  after acquiring the writer. One read supplies both the guard and open response, preventing
  stale readback if another process publishes before we acquire its released writer lock.
  Do not reread files on every commit merely to enforce the guard.
- Preserve existing dirty-close, rollback, lifecycle, queue limits, metrics and search behavior.
- Keep test fault injection behind the existing test feature and out of shipped binaries.
- Keep the payload opaque. Fulltext must not parse Harper cursors or make replay and rebuild
  decisions.
- Use only the native Tantivy filesystem entry point. There is no hosted compatibility delegate.

## Verification

The native publication tests cover initial absence, empty and Unicode payloads, UTF-8 size limits,
reopen, idempotent replay by record ID, cursor-only publication, overlapping calls, queued commit
after publication, rollback, queue rejection, and abrupt process exit before and after commit. Rust
fault injection also covers a commit whose metadata write succeeds before an error is returned.

Admission is tracked structurally by whether the native call returned normally, not by matching
error codes. Checkpoint-required rejection happens before prepare-commit and remains nonterminal.
Abrupt process-exit tests cover process crashes; they do not qualify power-loss durability.

## Alternatives

| Approach                    | Disposition                                                                          |
| --------------------------- | ------------------------------------------------------------------------------------ |
| Tantivy prepared commit     | Chosen: documents and checkpoint become durable at one native commit boundary.       |
| Separate checkpoint sidecar | Rejected: it creates a second crash-consistency and reconciliation protocol.         |
| TypeScript-only guard       | Rejected: queued operations can establish a checkpoint after the enqueue-time check. |

Lower-layer metadata interception would couple the wrapper to Tantivy's JSON format on every
write. A reserved checkpoint document would change schema identity and query filtering. Silently
carrying an old payload into plain commits would hide callers that advance content without its
checkpoint. None is needed for this contract.
