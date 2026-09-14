# Native checkpoint publication

This unit supplies the native persistence prerequisite for Harper's derived-index integration
([#7](https://github.com/HarperFast/fulltext/issues/7) and
[#8](https://github.com/HarperFast/fulltext/issues/8)). It does not wire Harper, remove the
experimental hosted backend, or change replication and source-log retention.

## Contract

Add `publish(payload: string): Promise<bigint>` and read-only
`committedPayload: string | undefined` to `NativeFullTextIndex`. Payloads are opaque strings,
bounded by the existing 64 KiB UTF-8 limit. An empty string is a checkpoint; absence is not.

Publication enters the existing bounded writer queue alongside mutations. The writer commits
the documents and payload using Tantivy's prepared commit, reloads the shared reader, then
acknowledges success. No additional queue, checkpoint file, journal, or storage provider is needed.
Harper will supply its own checkpoint envelope in a later unit.

The invariant is that a generation which has committed a checkpoint cannot subsequently commit
without one. Enforce this on the writer when the operation executes, including after reopen.
A rejected plain `commit()` returns `E_CHECKPOINT_REQUIRED` without poisoning the handle or
discarding staged mutations. Standalone indexes that never publish retain separate `commit()`
and `reload()` behavior. A caller may switch such an index to checkpointed publication once.

Readback comes from Tantivy commit metadata on open, not inferred queue counters. Expose it in
the native open response using the existing optional-payload encoding. Update the native ABI
version and TypeScript loader together so old binaries cannot misdecode the response.

Publication failure after entering the writer makes the handle terminal under the existing
poison/close path. The payload getter must not claim an older checkpoint after an ambiguous
failure. Recovery is close, reopen, read the durable payload, then replay from it. A failed
reader reload can follow a successful commit; reopening may therefore expose the new payload.
Argument validation and queue admission failures do not change native state and must not by
themselves make readback uncertain. Overlapping publications must not move the getter backward
if JavaScript completions arrive out of order.

The getter throws `E_POISONED` on uncertain publication or observed native poison. Reuse
monotonic local publication sequence numbers: calls synchronously enqueue in that order, while
their callbacks may settle in another order. Track whether native admission succeeded so a
definite queue rejection does not invalidate a known checkpoint. A native opstamp is not a
Harper generation or cursor.

## Code boundaries

- Reuse the hosted `WriterOperation::Publish` for native callers rather than duplicate its
  commit/reload/error handling. Keep the existing hosted entry point as a compatibility delegate.
- Put checkpoint-preservation state with the engine writer, initialized from committed metadata
  after acquiring the writer. One read supplies both the guard and open response, preventing
  stale readback if another process publishes before we acquire its released writer lock.
  Do not reread files on every commit merely to enforce the guard.
- Share the TypeScript publication/readback implementation between native and hosted handles.
- Preserve existing dirty-close, rollback, lifecycle, queue limits, metrics and search behavior.
- Keep test fault injection behind the existing test feature and out of shipped binaries.

## Alternatives

| Approach                                   | Correctness                                             | Performance                          | Complexity                 | Compatibility                           |
| ------------------------------------------ | ------------------------------------------------------- | ------------------------------------ | -------------------------- | --------------------------------------- |
| Reuse prepared commit + shared publication | Documents and checkpoint share one commit               | Existing worker queue and one reload | Small shared API extension | ABI bump; standalone semantics retained |
| Separate checkpoint sidecar                | Requires a second crash-consistency protocol            | Extra writes and sync                | New recovery state machine | Unnecessary storage format              |
| TypeScript commit guard only               | Queued commit can race publication; raw ABI bypasses it | Cheap but unsafe                     | Apparent simplicity        | Cannot enforce invariant                |
| Duplicate native publication               | Same semantics possible                                 | No benefit                           | Two paths to maintain      | Hosted drift during migration           |

## Verification

Exercise public Node APIs against real native files: initial absence, empty and Unicode payloads,
boundary size validation, mutation visibility without explicit reload, reopen, idempotent replay,
cursor-only publication, overlapping calls, queued publish followed by commit, and rollback.
Confirm a rejected commit retains mutations and that a later publish can commit them.

Extend process-exit tests to published documents/checkpoints and subsequent uncommitted work.
Use deterministic test-only fault points at publication boundaries to exercise failures before
commit and after commit/before reload; reopen must report the checkpoint actually persisted.
Retain independent directory-failure engine tests. Test queue overload without payload poisoning.
Run Rust tests (including native-only compilation), Node tests, packed-consumer tests, formatting
and clippy. Review the plan and committed implementation with Claude and Gemini CLI.

The acceptance boundary is a tested native checkpoint API. Harper recovery coordination and
end-to-end performance benchmarks follow; this unit makes no catalog-scale latency claim.
Abrupt process-exit tests do not simulate power loss or prove filesystem sync ordering.

The shared publication state has direct tests for reordered callbacks. Admission is tracked
structurally by whether the native call returned normally, not by matching error codes.
Checkpoint-required rejection is handled before prepare-commit and remains nonterminal.

Lower-layer metadata interception would couple the wrapper to Tantivy's JSON format on every
write. A reserved checkpoint document would change schema identity and query filtering. Silently
carrying an old payload into plain commits would hide callers that advance content without its
checkpoint. None is needed for this contract.
