# Terminal writer admission

## Scope and invariant

This fixes the shared native runtime's writer admission race before broader concurrent Harper integration. It changes no public API signature, ABI, storage format, queue limits, or Harper protocol. Late writer requests now reject with `E_POISONED` instead of executing on a terminal generation. Native Tantivy files remain the delivery target.

**Invariant:** once poison has drained the writer queue, a request that previously observed an open runtime cannot enter that queue. Close remains available to release the writer and cannot reopen a generation that has become poisoned.

## Failure on the baseline

Before this fix, traced at commit `d188ecc` in `src/native.rs`:

- `Runtime::enqueue_writer` checks `require_open()` before `BoundedQueue::try_push` takes the queue mutex. Apply, commit, publish, and reload share this path.
- `Runtime::poison` stores `STATE_POISONED`, drains the writer queue under its mutex, and preserves one queued close by converting it to rollback.
- A producer can pass the state check, pause, and enqueue after that drain. The writer loop does not revalidate such a command before executing it.
- Search poison closes its queue permanently. Writer poison cannot use that mechanism unchanged: the writer actor still needs to receive close.
- Normal close changes the runtime state before force-enqueuing its control command.
- A dirty close currently stores `STATE_OPEN` unconditionally. If poison occurs after close's state transition but before close enters the queue, that command misses the drain and can reopen a terminal generation.

## Approaches considered

- **Different layer:** reject in the TypeScript wrapper. Native actor failure occurs concurrently with JavaScript, so a wrapper check cannot fence native queue insertion. Direct addon callers would also remain affected.
- **Deeper cause:** combine runtime state and both queues into one lifecycle lock. This could express a wider state machine, but would couple writer/search admission and require changing close, status, and failure transitions. This race needs only the existing writer-queue mutex: poison stores the terminal state before taking that mutex to drain.
- **Do less:** check again when the writer dequeues, or close the writer queue on poison. A dequeue check leaves work counted as accepted after the drain rather than rejecting it at admission; fully closing the queue prevents the existing rollback-close control path. Automatic terminal teardown could remove that control path but would change handle retention, status, and cleanup ownership beyond this fix.
- **Chosen:** validate runtime state inside the existing bounded-queue insertion critical section. Keep the early writer check for prompt rejection, and revalidate under the mutex through `try_push_if`. Search keeps its existing `try_push` behavior. `push_force` remains the close-only capacity bypass. Replace dirty close's unconditional reopen with `compare_exchange(CLOSING, OPEN)`; if the transition loses to poison, close rolls back and tears down instead. No extra mutex or scheduling lane is introduced.

## Ordering and cost

If insertion wins the queue mutex and observes open, the command is admitted before the drain and is either already dequeued or drained on poison. If the drain wins, subsequent insertion sees poisoned while holding the same mutex and fails. The state store precedes the drain, so no producer can pass the guarded check and insert after a completed drain. Already-dequeued operations are not made cancelable by this change.

If ordinary close races admission, a request either observes closing under the queue mutex and rejects, or is inserted before the close command can acquire that mutex. A dirty `require-clean` close returns `E_DIRTY_CLOSE` only if it successfully restores `CLOSING` to `OPEN`. Losing that transition to poison instead rolls back and tears down, matching default close on an already-poisoned handle. Successful close acknowledges teardown, not publication of pending writes. An already-running clean close may finish concurrently with a later poison without calling rollback; close does not commit staged writes.

The added successful-path work is one atomic state load under a mutex already acquired for each writer insertion. No I/O, callbacks, decoding, or native work runs inside the validator. Rejection builds the existing error type, as capacity rejection already does; there is no new error representation. Completion failures and close preservation remain outside the queue lock. Capacity checks and counters retain their existing semantics. Search error codes are unchanged.

## Verification

- `test/native-admission.test.mjs` uses a test-build-only, one-shot per-handle fault seam immediately after the early writer check, before queue insertion. The same seam fires after close's state transition and before its control insertion. It never executes while holding the queue mutex.
- The four writer cases reject with `E_POISONED`, retain zero queued commands/bytes in these no-close-queued fixtures, and preserve the saved checkpoint. Both those cases and raced dirty close verify native handle removal and reopen with exactly the saved document set.
- The local negative-control run used the baseline production logic with only the fault seam added. All five Node cases failed on their intended assertions. The fixed tests run in CI; the negative-control source was not retained as a shipped alternate implementation.
- Rust queue tests assert the validator runs under the mutex, force a producer across a completed drain with channels, and check capacity accounting and the close-only bypass. These supplement rather than replace the real-addon cases.
- Existing dirty-close, worker teardown, publication failure, bounded-admission, and independent-index tests remain part of `npm test`. This does not claim complete coverage of every close/search-panic interleaving.
- The full gates are `npm run format:check`, `npm run lint`, and `npm test` (Rust, Node integration, packed-consumer). Native-only compilation uses `cargo check --locked --no-default-features --features node-api`; `npm run benchmark:smoke` verifies benchmark execution, not a large-catalog latency target.

Process-wide budgets, general shutdown-state redesign, panic cleanup, and Harper lifecycle wiring remain separate work under the existing execution and integration issues. This change does not complete the broader bounded-execution issue.
