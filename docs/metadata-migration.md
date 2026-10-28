# Metadata index migration

Status: draft; procedure awaiting human API/migration review.

## Decide whether to rebuild

The metadata implementation adds raw fast fields to string `filterFields`. Existing string-metadata
indexes need rebuilding. Inspection returns `{ state: 'incompatible', code: 'E_IDENTITY_MISMATCH' }`;
reader and writer opens reject with the same code. Do not rewrite the identity sidecar or bypass
schema validation.

Numeric/boolean-only indexes retain compatibility when the rest of the configuration is unchanged.
Always deploy the JavaScript facade and native addon from the same revision; compatibility of stored
numeric data does not make the old addon understand the new query API.

## Rebuild from authoritative records

1. Pause requests and ingestion for the affected index. Drain work and close **every** reader and
   writer using its path. An `E_QUIESCENCE_FAILED` close requires process restart before reset.
2. Preserve the authoritative records and their source checkpoint. The derived index is not the
   source for reconstruction.
3. With the new build, retire the old directory:
   `await resetNativeFullTextIndex({ path, indexId })`. Keep the same logical index ID. Reset
   atomically renames the old directory; it does not migrate its documents.
4. Open the path with a new `generation`, rebuild all required source records, then call
   `publish(sourceCheckpoint)` only after the rebuild covers that checkpoint. Resume ingestion
   according to the consuming application's checkpoint protocol.
5. Close and reopen; verify `inspectNativeFullTextIndex(options)` reports `checkpointed` with the
   expected `committedPayload`. Verify text/filter queries, sort ordering, and aggregations against
   the source records before switching application reads.
6. Reclaim the retired directory when rollback no longer needs it:
   `await reclaimRetiredNativeFullTextIndexes({ path, retiredPath: resetResult.retiredPath })`.
   Check `failed` is zero. To downgrade a rebuilt index, reset using the new build and rebuild using
   the old build; the old build does not understand the new string schema.

The [reset/rebuild example](../examples/reset-rebuild.mjs) demonstrates the lifecycle calls. The
[recovery contract](../README.md#lifecycle-and-recovery) covers ownership and error handling.
Application availability, source checkpoint retention, and cutover belong to the consuming application.

## Reproduce the migration qualification

Build two separate checkouts: the previous implementation and this metadata implementation. Use each
checkout's own `dist/native.js` and native addon. Do not copy a new addon over the old wrapper.

```sh
FULLTEXT_PREFER_LOCAL_BUILD=1 node scripts/qualify-metadata-migration.mjs \
  --old-module /path/to/old-checkout/dist/native.js \
  --module /path/to/new-checkout/dist/native.js \
  --scratch-dir /path/to/existing/scratch-directory
```

The normal command creates and removes only its generated `fulltext-migration-*` directory; it takes
no existing index path. Each phase runs in a separate process to load the requested facade/addon pair.
Assertions check the following:

| Case                                             | Required result                                                                                                                                |
| ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Genuine previous build, string metadata          | New inspection, reader open, and writer open reject `E_IDENTITY_MISMATCH`; every old index file name and byte remains unchanged.               |
| Reset and authoritative rebuild, new generation  | Reopened checkpoint matches publication; text/filter queries, numeric and string sorts, string terms and numeric statistics match the records. |
| Genuine previous build, numeric/boolean metadata | New reader opens the original generation; checkpoint, filter, numeric sorting and statistics work without rebuilding.                          |

The local fixture contains three authoritative records. It qualifies the migration boundary and rebuild
sequence, not application cutover under concurrent replication or large-scale rebuild performance. The script checks the previous query API is older; callers must supply the actual
previous build and record its source revision separately.

## Executed result

On 2026-10-28, the previous
[base implementation](https://github.com/HarperFast/fulltext/commit/0b5608778b2d80ed2a8dfbea79565d509837e7ff)
(query API 4, native ABI 9) created both indexes. The
[metadata implementation](https://github.com/HarperFast/fulltext/commit/a471b58c90b2daf5247745449dc975f155694b21)
(query API 5, native ABI 10) passed every assertion above. Both builds embed Tantivy 0.27.0.

The numeric/boolean control reopened without rebuilding. The string index was rejected without
changing its contents, then reset and rebuilt as generation `v2`. Reopening recovered
`source-checkpoint-3`; active records sorted by ascending price were `b`, `a`, with a numeric sum of 30.
Filtering `brand = cedar` returned `a`, `c`, and the string terms bucket counted two documents.

## Open items

- Human approval of API semantics and the application rebuild requirement.
- Application-specific cutover, checkpoint catch-up, and rollback qualification.

## Sources

- [Upgrade contract](../README.md#upgrading-from-07x).
- [Executable migration qualification](../scripts/qualify-metadata-migration.mjs).
