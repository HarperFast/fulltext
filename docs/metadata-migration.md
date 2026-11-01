# Metadata index migration

Status: the metadata API shipped in 0.8.0. Kyle approved the API/migration direction on 2026-10-28.
Local procedure qualification passed, including a controlled local Harper Table catch-up/cutover
rehearsal. Production cutover remains untested.

Published 0.8.0 includes the metadata API and uses native ABI 13. The unreleased observability source
tree requires ABI 14 and matching source-built facade/addon artifacts; selecting its package version
remains release work. Do not install 0.7.0 expecting the metadata API. Remaining qualification is
tracked in [Qualify remaining native metadata collector resource tradeoffs](https://github.com/HarperFast/fulltext/issues/76).

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
For an installed package, pass its `dist/native.js`; when no local `fulltext.*.node` exists beside
the package manifest, the script lets the facade load its matching optional platform package.
The script selects this per module, independently of the caller's `FULLTEXT_PREFER_LOCAL_BUILD` setting.

```sh
node scripts/qualify-metadata-migration.mjs \
  --old-module /path/to/old-checkout/dist/native.js \
  --module /path/to/new-checkout/dist/native.js \
  --scratch-dir /path/to/existing/scratch-directory
```

The normal command creates and removes only its generated `fulltext-migration-*` directory; it takes
no existing index path. Each phase runs in a separate process to load the requested facade/addon pair.
Assertions check the following:

| Case                                                        | Required result                                                                                                                                |
| ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Genuine previous build, string metadata                     | New inspection, reader open, and writer open reject `E_IDENTITY_MISMATCH`; every old index file name and byte remains unchanged.               |
| Reset and authoritative rebuild, new generation             | Reopened checkpoint matches publication; text/filter queries, numeric and string sorts, string terms and numeric statistics match the records. |
| Genuine previous build, numeric/boolean metadata            | New reader opens the original generation; checkpoint, filter, numeric sorting and statistics work without rebuilding.                          |
| Old reader after new writes to numeric/boolean index        | New writer republishes the original generation; old reader reopens its checkpoint and verifies text and boolean filter results.                |
| Old build against rebuilt string index                      | Inspection and both opens reject a compatibility/corrupt-format code; every rebuilt index file name and byte remains unchanged.                |
| Downgrade using the new reset and old authoritative rebuild | Old reader reopens generation `v3`; checkpoint, text results, boolean filter and string equality filter match the records.                     |

The local fixture contains three authoritative records. It qualifies upgrade rejection and rebuilding in both upgrade and downgrade directions. Application cutover under concurrent replication
and large-scale rebuild performance require separate qualification. The script checks the previous query API is older; callers must supply the actual
previous build and record its source revision separately.

## Executed result

On 2026-10-28, the previous
[base implementation (0b56087)](https://github.com/HarperFast/fulltext/commit/0b5608778b2d80ed2a8dfbea79565d509837e7ff)
(query API 4, native ABI 9) created both indexes. The
[metadata implementation (a471b58)](https://github.com/HarperFast/fulltext/pull/75/commits)
(query API 5, native ABI 10) passed every assertion above. Both builds embed Tantivy 0.27.0.

The numeric/boolean control reopened without rebuilding. The new writer then upserted the same
authoritative records and published into that original generation; the old reader reopened its
new `source-checkpoint-4`, recovered all three text hits, and matched active records `a`, `b`. The string index was rejected without
changing its contents, then reset and rebuilt as generation `v2`. Reopening recovered
`source-checkpoint-3`; active records sorted by ascending price were `b`, `a`, with a numeric sum of 30.
Filtering `brand = cedar` returned `a`, `c`, and the string terms bucket counted two documents.

The compiled previous build rejected inspection, writer open, and reader open of the rebuilt string
index with `E_IDENTITY_MISMATCH`; all its index file names and contents remained unchanged. The
script records the observed old-build code and accepts `E_IDENTITY_MISMATCH` or `E_INDEX_CORRUPT`,
since earlier sidecar parsers can classify an unknown format as corrupt.

A follow-up execution used the new build to reset that rebuilt string index, then the previous build
to reconstruct generation `v3` from the same authoritative records. After closing and reopening with
the previous build, checkpoint `source-checkpoint-3`, all three text hits, active records `a`, `b`,
and `brand = cedar` records `a`, `c` passed. This exercises downgrade reconstruction; it does not
qualify live application rollback or checkpoint catch-up.

The qualification was repeated on 2026-10-28 against the
[expanded metadata implementation (5c502fb)](https://github.com/HarperFast/fulltext/pull/75/commits)
(query API 6, native ABI 11), using the same genuine previous base build. Numeric compatibility,
reverse numeric writes, nonmutating string rejection, rebuild/publication/reopen, and downgrade
reconstruction all passed again. Multi-field sorting and nested aggregations add no stored-schema
change beyond the string fast-column migration described here.

## Additional premerge qualification

The installed published 0.7.0 facade and optional macOS arm64 native package also passed
numeric compatibility, reverse writes, nonmutating string rejection, rebuild/publication/reopen
and downgrade reconstruction on 2026-10-28. The consumer lockfile recorded registry tarball
integrities; this does not qualify every platform package.

The [premerge qualification](metadata-premerge.md) additionally exercised actual built Harper
Table/Resource writes, a durable harness-owned source journal, controlled interleaving during
snapshot rebuild, checkpoint catch-up, publication/reopen and old-build rollback. This local
rehearsal does not establish distributed replication or production adapter behavior.

## Open items

- Production-specific concurrent cutover, checkpoint catch-up and rollback qualification.
- Cross-platform installed release-artifact qualification and publication verification.

## Sources

- [Upgrade contract](../README.md#upgrading-from-07x).
- [Executable migration qualification](../scripts/qualify-metadata-migration.mjs).
