# @harperfast/fulltext

Native Tantivy full-text indexing for Node.js.

**Embedded search engine: Tantivy 0.27.0 · Wrapper version: 0.7.0.** The engine is compiled into
the native addon; no separate Tantivy installation is required. Upgrading from fulltext 0.6.x
requires an index rebuild; see [Upgrading from 0.6.x](#upgrading-from-06x) before deploying.

This repository is under active development. The native entry point provides a standalone Tantivy
index backed by `MmapDirectory`. Applications own their source records and may reuse compatible
local index files or rebuild them from that authoritative data.

## Start here

- [Capabilities](#capabilities): what the wrapper can search and return.
- [Install](#installation) and [create your first index](#quick-start).
- [Choose an API](#choose-an-api) and [understand search results](#reading-search-results).
- [Configure analysis](#analyzer-and-field-configuration), [filters](#structured-filters), and [highlighting](#highlighting-and-snippets).
- [Manage writes](#mutations-and-backpressure), [recover an index](#lifecycle-and-recovery), or [upgrade from 0.6.x](#upgrading-from-06x).
- Reference: [limits and recovery](docs/limits.md), [benchmarks](docs/benchmarks.md), and [backend internals](docs/native-backend-implementation.md).

## Capabilities

The wrapper stores a derived search index and returns ranked IDs. Your application owns source
records, authorization, and fetching the current records for those IDs. It does not need to run a
separate search server.

| Capability                  | Use it for                                                                 | Configuration or limits                                                          |
| --------------------------- | -------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `any` (default)             | Match at least one analyzed query term; rank by relevance.                 | Field weights boost selected fields.                                             |
| `all`                       | Require every analyzed query term to match.                                | Terms may match across selected fields.                                          |
| `phrase`                    | Match terms in order at their analyzed positions.                          | Requires `positions: true` (the default).                                        |
| `prefix`                    | Autocomplete: match completed terms and the final unfinished term.         | Requires `surfaceTerms: true`; expansion is bounded.                             |
| `fuzzy`                     | Match misspelled terms with up to one edit, including transpositions.      | Eligibility and term limits apply; this is not arbitrary spelling correction.    |
| `fuzzy-prefix` **preview**  | Combine final-term autocomplete with typo tolerance.                       | Requires `surfaceTerms: true`; catalog-scale qualification remains pending.      |
| Boolean queries and filters | Combine text clauses, restrict candidate IDs, or filter declared metadata. | Filters narrow results without adding relevance scores.                          |
| Ranked traversal            | Continue through a large result set in a retained snapshot.                | Use `searchPage()` with a read-only handle.                                      |
| Highlighting and snippets   | Locate matches in current source values supplied by your application.      | Requires `surfaceTerms: true`; returns offsets and optional fragments, not HTML. |

All modes use typed requests, not Tantivy query-string syntax. See [Search](#search) for examples
and [limits and their effects](docs/limits.md) for bounds and recovery choices.

## Requirements

- Node.js 22.18 or newer, or Node.js 24 or newer
- Rust 1.90 when building from source

The package never compiles or downloads native code during installation. It installs a matching
exact-version native package through npm optional dependencies.

## Installation

```bash
npm install @harperfast/fulltext
```

Do not omit optional dependencies: the matching native package is selected from them at runtime.
Applications import the single public entry point, `@harperfast/fulltext/native`.

The CI-qualified targets are Linux x64 and Linux arm64 with glibc 2.35 or newer, macOS arm64, and
Windows x64. Additional targets are added only after their packed artifacts are loaded and tested
on the target runtime. Unsupported targets fail with `E_NATIVE_ADDON_NOT_FOUND` when the native
entry point is first used; importing the JavaScript facade does not eagerly load an addon.

## Quick start

```js
import { openNativeFullTextIndex } from '@harperfast/fulltext/native';

const index = await openNativeFullTextIndex({
	path: './search/products',
	indexId: 'products',
	generation: 'v1',
	fields: [{ name: 'title', weight: 3 }, { name: 'description' }],
	filterFields: [
		{ name: 'category', type: 'string' },
		{ name: 'price', type: 'number' },
	],
	analyzer: 'english@2',
	surfaceTerms: true,
	limits: {
		indexingThreads: 2,
		searchThreads: 4,
		writerMemoryBytes: 60_000_000,
		maxQueuedCommands: 128,
		maxQueuedBytes: 16 * 1024 * 1024,
		maxBatchBytes: 8 * 1024 * 1024,
	},
});

try {
	await index.applyMutationBatch({
		upserts: [
			{
				id: 'shoe-1',
				version: '42',
				fields: { title: 'Trail running shoe', description: 'Waterproof' },
				filters: { category: 'outdoor', price: 89.99 },
			},
		],
	});
	await index.commit();
	await index.reload();
	console.log(await index.search({ text: 'waterproof running shoes', limit: 10 }));
} finally {
	await index.close({ mode: 'rollback' });
}
```

The quick start uses standalone commit and reload. Applications that track an external checkpoint
should use [`publish(payload)`](#checkpointed-publication) instead, so mutations and that checkpoint
commit together. `commit()` makes staged writes durable; `reload()` makes them visible to this
handle’s searches. Search hits contain IDs and optional versions, not the original records.

## Choose an API

| I want to…                                          | Use                                             | Result                                                                                                                                                |
| --------------------------------------------------- | ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| Get a bounded result page, with an optional offset  | `index.search(request)`                         | Ranked hits and a total marked exact or lower-bound. Ordinary offset + limit is bounded to 10,000; prefix modes allow offset zero and up to 100 hits. |
| Walk beyond that window, or continue prefix results | `reader.searchPage(request)`                    | Ranked hits, an exact total, and an optional continuation token. Each page is bounded to 10,000 hits.                                                 |
| Highlight matches in records I already fetched      | `index.traceMatches(request, records, options)` | UTF-16 spans, optional snippets, and a completeness flag. It does not retrieve or rank records.                                                       |

For a normal search UI, start with `search()`. For a long traversal, open a dedicated read-only
handle with `openNativeFullTextReader()` and use [continuation pagination](#traversing-beyond-10000-matches).
To display highlights, fetch the current source records and pass them to [match tracing](#highlighting-and-snippets).
A continuation belongs to its reader instance; keep that handle open until traversal finishes.

## Examples

Every example runs against the local build and the packed npm package in CI:

| Example                                                                | Demonstrates                                               |
| ---------------------------------------------------------------------- | ---------------------------------------------------------- |
| [`examples/basic.mjs`](examples/basic.mjs)                             | Open, mutate, commit, search, and close                    |
| [`examples/query-modes.mjs`](examples/query-modes.mjs)                 | BM25, phrase, prefix, fuzzy, and candidate filtering       |
| [`examples/structured-filters.mjs`](examples/structured-filters.mjs)   | Exact and numeric filters combined with BM25               |
| [`examples/checkpoint-recovery.mjs`](examples/checkpoint-recovery.mjs) | Atomic checkpoint publication, inspection, and reopen      |
| [`examples/highlighting.mjs`](examples/highlighting.mjs)               | Match tracing, UTF-16 spans, and opt-in snippets           |
| [`examples/multi-index.mjs`](examples/multi-index.mjs)                 | Optional process limits and concurrent independent indexes |
| [`examples/reset-rebuild.mjs`](examples/reset-rebuild.mjs)             | Safe retirement, cleanup, and rebuilding a generation      |

From a repository checkout, build the native addon and run any example through the local-build
loader:

```bash
npm run build
node scripts/run-local.mjs examples/basic.mjs
```

## Search

See [limits and their effects](docs/limits.md) for query, result, analyzer, mutation, and runtime
constraints, completeness signals, and recovery paths. Use the installed addon’s `(await runtimeInfo()).limits` rather than copying numeric ceilings into integrations.

Search uses weighted BM25 and a typed request; raw Tantivy query syntax is not exposed. `mode` is
`any`, `all`, `phrase`, `prefix`, `fuzzy`, or `fuzzy-prefix`. The older `operator: 'any' | 'all'`
spelling remains a compatibility alias and cannot be combined with `mode`. Candidate IDs compile to
a required filter that does not change scores. Prefix modes support autocomplete. For `prefix` and
`fuzzy-prefix`, `search()` requires offset zero and returns at most 100 hits; `searchPage()` can
continue through all accepted prefix matches. Fuzzy and prefix work has fixed term, clause,
expansion, request, result, and execution ceilings. Prefix expansion fails with `E_PREFIX_TOO_BROAD` instead of silently
truncating the term set and returning incomplete rankings. `fuzzy-prefix` is a preview capability
until catalog-scale benchmark qualification is complete.

Record IDs are limited to 4,096 UTF-8 bytes, keeping deterministic tie-page sorting memory bounded.
An upsert may include an opaque `version` string of up to 4,096 UTF-8 bytes. The version is returned
with its search hit, allowing a derived-index consumer to discard a hit when the authoritative
record has moved past the indexed version.

Use `query` for Boolean expressions within one index. Expression nesting is limited to eight edges
from the root. Expressions are bounded by the same request, term, and clause limits as simple searches:

```js
const result = await index.search({
	query: {
		operator: 'and',
		clauses: [
			{ text: 'waterproof trail', mode: 'all', fields: ['title'] },
			{ operator: 'not', clause: { text: 'used' } },
		],
	},
});
```

Negation filters do not add to BM25 scores. A query made only of negation matches assigns every
surviving hit a score of zero and orders ties by UTF-8 ID.

### Reading search results

A search returns this shape (score shown for illustration):

```js
{
  total: 1,
  totalRelation: 'exact',
  hits: [{ id: 'shoe-1', score: 0.86, version: '42' }]
}
```

- `hits` contains ranked IDs, scores, and the optional versions supplied during indexing. Fetch source records in your application; compare versions if you need to reject stale indexed hits.
- `score` expresses relevance within this query. Results sort by descending score, then ascending UTF-8 ID for ties. Do not treat scores as probabilities or compare them across engine versions.
- `totalRelation: 'exact'` means `total` is the full match count. `'lower-bound'` means at least that many matches exist; it does not mean the returned hits are approximate.
- Set `exactTotal: true` when a UI needs an exact count. It may require extra work across all matches. The default can still return an exact count when it is already known.

For example, `index.search({ text: 'waterproof', limit: 20, exactTotal: true })` requests the top
20 hits and an exact count. `searchPage()` always returns exact totals; it adds a `continuation`
only while another page remains. Match tracing has a separate `complete` flag: `false` means
some spans may have been omitted because a tracing limit was reached. Search results do not have
that flag. See [limits and completeness](docs/limits.md) and the [collector reference](docs/native-backend-implementation.md#schema-and-query-behavior).

### Traversing beyond 10,000 matches

`searchPage()` traverses ordinary, prefix, and fuzzy-prefix full-text results in score-descending,
UTF-8 ID-ascending order.
It returns an authenticated `continuation` while another page remains. Each page has an exact `total`
for the entire matching set; `limit` bounds an individual page to at most 10,000 hits. There is no
fixed ceiling on the number of matching results you can traverse and no direct page-number jump.
The existing `search({ offset, limit })` window and its legacy 100-hit autocomplete limit remain unchanged.
A defined `continuation` passed to `search()` rejects with `E_INVALID_ARGUMENT`; resume through
`searchPage()`.
Both methods reject a `null` continuation. Omit it or pass `undefined` to start without a continuation.
Shared request objects may include a zero or null offset and an `exactTotal` preference; nonzero
offsets are rejected, and `searchPage()` always returns exact totals.

Use a dedicated read-only handle to retain the original snapshot while the writer publishes or
merges newer data:

```js
import { openNativeFullTextReader } from '@harperfast/fulltext/native';

const reader = await openNativeFullTextReader(options);
try {
	let continuation;
	do {
		const page = await reader.searchPage({ text: 'widget', limit: 1_000, continuation });
		for (const hit of page.hits) await consume(hit);
		continuation = page.continuation;
	} while (continuation);
} finally {
	await reader.close();
}
```

Do not call `reader.reload()` during traversal. Reloading to a different segment/deletion snapshot
expires its continuations with `E_CURSOR_EXPIRED`; reloading the same snapshot keeps them valid.
Closing the reader releases its retained snapshot. Writer-handle continuations expire when `publish()`
changes the active indexed snapshot; use a dedicated reader when ingestion continues.
Continuations are bound to that handle and the original
query, fields, filters, and candidate IDs; they cannot be moved to a reopened handle, another worker,
or another process. Restart traversal after an addon upgrade. You may change page size or execution
budget between pages. Traversal sorts a copy of candidate IDs for request identity on each page, so reordering
the same candidate set preserves continuation even if order or duplicate counts change. The original
list still counts toward native count/byte limits and the packed-request bound before duplicates are
removed. The caller's array is unchanged. Preserve field
and query-clause order between pages. Tokens authenticate the last hit and the query identity. Their
contents are readable, including the last hit’s ID and score. Applications must still check permissions and close readers when the search
finishes or expires; the library does not set a reader expiration time.

A deterministic many-segment native qualification is available with `cargo test --release --lib --no-default-features traversal_many_segment_qualification -- --ignored --nocapture`; see the
[qualified snapshot costs](docs/limits.md#sha-256-snapshot-qualification).

Retained winners depend on page size rather than depth, but each page scans all matching documents
and computes an exact total. Traversing N matches in pages of P requires approximately N²/P
matching-document visits. This is a bounded-memory traversal API, not a constant-time seek.

#### Preparing large requests

The execution budget includes request preparation. Query identities up to 64 KiB hash on the
JavaScript thread; larger ones use asynchronous WebCrypto, which shares Node’s libuv worker pool.
Encoding and copying still run on the JavaScript thread. Other work in the pool can delay hashing,
and a dispatched hash runs to completion before timeout or close is checked.

Each handle bounds large-request preparation by its configured search queue limits, with ceilings
of 128 requests and 8 MiB of retained frames. WebCrypto copies and caller-owned input use additional
memory. Small synchronous requests bypass these preparation limits; native queue limits still apply
to every request. `E_QUEUE_FULL` identifies preparation overload in its message, but native `status()`
counters exclude preparation. A request larger than the configured byte capacity cannot succeed by
retrying unchanged, even on an idle handle. Reduce it or increase capacity within the process budget.

The open functions pass the configured limits to the handle. Callers constructing
`NativeFullTextIndex` directly can supply `searchLimits`; otherwise the preparation ceilings apply.

#### Execution and reader lifetime

Traversal checks its deadline between batches of matching documents. Query construction and individual
Tantivy operations can still exceed the budget; `close()` waits for in-flight native work. Use at
least two search threads to keep ordinary searches running alongside expensive ones; with one
thread, traversal shares the handle's only search lane. Each page is classified as expensive and holds a process-wide
expensive-search permit when a runtime budget is configured. Size `maxExpensiveSearches` for
concurrent traversals alongside phrase, fuzzy, and other expensive searches on every index.
A retained reader can keep old segment files on disk. Bound the number of open readers with the
runtime budget and close each one when its search finishes or expires. Prefix traversal
still enforces the independent 50-term expansion guard; continuation does not make an overly broad
prefix acceptable. Fuzzy-prefix retains its existing preview status.
Source projection, current-record validation, and Harper REST integration remain host responsibilities.

### Structured filters

Filter fields are typed metadata stored in the Tantivy index alongside the analyzed text. They let
Tantivy discard non-matching documents before returning ranked IDs, reducing source-record reads
without changing BM25 scores. The caller's records remain authoritative; filter metadata is a
derived copy used only to narrow search results.

The library does not infer this metadata. Declare each field in `filterFields` when opening the
index and supply its value in each upsert's `filters` object. A field omitted from `filterFields`
cannot be used in a native `filter` expression. The caller may still apply that condition after
search or use `candidateIds` when it already has a bounded set of matching IDs.

Declare only fields commonly combined with text search. Filter metadata increases index size and
mutation work, and changing its definition requires a new generation or rebuild. Filter fields are
separate from analyzed text fields and may reuse the same logical name. String filters are exact
and case-sensitive; number filters support exact and range comparisons; Boolean filters support
exact comparison. Arrays add one value per element. Missing fields add no value. String values are
limited to 65,530 UTF-8 bytes. Number fields are also stored as Tantivy fast fields to keep broad
range filters from materializing term bitsets; this further increases index size.

```js
const result = await index.search({
	text: 'waterproof shoes',
	filter: {
		operator: 'and',
		clauses: [
			{ field: 'category', comparator: 'in', value: ['outdoor', 'running'] },
			{ field: 'price', comparator: 'between', value: [50, 100] },
		],
	},
});
```

Supported comparators are `equals`, `in`, `lt`, `le`, `gt`, `ge`, and inclusive `between`.
Range comparators require a number field. Candidate IDs and structured filters may be combined;
both must match. Filter definitions are part of the persisted index identity, so changing them
requires a new generation or a rebuild from the authoritative source. Filter terms are present in
the local Tantivy term dictionary and should be protected like the indexed text itself.

## Analyzer and field configuration

Choose these settings when creating the index. Enable `surfaceTerms: true` if you need prefix
search, fuzzy-prefix search, or highlighting. The quick start enables it so the later examples
work with the same schema; omit it for indexes that need only ordinary text search.

`positions` defaults on and is required for phrase search. `surfaceTerms` defaults off and creates
an internal unstemmed companion term field used by prefix, fuzzy-prefix, and match tracing. It does
not store source values or positions; it retains term frequencies for scoring. Both settings are
persisted and must match when the index is reopened. Indexes created before this change with both
`surfaceTerms: true` and `positions: true` fail to open with `E_SCHEMA_MISMATCH` and must be rebuilt
from their authoritative source. Enable `surfaceTerms` only on indexes that need those operations.
Field weights are query-time boosts and may change on reopen without rebuilding the index.

`english@2` applies Unicode NFKC normalization, lowercase and Latin-to-ASCII folding, English
possessive removal, optional English stop words, and English stemming. Token offsets continue to
refer to the original source value. Normalization is streamed with source-span tracking, and token
text is bounded before Tantivy's long-token filter, avoiding memory growth proportional to Unicode
compatibility expansion. Inputs that exceed Unicode's 30-non-starter Stream-Safe limit receive a
standard combining-grapheme-joiner boundary before normalization. Index-time synonyms are optional
and bounded. Each source and replacement must produce exactly one normalized and analyzed term.
Rules are canonicalized,
persisted in the index identity, and expanded once at the source token's position; query text is not
synonym-expanded. The expansion is also written to the surface field, so prefix autocomplete can
match replacement terms. Tantivy counts the stacked alternatives in BM25 field length, which can
lower unrelated-term scores for documents containing a synonym source. Changing analyzer settings
or synonyms requires a new generation/rebuild. Match tracing applies the same index-time expansion
so synonym-derived hits map back to the original token span.

The public analyzer name and identity-sidecar version jointly identify persisted analyzer semantics.
Any future filter or ordering change must use a new analyzer name or sidecar version so existing
indexes fail closed and rebuild instead of silently changing recall.

## Upgrading from 0.6.x

Version 0.7.0 embeds Tantivy 0.27.0 and requires rebuilding existing indexes. The new English
stemmer changes some terms (`added`: `ad` → `add`), so opening an old index with the same
`english@2` configuration returns `E_IDENTITY_MISMATCH`. Inspection reports that incompatibility
before search or mutation. The public analyzer name and query API are unchanged.

Drain and close readers/writers, call `resetNativeFullTextIndex()` with the existing logical index ID,
and rebuild from authoritative source records. Use the checkpoint-publication lifecycle to publish
coverage only after rebuilding. Reset can retire old identity formats; do not edit sidecars to force
an index open. Before downgrading to 0.6.x, retire the 0.7 index with the 0.7 reset API, then rebuild
with 0.6.x; the older reset API reports `E_INDEX_CORRUPT` for the unrecognized identity. Relevance
scores may differ between engine versions; continuation tokens remain reader-instance scoped.

## Highlighting and snippets

Highlighting is opt-in and operates on caller-supplied current source values, so the library never
returns stale stored text. It returns UTF-16 half-open offsets and no HTML. Snippets are also off by
default:

```js
const traced = await index.traceMatches(
	{ text: 'trail running', mode: 'phrase' },
	[{ id: 'shoe-1', fields: { title: 'Waterproof trail running shoe' } }],
	{ snippets: true, fragmentLength: 160, maxFragmentsPerValue: 3 },
);
```

Tracing evaluates only the supplied current values; it does not expand the live index dictionary.
Analysis is limited to 262,144 emitted tokens per value after synonym expansion. When that token
ceiling, a span ceiling, or the response ceiling is reached, `complete` is false and later matches
may be omitted.

## Deadlines and query isolation

Search and tracing share a maximum 30-second queue-plus-execution budget. Applications can pass a
shorter remaining request budget through the second method argument; larger values are clamped to
30 seconds. The budget determines whether queued or completed work is accepted; it is not a
callback timer. If every eligible worker is already executing non-interruptible work, an expired
queued request is rejected when a worker next examines it. Ordinary offset search is not
interruptible, so an expired offset search is discarded after Tantivy returns. Traversal checks its
deadline during collection. Match tracing
checks its deadline while tokenizing and matching. With at least two search threads, one worker is
reserved for ordinary bounded `any`/`all` BM25. The remaining workers prioritize phrase, prefix,
fuzzy, unanchored-negation, exact-total, and trace work, then steal ordinary work when that queue is
idle. A negation intersected with a positive ordinary clause stays in the ordinary lane. A
one-thread configuration remains valid but cannot isolate query classes.

## Mutations and backpressure

Mutation batches are versioned packed values, so indexing crosses Node-API once per batch rather
than once per document. One dedicated actor owns Tantivy's single writer for each index. A bounded
search pool shares immutable searchers and can execute reads while indexing or commit work is in
progress. Queue limits reject overload with `E_QUEUE_FULL` rather than blocking the JavaScript
thread. `applyMutationBatch()` is the normal logical mutation API. It partitions one logical batch
into bounded native frames, applies them sequentially, validates native counts, and returns
`{ processed, rejected, encodedBytes, frames }`. IDs must be distinct across the logical batch.
By default, any rejected record fails the operation. Pass `{ rejectedUpsert: 'delete' }` when an
unindexable replacement must delete previously searchable content for the same ID. A failure after
native application is attempted leaves the handle incomplete: writer operations reject
`E_BATCH_INCOMPLETE` until the handle is closed with `{ mode: 'rollback' }`. A writer operation that
only races an in-flight logical batch rejects with `E_BATCH_ACTIVE`; wait for that batch to settle
and retry instead of rolling it back. This prevents a later publish from exposing part of a logical
batch. A closing or closed handle rejects every logical batch with `E_CLOSED`, including an empty
batch, without taking the latch.

### Process-wide resource limits

`configureNativeFullTextRuntime()` is optional and idempotent for an identical configuration. It
must be called before the first successful open when one process may host many indexes; configuring
while an open is pending fails with retryable `E_LOCK_BUSY`; configuration after an unbudgeted open
fails with `E_RESOURCE_LIMIT`. A failed first open that proves its native resources were released
does not prevent later configuration. An
unproven pre-publication teardown blocks configuration until restart. The governor bounds aggregate
resident indexes, indexing and search threads, writer memory, configured queue bytes, and concurrent
expensive searches. Each writer reserves twice its `maxQueuedBytes` for the writer and shared search
queues; each read-only handle reserves it once for search. Every writer or reader also consumes one
resident-index slot and its configured search-thread count. A conflicting second configuration or an
open that would exceed an admission cap fails with `E_RESOURCE_LIMIT`; the library never evicts a
live generation. Expensive searches wait off the JavaScript thread for process capacity, bounded
by the request deadline and interrupted by close, allowing the search queues to apply backpressure.
A flexible worker serves ordinary searches while the expensive-search budget is saturated. With a
one-thread index, ordinary and expensive work share one FIFO, so a process-wide permit held by a
different index can delay ordinary work queued behind an expensive request. Closing an index
interrupts expensive requests that are waiting for a permit with `E_CLOSED`.
A completed close has released its reservation.
If teardown cannot prove native resources were released, the path is quarantined and its aggregate
capacity remains reserved until process restart.
Callers that omit this function retain the per-index limits.

### Rejected records and schema errors

Schema mismatches fail the whole call even with `{ rejectedUpsert: 'delete' }`; treating schema drift
as record-local rejection could remove many documents under the wrong schema. The option applies
only to record-local `E_INVALID_ARGUMENT` and `E_BATCH_TOO_LARGE` rejections with usable IDs.
Delete mode always preflights every ID as a bounded delete frame before native admission;
`assumeDistinctIds` skips duplicate detection, not that feasibility scan. An unusable or oversized
ID therefore fails with nothing staged. Schema drift is detected during framing and can leave the
handle incomplete when earlier frames were already admitted.

Trusted callers that already enforce distinct IDs may pass `assumeDistinctIds: true` to skip the
whole-batch duplicate prepass. Supplying duplicates with that option violates the API contract.
The library snapshots the two mutation arrays, but callers must not mutate record objects, field or
filter maps, or nested field- or filter-value arrays until the returned promise settles.

`encodeMutationBatch(batch, maxBytes)` rejects output beyond its encoding bound with
`E_BATCH_TOO_LARGE`; `maxBytes` defaults to 8 MiB and is intended for low-level callers producing a
single native frame. Low-level callers may use `index.encodeMutationBatches()`. It uses the limit from
the index's open configuration, validates IDs and fields against that handle, and greedily emits
admissible frames. IDs must be distinct across the logical batch. Aggregate size creates more
frames; a single invalid or unencodable mutation is returned in `rejected` with its operation and
zero-based index in the corresponding input array. It is never dropped automatically.

Encoding is synchronous and runs on the JavaScript thread. The total encoded output of one logical
call defaults to 64 MiB and can be lowered with `encodeMutationBatches(batch, { maxTotalBytes })`.
The default behavior throws `E_BATCH_TOO_LARGE` when the complete logical batch exceeds that
ceiling. Callers that pass `allowPartial: true` instead receive a leading prefix;
`consumedUpserts` and `consumedDeletes` identify the mutations represented by the returned frames
and rejections so they can continue with each array's remaining suffix. The low-level API validates
distinct IDs across the full input before applying the output ceiling, so repeated suffix calls
repeat that validation scan. Prefer `applyMutationBatch()` for large logical batches.
Producers using the low-level API should keep logical batches comfortably below that limit.
Applying multiple frames stages them in one Tantivy writer. Commit or publish only after every frame
succeeds; on a later failure, close with rollback rather than publishing the partial logical batch.
Concurrent low-level callers should leave queue-byte headroom for commit or publish.
A successful `apply()` resolves to the number of accepted mutation commands, including deletes for
IDs that are not currently indexed.

One caller must own a handle's complete apply-and-publish sequence at a time. The high-level method
blocks low-level writer interleaving while its logical batch is active. The low-level frame API does
not infer logical batch boundaries, so callers using it remain responsible for excluding another
publisher between frames. Searches may still run concurrently with either writer sequence.

## Lifecycle and recovery

`close()` rejects uncommitted data by default. Use `close({ mode: 'rollback' })` to discard it
explicitly. `commit()` publishes mutations, and `reload()` makes the latest commit visible to this
handle's searches. A reload that cannot align Tantivy's snapshot with its checkpoint after three
bounded attempts returns `E_RELOAD_FAILED`; callers can retry because the reader remains open.
The failed attempt leaves the reader on its previous aligned snapshot and checkpoint.

### Read-only handles

One process may open one writer and multiple readers for the same physical index. Readers share the
same physical files, but each has an independently bounded search runtime and never reserves
Tantivy's writer lock:

```js
import { openNativeFullTextReader } from '@harperfast/fulltext/native';

const reader = await openNativeFullTextReader(options);
await reader.reload(); // call after the writer publishes a newer checkpoint
const result = await reader.search({ text: 'trail shoe' });
await reader.close();
```

Readers use Tantivy's manual reload policy. They do not watch or poll the filesystem; the caller
coordinates publication and calls `reload()` only when a newer revision is available. Reset rejects
while any writer or reader is live in the current process; separate processes must coordinate reset
with their own reader lifecycle. A reader never creates missing storage and fails with
`E_INDEX_NOT_READY` until a writer has created a complete index. A successful reload also refreshes
the reader's `committedPayload`.

### Checkpointed publication

Use `publish(payload)` when a consumer needs to resume from a durable checkpoint:

```js
console.log(index.committedPayload); // undefined on a new index; recovered from files on reopen
await index.applyMutationBatch({
	upserts: [{ id: 'shoe-1', fields: { title: 'Trail shoes' } }],
});
await index.publish('source-checkpoint-42');
// Both the mutations and the checkpoint are committed; searches now see that commit.
```

The payload is an opaque, well-formed Unicode string, limited to 64 KiB in UTF-8. Fulltext does not
interpret it or verify that it describes the mutations supplied by the caller. Empty strings are
valid checkpoints; `undefined` means the index has never committed one. Publication uses the same
bounded writer queue as mutations and returns a Tantivy opstamp, not an application cursor.

Once a generation has a checkpoint, plain `commit()` rejects with `E_CHECKPOINT_REQUIRED`, including
after reopen. Pending mutations remain available to a subsequent `publish()`, or can be discarded
with rollback close. A publication without mutations can advance the checkpoint. Ordinary indexes
that never publish retain the separate `commit()` and `reload()` API.

An accepted publication that fails can have an ambiguous durable outcome: the commit may have
succeeded before reader reload failed. The handle becomes terminal and `committedPayload` throws
`E_POISONED`; close it, reopen the same files and identity, and recover the checkpoint from that new
handle. Do not infer recovery progress from `status()` counters. Validation and queue admission
failures do not themselves poison the handle or invalidate a known checkpoint.

`inspectNativeFullTextIndex(options)` synchronously validates an existing index's identity, schema,
metadata, and committed payload without creating files, reserving a handle, starting actors, or
acquiring the Tantivy writer. It returns `missing`, `cursorless`, `checkpointed` with the committed
payload, or `incompatible` with a stable identity/schema or corrupt-format code. `checkpointed`
means the committed metadata is compatible; callers must still open the index successfully before
serving queries. Opening maps missing segment files and unreadable segment metadata or footers to
rebuildable error codes.
Operational storage failures throw. Inspection is intended for short lifecycle checks such as
derived-index election, not request hot paths. Its options intentionally omit writer, queue, and
search limits because inspection creates none of those resources.

`validateNativeFullTextIndexOptions(options)` runs the same native configuration decoder as open
without creating files or starting actors. This is intended for activation-time validation.

`close()` also waits for the handle’s native work to stop and its file mappings to be released.
Other open handles still own their readers and mappings; close those before resetting the path.
After closing an incompatible or corrupt local index, retire it atomically before rebuilding:

```js
const result = await resetNativeFullTextIndex({ path: './search/products', indexId: 'products' });
if (result.state === 'reset') {
	console.log(`retired native files at ${result.retiredPath}`);
}
```

The resolved close result is `{}` normally. If native resources were released but shutdown also
reported an operational cleanup error, it is `{ cleanupError }` and that error has code
`E_CLOSE_FAILED`; the path is still safe to reset. `E_QUIESCENCE_FAILED` rejects because the library
could not prove all native work stopped. Do not reset, remove, or rename that path until the process
restarts.

Reset returns `missing` without creating the path. It rejects a live owner with `E_LOCK_BUSY`, a
different persisted logical index with `E_IDENTITY_MISMATCH`, and unrelated nonempty directories
with `E_INVALID_ARGUMENT`. A malformed identity sidecar fails closed with `E_INDEX_CORRUPT`. On
success, reset renames the live directory into a unique path below the parent's `.fulltext-retired`
directory. The library does not delete it automatically. Call
`reclaimRetiredNativeFullTextIndexes({ path, retiredPath: result.retiredPath })` at a lifecycle point
chosen by the application. Passing the returned `retiredPath` verifies that the hint belongs to the
requested index. Use the same `path` value for reset and reclaim so generated names match. The
reclaimer removes only retired trees generated for that index path, ignores unrelated entries and
other indices, and returns `{ removed, failed }`. Applications choose when retired files are safe
to reclaim.

An established duplicate open returns `E_DUPLICATE_OPEN`. An open racing another open or reset can
return `E_LOCK_BUSY` while the shared lifecycle lock is held; callers may retry that acquisition.
An open that cannot capture one checkpoint-aligned Tantivy snapshot returns `E_RELOAD_FAILED`; retry
the open.
Lifecycle lock files are stored in the index parent's `.fulltext-locks` directory so reset can keep
the handoff lock while renaming the native directory on Windows. The library does not remove this
lock directory. The parent must permit creating this directory, and `.fulltext-locks` must remain
writable while indices are opened or reset; failures name the lock-directory path.

This API uses native ABI 9 and packed protocol 5. The loader rejects older addon binaries. Native
identity sidecar v6 identifies indexes without structured filter fields. Configuring filter fields
uses sidecar v7, which also fingerprints their names and types. Older v1–v5 indexes remain
identifiable for safe reset but cannot be reopened; rebuild them from the authoritative source.

## Diagnostics and errors

All public failures are `FulltextError` instances with a stable `code`. Branch on the code instead
of parsing the message:

```js
import { FulltextError, openNativeFullTextIndex, runtimeInfo } from '@harperfast/fulltext/native';

console.log(await runtimeInfo());

try {
	await openNativeFullTextIndex(options);
} catch (error) {
	if (error instanceof FulltextError && error.code === 'E_IDENTITY_MISMATCH') {
		// Retire and rebuild this derived index generation.
	} else {
		throw error;
	}
}
```

`runtimeInfo()` reports the package, Tantivy, ABI, query API, lifecycle API, mutation API, supported
storage backend, and hard protocol limits. It does not open an index or create files. To verify the
versions in the installed native addon:

```js
import { runtimeInfo } from '@harperfast/fulltext/native';

const { packageVersion, tantivyVersion } = await runtimeInfo();
console.log({ packageVersion, tantivyVersion });
// { packageVersion: '0.7.0', tantivyVersion: '0.27.0' }
```

The package version and engine version are independent. Use `runtimeInfo()` when diagnosing an
installed build; [`Cargo.toml`](Cargo.toml) records this source revision's exact engine pin.

## Storage boundary

The package has one public entry point: `@harperfast/fulltext/native`. It stores application-owned
derived indexes in Tantivy's native filesystem directory. The addon does not link RocksDB, depend
on rocksdb-js, or fall back to another storage backend.

## Development

```bash
npm ci --ignore-scripts
npm run build:debug
npm test
npm run lint
npm run format:check
```

CI reports JavaScript/TypeScript and Rust coverage separately. The Node report uses the built-in
test runner coverage and stores raw V8 coverage; the Rust report stores LCOV output. Coverage is
reported for visibility and release-to-release comparison, not enforced as a single blended gate.

For contributor performance work, see [Benchmarking the wrapper](docs/benchmarks.md): native and
multi-index workloads, hydration and trace cases, traversal and inspection probes, artifact
retention, and comparison limits. User-facing behavior is documented above; collector and native
implementation details live in the [backend reference](docs/native-backend-implementation.md).

Generated Node-API declarations in `ts/addon.d.ts` are private implementation types. Consumers use
only the types exported from a package entry point.

## Releases

The root `@harperfast/fulltext` package contains only the JavaScript and TypeScript facade. Native
artifacts are published as exact-version platform packages and selected at runtime. Release tags
must match both npm and Cargo versions. The release workflow builds and tests each supported native
artifact, installs the packed root and platform tarballs in a clean consumer with lifecycle scripts
disabled, and publishes with npm provenance. Platform packages are published before the root, so a
root version is never available until every supported artifact has passed its consumer test.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development workflow and
[the native backend design](docs/native-backend-implementation.md) for implementation details.

## License

Apache-2.0
