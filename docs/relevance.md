# Catalog queries and relevance regressions

The [catalog example](../examples/catalog-search.mjs) demonstrates weighted fields, field selection,
Boolean queries, Unicode normalization, index-time synonyms and snippets through the public wrapper
API. The [query modes example](../examples/query-modes.mjs) shows all six search modes.

From a repository checkout:

```sh
npm run build
node scripts/run-local.mjs examples/catalog-search.mjs
node scripts/run-local.mjs --test test/native-relevance.test.mjs
```

The example reads [shared catalog records](../examples/data/catalog.json), which are also shipped
in the npm package. It indexes those records, searches them and traces a hit against its current
source values. It does not fetch records, enforce authorization or call Harper APIs.

## What the fixtures protect

The [regression fixture](../test/fixtures/relevance.json) defines expected ordered IDs rather than
numeric score snapshots. The [public-API test](../test/native-relevance.test.mjs) runs 21 catalog
queries under four configurations: title boost, equal field weights, stop words disabled and
index-time synonyms. Eight focused stemming queries bring the total to 92 cases. Each runs with
forward and reverse insertion order; selected cases also compare complete continuation pages
with ordinary results. Exact counts, versions, finite descending scores and UTF-8 tie ordering
are checked alongside the expected IDs. `the guide` distinguishes stop-word removal from
keeping stop words; title-only `television` distinguishes index-time synonym expansion from
matching a term already present in another field.

The corpus covers term/phrase/prefix/fuzzy modes, Boolean clauses, source-field selection, arrays,
accent and width folding, possessives, no matches and tied IDs. The stemming cases intentionally
keep `ad` separate from `add`/`added`/`adding`/`adds` under Tantivy 0.27. The fuzzy `added` case
also protects exact-match priority. These are observable language and ranking contracts, not a
claim that every user considers the resulting ranking optimal.

The records and query shapes came from the paired 0.6.0/0.7.0 qualification based on
[Harper's checked-in usage](https://github.com/HarperFast/harper/blob/8b381e5881124a4a51c35a3a092cd77d9b530ac9/resources/FULL_TEXT.md).
Harper's source-record filtering, permissions, Table.search, REST, version-race and lifecycle tests
remain in Harper. No Harper imports or historical native binaries are needed by these tests.

## Updating expectations

Keep expected IDs explicit and review why a result or its relative rank changes. Numeric scores can
change slightly without changing ranking, so do not replace these expectations with exact score
snapshots. An intentional analyzer change may affect matching and requires the corresponding
persisted compatibility decision and rebuild guidance.

The earlier paired comparison also covered additional diagnostic words and actual Harper
Table.search calls. This committed subset does not reproduce that entire comparison or provide a
production relevance-quality score. Customer corpora and judgments, broader domain vocabulary,
controlled segment/delete/merge cases, packaged-platform qualification and the reusable historical
upgrade-comparison driver remain tracked in
[Qualify native full-text filtering and catalog-scale operation for v5.4](https://github.com/HarperFast/harper/issues/2512).
