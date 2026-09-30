use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use tantivy::collector::sort_key::{NaturalComparator, ReverseComparator};
use tantivy::collector::{Collector, Count, SegmentCollector, TopDocs, TopNComputer};
use tantivy::columnar::StrColumn;
use tantivy::directory::error::OpenReadError;
use tantivy::directory::Directory;
use tantivy::query::{
	BooleanQuery, BoostQuery, ConstScoreQuery, DisjunctionMaxQuery, EmptyQuery, FuzzyTermQuery, Occur, PhraseQuery,
	Query, TermQuery, TermSetQuery,
};
use tantivy::schema::{Field, IndexRecordOption, Schema, TantivyDocument, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{
	AsciiFoldingFilter, Language, LowerCaser, RemoveLongFilter, Stemmer, StopWordFilter, TextAnalyzer, Token,
	TokenFilter, TokenStream, Tokenizer,
};
use tantivy::{
	DocAddress, DocId, Index, IndexReader, IndexSettings, IndexWriter, ReloadPolicy, Score, Searcher, SegmentReader,
	Term,
};
use unicode_normalization::char::{canonical_combining_class, compose, decompose_compatible};
use unicode_normalization::is_nfkc;

use crate::error::{FulltextError, Result};
use crate::protocol::{
	validate_record_id, EngineConfig, EngineIdentityConfig, MutationBatch, SearchClause, SearchExpression, SearchMode,
	SearchRequest, SynonymRule, TraceRecord, MAX_FUZZY_TERMS, MAX_PREFIX_EXPANSIONS, MAX_QUERY_CLAUSES,
	MAX_QUERY_TERMS, MAX_SEARCH_RESPONSE_BYTES, MAX_TRACE_SPANS,
};

const ID_FIELD_NAME: &str = "__fulltext_id";
const VERSION_FIELD_NAME: &str = "__fulltext_version";
pub(crate) const IDENTITY_PATH: &str = ".harper-fulltext-identity";
const META_PATH: &str = "meta.json";
const ANALYZER_NAME: &str = "english@2";
const SURFACE_ANALYZER_NAME: &str = "english_surface@2";
pub const MAX_COMMIT_PAYLOAD_BYTES: usize = 64 * 1024;
const MAX_TRACE_TOKENS_PER_VALUE: usize = 262_144;
const MAX_TOKEN_CHARACTERS: usize = 40;
const MAX_NONSTARTERS: usize = 30;
const COMBINING_GRAPHEME_JOINER: char = '\u{034f}';
type SynonymMap = Arc<HashMap<String, Vec<String>>>;

struct CanonicalIdentity {
	config: EngineIdentityConfig,
	analyzer: TextAnalyzer,
	analyzed_synonyms: SynonymMap,
	surface_synonyms: SynonymMap,
}

#[derive(Clone)]
pub struct Engine {
	index: Index,
	id_field: Field,
	version_field: Field,
	fields: Vec<EngineField>,
	field_lookup: HashMap<String, usize>,
	analyzer: TextAnalyzer,
	surface_analyzer: TextAnalyzer,
	index_analyzer: TextAnalyzer,
	surface_index_analyzer: TextAnalyzer,
	positions: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InspectionResult {
	Missing,
	Cursorless,
	Payload(String),
}

#[derive(Clone)]
struct EngineField {
	name: String,
	field: Field,
	surface_field: Option<Field>,
	weight: f32,
}

struct BuiltQuery {
	query: Box<dyn Query>,
	clauses: usize,
}

type StableScoreTieHit = ((Score, String), DocAddress);

struct StableScoreTieCollector {
	doc_range: Range<usize>,
}

impl StableScoreTieCollector {
	fn new(doc_range: Range<usize>) -> Self {
		Self { doc_range }
	}
}

struct StableScoreTieSegmentCollector {
	top_docs: TopNComputer<(Score, u64), DocId, (NaturalComparator, ReverseComparator)>,
	id_column: StrColumn,
	segment_ord: u32,
	missing_id: bool,
}

impl SegmentCollector for StableScoreTieSegmentCollector {
	type Fruit = Result<Vec<StableScoreTieHit>>;

	fn collect(&mut self, doc: DocId, score: Score) {
		let Some(id_ord) = self.id_column.ords().first(doc) else {
			self.missing_id = true;
			return;
		};
		// A segment's term ordinals preserve the dictionary's byte order.
		self.top_docs.push((score, id_ord), doc);
	}

	fn harvest(self) -> Self::Fruit {
		if self.missing_id {
			return Err(FulltextError::new("E_NATIVE_FAILURE", "search hit has no ID ordinal"));
		}
		let top_docs = self.top_docs.into_vec();
		let ordinals = top_docs
			.iter()
			.enumerate()
			.map(|(index, hit)| (hit.sort_key.1, index))
			.collect::<Vec<_>>();
		let ids = resolve_string_ordinals(
			&self.id_column,
			ordinals,
			top_docs.len(),
			"search hit ID ordinal is missing",
		)?;
		top_docs
			.into_iter()
			.zip(ids)
			.map(|(hit, id)| {
				let id =
					id.ok_or_else(|| FulltextError::new("E_NATIVE_FAILURE", "search hit ID ordinal is missing"))?;
				let id = String::from_utf8(id)
					.map_err(|_| FulltextError::new("E_NATIVE_FAILURE", "search hit ID is not UTF-8"))?;
				Ok(((hit.sort_key.0, id), DocAddress::new(self.segment_ord, hit.doc)))
			})
			.collect()
	}
}

impl Collector for StableScoreTieCollector {
	type Fruit = Result<(usize, Vec<StableScoreTieHit>)>;
	type Child = StableScoreTieSegmentCollector;

	fn for_segment(&self, segment_ord: u32, segment: &SegmentReader) -> tantivy::Result<Self::Child> {
		let id_column = segment
			.fast_fields()
			.str(ID_FIELD_NAME)?
			.ok_or_else(|| tantivy::TantivyError::InternalError("search segment has no ID fast field".to_owned()))?;
		Ok(StableScoreTieSegmentCollector {
			top_docs: TopNComputer::new_with_comparator(self.doc_range.end, (NaturalComparator, ReverseComparator)),
			id_column,
			segment_ord,
			missing_id: false,
		})
	}

	fn requires_scoring(&self) -> bool {
		true
	}

	fn merge_fruits(&self, segment_fruits: Vec<Result<Vec<StableScoreTieHit>>>) -> tantivy::Result<Self::Fruit> {
		let mut top_docs = TopNComputer::new_with_comparator(
			self.doc_range.end,
			((NaturalComparator, ReverseComparator), ReverseComparator),
		);
		for segment_hits in segment_fruits {
			let segment_hits = match segment_hits {
				Ok(segment_hits) => segment_hits,
				Err(error) => return Ok(Err(error)),
			};
			for (sort_key, address) in segment_hits {
				// DocAddress makes each merge key unique, so push order cannot break ties.
				top_docs.push((sort_key, address), address);
			}
		}
		let top_docs = top_docs.into_sorted_vec();
		let retained = top_docs.len();
		Ok(Ok((
			retained,
			top_docs
				.into_iter()
				.skip(self.doc_range.start)
				.map(|hit| (hit.sort_key.0, hit.doc))
				.collect(),
		)))
	}
}

pub struct Writer {
	inner: IndexWriter,
	checkpoint_required: bool,
	id_field: Field,
	version_field: Field,
	fields: Vec<EngineField>,
	field_lookup: HashMap<String, usize>,
}

pub(crate) struct PreparedBatch {
	mutation_count: u64,
	deletes: Vec<String>,
	documents: Vec<(String, TantivyDocument)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchHit {
	pub id: String,
	pub score: f32,
	pub version: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TotalRelation {
	Exact,
	LowerBound,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchResult {
	pub total: u64,
	pub total_relation: TotalRelation,
	pub hits: Vec<SearchHit>,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct TraceSpan {
	pub start: u32,
	pub end: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceValueMatch {
	pub field: String,
	pub value_index: u32,
	pub spans: Vec<TraceSpan>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceRecordMatch {
	pub id: String,
	pub values: Vec<TraceValueMatch>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceResult {
	pub complete: bool,
	pub records: Vec<TraceRecordMatch>,
}

#[derive(Clone)]
struct SourceToken {
	text: String,
	start: usize,
	end: usize,
	position: usize,
}

enum TracePlan {
	Any(Vec<String>),
	All(Vec<String>),
	Phrase(Vec<(usize, String)>),
	Prefix {
		completed: Vec<String>,
		prefix: String,
		fuzzy: bool,
	},
	Fuzzy(Vec<(String, String)>),
}

impl TracePlan {
	fn record_matches(&self, found: &HashSet<String>) -> bool {
		match self {
			Self::Any(terms) if terms.is_empty() => false,
			Self::Any(terms) => terms.iter().any(|term| found.contains(term)),
			Self::All(terms) => !terms.is_empty() && terms.iter().all(|term| found.contains(term)),
			Self::Phrase(terms) => !terms.is_empty() && found.contains("__phrase"),
			Self::Prefix { completed, prefix, .. } => {
				!prefix.is_empty() && found.contains("__prefix") && completed.iter().all(|term| found.contains(term))
			}
			Self::Fuzzy(terms) => !terms.is_empty() && terms.iter().any(|(term, _)| found.contains(term)),
		}
	}
}

impl Engine {
	pub(crate) fn validate(config: &EngineConfig) -> Result<()> {
		canonical_identity(&config.identity).map(|_| ())
	}

	pub fn inspect<D: Directory + Clone>(directory: D, config: &EngineIdentityConfig) -> Result<InspectionResult> {
		let identity = canonical_identity(config)?.config;
		let (expected_schema, _, _, _) = build_schema(&identity)?;
		let expected_identity = identity_bytes(&identity);
		let sidecar_exists = directory.exists(Path::new(IDENTITY_PATH)).map_err(storage_error)?;
		let meta_exists = directory.exists(Path::new(META_PATH)).map_err(storage_error)?;

		if !sidecar_exists && !meta_exists {
			return Ok(InspectionResult::Missing);
		}
		if !sidecar_exists {
			return Err(FulltextError::new(
				"E_INCOMPLETE_CREATE",
				"meta.json exists without a fulltext identity sidecar",
			));
		}
		let actual_identity = directory.atomic_read(Path::new(IDENTITY_PATH)).map_err(storage_error)?;
		if actual_identity != expected_identity {
			return Err(FulltextError::new(
				"E_IDENTITY_MISMATCH",
				"the persisted index identity does not match the requested configuration",
			));
		}
		if !meta_exists {
			return Ok(InspectionResult::Cursorless);
		}
		let index = Index::open(directory).map_err(recovery_index_error)?;
		if index.schema() != expected_schema {
			return Err(FulltextError::new(
				"E_SCHEMA_MISMATCH",
				"the persisted Tantivy schema does not match the requested configuration",
			));
		}
		Ok(match validated_committed_payload(&index)? {
			Some(payload) => InspectionResult::Payload(payload),
			None => InspectionResult::Cursorless,
		})
	}

	pub fn open<D: Directory + Clone>(directory: D, config: &EngineConfig) -> Result<Self> {
		let canonical = canonical_identity(&config.identity)?;
		let identity = canonical.config;
		let analyzer = canonical.analyzer;
		let (schema, id_field, version_field, fields) = build_schema(&identity)?;
		let expected_identity = identity_bytes(&identity);
		let sidecar_exists = directory.exists(Path::new(IDENTITY_PATH)).map_err(storage_error)?;
		let meta_exists = directory.exists(Path::new(META_PATH)).map_err(storage_error)?;

		if sidecar_exists {
			let actual = directory.atomic_read(Path::new(IDENTITY_PATH)).map_err(storage_error)?;
			if actual != expected_identity {
				return Err(FulltextError::new(
					"E_IDENTITY_MISMATCH",
					"the persisted index identity does not match the requested configuration",
				));
			}
		} else if meta_exists {
			return Err(FulltextError::new(
				"E_INCOMPLETE_CREATE",
				"meta.json exists without a fulltext identity sidecar",
			));
		} else {
			directory
				.atomic_write(Path::new(IDENTITY_PATH), &expected_identity)
				.map_err(storage_error)?;
			directory.sync_directory().map_err(storage_error)?;
		}

		let index = if meta_exists {
			Index::open(directory).map_err(recovery_index_error)?
		} else {
			Index::create(directory, schema.clone(), IndexSettings::default()).map_err(index_error)?
		};
		if index.schema() != schema {
			return Err(FulltextError::new(
				"E_SCHEMA_MISMATCH",
				"the persisted Tantivy schema does not match the requested configuration",
			));
		}
		let surface_analyzer = build_surface_analyzer(None);
		let index_analyzer = build_analyzer(identity.stop_words, Some(canonical.analyzed_synonyms))?;
		let surface_index_analyzer = build_surface_analyzer(Some(canonical.surface_synonyms));
		index.tokenizers().register(ANALYZER_NAME, index_analyzer.clone());
		index
			.tokenizers()
			.register(SURFACE_ANALYZER_NAME, surface_index_analyzer.clone());
		let field_lookup = fields
			.iter()
			.enumerate()
			.map(|(index, field)| (field.name.clone(), index))
			.collect();
		Ok(Self {
			index,
			id_field,
			version_field,
			fields,
			field_lookup,
			analyzer,
			surface_analyzer,
			index_analyzer,
			surface_index_analyzer,
			positions: identity.positions,
		})
	}

	pub fn writer(&self, config: &EngineConfig) -> Result<Writer> {
		self.writer_with_payload_using(config, index_error)
			.map(|(writer, _)| writer)
	}

	pub(crate) fn writer_with_payload(&self, config: &EngineConfig) -> Result<(Writer, Option<String>)> {
		self.writer_with_payload_using(config, recovery_index_error)
	}

	fn writer_with_payload_using(
		&self,
		config: &EngineConfig,
		map_error: fn(tantivy::TantivyError) -> FulltextError,
	) -> Result<(Writer, Option<String>)> {
		let inner = self
			.index
			.writer_with_num_threads(config.limits.indexing_threads, config.limits.writer_memory_bytes)
			.map_err(map_error)?;
		// A competing process can publish until we acquire the writer lock.
		let payload = self.committed_payload()?;
		Ok((
			Writer {
				inner,
				checkpoint_required: payload.is_some(),
				id_field: self.id_field,
				version_field: self.version_field,
				fields: self.fields.clone(),
				field_lookup: self.field_lookup.clone(),
			},
			payload,
		))
	}

	pub fn reader(&self) -> Result<IndexReader> {
		self.reader_using(index_error)
	}

	pub(crate) fn reader_for_open(&self) -> Result<IndexReader> {
		self.reader_using(recovery_index_error)
	}

	fn reader_using(&self, map_error: fn(tantivy::TantivyError) -> FulltextError) -> Result<IndexReader> {
		self.index
			.reader_builder()
			.reload_policy(ReloadPolicy::Manual)
			.try_into()
			.map_err(map_error)
	}

	pub fn committed_payload(&self) -> Result<Option<String>> {
		validated_committed_payload(&self.index)
	}

	pub fn committed_payload_for_searcher(&self, searcher: &Searcher) -> Result<Option<String>> {
		let metas = self.index.load_metas().map_err(recovery_index_error)?;
		let segments = metas
			.segments
			.iter()
			.map(|segment| (segment.id(), segment.delete_opstamp()))
			.collect::<std::collections::BTreeMap<_, _>>();
		if searcher.generation().segments() != &segments {
			return Err(FulltextError::new(
				"E_RELOAD_FAILED",
				"native index changed during reload; retry the reload",
			));
		}
		validated_payload(metas.payload)
	}

	pub fn search(&self, searcher: &Searcher, request: &SearchRequest) -> Result<SearchResult> {
		self.search_inner(searcher, request, None)
	}

	pub fn search_with_deadline(
		&self,
		searcher: &Searcher,
		request: &SearchRequest,
		deadline: Instant,
	) -> Result<SearchResult> {
		self.search_inner(searcher, request, Some(deadline))
	}

	fn search_inner(
		&self,
		searcher: &Searcher,
		request: &SearchRequest,
		deadline: Option<Instant>,
	) -> Result<SearchResult> {
		if request.limit == 0 {
			return Err(FulltextError::invalid("search limit must be greater than zero"));
		}
		if let Some(candidate_ids) = &request.candidate_ids {
			for id in candidate_ids {
				validate_record_id(id)?;
			}
		}
		check_deadline(deadline)?;
		let query = self.query(searcher, request)?;
		check_deadline(deadline)?;
		let window_end = request.offset + request.limit;
		let stable_any = request.candidate_ids.is_none()
			&& query.clauses > 1
			&& matches!(&request.expression, SearchExpression::Clause(clause) if clause.mode == SearchMode::Any);
		let (hits, bounded_total, bounded_total_relation) = if stable_any {
			let (retained, mut scored_docs) = searcher
				.search(
					query.query.as_ref(),
					&StableScoreTieCollector::new(request.offset..window_end.saturating_add(1)),
				)
				.map_err(index_error)??;
			check_deadline(deadline)?;
			scored_docs.truncate(request.limit);
			let addresses = scored_docs.iter().map(|(_, address)| *address).collect::<Vec<_>>();
			let versions = search_hit_versions(searcher, &addresses)?;
			let hits = scored_docs
				.into_iter()
				.zip(versions)
				.map(|(((score, id), _), version)| SearchHit { id, score, version })
				.collect::<Vec<_>>();
			let (total, relation) = if retained < window_end.saturating_add(1) {
				(retained as u64, TotalRelation::Exact)
			} else {
				(window_end as u64, TotalRelation::LowerBound)
			};
			(hits, total, relation)
		} else {
			let scored_docs = searcher
				.search(
					query.query.as_ref(),
					&TopDocs::with_limit(window_end.saturating_add(1)).order_by_score(),
				)
				.map_err(index_error)?;
			check_deadline(deadline)?;
			let boundary_tie =
				scored_docs.len() > window_end && scored_docs[window_end - 1].0 == scored_docs[window_end].0;
			let hits = if boundary_tie {
				let (_, scored_docs) = searcher
					.search(
						query.query.as_ref(),
						&StableScoreTieCollector::new(request.offset..window_end),
					)
					.map_err(index_error)??;
				let addresses = scored_docs.iter().map(|(_, address)| *address).collect::<Vec<_>>();
				let versions = search_hit_versions(searcher, &addresses)?;
				scored_docs
					.into_iter()
					.zip(versions)
					.map(|(((score, id), _), version)| SearchHit { id, score, version })
					.collect::<Vec<_>>()
			} else {
				let metadata = search_hit_metadata(searcher, &scored_docs)?;
				let mut ranked = scored_docs
					.iter()
					.zip(metadata)
					.map(|((score, _), metadata)| SearchHit {
						id: metadata.id,
						score: *score,
						version: metadata.version,
					})
					.collect::<Vec<_>>();
				ranked.sort_by(|left, right| {
					right
						.score
						.total_cmp(&left.score)
						.then_with(|| left.id.as_bytes().cmp(right.id.as_bytes()))
				});
				ranked.into_iter().skip(request.offset).take(request.limit).collect()
			};
			let (total, relation) = if scored_docs.len() <= window_end {
				(scored_docs.len() as u64, TotalRelation::Exact)
			} else {
				(window_end as u64, TotalRelation::LowerBound)
			};
			(hits, total, relation)
		};
		let (total, total_relation) = if request.exact_total {
			check_deadline(deadline)?;
			(
				searcher.search(query.query.as_ref(), &Count).map_err(index_error)? as u64,
				TotalRelation::Exact,
			)
		} else {
			(bounded_total, bounded_total_relation)
		};
		check_deadline(deadline)?;
		let response_bytes = hits
			.iter()
			.try_fold(13usize, |bytes, hit| {
				bytes.checked_add(9 + hit.id.len() + hit.version.as_ref().map_or(0, |version| 4 + version.len()))
			})
			.ok_or_else(|| FulltextError::new("E_RESULT_TOO_LARGE", "search response size overflow"))?;
		if response_bytes > MAX_SEARCH_RESPONSE_BYTES {
			return Err(FulltextError::new(
				"E_RESULT_TOO_LARGE",
				format!("search response exceeds {MAX_SEARCH_RESPONSE_BYTES} bytes"),
			));
		}
		Ok(SearchResult {
			total,
			total_relation,
			hits,
		})
	}

	pub fn trace_matches(
		&self,
		request: &SearchRequest,
		records: &[TraceRecord],
		deadline: Option<Instant>,
	) -> Result<TraceResult> {
		check_deadline(deadline)?;
		if let Some(candidate_ids) = &request.candidate_ids {
			for id in candidate_ids {
				validate_record_id(id)?;
			}
		}
		for record in records {
			validate_record_id(&record.id)?;
		}
		let clause = match &request.expression {
			SearchExpression::Clause(clause) => clause,
			_ => {
				return Err(FulltextError::invalid(
					"traceMatches does not support boolean expressions",
				))
			}
		};
		let selected = self.selected_fields(&clause.fields)?;
		self.require_surface_fields(&selected)?;
		let selected_names = selected.iter().map(|field| field.name.as_str()).collect::<HashSet<_>>();
		let candidates = request
			.candidate_ids
			.as_ref()
			.map(|ids| ids.iter().map(String::as_str).collect::<HashSet<_>>());
		if candidates.as_ref().is_some_and(HashSet::is_empty) {
			return Ok(TraceResult {
				complete: true,
				records: Vec::new(),
			});
		}
		let plan = self.trace_plan(clause)?;
		let mut complete = true;
		let mut remaining_spans = MAX_TRACE_SPANS;
		let mut response_bytes = 3usize;
		let mut matched_records = Vec::new();
		let mut analyzed_source_analyzer = self.index_analyzer.clone();
		let mut surface_source_analyzer = self.surface_index_analyzer.clone();
		for record in records {
			check_deadline(deadline)?;
			if candidates
				.as_ref()
				.is_some_and(|candidates| !candidates.contains(record.id.as_str()))
			{
				continue;
			}
			let mut seen_fields = HashSet::new();
			let mut pending_values = Vec::new();
			let mut found = HashSet::new();
			let mut record_span_budget = remaining_spans;
			let mut record_truncated = false;
			let mut record_analysis_truncated = false;
			for (field, source_values) in &record.fields {
				if !seen_fields.insert(field.as_str()) {
					return Err(FulltextError::invalid(format!("duplicate trace field {field}")));
				}
				if !self.field_lookup.contains_key(field) {
					return Err(FulltextError::invalid(format!("unknown trace field {field}")));
				}
				if !selected_names.contains(field.as_str()) {
					continue;
				}
				for (value_index, value) in source_values.iter().enumerate() {
					check_deadline(deadline)?;
					let (spans, terms, spans_truncated, analysis_truncated) = self.trace_value(
						&plan,
						value,
						record_span_budget,
						deadline,
						&mut analyzed_source_analyzer,
						&mut surface_source_analyzer,
					)?;
					record_truncated |= spans_truncated || analysis_truncated;
					record_analysis_truncated |= analysis_truncated;
					found.extend(terms);
					if spans.is_empty() {
						continue;
					}
					record_span_budget = record_span_budget.saturating_sub(spans.len());
					pending_values.push((field.as_str(), value_index as u32, spans));
				}
			}
			complete &= !record_analysis_truncated;
			if plan.record_matches(&found) {
				complete &= !record_truncated;
				if pending_values.is_empty() {
					continue;
				}
				let record_header_bytes = 6usize.saturating_add(record.id.len());
				let mut record_bytes = 0usize;
				let mut values = Vec::new();
				for (field, value_index, spans) in pending_values {
					let value_header_bytes = 10usize.saturating_add(field.len());
					let available = MAX_SEARCH_RESPONSE_BYTES
						.saturating_sub(response_bytes)
						.saturating_sub(record_header_bytes)
						.saturating_sub(record_bytes);
					let byte_limited_spans = available.saturating_sub(value_header_bytes) / 8;
					let keep = spans.len().min(remaining_spans).min(byte_limited_spans);
					if keep < spans.len() {
						complete = false;
					}
					if keep == 0 {
						continue;
					}
					values.push(TraceValueMatch {
						field: field.to_owned(),
						value_index,
						spans: spans.into_iter().take(keep).collect(),
					});
					remaining_spans -= keep;
					record_bytes = record_bytes.saturating_add(value_header_bytes + keep * 8);
				}
				if values.is_empty() {
					continue;
				}
				response_bytes = response_bytes.saturating_add(record_header_bytes + record_bytes);
				matched_records.push(TraceRecordMatch {
					id: record.id.clone(),
					values,
				});
			}
		}
		Ok(TraceResult {
			complete,
			records: matched_records,
		})
	}

	fn trace_plan(&self, request: &SearchClause) -> Result<TracePlan> {
		match request.mode {
			SearchMode::Any => Ok(TracePlan::Any(self.analyze(&request.text, false, true)?)),
			SearchMode::All => Ok(TracePlan::All(self.analyze(&request.text, false, true)?)),
			SearchMode::Phrase => {
				if !self.positions {
					return Err(FulltextError::invalid("phrase search requires positions to be enabled"));
				}
				Ok(TracePlan::Phrase(self.analyze_positioned(&request.text)?))
			}
			SearchMode::Prefix | SearchMode::FuzzyPrefix => {
				if request.text.chars().last().is_some_and(char::is_whitespace) {
					return Ok(TracePlan::All(self.analyze(&request.text, false, true)?));
				}
				let (completed, prefix) = self.final_surface_term(&request.text)?;
				let prefix = prefix.unwrap_or_default();
				let minimum = if request.mode == SearchMode::FuzzyPrefix { 4 } else { 3 };
				if !prefix.is_empty() && prefix.chars().count() < minimum {
					return Err(FulltextError::invalid(format!(
						"{} prefix must contain at least {minimum} Unicode characters",
						if request.mode == SearchMode::FuzzyPrefix {
							"fuzzy"
						} else {
							"exact"
						}
					)));
				}
				Ok(TracePlan::Prefix {
					completed,
					prefix,
					fuzzy: request.mode == SearchMode::FuzzyPrefix,
				})
			}
			SearchMode::Fuzzy => {
				let mut terms = Vec::new();
				for surface in self.analyze(&request.text, true, true)? {
					if let [analyzed] = self.analyze(&surface, false, false)?.as_slice() {
						terms.push((analyzed.clone(), surface));
					}
				}
				if terms.iter().filter(|(_, surface)| fuzzy_eligible(surface)).count() > MAX_FUZZY_TERMS {
					return Err(FulltextError::invalid(format!(
						"fuzzy search contains more than {MAX_FUZZY_TERMS} eligible terms"
					)));
				}
				Ok(TracePlan::Fuzzy(terms))
			}
		}
	}

	fn trace_value(
		&self,
		plan: &TracePlan,
		value: &str,
		max_spans: usize,
		deadline: Option<Instant>,
		analyzed_source_analyzer: &mut TextAnalyzer,
		surface_source_analyzer: &mut TextAnalyzer,
	) -> Result<(Vec<TraceSpan>, HashSet<String>, bool, bool)> {
		let (analyzed, mut analysis_truncated) = Self::source_tokens(analyzed_source_analyzer, value, deadline)?;
		let utf16_offsets = utf16_offsets(value);
		let mut spans = Vec::new();
		let mut seen_spans = HashSet::new();
		let mut found = HashSet::new();
		let mut spans_truncated = false;
		match plan {
			TracePlan::Any(terms) | TracePlan::All(terms) => {
				for (index, token) in analyzed.iter().enumerate() {
					if index % 256 == 0 {
						check_deadline(deadline)?;
					}
					if terms.contains(&token.text) {
						found.insert(token.text.clone());
						spans_truncated |= push_trace_span(
							&mut spans,
							&mut seen_spans,
							source_span(&utf16_offsets, token.start, token.end),
							max_spans,
						);
						if spans_truncated && plan.record_matches(&found) {
							break;
						}
					}
				}
			}
			TracePlan::Phrase(terms) => {
				if !terms.is_empty() {
					for (anchor_index, anchor) in analyzed.iter().enumerate() {
						check_deadline(deadline)?;
						if anchor.text != terms[0].1 {
							continue;
						}
						let first_source_position = anchor.position;
						let first_query_position = terms[0].0;
						let mut source_index = anchor_index + 1;
						let mut final_token = anchor;
						let mut matches = true;
						for (position, term) in terms.iter().skip(1) {
							let expected_position = first_source_position + (position - first_query_position);
							while source_index < analyzed.len() && analyzed[source_index].position < expected_position {
								source_index += 1;
							}
							let position_start = source_index;
							while source_index < analyzed.len() && analyzed[source_index].position == expected_position
							{
								source_index += 1;
							}
							let Some(token) = analyzed[position_start..source_index]
								.iter()
								.find(|token| token.text == *term)
							else {
								matches = false;
								break;
							};
							final_token = token;
						}
						if matches {
							found.insert("__phrase".to_owned());
							spans_truncated |= push_trace_span(
								&mut spans,
								&mut seen_spans,
								source_span(&utf16_offsets, anchor.start, final_token.end),
								max_spans,
							);
							if spans_truncated && plan.record_matches(&found) {
								break;
							}
						}
					}
				}
			}
			TracePlan::Prefix {
				completed,
				prefix,
				fuzzy,
			} => {
				let (surface, surface_truncated) = Self::source_tokens(surface_source_analyzer, value, deadline)?;
				analysis_truncated |= surface_truncated;
				for (index, token) in analyzed.iter().enumerate() {
					if index % 256 == 0 {
						check_deadline(deadline)?;
					}
					if completed.contains(&token.text) {
						found.insert(token.text.clone());
						spans_truncated |= push_trace_span(
							&mut spans,
							&mut seen_spans,
							source_span(&utf16_offsets, token.start, token.end),
							max_spans,
						);
						if spans_truncated && plan.record_matches(&found) {
							break;
						}
					}
				}
				for (index, token) in surface.iter().enumerate() {
					if index % 256 == 0 {
						check_deadline(deadline)?;
					}
					if token.text.starts_with(prefix)
						|| (*fuzzy && fuzzy_eligible(prefix) && fuzzy_prefix_matches(prefix, &token.text))
					{
						found.insert("__prefix".to_owned());
						spans_truncated |= push_trace_span(
							&mut spans,
							&mut seen_spans,
							source_span(&utf16_offsets, token.start, token.end),
							max_spans,
						);
						if spans_truncated && plan.record_matches(&found) {
							break;
						}
					}
				}
			}
			TracePlan::Fuzzy(terms) => {
				let (surface, surface_truncated) = Self::source_tokens(surface_source_analyzer, value, deadline)?;
				analysis_truncated |= surface_truncated;
				let mut analyzed_by_position = HashMap::<usize, Vec<&str>>::new();
				for token in &analyzed {
					analyzed_by_position
						.entry(token.position)
						.or_default()
						.push(token.text.as_str());
				}
				for (index, token) in surface.iter().enumerate() {
					if index % 256 == 0 {
						check_deadline(deadline)?;
					}
					for (analyzed_term, surface_term) in terms {
						let analyzed_matches = analyzed_by_position.get(&token.position).is_some_and(|tokens| {
							tokens.iter().any(|token| {
								*token == analyzed_term
									|| (fuzzy_eligible(surface_term) && within_one_edit(analyzed_term, token))
							})
						});
						if analyzed_matches
							|| (fuzzy_eligible(surface_term) && within_one_edit(surface_term, &token.text))
						{
							found.insert(analyzed_term.clone());
							spans_truncated |= push_trace_span(
								&mut spans,
								&mut seen_spans,
								source_span(&utf16_offsets, token.start, token.end),
								max_spans,
							);
							break;
						}
					}
					if spans_truncated && plan.record_matches(&found) {
						break;
					}
				}
			}
		}
		spans.sort_by_key(|span| (span.start, span.end));
		spans.dedup();
		Ok((spans, found, spans_truncated, analysis_truncated))
	}

	fn source_tokens(
		analyzer: &mut TextAnalyzer,
		text: &str,
		deadline: Option<Instant>,
	) -> Result<(Vec<SourceToken>, bool)> {
		let mut stream = analyzer.token_stream(text);
		let mut tokens = Vec::new();
		while stream.advance() {
			if tokens.len() % 256 == 0 {
				check_deadline(deadline)?;
			}
			if tokens.len() == MAX_TRACE_TOKENS_PER_VALUE {
				return Ok((tokens, true));
			}
			let token = stream.token();
			tokens.push(SourceToken {
				text: token.text.clone(),
				start: token.offset_from,
				end: token.offset_to,
				position: token.position,
			});
		}
		Ok((tokens, false))
	}

	fn selected_fields(&self, requested: &[String]) -> Result<Vec<&EngineField>> {
		if requested.is_empty() {
			return Ok(self.fields.iter().collect());
		}
		let mut seen = HashSet::with_capacity(requested.len());
		let mut fields = Vec::with_capacity(requested.len());
		for name in requested {
			if !seen.insert(name) {
				return Err(FulltextError::invalid(format!("duplicate search field {name}")));
			}
			let index = self
				.field_lookup
				.get(name)
				.ok_or_else(|| FulltextError::invalid(format!("unknown search field {name}")))?;
			fields.push(&self.fields[*index]);
		}
		Ok(fields)
	}

	fn query(&self, searcher: &Searcher, request: &SearchRequest) -> Result<BuiltQuery> {
		let query = self.expression_query(searcher, &request.expression)?;
		Ok(BuiltQuery {
			query: self.with_candidates(query.query, request.candidate_ids.as_deref())?,
			clauses: query.clauses,
		})
	}

	fn expression_query(&self, searcher: &Searcher, expression: &SearchExpression) -> Result<BuiltQuery> {
		match expression {
			SearchExpression::Clause(clause) => self.clause_query(searcher, clause),
			SearchExpression::And(children) => self.boolean_query(searcher, children, Occur::Must),
			SearchExpression::Or(children) => self.boolean_query(searcher, children, Occur::Should),
			SearchExpression::Not(child) => {
				let child = self.expression_query(searcher, child)?;
				self.check_total_clause_count(child.clauses, 2)?;
				Ok(BuiltQuery {
					query: Box::new(ConstScoreQuery::new(
						Box::new(BooleanQuery::new(vec![
							(Occur::Must, Box::new(tantivy::query::AllQuery)),
							(Occur::MustNot, child.query),
						])),
						0.0,
					)),
					clauses: child.clauses + 2,
				})
			}
		}
	}

	fn boolean_query(&self, searcher: &Searcher, children: &[SearchExpression], occur: Occur) -> Result<BuiltQuery> {
		let mut clauses = children.len();
		let mut queries = Vec::with_capacity(children.len());
		for child in children {
			let child = self.expression_query(searcher, child)?;
			self.check_total_clause_count(clauses, child.clauses)?;
			clauses += child.clauses;
			queries.push((occur, child.query));
		}
		Ok(BuiltQuery {
			query: Box::new(BooleanQuery::new(queries)),
			clauses,
		})
	}

	fn clause_query(&self, searcher: &Searcher, clause: &SearchClause) -> Result<BuiltQuery> {
		let fields = self.selected_fields(&clause.fields)?;
		let (query, clauses) = match clause.mode {
			SearchMode::Any => self.term_query(&clause.text, &fields, Occur::Should)?,
			SearchMode::All => self.term_query(&clause.text, &fields, Occur::Must)?,
			SearchMode::Phrase => self.phrase_query(&clause.text, &fields)?,
			SearchMode::Prefix => self.prefix_query(searcher, &clause.text, &fields, false)?,
			SearchMode::Fuzzy => self.fuzzy_query(&clause.text, &fields)?,
			SearchMode::FuzzyPrefix => self.prefix_query(searcher, &clause.text, &fields, true)?,
		};
		Ok(BuiltQuery { query, clauses })
	}

	fn analyze(&self, text: &str, surface: bool, deduplicate: bool) -> Result<Vec<String>> {
		let mut analyzer = if surface {
			self.surface_analyzer.clone()
		} else {
			self.analyzer.clone()
		};
		let mut stream = analyzer.token_stream(text);
		let mut terms = Vec::new();
		let mut seen = HashSet::new();
		stream.process(&mut |token| {
			if !deduplicate || seen.insert(token.text.clone()) {
				terms.push(token.text.clone());
			}
		});
		if terms.len() > MAX_QUERY_TERMS {
			return Err(FulltextError::invalid(format!(
				"search text produces more than {MAX_QUERY_TERMS} terms"
			)));
		}
		Ok(terms)
	}

	fn analyze_positioned(&self, text: &str) -> Result<Vec<(usize, String)>> {
		let mut analyzer = self.analyzer.clone();
		let mut stream = analyzer.token_stream(text);
		let mut terms = Vec::new();
		while stream.advance() {
			let token = stream.token();
			terms.push((token.position, token.text.clone()));
			if terms.len() > MAX_QUERY_TERMS {
				return Err(FulltextError::invalid(format!(
					"search text produces more than {MAX_QUERY_TERMS} terms"
				)));
			}
		}
		Ok(terms)
	}

	fn term_query(&self, text: &str, fields: &[&EngineField], occur: Occur) -> Result<(Box<dyn Query>, usize)> {
		let terms = self.analyze(text, false, true)?;
		if terms.is_empty() {
			return Ok((Box::new(EmptyQuery), 0));
		}
		self.check_clause_count(terms.len(), fields.len())?;
		let clauses = terms.len().saturating_mul(fields.len());
		if occur == Occur::Should {
			let mut alternatives = Vec::with_capacity(clauses);
			for term in terms {
				for field in fields {
					let query: Box<dyn Query> = Box::new(TermQuery::new(
						Term::from_field_text(field.field, &term),
						IndexRecordOption::WithFreqs,
					));
					alternatives.push((Occur::Should, boosted(query, field.weight)));
				}
			}
			return Ok((Box::new(BooleanQuery::new(alternatives)), clauses));
		}
		Ok((
			Box::new(BooleanQuery::new(
				terms
					.into_iter()
					.map(|term| (occur, self.term_group(&term, fields)))
					.collect(),
			)),
			clauses,
		))
	}

	fn phrase_query(&self, text: &str, fields: &[&EngineField]) -> Result<(Box<dyn Query>, usize)> {
		if !self.positions {
			return Err(FulltextError::invalid("phrase search requires positions to be enabled"));
		}
		let terms = self.analyze_positioned(text)?;
		if terms.is_empty() {
			return Ok((Box::new(EmptyQuery), 0));
		}
		self.check_clause_count(1, fields.len())?;
		if terms.len() == 1 {
			return Ok((self.term_group(&terms[0].1, fields), fields.len()));
		}
		let alternatives = fields
			.iter()
			.map(|field| {
				let first_position = terms[0].0;
				let query: Box<dyn Query> = Box::new(PhraseQuery::new_with_offset(
					terms
						.iter()
						.map(|(position, term)| (position - first_position, Term::from_field_text(field.field, term)))
						.collect(),
				));
				(Occur::Should, boosted(query, field.weight))
			})
			.collect();
		Ok((Box::new(BooleanQuery::new(alternatives)), fields.len()))
	}

	fn fuzzy_query(&self, text: &str, fields: &[&EngineField]) -> Result<(Box<dyn Query>, usize)> {
		let surface = self.analyze(text, true, true)?;
		let mut pairs = Vec::new();
		for surface_term in surface {
			let analyzed = self.analyze(&surface_term, false, false)?;
			if let [analyzed_term] = analyzed.as_slice() {
				pairs.push((analyzed_term.clone(), surface_term));
			}
		}
		if pairs.is_empty() {
			return Ok((Box::new(EmptyQuery), 0));
		}
		let fuzzy_terms = pairs.iter().filter(|(_, term)| fuzzy_eligible(term)).count();
		if fuzzy_terms > MAX_FUZZY_TERMS {
			return Err(FulltextError::invalid(format!(
				"fuzzy search contains more than {MAX_FUZZY_TERMS} eligible terms"
			)));
		}
		self.check_clause_count(pairs.len(), fields.len().saturating_mul(3))?;
		let exact_bonus = fields.iter().map(|field| field.weight).fold(0.0f32, f32::max) * 0.25 + 1.0;
		let clause_count = pairs.len().saturating_mul(fields.len().saturating_mul(3));
		let clauses = pairs
			.into_iter()
			.map(|(analyzed, surface)| {
				let exact = self.term_group(&analyzed, fields);
				if !fuzzy_eligible(&surface) {
					return Ok((Occur::Should, exact));
				}
				let exact_branch: Box<dyn Query> = Box::new(BooleanQuery::new(vec![
					(Occur::Must, exact.box_clone()),
					(Occur::Must, Box::new(ConstScoreQuery::new(exact, exact_bonus))),
				]));
				let fuzzy = self.fuzzy_group(&analyzed, fields, false)?;
				Ok((
					Occur::Should,
					Box::new(DisjunctionMaxQuery::new(vec![exact_branch, fuzzy])) as Box<dyn Query>,
				))
			})
			.collect::<Result<Vec<_>>>()?;
		Ok((Box::new(BooleanQuery::new(clauses)), clause_count))
	}

	fn prefix_query(
		&self,
		searcher: &Searcher,
		text: &str,
		fields: &[&EngineField],
		fuzzy: bool,
	) -> Result<(Box<dyn Query>, usize)> {
		self.require_surface_fields(fields)?;
		if text.chars().last().is_some_and(char::is_whitespace) {
			return self.term_query(text, fields, Occur::Must);
		}
		let (completed, surface_prefix) = self.final_surface_term(text)?;
		let Some(surface_prefix) = surface_prefix else {
			return Ok((Box::new(EmptyQuery), 0));
		};
		let minimum = if fuzzy { 4 } else { 3 };
		if surface_prefix.chars().count() < minimum {
			return Err(FulltextError::invalid(format!(
				"{} prefix must contain at least {minimum} Unicode characters",
				if fuzzy { "fuzzy" } else { "exact" }
			)));
		}
		let completed_clause_count = completed.len().saturating_mul(fields.len());
		let mut clauses = Vec::with_capacity(completed.len() + 1);
		for term in completed {
			clauses.push((Occur::Must, self.term_group(&term, fields)));
		}
		let (exact_prefix, prefix_clause_count) = self.expanded_prefix_group(searcher, &surface_prefix, fields)?;
		let fuzzy_clause_count = usize::from(fuzzy && fuzzy_eligible(&surface_prefix)) * fields.len();
		if completed_clause_count
			.saturating_add(prefix_clause_count)
			.saturating_add(fuzzy_clause_count)
			> MAX_QUERY_CLAUSES
		{
			return Err(FulltextError::new(
				"E_PREFIX_TOO_BROAD",
				format!("prefix query exceeds {MAX_QUERY_CLAUSES} clauses"),
			));
		}
		let final_group = if fuzzy && fuzzy_eligible(&surface_prefix) {
			let exact_bonus = fields.iter().map(|field| field.weight).fold(0.0f32, f32::max) * 0.25 + 1.0;
			let exact_for_bonus = exact_prefix.box_clone();
			let exact_branch: Box<dyn Query> = Box::new(BooleanQuery::new(vec![
				(Occur::Must, exact_prefix),
				(
					Occur::Must,
					Box::new(ConstScoreQuery::new(exact_for_bonus, exact_bonus)),
				),
			]));
			Box::new(DisjunctionMaxQuery::new(vec![
				exact_branch,
				self.fuzzy_group(&surface_prefix, fields, true)?,
			])) as Box<dyn Query>
		} else {
			exact_prefix
		};
		clauses.push((Occur::Must, final_group));
		Ok((
			Box::new(BooleanQuery::new(clauses)),
			completed_clause_count + prefix_clause_count + fuzzy_clause_count,
		))
	}

	fn final_surface_term(&self, text: &str) -> Result<(Vec<String>, Option<String>)> {
		let mut analyzer = TextAnalyzer::builder(NfkcTokenizer::default())
			.filter_dynamic(EnglishPossessiveFilter)
			.filter_dynamic(LowerCaser)
			.filter_dynamic(AsciiFoldingFilter)
			.build();
		let mut stream = analyzer.token_stream(text);
		let mut final_term = None;
		let mut final_position = None;
		while stream.advance() {
			let token = stream.token();
			final_term = Some(token.text.clone());
			final_position = Some(token.position);
		}
		if final_term.as_ref().is_some_and(|term| term.len() >= 40) {
			return Err(FulltextError::invalid(
				"the final prefix token must be shorter than 40 UTF-8 bytes",
			));
		}
		let mut seen = HashSet::new();
		let completed = match final_position {
			Some(final_position) => self
				.analyze_positioned(text)?
				.into_iter()
				.filter_map(|(position, term)| (position < final_position && seen.insert(term.clone())).then_some(term))
				.collect(),
			None => Vec::new(),
		};
		Ok((completed, final_term))
	}

	fn term_group(&self, term: &str, fields: &[&EngineField]) -> Box<dyn Query> {
		Box::new(BooleanQuery::new(
			fields
				.iter()
				.map(|field| {
					let query: Box<dyn Query> = Box::new(TermQuery::new(
						Term::from_field_text(field.field, term),
						IndexRecordOption::WithFreqs,
					));
					(Occur::Should, boosted(query, field.weight))
				})
				.collect(),
		))
	}

	fn fuzzy_group(&self, term: &str, fields: &[&EngineField], prefix: bool) -> Result<Box<dyn Query>> {
		let alternatives = fields
			.iter()
			.map(|field| {
				let query_field = if prefix {
					field
						.surface_field
						.ok_or_else(|| FulltextError::invalid("prefix search requires surfaceTerms to be enabled"))?
				} else {
					field.field
				};
				let term = Term::from_field_text(query_field, term);
				let query: Box<dyn Query> = if prefix {
					Box::new(FuzzyTermQuery::new_prefix(term, 1, true))
				} else {
					Box::new(FuzzyTermQuery::new(term, 1, true))
				};
				Ok(Box::new(BoostQuery::new(
					Box::new(ConstScoreQuery::new(query, 0.25)),
					field.weight,
				)) as Box<dyn Query>)
			})
			.collect::<Result<Vec<_>>>()?;
		Ok(Box::new(DisjunctionMaxQuery::new(alternatives)))
	}

	fn expanded_prefix_group(
		&self,
		searcher: &Searcher,
		prefix: &str,
		fields: &[&EngineField],
	) -> Result<(Box<dyn Query>, usize)> {
		let mut alternatives = Vec::new();
		for field in fields {
			let surface_field = field
				.surface_field
				.ok_or_else(|| FulltextError::invalid("prefix search requires surfaceTerms to be enabled"))?;
			let mut terms = BTreeSet::new();
			for segment in searcher.segment_readers() {
				let inverted = segment.inverted_index(surface_field).map_err(index_error)?;
				let dictionary = inverted.terms();
				let mut range = dictionary.range().ge(prefix.as_bytes());
				let end = prefix_end(prefix.as_bytes());
				if let Some(end) = end.as_deref() {
					range = range.lt(end);
				}
				let mut stream = range.into_stream().map_err(storage_error)?;
				while stream.advance() {
					terms.insert(stream.key().to_vec());
					if terms.len() > MAX_PREFIX_EXPANSIONS {
						return Err(FulltextError::new(
							"E_PREFIX_TOO_BROAD",
							format!("prefix expands to more than {MAX_PREFIX_EXPANSIONS} terms"),
						));
					}
				}
			}
			for term in terms {
				let query: Box<dyn Query> = Box::new(TermQuery::new(
					Term::from_field_bytes(surface_field, &term),
					IndexRecordOption::WithFreqs,
				));
				alternatives.push(boosted(query, field.weight));
				if alternatives.len() > MAX_QUERY_CLAUSES {
					return Err(FulltextError::new(
						"E_PREFIX_TOO_BROAD",
						format!("prefix query exceeds {MAX_QUERY_CLAUSES} clauses"),
					));
				}
			}
		}
		let clause_count = alternatives.len();
		if alternatives.is_empty() {
			Ok((Box::new(EmptyQuery), 0))
		} else {
			Ok((Box::new(DisjunctionMaxQuery::new(alternatives)), clause_count))
		}
	}

	fn with_candidates(&self, query: Box<dyn Query>, candidate_ids: Option<&[String]>) -> Result<Box<dyn Query>> {
		let Some(candidate_ids) = candidate_ids else {
			return Ok(query);
		};
		if candidate_ids.is_empty() {
			return Ok(Box::new(EmptyQuery));
		}
		let terms = candidate_ids
			.iter()
			.collect::<HashSet<_>>()
			.into_iter()
			.map(|id| Term::from_field_text(self.id_field, id));
		let filter: Box<dyn Query> = Box::new(ConstScoreQuery::new(Box::new(TermSetQuery::new(terms)), 0.0));
		Ok(Box::new(BooleanQuery::new(vec![
			(Occur::Must, query),
			(Occur::Must, filter),
		])))
	}

	fn require_surface_fields(&self, fields: &[&EngineField]) -> Result<()> {
		if fields.iter().any(|field| field.surface_field.is_none()) {
			return Err(FulltextError::invalid(
				"prefix search and match tracing require surfaceTerms to be enabled",
			));
		}
		Ok(())
	}

	fn check_clause_count(&self, groups: usize, alternatives: usize) -> Result<()> {
		if groups.saturating_mul(alternatives) > MAX_QUERY_CLAUSES {
			return Err(FulltextError::invalid(format!(
				"search query exceeds {MAX_QUERY_CLAUSES} clauses"
			)));
		}
		Ok(())
	}

	fn check_total_clause_count(&self, clauses: usize, additional: usize) -> Result<()> {
		if clauses.saturating_add(additional) > MAX_QUERY_CLAUSES {
			return Err(FulltextError::invalid(format!(
				"search query exceeds {MAX_QUERY_CLAUSES} clauses"
			)));
		}
		Ok(())
	}
}

fn check_deadline(deadline: Option<Instant>) -> Result<()> {
	if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
		Err(FulltextError::new(
			"E_TIMEOUT",
			"full-text search exceeded its execution budget",
		))
	} else {
		Ok(())
	}
}

fn search_hit_versions(searcher: &Searcher, addresses: &[DocAddress]) -> Result<Vec<Option<String>>> {
	let mut hits_by_segment = HashMap::new();
	for (index, address) in addresses.iter().enumerate() {
		hits_by_segment
			.entry(address.segment_ord)
			.or_insert_with(Vec::new)
			.push((index, address.doc_id));
	}
	let mut versions = vec![None; addresses.len()];
	for (segment_ord, segment_hits) in hits_by_segment {
		let segment = &searcher.segment_readers()[segment_ord as usize];
		let Some(column) = segment.fast_fields().str(VERSION_FIELD_NAME).map_err(index_error)? else {
			continue;
		};
		let ordinals = segment_hits
			.iter()
			.enumerate()
			.filter_map(|(result_index, (_, doc_id))| {
				column.term_ords(*doc_id).next().map(|ordinal| (ordinal, result_index))
			})
			.collect();
		let values = resolve_string_ordinals(
			&column,
			ordinals,
			segment_hits.len(),
			"search hit version ordinal is missing",
		)?;
		for ((index, _), value) in segment_hits.into_iter().zip(values) {
			let Some(value) = value else {
				continue;
			};
			versions[index] = Some(
				String::from_utf8(value)
					.map_err(|_| FulltextError::new("E_NATIVE_FAILURE", "search hit version is not UTF-8"))?,
			);
		}
	}
	Ok(versions)
}

struct SearchHitMetadata {
	id: String,
	version: Option<String>,
}

fn search_hit_metadata(searcher: &Searcher, scored_docs: &[(f32, DocAddress)]) -> Result<Vec<SearchHitMetadata>> {
	let mut metadata = (0..scored_docs.len())
		.map(|_| SearchHitMetadata {
			id: String::new(),
			version: None,
		})
		.collect::<Vec<_>>();
	let mut hits_by_segment = HashMap::new();
	for (index, (_, address)) in scored_docs.iter().enumerate() {
		hits_by_segment
			.entry(address.segment_ord)
			.or_insert_with(Vec::new)
			.push((index, address.doc_id));
	}
	for (segment_ord, segment_hits) in hits_by_segment {
		let segment = &searcher.segment_readers()[segment_ord as usize];
		let id_column = segment
			.fast_fields()
			.str(ID_FIELD_NAME)
			.map_err(index_error)?
			.ok_or_else(|| FulltextError::new("E_NATIVE_FAILURE", "search segment has no ID fast field"))?;
		let version_column = segment.fast_fields().str(VERSION_FIELD_NAME).map_err(index_error)?;
		let id_ordinals = segment_hits
			.iter()
			.enumerate()
			.map(|(result_index, (_, doc_id))| {
				let ordinal = id_column
					.term_ords(*doc_id)
					.next()
					.ok_or_else(|| FulltextError::new("E_NATIVE_FAILURE", "search hit has no ID ordinal"))?;
				Ok((ordinal, result_index))
			})
			.collect::<Result<Vec<_>>>()?;
		let ids = resolve_string_ordinals(
			&id_column,
			id_ordinals,
			segment_hits.len(),
			"search hit ID ordinal is missing",
		)?;
		for ((index, _), id) in segment_hits.iter().zip(ids) {
			let id = id.ok_or_else(|| FulltextError::new("E_NATIVE_FAILURE", "search hit ID ordinal is missing"))?;
			metadata[*index].id = String::from_utf8(id)
				.map_err(|_| FulltextError::new("E_NATIVE_FAILURE", "search hit ID is not UTF-8"))?;
		}
		if let Some(version_column) = version_column {
			let ordinals = segment_hits
				.iter()
				.enumerate()
				.filter_map(|(result_index, (_, doc_id))| {
					version_column
						.term_ords(*doc_id)
						.next()
						.map(|ordinal| (ordinal, result_index))
				})
				.collect();
			let versions = resolve_string_ordinals(
				&version_column,
				ordinals,
				segment_hits.len(),
				"search hit version ordinal is missing",
			)?;
			for ((index, _), version) in segment_hits.into_iter().zip(versions) {
				let Some(version) = version else {
					continue;
				};
				metadata[index].version = Some(
					String::from_utf8(version)
						.map_err(|_| FulltextError::new("E_NATIVE_FAILURE", "search hit version is not UTF-8"))?,
				);
			}
		}
	}
	Ok(metadata)
}

fn resolve_string_ordinals(
	column: &StrColumn,
	mut ordinals: Vec<(u64, usize)>,
	value_count: usize,
	missing_message: &'static str,
) -> Result<Vec<Option<Vec<u8>>>> {
	ordinals.sort_unstable();
	let mut values = vec![None; value_count];
	let mut next_ordinal = 0;
	let found_all = column
		.dictionary()
		.sorted_ords_to_term_cb(ordinals.iter().map(|(ordinal, _)| *ordinal), |value| {
			let Some((_, result_index)) = ordinals.get(next_ordinal) else {
				return Err(std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"string dictionary returned too many terms",
				));
			};
			next_ordinal += 1;
			values[*result_index] = Some(value.to_vec());
			Ok(())
		})
		.map_err(storage_error)?;
	if !found_all || next_ordinal != ordinals.len() {
		return Err(FulltextError::new("E_NATIVE_FAILURE", missing_message));
	}
	Ok(values)
}

fn boosted(query: Box<dyn Query>, weight: f32) -> Box<dyn Query> {
	if weight == 1.0 {
		query
	} else {
		Box::new(BoostQuery::new(query, weight))
	}
}

fn fuzzy_eligible(term: &str) -> bool {
	term.chars().count() >= 4
		&& term.chars().any(char::is_alphabetic)
		&& !term
			.chars()
			.any(|character| character.is_numeric() || matches!(character, '-' | '_' | '/'))
}

fn utf16_offsets(source: &str) -> Option<Vec<u32>> {
	if source.is_ascii() {
		return None;
	}
	let mut offsets = vec![0; source.len() + 1];
	let mut utf16_offset = 0u32;
	for (byte_offset, character) in source.char_indices() {
		offsets[byte_offset] = utf16_offset;
		utf16_offset += character.len_utf16() as u32;
		offsets[byte_offset + character.len_utf8()] = utf16_offset;
	}
	Some(offsets)
}

fn source_span(utf16_offsets: &Option<Vec<u32>>, start: usize, end: usize) -> Option<TraceSpan> {
	if start > end {
		return None;
	}
	let (start, end) = match utf16_offsets {
		Some(offsets) => (*offsets.get(start)?, *offsets.get(end)?),
		None => (u32::try_from(start).ok()?, u32::try_from(end).ok()?),
	};
	(start <= end).then_some(TraceSpan { start, end })
}

fn push_trace_span(
	spans: &mut Vec<TraceSpan>,
	seen: &mut HashSet<TraceSpan>,
	span: Option<TraceSpan>,
	max_spans: usize,
) -> bool {
	let Some(span) = span else {
		return false;
	};
	if seen.contains(&span) {
		return false;
	}
	if spans.len() < max_spans {
		seen.insert(span);
		spans.push(span);
		false
	} else {
		true
	}
}

fn within_one_edit(left: &str, right: &str) -> bool {
	let left_length = left.chars().count();
	let right_length = right.chars().count();
	if left_length.abs_diff(right_length) > 1 {
		return false;
	}
	if left == right {
		return true;
	}
	if left_length == right_length {
		let mut differences = [(0usize, '\0', '\0'); 2];
		let mut count = 0;
		for (index, (left, right)) in left.chars().zip(right.chars()).enumerate() {
			if left == right {
				continue;
			}
			if count == differences.len() {
				return false;
			}
			differences[count] = (index, left, right);
			count += 1;
		}
		return count == 1
			|| (count == 2
				&& differences[1].0 == differences[0].0 + 1
				&& differences[0].1 == differences[1].2
				&& differences[1].1 == differences[0].2);
	}
	let (shorter, longer) = if left_length < right_length {
		(left, right)
	} else {
		(right, left)
	};
	let mut shorter = shorter.chars().peekable();
	let mut longer = longer.chars().peekable();
	let mut edits = 0;
	while let (Some(short), Some(long)) = (shorter.peek(), longer.peek()) {
		if short == long {
			shorter.next();
		} else {
			edits += 1;
			if edits > 1 {
				return false;
			}
		}
		longer.next();
	}
	true
}

fn fuzzy_prefix_matches(prefix: &str, candidate: &str) -> bool {
	let prefix_length = prefix.chars().count();
	(prefix_length.saturating_sub(1)..=prefix_length.saturating_add(1)).any(|length| {
		let end = candidate
			.char_indices()
			.nth(length)
			.map_or(candidate.len(), |(index, _)| index);
		let candidate_prefix = &candidate[..end];
		within_one_edit(prefix, candidate_prefix)
	})
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
	let mut end = prefix.to_vec();
	while let Some(last) = end.last_mut() {
		if *last != u8::MAX {
			*last += 1;
			return Some(end);
		}
		end.pop();
	}
	None
}

impl Writer {
	pub fn apply(&self, batch: MutationBatch) -> Result<u64> {
		let prepared = self.prepare(batch)?;
		self.apply_prepared(prepared)
	}

	pub(crate) fn prepare(&self, batch: MutationBatch) -> Result<PreparedBatch> {
		let mutation_count = batch.upserts.len() + batch.deletes.len();
		for id in &batch.deletes {
			validate_record_id(id)?;
		}
		let mut documents = Vec::with_capacity(batch.upserts.len());
		for upsert in batch.upserts {
			validate_record_id(&upsert.id)?;
			let mut seen = HashSet::with_capacity(upsert.fields.len());
			let mut document = TantivyDocument::default();
			document.add_text(self.id_field, &upsert.id);
			if let Some(version) = &upsert.version {
				document.add_text(self.version_field, version);
			}
			for (name, values) in upsert.fields {
				if !seen.insert(name.clone()) {
					return Err(FulltextError::invalid(format!("duplicate mutation field {name}")));
				}
				let index = self
					.field_lookup
					.get(&name)
					.ok_or_else(|| FulltextError::invalid(format!("unknown mutation field {name}")))?;
				for value in values {
					let field = &self.fields[*index];
					document.add_text(field.field, &value);
					if let Some(surface_field) = field.surface_field {
						document.add_text(surface_field, &value);
					}
				}
			}
			documents.push((upsert.id, document));
		}
		Ok(PreparedBatch {
			mutation_count: mutation_count as u64,
			deletes: batch.deletes,
			documents,
		})
	}

	pub(crate) fn apply_prepared(&self, prepared: PreparedBatch) -> Result<u64> {
		for id in prepared.deletes {
			self.inner.delete_term(Term::from_field_text(self.id_field, &id));
		}
		for (id, document) in prepared.documents {
			self.inner.delete_term(Term::from_field_text(self.id_field, &id));
			self.inner.add_document(document).map_err(index_error)?;
		}
		Ok(prepared.mutation_count)
	}

	pub fn commit(&mut self) -> Result<u64> {
		self.commit_with_payload(None)
	}

	pub fn commit_with_payload(&mut self, payload: Option<&str>) -> Result<u64> {
		if self.checkpoint_required && payload.is_none() {
			return Err(FulltextError::new(
				"E_CHECKPOINT_REQUIRED",
				"index has a checkpoint; use publish with a payload",
			));
		}
		if payload.is_some_and(|payload| payload.len() > MAX_COMMIT_PAYLOAD_BYTES) {
			return Err(FulltextError::invalid(format!(
				"commit payload exceeds {MAX_COMMIT_PAYLOAD_BYTES} UTF-8 bytes"
			)));
		}
		let mut commit = self.inner.prepare_commit().map_err(index_error)?;
		if let Some(payload) = payload {
			commit.set_payload(payload);
		}
		self.checkpoint_required |= payload.is_some();
		let opstamp = commit.commit().map_err(index_error)?;
		Ok(opstamp)
	}

	pub fn rollback(&mut self) -> Result<u64> {
		self.inner.rollback().map_err(index_error)
	}

	pub fn close(self) -> Result<()> {
		self.inner.wait_merging_threads().map_err(index_error)
	}
}

fn build_schema(config: &EngineIdentityConfig) -> Result<(Schema, Field, Field, Vec<EngineField>)> {
	let mut builder = Schema::builder();
	let id_indexing = TextFieldIndexing::default()
		.set_tokenizer("raw")
		.set_index_option(IndexRecordOption::Basic)
		.set_fieldnorms(false);
	let id_options = TextOptions::default().set_indexing_options(id_indexing).set_fast(None);
	let id_field = builder.add_text_field(ID_FIELD_NAME, id_options);
	let version_field = builder.add_text_field(VERSION_FIELD_NAME, TextOptions::default().set_fast(None));
	let record = if config.positions {
		IndexRecordOption::WithFreqsAndPositions
	} else {
		IndexRecordOption::WithFreqs
	};
	let mut fields = Vec::with_capacity(config.fields.len());
	for field in &config.fields {
		let indexing = TextFieldIndexing::default()
			.set_tokenizer(ANALYZER_NAME)
			.set_index_option(record);
		let options = TextOptions::default().set_indexing_options(indexing);
		let schema_field = builder.add_text_field(&field.name, options);
		let surface_field = if config.surface_terms {
			let surface_name = format!("__fulltext_surface_{}", fields.len());
			let surface_indexing = TextFieldIndexing::default()
				.set_tokenizer(SURFACE_ANALYZER_NAME)
				.set_index_option(record);
			Some(builder.add_text_field(
				&surface_name,
				TextOptions::default().set_indexing_options(surface_indexing),
			))
		} else {
			None
		};
		fields.push(EngineField {
			name: field.name.clone(),
			field: schema_field,
			surface_field,
			weight: field.weight,
		});
	}
	Ok((builder.build(), id_field, version_field, fields))
}

#[derive(Clone, Default)]
struct NfkcTokenizer {
	token: Token,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SourceSpan {
	start: usize,
	end: usize,
}

impl SourceSpan {
	fn merge(&mut self, other: Self) {
		self.start = self.start.min(other.start);
		self.end = self.end.max(other.end);
	}
}

#[derive(Clone, Copy)]
struct MappedCharacter {
	character: char,
	span: SourceSpan,
}

enum MappedCharacters<'a> {
	Original(std::str::CharIndices<'a>),
	Normalized(MappedNfkc<'a>),
}

impl MappedCharacters<'_> {
	fn next(&mut self) -> Option<MappedCharacter> {
		match self {
			Self::Original(characters) => characters.next().map(|(start, character)| MappedCharacter {
				character,
				span: SourceSpan {
					start,
					end: start + character.len_utf8(),
				},
			}),
			Self::Normalized(characters) => characters.next(),
		}
	}
}

struct MappedNfkc<'a> {
	source: std::str::CharIndices<'a>,
	decomposed: Vec<char>,
	decomposition_pending: Vec<(u8, MappedCharacter)>,
	nonstarter_count: usize,
	composee: Option<MappedCharacter>,
	composition_pending: Vec<MappedCharacter>,
	last_combining_class: Option<u8>,
	output: Vec<MappedCharacter>,
	output_offset: usize,
	finished: bool,
}

impl<'a> MappedNfkc<'a> {
	fn new(source: &'a str) -> Self {
		Self {
			source: source.char_indices(),
			decomposed: Vec::new(),
			decomposition_pending: Vec::new(),
			nonstarter_count: 0,
			composee: None,
			composition_pending: Vec::new(),
			last_combining_class: None,
			output: Vec::new(),
			output_offset: 0,
			finished: false,
		}
	}

	fn push_decomposed(&mut self, character: MappedCharacter) {
		let combining_class = canonical_combining_class(character.character);
		if combining_class == 0 {
			self.flush_decomposition();
			self.push_recomposition(character);
		} else {
			self.decomposition_pending.push((combining_class, character));
		}
	}

	fn flush_decomposition(&mut self) {
		let mut pending = std::mem::take(&mut self.decomposition_pending);
		pending.sort_by_key(|(combining_class, _)| *combining_class);
		for (_, character) in pending.drain(..) {
			self.push_recomposition(character);
		}
		self.decomposition_pending = pending;
	}

	fn push_recomposition(&mut self, character: MappedCharacter) {
		let combining_class = canonical_combining_class(character.character);
		let Some(mut composee) = self.composee.take() else {
			if combining_class == 0 {
				self.composee = Some(character);
			} else {
				self.output.push(character);
			}
			return;
		};
		let composition = match self.last_combining_class {
			None => compose(composee.character, character.character),
			Some(last) if last < combining_class => compose(composee.character, character.character),
			Some(_) => None,
		};
		if let Some(composed) = composition {
			composee.character = composed;
			composee.span.merge(character.span);
			self.composee = Some(composee);
			return;
		}
		if combining_class == 0 {
			self.output.push(composee);
			self.output.append(&mut self.composition_pending);
			self.composee = Some(character);
			self.last_combining_class = None;
		} else {
			self.composee = Some(composee);
			self.composition_pending.push(character);
			self.last_combining_class = Some(combining_class);
		}
	}

	fn finish(&mut self) {
		self.flush_decomposition();
		if let Some(composee) = self.composee.take() {
			self.output.push(composee);
		}
		self.output.append(&mut self.composition_pending);
		self.finished = true;
	}

	fn next(&mut self) -> Option<MappedCharacter> {
		loop {
			if let Some(character) = self.output.get(self.output_offset).copied() {
				self.output_offset += 1;
				return Some(character);
			}
			self.output.clear();
			self.output_offset = 0;
			if self.finished {
				return None;
			}
			if let Some((start, source_character)) = self.source.next() {
				let span = SourceSpan {
					start,
					end: start + source_character.len_utf8(),
				};
				let mut decomposed = std::mem::take(&mut self.decomposed);
				decomposed.clear();
				decompose_compatible(source_character, |character| decomposed.push(character));
				let leading_nonstarters = decomposed
					.iter()
					.take_while(|character| canonical_combining_class(**character) != 0)
					.count();
				if self.nonstarter_count + leading_nonstarters > MAX_NONSTARTERS {
					self.push_decomposed(MappedCharacter {
						character: COMBINING_GRAPHEME_JOINER,
						span,
					});
					self.nonstarter_count = 0;
				}
				if leading_nonstarters == decomposed.len() {
					self.nonstarter_count += decomposed.len();
				} else {
					self.nonstarter_count = decomposed
						.iter()
						.rev()
						.take_while(|character| canonical_combining_class(**character) != 0)
						.count();
				}
				for character in decomposed.drain(..) {
					self.push_decomposed(MappedCharacter { character, span });
				}
				self.decomposed = decomposed;
			} else {
				self.finish();
			}
		}
	}
}

fn is_stream_safe_nfkc(text: &str) -> bool {
	if !is_nfkc(text) {
		return false;
	}
	let mut nonstarter_count = 0;
	for source_character in text.chars() {
		let mut decomposition_length = 0;
		let mut leading_nonstarters = 0;
		let mut trailing_nonstarters = 0;
		let mut saw_starter = false;
		decompose_compatible(source_character, |character| {
			decomposition_length += 1;
			if canonical_combining_class(character) == 0 {
				saw_starter = true;
				trailing_nonstarters = 0;
			} else {
				if !saw_starter {
					leading_nonstarters += 1;
				}
				trailing_nonstarters += 1;
			}
		});
		if nonstarter_count + leading_nonstarters > MAX_NONSTARTERS {
			return false;
		}
		nonstarter_count = if leading_nonstarters == decomposition_length {
			nonstarter_count + decomposition_length
		} else {
			trailing_nonstarters
		};
	}
	true
}

struct NfkcTokenStream<'a> {
	characters: MappedCharacters<'a>,
	token: &'a mut Token,
}

impl Tokenizer for NfkcTokenizer {
	type TokenStream<'a> = NfkcTokenStream<'a>;

	fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
		self.token.reset();
		let characters = if text.is_ascii() || is_stream_safe_nfkc(text) {
			MappedCharacters::Original(text.char_indices())
		} else {
			MappedCharacters::Normalized(MappedNfkc::new(text))
		};
		NfkcTokenStream {
			characters,
			token: &mut self.token,
		}
	}
}

impl TokenStream for NfkcTokenStream<'_> {
	fn advance(&mut self) -> bool {
		self.token.text.clear();
		self.token.position = self.token.position.wrapping_add(1);
		while let Some(character) = self.characters.next() {
			if !character.character.is_alphanumeric() {
				continue;
			}
			let mut source_span = character.span;
			let mut token_characters = 1;
			self.token.text.push(character.character);
			while let Some(character) = self.characters.next() {
				if !character.character.is_alphanumeric() {
					break;
				}
				source_span.merge(character.span);
				if token_characters < MAX_TOKEN_CHARACTERS {
					self.token.text.push(character.character);
					token_characters += 1;
				}
			}
			self.token.offset_from = source_span.start;
			self.token.offset_to = source_span.end;
			return true;
		}
		false
	}

	fn token(&self) -> &Token {
		self.token
	}

	fn token_mut(&mut self) -> &mut Token {
		self.token
	}
}

#[derive(Clone)]
struct EnglishPossessiveFilter;

impl TokenFilter for EnglishPossessiveFilter {
	type Tokenizer<T: Tokenizer> = EnglishPossessiveFilterWrapper<T>;

	fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T> {
		EnglishPossessiveFilterWrapper { tokenizer }
	}
}

#[derive(Clone)]
struct EnglishPossessiveFilterWrapper<T> {
	tokenizer: T,
}

impl<T: Tokenizer> Tokenizer for EnglishPossessiveFilterWrapper<T> {
	type TokenStream<'a> = EnglishPossessiveTokenStream<'a, T::TokenStream<'a>>;

	fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
		EnglishPossessiveTokenStream {
			source: text,
			tail: self.tokenizer.token_stream(text),
		}
	}
}

struct EnglishPossessiveTokenStream<'a, T> {
	source: &'a str,
	tail: T,
}

impl<T: TokenStream> TokenStream for EnglishPossessiveTokenStream<'_, T> {
	fn advance(&mut self) -> bool {
		while self.tail.advance() {
			let (position, possessive) = {
				let token = self.tail.token();
				(
					token.position,
					token.text.eq_ignore_ascii_case("s") && is_possessive_suffix(self.source, token.offset_from),
				)
			};
			if possessive {
				self.tail.token_mut().position = position.wrapping_sub(1);
			} else {
				return true;
			}
		}
		false
	}

	fn token(&self) -> &Token {
		self.tail.token()
	}

	fn token_mut(&mut self) -> &mut Token {
		self.tail.token_mut()
	}
}

fn is_possessive_suffix(source: &str, offset: usize) -> bool {
	let Some(prefix) = source.get(..offset) else {
		return false;
	};
	let stem = prefix
		.strip_suffix('\'')
		.or_else(|| prefix.strip_suffix('’'))
		.or_else(|| prefix.strip_suffix('＇'));
	stem.and_then(|stem| stem.chars().next_back())
		.is_some_and(char::is_alphanumeric)
}

#[derive(Clone)]
struct SynonymFilter {
	rules: SynonymMap,
}

impl TokenFilter for SynonymFilter {
	type Tokenizer<T: Tokenizer> = SynonymFilterWrapper<T>;

	fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T> {
		SynonymFilterWrapper {
			tokenizer,
			rules: self.rules,
			pending: Vec::new(),
		}
	}
}

#[derive(Clone)]
struct SynonymFilterWrapper<T> {
	tokenizer: T,
	rules: SynonymMap,
	pending: Vec<Token>,
}

impl<T: Tokenizer> Tokenizer for SynonymFilterWrapper<T> {
	type TokenStream<'a> = SynonymTokenStream<'a, T::TokenStream<'a>>;

	fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
		self.pending.clear();
		SynonymTokenStream {
			tail: self.tokenizer.token_stream(text),
			rules: &self.rules,
			pending: &mut self.pending,
		}
	}
}

struct SynonymTokenStream<'a, T> {
	tail: T,
	rules: &'a HashMap<String, Vec<String>>,
	pending: &'a mut Vec<Token>,
}

impl<T: TokenStream> TokenStream for SynonymTokenStream<'_, T> {
	fn advance(&mut self) -> bool {
		self.pending.pop();
		if !self.pending.is_empty() {
			return true;
		}
		if !self.tail.advance() {
			return false;
		}
		let token = self.tail.token();
		if let Some(replacements) = self.rules.get(&token.text) {
			for replacement in replacements.iter().rev() {
				let mut expanded = token.clone();
				expanded.text = replacement.clone();
				self.pending.push(expanded);
			}
			self.pending.push(token.clone());
		}
		true
	}

	fn token(&self) -> &Token {
		self.pending.last().unwrap_or_else(|| self.tail.token())
	}

	fn token_mut(&mut self) -> &mut Token {
		self.pending.last_mut().unwrap_or_else(|| self.tail.token_mut())
	}
}

fn build_analyzer(stop_words: bool, synonyms: Option<SynonymMap>) -> Result<TextAnalyzer> {
	let mut builder = TextAnalyzer::builder(NfkcTokenizer::default())
		.filter_dynamic(EnglishPossessiveFilter)
		.filter_dynamic(LowerCaser)
		.filter_dynamic(AsciiFoldingFilter)
		.filter_dynamic(RemoveLongFilter::limit(40));
	if stop_words {
		let stop_filter = StopWordFilter::new(Language::English)
			.ok_or_else(|| FulltextError::new("E_NATIVE_FAILURE", "English stop words are unavailable"))?;
		builder = builder.filter_dynamic(stop_filter);
	}
	builder = builder.filter_dynamic(Stemmer::new(Language::English));
	if let Some(rules) = synonyms.filter(|rules| !rules.is_empty()) {
		builder = builder.filter_dynamic(SynonymFilter { rules });
	}
	Ok(builder.build())
}

fn build_surface_analyzer(synonyms: Option<SynonymMap>) -> TextAnalyzer {
	let mut builder = TextAnalyzer::builder(NfkcTokenizer::default())
		.filter_dynamic(EnglishPossessiveFilter)
		.filter_dynamic(LowerCaser)
		.filter_dynamic(AsciiFoldingFilter)
		.filter_dynamic(RemoveLongFilter::limit(40));
	if let Some(rules) = synonyms.filter(|rules| !rules.is_empty()) {
		builder = builder.filter_dynamic(SynonymFilter { rules });
	}
	builder.build()
}

fn canonical_identity(config: &EngineIdentityConfig) -> Result<CanonicalIdentity> {
	let mut analyzer = build_analyzer(config.stop_words, None)?;
	let mut normalizer = build_surface_analyzer(None);
	let mut canonical = config.clone();
	let mut analyzed_sources = HashSet::with_capacity(config.synonyms.len());
	let mut surface_sources = HashSet::with_capacity(config.synonyms.len());
	let mut rules = Vec::with_capacity(config.synonyms.len());
	let mut analyzed_lookup = HashMap::with_capacity(config.synonyms.len());
	let mut surface_lookup = HashMap::with_capacity(config.synonyms.len());
	for rule in &config.synonyms {
		let analyzed_source = canonical_synonym_term(&mut analyzer, &rule.source, "source")?;
		let surface_source = canonical_synonym_term(&mut normalizer, &rule.source, "source")?;
		if !analyzed_sources.insert(analyzed_source.clone()) || !surface_sources.insert(surface_source.clone()) {
			return Err(FulltextError::invalid(format!(
				"synonym source {surface_source:?} is declared more than once after analysis"
			)));
		}
		let mut analyzed_replacements = Vec::with_capacity(rule.replacements.len());
		let mut surface_replacements = Vec::with_capacity(rule.replacements.len());
		for replacement in &rule.replacements {
			let analyzed_replacement = canonical_synonym_term(&mut analyzer, replacement, "replacement")?;
			let surface_replacement = canonical_synonym_term(&mut normalizer, replacement, "replacement")?;
			if analyzed_replacement == analyzed_source || surface_replacement == surface_source {
				return Err(FulltextError::invalid(
					"a synonym replacement must differ from its source after analysis",
				));
			}
			analyzed_replacements.push(analyzed_replacement);
			surface_replacements.push(surface_replacement);
		}
		analyzed_replacements.sort();
		analyzed_replacements.dedup();
		surface_replacements.sort();
		surface_replacements.dedup();
		if analyzed_replacements.len() != rule.replacements.len()
			|| surface_replacements.len() != rule.replacements.len()
		{
			return Err(FulltextError::invalid(
				"synonym replacements must be unique after analysis",
			));
		}
		analyzed_lookup.insert(analyzed_source, analyzed_replacements);
		surface_lookup.insert(surface_source.clone(), surface_replacements.clone());
		rules.push(SynonymRule {
			source: surface_source,
			replacements: surface_replacements,
		});
	}
	rules.sort_by(|left, right| left.source.cmp(&right.source));
	canonical.synonyms = rules;
	Ok(CanonicalIdentity {
		config: canonical,
		analyzer,
		analyzed_synonyms: Arc::new(analyzed_lookup),
		surface_synonyms: Arc::new(surface_lookup),
	})
}

fn canonical_synonym_term(analyzer: &mut TextAnalyzer, text: &str, label: &str) -> Result<String> {
	let mut stream = analyzer.token_stream(text);
	if !stream.advance() {
		return Err(FulltextError::invalid(format!(
			"synonym {label} must produce exactly one analyzed term"
		)));
	}
	let term = stream.token().text.clone();
	if stream.advance() {
		return Err(FulltextError::invalid(format!(
			"synonym {label} must produce exactly one analyzed term"
		)));
	}
	Ok(term)
}

fn identity_bytes(config: &EngineIdentityConfig) -> Vec<u8> {
	let mut bytes = b"HTFI\x04\x00".to_vec();
	push_string(&mut bytes, &config.index_id);
	push_string(&mut bytes, &config.generation);
	push_string(&mut bytes, &config.analyzer);
	bytes.extend_from_slice(&[
		config.stop_words as u8,
		config.positions as u8,
		config.surface_terms as u8,
	]);
	bytes.extend_from_slice(&(config.synonyms.len() as u16).to_le_bytes());
	for rule in &config.synonyms {
		push_string(&mut bytes, &rule.source);
		bytes.extend_from_slice(&(rule.replacements.len() as u16).to_le_bytes());
		for replacement in &rule.replacements {
			push_string(&mut bytes, replacement);
		}
	}
	bytes.extend_from_slice(&(config.fields.len() as u16).to_le_bytes());
	for field in &config.fields {
		push_string(&mut bytes, &field.name);
	}
	bytes
}

pub(crate) fn persisted_index_id(bytes: &[u8]) -> Option<&str> {
	let version = bytes.get(4..6)?;
	if bytes.get(..4)? != b"HTFI" || !matches!(version, b"\x01\x00" | b"\x02\x00" | b"\x03\x00" | b"\x04\x00") {
		return None;
	}
	let mut offset = 6;
	let index_id = take_string(bytes, &mut offset)?;
	take_string(bytes, &mut offset)?;
	take_string(bytes, &mut offset)?;
	if bytes
		.get(offset..offset.checked_add(3)?)?
		.iter()
		.any(|value| *value > 1)
	{
		return None;
	}
	offset += 3;
	if matches!(version, b"\x03\x00" | b"\x04\x00") {
		let synonym_count = u16::from_le_bytes(bytes.get(offset..offset.checked_add(2)?)?.try_into().ok()?) as usize;
		offset += 2;
		for _ in 0..synonym_count {
			take_string(bytes, &mut offset)?;
			let replacement_count =
				u16::from_le_bytes(bytes.get(offset..offset.checked_add(2)?)?.try_into().ok()?) as usize;
			offset += 2;
			for _ in 0..replacement_count {
				take_string(bytes, &mut offset)?;
			}
		}
	}
	let field_count = u16::from_le_bytes(bytes.get(offset..offset.checked_add(2)?)?.try_into().ok()?) as usize;
	offset += 2;
	for _ in 0..field_count {
		take_string(bytes, &mut offset)?;
	}
	(offset == bytes.len()).then_some(index_id)
}

fn take_string<'a>(bytes: &'a [u8], offset: &mut usize) -> Option<&'a str> {
	let length = u32::from_le_bytes(bytes.get(*offset..(*offset).checked_add(4)?)?.try_into().ok()?) as usize;
	*offset += 4;
	let end = (*offset).checked_add(length)?;
	let value = std::str::from_utf8(bytes.get(*offset..end)?).ok()?;
	*offset = end;
	Some(value)
}

fn push_string(bytes: &mut Vec<u8>, value: &str) {
	bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
	bytes.extend_from_slice(value.as_bytes());
}

fn storage_error(error: impl std::fmt::Display) -> FulltextError {
	FulltextError::new("E_STORAGE", error.to_string())
}

fn index_error(error: tantivy::TantivyError) -> FulltextError {
	match error {
		tantivy::TantivyError::LockFailure(tantivy::directory::error::LockError::LockBusy, _) => {
			FulltextError::new("E_LOCK_BUSY", "another writer owns the Tantivy index lock")
		}
		tantivy::TantivyError::OpenDirectoryError(_)
		| tantivy::TantivyError::OpenReadError(_)
		| tantivy::TantivyError::OpenWriteError(_)
		| tantivy::TantivyError::IoError(_) => storage_error(error),
		other => FulltextError::native(other),
	}
}

pub(crate) fn recovery_index_error(error: tantivy::TantivyError) -> FulltextError {
	match error {
		tantivy::TantivyError::DataCorruption(_) => FulltextError::new("E_INDEX_CORRUPT", error.to_string()),
		tantivy::TantivyError::IncompatibleIndex(_)
		| tantivy::TantivyError::OpenReadError(OpenReadError::IncompatibleIndex(_)) => {
			FulltextError::new("E_INDEX_FORMAT_INCOMPATIBLE", error.to_string())
		}
		tantivy::TantivyError::OpenReadError(OpenReadError::FileDoesNotExist(_)) => {
			FulltextError::new("E_INDEX_CORRUPT", error.to_string())
		}
		tantivy::TantivyError::OpenReadError(OpenReadError::IoError { ref io_error, .. })
			if matches!(
				io_error.kind(),
				std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
			) =>
		{
			FulltextError::new("E_INDEX_CORRUPT", error.to_string())
		}
		tantivy::TantivyError::IoError(ref io_error)
			if matches!(
				io_error.kind(),
				std::io::ErrorKind::InvalidData | std::io::ErrorKind::UnexpectedEof
			) =>
		{
			FulltextError::new("E_INDEX_CORRUPT", error.to_string())
		}
		other => index_error(other),
	}
}

fn validated_committed_payload(index: &Index) -> Result<Option<String>> {
	let payload = index.load_metas().map_err(recovery_index_error)?.payload;
	validated_payload(payload)
}

fn validated_payload(payload: Option<String>) -> Result<Option<String>> {
	if payload
		.as_ref()
		.is_some_and(|payload| payload.len() > MAX_COMMIT_PAYLOAD_BYTES)
	{
		return Err(FulltextError::new(
			"E_INDEX_CORRUPT",
			"the persisted commit payload exceeds the supported bound",
		));
	}
	Ok(payload)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::protocol::{FieldConfig, Limits};
	use std::io;
	use std::sync::atomic::{AtomicBool, Ordering};
	use std::sync::Arc;
	use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
	use tantivy::directory::{
		Directory, DirectoryLock, FileHandle, RamDirectory, WatchCallback, WatchHandle, WritePtr,
	};

	#[derive(Clone, Debug)]
	struct AmbiguousAtomicWriteDirectory {
		inner: RamDirectory,
		fail_next: Arc<AtomicBool>,
	}

	impl AmbiguousAtomicWriteDirectory {
		fn new() -> Self {
			Self {
				inner: RamDirectory::create(),
				fail_next: Arc::new(AtomicBool::new(false)),
			}
		}

		fn fail_after_next_atomic_write(&self) {
			self.fail_next.store(true, Ordering::Release);
		}
	}

	impl Directory for AmbiguousAtomicWriteDirectory {
		fn get_file_handle(&self, path: &Path) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
			self.inner.get_file_handle(path)
		}

		fn delete(&self, path: &Path) -> std::result::Result<(), DeleteError> {
			self.inner.delete(path)
		}

		fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
			self.inner.exists(path)
		}

		fn open_write(&self, path: &Path) -> std::result::Result<WritePtr, OpenWriteError> {
			self.inner.open_write(path)
		}

		fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
			self.inner.atomic_read(path)
		}

		fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
			self.inner.atomic_write(path, data)?;
			if path == Path::new(META_PATH) && self.fail_next.swap(false, Ordering::AcqRel) {
				return Err(io::Error::other("injected ambiguous atomic write"));
			}
			Ok(())
		}

		fn sync_directory(&self) -> io::Result<()> {
			self.inner.sync_directory()
		}

		fn acquire_lock(&self, lock: &tantivy::directory::Lock) -> std::result::Result<DirectoryLock, LockError> {
			self.inner.acquire_lock(lock)
		}

		fn watch(&self, callback: WatchCallback) -> tantivy::Result<WatchHandle> {
			self.inner.watch(callback)
		}
	}

	fn config() -> EngineConfig {
		EngineConfig {
			identity: EngineIdentityConfig {
				index_id: "products".to_owned(),
				generation: "one".to_owned(),
				fields: vec![
					FieldConfig {
						name: "title".to_owned(),
						weight: 3.0,
					},
					FieldConfig {
						name: "description".to_owned(),
						weight: 1.0,
					},
				],
				analyzer: ANALYZER_NAME.to_owned(),
				stop_words: true,
				positions: true,
				surface_terms: false,
				synonyms: Vec::new(),
			},
			limits: Limits {
				indexing_threads: 1,
				search_threads: 2,
				writer_memory_bytes: 15_000_000,
				max_queued_commands: 8,
				max_queued_bytes: 1 << 20,
				max_batch_bytes: 1 << 20,
			},
		}
	}

	fn expression(text: &str, mode: SearchMode, fields: Vec<String>) -> SearchExpression {
		SearchExpression::Clause(SearchClause {
			text: text.to_owned(),
			mode,
			fields,
		})
	}

	fn batch() -> MutationBatch {
		MutationBatch {
			upserts: vec![
				crate::protocol::Upsert {
					id: "one".to_owned(),
					version: None,
					fields: vec![("title".to_owned(), vec!["Running Shoes".to_owned()])],
				},
				crate::protocol::Upsert {
					id: "two".to_owned(),
					version: None,
					fields: vec![("description".to_owned(), vec!["shoe rack".to_owned()])],
				},
			],
			deletes: Vec::new(),
		}
	}

	#[test]
	fn single_term_phrases_respect_the_clause_limit() {
		let mut config = config();
		config.identity.fields = (0..=MAX_QUERY_CLAUSES)
			.map(|index| FieldConfig {
				name: format!("field_{index}"),
				weight: 1.0,
			})
			.collect();
		let engine = Engine::open(RamDirectory::create(), &config).unwrap();
		let fields = engine.fields.iter().collect::<Vec<_>>();
		assert_eq!(
			engine.phrase_query("shoe", &fields).unwrap_err().code,
			"E_INVALID_ARGUMENT"
		);
	}

	#[test]
	fn any_terms_flatten_across_fields_without_changing_matches_or_scores() {
		let mut config = config();
		config.identity.surface_terms = true;
		let engine = Engine::open(RamDirectory::create(), &config).unwrap();
		let fields = engine.fields.iter().collect::<Vec<_>>();
		let (flat_query, clauses) = engine.term_query("waterproof trail", &fields, Occur::Should).unwrap();
		assert_eq!(clauses, 4);
		assert_eq!(format!("{flat_query:?}").matches("BooleanQuery").count(), 1);
		let (grouped_query, _) = engine.term_query("waterproof trail", &fields, Occur::Must).unwrap();
		assert_eq!(format!("{grouped_query:?}").matches("BooleanQuery").count(), 3);

		let mut writer = engine.writer(&config).unwrap();
		writer
			.apply(MutationBatch {
				upserts: vec![
					crate::protocol::Upsert {
						id: "both".to_owned(),
						version: None,
						fields: vec![
							("title".to_owned(), vec!["Waterproof shell".to_owned()]),
							("description".to_owned(), vec!["Trail pack".to_owned()]),
						],
					},
					crate::protocol::Upsert {
						id: "title-only".to_owned(),
						version: None,
						fields: vec![("title".to_owned(), vec!["Waterproof jacket".to_owned()])],
					},
					crate::protocol::Upsert {
						id: "description-only".to_owned(),
						version: None,
						fields: vec![("description".to_owned(), vec!["Trail guide".to_owned()])],
					},
				],
				deletes: Vec::new(),
			})
			.unwrap();
		writer.commit().unwrap();
		let reader = engine.reader().unwrap();
		let searcher = reader.searcher();
		let current = engine
			.search(
				&searcher,
				&SearchRequest {
					expression: expression("waterproof trail", SearchMode::Any, Vec::new()),
					candidate_ids: None,
					offset: 0,
					limit: 10,
					exact_total: true,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap();
		let legacy_query = BooleanQuery::new(
			engine
				.analyze("waterproof trail", false, true)
				.unwrap()
				.into_iter()
				.map(|term| (Occur::Should, engine.term_group(&term, &fields)))
				.collect(),
		);
		let legacy_docs = searcher
			.search(&legacy_query, &TopDocs::with_limit(10).order_by_score())
			.unwrap();
		let mut legacy = legacy_docs
			.iter()
			.zip(search_hit_metadata(&searcher, &legacy_docs).unwrap())
			.map(|((score, _), metadata)| (metadata.id, *score))
			.collect::<Vec<_>>();
		legacy.sort_by(|left, right| right.1.total_cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
		assert_eq!(
			current.hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
			legacy.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>()
		);
		for (current, (_, legacy_score)) in current.hits.iter().zip(legacy) {
			assert!((current.score - legacy_score).abs() <= f32::EPSILON * current.score.abs().max(1.0));
		}
		let all = engine
			.search(
				&searcher,
				&SearchRequest {
					expression: expression("waterproof trail", SearchMode::All, Vec::new()),
					candidate_ids: None,
					offset: 0,
					limit: 10,
					exact_total: true,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap();
		assert_eq!(
			all.hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
			vec!["both"]
		);
		let completed_prefix = engine
			.search(
				&searcher,
				&SearchRequest {
					expression: expression("waterproof trail ", SearchMode::Prefix, Vec::new()),
					candidate_ids: Some(vec!["both".to_owned()]),
					offset: 0,
					limit: 10,
					exact_total: true,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap();
		assert_eq!(completed_prefix.hits[0].id, "both");
		writer.close().unwrap();
	}

	#[test]
	fn any_terms_respect_the_clause_limit() {
		let mut config = config();
		config.identity.fields = (0..=MAX_QUERY_CLAUSES)
			.map(|index| FieldConfig {
				name: format!("field_{index}"),
				weight: 1.0,
			})
			.collect();
		let engine = Engine::open(RamDirectory::create(), &config).unwrap();
		let fields = engine.fields.iter().collect::<Vec<_>>();
		assert_eq!(
			engine.term_query("shoe", &fields, Occur::Should).unwrap_err().code,
			"E_INVALID_ARGUMENT"
		);
	}

	#[test]
	fn edit_distance_helpers_handle_unicode_and_transposition_without_allocation() {
		assert!(within_one_edit("shoe", "shoe"));
		assert!(within_one_edit("shoe", "shoo"));
		assert!(within_one_edit("shoe", "sohe"));
		assert!(within_one_edit("shoe", "shoes"));
		assert!(within_one_edit("café", "cafe"));
		assert!(!within_one_edit("shoe", "boot"));
		assert!(fuzzy_prefix_matches("shoe", "shoestring"));
		assert!(fuzzy_prefix_matches("shoe", "sjoestring"));
		assert!(!fuzzy_prefix_matches("shoe", "boots"));
	}

	#[test]
	fn english_analyzer_normalizes_unicode_and_possessives_with_source_offsets() {
		let mut analyzer = build_analyzer(true, None).unwrap();
		let source = "Müller's ＳＨＯＥＳ re\u{301}sume\u{301} ß";
		let mut stream = analyzer.token_stream(source);
		let mut tokens = Vec::new();
		while stream.advance() {
			tokens.push(stream.token().clone());
		}
		drop(stream);
		assert_eq!(
			tokens.iter().map(|token| token.text.as_str()).collect::<Vec<_>>(),
			["muller", "shoe", "resum", "ss"]
		);
		let resume = &tokens[2];
		assert_eq!(&source[resume.offset_from..resume.offset_to], "re\u{301}sume\u{301}");
		let mut composed = analyzer.token_stream("résumé");
		assert!(composed.advance());
		assert_eq!(composed.token().text, "resum");
		assert!(!composed.advance());
		drop(composed);
		let mut possessive = analyzer.token_stream("dog＇s shoe");
		assert!(possessive.advance());
		assert_eq!(
			(possessive.token().position, possessive.token().text.as_str()),
			(0, "dog")
		);
		assert!(possessive.advance());
		assert_eq!(
			(possessive.token().position, possessive.token().text.as_str()),
			(1, "shoe")
		);
		assert!(!possessive.advance());
		for token in tokens {
			assert!(token.offset_from < token.offset_to);
			assert!(source.get(token.offset_from..token.offset_to).is_some());
		}
		assert_eq!(
			source_span(&utf16_offsets(source), source.len() + 1, source.len() + 2),
			None
		);
	}

	#[test]
	fn nfkc_tokenizer_normalizes_across_source_character_boundaries() {
		use unicode_normalization::UnicodeNormalization;

		for source in [
			"㉠ᅡ",
			"A\u{315}\u{300}",
			"\u{1100}\u{1161}\u{11a8}",
			"\u{301}A\u{30a}",
			"ﷺ\u{301}",
		] {
			let mut mapped = MappedNfkc::new(source);
			let mut actual = String::new();
			while let Some(character) = mapped.next() {
				actual.push(character.character);
				assert!(source.get(character.span.start..character.span.end).is_some());
			}
			assert_eq!(actual, source.nfkc().collect::<String>());
		}
		let source = format!("e\u{301}{}", "\u{315}".repeat(MAX_NONSTARTERS + 1));
		let mut mapped = MappedNfkc::new(&source);
		let mut actual = String::new();
		while let Some(character) = mapped.next() {
			actual.push(character.character);
		}
		assert_eq!(actual, source.stream_safe().nfkc().collect::<String>());
		assert!(actual.contains(COMBINING_GRAPHEME_JOINER));
		let source = "㉠ᅡ";
		let mut tokenizer = NfkcTokenizer::default();
		let mut stream = tokenizer.token_stream(source);
		assert!(stream.advance());
		assert_eq!(stream.token().text, "가");
		assert_eq!(stream.token().offset_from, 0);
		assert_eq!(stream.token().offset_to, source.len());
		assert!(!stream.advance());
	}

	#[test]
	fn mapped_nfkc_matches_stream_safe_unicode_normalization() {
		use unicode_normalization::UnicodeNormalization;

		let ranges = [
			(0x20, 0x7e),
			(0xa0, 0x24f),
			(0x300, 0x36f),
			(0x590, 0x6ff),
			(0x1100, 0x11ff),
			(0x1e00, 0x1eff),
			(0x2100, 0x214f),
			(0x2460, 0x24ff),
			(0xfb00, 0xfb4f),
			(0xff00, 0xffef),
			(0x1f300, 0x1f64f),
		];
		let mut random = 0x9e37_79b9_u32;
		for _ in 0..4_096 {
			random = random.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
			let length = (random as usize % 48) + 1;
			let mut source = String::new();
			for _ in 0..length {
				random = random.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
				let (start, end) = ranges[random as usize % ranges.len()];
				random = random.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
				let character = char::from_u32(start + random % (end - start + 1)).unwrap();
				source.push(character);
			}
			let mut mapped = MappedNfkc::new(&source);
			let mut actual = String::new();
			while let Some(character) = mapped.next() {
				actual.push(character.character);
				assert!(source.get(character.span.start..character.span.end).is_some());
			}
			assert_eq!(actual, source.stream_safe().nfkc().collect::<String>());
		}

		let composed = format!("\u{e9}{}", "\u{315}".repeat(MAX_NONSTARTERS + 1));
		let decomposed = format!("e\u{301}{}", "\u{315}".repeat(MAX_NONSTARTERS + 1));
		let mut tokenizer = NfkcTokenizer::default();
		let tokens = |tokenizer: &mut NfkcTokenizer, value: &str| {
			let mut stream = tokenizer.token_stream(value);
			let mut tokens = Vec::new();
			while stream.advance() {
				tokens.push(stream.token().text.clone());
			}
			tokens
		};
		assert_eq!(tokens(&mut tokenizer, &composed), tokens(&mut tokenizer, &decomposed));
	}

	#[test]
	fn prefix_length_is_checked_after_normalization() {
		let engine = Engine::open(RamDirectory::create(), &config()).unwrap();
		let query = "Ａ".repeat(14);
		let (completed, prefix) = engine.final_surface_term(&query).unwrap();
		assert!(completed.is_empty());
		assert_eq!(prefix.unwrap(), "a".repeat(14));
		let (completed, prefix) = engine.final_surface_term("½abc").unwrap();
		assert_eq!(completed, ["1"]);
		assert_eq!(prefix.unwrap(), "2abc");
		let (_, prefix) = engine.final_surface_term(&"é".repeat(39)).unwrap();
		assert_eq!(prefix.unwrap(), "e".repeat(39));
	}

	#[test]
	fn trace_token_ceiling_marks_an_unmatched_record_incomplete() {
		let mut config = config();
		config.identity.surface_terms = true;
		let engine = Engine::open(RamDirectory::create(), &config).unwrap();
		let mut source = "x ".repeat(MAX_TRACE_TOKENS_PER_VALUE);
		source.push_str("needle");
		let trace = engine
			.trace_matches(
				&SearchRequest {
					expression: expression("needle", SearchMode::Any, vec!["description".to_owned()]),
					candidate_ids: None,
					offset: 0,
					limit: 10,
					exact_total: true,
					budget_milliseconds: 30_000,
				},
				&[TraceRecord {
					id: "one".to_owned(),
					fields: vec![("description".to_owned(), vec![source])],
				}],
				None,
			)
			.unwrap();
		assert!(!trace.complete);
		assert!(trace.records.is_empty());
	}

	#[test]
	fn index_time_synonyms_are_canonical_persisted_and_traceable() {
		let directory = RamDirectory::create();
		let mut config = config();
		config.identity.surface_terms = true;
		config.identity.synonyms = vec![SynonymRule {
			source: "ＴＶ".to_owned(),
			replacements: vec!["telly".to_owned(), "Televisions".to_owned()],
		}];
		let engine = Engine::open(directory.clone(), &config).unwrap();
		let mut writer = engine.writer(&config).unwrap();
		writer
			.apply(MutationBatch {
				upserts: vec![
					crate::protocol::Upsert {
						id: "one".to_owned(),
						version: None,
						fields: vec![("title".to_owned(), vec!["TV stand".to_owned()])],
					},
					crate::protocol::Upsert {
						id: "two".to_owned(),
						version: None,
						fields: vec![("title".to_owned(), vec!["monitor stand".to_owned()])],
					},
				],
				deletes: Vec::new(),
			})
			.unwrap();
		writer.commit().unwrap();
		let reader = engine.reader().unwrap();
		for text in ["tv", "telly", "television"] {
			let result = engine
				.search(
					&reader.searcher(),
					&SearchRequest {
						expression: expression(text, SearchMode::Any, Vec::new()),
						candidate_ids: None,
						offset: 0,
						limit: 10,
						exact_total: true,
						budget_milliseconds: 30_000,
					},
				)
				.unwrap();
			assert_eq!(result.hits[0].id, "one");
		}
		let trace = engine
			.trace_matches(
				&SearchRequest {
					expression: expression("television", SearchMode::Any, Vec::new()),
					candidate_ids: None,
					offset: 0,
					limit: 10,
					exact_total: true,
					budget_milliseconds: 30_000,
				},
				&[TraceRecord {
					id: "one".to_owned(),
					fields: vec![("title".to_owned(), vec!["TV stand".to_owned()])],
				}],
				None,
			)
			.unwrap();
		assert_eq!(trace.records[0].values[0].spans, [TraceSpan { start: 0, end: 2 }]);
		let synonym_score = engine
			.search(
				&reader.searcher(),
				&SearchRequest {
					expression: expression("stand", SearchMode::Any, Vec::new()),
					candidate_ids: None,
					offset: 0,
					limit: 10,
					exact_total: true,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap()
			.hits
			.into_iter()
			.find(|hit| hit.id == "one")
			.unwrap()
			.score;
		let mut baseline_config = config.clone();
		baseline_config.identity.index_id = "baseline".to_owned();
		baseline_config.identity.synonyms.clear();
		let baseline = Engine::open(RamDirectory::create(), &baseline_config).unwrap();
		let mut baseline_writer = baseline.writer(&baseline_config).unwrap();
		baseline_writer
			.apply(MutationBatch {
				upserts: vec![
					crate::protocol::Upsert {
						id: "one".to_owned(),
						version: None,
						fields: vec![("title".to_owned(), vec!["TV stand".to_owned()])],
					},
					crate::protocol::Upsert {
						id: "two".to_owned(),
						version: None,
						fields: vec![("title".to_owned(), vec!["monitor stand".to_owned()])],
					},
				],
				deletes: Vec::new(),
			})
			.unwrap();
		baseline_writer.commit().unwrap();
		let baseline_reader = baseline.reader().unwrap();
		let baseline_score = baseline
			.search(
				&baseline_reader.searcher(),
				&SearchRequest {
					expression: expression("stand", SearchMode::Any, Vec::new()),
					candidate_ids: None,
					offset: 0,
					limit: 10,
					exact_total: true,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap()
			.hits
			.into_iter()
			.find(|hit| hit.id == "one")
			.unwrap()
			.score;
		assert!(synonym_score < baseline_score);
		baseline_writer.close().unwrap();

		writer.close().unwrap();
		let mut reordered = config.clone();
		reordered.identity.synonyms[0].replacements.reverse();
		Engine::open(directory.clone(), &reordered).unwrap();
		reordered.identity.synonyms[0].replacements[0] = "display".to_owned();
		assert_eq!(
			Engine::inspect(directory, &reordered.identity).unwrap_err().code,
			"E_IDENTITY_MISMATCH"
		);
	}

	#[test]
	fn persisted_index_id_accepts_legacy_sidecars_for_reset_only() {
		let config = config();
		for version in [b"\x02\x00".as_slice(), b"\x03\x00".as_slice()] {
			let mut identity = b"HTFI".to_vec();
			identity.extend_from_slice(version);
			push_string(&mut identity, &config.identity.index_id);
			push_string(&mut identity, &config.identity.generation);
			push_string(&mut identity, &config.identity.analyzer);
			identity.extend_from_slice(&[1, 1, 0]);
			if version == b"\x03\x00" {
				identity.extend_from_slice(&0u16.to_le_bytes());
			}
			identity.extend_from_slice(&(config.identity.fields.len() as u16).to_le_bytes());
			for field in &config.identity.fields {
				push_string(&mut identity, &field.name);
			}
			assert_eq!(persisted_index_id(&identity), Some("products"));
			let directory = RamDirectory::create();
			directory.atomic_write(Path::new(IDENTITY_PATH), &identity).unwrap();
			assert_eq!(
				Engine::inspect(directory, &config.identity).unwrap_err().code,
				"E_IDENTITY_MISMATCH"
			);
		}
	}

	#[test]
	fn indexes_searches_updates_and_reopens() {
		let directory = RamDirectory::create();
		let config = config();
		let engine = Engine::open(directory.clone(), &config).unwrap();
		let mut writer = engine.writer(&config).unwrap();
		assert_eq!(
			writer
				.apply(MutationBatch {
					upserts: Vec::new(),
					deletes: vec!["x".repeat(crate::protocol::MAX_RECORD_ID_BYTES + 1)],
				})
				.unwrap_err()
				.code,
			"E_INVALID_ARGUMENT"
		);
		assert_eq!(writer.apply(batch()).unwrap(), 2);
		writer.commit().unwrap();
		let reader = engine.reader().unwrap();
		reader.reload().unwrap();
		let mut request = SearchRequest {
			expression: expression("shoes", SearchMode::Any, Vec::new()),
			candidate_ids: None,
			offset: 0,
			limit: 10,
			exact_total: true,
			budget_milliseconds: 30_000,
		};
		let result = engine.search(&reader.searcher(), &request).unwrap();
		assert_eq!(result.total, 2);
		assert_eq!(result.hits[0].id, "one");
		writer
			.apply(MutationBatch {
				upserts: Vec::new(),
				deletes: vec!["one".to_owned()],
			})
			.unwrap();
		writer.commit().unwrap();
		reader.reload().unwrap();
		assert_eq!(engine.search(&reader.searcher(), &request).unwrap().total, 1);
		writer.close().unwrap();
		let reopened = Engine::open(directory, &config).unwrap();
		let reopened_reader = reopened.reader().unwrap();
		assert_eq!(reopened.search(&reopened_reader.searcher(), &request).unwrap().total, 1);
		request.limit = 0;
		assert_eq!(
			reopened.search(&reopened_reader.searcher(), &request).unwrap_err().code,
			"E_INVALID_ARGUMENT"
		);
	}

	#[test]
	fn supports_structured_query_modes_and_candidate_filters() {
		let mut config = config();
		config.identity.surface_terms = true;
		let engine = Engine::open(RamDirectory::create(), &config).unwrap();
		let mut writer = engine.writer(&config).unwrap();
		writer
			.apply(MutationBatch {
				upserts: vec![
					crate::protocol::Upsert {
						id: "one".to_owned(),
						version: None,
						fields: vec![("title".to_owned(), vec!["Waterproof Trail Running Shoes".to_owned()])],
					},
					crate::protocol::Upsert {
						id: "two".to_owned(),
						version: None,
						fields: vec![("title".to_owned(), vec!["Waterproof Road Shoes".to_owned()])],
					},
					crate::protocol::Upsert {
						id: "three".to_owned(),
						version: None,
						fields: vec![("title".to_owned(), vec!["Wireless Headphones".to_owned()])],
					},
				],
				deletes: Vec::new(),
			})
			.unwrap();
		writer.commit().unwrap();
		let reader = engine.reader().unwrap();

		let search = |text: &str, mode: SearchMode, candidates: Option<Vec<&str>>| {
			engine
				.search(
					&reader.searcher(),
					&SearchRequest {
						expression: expression(text, mode, Vec::new()),
						candidate_ids: candidates.map(|ids| ids.into_iter().map(str::to_owned).collect()),
						offset: 0,
						limit: 10,
						exact_total: true,
						budget_milliseconds: 30_000,
					},
				)
				.unwrap()
		};

		assert_eq!(search("trail running", SearchMode::Phrase, None).hits[0].id, "one");
		assert_eq!(search("waterproof trai", SearchMode::Prefix, None).hits[0].id, "one");
		assert_eq!(search("waterprof", SearchMode::Fuzzy, None).total, 2);
		assert_eq!(
			search("waterproof tral", SearchMode::FuzzyPrefix, None).hits[0].id,
			"one"
		);
		assert_eq!(
			search("waterproof", SearchMode::Any, Some(vec!["two"])).hits[0].id,
			"two"
		);
		let baseline = search("waterproof", SearchMode::Any, None);
		let excluded = engine
			.search(
				&reader.searcher(),
				&SearchRequest {
					expression: SearchExpression::And(vec![
						expression("waterproof", SearchMode::Any, Vec::new()),
						SearchExpression::Not(Box::new(expression("wireless", SearchMode::Any, Vec::new()))),
					]),
					candidate_ids: None,
					offset: 0,
					limit: 10,
					exact_total: true,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap();
		assert_eq!(
			excluded.hits.iter().map(|hit| (&hit.id, hit.score)).collect::<Vec<_>>(),
			baseline.hits.iter().map(|hit| (&hit.id, hit.score)).collect::<Vec<_>>()
		);
		let only_negated = engine
			.search(
				&reader.searcher(),
				&SearchRequest {
					expression: SearchExpression::Not(Box::new(expression("wireless", SearchMode::Any, Vec::new()))),
					candidate_ids: None,
					offset: 0,
					limit: 10,
					exact_total: true,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap();
		assert_eq!(only_negated.total, 2);
		assert_eq!(
			only_negated
				.hits
				.iter()
				.map(|hit| (hit.id.as_str(), hit.score))
				.collect::<Vec<_>>(),
			vec![("one", 0.0), ("two", 0.0)]
		);
		assert!(search("waterproof", SearchMode::Any, Some(Vec::new())).hits.is_empty());
		assert!(search("the", SearchMode::Any, None).hits.is_empty());
		writer.close().unwrap();
	}

	#[test]
	fn rejects_structured_queries_with_excessive_aggregate_expansion() {
		let engine = Engine::open(RamDirectory::create(), &config()).unwrap();
		let reader = engine.reader().unwrap();
		let expression = SearchExpression::And(
			(0..129)
				.map(|_| expression("shoe", SearchMode::Any, Vec::new()))
				.collect(),
		);
		let error = engine
			.search(
				&reader.searcher(),
				&SearchRequest {
					expression,
					candidate_ids: None,
					offset: 0,
					limit: 10,
					exact_total: false,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap_err();
		assert_eq!(error.code, "E_INVALID_ARGUMENT");
	}

	#[test]
	fn orders_equal_scores_by_id_across_segments_and_pages() {
		let directory = RamDirectory::create();
		let config = config();
		let engine = Engine::open(directory.clone(), &config).unwrap();
		let mut writer = engine.writer(&config).unwrap();
		writer
			.inner
			.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
		let inserted_ids = [
			"catalog-0009",
			"catalog-café",
			"catalog-0001",
			"catalog-0010",
			"catalog-café",
			"catalog-0004",
			"catalog-0007",
			"catalog-cafe",
			"catalog-0002",
			"catalog-0008",
			"catalog-0003",
			"catalog-0006",
			"catalog-0005",
			"catalog-0004",
		];
		for (chunk_index, ids) in inserted_ids.chunks(7).enumerate() {
			writer
				.apply(MutationBatch {
					upserts: ids
						.iter()
						.enumerate()
						.map(|(index, id)| crate::protocol::Upsert {
							id: (*id).to_owned(),
							version: (*id == "catalog-0004").then(|| {
								if chunk_index * 7 + index + 1 == inserted_ids.len() {
									"latest"
								} else {
									"original"
								}
								.to_owned()
							}),
							fields: vec![("title".to_owned(), vec!["identical catalog text".to_owned()])],
						})
						.collect(),
					deletes: Vec::new(),
				})
				.unwrap();
			writer.commit().unwrap();
		}
		let reader = engine.reader().unwrap();
		let page = |offset, limit| {
			engine
				.search(
					&reader.searcher(),
					&SearchRequest {
						expression: expression("identical catalog text", SearchMode::Any, Vec::new()),
						candidate_ids: None,
						offset,
						limit,
						exact_total: false,
						budget_milliseconds: 30_000,
					},
				)
				.unwrap()
				.hits
		};
		let expected_ids = inserted_ids
			.into_iter()
			.collect::<BTreeSet<_>>()
			.into_iter()
			.collect::<Vec<_>>();
		let bounded = engine
			.search(
				&reader.searcher(),
				&SearchRequest {
					expression: expression("identical catalog text", SearchMode::Any, Vec::new()),
					candidate_ids: None,
					offset: 0,
					limit: 3,
					exact_total: false,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap();
		assert_eq!(bounded.total, 3);
		assert_eq!(bounded.total_relation, TotalRelation::LowerBound);
		let exact = engine
			.search(
				&reader.searcher(),
				&SearchRequest {
					expression: expression("identical catalog text", SearchMode::Any, Vec::new()),
					candidate_ids: None,
					offset: 0,
					limit: 3,
					exact_total: true,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap();
		assert_eq!(exact.total, expected_ids.len() as u64);
		assert_eq!(exact.total_relation, TotalRelation::Exact);
		let exact_boundary_tie = engine
			.search(
				&reader.searcher(),
				&SearchRequest {
					expression: expression("identical", SearchMode::Any, vec!["title".to_owned()]),
					candidate_ids: None,
					offset: 0,
					limit: 3,
					exact_total: true,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap();
		assert_eq!(exact_boundary_tie.total, expected_ids.len() as u64);
		assert_eq!(exact_boundary_tie.total_relation, TotalRelation::Exact);
		assert_eq!(
			exact_boundary_tie
				.hits
				.iter()
				.map(|hit| hit.id.as_str())
				.collect::<Vec<_>>(),
			expected_ids[..3]
		);
		let full_page = page(0, expected_ids.len());
		let paged_hits = (0..expected_ids.len())
			.flat_map(|offset| page(offset, 1))
			.collect::<Vec<_>>();
		assert_eq!(
			paged_hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
			full_page.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>()
		);
		assert_eq!(
			paged_hits.iter().map(|hit| hit.score.to_bits()).collect::<Vec<_>>(),
			full_page.iter().map(|hit| hit.score.to_bits()).collect::<Vec<_>>()
		);
		assert_eq!(
			full_page.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
			expected_ids
		);
		assert_eq!(
			paged_hits
				.iter()
				.find(|hit| hit.id == "catalog-0004")
				.unwrap()
				.version
				.as_deref(),
			Some("latest")
		);
		let past_end = engine
			.search(
				&reader.searcher(),
				&SearchRequest {
					expression: expression("identical catalog text", SearchMode::Any, Vec::new()),
					candidate_ids: None,
					offset: 20,
					limit: 5,
					exact_total: false,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap();
		assert!(past_end.hits.is_empty());
		assert_eq!(past_end.total, expected_ids.len() as u64);
		assert_eq!(past_end.total_relation, TotalRelation::Exact);
		writer.close().unwrap();
		let reopened = Engine::open(directory, &config).unwrap();
		let result = reopened
			.search(
				&reopened.reader().unwrap().searcher(),
				&SearchRequest {
					expression: expression("identical catalog", SearchMode::Any, Vec::new()),
					candidate_ids: None,
					offset: 0,
					limit: 3,
					exact_total: false,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap();
		assert_eq!(
			result.hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
			expected_ids[..3].to_vec()
		);
	}

	#[test]
	fn resolves_hit_metadata_in_document_order_with_duplicate_and_missing_versions() {
		let config = config();
		let engine = Engine::open(RamDirectory::create(), &config).unwrap();
		let mut writer = engine.writer(&config).unwrap();
		writer
			.apply(MutationBatch {
				upserts: [("one", Some("shared")), ("two", None), ("three", Some("shared"))]
					.into_iter()
					.map(|(id, version)| crate::protocol::Upsert {
						id: id.to_owned(),
						version: version.map(str::to_owned),
						fields: vec![("title".to_owned(), vec!["catalog".to_owned()])],
					})
					.collect(),
				deletes: Vec::new(),
			})
			.unwrap();
		writer.commit().unwrap();
		let searcher = engine.reader().unwrap().searcher();
		let scored_docs = [2, 0, 1]
			.into_iter()
			.map(|doc_id| (1.0, DocAddress::new(0, doc_id)))
			.collect::<Vec<_>>();
		let metadata = search_hit_metadata(&searcher, &scored_docs).unwrap();
		assert_eq!(
			metadata
				.iter()
				.map(|value| (value.id.as_str(), value.version.as_deref()))
				.collect::<Vec<_>>(),
			vec![("three", Some("shared")), ("one", Some("shared")), ("two", None)]
		);
		assert_eq!(
			search_hit_versions(
				&searcher,
				&scored_docs.iter().map(|(_, address)| *address).collect::<Vec<_>>()
			)
			.unwrap(),
			vec![Some("shared".to_owned()), Some("shared".to_owned()), None]
		);
		writer.close().unwrap();
	}

	#[test]
	fn rejects_prefix_expansion_past_the_release_ceiling() {
		let directory = RamDirectory::create();
		let mut config = config();
		config.identity.surface_terms = true;
		let engine = Engine::open(directory, &config).unwrap();
		let mut writer = engine.writer(&config).unwrap();
		writer
			.apply(MutationBatch {
				upserts: (0..=MAX_PREFIX_EXPANSIONS)
					.map(|id| crate::protocol::Upsert {
						id: id.to_string(),
						version: None,
						fields: vec![("title".to_owned(), vec![format!("catalog{id:03}")])],
					})
					.collect(),
				deletes: Vec::new(),
			})
			.unwrap();
		writer.commit().unwrap();
		let reader = engine.reader().unwrap();
		let error = engine
			.search(
				&reader.searcher(),
				&SearchRequest {
					expression: expression("cat", SearchMode::Prefix, Vec::new()),
					candidate_ids: None,
					offset: 0,
					limit: 10,
					exact_total: false,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap_err();
		assert_eq!(error.code, "E_PREFIX_TOO_BROAD");
		assert!(error.message.contains("more than 50 terms"));
		writer.close().unwrap();
	}

	#[test]
	fn rejects_identity_mismatch() {
		let directory = RamDirectory::create();
		let config = config();
		Engine::open(directory.clone(), &config).unwrap();
		let mut different = config;
		different.identity.generation = "two".to_owned();
		let error = match Engine::open(directory, &different) {
			Ok(_) => panic!("identity mismatch was accepted"),
			Err(error) => error,
		};
		assert_eq!(error.code, "E_IDENTITY_MISMATCH");
	}

	#[test]
	fn validates_the_complete_persisted_identity() {
		let config = config();
		let mut identity = identity_bytes(&config.identity);
		assert_eq!(persisted_index_id(&identity), Some("products"));

		identity.pop();
		assert_eq!(persisted_index_id(&identity), None);

		let mut identity = identity_bytes(&config.identity);
		identity.push(0);
		assert_eq!(persisted_index_id(&identity), None);
	}

	#[test]
	fn inspection_is_read_only_and_reports_committed_payload() {
		let directory = RamDirectory::create();
		let config = config();
		assert_eq!(
			Engine::inspect(directory.clone(), &config.identity).unwrap(),
			InspectionResult::Missing
		);
		assert!(!directory.exists(Path::new(IDENTITY_PATH)).unwrap());
		assert!(!directory.exists(Path::new(META_PATH)).unwrap());

		let engine = Engine::open(directory.clone(), &config).unwrap();
		assert_eq!(
			Engine::inspect(directory.clone(), &config.identity).unwrap(),
			InspectionResult::Cursorless
		);
		let mut writer = engine.writer(&config).unwrap();
		writer.commit_with_payload(Some("cursor-v1")).unwrap();
		assert_eq!(
			Engine::inspect(directory.clone(), &config.identity).unwrap(),
			InspectionResult::Payload("cursor-v1".to_owned())
		);
		writer.close().unwrap();

		let mut different = config;
		different.identity.generation = "two".to_owned();
		assert_eq!(
			Engine::inspect(directory, &different.identity).unwrap_err().code,
			"E_IDENTITY_MISMATCH"
		);
	}

	#[test]
	fn recovery_maps_tantivy_format_incompatibility() {
		let error =
			tantivy::TantivyError::IncompatibleIndex(tantivy::directory::error::Incompatibility::CompressionMismatch {
				library_compression_format: "zstd".to_owned(),
				index_compression_format: "lz4".to_owned(),
			});
		assert_eq!(recovery_index_error(error).code, "E_INDEX_FORMAT_INCOMPATIBLE");
		let error = tantivy::TantivyError::DataCorruption(tantivy::error::DataCorruption::comment_only("broken meta"));
		assert_eq!(recovery_index_error(error).code, "E_INDEX_CORRUPT");
		let missing = || {
			tantivy::TantivyError::OpenReadError(OpenReadError::FileDoesNotExist(
				Path::new("missing.term").to_path_buf(),
			))
		};
		assert_eq!(recovery_index_error(missing()).code, "E_INDEX_CORRUPT");
		assert_eq!(index_error(missing()).code, "E_STORAGE");
	}

	#[test]
	fn tantivy_plain_commit_erases_a_prior_payload_without_the_guard() {
		let config = config();
		let engine = Engine::open(RamDirectory::create(), &config).unwrap();
		let mut writer = engine.writer(&config).unwrap();
		writer.commit_with_payload(Some("checkpoint")).unwrap();
		writer.inner.commit().unwrap();
		assert_eq!(engine.committed_payload().unwrap(), None);
	}

	#[test]
	fn checkpoint_guard_survives_reopen_and_rejected_commits_preserve_mutations() {
		let directory = RamDirectory::create();
		let config = config();
		let engine = Engine::open(directory.clone(), &config).unwrap();
		let mut writer = engine.writer(&config).unwrap();
		writer.commit_with_payload(Some("")).unwrap();
		writer.apply(batch()).unwrap();
		assert_eq!(writer.commit().unwrap_err().code, "E_CHECKPOINT_REQUIRED");
		assert_eq!(engine.committed_payload().unwrap().as_deref(), Some(""));
		writer.commit_with_payload(Some("next")).unwrap();
		assert_eq!(engine.reader().unwrap().searcher().num_docs(), 2);
		writer.close().unwrap();
		let reopened = Engine::open(directory, &config).unwrap();
		let (mut writer, payload) = reopened.writer_with_payload(&config).unwrap();
		assert_eq!(payload.as_deref(), Some("next"));
		assert_eq!(writer.commit().unwrap_err().code, "E_CHECKPOINT_REQUIRED");
		writer.rollback().unwrap();
		assert_eq!(writer.commit().unwrap_err().code, "E_CHECKPOINT_REQUIRED");
	}

	#[test]
	fn writer_reads_checkpoint_after_another_owner_publishes() {
		let directory = RamDirectory::create();
		let config = config();
		let old_view = Engine::open(directory.clone(), &config).unwrap();
		assert_eq!(old_view.committed_payload().unwrap(), None);
		let other = Engine::open(directory, &config).unwrap();
		let mut owner = other.writer(&config).unwrap();
		owner.commit_with_payload(Some("published-by-other-owner")).unwrap();
		owner.close().unwrap();
		let (mut writer, payload) = old_view.writer_with_payload(&config).unwrap();
		assert_eq!(payload.as_deref(), Some("published-by-other-owner"));
		assert_eq!(writer.commit().unwrap_err().code, "E_CHECKPOINT_REQUIRED");
	}

	#[test]
	fn committed_payload_requires_the_same_searcher_snapshot() {
		let directory = RamDirectory::create();
		let config = config();
		let reader_engine = Engine::open(directory.clone(), &config).unwrap();
		let reader = reader_engine.reader().unwrap();
		let writer_engine = Engine::open(directory, &config).unwrap();
		let mut writer = writer_engine.writer(&config).unwrap();
		writer.apply(batch()).unwrap();
		writer.commit_with_payload(Some("cursor-v1")).unwrap();
		assert_eq!(
			reader_engine
				.committed_payload_for_searcher(&reader.searcher())
				.unwrap_err()
				.code,
			"E_RELOAD_FAILED"
		);
		reader.reload().unwrap();
		assert_eq!(
			reader_engine
				.committed_payload_for_searcher(&reader.searcher())
				.unwrap()
				.as_deref(),
			Some("cursor-v1")
		);
	}

	#[test]
	fn failed_checkpoint_commit_keeps_the_guard_conservative() {
		let directory = AmbiguousAtomicWriteDirectory::new();
		let config = config();
		let engine = Engine::open(directory.clone(), &config).unwrap();
		let mut writer = engine.writer(&config).unwrap();
		directory.fail_after_next_atomic_write();
		assert!(writer.commit_with_payload(Some("uncertain")).is_err());
		assert_eq!(engine.committed_payload().unwrap().as_deref(), Some("uncertain"));
		assert_eq!(writer.commit().unwrap_err().code, "E_CHECKPOINT_REQUIRED");
	}

	#[test]
	fn commit_payload_survives_merge_and_reopen() {
		let directory = RamDirectory::create();
		let config = config();
		let engine = Engine::open(directory.clone(), &config).unwrap();
		let mut writer = engine.writer(&config).unwrap();
		writer
			.inner
			.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
		writer.apply(batch()).unwrap();
		writer.commit_with_payload(Some("cursor-v1:42")).unwrap();
		writer
			.apply(MutationBatch {
				upserts: vec![crate::protocol::Upsert {
					id: "three".to_owned(),
					version: None,
					fields: vec![("title".to_owned(), vec!["Hiking Boots".to_owned()])],
				}],
				deletes: Vec::new(),
			})
			.unwrap();
		writer.commit_with_payload(Some("cursor-v1:43")).unwrap();
		let segments = engine.index.searchable_segment_ids().unwrap();
		assert_eq!(segments.len(), 2);
		writer.inner.merge(&segments).wait().unwrap();
		assert_eq!(engine.committed_payload().unwrap().as_deref(), Some("cursor-v1:43"));
		writer.close().unwrap();

		let reopened = Engine::open(directory, &config).unwrap();
		assert_eq!(reopened.committed_payload().unwrap().as_deref(), Some("cursor-v1:43"));
	}

	#[test]
	fn completes_an_interrupted_sidecar_first_create() {
		let directory = RamDirectory::create();
		let config = config();
		directory
			.atomic_write(Path::new(IDENTITY_PATH), &identity_bytes(&config.identity))
			.unwrap();
		Engine::open(directory.clone(), &config).unwrap();
		assert!(directory.exists(Path::new(META_PATH)).unwrap());
	}

	#[test]
	fn rejects_meta_without_an_identity_sidecar() {
		let directory = RamDirectory::create();
		let config = config();
		let (schema, _, _, _) = build_schema(&config.identity).unwrap();
		Index::create(directory.clone(), schema, IndexSettings::default()).unwrap();
		let error = match Engine::open(directory, &config) {
			Ok(_) => panic!("meta without an identity sidecar was accepted"),
			Err(error) => error,
		};
		assert_eq!(error.code, "E_INCOMPLETE_CREATE");
	}
}
