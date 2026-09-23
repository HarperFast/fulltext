use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

use tantivy::collector::sort_key::{SortBySimilarityScore, SortByString};
use tantivy::collector::{Count, TopDocs};
use tantivy::directory::error::OpenReadError;
use tantivy::directory::Directory;
use tantivy::query::{
	BooleanQuery, BoostQuery, ConstScoreQuery, DisjunctionMaxQuery, EmptyQuery, FuzzyTermQuery, Occur, PhraseQuery,
	Query, TermQuery, TermSetQuery,
};
use tantivy::schema::{Field, IndexRecordOption, Schema, TantivyDocument, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{
	Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer, TokenStream,
	Tokenizer,
};
use tantivy::{DocAddress, Index, IndexReader, IndexSettings, IndexWriter, Order, ReloadPolicy, Searcher, Term};

use crate::error::{FulltextError, Result};
use crate::protocol::{
	validate_record_id, EngineConfig, EngineIdentityConfig, MutationBatch, SearchMode, SearchRequest, TraceRecord,
	MAX_FUZZY_TERMS, MAX_PREFIX_EXPANSIONS, MAX_QUERY_CLAUSES, MAX_QUERY_TERMS, MAX_SEARCH_RESPONSE_BYTES,
	MAX_TRACE_SPANS,
};

const ID_FIELD_NAME: &str = "__fulltext_id";
pub(crate) const IDENTITY_PATH: &str = ".harper-fulltext-identity";
const META_PATH: &str = "meta.json";
const ANALYZER_NAME: &str = "english@1";
const SURFACE_ANALYZER_NAME: &str = "english_surface@1";
pub const MAX_COMMIT_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub struct Engine {
	index: Index,
	id_field: Field,
	fields: Vec<EngineField>,
	field_lookup: HashMap<String, usize>,
	analyzer: TextAnalyzer,
	surface_analyzer: TextAnalyzer,
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

pub struct Writer {
	inner: IndexWriter,
	checkpoint_required: bool,
	id_field: Field,
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

#[derive(Clone, Debug, PartialEq, Eq)]
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
	pub fn inspect<D: Directory + Clone>(directory: D, config: &EngineIdentityConfig) -> Result<InspectionResult> {
		let (expected_schema, _, _) = build_schema(config)?;
		let expected_identity = identity_bytes(config);
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
		let (schema, id_field, fields) = build_schema(&config.identity)?;
		let expected_identity = identity_bytes(&config.identity);
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
		let analyzer = build_analyzer(config.identity.stop_words)?;
		let surface_analyzer = build_surface_analyzer();
		index.tokenizers().register(ANALYZER_NAME, analyzer.clone());
		index
			.tokenizers()
			.register(SURFACE_ANALYZER_NAME, surface_analyzer.clone());
		let field_lookup = fields
			.iter()
			.enumerate()
			.map(|(index, field)| (field.name.clone(), index))
			.collect();
		Ok(Self {
			index,
			id_field,
			fields,
			field_lookup,
			analyzer,
			surface_analyzer,
			positions: config.identity.positions,
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
		let selected = self.selected_fields(&request.fields)?;
		let query = self.query(searcher, request, &selected)?;
		check_deadline(deadline)?;
		let window_end = request.offset + request.limit;
		let scored_docs = searcher
			.search(
				query.as_ref(),
				&TopDocs::with_limit(window_end.saturating_add(1)).order_by_score(),
			)
			.map_err(index_error)?;
		check_deadline(deadline)?;
		let boundary_tie = scored_docs.len() > window_end && scored_docs[window_end - 1].0 == scored_docs[window_end].0;
		let hits = if boundary_tie {
			let collector = TopDocs::for_doc_range(request.offset..window_end).order_by((
				(SortBySimilarityScore, Order::Desc),
				(SortByString::for_field(ID_FIELD_NAME), Order::Asc),
			));
			searcher
				.search(query.as_ref(), &collector)
				.map_err(index_error)?
				.into_iter()
				.map(|((score, id), _)| {
					id.map(|id| SearchHit { id, score })
						.ok_or_else(|| FulltextError::new("E_NATIVE_FAILURE", "search hit has no ID fast field value"))
				})
				.collect::<Result<Vec<_>>>()?
		} else {
			let ids = search_hit_ids(searcher, &scored_docs)?;
			let mut ranked = scored_docs
				.iter()
				.zip(ids)
				.map(|((score, _), id)| SearchHit { id, score: *score })
				.collect::<Vec<_>>();
			ranked.sort_by(|left, right| {
				right
					.score
					.total_cmp(&left.score)
					.then_with(|| left.id.as_bytes().cmp(right.id.as_bytes()))
			});
			ranked.into_iter().skip(request.offset).take(request.limit).collect()
		};
		let (total, total_relation) = if request.exact_total {
			check_deadline(deadline)?;
			(
				searcher.search(query.as_ref(), &Count).map_err(index_error)? as u64,
				TotalRelation::Exact,
			)
		} else if scored_docs.len() <= window_end {
			(scored_docs.len() as u64, TotalRelation::Exact)
		} else {
			(window_end as u64, TotalRelation::LowerBound)
		};
		check_deadline(deadline)?;
		let response_bytes = hits
			.iter()
			.try_fold(13usize, |bytes, hit| bytes.checked_add(8 + hit.id.len()))
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
		let selected = self.selected_fields(&request.fields)?;
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
		let plan = self.trace_plan(request)?;
		let mut complete = true;
		let mut remaining_spans = MAX_TRACE_SPANS;
		let mut response_bytes = 3usize;
		let mut matched_records = Vec::new();
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
					let (spans, terms, truncated) = self.trace_value(&plan, value, record_span_budget, deadline)?;
					record_truncated |= truncated;
					found.extend(terms);
					if spans.is_empty() {
						continue;
					}
					record_span_budget = record_span_budget.saturating_sub(spans.len());
					pending_values.push((field.as_str(), value_index as u32, spans));
				}
			}
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

	fn trace_plan(&self, request: &SearchRequest) -> Result<TracePlan> {
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
					completed: self.analyze(completed, false, true)?,
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
	) -> Result<(Vec<TraceSpan>, HashSet<String>, bool)> {
		let analyzed = self.source_tokens(value, false, deadline)?;
		let surface = self.source_tokens(value, true, deadline)?;
		let utf16_offsets = utf16_offsets(value);
		let mut spans = Vec::new();
		let mut found = HashSet::new();
		let mut truncated = false;
		match plan {
			TracePlan::Any(terms) | TracePlan::All(terms) => {
				for (index, token) in analyzed.iter().enumerate() {
					if index % 256 == 0 {
						check_deadline(deadline)?;
					}
					if terms.contains(&token.text) {
						found.insert(token.text.clone());
						truncated |= push_trace_span(
							&mut spans,
							source_span(&utf16_offsets, token.start, token.end),
							max_spans,
						);
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
							let Some(token) = analyzed.get(source_index) else {
								matches = false;
								break;
							};
							if token.position != expected_position || token.text != *term {
								matches = false;
								break;
							}
							final_token = token;
							source_index += 1;
						}
						if matches {
							found.insert("__phrase".to_owned());
							truncated |= push_trace_span(
								&mut spans,
								source_span(&utf16_offsets, anchor.start, final_token.end),
								max_spans,
							);
						}
					}
				}
			}
			TracePlan::Prefix {
				completed,
				prefix,
				fuzzy,
			} => {
				for (index, token) in analyzed.iter().enumerate() {
					if index % 256 == 0 {
						check_deadline(deadline)?;
					}
					if completed.contains(&token.text) {
						found.insert(token.text.clone());
						truncated |= push_trace_span(
							&mut spans,
							source_span(&utf16_offsets, token.start, token.end),
							max_spans,
						);
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
						truncated |= push_trace_span(
							&mut spans,
							source_span(&utf16_offsets, token.start, token.end),
							max_spans,
						);
					}
				}
			}
			TracePlan::Fuzzy(terms) => {
				let analyzed_by_position = analyzed
					.iter()
					.map(|token| (token.position, token.text.as_str()))
					.collect::<HashMap<_, _>>();
				for (index, token) in surface.iter().enumerate() {
					if index % 256 == 0 {
						check_deadline(deadline)?;
					}
					for (analyzed_term, surface_term) in terms {
						if analyzed_by_position.get(&token.position) == Some(&analyzed_term.as_str())
							|| (fuzzy_eligible(surface_term) && within_one_edit(surface_term, &token.text))
						{
							found.insert(analyzed_term.clone());
							truncated |= push_trace_span(
								&mut spans,
								source_span(&utf16_offsets, token.start, token.end),
								max_spans,
							);
							break;
						}
					}
				}
			}
		}
		spans.sort_by_key(|span| (span.start, span.end));
		spans.dedup();
		Ok((spans, found, truncated))
	}

	fn source_tokens(&self, text: &str, surface: bool, deadline: Option<Instant>) -> Result<Vec<SourceToken>> {
		let mut analyzer = if surface {
			self.surface_analyzer.clone()
		} else {
			self.analyzer.clone()
		};
		let mut tokens = Vec::new();
		let mut stream = analyzer.token_stream(text);
		while stream.advance() {
			if tokens.len() % 256 == 0 {
				check_deadline(deadline)?;
			}
			let token = stream.token();
			tokens.push(SourceToken {
				text: token.text.clone(),
				start: token.offset_from,
				end: token.offset_to,
				position: token.position,
			});
		}
		Ok(tokens)
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

	fn query(&self, searcher: &Searcher, request: &SearchRequest, fields: &[&EngineField]) -> Result<Box<dyn Query>> {
		let query = match request.mode {
			SearchMode::Any => self.term_query(&request.text, fields, Occur::Should)?,
			SearchMode::All => self.term_query(&request.text, fields, Occur::Must)?,
			SearchMode::Phrase => self.phrase_query(&request.text, fields)?,
			SearchMode::Prefix => self.prefix_query(searcher, &request.text, fields, false)?,
			SearchMode::Fuzzy => self.fuzzy_query(&request.text, fields)?,
			SearchMode::FuzzyPrefix => self.prefix_query(searcher, &request.text, fields, true)?,
		};
		self.with_candidates(query, request.candidate_ids.as_deref())
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

	fn term_query(&self, text: &str, fields: &[&EngineField], occur: Occur) -> Result<Box<dyn Query>> {
		let terms = self.analyze(text, false, true)?;
		if terms.is_empty() {
			return Ok(Box::new(EmptyQuery));
		}
		self.check_clause_count(terms.len(), fields.len())?;
		Ok(Box::new(BooleanQuery::new(
			terms
				.into_iter()
				.map(|term| (occur, self.term_group(&term, fields)))
				.collect(),
		)))
	}

	fn phrase_query(&self, text: &str, fields: &[&EngineField]) -> Result<Box<dyn Query>> {
		if !self.positions {
			return Err(FulltextError::invalid("phrase search requires positions to be enabled"));
		}
		let terms = self.analyze_positioned(text)?;
		if terms.is_empty() {
			return Ok(Box::new(EmptyQuery));
		}
		if terms.len() == 1 {
			return Ok(self.term_group(&terms[0].1, fields));
		}
		self.check_clause_count(1, fields.len())?;
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
		Ok(Box::new(BooleanQuery::new(alternatives)))
	}

	fn fuzzy_query(&self, text: &str, fields: &[&EngineField]) -> Result<Box<dyn Query>> {
		let surface = self.analyze(text, true, true)?;
		let mut pairs = Vec::new();
		for surface_term in surface {
			let analyzed = self.analyze(&surface_term, false, false)?;
			if let [analyzed_term] = analyzed.as_slice() {
				pairs.push((analyzed_term.clone(), surface_term));
			}
		}
		if pairs.is_empty() {
			return Ok(Box::new(EmptyQuery));
		}
		let fuzzy_terms = pairs.iter().filter(|(_, term)| fuzzy_eligible(term)).count();
		if fuzzy_terms > MAX_FUZZY_TERMS {
			return Err(FulltextError::invalid(format!(
				"fuzzy search contains more than {MAX_FUZZY_TERMS} eligible terms"
			)));
		}
		self.check_clause_count(pairs.len(), fields.len().saturating_mul(3))?;
		let exact_bonus = fields.iter().map(|field| field.weight).fold(0.0f32, f32::max) * 0.25 + 1.0;
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
		Ok(Box::new(BooleanQuery::new(clauses)))
	}

	fn prefix_query(
		&self,
		searcher: &Searcher,
		text: &str,
		fields: &[&EngineField],
		fuzzy: bool,
	) -> Result<Box<dyn Query>> {
		self.require_surface_fields(fields)?;
		if text.chars().last().is_some_and(char::is_whitespace) {
			return self.term_query(text, fields, Occur::Must);
		}
		let (completed_text, surface_prefix) = self.final_surface_term(text)?;
		let Some(surface_prefix) = surface_prefix else {
			return Ok(Box::new(EmptyQuery));
		};
		let minimum = if fuzzy { 4 } else { 3 };
		if surface_prefix.chars().count() < minimum {
			return Err(FulltextError::invalid(format!(
				"{} prefix must contain at least {minimum} Unicode characters",
				if fuzzy { "fuzzy" } else { "exact" }
			)));
		}
		let completed = self.analyze(completed_text, false, true)?;
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
		Ok(Box::new(BooleanQuery::new(clauses)))
	}

	fn final_surface_term<'a>(&self, text: &'a str) -> Result<(&'a str, Option<String>)> {
		let mut tokenizer = SimpleTokenizer::default();
		let mut stream = tokenizer.token_stream(text);
		let mut final_term = None;
		let mut final_offset = 0;
		while stream.advance() {
			let token = stream.token();
			final_term = Some(token.text.to_lowercase());
			final_offset = token.offset_from;
		}
		if final_term.as_ref().is_some_and(|term| term.len() >= 40) {
			return Err(FulltextError::invalid(
				"the final prefix token must be shorter than 40 UTF-8 bytes",
			));
		}
		Ok((&text[..final_offset], final_term))
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

fn search_hit_ids(searcher: &Searcher, scored_docs: &[(f32, DocAddress)]) -> Result<Vec<String>> {
	let mut hits_by_segment = HashMap::new();
	for (index, (_, address)) in scored_docs.iter().enumerate() {
		hits_by_segment
			.entry(address.segment_ord)
			.or_insert_with(Vec::new)
			.push((index, address.doc_id));
	}
	let mut ids = vec![String::new(); scored_docs.len()];
	for (segment_ord, segment_hits) in hits_by_segment {
		let segment = &searcher.segment_readers()[segment_ord as usize];
		let column = segment
			.fast_fields()
			.str(ID_FIELD_NAME)
			.map_err(index_error)?
			.ok_or_else(|| FulltextError::new("E_NATIVE_FAILURE", "search segment has no ID fast field"))?;
		let mut id = Vec::new();
		for (index, doc_id) in segment_hits {
			let ordinal = column
				.term_ords(doc_id)
				.next()
				.ok_or_else(|| FulltextError::new("E_NATIVE_FAILURE", "search hit has no ID ordinal"))?;
			id.clear();
			if !column
				.dictionary()
				.ord_to_term(ordinal, &mut id)
				.map_err(storage_error)?
			{
				return Err(FulltextError::new(
					"E_NATIVE_FAILURE",
					"search hit ID ordinal is missing",
				));
			}
			let id = std::str::from_utf8(&id)
				.map_err(|_| FulltextError::new("E_NATIVE_FAILURE", "search hit ID is not UTF-8"))?
				.to_owned();
			ids[index] = id;
		}
	}
	Ok(ids)
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

fn source_span(utf16_offsets: &Option<Vec<u32>>, start: usize, end: usize) -> TraceSpan {
	TraceSpan {
		start: utf16_offsets.as_ref().map_or(start as u32, |offsets| offsets[start]),
		end: utf16_offsets.as_ref().map_or(end as u32, |offsets| offsets[end]),
	}
}

fn push_trace_span(spans: &mut Vec<TraceSpan>, span: TraceSpan, max_spans: usize) -> bool {
	if spans.len() < max_spans {
		spans.push(span);
		false
	} else {
		true
	}
}

fn within_one_edit(left: &str, right: &str) -> bool {
	let left = left.chars().collect::<Vec<_>>();
	let right = right.chars().collect::<Vec<_>>();
	if left.len().abs_diff(right.len()) > 1 {
		return false;
	}
	if left == right {
		return true;
	}
	if left.len() == right.len() {
		let differences = left
			.iter()
			.zip(&right)
			.enumerate()
			.filter_map(|(index, (left, right))| (left != right).then_some(index))
			.collect::<Vec<_>>();
		return differences.len() == 1
			|| (differences.len() == 2
				&& differences[1] == differences[0] + 1
				&& left[differences[0]] == right[differences[1]]
				&& left[differences[1]] == right[differences[0]]);
	}
	let (shorter, longer) = if left.len() < right.len() {
		(&left, &right)
	} else {
		(&right, &left)
	};
	let mut short = 0;
	let mut long = 0;
	let mut edits = 0;
	while short < shorter.len() && long < longer.len() {
		if shorter[short] == longer[long] {
			short += 1;
		} else {
			edits += 1;
			if edits > 1 {
				return false;
			}
		}
		long += 1;
	}
	true
}

fn fuzzy_prefix_matches(prefix: &str, candidate: &str) -> bool {
	let prefix_length = prefix.chars().count();
	(prefix_length.saturating_sub(1)..=prefix_length.saturating_add(1)).any(|length| {
		let candidate_prefix = candidate.chars().take(length).collect::<String>();
		within_one_edit(prefix, &candidate_prefix)
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

fn build_schema(config: &EngineIdentityConfig) -> Result<(Schema, Field, Vec<EngineField>)> {
	let mut builder = Schema::builder();
	let id_indexing = TextFieldIndexing::default()
		.set_tokenizer("raw")
		.set_index_option(IndexRecordOption::Basic)
		.set_fieldnorms(false);
	let id_options = TextOptions::default().set_indexing_options(id_indexing).set_fast(None);
	let id_field = builder.add_text_field(ID_FIELD_NAME, id_options);
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
	Ok((builder.build(), id_field, fields))
}

fn build_analyzer(stop_words: bool) -> Result<TextAnalyzer> {
	let mut builder = TextAnalyzer::builder(SimpleTokenizer::default())
		.filter_dynamic(RemoveLongFilter::limit(40))
		.filter_dynamic(LowerCaser);
	if stop_words {
		let stop_filter = StopWordFilter::new(Language::English)
			.ok_or_else(|| FulltextError::new("E_NATIVE_FAILURE", "English stop words are unavailable"))?;
		builder = builder.filter_dynamic(stop_filter);
	}
	Ok(builder.filter_dynamic(Stemmer::new(Language::English)).build())
}

fn build_surface_analyzer() -> TextAnalyzer {
	TextAnalyzer::builder(SimpleTokenizer::default())
		.filter_dynamic(RemoveLongFilter::limit(40))
		.filter_dynamic(LowerCaser)
		.build()
}

fn identity_bytes(config: &EngineIdentityConfig) -> Vec<u8> {
	let mut bytes = b"HTFI\x02\x00".to_vec();
	push_string(&mut bytes, &config.index_id);
	push_string(&mut bytes, &config.generation);
	push_string(&mut bytes, &config.analyzer);
	bytes.extend_from_slice(&[
		config.stop_words as u8,
		config.positions as u8,
		config.surface_terms as u8,
	]);
	bytes.extend_from_slice(&(config.fields.len() as u16).to_le_bytes());
	for field in &config.fields {
		push_string(&mut bytes, &field.name);
	}
	bytes
}

pub(crate) fn persisted_index_id(bytes: &[u8]) -> Option<&str> {
	let version = bytes.get(4..6)?;
	if bytes.get(..4)? != b"HTFI" || !matches!(version, b"\x01\x00" | b"\x02\x00") {
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

fn recovery_index_error(error: tantivy::TantivyError) -> FulltextError {
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

	fn batch() -> MutationBatch {
		MutationBatch {
			upserts: vec![
				crate::protocol::Upsert {
					id: "one".to_owned(),
					fields: vec![("title".to_owned(), vec!["Running Shoes".to_owned()])],
				},
				crate::protocol::Upsert {
					id: "two".to_owned(),
					fields: vec![("description".to_owned(), vec!["shoe rack".to_owned()])],
				},
			],
			deletes: Vec::new(),
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
			text: "shoes".to_owned(),
			mode: SearchMode::Any,
			fields: Vec::new(),
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
						fields: vec![("title".to_owned(), vec!["Waterproof Trail Running Shoes".to_owned()])],
					},
					crate::protocol::Upsert {
						id: "two".to_owned(),
						fields: vec![("title".to_owned(), vec!["Waterproof Road Shoes".to_owned()])],
					},
					crate::protocol::Upsert {
						id: "three".to_owned(),
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
						text: text.to_owned(),
						mode,
						fields: Vec::new(),
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
		assert!(search("waterproof", SearchMode::Any, Some(Vec::new())).hits.is_empty());
		assert!(search("the", SearchMode::Any, None).hits.is_empty());
		writer.close().unwrap();
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
		for id in ["z", "a"] {
			writer
				.apply(MutationBatch {
					upserts: vec![crate::protocol::Upsert {
						id: id.to_owned(),
						fields: vec![("title".to_owned(), vec!["identical catalog text".to_owned()])],
					}],
					deletes: Vec::new(),
				})
				.unwrap();
			writer.commit().unwrap();
		}
		let reader = engine.reader().unwrap();
		let page = |offset| {
			engine
				.search(
					&reader.searcher(),
					&SearchRequest {
						text: "identical".to_owned(),
						mode: SearchMode::Any,
						fields: Vec::new(),
						candidate_ids: None,
						offset,
						limit: 1,
						exact_total: false,
						budget_milliseconds: 30_000,
					},
				)
				.unwrap()
				.hits[0]
				.id
				.clone()
		};
		assert_eq!(page(0), "a");
		assert_eq!(page(1), "z");
		writer.close().unwrap();
		let reopened = Engine::open(directory, &config).unwrap();
		let result = reopened
			.search(
				&reopened.reader().unwrap().searcher(),
				&SearchRequest {
					text: "identical".to_owned(),
					mode: SearchMode::Any,
					fields: Vec::new(),
					candidate_ids: None,
					offset: 0,
					limit: 2,
					exact_total: false,
					budget_milliseconds: 30_000,
				},
			)
			.unwrap();
		assert_eq!(
			result.hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
			["a", "z"]
		);
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
					text: "cat".to_owned(),
					mode: SearchMode::Prefix,
					fields: Vec::new(),
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
		let (schema, _, _) = build_schema(&config.identity).unwrap();
		Index::create(directory.clone(), schema, IndexSettings::default()).unwrap();
		let error = match Engine::open(directory, &config) {
			Ok(_) => panic!("meta without an identity sidecar was accepted"),
			Err(error) => error,
		};
		assert_eq!(error.code, "E_INCOMPLETE_CREATE");
	}
}
