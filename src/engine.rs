use std::collections::{HashMap, HashSet};
use std::path::Path;

use tantivy::collector::{Count, TopDocs};
use tantivy::directory::Directory;
use tantivy::query::{BooleanQuery, BoostQuery, Occur, Query, TermQuery};
use tantivy::schema::{Field, IndexRecordOption, Schema, TantivyDocument, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{
	Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer,
};
use tantivy::{Index, IndexReader, IndexSettings, IndexWriter, ReloadPolicy, Searcher, Term};

use crate::error::{FulltextError, Result};
use crate::protocol::{EngineConfig, MutationBatch, SearchOperator, SearchRequest};

const ID_FIELD_NAME: &str = "__fulltext_id";
const IDENTITY_PATH: &str = ".harper-fulltext-identity";
const META_PATH: &str = "meta.json";
const ANALYZER_NAME: &str = "english@1";
pub const MAX_COMMIT_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub struct Engine {
	index: Index,
	id_field: Field,
	fields: Vec<EngineField>,
	field_lookup: HashMap<String, usize>,
	analyzer: TextAnalyzer,
}

#[derive(Clone)]
struct EngineField {
	name: String,
	field: Field,
	weight: f32,
}

pub struct Writer {
	inner: IndexWriter,
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

impl Engine {
	pub fn open<D: Directory + Clone>(directory: D, config: &EngineConfig) -> Result<Self> {
		let (schema, id_field, fields) = build_schema(config)?;
		let expected_identity = identity_bytes(config);
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
			Index::open(directory).map_err(index_error)?
		} else {
			Index::create(directory, schema.clone(), IndexSettings::default()).map_err(index_error)?
		};
		if index.schema() != schema {
			return Err(FulltextError::new(
				"E_SCHEMA_MISMATCH",
				"the persisted Tantivy schema does not match the requested configuration",
			));
		}
		let analyzer = build_analyzer(config.stop_words)?;
		index.tokenizers().register(ANALYZER_NAME, analyzer.clone());
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
		})
	}

	pub fn writer(&self, config: &EngineConfig) -> Result<Writer> {
		let inner = self
			.index
			.writer_with_num_threads(config.limits.indexing_threads, config.limits.writer_memory_bytes)
			.map_err(index_error)?;
		Ok(Writer {
			inner,
			id_field: self.id_field,
			fields: self.fields.clone(),
			field_lookup: self.field_lookup.clone(),
		})
	}

	pub fn reader(&self) -> Result<IndexReader> {
		self.index
			.reader_builder()
			.reload_policy(ReloadPolicy::Manual)
			.try_into()
			.map_err(index_error)
	}

	pub fn committed_payload(&self) -> Result<Option<String>> {
		Ok(self.index.load_metas().map_err(index_error)?.payload)
	}

	pub fn search(&self, searcher: &Searcher, request: &SearchRequest) -> Result<SearchResult> {
		let selected = self.selected_fields(&request.fields)?;
		let query = self.query(&request.text, request.operator, &selected)?;
		let top_docs = searcher
			.search(
				query.as_ref(),
				&TopDocs::with_limit(request.limit)
					.and_offset(request.offset)
					.order_by_score(),
			)
			.map_err(index_error)?;
		let mut hits_by_segment = HashMap::new();
		for (index, (_, address)) in top_docs.iter().enumerate() {
			hits_by_segment
				.entry(address.segment_ord)
				.or_insert_with(Vec::new)
				.push((index, address.doc_id));
		}
		let mut ids = vec![String::new(); top_docs.len()];
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
				ids[index] = std::str::from_utf8(&id)
					.map_err(|_| FulltextError::new("E_NATIVE_FAILURE", "search hit ID is not UTF-8"))?
					.to_owned();
			}
		}
		let hits = top_docs
			.into_iter()
			.zip(ids)
			.map(|((score, _), id)| SearchHit { id, score })
			.collect::<Vec<_>>();
		let (total, total_relation) = if request.exact_total {
			(
				searcher.search(query.as_ref(), &Count).map_err(index_error)? as u64,
				TotalRelation::Exact,
			)
		} else if request.offset == 0 && hits.len() < request.limit {
			(hits.len() as u64, TotalRelation::Exact)
		} else if !hits.is_empty() && hits.len() < request.limit {
			((request.offset + hits.len()) as u64, TotalRelation::Exact)
		} else {
			(
				if hits.is_empty() {
					0
				} else {
					(request.offset + hits.len()) as u64
				},
				TotalRelation::LowerBound,
			)
		};
		Ok(SearchResult {
			total,
			total_relation,
			hits,
		})
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

	fn query(&self, text: &str, operator: SearchOperator, fields: &[&EngineField]) -> Result<Box<dyn Query>> {
		let mut analyzer = self.analyzer.clone();
		let mut stream = analyzer.token_stream(text);
		let mut tokens = Vec::new();
		stream.process(&mut |token| tokens.push(token.text.clone()));
		if tokens.is_empty() {
			return Err(FulltextError::invalid("search text produced no searchable terms"));
		}
		let outer_occur = match operator {
			SearchOperator::Any => Occur::Should,
			SearchOperator::All => Occur::Must,
		};
		let mut terms = Vec::with_capacity(tokens.len());
		for token in tokens {
			let mut alternatives: Vec<(Occur, Box<dyn Query>)> = Vec::with_capacity(fields.len());
			for field in fields {
				let query: Box<dyn Query> = Box::new(TermQuery::new(
					Term::from_field_text(field.field, &token),
					IndexRecordOption::WithFreqs,
				));
				let query = if field.weight == 1.0 {
					query
				} else {
					Box::new(BoostQuery::new(query, field.weight))
				};
				alternatives.push((Occur::Should, query));
			}
			terms.push((outer_occur, Box::new(BooleanQuery::new(alternatives)) as Box<dyn Query>));
		}
		Ok(Box::new(BooleanQuery::new(terms)))
	}
}

impl Writer {
	pub fn apply(&self, batch: MutationBatch) -> Result<u64> {
		let prepared = self.prepare(batch)?;
		self.apply_prepared(prepared)
	}

	pub(crate) fn prepare(&self, batch: MutationBatch) -> Result<PreparedBatch> {
		let mutation_count = batch.upserts.len() + batch.deletes.len();
		for id in &batch.deletes {
			if id.is_empty() {
				return Err(FulltextError::invalid("delete ID must not be empty"));
			}
		}
		let mut documents = Vec::with_capacity(batch.upserts.len());
		for upsert in batch.upserts {
			if upsert.id.is_empty() {
				return Err(FulltextError::invalid("upsert ID must not be empty"));
			}
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
					document.add_text(self.fields[*index].field, &value);
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
		if payload.is_some_and(|payload| payload.len() > MAX_COMMIT_PAYLOAD_BYTES) {
			return Err(FulltextError::invalid(format!(
				"commit payload exceeds {MAX_COMMIT_PAYLOAD_BYTES} UTF-8 bytes"
			)));
		}
		let mut commit = self.inner.prepare_commit().map_err(index_error)?;
		if let Some(payload) = payload {
			commit.set_payload(payload);
		}
		commit.commit().map_err(index_error)
	}

	pub fn rollback(&mut self) -> Result<u64> {
		self.inner.rollback().map_err(index_error)
	}

	pub fn close(self) -> Result<()> {
		self.inner.wait_merging_threads().map_err(index_error)
	}
}

fn build_schema(config: &EngineConfig) -> Result<(Schema, Field, Vec<EngineField>)> {
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
		let mut options = TextOptions::default().set_indexing_options(indexing);
		if config.surface_terms {
			options = options.set_stored();
		}
		let schema_field = builder.add_text_field(&field.name, options);
		fields.push(EngineField {
			name: field.name.clone(),
			field: schema_field,
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

fn identity_bytes(config: &EngineConfig) -> Vec<u8> {
	let mut bytes = b"HTFI\x01\x00".to_vec();
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::protocol::{FieldConfig, Limits};
	use tantivy::directory::RamDirectory;

	fn config() -> EngineConfig {
		EngineConfig {
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
		assert_eq!(writer.apply(batch()).unwrap(), 2);
		writer.commit().unwrap();
		let reader = engine.reader().unwrap();
		reader.reload().unwrap();
		let request = SearchRequest {
			text: "shoes".to_owned(),
			operator: SearchOperator::Any,
			fields: Vec::new(),
			offset: 0,
			limit: 10,
			exact_total: true,
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
	}

	#[test]
	fn rejects_identity_mismatch() {
		let directory = RamDirectory::create();
		let config = config();
		Engine::open(directory.clone(), &config).unwrap();
		let mut different = config;
		different.generation = "two".to_owned();
		let error = match Engine::open(directory, &different) {
			Ok(_) => panic!("identity mismatch was accepted"),
			Err(error) => error,
		};
		assert_eq!(error.code, "E_IDENTITY_MISMATCH");
	}

	#[test]
	fn same_engine_runs_on_the_kv_directory() {
		let directory = crate::phase0::FaultingDirectory::new(crate::phase0::FaultingKv::default());
		let config = config();
		let engine = Engine::open(directory.clone(), &config).unwrap();
		let mut writer = engine.writer(&config).unwrap();
		writer.apply(batch()).unwrap();
		writer.commit().unwrap();
		let reader = engine.reader().unwrap();
		let request = SearchRequest {
			text: "running shoes".to_owned(),
			operator: SearchOperator::All,
			fields: Vec::new(),
			offset: 0,
			limit: 10,
			exact_total: true,
		};
		assert_eq!(engine.search(&reader.searcher(), &request).unwrap().hits[0].id, "one");
		writer.close().unwrap();
		let reopened = Engine::open(directory, &config).unwrap();
		let reopened_reader = reopened.reader().unwrap();
		assert_eq!(
			reopened.search(&reopened_reader.searcher(), &request).unwrap().hits[0].id,
			"one"
		);
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
			.atomic_write(Path::new(IDENTITY_PATH), &identity_bytes(&config))
			.unwrap();
		Engine::open(directory.clone(), &config).unwrap();
		assert!(directory.exists(Path::new(META_PATH)).unwrap());
	}

	#[test]
	fn rejects_meta_without_an_identity_sidecar() {
		let directory = RamDirectory::create();
		let config = config();
		let (schema, _, _) = build_schema(&config).unwrap();
		Index::create(directory.clone(), schema, IndexSettings::default()).unwrap();
		let error = match Engine::open(directory, &config) {
			Ok(_) => panic!("meta without an identity sidecar was accepted"),
			Err(error) => error,
		};
		assert_eq!(error.code, "E_INCOMPLETE_CREATE");
	}
}
