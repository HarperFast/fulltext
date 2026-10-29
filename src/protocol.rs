use crate::catalog::{
	AggregationKind, AggregationRequest, MissingOrder, SortDirection, SortRequest, MAX_AGGREGATION_DEPTH,
};
use crate::error::{FulltextError, Result};

pub const PROTOCOL_VERSION: u16 = 7;
const MAX_STRING_BYTES: usize = 1 << 20;
pub const MAX_RECORD_ID_BYTES: usize = 4 << 10;
pub const MAX_RECORD_VERSION_BYTES: usize = 4 << 10;
const MAX_FIELDS: usize = 1_024;
pub const MAX_SYNONYM_RULES: usize = 1_024;
pub const MAX_SYNONYM_REPLACEMENTS: usize = 16;
pub const MAX_SYNONYM_BYTES: usize = 1 << 20;
const MUTATION_BATCH_HEADER_BYTES: usize = 14;
const MIN_MUTATION_BATCH_BYTES: usize = MUTATION_BATCH_HEADER_BYTES + 10;

#[derive(Clone, Debug, PartialEq)]
pub struct FieldConfig {
	pub name: String,
	pub weight: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterFieldType {
	String,
	Number,
	Boolean,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterFieldConfig {
	pub name: String,
	pub field_type: FilterFieldType,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Limits {
	pub max_query_terms: usize,
	pub max_query_clauses: usize,
	pub indexing_threads: usize,
	pub search_threads: usize,
	pub writer_memory_bytes: usize,
	pub max_queued_commands: usize,
	pub max_queued_bytes: usize,
	pub max_batch_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeBudgetLimits {
	pub max_resident_indexes: usize,
	pub max_indexing_threads: usize,
	pub max_search_threads: usize,
	pub max_writer_memory_bytes: usize,
	pub max_queued_bytes: usize,
	pub max_expensive_searches: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SynonymRule {
	pub source: String,
	pub replacements: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EngineIdentityConfig {
	pub index_id: String,
	pub generation: String,
	pub fields: Vec<FieldConfig>,
	pub analyzer: String,
	pub stop_words: bool,
	pub positions: bool,
	pub surface_terms: bool,
	pub synonyms: Vec<SynonymRule>,
	pub filter_fields: Vec<FilterFieldConfig>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EngineConfig {
	pub identity: EngineIdentityConfig,
	pub limits: Limits,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NativeOpenConfig {
	pub path: String,
	pub engine: EngineConfig,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NativeInspectConfig {
	pub path: String,
	pub identity: EngineIdentityConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeResetConfig {
	pub path: String,
	pub index_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FilterValue {
	String(String),
	Number(f64),
	Boolean(bool),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterComparator {
	Equals,
	In,
	Lt,
	Le,
	Gt,
	Ge,
	Between,
	Exists,
	Missing,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FilterExpression {
	Clause {
		field: String,
		comparator: FilterComparator,
		values: Vec<FilterValue>,
	},
	And(Vec<FilterExpression>),
	Or(Vec<FilterExpression>),
	Not(Box<FilterExpression>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Upsert {
	pub id: String,
	pub version: Option<String>,
	pub fields: Vec<(String, Vec<String>)>,
	pub filters: Vec<(String, Vec<FilterValue>)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MutationBatch {
	pub upserts: Vec<Upsert>,
	pub deletes: Vec<String>,
}

pub const MAX_QUERY_TEXT_BYTES: usize = 64 * 1024;
pub const MAX_QUERY_TERMS: usize = 64;
pub const MAX_QUERY_CLAUSES: usize = 256;
pub const MAX_CONFIGURED_QUERY_TERMS: usize = 1_024;
pub const MAX_CONFIGURED_QUERY_CLAUSES: usize = 4_096;

pub fn validate_query_limits(terms: usize, clauses: usize) -> Result<()> {
	if terms == 0 || terms > MAX_CONFIGURED_QUERY_TERMS {
		return Err(FulltextError::invalid(format!(
			"maxQueryTerms must be between 1 and {MAX_CONFIGURED_QUERY_TERMS}"
		)));
	}
	if clauses == 0 || clauses > MAX_CONFIGURED_QUERY_CLAUSES {
		return Err(FulltextError::invalid(format!(
			"maxQueryClauses must be between 1 and {MAX_CONFIGURED_QUERY_CLAUSES}"
		)));
	}
	Ok(())
}
pub const MAX_EXPRESSION_DEPTH: usize = 8;

pub(crate) struct SearchStructureBudget {
	limit: usize,
	remaining: usize,
	text_bytes: usize,
	fields: usize,
}

impl SearchStructureBudget {
	pub(crate) fn new(limit: usize, fields: usize) -> Result<Self> {
		validate_query_limits(MAX_QUERY_TERMS, limit)?;
		Ok(Self {
			limit,
			remaining: limit,
			text_bytes: 0,
			fields,
		})
	}

	pub(crate) fn depth(&self, depth: usize) -> Result<()> {
		if depth > MAX_EXPRESSION_DEPTH {
			return Err(FulltextError::invalid(format!(
				"search expression nesting exceeds {MAX_EXPRESSION_DEPTH} levels"
			)));
		}
		Ok(())
	}

	pub(crate) fn charge(&mut self, cost: usize) -> Result<()> {
		self.remaining = self.remaining.checked_sub(cost).ok_or_else(|| {
			FulltextError::invalid(format!("search structure exceeds {} compiled clauses", self.limit))
		})?;
		Ok(())
	}

	pub(crate) fn children(&mut self, count: usize) -> Result<()> {
		if count == 0 {
			return Err(FulltextError::invalid(
				"boolean search expressions require at least one child",
			));
		}
		self.charge(count)
	}

	pub(crate) fn leaf(&mut self, text_bytes: usize, fields: usize) -> Result<()> {
		self.text_bytes = self.text_bytes.saturating_add(text_bytes);
		if self.text_bytes > MAX_QUERY_TEXT_BYTES {
			return Err(FulltextError::invalid("search text exceeds 65536 UTF-8 bytes"));
		}
		if fields > self.fields {
			return Err(FulltextError::invalid(format!(
				"search field count exceeds {} fields",
				self.fields
			)));
		}
		Ok(())
	}
}

pub(crate) fn validate_search_structure(expression: &SearchExpression, clauses: usize, fields: usize) -> Result<()> {
	fn visit(expression: &SearchExpression, depth: usize, budget: &mut SearchStructureBudget) -> Result<()> {
		budget.depth(depth)?;
		match expression {
			SearchExpression::Clause(clause) => budget.leaf(clause.text.len(), clause.fields.len()),
			SearchExpression::And(children) | SearchExpression::Or(children) => {
				budget.children(children.len())?;
				for child in children {
					visit(child, depth + 1, budget)?;
				}
				Ok(())
			}
			SearchExpression::Not(child) => {
				budget.charge(2)?;
				visit(child, depth + 1, budget)
			}
		}
	}
	visit(expression, 0, &mut SearchStructureBudget::new(clauses, fields)?)
}

pub const MAX_CANDIDATE_IDS: usize = 1_024;
pub const MAX_CANDIDATE_BYTES: usize = 1 << 20;
pub const MAX_PREFIX_EXPANSIONS: usize = 50;
pub const MAX_FUZZY_TERMS: usize = 16;
pub const MAX_SEARCH_WINDOW: usize = 10_000;
pub const MAX_SEARCH_PAGE_SIZE: usize = 10_000;
pub const MAX_AUTOCOMPLETE_RESULTS: usize = 100;
pub const MAX_SEARCH_REQUEST_BYTES: usize = 8 << 20;
pub const MAX_SEARCH_RESPONSE_BYTES: usize = 8 << 20;
pub const MAX_SEARCH_BUDGET_MILLISECONDS: u32 = 30_000;

pub(crate) fn validate_search_window(offset: usize, limit: usize, traversal: bool, has_catalog: bool) -> Result<()> {
	if limit == 0 {
		return Err(FulltextError::invalid("search limit must be greater than zero"));
	}
	if traversal {
		if offset != 0 || limit > MAX_SEARCH_PAGE_SIZE {
			return Err(FulltextError::invalid(format!(
				"searchPage requires offset zero and limit at most {MAX_SEARCH_PAGE_SIZE}; reduce page size, not total traversal results"
			)));
		}
	} else if offset.checked_add(limit).is_none_or(|end| end > MAX_SEARCH_WINDOW) {
		return Err(FulltextError::invalid(format!(
			"offset + limit must not exceed {MAX_SEARCH_WINDOW}; {}",
			if has_catalog {
				"metadata continuation is not supported"
			} else {
				"use searchPage for deeper results"
			}
		)));
	}
	Ok(())
}

pub(crate) fn validate_record_id(id: &str) -> Result<()> {
	if id.is_empty() {
		return Err(FulltextError::invalid("record IDs must not be empty"));
	}
	if id.len() > MAX_RECORD_ID_BYTES {
		return Err(FulltextError::invalid(format!(
			"record IDs must not exceed {MAX_RECORD_ID_BYTES} UTF-8 bytes"
		)));
	}
	Ok(())
}
pub const MAX_TRACE_RECORDS: usize = 100;
pub const MAX_TRACE_SOURCE_BYTES: usize = 1 << 20;
pub const MAX_TRACE_SPANS: usize = 1_024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchMode {
	Any,
	All,
	Phrase,
	Prefix,
	Fuzzy,
	FuzzyPrefix,
}

impl SearchMode {
	pub fn is_expensive(self) -> bool {
		matches!(self, Self::Phrase | Self::Prefix | Self::Fuzzy | Self::FuzzyPrefix)
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchClause {
	pub text: String,
	pub mode: SearchMode,
	pub fields: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SearchExpression {
	Clause(SearchClause),
	And(Vec<SearchExpression>),
	Or(Vec<SearchExpression>),
	Not(Box<SearchExpression>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchBoundary {
	pub snapshot: String,
	pub score: f32,
	pub id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchRequest {
	pub sort: Option<Vec<SortRequest>>,
	pub aggregations: Vec<AggregationRequest>,
	pub expression: SearchExpression,
	pub candidate_ids: Option<Vec<String>>,
	pub filter: Option<FilterExpression>,
	pub offset: usize,
	pub limit: usize,
	pub exact_total: bool,
	pub budget_milliseconds: u32,
	pub traversal: bool,
	pub after: Option<SearchBoundary>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceRecord {
	pub id: String,
	pub fields: Vec<(String, Vec<String>)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TraceRequest {
	pub search: SearchRequest,
	pub records: Vec<TraceRecord>,
}

pub fn decode_open(bytes: &[u8]) -> Result<NativeOpenConfig> {
	let mut cursor = Cursor::new(bytes, *b"FTOP")?;
	let path = validate_path(cursor.string()?)?;
	let engine = decode_engine_config(&mut cursor)?;
	cursor.finish()?;
	Ok(NativeOpenConfig { path, engine })
}

pub fn decode_runtime_budget(bytes: &[u8]) -> Result<RuntimeBudgetLimits> {
	let mut cursor = Cursor::new(bytes, *b"FTGC")?;
	let limits = RuntimeBudgetLimits {
		max_resident_indexes: cursor.u32()? as usize,
		max_indexing_threads: cursor.u32()? as usize,
		max_search_threads: cursor.u32()? as usize,
		max_writer_memory_bytes: cursor.u64_usize()?,
		max_queued_bytes: cursor.u64_usize()?,
		max_expensive_searches: cursor.u32()? as usize,
	};
	cursor.finish()?;
	if limits.max_resident_indexes == 0
		|| limits.max_indexing_threads == 0
		|| limits.max_search_threads == 0
		|| limits.max_writer_memory_bytes == 0
		|| limits.max_queued_bytes == 0
		|| limits.max_expensive_searches == 0
	{
		return Err(FulltextError::invalid(
			"runtime budget limits must be greater than zero",
		));
	}
	Ok(limits)
}

pub fn decode_inspect(bytes: &[u8]) -> Result<NativeInspectConfig> {
	let mut cursor = Cursor::new(bytes, *b"FTIP")?;
	let path = validate_path(cursor.string()?)?;
	let identity = decode_engine_identity_config(&mut cursor)?;
	cursor.finish()?;
	validate_identity_config(&identity)?;
	Ok(NativeInspectConfig { path, identity })
}

pub fn decode_reset(bytes: &[u8]) -> Result<NativeResetConfig> {
	let mut cursor = Cursor::new(bytes, *b"FTRX")?;
	let path = validate_path(cursor.string()?)?;
	let index_id = cursor.string()?;
	cursor.finish()?;
	if index_id.is_empty() {
		return Err(FulltextError::invalid("indexId must not be empty"));
	}
	Ok(NativeResetConfig { path, index_id })
}

fn validate_path(path: String) -> Result<String> {
	if path.is_empty() {
		return Err(FulltextError::invalid("path must not be empty"));
	}
	Ok(path)
}

fn decode_engine_config(cursor: &mut Cursor<'_>) -> Result<EngineConfig> {
	let identity = decode_engine_identity_config(cursor)?;
	let mut limits = Limits {
		max_query_terms: MAX_QUERY_TERMS,
		max_query_clauses: MAX_QUERY_CLAUSES,
		indexing_threads: cursor.u16()? as usize,
		search_threads: cursor.u16()? as usize,
		writer_memory_bytes: cursor.u64_usize()?,
		max_queued_commands: cursor.u32()? as usize,
		max_queued_bytes: cursor.u64_usize()?,
		max_batch_bytes: cursor.u64_usize()?,
	};
	if cursor.remaining() != 0 {
		limits.max_query_terms = cursor.u32()? as usize;
		limits.max_query_clauses = cursor.u32()? as usize;
	}
	validate_config(EngineConfig { identity, limits })
}

fn decode_engine_identity_config(cursor: &mut Cursor<'_>) -> Result<EngineIdentityConfig> {
	let index_id = cursor.string()?;
	let generation = cursor.string()?;
	let analyzer = cursor.string()?;
	let stop_words = cursor.boolean()?;
	let positions = cursor.boolean()?;
	let surface_terms = cursor.boolean()?;
	let synonym_count = cursor.u16()? as usize;
	if synonym_count > MAX_SYNONYM_RULES {
		return Err(FulltextError::invalid(format!(
			"synonyms must not contain more than {MAX_SYNONYM_RULES} rules"
		)));
	}
	let mut synonym_bytes_remaining = MAX_SYNONYM_BYTES;
	let mut synonyms = Vec::with_capacity(synonym_count);
	for _ in 0..synonym_count {
		let source = cursor.string_with_budget(&mut synonym_bytes_remaining, "encoded synonyms")?;
		let replacement_count = cursor.u16()? as usize;
		if replacement_count == 0 || replacement_count > MAX_SYNONYM_REPLACEMENTS {
			return Err(FulltextError::invalid(format!(
				"each synonym rule must contain between 1 and {MAX_SYNONYM_REPLACEMENTS} replacements"
			)));
		}
		let mut replacements = Vec::with_capacity(replacement_count);
		for _ in 0..replacement_count {
			replacements.push(cursor.string_with_budget(&mut synonym_bytes_remaining, "encoded synonyms")?);
		}
		synonyms.push(SynonymRule { source, replacements });
	}
	let field_count = cursor.u16()? as usize;
	if field_count == 0 || field_count > MAX_FIELDS {
		return Err(FulltextError::invalid("fields must contain between 1 and 1024 entries"));
	}
	let mut fields = Vec::with_capacity(field_count);
	for _ in 0..field_count {
		let name = cursor.string()?;
		let weight = cursor.f32()?;
		if !weight.is_finite() || weight <= 0.0 {
			return Err(FulltextError::invalid(
				"field weights must be finite and greater than zero",
			));
		}
		fields.push(FieldConfig { name, weight });
	}
	let filter_field_count = cursor.u16()? as usize;
	if filter_field_count > MAX_FIELDS {
		return Err(FulltextError::invalid(
			"filterFields must not contain more than 1024 entries",
		));
	}
	let mut filter_fields = Vec::with_capacity(filter_field_count);
	for _ in 0..filter_field_count {
		let name = cursor.string()?;
		let field_type = match cursor.u8()? {
			0 => FilterFieldType::String,
			1 => FilterFieldType::Number,
			2 => FilterFieldType::Boolean,
			_ => return Err(FulltextError::invalid("unknown filter field type")),
		};
		filter_fields.push(FilterFieldConfig { name, field_type });
	}
	Ok(EngineIdentityConfig {
		index_id,
		generation,
		fields,
		analyzer,
		stop_words,
		positions,
		surface_terms,
		synonyms,
		filter_fields,
	})
}

pub fn validate_batch_header(bytes: &[u8], max_batch_bytes: usize) -> Result<()> {
	if bytes.len() > max_batch_bytes {
		return Err(FulltextError::new(
			"E_BATCH_TOO_LARGE",
			format!("mutation batch is {} bytes; maximum is {max_batch_bytes}", bytes.len()),
		));
	}
	let mut cursor = Cursor::new(bytes, *b"FTMB")?;
	let _ = cursor.u32()?;
	let _ = cursor.u32()?;
	Ok(())
}

pub fn validate_search_header(bytes: &[u8]) -> Result<()> {
	if bytes.len() > MAX_SEARCH_REQUEST_BYTES {
		return Err(FulltextError::invalid("packed search request exceeds 8388608 bytes"));
	}
	let _ = Cursor::new(bytes, *b"FTSQ")?;
	Ok(())
}

pub fn validate_trace_header(bytes: &[u8]) -> Result<()> {
	if bytes.len() > MAX_SEARCH_REQUEST_BYTES {
		return Err(FulltextError::invalid("packed trace request exceeds 8388608 bytes"));
	}
	let _ = Cursor::new(bytes, *b"FTTM")?;
	Ok(())
}

pub fn search_budget(bytes: &[u8]) -> Result<u32> {
	packed_budget(bytes, *b"FTSQ", "search")
}

pub fn trace_budget(bytes: &[u8]) -> Result<u32> {
	packed_budget(bytes, *b"FTTM", "trace")
}

fn packed_budget(bytes: &[u8], magic: [u8; 4], operation: &str) -> Result<u32> {
	let _ = Cursor::new(bytes, magic)?;
	let budget_bytes = bytes
		.get(bytes.len().saturating_sub(4)..)
		.filter(|bytes| bytes.len() == 4)
		.ok_or_else(|| FulltextError::invalid("packed request is truncated"))?;
	let budget = u32::from_le_bytes(budget_bytes.try_into().unwrap());
	if budget == 0 || budget > MAX_SEARCH_BUDGET_MILLISECONDS {
		return Err(FulltextError::invalid(format!(
			"{operation} budget must be between 1 and {MAX_SEARCH_BUDGET_MILLISECONDS} milliseconds"
		)));
	}
	Ok(budget)
}

pub fn search_is_expensive(bytes: &[u8]) -> Result<bool> {
	search_is_expensive_for_fields(bytes, MAX_FIELDS, MAX_QUERY_TERMS, MAX_QUERY_CLAUSES)
}

pub fn search_is_expensive_for_fields(
	bytes: &[u8],
	schema_fields: usize,
	max_query_terms: usize,
	max_query_clauses: usize,
) -> Result<bool> {
	validate_query_limits(max_query_terms, max_query_clauses)?;
	let raised_profile_fields =
		(max_query_terms > MAX_QUERY_TERMS || max_query_clauses > MAX_QUERY_CLAUSES).then_some(schema_fields);
	let mut cursor = Cursor::new(bytes, *b"FTSQ")?;
	let mut structure = SearchStructureBudget::new(max_query_clauses, schema_fields)?;
	let cost = scan_search_expression(&mut cursor, 0, &mut structure, raised_profile_fields)?;
	let has_candidates = cursor.boolean()?;
	let candidate_count = if has_candidates { cursor.u16()? as usize } else { 0 };
	if candidate_count > MAX_CANDIDATE_IDS || candidate_count > cursor.remaining() / 4 {
		return Err(FulltextError::invalid("candidate ID count exceeds its packed request"));
	}
	for _ in 0..candidate_count {
		let _ = cursor.bytes()?;
	}
	let filter_is_expensive = if cursor.boolean()? {
		let mut filter_clauses = 0usize;
		scan_filter_expression(&mut cursor, 0, &mut filter_clauses)? || filter_clauses > 16
	} else {
		false
	};
	let catalog_is_expensive = scan_catalog(&mut cursor)?;
	let _ = cursor.u32()?;
	let _ = cursor.u32()?;
	let exact_total = cursor.boolean()?;
	let (traversal, _) = decode_search_traversal(&mut cursor)?;
	Ok(traversal
		|| catalog_is_expensive
		|| (raised_profile_fields.is_some()
			&& (cost.terms_upper > MAX_QUERY_TERMS || cost.construction_upper > MAX_QUERY_CLAUSES))
		|| cost.has_expensive_mode
		|| !cost.has_positive_anchor
		|| filter_is_expensive
		|| exact_total)
}

fn scan_filter_expression(cursor: &mut Cursor<'_>, depth: usize, clauses: &mut usize) -> Result<bool> {
	if depth > MAX_EXPRESSION_DEPTH {
		return Err(FulltextError::invalid(format!(
			"filter expression nesting exceeds {MAX_EXPRESSION_DEPTH} levels"
		)));
	}
	match cursor.u8()? {
		0 => {
			*clauses += 1;
			if *clauses > MAX_QUERY_CLAUSES {
				return Err(FulltextError::invalid("filter expression exceeds 256 clauses"));
			}
			let _ = cursor.bytes()?;
			let comparator = cursor.u8()?;
			if comparator > 8 {
				return Err(FulltextError::invalid("unknown filter comparator"));
			}
			let count = cursor.u16()? as usize;
			let expected = match comparator {
				1 => None,
				6 => Some(2),
				7 | 8 => Some(0),
				_ => Some(1),
			};

			if (count == 0 && comparator < 7)
				|| expected.is_some_and(|expected| expected != count)
				|| count > MAX_QUERY_CLAUSES
				|| count > cursor.remaining() / 2
			{
				return Err(FulltextError::invalid("invalid filter value count"));
			}
			for _ in 0..count {
				scan_filter_value(cursor)?;
			}
			Ok(comparator >= 2 || count > 16)
		}
		1 | 2 => {
			let count = cursor.u16()? as usize;
			if count == 0 || count > MAX_QUERY_CLAUSES {
				return Err(FulltextError::invalid(
					"filter boolean expression requires 1 to 256 children",
				));
			}
			let mut expensive = false;
			for _ in 0..count {
				expensive |= scan_filter_expression(cursor, depth + 1, clauses)?;
			}
			Ok(expensive)
		}
		3 => {
			scan_filter_expression(cursor, depth + 1, clauses)?;
			Ok(true)
		}
		_ => Err(FulltextError::invalid("unknown filter expression type")),
	}
}

pub(crate) fn ascii_query_term_upper(text: &[u8]) -> Option<usize> {
	let mut terms = 0;
	let mut in_term = false;
	for byte in text {
		if !byte.is_ascii() {
			return None;
		}
		let alphanumeric = byte.is_ascii_alphanumeric();
		terms += usize::from(alphanumeric && !in_term);
		in_term = alphanumeric;
	}
	Some(terms)
}

struct SearchCost {
	terms_upper: usize,
	construction_upper: usize,
	has_expensive_mode: bool,
	has_positive_anchor: bool,
}

fn scan_search_expression(
	cursor: &mut Cursor<'_>,
	depth: usize,
	structure: &mut SearchStructureBudget,
	raised_profile_fields: Option<usize>,
) -> Result<SearchCost> {
	structure.depth(depth)?;
	match cursor.u8()? {
		0 => {
			let text = cursor.bytes()?;
			let expensive = decode_search_mode(cursor.u8()?)?.is_expensive();
			let field_count = cursor.u16()? as usize;
			if field_count > MAX_FIELDS || field_count > cursor.remaining() / 4 {
				return Err(FulltextError::invalid("search field count exceeds its packed request"));
			}
			structure.leaf(text.len(), field_count)?;
			let terms_upper = if raised_profile_fields.is_some() {
				ascii_query_term_upper(text).unwrap_or(usize::MAX)
			} else {
				0
			};
			for _ in 0..field_count {
				let _ = cursor.bytes()?;
			}
			Ok(SearchCost {
				terms_upper,
				construction_upper: terms_upper.saturating_mul(if field_count == 0 {
					raised_profile_fields.unwrap_or(0)
				} else {
					field_count
				}),
				has_expensive_mode: expensive,
				has_positive_anchor: true,
			})
		}
		kind @ (1 | 2) => {
			let count = cursor.u16()? as usize;
			structure.children(count)?;
			let mut terms_upper = 0usize;
			let mut construction_upper = count;
			let mut has_expensive_mode = false;
			let mut has_positive_anchor = kind == 2;
			for _ in 0..count {
				let child = scan_search_expression(cursor, depth + 1, structure, raised_profile_fields)?;
				terms_upper = terms_upper.max(child.terms_upper);
				construction_upper = construction_upper.saturating_add(child.construction_upper);
				has_expensive_mode |= child.has_expensive_mode;
				if kind == 1 {
					has_positive_anchor |= child.has_positive_anchor;
				} else {
					has_positive_anchor &= child.has_positive_anchor;
				}
			}
			Ok(SearchCost {
				terms_upper,
				construction_upper,
				has_expensive_mode,
				has_positive_anchor,
			})
		}
		3 => {
			structure.charge(2)?;
			let child = scan_search_expression(cursor, depth + 1, structure, raised_profile_fields)?;
			Ok(SearchCost {
				terms_upper: child.terms_upper,
				construction_upper: child.construction_upper.saturating_add(2),
				has_expensive_mode: child.has_expensive_mode,
				has_positive_anchor: false,
			})
		}
		_ => Err(FulltextError::invalid("unknown search expression type")),
	}
}

pub fn decode_batch(bytes: &[u8]) -> Result<MutationBatch> {
	let mut cursor = Cursor::new(bytes, *b"FTMB")?;
	let upsert_count = cursor.u32()? as usize;
	let delete_count = cursor.u32()? as usize;
	let minimum_bytes = upsert_count
		.checked_mul(6)
		.and_then(|bytes| {
			delete_count
				.checked_mul(4)
				.and_then(|deletes| bytes.checked_add(deletes))
		})
		.ok_or_else(|| FulltextError::invalid("mutation count overflow"))?;
	if minimum_bytes > cursor.remaining() {
		return Err(FulltextError::invalid("mutation counts exceed the packed batch length"));
	}
	let mut upserts = Vec::with_capacity(upsert_count);
	for _ in 0..upsert_count {
		let id = cursor.record_id()?;
		let version = if cursor.boolean()? {
			let version = cursor.string()?;
			if version.len() > MAX_RECORD_VERSION_BYTES {
				return Err(FulltextError::invalid(
					"record versions must not exceed 4096 UTF-8 bytes",
				));
			}
			Some(version)
		} else {
			None
		};
		let field_count = cursor.u16()? as usize;
		if field_count > MAX_FIELDS {
			return Err(FulltextError::invalid("upsert field count exceeds 1024"));
		}
		if field_count > cursor.remaining() / 6 {
			return Err(FulltextError::invalid(
				"upsert field count exceeds the packed batch length",
			));
		}
		let mut fields = Vec::with_capacity(field_count);
		for _ in 0..field_count {
			let name = cursor.string()?;
			let value_count = cursor.u16()? as usize;
			if value_count > cursor.remaining() / 4 {
				return Err(FulltextError::invalid("value count exceeds the packed batch length"));
			}
			let mut values = Vec::with_capacity(value_count);
			for _ in 0..value_count {
				values.push(cursor.string()?);
			}
			fields.push((name, values));
		}
		let filter_count = cursor.u16()? as usize;
		if filter_count > MAX_FIELDS || filter_count > cursor.remaining() / 6 {
			return Err(FulltextError::invalid(
				"upsert filter count exceeds the packed batch length",
			));
		}
		let mut filters = Vec::with_capacity(filter_count);
		for _ in 0..filter_count {
			let name = cursor.string()?;
			let value_count = cursor.u16()? as usize;
			if value_count > cursor.remaining() / 2 {
				return Err(FulltextError::invalid(
					"filter value count exceeds the packed batch length",
				));
			}
			let mut values = Vec::with_capacity(value_count);
			for _ in 0..value_count {
				values.push(decode_filter_value(&mut cursor)?);
			}
			filters.push((name, values));
		}
		upserts.push(Upsert {
			id,
			version,
			fields,
			filters,
		});
	}
	if delete_count > cursor.remaining() / 4 {
		return Err(FulltextError::invalid("delete count exceeds the packed batch length"));
	}
	let mut deletes = Vec::with_capacity(delete_count);
	for _ in 0..delete_count {
		deletes.push(cursor.record_id()?);
	}
	cursor.finish()?;
	Ok(MutationBatch { upserts, deletes })
}

pub fn decode_search(bytes: &[u8]) -> Result<SearchRequest> {
	let mut cursor = Cursor::new(bytes, *b"FTSQ")?;
	let mut structure = SearchStructureBudget::new(MAX_CONFIGURED_QUERY_CLAUSES, MAX_FIELDS)?;
	let expression = decode_search_expression(&mut cursor, 0, &mut structure)?;
	let has_candidates = cursor.boolean()?;
	let candidate_count = if has_candidates { cursor.u16()? as usize } else { 0 };
	if candidate_count > MAX_CANDIDATE_IDS {
		return Err(FulltextError::invalid("candidate ID count exceeds 1024"));
	}
	if candidate_count > cursor.remaining() / 4 {
		return Err(FulltextError::invalid(
			"candidate ID count exceeds the packed request length",
		));
	}
	let mut candidate_bytes = 0usize;
	let mut candidate_ids = Vec::with_capacity(candidate_count);
	for _ in 0..candidate_count {
		let id = cursor.record_id()?;
		candidate_bytes = candidate_bytes
			.checked_add(id.len())
			.ok_or_else(|| FulltextError::invalid("candidate ID byte count overflow"))?;
		if candidate_bytes > MAX_CANDIDATE_BYTES {
			return Err(FulltextError::invalid("candidate IDs exceed 1048576 UTF-8 bytes"));
		}
		candidate_ids.push(id);
	}
	let filter = if cursor.boolean()? {
		let mut filter_clauses = 0usize;
		Some(decode_filter_expression(&mut cursor, 0, &mut filter_clauses)?)
	} else {
		None
	};
	let (sort, aggregations) = decode_catalog(&mut cursor)?;
	let offset = cursor.u32()? as usize;
	let limit = cursor.u32()? as usize;
	let exact_total = cursor.boolean()?;
	let (traversal, after) = decode_search_traversal(&mut cursor)?;
	let budget_milliseconds = cursor.u32()?;
	cursor.finish()?;
	if budget_milliseconds == 0 || budget_milliseconds > MAX_SEARCH_BUDGET_MILLISECONDS {
		return Err(FulltextError::invalid(format!(
			"search budget must be between 1 and {MAX_SEARCH_BUDGET_MILLISECONDS} milliseconds"
		)));
	}
	validate_search_window(offset, limit, traversal, sort.is_some() || !aggregations.is_empty())?;
	if contains_prefix(&expression) && !traversal && (offset != 0 || limit > MAX_AUTOCOMPLETE_RESULTS) {
		return Err(FulltextError::invalid(if sort.is_some() || !aggregations.is_empty() {
			"prefix search with sort or aggregations requires offset zero and a limit no greater than 100; metadata continuation is not supported"
		} else {
			"prefix search requires offset zero and a limit no greater than 100; use searchPage for subsequent matches"
		}));
	}
	Ok(SearchRequest {
		sort,
		aggregations,
		expression,
		candidate_ids: has_candidates.then_some(candidate_ids),
		filter,
		offset,
		limit,
		exact_total,
		budget_milliseconds,
		traversal,
		after,
	})
}

fn catalog_count(cursor: &mut Cursor<'_>, minimum_bytes: usize) -> Result<usize> {
	let count = cursor.u32()? as usize;
	if count > cursor.remaining() / minimum_bytes {
		return Err(FulltextError::invalid("catalog count exceeds remaining request bytes"));
	}
	Ok(count)
}

fn scan_catalog(cursor: &mut Cursor<'_>) -> Result<bool> {
	let sorts = catalog_count(cursor, 6)?;
	for _ in 0..sorts {
		let _ = cursor.bytes()?;
		if cursor.u8()? > 1 || cursor.u8()? > 1 {
			return Err(FulltextError::invalid("invalid sort ordering"));
		}
	}
	let count = scan_aggregations(cursor, 1)?;
	Ok(sorts > 0 || count > 0)
}

fn scan_aggregations(cursor: &mut Cursor<'_>, depth: usize) -> Result<usize> {
	let count = catalog_count(cursor, 13)?;
	if count > 0 && depth > MAX_AGGREGATION_DEPTH {
		return Err(FulltextError::invalid("aggregation nesting exceeds its depth limit"));
	}
	for _ in 0..count {
		let name = cursor.bytes()?;
		if name.is_empty() || name.len() > 128 {
			return Err(FulltextError::invalid("aggregation names require 1 to 128 UTF-8 bytes"));
		}
		let _ = cursor.bytes()?;
		let kind = cursor.u8()?;
		match kind {
			0 => {
				let _ = cursor.u32()?;
			}
			1 => {}
			2 => {
				let _ = cursor.f64()?;
			}
			3 => {
				let ranges = catalog_count(cursor, 2)?;
				if ranges == 0 {
					return Err(FulltextError::invalid("range aggregation requires ranges"));
				}
				for _ in 0..ranges {
					if cursor.boolean()? {
						let _ = cursor.f64()?;
					}
					if cursor.boolean()? {
						let _ = cursor.f64()?;
					}
				}
			}
			_ => return Err(FulltextError::invalid("unknown aggregation kind")),
		}
		let children = scan_aggregations(cursor, depth + 1)?;
		if kind == 1 && children > 0 {
			return Err(FulltextError::invalid("stats aggregations cannot have children"));
		}
	}
	Ok(count)
}

fn decode_catalog(cursor: &mut Cursor<'_>) -> Result<(Option<Vec<SortRequest>>, Vec<AggregationRequest>)> {
	let count = catalog_count(cursor, 6)?;
	let mut sorts = Vec::with_capacity(count);
	for _ in 0..count {
		let field = cursor.string()?;
		let direction = match cursor.u8()? {
			0 => SortDirection::Asc,
			1 => SortDirection::Desc,
			_ => return Err(FulltextError::invalid("unknown sort direction")),
		};
		let missing = match cursor.u8()? {
			0 => MissingOrder::First,
			1 => MissingOrder::Last,
			_ => return Err(FulltextError::invalid("unknown missing sort order")),
		};
		sorts.push(SortRequest {
			field,
			direction,
			missing,
		});
	}
	Ok(((!sorts.is_empty()).then_some(sorts), decode_aggregations(cursor, 1)?))
}

fn decode_aggregations(cursor: &mut Cursor<'_>, depth: usize) -> Result<Vec<AggregationRequest>> {
	let count = catalog_count(cursor, 13)?;
	if count > 0 && depth > MAX_AGGREGATION_DEPTH {
		return Err(FulltextError::invalid("aggregation nesting exceeds its depth limit"));
	}
	let mut results = Vec::with_capacity(count);
	for _ in 0..count {
		let name = cursor.string()?;
		let field = cursor.string()?;
		let kind = match cursor.u8()? {
			0 => AggregationKind::Terms { size: cursor.u32()? },
			1 => AggregationKind::Stats,
			2 => AggregationKind::Histogram {
				interval: cursor.f64()?,
			},
			3 => {
				let count = catalog_count(cursor, 2)?;
				if count == 0 {
					return Err(FulltextError::invalid("range aggregation requires ranges"));
				}
				let mut ranges = Vec::with_capacity(count);
				for _ in 0..count {
					let from = if cursor.boolean()? { Some(cursor.f64()?) } else { None };
					let to = if cursor.boolean()? { Some(cursor.f64()?) } else { None };
					ranges.push((from, to));
				}
				AggregationKind::Range { ranges }
			}
			_ => return Err(FulltextError::invalid("unknown aggregation kind")),
		};
		let aggregations = decode_aggregations(cursor, depth + 1)?;
		if matches!(kind, AggregationKind::Stats) && !aggregations.is_empty() {
			return Err(FulltextError::invalid("stats aggregations cannot have children"));
		}
		results.push(AggregationRequest {
			name,
			field,
			kind,
			aggregations,
		});
	}
	Ok(results)
}

fn decode_search_traversal(cursor: &mut Cursor<'_>) -> Result<(bool, Option<SearchBoundary>)> {
	let traversal = cursor.remaining() > 4 && cursor.boolean()?;
	let after = if traversal && cursor.boolean()? {
		let snapshot = cursor.string()?;
		let score = cursor.f32()?;
		let id = cursor.record_id()?;
		if snapshot.len() != 64
			|| !snapshot
				.bytes()
				.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
			|| !score.is_finite()
		{
			return Err(FulltextError::invalid("invalid search continuation boundary"));
		}
		Some(SearchBoundary { snapshot, score, id })
	} else {
		None
	};
	Ok((traversal, after))
}

fn decode_filter_expression(cursor: &mut Cursor<'_>, depth: usize, clauses: &mut usize) -> Result<FilterExpression> {
	if depth > MAX_EXPRESSION_DEPTH {
		return Err(FulltextError::invalid(format!(
			"filter expression nesting exceeds {MAX_EXPRESSION_DEPTH} levels"
		)));
	}
	match cursor.u8()? {
		0 => {
			*clauses += 1;
			if *clauses > MAX_QUERY_CLAUSES {
				return Err(FulltextError::invalid("filter expression exceeds 256 clauses"));
			}
			let field = cursor.string()?;
			let comparator = match cursor.u8()? {
				0 => FilterComparator::Equals,
				1 => FilterComparator::In,
				2 => FilterComparator::Lt,
				3 => FilterComparator::Le,
				4 => FilterComparator::Gt,
				5 => FilterComparator::Ge,
				6 => FilterComparator::Between,
				7 => FilterComparator::Exists,
				8 => FilterComparator::Missing,
				_ => return Err(FulltextError::invalid("unknown filter comparator")),
			};
			let count = cursor.u16()? as usize;
			let expected = match comparator {
				FilterComparator::In => None,
				FilterComparator::Between => Some(2),
				FilterComparator::Exists | FilterComparator::Missing => Some(0),
				_ => Some(1),
			};
			if (count == 0 && expected != Some(0))
				|| count > MAX_QUERY_CLAUSES
				|| expected.is_some_and(|expected| expected != count)
			{
				return Err(FulltextError::invalid("invalid filter value count"));
			}
			let mut values = Vec::with_capacity(count);
			for _ in 0..count {
				values.push(decode_filter_value(cursor)?);
			}
			Ok(FilterExpression::Clause {
				field,
				comparator,
				values,
			})
		}
		kind @ (1 | 2) => {
			let count = cursor.u16()? as usize;
			if count == 0 || count > MAX_QUERY_CLAUSES {
				return Err(FulltextError::invalid(
					"filter boolean expression requires 1 to 256 children",
				));
			}
			let mut children = Vec::with_capacity(count);
			for _ in 0..count {
				children.push(decode_filter_expression(cursor, depth + 1, clauses)?);
			}
			Ok(if kind == 1 {
				FilterExpression::And(children)
			} else {
				FilterExpression::Or(children)
			})
		}
		3 => Ok(FilterExpression::Not(Box::new(decode_filter_expression(
			cursor,
			depth + 1,
			clauses,
		)?))),
		_ => Err(FulltextError::invalid("unknown filter expression type")),
	}
}

fn decode_search_expression(
	cursor: &mut Cursor<'_>,
	depth: usize,
	structure: &mut SearchStructureBudget,
) -> Result<SearchExpression> {
	structure.depth(depth)?;
	let kind = cursor.u8()?;
	match kind {
		0 => {
			let text = cursor.bytes()?;
			let mode = decode_search_mode(cursor.u8()?)?;
			let field_count = cursor.u16()? as usize;
			structure.leaf(text.len(), field_count)?;
			let text = std::str::from_utf8(text)
				.map_err(|_| FulltextError::invalid("packed string is not valid UTF-8"))?
				.to_owned();
			let mut fields = Vec::with_capacity(field_count);
			for _ in 0..field_count {
				fields.push(cursor.string()?);
			}
			Ok(SearchExpression::Clause(SearchClause { text, mode, fields }))
		}
		1 | 2 => {
			let count = cursor.u16()? as usize;
			structure.children(count)?;
			let mut children = Vec::with_capacity(count);
			for _ in 0..count {
				children.push(decode_search_expression(cursor, depth + 1, structure)?);
			}
			Ok(if kind == 1 {
				SearchExpression::And(children)
			} else {
				SearchExpression::Or(children)
			})
		}
		3 => {
			structure.charge(2)?;
			Ok(SearchExpression::Not(Box::new(decode_search_expression(
				cursor,
				depth + 1,
				structure,
			)?)))
		}
		_ => Err(FulltextError::invalid("unknown search expression type")),
	}
}

fn contains_prefix(expression: &SearchExpression) -> bool {
	match expression {
		SearchExpression::Clause(clause) => matches!(clause.mode, SearchMode::Prefix | SearchMode::FuzzyPrefix),
		SearchExpression::And(children) | SearchExpression::Or(children) => children.iter().any(contains_prefix),
		SearchExpression::Not(child) => contains_prefix(child),
	}
}

pub fn decode_trace(bytes: &[u8]) -> Result<TraceRequest> {
	let mut cursor = Cursor::new(bytes, *b"FTTM")?;
	let text = cursor.string()?;
	if text.len() > MAX_QUERY_TEXT_BYTES {
		return Err(FulltextError::invalid("search text exceeds 65536 UTF-8 bytes"));
	}
	let mode = decode_search_mode(cursor.u8()?)?;
	let field_count = cursor.u16()? as usize;
	if field_count > MAX_FIELDS || field_count > cursor.remaining() / 4 {
		return Err(FulltextError::invalid("trace field count exceeds its packed request"));
	}
	let mut fields = Vec::with_capacity(field_count);
	for _ in 0..field_count {
		fields.push(cursor.string()?);
	}
	let has_candidates = cursor.boolean()?;
	let candidate_count = if has_candidates { cursor.u16()? as usize } else { 0 };
	if candidate_count > MAX_CANDIDATE_IDS || candidate_count > cursor.remaining() / 4 {
		return Err(FulltextError::invalid(
			"trace candidate count exceeds its packed request",
		));
	}
	let mut candidate_bytes = 0usize;
	let mut candidate_ids = Vec::with_capacity(candidate_count);
	for _ in 0..candidate_count {
		let id = cursor.record_id()?;
		candidate_bytes = candidate_bytes.saturating_add(id.len());
		if candidate_bytes > MAX_CANDIDATE_BYTES {
			return Err(FulltextError::invalid("candidate IDs exceed 1048576 UTF-8 bytes"));
		}
		candidate_ids.push(id);
	}
	let record_count = cursor.u16()? as usize;
	if record_count > MAX_TRACE_RECORDS || record_count > cursor.remaining() / 6 {
		return Err(FulltextError::invalid("trace record count exceeds its packed request"));
	}
	let mut source_bytes = 0usize;
	let mut records = Vec::with_capacity(record_count);
	for _ in 0..record_count {
		let id = cursor.record_id()?;
		let field_count = cursor.u16()? as usize;
		if field_count > MAX_FIELDS || field_count > cursor.remaining() / 6 {
			return Err(FulltextError::invalid(
				"trace record field count exceeds its packed request",
			));
		}
		let mut record_fields = Vec::with_capacity(field_count);
		for _ in 0..field_count {
			let name = cursor.string()?;
			let value_count = cursor.u16()? as usize;
			if value_count > cursor.remaining() / 4 {
				return Err(FulltextError::invalid("trace value count exceeds its packed request"));
			}
			let mut values = Vec::with_capacity(value_count);
			for _ in 0..value_count {
				let value = cursor.string()?;
				source_bytes = source_bytes.saturating_add(value.len());
				if source_bytes > MAX_TRACE_SOURCE_BYTES {
					return Err(FulltextError::invalid("trace source text exceeds 1048576 UTF-8 bytes"));
				}
				values.push(value);
			}
			record_fields.push((name, values));
		}
		records.push(TraceRecord {
			id,
			fields: record_fields,
		});
	}
	let budget_milliseconds = cursor.u32()?;
	cursor.finish()?;
	if budget_milliseconds == 0 || budget_milliseconds > MAX_SEARCH_BUDGET_MILLISECONDS {
		return Err(FulltextError::invalid(format!(
			"trace budget must be between 1 and {MAX_SEARCH_BUDGET_MILLISECONDS} milliseconds"
		)));
	}
	Ok(TraceRequest {
		search: SearchRequest {
			sort: None,
			aggregations: Vec::new(),
			expression: SearchExpression::Clause(SearchClause { text, mode, fields }),
			candidate_ids: has_candidates.then_some(candidate_ids),
			filter: None,
			offset: 0,
			limit: record_count,
			exact_total: false,
			budget_milliseconds,
			traversal: false,
			after: None,
		},
		records,
	})
}

fn decode_search_mode(value: u8) -> Result<SearchMode> {
	match value {
		0 => Ok(SearchMode::Any),
		1 => Ok(SearchMode::All),
		2 => Ok(SearchMode::Phrase),
		3 => Ok(SearchMode::Prefix),
		4 => Ok(SearchMode::Fuzzy),
		5 => Ok(SearchMode::FuzzyPrefix),
		_ => Err(FulltextError::invalid("unknown search mode")),
	}
}

fn validate_config(config: EngineConfig) -> Result<EngineConfig> {
	validate_identity_config(&config.identity)?;
	let limits = &config.limits;
	validate_query_limits(limits.max_query_terms, limits.max_query_clauses)?;
	if limits.indexing_threads == 0 || limits.search_threads == 0 || limits.max_queued_commands == 0 {
		return Err(FulltextError::invalid(
			"thread and queue command limits must be greater than zero",
		));
	}
	if limits.indexing_threads > 64 || limits.search_threads > 64 {
		return Err(FulltextError::invalid("thread limits must not exceed 64"));
	}
	let per_thread = limits.writer_memory_bytes / limits.indexing_threads;
	if !(15_000_000..u32::MAX as usize).contains(&per_thread) {
		return Err(FulltextError::invalid(format!(
			"writerMemoryBytes/indexingThreads is {per_thread}; Tantivy requires 15000000..{}",
			u32::MAX
		)));
	}
	if limits.max_batch_bytes < MIN_MUTATION_BATCH_BYTES || limits.max_batch_bytes > limits.max_queued_bytes {
		return Err(FulltextError::invalid(
			"maxBatchBytes must hold at least one upsert and be no larger than maxQueuedBytes",
		));
	}
	Ok(config)
}

fn validate_identity_config(config: &EngineIdentityConfig) -> Result<()> {
	if config.index_id.is_empty() || config.generation.is_empty() {
		return Err(FulltextError::invalid("indexId and generation must not be empty"));
	}
	if config.index_id.len() > 4_096 || config.generation.len() > 4_096 {
		return Err(FulltextError::invalid(
			"indexId and generation must not exceed 4096 UTF-8 bytes",
		));
	}
	if config.analyzer != "english@2" {
		return Err(FulltextError::invalid("only analyzer english@2 is supported"));
	}
	if config.synonyms.len() > MAX_SYNONYM_RULES {
		return Err(FulltextError::invalid(format!(
			"synonyms must not contain more than {MAX_SYNONYM_RULES} rules"
		)));
	}
	let mut synonym_bytes = 0usize;
	for rule in &config.synonyms {
		if rule.source.is_empty() || rule.replacements.is_empty() || rule.replacements.len() > MAX_SYNONYM_REPLACEMENTS
		{
			return Err(FulltextError::invalid(format!(
				"synonym rules require a source and between 1 and {MAX_SYNONYM_REPLACEMENTS} replacements"
			)));
		}
		synonym_bytes = synonym_bytes.saturating_add(rule.source.len());
		for replacement in &rule.replacements {
			if replacement.is_empty() {
				return Err(FulltextError::invalid("synonym replacements must not be empty"));
			}
			synonym_bytes = synonym_bytes.saturating_add(replacement.len());
		}
	}
	if synonym_bytes > MAX_SYNONYM_BYTES {
		return Err(FulltextError::invalid(format!(
			"synonyms must not exceed {MAX_SYNONYM_BYTES} UTF-8 bytes"
		)));
	}
	let mut names = std::collections::HashSet::with_capacity(config.fields.len());
	for field in &config.fields {
		if field.name.is_empty() || field.name.starts_with("__fulltext_") || !names.insert(field.name.as_str()) {
			return Err(FulltextError::invalid(
				"field names must be non-empty, unique, and outside the reserved __fulltext_ namespace",
			));
		}
	}
	let mut filter_names = std::collections::HashSet::with_capacity(config.filter_fields.len());
	for field in &config.filter_fields {
		if field.name.is_empty() || field.name.starts_with("__fulltext_") || !filter_names.insert(field.name.as_str()) {
			return Err(FulltextError::invalid(
				"filter field names must be non-empty, unique, and outside the reserved __fulltext_ namespace",
			));
		}
	}
	Ok(())
}

fn decode_filter_value(cursor: &mut Cursor<'_>) -> Result<FilterValue> {
	match cursor.u8()? {
		0 => Ok(FilterValue::String(cursor.string()?)),
		1 => {
			let value = cursor.f64()?;
			if !value.is_finite() {
				return Err(FulltextError::invalid("filter numbers must be finite"));
			}
			Ok(FilterValue::Number(value))
		}
		2 => Ok(FilterValue::Boolean(cursor.boolean()?)),
		_ => Err(FulltextError::invalid("unknown filter value type")),
	}
}

fn scan_filter_value(cursor: &mut Cursor<'_>) -> Result<()> {
	match cursor.u8()? {
		0 => {
			let _ = cursor.bytes()?;
		}
		1 => {
			if !cursor.f64()?.is_finite() {
				return Err(FulltextError::invalid("filter numbers must be finite"));
			}
		}
		2 => {
			let _ = cursor.boolean()?;
		}
		_ => return Err(FulltextError::invalid("unknown filter value type")),
	}
	Ok(())
}

struct Cursor<'a> {
	bytes: &'a [u8],
	offset: usize,
}

impl<'a> Cursor<'a> {
	fn new(bytes: &'a [u8], magic: [u8; 4]) -> Result<Self> {
		if bytes.len() < 6 || bytes[..4] != magic {
			return Err(FulltextError::invalid("invalid packed request magic"));
		}
		let version = u16::from_le_bytes([bytes[4], bytes[5]]);
		if version != PROTOCOL_VERSION {
			return Err(FulltextError::invalid(format!(
				"unsupported packed request version {version}"
			)));
		}
		Ok(Self { bytes, offset: 6 })
	}

	fn take(&mut self, length: usize) -> Result<&'a [u8]> {
		let end = self
			.offset
			.checked_add(length)
			.ok_or_else(|| FulltextError::invalid("packed request length overflow"))?;
		let value = self
			.bytes
			.get(self.offset..end)
			.ok_or_else(|| FulltextError::invalid("packed request is truncated"))?;
		self.offset = end;
		Ok(value)
	}

	fn u8(&mut self) -> Result<u8> {
		Ok(self.take(1)?[0])
	}

	fn boolean(&mut self) -> Result<bool> {
		match self.u8()? {
			0 => Ok(false),
			1 => Ok(true),
			_ => Err(FulltextError::invalid("packed boolean must be zero or one")),
		}
	}

	fn u16(&mut self) -> Result<u16> {
		let bytes = self.take(2)?;
		Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
	}

	fn u32(&mut self) -> Result<u32> {
		let bytes = self.take(4)?;
		Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
	}

	fn u64_usize(&mut self) -> Result<usize> {
		let value = self.u64()?;
		usize::try_from(value).map_err(|_| FulltextError::invalid("numeric limit exceeds usize"))
	}

	fn u64(&mut self) -> Result<u64> {
		let bytes = self.take(8)?;
		Ok(u64::from_le_bytes([
			bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
		]))
	}

	fn bytes(&mut self) -> Result<&'a [u8]> {
		let length = self.u32()? as usize;
		if length > MAX_STRING_BYTES {
			return Err(FulltextError::invalid("packed byte string exceeds 1 MiB"));
		}
		self.take(length)
	}

	fn f32(&mut self) -> Result<f32> {
		let bytes = self.take(4)?;
		Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
	}

	fn f64(&mut self) -> Result<f64> {
		let bytes = self.take(8)?;
		Ok(f64::from_le_bytes([
			bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
		]))
	}

	fn string(&mut self) -> Result<String> {
		let bytes = self.bytes()?;
		String::from_utf8(bytes.to_vec()).map_err(|_| FulltextError::invalid("packed string is not valid UTF-8"))
	}

	fn string_with_budget(&mut self, remaining: &mut usize, label: &str) -> Result<String> {
		let length = self.u32()? as usize;
		if length > *remaining || length > MAX_STRING_BYTES {
			return Err(FulltextError::invalid(format!(
				"{label} must not exceed {MAX_SYNONYM_BYTES} bytes"
			)));
		}
		*remaining -= length;
		let bytes = self.take(length)?;
		String::from_utf8(bytes.to_vec()).map_err(|_| FulltextError::invalid("packed string is not valid UTF-8"))
	}

	fn record_id(&mut self) -> Result<String> {
		let id = self.string()?;
		validate_record_id(&id)?;
		Ok(id)
	}

	fn finish(&self) -> Result<()> {
		if self.offset == self.bytes.len() {
			Ok(())
		} else {
			Err(FulltextError::invalid("packed request has trailing bytes"))
		}
	}

	fn remaining(&self) -> usize {
		self.bytes.len() - self.offset
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn catalog_counts_and_depth_are_checked_before_allocation() {
		fn u32_bytes(value: u32, bytes: &mut Vec<u8>) {
			bytes.extend_from_slice(&value.to_le_bytes());
		}
		fn tree(depth: usize, bytes: &mut Vec<u8>) {
			u32_bytes(u32::from(depth > 0), bytes);
			if depth == 0 {
				return;
			}
			for value in ["n", "brand"] {
				u32_bytes(value.len() as u32, bytes);
				bytes.extend_from_slice(value.as_bytes());
			}
			bytes.push(0);
			u32_bytes(1, bytes);
			tree(depth - 1, bytes);
		}
		for depth in [32, 33] {
			let mut bytes = Vec::new();
			u32_bytes(0, &mut bytes);
			tree(depth, &mut bytes);
			assert_eq!(
				scan_catalog(&mut Cursor {
					bytes: &bytes,
					offset: 0
				})
				.is_ok(),
				depth == 32
			);
			assert_eq!(
				decode_catalog(&mut Cursor {
					bytes: &bytes,
					offset: 0
				})
				.is_ok(),
				depth == 32
			);
		}
		for bytes in [
			u32::MAX.to_le_bytes().to_vec(),
			[0u32.to_le_bytes(), u32::MAX.to_le_bytes()].concat(),
		] {
			assert!(scan_catalog(&mut Cursor {
				bytes: &bytes,
				offset: 0
			})
			.is_err());
			assert!(decode_catalog(&mut Cursor {
				bytes: &bytes,
				offset: 0
			})
			.is_err());
		}
	}

	#[test]
	fn filter_presence_and_negation_scanner_matches_decoder() {
		let leaf = |comparator: u8, count: u16| {
			let mut bytes = vec![0, 1, 0, 0, 0, b'x', comparator];
			bytes.extend_from_slice(&count.to_le_bytes());
			bytes
		};
		for comparator in [7, 8] {
			let bytes = leaf(comparator, 0);
			assert!(scan_filter_expression(
				&mut Cursor {
					bytes: &bytes,
					offset: 0
				},
				0,
				&mut 0
			)
			.unwrap());
			assert!(decode_filter_expression(
				&mut Cursor {
					bytes: &bytes,
					offset: 0
				},
				0,
				&mut 0
			)
			.is_ok());
			let invalid = leaf(comparator, 1);
			assert!(scan_filter_expression(
				&mut Cursor {
					bytes: &invalid,
					offset: 0
				},
				0,
				&mut 0
			)
			.is_err());
			assert!(decode_filter_expression(
				&mut Cursor {
					bytes: &invalid,
					offset: 0
				},
				0,
				&mut 0
			)
			.is_err());
		}
		let mut negated = vec![3];
		negated.extend_from_slice(&leaf(7, 0));
		assert!(scan_filter_expression(
			&mut Cursor {
				bytes: &negated,
				offset: 0
			},
			0,
			&mut 0
		)
		.unwrap());
		assert!(matches!(
			decode_filter_expression(
				&mut Cursor {
					bytes: &negated,
					offset: 0
				},
				0,
				&mut 0
			)
			.unwrap(),
			FilterExpression::Not(_)
		));
		for _ in 0..8 {
			negated.insert(0, 3);
		}
		assert!(scan_filter_expression(
			&mut Cursor {
				bytes: &negated,
				offset: 0
			},
			0,
			&mut 0
		)
		.is_err());
		assert!(decode_filter_expression(
			&mut Cursor {
				bytes: &negated,
				offset: 0
			},
			0,
			&mut 0
		)
		.is_err());
	}

	#[test]
	fn reset_frame_contains_only_path_and_logical_index_id() {
		let mut bytes = b"FTRX\x07\x00".to_vec();
		for value in ["/tmp/index", "products"] {
			bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
			bytes.extend_from_slice(value.as_bytes());
		}

		assert_eq!(
			decode_reset(&bytes).unwrap(),
			NativeResetConfig {
				path: "/tmp/index".to_owned(),
				index_id: "products".to_owned(),
			}
		);
		assert_eq!(decode_inspect(&bytes).unwrap_err().code, "E_INVALID_ARGUMENT");

		bytes.push(0);
		assert_eq!(decode_reset(&bytes).unwrap_err().code, "E_INVALID_ARGUMENT");
	}

	#[test]
	fn inspection_frame_is_distinct_and_rejects_trailing_limits() {
		let mut bytes = b"FTIP\x07\x00".to_vec();
		for value in ["/tmp/index", "products", "one", "english@2"] {
			bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
			bytes.extend_from_slice(value.as_bytes());
		}
		bytes.extend_from_slice(&[1, 1, 0]);
		bytes.extend_from_slice(&0u16.to_le_bytes());
		bytes.extend_from_slice(&1u16.to_le_bytes());
		bytes.extend_from_slice(&5u32.to_le_bytes());
		bytes.extend_from_slice(b"title");
		bytes.extend_from_slice(&1f32.to_le_bytes());
		bytes.extend_from_slice(&0u16.to_le_bytes());

		let decoded = decode_inspect(&bytes).unwrap();
		assert_eq!(decoded.path, "/tmp/index");
		assert_eq!(decoded.identity.fields[0].name, "title");
		assert_eq!(decode_open(&bytes).unwrap_err().code, "E_INVALID_ARGUMENT");

		bytes.push(0);
		assert_eq!(decode_inspect(&bytes).unwrap_err().code, "E_INVALID_ARGUMENT");
	}

	#[test]
	fn rejects_counts_before_allocating() {
		let mut bytes = b"FTMB\x07\x00".to_vec();
		bytes.extend_from_slice(&u32::MAX.to_le_bytes());
		bytes.extend_from_slice(&0u32.to_le_bytes());
		assert_eq!(decode_batch(&bytes).unwrap_err().code, "E_INVALID_ARGUMENT");
	}

	#[test]
	fn rejects_synonym_strings_before_allocating_past_the_aggregate_budget() {
		let bytes = ((MAX_SYNONYM_BYTES + 1) as u32).to_le_bytes();
		let mut cursor = Cursor {
			bytes: &bytes,
			offset: 0,
		};
		let mut remaining = MAX_SYNONYM_BYTES;
		assert_eq!(
			cursor
				.string_with_budget(&mut remaining, "encoded synonyms")
				.unwrap_err()
				.code,
			"E_INVALID_ARGUMENT"
		);
	}

	#[test]
	fn accepts_exact_synonym_text_budget_without_charging_frame_bytes() {
		let mut bytes = (MAX_SYNONYM_BYTES as u32).to_le_bytes().to_vec();
		bytes.resize(4 + MAX_SYNONYM_BYTES, b'x');
		let mut cursor = Cursor {
			bytes: &bytes,
			offset: 0,
		};
		let mut remaining = MAX_SYNONYM_BYTES;
		assert_eq!(
			cursor
				.string_with_budget(&mut remaining, "encoded synonyms")
				.unwrap()
				.len(),
			MAX_SYNONYM_BYTES
		);
		assert_eq!(remaining, 0);
	}

	#[test]
	fn distinguishes_batch_size_from_invalid_encoding() {
		let bytes = b"FTMB\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00";
		assert_eq!(
			validate_batch_header(bytes, bytes.len() - 1).unwrap_err().code,
			"E_BATCH_TOO_LARGE"
		);
		let malformed = b"NOPE\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00";
		assert_eq!(
			validate_batch_header(malformed, malformed.len()).unwrap_err().code,
			"E_INVALID_ARGUMENT"
		);
	}

	#[test]
	fn rejects_a_batch_limit_that_cannot_hold_the_header() {
		let identity = EngineIdentityConfig {
			index_id: "products".to_owned(),
			generation: "one".to_owned(),
			fields: vec![FieldConfig {
				name: "title".to_owned(),
				weight: 1.0,
			}],
			analyzer: "english@2".to_owned(),
			stop_words: true,
			positions: true,
			surface_terms: false,
			synonyms: Vec::new(),
			filter_fields: Vec::new(),
		};
		let limits = Limits {
			max_query_terms: MAX_QUERY_TERMS,
			max_query_clauses: MAX_QUERY_CLAUSES,
			indexing_threads: 1,
			search_threads: 1,
			writer_memory_bytes: 15_000_000,
			max_queued_commands: 1,
			max_queued_bytes: 1024,
			max_batch_bytes: MIN_MUTATION_BATCH_BYTES - 1,
		};
		assert_eq!(
			validate_config(EngineConfig {
				identity: identity.clone(),
				limits: limits.clone(),
			})
			.unwrap_err()
			.code,
			"E_INVALID_ARGUMENT"
		);
		let mut accepted = limits;
		accepted.max_batch_bytes = MIN_MUTATION_BATCH_BYTES;
		assert!(validate_config(EngineConfig {
			identity,
			limits: accepted,
		})
		.is_ok());
	}

	#[test]
	fn rejects_invalid_utf8() {
		let mut bytes = b"FTSQ\x01\x00".to_vec();
		bytes.extend_from_slice(&1u32.to_le_bytes());
		bytes.push(0xff);
		assert_eq!(decode_search(&bytes).unwrap_err().code, "E_INVALID_ARGUMENT");
	}

	#[test]
	fn raised_capacity_classification_keeps_small_ascii_queries_ordinary() {
		fn request(text: &str) -> Vec<u8> {
			let mut bytes = b"FTSQ\x07\x00".to_vec();
			bytes.push(0);
			bytes.extend_from_slice(&(text.len() as u32).to_le_bytes());
			bytes.extend_from_slice(text.as_bytes());
			bytes.extend_from_slice(&[0, 0, 0, 0, 0]);
			bytes.extend_from_slice(&[0; 8]);
			bytes.extend_from_slice(&0u32.to_le_bytes());
			bytes.extend_from_slice(&1u32.to_le_bytes());
			bytes.push(0);
			bytes.extend_from_slice(&1000u32.to_le_bytes());
			bytes
		}
		for text in ["a", "shoe", "alpha beta", &"a".repeat(65)] {
			let bytes = request(text);
			assert!(!search_is_expensive(&bytes).unwrap());
			assert!(
				!search_is_expensive_for_fields(&bytes, 2, MAX_QUERY_TERMS + 1, MAX_CONFIGURED_QUERY_CLAUSES).unwrap()
			);
		}
		for (text, fields) in [("word ".repeat(65), 1), ("shoe".to_owned(), 257), ("ﷺ".to_owned(), 1)] {
			let bytes = request(&text);
			assert!(!search_is_expensive(&bytes).unwrap());
			assert!(
				search_is_expensive_for_fields(&bytes, fields, MAX_QUERY_TERMS + 1, MAX_CONFIGURED_QUERY_CLAUSES)
					.unwrap()
			);
		}
		for length in 0..6 {
			let bytes = &request("shoe")[..length];
			assert_eq!(
				search_is_expensive(bytes).unwrap_err().code,
				search_is_expensive_for_fields(bytes, 2, MAX_QUERY_TERMS + 1, MAX_CONFIGURED_QUERY_CLAUSES)
					.unwrap_err()
					.code
			);
		}
	}

	#[test]
	fn raised_capacity_accounts_for_boolean_construction_and_per_leaf_terms() {
		fn leaf(text: &str) -> Vec<u8> {
			let mut bytes = vec![0];
			bytes.extend_from_slice(&(text.len() as u32).to_le_bytes());
			bytes.extend_from_slice(text.as_bytes());
			bytes.extend_from_slice(&[0, 0, 0]);
			bytes
		}
		fn group(kind: u8, children: &[Vec<u8>]) -> Vec<u8> {
			let mut bytes = vec![kind];
			bytes.extend_from_slice(&(children.len() as u16).to_le_bytes());
			for child in children {
				bytes.extend_from_slice(child);
			}
			bytes
		}
		fn expensive(expression: &[u8], fields: usize) -> bool {
			let mut bytes = b"FTSQ\x07\x00".to_vec();
			bytes.extend_from_slice(expression);
			bytes.extend_from_slice(&[0, 0]);
			bytes.extend_from_slice(&[0; 8]);
			bytes.extend_from_slice(&0u32.to_le_bytes());
			bytes.extend_from_slice(&1u32.to_le_bytes());
			bytes.push(0);
			bytes.extend_from_slice(&1000u32.to_le_bytes());
			search_is_expensive_for_fields(&bytes, fields, MAX_QUERY_TERMS + 1, MAX_CONFIGURED_QUERY_CLAUSES).unwrap()
		}
		for kind in [1, 2] {
			assert!(!expensive(&group(kind, &vec![leaf("a"); 65]), 1));
			assert!(!expensive(&group(kind, &vec![leaf("a"); 128]), 1));
			assert!(expensive(&group(kind, &vec![leaf("a"); 129]), 1));
			assert!(expensive(&group(kind, &vec![leaf("a"); 64]), 4));
		}
		let mut nested = leaf("a");
		for _ in 0..7 {
			nested = group(2, &[nested]);
		}
		assert!(expensive(&group(2, &vec![nested; 64]), 1));
		let mut negated = vec![3];
		negated.extend_from_slice(&leaf("!"));
		for (count, expected) in [(84, false), (85, true)] {
			let mut children = vec![leaf("a")];
			children.extend(vec![negated.clone(); count]);
			assert_eq!(expensive(&group(1, &children), 1), expected);
		}
	}

	#[test]
	fn text_structure_uses_host_policy_and_decoder_ceiling_before_descendants() {
		fn leaf(text: &str) -> Vec<u8> {
			let mut bytes = vec![0];
			bytes.extend_from_slice(&(text.len() as u32).to_le_bytes());
			bytes.extend_from_slice(text.as_bytes());
			bytes.extend_from_slice(&[0, 0, 0]);
			bytes
		}
		fn group(count: usize, child: &[u8]) -> Vec<u8> {
			let mut bytes = vec![2];
			bytes.extend_from_slice(&(count as u16).to_le_bytes());
			for _ in 0..count {
				bytes.extend_from_slice(child);
			}
			bytes
		}
		fn request(expression: &[u8]) -> Vec<u8> {
			let mut bytes = b"FTSQ\x07\x00".to_vec();
			bytes.extend_from_slice(expression);
			bytes.extend_from_slice(&[0; 10]);
			bytes.extend_from_slice(&0u32.to_le_bytes());
			bytes.extend_from_slice(&1u32.to_le_bytes());
			bytes.push(0);
			bytes.extend_from_slice(&1000u32.to_le_bytes());
			bytes
		}
		let wide = request(&group(300, &leaf("shoe")));
		assert!(search_is_expensive(&wide)
			.unwrap_err()
			.message
			.contains("structure exceeds 256"));
		assert!(search_is_expensive_for_fields(&wide, 1, 64, 1024).unwrap());
		assert!(
			matches!(decode_search(&wide).unwrap().expression, SearchExpression::Or(children) if children.len() == 300)
		);
		let ceiling = request(&group(4096, &leaf("")));
		assert!(search_is_expensive_for_fields(&ceiling, 1, 64, 4096).unwrap());
		assert!(decode_search(&ceiling).is_ok());
		for expression in [group(4097, &[]), group(2, &group(2048, &leaf("")))] {
			let bytes = request(&expression);
			for error in [
				search_is_expensive_for_fields(&bytes, 1, 64, 4096).unwrap_err(),
				decode_search(&bytes).unwrap_err(),
			] {
				assert_eq!(error.code, "E_INVALID_ARGUMENT");
				assert!(error.message.contains("structure exceeds 4096"));
			}
		}
		let mut negated = vec![3];
		negated.extend(leaf(""));
		assert!(search_is_expensive_for_fields(&request(&negated), 1, 64, 2).unwrap());
		assert!(search_is_expensive_for_fields(&request(&negated), 1, 64, 1)
			.unwrap_err()
			.message
			.contains("structure exceeds 1"));
		let mut fields = leaf("");
		let length = fields.len();
		fields[length - 2..].copy_from_slice(&3u16.to_le_bytes());
		fields.extend_from_slice(&[0; 12]);
		assert!(search_is_expensive_for_fields(&request(&fields), 2, 64, 4096)
			.unwrap_err()
			.message
			.contains("field count exceeds 2"));
	}

	#[test]
	fn classifies_negation_and_exact_totals_as_expensive() {
		fn request(expression: &[u8], exact_total: bool) -> Vec<u8> {
			let mut bytes = b"FTSQ\x07\x00".to_vec();
			bytes.extend_from_slice(expression);
			bytes.push(0);
			bytes.push(0);
			bytes.extend_from_slice(&[0; 8]);
			bytes.extend_from_slice(&0u32.to_le_bytes());
			bytes.extend_from_slice(&1u32.to_le_bytes());
			bytes.push(u8::from(exact_total));
			bytes.extend_from_slice(&100u32.to_le_bytes());
			bytes
		}
		let leaf = [0, 1, 0, 0, 0, b'a', 0, 0, 0];
		assert!(!search_is_expensive(&request(&leaf, false)).unwrap());
		assert!(search_is_expensive(&request(&leaf, true)).unwrap());
		let mut negated = vec![3];
		negated.extend_from_slice(&leaf);
		assert!(search_is_expensive(&request(&negated, false)).unwrap());
		let mut anchored = vec![1, 2, 0];
		anchored.extend_from_slice(&leaf);
		anchored.extend_from_slice(&negated);
		assert!(!search_is_expensive(&request(&anchored, false)).unwrap());
		let mut unanchored_or = vec![2, 2, 0];
		unanchored_or.extend_from_slice(&leaf);
		unanchored_or.extend_from_slice(&negated);
		assert!(search_is_expensive(&request(&unanchored_or, false)).unwrap());

		let filtered_request = |comparator: u8| {
			let mut bytes = b"FTSQ\x07\x00".to_vec();
			bytes.extend_from_slice(&leaf);
			bytes.push(0);
			bytes.push(1);
			bytes.push(0);
			bytes.extend_from_slice(&5u32.to_le_bytes());
			bytes.extend_from_slice(b"price");
			bytes.push(comparator);
			bytes.extend_from_slice(&1u16.to_le_bytes());
			bytes.push(1);
			bytes.extend_from_slice(&50f64.to_le_bytes());
			bytes.extend_from_slice(&[0; 8]);
			bytes.extend_from_slice(&0u32.to_le_bytes());
			bytes.extend_from_slice(&1u32.to_le_bytes());
			bytes.push(0);
			bytes.extend_from_slice(&100u32.to_le_bytes());
			bytes
		};
		assert!(!search_is_expensive(&filtered_request(0)).unwrap());
		assert!(search_is_expensive(&filtered_request(2)).unwrap());

		let mut many_exact_filters = b"FTSQ\x07\x00".to_vec();
		many_exact_filters.extend_from_slice(&leaf);
		many_exact_filters.push(0);
		many_exact_filters.push(1);
		many_exact_filters.push(1);
		many_exact_filters.extend_from_slice(&17u16.to_le_bytes());
		for _ in 0..17 {
			many_exact_filters.push(0);
			many_exact_filters.extend_from_slice(&5u32.to_le_bytes());
			many_exact_filters.extend_from_slice(b"price");
			many_exact_filters.push(0);
			many_exact_filters.extend_from_slice(&1u16.to_le_bytes());
			many_exact_filters.push(1);
			many_exact_filters.extend_from_slice(&50f64.to_le_bytes());
		}
		many_exact_filters.extend_from_slice(&[0; 8]);
		many_exact_filters.extend_from_slice(&0u32.to_le_bytes());
		many_exact_filters.extend_from_slice(&1u32.to_le_bytes());
		many_exact_filters.push(0);
		many_exact_filters.extend_from_slice(&100u32.to_le_bytes());
		assert!(search_is_expensive(&many_exact_filters).unwrap());
	}

	#[test]
	fn rejects_nested_counts_before_allocating() {
		let mut fields = b"FTMB\x07\x00".to_vec();
		fields.extend_from_slice(&1u32.to_le_bytes());
		fields.extend_from_slice(&0u32.to_le_bytes());
		fields.extend_from_slice(&0u32.to_le_bytes());
		fields.extend_from_slice(&u16::MAX.to_le_bytes());
		assert_eq!(decode_batch(&fields).unwrap_err().code, "E_INVALID_ARGUMENT");

		let mut values = b"FTMB\x07\x00".to_vec();
		values.extend_from_slice(&1u32.to_le_bytes());
		values.extend_from_slice(&0u32.to_le_bytes());
		values.extend_from_slice(&0u32.to_le_bytes());
		values.extend_from_slice(&1u16.to_le_bytes());
		values.extend_from_slice(&0u32.to_le_bytes());
		values.extend_from_slice(&u16::MAX.to_le_bytes());
		assert_eq!(decode_batch(&values).unwrap_err().code, "E_INVALID_ARGUMENT");
	}
}
