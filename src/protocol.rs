use crate::error::{FulltextError, Result};

pub const PROTOCOL_VERSION: u16 = 2;
const MAX_STRING_BYTES: usize = 1 << 20;
const MAX_FIELDS: usize = 1_024;
const MUTATION_BATCH_HEADER_BYTES: usize = 14;
const MIN_MUTATION_BATCH_BYTES: usize = MUTATION_BATCH_HEADER_BYTES + 7;

#[derive(Clone, Debug, PartialEq)]
pub struct FieldConfig {
	pub name: String,
	pub weight: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Limits {
	pub indexing_threads: usize,
	pub search_threads: usize,
	pub writer_memory_bytes: usize,
	pub max_queued_commands: usize,
	pub max_queued_bytes: usize,
	pub max_batch_bytes: usize,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Upsert {
	pub id: String,
	pub fields: Vec<(String, Vec<String>)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationBatch {
	pub upserts: Vec<Upsert>,
	pub deletes: Vec<String>,
}

pub const MAX_QUERY_TEXT_BYTES: usize = 64 * 1024;
pub const MAX_QUERY_TERMS: usize = 64;
pub const MAX_QUERY_CLAUSES: usize = 256;
pub const MAX_CANDIDATE_IDS: usize = 1_024;
pub const MAX_CANDIDATE_BYTES: usize = 1 << 20;
pub const MAX_PREFIX_EXPANSIONS: usize = 50;
pub const MAX_FUZZY_TERMS: usize = 16;
pub const MAX_SEARCH_WINDOW: usize = 10_000;
pub const MAX_AUTOCOMPLETE_RESULTS: usize = 100;
pub const MAX_SEARCH_REQUEST_BYTES: usize = 8 << 20;
pub const MAX_SEARCH_RESPONSE_BYTES: usize = 8 << 20;
pub const MAX_SEARCH_BUDGET_MILLISECONDS: u32 = 30_000;
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
pub struct SearchRequest {
	pub text: String,
	pub mode: SearchMode,
	pub fields: Vec<String>,
	pub candidate_ids: Option<Vec<String>>,
	pub offset: usize,
	pub limit: usize,
	pub exact_total: bool,
	pub budget_milliseconds: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceRecord {
	pub id: String,
	pub fields: Vec<(String, Vec<String>)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
	let limits = Limits {
		indexing_threads: cursor.u16()? as usize,
		search_threads: cursor.u16()? as usize,
		writer_memory_bytes: cursor.u64_usize()?,
		max_queued_commands: cursor.u32()? as usize,
		max_queued_bytes: cursor.u64_usize()?,
		max_batch_bytes: cursor.u64_usize()?,
	};
	validate_config(EngineConfig { identity, limits })
}

fn decode_engine_identity_config(cursor: &mut Cursor<'_>) -> Result<EngineIdentityConfig> {
	let index_id = cursor.string()?;
	let generation = cursor.string()?;
	let analyzer = cursor.string()?;
	let stop_words = cursor.boolean()?;
	let positions = cursor.boolean()?;
	let surface_terms = cursor.boolean()?;
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
	Ok(EngineIdentityConfig {
		index_id,
		generation,
		fields,
		analyzer,
		stop_words,
		positions,
		surface_terms,
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

pub fn search_mode(bytes: &[u8]) -> Result<SearchMode> {
	validate_search_header(bytes)?;
	let mut cursor = Cursor::new(bytes, *b"FTSQ")?;
	let query_length = cursor.u32()? as usize;
	if query_length > MAX_QUERY_TEXT_BYTES {
		return Err(FulltextError::invalid("search text exceeds 65536 UTF-8 bytes"));
	}
	cursor.take(query_length)?;
	decode_search_mode(cursor.u8()?)
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
		let id = cursor.string()?;
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
		upserts.push(Upsert { id, fields });
	}
	if delete_count > cursor.remaining() / 4 {
		return Err(FulltextError::invalid("delete count exceeds the packed batch length"));
	}
	let mut deletes = Vec::with_capacity(delete_count);
	for _ in 0..delete_count {
		deletes.push(cursor.string()?);
	}
	cursor.finish()?;
	Ok(MutationBatch { upserts, deletes })
}

pub fn decode_search(bytes: &[u8]) -> Result<SearchRequest> {
	let mut cursor = Cursor::new(bytes, *b"FTSQ")?;
	let text = cursor.string()?;
	if text.len() > MAX_QUERY_TEXT_BYTES {
		return Err(FulltextError::invalid("search text exceeds 65536 UTF-8 bytes"));
	}
	let mode = decode_search_mode(cursor.u8()?)?;
	let field_count = cursor.u16()? as usize;
	if field_count > MAX_FIELDS {
		return Err(FulltextError::invalid("search field count exceeds 1024"));
	}
	let mut fields = Vec::with_capacity(field_count);
	for _ in 0..field_count {
		fields.push(cursor.string()?);
	}
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
		let id = cursor.string()?;
		if id.is_empty() {
			return Err(FulltextError::invalid("candidate IDs must not be empty"));
		}
		candidate_bytes = candidate_bytes
			.checked_add(id.len())
			.ok_or_else(|| FulltextError::invalid("candidate ID byte count overflow"))?;
		if candidate_bytes > MAX_CANDIDATE_BYTES {
			return Err(FulltextError::invalid("candidate IDs exceed 1048576 UTF-8 bytes"));
		}
		candidate_ids.push(id);
	}
	let offset = cursor.u32()? as usize;
	let limit = cursor.u32()? as usize;
	let exact_total = cursor.boolean()?;
	let budget_milliseconds = cursor.u32()?;
	cursor.finish()?;
	if budget_milliseconds == 0 || budget_milliseconds > MAX_SEARCH_BUDGET_MILLISECONDS {
		return Err(FulltextError::invalid(format!(
			"search budget must be between 1 and {MAX_SEARCH_BUDGET_MILLISECONDS} milliseconds"
		)));
	}
	if limit == 0 || offset.saturating_add(limit) > MAX_SEARCH_WINDOW {
		return Err(FulltextError::invalid("search window must be between 1 and 10000"));
	}
	if matches!(mode, SearchMode::Prefix | SearchMode::FuzzyPrefix) && (offset != 0 || limit > MAX_AUTOCOMPLETE_RESULTS)
	{
		return Err(FulltextError::invalid(
			"prefix search requires offset zero and a limit no greater than 100",
		));
	}
	Ok(SearchRequest {
		text,
		mode,
		fields,
		candidate_ids: has_candidates.then_some(candidate_ids),
		offset,
		limit,
		exact_total,
		budget_milliseconds,
	})
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
		let id = cursor.string()?;
		if id.is_empty() {
			return Err(FulltextError::invalid("candidate IDs must not be empty"));
		}
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
		let id = cursor.string()?;
		if id.is_empty() {
			return Err(FulltextError::invalid("trace record IDs must not be empty"));
		}
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
			text,
			mode,
			fields,
			candidate_ids: has_candidates.then_some(candidate_ids),
			offset: 0,
			limit: record_count,
			exact_total: false,
			budget_milliseconds,
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
	if config.analyzer != "english@1" {
		return Err(FulltextError::invalid("only analyzer english@1 is supported"));
	}
	let mut names = std::collections::HashSet::with_capacity(config.fields.len());
	for field in &config.fields {
		if field.name.is_empty() || field.name.starts_with("__fulltext_") || !names.insert(field.name.as_str()) {
			return Err(FulltextError::invalid(
				"field names must be non-empty, unique, and outside the reserved __fulltext_ namespace",
			));
		}
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

	fn string(&mut self) -> Result<String> {
		let bytes = self.bytes()?;
		String::from_utf8(bytes.to_vec()).map_err(|_| FulltextError::invalid("packed string is not valid UTF-8"))
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
	fn reset_frame_contains_only_path_and_logical_index_id() {
		let mut bytes = b"FTRX\x02\x00".to_vec();
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
		let mut bytes = b"FTIP\x02\x00".to_vec();
		for value in ["/tmp/index", "products", "one", "english@1"] {
			bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
			bytes.extend_from_slice(value.as_bytes());
		}
		bytes.extend_from_slice(&[1, 1, 0]);
		bytes.extend_from_slice(&1u16.to_le_bytes());
		bytes.extend_from_slice(&5u32.to_le_bytes());
		bytes.extend_from_slice(b"title");
		bytes.extend_from_slice(&1f32.to_le_bytes());

		let decoded = decode_inspect(&bytes).unwrap();
		assert_eq!(decoded.path, "/tmp/index");
		assert_eq!(decoded.identity.fields[0].name, "title");
		assert_eq!(decode_open(&bytes).unwrap_err().code, "E_INVALID_ARGUMENT");

		bytes.push(0);
		assert_eq!(decode_inspect(&bytes).unwrap_err().code, "E_INVALID_ARGUMENT");
	}

	#[test]
	fn rejects_counts_before_allocating() {
		let mut bytes = b"FTMB\x02\x00".to_vec();
		bytes.extend_from_slice(&u32::MAX.to_le_bytes());
		bytes.extend_from_slice(&0u32.to_le_bytes());
		assert_eq!(decode_batch(&bytes).unwrap_err().code, "E_INVALID_ARGUMENT");
	}

	#[test]
	fn distinguishes_batch_size_from_invalid_encoding() {
		let bytes = b"FTMB\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00";
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
			analyzer: "english@1".to_owned(),
			stop_words: true,
			positions: true,
			surface_terms: false,
		};
		let limits = Limits {
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
	fn rejects_nested_counts_before_allocating() {
		let mut fields = b"FTMB\x02\x00".to_vec();
		fields.extend_from_slice(&1u32.to_le_bytes());
		fields.extend_from_slice(&0u32.to_le_bytes());
		fields.extend_from_slice(&0u32.to_le_bytes());
		fields.extend_from_slice(&u16::MAX.to_le_bytes());
		assert_eq!(decode_batch(&fields).unwrap_err().code, "E_INVALID_ARGUMENT");

		let mut values = b"FTMB\x02\x00".to_vec();
		values.extend_from_slice(&1u32.to_le_bytes());
		values.extend_from_slice(&0u32.to_le_bytes());
		values.extend_from_slice(&0u32.to_le_bytes());
		values.extend_from_slice(&1u16.to_le_bytes());
		values.extend_from_slice(&0u32.to_le_bytes());
		values.extend_from_slice(&u16::MAX.to_le_bytes());
		assert_eq!(decode_batch(&values).unwrap_err().code, "E_INVALID_ARGUMENT");
	}
}
