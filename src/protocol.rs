use crate::error::{FulltextError, Result};

pub const PROTOCOL_VERSION: u16 = 1;
const MAX_STRING_BYTES: usize = 1 << 20;
const MAX_FIELDS: usize = 1_024;
const MUTATION_BATCH_HEADER_BYTES: usize = 14;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchOperator {
	Any,
	All,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchRequest {
	pub text: String,
	pub operator: SearchOperator,
	pub fields: Vec<String>,
	pub offset: usize,
	pub limit: usize,
	pub exact_total: bool,
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
	let _ = Cursor::new(bytes, *b"FTSQ")?;
	Ok(())
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
	let operator = match cursor.u8()? {
		0 => SearchOperator::Any,
		1 => SearchOperator::All,
		_ => return Err(FulltextError::invalid("unknown search operator")),
	};
	let field_count = cursor.u16()? as usize;
	if field_count > MAX_FIELDS {
		return Err(FulltextError::invalid("search field count exceeds 1024"));
	}
	let mut fields = Vec::with_capacity(field_count);
	for _ in 0..field_count {
		fields.push(cursor.string()?);
	}
	let offset = cursor.u32()? as usize;
	let limit = cursor.u32()? as usize;
	let exact_total = cursor.boolean()?;
	cursor.finish()?;
	if text.trim().is_empty() {
		return Err(FulltextError::invalid("search text must not be empty"));
	}
	if limit == 0 || offset.saturating_add(limit) > 10_000 {
		return Err(FulltextError::invalid("search window must be between 1 and 10000"));
	}
	Ok(SearchRequest {
		text,
		operator,
		fields,
		offset,
		limit,
		exact_total,
	})
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
	if limits.max_batch_bytes <= MUTATION_BATCH_HEADER_BYTES
		|| limits.max_batch_bytes > limits.max_queued_bytes
	{
		return Err(FulltextError::invalid(
			"maxBatchBytes must exceed the mutation batch header and be no larger than maxQueuedBytes",
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
		if field.name.is_empty() || field.name == "__fulltext_id" || !names.insert(field.name.as_str()) {
			return Err(FulltextError::invalid(
				"field names must be non-empty, unique, and not reserved",
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
		let mut bytes = b"FTRX\x01\x00".to_vec();
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
		let mut bytes = b"FTIP\x01\x00".to_vec();
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
		let mut bytes = b"FTMB\x01\x00".to_vec();
		bytes.extend_from_slice(&u32::MAX.to_le_bytes());
		bytes.extend_from_slice(&0u32.to_le_bytes());
		assert_eq!(decode_batch(&bytes).unwrap_err().code, "E_INVALID_ARGUMENT");
	}

	#[test]
	fn distinguishes_batch_size_from_invalid_encoding() {
		let bytes = b"FTMB\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00";
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
	fn rejects_invalid_utf8() {
		let mut bytes = b"FTSQ\x01\x00".to_vec();
		bytes.extend_from_slice(&1u32.to_le_bytes());
		bytes.push(0xff);
		assert_eq!(decode_search(&bytes).unwrap_err().code, "E_INVALID_ARGUMENT");
	}

	#[test]
	fn rejects_nested_counts_before_allocating() {
		let mut fields = b"FTMB\x01\x00".to_vec();
		fields.extend_from_slice(&1u32.to_le_bytes());
		fields.extend_from_slice(&0u32.to_le_bytes());
		fields.extend_from_slice(&0u32.to_le_bytes());
		fields.extend_from_slice(&u16::MAX.to_le_bytes());
		assert_eq!(decode_batch(&fields).unwrap_err().code, "E_INVALID_ARGUMENT");

		let mut values = b"FTMB\x01\x00".to_vec();
		values.extend_from_slice(&1u32.to_le_bytes());
		values.extend_from_slice(&0u32.to_le_bytes());
		values.extend_from_slice(&0u32.to_le_bytes());
		values.extend_from_slice(&1u16.to_le_bytes());
		values.extend_from_slice(&0u32.to_le_bytes());
		values.extend_from_slice(&u16::MAX.to_le_bytes());
		assert_eq!(decode_batch(&values).unwrap_err().code, "E_INVALID_ARGUMENT");
	}
}
