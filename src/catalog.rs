use crate::{
	error::{FulltextError, Result},
	protocol::{FilterFieldConfig, FilterFieldType},
};
use std::{collections::HashSet, time::Instant};
use tantivy::aggregation::{
	agg_req::{Aggregation, AggregationVariants},
	agg_result::{AggregationResult as NativeResult, BucketResult, MetricResult},
	bucket::{HistogramAggregation, RangeAggregation, RangeAggregationRange, TermsAggregation},
	metric::StatsAggregation,
	AggContextParams, AggregationCollector, AggregationError, AggregationLimitsGuard, Key,
};
use tantivy::collector::sort_key::{ComparatorEnum, SortBySimilarityScore, SortByStaticFastValue, SortByString};
use tantivy::collector::{Collector, Count, SegmentCollector, TopDocs};
use tantivy::{
	query::{Query, Weight},
	tokenizer::TokenizerManager,
	DocAddress, Score, Searcher, SegmentReader, TantivyError,
};
pub const MAX_AGGREGATIONS: usize = 8;
pub const MAX_AGGREGATION_BUCKETS: usize = 1024;
pub const MAX_TERMS_SIZE: u32 = 100;
pub const MAX_AGGREGATION_MEMORY_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_SORT_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const MAX_AGGREGATION_RESULT_BYTES: usize = 8 * 1024 * 1024;
#[derive(Clone, Debug, PartialEq)]
pub enum SortDirection {
	Asc,
	Desc,
}
#[derive(Clone, Debug, PartialEq)]
pub enum MissingOrder {
	First,
	Last,
}
#[derive(Clone, Debug, PartialEq)]
pub struct SortRequest {
	pub field: String,
	pub direction: SortDirection,
	pub missing: MissingOrder,
}
#[derive(Clone, Debug, PartialEq)]
pub struct AggregationRequest {
	pub name: String,
	pub field: String,
	pub kind: AggregationKind,
}
#[derive(Clone, Debug, PartialEq)]
pub enum AggregationKind {
	Terms { size: u32 },
	Stats,
	Histogram { interval: f64 },
	Range { ranges: Vec<(Option<f64>, Option<f64>)> },
}
#[derive(Clone, Debug, PartialEq)]
pub struct AggregationResult {
	pub name: String,
	pub value: AggregationValue,
}
#[derive(Clone, Debug, PartialEq)]
pub struct TermBucket {
	pub key: String,
	pub count: u64,
}
#[derive(Clone, Debug, PartialEq)]
pub struct NumericBucket {
	pub key: f64,
	pub count: u64,
}
#[derive(Clone, Debug, PartialEq)]
pub struct RangeBucket {
	pub key: String,
	pub from: Option<f64>,
	pub to: Option<f64>,
	pub count: u64,
}
#[derive(Clone, Debug, PartialEq)]
pub enum AggregationValue {
	Terms {
		buckets: Vec<TermBucket>,
		sum_other_doc_count: u64,
		doc_count_error_upper_bound: Option<u64>,
	},
	Stats {
		count: u64,
		sum: f64,
		min: Option<f64>,
		max: Option<f64>,
		avg: Option<f64>,
	},
	Histogram {
		buckets: Vec<NumericBucket>,
	},
	Range {
		buckets: Vec<RangeBucket>,
	},
}
fn resource(message: impl Into<String>) -> FulltextError {
	FulltextError::new("E_RESOURCE_LIMIT", message)
}
fn field<'a>(name: &str, fields: &'a [FilterFieldConfig]) -> Result<(usize, &'a FilterFieldConfig)> {
	fields
		.iter()
		.enumerate()
		.find(|(_, f)| f.name == name)
		.ok_or_else(|| FulltextError::invalid(format!("Unknown filter field: {name}")))
}
pub fn validate_requests(
	sort: Option<&SortRequest>,
	aggs: &[AggregationRequest],
	fields: &[FilterFieldConfig],
) -> Result<()> {
	if let Some(sort) = sort {
		if field(&sort.field, fields)?.1.field_type == FilterFieldType::Boolean {
			return Err(FulltextError::invalid("Sorting requires a string or number field"));
		}
	}
	if aggs.len() > MAX_AGGREGATIONS {
		return Err(FulltextError::invalid("Too many aggregations"));
	}
	let mut names = HashSet::new();
	for agg in aggs {
		if agg.name.is_empty() || agg.name.len() > 128 || !names.insert(&agg.name) {
			return Err(FulltextError::invalid(
				"Aggregation names must be unique and contain from 1 through 128 UTF-8 bytes",
			));
		}
		let (_, config) = field(&agg.field, fields)?;
		match &agg.kind {
			AggregationKind::Terms { size } => {
				if config.field_type != FilterFieldType::String || *size == 0 || *size > MAX_TERMS_SIZE {
					return Err(FulltextError::invalid(
						"Terms requires a string field and size from 1 through 100",
					));
				}
			}
			kind => {
				if config.field_type != FilterFieldType::Number {
					return Err(FulltextError::invalid("Numeric aggregations require a number field"));
				}
				match kind {
					AggregationKind::Histogram { interval } if !interval.is_finite() || *interval <= 0.0 => {
						return Err(FulltextError::invalid("Histogram interval must be finite and positive"))
					}
					AggregationKind::Range { ranges } => {
						if ranges.is_empty() || ranges.len() > 100 {
							return Err(FulltextError::invalid(
								"Range requires from 1 through 100 contiguous ranges",
							));
						}
						for (i, (from, to)) in ranges.iter().enumerate() {
							if from.is_some_and(|n| !n.is_finite())
								|| to.is_some_and(|n| !n.is_finite())
								|| matches!((from,to),(Some(a),Some(b)) if a>=b)
								|| (i > 0 && (from.is_none() || *from != ranges[i - 1].1))
								|| (i + 1 < ranges.len() && to.is_none())
							{
								return Err(FulltextError::invalid(
									"Ranges must be finite, ascending, contiguous, and nonoverlapping",
								));
							}
						}
					}
					_ => {}
				}
			}
		}
	}
	Ok(())
}
fn native_error(error: TantivyError) -> FulltextError {
	match &error {
		TantivyError::AggregationError(
			AggregationError::MemoryExceeded { .. } | AggregationError::BucketLimitExceeded { .. },
		) => resource(error.to_string()),
		TantivyError::InvalidArgument(message) if message == "fulltext catalog deadline exceeded" => {
			FulltextError::new("E_TIMEOUT", "Search deadline exceeded")
		}
		TantivyError::AggregationError(AggregationError::InvalidRequest(_)) | TantivyError::InvalidArgument(_) => {
			FulltextError::invalid(error.to_string())
		}
		_ => FulltextError::native(error),
	}
}
fn check_deadline(deadline: Option<Instant>) -> tantivy::Result<()> {
	if deadline.is_some_and(|d| Instant::now() >= d) {
		return Err(TantivyError::InvalidArgument(
			"fulltext catalog deadline exceeded".to_owned(),
		));
	}
	Ok(())
}
struct DeadlineCollector<C> {
	inner: C,
	deadline: Option<Instant>,
}
impl<C: Collector> Collector for DeadlineCollector<C> {
	type Fruit = C::Fruit;
	type Child = C::Child;
	fn check_schema(&self, schema: &tantivy::schema::Schema) -> tantivy::Result<()> {
		self.inner.check_schema(schema)
	}
	fn for_segment(&self, ordinal: u32, reader: &SegmentReader) -> tantivy::Result<Self::Child> {
		check_deadline(self.deadline)?;
		self.inner.for_segment(ordinal, reader)
	}
	fn requires_scoring(&self) -> bool {
		self.inner.requires_scoring()
	}
	fn merge_fruits(&self, fruits: Vec<<Self::Child as SegmentCollector>::Fruit>) -> tantivy::Result<Self::Fruit> {
		check_deadline(self.deadline)?;
		let result = self.inner.merge_fruits(fruits)?;
		check_deadline(self.deadline)?;
		Ok(result)
	}
	fn collect_segment(
		&self,
		weight: &dyn Weight,
		ordinal: u32,
		reader: &SegmentReader,
	) -> tantivy::Result<<Self::Child as SegmentCollector>::Fruit> {
		if self.deadline.is_none() {
			return self.inner.collect_segment(weight, ordinal, reader);
		}
		check_deadline(self.deadline)?;
		let mut collector = self.for_segment(ordinal, reader)?;
		let mut scorer = weight.scorer(reader, 1.0)?;
		let alive = reader.alive_bitset();
		let mut visited = 0usize;
		let mut doc = scorer.doc();
		while doc != tantivy::TERMINATED {
			if visited.is_multiple_of(1024) {
				check_deadline(self.deadline)?;
			}
			if alive.is_none_or(|a| a.is_alive(doc)) {
				collector.collect(doc, if self.requires_scoring() { scorer.score() } else { 0.0 });
			}
			visited += 1;
			doc = scorer.advance();
		}
		check_deadline(self.deadline)?;
		Ok(collector.harvest())
	}
}
pub fn count_matches(searcher: &Searcher, query: &dyn Query, deadline: Option<Instant>) -> Result<u64> {
	check_deadline(deadline).map_err(native_error)?;
	searcher
		.search(query, &DeadlineCollector { inner: Count, deadline })
		.map(|n| n as u64)
		.map_err(native_error)
}
pub fn sorted_hits(
	searcher: &Searcher,
	query: &dyn Query,
	sort: &SortRequest,
	fields: &[FilterFieldConfig],
	offset: usize,
	limit: usize,
	deadline: Option<Instant>,
) -> Result<Vec<((Score, String), DocAddress)>> {
	validate_requests(Some(sort), &[], fields)?;
	check_deadline(deadline).map_err(native_error)?;
	if limit == 0 {
		return Ok(Vec::new());
	}
	let (index, config) = field(&sort.field, fields)?;
	let end = offset
		.checked_add(limit)
		.ok_or_else(|| resource("Sort window overflow"))?;
	let key_bytes = 4096
		+ 128 + if config.field_type == FilterFieldType::String {
		65530
	} else {
		8
	};
	if end
		.checked_mul(searcher.segment_readers().len())
		.and_then(|n| n.checked_mul(key_bytes))
		.is_none_or(|n| n > MAX_SORT_MEMORY_BYTES)
	{
		return Err(resource("Sort window exceeds retained key memory budget"));
	}
	let comparator = match (&sort.direction, &sort.missing) {
		(SortDirection::Asc, MissingOrder::First) => ComparatorEnum::Reverse,
		(SortDirection::Asc, MissingOrder::Last) => ComparatorEnum::ReverseNoneLower,
		(SortDirection::Desc, MissingOrder::First) => ComparatorEnum::NaturalNoneHigher,
		(SortDirection::Desc, MissingOrder::Last) => ComparatorEnum::Natural,
	};
	let name = format!("__fulltext_filter_{index}");
	let id = || (SortByString::for_field("__fulltext_id"), tantivy::Order::Asc);
	let hits = if config.field_type == FilterFieldType::String {
		let collector = TopDocs::for_doc_range(offset..end).order_by((
			(SortByString::for_field(name), comparator),
			(id(), SortBySimilarityScore),
		));
		searcher
			.search(
				query,
				&DeadlineCollector {
					inner: collector,
					deadline,
				},
			)
			.map_err(native_error)?
			.into_iter()
			.map(|((_, (id, score)), doc)| (id, score, doc))
			.collect::<Vec<_>>()
	} else {
		let collector = TopDocs::for_doc_range(offset..end).order_by((
			(SortByStaticFastValue::<f64>::for_field(name), comparator),
			(id(), SortBySimilarityScore),
		));
		searcher
			.search(
				query,
				&DeadlineCollector {
					inner: collector,
					deadline,
				},
			)
			.map_err(native_error)?
			.into_iter()
			.map(|((_, (id, score)), doc)| (id, score, doc))
			.collect::<Vec<_>>()
	};
	hits.into_iter()
		.map(|(id, score, doc)| {
			Ok((
				(
					score,
					id.ok_or_else(|| FulltextError::new("E_INDEX_CORRUPT", "Sort hit has no ID"))?,
				),
				doc,
			))
		})
		.collect()
}
pub fn aggregate(
	searcher: &Searcher,
	query: &dyn Query,
	requests: &[AggregationRequest],
	fields: &[FilterFieldConfig],
	tokenizers: TokenizerManager,
	deadline: Option<Instant>,
) -> Result<Vec<AggregationResult>> {
	validate_requests(None, requests, fields)?;
	check_deadline(deadline).map_err(native_error)?;
	if requests.is_empty() {
		return Ok(Vec::new());
	}
	let mut term_bytes = 0usize;
	let mut native: tantivy::aggregation::agg_req::Aggregations = Default::default();
	for request in requests {
		let (index, _) = field(&request.field, fields)?;
		let name = format!("__fulltext_filter_{index}");
		let agg = match &request.kind {
			AggregationKind::Terms { size } => {
				let bytes = (*size as usize)
					.checked_mul(searcher.segment_readers().len())
					.and_then(|n| n.checked_mul(65530 + 128))
					.ok_or_else(|| resource("Terms key memory overflow"))?;
				term_bytes = term_bytes
					.checked_add(bytes)
					.ok_or_else(|| resource("Terms key memory overflow"))?;
				if term_bytes > MAX_AGGREGATION_MEMORY_BYTES as usize {
					return Err(resource("Terms keys exceed materialization memory budget"));
				}
				AggregationVariants::Terms(TermsAggregation {
					field: name,
					size: Some(*size),
					segment_size: Some(*size),
					show_term_doc_count_error: Some(true),
					min_doc_count: Some(1),
					order: None,
					missing: None,
					include: None,
					exclude: None,
				})
			}
			AggregationKind::Stats => AggregationVariants::Stats(StatsAggregation::from_field_name(name)),
			AggregationKind::Histogram { interval } => {
				for segment in searcher.segment_readers() {
					check_deadline(deadline).map_err(native_error)?;
					let column = segment.fast_fields().f64(&name).map_err(native_error)?;
					if column.values.num_vals() > 0 {
						for value in [column.min_value(), column.max_value()] {
							let position = (value / interval).floor();
							if !position.is_finite() || position < i64::MIN as f64 || position >= i64::MAX as f64 {
								return Err(resource("Histogram values exceed representable bucket positions"));
							}
						}
					}
				}
				AggregationVariants::Histogram(HistogramAggregation {
					field: name,
					interval: *interval,
					offset: None,
					min_doc_count: Some(1),
					hard_bounds: None,
					extended_bounds: None,
					keyed: false,
					is_normalized_to_ns: false,
				})
			}
			AggregationKind::Range { ranges } => AggregationVariants::Range(RangeAggregation {
				field: name,
				ranges: ranges
					.iter()
					.map(|(from, to)| RangeAggregationRange {
						key: None,
						from: *from,
						to: *to,
					})
					.collect(),
				keyed: false,
			}),
		};
		native.insert(
			request.name.clone(),
			Aggregation {
				agg,
				sub_aggregation: Default::default(),
			},
		);
	}
	let context = AggContextParams::new(
		AggregationLimitsGuard::new(Some(MAX_AGGREGATION_MEMORY_BYTES), Some(MAX_AGGREGATION_BUCKETS as u32)),
		tokenizers,
	);
	let collector = AggregationCollector::from_aggs(native, context);
	let mut results = searcher
		.search(
			query,
			&DeadlineCollector {
				inner: collector,
				deadline,
			},
		)
		.map_err(native_error)?
		.0;
	let mut output = Vec::with_capacity(requests.len());
	for request in requests {
		let result = results
			.remove(&request.name)
			.ok_or_else(|| FulltextError::native("Missing aggregation result"))?;
		let value = match result {
			NativeResult::BucketResult(BucketResult::Terms {
				buckets,
				sum_other_doc_count,
				doc_count_error_upper_bound,
			}) => AggregationValue::Terms {
				buckets: buckets
					.into_iter()
					.map(|b| {
						Ok(TermBucket {
							key: string_key(b.key)?,
							count: b.doc_count,
						})
					})
					.collect::<Result<_>>()?,
				sum_other_doc_count,
				doc_count_error_upper_bound,
			},
			NativeResult::MetricResult(MetricResult::Stats(s)) => AggregationValue::Stats {
				count: s.count,
				sum: s.sum,
				min: s.min,
				max: s.max,
				avg: s.avg,
			},
			NativeResult::BucketResult(BucketResult::Histogram { buckets }) => AggregationValue::Histogram {
				buckets: buckets
					.iter()
					.map(|b| {
						Ok(NumericBucket {
							key: number_key(&b.key)?,
							count: b.doc_count,
						})
					})
					.collect::<Result<_>>()?,
			},
			NativeResult::BucketResult(BucketResult::Range { buckets }) => AggregationValue::Range {
				buckets: buckets
					.iter()
					.map(|b| {
						Ok(RangeBucket {
							key: string_key(b.key.clone())?,
							from: b.from,
							to: b.to,
							count: b.doc_count,
						})
					})
					.collect::<Result<_>>()?,
			},
			_ => return Err(FulltextError::native("Unexpected aggregation result kind")),
		};
		output.push(AggregationResult {
			name: request.name.clone(),
			value,
		});
	}
	encode_results(&output)?;
	check_deadline(deadline).map_err(native_error)?;
	Ok(output)
}
fn string_key(key: Key) -> Result<String> {
	if let Key::Str(s) = key {
		Ok(s)
	} else {
		Err(FulltextError::native("Expected string bucket key"))
	}
}
fn number_key(key: &Key) -> Result<f64> {
	match key {
		Key::F64(n) => Ok(*n),
		Key::U64(n) => Ok(*n as f64),
		Key::I64(n) => Ok(*n as f64),
		_ => Err(FulltextError::native("Expected numeric bucket key")),
	}
}
struct Encoder(Vec<u8>);
impl Encoder {
	fn bytes(&mut self, bytes: &[u8]) -> Result<()> {
		if bytes.len() > MAX_AGGREGATION_RESULT_BYTES - self.0.len() {
			return Err(FulltextError::new(
				"E_RESULT_TOO_LARGE",
				"Aggregation response exceeds byte budget",
			));
		}
		self.0.extend_from_slice(bytes);
		Ok(())
	}
	fn string(&mut self, s: &str) -> Result<()> {
		let len = u32::try_from(s.len()).map_err(|_| resource("Aggregation key too large"))?;
		self.bytes(&len.to_le_bytes())?;
		self.bytes(s.as_bytes())
	}
	fn count(&mut self, n: u64) -> Result<()> {
		self.bytes(&n.to_le_bytes())
	}
	fn number(&mut self, n: f64) -> Result<()> {
		if !n.is_finite() {
			return Err(resource("Aggregation numeric result is not finite"));
		}
		self.bytes(&n.to_le_bytes())
	}
	fn optional_number(&mut self, n: Option<f64>) -> Result<()> {
		self.bytes(&[u8::from(n.is_some())])?;
		if let Some(n) = n {
			self.number(n)?;
		}
		Ok(())
	}
}
pub fn encode_results(results: &[AggregationResult]) -> Result<Vec<u8>> {
	if results.len() > MAX_AGGREGATIONS {
		return Err(resource("Too many aggregation results"));
	}
	let mut w = Encoder(Vec::new());
	w.bytes(&[results.len() as u8])?;
	let mut total = 0usize;
	for result in results {
		let (kind, buckets) = match &result.value {
			AggregationValue::Terms { buckets, .. } => (0, buckets.len()),
			AggregationValue::Stats { .. } => (1, 0),
			AggregationValue::Histogram { buckets } => (2, buckets.len()),
			AggregationValue::Range { buckets } => (3, buckets.len()),
		};
		total += buckets;
		if total > MAX_AGGREGATION_BUCKETS {
			return Err(resource("Aggregation result bucket limit exceeded"));
		}
		w.string(&result.name)?;
		w.bytes(&[kind])?;
		if kind != 1 {
			w.bytes(&(buckets as u16).to_le_bytes())?;
		}
		match &result.value {
			AggregationValue::Terms {
				buckets,
				sum_other_doc_count,
				doc_count_error_upper_bound,
			} => {
				for b in buckets {
					w.string(&b.key)?;
					w.count(b.count)?;
				}
				w.count(*sum_other_doc_count)?;
				w.bytes(&[u8::from(doc_count_error_upper_bound.is_some())])?;
				if let Some(n) = doc_count_error_upper_bound {
					w.count(*n)?;
				}
			}
			AggregationValue::Stats {
				count,
				sum,
				min,
				max,
				avg,
			} => {
				w.count(*count)?;
				w.number(*sum)?;
				w.optional_number(*min)?;
				w.optional_number(*max)?;
				w.optional_number(*avg)?;
			}
			AggregationValue::Histogram { buckets } => {
				for b in buckets {
					w.number(b.key)?;
					w.count(b.count)?;
				}
			}
			AggregationValue::Range { buckets } => {
				for b in buckets {
					w.string(&b.key)?;
					w.optional_number(b.from)?;
					w.optional_number(b.to)?;
					w.count(b.count)?;
				}
			}
		}
	}
	Ok(w.0)
}

#[cfg(test)]
mod tests {
	use super::*;
	use tantivy::query::AllQuery;
	use tantivy::{
		doc,
		schema::{Schema, FAST, INDEXED, STRING},
		Index, TantivyDocument, Term,
	};
	fn fields() -> Vec<FilterFieldConfig> {
		vec![
			FilterFieldConfig {
				name: "category".into(),
				field_type: FilterFieldType::String,
			},
			FilterFieldConfig {
				name: "price".into(),
				field_type: FilterFieldType::Number,
			},
		]
	}
	fn fixture() -> Index {
		let mut schema = Schema::builder();
		let id = schema.add_text_field("__fulltext_id", STRING | FAST);
		let category = schema.add_text_field("__fulltext_filter_0", STRING | FAST);
		let price = schema.add_f64_field("__fulltext_filter_1", INDEXED | FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer = index.writer::<TantivyDocument>(20_000_000).unwrap();
		writer.set_merge_policy(Box::new(tantivy::indexer::NoMergePolicy));
		for record in [
			doc!(id=>"b",category=>"z",category=>"a",price=>20.0,price=>1.0),
			doc!(id=>"a",category=>"a",price=>20.0),
			doc!(id=>"c",category=>"a",price=>30.0),
			doc!(id=>"missing"),
			doc!(id=>"deleted",category=>"gone",price=>999.0),
		] {
			writer.add_document(record).unwrap();
		}
		writer.commit().unwrap();
		writer.add_document(doc!(id=>"d",category=>"b",price=>10.0)).unwrap();
		writer.commit().unwrap();
		writer.delete_term(Term::from_field_text(id, "deleted"));
		writer.commit().unwrap();
		drop(writer);
		index
	}
	fn ids(
		index: &Index,
		field: &str,
		direction: SortDirection,
		missing: MissingOrder,
		offset: usize,
		limit: usize,
	) -> Vec<String> {
		let searcher = index.reader().unwrap().searcher();
		sorted_hits(
			&searcher,
			&AllQuery,
			&SortRequest {
				field: field.into(),
				direction,
				missing,
			},
			&fields(),
			offset,
			limit,
			Some(Instant::now() + std::time::Duration::from_secs(10)),
		)
		.unwrap()
		.into_iter()
		.map(|((_, id), _)| id)
		.collect()
	}
	#[test]
	fn native_sort_keeps_global_id_ties_missing_policy_and_first_value() {
		let index = fixture();
		assert_eq!(
			ids(&index, "price", SortDirection::Asc, MissingOrder::Last, 0, 10),
			["d", "a", "b", "c", "missing"]
		);
		assert_eq!(
			ids(&index, "price", SortDirection::Asc, MissingOrder::First, 0, 10),
			["missing", "d", "a", "b", "c"]
		);
		assert_eq!(
			ids(&index, "price", SortDirection::Desc, MissingOrder::Last, 0, 10),
			["c", "a", "b", "d", "missing"]
		);
		assert_eq!(
			ids(&index, "price", SortDirection::Desc, MissingOrder::First, 0, 10),
			["missing", "c", "a", "b", "d"]
		);
		assert_eq!(
			ids(&index, "price", SortDirection::Asc, MissingOrder::Last, 2, 2),
			["b", "c"]
		);
		assert_eq!(
			ids(&index, "category", SortDirection::Asc, MissingOrder::Last, 0, 10),
			["a", "c", "d", "b", "missing"]
		);
	}
	#[test]
	fn native_aggregations_cover_full_live_matching_set() {
		let index = fixture();
		let searcher = index.reader().unwrap().searcher();
		let requests = vec![
			AggregationRequest {
				name: "categories".into(),
				field: "category".into(),
				kind: AggregationKind::Terms { size: 10 },
			},
			AggregationRequest {
				name: "stats".into(),
				field: "price".into(),
				kind: AggregationKind::Stats,
			},
			AggregationRequest {
				name: "hist".into(),
				field: "price".into(),
				kind: AggregationKind::Histogram { interval: 10.0 },
			},
			AggregationRequest {
				name: "ranges".into(),
				field: "price".into(),
				kind: AggregationKind::Range {
					ranges: vec![(Some(10.0), Some(20.0)), (Some(20.0), Some(30.0))],
				},
			},
		];
		let results = aggregate(
			&searcher,
			&AllQuery,
			&requests,
			&fields(),
			index.tokenizers().clone(),
			None,
		)
		.unwrap();
		assert_eq!(count_matches(&searcher, &AllQuery, None).unwrap(), 5);
		match &results[0].value {
			AggregationValue::Terms { buckets, .. } => {
				assert_eq!(
					buckets[0],
					TermBucket {
						key: "a".into(),
						count: 3
					}
				);
				assert!(!buckets.iter().any(|b| b.key == "gone"));
			}
			_ => panic!(),
		}
		assert_eq!(
			results[1].value,
			AggregationValue::Stats {
				count: 5,
				sum: 81.0,
				min: Some(1.0),
				max: Some(30.0),
				avg: Some(16.2)
			}
		);
		assert_eq!(
			results[2].value,
			AggregationValue::Histogram {
				buckets: vec![
					NumericBucket { key: 0.0, count: 1 },
					NumericBucket { key: 10.0, count: 1 },
					NumericBucket { key: 20.0, count: 2 },
					NumericBucket { key: 30.0, count: 1 }
				]
			}
		);
		match &results[3].value {
			AggregationValue::Range { buckets } => {
				assert_eq!(buckets.len(), 4);
				assert_eq!(buckets.iter().map(|b| b.count).collect::<Vec<_>>(), [1, 1, 2, 1]);
			}
			_ => panic!(),
		}
		assert!(!encode_results(&results).unwrap().is_empty());
	}
	#[test]
	fn validation_deadlines_and_memory_limits_reject_before_collection() {
		let index = fixture();
		let searcher = index.reader().unwrap().searcher();
		let sort = SortRequest {
			field: "category".into(),
			direction: SortDirection::Asc,
			missing: MissingOrder::Last,
		};
		assert_eq!(
			sorted_hits(&searcher, &AllQuery, &sort, &fields(), 0, 10000, None)
				.unwrap_err()
				.code,
			"E_RESOURCE_LIMIT"
		);
		assert_eq!(
			count_matches(&searcher, &AllQuery, Some(Instant::now()))
				.unwrap_err()
				.code,
			"E_TIMEOUT"
		);
		let bad = AggregationRequest {
			name: "bad".into(),
			field: "price".into(),
			kind: AggregationKind::Histogram { interval: f64::NAN },
		};
		assert_eq!(
			validate_requests(None, &[bad], &fields()).unwrap_err().code,
			"E_INVALID_ARGUMENT"
		);
		let overflow = AggregationResult {
			name: "overflow".into(),
			value: AggregationValue::Stats {
				count: 2,
				sum: f64::INFINITY,
				min: None,
				max: None,
				avg: None,
			},
		};
		assert_eq!(encode_results(&[overflow]).unwrap_err().code, "E_RESOURCE_LIMIT");
		let oversized = AggregationResult {
			name: "large".into(),
			value: AggregationValue::Terms {
				buckets: vec![TermBucket {
					key: "x".repeat(MAX_AGGREGATION_RESULT_BYTES),
					count: 1,
				}],
				sum_other_doc_count: 0,
				doc_count_error_upper_bound: Some(0),
			},
		};
		assert_eq!(encode_results(&[oversized]).unwrap_err().code, "E_RESULT_TOO_LARGE");
	}
	#[test]
	fn histogram_rejects_saturating_native_positions() {
		let index = fixture();
		let searcher = index.reader().unwrap().searcher();
		let request = AggregationRequest {
			name: "tiny".into(),
			field: "price".into(),
			kind: AggregationKind::Histogram {
				interval: f64::MIN_POSITIVE,
			},
		};
		assert_eq!(
			aggregate(
				&searcher,
				&AllQuery,
				&[request],
				&fields(),
				index.tokenizers().clone(),
				None
			)
			.unwrap_err()
			.code,
			"E_RESOURCE_LIMIT"
		);
	}
	#[test]
	fn deadline_interrupts_an_active_collection() {
		use std::sync::{
			atomic::{AtomicUsize, Ordering},
			Arc,
		};
		struct Probe {
			calls: Arc<AtomicUsize>,
			deadline: Instant,
		}
		struct SegmentProbe {
			calls: Arc<AtomicUsize>,
			deadline: Instant,
		}
		impl SegmentCollector for SegmentProbe {
			type Fruit = ();
			fn collect(&mut self, _: u32, _: Score) {
				self.calls.fetch_add(1, Ordering::Relaxed);
				while Instant::now() < self.deadline {
					std::thread::yield_now();
				}
			}
			fn harvest(self) {}
		}
		impl Collector for Probe {
			type Fruit = ();
			type Child = SegmentProbe;
			fn for_segment(&self, _: u32, _: &SegmentReader) -> tantivy::Result<Self::Child> {
				Ok(SegmentProbe {
					calls: self.calls.clone(),
					deadline: self.deadline,
				})
			}
			fn requires_scoring(&self) -> bool {
				false
			}
			fn merge_fruits(&self, _: Vec<()>) -> tantivy::Result<()> {
				Ok(())
			}
		}
		let mut schema = Schema::builder();
		let id = schema.add_u64_field("id", FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer = index.writer::<TantivyDocument>(20_000_000).unwrap();
		for value in 0..2048u64 {
			writer.add_document(doc!(id=>value)).unwrap();
		}
		writer.commit().unwrap();
		drop(writer);
		let searcher = index.reader().unwrap().searcher();
		let calls = Arc::new(AtomicUsize::new(0));
		let deadline = Instant::now() + std::time::Duration::from_secs(1);
		let collector = DeadlineCollector {
			inner: Probe {
				calls: calls.clone(),
				deadline,
			},
			deadline: Some(deadline),
		};
		let error = searcher.search(&AllQuery, &collector).unwrap_err();
		assert_eq!(native_error(error).code, "E_TIMEOUT");
		assert!(calls.load(Ordering::Relaxed) > 0);
		assert!(calls.load(Ordering::Relaxed) <= 1024);
	}
	#[test]
	fn missing_metadata_segments_and_empty_indexes_are_supported() {
		let mut schema = Schema::builder();
		let id = schema.add_text_field("__fulltext_id", STRING | FAST);
		schema.add_text_field("__fulltext_filter_0", STRING | FAST);
		schema.add_f64_field("__fulltext_filter_1", INDEXED | FAST);
		let index = Index::create_in_ram(schema.build());
		let requests = vec![
			AggregationRequest {
				name: "terms".into(),
				field: "category".into(),
				kind: AggregationKind::Terms { size: 10 },
			},
			AggregationRequest {
				name: "stats".into(),
				field: "price".into(),
				kind: AggregationKind::Stats,
			},
			AggregationRequest {
				name: "hist".into(),
				field: "price".into(),
				kind: AggregationKind::Histogram { interval: 10.0 },
			},
			AggregationRequest {
				name: "range".into(),
				field: "price".into(),
				kind: AggregationKind::Range {
					ranges: vec![(None, Some(20.0)), (Some(20.0), None)],
				},
			},
		];
		for with_docs in [false, true] {
			if with_docs {
				let mut writer = index.writer::<TantivyDocument>(20_000_000).unwrap();
				writer.add_document(doc!(id=>"no-metadata")).unwrap();
				writer.commit().unwrap();
			}
			let searcher = index.reader().unwrap().searcher();
			let result = aggregate(
				&searcher,
				&AllQuery,
				&requests,
				&fields(),
				index.tokenizers().clone(),
				None,
			)
			.unwrap();
			assert_eq!(
				result[0].value,
				AggregationValue::Terms {
					buckets: vec![],
					sum_other_doc_count: 0,
					doc_count_error_upper_bound: Some(0)
				}
			);
			assert_eq!(
				result[1].value,
				AggregationValue::Stats {
					count: 0,
					sum: 0.0,
					min: None,
					max: None,
					avg: None
				}
			);
			assert_eq!(result[2].value, AggregationValue::Histogram { buckets: vec![] });
			assert_eq!(
				ids(&index, "price", SortDirection::Asc, MissingOrder::Last, 0, 10).len(),
				usize::from(with_docs)
			);
		}
	}
	#[test]
	fn direct_requests_reject_oversized_utf8_names_and_native_sum_overflow() {
		let bad = AggregationRequest {
			name: "é".repeat(65),
			field: "price".into(),
			kind: AggregationKind::Stats,
		};
		assert_eq!(
			validate_requests(None, &[bad], &fields()).unwrap_err().code,
			"E_INVALID_ARGUMENT"
		);
		let mut schema = Schema::builder();
		let price = schema.add_f64_field("__fulltext_filter_1", INDEXED | FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer = index.writer::<TantivyDocument>(20_000_000).unwrap();
		writer.add_document(doc!(price=>f64::MAX)).unwrap();
		writer.add_document(doc!(price=>f64::MAX)).unwrap();
		writer.commit().unwrap();
		drop(writer);
		let request = AggregationRequest {
			name: "stats".into(),
			field: "price".into(),
			kind: AggregationKind::Stats,
		};
		assert_eq!(
			aggregate(
				&index.reader().unwrap().searcher(),
				&AllQuery,
				&[request],
				&fields(),
				index.tokenizers().clone(),
				None
			)
			.unwrap_err()
			.code,
			"E_RESOURCE_LIMIT"
		);
	}
}
