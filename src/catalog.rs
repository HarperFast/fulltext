use crate::{
	error::{FulltextError, Result},
	protocol::{FilterFieldConfig, FilterFieldType},
};
use std::{
	collections::{HashMap, HashSet, VecDeque},
	sync::{Mutex, OnceLock},
	time::Instant,
};
use tantivy::aggregation::{
	agg_req::{Aggregation, AggregationVariants},
	agg_result::{AggregationResult as NativeResult, BucketResult, MetricResult},
	bucket::{HistogramAggregation, RangeAggregation, RangeAggregationRange, TermsAggregation},
	metric::StatsAggregation,
	AggContextParams, AggregationCollector, AggregationError, AggregationLimitsGuard, Key,
};
use tantivy::collector::sort_key::{
	ComparatorEnum, SortBySimilarityScore, SortByStaticFastValue, SortByString, SortKeyComputer,
};
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
pub const MAX_TERM_KEY_BYTES: usize = 16 * 1024 * 1024;
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
#[derive(Clone, Copy, Debug)]
pub(crate) struct ValidatedCatalog<'a> {
	sort: Option<&'a SortRequest>,
	requests: &'a [AggregationRequest],
	fields: &'a [FilterFieldConfig],
}
pub(crate) fn validate_requests<'a>(
	sort: Option<&'a SortRequest>,
	aggs: &'a [AggregationRequest],
	fields: &'a [FilterFieldConfig],
) -> Result<ValidatedCatalog<'a>> {
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
	Ok(ValidatedCatalog {
		sort,
		requests: aggs,
		fields,
	})
}
fn native_error(error: TantivyError) -> FulltextError {
	match &error {
		TantivyError::AggregationError(
			AggregationError::MemoryExceeded { .. } | AggregationError::BucketLimitExceeded { .. },
		) => resource(error.to_string()),
		TantivyError::InvalidArgument(message)
			if message == "Histogram values exceed representable bucket positions" =>
		{
			resource(message)
		}
		TantivyError::InvalidArgument(message) if message == crate::error::SEARCH_TIMEOUT_MESSAGE => {
			FulltextError::new("E_TIMEOUT", crate::error::SEARCH_TIMEOUT_MESSAGE)
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
			crate::error::SEARCH_TIMEOUT_MESSAGE.to_owned(),
		));
	}
	Ok(())
}
fn histogram_position_safe(value: f64, interval: f64) -> bool {
	let position = (value / interval).floor();
	position.is_finite() && position >= i64::MIN as f64 && position < i64::MAX as f64
}
fn validate_histogram_doc(histograms: &[(tantivy::fastfield::Column<f64>, f64)], doc: u32) -> tantivy::Result<()> {
	for (column, interval) in histograms {
		for value in column.values_for_doc(doc) {
			if !histogram_position_safe(value, *interval) {
				return Err(TantivyError::InvalidArgument(
					"Histogram values exceed representable bucket positions".into(),
				));
			}
		}
	}
	Ok(())
}
struct DeadlineCollector<C> {
	histograms: Vec<(String, f64)>,
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
		if self.deadline.is_none() && self.histograms.is_empty() {
			return self.inner.collect_segment(weight, ordinal, reader);
		}
		check_deadline(self.deadline)?;
		let mut collector = self.for_segment(ordinal, reader)?;
		let histograms = self
			.histograms
			.iter()
			.map(|(name, interval)| Ok((reader.fast_fields().f64(name)?, *interval)))
			.collect::<tantivy::Result<Vec<_>>>()?
			.into_iter()
			.filter(|(column, interval)| {
				column.values.num_vals() > 0
					&& [column.min_value(), column.max_value()]
						.into_iter()
						.any(|value| !histogram_position_safe(value, *interval))
			})
			.collect::<Vec<_>>();
		let mut scorer = weight.scorer(reader, 1.0)?;
		let alive = reader.alive_bitset();
		let scoring = self.requires_scoring();
		let mut visited = 0usize;
		if scoring {
			let mut doc = scorer.doc();
			while doc != tantivy::TERMINATED {
				if visited.is_multiple_of(1024) {
					check_deadline(self.deadline)?;
				}
				if alive.is_none_or(|alive| alive.is_alive(doc)) {
					validate_histogram_doc(&histograms, doc)?;
					collector.collect(doc, scorer.score());
				}
				visited += 1;
				doc = scorer.advance();
			}
		} else {
			let mut buffer = [0u32; tantivy::COLLECT_BLOCK_BUFFER_LEN];
			loop {
				if visited.is_multiple_of(1024 / tantivy::COLLECT_BLOCK_BUFFER_LEN) {
					check_deadline(self.deadline)?;
				}
				let count = scorer.fill_buffer(&mut buffer);
				if count == 0 {
					break;
				}
				visited += 1;
				let count = if let Some(alive) = alive {
					let mut retained = 0;
					for index in 0..count {
						let doc = buffer[index];
						if alive.is_alive(doc) {
							buffer[retained] = doc;
							retained += 1;
						}
					}
					retained
				} else {
					count
				};
				if !histograms.is_empty() {
					for &doc in &buffer[..count] {
						validate_histogram_doc(&histograms, doc)?;
					}
				}
				if count > 0 {
					collector.collect_block(&buffer[..count]);
				}
			}
		}
		check_deadline(self.deadline)?;
		Ok(collector.harvest())
	}
}
pub(crate) fn count_matches(searcher: &Searcher, query: &dyn Query, deadline: Option<Instant>) -> tantivy::Result<u64> {
	check_deadline(deadline)?;
	searcher
		.search(
			query,
			&DeadlineCollector {
				inner: Count,
				deadline,
				histograms: vec![],
			},
		)
		.map(|n| n as u64)
}

const MAX_DICTIONARY_CACHE_ENTRIES: usize = 1024;
type DictionaryCacheKey = (tantivy::index::SegmentId, tantivy::schema::Field);
#[derive(Default)]
struct DictionaryCache {
	maxima: HashMap<DictionaryCacheKey, usize>,
	insertion_order: VecDeque<DictionaryCacheKey>,
}
impl DictionaryCache {
	fn insert(&mut self, key: DictionaryCacheKey, maximum: usize) {
		if self.maxima.contains_key(&key) {
			return;
		}
		if self.maxima.len() == MAX_DICTIONARY_CACHE_ENTRIES {
			if let Some(oldest) = self.insertion_order.pop_front() {
				self.maxima.remove(&oldest);
			}
		}
		self.insertion_order.push_back(key);
		self.maxima.insert(key, maximum);
	}
}
fn dictionary_cache() -> &'static Mutex<DictionaryCache> {
	static CACHE: OnceLock<Mutex<DictionaryCache>> = OnceLock::new();
	CACHE.get_or_init(|| Mutex::new(DictionaryCache::default()))
}
fn scan_dictionary_max(
	mut next_key_length: impl FnMut() -> Option<usize>,
	mut check: impl FnMut() -> Result<()>,
) -> Result<usize> {
	let mut maximum = 0;
	let mut visited = 0usize;
	loop {
		if visited.is_multiple_of(1024) {
			check()?;
		}
		let Some(length) = next_key_length() else {
			break;
		};
		maximum = maximum.max(length);
		visited += 1;
	}
	Ok(maximum)
}
fn max_dictionary_key(segment: &SegmentReader, name: &str, deadline: Option<Instant>) -> Result<usize> {
	max_dictionary_key_with_check(segment, name, || check_deadline(deadline).map_err(native_error))
}
fn max_dictionary_key_with_check(
	segment: &SegmentReader,
	name: &str,
	mut check: impl FnMut() -> Result<()>,
) -> Result<usize> {
	check()?;
	let key = (
		segment.segment_id(),
		segment.schema().get_field(name).map_err(native_error)?,
	);
	if let Ok(cache) = dictionary_cache().lock() {
		if let Some(maximum) = cache.maxima.get(&key) {
			return Ok(*maximum);
		}
	}
	let maximum = if let Some(column) = segment.fast_fields().str(name).map_err(native_error)? {
		let mut stream = column.dictionary().stream().map_err(FulltextError::native)?;
		scan_dictionary_max(|| stream.advance().then(|| stream.key().len()), &mut check)?
	} else {
		0
	};
	if let Ok(mut cache) = dictionary_cache().lock() {
		cache.insert(key, maximum);
	}
	check()?;
	Ok(maximum)
}
fn add_key_budget(total: &mut usize, count: usize, bytes: usize, limit: usize) -> Result<()> {
	*total = total
		.checked_add(
			count
				.checked_mul(bytes)
				.ok_or_else(|| resource("Retained key memory overflow"))?,
		)
		.ok_or_else(|| resource("Retained key memory overflow"))?;
	if *total > limit {
		return Err(resource("Retained keys exceed materialization memory budget"));
	}
	Ok(())
}
fn sort_buffer_bytes(end: usize, segments: usize, parallelism: usize) -> Result<usize> {
	let segment_key_bytes =
		std::mem::size_of::<tantivy::collector::ComparableDoc<(Option<u64>, (Option<u64>, Score)), u32>>();
	let global_key_bytes =
		std::mem::size_of::<tantivy::collector::ComparableDoc<(Option<String>, (Option<String>, Score)), DocAddress>>();
	end.checked_mul(2)
		.and_then(|n| n.checked_mul(segment_key_bytes))
		.and_then(|n| n.checked_mul(segments.min(parallelism)))
		.and_then(|n| n.checked_add(end.checked_mul(2)?.checked_mul(global_key_bytes)?))
		.ok_or_else(|| resource("Sort buffer memory overflow"))
}
fn check_sort_budget(
	searcher: &Searcher,
	query: &dyn Query,
	end: usize,
	index: usize,
	is_string: bool,
	deadline: Option<Instant>,
) -> Result<()> {
	let segments = searcher.segment_readers().len();
	let parallelism = match searcher.index().search_executor() {
		tantivy::Executor::SingleThread => 1,
		tantivy::Executor::ThreadPool(pool) => pool.current_num_threads(),
	};
	let fixed = sort_buffer_bytes(end, segments, parallelism)?;
	if fixed > MAX_SORT_MEMORY_BYTES {
		return Err(resource("Sort buffers exceed memory budget"));
	}
	let worst = crate::protocol::MAX_RECORD_ID_BYTES + 128 + if is_string { 65530 } else { 8 };
	let cheap = searcher.segment_readers().iter().try_fold(fixed, |total, s| {
		total.checked_add(end.min(s.num_docs() as usize).checked_mul(worst)?)
	});
	if cheap.is_some_and(|total| total <= MAX_SORT_MEMORY_BYTES) {
		return Ok(());
	}
	let name = format!("__fulltext_filter_{index}");
	let mut values = Vec::with_capacity(searcher.segment_readers().len());
	let mut total = Some(fixed);
	for segment in searcher.segment_readers() {
		let value = if is_string {
			max_dictionary_key(segment, &name, deadline)?
		} else {
			8
		};
		values.push(value);
		total = total.and_then(|total| {
			total.checked_add(
				end.min(segment.num_docs() as usize)
					.checked_mul(crate::protocol::MAX_RECORD_ID_BYTES + value + 128)?,
			)
		});
	}
	if total.is_some_and(|total| total <= MAX_SORT_MEMORY_BYTES) {
		return Ok(());
	}
	let mut total = Some(fixed);
	let mut retained = 0usize;
	let mut maximum = 0usize;
	for (segment, value) in searcher.segment_readers().iter().zip(values) {
		let ids = max_dictionary_key(segment, "__fulltext_id", deadline)?;
		let bytes = ids
			.checked_add(value)
			.and_then(|n| n.checked_add(128))
			.ok_or_else(|| resource("Sort key size overflow"))?;
		let count = end.min(segment.num_docs() as usize);
		maximum = maximum.max(bytes);
		retained = retained
			.checked_add(count)
			.ok_or_else(|| resource("Retained key count overflow"))?;
		total = total.and_then(|total| total.checked_add(count.checked_mul(bytes)?));
	}
	if total.is_some_and(|total| total <= MAX_SORT_MEMORY_BYTES) {
		return Ok(());
	}
	let matching = count_matches(searcher, query, deadline).map_err(native_error)?;
	let matching = usize::try_from(matching).unwrap_or(usize::MAX);
	let mut total = fixed;
	add_key_budget(&mut total, retained.min(matching), maximum, MAX_SORT_MEMORY_BYTES)
}
fn check_terms_budget(
	searcher: &Searcher,
	requests: &[AggregationRequest],
	fields: &[FilterFieldConfig],
	deadline: Option<Instant>,
) -> Result<()> {
	let mut cheap = Some(0usize);
	for request in requests {
		if let AggregationKind::Terms { size } = request.kind {
			let (index, _) = field(&request.field, fields)?;
			let name = format!("__fulltext_filter_{index}");
			for segment in searcher.segment_readers() {
				check_deadline(deadline).map_err(native_error)?;
				let count = segment
					.fast_fields()
					.str(&name)
					.map_err(native_error)?
					.map_or(0, |column| column.num_terms());
				let retained = (size as usize * 10).min(count);
				cheap = cheap.and_then(|total| total.checked_add(retained.checked_mul(65530 + 128)?));
			}
		}
	}
	if cheap.is_some_and(|total| total <= MAX_TERM_KEY_BYTES) {
		return Ok(());
	}
	let mut total = 0;
	for request in requests {
		if let AggregationKind::Terms { size } = request.kind {
			let (index, _) = field(&request.field, fields)?;
			let name = format!("__fulltext_filter_{index}");
			for segment in searcher.segment_readers() {
				let count = segment
					.fast_fields()
					.str(&name)
					.map_err(native_error)?
					.map_or(0, |column| column.num_terms());
				let maximum = max_dictionary_key(segment, &name, deadline)?;
				add_key_budget(
					&mut total,
					(size as usize * 10).min(count),
					maximum + 128,
					MAX_TERM_KEY_BYTES,
				)?;
			}
		}
	}
	Ok(())
}
#[cfg(test)]
pub(crate) struct SortedCatalogRequest<'a> {
	pub sort: &'a SortRequest,
	pub aggregations: &'a [AggregationRequest],
	pub offset: usize,
	pub limit: usize,
}
pub(crate) struct SortedCatalogResult {
	pub hits: Vec<((Score, String), DocAddress)>,
	pub total: u64,
	pub aggregations: Vec<AggregationResult>,
}
#[cfg(test)]
pub(crate) fn sorted_hits(
	searcher: &Searcher,
	query: &dyn Query,
	sort: &SortRequest,
	fields: &[FilterFieldConfig],
	offset: usize,
	limit: usize,
	deadline: Option<Instant>,
) -> Result<Vec<((Score, String), DocAddress)>> {
	Ok(sorted_catalog(
		searcher,
		query,
		SortedCatalogRequest {
			sort,
			aggregations: &[],
			offset,
			limit,
		},
		fields,
		TokenizerManager::default(),
		deadline,
	)?
	.hits)
}
impl ValidatedCatalog<'_> {
	pub(crate) fn sorted_catalog(
		&self,
		searcher: &Searcher,
		query: &dyn Query,
		offset: usize,
		limit: usize,
		tokenizers: TokenizerManager,
		deadline: Option<Instant>,
	) -> Result<SortedCatalogResult> {
		let sort = self
			.sort
			.ok_or_else(|| FulltextError::invalid("Sorted collection requires a sort request"))?;
		let requests = self.requests;
		let fields = self.fields;
		check_deadline(deadline).map_err(native_error)?;
		let (index, config) = field(&sort.field, fields)?;
		let end = offset
			.checked_add(limit)
			.ok_or_else(|| resource("Sort window overflow"))?
			.min(searcher.num_docs().max(1) as usize);
		check_sort_budget(
			searcher,
			query,
			end,
			index,
			config.field_type == FilterFieldType::String,
			deadline,
		)?;
		let aggregation_collector = prepare_aggregations(searcher, requests, fields, tokenizers, deadline)?;
		let comparator = match (&sort.direction, &sort.missing) {
			(SortDirection::Asc, MissingOrder::First) => ComparatorEnum::Reverse,
			(SortDirection::Asc, MissingOrder::Last) => ComparatorEnum::ReverseNoneLower,
			(SortDirection::Desc, MissingOrder::First) => ComparatorEnum::NaturalNoneHigher,
			(SortDirection::Desc, MissingOrder::Last) => ComparatorEnum::Natural,
		};
		let name = format!("__fulltext_filter_{index}");
		let histograms = histogram_checks(requests, fields)?;
		let collected = if config.field_type == FilterFieldType::String {
			collect_sorted(
				searcher,
				query,
				(SortByString::for_field(name), comparator),
				end,
				aggregation_collector,
				histograms,
				deadline,
			)?
		} else {
			collect_sorted(
				searcher,
				query,
				(SortByStaticFastValue::<f64>::for_field(name), comparator),
				end,
				aggregation_collector,
				histograms,
				deadline,
			)?
		};
		let CollectedSortedCatalog {
			hits,
			total: count,
			aggregations,
		} = collected;

		let hits = hits
			.into_iter()
			.map(|(id, score, doc)| {
				let id = id
					.filter(|id| !id.is_empty() && id.len() <= crate::protocol::MAX_RECORD_ID_BYTES)
					.ok_or_else(|| FulltextError::new("E_INDEX_CORRUPT", "Sort hit has missing or invalid ID"))?;
				Ok(((score, id), doc))
			})
			.collect::<Result<Vec<_>>>()?;
		Ok(SortedCatalogResult {
			hits: hits.into_iter().skip(offset).collect(),
			total: count as u64,
			aggregations: finish_aggregations(aggregations, requests, deadline)?,
		})
	}
	pub(crate) fn aggregate_count(
		&self,
		searcher: &Searcher,
		query: &dyn Query,
		tokenizers: TokenizerManager,
		deadline: Option<Instant>,
	) -> Result<(u64, Vec<AggregationResult>)> {
		let requests = self.requests;
		let fields = self.fields;
		check_deadline(deadline).map_err(native_error)?;
		let collector = prepare_aggregations(searcher, requests, fields, tokenizers, deadline)?;
		let (count, aggregations) = searcher
			.search(
				query,
				&DeadlineCollector {
					inner: (Count, collector),
					deadline,
					histograms: histogram_checks(requests, fields)?,
				},
			)
			.map_err(native_error)?;
		Ok((count as u64, finish_aggregations(aggregations, requests, deadline)?))
	}
	pub(crate) fn aggregate(
		&self,
		searcher: &Searcher,
		query: &dyn Query,
		tokenizers: TokenizerManager,
		deadline: Option<Instant>,
	) -> Result<Vec<AggregationResult>> {
		let requests = self.requests;
		let fields = self.fields;
		check_deadline(deadline).map_err(native_error)?;
		if requests.is_empty() {
			return Ok(Vec::new());
		}
		let collector = prepare_aggregations(searcher, requests, fields, tokenizers, deadline)?;
		let results = searcher
			.search(
				query,
				&DeadlineCollector {
					inner: collector,
					histograms: histogram_checks(requests, fields)?,
					deadline,
				},
			)
			.map_err(native_error)?;
		finish_aggregations(results, requests, deadline)
	}
}

#[cfg(test)]
pub(crate) fn sorted_catalog(
	searcher: &Searcher,
	query: &dyn Query,
	request: SortedCatalogRequest<'_>,
	fields: &[FilterFieldConfig],
	tokenizers: TokenizerManager,
	deadline: Option<Instant>,
) -> Result<SortedCatalogResult> {
	validate_requests(Some(request.sort), request.aggregations, fields)?.sorted_catalog(
		searcher,
		query,
		request.offset,
		request.limit,
		tokenizers,
		deadline,
	)
}
#[cfg(test)]
pub(crate) fn aggregate_count(
	searcher: &Searcher,
	query: &dyn Query,
	requests: &[AggregationRequest],
	fields: &[FilterFieldConfig],
	tokenizers: TokenizerManager,
	deadline: Option<Instant>,
) -> Result<(u64, Vec<AggregationResult>)> {
	validate_requests(None, requests, fields)?.aggregate_count(searcher, query, tokenizers, deadline)
}
#[cfg(test)]
pub(crate) fn aggregate(
	searcher: &Searcher,
	query: &dyn Query,
	requests: &[AggregationRequest],
	fields: &[FilterFieldConfig],
	tokenizers: TokenizerManager,
	deadline: Option<Instant>,
) -> Result<Vec<AggregationResult>> {
	validate_requests(None, requests, fields)?.aggregate(searcher, query, tokenizers, deadline)
}
struct CollectedSortedCatalog {
	hits: Vec<(Option<String>, Score, DocAddress)>,
	total: usize,
	aggregations: Option<tantivy::aggregation::agg_result::AggregationResults>,
}
fn collect_sorted<S: SortKeyComputer + Send + 'static>(
	searcher: &Searcher,
	query: &dyn Query,
	primary: S,
	end: usize,
	aggregations: Option<AggregationCollector>,
	histograms: Vec<(String, f64)>,
	deadline: Option<Instant>,
) -> Result<CollectedSortedCatalog> {
	let collector = TopDocs::for_doc_range(0..end).order_by((
		primary,
		(
			(SortByString::for_field("__fulltext_id"), tantivy::Order::Asc),
			SortBySimilarityScore,
		),
	));
	let (hits, total, aggregations) = searcher
		.search(
			query,
			&DeadlineCollector {
				inner: (collector, Count, aggregations),
				histograms,
				deadline,
			},
		)
		.map_err(native_error)?;
	Ok(CollectedSortedCatalog {
		hits: hits
			.into_iter()
			.map(|((_, (id, score)), doc)| (id, score, doc))
			.collect(),
		total,
		aggregations,
	})
}

fn histogram_checks(requests: &[AggregationRequest], fields: &[FilterFieldConfig]) -> Result<Vec<(String, f64)>> {
	let mut seen = HashSet::new();
	let mut checks = Vec::new();
	for request in requests {
		if let AggregationKind::Histogram { interval } = request.kind {
			let (index, _) = field(&request.field, fields)?;
			if seen.insert((index, interval.to_bits())) {
				checks.push((format!("__fulltext_filter_{index}"), interval));
			}
		}
	}
	Ok(checks)
}
fn prepare_aggregations(
	searcher: &Searcher,
	requests: &[AggregationRequest],
	fields: &[FilterFieldConfig],
	tokenizers: TokenizerManager,
	deadline: Option<Instant>,
) -> Result<Option<AggregationCollector>> {
	if requests.is_empty() {
		return Ok(None);
	}
	check_terms_budget(searcher, requests, fields, deadline)?;
	let mut native: tantivy::aggregation::agg_req::Aggregations = Default::default();
	for request in requests {
		let (index, _) = field(&request.field, fields)?;
		let name = format!("__fulltext_filter_{index}");
		let agg = match &request.kind {
			AggregationKind::Terms { size } => AggregationVariants::Terms(TermsAggregation {
				field: name,
				size: Some(*size),
				segment_size: Some(size * 10),
				show_term_doc_count_error: Some(true),
				min_doc_count: Some(1),
				order: None,
				missing: None,
				include: None,
				exclude: None,
			}),
			AggregationKind::Stats => AggregationVariants::Stats(StatsAggregation::from_field_name(name)),
			AggregationKind::Histogram { interval } => AggregationVariants::Histogram(HistogramAggregation {
				field: name,
				interval: *interval,
				offset: None,
				min_doc_count: Some(1),
				hard_bounds: None,
				extended_bounds: None,
				keyed: false,
				is_normalized_to_ns: false,
			}),
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
	Ok(Some(AggregationCollector::from_aggs(native, context)))
}
fn finish_aggregations(
	results: Option<tantivy::aggregation::agg_result::AggregationResults>,
	requests: &[AggregationRequest],
	deadline: Option<Instant>,
) -> Result<Vec<AggregationResult>> {
	let Some(results) = results else {
		return Ok(Vec::new());
	};
	let mut results = results.0;
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
		match &value {
			AggregationValue::Stats { sum, min, max, avg, .. } => {
				if !sum.is_finite() || [min, max, avg].into_iter().any(|n| n.is_some_and(|n| !n.is_finite())) {
					return Err(resource("Aggregation numeric result is not finite"));
				}
			}
			AggregationValue::Histogram { buckets } => {
				if buckets.iter().any(|bucket| !bucket.key.is_finite()) {
					return Err(resource("Aggregation numeric result is not finite"));
				}
			}
			_ => {}
		}
		output.push(AggregationResult {
			name: request.name.clone(),
			value,
		});
	}
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
pub(crate) fn encode_results(results: &[AggregationResult]) -> Result<Vec<u8>> {
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
		assert_eq!(
			ids(&index, "category", SortDirection::Asc, MissingOrder::First, 0, 10),
			["missing", "a", "c", "d", "b"]
		);
		assert_eq!(
			ids(&index, "category", SortDirection::Desc, MissingOrder::First, 0, 10),
			["missing", "b", "d", "a", "c"]
		);
		assert_eq!(
			ids(&index, "category", SortDirection::Desc, MissingOrder::Last, 0, 10),
			["b", "d", "a", "c", "missing"]
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
			sorted_hits(&searcher, &AllQuery, &sort, &fields(), usize::MAX, 1, None)
				.unwrap_err()
				.code,
			"E_RESOURCE_LIMIT"
		);
		assert_eq!(
			native_error(count_matches(&searcher, &AllQuery, Some(Instant::now())).unwrap_err()).code,
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
			scoring: bool,
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
				self.scoring
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
		for scoring in [false, true] {
			let calls = Arc::new(AtomicUsize::new(0));
			let deadline = Instant::now() + std::time::Duration::from_secs(1);
			let collector = DeadlineCollector {
				histograms: vec![],
				inner: Probe {
					scoring,
					calls: calls.clone(),
					deadline,
				},
				deadline: Some(deadline),
			};
			let error = searcher.search(&AllQuery, &collector).unwrap_err();
			assert_eq!(native_error(error).code, "E_TIMEOUT");
			assert!(calls.load(Ordering::Relaxed) > 0);
			assert!(calls.load(Ordering::Relaxed) <= 1024);
			assert_eq!(count_matches(&searcher, &AllQuery, None).unwrap(), 2048);
		}
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
	#[test]
	fn bounded_terms_may_omit_the_global_winner_and_report_uncertainty() {
		let mut schema = Schema::builder();
		let category = schema.add_text_field("__fulltext_filter_0", STRING | FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer = index.writer::<TantivyDocument>(20_000_000).unwrap();
		writer.set_merge_policy(Box::new(tantivy::indexer::NoMergePolicy));
		for segment in 0..3 {
			for local in 0..10 {
				for _ in 0..4 {
					writer
						.add_document(doc!(category=>format!("local-{segment}-{local}")))
						.unwrap();
				}
			}
			for _ in 0..3 {
				writer.add_document(doc!(category=>"common")).unwrap();
			}
			writer.commit().unwrap();
		}
		drop(writer);
		let searcher = index.reader().unwrap().searcher();
		assert_eq!(searcher.segment_readers().len(), 3);
		let request = AggregationRequest {
			name: "terms".into(),
			field: "category".into(),
			kind: AggregationKind::Terms { size: 1 },
		};
		let result = aggregate(
			&searcher,
			&AllQuery,
			&[request],
			&fields(),
			index.tokenizers().clone(),
			None,
		)
		.unwrap();
		match &result[0].value {
			AggregationValue::Terms {
				buckets,
				sum_other_doc_count,
				doc_count_error_upper_bound,
			} => {
				assert_eq!(buckets.len(), 1);
				assert!(buckets[0].key.starts_with("local-"));
				assert_eq!(buckets[0].count, 4);
				assert!(*sum_other_doc_count > 0);
				assert!(doc_count_error_upper_bound.is_some_and(|bound| bound > 0));
			}
			_ => panic!(),
		}
	}
	#[test]
	fn native_histogram_bucket_limit_rejects_without_poisoning_searcher() {
		let mut schema = Schema::builder();
		let price = schema.add_f64_field("__fulltext_filter_1", INDEXED | FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer = index.writer::<TantivyDocument>(20_000_000).unwrap();
		for value in 0..1100 {
			writer.add_document(doc!(price=>value as f64)).unwrap();
		}
		writer.commit().unwrap();
		drop(writer);
		let searcher = index.reader().unwrap().searcher();
		let request = AggregationRequest {
			name: "hist".into(),
			field: "price".into(),
			kind: AggregationKind::Histogram { interval: 1.0 },
		};
		let error = aggregate(
			&searcher,
			&AllQuery,
			&[request],
			&fields(),
			index.tokenizers().clone(),
			None,
		)
		.unwrap_err();
		assert_eq!(error.code, "E_RESOURCE_LIMIT");
		assert!(error.message.contains("bucket limit"));
		assert_eq!(count_matches(&searcher, &AllQuery, None).unwrap(), 1100);
	}

	#[test]
	fn terms_key_preflight_rejects_actual_large_keys() {
		let mut schema = Schema::builder();
		let category = schema.add_text_field("__fulltext_filter_0", STRING | FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer = index.writer::<TantivyDocument>(20_000_000).unwrap();
		writer.set_merge_policy(Box::new(tantivy::indexer::NoMergePolicy));
		for _ in 0..2 {
			for value in 0..200 {
				writer
					.add_document(doc!(category=>format!("{value:04}{}","x".repeat(65526))))
					.unwrap();
			}
			writer.commit().unwrap();
		}
		drop(writer);
		let searcher = index.reader().unwrap().searcher();
		let requests = vec![AggregationRequest {
			name: "terms".into(),
			field: "category".into(),
			kind: AggregationKind::Terms { size: 100 },
		}];
		let error = aggregate(
			&searcher,
			&AllQuery,
			&requests,
			&fields(),
			index.tokenizers().clone(),
			None,
		)
		.unwrap_err();
		assert_eq!(error.code, "E_RESOURCE_LIMIT");
	}

	#[test]
	fn sorted_pages_validate_corrupt_ids_before_skipping_the_offset() {
		for corrupt_id in [
			None,
			Some(String::new()),
			Some("x".repeat(crate::protocol::MAX_RECORD_ID_BYTES + 1)),
		] {
			let mut schema = Schema::builder();
			let id = schema.add_text_field("__fulltext_id", STRING | FAST);
			let price = schema.add_f64_field("__fulltext_filter_1", INDEXED | FAST);
			let index = Index::create_in_ram(schema.build());
			let mut writer = index.writer::<TantivyDocument>(20_000_000).unwrap();
			let mut corrupt = doc!(price=>0.0);
			if let Some(value) = corrupt_id {
				corrupt.add_text(id, value);
			}
			writer.add_document(corrupt).unwrap();
			writer.add_document(doc!(id=>"valid",price=>1.0)).unwrap();
			writer.commit().unwrap();
			drop(writer);
			let searcher = index.reader().unwrap().searcher();
			let sort = SortRequest {
				field: "price".into(),
				direction: SortDirection::Asc,
				missing: MissingOrder::Last,
			};
			for offset in [0, 1] {
				assert_eq!(
					sorted_hits(&searcher, &AllQuery, &sort, &fields(), offset, 1, None)
						.unwrap_err()
						.code,
					"E_INDEX_CORRUPT"
				);
			}
		}
	}
	#[test]
	fn small_keys_across_twenty_segments_and_a_real_merge_remain_available() {
		let mut schema = Schema::builder();
		let id = schema.add_text_field("__fulltext_id", STRING | FAST);
		let category = schema.add_text_field("__fulltext_filter_0", STRING | FAST);
		let price = schema.add_f64_field("__fulltext_filter_1", INDEXED | FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer = index.writer::<TantivyDocument>(20_000_000).unwrap();
		writer.set_merge_policy(Box::new(tantivy::indexer::NoMergePolicy));
		for segment in 0..20 {
			for value in 0..500 {
				writer.add_document(doc!(id=>format!("{segment:02}-{value:03}"),category=>format!("brand-{:02}",value%20),category=>"AAA",price=>value as f64,price=>-1.0)).unwrap();
			}
			writer.commit().unwrap();
		}
		let requests = vec![
			AggregationRequest {
				name: "terms".into(),
				field: "category".into(),
				kind: AggregationKind::Terms { size: 100 },
			},
			AggregationRequest {
				name: "stats".into(),
				field: "price".into(),
				kind: AggregationKind::Stats,
			},
		];
		let reader = index.reader().unwrap();
		let before = reader.searcher();
		assert_eq!(before.segment_readers().len(), 20);
		let sort = SortRequest {
			field: "category".into(),
			direction: SortDirection::Asc,
			missing: MissingOrder::Last,
		};
		let result = sorted_catalog(
			&before,
			&AllQuery,
			SortedCatalogRequest {
				sort: &sort,
				aggregations: &requests,
				offset: 200,
				limit: 20,
			},
			&fields(),
			index.tokenizers().clone(),
			None,
		)
		.unwrap();
		assert_eq!(result.hits.len(), 20);
		assert_eq!(result.total, 10000);
		assert_eq!(result.hits[0].0 .1, "08-000");
		let numeric = SortRequest {
			field: "price".into(),
			direction: SortDirection::Asc,
			missing: MissingOrder::Last,
		};
		assert_eq!(
			sorted_hits(&before, &AllQuery, &numeric, &fields(), 9980, 20, None)
				.unwrap()
				.len(),
			20
		);
		let segment_ids = index.searchable_segment_ids().unwrap();
		writer.merge(&segment_ids).wait().unwrap();
		reader.reload().unwrap();
		let after = reader.searcher();
		assert_eq!(after.segment_readers().len(), 1);
		let merged = sorted_catalog(
			&after,
			&AllQuery,
			SortedCatalogRequest {
				sort: &sort,
				aggregations: &requests,
				offset: 200,
				limit: 20,
			},
			&fields(),
			index.tokenizers().clone(),
			None,
		)
		.unwrap();
		assert_eq!(
			result.hits.iter().map(|hit| &hit.0).collect::<Vec<_>>(),
			merged.hits.iter().map(|hit| &hit.0).collect::<Vec<_>>()
		);
		assert_eq!(result.total, merged.total);
		assert_eq!(result.aggregations, merged.aggregations);
	}
	#[test]
	fn histogram_does_not_reject_an_unmatched_extreme_outlier() {
		use tantivy::query::TermQuery;
		use tantivy::schema::IndexRecordOption;
		let mut schema = Schema::builder();
		let id = schema.add_text_field("__fulltext_id", STRING | FAST);
		let price = schema.add_f64_field("__fulltext_filter_1", INDEXED | FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer = index.writer::<TantivyDocument>(20_000_000).unwrap();
		writer.add_document(doc!(id=>"match",price=>1.0)).unwrap();
		writer.add_document(doc!(id=>"outlier",price=>f64::MAX)).unwrap();
		writer.commit().unwrap();
		drop(writer);
		let request = AggregationRequest {
			name: "hist".into(),
			field: "price".into(),
			kind: AggregationKind::Histogram { interval: 1.0 },
		};
		let query = TermQuery::new(Term::from_field_text(id, "match"), IndexRecordOption::Basic);
		let result = aggregate(
			&index.reader().unwrap().searcher(),
			&query,
			&[request],
			&fields(),
			index.tokenizers().clone(),
			None,
		)
		.unwrap();
		assert_eq!(
			result[0].value,
			AggregationValue::Histogram {
				buckets: vec![NumericBucket { key: 1.0, count: 1 }]
			}
		);
	}
	#[test]
	fn large_short_key_catalogs_qualify_without_scanning_ids() {
		let mut schema = Schema::builder();
		let id = schema.add_text_field("__fulltext_id", STRING | FAST);
		let category = schema.add_text_field("__fulltext_filter_0", STRING | FAST);
		schema.add_f64_field("__fulltext_filter_1", INDEXED | FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer = index.writer_with_num_threads::<TantivyDocument>(1, 50_000_000).unwrap();
		writer.set_merge_policy(Box::new(tantivy::indexer::NoMergePolicy));
		for segment in 0..2 {
			for value in 0..75001 {
				let ordinal = segment * 75001 + value;
				writer
					.add_document(doc!(id=>format!("record-{ordinal:06}"),category=>format!("term-{ordinal:06}")))
					.unwrap();
			}
			writer.commit().unwrap();
		}
		drop(writer);
		let searcher = index.reader().unwrap().searcher();
		assert_eq!(searcher.segment_readers().len(), 2);
		let sort = SortRequest {
			field: "category".into(),
			direction: SortDirection::Asc,
			missing: MissingOrder::Last,
		};
		let hits = sorted_hits(&searcher, &AllQuery, &sort, &fields(), 980, 20, None).unwrap();
		assert_eq!(hits.len(), 20);
		assert_eq!(hits[0].0 .1, "record-000980");
		for segment in searcher.segment_readers() {
			let id_key = (segment.segment_id(), id);
			assert!(!dictionary_cache().lock().unwrap().maxima.contains_key(&id_key));
			assert_eq!(max_dictionary_key(segment, "__fulltext_filter_0", None).unwrap(), 11);
		}
		let request = AggregationRequest {
			name: "terms".into(),
			field: "category".into(),
			kind: AggregationKind::Terms { size: 100 },
		};
		let result = aggregate(
			&searcher,
			&AllQuery,
			&[request],
			&fields(),
			index.tokenizers().clone(),
			None,
		)
		.unwrap();
		match &result[0].value {
			AggregationValue::Terms { buckets, .. } => assert_eq!(buckets.len(), 100),
			_ => panic!(),
		}
		assert_eq!(
			max_dictionary_key(
				&searcher.segment_readers()[0],
				"__fulltext_filter_0",
				Some(Instant::now())
			)
			.unwrap_err()
			.code,
			"E_TIMEOUT"
		);
	}
	#[test]
	fn dictionary_maxima_cache_is_bounded_and_fifo() {
		let mut cache = DictionaryCache::default();
		let field = tantivy::schema::Field::from_field_id(0);
		let first = (tantivy::index::SegmentId::generate_random(), field);
		cache.insert(first, 7);
		for _ in 0..MAX_DICTIONARY_CACHE_ENTRIES {
			cache.insert((tantivy::index::SegmentId::generate_random(), field), 11);
		}
		assert_eq!(cache.maxima.len(), MAX_DICTIONARY_CACHE_ENTRIES);
		assert_eq!(cache.insertion_order.len(), MAX_DICTIONARY_CACHE_ENTRIES);
		assert!(!cache.maxima.contains_key(&first));
		let newest = *cache.insertion_order.back().unwrap();
		cache.insert(newest, 99);
		assert_eq!(cache.maxima[&newest], 11);
		assert_eq!(cache.insertion_order.len(), MAX_DICTIONARY_CACHE_ENTRIES);
	}
	#[test]
	fn sort_buffer_budget_counts_active_executor_threads() {
		let mut single = sort_buffer_bytes(10000, 40, 1).unwrap();
		add_key_budget(&mut single, 400000, 128 + 20 + 8, MAX_SORT_MEMORY_BYTES).unwrap();
		let mut concurrent = sort_buffer_bytes(10000, 40, 40).unwrap();
		assert_eq!(
			add_key_budget(&mut concurrent, 400000, 128 + 20 + 8, MAX_SORT_MEMORY_BYTES)
				.unwrap_err()
				.code,
			"E_RESOURCE_LIMIT"
		);
		assert_eq!(
			sort_buffer_bytes(10000, 40, 2).unwrap(),
			sort_buffer_bytes(10000, 2, 40).unwrap()
		);
	}
	#[test]
	fn dictionary_scan_checks_deadline_midstream_at_bounded_intervals() {
		use std::cell::Cell;
		let visited = Cell::new(0usize);
		let checks = Cell::new(0usize);
		let error = scan_dictionary_max(
			|| {
				visited.set(visited.get() + 1);
				Some(7)
			},
			|| {
				checks.set(checks.get() + 1);
				if checks.get() == 2 {
					Err(FulltextError::new("E_TIMEOUT", crate::error::SEARCH_TIMEOUT_MESSAGE))
				} else {
					Ok(())
				}
			},
		)
		.unwrap_err();
		assert_eq!(error.code, "E_TIMEOUT");
		assert_eq!(visited.get(), 1024);
		assert_eq!(checks.get(), 2);
	}
	#[test]
	fn deep_string_sort_qualifies_id_fallback_and_rejects_large_ids() {
		for id_bytes in [20, crate::protocol::MAX_RECORD_ID_BYTES] {
			let mut schema = Schema::builder();
			let id = schema.add_text_field("__fulltext_id", STRING | FAST);
			let category = schema.add_text_field("__fulltext_filter_0", STRING | FAST);
			schema.add_f64_field("__fulltext_filter_1", INDEXED | FAST);
			let index = Index::create_in_ram(schema.build());
			let mut writer = index
				.writer_with_num_threads::<TantivyDocument>(1, 100_000_000)
				.unwrap();
			writer.set_merge_policy(Box::new(tantivy::indexer::NoMergePolicy));
			let value = "v".repeat(30000);
			for segment in 0..2 {
				for record in 0..1000 {
					let prefix = format!("{segment:01}-{record:04}");
					let key = format!("{prefix}{}", "x".repeat(id_bytes - prefix.len()));
					writer.add_document(doc!(id=>key,category=>value.as_str())).unwrap();
				}
				writer.commit().unwrap();
			}
			drop(writer);
			let searcher = index.reader().unwrap().searcher();
			assert_eq!(searcher.segment_readers().len(), 2);
			let sort = SortRequest {
				field: "category".into(),
				direction: SortDirection::Asc,
				missing: MissingOrder::Last,
			};
			let result = sorted_hits(&searcher, &AllQuery, &sort, &fields(), 0, 1000, None);
			if id_bytes == 20 {
				assert_eq!(result.unwrap().len(), 1000);
				for segment in searcher.segment_readers() {
					assert_eq!(max_dictionary_key(segment, "__fulltext_id", None).unwrap(), 20);
					assert!(dictionary_cache()
						.lock()
						.unwrap()
						.maxima
						.contains_key(&(segment.segment_id(), id)));
				}
			} else {
				assert_eq!(result.unwrap_err().code, "E_RESOURCE_LIMIT");
			}
		}
	}
	#[test]
	fn combined_aggregation_count_matches_independent_results() {
		let index = fixture();
		let searcher = index.reader().unwrap().searcher();
		let requests = vec![AggregationRequest {
			name: "stats".into(),
			field: "price".into(),
			kind: AggregationKind::Stats,
		}];
		let (count, aggregations) = aggregate_count(
			&searcher,
			&AllQuery,
			&requests,
			&fields(),
			index.tokenizers().clone(),
			None,
		)
		.unwrap();
		assert_eq!(count, 5);
		assert_eq!(
			aggregations,
			aggregate(
				&searcher,
				&AllQuery,
				&requests,
				&fields(),
				index.tokenizers().clone(),
				None
			)
			.unwrap()
		);
	}
	#[test]
	fn selective_fragmented_sort_uses_matching_rows_and_survives_merge() {
		use tantivy::{query::TermQuery, schema::IndexRecordOption};
		let mut schema = Schema::builder();
		let id = schema.add_text_field("__fulltext_id", STRING | FAST);
		let category = schema.add_text_field("__fulltext_filter_0", STRING | FAST);
		let price = schema.add_f64_field("__fulltext_filter_1", INDEXED | FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer = index.writer_with_num_threads::<TantivyDocument>(1, 50_000_000).unwrap();
		writer.set_merge_policy(Box::new(tantivy::indexer::NoMergePolicy));
		for segment in 0..40 {
			for record in 0..10000 {
				let prefix = format!("{segment:02}-{record:05}");
				let key = format!("{prefix}{}", "x".repeat(36 - prefix.len()));
				writer
					.add_document(
						doc!(id=>key,category=>if segment==0&&record<50{"match"}else{"other"},price=>record as f64),
					)
					.unwrap();
			}
			writer.commit().unwrap();
		}
		let reader = index.reader().unwrap();
		let before = reader.searcher();
		assert_eq!(before.segment_readers().len(), 40);
		let query = TermQuery::new(Term::from_field_text(category, "match"), IndexRecordOption::Basic);
		let sort = SortRequest {
			field: "price".into(),
			direction: SortDirection::Asc,
			missing: MissingOrder::Last,
		};
		let selected = sorted_hits(&before, &query, &sort, &fields(), 0, 10000, None).unwrap();
		assert_eq!(selected.len(), 50);
		assert_eq!(
			sorted_hits(&before, &AllQuery, &sort, &fields(), 0, 10000, None)
				.unwrap_err()
				.code,
			"E_RESOURCE_LIMIT"
		);
		writer.merge(&index.searchable_segment_ids().unwrap()).wait().unwrap();
		reader.reload().unwrap();
		let after = reader.searcher();
		assert_eq!(after.segment_readers().len(), 1);
		let merged = sorted_hits(&after, &query, &sort, &fields(), 0, 10000, None).unwrap();
		assert_eq!(
			selected.iter().map(|hit| &hit.0).collect::<Vec<_>>(),
			merged.iter().map(|hit| &hit.0).collect::<Vec<_>>()
		);
		assert_eq!(
			sorted_hits(&after, &AllQuery, &sort, &fields(), 0, 10000, None)
				.unwrap()
				.len(),
			10000
		);
	}
	#[test]
	fn dictionary_cache_keeps_completed_scans_but_discards_partial_scans_on_timeout() {
		use std::cell::Cell;
		for partial in [false, true] {
			let mut schema = Schema::builder();
			let category = schema.add_text_field("__fulltext_filter_0", STRING | FAST);
			let index = Index::create_in_ram(schema.build());
			let mut writer = index.writer_with_num_threads::<TantivyDocument>(1, 20_000_000).unwrap();
			for value in 0..1500 {
				writer.add_document(doc!(category=>format!("key-{value:04}"))).unwrap();
			}
			writer.commit().unwrap();
			drop(writer);
			let searcher = index.reader().unwrap().searcher();
			let segment = &searcher.segment_readers()[0];
			let checks = Cell::new(0usize);
			let fail_at = if partial { 3 } else { 4 };
			let error = max_dictionary_key_with_check(segment, "__fulltext_filter_0", || {
				checks.set(checks.get() + 1);
				if checks.get() == fail_at {
					Err(FulltextError::new("E_TIMEOUT", crate::error::SEARCH_TIMEOUT_MESSAGE))
				} else {
					Ok(())
				}
			})
			.unwrap_err();
			assert_eq!(error.code, "E_TIMEOUT");
			assert_eq!(
				dictionary_cache()
					.lock()
					.unwrap()
					.maxima
					.contains_key(&(segment.segment_id(), category)),
				!partial
			);
		}
	}
	#[test]
	fn identical_histogram_requests_share_validation_checks() {
		let request = AggregationRequest {
			name: "first".into(),
			field: "price".into(),
			kind: AggregationKind::Histogram { interval: 10.0 },
		};
		let mut second = request.clone();
		second.name = "second".into();
		assert_eq!(
			histogram_checks(&[request, second], &fields()).unwrap(),
			vec![("__fulltext_filter_1".into(), 10.0)]
		);
	}
	#[test]
	fn production_count_interrupts_advancing_matches_and_remains_reusable() {
		use std::sync::{
			atomic::{AtomicUsize, Ordering},
			Arc,
		};
		use tantivy::{
			query::{EnableScoring, Explanation, Scorer},
			DocSet,
		};
		#[derive(Clone, Debug)]
		struct SlowQuery {
			advances: Arc<AtomicUsize>,
			deadline: Instant,
		}
		struct SlowWeight {
			inner: Box<dyn Weight>,
			advances: Arc<AtomicUsize>,
			deadline: Instant,
		}
		struct SlowScorer {
			inner: Box<dyn Scorer>,
			advances: Arc<AtomicUsize>,
			deadline: Instant,
		}
		impl Query for SlowQuery {
			fn weight(&self, scoring: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
				Ok(Box::new(SlowWeight {
					inner: AllQuery.weight(scoring)?,
					advances: self.advances.clone(),
					deadline: self.deadline,
				}))
			}
		}
		impl Weight for SlowWeight {
			fn scorer(&self, reader: &SegmentReader, boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
				Ok(Box::new(SlowScorer {
					inner: self.inner.scorer(reader, boost)?,
					advances: self.advances.clone(),
					deadline: self.deadline,
				}))
			}
			fn explain(&self, reader: &SegmentReader, doc: u32) -> tantivy::Result<Explanation> {
				self.inner.explain(reader, doc)
			}
		}
		impl DocSet for SlowScorer {
			fn advance(&mut self) -> u32 {
				if self.advances.fetch_add(1, Ordering::Relaxed) == 0 {
					while Instant::now() < self.deadline {
						std::thread::yield_now();
					}
				}
				self.inner.advance()
			}
			fn doc(&self) -> u32 {
				self.inner.doc()
			}
			fn size_hint(&self) -> u32 {
				self.inner.size_hint()
			}
		}
		impl Scorer for SlowScorer {
			fn score(&mut self) -> Score {
				self.inner.score()
			}
		}
		let mut schema = Schema::builder();
		let id = schema.add_u64_field("id", FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer = index.writer_with_num_threads::<TantivyDocument>(1, 20_000_000).unwrap();
		for id_value in 0..2048u64 {
			writer.add_document(doc!(id=>id_value)).unwrap();
		}
		writer.commit().unwrap();
		drop(writer);
		let searcher = index.reader().unwrap().searcher();
		let advances = Arc::new(AtomicUsize::new(0));
		let deadline = Instant::now() + std::time::Duration::from_secs(1);
		let query = SlowQuery {
			advances: advances.clone(),
			deadline,
		};
		let result = count_matches(&searcher, &query, Some(deadline));
		let visited = advances.load(Ordering::Relaxed);
		assert!(
			visited > 0 && visited <= 1024,
			"count advanced {visited} matches before returning"
		);
		assert_eq!(native_error(result.unwrap_err()).code, "E_TIMEOUT");
		assert_eq!(count_matches(&searcher, &AllQuery, None).unwrap(), 2048);
	}
}

#[cfg(test)]
#[path = "catalog_qualification.rs"]
mod qualification;
