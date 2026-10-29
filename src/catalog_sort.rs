use super::*;
use std::cmp::Ordering;
use tantivy::collector::sort_key::{Comparator, SegmentSortKeyComputer, SortByErasedType};
use tantivy::schema::OwnedValue;

type StringChild = <SortByString as SortKeyComputer>::Child;
type NumberChild = <SortByStaticFastValue<f64> as SortKeyComputer>::Child;

pub(super) struct DynamicSortKeys {
	fields: Vec<(String, FilterFieldType, ComparatorEnum)>,
}
impl DynamicSortKeys {
	pub(super) fn new(sorts: &[SortRequest], fields: &[FilterFieldConfig]) -> Result<Self> {
		Ok(Self {
			fields: sorts
				.iter()
				.map(|sort| {
					let (index, config) = field(&sort.field, fields)?;
					Ok((
						format!("__fulltext_filter_{index}"),
						config.field_type,
						comparator(sort),
					))
				})
				.collect::<Result<_>>()?,
		})
	}
}
pub(super) fn comparator(sort: &SortRequest) -> ComparatorEnum {
	match (&sort.direction, &sort.missing) {
		(SortDirection::Asc, MissingOrder::First) => ComparatorEnum::Reverse,
		(SortDirection::Asc, MissingOrder::Last) => ComparatorEnum::ReverseNoneLower,
		(SortDirection::Desc, MissingOrder::First) => ComparatorEnum::NaturalNoneHigher,
		(SortDirection::Desc, MissingOrder::Last) => ComparatorEnum::Natural,
	}
}
pub(super) fn collect_native(
	searcher: &Searcher,
	query: &dyn Query,
	catalog: ValidatedCatalog<'_>,
	end: usize,
	aggregations: Option<AggregationCollector>,
	histograms: Vec<HistogramCheck>,
	deadline: Option<Instant>,
) -> Result<CollectedSortedCatalog> {
	let sorts = catalog
		.sort
		.ok_or_else(|| FulltextError::invalid("Sorted collection requires a sort request"))?;
	let fields = catalog.fields;
	macro_rules! collect {
		(@primary $primary:expr) => {
			collect_sorted(searcher, query, $primary, end, aggregations, histograms, deadline)
		};
		($first:expr, $second:expr) => {
			collect!(@primary (($first, comparator(&sorts[0])), ($second, comparator(&sorts[1]))))
		};
	}
	let erased = |position: usize| -> Result<_> {
		let index = field(&sorts[position].field, fields)?.0;
		Ok((
			SortByErasedType::for_field(format!("__fulltext_filter_{index}")),
			comparator(&sorts[position]),
		))
	};
	if sorts.len() == 3 {
		return collect!(@primary (erased(0)?, erased(1)?, erased(2)?));
	}
	if sorts.len() == 4 {
		// Tantivy 0.27's flat four-key implementation drops configured comparators.
		return collect!(@primary (erased(0)?, (erased(1)?, (erased(2)?, erased(3)?))));
	}
	let (first, first_config) = field(&sorts[0].field, fields)?;
	let (second, second_config) = field(&sorts[1].field, fields)?;
	let first = format!("__fulltext_filter_{first}");
	let second = format!("__fulltext_filter_{second}");
	match (&first_config.field_type, &second_config.field_type) {
		(FilterFieldType::String, FilterFieldType::String) => {
			collect!(SortByString::for_field(first), SortByString::for_field(second))
		}
		(FilterFieldType::String, FilterFieldType::Number) => collect!(
			SortByString::for_field(first),
			SortByStaticFastValue::<f64>::for_field(second)
		),
		(FilterFieldType::Number, FilterFieldType::String) => collect!(
			SortByStaticFastValue::<f64>::for_field(first),
			SortByString::for_field(second)
		),
		(FilterFieldType::Number, FilterFieldType::Number) => collect!(
			SortByStaticFastValue::<f64>::for_field(first),
			SortByStaticFastValue::<f64>::for_field(second)
		),
		_ => unreachable!(),
	}
}
#[derive(Clone, Debug, Default)]
pub(super) struct DynamicComparator(Vec<ComparatorEnum>);
impl<T> Comparator<Vec<T>> for DynamicComparator
where
	ComparatorEnum: Comparator<T>,
{
	fn compare(&self, left: &Vec<T>, right: &Vec<T>) -> Ordering {
		for ((left, right), comparator) in left.iter().zip(right).zip(&self.0) {
			let order = comparator.compare(left, right);
			if order != Ordering::Equal {
				return order;
			}
		}
		Ordering::Equal
	}
}
enum NativeChild {
	String(StringChild),
	Number(NumberChild),
}
impl NativeChild {
	fn key(&mut self, doc: u32, score: Score) -> Option<u64> {
		match self {
			Self::String(child) => child.segment_sort_key(doc, score),
			Self::Number(child) => child.segment_sort_key(doc, score),
		}
	}
	fn convert(&self, key: Option<u64>) -> OwnedValue {
		match self {
			Self::String(child) => child
				.convert_segment_sort_key(key)
				.map(OwnedValue::Str)
				.unwrap_or(OwnedValue::Null),
			Self::Number(child) => child
				.convert_segment_sort_key(key)
				.map(OwnedValue::F64)
				.unwrap_or(OwnedValue::Null),
		}
	}
}
pub(super) struct DynamicSegmentKeys {
	children: Vec<NativeChild>,
	comparator: DynamicComparator,
	scratch: Vec<Option<u64>>,
}
impl SortKeyComputer for DynamicSortKeys {
	type SortKey = Vec<OwnedValue>;
	type Child = DynamicSegmentKeys;
	type Comparator = DynamicComparator;
	fn comparator(&self) -> Self::Comparator {
		DynamicComparator(self.fields.iter().map(|(_, _, comparator)| *comparator).collect())
	}
	fn check_schema(&self, schema: &tantivy::schema::Schema) -> tantivy::Result<()> {
		for (name, kind, _) in &self.fields {
			match kind {
				FilterFieldType::String => SortByString::for_field(name).check_schema(schema)?,
				FilterFieldType::Number => SortByStaticFastValue::<f64>::for_field(name).check_schema(schema)?,
				FilterFieldType::Boolean => unreachable!(),
			}
		}
		Ok(())
	}
	fn segment_sort_key_computer(&self, reader: &SegmentReader) -> tantivy::Result<Self::Child> {
		let children = self
			.fields
			.iter()
			.map(|(name, kind, _)| {
				Ok(match kind {
					FilterFieldType::String => {
						NativeChild::String(SortByString::for_field(name).segment_sort_key_computer(reader)?)
					}
					FilterFieldType::Number => NativeChild::Number(
						SortByStaticFastValue::<f64>::for_field(name).segment_sort_key_computer(reader)?,
					),
					FilterFieldType::Boolean => unreachable!(),
				})
			})
			.collect::<tantivy::Result<_>>()?;
		Ok(DynamicSegmentKeys {
			children,
			comparator: self.comparator(),
			scratch: vec![None; self.fields.len()],
		})
	}
}
impl SegmentSortKeyComputer for DynamicSegmentKeys {
	type SortKey = Vec<OwnedValue>;
	type SegmentSortKey = Vec<Option<u64>>;
	type SegmentComparator = DynamicComparator;
	fn segment_comparator(&self) -> Self::SegmentComparator {
		self.comparator.clone()
	}
	fn segment_sort_key(&mut self, doc: u32, score: Score) -> Self::SegmentSortKey {
		self.children.iter_mut().map(|child| child.key(doc, score)).collect()
	}
	fn accept_sort_key_lazy(
		&mut self,
		doc: u32,
		score: Score,
		threshold: &Self::SegmentSortKey,
	) -> Option<(Ordering, Self::SegmentSortKey)> {
		let mut order = Ordering::Equal;
		for (position, threshold) in threshold.iter().enumerate() {
			let key = self.children[position].key(doc, score);
			self.scratch[position] = key;
			order = self.comparator.0[position].compare(&key, threshold);
			if order == Ordering::Less {
				return None;
			}
			if order == Ordering::Greater {
				for position in position + 1..self.children.len() {
					self.scratch[position] = self.children[position].key(doc, score);
				}
				break;
			}
		}
		Some((order, self.scratch.clone()))
	}
	fn convert_segment_sort_key(&self, key: Self::SegmentSortKey) -> Self::SortKey {
		self.children
			.iter()
			.zip(key)
			.map(|(child, key)| child.convert(key))
			.collect()
	}
}
fn global_key_bytes(key_count: usize) -> Result<usize> {
	let global_header = std::mem::size_of::<
		tantivy::collector::ComparableDoc<(Vec<OwnedValue>, (Option<String>, Score)), DocAddress>,
	>();
	let bytes = key_count
		.checked_mul(std::mem::size_of::<OwnedValue>())
		.and_then(|n| n.checked_add(global_header))
		.ok_or_else(|| resource("Sort key size overflow"))?;

	Ok(bytes)
}
pub(super) fn buffer_bytes(end: usize, segments: usize, parallelism: usize, key_count: usize) -> Result<usize> {
	let segment_header =
		std::mem::size_of::<tantivy::collector::ComparableDoc<(Vec<Option<u64>>, (Option<u64>, Score)), u32>>();
	let segment_bytes = key_count
		.checked_mul(std::mem::size_of::<Option<u64>>())
		.and_then(|n| n.checked_add(segment_header))
		.ok_or_else(|| resource("Sort key size overflow"))?;
	let global_bytes = if key_count <= 4 {
		global_key_bytes(key_count)?
	} else {
		std::mem::size_of::<tantivy::collector::ComparableDoc<(Vec<OwnedValue>, (Option<String>, Score)), DocAddress>>()
	};
	let fixed = end
		.checked_mul(2)
		.and_then(|n| n.checked_mul(segment_bytes))
		.and_then(|n| n.checked_mul(segments.min(parallelism)))
		.and_then(|n| n.checked_add(end.checked_mul(2)?.checked_mul(global_bytes)?))
		.and_then(|n| {
			n.checked_add(
				key_count
					.checked_mul(std::mem::size_of::<Option<u64>>())?
					.checked_mul(3)?
					.checked_mul(segments.min(parallelism))?,
			)
		})
		.ok_or_else(|| resource("Sort buffer memory overflow"))?;
	Ok(fixed)
}
pub(super) fn check_budget(
	searcher: &Searcher,
	query: &dyn Query,
	end: usize,
	sorts: &[SortRequest],
	fields: &[FilterFieldConfig],
	deadline: Option<Instant>,
) -> Result<()> {
	let segments = searcher.segment_readers();
	let parallelism = match searcher.index().search_executor() {
		tantivy::Executor::SingleThread => 1,
		tantivy::Executor::ThreadPool(pool) => pool.current_num_threads(),
	};
	let global_bytes = global_key_bytes(sorts.len())?;
	let fixed = buffer_bytes(end, segments.len(), parallelism, sorts.len())?;
	if fixed > MAX_SORT_MEMORY_BYTES {
		return Err(resource("Sort buffers exceed memory budget"));
	}
	let mut selected = Vec::with_capacity(sorts.len());
	let mut worst = global_bytes
		.checked_add(crate::protocol::MAX_RECORD_ID_BYTES)
		.ok_or_else(|| resource("Sort key size overflow"))?;
	for sort in sorts {
		check_deadline(deadline).map_err(native_error)?;
		let (index, config) = field(&sort.field, fields)?;
		let string = config.field_type == FilterFieldType::String;
		selected.push((format!("__fulltext_filter_{index}"), string));
		if string {
			worst = worst
				.checked_add(65530)
				.ok_or_else(|| resource("Sort key size overflow"))?;
		}
	}
	let fits = |sizes: &[usize]| -> bool {
		let maximum = sizes.iter().copied().max().unwrap_or(0);
		let total = segments.iter().zip(sizes).try_fold(fixed, |total, (segment, bytes)| {
			total.checked_add(end.min(segment.num_docs() as usize).checked_mul(*bytes)?)
		});
		total
			.and_then(|total| total.checked_add(2usize.checked_mul(maximum)?))
			.is_some_and(|total| total <= MAX_SORT_MEMORY_BYTES)
	};
	if fits(&vec![worst; segments.len()]) {
		return Ok(());
	}
	let mut sizes = Vec::with_capacity(segments.len());
	for segment in segments {
		let mut bytes = global_bytes
			.checked_add(crate::protocol::MAX_RECORD_ID_BYTES)
			.ok_or_else(|| resource("Sort key size overflow"))?;
		for (name, string) in &selected {
			check_deadline(deadline).map_err(native_error)?;
			if *string {
				bytes = bytes
					.checked_add(max_dictionary_key(segment, name, deadline)?)
					.ok_or_else(|| resource("Sort key size overflow"))?;
			}
		}
		sizes.push(bytes);
	}
	if fits(&sizes) {
		return Ok(());
	}
	for (segment, bytes) in segments.iter().zip(&mut sizes) {
		let ids = max_dictionary_key(segment, "__fulltext_id", deadline)?;
		*bytes = bytes
			.checked_sub(crate::protocol::MAX_RECORD_ID_BYTES)
			.and_then(|bytes| bytes.checked_add(ids))
			.ok_or_else(|| resource("Sort key size overflow"))?;
	}
	if fits(&sizes) {
		return Ok(());
	}
	let maximum = sizes.into_iter().max().unwrap_or(0);
	let retained = segments
		.iter()
		.try_fold(0usize, |total, segment| {
			total.checked_add(end.min(segment.num_docs() as usize))
		})
		.ok_or_else(|| resource("Retained key count overflow"))?;
	let matching =
		usize::try_from(count_matches(searcher, query, deadline).map_err(native_error)?).unwrap_or(usize::MAX);
	let mut total = fixed;
	add_key_budget(
		&mut total,
		retained.min(matching).saturating_add(matching.min(2)),
		maximum,
		MAX_SORT_MEMORY_BYTES,
	)
}

#[cfg(test)]
mod tests {
	use super::*;
	use tantivy::query::AllQuery;
	use tantivy::schema::{Schema, FAST, STRING};
	use tantivy::{doc, Index, IndexWriter, TantivyDocument};

	fn sorts() -> Vec<SortRequest> {
		vec![
			SortRequest {
				field: "group".into(),
				direction: SortDirection::Asc,
				missing: MissingOrder::Last,
			},
			SortRequest {
				field: "price".into(),
				direction: SortDirection::Desc,
				missing: MissingOrder::First,
			},
		]
	}
	fn fields() -> Vec<FilterFieldConfig> {
		vec![
			FilterFieldConfig {
				name: "group".into(),
				field_type: FilterFieldType::String,
			},
			FilterFieldConfig {
				name: "price".into(),
				field_type: FilterFieldType::Number,
			},
		]
	}
	fn fixture(rows: usize, monotone: bool) -> Index {
		let mut schema = Schema::builder();
		let id = schema.add_text_field("__fulltext_id", STRING | FAST);
		let group = schema.add_text_field("__fulltext_filter_0", STRING | FAST);
		let price = schema.add_f64_field("__fulltext_filter_1", FAST);
		let label = schema.add_text_field("__fulltext_filter_2", STRING | FAST);
		let rating = schema.add_f64_field("__fulltext_filter_3", FAST);
		let zone = schema.add_text_field("__fulltext_filter_4", STRING | FAST);
		let stock = schema.add_f64_field("__fulltext_filter_5", FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer: IndexWriter<TantivyDocument> = index.writer_with_num_threads(1, 15_000_000).unwrap();
		writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
		let mut random = 17u64;
		for row in 0..rows {
			random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
			let value = if monotone { row as u64 } else { random % 1000 };
			writer
				.add_document(
					doc!(id => format!("{row:08}"), group => format!("g{:08}", if monotone { value } else { value % 100 }), price => value as f64, label => format!("l{:04}", value % 37), rating => (value % 19) as f64, zone => format!("z{:03}", value % 11), stock => (value % 7) as f64),
				)
				.unwrap();
			if row % 1000 == 999 {
				writer.commit().unwrap();
			}
		}
		writer.commit().unwrap();
		index
	}
	fn collect<S: SortKeyComputer + Send + 'static>(searcher: &Searcher, key: S, end: usize) -> Vec<DocAddress> {
		searcher
			.search(
				&AllQuery,
				&TopDocs::with_limit(end).order_by((
					key,
					(
						(SortByString::for_field("__fulltext_id"), tantivy::Order::Asc),
						SortBySimilarityScore,
					),
				)),
			)
			.unwrap()
			.into_iter()
			.map(|(_, doc)| doc)
			.collect()
	}
	#[test]
	fn runtime_sort_matches_native_tuple_across_segments_and_directions() {
		for monotone in [false, true] {
			let index = fixture(3000, monotone);
			let searcher = index.reader().unwrap().searcher();
			for direction in [SortDirection::Asc, SortDirection::Desc] {
				for missing in [MissingOrder::First, MissingOrder::Last] {
					let mut sorts = sorts();
					sorts[0].direction = direction.clone();
					sorts[0].missing = missing.clone();
					let dynamic = collect(&searcher, DynamicSortKeys::new(&sorts, &fields()).unwrap(), 137);
					let native = collect(
						&searcher,
						(
							(SortByString::for_field("__fulltext_filter_0"), comparator(&sorts[0])),
							(
								SortByStaticFastValue::<f64>::for_field("__fulltext_filter_1"),
								comparator(&sorts[1]),
							),
						),
						137,
					);
					assert_eq!(dynamic, native);
				}
			}
		}
	}
	#[test]
	fn native_pair_dispatch_matches_dynamic_for_all_type_combinations() {
		let index = fixture(3000, false);
		let searcher = index.reader().unwrap().searcher();
		let mut fields = fields();
		fields.extend([
			FilterFieldConfig {
				name: "label".into(),
				field_type: FilterFieldType::String,
			},
			FilterFieldConfig {
				name: "rating".into(),
				field_type: FilterFieldType::Number,
			},
		]);
		for (first, second) in [(0, 2), (0, 1), (1, 2), (1, 3)] {
			let sorts = vec![
				SortRequest {
					field: fields[first].name.clone(),
					direction: SortDirection::Asc,
					missing: MissingOrder::Last,
				},
				SortRequest {
					field: fields[second].name.clone(),
					direction: SortDirection::Desc,
					missing: MissingOrder::First,
				},
			];
			let dynamic = collect(&searcher, DynamicSortKeys::new(&sorts, &fields).unwrap(), 137);
			let native = collect_native(
				&searcher,
				&AllQuery,
				validate_requests(Some(&sorts), &[], &fields).unwrap(),
				137,
				None,
				Vec::new(),
				None,
			)
			.unwrap();
			assert_eq!(
				native.hits.into_iter().map(|(_, _, doc)| doc).collect::<Vec<_>>(),
				dynamic
			);
		}
	}
	#[test]
	fn runtime_sort_handles_missing_values_in_entire_segments() {
		let mut schema = Schema::builder();
		let id = schema.add_text_field("__fulltext_id", STRING | FAST);
		let group = schema.add_text_field("__fulltext_filter_0", STRING | FAST);
		let price = schema.add_f64_field("__fulltext_filter_1", FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer: IndexWriter<TantivyDocument> = index.writer_with_num_threads(1, 15_000_000).unwrap();
		writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
		writer
			.add_document(doc!(id => "a", group => "x", price => 2f64))
			.unwrap();
		writer.commit().unwrap();
		writer.add_document(doc!(id => "b", group => "x")).unwrap();
		writer.commit().unwrap();
		writer.add_document(doc!(id => "c", price => 1f64)).unwrap();
		writer.commit().unwrap();
		let searcher = index.reader().unwrap().searcher();
		let keys = DynamicSortKeys::new(&sorts(), &fields()).unwrap();
		let results = searcher
			.search(
				&AllQuery,
				&TopDocs::with_limit(3)
					.order_by((keys, (SortByString::for_field("__fulltext_id"), tantivy::Order::Asc))),
			)
			.unwrap();
		assert_eq!(
			results.into_iter().map(|((_, id), _)| id.unwrap()).collect::<Vec<_>>(),
			["b", "a", "c"]
		);
		let single = vec![sorts()[1].clone()];
		let fields = fields();
		let result = validate_requests(Some(&single), &[], &fields)
			.unwrap()
			.sorted_catalog(&searcher, &AllQuery, 0, 3, index.tokenizers().clone(), None)
			.unwrap();
		assert_eq!(
			result.hits.into_iter().map(|((_, id), _)| id).collect::<Vec<_>>(),
			["b", "a", "c"]
		);
	}
	#[test]
	fn duplicate_and_empty_sort_fields_are_rejected() {
		let fields = fields();
		let sorts = sorts();
		assert_eq!(
			validate_requests(Some(&[]), &[], &fields).unwrap_err().code,
			"E_INVALID_ARGUMENT"
		);
		assert_eq!(
			validate_requests(Some(&[sorts[0].clone(), sorts[0].clone()]), &[], &fields)
				.unwrap_err()
				.code,
			"E_INVALID_ARGUMENT"
		);
	}
	#[test]
	fn lazy_runtime_head_rejects_before_materializing_keys() {
		let index = fixture(10, true);
		let searcher = index.reader().unwrap().searcher();
		let sort = DynamicSortKeys::new(&sorts(), &fields()).unwrap();
		let mut child = sort.segment_sort_key_computer(searcher.segment_reader(0)).unwrap();
		let threshold = child.segment_sort_key(0, 1.0);
		assert!(child.accept_sort_key_lazy(9, 1.0, &threshold).is_none());
		assert_eq!(
			child.accept_sort_key_lazy(0, 1.0, &threshold).unwrap().0,
			Ordering::Equal
		);
	}
	#[test]
	fn dynamic_budget_charges_all_field_elements() {
		let index = fixture(1000, false);
		let searcher = index.reader().unwrap().searcher();
		check_budget(&searcher, &AllQuery, 1000, &sorts(), &fields(), None).unwrap();
		let repeated = vec![sorts()[0].clone(); 5000];
		assert_eq!(
			check_budget(&searcher, &AllQuery, 1000, &repeated, &fields(), None)
				.unwrap_err()
				.code,
			"E_RESOURCE_LIMIT"
		);
	}
	#[test]
	#[ignore = "manual runtime sort versus native tuple qualification"]
	fn runtime_sort_native_tuple_qualification() {
		for monotone in [true, false] {
			let index = fixture(50000, monotone);
			let searcher = index.reader().unwrap().searcher();
			let mut sorts = sorts();
			if monotone {
				sorts[0].direction = SortDirection::Desc;
			}
			let mut elapsed = Vec::new();
			for sample in 0..5 {
				let native = || {
					let started = Instant::now();
					let hits = collect(
						&searcher,
						(
							(SortByString::for_field("__fulltext_filter_0"), comparator(&sorts[0])),
							(
								SortByStaticFastValue::<f64>::for_field("__fulltext_filter_1"),
								comparator(&sorts[1]),
							),
						),
						10,
					);
					(hits, started.elapsed().as_secs_f64() * 1000.)
				};
				let dynamic = || {
					let started = Instant::now();
					let hits = collect(&searcher, DynamicSortKeys::new(&sorts, &fields()).unwrap(), 10);
					(hits, started.elapsed().as_secs_f64() * 1000.)
				};
				let (native, dynamic) = if sample % 2 == 0 {
					(native(), dynamic())
				} else {
					let dynamic = dynamic();
					(native(), dynamic)
				};
				assert_eq!(native.0, dynamic.0);
				elapsed.push((native.1, dynamic.1));
			}
			println!("runtime_sort_qualification adversarial={monotone} native_dynamic_ms={elapsed:?}");
		}
	}
	fn six_fields() -> Vec<FilterFieldConfig> {
		let mut fields = fields();
		fields.extend([
			FilterFieldConfig {
				name: "label".into(),
				field_type: FilterFieldType::String,
			},
			FilterFieldConfig {
				name: "rating".into(),
				field_type: FilterFieldType::Number,
			},
			FilterFieldConfig {
				name: "zone".into(),
				field_type: FilterFieldType::String,
			},
			FilterFieldConfig {
				name: "stock".into(),
				field_type: FilterFieldType::Number,
			},
		]);
		fields
	}
	fn sort_list(count: usize, adversarial: bool) -> Vec<SortRequest> {
		six_fields()
			.into_iter()
			.take(count)
			.enumerate()
			.map(|(position, field)| SortRequest {
				field: field.name,
				direction: if position % 2 == 1 || (position == 0 && adversarial) {
					SortDirection::Desc
				} else {
					SortDirection::Asc
				},
				missing: if position % 2 == 1 {
					MissingOrder::First
				} else {
					MissingOrder::Last
				},
			})
			.collect()
	}
	fn native_reference(
		searcher: &Searcher,
		sorts: &[SortRequest],
		end: usize,
		deadline: Option<Instant>,
	) -> Result<CollectedSortedCatalog> {
		let string = |position: usize| {
			(
				SortByString::for_field(format!("__fulltext_filter_{position}")),
				comparator(&sorts[position]),
			)
		};
		let number = |position: usize| {
			(
				SortByStaticFastValue::<f64>::for_field(format!("__fulltext_filter_{position}")),
				comparator(&sorts[position]),
			)
		};
		macro_rules! collect {
			($primary:expr) => {
				collect_sorted(searcher, &AllQuery, $primary, end, None, Vec::new(), deadline)
			};
		}
		match sorts.len() {
			2 => collect!((string(0), number(1))),
			3 => collect!((string(0), (number(1), string(2)))),
			4 => collect!((string(0), (number(1), (string(2), number(3))))),
			6 => collect!((string(0), (number(1), (string(2), (number(3), (string(4), number(5))))))),
			_ => unreachable!(),
		}
	}
	#[test]
	fn unrestricted_six_keys_match_nested_native_pairs() {
		let index = fixture(3000, false);
		let searcher = index.reader().unwrap().searcher();
		let sorts = sort_list(6, false);
		let native = native_reference(&searcher, &sorts, 137, None).unwrap();
		let dynamic = collect_sorted(
			&searcher,
			&AllQuery,
			DynamicSortKeys::new(&sorts, &six_fields()).unwrap(),
			137,
			None,
			Vec::new(),
			None,
		)
		.unwrap();
		assert_eq!(native.hits, dynamic.hits);
		assert_eq!(native.total, dynamic.total);
	}
	#[test]
	fn native_erased_three_and_four_keys_match_independent_missing_array_oracle() {
		#[derive(Clone, Copy)]
		struct Row<'a> {
			id: &'a str,
			group: Option<&'a str>,
			price: Option<f64>,
			label: Option<&'a str>,
			rating: Option<f64>,
		}
		let rows = [
			Row {
				id: "a",
				group: Some("x"),
				price: Some(2.),
				label: Some("a"),
				rating: Some(3.),
			},
			Row {
				id: "b",
				group: Some("x"),
				price: Some(2.),
				label: Some("a"),
				rating: Some(4.),
			},
			Row {
				id: "c",
				group: Some("x"),
				price: Some(1.),
				label: Some("b"),
				rating: Some(5.),
			},
			Row {
				id: "d",
				group: None,
				price: Some(2.),
				label: Some("a"),
				rating: None,
			},
			Row {
				id: "e",
				group: Some("x"),
				price: None,
				label: None,
				rating: Some(2.),
			},
			Row {
				id: "f",
				group: Some("x"),
				price: Some(2.),
				label: Some("a"),
				rating: Some(4.),
			},
			Row {
				id: "h",
				group: Some(""),
				price: Some(0.),
				label: Some(""),
				rating: Some(0.),
			},
			Row {
				id: "deleted",
				group: Some(""),
				price: Some(100.),
				label: Some(""),
				rating: Some(100.),
			},
			Row {
				id: "g",
				group: None,
				price: None,
				label: None,
				rating: None,
			},
		];
		let mut schema = Schema::builder();
		let id = schema.add_text_field("__fulltext_id", STRING | FAST);
		let group = schema.add_text_field("__fulltext_filter_0", STRING | FAST);
		let price = schema.add_f64_field("__fulltext_filter_1", FAST);
		let label = schema.add_text_field("__fulltext_filter_2", STRING | FAST);
		let rating = schema.add_f64_field("__fulltext_filter_3", FAST);
		let index = Index::create_in_ram(schema.build());
		let mut writer: IndexWriter<TantivyDocument> = index.writer_with_num_threads(1, 15_000_000).unwrap();
		writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
		for (position, row) in rows.iter().enumerate() {
			let mut document = TantivyDocument::default();
			document.add_text(id, row.id);
			if let Some(value) = row.group {
				document.add_text(group, value);
				document.add_text(group, "other");
			}
			if let Some(value) = row.price {
				document.add_f64(price, value);
				document.add_f64(price, -99.);
			}
			if let Some(value) = row.label {
				document.add_text(label, value);
				document.add_text(label, "later");
			}
			if let Some(value) = row.rating {
				document.add_f64(rating, value);
				document.add_f64(rating, 99.);
			}
			writer.add_document(document).unwrap();
			if position % 2 == 1 {
				writer.commit().unwrap();
			}
		}
		writer.commit().unwrap();
		writer.delete_term(tantivy::Term::from_field_text(id, "deleted"));
		writer.commit().unwrap();
		let searcher = index.reader().unwrap().searcher();
		let fields = six_fields();
		fn compare<T: PartialOrd>(left: Option<T>, right: Option<T>, sort: &SortRequest) -> Ordering {
			match (left, right) {
				(None, None) => Ordering::Equal,
				(None, Some(_)) => {
					if sort.missing == MissingOrder::First {
						Ordering::Less
					} else {
						Ordering::Greater
					}
				}
				(Some(_), None) => {
					if sort.missing == MissingOrder::First {
						Ordering::Greater
					} else {
						Ordering::Less
					}
				}
				(Some(left), Some(right)) => {
					let order = left.partial_cmp(&right).unwrap();
					if sort.direction == SortDirection::Asc {
						order
					} else {
						order.reverse()
					}
				}
			}
		}
		for count in [3, 4] {
			for directions in 0..1 << count {
				for missing in 0..1 << count {
					let mut sorts = sort_list(count, false);
					for (position, sort) in sorts.iter_mut().enumerate() {
						sort.direction = if directions & (1 << position) == 0 {
							SortDirection::Asc
						} else {
							SortDirection::Desc
						};
						sort.missing = if missing & (1 << position) == 0 {
							MissingOrder::First
						} else {
							MissingOrder::Last
						};
					}
					let mut expected: Vec<_> = rows.iter().filter(|row| row.id != "deleted").collect();
					expected.sort_by(|left, right| {
						compare(left.group, right.group, &sorts[0])
							.then_with(|| compare(left.price, right.price, &sorts[1]))
							.then_with(|| compare(left.label, right.label, &sorts[2]))
							.then_with(|| {
								if count == 4 {
									compare(left.rating, right.rating, &sorts[3])
								} else {
									Ordering::Equal
								}
							})
							.then_with(|| left.id.cmp(right.id))
					});
					let catalog = validate_requests(Some(&sorts), &[], &fields).unwrap();
					let result = catalog
						.sorted_catalog(&searcher, &AllQuery, 0, 20, index.tokenizers().clone(), None)
						.unwrap();
					assert_eq!(result.total, expected.len() as u64);
					assert_eq!(
						result.hits.into_iter().map(|((_, id), _)| id).collect::<Vec<_>>(),
						expected.into_iter().map(|row| row.id.to_owned()).collect::<Vec<_>>(),
						"count={count} directions={directions} missing={missing}"
					);
				}
			}
		}
	}
	#[test]
	#[ignore = "manual production sort collector qualification"]
	fn runtime_production_sort_qualification() {
		for key_count in [2, 3, 4, 6] {
			for adversarial in [true, false] {
				let index = fixture(50000, adversarial);
				let searcher = index.reader().unwrap().searcher();
				let sorts = sort_list(key_count, adversarial);
				let fields = six_fields();
				let catalog = validate_requests(Some(&sorts), &[], &fields).unwrap();
				let mut elapsed = Vec::new();
				for sample in 0..9 {
					let native = || {
						let deadline = Some(Instant::now() + std::time::Duration::from_secs(30));
						let started = Instant::now();
						let hits = native_reference(&searcher, &sorts, 10, deadline).unwrap();
						(hits, started.elapsed().as_secs_f64() * 1000.)
					};
					let production = || {
						let deadline = Some(Instant::now() + std::time::Duration::from_secs(30));
						let started = Instant::now();
						let hits = if key_count <= 4 {
							collect_native(&searcher, &AllQuery, catalog, 10, None, Vec::new(), deadline)
						} else {
							collect_sorted(
								&searcher,
								&AllQuery,
								DynamicSortKeys::new(&sorts, &fields).unwrap(),
								10,
								None,
								Vec::new(),
								deadline,
							)
						}
						.unwrap();
						(hits, started.elapsed().as_secs_f64() * 1000.)
					};
					let (native, production) = if sample % 2 == 0 {
						(native(), production())
					} else {
						let production = production();
						(native(), production)
					};
					assert_eq!(native.0.hits, production.0.hits);
					assert_eq!(native.0.total, production.0.total);
					elapsed.push((native.1, production.1));
				}
				println!("runtime_production_sort_qualification keys={key_count} adversarial={adversarial} native_production_ms={elapsed:?}");
			}
		}
	}
}
