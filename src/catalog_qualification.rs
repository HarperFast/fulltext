use super::*;
use std::time::Duration;
use tantivy::query::{BooleanQuery, FuzzyTermQuery, Occur, TermQuery};
use tantivy::schema::{IndexRecordOption, Schema, FAST, INDEXED, STRING, TEXT};
use tantivy::{doc, Index, Term};

const MATCHES: usize = 1_000_000;
const SEGMENTS: usize = 10;

fn paired_case(name: &str, matches: usize, mut run: impl FnMut(bool) -> String) {
	let expected = run(false);
	assert_eq!(run(true), expected);
	let mut native = Vec::new();
	let mut guarded = Vec::new();
	for pair in 0..5 {
		for wrapped in if pair % 2 == 0 { [false, true] } else { [true, false] } {
			let start = Instant::now();
			let actual = run(wrapped);
			let elapsed = start.elapsed().as_secs_f64() * 1_000.0;
			assert_eq!(actual, expected);
			if wrapped {
				guarded.push(elapsed);
			} else {
				native.push(elapsed);
			}
		}
	}
	native.sort_by(f64::total_cmp);
	guarded.sort_by(f64::total_cmp);
	println!("{{\"qualification\":\"metadata_collector\",\"case\":\"{name}\",\"matches\":{matches},\"segments\":{SEGMENTS},\"pairs\":5,\"nativeMedianMs\":{:.3},\"deadlineMedianMs\":{:.3},\"ratio\":{:.4}}}", native[2], guarded[2], guarded[2] / native[2]);
}

#[test]
#[ignore = "one-million-document paired release collector qualification"]
fn metadata_collector_qualification() {
	let mut builder = Schema::builder();
	let text = builder.add_text_field("text", TEXT);
	let flags = builder.add_text_field("flags", TEXT);
	let multivalue = builder.add_f64_field("__fulltext_filter_3", INDEXED | FAST);
	let id = builder.add_text_field("__fulltext_id", STRING | FAST);
	let brand = builder.add_text_field("__fulltext_filter_0", STRING | FAST);
	let price = builder.add_f64_field("__fulltext_filter_1", INDEXED | FAST);
	let unsafe_price = builder.add_f64_field("__fulltext_filter_2", INDEXED | FAST);
	let index = Index::create_in_ram(builder.build());
	let mut writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
	writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
	for segment in 0..SEGMENTS {
		for row in 0..MATCHES / SEGMENTS {
			let number = segment * (MATCHES / SEGMENTS) + row;
			writer.add_document(doc!(text => "shoe", flags => if number.is_multiple_of(2) { "red" } else { "blue" }, multivalue => (number % 1000) as f64, multivalue => (number % 1000) as f64 + 0.25, id => format!("item-{number:07}"), brand => format!("brand-{:02}", number % 32), price => (number % 1000) as f64, unsafe_price => (number % 1000) as f64)).unwrap();
		}
		if segment == SEGMENTS - 1 {
			writer
				.add_document(
					doc!(text => "other", id => "outlier", brand => "outlier", price => 1.0, unsafe_price => f64::MAX),
				)
				.unwrap();
		}
		writer.commit().unwrap();
	}
	writer.wait_merging_threads().unwrap();
	let reader = index.reader().unwrap();
	let searcher = reader.searcher();
	assert_eq!(searcher.segment_readers().len(), SEGMENTS);
	let query = TermQuery::new(Term::from_field_text(text, "shoe"), IndexRecordOption::WithFreqs);
	assert_eq!(searcher.search(&query, &Count).unwrap(), MATCHES);
	let mut fields = vec![
		FilterFieldConfig {
			name: "brand".into(),
			field_type: FilterFieldType::String,
		},
		FilterFieldConfig {
			name: "price".into(),
			field_type: FilterFieldType::Number,
		},
		FilterFieldConfig {
			name: "unsafePrice".into(),
			field_type: FilterFieldType::Number,
		},
	];
	let mut dictionary_passes = Vec::new();
	for pass in 0..2 {
		let start = Instant::now();
		let deadline = Some(Instant::now() + Duration::from_secs(30));
		let maxima = searcher
			.segment_readers()
			.iter()
			.map(|segment| max_dictionary_key(segment, "__fulltext_id", deadline).unwrap())
			.collect::<Vec<_>>();
		assert_eq!(maxima, vec![12; SEGMENTS]);
		dictionary_passes.push(start.elapsed().as_secs_f64() * 1_000.0);
		if pass == 1 {
			let cache = dictionary_cache().lock().unwrap();
			for segment in searcher.segment_readers() {
				assert_eq!(cache.maxima.get(&(segment.segment_id(), id)), Some(&12));
			}
		}
	}
	println!("{{\"qualification\":\"metadata_collector\",\"case\":\"id_dictionary_maximum\",\"keys\":{},\"segments\":{SEGMENTS},\"coldMs\":{:.3},\"warmMs\":{:.3}}}", MATCHES + 1, dictionary_passes[0], dictionary_passes[1]);
	paired_case("numeric_sort_count", MATCHES, |wrapped| {
		let collector = (
			TopDocs::for_doc_range(0..20).order_by((
				(
					SortByStaticFastValue::<f64>::for_field("__fulltext_filter_1"),
					ComparatorEnum::ReverseNoneLower,
				),
				(
					(SortByString::for_field("__fulltext_id"), tantivy::Order::Asc),
					SortBySimilarityScore,
				),
			)),
			Count,
		);
		let result = if wrapped {
			searcher
				.search(
					&query,
					&DeadlineCollector {
						inner: collector,
						histograms: vec![],
						deadline: Some(Instant::now() + Duration::from_secs(30)),
					},
				)
				.unwrap()
		} else {
			searcher.search(&query, &collector).unwrap()
		};
		assert_eq!(result.1, MATCHES);
		assert_eq!(result.0.len(), 20);
		format!("{result:?}")
	});
	for (name, requests) in [
		(
			"terms_stats",
			vec![
				AggregationRequest {
					aggregations: Vec::new(),
					name: "brands".into(),
					field: "brand".into(),
					kind: AggregationKind::Terms { size: 32 },
				},
				AggregationRequest {
					aggregations: Vec::new(),
					name: "prices".into(),
					field: "price".into(),
					kind: AggregationKind::Stats,
				},
			],
		),
		(
			"safe_histogram",
			vec![AggregationRequest {
				aggregations: Vec::new(),
				name: "prices".into(),
				field: "price".into(),
				kind: AggregationKind::Histogram { interval: 10.0 },
			}],
		),
		(
			"unsafe_histogram_unmatched_outlier",
			vec![AggregationRequest {
				aggregations: Vec::new(),
				name: "prices".into(),
				field: "unsafePrice".into(),
				kind: AggregationKind::Histogram { interval: 10.0 },
			}],
		),
	] {
		paired_case(name, MATCHES, |wrapped| {
			let deadline = Some(Instant::now() + Duration::from_secs(30));
			let collector = (
				Count,
				prepare_aggregations(
					&searcher,
					&query,
					&requests,
					&fields,
					index.tokenizers().clone(),
					deadline,
				)
				.unwrap(),
			);
			let (count, result) = if wrapped {
				searcher
					.search(
						&query,
						&DeadlineCollector {
							inner: collector,
							histograms: histogram_checks(&requests, &fields).unwrap(),
							deadline,
						},
					)
					.unwrap()
			} else {
				searcher.search(&query, &collector).unwrap()
			};
			assert_eq!(count, MATCHES);
			let results = finish_aggregations(result, &requests, deadline).unwrap();
			for result in &results {
				match &result.value {
					AggregationValue::Stats { count, sum, .. } => {
						assert_eq!(*count, MATCHES as u64);
						assert_eq!(*sum, 499_500_000.0);
					}
					AggregationValue::Terms { buckets, .. } => {
						assert_eq!(buckets.len(), 32);
						assert_eq!(buckets.iter().map(|bucket| bucket.count).sum::<u64>(), MATCHES as u64);
					}
					AggregationValue::Histogram { buckets } => {
						assert_eq!(buckets.len(), 100);
						assert_eq!(buckets.iter().map(|bucket| bucket.count).sum::<u64>(), MATCHES as u64);
					}
					_ => panic!("unexpected qualification aggregation"),
				}
			}
			format!("{results:?}")
		});
	}
	let term = |field, value| {
		Box::new(TermQuery::new(
			Term::from_field_text(field, value),
			IndexRecordOption::WithFreqs,
		)) as Box<dyn Query>
	};
	let dense_and = BooleanQuery::new(vec![
		(Occur::Must, term(text, "shoe")),
		(Occur::Must, term(flags, "red")),
	]);
	let dense_or = BooleanQuery::new(vec![
		(Occur::Should, term(flags, "red")),
		(Occur::Should, term(flags, "blue")),
	]);
	for (name, query, expected_count) in [
		("bare_count_term", &query as &dyn Query, MATCHES),
		("bare_count_dense_and", &dense_and as &dyn Query, MATCHES / 2),
		("bare_count_dense_or", &dense_or as &dyn Query, MATCHES),
	] {
		paired_case(name, expected_count, |wrapped| {
			let count = if wrapped {
				count_matches(&searcher, query, Some(Instant::now() + Duration::from_secs(30))).unwrap() as usize
			} else {
				searcher.search(query, &Count).unwrap()
			};
			assert_eq!(count, expected_count);
			count.to_string()
		});
	}
	let mut writer = index
		.writer_with_num_threads::<tantivy::schema::TantivyDocument>(1, 50_000_000)
		.unwrap();
	writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
	for value in 0..16 {
		writer.delete_term(Term::from_field_text(brand, &format!("brand-{value:02}")));
	}
	writer.commit().unwrap();
	writer.wait_merging_threads().unwrap();
	reader.reload().unwrap();
	let searcher = reader.searcher();
	assert_eq!(searcher.num_docs(), (MATCHES / 2 + 1) as u64);
	assert_eq!(searcher.segment_readers().len(), SEGMENTS);
	fields.push(FilterFieldConfig {
		name: "multivalue".into(),
		field_type: FilterFieldType::Number,
	});
	let term = |field, value| {
		Box::new(TermQuery::new(
			Term::from_field_text(field, value),
			IndexRecordOption::WithFreqs,
		)) as Box<dyn Query>
	};
	let boolean = BooleanQuery::new(vec![
		(Occur::Must, term(text, "shoe")),
		(
			Occur::Must,
			Box::new(BooleanQuery::new(vec![
				(Occur::Should, term(flags, "red")),
				(Occur::Should, term(flags, "blue")),
			])),
		),
		(Occur::MustNot, term(brand, "brand-31")),
	]);
	let fuzzy = FuzzyTermQuery::new(Term::from_field_text(text, "shoo"), 1, true);
	let boolean_count = MATCHES / 2 - MATCHES / 32;
	assert_eq!(searcher.search(&boolean, &Count).unwrap(), boolean_count);
	assert_eq!(searcher.search(&fuzzy, &Count).unwrap(), MATCHES / 2);
	paired_case("deleted_multivalue_boolean_scored_sort", boolean_count, |wrapped| {
		let collector = (
			TopDocs::for_doc_range(0..20).order_by((
				(
					SortByStaticFastValue::<f64>::for_field("__fulltext_filter_3"),
					ComparatorEnum::ReverseNoneLower,
				),
				(
					(SortByString::for_field("__fulltext_id"), tantivy::Order::Asc),
					SortBySimilarityScore,
				),
			)),
			Count,
		);
		let result = if wrapped {
			searcher
				.search(
					&boolean,
					&DeadlineCollector {
						inner: collector,
						histograms: vec![],
						deadline: Some(Instant::now() + Duration::from_secs(30)),
					},
				)
				.unwrap()
		} else {
			searcher.search(&boolean, &collector).unwrap()
		};
		assert_eq!(result.1, boolean_count);
		assert_eq!(result.0.len(), 20);
		format!("{result:?}")
	});
	let requests = vec![
		AggregationRequest {
			aggregations: Vec::new(),
			name: "stats".into(),
			field: "multivalue".into(),
			kind: AggregationKind::Stats,
		},
		AggregationRequest {
			aggregations: Vec::new(),
			name: "histogram".into(),
			field: "multivalue".into(),
			kind: AggregationKind::Histogram { interval: 10.0 },
		},
		AggregationRequest {
			aggregations: Vec::new(),
			name: "brands".into(),
			field: "brand".into(),
			kind: AggregationKind::Terms { size: 32 },
		},
	];
	for (name, query, expected_count, exclude_last) in [
		(
			"deleted_multivalue_boolean_aggregations",
			&boolean as &dyn Query,
			boolean_count,
			true,
		),
		(
			"deleted_multivalue_fuzzy_aggregations",
			&fuzzy as &dyn Query,
			MATCHES / 2,
			false,
		),
	] {
		let expected_sum = (0..MATCHES)
			.filter(|number| number % 32 >= 16 && (!exclude_last || number % 32 != 31))
			.map(|number| 2.0 * (number % 1000) as f64 + 0.25)
			.sum::<f64>();
		paired_case(name, expected_count, |wrapped| {
			let deadline = Some(Instant::now() + Duration::from_secs(30));
			let collector = (
				Count,
				prepare_aggregations(
					&searcher,
					query,
					&requests,
					&fields,
					index.tokenizers().clone(),
					deadline,
				)
				.unwrap(),
			);
			let (count, result) = if wrapped {
				searcher
					.search(
						query,
						&DeadlineCollector {
							inner: collector,
							histograms: histogram_checks(&requests, &fields).unwrap(),
							deadline,
						},
					)
					.unwrap()
			} else {
				searcher.search(query, &collector).unwrap()
			};
			assert_eq!(count, expected_count);
			let results = finish_aggregations(result, &requests, deadline).unwrap();
			for result in &results {
				match &result.value {
					AggregationValue::Stats { count, sum, .. } => {
						assert_eq!(*count, (expected_count * 2) as u64);
						assert_eq!(*sum, expected_sum);
					}
					AggregationValue::Histogram { buckets } => {
						assert_eq!(buckets.len(), 100);
						assert_eq!(
							buckets.iter().map(|bucket| bucket.count).sum::<u64>(),
							expected_count as u64
						);
					}
					AggregationValue::Terms { buckets, .. } => {
						assert_eq!(buckets.len(), if exclude_last { 15 } else { 16 });
						assert_eq!(
							buckets.iter().map(|bucket| bucket.count).sum::<u64>(),
							expected_count as u64
						);
					}
					_ => panic!("unexpected expanded qualification aggregation"),
				}
			}
			format!("{results:?}")
		});
	}
}

#[test]
#[ignore = "concurrent multi-index metadata cache qualification"]
fn dictionary_cache_concurrency_qualification() {
	use std::sync::Barrier;
	const INDEXES: usize = 8;
	const SEGMENTS_PER_INDEX: usize = 40;
	const ROWS_PER_SEGMENT: usize = 128;
	let fields = vec![FilterFieldConfig {
		name: "brand".into(),
		field_type: FilterFieldType::String,
	}];
	let requests = vec![AggregationRequest {
		aggregations: Vec::new(),
		name: "brands".into(),
		field: "brand".into(),
		kind: AggregationKind::Terms { size: 10 },
	}];
	let searchers = (0..INDEXES)
		.map(|_| {
			let mut schema = Schema::builder();
			let brand = schema.add_text_field("__fulltext_filter_0", STRING | FAST);
			let index = Index::create_in_ram(schema.build());
			let mut writer = index.writer_with_num_threads(1, 20_000_000).unwrap();
			writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
			for _ in 0..SEGMENTS_PER_INDEX {
				for row in 0..ROWS_PER_SEGMENT {
					writer.add_document(doc!(brand => format!("brand-{row:04}"))).unwrap();
				}
				writer.commit().unwrap();
			}
			writer.wait_merging_threads().unwrap();
			let searcher = index.reader().unwrap().searcher();
			assert_eq!(searcher.segment_readers().len(), SEGMENTS_PER_INDEX);
			check_terms_budget(
				&searcher,
				&tantivy::query::AllQuery,
				&requests,
				&fields,
				None,
				TokenizerManager::default(),
			)
			.unwrap();
			searcher
		})
		.collect::<Vec<_>>();
	{
		let cache = dictionary_cache().lock().unwrap();
		for searcher in &searchers {
			for segment in searcher.segment_readers() {
				let field = segment.schema().get_field("__fulltext_filter_0").unwrap();
				assert_eq!(cache.maxima.get(&(segment.segment_id(), field)), Some(&10));
			}
		}
		println!(
			"{{\"qualification\":\"dictionary_cache_warmup\",\"entries\":{},\"expectedEntries\":{}}}",
			cache.maxima.len(),
			INDEXES * SEGMENTS_PER_INDEX
		);
	}
	for workers in [1, 4, 8] {
		for preflight_only in [true, false] {
			let iterations = if preflight_only { 2000 } else { 100 };
			let mut latencies = Vec::new();
			let mut walls = Vec::new();
			for _ in 0..5 {
				let barrier = Barrier::new(workers);
				let start = Instant::now();
				let samples = std::thread::scope(|scope| {
					let handles = searchers
						.iter()
						.take(workers)
						.map(|searcher| {
							let barrier = &barrier;
							let requests = &requests;
							let fields = &fields;
							scope.spawn(move || {
								let mut samples = Vec::with_capacity(iterations);
								barrier.wait();
								for _ in 0..iterations {
									let start = Instant::now();
									let deadline = Some(start + Duration::from_secs(30));
									if preflight_only {
										check_terms_budget(
											searcher,
											&tantivy::query::AllQuery,
											requests,
											fields,
											deadline,
											TokenizerManager::default(),
										)
										.unwrap();
									} else {
										let (count, results) = aggregate_count(
											searcher,
											&tantivy::query::AllQuery,
											requests,
											fields,
											TokenizerManager::default(),
											deadline,
										)
										.unwrap();
										assert_eq!(count, (SEGMENTS_PER_INDEX * ROWS_PER_SEGMENT) as u64);
										let AggregationValue::Terms { buckets, .. } = &results[0].value else {
											panic!("expected terms")
										};
										assert_eq!(buckets.len(), 10);
										assert!(buckets.iter().all(|bucket| bucket.count == SEGMENTS_PER_INDEX as u64));
									}
									samples.push(start.elapsed().as_secs_f64() * 1000.0);
								}
								samples
							})
						})
						.collect::<Vec<_>>();
					handles
						.into_iter()
						.flat_map(|handle| handle.join().unwrap())
						.collect::<Vec<_>>()
				});
				walls.push(start.elapsed().as_secs_f64() * 1000.0);
				latencies.extend(samples);
			}
			latencies.sort_by(f64::total_cmp);
			walls.sort_by(f64::total_cmp);
			println!("{{\"qualification\":\"dictionary_cache_concurrency\",\"preflightOnly\":{preflight_only},\"indexes\":{workers},\"segmentsPerIndex\":{SEGMENTS_PER_INDEX},\"iterationsPerWorker\":{iterations},\"samples\":{},\"p50Ms\":{:.6},\"p95Ms\":{:.6},\"medianWallMs\":{:.3}}}", latencies.len(), latencies[latencies.len()/2], latencies[latencies.len()*95/100], walls[2]);
		}
	}
}
