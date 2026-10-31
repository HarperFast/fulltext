use std::collections::BTreeMap;
use std::fmt::Write;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tantivy::index::SegmentId;
use tantivy::{Opstamp, Searcher};

use crate::error::ERROR_CODES;

pub(crate) const OP_NAMES: [&str; 8] = [
	"applyFrame",
	"commit",
	"publish",
	"reload",
	"close",
	"search",
	"expensiveSearch",
	"trace",
];
pub(crate) const BOUNDS: [u64; 12] = [
	1_000,
	10_000,
	50_000,
	100_000,
	500_000,
	1_000_000,
	5_000_000,
	10_000_000,
	50_000_000,
	100_000_000,
	1_000_000_000,
	u64::MAX,
];

pub(crate) fn nanos(duration: Duration) -> u64 {
	duration.as_nanos().min(u64::MAX as u128) as u64
}
fn load(value: &AtomicU64) -> u64 {
	value.load(Relaxed)
}
fn add(value: &AtomicU64, amount: u64) {
	value.fetch_add(amount, Relaxed);
}

#[derive(Default)]
pub(crate) struct Latency {
	count: AtomicU64,
	sum: AtomicU64,
	histogram_count: AtomicU64,
	histogram_sum: AtomicU64,
	buckets: [AtomicU64; 12],
}

impl Latency {
	pub(crate) fn record(&self, duration: Duration, enabled: bool) {
		let value = nanos(duration);
		add(&self.count, 1);
		add(&self.sum, value);
		if enabled {
			let bucket = BOUNDS
				.iter()
				.position(|bound| value <= *bound)
				.unwrap_or(BOUNDS.len() - 1);
			add(&self.buckets[bucket], 1);
			add(&self.histogram_count, 1);
			add(&self.histogram_sum, value);
		}
	}
	fn json(&self) -> String {
		let mut values = LatencyValues::default();
		values.include(self);
		values.json()
	}
}

#[repr(align(128))]
#[derive(Default)]
struct Admission {
	attempted: AtomicU64,
	accepted: AtomicU64,
	rejected: AtomicU64,
	request_bytes: AtomicU64,
}
#[repr(align(128))]
#[derive(Default)]
struct ActorCounters {
	dispatched: AtomicU64,
	succeeded: AtomicU64,
	failed: AtomicU64,
	queue: Latency,
	execution: Latency,
}
struct Actor {
	counters: ActorCounters,
	mutations: AtomicU64,
	errors: [AtomicU64; ERROR_CODES.len() + 1],
}
impl Default for Actor {
	fn default() -> Self {
		Self {
			counters: ActorCounters::default(),
			mutations: AtomicU64::new(0),
			errors: std::array::from_fn(|_| AtomicU64::new(0)),
		}
	}
}
#[derive(Default)]
struct LatencyValues {
	count: u64,
	sum: u64,
	histogram_count: u64,
	histogram_sum: u64,
	buckets: [u64; 12],
}
impl LatencyValues {
	fn include(&mut self, source: &Latency) {
		self.count = self.count.wrapping_add(load(&source.count));
		self.sum = self.sum.wrapping_add(load(&source.sum));
		self.histogram_count = self.histogram_count.wrapping_add(load(&source.histogram_count));
		self.histogram_sum = self.histogram_sum.wrapping_add(load(&source.histogram_sum));
		for (value, source) in self.buckets.iter_mut().zip(&source.buckets) {
			*value = value.wrapping_add(load(source));
		}
	}
	fn json(&self) -> String {
		let buckets = self
			.buckets
			.iter()
			.map(|value| format!("\"{value}\""))
			.collect::<Vec<_>>()
			.join(",");
		let bounds = BOUNDS
			.iter()
			.enumerate()
			.map(|(index, value)| {
				if index + 1 == BOUNDS.len() {
					"null".to_owned()
				} else {
					format!("\"{value}\"")
				}
			})
			.collect::<Vec<_>>()
			.join(",");
		format!("{{\"count\":\"{}\",\"sumNanoseconds\":\"{}\",\"histogram\":{{\"count\":\"{}\",\"sumNanoseconds\":\"{}\",\"buckets\":[{buckets}],\"upperBoundsNanoseconds\":[{bounds}]}}}}", self.count,self.sum,self.histogram_count,self.histogram_sum)
	}
}
#[derive(Default)]
struct ActorValues {
	dispatched: u64,
	succeeded: u64,
	failed: u64,
	queue: LatencyValues,
	execution: LatencyValues,
}
impl ActorValues {
	fn include(&mut self, source: &ActorCounters) {
		self.dispatched = self.dispatched.wrapping_add(load(&source.dispatched));
		self.succeeded = self.succeeded.wrapping_add(load(&source.succeeded));
		self.failed = self.failed.wrapping_add(load(&source.failed));
		self.queue.include(&source.queue);
		self.execution.include(&source.execution);
	}
}
#[derive(Default)]
struct Operation {
	admission: Admission,
	actor: Actor,
}

#[derive(Default)]
struct ReaderStats {
	error: Option<&'static str>,
	available: bool,
	documents: u64,
	deleted: u64,
	segments: u64,
	generation: u64,
	observed: Option<Instant>,
	duration: u64,
	signature: BTreeMap<SegmentId, Option<Opstamp>>,
	bytes: Option<u64>,
	bytes_observed: Option<Instant>,
	bytes_signature: Option<BTreeMap<SegmentId, Option<Opstamp>>>,
	bytes_pending: bool,
	bytes_error: Option<&'static str>,
	bytes_duration: u64,
}

struct ReaderValues {
	documents: u64,
	deleted: u64,
	segments: u64,
	generation: u64,
	signature: BTreeMap<SegmentId, Option<Opstamp>>,
}
pub(crate) struct ReaderObservation {
	values: Option<ReaderValues>,
	duration: u64,
}

#[cfg(feature = "test-panic")]
static LIVE_METRICS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "test-panic")]
pub(crate) fn live_metrics() -> u32 {
	LIVE_METRICS.load(Relaxed) as u32
}

pub(crate) struct Metrics {
	#[cfg(feature = "test-panic")]
	fail_observation: AtomicBool,
	operations: [Operation; 8],
	search_workers: Box<[[ActorCounters; 3]]>,
	pub(crate) histograms: AtomicBool,
	pub(crate) snapshot_bytes: AtomicBool,
	pub(crate) state: Arc<AtomicU8>,
	teardown_started: Mutex<Option<Instant>>,
	phases: [Latency; 3],
	phase_succeeded: [AtomicU64; 3],
	phase_failed: [AtomicU64; 3],
	reader: Mutex<ReaderStats>,
}

impl Metrics {
	#[cfg(test)]
	pub(crate) fn new() -> Arc<Self> {
		Self::with_search_workers(0)
	}
	pub(crate) fn with_search_workers(workers: usize) -> Arc<Self> {
		#[cfg(feature = "test-panic")]
		LIVE_METRICS.fetch_add(1, Relaxed);
		Arc::new(Self {
			#[cfg(feature = "test-panic")]
			fail_observation: AtomicBool::new(false),
			operations: std::array::from_fn(|_| Operation::default()),
			search_workers: (0..workers)
				.map(|_| std::array::from_fn(|_| ActorCounters::default()))
				.collect(),
			histograms: AtomicBool::new(false),
			snapshot_bytes: AtomicBool::new(false),
			state: Arc::new(AtomicU8::new(0)),
			teardown_started: Mutex::new(None),
			phases: std::array::from_fn(|_| Latency::default()),
			phase_succeeded: std::array::from_fn(|_| AtomicU64::new(0)),
			phase_failed: std::array::from_fn(|_| AtomicU64::new(0)),
			reader: Mutex::new(ReaderStats::default()),
		})
	}
	pub(crate) fn attempt(&self, operation: usize) {
		add(&self.operations[operation].admission.attempted, 1);
	}
	pub(crate) fn reject(&self, operation: usize, error: &str) {
		let op = &self.operations[operation];
		add(&op.admission.rejected, 1);
		let i = ERROR_CODES
			.iter()
			.position(|v| *v == error)
			.unwrap_or(ERROR_CODES.len());
		add(&op.actor.errors[i], 1);
	}
	fn actor_values(&self, operation: usize) -> ActorValues {
		let mut values = ActorValues::default();
		values.include(&self.operations[operation].actor.counters);
		if let Some(class) = operation.checked_sub(5).filter(|class| *class < 3) {
			for worker in &self.search_workers {
				values.include(&worker[class]);
			}
		}
		values
	}
	pub(crate) fn ticket(self: &Arc<Self>, operation: usize, bytes: usize) -> Ticket {
		Ticket {
			metrics: self.clone(),
			operation,
			worker: None,
			bytes: bytes as u64,
			accepted: false,
			dispatched: false,
			settled: false,
			enqueued: None,
			started: None,
			execution: None,
		}
	}

	pub(crate) fn begin_teardown(&self) {
		self.begin_teardown_at(Instant::now());
	}
	fn begin_teardown_at(&self, started: Instant) {
		self.teardown_started
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.get_or_insert(started);
	}
	pub(crate) fn finish_teardown(&self, success: bool) {
		self.finish_teardown_at(Instant::now(), success);
	}
	fn finish_teardown_at(&self, finished: Instant, success: bool) {
		if let Some(started) = self.teardown_started.lock().unwrap_or_else(|e| e.into_inner()).take() {
			self.phase(2, finished.saturating_duration_since(started), success);
		}
	}
	#[cfg(test)]
	pub(crate) fn teardown_pending(&self) -> bool {
		self.teardown_started
			.lock()
			.unwrap_or_else(|e| e.into_inner())
			.is_some()
	}
	pub(crate) fn phase(&self, phase: usize, duration: Duration, success: bool) {
		add(
			if success {
				&self.phase_succeeded[phase]
			} else {
				&self.phase_failed[phase]
			},
			1,
		);
		self.phases[phase].record(duration, self.histograms.load(Relaxed));
	}
	pub(crate) fn observe_reader(&self, searcher: &Searcher) {
		self.install_reader(Self::prepare_reader(searcher));
	}
	pub(crate) fn prepare_reader(searcher: &Searcher) -> ReaderObservation {
		let started = Instant::now();
		let values = catch_unwind(AssertUnwindSafe(|| ReaderValues {
			documents: searcher.num_docs(),
			deleted: searcher
				.segment_readers()
				.iter()
				.map(|segment| u64::from(segment.num_deleted_docs()))
				.sum(),
			segments: searcher.segment_readers().len() as u64,
			generation: searcher.generation().generation_id(),
			signature: searcher.generation().segments().clone(),
		}))
		.ok();
		ReaderObservation {
			values,
			duration: nanos(started.elapsed()),
		}
	}
	pub(crate) fn install_reader(&self, observation: ReaderObservation) {
		if let Some(ReaderValues {
			documents,
			deleted,
			segments,
			generation,
			signature,
		}) = observation.values
		{
			let mut reader = self.reader.lock().unwrap_or_else(|e| e.into_inner());
			reader.available = true;
			reader.error = None;
			reader.documents = documents;
			reader.deleted = deleted;
			reader.segments = segments;
			reader.generation = generation;
			reader.observed = Some(Instant::now());
			reader.duration = observation.duration;
			if reader.bytes_signature.as_ref() != Some(&signature) {
				reader.bytes = None;
				reader.bytes_observed = None;
				reader.bytes_signature = None;
				reader.bytes_error = None;
			}
			reader.bytes_pending =
				self.snapshot_bytes.load(Relaxed) && reader.bytes_signature.as_ref() != Some(&signature);
			reader.signature = signature;
		} else {
			self.reader.lock().unwrap_or_else(|e| e.into_inner()).error = Some("E_NATIVE_PANIC");
		}
	}
	#[cfg(test)]
	pub(crate) fn test_reader_documents(&self) -> u64 {
		self.reader.lock().unwrap_or_else(|e| e.into_inner()).documents
	}

	#[cfg(feature = "test-panic")]
	pub(crate) fn fail_next_observation(&self) {
		self.fail_observation.store(true, Relaxed);
	}
	pub(crate) fn observe_bytes(&self, searcher: &Searcher) {
		if !self.snapshot_bytes.load(Relaxed) {
			return;
		}
		let signature = {
			let reader = self.reader.lock().unwrap_or_else(|e| e.into_inner());
			if reader.bytes_signature.as_ref() == Some(&reader.signature) {
				return;
			}
			reader.signature.clone()
		};
		let started = Instant::now();
		let result = catch_unwind(AssertUnwindSafe(|| {
			#[cfg(feature = "test-panic")]
			if self.fail_observation.swap(false, Relaxed) {
				return Err(std::io::Error::other("injected observation failure"));
			}
			searcher.space_usage().map(|usage| usage.total().get_bytes())
		}));
		let mut reader = self.reader.lock().unwrap_or_else(|e| e.into_inner());
		if reader.signature != signature {
			return;
		}
		reader.bytes_pending = false;
		reader.bytes_duration = nanos(started.elapsed());
		match result {
			Ok(Ok(bytes)) => {
				reader.bytes = Some(bytes);
				reader.bytes_observed = Some(Instant::now());
				reader.bytes_signature = Some(signature);
				reader.bytes_error = None;
			}
			Ok(Err(_)) => reader.bytes_error = Some("E_STORAGE"),
			Err(_) => reader.bytes_error = Some("E_NATIVE_PANIC"),
		}
	}
	pub(crate) fn configure(&self, histograms: bool, bytes: bool) {
		self.histograms.store(histograms, Relaxed);
		self.snapshot_bytes.store(bytes, Relaxed);
		let mut reader = self.reader.lock().unwrap_or_else(|e| e.into_inner());
		reader.bytes_pending = bytes && reader.bytes_signature.as_ref() != Some(&reader.signature);
	}
	fn phase_json(&self, phase: usize) -> String {
		let mut json = self.phases[phase].json();
		json.pop();
		let _ = write!(
			json,
			",\"succeeded\":\"{}\",\"failed\":\"{}\"}}",
			load(&self.phase_succeeded[phase]),
			load(&self.phase_failed[phase])
		);
		json
	}
	#[cfg(test)]
	pub(crate) fn test_actor_counts(&self, operation: usize, worker: Option<usize>) -> [u64; 3] {
		let actor = worker.map_or(&self.operations[operation].actor.counters, |worker| {
			&self.search_workers[worker][operation - 5]
		});
		[load(&actor.dispatched), load(&actor.succeeded), load(&actor.failed)]
	}
	#[cfg(test)]
	pub(crate) fn test_counts(&self, operation: usize) -> [u64; 7] {
		let o = &self.operations[operation];
		let actor = self.actor_values(operation);
		let succeeded = actor.succeeded;
		let failed = actor.failed;
		[
			load(&o.admission.attempted),
			load(&o.admission.accepted),
			load(&o.admission.rejected),
			actor.dispatched,
			succeeded.wrapping_add(failed),
			succeeded,
			failed,
		]
	}
	pub(crate) fn json(&self, queues: &str) -> String {
		let mut output = String::from("{\"apiVersion\":1,");
		let state = match self.state.load(Relaxed) {
			0 => "open",
			1 => "closing",
			2 => "closed",
			_ => "poisoned",
		};
		let _ = write!(
			output,
			"\"histogramsEnabled\":{},\"snapshotBytesEnabled\":{},\"state\":\"{state}\",\"operations\":{{",
			self.histograms.load(Relaxed),
			self.snapshot_bytes.load(Relaxed)
		);
		for (i, op) in self.operations.iter().enumerate() {
			if i > 0 {
				output.push(',');
			}
			let a = &op.admission;
			let b = self.actor_values(i);
			let succeeded = b.succeeded;
			let failed = b.failed;
			let completed = succeeded.wrapping_add(failed);
			let _=write!(output,"\"{}\":{{\"attempted\":\"{}\",\"accepted\":\"{}\",\"rejected\":\"{}\",\"dispatched\":\"{}\",\"completed\":\"{}\",\"succeeded\":\"{}\",\"failed\":\"{}\",\"requestBytes\":\"{}\",\"mutations\":\"{}\",\"queue\":{},\"execution\":{},\"errors\":{{",OP_NAMES[i],load(&a.attempted),load(&a.accepted),load(&a.rejected),b.dispatched,completed,succeeded,failed,load(&a.request_bytes),load(&op.actor.mutations),b.queue.json(),b.execution.json());
			for (j, error) in ERROR_CODES.iter().chain(std::iter::once(&"other")).enumerate() {
				if j > 0 {
					output.push(',');
				}
				let _ = write!(output, "\"{error}\":\"{}\"", load(&op.actor.errors[j]));
			}
			output.push_str("}}");
		}
		let _ = write!(
			output,
			"}},\"phases\":{{\"commit\":{},\"readerReload\":{},\"teardown\":{}}},\"queues\":{queues},\"reader\":",
			self.phase_json(0),
			self.phase_json(1),
			self.phase_json(2)
		);
		let r = self.reader.lock().unwrap_or_else(|e| e.into_inner());
		let cheap_error = r.error.map(|v| format!("\"{v}\"")).unwrap_or_else(|| "null".to_owned());
		let age = r
			.observed
			.map(|v| format!("\"{}\"", nanos(v.elapsed())))
			.unwrap_or_else(|| "null".to_owned());
		let bytes_age = r
			.bytes_observed
			.map(|v| format!("\"{}\"", nanos(v.elapsed())))
			.unwrap_or_else(|| "null".to_owned());
		let bytes = r.bytes.map(|v| format!("\"{v}\"")).unwrap_or_else(|| "null".to_owned());
		let error = r
			.bytes_error
			.map(|v| format!("\"{v}\""))
			.unwrap_or_else(|| "null".to_owned());
		let _=write!(output,"{{\"error\":{cheap_error},\"available\":{},\"documents\":\"{}\",\"deletedDocuments\":\"{}\",\"segments\":\"{}\",\"generationId\":\"{}\",\"ageNanoseconds\":{age},\"durationNanoseconds\":\"{}\",\"snapshotBytes\":{bytes},\"snapshotBytesAgeNanoseconds\":{bytes_age},\"snapshotBytesPending\":{},\"snapshotBytesError\":{error},\"snapshotBytesDurationNanoseconds\":\"{}\"}},\"unsupported\":{{\"directoryTotalBytes\":\"native snapshot bytes exclude directory metadata and uncommitted files\",\"automaticMergeStats\":\"Tantivy public API has no automatic merge counters\",\"perIndexRss\":\"RSS is process-wide and cannot be attributed to one index\",\"nativeCancellation\":\"cooperative deadlines do not provide active cancellation\"}}}}",r.available,r.documents,r.deleted,r.segments,r.generation,r.duration,r.bytes_pending,r.bytes_duration);
		output
	}
}

impl Drop for Metrics {
	fn drop(&mut self) {
		#[cfg(feature = "test-panic")]
		LIVE_METRICS.fetch_sub(1, Relaxed);
	}
}

pub(crate) struct Ticket {
	metrics: Arc<Metrics>,
	operation: usize,
	worker: Option<usize>,
	bytes: u64,
	accepted: bool,
	dispatched: bool,
	settled: bool,
	enqueued: Option<Instant>,
	started: Option<Instant>,
	execution: Option<Duration>,
}
impl Ticket {
	pub(crate) fn bind_worker(&mut self, worker: usize) {
		if self.worker.is_none()
			&& !self.dispatched
			&& !self.settled
			&& (5..8).contains(&self.operation)
			&& worker < self.metrics.search_workers.len()
		{
			self.worker = Some(worker);
		}
	}
	fn actor(&self) -> &ActorCounters {
		self.worker
			.map_or(&self.metrics.operations[self.operation].actor.counters, |worker| {
				&self.metrics.search_workers[worker][self.operation - 5]
			})
	}
	pub(crate) fn accepted(&self) -> bool {
		self.accepted
	}
	pub(crate) fn arm(&mut self) {
		self.arm_at(Instant::now());
	}
	pub(crate) fn arm_at(&mut self, enqueued: Instant) {
		if !self.accepted {
			self.accepted = true;
			self.enqueued = Some(enqueued);
			add(&self.metrics.operations[self.operation].admission.accepted, 1);
			add(
				&self.metrics.operations[self.operation].admission.request_bytes,
				self.bytes,
			);
		}
	}
	pub(crate) fn dispatch(&mut self) {
		if self.accepted && !self.dispatched {
			self.dispatched = true;
			add(&self.actor().dispatched, 1);
		}
	}
	pub(crate) fn start(&mut self) {
		self.start_at(Instant::now());
	}
	pub(crate) fn start_at(&mut self, started: Instant) {
		if !self.accepted || self.started.is_some() || self.settled {
			return;
		}
		self.dispatch();
		if let Some(enqueued) = self.enqueued {
			self.actor().queue.record(
				started.saturating_duration_since(enqueued),
				self.metrics.histograms.load(Relaxed),
			);
		}
		self.started = Some(started);
	}
	pub(crate) fn finish_at(&mut self, finished: Instant) {
		if !self.settled && self.execution.is_none() {
			self.execution = self.started.map(|started| finished.saturating_duration_since(started));
		}
	}

	pub(crate) fn mutations(&self, count: u64) {
		add(&self.metrics.operations[self.operation].actor.mutations, count);
	}
	pub(crate) fn settle(&mut self, error: Option<&str>) {
		if self.settled {
			return;
		}
		self.settled = true;
		let op = &self.metrics.operations[self.operation];
		let actor = self.actor();
		if !self.accepted {
			return;
		} else {
			if self.dispatched {
				actor.dispatched.fetch_sub(1, Relaxed);
			}
			if let Some(execution) = self.execution.or_else(|| self.started.map(|started| started.elapsed())) {
				actor.execution.record(execution, self.metrics.histograms.load(Relaxed));
			} else if let Some(enqueued) = self.enqueued {
				actor
					.queue
					.record(enqueued.elapsed(), self.metrics.histograms.load(Relaxed));
			}
			add(
				if error.is_none() {
					&actor.succeeded
				} else {
					&actor.failed
				},
				1,
			);
		}
		if let Some(error) = error {
			let index = ERROR_CODES
				.iter()
				.position(|v| *v == error)
				.unwrap_or(ERROR_CODES.len());
			add(&op.actor.errors[index], 1);
		}
	}
}
impl Drop for Ticket {
	fn drop(&mut self) {
		self.settle(Some("E_CLOSED"));
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	#[test]
	fn ticket_reuses_actor_clocks_for_exact_queue_and_execution_spans() {
		let metrics = Metrics::new();
		metrics.attempt(5);
		let origin = Instant::now();
		let mut ticket = metrics.ticket(5, 0);
		ticket.arm_at(origin);
		ticket.start_at(origin + Duration::from_millis(2));
		ticket.finish_at(origin + Duration::from_millis(5));
		ticket.settle(None);
		assert_eq!(load(&metrics.operations[5].actor.counters.queue.sum), 2_000_000);
		assert_eq!(load(&metrics.operations[5].actor.counters.execution.sum), 3_000_000);
		assert_eq!(metrics.test_counts(5), [1, 1, 0, 0, 1, 1, 0]);
	}

	#[test]
	fn ticket_uses_admission_timestamp_without_constructor_clock() {
		let metrics = Metrics::new();
		metrics.attempt(5);
		let mut ticket = metrics.ticket(5, 0);
		assert!(ticket.enqueued.is_none());
		ticket.start();
		assert!(ticket.started.is_none());
		let enqueued = Instant::now();
		ticket.arm_at(enqueued);
		assert_eq!(ticket.enqueued, Some(enqueued));
		ticket.start();
		ticket.start();
		ticket.settle(None);
		assert_eq!(metrics.test_counts(5), [1, 1, 0, 0, 1, 1, 0]);
		assert_eq!(load(&metrics.operations[5].actor.counters.queue.count), 1);
		assert_eq!(load(&metrics.operations[5].actor.counters.execution.count), 1);
	}

	#[test]
	fn completed_is_derived_from_explicit_outcomes_with_wrapping() {
		let metrics = Metrics::new();
		let operation = &metrics.operations[0];
		operation.admission.attempted.store(9, Relaxed);
		operation.actor.counters.succeeded.store(u64::MAX, Relaxed);
		operation.actor.counters.failed.store(2, Relaxed);
		assert_eq!(metrics.test_counts(0), [9, 0, 0, 0, 1, u64::MAX, 2]);
	}

	#[test]
	fn teardown_phase_covers_writer_close_through_cleanup_once() {
		let metrics = Metrics::new();
		let origin = Instant::now();
		metrics.begin_teardown_at(origin);
		metrics.begin_teardown_at(origin + Duration::from_secs(2));
		metrics.finish_teardown_at(origin + Duration::from_secs(3), false);
		metrics.finish_teardown_at(origin + Duration::from_secs(4), true);
		assert_eq!(load(&metrics.phases[2].count), 1);
		assert_eq!(load(&metrics.phases[2].sum), 3_000_000_000);
		assert_eq!(load(&metrics.phase_failed[2]), 1);
		assert_eq!(load(&metrics.phase_succeeded[2]), 0);
	}

	#[test]
	fn boundaries_and_disabled_histograms() {
		let l = Latency::default();
		l.record(Duration::from_nanos(1_000), true);
		l.record(Duration::from_nanos(1_001), true);
		l.record(Duration::MAX, true);
		l.record(Duration::ZERO, false);
		assert_eq!(load(&l.count), 4);
		assert_eq!(load(&l.histogram_count), 3);
		assert_eq!(load(&l.buckets[0]), 1);
		assert_eq!(load(&l.buckets[1]), 1);
		assert_eq!(load(&l.buckets[11]), 1);
	}
	#[test]
	fn inactive_reject_and_accepted_drop_conserve() {
		let m = Metrics::new();
		m.attempt(0);
		m.reject(0, "E_QUEUE_FULL");
		m.attempt(0);
		{
			let mut t = m.ticket(0, 8);
			t.settle(Some("E_QUEUE_FULL"));
		}
		{
			let mut t = m.ticket(0, 8);
			t.arm();
			t.dispatch();
		}
		let op = &m.operations[0];
		assert_eq!(load(&op.admission.attempted), 2);
		assert_eq!(load(&op.admission.accepted), 1);
		assert_eq!(load(&op.admission.rejected), 1);
		assert_eq!(
			load(&op.actor.counters.succeeded).wrapping_add(load(&op.actor.counters.failed)),
			1
		);
		assert_eq!(load(&op.actor.counters.failed), 1);
		assert_eq!(load(&op.actor.counters.dispatched), 0);
	}
	#[test]
	fn repeat_arm_settle_and_error_slots() {
		let m = Metrics::new();
		for code in ERROR_CODES {
			let mut t = m.ticket(1, 0);
			t.arm();
			t.arm();
			t.start();
			t.settle(Some(code));
			t.settle(None);
		}
		assert_eq!(load(&m.operations[1].admission.accepted), ERROR_CODES.len() as u64);
		for error in &m.operations[1].actor.errors[..ERROR_CODES.len()] {
			assert_eq!(load(error), 1);
		}
	}
	#[test]
	fn worker_banks_aggregate_all_search_classes_and_shared_drain() {
		let metrics = Metrics::with_search_workers(4);
		std::thread::scope(|scope| {
			for worker in 0..4 {
				let metrics = &metrics;
				scope.spawn(move || {
					for operation in 5..8 {
						metrics.attempt(operation);
						let mut ticket = metrics.ticket(operation, 1);
						ticket.arm();
						ticket.bind_worker(worker);
						ticket.start();
						ticket.settle(None);
					}
				});
			}
		});
		for operation in 5..8 {
			metrics.attempt(operation);
			metrics.reject(operation, "E_QUEUE_FULL");
			metrics.attempt(operation);
			let mut queued = metrics.ticket(operation, 1);
			queued.arm();
			drop(queued);
			assert_eq!(metrics.test_counts(operation), [6, 5, 1, 0, 5, 4, 1]);
			let values = metrics.actor_values(operation);
			assert_eq!(values.queue.count, 5);
			assert_eq!(values.execution.count, 4);
			assert_eq!(load(&metrics.operations[operation].actor.errors[0]), 1);
		}
		let json = metrics.json("{}");
		assert!(json.contains("\"completed\":\"5\""));
	}
	#[test]
	fn worker_binding_is_stable_across_invalid_rebinding_and_thread_migration() {
		let metrics = Metrics::with_search_workers(2);
		let mut ticket = metrics.ticket(5, 0);
		ticket.arm();
		ticket.bind_worker(usize::MAX);
		assert_eq!(ticket.worker, None);
		ticket.bind_worker(0);
		ticket.bind_worker(1);
		assert_eq!(ticket.worker, Some(0));
		ticket.start();
		std::thread::spawn(move || {
			ticket.bind_worker(1);
			ticket.settle(None);
		})
		.join()
		.unwrap();
		assert_eq!(load(&metrics.search_workers[0][0].succeeded), 1);
		assert_eq!(load(&metrics.search_workers[1][0].succeeded), 0);
		let mut fallback = metrics.ticket(6, 0);
		fallback.arm();
		fallback.dispatch();
		fallback.bind_worker(1);
		assert_eq!(fallback.worker, None);
		fallback.settle(None);
		assert_eq!(load(&metrics.operations[6].actor.counters.succeeded), 1);
		let mut writer = metrics.ticket(0, 0);
		writer.bind_worker(0);
		assert_eq!(writer.worker, None);
	}
	#[test]
	fn duplicate_worker_assignment_keeps_rmw_updates_lossless() {
		let metrics = Metrics::with_search_workers(1);
		std::thread::scope(|scope| {
			for _ in 0..4 {
				let metrics = &metrics;
				scope.spawn(move || {
					for _ in 0..1000 {
						let mut ticket = metrics.ticket(5, 0);
						ticket.arm();
						ticket.bind_worker(0);
						ticket.start();
						ticket.settle(None);
					}
				});
			}
		});
		assert_eq!(metrics.test_counts(5), [0, 4000, 0, 0, 4000, 4000, 0]);
	}
	#[test]
	fn bank_aggregation_wraps_all_scalar_and_histogram_fields() {
		let metrics = Metrics::with_search_workers(2);
		for counters in [&metrics.operations[5].actor.counters, &metrics.search_workers[0][0]] {
			counters.dispatched.store(u64::MAX, Relaxed);
			counters.succeeded.store(u64::MAX, Relaxed);
			counters.failed.store(u64::MAX, Relaxed);
			for latency in [&counters.queue, &counters.execution] {
				latency.count.store(u64::MAX, Relaxed);
				latency.sum.store(u64::MAX, Relaxed);
				latency.histogram_count.store(u64::MAX, Relaxed);
				latency.histogram_sum.store(u64::MAX, Relaxed);
				for bucket in &latency.buckets {
					bucket.store(u64::MAX, Relaxed);
				}
			}
		}
		let values = metrics.actor_values(5);
		assert_eq!(values.dispatched, u64::MAX - 1);
		assert_eq!(values.succeeded, u64::MAX - 1);
		assert_eq!(values.failed, u64::MAX - 1);
		for latency in [values.queue, values.execution] {
			assert_eq!(latency.count, u64::MAX - 1);
			assert_eq!(latency.sum, u64::MAX - 1);
			assert_eq!(latency.histogram_count, u64::MAX - 1);
			assert_eq!(latency.histogram_sum, u64::MAX - 1);
			assert!(latency.buckets.iter().all(|value| *value == u64::MAX - 1));
		}
		assert!(metrics.json("{}").contains("\"completed\":\"18446744073709551612\""));
	}
	#[test]
	fn bank_histograms_follow_configuration_without_resetting_totals() {
		let metrics = Metrics::with_search_workers(1);
		for enabled in [false, true, false] {
			metrics.histograms.store(enabled, Relaxed);
			let mut ticket = metrics.ticket(7, 0);
			ticket.arm();
			ticket.bind_worker(0);
			ticket.start();
			ticket.settle(None);
		}
		let values = metrics.actor_values(7);
		assert_eq!(values.queue.count, 3);
		assert_eq!(values.execution.count, 3);
		assert_eq!(values.queue.histogram_count, 1);
		assert_eq!(values.execution.histogram_count, 1);
	}
	#[test]
	fn compact_bank_storage_is_bounded_by_configured_workers() {
		assert_eq!(std::mem::size_of::<ActorCounters>(), 384);
		for (workers, bytes) in [(1, 1152), (4, 4608), (64, 73728)] {
			let metrics = Metrics::with_search_workers(workers);
			assert_eq!(std::mem::size_of_val(&*metrics.search_workers), bytes);
		}
		assert!(Metrics::new().search_workers.is_empty());
	}
	#[test]
	#[ignore]
	fn metrics_recording_microbenchmark() {
		for threads in [1, 4] {
			for banked in [false, true] {
				for enabled in [false, true] {
					let m = Metrics::with_search_workers(if banked { threads } else { 0 });
					m.histograms.store(enabled, Relaxed);
					let started = Instant::now();
					std::thread::scope(|scope| {
						for worker in 0..threads {
							let m = &m;
							scope.spawn(move || {
								for _ in 0..100_000 {
									m.attempt(5);
									let mut t = m.ticket(5, 64);
									t.bind_worker(worker);
									t.arm();
									t.start();
									t.settle(None);
								}
							});
						}
					});
					eprintln!(
						"threads={threads} banked={banked} histograms={enabled} operations={} elapsed_ns={}",
						threads * 100_000,
						started.elapsed().as_nanos()
					);
				}
			}
		}
	}
	#[test]
	#[ignore]
	fn metrics_producer_worker_microbenchmark() {
		for workers in [1, 4] {
			for banked in [false, true] {
				for enabled in [false, true] {
					let metrics = Metrics::with_search_workers(if banked { workers } else { 0 });
					metrics.histograms.store(enabled, Relaxed);
					let started = Instant::now();
					std::thread::scope(|scope| {
						let mut senders = Vec::new();
						for worker in 0..workers {
							let (sender, receiver) = std::sync::mpsc::sync_channel::<Ticket>(256);
							senders.push(sender);
							scope.spawn(move || {
								for mut ticket in receiver {
									ticket.bind_worker(worker);
									let started = Instant::now();
									ticket.start_at(started);
									let mut value = 1u64;
									while started.elapsed() < Duration::from_micros(10) {
										value = std::hint::black_box(
											value.wrapping_mul(6364136223846793005).wrapping_add(1),
										);
									}
									ticket.finish_at(Instant::now());
									ticket.settle(None);
								}
							});
						}
						for operation in 0..10_000 * workers {
							metrics.attempt(5);
							let mut ticket = metrics.ticket(5, 64);
							ticket.arm();
							senders[operation % workers].send(ticket).unwrap();
						}
					});
					let elapsed = started.elapsed().as_nanos();
					assert_eq!(
						metrics.test_counts(5),
						[
							(10_000 * workers) as u64,
							(10_000 * workers) as u64,
							0,
							0,
							(10_000 * workers) as u64,
							(10_000 * workers) as u64,
							0
						]
					);
					eprintln!("single_producer_workers={workers} banked={banked} histograms={enabled} busywork_target_us=10 operations={} elapsed_ns={elapsed}", workers * 10_000);
				}
			}
		}
	}
}
