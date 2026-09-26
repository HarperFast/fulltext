use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::mem;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use napi::bindgen_prelude::{Buffer, Function};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Env, Status};
use napi_derive::napi;
use tantivy::directory::{Directory, DirectoryLock, Lock, MmapDirectory, INDEX_WRITER_LOCK, META_LOCK};
use tantivy::IndexReader;

use crate::boundary;
use crate::engine::{
	persisted_index_id, Engine, InspectionResult, SearchResult, TotalRelation, TraceResult, Writer, IDENTITY_PATH,
};
use crate::error::{FulltextError, Result};
use crate::protocol::{
	decode_batch, decode_inspect, decode_open, decode_reset, decode_runtime_budget, decode_search, decode_trace,
	search_budget, search_mode, trace_budget, validate_batch_header, validate_search_header, validate_trace_header,
	EngineConfig, RuntimeBudgetLimits, PROTOCOL_VERSION,
};

const STATE_OPEN: u8 = 0;
const STATE_CLOSING: u8 = 1;
const STATE_CLOSED: u8 = 2;
const STATE_POISONED: u8 = 3;
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

static NEXT_HANDLE: AtomicU32 = AtomicU32::new(1);
static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
static RUNTIME_BUDGET: OnceLock<Mutex<RuntimeBudgetState>> = OnceLock::new();
#[cfg(feature = "test-panic")]
static FAIL_NEXT_OPEN_CLEANUP: AtomicBool = AtomicBool::new(false);
const LIFECYCLE_ROOT: &str = ".fulltext-locks";
const LEGACY_LIFECYCLE_LOCK: &str = ".harper-fulltext-lifecycle.lock";
const RETIRED_ROOT: &str = ".fulltext-retired";

type CompletionCallback<'scope> = Function<'scope, Buffer, ()>;
type CompletionThreadsafeFunction = ThreadsafeFunction<Buffer, (), Buffer, Status, false>;

#[derive(Default)]
struct Registry {
	handles: HashMap<u32, Arc<Runtime>>,
	paths: HashMap<PathIdentity, PathReservation>,
	unproven_paths: HashSet<PathBuf>,
	opening: HashSet<u32>,
	cancelled: HashSet<u32>,
	environments: HashMap<usize, Weak<EnvironmentState>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PathReservation {
	Open(u32),
	Reset(u32),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum PathIdentity {
	#[cfg(unix)]
	Unix(u64, u64),
	#[cfg(not(unix))]
	Path(PathBuf),
}

struct Runtime {
	handle: u32,
	path: PathBuf,
	path_identity: PathIdentity,
	config: EngineConfig,
	writer_queue: Arc<BoundedQueue<WriterCommand>>,
	ordinary_search_queue: Arc<BoundedQueue<SearchCommand>>,
	expensive_search_queue: Option<Arc<BoundedQueue<SearchCommand>>>,
	search_wake: Option<Arc<QueueWake>>,
	state: AtomicU8,
	environment: Arc<EnvironmentState>,
	uncommitted_mutations: AtomicU64,
	commit_opstamp: AtomicU64,
	writer_queue_nanoseconds: AtomicU64,
	writer_execution_nanoseconds: AtomicU64,
	search_queue_nanoseconds: AtomicU64,
	search_execution_nanoseconds: AtomicU64,
	search_threads: Mutex<Vec<thread::JoinHandle<()>>>,
	closed: Arc<CompletionSignal>,
	reservation: Mutex<Option<RuntimeAdmission>>,
	expensive_search_budget: Option<Arc<RuntimeBudget>>,
	#[cfg(feature = "test-panic")]
	publish_fault: AtomicU8,
	#[cfg(feature = "test-panic")]
	poison_before_admission: AtomicBool,
	#[cfg(feature = "test-panic")]
	close_fault: AtomicU8,
	#[cfg(feature = "test-panic")]
	expensive_search_delay_milliseconds: AtomicU64,
	#[cfg(feature = "test-panic")]
	ordinary_search_delay_milliseconds: AtomicU64,
}

struct RuntimeBudget {
	limits: RuntimeBudgetLimits,
	resident_indexes: AtomicUsize,
	indexing_threads: AtomicUsize,
	search_threads: AtomicUsize,
	writer_memory_bytes: AtomicUsize,
	queued_bytes: AtomicUsize,
	expensive_searches: Mutex<usize>,
	expensive_search_ready: Condvar,
	expensive_search_wakes: Mutex<Vec<Weak<QueueWake>>>,
	expensive_wake_demand: AtomicUsize,
	#[cfg(feature = "test-panic")]
	expensive_search_waiters: AtomicUsize,
}

enum RuntimeBudgetState {
	Unconfigured { pending_opens: usize },
	Configured(Arc<RuntimeBudget>),
	OpenedWithoutBudget,
}

struct RuntimeAdmission {
	kind: RuntimeAdmissionKind,
}

enum RuntimeAdmissionKind {
	Budgeted(BudgetReservation),
	Unbudgeted { pending: bool },
}

struct BudgetReservation {
	budget: Arc<RuntimeBudget>,
	resident: bool,
	indexing_threads: usize,
	search_threads: usize,
	writer_memory_bytes: usize,
	queued_bytes: usize,
}

struct ExpensiveSearchPermit {
	budget: Arc<RuntimeBudget>,
}

struct ExpensiveWakeDemand {
	budget: Arc<RuntimeBudget>,
}

enum ResetResult {
	Missing,
	Reset(String),
}

struct ResetReservation {
	operation: u32,
	path_identity: PathIdentity,
}

struct RuntimeParts {
	path: PathBuf,
	path_identity: PathIdentity,
	config: EngineConfig,
	engine: Engine,
	writer: Writer,
	reader: IndexReader,
	reservation: RuntimeAdmission,
}

struct CompletionSignal {
	done: Mutex<bool>,
	ready: Condvar,
}

struct EnvironmentState {
	callbacks: Arc<CallbackGate>,
	handles: Mutex<HashMap<u32, Arc<CompletionSignal>>>,
}

struct CallbackGate {
	alive: AtomicBool,
}

struct QueueState<T> {
	items: VecDeque<Queued<T>>,
	bytes: usize,
	closed: bool,
}

struct Queued<T> {
	value: T,
	bytes: usize,
	enqueued: Instant,
}

struct BoundedQueue<T> {
	state: Mutex<QueueState<T>>,
	ready: Condvar,
	max_commands: usize,
	max_bytes: usize,
	queued_commands: AtomicU64,
	queued_bytes: AtomicU64,
	shared_budget: Option<Arc<QueueBudget>>,
	shared_wake: Option<Arc<QueueWake>>,
}

struct QueueBudget {
	state: Mutex<(usize, usize)>,
	max_commands: usize,
	max_bytes: usize,
}

struct QueueWake {
	generation: Mutex<u64>,
	ready: Condvar,
}

struct Completion {
	callback: Option<CompletionThreadsafeFunction>,
	callbacks: Arc<CallbackGate>,
}

struct WriterCommand {
	operation: WriterOperation,
	completion: Completion,
}

enum WriterOperation {
	Apply(Vec<u8>),
	Commit,
	Publish(String),
	Reload,
	Close { rollback: bool },
}

enum WriterOutcome {
	Continue(Result<Vec<u8>>),
	Stop(WriterCloseOutcome),
	Poison(Result<Vec<u8>>, FulltextError),
}

struct WriterCloseOutcome {
	quiesced: bool,
	error: Option<FulltextError>,
}

struct SearchCommand {
	operation: SearchOperation,
	completion: Completion,
}

enum SearchOperation {
	Search(Vec<u8>),
	Trace(Vec<u8>),
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeConfigureRuntime")]
pub fn native_configure_runtime(packed_limits: Buffer) -> boundary::Result<Buffer> {
	boundary::run_stateless(|| {
		let response = match decode_runtime_budget(&packed_limits).and_then(configure_runtime_budget) {
			Ok(()) => success_envelope(Vec::new()),
			Err(error) => error_envelope(error),
		};
		Ok(Buffer::from(response))
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeOpen")]
pub fn native_open(env: Env, packed_config: Buffer, callback: CompletionCallback<'_>) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let environment = environment_state(&env)?;
		let opening_done = Arc::new(CompletionSignal::new());
		let completion = completion(callback, &environment)?;
		let handle = next_handle().map_err(fulltext_napi_error)?;
		registry().opening.insert(handle);
		environment.track(handle, opening_done.clone());
		let bytes = packed_config.to_vec();
		let thread_opening_done = opening_done.clone();
		let thread_environment = environment.clone();
		if let Err(error) = thread::Builder::new()
			.name(format!("fulltext-open-{handle}"))
			.spawn(move || open_on_thread(handle, bytes, completion, thread_opening_done, thread_environment))
		{
			registry().opening.remove(&handle);
			environment.release(handle);
			opening_done.signal();
			return Err(napi_error("E_NATIVE_FAILURE", error));
		}
		Ok(())
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeInspect")]
pub fn native_inspect(packed_config: Buffer) -> boundary::Result<Buffer> {
	boundary::run_stateless(|| {
		let response = match inspect_runtime(&packed_config) {
			Ok(result) => success_envelope(inspection_body(result)),
			Err(error) => error_envelope(error),
		};
		Buffer::from(response)
	})
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeValidateOpen")]
pub fn native_validate_open(packed_config: Buffer) -> boundary::Result<Buffer> {
	boundary::run_stateless(|| {
		let response = match decode_open(&packed_config).and_then(|config| Engine::validate(&config.engine)) {
			Ok(()) => success_envelope(Vec::new()),
			Err(error) => error_envelope(error),
		};
		Buffer::from(response)
	})
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeReset")]
pub fn native_reset(env: Env, packed_config: Buffer, callback: CompletionCallback<'_>) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let environment = environment_state(&env)?;
		let reset_done = Arc::new(CompletionSignal::new());
		let completion = completion(callback, &environment)?;
		let operation = next_handle().map_err(fulltext_napi_error)?;
		registry().opening.insert(operation);
		environment.track(operation, reset_done.clone());
		let bytes = packed_config.to_vec();
		let thread_reset_done = reset_done.clone();
		let thread_environment = environment.clone();
		if let Err(error) = thread::Builder::new()
			.name(format!("fulltext-reset-{operation}"))
			.spawn(move || reset_on_thread(operation, bytes, completion, thread_reset_done, thread_environment))
		{
			registry().opening.remove(&operation);
			environment.release(operation);
			reset_done.signal();
			return Err(napi_error("E_NATIVE_FAILURE", error));
		}
		Ok(())
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeApply")]
pub fn native_apply(
	env: Env,
	handle: u32,
	packed_batch: Buffer,
	callback: CompletionCallback<'_>,
) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let runtime = runtime(&env, handle)?;
		validate_batch_header(&packed_batch, runtime.config.limits.max_batch_bytes).map_err(fulltext_napi_error)?;
		runtime
			.writer_queue
			.check_capacity(packed_batch.len())
			.map_err(fulltext_napi_error)?;
		let completion = completion(callback, &runtime.environment)?;
		let bytes = packed_batch.to_vec();
		runtime.enqueue_writer(
			WriterCommand {
				operation: WriterOperation::Apply(bytes),
				completion,
			},
			packed_batch.len(),
		)
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeCommit")]
pub fn native_commit(env: Env, handle: u32, callback: CompletionCallback<'_>) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let runtime = runtime(&env, handle)?;
		let completion = completion(callback, &runtime.environment)?;
		runtime.enqueue_writer(
			WriterCommand {
				operation: WriterOperation::Commit,
				completion,
			},
			0,
		)
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativePublish")]
pub fn native_publish(
	env: Env,
	handle: u32,
	payload: String,
	callback: CompletionCallback<'_>,
) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		if payload.len() > crate::engine::MAX_COMMIT_PAYLOAD_BYTES {
			return Err(fulltext_napi_error(FulltextError::invalid(format!(
				"commit payload exceeds {} UTF-8 bytes",
				crate::engine::MAX_COMMIT_PAYLOAD_BYTES
			))));
		}
		let runtime = runtime(&env, handle)?;
		let completion = completion(callback, &runtime.environment)?;
		let bytes = payload.len();
		runtime.enqueue_writer(
			WriterCommand {
				operation: WriterOperation::Publish(payload),
				completion,
			},
			bytes,
		)
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeReload")]
pub fn native_reload(env: Env, handle: u32, callback: CompletionCallback<'_>) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let runtime = runtime(&env, handle)?;
		let completion = completion(callback, &runtime.environment)?;
		runtime.enqueue_writer(
			WriterCommand {
				operation: WriterOperation::Reload,
				completion,
			},
			0,
		)
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeSearch")]
pub fn native_search(
	env: Env,
	handle: u32,
	packed_request: Buffer,
	callback: CompletionCallback<'_>,
) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let runtime = runtime(&env, handle)?;
		runtime.require_open().map_err(fulltext_napi_error)?;
		validate_search_header(&packed_request).map_err(fulltext_napi_error)?;
		let mode = search_mode(&packed_request).map_err(fulltext_napi_error)?;
		let queue = runtime.search_queue(mode.is_expensive());
		queue
			.check_capacity(packed_request.len())
			.map_err(fulltext_napi_error)?;
		let completion = completion(callback, &runtime.environment)?;
		let request = packed_request.to_vec();
		queue
			.try_push(
				SearchCommand {
					operation: SearchOperation::Search(request),
					completion,
				},
				packed_request.len(),
			)
			.map_err(fulltext_napi_error)
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeTraceMatches")]
pub fn native_trace_matches(
	env: Env,
	handle: u32,
	packed_request: Buffer,
	callback: CompletionCallback<'_>,
) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let runtime = runtime(&env, handle)?;
		runtime.require_open().map_err(fulltext_napi_error)?;
		validate_trace_header(&packed_request).map_err(fulltext_napi_error)?;
		let queue = runtime.search_queue(true);
		queue
			.check_capacity(packed_request.len())
			.map_err(fulltext_napi_error)?;
		let completion = completion(callback, &runtime.environment)?;
		let request = packed_request.to_vec();
		queue
			.try_push(
				SearchCommand {
					operation: SearchOperation::Trace(request),
					completion,
				},
				packed_request.len(),
			)
			.map_err(fulltext_napi_error)
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeClose")]
pub fn native_close(env: Env, handle: u32, rollback: bool, callback: CompletionCallback<'_>) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let runtime = runtime(&env, handle)?;
		let completion = completion(callback, &runtime.environment)?;
		match runtime
			.state
			.compare_exchange(STATE_OPEN, STATE_CLOSING, Ordering::AcqRel, Ordering::Acquire)
		{
			Ok(_) => {
				runtime.notify_expensive_waiters();
				#[cfg(feature = "test-panic")]
				runtime.poison_before_admission();
				runtime
					.writer_queue
					.push_force(
						WriterCommand {
							operation: WriterOperation::Close { rollback },
							completion,
						},
						0,
					)
					.map_err(fulltext_napi_error)
			}
			Err(STATE_CLOSED) => {
				completion.success(Vec::new());
				Ok(())
			}
			Err(STATE_POISONED) => runtime
				.writer_queue
				.push_force(
					WriterCommand {
						operation: WriterOperation::Close { rollback: true },
						completion,
					},
					0,
				)
				.map_err(fulltext_napi_error),
			Err(_) => Err(fulltext_napi_error(FulltextError::new(
				"E_CLOSED",
				"index is closing or poisoned",
			))),
		}
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testPoisonNativeHandle")]
pub fn test_poison_native_handle(env: Env, handle: u32) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let runtime = runtime(&env, handle)?;
		runtime.poison(FulltextError::new("E_POISONED", "test poison"));
		Ok(())
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testPoisonBeforeNextAdmission")]
pub fn test_poison_before_next_admission(env: Env, handle: u32) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		runtime(&env, handle)?
			.poison_before_admission
			.store(true, Ordering::Release);
		Ok(())
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testFailNextPublish")]
pub fn test_fail_next_publish(env: Env, handle: u32, after_commit: bool) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		runtime(&env, handle)?
			.publish_fault
			.store(if after_commit { 2 } else { 1 }, Ordering::Release);
		Ok(())
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testFailNextClose")]
pub fn test_fail_next_close(env: Env, handle: u32, quiesced: bool) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		runtime(&env, handle)?
			.close_fault
			.store(if quiesced { 1 } else { 2 }, Ordering::Release);
		Ok(())
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testFailNextOpenCleanup")]
pub fn test_fail_next_open_cleanup() {
	FAIL_NEXT_OPEN_CLEANUP.store(true, Ordering::Release);
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testDelayNextExpensiveSearch")]
pub fn test_delay_next_expensive_search(env: Env, handle: u32, milliseconds: u32) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		if milliseconds == 0 || milliseconds > 5_000 {
			return Err(fulltext_napi_error(FulltextError::invalid(
				"test search delay must be between 1 and 5000 milliseconds",
			)));
		}
		runtime(&env, handle)?
			.expensive_search_delay_milliseconds
			.store(u64::from(milliseconds), Ordering::Release);
		Ok(())
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testDelayNextOrdinarySearch")]
pub fn test_delay_next_ordinary_search(env: Env, handle: u32, milliseconds: u32) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		if milliseconds == 0 || milliseconds > 5_000 {
			return Err(fulltext_napi_error(FulltextError::invalid(
				"test search delay must be between 1 and 5000 milliseconds",
			)));
		}
		runtime(&env, handle)?
			.ordinary_search_delay_milliseconds
			.store(u64::from(milliseconds), Ordering::Release);
		Ok(())
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testExpensiveSearchState")]
pub fn test_expensive_search_state(env: Env, handle: u32) -> boundary::Result<Vec<u32>> {
	boundary::run_stateless(|| {
		let runtime = runtime(&env, handle)?;
		Ok(vec![
			u32::from(runtime.expensive_search_delay_milliseconds.load(Ordering::Acquire) != 0),
			runtime.expensive_search_budget.as_ref().map_or(0, |budget| {
				budget.expensive_search_waiters.load(Ordering::Acquire) as u32
			}),
			u32::from(runtime.ordinary_search_delay_milliseconds.load(Ordering::Acquire) != 0),
		])
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeStatus")]
pub fn native_status(env: Env, handle: u32) -> boundary::Result<Buffer> {
	boundary::run_stateless(|| {
		let runtime = runtime(&env, handle)?;
		Ok(Buffer::from(success_envelope(runtime.status_bytes())))
	})?
}

fn configure_runtime_budget(limits: RuntimeBudgetLimits) -> Result<()> {
	configure_runtime_budget_in(runtime_budget_state(), limits)
}

fn configure_runtime_budget_in(state: &Mutex<RuntimeBudgetState>, limits: RuntimeBudgetLimits) -> Result<()> {
	let mut state = lock(state);
	match &*state {
		RuntimeBudgetState::Unconfigured { pending_opens: 0 } => {
			*state = RuntimeBudgetState::Configured(Arc::new(RuntimeBudget::new(limits)));
			Ok(())
		}
		RuntimeBudgetState::Unconfigured { .. } => Err(FulltextError::new(
			"E_LOCK_BUSY",
			"the process-wide fulltext runtime budget cannot be configured while an index open is pending",
		)),
		RuntimeBudgetState::Configured(configured) if configured.limits == limits => Ok(()),
		RuntimeBudgetState::Configured(_) => Err(FulltextError::new(
			"E_RESOURCE_LIMIT",
			"the process-wide fulltext runtime budget is already configured differently",
		)),
		RuntimeBudgetState::OpenedWithoutBudget => Err(FulltextError::new(
			"E_RESOURCE_LIMIT",
			"the process-wide fulltext runtime budget cannot be configured after an unbudgeted open or unproven teardown; restart the process",
		)),
	}
}

fn runtime_budget_state() -> &'static Mutex<RuntimeBudgetState> {
	RUNTIME_BUDGET.get_or_init(|| Mutex::new(RuntimeBudgetState::Unconfigured { pending_opens: 0 }))
}

fn admit_runtime(limits: &crate::protocol::Limits) -> Result<RuntimeAdmission> {
	let mut state = lock(runtime_budget_state());
	match &mut *state {
		RuntimeBudgetState::Unconfigured { pending_opens } => {
			*pending_opens = pending_opens
				.checked_add(1)
				.ok_or_else(|| FulltextError::new("E_RESOURCE_LIMIT", "too many pending index opens"))?;
			Ok(RuntimeAdmission {
				kind: RuntimeAdmissionKind::Unbudgeted { pending: true },
			})
		}
		RuntimeBudgetState::Configured(budget) => budget.admit(limits).map(|reservation| RuntimeAdmission {
			kind: RuntimeAdmissionKind::Budgeted(reservation),
		}),
		RuntimeBudgetState::OpenedWithoutBudget => Ok(RuntimeAdmission {
			kind: RuntimeAdmissionKind::Unbudgeted { pending: false },
		}),
	}
}

impl RuntimeBudget {
	fn new(limits: RuntimeBudgetLimits) -> Self {
		Self {
			limits,
			resident_indexes: AtomicUsize::new(0),
			indexing_threads: AtomicUsize::new(0),
			search_threads: AtomicUsize::new(0),
			writer_memory_bytes: AtomicUsize::new(0),
			queued_bytes: AtomicUsize::new(0),
			expensive_searches: Mutex::new(0),
			expensive_search_ready: Condvar::new(),
			expensive_search_wakes: Mutex::new(Vec::new()),
			expensive_wake_demand: AtomicUsize::new(0),
			#[cfg(feature = "test-panic")]
			expensive_search_waiters: AtomicUsize::new(0),
		}
	}

	fn admit(self: &Arc<Self>, limits: &crate::protocol::Limits) -> Result<BudgetReservation> {
		let mut admission = BudgetReservation {
			budget: self.clone(),
			resident: false,
			indexing_threads: 0,
			search_threads: 0,
			writer_memory_bytes: 0,
			queued_bytes: 0,
		};
		reserve(
			&self.resident_indexes,
			1,
			self.limits.max_resident_indexes,
			"resident indexes",
		)?;
		admission.resident = true;
		reserve(
			&self.indexing_threads,
			limits.indexing_threads,
			self.limits.max_indexing_threads,
			"indexing threads",
		)?;
		admission.indexing_threads = limits.indexing_threads;
		reserve(
			&self.search_threads,
			limits.search_threads,
			self.limits.max_search_threads,
			"search threads",
		)?;
		admission.search_threads = limits.search_threads;
		reserve(
			&self.writer_memory_bytes,
			limits.writer_memory_bytes,
			self.limits.max_writer_memory_bytes,
			"writer memory",
		)?;
		admission.writer_memory_bytes = limits.writer_memory_bytes;
		let queued_bytes = limits
			.max_queued_bytes
			.checked_mul(2)
			.ok_or_else(|| FulltextError::new("E_RESOURCE_LIMIT", "configured queue capacity overflows"))?;
		reserve(
			&self.queued_bytes,
			queued_bytes,
			self.limits.max_queued_bytes,
			"queued bytes",
		)?;
		admission.queued_bytes = queued_bytes;
		Ok(admission)
	}

	fn acquire_expensive(
		self: &Arc<Self>,
		deadline: Instant,
		runtime_state: &AtomicU8,
	) -> Result<ExpensiveSearchPermit> {
		let mut active = lock(&self.expensive_searches);
		while *active >= self.limits.max_expensive_searches {
			if runtime_state.load(Ordering::Acquire) != STATE_OPEN {
				return Err(FulltextError::new("E_CLOSED", "index is closing or closed"));
			}
			let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
				return Err(search_timeout());
			};
			#[cfg(feature = "test-panic")]
			self.expensive_search_waiters.fetch_add(1, Ordering::AcqRel);
			let (next, timeout) = self
				.expensive_search_ready
				.wait_timeout(active, remaining)
				.unwrap_or_else(|error| error.into_inner());
			#[cfg(feature = "test-panic")]
			self.expensive_search_waiters.fetch_sub(1, Ordering::AcqRel);
			active = next;
			if timeout.timed_out() && *active >= self.limits.max_expensive_searches {
				return Err(search_timeout());
			}
		}
		if runtime_state.load(Ordering::Acquire) != STATE_OPEN {
			return Err(FulltextError::new("E_CLOSED", "index is closing or closed"));
		}
		*active += 1;
		Ok(ExpensiveSearchPermit { budget: self.clone() })
	}

	fn try_acquire_expensive(self: &Arc<Self>, runtime_state: &AtomicU8) -> Option<ExpensiveSearchPermit> {
		if runtime_state.load(Ordering::Acquire) != STATE_OPEN {
			return None;
		}
		let mut active = lock(&self.expensive_searches);
		if *active >= self.limits.max_expensive_searches || runtime_state.load(Ordering::Acquire) != STATE_OPEN {
			return None;
		}
		*active += 1;
		Some(ExpensiveSearchPermit { budget: self.clone() })
	}

	fn register_expensive_wake(&self, wake: &Arc<QueueWake>) {
		let mut wakes = lock(&self.expensive_search_wakes);
		wakes.retain(|registered| registered.strong_count() > 0);
		let weak = Arc::downgrade(wake);
		if !wakes.iter().any(|registered| registered.ptr_eq(&weak)) {
			wakes.push(weak);
		}
	}

	fn notify_expensive_wakes(&self) {
		if self.expensive_wake_demand.load(Ordering::Acquire) == 0 {
			return;
		}
		let mut wakes = lock(&self.expensive_search_wakes);
		wakes.retain(|registered| {
			if let Some(wake) = registered.upgrade() {
				wake.notify(true);
				true
			} else {
				false
			}
		});
	}
}

impl RuntimeAdmission {
	fn budget(&self) -> Option<Arc<RuntimeBudget>> {
		match &self.kind {
			RuntimeAdmissionKind::Budgeted(reservation) => Some(reservation.budget.clone()),
			RuntimeAdmissionKind::Unbudgeted { .. } => None,
		}
	}

	fn confirm_open(&mut self) {
		let RuntimeAdmissionKind::Unbudgeted { pending } = &mut self.kind else {
			return;
		};
		if !*pending {
			return;
		}
		let mut state = lock(runtime_budget_state());
		if matches!(&*state, RuntimeBudgetState::Unconfigured { pending_opens } if *pending_opens > 0) {
			*state = RuntimeBudgetState::OpenedWithoutBudget;
		}
		*pending = false;
	}

	fn retain_until_restart(self) {
		self.retain_until_restart_in(runtime_budget_state());
	}

	fn retain_until_restart_in(mut self, state: &Mutex<RuntimeBudgetState>) {
		let pending = match &mut self.kind {
			RuntimeAdmissionKind::Budgeted(_) => {
				mem::forget(self);
				return;
			}
			RuntimeAdmissionKind::Unbudgeted { pending } => pending,
		};
		if !*pending {
			return;
		}
		let mut state = lock(state);
		if matches!(&*state, RuntimeBudgetState::Unconfigured { .. }) {
			*state = RuntimeBudgetState::OpenedWithoutBudget;
		}
		*pending = false;
	}
}

impl Drop for RuntimeAdmission {
	fn drop(&mut self) {
		if !matches!(&self.kind, RuntimeAdmissionKind::Unbudgeted { pending: true }) {
			return;
		}
		let mut state = lock(runtime_budget_state());
		if let RuntimeBudgetState::Unconfigured { pending_opens } = &mut *state {
			debug_assert!(*pending_opens > 0);
			*pending_opens = pending_opens.saturating_sub(1);
		}
	}
}

impl Drop for BudgetReservation {
	fn drop(&mut self) {
		if self.resident {
			release(&self.budget.resident_indexes, 1);
		}
		release(&self.budget.indexing_threads, self.indexing_threads);
		release(&self.budget.search_threads, self.search_threads);
		release(&self.budget.writer_memory_bytes, self.writer_memory_bytes);
		release(&self.budget.queued_bytes, self.queued_bytes);
	}
}

impl Drop for ExpensiveSearchPermit {
	fn drop(&mut self) {
		{
			let mut active = lock(&self.budget.expensive_searches);
			debug_assert!(*active > 0);
			*active = active.saturating_sub(1);
			self.budget.expensive_search_ready.notify_all();
		}
		self.budget.notify_expensive_wakes();
	}
}

impl Drop for ExpensiveWakeDemand {
	fn drop(&mut self) {
		let previous = self.budget.expensive_wake_demand.fetch_sub(1, Ordering::AcqRel);
		debug_assert!(previous > 0);
	}
}

fn reserve(counter: &AtomicUsize, amount: usize, maximum: usize, resource: &str) -> Result<()> {
	counter
		.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
			current.checked_add(amount).filter(|next| *next <= maximum)
		})
		.map(|_| ())
		.map_err(|_| FulltextError::new("E_RESOURCE_LIMIT", format!("process-wide {resource} limit exceeded")))
}

fn release(counter: &AtomicUsize, amount: usize) {
	if amount == 0 {
		return;
	}
	let released = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
		current.checked_sub(amount)
	});
	debug_assert!(released.is_ok());
}

impl Runtime {
	fn start(handle: u32, environment: Arc<EnvironmentState>, parts: RuntimeParts) -> Result<Arc<Self>> {
		let search_thread_count = parts.config.limits.search_threads;
		let expensive_search_budget = parts.reservation.budget();
		let engine = Arc::new(parts.engine);
		let reader = Arc::new(parts.reader);
		let writer_queue = Arc::new(BoundedQueue::new(
			parts.config.limits.max_queued_commands,
			parts.config.limits.max_queued_bytes,
		));
		let search_budget = (search_thread_count >= 2).then(|| {
			Arc::new(QueueBudget::new(
				parts.config.limits.max_queued_commands,
				parts.config.limits.max_queued_bytes,
			))
		});
		let search_wake = (search_thread_count >= 2).then(|| Arc::new(QueueWake::new()));
		let ordinary_search_queue = Arc::new(BoundedQueue::new_with_budget_and_wake(
			parts.config.limits.max_queued_commands,
			parts.config.limits.max_queued_bytes,
			search_budget.clone(),
			search_wake.clone(),
		));
		let expensive_search_queue = (search_thread_count >= 2).then(|| {
			Arc::new(BoundedQueue::new_with_budget_and_wake(
				parts.config.limits.max_queued_commands,
				parts.config.limits.max_queued_bytes,
				search_budget,
				search_wake.clone(),
			))
		});
		let runtime = Arc::new(Self {
			handle,
			path: parts.path,
			path_identity: parts.path_identity,
			config: parts.config,
			writer_queue,
			ordinary_search_queue,
			expensive_search_queue,
			search_wake,
			state: AtomicU8::new(STATE_OPEN),
			environment,
			uncommitted_mutations: AtomicU64::new(0),
			commit_opstamp: AtomicU64::new(0),
			writer_queue_nanoseconds: AtomicU64::new(0),
			writer_execution_nanoseconds: AtomicU64::new(0),
			search_queue_nanoseconds: AtomicU64::new(0),
			search_execution_nanoseconds: AtomicU64::new(0),
			search_threads: Mutex::new(Vec::with_capacity(search_thread_count)),
			closed: Arc::new(CompletionSignal::new()),
			reservation: Mutex::new(Some(parts.reservation)),
			expensive_search_budget,
			#[cfg(feature = "test-panic")]
			publish_fault: AtomicU8::new(0),
			#[cfg(feature = "test-panic")]
			poison_before_admission: AtomicBool::new(false),
			#[cfg(feature = "test-panic")]
			close_fault: AtomicU8::new(0),
			#[cfg(feature = "test-panic")]
			expensive_search_delay_milliseconds: AtomicU64::new(0),
			#[cfg(feature = "test-panic")]
			ordinary_search_delay_milliseconds: AtomicU64::new(0),
		});
		if let (Some(budget), Some(wake)) = (&runtime.expensive_search_budget, &runtime.search_wake) {
			budget.register_expensive_wake(wake);
		}
		let writer_runtime = runtime.clone();
		let writer_engine = engine.clone();
		let writer_reader = reader.clone();
		thread::Builder::new()
			.name(format!("fulltext-writer-{handle}"))
			.spawn(move || writer_loop(writer_runtime, parts.writer, writer_engine, writer_reader))
			.map_err(FulltextError::native)?;
		for worker in 0..search_thread_count {
			let search_runtime = runtime.clone();
			let search_engine = engine.clone();
			let search_reader = reader.clone();
			let join = match thread::Builder::new()
				.name(format!("fulltext-search-{handle}-{worker}"))
				.spawn(move || {
					if worker == 0 || search_runtime.expensive_search_queue.is_none() {
						ordinary_search_loop(search_runtime, search_engine, search_reader);
					} else {
						flexible_search_loop(search_runtime, search_engine, search_reader);
					}
				}) {
				Ok(join) => join,
				Err(error) => {
					drop(engine);
					drop(reader);
					runtime.force_close();
					let _ = runtime.wait_closed(CLEANUP_TIMEOUT);
					return Err(FulltextError::native(error));
				}
			};
			lock(&runtime.search_threads).push(join);
		}
		Ok(runtime)
	}

	fn require_open(&self) -> Result<()> {
		match self.state.load(Ordering::Acquire) {
			STATE_OPEN => Ok(()),
			STATE_POISONED => Err(FulltextError::new("E_POISONED", "index is poisoned")),
			_ => Err(FulltextError::new("E_CLOSED", "index is closing or closed")),
		}
	}

	fn search_queue(&self, expensive: bool) -> &Arc<BoundedQueue<SearchCommand>> {
		if expensive {
			self.expensive_search_queue
				.as_ref()
				.unwrap_or(&self.ordinary_search_queue)
		} else {
			&self.ordinary_search_queue
		}
	}

	fn expensive_search_permit(&self, deadline: Instant) -> Result<Option<ExpensiveSearchPermit>> {
		let Some(budget) = &self.expensive_search_budget else {
			return Ok(None);
		};
		let started = Instant::now();
		let permit = budget.acquire_expensive(deadline, &self.state);
		self.search_queue_nanoseconds
			.fetch_add(duration_ns(started.elapsed()), Ordering::Relaxed);
		permit.map(Some)
	}

	fn try_expensive_search_permit(&self) -> Option<ExpensiveSearchPermit> {
		self.expensive_search_budget
			.as_ref()
			.and_then(|budget| budget.try_acquire_expensive(&self.state))
	}

	fn expensive_wake_demand(&self) -> Option<ExpensiveWakeDemand> {
		let budget = self.expensive_search_budget.as_ref()?.clone();
		budget.expensive_wake_demand.fetch_add(1, Ordering::AcqRel);
		Some(ExpensiveWakeDemand { budget })
	}

	fn confirm_open(&self) {
		if let Some(reservation) = lock(&self.reservation).as_mut() {
			reservation.confirm_open();
		}
	}

	fn notify_expensive_waiters(&self) {
		if let Some(budget) = &self.expensive_search_budget {
			let _active = lock(&budget.expensive_searches);
			budget.expensive_search_ready.notify_all();
		}
	}

	fn release_reservation(&self) {
		drop(lock(&self.reservation).take());
	}

	fn retain_reservation_until_restart(&self) {
		if let Some(reservation) = lock(&self.reservation).take() {
			reservation.retain_until_restart();
		}
	}

	fn enqueue_writer(&self, command: WriterCommand, bytes: usize) -> boundary::Result<()> {
		self.require_open().map_err(fulltext_napi_error)?;
		#[cfg(feature = "test-panic")]
		self.poison_before_admission();
		self.writer_queue
			.try_push_if(command, bytes, || self.require_open())
			.map_err(fulltext_napi_error)
	}

	#[cfg(feature = "test-panic")]
	fn poison_before_admission(&self) {
		if self.poison_before_admission.swap(false, Ordering::AcqRel) {
			self.poison(FulltextError::new("E_POISONED", "test poison before admission"));
		}
	}

	fn force_close(&self) {
		let previous = self.state.swap(STATE_CLOSING, Ordering::AcqRel);
		if previous == STATE_CLOSED || previous == STATE_CLOSING {
			return;
		}
		self.notify_expensive_waiters();
		let _ = self.writer_queue.push_force(
			WriterCommand {
				operation: WriterOperation::Close { rollback: true },
				completion: Completion {
					callback: None,
					callbacks: self.environment.callbacks.clone(),
				},
			},
			0,
		);
	}

	fn poison(&self, error: FulltextError) {
		self.state.store(STATE_POISONED, Ordering::Release);
		self.notify_expensive_waiters();
		let mut close = None;
		for command in self.writer_queue.drain() {
			if matches!(&command.value.operation, WriterOperation::Close { .. }) && close.is_none() {
				close = Some(command.value);
			} else {
				command.value.fail(error.clone());
			}
		}
		if let Some(close) = close {
			let _ = self.writer_queue.push_force(close.force_rollback(), 0);
		}
		for command in self.ordinary_search_queue.close() {
			command.value.completion.failure(error.clone());
		}
		if let Some(queue) = &self.expensive_search_queue {
			for command in queue.close() {
				command.value.completion.failure(error.clone());
			}
		}
	}

	fn status_bytes(&self) -> Vec<u8> {
		let mut bytes = Vec::with_capacity(80);
		bytes.push(self.state.load(Ordering::Acquire));
		push_u64(&mut bytes, self.uncommitted_mutations.load(Ordering::Acquire));
		push_u64(&mut bytes, self.writer_queue.queued_commands.load(Ordering::Relaxed));
		push_u64(&mut bytes, self.writer_queue.queued_bytes.load(Ordering::Relaxed));
		let search_queued_commands = self.ordinary_search_queue.queued_commands.load(Ordering::Relaxed)
			+ self
				.expensive_search_queue
				.as_ref()
				.map_or(0, |queue| queue.queued_commands.load(Ordering::Relaxed));
		let search_queued_bytes = self.ordinary_search_queue.queued_bytes.load(Ordering::Relaxed)
			+ self
				.expensive_search_queue
				.as_ref()
				.map_or(0, |queue| queue.queued_bytes.load(Ordering::Relaxed));
		push_u64(&mut bytes, search_queued_commands);
		push_u64(&mut bytes, search_queued_bytes);
		push_u64(&mut bytes, self.commit_opstamp.load(Ordering::Acquire));
		push_u64(&mut bytes, self.writer_queue_nanoseconds.load(Ordering::Relaxed));
		push_u64(&mut bytes, self.writer_execution_nanoseconds.load(Ordering::Relaxed));
		push_u64(&mut bytes, self.search_queue_nanoseconds.load(Ordering::Relaxed));
		push_u64(&mut bytes, self.search_execution_nanoseconds.load(Ordering::Relaxed));
		bytes
	}

	fn wait_closed(&self, timeout: Duration) -> bool {
		self.closed.wait(timeout)
	}
}

impl CompletionSignal {
	fn new() -> Self {
		Self {
			done: Mutex::new(false),
			ready: Condvar::new(),
		}
	}

	fn signal(&self) {
		*lock(&self.done) = true;
		self.ready.notify_all();
	}

	fn wait(&self, timeout: Duration) -> bool {
		let done = lock(&self.done);
		if *done {
			return true;
		}
		let (done, _) = self
			.ready
			.wait_timeout_while(done, timeout, |done| !*done)
			.unwrap_or_else(|error| error.into_inner());
		*done
	}
}

impl<T> BoundedQueue<T> {
	fn new(max_commands: usize, max_bytes: usize) -> Self {
		Self::new_with_budget_and_wake(max_commands, max_bytes, None, None)
	}

	fn new_with_budget_and_wake(
		max_commands: usize,
		max_bytes: usize,
		shared_budget: Option<Arc<QueueBudget>>,
		shared_wake: Option<Arc<QueueWake>>,
	) -> Self {
		Self {
			state: Mutex::new(QueueState {
				items: VecDeque::new(),
				bytes: 0,
				closed: false,
			}),
			ready: Condvar::new(),
			max_commands,
			max_bytes,
			queued_commands: AtomicU64::new(0),
			queued_bytes: AtomicU64::new(0),
			shared_budget,
			shared_wake,
		}
	}

	fn try_push(&self, value: T, bytes: usize) -> Result<()> {
		self.try_push_if(value, bytes, || Ok(()))
	}

	fn try_push_if(&self, value: T, bytes: usize, admit: impl FnOnce() -> Result<()>) -> Result<()> {
		let mut state = lock(&self.state);
		admit()?;
		self.validate_capacity(&state, bytes)?;
		self.reserve_shared(bytes)?;
		state.bytes += bytes;
		state.items.push_back(Queued {
			value,
			bytes,
			enqueued: Instant::now(),
		});
		self.queued_commands.fetch_add(1, Ordering::Relaxed);
		self.queued_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
		self.ready.notify_one();
		self.notify_shared(false);
		Ok(())
	}

	fn check_capacity(&self, bytes: usize) -> Result<()> {
		let state = lock(&self.state);
		self.validate_capacity(&state, bytes)?;
		self.validate_shared_capacity(bytes)
	}

	fn validate_capacity(&self, state: &QueueState<T>, bytes: usize) -> Result<()> {
		if state.closed {
			return Err(FulltextError::new("E_CLOSED", "operation queue is closed"));
		}
		if state.items.len() >= self.max_commands || state.bytes.saturating_add(bytes) > self.max_bytes {
			return Err(FulltextError::new(
				"E_QUEUE_FULL",
				"operation queue limits are exhausted",
			));
		}
		Ok(())
	}

	fn push_force(&self, value: T, bytes: usize) -> Result<()> {
		if self.shared_budget.is_some() {
			return Err(FulltextError::new(
				"E_NATIVE_FAILURE",
				"forced queue admission cannot bypass a shared budget",
			));
		}
		let mut state = lock(&self.state);
		if state.closed {
			return Err(FulltextError::new("E_CLOSED", "operation queue is closed"));
		}
		state.bytes += bytes;
		state.items.push_back(Queued {
			value,
			bytes,
			enqueued: Instant::now(),
		});
		self.queued_commands.fetch_add(1, Ordering::Relaxed);
		self.queued_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
		self.ready.notify_one();
		self.notify_shared(false);
		Ok(())
	}

	fn pop(&self) -> Option<Queued<T>> {
		let mut state = lock(&self.state);
		loop {
			if let Some(item) = self.take_front(&mut state) {
				return Some(item);
			}
			if state.closed {
				return None;
			}
			state = self.ready.wait(state).unwrap_or_else(|error| error.into_inner());
		}
	}

	fn try_pop(&self) -> Option<Queued<T>> {
		self.take_front(&mut lock(&self.state))
	}

	fn is_closed(&self) -> bool {
		lock(&self.state).closed
	}

	fn has_items(&self) -> bool {
		!lock(&self.state).items.is_empty()
	}

	fn take_front(&self, state: &mut QueueState<T>) -> Option<Queued<T>> {
		let item = state.items.pop_front()?;
		state.bytes -= item.bytes;
		self.queued_commands.fetch_sub(1, Ordering::Relaxed);
		self.queued_bytes.fetch_sub(item.bytes as u64, Ordering::Relaxed);
		self.release_shared(1, item.bytes);
		Some(item)
	}

	fn close(&self) -> Vec<Queued<T>> {
		let mut state = lock(&self.state);
		state.closed = true;
		let items = drain_queue(&mut state, &self.queued_commands, &self.queued_bytes);
		self.release_shared(items.len(), items.iter().map(|item| item.bytes).sum());
		self.ready.notify_all();
		self.notify_shared(true);
		items
	}

	fn drain(&self) -> Vec<Queued<T>> {
		let mut state = lock(&self.state);
		let items = drain_queue(&mut state, &self.queued_commands, &self.queued_bytes);
		self.release_shared(items.len(), items.iter().map(|item| item.bytes).sum());
		self.notify_shared(false);
		items
	}

	fn shutdown_after_drain(&self) {
		let mut state = lock(&self.state);
		state.closed = true;
		self.ready.notify_all();
		self.notify_shared(true);
	}

	fn validate_shared_capacity(&self, bytes: usize) -> Result<()> {
		let Some(budget) = &self.shared_budget else {
			return Ok(());
		};
		budget.validate(&lock(&budget.state), bytes)
	}

	fn reserve_shared(&self, bytes: usize) -> Result<()> {
		let Some(budget) = &self.shared_budget else {
			return Ok(());
		};
		let mut state = lock(&budget.state);
		budget.validate(&state, bytes)?;
		state.0 += 1;
		state.1 += bytes;
		Ok(())
	}

	fn release_shared(&self, commands: usize, bytes: usize) {
		if let Some(budget) = &self.shared_budget {
			let mut state = lock(&budget.state);
			state.0 -= commands;
			state.1 -= bytes;
		}
	}

	fn notify_shared(&self, all: bool) {
		if let Some(wake) = &self.shared_wake {
			wake.notify(all);
		}
	}
}

impl QueueBudget {
	fn new(max_commands: usize, max_bytes: usize) -> Self {
		Self {
			state: Mutex::new((0, 0)),
			max_commands,
			max_bytes,
		}
	}

	fn validate(&self, state: &(usize, usize), bytes: usize) -> Result<()> {
		if state.0 >= self.max_commands || state.1.saturating_add(bytes) > self.max_bytes {
			return Err(FulltextError::new(
				"E_QUEUE_FULL",
				"operation queue limits are exhausted",
			));
		}
		Ok(())
	}
}

impl QueueWake {
	fn new() -> Self {
		Self {
			generation: Mutex::new(0),
			ready: Condvar::new(),
		}
	}

	fn snapshot(&self) -> u64 {
		*lock(&self.generation)
	}

	fn notify(&self, all: bool) {
		let mut generation = lock(&self.generation);
		*generation = generation.wrapping_add(1);
		if all {
			self.ready.notify_all();
		} else {
			self.ready.notify_one();
		}
	}

	fn wait_for_change(&self, observed: u64) {
		let generation = lock(&self.generation);
		let _guard = self
			.ready
			.wait_while(generation, |generation| *generation == observed)
			.unwrap_or_else(|error| error.into_inner());
	}

	fn wait_for_change_timeout(&self, observed: u64, timeout: Duration) {
		let generation = lock(&self.generation);
		let _guard = self
			.ready
			.wait_timeout_while(generation, timeout, |generation| *generation == observed)
			.unwrap_or_else(|error| error.into_inner());
	}
}

impl BoundedQueue<SearchCommand> {
	fn front_remaining_budget(&self) -> Option<Duration> {
		let state = lock(&self.state);
		let queued = state.items.front()?;
		let milliseconds = match &queued.value.operation {
			SearchOperation::Search(bytes) => search_budget(bytes),
			SearchOperation::Trace(bytes) => trace_budget(bytes),
		}
		.unwrap_or(0);
		Some(
			Duration::from_millis(u64::from(milliseconds))
				.checked_sub(queued.enqueued.elapsed())
				.unwrap_or(Duration::ZERO),
		)
	}
}

fn drain_queue<T>(state: &mut QueueState<T>, commands: &AtomicU64, bytes: &AtomicU64) -> Vec<Queued<T>> {
	let items = state.items.drain(..).collect::<Vec<_>>();
	state.bytes = 0;
	commands.store(0, Ordering::Relaxed);
	bytes.store(0, Ordering::Relaxed);
	items
}

impl Completion {
	fn success(mut self, body: Vec<u8>) {
		self.send(success_envelope(body));
	}

	fn failure(mut self, error: FulltextError) {
		self.send(error_envelope(error));
	}

	fn send(&mut self, bytes: Vec<u8>) {
		if let Some(callback) = self.callback.take() {
			self.callbacks.send(callback, bytes);
		}
	}
}

impl CallbackGate {
	fn new() -> Self {
		Self {
			alive: AtomicBool::new(true),
		}
	}

	fn is_alive(&self) -> bool {
		self.alive.load(Ordering::Acquire)
	}

	fn close(&self) {
		self.alive.store(false, Ordering::Release);
	}

	fn send(&self, callback: CompletionThreadsafeFunction, bytes: Vec<u8>) {
		if self.is_alive() {
			callback.call(Buffer::from(bytes), ThreadsafeFunctionCallMode::NonBlocking);
		}
	}
}

impl Drop for Completion {
	fn drop(&mut self) {
		if self.callback.is_some() {
			self.send(error_envelope(FulltextError::new(
				"E_CLOSED",
				"native operation ended before completion",
			)));
		}
	}
}

impl WriterCommand {
	fn fail(self, error: FulltextError) {
		self.completion.failure(error);
	}

	fn force_rollback(mut self) -> Self {
		debug_assert!(matches!(self.operation, WriterOperation::Close { .. }));
		self.operation = WriterOperation::Close { rollback: true };
		self
	}
}

fn writer_loop(runtime: Arc<Runtime>, writer: Writer, engine: Arc<Engine>, reader: Arc<IndexReader>) {
	let closed = runtime.closed.clone();
	writer_loop_inner(runtime, writer, engine, reader);
	closed.signal();
}

fn writer_loop_inner(runtime: Arc<Runtime>, writer: Writer, engine: Arc<Engine>, reader: Arc<IndexReader>) {
	let mut writer = Some(writer);
	while let Some(queued) = runtime.writer_queue.pop() {
		runtime
			.writer_queue_nanoseconds
			.fetch_add(duration_ns(queued.enqueued.elapsed()), Ordering::Relaxed);
		let WriterCommand { operation, completion } = queued.value;
		let started = Instant::now();
		let outcome = catch_unwind(AssertUnwindSafe(|| match operation {
			WriterOperation::Apply(bytes) => {
				match decode_batch(&bytes).and_then(|batch| active_writer(&writer)?.prepare(batch)) {
					Ok(prepared) => match active_writer(&writer).and_then(|writer| writer.apply_prepared(prepared)) {
						Ok(count) => {
							runtime.uncommitted_mutations.fetch_add(count, Ordering::AcqRel);
							WriterOutcome::Continue(Ok(u64_body(count)))
						}
						Err(error) => WriterOutcome::Poison(
							Err(error),
							FulltextError::new(
								"E_POISONED",
								"a mutation failed after writer state changed and the index generation is terminal",
							),
						),
					},
					Err(error) => WriterOutcome::Continue(Err(error)),
				}
			}
			WriterOperation::Commit => match active_writer_mut(&mut writer).and_then(|writer| writer.commit()) {
				Ok(opstamp) => {
					runtime.uncommitted_mutations.store(0, Ordering::Release);
					runtime.commit_opstamp.store(opstamp, Ordering::Release);
					WriterOutcome::Continue(Ok(u64_body(opstamp)))
				}
				Err(error) if error.code == "E_CHECKPOINT_REQUIRED" => WriterOutcome::Continue(Err(error)),
				Err(error) => WriterOutcome::Poison(
					Err(error),
					FulltextError::new(
						"E_POISONED",
						"a prior commit failed and the index generation is terminal",
					),
				),
			},
			WriterOperation::Publish(payload) => match active_writer_mut(&mut writer)
				.and_then(|writer| {
					#[cfg(feature = "test-panic")]
					fail_publish_at(&runtime, 1)?;
					writer.commit_with_payload(Some(&payload))
				})
				.and_then(|opstamp| {
					#[cfg(feature = "test-panic")]
					fail_publish_at(&runtime, 2)?;
					reader.reload().map_err(FulltextError::native)?;
					Ok(opstamp)
				}) {
				Ok(opstamp) => {
					runtime.uncommitted_mutations.store(0, Ordering::Release);
					runtime.commit_opstamp.store(opstamp, Ordering::Release);
					WriterOutcome::Continue(Ok(u64_body(opstamp)))
				}
				Err(error) => WriterOutcome::Poison(
					Err(error),
					FulltextError::new(
						"E_POISONED",
						"a publish failed after writer state changed and the index generation is terminal",
					),
				),
			},
			WriterOperation::Reload => {
				WriterOutcome::Continue(reader.reload().map(|()| Vec::new()).map_err(FulltextError::native))
			}
			WriterOperation::Close { rollback } => {
				let dirty = runtime.uncommitted_mutations.load(Ordering::Acquire) > 0;
				if !rollback
					&& dirty && runtime
					.state
					.compare_exchange(STATE_CLOSING, STATE_OPEN, Ordering::AcqRel, Ordering::Acquire)
					.is_ok()
				{
					WriterOutcome::Continue(Err(FulltextError::new(
						"E_DIRTY_CLOSE",
						"index has uncommitted mutations; publish (or commit if uncheckpointed), or close with rollback",
					)))
				} else {
					let rollback = rollback || dirty || runtime.state.load(Ordering::Acquire) == STATE_POISONED;
					WriterOutcome::Stop(close_writer(&runtime, &mut writer, rollback))
				}
			}
		}));
		runtime
			.writer_execution_nanoseconds
			.fetch_add(duration_ns(started.elapsed()), Ordering::Relaxed);
		match outcome {
			Ok(WriterOutcome::Continue(result)) => settle(completion, result),
			Ok(WriterOutcome::Stop(outcome)) => {
				let cleanup_runtime = runtime.clone();
				match catch_unwind(AssertUnwindSafe(move || {
					finish_runtime(&cleanup_runtime, engine, reader, outcome)
				})) {
					Ok(outcome) => settle(completion, close_result(outcome)),
					Err(_) => {
						finish_unproven_runtime(&runtime);
						completion.failure(quiescence_error(FulltextError::new(
							"E_NATIVE_PANIC",
							"native runtime teardown panicked",
						)));
					}
				}
				return;
			}
			Ok(WriterOutcome::Poison(result, poison)) => {
				settle(completion, result);
				runtime.poison(poison);
			}
			Err(_) => {
				let panic_error = FulltextError::new("E_NATIVE_PANIC", "native writer actor panicked");
				runtime.poison(panic_error.clone());
				let cleanup_runtime = runtime.clone();
				match catch_unwind(AssertUnwindSafe(move || {
					let close = close_writer(&cleanup_runtime, &mut writer, true);
					finish_runtime(&cleanup_runtime, engine, reader, close)
				})) {
					Ok(outcome) => {
						completion.failure(if outcome.quiesced {
							panic_error
						} else {
							quiescence_error(outcome.error.unwrap_or_else(|| {
								FulltextError::new("E_NATIVE_FAILURE", "native writer shutdown failed")
							}))
						});
					}
					Err(_) => {
						finish_unproven_runtime(&runtime);
						completion.failure(quiescence_error(FulltextError::new(
							"E_NATIVE_PANIC",
							"native writer shutdown panicked",
						)));
					}
				}
				return;
			}
		}
	}
	let cleanup_runtime = runtime.clone();
	if catch_unwind(AssertUnwindSafe(move || {
		let outcome = close_writer(&cleanup_runtime, &mut writer, true);
		finish_runtime(&cleanup_runtime, engine, reader, outcome)
	}))
	.is_err()
	{
		finish_unproven_runtime(&runtime);
	}
}

fn close_writer(_runtime: &Runtime, writer: &mut Option<Writer>, rollback: bool) -> WriterCloseOutcome {
	let Some(mut writer) = writer.take() else {
		return WriterCloseOutcome {
			quiesced: false,
			error: Some(FulltextError::new("E_POISONED", "writer is unavailable")),
		};
	};
	let rollback_error = if rollback { writer.rollback().err() } else { None };
	let outcome = match writer.close() {
		Ok(()) => WriterCloseOutcome {
			quiesced: true,
			error: rollback_error,
		},
		Err(error) => WriterCloseOutcome {
			quiesced: false,
			error: rollback_error.or(Some(error)),
		},
	};
	#[cfg(feature = "test-panic")]
	let outcome = {
		let mut outcome = outcome;
		match _runtime.close_fault.swap(0, Ordering::AcqRel) {
			1 if outcome.quiesced => {
				outcome.error = Some(FulltextError::new("E_STORAGE", "injected close failure"));
			}
			2 if outcome.quiesced => {
				outcome.quiesced = false;
				outcome.error = Some(FulltextError::new("E_STORAGE", "injected quiescence failure"));
			}
			_ => {}
		}
		outcome
	};
	outcome
}

fn finish_runtime(
	runtime: &Arc<Runtime>,
	engine: Arc<Engine>,
	reader: Arc<IndexReader>,
	mut outcome: WriterCloseOutcome,
) -> WriterCloseOutcome {
	runtime.ordinary_search_queue.shutdown_after_drain();
	if let Some(queue) = &runtime.expensive_search_queue {
		queue.shutdown_after_drain();
	}
	for join in std::mem::take(&mut *lock(&runtime.search_threads)) {
		if join.join().is_err() && outcome.error.is_none() {
			outcome.error = Some(FulltextError::new("E_NATIVE_PANIC", "native search actor panicked"));
		}
	}
	let actors_released = Arc::strong_count(&engine) == 1 && Arc::strong_count(&reader) == 1;
	debug_assert!(actors_released);
	if !actors_released {
		outcome.quiesced = false;
		outcome.error.get_or_insert_with(|| {
			FulltextError::new(
				"E_NATIVE_FAILURE",
				"native actors retained index resources after shutdown",
			)
		});
	}
	drop(reader);
	drop(engine);
	runtime.writer_queue.close();
	if outcome.quiesced {
		runtime.state.store(STATE_CLOSED, Ordering::Release);
		runtime.release_reservation();
		release_runtime(runtime.handle, &runtime.path_identity, &runtime.environment);
	} else {
		runtime.state.store(STATE_POISONED, Ordering::Release);
		runtime.retain_reservation_until_restart();
		release_runtime_handle(
			runtime.handle,
			&runtime.path,
			&runtime.path_identity,
			&runtime.environment,
		);
	}
	outcome
}

fn finish_unproven_runtime(runtime: &Arc<Runtime>) {
	runtime.ordinary_search_queue.shutdown_after_drain();
	if let Some(queue) = &runtime.expensive_search_queue {
		queue.shutdown_after_drain();
	}
	for join in std::mem::take(&mut *lock(&runtime.search_threads)) {
		let _ = join.join();
	}
	runtime.writer_queue.close();
	runtime.state.store(STATE_POISONED, Ordering::Release);
	runtime.retain_reservation_until_restart();
	release_runtime_handle(
		runtime.handle,
		&runtime.path,
		&runtime.path_identity,
		&runtime.environment,
	);
}

fn close_result(outcome: WriterCloseOutcome) -> Result<Vec<u8>> {
	match (outcome.quiesced, outcome.error) {
		(true, None) => Ok(Vec::new()),
		(true, Some(error)) => Err(FulltextError::new(
			"E_CLOSE_FAILED",
			format!("native resources were released, but shutdown reported: {error}"),
		)),
		(false, Some(error)) => Err(quiescence_error(error)),
		(false, None) => Err(quiescence_error(FulltextError::new(
			"E_NATIVE_FAILURE",
			"native writer shutdown failed",
		))),
	}
}

fn quiescence_error(error: FulltextError) -> FulltextError {
	FulltextError::new(
		"E_QUIESCENCE_FAILED",
		format!("native writer teardown did not prove quiescence: {error}"),
	)
}

fn settle(completion: Completion, result: Result<Vec<u8>>) {
	match result {
		Ok(body) => completion.success(body),
		Err(error) => completion.failure(error),
	}
}

#[cfg(feature = "test-panic")]
fn fail_publish_at(runtime: &Runtime, stage: u8) -> Result<()> {
	if runtime
		.publish_fault
		.compare_exchange(stage, 0, Ordering::AcqRel, Ordering::Acquire)
		.is_ok()
	{
		return Err(FulltextError::new("E_STORAGE", "injected publication failure"));
	}
	Ok(())
}

fn active_writer(writer: &Option<Writer>) -> Result<&Writer> {
	writer
		.as_ref()
		.ok_or_else(|| FulltextError::new("E_POISONED", "writer is unavailable"))
}

fn active_writer_mut(writer: &mut Option<Writer>) -> Result<&mut Writer> {
	writer
		.as_mut()
		.ok_or_else(|| FulltextError::new("E_POISONED", "writer is unavailable"))
}

fn ordinary_search_loop(runtime: Arc<Runtime>, engine: Arc<Engine>, reader: Arc<IndexReader>) {
	while let Some(queued) = runtime.ordinary_search_queue.pop() {
		#[cfg(feature = "test-panic")]
		{
			let milliseconds = runtime.ordinary_search_delay_milliseconds.swap(0, Ordering::AcqRel);
			if milliseconds > 0 {
				thread::sleep(Duration::from_millis(milliseconds));
			}
		}
		if !execute_search_command(&runtime, &engine, &reader, queued, None) {
			return;
		}
	}
}

fn flexible_search_loop(runtime: Arc<Runtime>, engine: Arc<Engine>, reader: Arc<IndexReader>) {
	let expensive = runtime.expensive_search_queue.as_ref().unwrap();
	let wake = runtime.search_wake.as_ref().unwrap();
	loop {
		let observed = wake.snapshot();
		let has_expensive = expensive.has_items();
		if has_expensive && expensive.front_remaining_budget() == Some(Duration::ZERO) {
			if let Some(queued) = expensive.try_pop() {
				if !execute_search_command(&runtime, &engine, &reader, queued, None) {
					return;
				}
				continue;
			}
		}
		let wake_demand = has_expensive.then(|| runtime.expensive_wake_demand()).flatten();
		let permit = has_expensive.then(|| runtime.try_expensive_search_permit()).flatten();
		if has_expensive && (permit.is_some() || runtime.expensive_search_budget.is_none()) {
			if let Some(queued) = expensive.try_pop() {
				drop(wake_demand);
				if !execute_search_command(&runtime, &engine, &reader, queued, permit) {
					return;
				}
				continue;
			}
		}
		drop(permit);
		if let Some(queued) = runtime.ordinary_search_queue.try_pop() {
			drop(wake_demand);
			if !execute_search_command(&runtime, &engine, &reader, queued, None) {
				return;
			}
			continue;
		}
		if runtime.state.load(Ordering::Acquire) != STATE_OPEN {
			if let Some(queued) = expensive.try_pop() {
				drop(wake_demand);
				if !execute_search_command(&runtime, &engine, &reader, queued, None) {
					return;
				}
				continue;
			}
		}
		if expensive.is_closed() {
			drop(wake_demand);
			while let Some(queued) = runtime.ordinary_search_queue.pop() {
				if !execute_search_command(&runtime, &engine, &reader, queued, None) {
					return;
				}
			}
			return;
		}
		if let Some(remaining) = expensive.front_remaining_budget() {
			wake.wait_for_change_timeout(observed, remaining);
		} else {
			wake.wait_for_change(observed);
		}
		drop(wake_demand);
	}
}

fn execute_search_command(
	runtime: &Runtime,
	engine: &Engine,
	reader: &IndexReader,
	queued: Queued<SearchCommand>,
	preacquired_permit: Option<ExpensiveSearchPermit>,
) -> bool {
	let queued_for = queued.enqueued.elapsed();
	runtime
		.search_queue_nanoseconds
		.fetch_add(duration_ns(queued_for), Ordering::Relaxed);
	let SearchCommand { operation, completion } = queued.value;
	let result = catch_unwind(AssertUnwindSafe(|| match operation {
		SearchOperation::Search(bytes) => {
			let deadline = operation_deadline(search_budget(&bytes)?, queued.enqueued.elapsed())?;
			let expensive = search_mode(&bytes)?.is_expensive();
			let _permit = if expensive {
				match preacquired_permit {
					Some(permit) => Some(permit),
					None => runtime.expensive_search_permit(deadline)?,
				}
			} else {
				None
			};
			let started = Instant::now();
			#[cfg(feature = "test-panic")]
			{
				let milliseconds = if expensive {
					runtime.expensive_search_delay_milliseconds.swap(0, Ordering::AcqRel)
				} else {
					0
				};
				if milliseconds > 0 {
					thread::sleep(Duration::from_millis(milliseconds));
				}
			}
			let result = decode_search(&bytes).and_then(|request| {
				engine
					.search_with_deadline(&reader.searcher(), &request, deadline)
					.map(SearchOutcome::Search)
			});
			runtime
				.search_execution_nanoseconds
				.fetch_add(duration_ns(started.elapsed()), Ordering::Relaxed);
			result
		}
		SearchOperation::Trace(bytes) => {
			let deadline = operation_deadline(trace_budget(&bytes)?, queued.enqueued.elapsed())?;
			let _permit = match preacquired_permit {
				Some(permit) => Some(permit),
				None => runtime.expensive_search_permit(deadline)?,
			};
			let started = Instant::now();
			#[cfg(feature = "test-panic")]
			if _permit.is_some() {
				let milliseconds = runtime.expensive_search_delay_milliseconds.swap(0, Ordering::AcqRel);
				if milliseconds > 0 {
					thread::sleep(Duration::from_millis(milliseconds));
				}
			}
			let result = decode_trace(&bytes).and_then(|request| {
				if Instant::now() >= deadline {
					return Err(search_timeout());
				}
				let result = engine.trace_matches(&request.search, &request.records, Some(deadline))?;
				if Instant::now() >= deadline {
					return Err(search_timeout());
				}
				Ok(SearchOutcome::Trace(result))
			});
			runtime
				.search_execution_nanoseconds
				.fetch_add(duration_ns(started.elapsed()), Ordering::Relaxed);
			result
		}
	}));
	match result {
		Ok(Ok(SearchOutcome::Search(result))) => completion.success(search_body(result)),
		Ok(Ok(SearchOutcome::Trace(result))) => completion.success(trace_body(result)),
		Ok(Err(error)) => completion.failure(error),
		Err(_) => {
			completion.failure(FulltextError::new("E_NATIVE_PANIC", "native search actor panicked"));
			runtime.poison(FulltextError::new("E_NATIVE_PANIC", "native search actor panicked"));
			return false;
		}
	}
	true
}

enum SearchOutcome {
	Search(SearchResult),
	Trace(TraceResult),
}

fn operation_deadline(budget_milliseconds: u32, queued_for: Duration) -> Result<Instant> {
	let budget = Duration::from_millis(u64::from(budget_milliseconds));
	let remaining = budget.checked_sub(queued_for).ok_or_else(search_timeout)?;
	Ok(Instant::now() + remaining)
}

fn search_timeout() -> FulltextError {
	FulltextError::new("E_TIMEOUT", "full-text search exceeded its execution budget")
}

fn open_on_thread(
	handle: u32,
	bytes: Vec<u8>,
	completion: Completion,
	opening_done: Arc<CompletionSignal>,
	environment: Arc<EnvironmentState>,
) {
	open_on_thread_inner(handle, bytes, completion, environment);
	opening_done.signal();
}

fn open_on_thread_inner(handle: u32, bytes: Vec<u8>, completion: Completion, environment: Arc<EnvironmentState>) {
	let result = catch_unwind(AssertUnwindSafe(|| open_runtime(handle, bytes, environment.clone())));
	let opened = matches!(result, Ok(Ok(_)));
	match result {
		Ok(Ok(payload)) => completion.success(open_body(handle, payload.as_deref())),
		Ok(Err(error)) => completion.failure(error),
		Err(_) => completion.failure(FulltextError::new("E_NATIVE_PANIC", "native index open panicked")),
	}
	registry().opening.remove(&handle);
	if !opened {
		environment.release(handle);
	}
}

fn open_runtime(handle: u32, bytes: Vec<u8>, environment: Arc<EnvironmentState>) -> Result<Option<String>> {
	let open = decode_open(&bytes)?;
	Engine::validate(&open.engine)?;
	let canonical = create_and_canonicalize(Path::new(&open.path))?;
	let (_lifecycle_directory, _lifecycle_lock) = acquire_lifecycle_lock(&canonical)?;
	let directory = MmapDirectory::open(&canonical).map_err(storage_error)?;
	let path_identity = path_identity(&canonical)?;
	open_runtime_with_directory(handle, canonical, path_identity, open.engine, directory, environment)
}

fn inspect_runtime(bytes: &[u8]) -> Result<InspectionResult> {
	let open = decode_inspect(bytes)?;
	let path = Path::new(&open.path);
	let canonical = match fs::canonicalize(path) {
		Ok(canonical) => canonical,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(InspectionResult::Missing),
		Err(error) => return Err(storage_error(error)),
	};
	let directory = MmapDirectory::open(&canonical).map_err(storage_error)?;
	Engine::inspect(directory, &open.identity)
}

fn reset_on_thread(
	operation: u32,
	bytes: Vec<u8>,
	completion: Completion,
	reset_done: Arc<CompletionSignal>,
	environment: Arc<EnvironmentState>,
) {
	reset_on_thread_inner(operation, bytes, completion, environment);
	reset_done.signal();
}

fn reset_on_thread_inner(operation: u32, bytes: Vec<u8>, completion: Completion, environment: Arc<EnvironmentState>) {
	let result = catch_unwind(AssertUnwindSafe(|| reset_runtime(operation, &bytes, &environment)));
	let response = match result {
		Ok(Ok(result)) => Ok(reset_body(result)),
		Ok(Err(error)) => Err(error),
		Err(_) => Err(FulltextError::new("E_NATIVE_PANIC", "native index reset panicked")),
	};
	settle(completion, response);
	{
		let mut registry = registry();
		registry.opening.remove(&operation);
		registry.cancelled.remove(&operation);
	}
	environment.release(operation);
}

fn reset_runtime(operation: u32, bytes: &[u8], environment: &EnvironmentState) -> Result<ResetResult> {
	let reset = decode_reset(bytes)?;
	{
		let mut registry = registry();
		if registry.cancelled.remove(&operation) || !environment.callbacks.is_alive() {
			return Err(FulltextError::new(
				"E_CLOSED",
				"Node environment closed during index reset",
			));
		}
	}
	let path = Path::new(&reset.path);
	let metadata = match fs::symlink_metadata(path) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(ResetResult::Missing),
		Err(error) => return Err(storage_error(error)),
	};
	if is_link_like(&metadata) {
		return Err(FulltextError::invalid("reset path must not be a symbolic link"));
	}
	if !metadata.is_dir() {
		return Err(FulltextError::invalid("reset path must be a directory"));
	}
	let canonical = match fs::canonicalize(path) {
		Ok(canonical) => canonical,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(ResetResult::Missing),
		Err(error) => return Err(storage_error(error)),
	};
	let parent = canonical
		.parent()
		.ok_or_else(|| FulltextError::invalid("reset path must not be a filesystem root"))?;
	let initial_identity = path_identity_from_metadata(&canonical, &metadata);
	let (lifecycle_directory, lifecycle_lock) = acquire_lifecycle_lock(&canonical)?;
	let public_metadata = match fs::symlink_metadata(path) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(ResetResult::Missing),
		Err(error) => return Err(storage_error(error)),
	};
	if is_link_like(&public_metadata) || !public_metadata.is_dir() {
		return Err(FulltextError::new(
			"E_LOCK_BUSY",
			"the physical index path changed before reset acquired ownership",
		));
	}
	let current_metadata = match fs::symlink_metadata(&canonical) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(ResetResult::Missing),
		Err(error) => return Err(storage_error(error)),
	};
	if is_link_like(&current_metadata) || !current_metadata.is_dir() {
		return Err(FulltextError::new(
			"E_LOCK_BUSY",
			"the physical index path changed before reset acquired ownership",
		));
	}
	let physical_identity = path_identity_from_metadata(&canonical, &current_metadata);
	if physical_identity != initial_identity {
		return Err(FulltextError::new(
			"E_LOCK_BUSY",
			"the physical index path changed before reset acquired ownership",
		));
	}
	let directory = MmapDirectory::open(&canonical).map_err(storage_error)?;
	validate_reset_target(&canonical, &reset.index_id)?;
	{
		let mut registry = registry();
		if registry.unproven_paths.contains(&quiescence_key(&canonical)) {
			return Err(quiescence_error(FulltextError::new(
				"E_LOCK_BUSY",
				"this native index was not proven quiescent; restart is required",
			)));
		}
		if registry.paths.contains_key(&physical_identity) {
			return Err(FulltextError::new("E_LOCK_BUSY", "the physical index is still active"));
		}
		registry
			.paths
			.insert(physical_identity.clone(), PathReservation::Reset(operation));
	}
	let reservation = ResetReservation {
		operation,
		path_identity: physical_identity,
	};
	match path_identity(&canonical) {
		Ok(current) if current == reservation.path_identity => {}
		_ => {
			return Err(FulltextError::new(
				"E_LOCK_BUSY",
				"the physical index path changed during reset",
			))
		}
	}
	let writer_lock = directory.acquire_lock(&INDEX_WRITER_LOCK).map_err(reset_lock_error)?;
	let retired_root = parent.join(RETIRED_ROOT);
	ensure_retired_root(&retired_root)?;
	let retired_path = next_retired_path(&retired_root, path, &canonical, operation)?;
	let public_path = public_path(&retired_path)?;
	drop(writer_lock);
	drop(directory);
	fs::rename(&canonical, &retired_path).map_err(rename_error)?;
	drop(lifecycle_lock);
	drop(lifecycle_directory);
	drop(reservation);
	Ok(ResetResult::Reset(public_path))
}

fn validate_reset_target(path: &Path, expected_index_id: &str) -> Result<()> {
	let mut identity_found = false;
	let mut content_found = false;
	for entry in fs::read_dir(path).map_err(storage_error)? {
		let entry = entry.map_err(storage_error)?;
		let name = entry.file_name();
		if name == IDENTITY_PATH {
			let metadata = fs::symlink_metadata(entry.path()).map_err(storage_error)?;
			if !metadata.is_file() || is_link_like(&metadata) {
				return Err(FulltextError::new(
					"E_INDEX_CORRUPT",
					"the persisted index identity is invalid",
				));
			}
			let bytes = fs::read(entry.path()).map_err(storage_error)?;
			let index_id = persisted_index_id(&bytes)
				.ok_or_else(|| FulltextError::new("E_INDEX_CORRUPT", "the persisted index identity is invalid"))?;
			if index_id != expected_index_id {
				return Err(FulltextError::new(
					"E_IDENTITY_MISMATCH",
					"the persisted index ID does not match the reset request",
				));
			}
			identity_found = true;
		} else if name.as_os_str() != INDEX_WRITER_LOCK.filepath.as_os_str()
			&& name.as_os_str() != META_LOCK.filepath.as_os_str()
			&& name != LEGACY_LIFECYCLE_LOCK
		{
			content_found = true;
		}
	}
	if content_found && !identity_found {
		return Err(FulltextError::invalid(
			"reset path is not an empty or recognizable Fulltext index directory",
		));
	}
	Ok(())
}

fn next_retired_path(root: &Path, source: &Path, canonical: &Path, operation: u32) -> Result<PathBuf> {
	let basename = source
		.file_name()
		.or_else(|| canonical.file_name())
		.ok_or_else(|| FulltextError::invalid("reset path must have a final component"))?
		.to_string_lossy();
	let timestamp = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_nanos();
	for attempt in 0..16u8 {
		let candidate = root.join(format!(
			"{basename}.{}.{}.{}.{attempt}",
			std::process::id(),
			operation,
			timestamp
		));
		match fs::symlink_metadata(&candidate) {
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(candidate),
			Ok(_) => {}
			Err(error) => return Err(storage_error(error)),
		}
	}
	Err(FulltextError::new(
		"E_STORAGE",
		"could not allocate a unique retired index path",
	))
}

fn ensure_retired_root(path: &Path) -> Result<()> {
	ensure_directory_root(path, "the .fulltext-retired path")
}

fn ensure_lifecycle_root(path: &Path) -> Result<()> {
	ensure_directory_root(path, "the .fulltext-locks path")
}

fn ensure_directory_root(path: &Path, label: &str) -> Result<()> {
	match fs::symlink_metadata(path) {
		Ok(metadata) => validate_directory_root(metadata, label),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => match fs::create_dir(path) {
			Ok(()) => Ok(()),
			Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
				validate_directory_root(fs::symlink_metadata(path).map_err(storage_error)?, label)
			}
			Err(error) => Err(storage_error(error)),
		},
		Err(error) => Err(storage_error(error)),
	}
}

fn validate_directory_root(metadata: fs::Metadata, label: &str) -> Result<()> {
	if is_link_like(&metadata) || !metadata.is_dir() {
		return Err(FulltextError::invalid(format!(
			"{label} must be a directory and must not be a symbolic link"
		)));
	}
	Ok(())
}

fn is_link_like(metadata: &fs::Metadata) -> bool {
	metadata.file_type().is_symlink()
}

fn acquire_lifecycle_lock(index_path: &Path) -> Result<(MmapDirectory, DirectoryLock)> {
	let parent = index_path
		.parent()
		.ok_or_else(|| FulltextError::invalid("native index path must not be a filesystem root"))?;
	let lock_name = index_path
		.file_name()
		.ok_or_else(|| FulltextError::invalid("native index path must have a final component"))?;
	if lock_name == LIFECYCLE_ROOT || lock_name == RETIRED_ROOT {
		return Err(FulltextError::invalid(
			"native index path uses a reserved directory name",
		));
	}
	let root = parent.join(LIFECYCLE_ROOT);
	ensure_lifecycle_root(&root).map_err(|error| lifecycle_storage_error(error, &root))?;
	let directory = MmapDirectory::open(&root)
		.map_err(storage_error)
		.map_err(|error| lifecycle_storage_error(error, &root))?;
	let lock = Lock {
		filepath: PathBuf::from(lock_name),
		is_blocking: false,
	};
	let guard = directory
		.acquire_lock(&lock)
		.map_err(lifecycle_lock_error)
		.map_err(|error| lifecycle_storage_error(error, &root))?;
	Ok((directory, guard))
}

fn lifecycle_storage_error(error: FulltextError, root: &Path) -> FulltextError {
	FulltextError::new(
		error.code,
		format!(
			"could not use lifecycle lock directory {}: {}",
			root.display(),
			error.message
		),
	)
}

fn public_path(path: &Path) -> Result<String> {
	let value = path
		.to_str()
		.ok_or_else(|| FulltextError::invalid("reset paths must contain valid Unicode"))?;
	#[cfg(windows)]
	{
		if let Some(value) = value.strip_prefix("\\\\?\\UNC\\") {
			return Ok(format!("\\\\{value}"));
		}
		if let Some(value) = value.strip_prefix("\\\\?\\") {
			return Ok(value.to_owned());
		}
	}
	Ok(value.to_owned())
}

fn reset_lock_error(error: tantivy::directory::error::LockError) -> FulltextError {
	match error {
		tantivy::directory::error::LockError::LockBusy => {
			FulltextError::new("E_LOCK_BUSY", "another writer owns the Tantivy index lock")
		}
		other => storage_error(other),
	}
}

fn lifecycle_lock_error(error: tantivy::directory::error::LockError) -> FulltextError {
	match error {
		tantivy::directory::error::LockError::LockBusy => {
			FulltextError::new("E_LOCK_BUSY", "another native index lifecycle operation owns this path")
		}
		other => storage_error(other),
	}
}

fn rename_error(error: std::io::Error) -> FulltextError {
	#[cfg(windows)]
	if error.raw_os_error() == Some(32) {
		return FulltextError::new("E_LOCK_BUSY", "the native index directory is still in use");
	}
	storage_error(error)
}

impl Drop for ResetReservation {
	fn drop(&mut self) {
		let mut registry = registry();
		if registry.paths.get(&self.path_identity) == Some(&PathReservation::Reset(self.operation)) {
			registry.paths.remove(&self.path_identity);
		}
	}
}

fn open_runtime_with_directory(
	handle: u32,
	canonical: PathBuf,
	physical_identity: PathIdentity,
	config: EngineConfig,
	directory: MmapDirectory,
	environment: Arc<EnvironmentState>,
) -> Result<Option<String>> {
	{
		let mut registry = registry();
		if registry.cancelled.remove(&handle) || !environment.callbacks.is_alive() {
			registry.opening.remove(&handle);
			return Err(FulltextError::new(
				"E_CLOSED",
				"Node environment closed during index open",
			));
		}
		if registry.unproven_paths.contains(&quiescence_key(&canonical)) {
			return Err(quiescence_error(FulltextError::new(
				"E_LOCK_BUSY",
				"this native index was not proven quiescent; restart is required",
			)));
		}
		if let Some(reservation) = registry.paths.get(&physical_identity) {
			return Err(match reservation {
				PathReservation::Open(_) => {
					FulltextError::new("E_DUPLICATE_OPEN", "the physical index is already open")
				}
				PathReservation::Reset(_) => FulltextError::new("E_LOCK_BUSY", "the physical index is being reset"),
			});
		}
		registry
			.paths
			.insert(physical_identity.clone(), PathReservation::Open(handle));
	}
	let result = (|| {
		match path_identity(&canonical) {
			Ok(current) if current == physical_identity => {}
			_ => {
				return Err(FulltextError::new(
					"E_LOCK_BUSY",
					"the physical index path changed during open",
				))
			}
		}
		let reservation = admit_runtime(&config.limits)?;
		let engine = Engine::open(directory, &config)?;
		let (writer, committed_payload) = engine.writer_with_payload(&config)?;
		let reader = engine.reader_for_open()?;
		let runtime = Runtime::start(
			handle,
			environment.clone(),
			RuntimeParts {
				path: canonical.clone(),
				path_identity: physical_identity.clone(),
				config,
				engine,
				writer,
				reader,
				reservation,
			},
		)?;
		#[cfg(feature = "test-panic")]
		if FAIL_NEXT_OPEN_CLEANUP.swap(false, Ordering::AcqRel) {
			runtime.close_fault.store(2, Ordering::Release);
			runtime.force_close();
			let _ = runtime.wait_closed(CLEANUP_TIMEOUT);
			return Err(FulltextError::new("E_CLOSED", "injected open cleanup failure"));
		}
		let mut registry = registry();
		if registry.cancelled.remove(&handle) || !environment.callbacks.is_alive() {
			drop(registry);
			runtime.force_close();
			let _ = runtime.wait_closed(CLEANUP_TIMEOUT);
			return Err(FulltextError::new(
				"E_CLOSED",
				"Node environment closed during index open",
			));
		}
		runtime.confirm_open();
		registry.handles.insert(handle, runtime);
		registry.opening.remove(&handle);
		Ok(committed_payload)
	})();
	if result.is_err() {
		release_runtime(handle, &physical_identity, &environment);
	}
	result
}

fn completion(callback: CompletionCallback<'_>, environment: &Arc<EnvironmentState>) -> boundary::Result<Completion> {
	let callback = callback
		.build_threadsafe_function::<Buffer>()
		.build()
		.map_err(|error| napi_error("E_NATIVE_FAILURE", error))?;
	Ok(Completion {
		callback: Some(callback),
		callbacks: environment.callbacks.clone(),
	})
}

fn runtime(env: &Env, handle: u32) -> boundary::Result<Arc<Runtime>> {
	let registry = registry();
	let runtime = registry
		.handles
		.get(&handle)
		.cloned()
		.ok_or_else(|| napi_error("E_CLOSED", "unknown or closed fulltext index handle"))?;
	let environment = registry.environments.get(&(env.raw() as usize)).and_then(Weak::upgrade);
	if !environment.is_some_and(|environment| Arc::ptr_eq(&environment, &runtime.environment)) {
		return Err(napi_error(
			"E_NATIVE_FAILURE",
			"native index handles cannot be used from another Node environment",
		));
	}
	Ok(runtime)
}

fn cleanup_handle(handle: u32) -> Option<Arc<Runtime>> {
	let runtime = {
		let mut registry = registry();
		match registry.handles.get(&handle).cloned() {
			Some(runtime) => Some(runtime),
			None if registry.opening.remove(&handle) => {
				registry.cancelled.insert(handle);
				None
			}
			None => None,
		}
	};
	if let Some(runtime) = runtime {
		runtime.force_close();
		Some(runtime)
	} else {
		None
	}
}

struct EnvironmentHookData {
	key: usize,
	environment: Arc<EnvironmentState>,
}

enum CleanupWait {
	Runtime(Arc<Runtime>),
	Opening(Arc<CompletionSignal>),
}

fn environment_state(env: &Env) -> boundary::Result<Arc<EnvironmentState>> {
	let key = env.raw() as usize;
	if let Some(environment) = registry().environments.get(&key).and_then(Weak::upgrade) {
		return Ok(environment);
	}
	let environment = Arc::new(EnvironmentState {
		callbacks: Arc::new(CallbackGate::new()),
		handles: Mutex::new(HashMap::new()),
	});
	env.add_async_cleanup_hook(
		EnvironmentHookData {
			key,
			environment: environment.clone(),
		},
		|data| {
			let _ = catch_unwind(AssertUnwindSafe(|| finish_environment_cleanup(data)));
		},
	)
	.map_err(|error| napi_error("E_NATIVE_FAILURE", error))?;
	registry().environments.insert(key, Arc::downgrade(&environment));
	Ok(environment)
}

fn finish_environment_cleanup(data: EnvironmentHookData) {
	data.environment.callbacks.close();
	let tracked = data.environment.take_handles();
	let waits = tracked
		.into_iter()
		.map(|(handle, opening_done)| match cleanup_handle(handle) {
			Some(runtime) => CleanupWait::Runtime(runtime),
			None => CleanupWait::Opening(opening_done),
		})
		.collect::<Vec<_>>();
	let remove_environment = registry()
		.environments
		.get(&data.key)
		.and_then(Weak::upgrade)
		.is_some_and(|environment| Arc::ptr_eq(&environment, &data.environment));
	if remove_environment {
		registry().environments.remove(&data.key);
	}
	let deadline = Instant::now() + CLEANUP_TIMEOUT;
	let finished = waits.into_iter().all(|wait| {
		let remaining = deadline.saturating_duration_since(Instant::now());
		!remaining.is_zero() && wait.wait(remaining)
	});
	if !finished {
		eprintln!("fulltext native cleanup exceeded {} seconds", CLEANUP_TIMEOUT.as_secs());
	}
}

impl EnvironmentState {
	fn track(&self, handle: u32, opening_done: Arc<CompletionSignal>) {
		lock(&self.handles).insert(handle, opening_done);
	}

	fn release(&self, handle: u32) {
		lock(&self.handles).remove(&handle);
	}

	fn take_handles(&self) -> HashMap<u32, Arc<CompletionSignal>> {
		mem::take(&mut *lock(&self.handles))
	}
}

impl CleanupWait {
	fn wait(&self, timeout: Duration) -> bool {
		match self {
			Self::Runtime(runtime) => runtime.wait_closed(timeout),
			Self::Opening(signal) => signal.wait(timeout),
		}
	}
}

fn release_runtime(handle: u32, path_identity: &PathIdentity, environment: &EnvironmentState) {
	let mut registry = registry();
	registry.handles.remove(&handle);
	if registry.paths.get(path_identity) == Some(&PathReservation::Open(handle)) {
		registry.paths.remove(path_identity);
	}
	drop(registry);
	environment.release(handle);
}

fn release_runtime_handle(handle: u32, path: &Path, path_identity: &PathIdentity, environment: &EnvironmentState) {
	let mut registry = registry();
	registry.handles.remove(&handle);
	if registry.paths.get(path_identity) == Some(&PathReservation::Open(handle)) {
		registry.paths.remove(path_identity);
	}
	registry.unproven_paths.insert(quiescence_key(path));
	drop(registry);
	environment.release(handle);
}

fn registry() -> std::sync::MutexGuard<'static, Registry> {
	REGISTRY
		.get_or_init(Default::default)
		.lock()
		.unwrap_or_else(|error| error.into_inner())
}

fn create_and_canonicalize(path: &Path) -> Result<PathBuf> {
	fs::create_dir_all(path).map_err(storage_error)?;
	fs::canonicalize(path).map_err(storage_error)
}

#[cfg(unix)]
fn path_identity(path: &Path) -> Result<PathIdentity> {
	let metadata = fs::metadata(path).map_err(storage_error)?;
	Ok(path_identity_from_metadata(path, &metadata))
}

#[cfg(windows)]
fn path_identity(path: &Path) -> Result<PathIdentity> {
	let metadata = fs::metadata(path).map_err(storage_error)?;
	Ok(path_identity_from_metadata(path, &metadata))
}

#[cfg(all(not(unix), not(windows)))]
fn path_identity(path: &Path) -> Result<PathIdentity> {
	let metadata = fs::metadata(path).map_err(storage_error)?;
	Ok(path_identity_from_metadata(path, &metadata))
}

#[cfg(unix)]
fn path_identity_from_metadata(_path: &Path, metadata: &fs::Metadata) -> PathIdentity {
	use std::os::unix::fs::MetadataExt;
	PathIdentity::Unix(metadata.dev(), metadata.ino())
}

#[cfg(windows)]
fn path_identity_from_metadata(path: &Path, _metadata: &fs::Metadata) -> PathIdentity {
	PathIdentity::Path(PathBuf::from(path.to_string_lossy().to_lowercase()))
}

#[cfg(all(not(unix), not(windows)))]
fn path_identity_from_metadata(path: &Path, _metadata: &fs::Metadata) -> PathIdentity {
	PathIdentity::Path(path.to_path_buf())
}

#[cfg(windows)]
fn quiescence_key(path: &Path) -> PathBuf {
	PathBuf::from(path.to_string_lossy().to_lowercase())
}

#[cfg(not(windows))]
fn quiescence_key(path: &Path) -> PathBuf {
	path.to_path_buf()
}

fn next_handle() -> Result<u32> {
	let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
	if handle == 0 {
		Err(FulltextError::new("E_NATIVE_FAILURE", "native handle space exhausted"))
	} else {
		Ok(handle)
	}
}

fn success_envelope(body: Vec<u8>) -> Vec<u8> {
	let mut bytes = b"FTRP".to_vec();
	bytes.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
	bytes.push(0);
	bytes.extend_from_slice(&body);
	bytes
}

fn error_envelope(error: FulltextError) -> Vec<u8> {
	let mut bytes = b"FTRP".to_vec();
	bytes.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
	bytes.push(1);
	push_string(&mut bytes, error.code);
	push_string(&mut bytes, &error.message);
	bytes
}

fn u32_body(value: u32) -> Vec<u8> {
	value.to_le_bytes().to_vec()
}

fn open_body(handle: u32, payload: Option<&str>) -> Vec<u8> {
	let mut bytes = u32_body(handle);
	bytes.push(u8::from(payload.is_some()));
	if let Some(payload) = payload {
		push_string(&mut bytes, payload);
	}
	bytes
}

fn inspection_body(result: InspectionResult) -> Vec<u8> {
	match result {
		InspectionResult::Missing => vec![0],
		InspectionResult::Cursorless => vec![1],
		InspectionResult::Payload(payload) => {
			let mut bytes = vec![2];
			push_string(&mut bytes, &payload);
			bytes
		}
	}
}

fn reset_body(result: ResetResult) -> Vec<u8> {
	match result {
		ResetResult::Missing => vec![0],
		ResetResult::Reset(path) => {
			let mut bytes = vec![1];
			push_string(&mut bytes, &path);
			bytes
		}
	}
}

fn u64_body(value: u64) -> Vec<u8> {
	value.to_le_bytes().to_vec()
}

fn search_body(result: SearchResult) -> Vec<u8> {
	let mut bytes = Vec::new();
	push_u64(&mut bytes, result.total);
	bytes.push(match result.total_relation {
		TotalRelation::Exact => 0,
		TotalRelation::LowerBound => 1,
	});
	bytes.extend_from_slice(&(result.hits.len() as u32).to_le_bytes());
	for hit in result.hits {
		bytes.extend_from_slice(&hit.score.to_le_bytes());
		push_string(&mut bytes, &hit.id);
	}
	bytes
}

fn trace_body(result: TraceResult) -> Vec<u8> {
	let mut bytes = Vec::new();
	bytes.push(result.complete as u8);
	bytes.extend_from_slice(&(result.records.len() as u16).to_le_bytes());
	for record in result.records {
		push_string(&mut bytes, &record.id);
		bytes.extend_from_slice(&(record.values.len() as u16).to_le_bytes());
		for value in record.values {
			push_string(&mut bytes, &value.field);
			bytes.extend_from_slice(&value.value_index.to_le_bytes());
			bytes.extend_from_slice(&(value.spans.len() as u16).to_le_bytes());
			for span in value.spans {
				bytes.extend_from_slice(&span.start.to_le_bytes());
				bytes.extend_from_slice(&span.end.to_le_bytes());
			}
		}
	}
	bytes
}

fn push_string(bytes: &mut Vec<u8>, value: &str) {
	bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
	bytes.extend_from_slice(value.as_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
	bytes.extend_from_slice(&value.to_le_bytes());
}

fn duration_ns(duration: std::time::Duration) -> u64 {
	u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
	mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn fulltext_napi_error(error: FulltextError) -> napi::Error<&'static str> {
	napi::Error::new(error.code, error.message)
}

fn napi_error(code: &'static str, error: impl std::fmt::Display) -> napi::Error<&'static str> {
	napi::Error::new(code, error.to_string())
}

fn storage_error(error: impl std::fmt::Display) -> FulltextError {
	FulltextError::new("E_STORAGE", error.to_string())
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::mpsc;
	use tantivy::directory::error::LockError;

	#[test]
	fn lifecycle_lock_excludes_other_directory_handles() {
		let first = MmapDirectory::create_from_tempdir().unwrap();
		let second = first.clone();
		let lifecycle_lock = Lock {
			filepath: PathBuf::from("index"),
			is_blocking: false,
		};
		let guard = first.acquire_lock(&lifecycle_lock).unwrap();
		assert!(matches!(second.acquire_lock(&lifecycle_lock), Err(LockError::LockBusy)));
		drop(guard);
		assert!(second.acquire_lock(&lifecycle_lock).is_ok());
	}

	#[test]
	fn process_budget_rolls_back_partial_admission_and_conserves_counters() {
		let budget = Arc::new(RuntimeBudget::new(RuntimeBudgetLimits {
			max_resident_indexes: 2,
			max_indexing_threads: 1,
			max_search_threads: 2,
			max_writer_memory_bytes: 15_000_000,
			max_queued_bytes: 32,
			max_expensive_searches: 1,
		}));
		let limits = crate::protocol::Limits {
			indexing_threads: 1,
			search_threads: 2,
			writer_memory_bytes: 15_000_000,
			max_queued_commands: 8,
			max_queued_bytes: 16,
			max_batch_bytes: 16,
		};
		let reservation = budget.admit(&limits).unwrap();
		assert_eq!(budget.admit(&limits).err().unwrap().code, "E_RESOURCE_LIMIT");
		assert_eq!(budget.resident_indexes.load(Ordering::Acquire), 1);
		let state = AtomicU8::new(STATE_OPEN);
		let permit = budget
			.acquire_expensive(Instant::now() + Duration::from_secs(1), &state)
			.unwrap();
		drop(permit);
		drop(reservation);
		assert_eq!(budget.resident_indexes.load(Ordering::Acquire), 0);
		assert_eq!(budget.indexing_threads.load(Ordering::Acquire), 0);
		assert_eq!(budget.search_threads.load(Ordering::Acquire), 0);
		assert_eq!(budget.writer_memory_bytes.load(Ordering::Acquire), 0);
		assert_eq!(budget.queued_bytes.load(Ordering::Acquire), 0);
		assert_eq!(*lock(&budget.expensive_searches), 0);
	}

	#[test]
	fn unproven_pending_open_blocks_later_runtime_configuration() {
		let state = Mutex::new(RuntimeBudgetState::Unconfigured { pending_opens: 1 });
		RuntimeAdmission {
			kind: RuntimeAdmissionKind::Unbudgeted { pending: true },
		}
		.retain_until_restart_in(&state);
		assert!(matches!(*lock(&state), RuntimeBudgetState::OpenedWithoutBudget));
	}

	#[test]
	fn pending_open_reports_retryable_runtime_configuration_error() {
		let state = Mutex::new(RuntimeBudgetState::Unconfigured { pending_opens: 1 });
		let error = configure_runtime_budget_in(
			&state,
			RuntimeBudgetLimits {
				max_resident_indexes: 1,
				max_indexing_threads: 1,
				max_search_threads: 1,
				max_writer_memory_bytes: 15_000_000,
				max_queued_bytes: 32,
				max_expensive_searches: 1,
			},
		)
		.unwrap_err();
		assert_eq!(error.code, "E_LOCK_BUSY");
		assert!(matches!(
			*lock(&state),
			RuntimeBudgetState::Unconfigured { pending_opens: 1 }
		));
	}

	#[test]
	fn expensive_search_budget_applies_backpressure() {
		let budget = Arc::new(RuntimeBudget::new(RuntimeBudgetLimits {
			max_resident_indexes: 1,
			max_indexing_threads: 1,
			max_search_threads: 2,
			max_writer_memory_bytes: 15_000_000,
			max_queued_bytes: 32,
			max_expensive_searches: 1,
		}));
		let state = Arc::new(AtomicU8::new(STATE_OPEN));
		let first = budget
			.acquire_expensive(Instant::now() + Duration::from_secs(1), &state)
			.unwrap();
		let waiting_budget = budget.clone();
		let waiting_state = state.clone();
		let (started_sender, started_receiver) = mpsc::channel();
		let (acquired_sender, acquired_receiver) = mpsc::channel();
		let waiting = thread::spawn(move || {
			started_sender.send(()).unwrap();
			let _permit = waiting_budget
				.acquire_expensive(Instant::now() + Duration::from_secs(1), &waiting_state)
				.unwrap();
			acquired_sender.send(()).unwrap();
		});
		started_receiver.recv().unwrap();
		assert!(acquired_receiver.recv_timeout(Duration::from_millis(20)).is_err());
		drop(first);
		acquired_receiver.recv_timeout(Duration::from_secs(1)).unwrap();
		waiting.join().unwrap();
	}

	#[test]
	fn expensive_search_budget_respects_deadline_and_close() {
		let budget = Arc::new(RuntimeBudget::new(RuntimeBudgetLimits {
			max_resident_indexes: 1,
			max_indexing_threads: 1,
			max_search_threads: 2,
			max_writer_memory_bytes: 15_000_000,
			max_queued_bytes: 32,
			max_expensive_searches: 1,
		}));
		let state = AtomicU8::new(STATE_OPEN);
		let permit = budget
			.acquire_expensive(Instant::now() + Duration::from_secs(1), &state)
			.unwrap();
		assert_eq!(
			budget
				.acquire_expensive(Instant::now() + Duration::from_millis(1), &state)
				.err()
				.unwrap()
				.code,
			"E_TIMEOUT"
		);
		let state = Arc::new(state);
		let waiting_state = state.clone();
		let waiting_budget = budget.clone();
		let (started_sender, started_receiver) = mpsc::channel();
		let (result_sender, result_receiver) = mpsc::channel();
		let waiting = thread::spawn(move || {
			started_sender.send(()).unwrap();
			let code = waiting_budget
				.acquire_expensive(Instant::now() + Duration::from_secs(1), &waiting_state)
				.err()
				.map(|error| error.code);
			result_sender.send(code).unwrap();
		});
		started_receiver.recv().unwrap();
		assert!(result_receiver.recv_timeout(Duration::from_millis(20)).is_err());
		state.store(STATE_CLOSING, Ordering::Release);
		let active = lock(&budget.expensive_searches);
		budget.expensive_search_ready.notify_all();
		drop(active);
		assert_eq!(
			result_receiver.recv_timeout(Duration::from_secs(1)).unwrap(),
			Some("E_CLOSED")
		);
		waiting.join().unwrap();
		drop(permit);
	}

	#[test]
	fn closing_waiter_does_not_consume_a_released_expensive_search_permit() {
		let budget = Arc::new(RuntimeBudget::new(RuntimeBudgetLimits {
			max_resident_indexes: 1,
			max_indexing_threads: 1,
			max_search_threads: 3,
			max_writer_memory_bytes: 15_000_000,
			max_queued_bytes: 32,
			max_expensive_searches: 1,
		}));
		let holder_state = AtomicU8::new(STATE_OPEN);
		let holder = budget
			.acquire_expensive(Instant::now() + Duration::from_secs(1), &holder_state)
			.unwrap();
		let closing_state = Arc::new(AtomicU8::new(STATE_OPEN));
		let open_state = Arc::new(AtomicU8::new(STATE_OPEN));
		let (started_sender, started_receiver) = mpsc::channel();
		let (result_sender, result_receiver) = mpsc::channel();
		let mut waiters = Vec::new();
		for (state, name) in [(closing_state.clone(), "closing"), (open_state.clone(), "open")] {
			let waiting_budget = budget.clone();
			let started_sender = started_sender.clone();
			let result_sender = result_sender.clone();
			waiters.push(thread::spawn(move || {
				started_sender.send(()).unwrap();
				let result = waiting_budget.acquire_expensive(Instant::now() + Duration::from_secs(1), &state);
				result_sender
					.send((name, result.as_ref().err().map(|error| error.code)))
					.unwrap();
			}));
		}
		started_receiver.recv().unwrap();
		started_receiver.recv().unwrap();
		assert!(result_receiver.recv_timeout(Duration::from_millis(20)).is_err());
		closing_state.store(STATE_CLOSING, Ordering::Release);
		drop(holder);
		let mut results = [result_receiver.recv_timeout(Duration::from_secs(1)).unwrap(); 2];
		results[1] = result_receiver.recv_timeout(Duration::from_secs(1)).unwrap();
		results.sort_by_key(|(name, _)| *name);
		assert_eq!(results, [("closing", Some("E_CLOSED")), ("open", None)]);
		for waiter in waiters {
			waiter.join().unwrap();
		}
	}

	#[test]
	fn admission_validator_runs_under_the_queue_mutex() {
		let queue = BoundedQueue::new(1, 8);
		queue
			.try_push_if(1, 8, || {
				assert!(queue.state.try_lock().is_err());
				Ok(())
			})
			.unwrap();
		assert_eq!(queue.queued_commands.load(Ordering::Relaxed), 1);
		assert_eq!(queue.queued_bytes.load(Ordering::Relaxed), 8);
		assert_eq!(queue.try_push_if(2, 1, || Ok(())).unwrap_err().code, "E_QUEUE_FULL");
		assert_eq!(queue.pop().unwrap().value, 1);
		assert_eq!(queue.queued_bytes.load(Ordering::Relaxed), 0);
	}

	#[test]
	fn isolated_search_lanes_share_one_admission_budget() {
		let budget = Arc::new(QueueBudget::new(2, 8));
		let ordinary = BoundedQueue::new_with_budget_and_wake(2, 8, Some(budget.clone()), None);
		let expensive = BoundedQueue::new_with_budget_and_wake(2, 8, Some(budget), None);
		ordinary.try_push(1, 4).unwrap();
		expensive.try_push(2, 4).unwrap();
		assert_eq!(ordinary.try_push(3, 1).unwrap_err().code, "E_QUEUE_FULL");
		assert_eq!(ordinary.pop().unwrap().value, 1);
		ordinary.try_push(3, 4).unwrap();
		assert_eq!(expensive.pop().unwrap().value, 2);
		assert_eq!(ordinary.pop().unwrap().value, 3);
	}

	#[test]
	fn forced_admission_rejects_a_shared_budget_queue() {
		let budget = Arc::new(QueueBudget::new(1, 8));
		let queue = BoundedQueue::new_with_budget_and_wake(1, 8, Some(budget), None);
		assert_eq!(queue.push_force(1, 0).unwrap_err().code, "E_NATIVE_FAILURE");
		assert_eq!(queue.queued_commands.load(Ordering::Relaxed), 0);
	}

	#[test]
	fn shared_search_wake_observes_both_lanes() {
		let wake = Arc::new(QueueWake::new());
		let ordinary = BoundedQueue::new_with_budget_and_wake(1, 8, None, Some(wake.clone()));
		let expensive = BoundedQueue::new_with_budget_and_wake(1, 8, None, Some(wake.clone()));

		let observed = wake.snapshot();
		ordinary.try_push(1, 1).unwrap();
		wake.wait_for_change(observed);
		assert_eq!(ordinary.pop().unwrap().value, 1);

		let observed = wake.snapshot();
		expensive.try_push(2, 1).unwrap();
		wake.wait_for_change(observed);
		assert_eq!(expensive.pop().unwrap().value, 2);
	}

	#[test]
	fn queued_time_consumes_the_search_budget() {
		assert_eq!(
			operation_deadline(1, Duration::from_millis(2)).unwrap_err().code,
			"E_TIMEOUT"
		);
	}

	#[test]
	fn terminal_admission_cannot_follow_a_completed_drain() {
		let queue = Arc::new(BoundedQueue::new(1, 8));
		let open = Arc::new(AtomicBool::new(true));
		queue.try_push(1, 8).unwrap();
		let (checked, after_check) = mpsc::channel();
		let (resume, after_drain) = mpsc::channel();
		let producer_queue = queue.clone();
		let producer_open = open.clone();
		let producer = thread::spawn(move || {
			assert!(producer_open.load(Ordering::Acquire));
			checked.send(()).unwrap();
			after_drain.recv_timeout(Duration::from_secs(5)).unwrap();
			producer_queue.try_push_if(2, 8, || {
				if producer_open.load(Ordering::Acquire) {
					Ok(())
				} else {
					Err(FulltextError::new("E_POISONED", "terminal generation"))
				}
			})
		});
		after_check.recv_timeout(Duration::from_secs(5)).unwrap();
		open.store(false, Ordering::Release);
		assert_eq!(queue.drain().len(), 1);
		resume.send(()).unwrap();
		assert_eq!(producer.join().unwrap().unwrap_err().code, "E_POISONED");
		assert_eq!(queue.queued_commands.load(Ordering::Relaxed), 0);
		assert_eq!(queue.queued_bytes.load(Ordering::Relaxed), 0);
		queue.push_force(3, 0).unwrap();
		assert_eq!(queue.pop().unwrap().value, 3);
	}

	#[test]
	fn close_control_bypasses_capacity_but_not_a_closed_queue() {
		let queue = BoundedQueue::new(1, 8);
		queue.try_push(1, 8).unwrap();
		queue.push_force(2, 0).unwrap();
		assert_eq!(queue.pop().unwrap().value, 1);
		assert_eq!(queue.pop().unwrap().value, 2);
		queue.close();
		assert_eq!(queue.try_push(3, 0).unwrap_err().code, "E_CLOSED");
		assert_eq!(queue.push_force(3, 0).unwrap_err().code, "E_CLOSED");
	}
}
