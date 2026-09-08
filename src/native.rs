use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::c_void;
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use napi::bindgen_prelude::Buffer;
use napi::threadsafe_function::{ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Env, JsFunction};
use napi_derive::napi;
use tantivy::directory::MmapDirectory;
use tantivy::IndexReader;

use crate::boundary;
use crate::engine::{Engine, SearchResult, TotalRelation, Writer};
use crate::error::{FulltextError, Result};
use crate::protocol::{
	decode_batch, decode_open, decode_search, validate_batch_header, validate_search_header, EngineConfig,
};

const STATE_OPEN: u8 = 0;
const STATE_CLOSING: u8 = 1;
const STATE_CLOSED: u8 = 2;
const STATE_POISONED: u8 = 3;
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(30);

static NEXT_HANDLE: AtomicU32 = AtomicU32::new(1);
static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();

#[derive(Default)]
struct Registry {
	handles: HashMap<u32, Arc<Runtime>>,
	paths: HashMap<PathIdentity, u32>,
	opening: HashSet<u32>,
	cancelled: HashSet<u32>,
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
	path_identity: PathIdentity,
	config: EngineConfig,
	engine: Arc<Engine>,
	reader: Arc<IndexReader>,
	writer_queue: Arc<BoundedQueue<WriterCommand>>,
	search_queue: Arc<BoundedQueue<SearchCommand>>,
	state: AtomicU8,
	env_alive: Arc<AtomicBool>,
	uncommitted_mutations: AtomicU64,
	commit_opstamp: AtomicU64,
	writer_queue_nanoseconds: AtomicU64,
	writer_execution_nanoseconds: AtomicU64,
	search_queue_nanoseconds: AtomicU64,
	search_execution_nanoseconds: AtomicU64,
	search_threads: Mutex<Vec<thread::JoinHandle<()>>>,
	closed: Arc<CompletionSignal>,
}

struct CompletionSignal {
	done: Mutex<bool>,
	ready: Condvar,
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
}

type Callback = ThreadsafeFunction<Vec<u8>, ErrorStrategy::Fatal>;

struct Completion {
	callback: Option<Callback>,
	env_alive: Arc<AtomicBool>,
}

struct WriterCommand {
	operation: WriterOperation,
	completion: Completion,
}

enum WriterOperation {
	Apply(Vec<u8>),
	Commit,
	Reload,
	Close { rollback: bool },
}

enum WriterOutcome {
	Continue(Result<Vec<u8>>),
	Stop(Result<Vec<u8>>),
	Poison(Result<Vec<u8>>, FulltextError),
}

struct SearchCommand {
	request: Vec<u8>,
	completion: Completion,
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeOpen")]
pub fn native_open(env: Env, packed_config: Buffer, callback: JsFunction) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let env_alive = Arc::new(AtomicBool::new(true));
		let opening_done = Arc::new(CompletionSignal::new());
		let completion = completion(callback, env_alive.clone())?;
		let handle = next_handle().map_err(fulltext_napi_error)?;
		registry().opening.insert(handle);
		if let Err(error) = register_async_cleanup(&env, handle, env_alive, opening_done.clone()) {
			registry().opening.remove(&handle);
			return Err(error);
		}
		let bytes = packed_config.to_vec();
		let thread_opening_done = opening_done.clone();
		if let Err(error) = thread::Builder::new()
			.name(format!("fulltext-open-{handle}"))
			.spawn(move || open_on_thread(handle, bytes, completion, thread_opening_done))
		{
			registry().opening.remove(&handle);
			opening_done.signal();
			return Err(napi_error("E_NATIVE_FAILURE", error));
		}
		Ok(())
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeApply")]
pub fn native_apply(handle: u32, packed_batch: Buffer, callback: JsFunction) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let runtime = runtime(handle)?;
		validate_batch_header(&packed_batch, runtime.config.limits.max_batch_bytes).map_err(fulltext_napi_error)?;
		runtime
			.writer_queue
			.check_capacity(packed_batch.len())
			.map_err(fulltext_napi_error)?;
		let completion = completion(callback, runtime.env_alive.clone())?;
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
pub fn native_commit(handle: u32, callback: JsFunction) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let runtime = runtime(handle)?;
		let completion = completion(callback, runtime.env_alive.clone())?;
		runtime.enqueue_writer(
			WriterCommand {
				operation: WriterOperation::Commit,
				completion,
			},
			0,
		)
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeReload")]
pub fn native_reload(handle: u32, callback: JsFunction) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let runtime = runtime(handle)?;
		let completion = completion(callback, runtime.env_alive.clone())?;
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
pub fn native_search(handle: u32, packed_request: Buffer, callback: JsFunction) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let runtime = runtime(handle)?;
		runtime.require_open()?;
		validate_search_header(&packed_request).map_err(fulltext_napi_error)?;
		runtime
			.search_queue
			.check_capacity(packed_request.len())
			.map_err(fulltext_napi_error)?;
		let completion = completion(callback, runtime.env_alive.clone())?;
		let request = packed_request.to_vec();
		runtime
			.search_queue
			.try_push(SearchCommand { request, completion }, packed_request.len())
			.map_err(fulltext_napi_error)
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeClose")]
pub fn native_close(handle: u32, rollback: bool, callback: JsFunction) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let runtime = runtime(handle)?;
		let completion = completion(callback, runtime.env_alive.clone())?;
		match runtime
			.state
			.compare_exchange(STATE_OPEN, STATE_CLOSING, Ordering::AcqRel, Ordering::Acquire)
		{
			Ok(_) => runtime
				.writer_queue
				.push_force(
					WriterCommand {
						operation: WriterOperation::Close { rollback },
						completion,
					},
					0,
				)
				.map_err(fulltext_napi_error),
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
pub fn test_poison_native_handle(handle: u32) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let runtime = runtime(handle)?;
		runtime.poison(FulltextError::new("E_POISONED", "test poison"));
		Ok(())
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__nativeStatus")]
pub fn native_status(handle: u32) -> boundary::Result<Buffer> {
	boundary::run_stateless(|| {
		let runtime = runtime(handle)?;
		Ok(Buffer::from(success_envelope(runtime.status_bytes())))
	})?
}

impl Runtime {
	fn start(
		handle: u32,
		path_identity: PathIdentity,
		config: EngineConfig,
		engine: Engine,
		writer: Writer,
		reader: IndexReader,
		env_alive: Arc<AtomicBool>,
	) -> Result<Arc<Self>> {
		let search_thread_count = config.limits.search_threads;
		let writer_queue = Arc::new(BoundedQueue::new(
			config.limits.max_queued_commands,
			config.limits.max_queued_bytes,
		));
		let search_queue = Arc::new(BoundedQueue::new(
			config.limits.max_queued_commands,
			config.limits.max_queued_bytes,
		));
		let runtime = Arc::new(Self {
			handle,
			path_identity,
			config,
			engine: Arc::new(engine),
			reader: Arc::new(reader),
			writer_queue,
			search_queue,
			state: AtomicU8::new(STATE_OPEN),
			env_alive,
			uncommitted_mutations: AtomicU64::new(0),
			commit_opstamp: AtomicU64::new(0),
			writer_queue_nanoseconds: AtomicU64::new(0),
			writer_execution_nanoseconds: AtomicU64::new(0),
			search_queue_nanoseconds: AtomicU64::new(0),
			search_execution_nanoseconds: AtomicU64::new(0),
			search_threads: Mutex::new(Vec::with_capacity(search_thread_count)),
			closed: Arc::new(CompletionSignal::new()),
		});
		let writer_runtime = runtime.clone();
		thread::Builder::new()
			.name(format!("fulltext-writer-{handle}"))
			.spawn(move || writer_loop(writer_runtime, writer))
			.map_err(FulltextError::native)?;
		for worker in 0..search_thread_count {
			let search_runtime = runtime.clone();
			let join = match thread::Builder::new()
				.name(format!("fulltext-search-{handle}-{worker}"))
				.spawn(move || search_loop(search_runtime))
			{
				Ok(join) => join,
				Err(error) => {
					runtime.writer_queue.close();
					runtime.search_queue.close();
					for join in std::mem::take(&mut *lock(&runtime.search_threads)) {
						let _ = join.join();
					}
					return Err(FulltextError::native(error));
				}
			};
			lock(&runtime.search_threads).push(join);
		}
		Ok(runtime)
	}

	fn require_open(&self) -> boundary::Result<()> {
		match self.state.load(Ordering::Acquire) {
			STATE_OPEN => Ok(()),
			STATE_POISONED => Err(fulltext_napi_error(FulltextError::new(
				"E_POISONED",
				"index is poisoned",
			))),
			_ => Err(fulltext_napi_error(FulltextError::new(
				"E_CLOSED",
				"index is closing or closed",
			))),
		}
	}

	fn enqueue_writer(&self, command: WriterCommand, bytes: usize) -> boundary::Result<()> {
		self.require_open()?;
		self.writer_queue.try_push(command, bytes).map_err(fulltext_napi_error)
	}

	fn force_close(&self) {
		let previous = self.state.swap(STATE_CLOSING, Ordering::AcqRel);
		if previous == STATE_CLOSED || previous == STATE_CLOSING {
			return;
		}
		let _ = self.writer_queue.push_force(
			WriterCommand {
				operation: WriterOperation::Close { rollback: true },
				completion: Completion {
					callback: None,
					env_alive: self.env_alive.clone(),
				},
			},
			0,
		);
	}

	fn poison(&self, error: FulltextError) {
		self.state.store(STATE_POISONED, Ordering::Release);
		for command in self.writer_queue.drain() {
			command.value.fail(error.clone());
		}
		for command in self.search_queue.close() {
			command.value.completion.failure(error.clone());
		}
	}

	fn status_bytes(&self) -> Vec<u8> {
		let mut bytes = Vec::with_capacity(80);
		bytes.push(self.state.load(Ordering::Acquire));
		push_u64(&mut bytes, self.uncommitted_mutations.load(Ordering::Acquire));
		push_u64(&mut bytes, self.writer_queue.queued_commands.load(Ordering::Relaxed));
		push_u64(&mut bytes, self.writer_queue.queued_bytes.load(Ordering::Relaxed));
		push_u64(&mut bytes, self.search_queue.queued_commands.load(Ordering::Relaxed));
		push_u64(&mut bytes, self.search_queue.queued_bytes.load(Ordering::Relaxed));
		push_u64(&mut bytes, self.commit_opstamp.load(Ordering::Acquire));
		push_u64(&mut bytes, self.writer_queue_nanoseconds.load(Ordering::Relaxed));
		push_u64(&mut bytes, self.writer_execution_nanoseconds.load(Ordering::Relaxed));
		push_u64(&mut bytes, self.search_queue_nanoseconds.load(Ordering::Relaxed));
		push_u64(&mut bytes, self.search_execution_nanoseconds.load(Ordering::Relaxed));
		bytes
	}

	fn signal_closed(&self) {
		self.closed.signal();
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

	fn is_done(&self) -> bool {
		*lock(&self.done)
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
		}
	}

	fn try_push(&self, value: T, bytes: usize) -> Result<()> {
		let mut state = lock(&self.state);
		self.validate_capacity(&state, bytes)?;
		state.bytes += bytes;
		state.items.push_back(Queued {
			value,
			bytes,
			enqueued: Instant::now(),
		});
		self.queued_commands.fetch_add(1, Ordering::Relaxed);
		self.queued_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
		self.ready.notify_one();
		Ok(())
	}

	fn check_capacity(&self, bytes: usize) -> Result<()> {
		self.validate_capacity(&lock(&self.state), bytes)
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
		Ok(())
	}

	fn pop(&self) -> Option<Queued<T>> {
		let mut state = lock(&self.state);
		loop {
			if let Some(item) = state.items.pop_front() {
				state.bytes -= item.bytes;
				self.queued_commands.fetch_sub(1, Ordering::Relaxed);
				self.queued_bytes.fetch_sub(item.bytes as u64, Ordering::Relaxed);
				return Some(item);
			}
			if state.closed {
				return None;
			}
			state = self.ready.wait(state).unwrap_or_else(|error| error.into_inner());
		}
	}

	fn close(&self) -> Vec<Queued<T>> {
		let mut state = lock(&self.state);
		state.closed = true;
		let items = drain_queue(&mut state, &self.queued_commands, &self.queued_bytes);
		self.ready.notify_all();
		items
	}

	fn drain(&self) -> Vec<Queued<T>> {
		let mut state = lock(&self.state);
		drain_queue(&mut state, &self.queued_commands, &self.queued_bytes)
	}

	fn shutdown_after_drain(&self) {
		let mut state = lock(&self.state);
		state.closed = true;
		self.ready.notify_all();
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
		if self.env_alive.load(Ordering::Acquire) {
			if let Some(callback) = self.callback.take() {
				let _ = callback.call(bytes, ThreadsafeFunctionCallMode::NonBlocking);
			}
		} else {
			self.callback.take();
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
}

fn writer_loop(runtime: Arc<Runtime>, writer: Writer) {
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
			WriterOperation::Commit => match active_writer_mut(&mut writer).and_then(Writer::commit) {
				Ok(opstamp) => {
					runtime.uncommitted_mutations.store(0, Ordering::Release);
					runtime.commit_opstamp.store(opstamp, Ordering::Release);
					WriterOutcome::Continue(Ok(u64_body(opstamp)))
				}
				Err(error) => WriterOutcome::Poison(
					Err(error),
					FulltextError::new(
						"E_POISONED",
						"a prior commit failed and the index generation is terminal",
					),
				),
			},
			WriterOperation::Reload => WriterOutcome::Continue(
				runtime
					.reader
					.reload()
					.map(|()| Vec::new())
					.map_err(FulltextError::native),
			),
			WriterOperation::Close { rollback } => {
				if !rollback && runtime.uncommitted_mutations.load(Ordering::Acquire) > 0 {
					runtime.state.store(STATE_OPEN, Ordering::Release);
					WriterOutcome::Continue(Err(FulltextError::new(
						"E_DIRTY_CLOSE",
						"index has uncommitted mutations; commit or close with rollback",
					)))
				} else {
					let close_result = writer
						.take()
						.ok_or_else(|| FulltextError::new("E_POISONED", "writer is unavailable"))
						.and_then(|mut owned_writer| {
							if rollback {
								owned_writer.rollback()?;
							}
							owned_writer.close()
						});
					runtime.search_queue.shutdown_after_drain();
					for join in std::mem::take(&mut *lock(&runtime.search_threads)) {
						let _ = join.join();
					}
					runtime.writer_queue.close();
					runtime.state.store(STATE_CLOSED, Ordering::Release);
					release_runtime(runtime.handle, &runtime.path_identity);
					runtime.signal_closed();
					WriterOutcome::Stop(close_result.map(|()| Vec::new()))
				}
			}
		}));
		runtime
			.writer_execution_nanoseconds
			.fetch_add(duration_ns(started.elapsed()), Ordering::Relaxed);
		match outcome {
			Ok(WriterOutcome::Continue(result)) => settle(completion, result),
			Ok(WriterOutcome::Stop(result)) => {
				settle(completion, result);
				return;
			}
			Ok(WriterOutcome::Poison(result, poison)) => {
				settle(completion, result);
				runtime.poison(poison);
			}
			Err(_) => {
				completion.failure(FulltextError::new("E_NATIVE_PANIC", "native writer actor panicked"));
				runtime.poison(FulltextError::new("E_NATIVE_PANIC", "native writer actor panicked"));
				runtime.writer_queue.close();
				release_runtime(runtime.handle, &runtime.path_identity);
				runtime.signal_closed();
				return;
			}
		}
	}
	runtime.state.store(STATE_CLOSED, Ordering::Release);
	release_runtime(runtime.handle, &runtime.path_identity);
	runtime.signal_closed();
}

fn settle(completion: Completion, result: Result<Vec<u8>>) {
	match result {
		Ok(body) => completion.success(body),
		Err(error) => completion.failure(error),
	}
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

fn search_loop(runtime: Arc<Runtime>) {
	while let Some(queued) = runtime.search_queue.pop() {
		runtime
			.search_queue_nanoseconds
			.fetch_add(duration_ns(queued.enqueued.elapsed()), Ordering::Relaxed);
		let started = Instant::now();
		let result = catch_unwind(AssertUnwindSafe(|| {
			decode_search(&queued.value.request)
				.and_then(|request| runtime.engine.search(&runtime.reader.searcher(), &request))
		}));
		runtime
			.search_execution_nanoseconds
			.fetch_add(duration_ns(started.elapsed()), Ordering::Relaxed);
		match result {
			Ok(Ok(result)) => queued.value.completion.success(search_body(result)),
			Ok(Err(error)) => queued.value.completion.failure(error),
			Err(_) => {
				queued
					.value
					.completion
					.failure(FulltextError::new("E_NATIVE_PANIC", "native search actor panicked"));
				runtime.poison(FulltextError::new("E_NATIVE_PANIC", "native search actor panicked"));
				return;
			}
		}
	}
}

fn open_on_thread(handle: u32, bytes: Vec<u8>, completion: Completion, opening_done: Arc<CompletionSignal>) {
	let env_alive = completion.env_alive.clone();
	let result = catch_unwind(AssertUnwindSafe(|| open_runtime(handle, bytes, env_alive)));
	match result {
		Ok(Ok(())) => completion.success(u32_body(handle)),
		Ok(Err(error)) => completion.failure(error),
		Err(_) => completion.failure(FulltextError::new("E_NATIVE_PANIC", "native index open panicked")),
	}
	registry().opening.remove(&handle);
	opening_done.signal();
}

fn open_runtime(handle: u32, bytes: Vec<u8>, env_alive: Arc<AtomicBool>) -> Result<()> {
	let config = decode_open(&bytes)?;
	let canonical = create_and_canonicalize(Path::new(&config.path))?;
	let path_identity = path_identity(&canonical)?;
	{
		let mut registry = registry();
		if registry.cancelled.remove(&handle) || !env_alive.load(Ordering::Acquire) {
			registry.opening.remove(&handle);
			return Err(FulltextError::new(
				"E_CLOSED",
				"Node environment closed during index open",
			));
		}
		if registry.paths.contains_key(&path_identity) {
			return Err(FulltextError::new(
				"E_DUPLICATE_OPEN",
				"the physical index is already open",
			));
		}
		registry.paths.insert(path_identity.clone(), handle);
	}
	let result = (|| {
		let directory = MmapDirectory::open(&canonical).map_err(storage_error)?;
		let engine = Engine::open(directory, &config)?;
		let writer = engine.writer(&config)?;
		let reader = engine.reader()?;
		let runtime = Runtime::start(
			handle,
			path_identity.clone(),
			config,
			engine,
			writer,
			reader,
			env_alive.clone(),
		)?;
		let mut registry = registry();
		if registry.cancelled.remove(&handle) || !env_alive.load(Ordering::Acquire) {
			drop(registry);
			runtime.force_close();
			let _ = runtime.wait_closed(CLEANUP_TIMEOUT);
			return Err(FulltextError::new(
				"E_CLOSED",
				"Node environment closed during index open",
			));
		}
		registry.handles.insert(handle, runtime);
		registry.opening.remove(&handle);
		Ok(())
	})();
	if result.is_err() {
		release_runtime(handle, &path_identity);
	}
	result
}

fn completion(callback: JsFunction, env_alive: Arc<AtomicBool>) -> boundary::Result<Completion> {
	let callback = callback
		.create_threadsafe_function::<Vec<u8>, Buffer, _, ErrorStrategy::Fatal>(
			0,
			|context: ThreadSafeCallContext<Vec<u8>>| Ok(vec![Buffer::from(context.value)]),
		)
		.map_err(|error| napi_error("E_NATIVE_FAILURE", error))?;
	Ok(Completion {
		callback: Some(callback),
		env_alive,
	})
}

fn runtime(handle: u32) -> boundary::Result<Arc<Runtime>> {
	registry()
		.handles
		.get(&handle)
		.cloned()
		.ok_or_else(|| napi_error("E_CLOSED", "unknown or closed fulltext index handle"))
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

struct CleanupHookData {
	handle: u32,
	env_alive: Arc<AtomicBool>,
	opening_done: Arc<CompletionSignal>,
}

struct CleanupHookGuard {
	raw_handle: usize,
}

enum CleanupWait {
	Runtime(Arc<Runtime>),
	Opening(Arc<CompletionSignal>),
}

fn register_async_cleanup(
	env: &Env,
	handle: u32,
	env_alive: Arc<AtomicBool>,
	opening_done: Arc<CompletionSignal>,
) -> boundary::Result<()> {
	let data = Box::into_raw(Box::new(CleanupHookData {
		handle,
		env_alive,
		opening_done,
	}));
	let mut cleanup_handle = ptr::null_mut();
	// Safety: `data` remains owned by the registered one-shot hook, and Node writes the handle to the supplied pointer.
	let status = unsafe {
		napi::sys::napi_add_async_cleanup_hook(
			env.raw(),
			Some(async_cleanup),
			data.cast::<c_void>(),
			&mut cleanup_handle,
		)
	};
	if status == napi::sys::Status::napi_ok {
		Ok(())
	} else {
		// Safety: registration failed, so Node did not take ownership of `data`.
		drop(unsafe { Box::from_raw(data) });
		Err(napi_error("E_NATIVE_FAILURE", napi::Status::from(status)))
	}
}

unsafe extern "C" fn async_cleanup(handle: napi::sys::napi_async_cleanup_hook_handle, data: *mut c_void) {
	let guard = CleanupHookGuard {
		raw_handle: handle as usize,
	};
	let _ = catch_unwind(AssertUnwindSafe(|| async_cleanup_inner(guard, data)));
}

fn async_cleanup_inner(guard: CleanupHookGuard, data: *mut c_void) {
	// Safety: `data` was allocated by `register_async_cleanup` for this one-shot callback.
	let data = unsafe { Box::from_raw(data.cast::<CleanupHookData>()) };
	data.env_alive.store(false, Ordering::Release);
	let wait = match cleanup_handle(data.handle) {
		Some(runtime) => CleanupWait::Runtime(runtime),
		None => CleanupWait::Opening(data.opening_done.clone()),
	};
	if wait.is_done() {
		return;
	}
	let _ = thread::Builder::new()
		.name(format!("fulltext-cleanup-{}", data.handle))
		.spawn(move || finish_async_cleanup(guard, wait));
}

impl CleanupWait {
	fn is_done(&self) -> bool {
		match self {
			Self::Runtime(runtime) => runtime.closed.is_done(),
			Self::Opening(signal) => signal.is_done(),
		}
	}

	fn wait(&self, timeout: Duration) -> bool {
		match self {
			Self::Runtime(runtime) => runtime.wait_closed(timeout),
			Self::Opening(signal) => signal.wait(timeout),
		}
	}
}

fn finish_async_cleanup(guard: CleanupHookGuard, wait: CleanupWait) {
	let finished = catch_unwind(AssertUnwindSafe(|| wait.wait(CLEANUP_TIMEOUT))).unwrap_or(false);
	if !finished {
		eprintln!("fulltext native cleanup exceeded {} seconds", CLEANUP_TIMEOUT.as_secs());
	}
	drop(guard);
}

impl Drop for CleanupHookGuard {
	fn drop(&mut self) {
		// Safety: this guard uniquely owns Node's one-shot async cleanup handle.
		let _ = unsafe {
			napi::sys::napi_remove_async_cleanup_hook(self.raw_handle as napi::sys::napi_async_cleanup_hook_handle)
		};
	}
}

fn release_runtime(handle: u32, identity: &PathIdentity) {
	let mut registry = registry();
	registry.handles.remove(&handle);
	if registry.paths.get(identity) == Some(&handle) {
		registry.paths.remove(identity);
	}
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
	use std::os::unix::fs::MetadataExt;
	let metadata = fs::metadata(path).map_err(storage_error)?;
	Ok(PathIdentity::Unix(metadata.dev(), metadata.ino()))
}

#[cfg(windows)]
fn path_identity(path: &Path) -> Result<PathIdentity> {
	Ok(PathIdentity::Path(PathBuf::from(path.to_string_lossy().to_lowercase())))
}

#[cfg(all(not(unix), not(windows)))]
fn path_identity(path: &Path) -> Result<PathIdentity> {
	Ok(PathIdentity::Path(path.to_path_buf()))
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
	let mut bytes = b"FTRP\x01\x00\x00".to_vec();
	bytes.extend_from_slice(&body);
	bytes
}

fn error_envelope(error: FulltextError) -> Vec<u8> {
	let mut bytes = b"FTRP\x01\x00\x01".to_vec();
	push_string(&mut bytes, error.code);
	push_string(&mut bytes, &error.message);
	bytes
}

fn u32_body(value: u32) -> Vec<u8> {
	value.to_le_bytes().to_vec()
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
