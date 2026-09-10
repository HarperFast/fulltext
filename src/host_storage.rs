use std::collections::HashMap;
use std::io;
#[cfg(feature = "test-panic")]
use std::io::Write as IoWrite;
#[cfg(feature = "test-panic")]
use std::panic::{catch_unwind, AssertUnwindSafe};
#[cfg(feature = "test-panic")]
use std::path::Path;
#[cfg(feature = "test-panic")]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
#[cfg(feature = "test-panic")]
use std::thread;
use std::time::{Duration, Instant};

use napi::bindgen_prelude::Buffer;
use napi::threadsafe_function::{ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Env, JsBuffer, JsFunction, JsUnknown, Status};
use napi_derive::napi;
use tantivy::directory::OwnedBytes;
#[cfg(feature = "test-panic")]
use tantivy::directory::{Directory, TerminatingWrite};

use crate::boundary;
#[cfg(feature = "test-panic")]
use crate::phase0::ReclaimBudget;
#[cfg(any(feature = "test-panic", all(test, feature = "host-storage")))]
use crate::phase0::{reclaim_read_key_bytes, RECLAIM_MAX_BATCH_REQUEST_BYTES};
use crate::phase0::{KvDirectory, KvStore, KvStoreIdentity, Mutation, WritePolicy, CHUNK_SIZE};
use crate::protocol::HostOpenConfig;

#[cfg(feature = "test-panic")]
type CompletionCallback = ThreadsafeFunction<Vec<u8>, ErrorStrategy::Fatal>;

#[cfg(feature = "test-panic")]
static NEXT_TRANSPORT_HANDLE: AtomicU32 = AtomicU32::new(1);
static NEXT_TRANSPORT_ID: AtomicU64 = AtomicU64::new(1);
#[cfg(feature = "test-panic")]
static HOST_TRANSPORTS: OnceLock<Mutex<HashMap<u32, Arc<HostTransport>>>> = OnceLock::new();
const TRANSPORT_SHARDS: usize = 64;
type TransportRegistry = HashMap<u64, Weak<HostTransport>>;
type TransportShards = [Mutex<TransportRegistry>; TRANSPORT_SHARDS];
static TRANSPORTS: OnceLock<TransportShards> = OnceLock::new();

struct HostDispatch {
	// napi 2.16 can abandon queued TSFN data during environment teardown, so keep this payload fixed-size.
	transport_id: u64,
	request_id: u64,
}

type HostCallback = ThreadsafeFunction<HostDispatch, ErrorStrategy::Fatal>;

pub(crate) struct HostTransport {
	id: u64,
	handler: Mutex<Option<HostCallback>>,
	state: Mutex<TransportState>,
	capacity: Condvar,
	next_request_id: AtomicU64,
	abandoned_waiters: AtomicU64,
	max_operations: usize,
	max_bytes: usize,
	read_timeout: Duration,
}

#[derive(Default)]
struct TransportState {
	closed: Option<String>,
	operations: usize,
	bytes: usize,
	cleanup_operations: usize,
	cleanup_bytes: usize,
	cleanup_capacity: Option<CleanupCapacity>,
	pending: HashMap<u64, PendingRequest>,
}

struct PendingRequest {
	retained_bytes: usize,
	response_budget: usize,
	class: AdmissionClass,
	entered: bool,
	request: Option<Vec<u8>>,
	response: Weak<ResponseSlot>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdmissionClass {
	Foreground,
	Cleanup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CleanupCapacity {
	foreground_reserved_bytes: usize,
	max_bytes: usize,
}

struct ResponseSlot {
	result: Mutex<Option<io::Result<Vec<u8>>>>,
	ready: Condvar,
}

impl HostTransport {
	fn new(
		env: &Env,
		handler: JsFunction,
		max_operations: usize,
		max_bytes: usize,
		read_timeout: Duration,
	) -> boundary::Result<Self> {
		if max_operations == 0 || max_bytes == 0 || read_timeout.is_zero() {
			return Err(napi::Error::new(
				"E_INVALID_ARGUMENT",
				"host transport limits must be greater than zero",
			));
		}
		// Production construction must supply the total callback created by createHostStorageHandler.
		let mut handler = handler
			.create_threadsafe_function::<HostDispatch, Buffer, _, ErrorStrategy::Fatal>(
				max_operations,
				|context: ThreadSafeCallContext<HostDispatch>| {
					let request = registered_transport(context.value.transport_id)
						.and_then(|transport| transport.begin(context.value.request_id))
						.unwrap_or_default();
					let mut dispatch_id = Vec::with_capacity(16);
					dispatch_id.extend_from_slice(&context.value.transport_id.to_le_bytes());
					dispatch_id.extend_from_slice(&context.value.request_id.to_le_bytes());
					Ok(vec![Buffer::from(dispatch_id), Buffer::from(request)])
				},
			)
			.map_err(|error| napi::Error::new("E_NATIVE_FAILURE", error.to_string()))?;
		handler
			.unref(env)
			.map_err(|error| napi::Error::new("E_NATIVE_FAILURE", error.to_string()))?;
		Ok(Self {
			id: next_id(&NEXT_TRANSPORT_ID, "host storage transport")
				.map_err(|error| napi::Error::new("E_NATIVE_FAILURE", error.to_string()))?,
			handler: Mutex::new(Some(handler)),
			state: Mutex::new(TransportState::default()),
			capacity: Condvar::new(),
			next_request_id: AtomicU64::new(1),
			abandoned_waiters: AtomicU64::new(0),
			max_operations,
			max_bytes,
			read_timeout,
		})
	}

	// Completion requires the owning JavaScript environment to run, so callers must be native worker threads.
	fn round_trip(
		self: &Arc<Self>,
		request: Vec<u8>,
		response_budget: usize,
		deadline: Option<Instant>,
		class: AdmissionClass,
	) -> io::Result<Vec<u8>> {
		let request_id = next_id(&self.next_request_id, "host storage request")?;
		let response = Arc::new(ResponseSlot::new());
		self.admit(request_id, Some(request), response_budget, &response, deadline, class)?;
		let handler = self
			.handler
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.as_ref()
			.cloned();
		let status = handler.map_or(Status::Closing, |handler| {
			handler.call(
				HostDispatch {
					transport_id: self.id,
					request_id,
				},
				ThreadsafeFunctionCallMode::NonBlocking,
			)
		});
		if status != Status::Ok {
			if status == Status::Closing {
				self.fail(io::ErrorKind::BrokenPipe, "host storage transport is closed");
			} else {
				self.fail(
					io::ErrorKind::Other,
					&format!("host storage callback rejected an admitted request: {status:?}"),
				);
			}
		}

		if let Some(result) = response.wait(deadline) {
			return result;
		}
		self.abandoned_waiters.fetch_add(1, Ordering::Relaxed);
		Err(io::Error::new(
			io::ErrorKind::TimedOut,
			"host storage response timed out",
		))
	}

	pub(crate) fn close(&self) {
		unregister_transport(self.id);
		self.fail(io::ErrorKind::BrokenPipe, "host storage transport is closed");
		if let Some(handler) = self
			.handler
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.take()
		{
			let _ = handler.abort();
		}
	}

	pub(crate) fn wait_idle(&self) {
		let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		while state.operations != 0 {
			state = self
				.capacity
				.wait(state)
				.unwrap_or_else(|poisoned| poisoned.into_inner());
		}
	}

	fn admit(
		&self,
		request_id: u64,
		request: Option<Vec<u8>>,
		response_bytes: usize,
		response: &Arc<ResponseSlot>,
		deadline: Option<Instant>,
		class: AdmissionClass,
	) -> io::Result<()> {
		let retained_bytes = request
			.as_ref()
			.map_or(0, Vec::len)
			.checked_add(response_bytes)
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "host storage byte reservation overflow"))?;
		if response_bytes == 0 || retained_bytes > self.max_bytes {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"host storage request and response reservation exceeds the byte limit",
			));
		}
		let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		loop {
			if let Some(error) = &state.closed {
				return Err(io::Error::new(io::ErrorKind::BrokenPipe, error.clone()));
			}
			if self.has_capacity(&state, retained_bytes, class)? {
				break;
			}
			if class == AdmissionClass::Cleanup {
				return Err(io::Error::new(
					io::ErrorKind::WouldBlock,
					"low-priority host storage capacity is unavailable",
				));
			}
			state = match deadline {
				Some(deadline) => {
					let remaining = deadline.saturating_duration_since(Instant::now());
					if remaining.is_zero() {
						return Err(io::Error::new(
							io::ErrorKind::TimedOut,
							"host storage admission timed out",
						));
					}
					let (state, wait) = self
						.capacity
						.wait_timeout(state, remaining)
						.unwrap_or_else(|poisoned| poisoned.into_inner());
					if wait.timed_out() {
						return Err(io::Error::new(
							io::ErrorKind::TimedOut,
							"host storage admission timed out",
						));
					}
					state
				}
				None => self
					.capacity
					.wait(state)
					.unwrap_or_else(|poisoned| poisoned.into_inner()),
			};
		}
		state.operations += 1;
		state.bytes += retained_bytes;
		if class == AdmissionClass::Cleanup {
			state.cleanup_operations += 1;
			state.cleanup_bytes += retained_bytes;
		}
		state.pending.insert(
			request_id,
			PendingRequest {
				retained_bytes,
				response_budget: response_bytes,
				class,
				entered: false,
				request,
				response: Arc::downgrade(response),
			},
		);
		Ok(())
	}

	fn begin(&self, request_id: u64) -> Option<Vec<u8>> {
		let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		if state.closed.is_some() {
			return None;
		}
		let pending = state.pending.get_mut(&request_id)?;
		if pending.entered {
			return None;
		}
		let request = pending.request.take()?;
		pending.entered = true;
		Some(request)
	}

	fn has_capacity(&self, state: &TransportState, retained_bytes: usize, class: AdmissionClass) -> io::Result<bool> {
		let cleanup_capacity = if class == AdmissionClass::Cleanup {
			let capacity = state.cleanup_capacity.ok_or_else(|| {
				io::Error::new(
					io::ErrorKind::InvalidInput,
					"low-priority host storage admission is not configured",
				)
			})?;
			if retained_bytes > capacity.max_bytes {
				return Err(io::Error::new(
					io::ErrorKind::InvalidInput,
					"low-priority host storage reservation exceeds its byte limit",
				));
			}
			Some(capacity)
		} else {
			None
		};
		if state.operations >= self.max_operations || state.bytes.saturating_add(retained_bytes) > self.max_bytes {
			return Ok(false);
		}
		let Some(capacity) = cleanup_capacity else {
			return Ok(true);
		};
		Ok(state.cleanup_operations == 0
			&& state.cleanup_bytes.saturating_add(retained_bytes) <= capacity.max_bytes
			&& state
				.bytes
				.saturating_add(retained_bytes)
				.saturating_add(capacity.foreground_reserved_bytes)
				<= self.max_bytes)
	}

	#[cfg(feature = "test-panic")]
	fn configure_cleanup(&self, foreground_reserved_bytes: usize, max_cleanup_bytes: usize) -> io::Result<()> {
		if self.max_operations < 2 || foreground_reserved_bytes == 0 || max_cleanup_bytes == 0 {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"low-priority host storage requires two operation slots and positive byte limits",
			));
		}
		if foreground_reserved_bytes
			.checked_add(max_cleanup_bytes)
			.is_none_or(|required| required > self.max_bytes)
		{
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"host storage byte capacity cannot hold foreground headroom and one cleanup request",
			));
		}
		let capacity = CleanupCapacity {
			foreground_reserved_bytes,
			max_bytes: max_cleanup_bytes,
		};
		let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		match state.cleanup_capacity {
			Some(existing) if existing != capacity => Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"host storage cleanup capacity is already configured differently",
			)),
			Some(_) => Ok(()),
			None => {
				state.cleanup_capacity = Some(capacity);
				Ok(())
			}
		}
	}

	fn complete(&self, request_id: u64, result: io::Result<Vec<u8>>) -> bool {
		let response = {
			let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
			let Some(pending) = state.pending.remove(&request_id) else {
				return false;
			};
			let cleanup_underflow = pending.class == AdmissionClass::Cleanup
				&& (state.cleanup_operations == 0 || state.cleanup_bytes < pending.retained_bytes);
			if state.operations == 0 || state.bytes < pending.retained_bytes || cleanup_underflow {
				state.closed = Some("host storage transport accounting failed".to_owned());
				state.operations = 0;
				state.bytes = 0;
				state.cleanup_operations = 0;
				state.cleanup_bytes = 0;
				let remaining = std::mem::take(&mut state.pending);
				drop(state);
				self.capacity.notify_all();
				let error = || io::Error::other("host storage transport accounting failed");
				if let Some(response) = pending.response.upgrade() {
					response.complete(Err(error()));
				}
				for pending in remaining.into_values() {
					if let Some(response) = pending.response.upgrade() {
						response.complete(Err(error()));
					}
				}
				return true;
			}
			let response = pending.response.upgrade();
			state.operations -= 1;
			state.bytes -= pending.retained_bytes;
			if pending.class == AdmissionClass::Cleanup {
				state.cleanup_operations -= 1;
				state.cleanup_bytes -= pending.retained_bytes;
			}
			self.capacity.notify_all();
			response
		};
		if let Some(response) = response {
			response.complete(result);
		}
		true
	}

	fn response_budget(&self, request_id: u64) -> Option<usize> {
		self.state
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.pending
			.get(&request_id)
			.filter(|request| request.entered)
			.map(|request| request.response_budget)
	}

	fn fail(&self, kind: io::ErrorKind, message: &str) {
		let pending = {
			let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
			if state.closed.is_none() {
				state.closed = Some(message.to_owned());
			}
			state.operations = 0;
			state.bytes = 0;
			state.cleanup_operations = 0;
			state.cleanup_bytes = 0;
			std::mem::take(&mut state.pending)
		};
		self.capacity.notify_all();
		for request in pending.into_values() {
			if let Some(response) = request.response.upgrade() {
				response.complete(Err(io::Error::new(kind, message.to_owned())));
			}
		}
	}
}

impl Drop for HostTransport {
	fn drop(&mut self) {
		unregister_transport(self.id);
	}
}

impl ResponseSlot {
	fn new() -> Self {
		Self {
			result: Mutex::new(None),
			ready: Condvar::new(),
		}
	}

	fn complete(&self, result: io::Result<Vec<u8>>) {
		let mut slot = self.result.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		if slot.is_none() {
			*slot = Some(result);
			self.ready.notify_all();
		}
	}

	fn wait(&self, deadline: Option<Instant>) -> Option<io::Result<Vec<u8>>> {
		let mut slot = self.result.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		match deadline {
			Some(deadline) => {
				while slot.is_none() {
					let remaining = deadline.saturating_duration_since(Instant::now());
					if remaining.is_zero() {
						return None;
					}
					let (next, wait) = self
						.ready
						.wait_timeout(slot, remaining)
						.unwrap_or_else(|poisoned| poisoned.into_inner());
					slot = next;
					if wait.timed_out() && slot.is_none() {
						return None;
					}
				}
			}
			None => {
				while slot.is_none() {
					slot = self.ready.wait(slot).unwrap_or_else(|poisoned| poisoned.into_inner());
				}
			}
		}
		slot.take()
	}
}

fn response_bytes(value: JsUnknown, max_bytes: usize) -> io::Result<Vec<u8>> {
	if !value.is_buffer().map_err(|error| io::Error::other(error.to_string()))? {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			"host storage callback must return a Buffer",
		));
	}
	let buffer: JsBuffer = unsafe { value.cast() };
	let buffer = buffer
		.into_value()
		.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "host storage callback must return a Buffer"))?;
	if buffer.len() > max_bytes {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			"host storage callback response exceeds the byte limit",
		));
	}
	Ok(buffer.as_ref().to_vec())
}

// These internal exports fence lifecycle but do not authenticate callers; the package trusts process-local JavaScript.
#[napi(catch_unwind, skip_typescript, js_name = "__hostStorageComplete")]
pub fn host_storage_complete(dispatch_id: Buffer, response: JsUnknown) -> boundary::Result<bool> {
	boundary::run_stateless(|| {
		let (transport_id, request_id) = parse_dispatch_id(&dispatch_id)?;
		let Some(transport) = registered_transport(transport_id) else {
			return Ok(false);
		};
		let Some(response_budget) = transport.response_budget(request_id) else {
			return Ok(false);
		};
		Ok(transport.complete(request_id, response_bytes(response, response_budget)))
	})?
}

#[napi(catch_unwind, skip_typescript, js_name = "__hostStorageFail")]
pub fn host_storage_fail(dispatch_id: Buffer, message: String) -> boundary::Result<bool> {
	boundary::run_stateless(|| {
		let (transport_id, request_id) = parse_dispatch_id(&dispatch_id)?;
		let Some(transport) = registered_transport(transport_id) else {
			return Ok(false);
		};
		let mut end = message.len().min(4_096);
		while !message.is_char_boundary(end) {
			end -= 1;
		}
		let message = &message[..end];
		Ok(transport.complete(request_id, Err(io::Error::other(message.to_owned()))))
	})?
}

fn parse_dispatch_id(id: &[u8]) -> boundary::Result<(u64, u64)> {
	if id.len() != 16 {
		return Err(napi::Error::new(
			"E_INVALID_ARGUMENT",
			"host storage dispatch id must contain sixteen bytes",
		));
	}
	let transport_id = u64::from_le_bytes(id[..8].try_into().expect("dispatch id length checked"));
	let request_id = u64::from_le_bytes(id[8..].try_into().expect("dispatch id length checked"));
	Ok((transport_id, request_id))
}

fn next_id(counter: &AtomicU64, name: &str) -> io::Result<u64> {
	loop {
		let id = counter.load(Ordering::Relaxed);
		if id == 0 {
			return Err(io::Error::other(format!("{name} id space exhausted")));
		}
		let next = id.checked_add(1).unwrap_or(0);
		if counter
			.compare_exchange_weak(id, next, Ordering::Relaxed, Ordering::Relaxed)
			.is_ok()
		{
			return Ok(id);
		}
	}
}

const HOST_PROTOCOL_VERSION: u8 = 1;
const OP_READ: u8 = 1;
const OP_WRITE: u8 = 2;
const OP_SYNC: u8 = 3;
const RESPONSE_OK: u8 = 0;
const RESPONSE_ERROR: u8 = 1;
const VALUE_MISSING: u8 = 0;
const VALUE_PRESENT: u8 = 1;
const MUTATION_PUT: u8 = 1;
const MUTATION_DELETE: u8 = 2;
#[cfg(any(test, feature = "test-panic"))]
const HOST_PROTOCOL_HEADER_BYTES: usize = 2;
#[cfg(any(test, feature = "test-panic"))]
const HOST_LENGTH_PREFIX_BYTES: usize = 4;
const READ_RESPONSE_OVERHEAD: usize = 7;

#[cfg(any(test, feature = "test-panic"))]
fn host_read_request_bytes(key_bytes: usize) -> io::Result<usize> {
	key_bytes
		.checked_add(HOST_PROTOCOL_HEADER_BYTES + HOST_LENGTH_PREFIX_BYTES)
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "host read request size overflow"))
}

#[cfg(any(test, feature = "test-panic"))]
fn minimum_cleanup_reservation(
	max_read_response_bytes: usize,
	max_control_response_bytes: usize,
	max_read_request_bytes: usize,
	max_mutation_request_bytes: usize,
) -> io::Result<usize> {
	let read = max_read_response_bytes
		.checked_add(max_read_request_bytes)
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cleanup read reservation overflow"))?;
	let write = max_control_response_bytes
		.checked_add(max_mutation_request_bytes.min(RECLAIM_MAX_BATCH_REQUEST_BYTES))
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "cleanup write reservation overflow"))?;
	Ok(read.max(write))
}

fn validate_cleanup_request(request_bytes: usize, limit: Option<usize>, operation: &str) -> io::Result<()> {
	if limit.is_some_and(|limit| request_bytes > limit) {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("low-priority host storage {operation} request exceeds its byte limit"),
		));
	}
	Ok(())
}

#[derive(Clone)]
pub(crate) struct HostKvStore {
	transport: Arc<HostTransport>,
	identity: KvStoreIdentity,
	max_read_response_bytes: usize,
	max_control_response_bytes: usize,
	max_cleanup_read_request_bytes: Option<usize>,
	max_cleanup_mutation_request_bytes: Option<usize>,
	class: AdmissionClass,
}

impl HostKvStore {
	fn new(
		transport: Arc<HostTransport>,
		identity: KvStoreIdentity,
		max_read_response_bytes: usize,
		max_control_response_bytes: usize,
	) -> io::Result<Self> {
		let minimum_read_response = CHUNK_SIZE + READ_RESPONSE_OVERHEAD;
		if max_read_response_bytes < minimum_read_response {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				format!("host read response budget must be at least {minimum_read_response} bytes"),
			));
		}
		Ok(Self {
			transport,
			identity,
			max_read_response_bytes,
			max_control_response_bytes,
			max_cleanup_read_request_bytes: None,
			max_cleanup_mutation_request_bytes: None,
			class: AdmissionClass::Foreground,
		})
	}

	#[cfg(feature = "test-panic")]
	fn minimum_cleanup_bytes(&self, namespace: &[u8], budget: ReclaimBudget) -> io::Result<usize> {
		minimum_cleanup_reservation(
			self.max_read_response_bytes,
			self.max_control_response_bytes,
			host_read_request_bytes(reclaim_read_key_bytes(namespace))?,
			budget.max_request_bytes,
		)
	}

	#[cfg(feature = "test-panic")]
	fn cleanup_directory(
		&self,
		namespace: &[u8],
		foreground_reserved_bytes: usize,
		max_cleanup_bytes: usize,
		budget: ReclaimBudget,
	) -> io::Result<KvDirectory<Self>> {
		let max_read_request_bytes = host_read_request_bytes(reclaim_read_key_bytes(namespace))?;
		let max_mutation_request_bytes = budget.max_request_bytes.min(RECLAIM_MAX_BATCH_REQUEST_BYTES);
		if max_read_request_bytes == 0 || max_mutation_request_bytes == 0 {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"low-priority host storage request limits must be positive",
			));
		}
		let minimum_reservation = minimum_cleanup_reservation(
			self.max_read_response_bytes,
			self.max_control_response_bytes,
			max_read_request_bytes,
			max_mutation_request_bytes,
		)?;
		if max_cleanup_bytes < minimum_reservation {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				format!("low-priority host storage byte limit must be at least {minimum_reservation} bytes"),
			));
		}
		self.transport
			.configure_cleanup(foreground_reserved_bytes, max_cleanup_bytes)?;
		let store = Self {
			class: AdmissionClass::Cleanup,
			max_cleanup_read_request_bytes: Some(max_read_request_bytes),
			max_cleanup_mutation_request_bytes: Some(max_mutation_request_bytes),
			..self.clone()
		};
		Ok(KvDirectory::with_namespace(store, namespace))
	}

	fn request(&self, request: Vec<u8>, response_budget: usize) -> io::Result<ResponseDecoder> {
		validate_cleanup_request(request.len(), self.max_cleanup_read_request_bytes, "read")?;
		// Read deadlines cover admission and host execution; a timed-out read has no storage side effect.
		let deadline = Instant::now()
			.checked_add(self.transport.read_timeout)
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "host storage timeout is too large"))?;
		self.request_with_deadline(request, response_budget, Some(deadline))
	}

	fn request_mutation(&self, request: Vec<u8>, response_budget: usize) -> io::Result<ResponseDecoder> {
		validate_cleanup_request(request.len(), self.max_cleanup_mutation_request_bytes, "mutation")?;
		// A dispatched JavaScript mutation cannot be canceled, so wait for its definitive result.
		self.request_with_deadline(request, response_budget, None)
	}

	fn request_with_deadline(
		&self,
		request: Vec<u8>,
		response_budget: usize,
		deadline: Option<Instant>,
	) -> io::Result<ResponseDecoder> {
		let response = self
			.transport
			.round_trip(request, response_budget, deadline, self.class)?;
		let mut decoder = ResponseDecoder::new(response);
		if decoder.u8()? != HOST_PROTOCOL_VERSION {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"host storage response has an unsupported protocol version",
			));
		}
		match decoder.u8()? {
			RESPONSE_OK => Ok(decoder),
			RESPONSE_ERROR => {
				let message = decoder.bytes()?.to_vec();
				decoder.finish()?;
				Err(io::Error::other(String::from_utf8_lossy(&message).into_owned()))
			}
			_ => Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"host storage response has an unknown status",
			)),
		}
	}
}

pub(crate) fn open_directory(
	env: &Env,
	handler: JsFunction,
	config: &HostOpenConfig,
) -> boundary::Result<(KvDirectory<HostKvStore>, Arc<HostTransport>)> {
	let transport = Arc::new(HostTransport::new(
		env,
		handler,
		config.max_operations,
		config.max_transport_bytes,
		config.read_timeout,
	)?);
	let identity = KvStoreIdentity(
		config.store_identity.0,
		config.store_identity.1,
		config.store_identity.2,
	);
	let store = HostKvStore::new(
		transport.clone(),
		identity,
		config.max_read_response_bytes,
		config.max_control_response_bytes,
	)
	.map_err(|error| napi::Error::new("E_INVALID_ARGUMENT", error.to_string()))?;
	register_transport(&transport);
	Ok((KvDirectory::with_namespace(store, &config.namespace), transport))
}

impl KvStore for HostKvStore {
	fn identity(&self) -> KvStoreIdentity {
		self.identity
	}

	fn read(&self, key: &[u8]) -> io::Result<Option<OwnedBytes>> {
		let mut request = RequestEncoder::new(OP_READ);
		request.bytes(key)?;
		let mut response = self.request(request.finish(), self.max_read_response_bytes)?;
		let value = match response.u8()? {
			VALUE_MISSING => {
				response.finish()?;
				None
			}
			VALUE_PRESENT => Some(response.into_owned_bytes()?),
			_ => {
				return Err(io::Error::new(
					io::ErrorKind::InvalidData,
					"host storage read response has an unknown value status",
				))
			}
		};
		Ok(value)
	}

	fn write(&self, mutations: &[Mutation], policy: WritePolicy) -> io::Result<()> {
		let mut request = RequestEncoder::new(OP_WRITE);
		let requires_sync = policy == WritePolicy::WAL_SYNC;
		// Harper exposes an atomic WAL batch and a separate database durability barrier.
		request.u8(match policy {
			WritePolicy::WAL | WritePolicy::WAL_SYNC => 1,
			WritePolicy::NO_WAL => 3,
			_ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid write policy")),
		});
		request.u32(mutations.len())?;
		for mutation in mutations {
			match mutation {
				Mutation::Put(key, value) => {
					request.u8(MUTATION_PUT);
					request.bytes(key)?;
					request.bytes(value)?;
				}
				Mutation::Delete(key) => {
					request.u8(MUTATION_DELETE);
					request.bytes(key)?;
				}
			}
		}
		self.request_mutation(request.finish(), self.max_control_response_bytes)?
			.finish()?;
		if requires_sync {
			// A barrier failure leaves the write outcome known-applied; the caller poisons the generation.
			self.sync()?;
		}
		Ok(())
	}

	fn sync(&self) -> io::Result<()> {
		self.request_mutation(RequestEncoder::new(OP_SYNC).finish(), self.max_control_response_bytes)?
			.finish()
	}
}

struct RequestEncoder(Vec<u8>);

impl RequestEncoder {
	fn new(operation: u8) -> Self {
		Self(vec![HOST_PROTOCOL_VERSION, operation])
	}

	fn u8(&mut self, value: u8) {
		self.0.push(value);
	}

	fn u32(&mut self, value: usize) -> io::Result<()> {
		let value =
			u32::try_from(value).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "value is too large"))?;
		self.0.extend_from_slice(&value.to_le_bytes());
		Ok(())
	}

	fn bytes(&mut self, value: &[u8]) -> io::Result<()> {
		self.u32(value.len())?;
		self.0.extend_from_slice(value);
		Ok(())
	}

	fn finish(self) -> Vec<u8> {
		self.0
	}
}

struct ResponseDecoder {
	bytes: Vec<u8>,
	offset: usize,
}

impl ResponseDecoder {
	fn new(bytes: Vec<u8>) -> Self {
		Self { bytes, offset: 0 }
	}

	fn u8(&mut self) -> io::Result<u8> {
		let value = *self
			.bytes
			.get(self.offset)
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "host storage response is truncated"))?;
		self.offset += 1;
		Ok(value)
	}

	fn bytes(&mut self) -> io::Result<&[u8]> {
		let range = self.byte_range()?;
		Ok(&self.bytes[range])
	}

	fn into_owned_bytes(mut self) -> io::Result<OwnedBytes> {
		let range = self.byte_range()?;
		if self.offset != self.bytes.len() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"host storage response has trailing bytes",
			));
		}
		let bytes = OwnedBytes::new(self.bytes);
		Ok(bytes.slice(range))
	}

	fn byte_range(&mut self) -> io::Result<std::ops::Range<usize>> {
		let length_end = self
			.offset
			.checked_add(4)
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "host storage response length overflow"))?;
		let length_bytes: [u8; 4] = self
			.bytes
			.get(self.offset..length_end)
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "host storage response is truncated"))?
			.try_into()
			.unwrap();
		self.offset = length_end;
		let length = u32::from_le_bytes(length_bytes) as usize;
		let end = self
			.offset
			.checked_add(length)
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "host storage response length overflow"))?;
		if end > self.bytes.len() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"host storage response is truncated",
			));
		}
		let start = self.offset;
		self.offset = end;
		Ok(start..end)
	}

	fn finish(self) -> io::Result<()> {
		if self.offset == self.bytes.len() {
			Ok(())
		} else {
			Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"host storage response has trailing bytes",
			))
		}
	}
}

#[cfg(feature = "test-panic")]
fn registry() -> std::sync::MutexGuard<'static, HashMap<u32, Arc<HostTransport>>> {
	HOST_TRANSPORTS
		.get_or_init(Default::default)
		.lock()
		.unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn transport_shard(transport_id: u64) -> &'static Mutex<HashMap<u64, Weak<HostTransport>>> {
	&TRANSPORTS.get_or_init(|| std::array::from_fn(|_| Mutex::new(HashMap::new())))
		[transport_id as usize % TRANSPORT_SHARDS]
}

fn register_transport(transport: &Arc<HostTransport>) {
	transport_shard(transport.id)
		.lock()
		.unwrap_or_else(|poisoned| poisoned.into_inner())
		.insert(transport.id, Arc::downgrade(transport));
}

fn registered_transport(transport_id: u64) -> Option<Arc<HostTransport>> {
	let mut shard = transport_shard(transport_id)
		.lock()
		.unwrap_or_else(|poisoned| poisoned.into_inner());
	let transport = shard.get(&transport_id).and_then(Weak::upgrade);
	if transport.is_none() {
		shard.remove(&transport_id);
	}
	transport
}

fn unregister_transport(transport_id: u64) {
	transport_shard(transport_id)
		.lock()
		.unwrap_or_else(|poisoned| poisoned.into_inner())
		.remove(&transport_id);
}

#[cfg(feature = "test-panic")]
fn completion(callback: JsFunction) -> boundary::Result<CompletionCallback> {
	callback
		.create_threadsafe_function::<Vec<u8>, Buffer, _, ErrorStrategy::Fatal>(
			0,
			|context: ThreadSafeCallContext<Vec<u8>>| Ok(vec![Buffer::from(context.value)]),
		)
		.map_err(|error| napi::Error::new("E_NATIVE_FAILURE", error.to_string()))
}

#[cfg(feature = "test-panic")]
fn test_result(result: io::Result<Vec<u8>>) -> Vec<u8> {
	match result {
		Ok(bytes) => {
			let mut encoded = Vec::with_capacity(bytes.len() + 1);
			encoded.push(0);
			encoded.extend_from_slice(&bytes);
			encoded
		}
		Err(error) => {
			let message = error.to_string();
			let mut encoded = Vec::with_capacity(message.len() + 1);
			encoded.push(1);
			encoded.extend_from_slice(message.as_bytes());
			encoded
		}
	}
}

#[cfg(feature = "test-panic")]
fn test_thread_result(operation: impl FnOnce() -> io::Result<Vec<u8>>) -> Vec<u8> {
	match catch_unwind(AssertUnwindSafe(operation)) {
		Ok(result) => test_result(result),
		Err(_) => test_result(Err(io::Error::other("native host storage test panicked"))),
	}
}

#[cfg(feature = "test-panic")]
struct CleanupTransport {
	handle: u32,
	transport: Weak<HostTransport>,
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testOpenHostTransport")]
pub fn test_open_host_transport(
	env: Env,
	handler: JsFunction,
	max_operations: u32,
	max_bytes: u32,
	read_timeout_ms: u32,
) -> boundary::Result<u32> {
	boundary::run_stateless(|| {
		let handle = NEXT_TRANSPORT_HANDLE.fetch_add(1, Ordering::Relaxed);
		if handle == 0 {
			return Err(napi::Error::new(
				"E_NATIVE_FAILURE",
				"host transport handle space exhausted",
			));
		}
		let transport = Arc::new(HostTransport::new(
			&env,
			handler,
			max_operations as usize,
			max_bytes as usize,
			Duration::from_millis(read_timeout_ms as u64),
		)?);
		register_transport(&transport);
		registry().insert(handle, transport.clone());
		if let Err(error) = env.add_async_cleanup_hook(
			CleanupTransport {
				handle,
				transport: Arc::downgrade(&transport),
			},
			|cleanup| {
				if let Some(transport) = cleanup.transport.upgrade() {
					transport.close();
				}
				registry().remove(&cleanup.handle);
			},
		) {
			registry().remove(&handle);
			return Err(napi::Error::new("E_NATIVE_FAILURE", error.to_string()));
		}
		Ok(handle)
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testHostRoundTrip")]
pub fn test_host_round_trip(
	handle: u32,
	request: Buffer,
	response_budget: u32,
	use_timeout: bool,
	low_priority: bool,
	callback: JsFunction,
) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let transport = registry()
			.get(&handle)
			.cloned()
			.ok_or_else(|| napi::Error::new("E_CLOSED", "unknown or closed host storage transport"))?;
		let completion = completion(callback)?;
		let request = request.to_vec();
		thread::Builder::new()
			.name(format!("fulltext-host-storage-test-{handle}"))
			.spawn(move || {
				let result = test_thread_result(|| {
					let deadline = use_timeout.then(|| Instant::now() + transport.read_timeout);
					transport.round_trip(
						request,
						response_budget as usize,
						deadline,
						if low_priority {
							AdmissionClass::Cleanup
						} else {
							AdmissionClass::Foreground
						},
					)
				});
				let _ = completion.call(result, ThreadsafeFunctionCallMode::NonBlocking);
			})
			.map_err(|error| napi::Error::new("E_NATIVE_FAILURE", error.to_string()))?;
		Ok(())
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testConfigureHostTransportCleanup")]
pub fn test_configure_host_transport_cleanup(
	handle: u32,
	foreground_reserved_bytes: u32,
	max_cleanup_bytes: u32,
) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		registry()
			.get(&handle)
			.cloned()
			.ok_or_else(|| napi::Error::new("E_CLOSED", "unknown or closed host storage transport"))?
			.configure_cleanup(foreground_reserved_bytes as usize, max_cleanup_bytes as usize)
			.map_err(|error| napi::Error::new("E_INVALID_ARGUMENT", error.to_string()))
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testHostTransportStats")]
pub fn test_host_transport_stats(handle: u32) -> boundary::Result<Vec<String>> {
	boundary::run_stateless(|| {
		let transport = registry()
			.get(&handle)
			.cloned()
			.ok_or_else(|| napi::Error::new("E_CLOSED", "unknown or closed host storage transport"))?;
		let state = transport.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		Ok(vec![
			state.operations.to_string(),
			state.bytes.to_string(),
			state.cleanup_operations.to_string(),
			state.cleanup_bytes.to_string(),
			transport.abandoned_waiters.load(Ordering::Relaxed).to_string(),
		])
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testHoldHostTransportCapacity")]
pub fn test_hold_host_transport_capacity(
	handle: u32,
	request_bytes: u32,
	response_bytes: u32,
	low_priority: bool,
) -> boundary::Result<String> {
	boundary::run_stateless(|| {
		let transport = registry()
			.get(&handle)
			.cloned()
			.ok_or_else(|| napi::Error::new("E_CLOSED", "unknown or closed host storage transport"))?;
		let request_id = next_id(&transport.next_request_id, "host storage request")
			.map_err(|error| napi::Error::new("E_NATIVE_FAILURE", error.to_string()))?;
		let response = Arc::new(ResponseSlot::new());
		transport
			.admit(
				request_id,
				Some(vec![0; request_bytes as usize]),
				response_bytes as usize,
				&response,
				Some(Instant::now()),
				if low_priority {
					AdmissionClass::Cleanup
				} else {
					AdmissionClass::Foreground
				},
			)
			.map_err(|error| napi::Error::new("E_NATIVE_FAILURE", error.to_string()))?;
		Ok(request_id.to_string())
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testReleaseHostTransportCapacity")]
pub fn test_release_host_transport_capacity(handle: u32, request_id: String) -> boundary::Result<bool> {
	boundary::run_stateless(|| {
		let transport = registry()
			.get(&handle)
			.cloned()
			.ok_or_else(|| napi::Error::new("E_CLOSED", "unknown or closed host storage transport"))?;
		let request_id = request_id
			.parse()
			.map_err(|_| napi::Error::new("E_INVALID_ARGUMENT", "invalid host storage request id"))?;
		let exists = transport
			.state
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.pending
			.contains_key(&request_id);
		if exists {
			transport.complete(request_id, Ok(Vec::new()));
		}
		Ok(exists)
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testVerifyTantivyOnHostTransport")]
pub fn test_verify_tantivy_on_host_transport(
	handle: u32,
	max_read_response_bytes: u32,
	max_control_response_bytes: u32,
	callback: JsFunction,
) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let transport = registry()
			.get(&handle)
			.cloned()
			.ok_or_else(|| napi::Error::new("E_CLOSED", "unknown or closed host storage transport"))?;
		let completion = completion(callback)?;
		thread::Builder::new()
			.name(format!("fulltext-host-directory-test-{handle}"))
			.spawn(move || {
				let result = test_thread_result(|| {
					let store = HostKvStore::new(
						transport,
						KvStoreIdentity(1, handle as u64, 1),
						max_read_response_bytes as usize,
						max_control_response_bytes as usize,
					)?;
					let run = NEXT_TRANSPORT_HANDLE.fetch_add(1, Ordering::Relaxed);
					let case = AtomicU32::new(0);
					crate::directory_harness::verify_directory_contract(|| {
						let namespace = format!("host-contract/{run}/{}", case.fetch_add(1, Ordering::Relaxed));
						KvDirectory::with_namespace(store.clone(), namespace.as_bytes())
					})
					.and_then(|_| {
						let namespace = format!("host-large-file/{run}");
						crate::directory_harness::verify_large_file(
							KvDirectory::with_namespace(store.clone(), namespace.as_bytes()),
							CHUNK_SIZE,
						)
					})
					.and_then(|_| {
						let namespace = format!("host-lifecycle/{run}");
						crate::directory_harness::verify_tantivy_lifecycle(KvDirectory::with_namespace(
							store,
							namespace.as_bytes(),
						))
					})
					.map(|_| Vec::new())
					.map_err(io::Error::other)
				});
				let _ = completion.call(result, ThreadsafeFunctionCallMode::NonBlocking);
			})
			.map_err(|error| napi::Error::new("E_NATIVE_FAILURE", error.to_string()))?;
		Ok(())
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testReclaimOnHostTransport")]
pub fn test_reclaim_on_host_transport(
	handle: u32,
	max_read_response_bytes: u32,
	max_control_response_bytes: u32,
	callback: JsFunction,
) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let transport = registry()
			.get(&handle)
			.cloned()
			.ok_or_else(|| napi::Error::new("E_CLOSED", "unknown or closed host storage transport"))?;
		let completion = completion(callback)?;
		thread::Builder::new()
			.name(format!("fulltext-host-reclaim-test-{handle}"))
			.spawn(move || {
				let result = test_thread_result(|| {
					let store = HostKvStore::new(
						transport,
						KvStoreIdentity(1, handle as u64, 1),
						max_read_response_bytes as usize,
						max_control_response_bytes as usize,
					)?;
					let foreground_reserved_bytes = (max_read_response_bytes as usize)
						.checked_add(max_control_response_bytes as usize)
						.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "foreground reserve overflow"))?;
					let budget = ReclaimBudget {
						max_point_reads: 1_024,
						max_mutations: 4_096,
						max_request_bytes: 1024 * 1024,
						max_elapsed: Duration::from_secs(5),
					};
					let run = NEXT_TRANSPORT_HANDLE.fetch_add(1, Ordering::Relaxed);
					let namespace = format!("host-reclaim/{run}");
					let max_cleanup_bytes = store.minimum_cleanup_bytes(namespace.as_bytes(), budget)?;
					if store
						.cleanup_directory(
							namespace.as_bytes(),
							foreground_reserved_bytes,
							max_cleanup_bytes - 1,
							budget,
						)
						.is_ok()
					{
						return Err(io::Error::other("undersized cleanup storage view was accepted"));
					}
					let cleanup = store.cleanup_directory(
						namespace.as_bytes(),
						foreground_reserved_bytes,
						max_cleanup_bytes,
						budget,
					)?;
					let foreground = KvDirectory::with_namespace(store, namespace.as_bytes());
					let payload = vec![7_u8; CHUNK_SIZE + 17];
					for path in ["pinned", "unpinned"] {
						let path = Path::new(path);
						let mut writer = foreground
							.open_write(path)
							.map_err(|error| io::Error::other(error.to_string()))?;
						writer.write_all(&payload)?;
						writer.terminate()?;
					}
					let pinned = foreground
						.open_read(Path::new("pinned"))
						.map_err(|error| io::Error::other(error.to_string()))?;
					for path in ["pinned", "unpinned"] {
						foreground
							.delete(Path::new(path))
							.map_err(|error| io::Error::other(error.to_string()))?;
					}
					let first = cleanup.reclaim(budget)?;
					if first.entries_reclaimed == 0 || first.pinned_skips == 0 {
						return Err(io::Error::other(
							"host reclamation did not reclaim an unpinned object while preserving a pinned object",
						));
					}
					drop(pinned);
					let second = cleanup.reclaim(budget)?;
					if second.entries_reclaimed == 0 || second.has_more {
						return Err(io::Error::other(
							"host reclamation did not drain after the retained reader closed",
						));
					}
					Ok(format!(
						"{},{},{},{}",
						first.entries_reclaimed + second.entries_reclaimed,
						first.pinned_skips,
						first.payload_delete_mutations + second.payload_delete_mutations,
						payload.len() * 2
					)
					.into_bytes())
				});
				let _ = completion.call(result, ThreadsafeFunctionCallMode::NonBlocking);
			})
			.map_err(|error| napi::Error::new("E_NATIVE_FAILURE", error.to_string()))?;
		Ok(())
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testCloseHostTransport")]
pub fn test_close_host_transport(handle: u32) -> boundary::Result<bool> {
	boundary::run_stateless(|| {
		let transport = registry().remove(&handle);
		if let Some(transport) = &transport {
			transport.close();
		}
		transport.is_some()
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn response_slot_returns_a_close_error() {
		let response = Arc::new(ResponseSlot::new());
		response.complete(Err(io::Error::new(io::ErrorKind::BrokenPipe, "closed")));
		assert_eq!(
			response.wait(Some(Instant::now())).unwrap().unwrap_err().kind(),
			io::ErrorKind::BrokenPipe
		);
	}

	#[test]
	fn response_slot_accepts_only_the_first_completion() {
		let response = ResponseSlot::new();
		response.complete(Ok(vec![1]));
		response.complete(Ok(vec![2]));
		assert_eq!(response.wait(Some(Instant::now())).unwrap().unwrap(), vec![1]);
	}

	#[test]
	fn cleanup_reservation_covers_reads_and_bounded_writes() {
		assert_eq!(minimum_cleanup_reservation(100, 10, 6, 20).unwrap(), 106);
		assert_eq!(minimum_cleanup_reservation(10, 100, 6, 20).unwrap(), 120);
		assert_eq!(
			minimum_cleanup_reservation(10, 100, 6, usize::MAX).unwrap(),
			100 + RECLAIM_MAX_BATCH_REQUEST_BYTES
		);
		assert_eq!(reclaim_read_key_bytes(b""), 30);
		assert_eq!(reclaim_read_key_bytes(b"index"), 35);
		assert_eq!(host_read_request_bytes(reclaim_read_key_bytes(b"")).unwrap(), 36);
		assert_eq!(host_read_request_bytes(reclaim_read_key_bytes(b"index")).unwrap(), 41);
		assert!(minimum_cleanup_reservation(usize::MAX, 10, 6, 20).is_err());
		assert!(minimum_cleanup_reservation(10, usize::MAX, 6, 20).is_err());
	}

	#[test]
	fn cleanup_request_limits_are_enforced() {
		assert!(validate_cleanup_request(6, Some(6), "read").is_ok());
		assert!(validate_cleanup_request(7, Some(6), "read").is_err());
		assert!(validate_cleanup_request(usize::MAX, None, "read").is_ok());
	}
}
