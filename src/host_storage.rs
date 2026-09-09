use std::collections::HashMap;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread;
use std::time::{Duration, Instant};

use napi::bindgen_prelude::Buffer;
use napi::threadsafe_function::{ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Env, JsBuffer, JsFunction, JsUnknown, Status};
use napi_derive::napi;
use tantivy::directory::OwnedBytes;

use crate::boundary;
use crate::phase0::{KvDirectory, KvStore, KvStoreIdentity, Mutation, WritePolicy, CHUNK_SIZE};

type HostCallback = ThreadsafeFunction<Vec<u8>, ErrorStrategy::Fatal>;
type CompletionCallback = ThreadsafeFunction<Vec<u8>, ErrorStrategy::Fatal>;

static NEXT_TRANSPORT_HANDLE: AtomicU32 = AtomicU32::new(1);
static HOST_TRANSPORTS: OnceLock<Mutex<HashMap<u32, Arc<HostTransport>>>> = OnceLock::new();

struct HostTransport {
	handler: HostCallback,
	state: Mutex<TransportState>,
	capacity: Condvar,
	next_request_id: AtomicU64,
	max_operations: usize,
	max_bytes: usize,
	read_timeout: Duration,
}

#[derive(Default)]
struct TransportState {
	closed: Option<String>,
	operations: usize,
	bytes: usize,
	pending: HashMap<u64, PendingRequest>,
}

struct PendingRequest {
	retained_bytes: usize,
	response: Weak<ResponseSlot>,
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
			.create_threadsafe_function::<Vec<u8>, Buffer, _, ErrorStrategy::Fatal>(
				max_operations,
				|context: ThreadSafeCallContext<Vec<u8>>| Ok(vec![Buffer::from(context.value)]),
			)
			.map_err(|error| napi::Error::new("E_NATIVE_FAILURE", error.to_string()))?;
		handler
			.unref(env)
			.map_err(|error| napi::Error::new("E_NATIVE_FAILURE", error.to_string()))?;
		Ok(Self {
			handler,
			state: Mutex::new(TransportState::default()),
			capacity: Condvar::new(),
			next_request_id: AtomicU64::new(1),
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
	) -> io::Result<Vec<u8>> {
		let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
		if request_id == 0 {
			self.fail(io::ErrorKind::Other, "host storage request id space exhausted");
			return Err(io::Error::other("host storage request id space exhausted"));
		}
		let response = Arc::new(ResponseSlot::new());
		self.admit(request_id, request.len(), response_budget, &response, deadline)?;

		let transport = Arc::downgrade(self);
		let callback_response = response.clone();
		let max_response_bytes = response_budget;
		let status = self.handler.call_with_return_value::<JsUnknown, _>(
			request,
			ThreadsafeFunctionCallMode::NonBlocking,
			move |value| {
				let completed = catch_unwind(AssertUnwindSafe(|| {
					let result = response_bytes(value, max_response_bytes);
					if let Some(transport) = transport.upgrade() {
						transport.complete(request_id, result);
					} else {
						callback_response.complete(Err(io::Error::new(
							io::ErrorKind::BrokenPipe,
							"host storage transport was released",
						)));
					}
				}));
				if completed.is_err() {
					if let Some(transport) = transport.upgrade() {
						transport.fail(io::ErrorKind::Other, "host storage completion panicked");
					}
					callback_response.complete(Err(io::Error::other("host storage completion panicked")));
				}
				Ok(())
			},
		);
		if status != Status::Ok {
			self.complete(
				request_id,
				Err(io::Error::new(
					io::ErrorKind::WouldBlock,
					format!("host storage callback rejected request: {status:?}"),
				)),
			);
		}

		if let Some(result) = response.wait(deadline) {
			return result;
		}
		self.release(request_id);
		let _ = response.take();
		Err(io::Error::new(
			io::ErrorKind::TimedOut,
			"host storage request timed out",
		))
	}

	fn close(&self) {
		self.fail(io::ErrorKind::BrokenPipe, "host storage transport is closed");
	}

	fn admit(
		&self,
		request_id: u64,
		request_bytes: usize,
		response_bytes: usize,
		response: &Arc<ResponseSlot>,
		deadline: Option<Instant>,
	) -> io::Result<()> {
		let retained_bytes = request_bytes
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
			if state.operations < self.max_operations && state.bytes.saturating_add(retained_bytes) <= self.max_bytes {
				break;
			}
			state = match deadline {
				Some(deadline) => {
					let remaining = deadline.saturating_duration_since(Instant::now());
					if remaining.is_zero() {
						return Err(io::Error::new(
							io::ErrorKind::TimedOut,
							"host storage request timed out",
						));
					}
					let (state, wait) = self
						.capacity
						.wait_timeout(state, remaining)
						.unwrap_or_else(|poisoned| poisoned.into_inner());
					if wait.timed_out() {
						return Err(io::Error::new(
							io::ErrorKind::TimedOut,
							"host storage request timed out",
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
		state.pending.insert(
			request_id,
			PendingRequest {
				retained_bytes,
				response: Arc::downgrade(response),
			},
		);
		Ok(())
	}

	fn complete(&self, request_id: u64, result: io::Result<Vec<u8>>) {
		let response = {
			let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
			let Some(pending) = state.pending.remove(&request_id) else {
				return;
			};
			if state.operations == 0 || state.bytes < pending.retained_bytes {
				state.closed = Some("host storage transport accounting failed".to_owned());
				state.operations = 0;
				state.bytes = 0;
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
				return;
			}
			let response = pending.response.upgrade();
			state.operations -= 1;
			state.bytes -= pending.retained_bytes;
			self.capacity.notify_all();
			response
		};
		if let Some(response) = response {
			response.complete(result);
		}
	}

	fn release(&self, request_id: u64) {
		let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		if let Some(pending) = state.pending.remove(&request_id) {
			if state.operations == 0 || state.bytes < pending.retained_bytes {
				drop(state);
				self.fail(io::ErrorKind::Other, "host storage transport accounting failed");
				return;
			}
			state.operations -= 1;
			state.bytes -= pending.retained_bytes;
			self.capacity.notify_all();
		}
	}

	fn fail(&self, kind: io::ErrorKind, message: &str) {
		let pending = {
			let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
			if state.closed.is_none() {
				state.closed = Some(message.to_owned());
			}
			state.operations = 0;
			state.bytes = 0;
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

	fn take(&self) -> Option<io::Result<Vec<u8>>> {
		self.result
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.take()
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
const READ_RESPONSE_OVERHEAD: usize = 7;

#[derive(Clone)]
struct HostKvStore {
	transport: Arc<HostTransport>,
	identity: KvStoreIdentity,
	max_read_response_bytes: usize,
	max_control_response_bytes: usize,
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
		})
	}

	fn request(&self, request: Vec<u8>, response_budget: usize) -> io::Result<ResponseDecoder> {
		// Read deadlines cover admission and host execution; a timed-out read has no storage side effect.
		let deadline = Instant::now()
			.checked_add(self.transport.read_timeout)
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "host storage timeout is too large"))?;
		self.request_with_deadline(request, response_budget, Some(deadline))
	}

	fn request_mutation(&self, request: Vec<u8>, response_budget: usize) -> io::Result<ResponseDecoder> {
		// A dispatched JavaScript mutation cannot be canceled, so wait for its definitive result.
		self.request_with_deadline(request, response_budget, None)
	}

	fn request_with_deadline(
		&self,
		request: Vec<u8>,
		response_budget: usize,
		deadline: Option<Instant>,
	) -> io::Result<ResponseDecoder> {
		let response = self.transport.round_trip(request, response_budget, deadline)?;
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

fn registry() -> std::sync::MutexGuard<'static, HashMap<u32, Arc<HostTransport>>> {
	HOST_TRANSPORTS
		.get_or_init(Default::default)
		.lock()
		.unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn completion(callback: JsFunction) -> boundary::Result<CompletionCallback> {
	callback
		.create_threadsafe_function::<Vec<u8>, Buffer, _, ErrorStrategy::Fatal>(
			0,
			|context: ThreadSafeCallContext<Vec<u8>>| Ok(vec![Buffer::from(context.value)]),
		)
		.map_err(|error| napi::Error::new("E_NATIVE_FAILURE", error.to_string()))
}

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

fn test_thread_result(operation: impl FnOnce() -> io::Result<Vec<u8>>) -> Vec<u8> {
	match catch_unwind(AssertUnwindSafe(operation)) {
		Ok(result) => test_result(result),
		Err(_) => test_result(Err(io::Error::other("native host storage test panicked"))),
	}
}

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
					transport.round_trip(request, response_budget as usize, deadline)
				});
				let _ = completion.call(result, ThreadsafeFunctionCallMode::NonBlocking);
			})
			.map_err(|error| napi::Error::new("E_NATIVE_FAILURE", error.to_string()))?;
		Ok(())
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
	fn closing_wakes_every_pending_request() {
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
}
