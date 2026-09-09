use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::io;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
	Directory, DirectoryLock, FileHandle, Lock, OwnedBytes, TerminatingWrite, WatchCallback, WatchCallbackList,
	WatchHandle, WritePtr,
};
use tantivy::HasLen;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WritePolicy {
	pub wal_enabled: bool,
	pub sync: bool,
}

impl WritePolicy {
	pub const WAL: Self = Self {
		wal_enabled: true,
		sync: false,
	};
	pub const WAL_SYNC: Self = Self {
		wal_enabled: true,
		sync: true,
	};
	pub const NO_WAL: Self = Self {
		wal_enabled: false,
		sync: false,
	};
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mutation {
	Put(Vec<u8>, Vec<u8>),
	Delete(Vec<u8>),
}

/// Identifies one storage incarnation; providers must mint a new value after close, restore, or replacement.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct KvStoreIdentity(pub u64, pub u64, pub u64);

pub trait KvStore: Clone + Send + Sync + 'static {
	fn identity(&self) -> KvStoreIdentity;
	fn read(&self, key: &[u8]) -> io::Result<Option<OwnedBytes>>;
	fn write(&self, mutations: &[Mutation], policy: WritePolicy) -> io::Result<()>;
	fn sync(&self) -> io::Result<()>;
}

pub(crate) const CHUNK_SIZE: usize = 256 * 1024;

impl Mutation {
	fn key(&self) -> &[u8] {
		match self {
			Self::Put(key, _) | Self::Delete(key) => key,
		}
	}

	fn value(&self) -> Option<Vec<u8>> {
		match self {
			Self::Put(_, value) => Some(value.clone()),
			Self::Delete(_) => None,
		}
	}
}

#[derive(Clone, Debug)]
struct VersionedValue {
	sequence: u64,
	value: Option<Vec<u8>>,
}

#[derive(Debug, Default)]
struct State {
	next_sequence: u64,
	visible: BTreeMap<Vec<u8>, VersionedValue>,
	durable: BTreeMap<Vec<u8>, VersionedValue>,
	pending_wal: Vec<Vec<(Vec<u8>, VersionedValue)>>,
	fail_next_write: bool,
	fail_after_next_write: bool,
	fail_next_flush: bool,
}

static NEXT_FAULTING_KV_IDENTITY: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct FaultingKv {
	identity: u64,
	state: Arc<Mutex<State>>,
}

impl Default for FaultingKv {
	fn default() -> Self {
		Self {
			identity: next_faulting_kv_identity(),
			state: Default::default(),
		}
	}
}

#[derive(Debug)]
pub struct FlushBarrier {
	snapshot: BTreeMap<Vec<u8>, VersionedValue>,
}

impl FaultingKv {
	pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
		self.state
			.lock()
			.unwrap()
			.visible
			.get(key)
			.and_then(|entry| entry.value.clone())
	}

	pub fn get_durable(&self, key: &[u8]) -> Option<Vec<u8>> {
		self.state
			.lock()
			.unwrap()
			.durable
			.get(key)
			.and_then(|entry| entry.value.clone())
	}

	pub fn write(&self, mutations: &[Mutation], policy: WritePolicy) -> io::Result<()> {
		if policy.sync && !policy.wal_enabled {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"a synchronous write must use the WAL",
			));
		}
		let mut state = self.state.lock().unwrap();
		if std::mem::take(&mut state.fail_next_write) {
			return Err(io::Error::other("injected write failure"));
		}
		let fail_after_write = std::mem::take(&mut state.fail_after_next_write);
		let mut wal_batch = Vec::with_capacity(mutations.len());
		for mutation in mutations {
			state.next_sequence += 1;
			let entry = VersionedValue {
				sequence: state.next_sequence,
				value: mutation.value(),
			};
			let key = mutation.key().to_vec();
			state.visible.insert(key.clone(), entry.clone());
			if policy.wal_enabled {
				wal_batch.push((key, entry));
			}
		}
		if !wal_batch.is_empty() {
			state.pending_wal.push(wal_batch);
		}
		if policy.sync {
			let pending = std::mem::take(&mut state.pending_wal);
			for batch in pending {
				for (key, entry) in batch {
					apply_if_newer(&mut state.durable, key, entry);
				}
			}
		}
		if fail_after_write {
			Err(io::Error::other("injected post-commit write failure"))
		} else {
			Ok(())
		}
	}

	pub fn begin_flush(&self) -> FlushBarrier {
		FlushBarrier {
			snapshot: self.state.lock().unwrap().visible.clone(),
		}
	}

	pub fn complete_flush(&self, barrier: FlushBarrier) -> io::Result<()> {
		let mut state = self.state.lock().unwrap();
		if std::mem::take(&mut state.fail_next_flush) {
			return Err(io::Error::other("injected flush failure"));
		}
		for (key, entry) in barrier.snapshot {
			apply_if_newer(&mut state.durable, key, entry);
		}
		Ok(())
	}

	pub fn crash(&self) -> Self {
		let durable = self.state.lock().unwrap().durable.clone();
		Self {
			identity: next_faulting_kv_identity(),
			state: Arc::new(Mutex::new(State {
				next_sequence: durable.values().map(|entry| entry.sequence).max().unwrap_or(0),
				visible: durable.clone(),
				durable,
				pending_wal: Vec::new(),
				fail_next_write: false,
				fail_after_next_write: false,
				fail_next_flush: false,
			})),
		}
	}

	pub fn fail_next_write(&self) {
		self.state.lock().unwrap().fail_next_write = true;
	}

	pub fn fail_after_next_write(&self) {
		self.state.lock().unwrap().fail_after_next_write = true;
	}

	pub fn fail_next_flush(&self) {
		self.state.lock().unwrap().fail_next_flush = true;
	}
}

impl KvStore for FaultingKv {
	fn identity(&self) -> KvStoreIdentity {
		KvStoreIdentity(0, self.identity, 0)
	}

	fn read(&self, key: &[u8]) -> io::Result<Option<OwnedBytes>> {
		Ok(self.get(key).map(OwnedBytes::new))
	}

	fn write(&self, mutations: &[Mutation], policy: WritePolicy) -> io::Result<()> {
		FaultingKv::write(self, mutations, policy)
	}

	fn sync(&self) -> io::Result<()> {
		Ok(())
	}
}

fn next_faulting_kv_identity() -> u64 {
	let identity = NEXT_FAULTING_KV_IDENTITY.fetch_add(1, Ordering::Relaxed);
	assert_ne!(identity, 0, "FaultingKv identity space exhausted");
	identity
}

fn apply_if_newer(entries: &mut BTreeMap<Vec<u8>, VersionedValue>, key: Vec<u8>, candidate: VersionedValue) {
	if entries
		.get(&key)
		.is_none_or(|existing| existing.sequence <= candidate.sequence)
	{
		entries.insert(key, candidate);
	}
}

#[derive(Clone)]
pub struct KvDirectory<S> {
	store: S,
	namespace: Arc<[u8]>,
	format: Arc<FormatValidation>,
	state: Arc<DirectoryState>,
}

pub type FaultingDirectory = KvDirectory<FaultingKv>;

struct DirectoryState {
	mutation: Mutex<()>,
	locks: Mutex<DirectoryLocks>,
	locks_changed: Condvar,
	watches: WatchCallbackList,
}

#[derive(Default)]
struct DirectoryLocks {
	held: HashSet<PathBuf>,
	waiters: HashMap<PathBuf, VecDeque<u64>>,
	next_ticket: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DirectoryIdentity {
	store: KvStoreIdentity,
	namespace: Vec<u8>,
}

static DIRECTORY_STATES: OnceLock<Mutex<HashMap<DirectoryIdentity, Weak<DirectoryState>>>> = OnceLock::new();

struct FormatValidation {
	state: AtomicU8,
}

impl Default for FormatValidation {
	fn default() -> Self {
		Self {
			state: AtomicU8::new(FORMAT_UNKNOWN),
		}
	}
}

impl FormatValidation {
	fn record(&self, state: u8) {
		self.state.fetch_max(state, Ordering::AcqRel);
	}
}

impl<S> fmt::Debug for KvDirectory<S> {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("KvDirectory")
	}
}

impl<S: KvStore> KvDirectory<S> {
	pub fn new(store: S) -> Self {
		Self::with_namespace(store, b"phase0")
	}

	pub fn with_namespace(store: S, namespace: &[u8]) -> Self {
		let identity = DirectoryIdentity {
			store: store.identity(),
			namespace: namespace.to_vec(),
		};
		let mut states = DIRECTORY_STATES
			.get_or_init(Default::default)
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		states.retain(|_, state| state.strong_count() != 0);
		let state = states.get(&identity).and_then(Weak::upgrade).unwrap_or_else(|| {
			let state = Arc::new(DirectoryState {
				mutation: Mutex::new(()),
				locks: Mutex::new(DirectoryLocks::default()),
				locks_changed: Condvar::new(),
				watches: WatchCallbackList::default(),
			});
			states.insert(identity, Arc::downgrade(&state));
			state
		});
		Self {
			store,
			namespace: Arc::from(namespace),
			format: Arc::new(FormatValidation::default()),
			state,
		}
	}

	fn ensure_format(&self, create: bool) -> io::Result<()> {
		let state = self.format.state.load(Ordering::Acquire);
		if state == FORMAT_PRESENT {
			return Ok(());
		}
		if state == FORMAT_ABSENT && !create {
			if let Some(state) = read_format_marker(&self.store, &format_marker_key(&self.namespace))? {
				self.format.record(state);
			}
			return Ok(());
		}
		let state = validate_format(&self.store, &self.namespace, create)?;
		self.format.record(state);
		Ok(())
	}

	fn read_binding(&self, path: &Path) -> Result<Binding, OpenReadError>
	where
		S: KvStore,
	{
		self.ensure_format(false)
			.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?;
		let bytes = self
			.store
			.read(&binding_key(&self.namespace, path))
			.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?
			.ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
		decode_binding(&bytes).map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))
	}
}

struct KvFileHandle<S> {
	store: S,
	namespace: Arc<[u8]>,
	path: PathBuf,
	binding: Binding,
}

impl<S> fmt::Debug for KvFileHandle<S> {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.debug_tuple("KvFileHandle").field(&self.path).finish()
	}
}

impl<S> HasLen for KvFileHandle<S> {
	fn len(&self) -> usize {
		self.binding.visible_length
	}
}

#[async_trait]
impl<S: KvStore> FileHandle for KvFileHandle<S> {
	fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
		if range.start > range.end || range.end > self.binding.visible_length {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"read range is outside the file",
			));
		}
		if range.is_empty() {
			return Ok(OwnedBytes::empty());
		}

		let full_length = usize::try_from(self.binding.full_chunks)
			.ok()
			.and_then(|chunks| chunks.checked_mul(CHUNK_SIZE))
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "binding chunk length overflow"))?;
		let first_chunk = range.start / CHUNK_SIZE;
		let last_chunk = (range.end - 1) / CHUNK_SIZE;
		if first_chunk == last_chunk && range.end <= full_length {
			let value = self.read_chunk(first_chunk)?;
			let end = match range.end % CHUNK_SIZE {
				0 => CHUNK_SIZE,
				end => end,
			};
			return Ok(value.slice(range.start % CHUNK_SIZE..end));
		}
		if range.start >= full_length {
			let tail = self.read_tail()?;
			return Ok(tail.slice(range.start - full_length..range.end - full_length));
		}

		let mut copied = Vec::new();
		copied
			.try_reserve(range.len())
			.map_err(|_| io::Error::other("requested file range cannot be allocated"))?;
		let full_chunk_limit = last_chunk.saturating_add(1).min(self.binding.full_chunks as usize);
		for chunk in first_chunk..full_chunk_limit {
			let value = self.read_chunk(chunk)?;
			let chunk_start = chunk * CHUNK_SIZE;
			let start = range.start.saturating_sub(chunk_start);
			let end = value.len().min(range.end - chunk_start);
			copied.extend_from_slice(&value[start..end]);
		}
		if range.end > full_length {
			let tail = self.read_tail()?;
			copied.extend_from_slice(&tail[..range.end - full_length]);
		}
		if copied.len() != range.len() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"binding length does not match its chunks",
			));
		}
		Ok(OwnedBytes::new(copied))
	}

	async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
		self.read_bytes(range)
	}
}

impl<S: KvStore> KvFileHandle<S> {
	fn read_chunk(&self, chunk: usize) -> io::Result<OwnedBytes> {
		let chunk = u32::try_from(chunk)
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "chunk index exceeds its format"))?;
		let value = self
			.store
			.read(&chunk_key(&self.namespace, self.binding.object_id, chunk))?
			.ok_or_else(|| io::Error::other("binding references a missing chunk"))?;
		if value.len() != CHUNK_SIZE {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"full chunk has an invalid length",
			));
		}
		Ok(value)
	}

	fn read_tail(&self) -> io::Result<OwnedBytes> {
		if self.binding.tail_length == 0 {
			return Err(io::Error::new(io::ErrorKind::InvalidData, "binding has no tail"));
		}
		let value = self
			.store
			.read(&tail_key(
				&self.namespace,
				self.binding.object_id,
				self.binding.tail_revision,
			))?
			.ok_or_else(|| io::Error::other("binding references a missing tail"))?;
		if value.len() != self.binding.tail_length as usize {
			return Err(io::Error::new(io::ErrorKind::InvalidData, "tail has an invalid length"));
		}
		Ok(value)
	}
}

struct KvDirectoryLock {
	state: Arc<DirectoryState>,
	path: PathBuf,
}

impl Drop for KvDirectoryLock {
	fn drop(&mut self) {
		let mut locks = self.state.locks.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		locks.held.remove(&self.path);
		self.state.locks_changed.notify_all();
	}
}

impl<S: KvStore> Directory for KvDirectory<S> {
	fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
		Ok(Arc::new(KvFileHandle {
			store: self.store.clone(),
			namespace: self.namespace.clone(),
			path: path.to_path_buf(),
			binding: self.read_binding(path)?,
		}))
	}

	fn delete(&self, path: &Path) -> Result<(), DeleteError> {
		self.ensure_format(false).map_err(|error| DeleteError::IoError {
			io_error: Arc::new(error),
			filepath: path.to_path_buf(),
		})?;
		let _mutation = self
			.state
			.mutation
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		let binding = binding_key(&self.namespace, path);
		let atomic = atomic_key(&self.namespace, path);
		if self
			.store
			.read(&binding)
			.map_err(|error| DeleteError::IoError {
				io_error: Arc::new(error),
				filepath: path.to_path_buf(),
			})?
			.is_none()
			&& self
				.store
				.read(&atomic)
				.map_err(|error| DeleteError::IoError {
					io_error: Arc::new(error),
					filepath: path.to_path_buf(),
				})?
				.is_none()
		{
			return Err(DeleteError::FileDoesNotExist(path.to_path_buf()));
		}
		self.store
			.write(&[Mutation::Delete(binding), Mutation::Delete(atomic)], WritePolicy::WAL)
			.map_err(|error| DeleteError::IoError {
				io_error: Arc::new(error),
				filepath: path.to_path_buf(),
			})
	}

	fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
		self.ensure_format(false)
			.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?;
		Ok(self
			.store
			.read(&binding_key(&self.namespace, path))
			.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?
			.is_some()
			|| self
				.store
				.read(&atomic_key(&self.namespace, path))
				.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?
				.is_some())
	}

	fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
		let _mutation = self
			.state
			.mutation
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		self.ensure_format(true)
			.map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?;
		let key = binding_key(&self.namespace, path);
		if self
			.store
			.read(&key)
			.map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?
			.is_some()
			|| self
				.store
				.read(&atomic_key(&self.namespace, path))
				.map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?
				.is_some()
		{
			return Err(OpenWriteError::FileAlreadyExists(path.to_path_buf()));
		}
		let counter = self
			.store
			.read(&counter_key(&self.namespace))
			.map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?
			.map(|bytes| decode_u64(&bytes))
			.transpose()
			.map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?
			.unwrap_or(0);
		let object_id = counter.checked_add(1).ok_or_else(|| {
			OpenWriteError::wrap_io_error(io::Error::other("object id exhausted"), path.to_path_buf())
		})?;
		let binding = Binding {
			object_id,
			full_chunks: 0,
			tail_revision: 0,
			tail_length: 0,
			visible_length: 0,
			chunk_high_water: 0,
			tail_high_water: 0,
		};
		self.store
			.write(
				&[
					Mutation::Put(counter_key(&self.namespace), object_id.to_be_bytes().to_vec()),
					Mutation::Put(key, encode_binding(&binding)),
				],
				WritePolicy::WAL,
			)
			.map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?;
		Ok(std::io::BufWriter::new(Box::new(KvWriter {
			store: self.store.clone(),
			state: self.state.clone(),
			namespace: self.namespace.clone(),
			path: path.to_path_buf(),
			binding,
			tail: Vec::new(),
			staged_full_chunks: 0,
			dirty: false,
			pending_publication: None,
		})))
	}

	fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
		self.ensure_format(false)
			.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?;
		self.store
			.read(&atomic_key(&self.namespace, path))
			.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?
			.map(|bytes| bytes.as_slice().to_vec())
			.ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
	}

	fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
		{
			let _mutation = self
				.state
				.mutation
				.lock()
				.unwrap_or_else(|poisoned| poisoned.into_inner());
			self.ensure_format(true)?;
			self.store.write(
				&[Mutation::Put(atomic_key(&self.namespace, path), data.to_vec())],
				WritePolicy::WAL_SYNC,
			)?;
		}
		if path == Path::new("meta.json") {
			drop(self.state.watches.broadcast());
		}
		Ok(())
	}

	fn sync_directory(&self) -> io::Result<()> {
		self.ensure_format(false)?;
		self.store.sync()
	}

	fn watch(&self, callback: WatchCallback) -> tantivy::Result<WatchHandle> {
		Ok(self.state.watches.subscribe(callback))
	}

	fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
		let mut locks = self
			.state
			.locks
			.lock()
			.map_err(|_| LockError::wrap_io_error(io::Error::other("directory lock state is poisoned")))?;
		if !lock.is_blocking {
			if locks.held.contains(&lock.filepath)
				|| locks
					.waiters
					.get(&lock.filepath)
					.is_some_and(|waiters| !waiters.is_empty())
			{
				return Err(LockError::LockBusy);
			}
			locks.held.insert(lock.filepath.clone());
			return Ok(DirectoryLock::from(Box::new(KvDirectoryLock {
				state: self.state.clone(),
				path: lock.filepath.clone(),
			})));
		}

		let ticket = locks.next_ticket;
		locks.next_ticket = locks
			.next_ticket
			.checked_add(1)
			.ok_or_else(|| LockError::wrap_io_error(io::Error::other("directory lock ticket exhausted")))?;
		locks
			.waiters
			.entry(lock.filepath.clone())
			.or_default()
			.push_back(ticket);
		let deadline = Instant::now() + Duration::from_secs(10);
		loop {
			let is_front = locks
				.waiters
				.get(&lock.filepath)
				.and_then(|waiters| waiters.front())
				.is_some_and(|front| *front == ticket);
			if is_front && !locks.held.contains(&lock.filepath) {
				if let Some(waiters) = locks.waiters.get_mut(&lock.filepath) {
					waiters.pop_front();
					if waiters.is_empty() {
						locks.waiters.remove(&lock.filepath);
					}
				}
				locks.held.insert(lock.filepath.clone());
				return Ok(DirectoryLock::from(Box::new(KvDirectoryLock {
					state: self.state.clone(),
					path: lock.filepath.clone(),
				})));
			}
			let remaining = deadline.saturating_duration_since(Instant::now());
			if remaining.is_zero() {
				remove_waiter(&mut locks, &lock.filepath, ticket);
				self.state.locks_changed.notify_all();
				return Err(LockError::LockBusy);
			}
			let (next, timeout) = match self.state.locks_changed.wait_timeout(locks, remaining) {
				Ok(result) => result,
				Err(poisoned) => {
					let (mut locks, _) = poisoned.into_inner();
					remove_waiter(&mut locks, &lock.filepath, ticket);
					self.state.locks_changed.notify_all();
					return Err(LockError::wrap_io_error(io::Error::other(
						"directory lock state is poisoned",
					)));
				}
			};
			locks = next;
			if timeout.timed_out() {
				remove_waiter(&mut locks, &lock.filepath, ticket);
				self.state.locks_changed.notify_all();
				return Err(LockError::LockBusy);
			}
		}
	}
}

fn remove_waiter(locks: &mut DirectoryLocks, path: &Path, ticket: u64) {
	if let Some(waiters) = locks.waiters.get_mut(path) {
		waiters.retain(|waiter| *waiter != ticket);
		if waiters.is_empty() {
			locks.waiters.remove(path);
		}
	}
}

struct KvWriter<S> {
	store: S,
	state: Arc<DirectoryState>,
	namespace: Arc<[u8]>,
	path: PathBuf,
	binding: Binding,
	tail: Vec<u8>,
	staged_full_chunks: u32,
	dirty: bool,
	pending_publication: Option<Binding>,
}

impl<S: KvStore> Write for KvWriter<S> {
	fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
		if bytes.is_empty() {
			return Ok(0);
		}
		self.reconcile_pending_publication()?;
		self.stage_full_tail()?;
		let accepted = bytes.len().min(CHUNK_SIZE - self.tail.len());
		self.tail
			.try_reserve(accepted)
			.map_err(|_| io::Error::other("writer buffer cannot be allocated"))?;
		self.tail.extend_from_slice(&bytes[..accepted]);
		self.dirty = true;
		Ok(accepted)
	}

	fn flush(&mut self) -> io::Result<()> {
		self.reconcile_pending_publication()?;
		if !self.dirty {
			return Ok(());
		}
		self.stage_full_tail()?;
		let state = self.state.clone();
		let _mutation = state.mutation.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		let current = self.current_binding()?;
		let expected = self.next_binding()?;
		if current == expected {
			self.finish_flush(expected);
			return Ok(());
		}
		self.adopt_high_waters(&current)?;
		let next = self.next_binding()?;
		let binding = Mutation::Put(binding_key(&self.namespace, &self.path), encode_binding(&next));
		self.pending_publication = Some(next.clone());
		let result = if self.tail.is_empty() {
			self.store.write(&[binding], WritePolicy::WAL)
		} else {
			self.store.write(
				&[
					Mutation::Put(
						tail_key(&self.namespace, self.binding.object_id, next.tail_revision),
						self.tail.clone(),
					),
					binding,
				],
				WritePolicy::WAL,
			)
		};
		result?;
		self.finish_flush(next);
		Ok(())
	}
}

impl<S: KvStore> KvWriter<S> {
	fn reconcile_pending_publication(&mut self) -> io::Result<()> {
		let Some(pending) = self.pending_publication.clone() else {
			return Ok(());
		};
		let state = self.state.clone();
		let _mutation = state.mutation.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		let current = self.current_binding()?;
		if current == pending {
			self.finish_flush(pending);
			return Ok(());
		}
		self.adopt_high_waters(&current)?;
		self.pending_publication = None;
		Ok(())
	}

	fn current_binding(&self) -> io::Result<Binding> {
		let current = self
			.store
			.read(&binding_key(&self.namespace, &self.path))?
			.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file was deleted while its writer was open"))?;
		decode_binding(&current)
	}

	fn adopt_high_waters(&mut self, current: &Binding) -> io::Result<()> {
		if !current.same_published_state(&self.binding) {
			return Err(io::Error::new(
				io::ErrorKind::NotFound,
				"file was replaced while its writer was open",
			));
		}
		if current.chunk_high_water < self.binding.chunk_high_water
			|| current.tail_high_water < self.binding.tail_high_water
		{
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"binding high-water moved backward",
			));
		}
		self.binding.chunk_high_water = current.chunk_high_water;
		self.binding.tail_high_water = current.tail_high_water;
		Ok(())
	}

	fn next_binding(&self) -> io::Result<Binding> {
		let full_chunks = self
			.binding
			.full_chunks
			.checked_add(self.staged_full_chunks)
			.ok_or_else(|| io::Error::other("chunk count exhausted"))?;
		let visible_length = usize::try_from(full_chunks)
			.ok()
			.and_then(|chunks| chunks.checked_mul(CHUNK_SIZE))
			.and_then(|length| length.checked_add(self.tail.len()))
			.ok_or_else(|| io::Error::other("visible length exhausted"))?;
		let tail_revision = self
			.binding
			.tail_high_water
			.checked_add(1)
			.ok_or_else(|| io::Error::other("tail revision exhausted"))?;
		let next = Binding {
			object_id: self.binding.object_id,
			full_chunks,
			tail_revision,
			tail_length: u32::try_from(self.tail.len())
				.map_err(|_| io::Error::other("tail length exceeds its format"))?,
			visible_length,
			chunk_high_water: self.binding.chunk_high_water,
			tail_high_water: tail_revision,
		};
		validate_binding(&next)?;
		Ok(next)
	}

	fn finish_flush(&mut self, next: Binding) {
		self.binding = next;
		self.staged_full_chunks = 0;
		self.dirty = false;
		self.pending_publication = None;
	}

	fn stage_full_tail(&mut self) -> io::Result<()> {
		if self.tail.len() != CHUNK_SIZE {
			return Ok(());
		}
		let chunk = self
			.binding
			.full_chunks
			.checked_add(self.staged_full_chunks)
			.ok_or_else(|| io::Error::other("chunk count exhausted"))?;
		self.reserve_chunk(chunk)?;
		let mutation = Mutation::Put(
			chunk_key(&self.namespace, self.binding.object_id, chunk),
			std::mem::take(&mut self.tail),
		);
		let result = self.store.write(std::slice::from_ref(&mutation), WritePolicy::WAL);
		let Mutation::Put(_, mut value) = mutation else {
			unreachable!();
		};
		if let Err(error) = result {
			self.tail = value;
			return Err(error);
		}
		value.clear();
		self.tail = value;
		self.staged_full_chunks += 1;
		Ok(())
	}

	fn reserve_chunk(&mut self, chunk: u32) -> io::Result<()> {
		if chunk < self.binding.chunk_high_water {
			return Ok(());
		}
		let state = self.state.clone();
		let _mutation = state.mutation.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		let current = self.current_binding()?;
		self.adopt_high_waters(&current)?;
		if chunk < self.binding.chunk_high_water {
			return Ok(());
		}
		if chunk != self.binding.chunk_high_water {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"chunk reservation is not contiguous",
			));
		}
		let tail_extent = self.binding.tail_high_water as u128 * (CHUNK_SIZE - 1) as u128;
		let available = MAX_OBJECT_EXTENT_BYTES
			.checked_sub(tail_extent)
			.ok_or_else(|| io::Error::other("object physical extent exhausted"))?;
		let max_high_water = u32::try_from(available / CHUNK_SIZE as u128)
			.map_err(|_| io::Error::other("chunk reservation exceeds its format"))?;
		let desired = chunk.saturating_add(CHUNK_RESERVATION_STRIDE).min(max_high_water);
		if desired <= chunk {
			return Err(io::Error::other("object physical extent exhausted"));
		}
		let mut reserved = self.binding.clone();
		reserved.chunk_high_water = desired;
		validate_binding(&reserved)?;
		let result = self.store.write(
			&[Mutation::Put(
				binding_key(&self.namespace, &self.path),
				encode_binding(&reserved),
			)],
			WritePolicy::WAL,
		);
		if result.is_ok() {
			self.binding = reserved;
		}
		result
	}
}

impl<S: KvStore> TerminatingWrite for KvWriter<S> {
	fn terminate_ref(&mut self, _: tantivy::directory::AntiCallToken) -> io::Result<()> {
		self.flush()
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Binding {
	object_id: u64,
	full_chunks: u32,
	tail_revision: u64,
	tail_length: u32,
	visible_length: usize,
	chunk_high_water: u32,
	tail_high_water: u64,
}

impl Binding {
	fn same_published_state(&self, other: &Self) -> bool {
		self.object_id == other.object_id
			&& self.full_chunks == other.full_chunks
			&& self.tail_revision == other.tail_revision
			&& self.tail_length == other.tail_length
			&& self.visible_length == other.visible_length
	}
}

fn counter_key(namespace: &[u8]) -> Vec<u8> {
	namespaced_prefix(namespace, KEY_KIND_COUNTER)
}

fn binding_key(namespace: &[u8], path: &Path) -> Vec<u8> {
	prefixed_path(&namespaced_prefix(namespace, KEY_KIND_BINDING), path)
}

fn atomic_key(namespace: &[u8], path: &Path) -> Vec<u8> {
	prefixed_path(&namespaced_prefix(namespace, KEY_KIND_ATOMIC), path)
}

const KEY_FORMAT_VERSION: u8 = 2;
const BINDING_FORMAT_VERSION: u8 = 3;
const CHUNK_RESERVATION_STRIDE: u32 = 64;
const MAX_OBJECT_EXTENT_BYTES: u128 = 1 << 40;
const KEY_KIND_COUNTER: u8 = 1;
const KEY_KIND_BINDING: u8 = 2;
const KEY_KIND_ATOMIC: u8 = 3;
const KEY_KIND_CHUNK: u8 = 4;
const KEY_KIND_TAIL: u8 = 5;
const KEY_PREFIX: &[u8; 4] = b"HFTK";
const FORMAT_MARKER_PREFIX: &[u8; 4] = b"HFTM";
const FORMAT_UNKNOWN: u8 = 0;
const FORMAT_ABSENT: u8 = 1;
const FORMAT_PRESENT: u8 = 2;

fn namespaced_prefix(namespace: &[u8], kind: u8) -> Vec<u8> {
	namespaced_prefix_with_capacity(namespace, kind, 0)
}

fn format_marker_key(namespace: &[u8]) -> Vec<u8> {
	let mut key = Vec::with_capacity(FORMAT_MARKER_PREFIX.len() + 8 + namespace.len());
	key.extend_from_slice(FORMAT_MARKER_PREFIX);
	key.extend_from_slice(&(namespace.len() as u64).to_be_bytes());
	key.extend_from_slice(namespace);
	key
}

fn validate_format<S: KvStore>(store: &S, namespace: &[u8], create: bool) -> io::Result<u8> {
	let marker = format_marker_key(namespace);
	if let Some(state) = read_format_marker(store, &marker)? {
		return Ok(state);
	}
	for legacy_key in legacy_sentinel_keys(namespace) {
		if store.read(&legacy_key)?.is_some() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"the namespace contains the unsupported prototype directory format",
			));
		}
	}
	if !create {
		return Ok(FORMAT_ABSENT);
	}
	store.write(
		&[Mutation::Put(marker, vec![KEY_FORMAT_VERSION])],
		WritePolicy::WAL_SYNC,
	)?;
	Ok(FORMAT_PRESENT)
}

fn read_format_marker<S: KvStore>(store: &S, marker: &[u8]) -> io::Result<Option<u8>> {
	let Some(value) = store.read(marker)? else {
		return Ok(None);
	};
	match value.as_slice() {
		[KEY_FORMAT_VERSION] => Ok(Some(FORMAT_PRESENT)),
		[version] => Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("unsupported directory format version {version}"),
		)),
		_ => Err(io::Error::new(
			io::ErrorKind::InvalidData,
			"malformed directory format marker",
		)),
	}
}

fn legacy_sentinel_keys(namespace: &[u8]) -> [Vec<u8>; 3] {
	[
		legacy_namespaced_key(namespace, b"counter"),
		legacy_namespaced_key(namespace, b"atomic/.managed.json"),
		legacy_namespaced_key(namespace, b"atomic/meta.json"),
	]
}

fn legacy_namespaced_key(namespace: &[u8], suffix: &[u8]) -> Vec<u8> {
	let mut key = Vec::with_capacity(namespace.len() + suffix.len() + 1);
	key.extend_from_slice(namespace);
	key.push(b'/');
	key.extend_from_slice(suffix);
	key
}

fn namespaced_prefix_with_capacity(namespace: &[u8], kind: u8, additional: usize) -> Vec<u8> {
	let mut prefix = Vec::with_capacity(KEY_PREFIX.len() + 10 + namespace.len() + additional);
	prefix.extend_from_slice(KEY_PREFIX);
	prefix.push(KEY_FORMAT_VERSION);
	prefix.extend_from_slice(&(namespace.len() as u64).to_be_bytes());
	prefix.extend_from_slice(namespace);
	prefix.push(kind);
	prefix
}

fn prefixed_path(prefix: &[u8], path: &Path) -> Vec<u8> {
	let mut key = Vec::with_capacity(prefix.len() + path.as_os_str().as_encoded_bytes().len());
	key.extend_from_slice(prefix);
	key.extend_from_slice(path.as_os_str().as_encoded_bytes());
	key
}

fn chunk_key(namespace: &[u8], object_id: u64, chunk: u32) -> Vec<u8> {
	let mut key = namespaced_prefix_with_capacity(namespace, KEY_KIND_CHUNK, 12);
	key.extend_from_slice(&object_id.to_be_bytes());
	key.extend_from_slice(&chunk.to_be_bytes());
	key
}

fn tail_key(namespace: &[u8], object_id: u64, revision: u64) -> Vec<u8> {
	let mut key = namespaced_prefix_with_capacity(namespace, KEY_KIND_TAIL, 16);
	key.extend_from_slice(&object_id.to_be_bytes());
	key.extend_from_slice(&revision.to_be_bytes());
	key
}

fn encode_binding(binding: &Binding) -> Vec<u8> {
	let mut bytes = Vec::with_capacity(45);
	bytes.push(BINDING_FORMAT_VERSION);
	bytes.extend_from_slice(&binding.object_id.to_be_bytes());
	bytes.extend_from_slice(&binding.full_chunks.to_be_bytes());
	bytes.extend_from_slice(&binding.tail_revision.to_be_bytes());
	bytes.extend_from_slice(&binding.tail_length.to_be_bytes());
	bytes.extend_from_slice(&(binding.visible_length as u64).to_be_bytes());
	bytes.extend_from_slice(&binding.chunk_high_water.to_be_bytes());
	bytes.extend_from_slice(&binding.tail_high_water.to_be_bytes());
	bytes
}

fn decode_binding(bytes: &[u8]) -> io::Result<Binding> {
	if bytes.first().copied() != Some(BINDING_FORMAT_VERSION) {
		let version = bytes.first().copied().unwrap_or(0);
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("unsupported binding format version {version}"),
		));
	}
	if bytes.len() != 45 {
		return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed binding"));
	}
	let visible_length = usize::try_from(decode_u64(&bytes[25..33])?)
		.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "binding length exceeds this platform"))?;
	let binding = Binding {
		object_id: decode_u64(&bytes[1..9])?,
		full_chunks: u32::from_be_bytes(
			bytes[9..13]
				.try_into()
				.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid chunk count"))?,
		),
		tail_revision: decode_u64(&bytes[13..21])?,
		tail_length: u32::from_be_bytes(
			bytes[21..25]
				.try_into()
				.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid tail length"))?,
		),
		visible_length,
		chunk_high_water: u32::from_be_bytes(
			bytes[33..37]
				.try_into()
				.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid chunk high-water"))?,
		),
		tail_high_water: decode_u64(&bytes[37..45])?,
	};
	validate_binding(&binding)?;
	Ok(binding)
}

fn validate_binding(binding: &Binding) -> io::Result<()> {
	let expected_length = usize::try_from(binding.full_chunks)
		.ok()
		.and_then(|chunks| chunks.checked_mul(CHUNK_SIZE))
		.and_then(|length| length.checked_add(binding.tail_length as usize));
	if binding.tail_length as usize >= CHUNK_SIZE || expected_length != Some(binding.visible_length) {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			"binding length is inconsistent",
		));
	}
	if binding.chunk_high_water < binding.full_chunks || binding.tail_high_water < binding.tail_revision {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			"binding high-water is below published state",
		));
	}
	let possible_extent = binding.chunk_high_water as u128 * CHUNK_SIZE as u128
		+ binding.tail_high_water as u128 * (CHUNK_SIZE - 1) as u128;
	if possible_extent > MAX_OBJECT_EXTENT_BYTES {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			"binding physical extent exceeds the format limit",
		));
	}
	Ok(())
}

fn decode_u64(bytes: &[u8]) -> io::Result<u64> {
	Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
		io::Error::new(io::ErrorKind::InvalidData, "invalid u64")
	})?))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::directory_harness::{verify_directory_contract, verify_tantivy_lifecycle};
	use std::sync::atomic::AtomicUsize;

	#[derive(Clone)]
	struct CountingKv {
		inner: FaultingKv,
		reads: Arc<AtomicUsize>,
		writes: Arc<AtomicUsize>,
		mutations: Arc<AtomicUsize>,
	}

	#[derive(Clone)]
	struct FixedIdentityKv {
		inner: FaultingKv,
		identity: KvStoreIdentity,
	}

	impl CountingKv {
		fn new() -> Self {
			Self {
				inner: FaultingKv::default(),
				reads: Arc::new(AtomicUsize::new(0)),
				writes: Arc::new(AtomicUsize::new(0)),
				mutations: Arc::new(AtomicUsize::new(0)),
			}
		}

		fn take_reads(&self) -> usize {
			self.reads.swap(0, Ordering::Relaxed)
		}

		fn take_io_counts(&self) -> (usize, usize, usize) {
			(
				self.reads.swap(0, Ordering::Relaxed),
				self.writes.swap(0, Ordering::Relaxed),
				self.mutations.swap(0, Ordering::Relaxed),
			)
		}
	}

	impl KvStore for CountingKv {
		fn identity(&self) -> KvStoreIdentity {
			self.inner.identity()
		}

		fn read(&self, key: &[u8]) -> io::Result<Option<OwnedBytes>> {
			self.reads.fetch_add(1, Ordering::Relaxed);
			KvStore::read(&self.inner, key)
		}

		fn write(&self, mutations: &[Mutation], policy: WritePolicy) -> io::Result<()> {
			self.writes.fetch_add(1, Ordering::Relaxed);
			self.mutations.fetch_add(mutations.len(), Ordering::Relaxed);
			KvStore::write(&self.inner, mutations, policy)
		}

		fn sync(&self) -> io::Result<()> {
			KvStore::sync(&self.inner)
		}
	}

	impl KvStore for FixedIdentityKv {
		fn identity(&self) -> KvStoreIdentity {
			self.identity
		}

		fn read(&self, key: &[u8]) -> io::Result<Option<OwnedBytes>> {
			KvStore::read(&self.inner, key)
		}

		fn write(&self, mutations: &[Mutation], policy: WritePolicy) -> io::Result<()> {
			KvStore::write(&self.inner, mutations, policy)
		}

		fn sync(&self) -> io::Result<()> {
			KvStore::sync(&self.inner)
		}
	}

	fn put(key: &[u8], value: &[u8]) -> Mutation {
		Mutation::Put(key.to_vec(), value.to_vec())
	}

	fn prototype_directory() -> FaultingDirectory {
		let store = FaultingKv::default();
		store
			.write(
				&[Mutation::Put(
					legacy_namespaced_key(b"catalog", b"counter"),
					1_u64.to_be_bytes().to_vec(),
				)],
				WritePolicy::WAL_SYNC,
			)
			.unwrap();
		FaultingDirectory::with_namespace(store, b"catalog")
	}

	fn former_format_directory() -> FaultingDirectory {
		let store = FaultingKv::default();
		store
			.write(
				&[Mutation::Put(format_marker_key(b"catalog"), vec![1])],
				WritePolicy::WAL_SYNC,
			)
			.unwrap();
		FaultingDirectory::with_namespace(store, b"catalog")
	}

	fn empty_binding() -> Binding {
		Binding {
			object_id: 1,
			full_chunks: 0,
			tail_revision: 0,
			tail_length: 0,
			visible_length: 0,
			chunk_high_water: 0,
			tail_high_water: 0,
		}
	}

	fn assert_physical_keys_within_high_water(store: &FaultingKv, namespace: &[u8], path: &Path) {
		let state = store.state.lock().unwrap();
		let binding = state
			.visible
			.get(&binding_key(namespace, path))
			.and_then(|entry| entry.value.as_ref())
			.map(|bytes| decode_binding(bytes).unwrap())
			.expect("object binding is missing");
		assert_object_keys_within_high_water(&state.visible, namespace, path, binding.object_id);
	}

	fn assert_object_keys_within_high_water(
		entries: &BTreeMap<Vec<u8>, VersionedValue>,
		namespace: &[u8],
		path: &Path,
		object_id: u64,
	) {
		let binding = entries
			.get(&binding_key(namespace, path))
			.and_then(|entry| entry.value.as_ref())
			.map(|bytes| decode_binding(bytes).unwrap());
		let mut chunk_prefix = namespaced_prefix_with_capacity(namespace, KEY_KIND_CHUNK, 8);
		chunk_prefix.extend_from_slice(&object_id.to_be_bytes());
		let mut tail_prefix = namespaced_prefix_with_capacity(namespace, KEY_KIND_TAIL, 8);
		tail_prefix.extend_from_slice(&object_id.to_be_bytes());
		for (key, value) in entries {
			if value.value.is_none() {
				continue;
			}
			if let Some(suffix) = key.strip_prefix(chunk_prefix.as_slice()) {
				let binding = binding.as_ref().expect("recovered chunk has no binding");
				assert_eq!(binding.object_id, object_id);
				let chunk = u32::from_be_bytes(suffix.try_into().unwrap());
				assert!(chunk < binding.chunk_high_water);
			}
			if let Some(suffix) = key.strip_prefix(tail_prefix.as_slice()) {
				let binding = binding.as_ref().expect("recovered tail has no binding");
				assert_eq!(binding.object_id, object_id);
				let revision = u64::from_be_bytes(suffix.try_into().unwrap());
				assert!(revision <= binding.tail_high_water);
			}
		}
	}

	fn assert_every_wal_prefix_respects_high_waters(store: &FaultingKv, namespace: &[u8], path: &Path) {
		let state = store.state.lock().unwrap();
		let mut recovered = state.durable.clone();
		assert_object_keys_within_high_water(&recovered, namespace, path, 1);
		for batch in &state.pending_wal {
			for (key, entry) in batch {
				apply_if_newer(&mut recovered, key.clone(), entry.clone());
			}
			assert_object_keys_within_high_water(&recovered, namespace, path, 1);
		}
	}

	#[test]
	fn synced_metadata_makes_earlier_wal_objects_durable() {
		let store = FaultingKv::default();
		store.write(&[put(b"object/1", b"segment")], WritePolicy::WAL).unwrap();
		assert_eq!(store.get(b"object/1"), Some(b"segment".to_vec()));
		assert_eq!(store.get_durable(b"object/1"), None);

		store
			.write(&[put(b"meta.json", b"object/1")], WritePolicy::WAL_SYNC)
			.unwrap();
		let recovered = store.crash();
		assert_eq!(recovered.get(b"meta.json"), Some(b"object/1".to_vec()));
		assert_eq!(recovered.get(b"object/1"), Some(b"segment".to_vec()));
	}

	#[test]
	fn unsynced_metadata_and_objects_disappear_together() {
		let store = FaultingKv::default();
		store
			.write(
				&[put(b"object/1", b"segment"), put(b"meta.json", b"object/1")],
				WritePolicy::WAL,
			)
			.unwrap();
		let recovered = store.crash();
		assert_eq!(recovered.get(b"meta.json"), None);
		assert_eq!(recovered.get(b"object/1"), None);
	}

	#[test]
	fn directory_sync_is_not_the_rocks_durability_barrier() {
		let store = FaultingKv::default();
		store.write(&[put(b"object/1", b"segment")], WritePolicy::WAL).unwrap();
		KvStore::sync(&store).unwrap();
		assert_eq!(store.crash().get(b"object/1"), None);

		store
			.write(&[put(b"meta.json", b"object/1")], WritePolicy::WAL_SYNC)
			.unwrap();
		let recovered = store.crash();
		assert_eq!(recovered.get(b"object/1"), Some(b"segment".to_vec()));
		assert_eq!(recovered.get(b"meta.json"), Some(b"object/1".to_vec()));
	}

	#[test]
	fn flush_captures_only_the_frozen_object_set() {
		let store = FaultingKv::default();
		store.write(&[put(b"object/1", b"first")], WritePolicy::NO_WAL).unwrap();
		let barrier = store.begin_flush();
		store.write(&[put(b"object/2", b"later")], WritePolicy::NO_WAL).unwrap();
		store.complete_flush(barrier).unwrap();
		store
			.write(&[put(b"meta.json", b"object/1")], WritePolicy::WAL_SYNC)
			.unwrap();

		let recovered = store.crash();
		assert_eq!(recovered.get(b"object/1"), Some(b"first".to_vec()));
		assert_eq!(recovered.get(b"object/2"), None);
		assert_eq!(recovered.get(b"meta.json"), Some(b"object/1".to_vec()));
	}

	#[test]
	fn failed_flush_cannot_be_followed_by_publication() {
		let store = FaultingKv::default();
		store
			.write(&[put(b"object/1", b"segment")], WritePolicy::NO_WAL)
			.unwrap();
		let barrier = store.begin_flush();
		store.fail_next_flush();
		assert!(store.complete_flush(barrier).is_err());

		let recovered = store.crash();
		assert_eq!(recovered.get(b"object/1"), None);
		assert_eq!(recovered.get(b"meta.json"), None);
	}

	#[test]
	fn failed_batch_changes_neither_visible_nor_durable_state() {
		let store = FaultingKv::default();
		store.write(&[put(b"key", b"before")], WritePolicy::WAL_SYNC).unwrap();
		store.fail_next_write();
		assert!(store.write(&[put(b"key", b"after")], WritePolicy::WAL_SYNC).is_err());
		assert_eq!(store.get(b"key"), Some(b"before".to_vec()));
		assert_eq!(store.crash().get(b"key"), Some(b"before".to_vec()));
	}

	#[test]
	fn deletion_obeys_wal_durability() {
		let store = FaultingKv::default();
		store.write(&[put(b"key", b"value")], WritePolicy::WAL_SYNC).unwrap();
		store
			.write(&[Mutation::Delete(b"key".to_vec())], WritePolicy::WAL)
			.unwrap();
		assert_eq!(store.get(b"key"), None);
		assert_eq!(store.crash().get(b"key"), Some(b"value".to_vec()));

		store
			.write(&[Mutation::Delete(b"key".to_vec())], WritePolicy::WAL_SYNC)
			.unwrap();
		assert_eq!(store.crash().get(b"key"), None);
	}

	#[test]
	fn directory_satisfies_the_shared_contract() {
		let results = verify_directory_contract(|| FaultingDirectory::new(FaultingKv::default())).unwrap();
		assert_eq!(results.len(), 8);
	}

	#[test]
	fn directory_keeps_simultaneous_file_revisions_immutable() {
		let directory = FaultingDirectory::new(FaultingKv::default());
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(b"first").unwrap();
		writer.flush().unwrap();
		let first = directory.open_read(Path::new("segment")).unwrap();

		writer.write_all(b" second").unwrap();
		writer.flush().unwrap();
		let second = directory.open_read(Path::new("segment")).unwrap();
		assert_eq!(first.read_bytes().unwrap().as_slice(), b"first");
		assert_eq!(second.read_bytes().unwrap().as_slice(), b"first second");
	}

	#[test]
	fn independently_constructed_directories_share_writer_locks() {
		let store = FaultingKv::default();
		let first = FaultingDirectory::new(store.clone());
		let second = FaultingDirectory::new(store);
		let lock = first.acquire_lock(&tantivy::directory::INDEX_WRITER_LOCK).unwrap();
		assert!(matches!(
			second.acquire_lock(&tantivy::directory::INDEX_WRITER_LOCK),
			Err(LockError::LockBusy)
		));
		drop(lock);
		second.acquire_lock(&tantivy::directory::INDEX_WRITER_LOCK).unwrap();
	}

	#[test]
	fn deleted_file_is_not_resurrected_by_its_writer() {
		let directory = FaultingDirectory::new(FaultingKv::default());
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(b"pending").unwrap();
		directory.delete(Path::new("segment")).unwrap();
		assert_eq!(writer.flush().unwrap_err().kind(), io::ErrorKind::NotFound);
		assert!(!directory.exists(Path::new("segment")).unwrap());
	}

	#[test]
	fn failed_writer_flush_can_be_retried_without_losing_bytes() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(b"pending").unwrap();
		store.fail_next_write();
		assert!(writer.flush().is_err());
		writer.write_all(b" plus more").unwrap();
		writer.flush().unwrap();
		let file = directory.open_read(Path::new("segment")).unwrap();
		assert_eq!(file.read_bytes().unwrap().as_slice(), b"pending plus more");
	}

	#[test]
	fn applied_but_reported_failed_publication_is_retryable() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(b"pending").unwrap();
		store.fail_after_next_write();
		assert!(writer.flush().is_err());
		writer.write_all(b" plus more").unwrap();
		writer.flush().unwrap();

		let file = directory.open_read(Path::new("segment")).unwrap();
		assert_eq!(file.read_bytes().unwrap().as_slice(), b"pending plus more");
	}

	#[test]
	fn namespaces_isolate_indexes_on_one_store() {
		let store = FaultingKv::default();
		let first = FaultingDirectory::with_namespace(store.clone(), b"index/one");
		let second = FaultingDirectory::with_namespace(store, b"index/two");
		first.atomic_write(Path::new("meta.json"), b"one").unwrap();
		second.atomic_write(Path::new("meta.json"), b"two").unwrap();
		assert_eq!(first.atomic_read(Path::new("meta.json")).unwrap(), b"one");
		assert_eq!(second.atomic_read(Path::new("meta.json")).unwrap(), b"two");
	}

	#[test]
	fn namespace_and_path_delimiters_cannot_alias() {
		let first = binding_key(b"a", Path::new("b/binding/x"));
		let second = binding_key(b"a/binding/b", Path::new("x"));
		assert_ne!(first, second);
		assert_ne!(binding_key(b"a", Path::new("x")), atomic_key(b"a", Path::new("x")));
	}

	#[test]
	fn format_marker_is_durable_and_reusable() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::with_namespace(store.clone(), b"catalog");
		assert!(!directory.exists(Path::new("missing")).unwrap());
		assert_eq!(store.get(&format_marker_key(b"catalog")), None);
		directory.atomic_write(Path::new("meta.json"), b"metadata").unwrap();
		assert_eq!(
			store.get_durable(&format_marker_key(b"catalog")),
			Some(vec![KEY_FORMAT_VERSION])
		);
		let reopened = FaultingDirectory::with_namespace(store.crash(), b"catalog");
		assert_eq!(reopened.atomic_read(Path::new("meta.json")).unwrap(), b"metadata");
	}

	#[test]
	fn format_initialization_failure_is_returned_and_retryable() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::with_namespace(store.clone(), b"catalog");
		store.fail_next_write();
		assert!(directory.atomic_write(Path::new("meta.json"), b"metadata").is_err());
		assert_eq!(store.get(&format_marker_key(b"catalog")), None);
		directory.atomic_write(Path::new("meta.json"), b"metadata").unwrap();
		assert_eq!(directory.atomic_read(Path::new("meta.json")).unwrap(), b"metadata");
	}

	#[test]
	fn absent_marker_is_revalidated_on_later_reads() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::with_namespace(store.clone(), b"catalog");
		assert!(!directory.exists(Path::new("meta.json")).unwrap());
		store
			.write(
				&[Mutation::Put(
					format_marker_key(b"catalog"),
					vec![KEY_FORMAT_VERSION + 1],
				)],
				WritePolicy::WAL_SYNC,
			)
			.unwrap();
		assert!(directory.exists(Path::new("meta.json")).is_err());
	}

	#[test]
	fn clean_absent_format_rechecks_only_the_marker() {
		let store = CountingKv::new();
		let directory = KvDirectory::with_namespace(store.clone(), b"catalog");
		assert!(!directory.exists(Path::new("meta.json")).unwrap());
		assert_eq!(store.take_reads(), 6);
		assert!(!directory.exists(Path::new("meta.json")).unwrap());
		assert_eq!(store.take_reads(), 3);
	}

	#[test]
	fn format_state_never_moves_backward() {
		let format = FormatValidation::default();
		format.record(FORMAT_PRESENT);
		format.record(FORMAT_ABSENT);
		assert_eq!(format.state.load(Ordering::Acquire), FORMAT_PRESENT);
	}

	#[test]
	fn reconstructed_store_revalidates_a_reused_identity() {
		let identity = KvStoreIdentity(7, 8, 9);
		let first_store = FixedIdentityKv {
			inner: FaultingKv::default(),
			identity,
		};
		let first = KvDirectory::with_namespace(first_store, b"catalog");
		first.atomic_write(Path::new("meta.json"), b"metadata").unwrap();

		let replacement = FaultingKv::default();
		replacement
			.write(
				&[Mutation::Put(
					legacy_namespaced_key(b"catalog", b"counter"),
					1_u64.to_be_bytes().to_vec(),
				)],
				WritePolicy::WAL_SYNC,
			)
			.unwrap();
		let second = KvDirectory::with_namespace(
			FixedIdentityKv {
				inner: replacement,
				identity,
			},
			b"catalog",
		);
		assert!(second.exists(Path::new("meta.json")).is_err());
	}

	#[test]
	fn rejects_the_prototype_key_format() {
		for sentinel in [b"counter".as_slice(), b"atomic/.managed.json", b"atomic/meta.json"] {
			let store = FaultingKv::default();
			store
				.write(
					&[Mutation::Put(
						legacy_namespaced_key(b"catalog", sentinel),
						b"prototype".to_vec(),
					)],
					WritePolicy::WAL_SYNC,
				)
				.unwrap();
			let directory = FaultingDirectory::with_namespace(store, b"catalog");
			let error = directory.exists(Path::new("meta.json")).unwrap_err();
			let OpenReadError::IoError { io_error, .. } = error else {
				panic!("prototype format did not return an I/O error");
			};
			assert_eq!(io_error.kind(), io::ErrorKind::InvalidData);
			assert!(io_error.to_string().contains("prototype directory format"));
		}
	}

	#[test]
	fn every_storage_entry_point_rejects_the_prototype_format() {
		fn assert_prototype_error<T, E: fmt::Display>(result: Result<T, E>) {
			let error = result.err().expect("prototype format should be rejected");
			assert!(
				error.to_string().contains("prototype directory format"),
				"unexpected error: {error}"
			);
		}

		let path = Path::new("meta.json");
		assert_prototype_error(prototype_directory().exists(path));
		assert_prototype_error(prototype_directory().atomic_read(path));
		assert_prototype_error(prototype_directory().atomic_write(path, b"metadata"));
		assert_prototype_error(prototype_directory().open_write(path));
		assert_prototype_error(prototype_directory().delete(path));
		assert_prototype_error(prototype_directory().sync_directory());
		assert_prototype_error(prototype_directory().open_read(path));
	}

	#[test]
	fn every_storage_entry_point_rejects_the_former_directory_format() {
		fn assert_version_error<T, E: fmt::Display>(result: Result<T, E>) {
			let error = result.err().expect("former directory format should be rejected");
			assert!(error.to_string().contains("directory format version 1"));
		}

		let path = Path::new("meta.json");
		assert_version_error(former_format_directory().exists(path));
		assert_version_error(former_format_directory().atomic_read(path));
		assert_version_error(former_format_directory().atomic_write(path, b"metadata"));
		assert_version_error(former_format_directory().open_write(path));
		assert_version_error(former_format_directory().delete(path));
		assert_version_error(former_format_directory().sync_directory());
		assert_version_error(former_format_directory().open_read(path));
	}

	#[test]
	fn rejects_an_unknown_directory_format() {
		let store = FaultingKv::default();
		store
			.write(
				&[Mutation::Put(
					format_marker_key(b"catalog"),
					vec![KEY_FORMAT_VERSION + 1],
				)],
				WritePolicy::WAL_SYNC,
			)
			.unwrap();
		let error = FaultingDirectory::with_namespace(store, b"catalog")
			.exists(Path::new("meta.json"))
			.unwrap_err();
		assert!(error.to_string().contains("version 3"));
	}

	#[test]
	fn rejects_malformed_directory_format_markers() {
		for value in [Vec::new(), vec![KEY_FORMAT_VERSION, 0]] {
			let store = FaultingKv::default();
			store
				.write(
					&[Mutation::Put(format_marker_key(b"catalog"), value)],
					WritePolicy::WAL_SYNC,
				)
				.unwrap();
			let error = FaultingDirectory::with_namespace(store, b"catalog")
				.exists(Path::new("meta.json"))
				.unwrap_err();
			assert!(error.to_string().contains("malformed directory format marker"));
		}
	}

	#[test]
	fn independent_directories_converge_on_one_format() {
		let store = FaultingKv::default();
		let first = FaultingDirectory::with_namespace(store.clone(), b"catalog");
		let second = FaultingDirectory::with_namespace(store.clone(), b"catalog");
		let first_write = std::thread::spawn(move || first.atomic_write(Path::new("one"), b"one"));
		let second_write = std::thread::spawn(move || second.atomic_write(Path::new("two"), b"two"));
		first_write.join().unwrap().unwrap();
		second_write.join().unwrap().unwrap();
		let directory = FaultingDirectory::with_namespace(store, b"catalog");
		assert_eq!(directory.atomic_read(Path::new("one")).unwrap(), b"one");
		assert_eq!(directory.atomic_read(Path::new("two")).unwrap(), b"two");
	}

	#[test]
	fn file_handle_reads_across_flush_boundaries() {
		let directory = FaultingDirectory::new(FaultingKv::default());
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(b"first").unwrap();
		writer.flush().unwrap();
		writer.write_all(b"second").unwrap();
		writer.flush().unwrap();
		let file = directory.open_read(Path::new("segment")).unwrap();
		assert_eq!(file.read_bytes_slice(3..9).unwrap().as_slice(), b"stseco");
	}

	#[test]
	fn large_files_use_fixed_chunks_and_a_versioned_tail() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let bytes = (0..CHUNK_SIZE * 3 + 17)
			.map(|offset| (offset % 251) as u8)
			.collect::<Vec<_>>();
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(&bytes).unwrap();
		writer.flush().unwrap();

		let binding = decode_binding(&store.get(&binding_key(b"phase0", Path::new("segment"))).unwrap()).unwrap();
		assert_eq!(binding.full_chunks, 3);
		assert_eq!(binding.tail_length, 17);
		assert_eq!(binding.visible_length, bytes.len());
		assert_eq!(binding.chunk_high_water, CHUNK_RESERVATION_STRIDE);
		assert_eq!(binding.tail_high_water, binding.tail_revision);
		assert_eq!(
			directory
				.open_read(Path::new("segment"))
				.unwrap()
				.read_bytes()
				.unwrap()
				.as_slice(),
			bytes
		);
	}

	#[test]
	fn exact_chunk_multiple_has_no_tail_and_reads_its_boundary() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let bytes = vec![7; CHUNK_SIZE * 2];
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(&bytes).unwrap();
		writer.flush().unwrap();

		let binding = decode_binding(&store.get(&binding_key(b"phase0", Path::new("segment"))).unwrap()).unwrap();
		assert_eq!(binding.full_chunks, 2);
		assert_eq!(binding.tail_length, 0);
		assert_eq!(binding.chunk_high_water, CHUNK_RESERVATION_STRIDE);
		assert_eq!(binding.tail_high_water, binding.tail_revision);
		assert_eq!(binding.tail_revision, 1);
		let file = directory.open_read(Path::new("segment")).unwrap();
		assert_eq!(
			file.read_bytes_slice(0..CHUNK_SIZE).unwrap().as_slice(),
			&bytes[..CHUNK_SIZE]
		);
	}

	#[test]
	fn chunk_reservations_amortize_binding_io() {
		let store = CountingKv::new();
		let directory = KvDirectory::new(store.clone());
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		store.take_io_counts();
		writer.write_all(&vec![7; CHUNK_SIZE * 65 + 17]).unwrap();
		writer.flush().unwrap();

		assert_eq!(store.take_io_counts(), (3, 68, 69));
		let binding = decode_binding(&store.inner.get(&binding_key(b"phase0", Path::new("segment"))).unwrap()).unwrap();
		assert_eq!(binding.full_chunks, 65);
		assert_eq!(binding.chunk_high_water, 128);
		assert_eq!(binding.tail_length, 17);
		assert_every_wal_prefix_respects_high_waters(&store.inner, b"phase0", Path::new("segment"));
	}

	#[test]
	fn physical_keys_stay_within_binding_high_waters_across_reopen() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let path = Path::new("segment");
		let mut writer = directory.open_write(path).unwrap();
		writer.write_all(&vec![3; CHUNK_SIZE * 2 + 17]).unwrap();
		writer.flush().unwrap();
		writer.write_all(b"next revision").unwrap();
		writer.flush().unwrap();
		assert_physical_keys_within_high_water(&store, b"phase0", path);

		directory
			.atomic_write(Path::new("meta.json"), b"durability barrier")
			.unwrap();
		let recovered = store.crash();
		assert_physical_keys_within_high_water(&recovered, b"phase0", path);
		let reopened = FaultingDirectory::new(recovered);
		assert_eq!(reopened.open_read(path).unwrap().len(), CHUNK_SIZE * 2 + 30);
	}

	#[test]
	fn range_reads_fetch_only_intersecting_chunks() {
		let store = CountingKv::new();
		let directory = KvDirectory::new(store.clone());
		let bytes = (0..CHUNK_SIZE * 4 + 29)
			.map(|offset| (offset % 251) as u8)
			.collect::<Vec<_>>();
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(&bytes).unwrap();
		writer.flush().unwrap();
		let file = directory.open_read(Path::new("segment")).unwrap();
		store.take_reads();

		let start = CHUNK_SIZE * 3 + 11;
		let end = start + 97;
		assert_eq!(
			file.read_bytes_slice(start..end).unwrap().as_slice(),
			&bytes[start..end]
		);
		assert_eq!(store.take_reads(), 1);

		let start = CHUNK_SIZE / 2;
		let end = CHUNK_SIZE * 2 + 100;
		assert_eq!(
			file.read_bytes_slice(start..end).unwrap().as_slice(),
			&bytes[start..end]
		);
		assert_eq!(store.take_reads(), 3);

		let start = CHUNK_SIZE * 4 - 11;
		let end = CHUNK_SIZE * 4 + 17;
		assert_eq!(
			file.read_bytes_slice(start..end).unwrap().as_slice(),
			&bytes[start..end]
		);
		assert_eq!(store.take_reads(), 2);
	}

	#[test]
	fn filling_a_published_tail_does_not_change_an_open_handle() {
		let directory = FaultingDirectory::new(FaultingKv::default());
		let first_bytes = vec![3; CHUNK_SIZE - 17];
		let appended = vec![5; 34];
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(&first_bytes).unwrap();
		writer.flush().unwrap();
		let first = directory.open_read(Path::new("segment")).unwrap();

		writer.write_all(&appended).unwrap();
		writer.flush().unwrap();
		let second = directory.open_read(Path::new("segment")).unwrap();
		assert_eq!(first.read_bytes().unwrap().as_slice(), first_bytes);
		assert_eq!(
			second.read_bytes().unwrap().as_slice(),
			[first_bytes, appended].concat()
		);
	}

	#[test]
	fn failed_full_chunk_staging_can_be_retried_during_flush() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let bytes = vec![7; CHUNK_SIZE];
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(&bytes).unwrap();
		store.fail_next_write();
		assert!(writer.flush().is_err());
		writer.flush().unwrap();
		assert_eq!(
			directory
				.open_read(Path::new("segment"))
				.unwrap()
				.read_bytes()
				.unwrap()
				.as_slice(),
			bytes
		);
	}

	#[test]
	fn applied_but_reported_failed_chunk_reservation_and_put_are_retryable() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let binding = empty_binding();
		store
			.write(
				&[Mutation::Put(
					binding_key(b"phase0", Path::new("segment")),
					encode_binding(&binding),
				)],
				WritePolicy::WAL,
			)
			.unwrap();
		let mut writer = KvWriter {
			store: store.clone(),
			state: directory.state.clone(),
			namespace: directory.namespace.clone(),
			path: PathBuf::from("segment"),
			binding,
			tail: vec![7; CHUNK_SIZE],
			staged_full_chunks: 0,
			dirty: true,
			pending_publication: None,
		};

		store.fail_after_next_write();
		assert!(writer.flush().is_err());
		let reserved = decode_binding(&store.get(&binding_key(b"phase0", Path::new("segment"))).unwrap()).unwrap();
		assert_eq!(reserved.chunk_high_water, CHUNK_RESERVATION_STRIDE);
		store.fail_after_next_write();
		assert!(writer.flush().is_err());
		assert_eq!(store.get(&chunk_key(b"phase0", 1, 0)), Some(vec![7; CHUNK_SIZE]));
		writer.flush().unwrap();

		let file = directory.open_read(Path::new("segment")).unwrap();
		assert_eq!(file.read_bytes().unwrap().as_slice(), vec![7; CHUNK_SIZE]);
	}

	#[test]
	fn staging_failure_does_not_consume_the_failing_write() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let binding = empty_binding();
		store
			.write(
				&[Mutation::Put(
					binding_key(b"phase0", Path::new("segment")),
					encode_binding(&binding),
				)],
				WritePolicy::WAL,
			)
			.unwrap();
		let mut writer = KvWriter {
			store,
			state: directory.state.clone(),
			namespace: directory.namespace.clone(),
			path: PathBuf::from("segment"),
			binding,
			tail: Vec::new(),
			staged_full_chunks: 0,
			dirty: false,
			pending_publication: None,
		};
		let prefix = vec![1; CHUNK_SIZE - 4_096];
		let suffix = vec![2; 8_192];
		assert_eq!(writer.write(&prefix).unwrap(), prefix.len());
		writer.store.fail_next_write();
		assert_eq!(writer.write(&suffix).unwrap(), 4_096);
		assert!(writer.write(&suffix[4_096..]).is_err());
		assert_eq!(writer.write(&suffix[4_096..]).unwrap(), 4_096);
		writer.flush().unwrap();

		let file = directory.open_read(Path::new("segment")).unwrap();
		assert_eq!(file.read_bytes().unwrap().as_slice(), [prefix, suffix].concat());
	}

	#[test]
	fn rejects_an_older_binding_format_with_its_version() {
		let error = decode_binding(&[2; 33]).unwrap_err();
		assert_eq!(error.kind(), io::ErrorKind::InvalidData);
		assert!(error.to_string().contains("version 2"));
	}

	#[test]
	fn distinguishes_a_malformed_current_binding() {
		let error = decode_binding(&[BINDING_FORMAT_VERSION; 20]).unwrap_err();
		assert_eq!(error.kind(), io::ErrorKind::InvalidData);
		assert_eq!(error.to_string(), "malformed binding");
	}

	#[test]
	fn rejects_binding_high_waters_below_published_state() {
		let mut chunk = empty_binding();
		chunk.full_chunks = 1;
		chunk.visible_length = CHUNK_SIZE;
		let error = decode_binding(&encode_binding(&chunk)).unwrap_err();
		assert_eq!(error.kind(), io::ErrorKind::InvalidData);
		assert!(error.to_string().contains("high-water is below"));

		let mut tail = empty_binding();
		tail.tail_revision = 1;
		let error = decode_binding(&encode_binding(&tail)).unwrap_err();
		assert_eq!(error.kind(), io::ErrorKind::InvalidData);
		assert!(error.to_string().contains("high-water is below"));
	}

	#[test]
	fn rejects_binding_high_waters_beyond_the_extent_limit() {
		let mut binding = empty_binding();
		binding.chunk_high_water = u32::MAX;
		let error = decode_binding(&encode_binding(&binding)).unwrap_err();
		assert_eq!(error.kind(), io::ErrorKind::InvalidData);
		assert!(error.to_string().contains("physical extent exceeds"));
	}

	#[test]
	fn directory_supports_a_real_tantivy_lifecycle_and_crash_reopen() {
		let store = FaultingKv::default();
		verify_tantivy_lifecycle(FaultingDirectory::new(store.clone())).unwrap();
		let reopened = tantivy::Index::open(FaultingDirectory::new(store.crash())).unwrap();
		assert_eq!(reopened.searchable_segment_ids().unwrap().len(), 1);
		reopened
			.writer::<tantivy::TantivyDocument>(15_000_000)
			.unwrap()
			.wait_merging_threads()
			.unwrap();
	}

	#[test]
	fn synchronous_write_without_wal_is_rejected() {
		let store = FaultingKv::default();
		assert_eq!(
			store
				.write(
					&[put(b"key", b"value")],
					WritePolicy {
						wal_enabled: false,
						sync: true,
					},
				)
				.unwrap_err()
				.kind(),
			io::ErrorKind::InvalidInput
		);
	}
}
