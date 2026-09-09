use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::io;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
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
	pending_wal: Vec<(Vec<u8>, VersionedValue)>,
	fail_next_write: bool,
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
		for mutation in mutations {
			state.next_sequence += 1;
			let entry = VersionedValue {
				sequence: state.next_sequence,
				value: mutation.value(),
			};
			let key = mutation.key().to_vec();
			state.visible.insert(key.clone(), entry.clone());
			if policy.wal_enabled {
				state.pending_wal.push((key, entry));
			}
		}
		if policy.sync {
			let pending = std::mem::take(&mut state.pending_wal);
			for (key, entry) in pending {
				apply_if_newer(&mut state.durable, key, entry);
			}
		}
		Ok(())
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
				fail_next_flush: false,
			})),
		}
	}

	pub fn fail_next_write(&self) {
		self.state.lock().unwrap().fail_next_write = true;
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
			state,
		}
	}

	fn read_binding(&self, path: &Path) -> Result<Binding, OpenReadError>
	where
		S: KvStore,
	{
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
		let last_full_chunk = last_chunk.min((self.binding.full_chunks as usize).saturating_sub(1));
		for chunk in first_chunk..=last_full_chunk {
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
		let _mutation = self.state.mutation.lock().unwrap();
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
		let _mutation = self.state.mutation.lock().unwrap();
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
		})))
	}

	fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
		self.store
			.read(&atomic_key(&self.namespace, path))
			.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?
			.map(|bytes| bytes.as_slice().to_vec())
			.ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
	}

	fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
		{
			let _mutation = self.state.mutation.lock().unwrap();
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
}

impl<S: KvStore> Write for KvWriter<S> {
	fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
		if bytes.is_empty() {
			return Ok(0);
		}
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
		if !self.dirty {
			return Ok(());
		}
		self.stage_full_tail()?;
		let _mutation = self.state.mutation.lock().unwrap();
		let current = self
			.store
			.read(&binding_key(&self.namespace, &self.path))?
			.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file was deleted while its writer was open"))?;
		if decode_binding(&current)? != self.binding {
			return Err(io::Error::new(
				io::ErrorKind::NotFound,
				"file was replaced while its writer was open",
			));
		}
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
			.tail_revision
			.checked_add(1)
			.ok_or_else(|| io::Error::other("tail revision exhausted"))?;
		let next = Binding {
			object_id: self.binding.object_id,
			full_chunks,
			tail_revision,
			tail_length: u32::try_from(self.tail.len())
				.map_err(|_| io::Error::other("tail length exceeds its format"))?,
			visible_length,
		};
		let binding = Mutation::Put(binding_key(&self.namespace, &self.path), encode_binding(&next));
		if self.tail.is_empty() {
			self.store.write(&[binding], WritePolicy::WAL)?;
		} else {
			self.store.write(
				&[
					Mutation::Put(
						tail_key(&self.namespace, self.binding.object_id, tail_revision),
						self.tail.clone(),
					),
					binding,
				],
				WritePolicy::WAL,
			)?;
		}
		self.binding = next;
		self.staged_full_chunks = 0;
		self.dirty = false;
		Ok(())
	}
}

impl<S: KvStore> KvWriter<S> {
	fn stage_full_tail(&mut self) -> io::Result<()> {
		if self.tail.len() != CHUNK_SIZE {
			return Ok(());
		}
		let chunk = self
			.binding
			.full_chunks
			.checked_add(self.staged_full_chunks)
			.ok_or_else(|| io::Error::other("chunk count exhausted"))?;
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
}

fn counter_key(namespace: &[u8]) -> Vec<u8> {
	namespaced_prefix(namespace, b"counter")
}

fn binding_key(namespace: &[u8], path: &Path) -> Vec<u8> {
	prefixed_path(&namespaced_prefix(namespace, b"binding/"), path)
}

fn atomic_key(namespace: &[u8], path: &Path) -> Vec<u8> {
	prefixed_path(&namespaced_prefix(namespace, b"atomic/"), path)
}

fn namespaced_prefix(namespace: &[u8], suffix: &[u8]) -> Vec<u8> {
	let mut prefix = Vec::with_capacity(namespace.len() + suffix.len() + 1);
	prefix.extend_from_slice(namespace);
	prefix.push(b'/');
	prefix.extend_from_slice(suffix);
	prefix
}

fn prefixed_path(prefix: &[u8], path: &Path) -> Vec<u8> {
	let mut key = Vec::with_capacity(prefix.len() + path.as_os_str().as_encoded_bytes().len());
	key.extend_from_slice(prefix);
	key.extend_from_slice(path.as_os_str().as_encoded_bytes());
	key
}

fn chunk_key(namespace: &[u8], object_id: u64, chunk: u32) -> Vec<u8> {
	let mut key = namespaced_prefix_with_capacity(namespace, b"chunk/", 12);
	key.extend_from_slice(&object_id.to_be_bytes());
	key.extend_from_slice(&chunk.to_be_bytes());
	key
}

fn tail_key(namespace: &[u8], object_id: u64, revision: u64) -> Vec<u8> {
	let mut key = namespaced_prefix_with_capacity(namespace, b"tail/", 16);
	key.extend_from_slice(&object_id.to_be_bytes());
	key.extend_from_slice(&revision.to_be_bytes());
	key
}

fn namespaced_prefix_with_capacity(namespace: &[u8], suffix: &[u8], additional: usize) -> Vec<u8> {
	let mut prefix = Vec::with_capacity(namespace.len() + suffix.len() + 1 + additional);
	prefix.extend_from_slice(namespace);
	prefix.push(b'/');
	prefix.extend_from_slice(suffix);
	prefix
}

fn encode_binding(binding: &Binding) -> Vec<u8> {
	let mut bytes = Vec::with_capacity(33);
	bytes.push(2);
	bytes.extend_from_slice(&binding.object_id.to_be_bytes());
	bytes.extend_from_slice(&binding.full_chunks.to_be_bytes());
	bytes.extend_from_slice(&binding.tail_revision.to_be_bytes());
	bytes.extend_from_slice(&binding.tail_length.to_be_bytes());
	bytes.extend_from_slice(&(binding.visible_length as u64).to_be_bytes());
	bytes
}

fn decode_binding(bytes: &[u8]) -> io::Result<Binding> {
	if bytes.first().copied() != Some(2) {
		let version = bytes.first().copied().unwrap_or(0);
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("unsupported binding format version {version}"),
		));
	}
	if bytes.len() != 33 {
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
	};
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
	Ok(binding)
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
	}

	impl CountingKv {
		fn new() -> Self {
			Self {
				inner: FaultingKv::default(),
				reads: Arc::new(AtomicUsize::new(0)),
			}
		}

		fn take_reads(&self) -> usize {
			self.reads.swap(0, Ordering::Relaxed)
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
			KvStore::write(&self.inner, mutations, policy)
		}

		fn sync(&self) -> io::Result<()> {
			KvStore::sync(&self.inner)
		}
	}

	fn put(key: &[u8], value: &[u8]) -> Mutation {
		Mutation::Put(key.to_vec(), value.to_vec())
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
		writer.flush().unwrap();
		let file = directory.open_read(Path::new("segment")).unwrap();
		assert_eq!(file.read_bytes().unwrap().as_slice(), b"pending");
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
		let file = directory.open_read(Path::new("segment")).unwrap();
		assert_eq!(
			file.read_bytes_slice(0..CHUNK_SIZE).unwrap().as_slice(),
			&bytes[..CHUNK_SIZE]
		);
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
	fn staging_failure_does_not_consume_the_failing_write() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let binding = Binding {
			object_id: 1,
			full_chunks: 0,
			tail_revision: 0,
			tail_length: 0,
			visible_length: 0,
		};
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
		let error = decode_binding(&[1]).unwrap_err();
		assert_eq!(error.kind(), io::ErrorKind::InvalidData);
		assert!(error.to_string().contains("version 1"));
	}

	#[test]
	fn distinguishes_a_malformed_current_binding() {
		let error = decode_binding(&[2; 20]).unwrap_err();
		assert_eq!(error.kind(), io::ErrorKind::InvalidData);
		assert_eq!(error.to_string(), "malformed binding");
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
