use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock, Weak};
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
	allocator: Mutex<ObjectIdAllocator>,
	paths: Arc<PathRegistry>,
	reader_pins: Arc<ReaderPinRegistry>,
	reader_registration_shards: [RwLock<()>; READER_REGISTRATION_SHARD_COUNT],
	reclaim_shards: [Mutex<()>; RECLAIM_SHARD_COUNT],
	locks: Mutex<DirectoryLocks>,
	locks_changed: Condvar,
	watches: WatchCallbackList,
}

#[derive(Default)]
struct ObjectIdAllocator {
	next: u64,
	remaining: u64,
}

#[derive(Default)]
struct PathRegistry {
	states: Mutex<HashMap<PathBuf, Weak<PathState>>>,
}

struct PathState {
	path: PathBuf,
	registry: Weak<PathRegistry>,
	lifecycle: Mutex<PathLifecycle>,
}

#[derive(Default)]
struct PathLifecycle {
	writer: Option<Weak<WriterFence>>,
}

struct WriterFence {
	object_id: u64,
	retired: AtomicBool,
	in_flight: AtomicUsize,
	waiting: Mutex<()>,
	idle: Condvar,
}

struct WriterClaim<'a> {
	fence: &'a WriterFence,
}

struct ReaderPinRegistry {
	shards: [Mutex<HashMap<u64, Weak<ObjectReaderPins>>>; RECLAIM_SHARD_COUNT],
}

struct ObjectReaderPins {
	object_id: u64,
	registry: Weak<ReaderPinRegistry>,
	state: Mutex<ObjectReaderPinState>,
}

#[derive(Default)]
struct ObjectReaderPinState {
	total: usize,
	revisions: HashMap<u64, usize>,
}

struct ReaderPin {
	object: Arc<ObjectReaderPins>,
	tail_revision: u64,
}

impl Default for ReaderPinRegistry {
	fn default() -> Self {
		Self {
			shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
		}
	}
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

impl PathRegistry {
	fn state(self: &Arc<Self>, path: &Path) -> Arc<PathState> {
		let mut states = self.states.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		if let Some(state) = states.get(path).and_then(Weak::upgrade) {
			return state;
		}
		let state = Arc::new(PathState {
			path: path.to_path_buf(),
			registry: Arc::downgrade(self),
			lifecycle: Mutex::new(PathLifecycle::default()),
		});
		states.insert(path.to_path_buf(), Arc::downgrade(&state));
		state
	}
}

impl Drop for PathState {
	fn drop(&mut self) {
		let Some(registry) = self.registry.upgrade() else {
			return;
		};
		let mut states = registry.states.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		if states
			.get(&self.path)
			.is_some_and(|state| std::ptr::eq(state.as_ptr(), self))
		{
			states.remove(&self.path);
		}
	}
}

impl WriterFence {
	fn new(object_id: u64) -> Self {
		Self {
			object_id,
			retired: AtomicBool::new(false),
			in_flight: AtomicUsize::new(0),
			waiting: Mutex::new(()),
			idle: Condvar::new(),
		}
	}

	fn claim(&self) -> io::Result<WriterClaim<'_>> {
		self.claim_after_first_check(|| {})
	}

	fn claim_after_first_check(&self, after_first_check: impl FnOnce()) -> io::Result<WriterClaim<'_>> {
		if self.retired.load(Ordering::SeqCst) {
			return Err(writer_retired_error());
		}
		after_first_check();
		let previous = self.in_flight.fetch_add(1, Ordering::SeqCst);
		if previous == usize::MAX {
			self.in_flight.fetch_sub(1, Ordering::SeqCst);
			return Err(io::Error::other("writer in-flight count exhausted"));
		}
		if self.retired.load(Ordering::SeqCst) {
			self.release();
			return Err(writer_retired_error());
		}
		Ok(WriterClaim { fence: self })
	}

	fn retire_and_wait(&self) {
		self.retired.store(true, Ordering::SeqCst);
		let mut waiting = self.waiting.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		while self.in_flight.load(Ordering::SeqCst) != 0 {
			waiting = self.idle.wait(waiting).unwrap_or_else(|poisoned| poisoned.into_inner());
		}
	}

	fn reactivate(&self) {
		self.retired.store(false, Ordering::SeqCst);
	}

	fn release(&self) {
		if self.in_flight.fetch_sub(1, Ordering::SeqCst) == 1 && self.retired.load(Ordering::SeqCst) {
			let _waiting = self.waiting.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
			self.idle.notify_all();
		}
	}
}

impl Drop for WriterClaim<'_> {
	fn drop(&mut self) {
		self.fence.release();
	}
}

impl ReaderPinRegistry {
	fn register(self: &Arc<Self>, binding: &Binding) -> io::Result<Arc<ReaderPin>> {
		let shard = usize::from(reclaim_shard(binding.object_id));
		let object = {
			let mut objects = self.shards[shard]
				.lock()
				.unwrap_or_else(|poisoned| poisoned.into_inner());
			objects
				.get(&binding.object_id)
				.and_then(Weak::upgrade)
				.unwrap_or_else(|| {
					let object = Arc::new(ObjectReaderPins {
						object_id: binding.object_id,
						registry: Arc::downgrade(self),
						state: Mutex::new(ObjectReaderPinState::default()),
					});
					objects.insert(binding.object_id, Arc::downgrade(&object));
					object
				})
		};
		let mut state = object.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		let total = state
			.total
			.checked_add(1)
			.ok_or_else(|| io::Error::other("reader pin count exhausted"))?;
		let revision = state
			.revisions
			.get(&binding.tail_revision)
			.copied()
			.unwrap_or(0)
			.checked_add(1)
			.ok_or_else(|| io::Error::other("reader revision pin count exhausted"))?;
		state.total = total;
		state.revisions.insert(binding.tail_revision, revision);
		drop(state);
		Ok(Arc::new(ReaderPin {
			object,
			tail_revision: binding.tail_revision,
		}))
	}

	#[cfg(test)]
	fn is_pinned(&self, object_id: u64, tail_revision: Option<u64>) -> bool {
		let shard = usize::from(reclaim_shard(object_id));
		let objects = self.shards[shard]
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		let Some(object) = objects.get(&object_id).and_then(Weak::upgrade) else {
			return false;
		};
		drop(objects);
		let state = object.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		tail_revision.map_or(state.total != 0, |revision| {
			state.revisions.get(&revision).is_some_and(|count| *count != 0)
		})
	}
}

impl Drop for ReaderPin {
	fn drop(&mut self) {
		let mut state = self
			.object
			.state
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		debug_assert_ne!(state.total, 0, "reader object pin count is missing");
		if state.total == 0 {
			return;
		}
		let revision_count = state.revisions.get(&self.tail_revision).copied().unwrap_or(0);
		debug_assert_ne!(revision_count, 0, "reader revision pin count is missing");
		let remove_revision = match state.revisions.get_mut(&self.tail_revision) {
			Some(revision) if *revision != 0 => {
				*revision -= 1;
				*revision == 0
			}
			_ => return,
		};
		state.total -= 1;
		if remove_revision {
			state.revisions.remove(&self.tail_revision);
		}
	}
}

impl Drop for ObjectReaderPins {
	fn drop(&mut self) {
		let Some(registry) = self.registry.upgrade() else {
			return;
		};
		let shard = usize::from(reclaim_shard(self.object_id));
		let mut objects = registry.shards[shard]
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		if objects
			.get(&self.object_id)
			.is_some_and(|object| std::ptr::eq(object.as_ptr(), self))
		{
			objects.remove(&self.object_id);
		}
	}
}

fn writer_retired_error() -> io::Error {
	io::Error::new(io::ErrorKind::NotFound, "file was deleted while its writer was open")
}

fn delete_io_error(path: &Path, error: io::Error) -> DeleteError {
	DeleteError::IoError {
		io_error: Arc::new(error),
		filepath: path.to_path_buf(),
	}
}

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
				allocator: Mutex::new(ObjectIdAllocator::default()),
				paths: Arc::new(PathRegistry::default()),
				reader_pins: Arc::new(ReaderPinRegistry::default()),
				reader_registration_shards: std::array::from_fn(|_| RwLock::new(())),
				reclaim_shards: std::array::from_fn(|_| Mutex::new(())),
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

	fn allocate_object_id(&self) -> io::Result<u64> {
		let mut allocator = self
			.state
			.allocator
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		if allocator.remaining != 0 {
			let object_id = allocator.next;
			allocator.next = allocator.next.wrapping_add(1);
			allocator.remaining -= 1;
			return Ok(object_id);
		}
		let previous = self
			.store
			.read(&counter_key(&self.namespace))?
			.map(|bytes| decode_u64(&bytes))
			.transpose()?
			.unwrap_or(0);
		let available = u64::MAX - previous;
		if available == 0 {
			return Err(io::Error::other("object id exhausted"));
		}
		let reserved = available.min(OBJECT_ID_RESERVATION_STRIDE);
		let object_id = previous + 1;
		let reserved_through = previous + reserved;
		self.store.write(
			&[Mutation::Put(
				counter_key(&self.namespace),
				reserved_through.to_be_bytes().to_vec(),
			)],
			WritePolicy::WAL,
		)?;
		allocator.next = object_id.wrapping_add(1);
		allocator.remaining = reserved - 1;
		Ok(object_id)
	}

	fn reclaim_enqueue_mutations(&self, binding: &Binding) -> io::Result<Vec<Mutation>> {
		let shard = reclaim_shard(binding.object_id);
		let tail_key = reclaim_tail_key(&self.namespace, shard);
		let sequence = self
			.store
			.read(&tail_key)?
			.map(|bytes| decode_u64(&bytes))
			.transpose()?
			.unwrap_or(0);
		let entry_key = reclaim_entry_key(&self.namespace, shard, sequence);
		if self.store.read(&entry_key)?.is_some() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"reclaim queue tail references an occupied entry",
			));
		}
		let next = sequence
			.checked_add(1)
			.ok_or_else(|| io::Error::other("reclaim queue sequence exhausted"))?;
		Ok(vec![
			Mutation::Put(entry_key, encode_reclaim_entry(&ReclaimEntry::from_binding(binding))?),
			Mutation::Put(tail_key, next.to_be_bytes().to_vec()),
		])
	}

	fn pinned_binding(
		&self,
		path: &Path,
		after_binding_read: impl FnOnce(),
	) -> Result<(Binding, Arc<ReaderPin>), OpenReadError> {
		let registration = self.state.reader_registration_shards[reader_registration_shard(path)]
			.read()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		let binding = self.read_binding(path)?;
		after_binding_read();
		let pin = self
			.state
			.reader_pins
			.register(&binding)
			.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?;
		drop(registration);
		Ok((binding, pin))
	}
}

struct KvFileHandle<S> {
	store: S,
	namespace: Arc<[u8]>,
	path: PathBuf,
	binding: Binding,
	_state: Arc<DirectoryState>,
	_pin: Arc<ReaderPin>,
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
		let (binding, pin) = self.pinned_binding(path, || {})?;
		Ok(Arc::new(KvFileHandle {
			store: self.store.clone(),
			namespace: self.namespace.clone(),
			path: path.to_path_buf(),
			binding,
			_state: self.state.clone(),
			_pin: pin,
		}))
	}

	fn delete(&self, path: &Path) -> Result<(), DeleteError> {
		self.ensure_format(false)
			.map_err(|error| delete_io_error(path, error))?;
		let path_state = self.state.paths.state(path);
		let mut lifecycle = path_state
			.lifecycle
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		let binding_key = binding_key(&self.namespace, path);
		let atomic_key = atomic_key(&self.namespace, path);
		let initial_binding = self
			.store
			.read(&binding_key)
			.map_err(|error| delete_io_error(path, error))?;
		let atomic = self
			.store
			.read(&atomic_key)
			.map_err(|error| delete_io_error(path, error))?;
		if initial_binding.is_none() && atomic.is_none() {
			return Err(DeleteError::FileDoesNotExist(path.to_path_buf()));
		}
		let Some(initial_binding) = initial_binding else {
			return self
				.store
				.write(&[Mutation::Delete(atomic_key)], WritePolicy::WAL)
				.map_err(|error| delete_io_error(path, error));
		};
		let initial = decode_binding(&initial_binding).map_err(|error| delete_io_error(path, error))?;
		let fence = lifecycle
			.writer
			.as_ref()
			.and_then(Weak::upgrade)
			.filter(|fence| fence.object_id == initial.object_id);
		if let Some(fence) = &fence {
			fence.retire_and_wait();
		}
		let final_bytes = if fence.is_none() {
			initial_binding
		} else {
			match self.store.read(&binding_key) {
				Ok(Some(binding)) => binding,
				Ok(None) => {
					return Err(delete_io_error(
						path,
						io::Error::other("file binding disappeared during deletion"),
					));
				}
				Err(error) => {
					if let Some(fence) = &fence {
						fence.reactivate();
					}
					return Err(delete_io_error(path, error));
				}
			}
		};
		let final_binding = decode_binding(&final_bytes).map_err(|error| delete_io_error(path, error))?;
		if final_binding.object_id != initial.object_id {
			return Err(delete_io_error(
				path,
				io::Error::new(io::ErrorKind::InvalidData, "file binding changed during deletion"),
			));
		}
		let shard = reclaim_shard(final_binding.object_id);
		let _queue = self.state.reclaim_shards[usize::from(shard)]
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		let mut mutations = match self.reclaim_enqueue_mutations(&final_binding) {
			Ok(mutations) => mutations,
			Err(error) => {
				if let Some(fence) = &fence {
					fence.reactivate();
				}
				return Err(delete_io_error(path, error));
			}
		};
		mutations.push(Mutation::Delete(binding_key.clone()));
		mutations.push(Mutation::Delete(atomic_key));
		let _reader_registration = self.state.reader_registration_shards[reader_registration_shard(path)]
			.write()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		match self.store.write(&mutations, WritePolicy::WAL) {
			Ok(()) => {
				lifecycle.writer = None;
				Ok(())
			}
			Err(error) => match self.store.read(&binding_key) {
				Ok(None) => {
					lifecycle.writer = None;
					Ok(())
				}
				Ok(Some(current)) if current.as_slice() == final_bytes.as_slice() => {
					if let Some(fence) = &fence {
						fence.reactivate();
					}
					Err(delete_io_error(path, error))
				}
				Ok(Some(_)) | Err(_) => Err(delete_io_error(path, error)),
			},
		}
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
		let path_state = self.state.paths.state(path);
		let mut lifecycle = path_state
			.lifecycle
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
		let object_id = self
			.allocate_object_id()
			.map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?;
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
			.write(&[Mutation::Put(key, encode_binding(&binding))], WritePolicy::WAL)
			.map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?;
		let fence = Arc::new(WriterFence::new(object_id));
		lifecycle.writer = Some(Arc::downgrade(&fence));
		drop(lifecycle);
		Ok(std::io::BufWriter::new(Box::new(KvWriter {
			store: self.store.clone(),
			_state: self.state.clone(),
			_path_state: path_state,
			fence,
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
			let path_state = self.state.paths.state(path);
			let _lifecycle = path_state
				.lifecycle
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
	_state: Arc<DirectoryState>,
	_path_state: Arc<PathState>,
	fence: Arc<WriterFence>,
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
		let fence = self.fence.clone();
		let _claim = fence.claim()?;
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
		let fence = self.fence.clone();
		let _claim = fence.claim()?;
		self.reconcile_pending_publication()?;
		if !self.dirty {
			return Ok(());
		}
		self.stage_full_tail()?;
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReclaimEntry {
	object_id: u64,
	chunk_high_water: u32,
	tail_high_water: u64,
}

impl ReclaimEntry {
	fn from_binding(binding: &Binding) -> Self {
		Self {
			object_id: binding.object_id,
			chunk_high_water: binding.chunk_high_water,
			tail_high_water: binding.tail_high_water,
		}
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
const OBJECT_ID_RESERVATION_STRIDE: u64 = 1_024;
const MAX_OBJECT_EXTENT_BYTES: u128 = 1 << 40;
const RECLAIM_ENTRY_FORMAT_VERSION: u8 = 1;
const RECLAIM_ENTRY_WHOLE_OBJECT: u8 = 1;
const RECLAIM_SHARD_COUNT: usize = 64;
const READER_REGISTRATION_SHARD_COUNT: usize = 256;
const KEY_KIND_COUNTER: u8 = 1;
const KEY_KIND_BINDING: u8 = 2;
const KEY_KIND_ATOMIC: u8 = 3;
const KEY_KIND_CHUNK: u8 = 4;
const KEY_KIND_TAIL: u8 = 5;
const KEY_KIND_RECLAIM_TAIL: u8 = 7;
const KEY_KIND_RECLAIM_ENTRY: u8 = 8;
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

fn reclaim_tail_key(namespace: &[u8], shard: u8) -> Vec<u8> {
	let mut key = namespaced_prefix_with_capacity(namespace, KEY_KIND_RECLAIM_TAIL, 1);
	key.push(shard);
	key
}

fn reclaim_entry_key(namespace: &[u8], shard: u8, sequence: u64) -> Vec<u8> {
	let mut key = namespaced_prefix_with_capacity(namespace, KEY_KIND_RECLAIM_ENTRY, 9);
	key.push(shard);
	key.extend_from_slice(&sequence.to_be_bytes());
	key
}

fn reclaim_shard(object_id: u64) -> u8 {
	(object_id % RECLAIM_SHARD_COUNT as u64) as u8
}

fn reader_registration_shard(path: &Path) -> usize {
	let mut hasher = DefaultHasher::new();
	path.hash(&mut hasher);
	(hasher.finish() % READER_REGISTRATION_SHARD_COUNT as u64) as usize
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

fn encode_reclaim_entry(entry: &ReclaimEntry) -> io::Result<Vec<u8>> {
	validate_physical_extent(entry.chunk_high_water, entry.tail_high_water, "reclaim entry")?;
	if entry.object_id == 0 {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			"reclaim entry has an invalid object id",
		));
	}
	let mut bytes = Vec::with_capacity(22);
	bytes.push(RECLAIM_ENTRY_FORMAT_VERSION);
	bytes.push(RECLAIM_ENTRY_WHOLE_OBJECT);
	bytes.extend_from_slice(&entry.object_id.to_be_bytes());
	bytes.extend_from_slice(&entry.chunk_high_water.to_be_bytes());
	bytes.extend_from_slice(&entry.tail_high_water.to_be_bytes());
	Ok(bytes)
}

#[cfg(test)]
fn decode_reclaim_entry(bytes: &[u8]) -> io::Result<ReclaimEntry> {
	if bytes.first().copied() != Some(RECLAIM_ENTRY_FORMAT_VERSION) {
		let version = bytes.first().copied().unwrap_or(0);
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("unsupported reclaim entry format version {version}"),
		));
	}
	if bytes.len() < 2 {
		return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed reclaim entry"));
	}
	if bytes[1] != RECLAIM_ENTRY_WHOLE_OBJECT {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("unsupported reclaim entry kind {}", bytes[1]),
		));
	}
	if bytes.len() != 22 {
		return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed reclaim entry"));
	}
	let entry = ReclaimEntry {
		object_id: decode_u64(&bytes[2..10])?,
		chunk_high_water: u32::from_be_bytes(
			bytes[10..14]
				.try_into()
				.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid reclaim chunk high-water"))?,
		),
		tail_high_water: decode_u64(&bytes[14..22])?,
	};
	validate_physical_extent(entry.chunk_high_water, entry.tail_high_water, "reclaim entry")?;
	if entry.object_id == 0 {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			"reclaim entry has an invalid object id",
		));
	}
	Ok(entry)
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
	validate_physical_extent(binding.chunk_high_water, binding.tail_high_water, "binding")
}

fn validate_physical_extent(chunk_high_water: u32, tail_high_water: u64, subject: &str) -> io::Result<()> {
	let possible_extent =
		chunk_high_water as u128 * CHUNK_SIZE as u128 + tail_high_water as u128 * (CHUNK_SIZE - 1) as u128;
	if possible_extent > MAX_OBJECT_EXTENT_BYTES {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("{subject} physical extent exceeds the format limit"),
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

	#[derive(Clone)]
	struct BlockingKv {
		inner: FaultingKv,
		block: Arc<(Mutex<BlockState>, Condvar)>,
	}

	#[derive(Default)]
	struct BlockState {
		armed: bool,
		entered: bool,
		released: bool,
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

	impl BlockingKv {
		fn new() -> Self {
			Self {
				inner: FaultingKv::default(),
				block: Arc::new((Mutex::new(BlockState::default()), Condvar::new())),
			}
		}

		fn arm_next_write(&self) {
			let (state, _) = &*self.block;
			let mut state = state.lock().unwrap();
			state.armed = true;
			state.entered = false;
			state.released = false;
		}

		fn wait_until_blocked(&self) -> bool {
			let (state, changed) = &*self.block;
			let deadline = Instant::now() + Duration::from_secs(5);
			let mut state = state.lock().unwrap();
			while !state.entered {
				let remaining = deadline.saturating_duration_since(Instant::now());
				if remaining.is_zero() {
					state.released = true;
					changed.notify_all();
					return false;
				}
				let (next, timeout) = changed.wait_timeout(state, remaining).unwrap();
				state = next;
				if timeout.timed_out() && !state.entered {
					state.released = true;
					changed.notify_all();
					return false;
				}
			}
			true
		}

		fn release_write(&self) {
			let (state, changed) = &*self.block;
			let mut state = state.lock().unwrap();
			state.released = true;
			changed.notify_all();
		}
	}

	impl KvStore for BlockingKv {
		fn identity(&self) -> KvStoreIdentity {
			self.inner.identity()
		}

		fn read(&self, key: &[u8]) -> io::Result<Option<OwnedBytes>> {
			KvStore::read(&self.inner, key)
		}

		fn write(&self, mutations: &[Mutation], policy: WritePolicy) -> io::Result<()> {
			let (state, changed) = &*self.block;
			let mut state = state.lock().unwrap();
			if state.armed {
				state.armed = false;
				state.entered = true;
				changed.notify_all();
				while !state.released {
					state = changed.wait(state).unwrap();
				}
			}
			drop(state);
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

	fn test_writer<S: KvStore>(directory: &KvDirectory<S>, binding: Binding) -> KvWriter<S> {
		let path = PathBuf::from("segment");
		let path_state = directory.state.paths.state(&path);
		let fence = Arc::new(WriterFence::new(binding.object_id));
		path_state
			.lifecycle
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.writer = Some(Arc::downgrade(&fence));
		KvWriter {
			store: directory.store.clone(),
			_state: directory.state.clone(),
			_path_state: path_state,
			fence,
			namespace: directory.namespace.clone(),
			path,
			binding,
			tail: Vec::new(),
			staged_full_chunks: 0,
			dirty: false,
			pending_publication: None,
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

	fn wait_for_retirement(fence: &WriterFence) -> bool {
		let deadline = Instant::now() + Duration::from_secs(5);
		loop {
			if fence.retired.load(Ordering::SeqCst) {
				return true;
			}
			if Instant::now() >= deadline {
				return false;
			}
			std::thread::sleep(Duration::from_millis(1));
		}
	}

	fn assert_reclamation_inventory(store: &FaultingKv, namespace: &[u8]) {
		let state = store.state.lock().unwrap();
		let binding_prefix = namespaced_prefix(namespace, KEY_KIND_BINDING);
		let entry_prefix = namespaced_prefix(namespace, KEY_KIND_RECLAIM_ENTRY);
		let chunk_prefix = namespaced_prefix(namespace, KEY_KIND_CHUNK);
		let tail_prefix = namespaced_prefix(namespace, KEY_KIND_TAIL);
		let mut live = HashMap::new();
		let mut retired = HashMap::new();
		for (key, value) in &state.visible {
			let Some(value) = &value.value else {
				continue;
			};
			if key.starts_with(&binding_prefix) {
				let binding = decode_binding(value).unwrap();
				assert!(live.insert(binding.object_id, binding).is_none());
			} else if key.starts_with(&entry_prefix) {
				let entry = decode_reclaim_entry(value).unwrap();
				assert!(retired.insert(entry.object_id, entry).is_none());
			}
		}
		for object_id in live.keys() {
			assert!(
				!retired.contains_key(object_id),
				"live object was queued for reclamation"
			);
		}
		for (key, value) in &state.visible {
			if value.value.is_none() {
				continue;
			}
			if let Some(suffix) = key.strip_prefix(chunk_prefix.as_slice()) {
				let object_id = decode_u64(&suffix[..8]).unwrap();
				let chunk = u32::from_be_bytes(suffix[8..].try_into().unwrap());
				if let Some(binding) = live.get(&object_id) {
					assert!(chunk < binding.chunk_high_water);
				} else {
					assert!(
						chunk
							< retired
								.get(&object_id)
								.expect("unreachable chunk was not queued")
								.chunk_high_water
					);
				}
			} else if let Some(suffix) = key.strip_prefix(tail_prefix.as_slice()) {
				let object_id = decode_u64(&suffix[..8]).unwrap();
				let revision = decode_u64(&suffix[8..]).unwrap();
				if let Some(binding) = live.get(&object_id) {
					assert!(revision <= binding.tail_high_water);
				} else {
					assert!(
						revision
							<= retired
								.get(&object_id)
								.expect("unreachable tail was not queued")
								.tail_high_water
					);
				}
			}
		}
	}

	fn reclaim_entry_count(store: &FaultingKv, namespace: &[u8]) -> usize {
		let prefix = namespaced_prefix(namespace, KEY_KIND_RECLAIM_ENTRY);
		store
			.state
			.lock()
			.unwrap()
			.visible
			.iter()
			.filter(|(key, value)| key.starts_with(&prefix) && value.value.is_some())
			.count()
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
	fn open_handles_pin_their_object_and_tail_revision() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let path = Path::new("segment");
		let mut writer = directory.open_write(path).unwrap();
		writer.write_all(b"first").unwrap();
		writer.flush().unwrap();
		let first_binding = decode_binding(&store.get(&binding_key(b"phase0", path)).unwrap()).unwrap();
		let first = directory.open_read(path).unwrap();
		assert!(directory.state.reader_pins.is_pinned(first_binding.object_id, None));
		assert!(directory
			.state
			.reader_pins
			.is_pinned(first_binding.object_id, Some(first_binding.tail_revision)));

		directory.delete(path).unwrap();
		let mut replacement = directory.open_write(path).unwrap();
		replacement.write_all(b"second").unwrap();
		replacement.flush().unwrap();
		let second_binding = decode_binding(&store.get(&binding_key(b"phase0", path)).unwrap()).unwrap();
		let second = directory.open_read(path).unwrap();
		assert_ne!(first_binding.object_id, second_binding.object_id);
		assert!(directory.state.reader_pins.is_pinned(first_binding.object_id, None));
		assert!(directory.state.reader_pins.is_pinned(second_binding.object_id, None));

		drop(first);
		assert!(!directory.state.reader_pins.is_pinned(first_binding.object_id, None));
		assert!(directory.state.reader_pins.is_pinned(second_binding.object_id, None));
		drop(second);
		assert!(!directory.state.reader_pins.is_pinned(second_binding.object_id, None));
	}

	#[test]
	fn revision_pins_are_independent_within_one_object() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let path = Path::new("segment");
		let mut writer = directory.open_write(path).unwrap();
		writer.write_all(b"first").unwrap();
		writer.flush().unwrap();
		let first_binding = decode_binding(&store.get(&binding_key(b"phase0", path)).unwrap()).unwrap();
		let first = directory.open_read(path).unwrap();

		writer.write_all(b" second").unwrap();
		writer.flush().unwrap();
		let second_binding = decode_binding(&store.get(&binding_key(b"phase0", path)).unwrap()).unwrap();
		let second = directory.open_read(path).unwrap();
		assert_eq!(first_binding.object_id, second_binding.object_id);
		assert_ne!(first_binding.tail_revision, second_binding.tail_revision);
		assert!(directory
			.state
			.reader_pins
			.is_pinned(first_binding.object_id, Some(first_binding.tail_revision)));
		assert!(directory
			.state
			.reader_pins
			.is_pinned(second_binding.object_id, Some(second_binding.tail_revision)));

		drop(second);
		assert!(directory.state.reader_pins.is_pinned(first_binding.object_id, None));
		assert!(directory
			.state
			.reader_pins
			.is_pinned(first_binding.object_id, Some(first_binding.tail_revision)));
		assert!(!directory
			.state
			.reader_pins
			.is_pinned(second_binding.object_id, Some(second_binding.tail_revision)));
		drop(first);
		assert!(!directory.state.reader_pins.is_pinned(first_binding.object_id, None));
	}

	#[test]
	fn binding_read_and_pin_registration_complete_before_delete() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let path = Path::new("segment");
		let mut writer = directory.open_write(path).unwrap();
		writer.write_all(b"contents").unwrap();
		writer.flush().unwrap();
		drop(writer);
		let binding = decode_binding(&store.get(&binding_key(b"phase0", path)).unwrap()).unwrap();
		let (binding_read, allow_delete) = std::sync::mpsc::sync_channel(0);
		let (delete_started, deletion_entered) = std::sync::mpsc::sync_channel(0);
		let (deletion_completed, deleted) = std::sync::mpsc::channel();
		let deleting_directory = directory.clone();
		let deletion = std::thread::spawn(move || {
			allow_delete.recv().unwrap();
			delete_started.send(()).unwrap();
			let result = deleting_directory.delete(path);
			deletion_completed.send(()).unwrap();
			result
		});

		let (opened, pin) = directory
			.pinned_binding(path, || {
				binding_read.send(()).unwrap();
				deletion_entered.recv().unwrap();
				assert!(deleted.recv_timeout(Duration::from_millis(50)).is_err());
			})
			.unwrap();
		deletion.join().unwrap().unwrap();

		assert_eq!(opened, binding);
		assert!(directory.state.reader_pins.is_pinned(binding.object_id, None));
		assert!(!directory.exists(path).unwrap());
		drop(pin);
		assert!(!directory.state.reader_pins.is_pinned(binding.object_id, None));
	}

	#[test]
	fn independently_constructed_directories_share_reader_pins() {
		let store = FaultingKv::default();
		let first = FaultingDirectory::new(store.clone());
		let second = FaultingDirectory::new(store.clone());
		let path = Path::new("segment");
		let mut writer = first.open_write(path).unwrap();
		writer.write_all(b"contents").unwrap();
		writer.flush().unwrap();
		let binding = decode_binding(&store.get(&binding_key(b"phase0", path)).unwrap()).unwrap();
		let handle = first.open_read(path).unwrap();

		assert!(second.state.reader_pins.is_pinned(binding.object_id, None));
		drop(handle);
		assert!(!second.state.reader_pins.is_pinned(binding.object_id, None));
	}

	#[test]
	fn open_handle_keeps_the_canonical_directory_state_alive() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let path = Path::new("segment");
		let mut writer = directory.open_write(path).unwrap();
		writer.write_all(b"contents").unwrap();
		writer.flush().unwrap();
		drop(writer);
		let binding = decode_binding(&store.get(&binding_key(b"phase0", path)).unwrap()).unwrap();
		let state = Arc::downgrade(&directory.state);
		let handle = directory.open_read(path).unwrap();
		drop(directory);

		let retained = state.upgrade().unwrap();
		let reopened = FaultingDirectory::new(store);
		assert!(Arc::ptr_eq(&retained, &reopened.state));
		assert!(reopened.state.reader_pins.is_pinned(binding.object_id, None));
		drop(handle);
		assert!(!reopened.state.reader_pins.is_pinned(binding.object_id, None));
	}

	#[test]
	fn open_read_adds_no_storage_operation_for_pin_registration() {
		let store = CountingKv::new();
		let directory = KvDirectory::new(store.clone());
		let path = Path::new("segment");
		let mut writer = directory.open_write(path).unwrap();
		writer.write_all(b"contents").unwrap();
		writer.flush().unwrap();
		store.take_io_counts();

		let handle = directory.open_read(path).unwrap();

		assert_eq!(store.take_io_counts(), (1, 0, 0));
		drop(handle);
	}

	#[test]
	fn open_drop_churn_bounds_retained_weak_pins() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let path = Path::new("segment");
		let mut writer = directory.open_write(path).unwrap();
		writer.write_all(b"contents").unwrap();
		writer.flush().unwrap();
		let binding = decode_binding(&store.get(&binding_key(b"phase0", path)).unwrap()).unwrap();

		for _ in 0..256 {
			drop(directory.open_read(path).unwrap());
		}

		let shard = usize::from(reclaim_shard(binding.object_id));
		let objects = directory.state.reader_pins.shards[shard]
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner());
		assert!(!objects.contains_key(&binding.object_id));
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
	fn deletion_enqueues_the_whole_object_extent() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(&vec![7; CHUNK_SIZE * 2 + 17]).unwrap();
		writer.write_all(b" second").unwrap();
		writer.flush().unwrap();
		let binding = decode_binding(&store.get(&binding_key(b"phase0", Path::new("segment"))).unwrap()).unwrap();

		directory.delete(Path::new("segment")).unwrap();

		let shard = reclaim_shard(binding.object_id);
		assert_eq!(
			decode_u64(&store.get(&reclaim_tail_key(b"phase0", shard)).unwrap()).unwrap(),
			1
		);
		assert_eq!(
			decode_reclaim_entry(&store.get(&reclaim_entry_key(b"phase0", shard, 0)).unwrap()).unwrap(),
			ReclaimEntry::from_binding(&binding)
		);
		assert_eq!(binding.chunk_high_water, CHUNK_RESERVATION_STRIDE);
		assert_eq!(store.get(&binding_key(b"phase0", Path::new("segment"))), None);
		assert!(store.get(&chunk_key(b"phase0", binding.object_id, 0)).is_some());
		assert!(store.get(&chunk_key(b"phase0", binding.object_id, 1)).is_some());
		assert!(store
			.get(&tail_key(b"phase0", binding.object_id, binding.tail_revision))
			.is_some());
		assert_reclamation_inventory(&store, b"phase0");
	}

	#[test]
	fn applied_but_reported_failed_delete_is_not_enqueued_twice() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(b"contents").unwrap();
		writer.flush().unwrap();
		let binding = decode_binding(&store.get(&binding_key(b"phase0", Path::new("segment"))).unwrap()).unwrap();
		let shard = reclaim_shard(binding.object_id);

		store.fail_after_next_write();
		directory.delete(Path::new("segment")).unwrap();
		assert!(matches!(
			directory.delete(Path::new("segment")),
			Err(DeleteError::FileDoesNotExist(_))
		));
		assert_eq!(
			decode_u64(&store.get(&reclaim_tail_key(b"phase0", shard)).unwrap()).unwrap(),
			1
		);
		assert!(store.get(&reclaim_entry_key(b"phase0", shard, 0)).is_some());
		assert!(store.get(&reclaim_entry_key(b"phase0", shard, 1)).is_none());
	}

	#[test]
	fn failed_delete_reactivates_its_writer() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(b"first").unwrap();
		writer.flush().unwrap();

		store.fail_next_write();
		assert!(directory.delete(Path::new("segment")).is_err());
		writer.write_all(b" second").unwrap();
		writer.flush().unwrap();
		assert_eq!(
			directory
				.open_read(Path::new("segment"))
				.unwrap()
				.read_bytes()
				.unwrap()
				.as_slice(),
			b"first second"
		);
	}

	#[test]
	fn atomic_only_deletion_does_not_enqueue_reclamation() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		directory.atomic_write(Path::new("meta.json"), b"metadata").unwrap();

		directory.delete(Path::new("meta.json")).unwrap();

		let state = store.state.lock().unwrap();
		assert!(!state.visible.iter().any(|(key, value)| {
			value.value.is_some()
				&& (key.starts_with(&namespaced_prefix(b"phase0", KEY_KIND_RECLAIM_TAIL))
					|| key.starts_with(&namespaced_prefix(b"phase0", KEY_KIND_RECLAIM_ENTRY)))
		}));
	}

	#[test]
	fn occupied_reclaim_slot_fails_without_retiring_the_writer() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(b"first").unwrap();
		writer.flush().unwrap();
		let binding = decode_binding(&store.get(&binding_key(b"phase0", Path::new("segment"))).unwrap()).unwrap();
		let shard = reclaim_shard(binding.object_id);
		store
			.write(
				&[Mutation::Put(
					reclaim_entry_key(b"phase0", shard, 0),
					encode_reclaim_entry(&ReclaimEntry::from_binding(&binding)).unwrap(),
				)],
				WritePolicy::WAL,
			)
			.unwrap();

		assert!(directory.delete(Path::new("segment")).is_err());
		writer.write_all(b" second").unwrap();
		writer.flush().unwrap();
	}

	#[test]
	fn malformed_reclaim_tail_fails_without_retiring_the_writer() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(b"first").unwrap();
		writer.flush().unwrap();
		let binding = decode_binding(&store.get(&binding_key(b"phase0", Path::new("segment"))).unwrap()).unwrap();
		store
			.write(
				&[Mutation::Put(
					reclaim_tail_key(b"phase0", reclaim_shard(binding.object_id)),
					vec![1],
				)],
				WritePolicy::WAL,
			)
			.unwrap();

		let error = directory.delete(Path::new("segment")).unwrap_err();
		assert!(error.to_string().contains("invalid u64"));
		writer.write_all(b" second").unwrap();
		writer.flush().unwrap();
	}

	#[test]
	fn delete_uses_bounded_point_io() {
		let store = CountingKv::new();
		let directory = KvDirectory::new(store.clone());
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(b"contents").unwrap();
		writer.flush().unwrap();
		store.take_io_counts();

		directory.delete(Path::new("segment")).unwrap();

		assert_eq!(store.take_io_counts(), (5, 1, 4));
	}

	#[test]
	fn closed_writer_delete_skips_the_fence_reread() {
		let store = CountingKv::new();
		let directory = KvDirectory::new(store.clone());
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(b"contents").unwrap();
		writer.flush().unwrap();
		drop(writer);
		store.take_io_counts();

		directory.delete(Path::new("segment")).unwrap();

		assert_eq!(store.take_io_counts(), (4, 1, 4));
	}

	#[test]
	fn object_ids_are_reserved_in_strides() {
		let store = CountingKv::new();
		let directory = KvDirectory::new(store.clone());
		let first = directory.open_write(Path::new("first")).unwrap();
		assert_eq!(
			decode_u64(&store.inner.get(&counter_key(b"phase0")).unwrap()).unwrap(),
			OBJECT_ID_RESERVATION_STRIDE
		);
		store.take_io_counts();
		let second = directory.open_write(Path::new("second")).unwrap();
		assert_eq!(store.take_io_counts(), (2, 1, 1));
		assert_eq!(
			decode_binding(&store.inner.get(&binding_key(b"phase0", Path::new("second"))).unwrap())
				.unwrap()
				.object_id,
			2
		);
		drop((first, second));
	}

	#[test]
	fn live_writer_keeps_shared_directory_state_discoverable() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let state = Arc::downgrade(&directory.state);
		let mut writer = directory.open_write(Path::new("segment")).unwrap();
		writer.write_all(b"pending").unwrap();
		drop(directory);

		let reopened = FaultingDirectory::new(store);
		assert!(Arc::ptr_eq(&state.upgrade().unwrap(), &reopened.state));
		reopened.delete(Path::new("segment")).unwrap();
		assert_eq!(writer.flush().unwrap_err().kind(), io::ErrorKind::NotFound);
	}

	#[test]
	fn writer_claim_rechecks_retirement_after_registering_in_flight() {
		let fence = Arc::new(WriterFence::new(1));
		let (checked, reached_check) = std::sync::mpsc::channel();
		let (resume, may_resume) = std::sync::mpsc::channel();
		let claiming_fence = fence.clone();
		let claiming = std::thread::spawn(move || {
			claiming_fence
				.claim_after_first_check(|| {
					checked.send(()).unwrap();
					may_resume.recv().unwrap();
				})
				.map(drop)
		});
		reached_check.recv().unwrap();
		fence.retire_and_wait();
		resume.send(()).unwrap();

		assert_eq!(claiming.join().unwrap().unwrap_err().kind(), io::ErrorKind::NotFound);
		assert_eq!(fence.in_flight.load(Ordering::SeqCst), 0);
	}

	#[test]
	fn active_writer_release_does_not_take_the_retirement_mutex() {
		let fence = Arc::new(WriterFence::new(1));
		let waiting = fence.waiting.lock().unwrap();
		let releasing_fence = fence.clone();
		let (completed, completion) = std::sync::mpsc::channel();
		let releasing = std::thread::spawn(move || {
			drop(releasing_fence.claim().unwrap());
			completed.send(()).unwrap();
		});

		completion
			.recv_timeout(Duration::from_secs(5))
			.expect("active writer release took the retirement mutex");
		drop(waiting);
		releasing.join().unwrap();
	}

	#[test]
	fn delete_waits_for_in_flight_writer_storage() {
		for iteration in 0..32 {
			let store = BlockingKv::new();
			let directory = KvDirectory::new(store.clone());
			let path = PathBuf::from(format!("segment-{iteration}"));
			let mut writer = directory.open_write(&path).unwrap();
			let path_state = directory.state.paths.state(&path);
			let fence = path_state
				.lifecycle
				.lock()
				.unwrap_or_else(|poisoned| poisoned.into_inner())
				.writer
				.as_ref()
				.and_then(Weak::upgrade)
				.unwrap();
			writer.write_all(b"contents").unwrap();
			store.arm_next_write();
			let helper_store = store.clone();
			let deleting_directory = directory.clone();
			let deleting_path = path.clone();
			let helper = std::thread::spawn(move || {
				assert!(
					helper_store.wait_until_blocked(),
					"write did not reach the test barrier"
				);
				let deletion = std::thread::spawn(move || deleting_directory.delete(&deleting_path));
				let retired = wait_for_retirement(&fence);
				helper_store.release_write();
				let deletion = deletion.join().unwrap();
				assert!(retired, "delete did not retire the writer");
				deletion
			});
			writer.flush().unwrap();
			helper.join().unwrap().unwrap();
			assert_reclamation_inventory(&store.inner, b"phase0");
			assert_eq!(writer.flush().unwrap_err().kind(), io::ErrorKind::NotFound);
		}
	}

	#[test]
	fn open_read_does_not_wait_for_writer_retirement() {
		let store = BlockingKv::new();
		let directory = KvDirectory::new(store.clone());
		let path = Path::new("segment");
		let mut writer = directory.open_write(path).unwrap();
		writer.write_all(b"first").unwrap();
		writer.flush().unwrap();
		writer.write_all(b" second").unwrap();
		let fence = directory
			.state
			.paths
			.state(path)
			.lifecycle
			.lock()
			.unwrap_or_else(|poisoned| poisoned.into_inner())
			.writer
			.as_ref()
			.and_then(Weak::upgrade)
			.unwrap();
		store.arm_next_write();
		let helper_store = store.clone();
		let deleting_directory = directory.clone();
		let reading_directory = directory.clone();
		let helper = std::thread::spawn(move || {
			assert!(
				helper_store.wait_until_blocked(),
				"write did not reach the test barrier"
			);
			let deletion = std::thread::spawn(move || deleting_directory.delete(path));
			assert!(wait_for_retirement(&fence), "delete did not retire the writer");
			let (opened, received) = std::sync::mpsc::channel();
			let reading = std::thread::spawn(move || opened.send(reading_directory.open_read(path)).unwrap());
			let handle = received.recv_timeout(Duration::from_secs(5));
			helper_store.release_write();
			let handle = handle.expect("read waited for writer retirement").unwrap();
			deletion.join().unwrap().unwrap();
			reading.join().unwrap();
			handle
		});

		writer.flush().unwrap();
		let handle = helper.join().unwrap();
		assert_eq!(handle.read_bytes().unwrap().as_slice(), b"first");
	}

	#[test]
	fn delete_waits_for_in_flight_chunk_storage() {
		let store = BlockingKv::new();
		let directory = KvDirectory::new(store.clone());
		directory.ensure_format(true).unwrap();
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
		let mut writer = test_writer(&directory, binding);
		writer.tail = vec![7; CHUNK_SIZE];
		writer.dirty = true;
		let fence = writer.fence.clone();
		store.arm_next_write();
		let helper_store = store.clone();
		let deleting_directory = directory.clone();
		let helper = std::thread::spawn(move || {
			assert!(
				helper_store.wait_until_blocked(),
				"write did not reach the test barrier"
			);
			let deletion = std::thread::spawn(move || deleting_directory.delete(Path::new("segment")));
			let retired = wait_for_retirement(&fence);
			helper_store.release_write();
			let deletion = deletion.join().unwrap();
			assert!(retired, "delete did not retire the writer");
			deletion
		});

		writer.flush().unwrap();
		helper.join().unwrap().unwrap();
		assert!(store.inner.get(&chunk_key(b"phase0", 1, 0)).is_some());
		assert_reclamation_inventory(&store.inner, b"phase0");
	}

	#[test]
	fn different_reclaim_shards_enqueue_concurrently() {
		let store = BlockingKv::new();
		let directory = KvDirectory::new(store.clone());
		let mut first = directory.open_write(Path::new("first")).unwrap();
		let mut second = directory.open_write(Path::new("second")).unwrap();
		first.write_all(b"first").unwrap();
		first.flush().unwrap();
		second.write_all(b"second").unwrap();
		second.flush().unwrap();
		let first_binding =
			decode_binding(&store.inner.get(&binding_key(b"phase0", Path::new("first"))).unwrap()).unwrap();
		let second_binding =
			decode_binding(&store.inner.get(&binding_key(b"phase0", Path::new("second"))).unwrap()).unwrap();
		assert_ne!(
			reclaim_shard(first_binding.object_id),
			reclaim_shard(second_binding.object_id)
		);
		store.arm_next_write();

		let first_directory = directory.clone();
		let first_delete = std::thread::spawn(move || first_directory.delete(Path::new("first")));
		assert!(store.wait_until_blocked(), "write did not reach the test barrier");
		let second_directory = directory.clone();
		let (sent, received) = std::sync::mpsc::channel();
		let second_delete = std::thread::spawn(move || {
			sent.send(second_directory.delete(Path::new("second"))).unwrap();
		});
		let second_result = received.recv_timeout(Duration::from_secs(5));
		store.release_write();
		first_delete.join().unwrap().unwrap();
		second_delete.join().unwrap();
		second_result.expect("different reclaim shard was serialized").unwrap();
	}

	#[test]
	fn persisted_reclaim_tail_is_read_after_directory_reopen() {
		let store = FaultingKv::default();
		let first_directory = FaultingDirectory::new(store.clone());
		let first = first_directory.open_write(Path::new("first")).unwrap();
		let first_binding = decode_binding(&store.get(&binding_key(b"phase0", Path::new("first"))).unwrap()).unwrap();
		first_directory.delete(Path::new("first")).unwrap();
		drop((first, first_directory));

		let second_directory = FaultingDirectory::new(store.clone());
		let second = second_directory.open_write(Path::new("second")).unwrap();
		let binding = decode_binding(&store.get(&binding_key(b"phase0", Path::new("second"))).unwrap()).unwrap();
		let shard = reclaim_shard(binding.object_id);
		assert_eq!(reclaim_shard(first_binding.object_id), shard);
		second_directory.delete(Path::new("second")).unwrap();
		drop(second);

		assert_eq!(
			decode_u64(&store.get(&reclaim_tail_key(b"phase0", shard)).unwrap()).unwrap(),
			2
		);
		assert!(store.get(&reclaim_entry_key(b"phase0", shard, 0)).is_some());
		assert!(store.get(&reclaim_entry_key(b"phase0", shard, 1)).is_some());
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
		assert_ne!(reclaim_tail_key(b"a", 1), reclaim_entry_key(b"a", 1, 0));
		assert_ne!(reclaim_tail_key(b"a", 1), reclaim_tail_key(b"a\0", 1));
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
		let mut writer = test_writer(&directory, binding);
		writer.tail = vec![7; CHUNK_SIZE];
		writer.dirty = true;

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
		let mut writer = test_writer(&directory, binding);
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
	fn reclaim_entries_round_trip_and_enforce_the_extent_limit() {
		let entry = ReclaimEntry {
			object_id: 17,
			chunk_high_water: 7,
			tail_high_water: 11,
		};
		assert_eq!(
			decode_reclaim_entry(&encode_reclaim_entry(&entry).unwrap()).unwrap(),
			entry
		);
		let error = encode_reclaim_entry(&ReclaimEntry {
			object_id: 17,
			chunk_high_water: u32::MAX,
			tail_high_water: u64::MAX,
		})
		.unwrap_err();
		assert_eq!(error.kind(), io::ErrorKind::InvalidData);
		assert!(error.to_string().contains("extent"));
		assert_eq!(
			decode_reclaim_entry(&[RECLAIM_ENTRY_FORMAT_VERSION, RECLAIM_ENTRY_WHOLE_OBJECT])
				.unwrap_err()
				.kind(),
			io::ErrorKind::InvalidData
		);
		let mut zero_id = encode_reclaim_entry(&entry).unwrap();
		zero_id[2..10].fill(0);
		assert!(decode_reclaim_entry(&zero_id)
			.unwrap_err()
			.to_string()
			.contains("object id"));
		let mut unknown_kind = encode_reclaim_entry(&entry).unwrap();
		unknown_kind[1] = RECLAIM_ENTRY_WHOLE_OBJECT + 1;
		assert!(decode_reclaim_entry(&unknown_kind)
			.unwrap_err()
			.to_string()
			.contains("unsupported reclaim entry kind"));
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
		assert_reclamation_inventory(&store, b"phase0");
		let recovered = store.crash();
		assert_reclamation_inventory(&recovered, b"phase0");
		let reopened = tantivy::Index::open(FaultingDirectory::new(recovered)).unwrap();
		assert_eq!(reopened.searchable_segment_ids().unwrap().len(), 1);
		reopened
			.writer::<tantivy::TantivyDocument>(15_000_000)
			.unwrap()
			.wait_merging_threads()
			.unwrap();
	}

	#[test]
	fn tantivy_merge_enqueues_garbage_collection_and_recovers_it_atomically() {
		let store = FaultingKv::default();
		let directory = FaultingDirectory::new(store.clone());
		let mut schema = tantivy::schema::Schema::builder();
		let body = schema.add_text_field("body", tantivy::schema::TEXT);
		let index =
			tantivy::Index::create(directory.clone(), schema.build(), tantivy::IndexSettings::default()).unwrap();
		let mut writer = index.writer(15_000_000).unwrap();
		writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
		writer.add_document(tantivy::doc!(body => "first segment")).unwrap();
		writer.commit().unwrap();
		writer.add_document(tantivy::doc!(body => "second segment")).unwrap();
		writer.commit().unwrap();
		let segments = index.searchable_segment_ids().unwrap();
		assert_eq!(segments.len(), 2);

		writer.merge(&segments).wait().unwrap();
		writer.garbage_collect_files().wait().unwrap();

		assert!(reclaim_entry_count(&store, b"phase0") > 0);
		assert_reclamation_inventory(&store, b"phase0");
		directory
			.atomic_write(Path::new("test-durability-barrier"), b"complete")
			.unwrap();
		let recovered = store.crash();
		assert!(reclaim_entry_count(&recovered, b"phase0") > 0);
		assert_reclamation_inventory(&recovered, b"phase0");
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
