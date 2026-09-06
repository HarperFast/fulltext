use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::io;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
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

#[derive(Clone, Debug, Default)]
pub struct FaultingKv {
	state: Arc<Mutex<State>>,
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

	pub fn sync_wal(&self) -> io::Result<()> {
		let mut state = self.state.lock().unwrap();
		let pending = std::mem::take(&mut state.pending_wal);
		for (key, entry) in pending {
			apply_if_newer(&mut state.durable, key, entry);
		}
		Ok(())
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
		KvStoreIdentity(0, Arc::as_ptr(&self.state) as usize as u64, 0)
	}

	fn read(&self, key: &[u8]) -> io::Result<Option<OwnedBytes>> {
		Ok(self.get(key).map(OwnedBytes::new))
	}

	fn write(&self, mutations: &[Mutation], policy: WritePolicy) -> io::Result<()> {
		FaultingKv::write(self, mutations, policy)
	}

	fn sync(&self) -> io::Result<()> {
		self.sync_wal()
	}
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

		let mut logical_offset = 0usize;
		let mut copied = Vec::new();
		for fragment in 0..self.binding.fragments {
			let value = self
				.store
				.read(&fragment_key(&self.namespace, self.binding.object_id, fragment))?
				.ok_or_else(|| io::Error::other("binding references a missing fragment"))?;
			let fragment_end = logical_offset
				.checked_add(value.len())
				.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "fragment length overflow"))?;
			if range.start < fragment_end && range.end > logical_offset {
				let start = range.start.saturating_sub(logical_offset);
				let end = value.len().min(range.end - logical_offset);
				if copied.is_empty() && range.start >= logical_offset && range.end <= fragment_end {
					return Ok(value.slice(start..end));
				}
				if copied.is_empty() {
					copied
						.try_reserve(range.len())
						.map_err(|_| io::Error::other("requested file range cannot be allocated"))?;
				}
				copied.extend_from_slice(&value[start..end]);
			}
			logical_offset = fragment_end;
			if logical_offset >= range.end {
				break;
			}
		}
		if copied.len() != range.len() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"binding length does not match its fragments",
			));
		}
		Ok(OwnedBytes::new(copied))
	}

	async fn read_bytes_async(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
		self.read_bytes(range)
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
			fragments: 0,
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
			pending: Vec::new(),
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
	pending: Vec<u8>,
}

impl<S: KvStore> Write for KvWriter<S> {
	fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
		self.pending
			.try_reserve(bytes.len())
			.map_err(|_| io::Error::other("writer buffer cannot be allocated"))?;
		self.pending.extend_from_slice(bytes);
		Ok(bytes.len())
	}

	fn flush(&mut self) -> io::Result<()> {
		if self.pending.is_empty() {
			return Ok(());
		}
		let _mutation = self.state.mutation.lock().unwrap();
		let current = self
			.store
			.read(&binding_key(&self.namespace, &self.path))?
			.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file was deleted while its writer was open"))?;
		if decode_binding(&current)?.object_id != self.binding.object_id {
			return Err(io::Error::new(
				io::ErrorKind::NotFound,
				"file was replaced while its writer was open",
			));
		}
		let fragment = self.binding.fragments;
		let visible_length = self
			.binding
			.visible_length
			.checked_add(self.pending.len())
			.ok_or_else(|| io::Error::other("visible length exhausted"))?;
		let next = Binding {
			object_id: self.binding.object_id,
			fragments: fragment
				.checked_add(1)
				.ok_or_else(|| io::Error::other("fragment count exhausted"))?,
			visible_length,
		};
		let pending = std::mem::take(&mut self.pending);
		let mutations = [
			Mutation::Put(fragment_key(&self.namespace, self.binding.object_id, fragment), pending),
			Mutation::Put(binding_key(&self.namespace, &self.path), encode_binding(&next)),
		];
		if let Err(error) = self.store.write(&mutations, WritePolicy::WAL) {
			if let Mutation::Put(_, pending) = mutations.into_iter().next().unwrap() {
				self.pending = pending;
			}
			return Err(error);
		}
		self.binding = next;
		Ok(())
	}
}

impl<S: KvStore> TerminatingWrite for KvWriter<S> {
	fn terminate_ref(&mut self, _: tantivy::directory::AntiCallToken) -> io::Result<()> {
		self.flush()
	}
}

#[derive(Clone, Debug)]
struct Binding {
	object_id: u64,
	fragments: u32,
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

fn fragment_key(namespace: &[u8], object_id: u64, fragment: u32) -> Vec<u8> {
	let mut key = namespaced_prefix(namespace, b"fragment/");
	key.extend_from_slice(&object_id.to_be_bytes());
	key.extend_from_slice(&fragment.to_be_bytes());
	key
}

fn encode_binding(binding: &Binding) -> Vec<u8> {
	let mut bytes = Vec::with_capacity(21);
	bytes.push(1);
	bytes.extend_from_slice(&binding.object_id.to_be_bytes());
	bytes.extend_from_slice(&binding.fragments.to_be_bytes());
	bytes.extend_from_slice(&(binding.visible_length as u64).to_be_bytes());
	bytes
}

fn decode_binding(bytes: &[u8]) -> io::Result<Binding> {
	if bytes.len() != 21 || bytes[0] != 1 {
		return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid binding"));
	}
	let visible_length = usize::try_from(decode_u64(&bytes[13..21])?)
		.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "binding length exceeds this platform"))?;
	Ok(Binding {
		object_id: decode_u64(&bytes[1..9])?,
		fragments: u32::from_be_bytes(
			bytes[9..13]
				.try_into()
				.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid fragment count"))?,
		),
		visible_length,
	})
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
	fn file_handle_reads_across_fragment_boundaries() {
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
