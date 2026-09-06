use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tantivy::directory::error::{DeleteError, OpenReadError, OpenWriteError};
use tantivy::directory::{
	Directory, FileHandle, OwnedBytes, TerminatingWrite, WatchCallback, WatchCallbackList, WatchHandle, WritePtr,
};

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

pub trait KvStore: Clone + Send + Sync + 'static {
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
	state: Arc<DirectoryState>,
}

pub type FaultingDirectory = KvDirectory<FaultingKv>;

struct DirectoryState {
	mutation: Mutex<()>,
	watches: WatchCallbackList,
}

impl<S> fmt::Debug for KvDirectory<S> {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("KvDirectory")
	}
}

impl<S> KvDirectory<S> {
	pub fn new(store: S) -> Self {
		Self {
			store,
			state: Arc::new(DirectoryState {
				mutation: Mutex::new(()),
				watches: WatchCallbackList::default(),
			}),
		}
	}

	fn read_binding(&self, path: &Path) -> Result<Binding, OpenReadError>
	where
		S: KvStore,
	{
		let bytes = self
			.store
			.read(&binding_key(path))
			.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?
			.ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))?;
		decode_binding(&bytes).map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))
	}

	fn file_bytes(&self, path: &Path) -> Result<Vec<u8>, OpenReadError>
	where
		S: KvStore,
	{
		let binding = self.read_binding(path)?;
		let mut bytes = Vec::with_capacity(binding.visible_length);
		for fragment in 0..binding.fragments {
			let value = self
				.store
				.read(&fragment_key(binding.object_id, fragment))
				.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?
				.ok_or_else(|| {
					OpenReadError::wrap_io_error(
						io::Error::other("binding references a missing fragment"),
						path.to_path_buf(),
					)
				})?;
			bytes.extend_from_slice(&value);
		}
		if bytes.len() != binding.visible_length {
			return Err(OpenReadError::wrap_io_error(
				io::Error::other("binding length does not match its fragments"),
				path.to_path_buf(),
			));
		}
		Ok(bytes)
	}
}

impl<S: KvStore> Directory for KvDirectory<S> {
	fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
		Ok(Arc::new(OwnedBytes::new(self.file_bytes(path)?)))
	}

	fn delete(&self, path: &Path) -> Result<(), DeleteError> {
		let _mutation = self.state.mutation.lock().unwrap();
		let binding = binding_key(path);
		let atomic = atomic_key(path);
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
			.read(&binding_key(path))
			.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?
			.is_some()
			|| self
				.store
				.read(&atomic_key(path))
				.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?
				.is_some())
	}

	fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
		let _mutation = self.state.mutation.lock().unwrap();
		let key = binding_key(path);
		if self
			.store
			.read(&key)
			.map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?
			.is_some()
			|| self
				.store
				.read(&atomic_key(path))
				.map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?
				.is_some()
		{
			return Err(OpenWriteError::FileAlreadyExists(path.to_path_buf()));
		}
		let counter = self
			.store
			.read(COUNTER_KEY)
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
					Mutation::Put(COUNTER_KEY.to_vec(), object_id.to_be_bytes().to_vec()),
					Mutation::Put(key, encode_binding(&binding)),
				],
				WritePolicy::WAL,
			)
			.map_err(|error| OpenWriteError::wrap_io_error(error, path.to_path_buf()))?;
		Ok(std::io::BufWriter::new(Box::new(KvWriter {
			store: self.store.clone(),
			state: self.state.clone(),
			path: path.to_path_buf(),
			binding,
			pending: Vec::new(),
		})))
	}

	fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
		self.store
			.read(&atomic_key(path))
			.map_err(|error| OpenReadError::wrap_io_error(error, path.to_path_buf()))?
			.map(|bytes| bytes.as_slice().to_vec())
			.ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
	}

	fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
		let _mutation = self.state.mutation.lock().unwrap();
		self.store
			.write(&[Mutation::Put(atomic_key(path), data.to_vec())], WritePolicy::WAL_SYNC)?;
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
}

struct KvWriter<S> {
	store: S,
	state: Arc<DirectoryState>,
	path: PathBuf,
	binding: Binding,
	pending: Vec<u8>,
}

impl<S: KvStore> Write for KvWriter<S> {
	fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
		self.pending.extend_from_slice(bytes);
		Ok(bytes.len())
	}

	fn flush(&mut self) -> io::Result<()> {
		if self.pending.is_empty() {
			return Ok(());
		}
		let _mutation = self.state.mutation.lock().unwrap();
		let fragment = self.binding.fragments;
		let visible_length = self
			.binding
			.visible_length
			.checked_add(self.pending.len())
			.ok_or_else(|| io::Error::other("visible length exhausted"))?;
		let next = Binding {
			object_id: self.binding.object_id,
			fragments: fragment + 1,
			visible_length,
		};
		self.store.write(
			&[
				Mutation::Put(fragment_key(self.binding.object_id, fragment), self.pending.clone()),
				Mutation::Put(binding_key(&self.path), encode_binding(&next)),
			],
			WritePolicy::WAL,
		)?;
		self.binding = next;
		self.pending.clear();
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

const COUNTER_KEY: &[u8] = b"phase0/counter";

fn binding_key(path: &Path) -> Vec<u8> {
	prefixed_path(b"phase0/binding/", path)
}

fn atomic_key(path: &Path) -> Vec<u8> {
	prefixed_path(b"phase0/atomic/", path)
}

fn prefixed_path(prefix: &[u8], path: &Path) -> Vec<u8> {
	let mut key = Vec::with_capacity(prefix.len() + path.as_os_str().as_encoded_bytes().len());
	key.extend_from_slice(prefix);
	key.extend_from_slice(path.as_os_str().as_encoded_bytes());
	key
}

fn fragment_key(object_id: u64, fragment: u32) -> Vec<u8> {
	let mut key = b"phase0/fragment/".to_vec();
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
	fn directory_supports_a_real_tantivy_lifecycle_and_crash_reopen() {
		let store = FaultingKv::default();
		verify_tantivy_lifecycle(FaultingDirectory::new(store.clone())).unwrap();
		let reopened = tantivy::Index::open(FaultingDirectory::new(store.crash())).unwrap();
		assert_eq!(reopened.searchable_segment_ids().unwrap().len(), 1);
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
