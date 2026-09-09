use std::fmt;
use std::future::Future;
use std::io::{self, Write};
use std::ops::Range;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
	Directory, DirectoryLock, FileHandle, OwnedBytes, TerminatingWrite, WatchCallback, WatchHandle, WritePtr,
	INDEX_WRITER_LOCK,
};
use tantivy::schema::{Schema, TEXT};
use tantivy::HasLen;
use tantivy::{doc, Index, IndexSettings, ReloadPolicy};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogicalReadMetrics {
	pub calls: u64,
	pub bytes_requested: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryCaseMetrics {
	pub name: &'static str,
	pub logical_reads: LogicalReadMetrics,
}

#[derive(Debug, Default)]
struct Counters {
	calls: AtomicU64,
	bytes_requested: AtomicU64,
}

#[derive(Clone)]
pub struct InstrumentedDirectory<D> {
	inner: D,
	counters: Arc<Counters>,
}

impl<D> InstrumentedDirectory<D> {
	pub fn new(inner: D) -> Self {
		Self {
			inner,
			counters: Arc::new(Counters::default()),
		}
	}

	pub fn logical_read_metrics(&self) -> LogicalReadMetrics {
		LogicalReadMetrics {
			calls: self.counters.calls.load(Ordering::Relaxed),
			bytes_requested: self.counters.bytes_requested.load(Ordering::Relaxed),
		}
	}
}

impl<D: fmt::Debug> fmt::Debug for InstrumentedDirectory<D> {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter
			.debug_tuple("InstrumentedDirectory")
			.field(&self.inner)
			.finish()
	}
}

#[derive(Debug)]
struct InstrumentedFileHandle {
	inner: Arc<dyn FileHandle>,
	counters: Arc<Counters>,
}

impl HasLen for InstrumentedFileHandle {
	fn len(&self) -> usize {
		self.inner.len()
	}
}

impl FileHandle for InstrumentedFileHandle {
	fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
		self.counters.calls.fetch_add(1, Ordering::Relaxed);
		self.counters
			.bytes_requested
			.fetch_add((range.end - range.start) as u64, Ordering::Relaxed);
		self.inner.read_bytes(range)
	}

	fn read_bytes_async<'life0, 'async_trait>(
		&'life0 self,
		range: Range<usize>,
	) -> Pin<Box<dyn Future<Output = io::Result<OwnedBytes>> + Send + 'async_trait>>
	where
		'life0: 'async_trait,
		Self: 'async_trait,
	{
		self.counters.calls.fetch_add(1, Ordering::Relaxed);
		self.counters
			.bytes_requested
			.fetch_add((range.end - range.start) as u64, Ordering::Relaxed);
		self.inner.read_bytes_async(range)
	}
}

impl<D> Directory for InstrumentedDirectory<D>
where
	D: Directory + Clone,
{
	fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
		Ok(Arc::new(InstrumentedFileHandle {
			inner: self.inner.get_file_handle(path)?,
			counters: self.counters.clone(),
		}))
	}

	fn delete(&self, path: &Path) -> Result<(), DeleteError> {
		self.inner.delete(path)
	}

	fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
		self.inner.exists(path)
	}

	fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
		self.inner.open_write(path)
	}

	fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
		let bytes = self.inner.atomic_read(path)?;
		self.counters.calls.fetch_add(1, Ordering::Relaxed);
		self.counters
			.bytes_requested
			.fetch_add(bytes.len() as u64, Ordering::Relaxed);
		Ok(bytes)
	}

	fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
		self.inner.atomic_write(path, data)
	}

	fn sync_directory(&self) -> io::Result<()> {
		self.inner.sync_directory()
	}

	fn acquire_lock(&self, lock: &tantivy::directory::Lock) -> Result<DirectoryLock, LockError> {
		self.inner.acquire_lock(lock)
	}

	fn watch(&self, callback: WatchCallback) -> tantivy::Result<WatchHandle> {
		self.inner.watch(callback)
	}
}

pub fn verify_directory_contract<D>(make_directory: impl Fn() -> D) -> Result<Vec<DirectoryCaseMetrics>, String>
where
	D: Directory + Clone,
{
	let mut results = Vec::new();
	run_contract_case(&mut results, "immediate creation", make_directory(), |directory| {
		verify_immediate_creation(directory)
	})?;
	run_contract_case(&mut results, "repeated flush", make_directory(), |directory| {
		verify_repeated_flush(directory)
	})?;
	run_contract_case(&mut results, "read and delete", make_directory(), |directory| {
		verify_write_read_delete(directory)
	})?;
	run_contract_case(&mut results, "missing paths", make_directory(), |directory| {
		verify_missing_file_errors(directory)
	})?;
	run_contract_case(&mut results, "atomic replacement", make_directory(), |directory| {
		verify_concurrent_atomic_visibility(directory)?;
		verify_atomic_metadata(directory)
	})?;
	run_contract_case(&mut results, "writer exclusion", make_directory(), |directory| {
		verify_in_process_writer_exclusion(directory)
	})?;
	run_contract_case(&mut results, "watch", make_directory(), |directory| {
		verify_watch(directory)
	})?;
	run_contract_case(&mut results, "directory sync", make_directory(), |directory| {
		directory.sync_directory().map_err(|error| error.to_string())
	})?;
	Ok(results)
}

pub fn verify_tantivy_lifecycle<D>(directory: D) -> Result<(), String>
where
	D: Directory + Clone,
{
	let mut schema_builder = Schema::builder();
	let body = schema_builder.add_text_field("body", TEXT);
	let schema = schema_builder.build();
	let index =
		Index::create(directory.clone(), schema, IndexSettings::default()).map_err(|error| error.to_string())?;
	let mut writer = index.writer(15_000_000).map_err(|error| error.to_string())?;
	writer
		.add_document(doc!(body => "red hiking boots"))
		.map_err(|error| error.to_string())?;
	writer
		.add_document(doc!(body => "blue running shoes"))
		.map_err(|error| error.to_string())?;
	writer.commit().map_err(|error| error.to_string())?;
	writer.wait_merging_threads().map_err(|error| error.to_string())?;

	verify_query_count(&index, body, "boots", 1)?;
	drop(index);
	let reopened = Index::open(directory).map_err(|error| error.to_string())?;
	verify_query_count(&reopened, body, "shoes", 1)
}

pub fn verify_large_file<D>(directory: D, chunk_size: usize) -> Result<(), String>
where
	D: Directory,
{
	let length = chunk_size
		.checked_mul(2)
		.and_then(|length| length.checked_add(17))
		.ok_or_else(|| "large-file test length overflowed".to_owned())?;
	let bytes = (0..length).map(|offset| (offset % 251) as u8).collect::<Vec<_>>();
	let path = Path::new("large-file");
	let mut writer = directory.open_write(path).map_err(|error| error.to_string())?;
	writer.write_all(&bytes).map_err(|error| error.to_string())?;
	writer.terminate().map_err(|error| error.to_string())?;

	let file = directory.open_read(path).map_err(|error| error.to_string())?;
	let full_chunk = file
		.slice(0..chunk_size)
		.read_bytes()
		.map_err(|error| error.to_string())?;
	if full_chunk.as_slice() != &bytes[..chunk_size] {
		return Err("large-file full chunk returned unexpected bytes".to_owned());
	}
	let range_start = chunk_size
		.checked_sub(13)
		.ok_or_else(|| "large-file test chunk size is too small".to_owned())?;
	let range = range_start..chunk_size + 19;
	let crossing = file
		.slice(range.clone())
		.read_bytes()
		.map_err(|error| error.to_string())?;
	if crossing.as_slice() != &bytes[range] {
		return Err("large-file boundary range returned unexpected bytes".to_owned());
	}
	let range = length - 30..length;
	let tail_crossing = file
		.slice(range.clone())
		.read_bytes()
		.map_err(|error| error.to_string())?;
	if tail_crossing.as_slice() != &bytes[range] {
		return Err("large-file tail boundary range returned unexpected bytes".to_owned());
	}
	Ok(())
}

fn verify_query_count(
	index: &Index,
	field: tantivy::schema::Field,
	query: &str,
	expected: usize,
) -> Result<(), String> {
	let reader = index
		.reader_builder()
		.reload_policy(ReloadPolicy::Manual)
		.try_into()
		.map_err(|error: tantivy::TantivyError| error.to_string())?;
	reader.reload().map_err(|error| error.to_string())?;
	let parsed = tantivy::query::QueryParser::for_index(index, vec![field])
		.parse_query(query)
		.map_err(|error| error.to_string())?;
	let count = reader
		.searcher()
		.search(&parsed, &tantivy::collector::Count)
		.map_err(|error| error.to_string())?;
	if count != expected {
		return Err(format!(
			"query {query:?} matched {count} documents instead of {expected}"
		));
	}
	Ok(())
}

fn run_contract_case<D>(
	results: &mut Vec<DirectoryCaseMetrics>,
	name: &'static str,
	directory: D,
	verify: impl FnOnce(&InstrumentedDirectory<D>) -> Result<(), String>,
) -> Result<(), String>
where
	D: Directory + Clone,
{
	let directory = InstrumentedDirectory::new(directory);
	verify(&directory).map_err(|error| format!("{name}: {error}"))?;
	results.push(DirectoryCaseMetrics {
		name,
		logical_reads: directory.logical_read_metrics(),
	});
	Ok(())
}

fn verify_immediate_creation(directory: &dyn Directory) -> Result<(), String> {
	let path = Path::new("immediate");
	let _writer = directory.open_write(path).map_err(|error| error.to_string())?;
	if !directory.exists(path).map_err(|error| error.to_string())? {
		return Err("new writer did not create the logical file".to_owned());
	}
	let file = directory.open_read(path).map_err(|error| error.to_string())?;
	if !file.is_empty() {
		return Err("new logical file was not empty".to_owned());
	}
	Ok(())
}

fn verify_repeated_flush(directory: &dyn Directory) -> Result<(), String> {
	let path = Path::new("growing");
	let mut writer = directory.open_write(path).map_err(|error| error.to_string())?;
	writer.write_all(b"abc").map_err(|error| error.to_string())?;
	writer.flush().map_err(|error| error.to_string())?;
	let first = directory.open_read(path).map_err(|error| error.to_string())?;
	if first.read_bytes().map_err(|error| error.to_string())?.as_slice() != b"abc" {
		return Err("first flush exposed unexpected bytes".to_owned());
	}

	writer.write_all(b"def").map_err(|error| error.to_string())?;
	writer.flush().map_err(|error| error.to_string())?;
	if first.read_bytes().map_err(|error| error.to_string())?.as_slice() != b"abc" {
		return Err("later flush changed an open file".to_owned());
	}
	drop(first);
	let second = directory.open_read(path).map_err(|error| error.to_string())?;
	if second.read_bytes().map_err(|error| error.to_string())?.as_slice() != b"abcdef" {
		return Err("second flush exposed unexpected bytes".to_owned());
	}

	writer.write_all(b"ghi").map_err(|error| error.to_string())?;
	writer.terminate().map_err(|error| error.to_string())?;
	if second.read_bytes().map_err(|error| error.to_string())?.as_slice() != b"abcdef" {
		return Err("termination changed an open file".to_owned());
	}
	drop(second);
	let terminated = directory.open_read(path).map_err(|error| error.to_string())?;
	if terminated.read_bytes().map_err(|error| error.to_string())?.as_slice() != b"abcdefghi" {
		return Err("termination exposed unexpected bytes".to_owned());
	}
	Ok(())
}

fn verify_missing_file_errors(directory: &dyn Directory) -> Result<(), String> {
	let path = Path::new("missing");
	if !matches!(directory.open_read(path), Err(OpenReadError::FileDoesNotExist(_))) {
		return Err("opening a missing file returned the wrong result".to_owned());
	}
	if !matches!(directory.delete(path), Err(DeleteError::FileDoesNotExist(_))) {
		return Err("deleting a missing file returned the wrong result".to_owned());
	}
	Ok(())
}

fn verify_write_read_delete<D>(directory: &D) -> Result<(), String>
where
	D: Directory + Clone,
{
	let path = Path::new("segment");
	let mut writer = directory.open_write(path).map_err(|error| error.to_string())?;
	writer.write_all(b"0123456789").map_err(|error| error.to_string())?;
	writer.terminate().map_err(|error| error.to_string())?;
	if !matches!(directory.open_write(path), Err(OpenWriteError::FileAlreadyExists(_))) {
		return Err("opening an existing file for write returned the wrong result".to_owned());
	}

	let file = directory.open_read(path).map_err(|error| error.to_string())?;
	let middle = file.slice(2..7).read_bytes().map_err(|error| error.to_string())?;
	if middle.as_slice() != b"23456" {
		return Err("range read returned unexpected bytes".to_owned());
	}
	let asynchronous = block_on(file.slice(7..10).read_bytes_async()).map_err(|error| error.to_string())?;
	if asynchronous.as_slice() != b"789" {
		return Err("asynchronous range read returned unexpected bytes".to_owned());
	}
	let empty = file.slice(4..4).read_bytes().map_err(|error| error.to_string())?;
	if !empty.is_empty() {
		return Err("zero-length range read returned bytes".to_owned());
	}
	let deleting_directory = directory.clone();
	let delete_result = std::thread::spawn(move || deleting_directory.delete(Path::new("segment")))
		.join()
		.map_err(|_| "segment deletion panicked".to_owned())?;
	let retained = file.read_bytes().map_err(|error| error.to_string())?;
	if retained.as_slice() != b"0123456789" {
		return Err("an open file changed while deletion was attempted".to_owned());
	}
	let deletion_succeeded = delete_result.is_ok();
	drop(retained);
	drop(asynchronous);
	drop(empty);
	drop(middle);
	drop(file);
	if !deletion_succeeded {
		directory.delete(path).map_err(|error| error.to_string())?;
	}
	if !matches!(directory.open_read(path), Err(OpenReadError::FileDoesNotExist(_))) {
		return Err("deleted file remained visible".to_owned());
	}
	Ok(())
}

fn verify_concurrent_atomic_visibility<D>(directory: &D) -> Result<(), String>
where
	D: Directory + Clone,
{
	let path = Path::new("meta.json");
	let before = vec![b'a'; 4096];
	let after = vec![b'b'; 4096];
	directory
		.atomic_write(path, &before)
		.map_err(|error| error.to_string())?;
	verify_concurrent_atomic_replacement(directory, &before, &after)
}

fn verify_concurrent_atomic_replacement<D>(directory: &D, before: &[u8], after: &[u8]) -> Result<(), String>
where
	D: Directory + Clone,
{
	let path = Path::new("meta.json");
	let observer_directory = directory.clone();
	let observer_before = before.to_vec();
	let observer_after = after.to_vec();
	let (ready_sender, ready_receiver) = mpsc::sync_channel(0);
	let observer = thread::spawn(move || -> Result<(), String> {
		let initial = observer_directory
			.atomic_read(path)
			.map_err(|error| error.to_string())?;
		if initial != observer_before {
			return Err("atomic metadata observer did not read the initial value".to_owned());
		}
		ready_sender.send(()).map_err(|error| error.to_string())?;
		let deadline = std::time::Instant::now() + Duration::from_secs(2);
		loop {
			let observed = observer_directory
				.atomic_read(path)
				.map_err(|error| error.to_string())?;
			if observed == observer_after {
				return Ok(());
			}
			if observed != observer_before {
				return Err("atomic metadata observer saw a partial value".to_owned());
			}
			if std::time::Instant::now() >= deadline {
				return Err("atomic metadata observer did not observe the replacement".to_owned());
			}
			thread::yield_now();
		}
	});
	ready_receiver
		.recv_timeout(Duration::from_secs(2))
		.map_err(|error| error.to_string())?;
	let write_result = atomic_replace_while_observed(directory, path, after);
	let observer_result = observer
		.join()
		.map_err(|_| "atomic metadata observer panicked".to_owned())?;
	write_result?;
	observer_result?;
	if directory.atomic_read(path).map_err(|error| error.to_string())? != after {
		return Err("atomic metadata replacement returned unexpected bytes".to_owned());
	}
	Ok(())
}

fn atomic_replace_while_observed<D>(directory: &D, path: &Path, data: &[u8]) -> Result<(), String>
where
	D: Directory,
{
	let deadline = std::time::Instant::now() + Duration::from_secs(2);
	loop {
		match directory.atomic_write(path, data) {
			Ok(()) => return Ok(()),
			Err(error)
				if cfg!(windows)
					&& error.kind() == io::ErrorKind::PermissionDenied
					&& std::time::Instant::now() < deadline =>
			{
				thread::yield_now();
			}
			Err(error) => return Err(error.to_string()),
		}
	}
}

fn block_on<F: Future>(future: F) -> F::Output {
	struct ThreadWake(thread::Thread);

	impl Wake for ThreadWake {
		fn wake(self: Arc<Self>) {
			self.0.unpark();
		}
	}

	let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
	let mut context = Context::from_waker(&waker);
	let mut future = std::pin::pin!(future);
	loop {
		match future.as_mut().poll(&mut context) {
			Poll::Ready(output) => return output,
			Poll::Pending => thread::park(),
		}
	}
}

pub fn verify_failed_atomic_replacement<D>(
	directory: &D,
	replacement: impl FnOnce() -> io::Result<()>,
) -> Result<(), String>
where
	D: Directory + Clone,
{
	let path = Path::new("meta.json");
	let before = directory.atomic_read(path).map_err(|error| error.to_string())?;
	let observer_directory = directory.clone();
	let observer_before = before.clone();
	let (ready_sender, ready_receiver) = mpsc::sync_channel(0);
	let (stop_sender, stop_receiver) = mpsc::channel();
	let observer = thread::spawn(move || -> Result<(), String> {
		ready_sender.send(()).map_err(|error| error.to_string())?;
		loop {
			match stop_receiver.try_recv() {
				Ok(()) | Err(mpsc::TryRecvError::Disconnected) => break,
				Err(mpsc::TryRecvError::Empty) => {}
			}
			let observed = observer_directory
				.atomic_read(path)
				.map_err(|error| error.to_string())?;
			if observed != observer_before {
				return Err("failed metadata replacement became observable".to_owned());
			}
			thread::yield_now();
		}
		Ok(())
	});
	ready_receiver
		.recv_timeout(Duration::from_secs(2))
		.map_err(|error| error.to_string())?;
	let replacement_result = replacement();
	let _ = stop_sender.send(());
	let observer_result = observer
		.join()
		.map_err(|_| "failed metadata observer panicked".to_owned())?;
	if replacement_result.is_ok() {
		return Err("fault injection did not fail the metadata replacement".to_owned());
	}
	observer_result?;
	let after = directory.atomic_read(path).map_err(|error| error.to_string())?;
	if before != after {
		return Err("failed metadata replacement became observable".to_owned());
	}
	Ok(())
}

fn verify_atomic_metadata(directory: &dyn Directory) -> Result<(), String> {
	let path = Path::new("meta.json");
	directory
		.atomic_write(path, b"first")
		.map_err(|error| error.to_string())?;
	if directory.atomic_read(path).map_err(|error| error.to_string())? != b"first" {
		return Err("atomic metadata read returned unexpected bytes".to_owned());
	}
	directory
		.atomic_write(path, b"second")
		.map_err(|error| error.to_string())?;
	if directory.atomic_read(path).map_err(|error| error.to_string())? != b"second" {
		return Err("atomic metadata replacement returned unexpected bytes".to_owned());
	}
	Ok(())
}

fn verify_in_process_writer_exclusion<D>(directory: &D) -> Result<(), String>
where
	D: Directory + Clone,
{
	let held = directory
		.acquire_lock(&INDEX_WRITER_LOCK)
		.map_err(|error| error.to_string())?;
	let contender = directory.clone();
	let result = std::thread::spawn(move || contender.acquire_lock(&INDEX_WRITER_LOCK).is_err())
		.join()
		.map_err(|_| "writer lock contender panicked".to_owned())?;
	if !result {
		return Err("two writers acquired the index lock".to_owned());
	}
	drop(held);
	directory
		.acquire_lock(&INDEX_WRITER_LOCK)
		.map_err(|error| error.to_string())?;
	Ok(())
}

fn verify_watch(directory: &dyn Directory) -> Result<(), String> {
	let (sender, receiver) = std::sync::mpsc::sync_channel(1);
	let armed = Arc::new(AtomicBool::new(false));
	let callback_armed = armed.clone();
	let _handle = directory
		.watch(WatchCallback::new(move || {
			if callback_armed.load(Ordering::Acquire) {
				let _ = sender.try_send(());
			}
		}))
		.map_err(|error| error.to_string())?;
	armed.store(true, Ordering::Release);
	directory
		.atomic_write(Path::new("meta.json"), b"watched")
		.map_err(|error| error.to_string())?;
	let deadline = std::time::Instant::now() + Duration::from_secs(2);
	loop {
		let remaining = deadline.saturating_duration_since(std::time::Instant::now());
		match receiver.recv_timeout(remaining) {
			Ok(()) => {
				let observed = directory
					.atomic_read(Path::new("meta.json"))
					.map_err(|error| error.to_string())?;
				if observed == b"watched" {
					return Ok(());
				}
			}
			Err(_) => return Err("meta.json watch did not observe the tested write".to_owned()),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::collections::HashMap;
	use std::path::PathBuf;
	use std::sync::atomic::AtomicBool;
	use std::sync::RwLock;

	use tantivy::directory::{Lock, MmapDirectory};

	#[derive(Clone, Debug)]
	struct BrokenDirectory {
		inner: MmapDirectory,
		atomic_files: Arc<RwLock<HashMap<PathBuf, Vec<u8>>>>,
		ignore_locks: bool,
		fail_next_atomic_write: Arc<AtomicBool>,
		non_atomic_next_write: Arc<AtomicBool>,
		non_atomic_write_active: Arc<AtomicBool>,
		partial_write_observed: Arc<AtomicBool>,
	}

	impl BrokenDirectory {
		fn new(ignore_locks: bool) -> Self {
			Self {
				inner: MmapDirectory::create_from_tempdir().unwrap(),
				atomic_files: Arc::new(RwLock::new(HashMap::new())),
				ignore_locks,
				fail_next_atomic_write: Arc::new(AtomicBool::new(false)),
				non_atomic_next_write: Arc::new(AtomicBool::new(false)),
				non_atomic_write_active: Arc::new(AtomicBool::new(false)),
				partial_write_observed: Arc::new(AtomicBool::new(false)),
			}
		}

		fn fail_next_atomic_write(&self) {
			self.fail_next_atomic_write.store(true, Ordering::Release);
		}

		fn make_next_write_non_atomic(&self) {
			self.non_atomic_next_write.store(true, Ordering::Release);
			self.partial_write_observed.store(false, Ordering::Release);
		}
	}

	impl Directory for BrokenDirectory {
		fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
			self.inner.get_file_handle(path)
		}

		fn delete(&self, path: &Path) -> Result<(), DeleteError> {
			self.inner.delete(path)
		}

		fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
			self.inner.exists(path)
		}

		fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
			self.inner.open_write(path)
		}

		fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
			let result = self
				.atomic_files
				.read()
				.unwrap()
				.get(path)
				.cloned()
				.ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()));
			if self.non_atomic_write_active.load(Ordering::Acquire)
				&& match &result {
					Ok(bytes) => bytes.len() != 4096,
					Err(_) => true,
				} {
				self.partial_write_observed.store(true, Ordering::Release);
			}
			result
		}

		fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
			if self.non_atomic_next_write.swap(false, Ordering::AcqRel) {
				self.non_atomic_write_active.store(true, Ordering::Release);
				self.atomic_files
					.write()
					.unwrap()
					.insert(path.to_path_buf(), data[..data.len() / 2].to_vec());
				let deadline = std::time::Instant::now() + Duration::from_secs(2);
				while !self.partial_write_observed.load(Ordering::Acquire) {
					if std::time::Instant::now() >= deadline {
						self.non_atomic_write_active.store(false, Ordering::Release);
						return Err(io::Error::other("observer did not inspect partial metadata"));
					}
					thread::yield_now();
				}
				self.atomic_files
					.write()
					.unwrap()
					.insert(path.to_path_buf(), data.to_vec());
				self.non_atomic_write_active.store(false, Ordering::Release);
				return Ok(());
			}
			if !self.fail_next_atomic_write.swap(false, Ordering::AcqRel) {
				self.atomic_files
					.write()
					.unwrap()
					.insert(path.to_path_buf(), data.to_vec());
				return Ok(());
			}
			let before = self.atomic_files.read().unwrap().get(path).cloned().unwrap_or_default();
			self.non_atomic_write_active.store(true, Ordering::Release);
			self.partial_write_observed.store(false, Ordering::Release);
			self.atomic_files
				.write()
				.unwrap()
				.insert(path.to_path_buf(), data[..data.len() / 2].to_vec());
			let deadline = std::time::Instant::now() + Duration::from_secs(2);
			while !self.partial_write_observed.load(Ordering::Acquire) {
				if std::time::Instant::now() >= deadline {
					self.non_atomic_write_active.store(false, Ordering::Release);
					return Err(io::Error::other("observer did not inspect failed partial metadata"));
				}
				thread::yield_now();
			}
			self.atomic_files.write().unwrap().insert(path.to_path_buf(), before);
			self.non_atomic_write_active.store(false, Ordering::Release);
			Err(io::Error::other("injected partial write"))
		}

		fn sync_directory(&self) -> io::Result<()> {
			self.inner.sync_directory()
		}

		fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
			if self.ignore_locks {
				return Ok(DirectoryLock::from(Box::new(())));
			}
			self.inner.acquire_lock(lock)
		}

		fn watch(&self, callback: WatchCallback) -> tantivy::Result<WatchHandle> {
			self.inner.watch(callback)
		}
	}

	#[test]
	fn mmap_directory_satisfies_the_contract() {
		let metrics = verify_directory_contract(|| MmapDirectory::create_from_tempdir().unwrap()).unwrap();
		assert_eq!(metrics.len(), 8);
		assert_eq!(metrics[0].name, "immediate creation");
		assert_eq!(metrics[1].name, "repeated flush");
		assert!(metrics.iter().any(|case| case.logical_reads.calls > 0));
	}

	#[test]
	fn mmap_directory_supports_a_real_tantivy_lifecycle() {
		verify_tantivy_lifecycle(MmapDirectory::create_from_tempdir().unwrap()).unwrap();
	}

	#[test]
	fn mmap_directory_supports_large_file_ranges() {
		verify_large_file(MmapDirectory::create_from_tempdir().unwrap(), 256 * 1024).unwrap();
	}

	#[test]
	fn harness_rejects_non_exclusive_locks() {
		let directory = BrokenDirectory::new(true);
		assert!(verify_in_process_writer_exclusion(&directory).is_err());
	}

	#[test]
	fn harness_rejects_visible_partial_metadata() {
		let directory = BrokenDirectory::new(false);
		let before = vec![b'a'; 4096];
		let after = vec![b'b'; 4096];
		directory.atomic_write(Path::new("meta.json"), &before).unwrap();
		directory.fail_next_atomic_write();
		assert_eq!(
			verify_failed_atomic_replacement(&directory, || directory.atomic_write(Path::new("meta.json"), &after))
				.unwrap_err(),
			"failed metadata replacement became observable"
		);
	}

	#[test]
	fn harness_rejects_partial_metadata_during_successful_replacement() {
		let directory = BrokenDirectory::new(false);
		let before = vec![b'a'; 4096];
		let after = vec![b'b'; 4096];
		directory.atomic_write(Path::new("meta.json"), &before).unwrap();
		directory.make_next_write_non_atomic();
		assert_eq!(
			verify_concurrent_atomic_replacement(&directory, &before, &after).unwrap_err(),
			"atomic metadata observer saw a partial value"
		);
	}
}
