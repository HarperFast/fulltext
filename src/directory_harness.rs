use std::fmt;
use std::io::{self, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
	Directory, DirectoryLock, FileHandle, OwnedBytes, WatchCallback, WatchHandle, WritePtr, INDEX_WRITER_LOCK,
};
use tantivy::HasLen;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReadMetrics {
	pub calls: u64,
	pub bytes_requested: u64,
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

	pub fn read_metrics(&self) -> ReadMetrics {
		ReadMetrics {
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
		self.inner.atomic_read(path)
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

pub fn verify_directory<D>(directory: D) -> Result<ReadMetrics, String>
where
	D: Directory + Clone,
{
	let directory = InstrumentedDirectory::new(directory);
	verify_write_read_delete(&directory)?;
	verify_atomic_metadata(&directory)?;
	verify_writer_exclusion(&directory)?;
	verify_watch(&directory)?;
	directory.sync_directory().map_err(|error| error.to_string())?;
	Ok(directory.read_metrics())
}

fn verify_write_read_delete<D>(directory: &D) -> Result<(), String>
where
	D: Directory + Clone,
{
	let path = Path::new("segment");
	let mut writer = directory.open_write(path).map_err(|error| error.to_string())?;
	writer.write_all(b"0123456789").map_err(|error| error.to_string())?;
	writer.flush().map_err(|error| error.to_string())?;

	let file = directory.open_read(path).map_err(|error| error.to_string())?;
	let middle = file.slice(2..7).read_bytes().map_err(|error| error.to_string())?;
	if middle.as_slice() != b"23456" {
		return Err("range read returned unexpected bytes".to_owned());
	}
	let deleting_directory = directory.clone();
	std::thread::spawn(move || deleting_directory.delete(Path::new("segment")))
		.join()
		.map_err(|_| "segment deletion panicked".to_owned())?
		.map_err(|error| error.to_string())?;
	let retained = file.read_bytes().map_err(|error| error.to_string())?;
	if retained.as_slice() != b"0123456789" {
		return Err("an open file changed after deletion".to_owned());
	}
	Ok(())
}

pub fn verify_failed_atomic_replacement(
	directory: &dyn Directory,
	replacement: impl FnOnce() -> io::Result<()>,
) -> Result<(), String> {
	let path = Path::new("meta.json");
	let before = directory.atomic_read(path).map_err(|error| error.to_string())?;
	if replacement().is_ok() {
		return Err("fault injection did not fail the metadata replacement".to_owned());
	}
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

fn verify_writer_exclusion<D>(directory: &D) -> Result<(), String>
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
	let _handle = directory
		.watch(WatchCallback::new(move || {
			let _ = sender.try_send(());
		}))
		.map_err(|error| error.to_string())?;
	directory
		.atomic_write(Path::new("meta.json"), b"watched")
		.map_err(|error| error.to_string())?;
	receiver
		.recv_timeout(Duration::from_secs(2))
		.map_err(|_| "meta.json watch did not fire".to_owned())
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::atomic::AtomicBool;

	use tantivy::directory::{Lock, MmapDirectory};

	#[derive(Clone, Debug)]
	struct BrokenDirectory {
		inner: MmapDirectory,
		ignore_locks: bool,
		fail_next_atomic_write: Arc<AtomicBool>,
	}

	impl BrokenDirectory {
		fn new(ignore_locks: bool) -> Self {
			Self {
				inner: MmapDirectory::create_from_tempdir().unwrap(),
				ignore_locks,
				fail_next_atomic_write: Arc::new(AtomicBool::new(false)),
			}
		}

		fn fail_next_atomic_write(&self) {
			self.fail_next_atomic_write.store(true, Ordering::Release);
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
			self.inner.atomic_read(path)
		}

		fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
			if !self.fail_next_atomic_write.swap(false, Ordering::AcqRel) {
				return self.inner.atomic_write(path, data);
			}
			if self.inner.exists(path).unwrap_or(false) {
				self.inner.delete(path).map_err(io::Error::other)?;
			}
			let mut writer = self.inner.open_write(path).map_err(io::Error::other)?;
			writer.write_all(&data[..data.len() / 2])?;
			writer.flush()?;
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
		let directory = MmapDirectory::create_from_tempdir().unwrap();
		let metrics = verify_directory(directory).unwrap();
		assert_eq!(metrics.calls, 2);
		assert_eq!(metrics.bytes_requested, 15);
	}

	#[test]
	fn harness_rejects_non_exclusive_locks() {
		let directory = BrokenDirectory::new(true);
		assert!(verify_writer_exclusion(&directory).is_err());
	}

	#[test]
	fn harness_rejects_visible_partial_metadata() {
		let directory = BrokenDirectory::new(false);
		directory.atomic_write(Path::new("meta.json"), b"stable").unwrap();
		directory.fail_next_atomic_write();
		assert!(verify_failed_atomic_replacement(&directory, || {
			directory.atomic_write(Path::new("meta.json"), b"replacement")
		})
		.is_err());
	}
}
