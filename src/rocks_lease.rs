use std::ffi::{c_char, c_void};
use std::io;
use std::ops::Deref;
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::{Arc, OnceLock};

use stable_deref_trait::StableDeref;
use tantivy::directory::OwnedBytes;

use crate::phase0::{KvStore, Mutation, WritePolicy};

const LEASE_MAGIC: u64 = 0x4852_4653_544c_5331;
const ABI_MAJOR: u32 = 1;
const TYPE_TAG_LOWER: u64 = 0x72f7_ab4f_5277_4465;
const TYPE_TAG_UPPER: u64 = 0xb654_680e_7e1e_4db9;
const CAP_GET_OWNED: u64 = 1 << 0;
const CAP_WRITE_BATCH: u64 = 1 << 1;
const CAP_SCAN_PAGE: u64 = 1 << 2;
const CAP_STATS: u64 = 1 << 3;
const REQUIRED_CAPABILITIES: u64 = CAP_GET_OWNED | CAP_WRITE_BATCH | CAP_SCAN_PAGE | CAP_STATS;

const STATUS_OK: u32 = 0;
const STATUS_NOT_FOUND: u32 = 1;
const STATUS_BUSY: u32 = 6;

const STATE_ACTIVE: u32 = 0;
const STATE_CLOSING: u32 = 1;
const STATE_REVOKED: u32 = 2;

const MUTATION_PUT: u32 = 1;
const MUTATION_DELETE: u32 = 2;
const POLICY_WAL: u32 = 1;
const POLICY_WAL_SYNC: u32 = 2;
const POLICY_NO_WAL: u32 = 3;
const MAX_BUILD_IDENTITY_BYTES: u64 = 4_096;

#[repr(C)]
struct ByteSpan {
	data: *const u8,
	length: u64,
}

#[repr(C)]
struct StatusBuffer {
	struct_size: u32,
	reserved: u32,
	data: *mut c_char,
	capacity: u64,
	length: u64,
}

#[repr(C)]
struct OwnedBytesResult {
	struct_size: u32,
	reserved: u32,
	data: *const u8,
	length: u64,
	release_context: *mut c_void,
	release: Option<unsafe extern "C" fn(*mut c_void)>,
}

#[repr(C)]
struct StorageMutation {
	struct_size: u32,
	kind: u32,
	key: ByteSpan,
	value: ByteSpan,
}

#[repr(C)]
#[derive(Default)]
pub struct StorageStats {
	struct_size: u32,
	reserved: u32,
	pub get_operations: u64,
	pub scan_operations: u64,
	pub batch_operations: u64,
	pub requested_bytes: u64,
	pub returned_bytes: u64,
	pub copied_bytes: u64,
	pub live_owned_buffers: u64,
	pub provider_errors: u64,
}

type RetainFn = unsafe extern "C" fn(*mut c_void, *mut StatusBuffer) -> u32;
type ReleaseFn = unsafe extern "C" fn(*mut c_void);
type PollStateFn = unsafe extern "C" fn(*mut c_void) -> u32;
type GetOwnedFn = unsafe extern "C" fn(*mut c_void, ByteSpan, *mut OwnedBytesResult, *mut StatusBuffer) -> u32;
type WriteBatchFn = unsafe extern "C" fn(*mut c_void, *const StorageMutation, u64, u32, *mut StatusBuffer) -> u32;
type ScanPageFn =
	unsafe extern "C" fn(*mut c_void, ByteSpan, ByteSpan, u64, u64, *mut OwnedBytesResult, *mut StatusBuffer) -> u32;
type CollectStatsFn = unsafe extern "C" fn(*mut c_void, *mut StorageStats, *mut StatusBuffer) -> u32;

#[repr(C)]
struct LeasePrefix {
	magic: u64,
	abi_major: u32,
	abi_minor: u32,
	struct_size: u32,
	status_size: u32,
	capabilities: u64,
	provider_image_token: u64,
}

#[repr(C)]
struct LeaseV1 {
	magic: u64,
	abi_major: u32,
	abi_minor: u32,
	struct_size: u32,
	status_size: u32,
	capabilities: u64,
	provider_image_token: u64,
	database_incarnation: u64,
	column_family_incarnation: u64,
	rocksdb_major: u32,
	rocksdb_minor: u32,
	rocksdb_patch: u32,
	reserved: u32,
	provider_build_identity: ByteSpan,
	context: *mut c_void,
	retain: Option<RetainFn>,
	release: Option<ReleaseFn>,
	poll_state: Option<PollStateFn>,
	get_owned: Option<GetOwnedFn>,
	write_batch: Option<WriteBatchFn>,
	scan_page: Option<ScanPageFn>,
	collect_stats: Option<CollectStatsFn>,
}

static PROVIDER_IMAGE_TOKEN: OnceLock<u64> = OnceLock::new();

struct RocksLeaseInner {
	table: NonNull<LeaseV1>,
	provider_build_identity: String,
}

unsafe impl Send for RocksLeaseInner {}
unsafe impl Sync for RocksLeaseInner {}

impl Drop for RocksLeaseInner {
	fn drop(&mut self) {
		let table = unsafe { self.table.as_ref() };
		if let Some(release) = table.release {
			unsafe { release(table.context) };
		}
	}
}

#[derive(Clone)]
pub struct RocksLease {
	inner: Arc<RocksLeaseInner>,
}

impl RocksLease {
	/// # Safety
	///
	/// `table` must come from a type-tag-validated rocksdb-js External that stays
	/// alive until this function returns. The provider's successful retain call
	/// then owns the lifetime used by `RocksLease`.
	pub unsafe fn from_table(table: *mut c_void) -> io::Result<Self> {
		let prefix = NonNull::new(table.cast::<LeasePrefix>())
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "storage lease is null"))?;
		let prefix = unsafe { prefix.as_ref() };
		if prefix.magic != LEASE_MAGIC {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"storage lease magic does not match",
			));
		}
		if prefix.abi_major != ABI_MAJOR {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"storage lease ABI is incompatible",
			));
		}
		if usize::try_from(prefix.struct_size).unwrap_or(0) < size_of::<LeaseV1>()
			|| usize::try_from(prefix.status_size).unwrap_or(0) < size_of::<StatusBuffer>()
		{
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"storage lease table is truncated",
			));
		}
		if prefix.capabilities & REQUIRED_CAPABILITIES != REQUIRED_CAPABILITIES {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"storage lease is missing required capabilities",
			));
		}
		if let Some(provider_image_token) = PROVIDER_IMAGE_TOKEN.get() {
			if *provider_image_token != prefix.provider_image_token {
				return Err(io::Error::new(
					io::ErrorKind::Unsupported,
					"storage leases came from different rocksdb-js addon images",
				));
			}
		} else {
			let _ = PROVIDER_IMAGE_TOKEN.set(prefix.provider_image_token);
			if PROVIDER_IMAGE_TOKEN.get() != Some(&prefix.provider_image_token) {
				return Err(io::Error::new(
					io::ErrorKind::Unsupported,
					"storage leases came from different rocksdb-js addon images",
				));
			}
		}

		let table = NonNull::new(table.cast::<LeaseV1>()).unwrap();
		let lease = unsafe { table.as_ref() };
		if lease.reserved != 0 || (lease.rocksdb_major, lease.rocksdb_minor, lease.rocksdb_patch) != (11, 8, 1) {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"storage lease provider build is incompatible",
			));
		}
		let identity_length = usize::try_from(lease.provider_build_identity.length).map_err(|_| {
			io::Error::new(
				io::ErrorKind::InvalidData,
				"provider build identity exceeds this platform",
			)
		})?;
		if lease.provider_build_identity.length > MAX_BUILD_IDENTITY_BYTES
			|| (identity_length != 0 && lease.provider_build_identity.data.is_null())
		{
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"provider build identity is invalid",
			));
		}
		let provider_build_identity = String::from_utf8_lossy(if identity_length == 0 {
			&[]
		} else {
			unsafe { slice::from_raw_parts(lease.provider_build_identity.data, identity_length) }
		})
		.into_owned();
		let retain = lease
			.retain
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "storage lease retain function is missing"))?;
		let mut status = Status::new();
		let code = unsafe { retain(lease.context, &mut status.raw) };
		status.result(code)?;

		Ok(Self {
			inner: Arc::new(RocksLeaseInner {
				table,
				provider_build_identity,
			}),
		})
	}

	fn table(&self) -> &LeaseV1 {
		unsafe { self.inner.table.as_ref() }
	}

	pub fn database_incarnation(&self) -> u64 {
		self.table().database_incarnation
	}

	pub fn column_family_incarnation(&self) -> u64 {
		self.table().column_family_incarnation
	}

	pub fn provider_build_identity(&self) -> String {
		self.inner.provider_build_identity.clone()
	}

	pub fn poll_state(&self) -> u32 {
		self.table()
			.poll_state
			.map_or(STATE_REVOKED, |poll| unsafe { poll(self.table().context) })
	}

	pub fn get(&self, key: &[u8]) -> io::Result<Option<OwnedBytes>> {
		let get = self
			.table()
			.get_owned
			.ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "get capability is missing"))?;
		let mut result = OwnedBytesResult::new();
		let mut status = Status::new();
		let code = unsafe { get(self.table().context, ByteSpan::new(key), &mut result, &mut status.raw) };
		if code == STATUS_NOT_FOUND {
			return Ok(None);
		}
		status.result(code)?;
		Ok(Some(OwnedBytes::new(ProviderBytes::new(result)?)))
	}

	pub fn write(&self, mutations: &[Mutation], policy: WritePolicy) -> io::Result<()> {
		let write = self
			.table()
			.write_batch
			.ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "write capability is missing"))?;
		let raw_mutations: Vec<_> = mutations
			.iter()
			.map(|mutation| match mutation {
				Mutation::Put(key, value) => StorageMutation::new(MUTATION_PUT, key, value),
				Mutation::Delete(key) => StorageMutation::new(MUTATION_DELETE, key, &[]),
			})
			.collect();
		let policy = match policy {
			WritePolicy::WAL => POLICY_WAL,
			WritePolicy::WAL_SYNC => POLICY_WAL_SYNC,
			WritePolicy::NO_WAL => POLICY_NO_WAL,
			_ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid write policy")),
		};
		let mut status = Status::new();
		let code = unsafe {
			write(
				self.table().context,
				raw_mutations.as_ptr(),
				raw_mutations.len() as u64,
				policy,
				&mut status.raw,
			)
		};
		status.result(code)
	}

	pub fn scan_page(&self, prefix: &[u8], start_after: &[u8], entries: u64, bytes: u64) -> io::Result<ScanPage> {
		let scan = self
			.table()
			.scan_page
			.ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "scan capability is missing"))?;
		let mut result = OwnedBytesResult::new();
		let mut status = Status::new();
		let code = unsafe {
			scan(
				self.table().context,
				ByteSpan::new(prefix),
				ByteSpan::new(start_after),
				entries,
				bytes,
				&mut result,
				&mut status.raw,
			)
		};
		status.result(code)?;
		ScanPage::decode(OwnedBytes::new(ProviderBytes::new(result)?))
	}

	pub fn stats(&self) -> io::Result<StorageStats> {
		let collect = self
			.table()
			.collect_stats
			.ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "statistics capability is missing"))?;
		let mut result = StorageStats {
			struct_size: size_of::<StorageStats>() as u32,
			..StorageStats::default()
		};
		let mut status = Status::new();
		let code = unsafe { collect(self.table().context, &mut result, &mut status.raw) };
		status.result(code)?;
		Ok(result)
	}
}

impl KvStore for RocksLease {
	fn read(&self, key: &[u8]) -> io::Result<Option<OwnedBytes>> {
		self.get(key)
	}

	fn write(&self, mutations: &[Mutation], policy: WritePolicy) -> io::Result<()> {
		self.write(mutations, policy)
	}

	fn sync(&self) -> io::Result<()> {
		if self.poll_state() == STATE_ACTIVE {
			Ok(())
		} else {
			Err(io::Error::new(
				io::ErrorKind::NotConnected,
				"storage lease is not active",
			))
		}
	}
}

struct ProviderBytes {
	data: NonNull<u8>,
	length: usize,
	release_context: *mut c_void,
	release: unsafe extern "C" fn(*mut c_void),
}

unsafe impl Send for ProviderBytes {}
unsafe impl Sync for ProviderBytes {}
unsafe impl StableDeref for ProviderBytes {}

impl ProviderBytes {
	fn new(mut result: OwnedBytesResult) -> io::Result<Self> {
		let length = usize::try_from(result.length)
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "provider result exceeds this platform"))?;
		let data = if length == 0 {
			NonNull::dangling()
		} else {
			NonNull::new(result.data.cast_mut())
				.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "provider returned null bytes"))?
		};
		let release = result
			.release
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "provider omitted buffer release"))?;
		result.release = None;
		Ok(Self {
			data,
			length,
			release_context: result.release_context,
			release,
		})
	}
}

impl Deref for ProviderBytes {
	type Target = [u8];

	fn deref(&self) -> &Self::Target {
		unsafe { slice::from_raw_parts(self.data.as_ptr(), self.length) }
	}
}

impl Drop for ProviderBytes {
	fn drop(&mut self) {
		unsafe { (self.release)(self.release_context) }
	}
}

pub struct ScanPage {
	bytes: OwnedBytes,
	entries: Vec<(std::ops::Range<usize>, std::ops::Range<usize>)>,
}

impl ScanPage {
	fn decode(bytes: OwnedBytes) -> io::Result<Self> {
		let data = bytes.as_slice();
		let count = read_u32(data, 0, "scan page is truncated")? as usize;
		let mut cursor = 4usize;
		let mut entries = Vec::with_capacity(count);
		for _ in 0..count {
			let key_len = usize::try_from(read_u64(data, cursor, "scan entry header is truncated")?)
				.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "scan key exceeds this platform"))?;
			let value_len = usize::try_from(read_u64(data, cursor + 8, "scan entry header is truncated")?)
				.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "scan value exceeds this platform"))?;
			cursor += 16;
			let key_end = cursor
				.checked_add(key_len)
				.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "scan key overflow"))?;
			let value_end = key_end
				.checked_add(value_len)
				.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "scan value overflow"))?;
			if value_end > data.len() {
				return Err(io::Error::new(io::ErrorKind::InvalidData, "scan entry is truncated"));
			}
			entries.push((cursor..key_end, key_end..value_end));
			cursor = value_end;
		}
		if cursor != data.len() {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"scan page has trailing bytes",
			));
		}
		Ok(Self { bytes, entries })
	}

	pub fn entries(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
		self.entries
			.iter()
			.map(|(key, value)| (&self.bytes[key.clone()], &self.bytes[value.clone()]))
	}
}

fn read_u32(bytes: &[u8], offset: usize, error: &'static str) -> io::Result<u32> {
	let value = bytes
		.get(offset..offset.saturating_add(4))
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, error))?;
	let value: [u8; 4] = value
		.try_into()
		.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, error))?;
	Ok(u32::from_le_bytes(value))
}

fn read_u64(bytes: &[u8], offset: usize, error: &'static str) -> io::Result<u64> {
	let value = bytes
		.get(offset..offset.saturating_add(8))
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, error))?;
	let value: [u8; 8] = value
		.try_into()
		.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, error))?;
	Ok(u64::from_le_bytes(value))
}

impl ByteSpan {
	fn new(bytes: &[u8]) -> Self {
		Self {
			data: bytes.as_ptr(),
			length: bytes.len() as u64,
		}
	}
}

impl StorageMutation {
	fn new(kind: u32, key: &[u8], value: &[u8]) -> Self {
		Self {
			struct_size: size_of::<Self>() as u32,
			kind,
			key: ByteSpan::new(key),
			value: ByteSpan::new(value),
		}
	}
}

impl OwnedBytesResult {
	fn new() -> Self {
		Self {
			struct_size: size_of::<Self>() as u32,
			reserved: 0,
			data: ptr::null(),
			length: 0,
			release_context: ptr::null_mut(),
			release: None,
		}
	}
}

impl Drop for OwnedBytesResult {
	fn drop(&mut self) {
		if let Some(release) = self.release.take() {
			unsafe { release(self.release_context) }
		}
	}
}

struct Status {
	raw: StatusBuffer,
	message: Box<[c_char; 256]>,
}

impl Status {
	fn new() -> Self {
		let message = Box::new([0; 256]);
		let mut status = Self {
			raw: StatusBuffer {
				struct_size: size_of::<StatusBuffer>() as u32,
				reserved: 0,
				data: ptr::null_mut(),
				capacity: 256,
				length: 0,
			},
			message,
		};
		status.raw.data = status.message.as_mut_ptr();
		status
	}

	fn result(&self, code: u32) -> io::Result<()> {
		if code == STATUS_OK {
			return Ok(());
		}
		let length = usize::try_from(self.raw.length)
			.unwrap_or(0)
			.min(self.message.len().saturating_sub(1));
		let message =
			String::from_utf8_lossy(unsafe { slice::from_raw_parts(self.message.as_ptr().cast::<u8>(), length) });
		let kind = if code == STATUS_BUSY {
			io::ErrorKind::WouldBlock
		} else if code == STATUS_NOT_FOUND {
			io::ErrorKind::NotFound
		} else {
			io::ErrorKind::Other
		};
		Err(io::Error::new(
			kind,
			format!("storage provider status {code}: {message}"),
		))
	}
}

#[cfg(feature = "node-api")]
mod node {
	use super::*;
	use napi::{sys, Env, JsUnknown, NapiRaw, Status};

	#[repr(C)]
	struct NapiTypeTag {
		lower: u64,
		upper: u64,
	}

	unsafe extern "C" {
		fn napi_check_object_type_tag(
			env: sys::napi_env,
			value: sys::napi_value,
			type_tag: *const NapiTypeTag,
			result: *mut bool,
		) -> sys::napi_status;
	}

	pub fn from_external(env: Env, value: JsUnknown) -> napi::Result<RocksLease> {
		let raw_value = unsafe { value.raw() };
		let tag = NapiTypeTag {
			lower: TYPE_TAG_LOWER,
			upper: TYPE_TAG_UPPER,
		};
		let mut matches = false;
		let tag_status = unsafe { napi_check_object_type_tag(env.raw(), raw_value, &tag, &mut matches) };
		if tag_status != sys::Status::napi_ok || !matches {
			return Err(napi::Error::new(
				Status::InvalidArg,
				"invalid rocksdb-js storage lease type tag",
			));
		}
		let mut table = ptr::null_mut();
		let external_status = unsafe { sys::napi_get_value_external(env.raw(), raw_value, &mut table) };
		if external_status != sys::Status::napi_ok {
			return Err(napi::Error::new(
				Status::InvalidArg,
				"could not read rocksdb-js storage lease",
			));
		}
		unsafe { RocksLease::from_table(table) }
			.map_err(|error| napi::Error::new(Status::InvalidArg, error.to_string()))
	}
}

#[cfg(feature = "node-api")]
pub use node::from_external;

pub fn state_name(state: u32) -> &'static str {
	match state {
		STATE_ACTIVE => "active",
		STATE_CLOSING => "closing",
		STATE_REVOKED => "revoked",
		_ => "unknown",
	}
}
