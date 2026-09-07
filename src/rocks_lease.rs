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
const ABI_MINOR: u32 = 0;
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

#[derive(Clone, Copy)]
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
type WriteBatchFn = unsafe extern "C" fn(*mut c_void, *const StorageMutation, u64, u64, u32, *mut StatusBuffer) -> u32;
type ScanPageFn =
	unsafe extern "C" fn(*mut c_void, ByteSpan, ByteSpan, u64, u64, *mut OwnedBytesResult, *mut StatusBuffer) -> u32;
type CollectStatsFn = unsafe extern "C" fn(*mut c_void, *mut StorageStats, *mut StatusBuffer) -> u32;

#[derive(Clone, Copy)]
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

#[derive(Clone, Copy)]
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
	table: LeaseV1,
	provider_build_identity: String,
}

unsafe impl Send for RocksLeaseInner {}
unsafe impl Sync for RocksLeaseInner {}

impl Drop for RocksLeaseInner {
	fn drop(&mut self) {
		let table = &self.table;
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
		let prefix = unsafe { ptr::read_unaligned(prefix.as_ptr()) };
		if prefix.magic != LEASE_MAGIC {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"storage lease magic does not match",
			));
		}
		if prefix.abi_major != ABI_MAJOR || prefix.abi_minor > ABI_MINOR {
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"storage lease ABI is incompatible",
			));
		}
		if usize::try_from(prefix.struct_size).unwrap_or(0) < size_of::<LeaseV1>()
			|| usize::try_from(prefix.status_size).unwrap_or(0) != size_of::<StatusBuffer>()
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
		if prefix.provider_image_token == 0 {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"storage lease provider image token is unset",
			));
		}
		let mut lease = unsafe { ptr::read_unaligned(table.cast::<LeaseV1>()) };
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
		let release = lease
			.release
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "storage lease release function is missing"))?;
		if lease.poll_state.is_none()
			|| lease.get_owned.is_none()
			|| lease.write_batch.is_none()
			|| lease.scan_page.is_none()
			|| lease.collect_stats.is_none()
		{
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"storage lease capability table is incomplete",
			));
		}
		if PROVIDER_IMAGE_TOKEN
			.get()
			.is_some_and(|provider_image_token| *provider_image_token != prefix.provider_image_token)
		{
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"storage leases came from different rocksdb-js addon images",
			));
		}
		let mut status = Status::new();
		let code = unsafe { retain(lease.context, &mut status.raw) };
		if let Err(error) = status.result(code) {
			if code == STATUS_OK {
				unsafe { release(lease.context) };
			}
			return Err(error);
		}
		let provider_image_token = PROVIDER_IMAGE_TOKEN.get_or_init(|| prefix.provider_image_token);
		if *provider_image_token != prefix.provider_image_token {
			unsafe { release(lease.context) };
			return Err(io::Error::new(
				io::ErrorKind::Unsupported,
				"storage leases came from different rocksdb-js addon images",
			));
		}
		lease.provider_build_identity = ByteSpan {
			data: ptr::null(),
			length: 0,
		};

		Ok(Self {
			inner: Arc::new(RocksLeaseInner {
				table: lease,
				provider_build_identity,
			}),
		})
	}

	fn table(&self) -> &LeaseV1 {
		&self.inner.table
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
			status.validate()?;
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
				size_of::<StorageMutation>() as u64,
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
		ScanPage::decode(OwnedBytes::new(ProviderBytes::new(result)?), entries, bytes)
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
	fn identity(&self) -> crate::phase0::KvStoreIdentity {
		crate::phase0::KvStoreIdentity(
			self.table().provider_image_token,
			self.database_incarnation(),
			self.column_family_incarnation(),
		)
	}

	fn read(&self, key: &[u8]) -> io::Result<Option<OwnedBytes>> {
		self.get(key)
	}

	fn write(&self, mutations: &[Mutation], policy: WritePolicy) -> io::Result<()> {
		RocksLease::write(self, mutations, policy)
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
	fn decode(bytes: OwnedBytes, entry_limit: u64, byte_limit: u64) -> io::Result<Self> {
		if bytes.len() as u64 > byte_limit {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"scan page exceeds byte limit",
			));
		}
		let data = bytes.as_slice();
		let count = read_u32(data, 0, "scan page is truncated")? as usize;
		if count as u64 > entry_limit || count > data.len().saturating_sub(4) / 16 {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"scan page count exceeds its bounds",
			));
		}
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

	fn validate(&self) -> io::Result<()> {
		if self.raw.struct_size as usize != size_of::<StatusBuffer>()
			|| self.raw.reserved != 0
			|| self.raw.data != self.message.as_ptr().cast_mut()
			|| self.raw.capacity as usize != self.message.len()
			|| self.raw.length as usize >= self.message.len()
		{
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"storage provider returned an invalid status buffer",
			));
		}
		Ok(())
	}

	fn result(&self, code: u32) -> io::Result<()> {
		self.validate()?;
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

	#[cfg(windows)]
	type CheckObjectTypeTagFn =
		unsafe extern "C" fn(sys::napi_env, sys::napi_value, *const NapiTypeTag, *mut bool) -> sys::napi_status;

	#[cfg(not(windows))]
	unsafe extern "C" {
		fn napi_check_object_type_tag(
			env: sys::napi_env,
			value: sys::napi_value,
			type_tag: *const NapiTypeTag,
			result: *mut bool,
		) -> sys::napi_status;
	}

	#[cfg(not(windows))]
	unsafe fn check_object_type_tag(
		env: sys::napi_env,
		value: sys::napi_value,
		type_tag: *const NapiTypeTag,
		result: *mut bool,
	) -> napi::Result<sys::napi_status> {
		Ok(unsafe { napi_check_object_type_tag(env, value, type_tag, result) })
	}

	#[cfg(windows)]
	unsafe fn check_object_type_tag(
		env: sys::napi_env,
		value: sys::napi_value,
		type_tag: *const NapiTypeTag,
		result: *mut bool,
	) -> napi::Result<sys::napi_status> {
		static CHECK_OBJECT_TYPE_TAG: OnceLock<Result<CheckObjectTypeTagFn, String>> = OnceLock::new();
		let function = CHECK_OBJECT_TYPE_TAG
			.get_or_init(|| {
				let host = libloading::os::windows::Library::this().map_err(|error| error.to_string())?;
				unsafe { host.get::<CheckObjectTypeTagFn>(b"napi_check_object_type_tag\0") }
					.map(|symbol| *symbol)
					.map_err(|error| error.to_string())
			})
			.as_ref()
			.map_err(|error| napi::Error::new(Status::GenericFailure, error.clone()))?;
		Ok(unsafe { function(env, value, type_tag, result) })
	}

	pub fn from_external(env: Env, value: JsUnknown) -> napi::Result<RocksLease> {
		let raw_value = unsafe { value.raw() };
		let tag = NapiTypeTag {
			lower: TYPE_TAG_LOWER,
			upper: TYPE_TAG_UPPER,
		};
		let mut matches = false;
		let tag_status = unsafe { check_object_type_tag(env.raw(), raw_value, &tag, &mut matches) }?;
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

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::atomic::{AtomicUsize, Ordering};

	static BUILD_IDENTITY: &[u8] = b"test-provider";

	unsafe extern "C" fn retain(_: *mut c_void, _: *mut StatusBuffer) -> u32 {
		STATUS_OK
	}

	unsafe extern "C" fn release(context: *mut c_void) {
		if !context.is_null() {
			unsafe { &*(context.cast::<AtomicUsize>()) }.fetch_add(1, Ordering::Relaxed);
		}
	}

	unsafe extern "C" fn poll(_: *mut c_void) -> u32 {
		STATE_ACTIVE
	}

	unsafe extern "C" fn get(_: *mut c_void, _: ByteSpan, _: *mut OwnedBytesResult, _: *mut StatusBuffer) -> u32 {
		STATUS_NOT_FOUND
	}

	unsafe extern "C" fn get_with_invalid_status(
		_: *mut c_void,
		_: ByteSpan,
		_: *mut OwnedBytesResult,
		status: *mut StatusBuffer,
	) -> u32 {
		unsafe { (*status).data = NonNull::<c_char>::dangling().as_ptr() };
		STATUS_NOT_FOUND
	}

	unsafe extern "C" fn write(
		_: *mut c_void,
		_: *const StorageMutation,
		_: u64,
		_: u64,
		_: u32,
		_: *mut StatusBuffer,
	) -> u32 {
		STATUS_OK
	}

	unsafe extern "C" fn scan(
		_: *mut c_void,
		_: ByteSpan,
		_: ByteSpan,
		_: u64,
		_: u64,
		_: *mut OwnedBytesResult,
		_: *mut StatusBuffer,
	) -> u32 {
		STATUS_OK
	}

	unsafe extern "C" fn stats(_: *mut c_void, _: *mut StorageStats, _: *mut StatusBuffer) -> u32 {
		STATUS_OK
	}

	fn table() -> LeaseV1 {
		LeaseV1 {
			magic: LEASE_MAGIC,
			abi_major: ABI_MAJOR,
			abi_minor: ABI_MINOR,
			struct_size: size_of::<LeaseV1>() as u32,
			status_size: size_of::<StatusBuffer>() as u32,
			capabilities: REQUIRED_CAPABILITIES,
			provider_image_token: 1,
			database_incarnation: 2,
			column_family_incarnation: 3,
			rocksdb_major: 11,
			rocksdb_minor: 8,
			rocksdb_patch: 1,
			reserved: 0,
			provider_build_identity: ByteSpan::new(BUILD_IDENTITY),
			context: ptr::null_mut(),
			retain: Some(retain),
			release: Some(release),
			poll_state: Some(poll),
			get_owned: Some(get),
			write_batch: Some(write),
			scan_page: Some(scan),
			collect_stats: Some(stats),
		}
	}

	#[test]
	fn lease_copies_the_table_before_the_external_can_be_released() {
		let releases = AtomicUsize::new(0);
		let mut table = Box::new(table());
		table.context = (&releases as *const AtomicUsize).cast_mut().cast();
		let pointer = Box::into_raw(table);
		let lease = unsafe { RocksLease::from_table(pointer.cast()) }.unwrap();
		unsafe { drop(Box::from_raw(pointer)) };
		assert_eq!(lease.poll_state(), STATE_ACTIVE);
		drop(lease);
		assert_eq!(releases.load(Ordering::Relaxed), 1);
	}

	#[test]
	fn not_found_still_validates_the_status_buffer() {
		let mut table = table();
		table.get_owned = Some(get_with_invalid_status);
		let lease = unsafe { RocksLease::from_table((&mut table as *mut LeaseV1).cast()) }.unwrap();
		assert_eq!(lease.get(b"missing").unwrap_err().kind(), io::ErrorKind::InvalidData);
	}

	#[test]
	fn lease_rejects_a_larger_provider_status_layout() {
		let mut table = table();
		table.status_size += 8;
		assert!(unsafe { RocksLease::from_table((&mut table as *mut LeaseV1).cast()) }.is_err());
	}

	#[test]
	fn scan_page_rejects_count_before_allocating_entries() {
		let bytes = OwnedBytes::new(u32::MAX.to_le_bytes().to_vec());
		assert!(ScanPage::decode(bytes, 4_096, 64 * 1024 * 1024).is_err());
	}

	#[test]
	fn scan_page_enforces_the_requested_caps() {
		let bytes = OwnedBytes::new(0u32.to_le_bytes().to_vec());
		assert!(ScanPage::decode(bytes.clone(), 4_096, 3).is_err());
		let mut one = 1u32.to_le_bytes().to_vec();
		one.extend_from_slice(&0u64.to_le_bytes());
		one.extend_from_slice(&0u64.to_le_bytes());
		assert!(ScanPage::decode(OwnedBytes::new(one), 0, 64).is_err());
	}
}
