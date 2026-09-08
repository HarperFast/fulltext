#![deny(clippy::all)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod directory_harness;
pub mod engine;
pub mod error;
pub mod protocol;

#[cfg(any(test, feature = "phase0"))]
pub mod phase0;

#[cfg(feature = "phase0")]
pub mod rocks_lease;

#[cfg(feature = "node-api")]
mod boundary;

#[cfg(feature = "test-panic")]
pub mod host_storage;

#[cfg(feature = "node-api")]
pub mod native;

#[cfg(feature = "node-api")]
use napi_derive::napi;

#[cfg(feature = "test-panic")]
use std::collections::HashMap;
#[cfg(feature = "test-panic")]
use std::sync::atomic::{AtomicU32, Ordering};
#[cfg(feature = "test-panic")]
use std::sync::{Arc, Mutex, OnceLock};

#[cfg(feature = "test-panic")]
use napi::{bindgen_prelude::Buffer, Env, JsUnknown};

pub const NATIVE_ABI_VERSION: u32 = 1;
pub const TANTIVY_VERSION: &str = "0.26.1";

#[cfg(feature = "node-api")]
#[napi(object)]
pub struct RuntimeInfo {
	pub package_version: String,
	pub tantivy_version: String,
	pub native_abi_version: u32,
	pub storage_backends: Vec<String>,
}

#[cfg(feature = "node-api")]
#[napi(catch_unwind, js_name = "runtimeInfo")]
pub fn runtime_info() -> boundary::Result<RuntimeInfo> {
	boundary::run_stateless(|| RuntimeInfo {
		package_version: env!("CARGO_PKG_VERSION").to_owned(),
		tantivy_version: TANTIVY_VERSION.to_owned(),
		native_abi_version: NATIVE_ABI_VERSION,
		storage_backends: vec!["native".to_owned()],
	})
}

#[cfg(feature = "test-panic")]
static NEXT_TEST_HANDLE: AtomicU32 = AtomicU32::new(1);
#[cfg(feature = "test-panic")]
static TEST_HANDLES: OnceLock<Mutex<HashMap<u32, Arc<boundary::PoisonState>>>> = OnceLock::new();
#[cfg(feature = "test-panic")]
static PHASE0_LEASES: OnceLock<Mutex<HashMap<u32, rocks_lease::RocksLease>>> = OnceLock::new();

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testCreateHandle")]
pub fn test_create_handle() -> boundary::Result<u32> {
	boundary::run_stateless(|| {
		let id = NEXT_TEST_HANDLE.fetch_add(1, Ordering::Relaxed);
		TEST_HANDLES
			.get_or_init(Default::default)
			.lock()
			.unwrap()
			.insert(id, Arc::new(boundary::PoisonState::default()));
		id
	})
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testPanic")]
pub fn test_panic(id: u32) -> boundary::Result<()> {
	test_handle(id)?.run(|| panic!("test panic"))
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__testCheck")]
pub fn test_check(id: u32) -> boundary::Result<bool> {
	test_handle(id)?.run(|| true)
}

#[cfg(feature = "test-panic")]
fn test_handle(id: u32) -> boundary::Result<Arc<boundary::PoisonState>> {
	let handle = boundary::run_stateless(|| {
		TEST_HANDLES
			.get_or_init(Default::default)
			.lock()
			.unwrap()
			.get(&id)
			.cloned()
	})?;
	handle.ok_or_else(|| napi::Error::new("E_NATIVE_FAILURE", "unknown test handle"))
}

#[cfg(feature = "test-panic")]
fn phase0_lease(id: u32) -> boundary::Result<rocks_lease::RocksLease> {
	PHASE0_LEASES
		.get_or_init(Default::default)
		.lock()
		.unwrap()
		.get(&id)
		.cloned()
		.ok_or_else(|| napi::Error::new("E_NATIVE_FAILURE", "unknown Phase 0 storage lease"))
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__phase0OpenStorageLease")]
pub fn phase0_open_storage_lease(env: Env, external: JsUnknown) -> boundary::Result<u32> {
	boundary::run_stateless(|| {
		let lease =
			rocks_lease::from_external(env, external).map_err(|error| napi::Error::new("E_STORAGE", error.reason))?;
		let id = NEXT_TEST_HANDLE.fetch_add(1, Ordering::Relaxed);
		PHASE0_LEASES
			.get_or_init(Default::default)
			.lock()
			.unwrap()
			.insert(id, lease);
		Ok(id)
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__phase0CloseStorageLease")]
pub fn phase0_close_storage_lease(id: u32) -> boundary::Result<bool> {
	boundary::run_stateless(|| {
		Ok(PHASE0_LEASES
			.get_or_init(Default::default)
			.lock()
			.unwrap()
			.remove(&id)
			.is_some())
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__phase0StorageLeaseInfo")]
pub fn phase0_storage_lease_info(id: u32) -> boundary::Result<Vec<String>> {
	boundary::run_stateless(|| {
		let lease = phase0_lease(id)?;
		Ok(vec![
			lease.database_incarnation().to_string(),
			lease.column_family_incarnation().to_string(),
			lease.provider_build_identity(),
			rocks_lease::state_name(lease.poll_state()).to_owned(),
		])
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__phase0StoragePut")]
pub fn phase0_storage_put(id: u32, key: Buffer, value: Buffer, sync: bool) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		phase0_lease(id)?
			.write(
				&[phase0::Mutation::Put(key.to_vec(), value.to_vec())],
				if sync {
					phase0::WritePolicy::WAL_SYNC
				} else {
					phase0::WritePolicy::WAL
				},
			)
			.map_err(|error| napi::Error::new("E_STORAGE", error.to_string()))
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__phase0StorageDelete")]
pub fn phase0_storage_delete(id: u32, key: Buffer, sync: bool) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		phase0_lease(id)?
			.write(
				&[phase0::Mutation::Delete(key.to_vec())],
				if sync {
					phase0::WritePolicy::WAL_SYNC
				} else {
					phase0::WritePolicy::WAL
				},
			)
			.map_err(|error| napi::Error::new("E_STORAGE", error.to_string()))
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__phase0StorageGet")]
pub fn phase0_storage_get(id: u32, key: Buffer) -> boundary::Result<Option<Buffer>> {
	boundary::run_stateless(|| {
		phase0_lease(id)?
			.get(&key)
			.map(|value| value.map(|bytes| Buffer::from(bytes.as_slice().to_vec())))
			.map_err(|error| napi::Error::new("E_STORAGE", error.to_string()))
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__phase0StorageScan")]
pub fn phase0_storage_scan(id: u32, prefix: Buffer, start_after: Buffer) -> boundary::Result<Vec<Buffer>> {
	boundary::run_stateless(|| {
		let page = phase0_lease(id)?
			.scan_page(&prefix, &start_after, 4_096, 64 * 1024 * 1024)
			.map_err(|error| napi::Error::new("E_STORAGE", error.to_string()))?;
		Ok(page
			.entries()
			.flat_map(|(key, value)| [Buffer::from(key.to_vec()), Buffer::from(value.to_vec())])
			.collect())
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__phase0StorageLeaseState")]
pub fn phase0_storage_lease_state(id: u32) -> boundary::Result<String> {
	boundary::run_stateless(|| Ok(rocks_lease::state_name(phase0_lease(id)?.poll_state()).to_owned()))?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__phase0StorageStats")]
pub fn phase0_storage_stats(id: u32) -> boundary::Result<Vec<String>> {
	boundary::run_stateless(|| {
		let stats = phase0_lease(id)?
			.stats()
			.map_err(|error| napi::Error::new("E_STORAGE", error.to_string()))?;
		Ok([
			stats.get_operations,
			stats.scan_operations,
			stats.batch_operations,
			stats.requested_bytes,
			stats.returned_bytes,
			stats.copied_bytes,
			stats.live_owned_buffers,
			stats.provider_errors,
		]
		.into_iter()
		.map(|value| value.to_string())
		.collect())
	})?
}

#[cfg(feature = "test-panic")]
#[napi(catch_unwind, skip_typescript, js_name = "__phase0VerifyTantivyOnStorageLease")]
pub fn phase0_verify_tantivy_on_storage_lease(id: u32) -> boundary::Result<()> {
	boundary::run_stateless(|| {
		let lease = phase0_lease(id)?;
		let run = NEXT_TEST_HANDLE.fetch_add(1, Ordering::Relaxed);
		let case = AtomicU32::new(0);
		directory_harness::verify_directory_contract(|| {
			let namespace = format!("phase0-contract/{run}/{}", case.fetch_add(1, Ordering::Relaxed));
			phase0::KvDirectory::with_namespace(lease.clone(), namespace.as_bytes())
		})
		.map_err(|error| napi::Error::new("E_STORAGE", error))?;
		let namespace = format!("phase0-lifecycle/{run}");
		directory_harness::verify_tantivy_lifecycle(phase0::KvDirectory::with_namespace(lease, namespace.as_bytes()))
			.map_err(|error| napi::Error::new("E_STORAGE", error))
	})?
}
