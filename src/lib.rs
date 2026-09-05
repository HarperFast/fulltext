#![deny(clippy::all)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod directory_harness;

#[cfg(feature = "node-api")]
mod boundary;

#[cfg(feature = "node-api")]
use napi_derive::napi;

#[cfg(feature = "test-panic")]
use std::collections::HashMap;
#[cfg(feature = "test-panic")]
use std::sync::atomic::{AtomicU32, Ordering};
#[cfg(feature = "test-panic")]
use std::sync::{Arc, Mutex, OnceLock};

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
