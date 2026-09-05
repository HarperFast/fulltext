#![deny(clippy::all)]
#![deny(unsafe_op_in_unsafe_fn)]

pub mod directory_harness;

#[cfg(feature = "node-api")]
mod boundary;

#[cfg(feature = "node-api")]
use napi_derive::napi;

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
#[napi(js_name = "runtimeInfo")]
pub fn runtime_info() -> napi::Result<RuntimeInfo> {
	boundary::run(|| RuntimeInfo {
		package_version: env!("CARGO_PKG_VERSION").to_owned(),
		tantivy_version: TANTIVY_VERSION.to_owned(),
		native_abi_version: NATIVE_ABI_VERSION,
		storage_backends: vec!["native".to_owned()],
	})
}

#[cfg(feature = "test-panic")]
#[napi(js_name = "__testPanic")]
pub fn test_panic() -> napi::Result<()> {
	boundary::run(|| panic!("test panic"))
}
