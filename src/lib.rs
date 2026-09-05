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
#[napi]
#[derive(Default)]
pub struct TestHandle {
	poison: boundary::PoisonState,
}

#[cfg(feature = "test-panic")]
#[napi]
impl TestHandle {
	#[napi(catch_unwind, constructor)]
	pub fn new() -> Self {
		Self::default()
	}

	#[napi(catch_unwind)]
	pub fn panic(&self) -> boundary::Result<()> {
		self.poison.run(|| panic!("test panic"))
	}

	#[napi(catch_unwind)]
	pub fn check(&self) -> boundary::Result<bool> {
		self.poison.run(|| true)
	}
}
