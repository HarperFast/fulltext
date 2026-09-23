#![deny(clippy::all)]
#![deny(unsafe_op_in_unsafe_fn)]

#[cfg(test)]
mod directory_harness;
pub mod engine;
pub mod error;
pub mod protocol;

#[cfg(feature = "node-api")]
mod boundary;

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

pub const NATIVE_ABI_VERSION: u32 = 6;
pub const TANTIVY_VERSION: &str = "0.26.1";

#[cfg(feature = "node-api")]
#[napi(object)]
pub struct RuntimeLimits {
	pub max_commit_payload_bytes: u32,
	pub max_query_text_bytes: u32,
	pub max_query_terms: u32,
	pub max_query_clauses: u32,
	pub max_candidate_ids: u32,
	pub max_candidate_bytes: u32,
	pub max_record_id_bytes: u32,
	pub max_prefix_expansions: u32,
	pub max_fuzzy_terms: u32,
	pub max_search_window: u32,
	pub max_autocomplete_results: u32,
	pub max_search_request_bytes: u32,
	pub max_search_response_bytes: u32,
	pub max_search_budget_milliseconds: u32,
	pub max_trace_records: u32,
	pub max_trace_source_bytes: u32,
	pub max_trace_spans: u32,
}

#[cfg(feature = "node-api")]
#[napi(object)]
pub struct RuntimeInfo {
	pub package_version: String,
	pub tantivy_version: String,
	pub native_abi_version: u32,
	pub query_api_version: u32,
	pub query_class_isolation_minimum_search_threads: u32,
	pub storage_backends: Vec<String>,
	pub limits: RuntimeLimits,
}

#[cfg(feature = "node-api")]
#[napi(catch_unwind, js_name = "runtimeInfo")]
pub fn runtime_info() -> boundary::Result<RuntimeInfo> {
	boundary::run_stateless(|| RuntimeInfo {
		package_version: env!("CARGO_PKG_VERSION").to_owned(),
		tantivy_version: TANTIVY_VERSION.to_owned(),
		native_abi_version: NATIVE_ABI_VERSION,
		query_api_version: 1,
		query_class_isolation_minimum_search_threads: 2,
		storage_backends: vec!["native".to_owned()],
		limits: RuntimeLimits {
			max_commit_payload_bytes: engine::MAX_COMMIT_PAYLOAD_BYTES as u32,
			max_query_text_bytes: protocol::MAX_QUERY_TEXT_BYTES as u32,
			max_query_terms: protocol::MAX_QUERY_TERMS as u32,
			max_query_clauses: protocol::MAX_QUERY_CLAUSES as u32,
			max_candidate_ids: protocol::MAX_CANDIDATE_IDS as u32,
			max_candidate_bytes: protocol::MAX_CANDIDATE_BYTES as u32,
			max_record_id_bytes: protocol::MAX_RECORD_ID_BYTES as u32,
			max_prefix_expansions: protocol::MAX_PREFIX_EXPANSIONS as u32,
			max_fuzzy_terms: protocol::MAX_FUZZY_TERMS as u32,
			max_search_window: protocol::MAX_SEARCH_WINDOW as u32,
			max_autocomplete_results: protocol::MAX_AUTOCOMPLETE_RESULTS as u32,
			max_search_request_bytes: protocol::MAX_SEARCH_REQUEST_BYTES as u32,
			max_search_response_bytes: protocol::MAX_SEARCH_RESPONSE_BYTES as u32,
			max_search_budget_milliseconds: protocol::MAX_SEARCH_BUDGET_MILLISECONDS,
			max_trace_records: protocol::MAX_TRACE_RECORDS as u32,
			max_trace_source_bytes: protocol::MAX_TRACE_SOURCE_BYTES as u32,
			max_trace_spans: protocol::MAX_TRACE_SPANS as u32,
		},
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
