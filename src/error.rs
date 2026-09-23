use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FulltextError {
	pub code: &'static str,
	pub message: String,
}

impl FulltextError {
	pub fn new(code: &'static str, message: impl Into<String>) -> Self {
		Self {
			code,
			message: message.into(),
		}
	}

	pub fn invalid(message: impl Into<String>) -> Self {
		Self::new("E_INVALID_ARGUMENT", message)
	}

	pub fn native(error: impl fmt::Display) -> Self {
		Self::new("E_NATIVE_FAILURE", error.to_string())
	}
}

impl fmt::Display for FulltextError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(formatter, "{}: {}", self.code, self.message)
	}
}

impl std::error::Error for FulltextError {}

pub type Result<T> = std::result::Result<T, FulltextError>;

pub const ERROR_CODES: &[&str] = &[
	"E_CLOSED",
	"E_CLOSE_FAILED",
	"E_DIRTY_CLOSE",
	"E_CHECKPOINT_REQUIRED",
	"E_DUPLICATE_OPEN",
	"E_IDENTITY_MISMATCH",
	"E_INCOMPLETE_CREATE",
	"E_INDEX_CORRUPT",
	"E_INDEX_FORMAT_INCOMPATIBLE",
	"E_BATCH_ACTIVE",
	"E_BATCH_INCOMPLETE",
	"E_BATCH_TOO_LARGE",
	"E_INVALID_ARGUMENT",
	"E_LOCK_BUSY",
	"E_NATIVE_ABI_MISMATCH",
	"E_NATIVE_ADDON_NOT_FOUND",
	"E_NATIVE_LOAD_FAILED",
	"E_NATIVE_CAPABILITY_MISMATCH",
	"E_NATIVE_FAILURE",
	"E_NATIVE_PANIC",
	"E_POISONED",
	"E_PREFIX_TOO_BROAD",
	"E_QUEUE_FULL",
	"E_QUIESCENCE_FAILED",
	"E_RESULT_TOO_LARGE",
	"E_SCHEMA_MISMATCH",
	"E_STORAGE",
	"E_TIMEOUT",
];
