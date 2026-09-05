use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};

use napi::{Error, Result, Status};

static POISONED: AtomicBool = AtomicBool::new(false);

pub fn run<T>(operation: impl FnOnce() -> T) -> Result<T> {
	if POISONED.load(Ordering::Acquire) {
		return Err(coded_error("E_POISONED", "the native addon is in a terminal state"));
	}
	match catch_unwind(AssertUnwindSafe(operation)) {
		Ok(value) => Ok(value),
		Err(_) => {
			POISONED.store(true, Ordering::Release);
			Err(coded_error("E_NATIVE_PANIC", "native operation panicked"))
		}
	}
}

fn coded_error(code: &str, message: impl AsRef<str>) -> Error {
	Error::new(Status::GenericFailure, format!("[{code}] {}", message.as_ref()))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn panic_poisoning_is_terminal() {
		POISONED.store(false, Ordering::Release);
		let panic_error = run(|| panic!("boom")).unwrap_err();
		assert!(panic_error.reason.contains("[E_NATIVE_PANIC]"));

		let poisoned_error = run(|| 1).unwrap_err();
		assert!(poisoned_error.reason.contains("[E_POISONED]"));
		POISONED.store(false, Ordering::Release);
	}
}
