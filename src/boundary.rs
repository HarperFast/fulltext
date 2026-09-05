use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};

use napi::Error;

pub type Result<T> = napi::Result<T, &'static str>;

#[derive(Default)]
pub struct PoisonState {
	poisoned: AtomicBool,
}

impl PoisonState {
	pub fn run<T>(&self, operation: impl FnOnce() -> T) -> Result<T> {
		if self.poisoned.load(Ordering::Acquire) {
			return Err(coded_error("E_POISONED", "the native handle is in a terminal state"));
		}
		match catch_unwind(AssertUnwindSafe(operation)) {
			Ok(value) => Ok(value),
			Err(_) => {
				self.poisoned.store(true, Ordering::Release);
				Err(coded_error("E_NATIVE_PANIC", "native operation panicked"))
			}
		}
	}
}

pub fn run_stateless<T>(operation: impl FnOnce() -> T) -> Result<T> {
	match catch_unwind(AssertUnwindSafe(operation)) {
		Ok(value) => Ok(value),
		Err(_) => Err(coded_error("E_NATIVE_PANIC", "native operation panicked")),
	}
}

fn coded_error(code: &'static str, message: impl AsRef<str>) -> Error<&'static str> {
	Error::new(code, message.as_ref())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn panic_poisoning_is_scoped_to_the_handle() {
		let poisoned = PoisonState::default();
		let healthy = PoisonState::default();
		let panic_error = poisoned.run(|| panic!("boom")).unwrap_err();
		assert_eq!(panic_error.status, "E_NATIVE_PANIC");

		let poisoned_error = poisoned.run(|| 1).unwrap_err();
		assert_eq!(poisoned_error.status, "E_POISONED");
		assert_eq!(healthy.run(|| 1).unwrap(), 1);
		assert_eq!(run_stateless(|| 1).unwrap(), 1);
	}
}
