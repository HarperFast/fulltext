use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};

use napi::Error;

pub type Result<T> = napi::Result<T, &'static str>;

#[derive(Default)]
#[cfg_attr(not(feature = "test-panic"), allow(dead_code))]
pub struct PoisonState {
	poisoned: AtomicBool,
}

impl PoisonState {
	#[cfg_attr(not(feature = "test-panic"), allow(dead_code))]
	pub fn run<T>(&self, operation: impl FnOnce() -> T) -> Result<T> {
		if self.poisoned.load(Ordering::Acquire) {
			return Err(coded_error("E_POISONED", "the native handle is in a terminal state"));
		}
		match catch_unwind(AssertUnwindSafe(operation)) {
			Ok(value) => {
				if self.poisoned.load(Ordering::Acquire) {
					Err(coded_error("E_POISONED", "the native handle is in a terminal state"))
				} else {
					Ok(value)
				}
			}
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
	use std::sync::{Arc, Barrier};
	use std::thread;

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

	#[test]
	fn operation_finishing_after_a_concurrent_panic_is_rejected() {
		let state = Arc::new(PoisonState::default());
		let entered = Arc::new(Barrier::new(2));
		let release = Arc::new(Barrier::new(2));
		let concurrent_state = state.clone();
		let concurrent_entered = entered.clone();
		let concurrent_release = release.clone();
		let operation = thread::spawn(move || {
			concurrent_state.run(|| {
				concurrent_entered.wait();
				concurrent_release.wait();
				1
			})
		});

		entered.wait();
		assert_eq!(state.run(|| panic!("boom")).unwrap_err().status, "E_NATIVE_PANIC");
		release.wait();
		assert_eq!(operation.join().unwrap().unwrap_err().status, "E_POISONED");
	}
}
