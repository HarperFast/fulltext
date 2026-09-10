use std::env;
use std::fmt::Write as FmtWrite;
use std::hint::black_box;
use std::io;
use std::io::Write as IoWrite;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use harper_fulltext::phase0::{FaultingDirectory, FaultingKv};
use harper_fulltext::TANTIVY_VERSION;
use tantivy::directory::{Directory, TerminatingWrite, WritePtr};

const WRITE_BYTES_PER_SAMPLE: usize = 128 * 1024;
const CHUNK_BYTES: usize = 256 * 1024;
const EMPTY_FLUSHES_PER_SAMPLE: usize = 10_000;
const CONCURRENT_BYTES_PER_FILE: usize = 4 * 1024;
const CONCURRENT_FILES_PER_THREAD: usize = 64;

struct Arguments {
	samples: usize,
	warmup_samples: usize,
	smoke: bool,
	revision: String,
}

struct Sample {
	elapsed_nanoseconds: u128,
	operations: u64,
	bytes: u64,
}

struct CaseResult {
	name: String,
	operation: &'static str,
	threads: usize,
	operations: u64,
	bytes: u64,
	total_nanoseconds: u128,
	sample_nanoseconds_per_operation: Vec<f64>,
}

#[derive(Clone, Copy)]
enum StartCommand {
	Waiting,
	Run(Instant),
	Cancel,
}

struct StartGate {
	state: Mutex<(usize, StartCommand)>,
	changed: Condvar,
}

impl StartGate {
	fn new() -> Self {
		Self {
			state: Mutex::new((0, StartCommand::Waiting)),
			changed: Condvar::new(),
		}
	}

	fn wait(&self) -> Option<Instant> {
		let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		state.0 += 1;
		self.changed.notify_all();
		loop {
			match state.1 {
				StartCommand::Waiting => {
					state = self
						.changed
						.wait(state)
						.unwrap_or_else(|poisoned| poisoned.into_inner());
				}
				StartCommand::Run(started) => return Some(started),
				StartCommand::Cancel => return None,
			}
		}
	}

	fn start(&self, workers: usize) {
		let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		while state.0 != workers {
			state = self
				.changed
				.wait(state)
				.unwrap_or_else(|poisoned| poisoned.into_inner());
		}
		let started = Instant::now();
		state.1 = StartCommand::Run(started);
		self.changed.notify_all();
	}

	fn cancel(&self) {
		let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		state.1 = StartCommand::Cancel;
		self.changed.notify_all();
	}
}

fn main() -> io::Result<()> {
	let arguments = parse_arguments()?;
	let mut results = Vec::new();

	for call_bytes in [16, 256, 4 * 1024, 64 * 1024] {
		results.push(measure_case(
			format!("buffered-write-{call_bytes}b"),
			"caller-write",
			1,
			&arguments,
			writer_case(call_bytes),
		)?);
	}
	results.push(measure_case(
		"empty-flush".to_owned(),
		"flush",
		1,
		&arguments,
		empty_flush_case()?,
	)?);
	results.push(measure_case(
		"dirty-flush-4k".to_owned(),
		"flush",
		1,
		&arguments,
		dirty_flush_case(),
	)?);
	results.push(measure_case(
		"chunk-publication-256k".to_owned(),
		"file",
		1,
		&arguments,
		chunk_case(),
	)?);
	results.push(measure_case(
		"delete-closed-writer".to_owned(),
		"delete",
		1,
		&arguments,
		delete_case(false),
	)?);
	results.push(measure_case(
		"delete-active-writer".to_owned(),
		"delete",
		1,
		&arguments,
		delete_case(true),
	)?);

	for threads in [1, 2, 4, 8] {
		results.push(measure_case(
			format!("concurrent-files-{threads}t"),
			"file",
			threads,
			&arguments,
			concurrent_case(threads, arguments.smoke),
		)?);
	}

	assert!(results.iter().all(|result| result.operations != 0));
	println!("{}", encode_results(&arguments, &results));
	Ok(())
}

fn parse_arguments() -> io::Result<Arguments> {
	let mut smoke = false;
	let mut samples = None;
	let mut warmup_samples = None;
	let mut revision = "unknown".to_owned();
	let mut arguments = env::args().skip(1);
	while let Some(argument) = arguments.next() {
		match argument.as_str() {
			"--bench" => {}
			"--smoke" => smoke = true,
			"--samples" => samples = Some(positive_argument("--samples", arguments.next())?),
			"--warmup" => warmup_samples = Some(nonnegative_argument("--warmup", arguments.next())?),
			"--revision" => {
				revision = arguments
					.next()
					.filter(|value| !value.is_empty())
					.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "--revision requires a value"))?;
			}
			_ => {
				return Err(io::Error::new(
					io::ErrorKind::InvalidInput,
					format!("unknown benchmark argument: {argument}"),
				));
			}
		}
	}
	Ok(Arguments {
		samples: samples.unwrap_or(if smoke { 2 } else { 20 }),
		warmup_samples: warmup_samples.unwrap_or(if smoke { 1 } else { 3 }),
		smoke,
		revision,
	})
}

fn positive_argument(name: &str, value: Option<String>) -> io::Result<usize> {
	let value = nonnegative_argument(name, value)?;
	if value == 0 {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("{name} must be greater than zero"),
		));
	}
	Ok(value)
}

fn nonnegative_argument(name: &str, value: Option<String>) -> io::Result<usize> {
	value
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("{name} requires a value")))?
		.parse()
		.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, format!("{name} must be an integer")))
}

fn measure_case<F>(
	name: String,
	operation: &'static str,
	threads: usize,
	arguments: &Arguments,
	mut run: F,
) -> io::Result<CaseResult>
where
	F: FnMut(usize) -> io::Result<Sample>,
{
	for sample in 0..arguments.warmup_samples {
		black_box(run(sample)?);
	}
	let mut operations = 0u64;
	let mut bytes = 0u64;
	let mut total_nanoseconds = 0u128;
	let mut sample_nanoseconds_per_operation = Vec::with_capacity(arguments.samples);
	for sample in 0..arguments.samples {
		let measured = run(arguments.warmup_samples + sample)?;
		if measured.elapsed_nanoseconds == 0 {
			return Err(io::Error::other("benchmark clock did not advance"));
		}
		operations = operations
			.checked_add(measured.operations)
			.ok_or_else(|| io::Error::other("benchmark operation count overflow"))?;
		bytes = bytes
			.checked_add(measured.bytes)
			.ok_or_else(|| io::Error::other("benchmark byte count overflow"))?;
		total_nanoseconds = total_nanoseconds
			.checked_add(measured.elapsed_nanoseconds)
			.ok_or_else(|| io::Error::other("benchmark duration overflow"))?;
		sample_nanoseconds_per_operation.push(measured.elapsed_nanoseconds as f64 / measured.operations as f64);
	}
	Ok(CaseResult {
		name,
		operation,
		threads,
		operations,
		bytes,
		total_nanoseconds,
		sample_nanoseconds_per_operation,
	})
}

fn writer_case(call_bytes: usize) -> impl FnMut(usize) -> io::Result<Sample> {
	let payload = vec![7u8; call_bytes];
	move |sample| {
		let directory = FaultingDirectory::new(FaultingKv::default());
		let path = format!("writer-{call_bytes}-{sample}");
		let mut writer = open_writer(&directory, Path::new(&path))?;
		let operations = WRITE_BYTES_PER_SAMPLE / call_bytes;
		let started = Instant::now();
		for _ in 0..operations {
			writer.write_all(black_box(&payload))?;
		}
		let elapsed_nanoseconds = started.elapsed().as_nanos();
		black_box(&mut writer);
		writer.terminate()?;
		Ok(Sample {
			elapsed_nanoseconds,
			operations: operations as u64,
			bytes: WRITE_BYTES_PER_SAMPLE as u64,
		})
	}
}

fn empty_flush_case() -> io::Result<impl FnMut(usize) -> io::Result<Sample>> {
	let directory = FaultingDirectory::new(FaultingKv::default());
	let mut writer = open_writer(&directory, Path::new("empty-flush"))?;
	Ok(move |_| {
		let started = Instant::now();
		for _ in 0..EMPTY_FLUSHES_PER_SAMPLE {
			writer.flush()?;
		}
		Ok(Sample {
			elapsed_nanoseconds: started.elapsed().as_nanos(),
			operations: EMPTY_FLUSHES_PER_SAMPLE as u64,
			bytes: 0,
		})
	})
}

fn dirty_flush_case() -> impl FnMut(usize) -> io::Result<Sample> {
	let payload = vec![11u8; 4 * 1024];
	move |sample| {
		let directory = FaultingDirectory::new(FaultingKv::default());
		let path = format!("dirty-flush-{sample}");
		let mut writer = open_writer(&directory, Path::new(&path))?;
		writer.write_all(&payload)?;
		let started = Instant::now();
		writer.flush()?;
		Ok(Sample {
			elapsed_nanoseconds: started.elapsed().as_nanos(),
			operations: 1,
			bytes: payload.len() as u64,
		})
	}
}

fn chunk_case() -> impl FnMut(usize) -> io::Result<Sample> {
	let payload = vec![13u8; CHUNK_BYTES];
	move |sample| {
		let directory = FaultingDirectory::new(FaultingKv::default());
		let path = format!("chunk-{sample}");
		let mut writer = open_writer(&directory, Path::new(&path))?;
		let started = Instant::now();
		writer.write_all(&payload)?;
		writer.terminate()?;
		Ok(Sample {
			elapsed_nanoseconds: started.elapsed().as_nanos(),
			operations: 1,
			bytes: payload.len() as u64,
		})
	}
}

fn delete_case(active_writer: bool) -> impl FnMut(usize) -> io::Result<Sample> {
	let payload = vec![17u8; 4 * 1024];
	move |sample| {
		let directory = FaultingDirectory::new(FaultingKv::default());
		let path = format!("delete-{active_writer}-{sample}");
		let path = Path::new(&path);
		let mut writer = open_writer(&directory, path)?;
		writer.write_all(&payload)?;
		let writer = if active_writer {
			writer.flush()?;
			Some(writer)
		} else {
			writer.terminate()?;
			None
		};
		let started = Instant::now();
		delete_path(&directory, path)?;
		let elapsed_nanoseconds = started.elapsed().as_nanos();
		drop(writer);
		Ok(Sample {
			elapsed_nanoseconds,
			operations: 1,
			bytes: 0,
		})
	}
}

fn concurrent_case(threads: usize, smoke: bool) -> impl FnMut(usize) -> io::Result<Sample> {
	let files_per_thread = if smoke { 1 } else { CONCURRENT_FILES_PER_THREAD };
	move |sample| {
		let directory = FaultingDirectory::new(FaultingKv::default());
		let start_gate = Arc::new(StartGate::new());
		let remaining = Arc::new(AtomicUsize::new(threads));
		let (completion, completed) = mpsc::sync_channel(1);
		let elapsed_nanoseconds = std::thread::scope(|scope| -> io::Result<u128> {
			let mut handles = Vec::with_capacity(threads);
			for thread in 0..threads {
				let directory = directory.clone();
				let worker_start_gate = start_gate.clone();
				let remaining = remaining.clone();
				let completion = completion.clone();
				let paths = (0..files_per_thread)
					.map(|file| format!("concurrent-{sample}-{thread}-{file}"))
					.collect::<Vec<_>>();
				let handle = std::thread::Builder::new()
					.name(format!("kv-directory-benchmark-{thread}"))
					.spawn_scoped(scope, move || -> io::Result<()> {
						let payload = [19u8; CONCURRENT_BYTES_PER_FILE];
						let Some(started) = worker_start_gate.wait() else {
							return Ok(());
						};
						let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> io::Result<()> {
							for path in paths {
								let mut writer = open_writer(&directory, Path::new(&path))?;
								writer.write_all(&payload)?;
								writer.terminate()?;
							}
							Ok(())
						}))
						.unwrap_or_else(|_| Err(io::Error::other("benchmark worker panicked")));
						if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
							let _ = completion.send(started.elapsed().as_nanos());
						}
						result
					});
				match handle {
					Ok(handle) => handles.push(handle),
					Err(error) => {
						start_gate.cancel();
						for handle in handles {
							handle
								.join()
								.map_err(|_| io::Error::other("benchmark worker panicked"))??;
						}
						return Err(error);
					}
				}
			}
			start_gate.start(threads);
			drop(completion);
			let elapsed_nanoseconds = completed
				.recv()
				.map_err(|_| io::Error::other("benchmark workers did not report completion"))?;
			for handle in handles {
				handle
					.join()
					.map_err(|_| io::Error::other("benchmark worker panicked"))??;
			}
			Ok(elapsed_nanoseconds)
		})?;
		let operations = threads * files_per_thread;
		Ok(Sample {
			elapsed_nanoseconds,
			operations: operations as u64,
			bytes: (operations * CONCURRENT_BYTES_PER_FILE) as u64,
		})
	}
}

fn open_writer(directory: &FaultingDirectory, path: &Path) -> io::Result<WritePtr> {
	directory
		.open_write(path)
		.map_err(|error| io::Error::other(error.to_string()))
}

fn delete_path(directory: &FaultingDirectory, path: &Path) -> io::Result<()> {
	directory
		.delete(path)
		.map_err(|error| io::Error::other(error.to_string()))
}

fn encode_results(arguments: &Arguments, results: &[CaseResult]) -> String {
	let mut output = String::new();
	writeln!(&mut output, "{{").unwrap();
	writeln!(&mut output, "  \"formatVersion\": 1,").unwrap();
	writeln!(&mut output, "  \"benchmark\": \"kv-directory\",").unwrap();
	writeln!(&mut output, "  \"revision\": \"{}\",", escape_json(&arguments.revision)).unwrap();
	writeln!(&mut output, "  \"runtime\": {{").unwrap();
	writeln!(&mut output, "    \"rustc\": \"{}\",", escape_json(&rustc_version())).unwrap();
	writeln!(&mut output, "    \"tantivy\": \"{}\"", escape_json(TANTIVY_VERSION)).unwrap();
	writeln!(&mut output, "  }},").unwrap();
	writeln!(&mut output, "  \"host\": {{").unwrap();
	writeln!(&mut output, "    \"os\": \"{}\",", env::consts::OS).unwrap();
	writeln!(&mut output, "    \"arch\": \"{}\",", env::consts::ARCH).unwrap();
	writeln!(
		&mut output,
		"    \"parallelism\": {}",
		std::thread::available_parallelism().map_or(1, usize::from)
	)
	.unwrap();
	writeln!(&mut output, "  }},").unwrap();
	writeln!(&mut output, "  \"workload\": {{").unwrap();
	writeln!(&mut output, "    \"samples\": {},", arguments.samples).unwrap();
	writeln!(&mut output, "    \"warmupSamples\": {},", arguments.warmup_samples).unwrap();
	writeln!(&mut output, "    \"smoke\": {}", arguments.smoke).unwrap();
	writeln!(&mut output, "  }},").unwrap();
	writeln!(&mut output, "  \"results\": [").unwrap();
	for (index, result) in results.iter().enumerate() {
		let mut samples = result.sample_nanoseconds_per_operation.clone();
		samples.sort_by(f64::total_cmp);
		let seconds = result.total_nanoseconds as f64 / 1_000_000_000.0;
		let operations_per_second = result.operations as f64 / seconds;
		let bytes_per_second = result.bytes as f64 / seconds;
		writeln!(&mut output, "    {{").unwrap();
		writeln!(&mut output, "      \"name\": \"{}\",", escape_json(&result.name)).unwrap();
		writeln!(&mut output, "      \"operation\": \"{}\",", result.operation).unwrap();
		writeln!(&mut output, "      \"threads\": {},", result.threads).unwrap();
		writeln!(&mut output, "      \"operations\": {},", result.operations).unwrap();
		writeln!(&mut output, "      \"bytes\": {},", result.bytes).unwrap();
		writeln!(&mut output, "      \"totalNanoseconds\": {},", result.total_nanoseconds).unwrap();
		writeln!(
			&mut output,
			"      \"operationsPerSecond\": {operations_per_second:.3},"
		)
		.unwrap();
		writeln!(&mut output, "      \"bytesPerSecond\": {bytes_per_second:.3},").unwrap();
		writeln!(
			&mut output,
			"      \"p50NanosecondsPerOperation\": {:.3},",
			percentile(&samples, 0.50)
		)
		.unwrap();
		writeln!(
			&mut output,
			"      \"p95NanosecondsPerOperation\": {:.3},",
			percentile(&samples, 0.95)
		)
		.unwrap();
		writeln!(
			&mut output,
			"      \"p99NanosecondsPerOperation\": {:.3}",
			percentile(&samples, 0.99)
		)
		.unwrap();
		writeln!(
			&mut output,
			"    }}{}",
			if index + 1 == results.len() { "" } else { "," }
		)
		.unwrap();
	}
	writeln!(&mut output, "  ]").unwrap();
	writeln!(&mut output, "}}").unwrap();
	output
}

fn percentile(sorted_values: &[f64], fraction: f64) -> f64 {
	sorted_values[(sorted_values.len() as f64 * fraction).ceil() as usize - 1]
}

fn rustc_version() -> String {
	Command::new("rustc")
		.arg("--version")
		.output()
		.ok()
		.filter(|output| output.status.success())
		.and_then(|output| String::from_utf8(output.stdout).ok())
		.map_or_else(|| "unknown".to_owned(), |version| version.trim().to_owned())
}

fn escape_json(value: &str) -> String {
	let mut escaped = String::with_capacity(value.len());
	for character in value.chars() {
		match character {
			'\"' => escaped.push_str("\\\""),
			'\\' => escaped.push_str("\\\\"),
			'\n' => escaped.push_str("\\n"),
			'\r' => escaped.push_str("\\r"),
			'\t' => escaped.push_str("\\t"),
			character if character <= '\u{1f}' => write!(&mut escaped, "\\u{:04x}", character as u32).unwrap(),
			character => escaped.push(character),
		}
	}
	escaped
}
