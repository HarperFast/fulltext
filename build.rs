fn main() {
	napi_build::setup();
	println!(
		"cargo:rustc-env=FULLTEXT_BUILD_PROFILE={}",
		std::env::var("PROFILE").unwrap()
	);
}
