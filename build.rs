use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    
    let config = cbindgen::Config::default();
    
    cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(config)
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_dir.join("iceberg_rust_ffi.h"));
    
    // Note: We're using a manually created header file instead of the cbindgen-generated one
    // The cbindgen output is available in the build output directory if needed for reference
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=include/iceberg_rust_ffi.h");
} 