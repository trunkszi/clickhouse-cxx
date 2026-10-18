//! Build orchestration for the statically linked `clickhouse-cpp` bridge.

use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

const REPOSITORY: &str = "https://github.com/ClickHouse/clickhouse-cpp.git";

/// Return the first source tree that contains the public client header.
fn resolve_source() -> PathBuf {
    if let Ok(configured) = env::var("CLICKHOUSE_CPP_DIR") {
        let candidate = PathBuf::from(configured);
        if is_source_tree(&candidate) {
            return candidate;
        }
    }
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let relative = manifest.join("../../vendor/clickhouse-cpp");
    if is_source_tree(&relative) {
        return relative;
    }
    let fetched = target_cache_dir();
    if is_source_tree(&fetched) {
        return fetched;
    }
    clone_source(&fetched);
    fetched
}

/// Compile-time cache next to Cargo's target directory.
fn target_cache_dir() -> PathBuf {
    if let Ok(configured) = env::var("CARGO_TARGET_DIR") {
        return PathBuf::from(configured)
            .join("vendor-cache")
            .join("clickhouse-cpp");
    }
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("out dir"));
    // OUT_DIR is <target>/<profile>/build/<crate>-<hash>/out.
    if let Some(target) = out_dir.ancestors().nth(4) {
        return target.join("vendor-cache").join("clickhouse-cpp");
    }
    out_dir.join("clickhouse-cpp-src")
}

/// A source tree is usable when the public header is present.
fn is_source_tree(path: &Path) -> bool {
    path.join("clickhouse/client.h").is_file()
}

/// Clone the pinned upstream tree, including vendored dependencies.
fn clone_source(target: &Path) {
    let status = Command::new("git")
        .args([
            "clone",
            "--depth",
            "1",
            "--recurse-submodules",
            REPOSITORY,
            target.to_str().expect("utf-8 target path"),
        ])
        .status()
        .expect("run git clone for clickhouse-cpp");
    assert!(status.success(), "failed to clone clickhouse-cpp");
}

fn main() {
    let source = resolve_source();
    let install = cmake::Config::new(&source)
        .define("BUILD_SHARED_LIBS", "OFF")
        .define("WITH_OPENSSL", "OFF")
        .define("CH_USE_3X_API", "ON")
        .define("WITH_SYSTEM_ABSEIL", "OFF")
        .define("WITH_SYSTEM_CITYHASH", "OFF")
        .define("WITH_SYSTEM_LZ4", "OFF")
        .define("WITH_SYSTEM_ZSTD", "OFF")
        .define("CMAKE_POSITION_INDEPENDENT_CODE", "ON")
        .define("BUILD_TESTS", "OFF")
        .define("BUILD_BENCHMARK", "OFF")
        .profile("Release")
        .build();

    println!(
        "cargo:rustc-link-search=native={}",
        install.join("lib").display()
    );
    // The CMake alias `clickhouse-cpp-lib-static` installs as
    // `libclickhouse-cpp-lib.a`; 3.x turns off the abseil bignum archive.
    for library in ["clickhouse-cpp-lib", "cityhash", "lz4", "zstdstatic"] {
        println!("cargo:rustc-link-lib=static={library}");
    }
    println!("cargo:rustc-link-lib=dylib=stdc++");
    println!("cargo:rustc-link-lib=dylib=pthread");
    println!("cargo:rustc-link-lib=dylib=m");

    cxx_build::bridge("src/lib.rs")
        .file("src/shim.cpp")
        .file("src/shim_block.cpp")
        .file("src/shim_client.cpp")
        .file("src/shim_column.cpp")
        .file("src/shim_composite.cpp")
        .include("src")
        .include(&source)
        .flag_if_supported("-std=c++17")
        .compile("clickhouse-sys");

    for path in [
        "src/lib.rs",
        "src/shim.cpp",
        "src/shim.h",
        "src/shim_matrix.h",
        "src/shim_block.cpp",
        "src/shim_client.cpp",
        "src/shim_column.cpp",
        "src/shim_composite.cpp",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    println!("cargo:rerun-if-env-changed=CLICKHOUSE_CPP_DIR");
}
