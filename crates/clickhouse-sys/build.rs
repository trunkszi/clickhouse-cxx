//! Build orchestration for the statically linked `clickhouse-cpp` bridge.

use std::{
    env, io,
    path::{Path, PathBuf},
    process::Command,
};

#[path = "build_support/source.rs"]
mod source;

const REPOSITORY: &str = "https://github.com/ClickHouse/clickhouse-cpp.git";

/// Return the first source tree that contains the public client header.
fn resolve_source(cache: &Path) -> io::Result<PathBuf> {
    if let Some(configured) = env::var_os("CLICKHOUSE_CPP_DIR") {
        let candidate = PathBuf::from(configured);
        if is_source_tree(&candidate) {
            return Ok(candidate);
        }
    }
    let manifest = environment_path("CARGO_MANIFEST_DIR")?;
    let relative = manifest.join("../../vendor/clickhouse-cpp");
    if is_source_tree(&relative) {
        return Ok(relative);
    }
    if is_source_tree(cache) {
        return Ok(cache.to_path_buf());
    }
    if cache.exists() {
        std::fs::remove_dir_all(cache)?;
    }
    clone_source(cache)?;
    Ok(cache.to_path_buf())
}

fn environment_path(name: &str) -> io::Result<PathBuf> {
    env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::other(format!("missing build environment variable {name}")))
}

/// Compile-time cache next to Cargo's target directory.
fn target_cache_dir() -> io::Result<PathBuf> {
    let manifest = environment_path("CARGO_MANIFEST_DIR")?;
    let out_dir = environment_path("OUT_DIR")?;
    let target_dir = env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                manifest.join("../..").join(path)
            }
        })
        .or_else(|| {
            out_dir
                .ancestors()
                .find(|path| path.file_name().is_some_and(|name| name == "build"))
                .and_then(Path::parent)
                .and_then(Path::parent)
                .map(Path::to_path_buf)
        });
    Ok(target_dir
        .map(|path| path.join("vendor-cache/clickhouse-cpp"))
        .unwrap_or_else(|| out_dir.join("clickhouse-cpp-src")))
}

/// A source tree is usable when the public header is present.
fn is_source_tree(path: &Path) -> bool {
    path.join("clickhouse/client.h").is_file()
}

/// Clone the pinned upstream tree, including vendored dependencies.
fn clone_source(target: &Path) -> io::Result<()> {
    let status = Command::new("git")
        .args(["clone", "--depth", "1", "--recurse-submodules", REPOSITORY])
        .arg(target)
        .status()?;
    if !status.success() {
        return Err(io::Error::other("failed to clone clickhouse-cpp"));
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let cache = source::SourceCache::open(target_cache_dir()?)?;
    let resolved = resolve_source(cache.root())?;
    println!("cargo:rerun-if-changed={}", resolved.display());
    let source = cache.prepare(&resolved)?;
    let install = cmake::Config::new(&source)
        .out_dir(environment_path("OUT_DIR")?.join("validated-native"))
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

    cxx_build::bridges(["src/lib.rs", "src/bridge.rs"])
        .file("src/shim.cpp")
        .file("src/shim_block.cpp")
        .file("src/shim_client.cpp")
        .file("src/shim_column.cpp")
        .file("src/shim_composite.cpp")
        .file("cpp/rustls_socket.cpp")
        .file("cpp/native_client.cpp")
        .file("src/shim_stream.cpp")
        .file("src/shim_compression.cpp")
        .include("cpp")
        .define("CH_NON_OPTIONAL_CURRENT_ENDPOINT", "1")
        .define("CH_MAP_BOOL_TO_UINT8", "0")
        .define("CH_USE_ABSEIL_FOR_BIGNUM", "0")
        .include("src")
        .include(&source)
        .flag_if_supported("-std=c++17")
        .compile("clickhouse-sys");

    for path in [
        "src/lib.rs",
        "src/bridge.rs",
        "src/session.rs",
        "cpp/rustls_socket.cpp",
        "cpp/rustls_socket.h",
        "cpp/native_client.cpp",
        "cpp/native_client.h",
        "src/shim.cpp",
        "src/shim.h",
        "src/shim_matrix.h",
        "src/shim_block.cpp",
        "src/shim_client.cpp",
        "src/shim_column.cpp",
        "src/shim_composite.cpp",
        "src/shim_stream.cpp",
        "src/shim_compression.cpp",
        "build_support/source.rs",
    ] {
        println!("cargo:rerun-if-changed={path}");
    }
    println!("cargo:rerun-if-env-changed=CLICKHOUSE_CPP_DIR");
    Ok(())
}
