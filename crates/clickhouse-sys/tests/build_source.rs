//! Source preparation must leave supplied trees untouched and fail closed on drift.

#[path = "../build_support/source.rs"]
mod source;

use std::{
    fs, io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use source::SourceCache;

const LEGACY: &str = r#"
        if (LZ4_decompress_safe((const char*)tmp.data() + HEADER_SIZE, (char*)data_.data(), static_cast<int>(compressed - HEADER_SIZE), original) < 0) {
            throw CompressionError("can't decompress LZ4-encoded data");
        }
        if (ZSTD_isError(res)) {
            throw CompressionError("can't decompress ZSTD-encoded data, ZSTD error: " + std::string(ZSTD_getErrorName(res)));
        } else {
            mem_.Reset(data_.data(), original);
        }
"#;

struct Directory(PathBuf);
impl Directory {
    fn new() -> io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "clickhouse-source-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _cleanup = fs::remove_dir_all(&self.0);
    }
}

fn fixture(root: &Path, text: &str) -> io::Result<PathBuf> {
    fs::create_dir_all(root.join("clickhouse/base"))?;
    fs::write(root.join("clickhouse/client.h"), "client header")?;
    let decoder = root.join("clickhouse/base/compressed.cpp");
    fs::write(&decoder, text)?;
    Ok(decoder)
}

#[test]
fn external_source_stays_read_only() -> io::Result<()> {
    let directory = Directory::new()?;
    let external = directory.0.join("external");
    let decoder = fixture(&external, LEGACY)?;
    let mut permissions = fs::metadata(&decoder)?.permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&decoder, permissions)?;
    let cache = SourceCache::open(directory.0.join("cache"))?;
    let prepared = cache.prepare(&external)?;
    assert_eq!(prepared, cache.root());
    assert_eq!(fs::read_to_string(&decoder)?, LEGACY);
    assert!(fs::metadata(&decoder)?.permissions().readonly());
    let cached = prepared.join("clickhouse/base/compressed.cpp");
    let contents = fs::read_to_string(&cached)?;
    assert!(contents.contains("static_cast<uint32_t>(decoded) != original"));
    assert!(contents.contains("res != original"));
    let identity = fs::metadata(&cached)?.ino();
    assert_eq!(cache.prepare(&external)?, prepared);
    assert_eq!(fs::metadata(&cached)?.ino(), identity);
    fs::write(external.join("clickhouse/client.h"), "updated header")?;
    cache.prepare(&external)?;
    assert_eq!(
        fs::read_to_string(prepared.join("clickhouse/client.h"))?,
        "updated header"
    );
    assert_eq!(fs::read_to_string(decoder)?, LEGACY);
    Ok(())
}

#[test]
fn unknown_decoder_preserves_valid_cache() -> io::Result<()> {
    let directory = Directory::new()?;
    let external = directory.0.join("external");
    fixture(&external, LEGACY)?;
    let cache = SourceCache::open(directory.0.join("cache"))?;
    let prepared = cache.prepare(&external)?;
    let cached = prepared.join("clickhouse/base/compressed.cpp");
    let contents = fs::read(&cached)?;
    let unknown = directory.0.join("unknown");
    fixture(&unknown, "unsupported upstream decoder")?;
    assert!(cache.prepare(&unknown).is_err());
    assert_eq!(fs::read(&cached)?, contents);
    Ok(())
}

#[test]
fn cached_patch_does_not_rewrite_source() -> io::Result<()> {
    let directory = Directory::new()?;
    let cache = SourceCache::open(directory.0.join("cache"))?;
    let decoder = fixture(cache.root(), LEGACY)?;
    cache.prepare(cache.root())?;
    let identity = fs::metadata(&decoder)?.ino();
    cache.prepare(cache.root())?;
    assert_eq!(fs::metadata(decoder)?.ino(), identity);
    Ok(())
}
