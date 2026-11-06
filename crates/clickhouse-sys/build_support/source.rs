//! An owned, locked source cache isolates upstream patches from supplied trees.

use std::{
    collections::hash_map::DefaultHasher,
    fs::{self, File, OpenOptions},
    hash::{Hash, Hasher},
    io::{self, Read},
    path::{Path, PathBuf},
};

pub struct SourceCache {
    root: PathBuf,
    _lock: File,
}

impl SourceCache {
    /// Lock one build-owned source tree through its complete compilation.
    /// # Errors
    /// Returns filesystem failures or rejects a symlinked cache root.
    pub fn open(root: PathBuf) -> io::Result<Self> {
        let parent = root
            .parent()
            .ok_or_else(|| invalid("cache has no parent"))?;
        fs::create_dir_all(parent)?;
        let root = parent.canonicalize()?.join(
            root.file_name()
                .ok_or_else(|| invalid("cache has no directory name"))?,
        );
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.with_extension("lock"))?;
        lock.lock()?;
        if root.symlink_metadata().is_ok_and(|meta| meta.is_symlink()) {
            return Err(invalid("source cache must not be a symlink"));
        }
        Ok(Self { root, _lock: lock })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Copy an external tree when its contents change, then validate its decoder.
    /// # Errors
    /// Rejects unsupported decoder source, overlapping paths, or source-copy drift.
    pub fn prepare(&self, source: &Path) -> io::Result<PathBuf> {
        let source = source.canonicalize()?;
        if source == self.root {
            patch_decoder(&self.root)?;
            return Ok(self.root.clone());
        }
        if source.starts_with(&self.root) || self.root.starts_with(&source) {
            return Err(invalid("external source and cache must not overlap"));
        }
        let fingerprint = fingerprint(&source)?;
        let stamp = self.root.join(".clickhouse-cxx-source");
        if fs::read_to_string(&stamp).is_ok_and(|saved| saved == fingerprint) {
            patch_decoder(&self.root)?;
            return Ok(self.root.clone());
        }
        let staged = self
            .root
            .with_extension(format!("stage-{}", std::process::id()));
        if staged.exists() {
            fs::remove_dir_all(&staged)?;
        }
        let result = (|| {
            copy_tree(&source, &staged)?;
            if fingerprint != self::fingerprint(&staged)? {
                return Err(invalid("source changed while populating the cache"));
            }
            patch_decoder(&staged)?;
            fs::write(staged.join(".clickhouse-cxx-source"), fingerprint)?;
            if self.root.exists() {
                fs::remove_dir_all(&self.root)?;
            }
            fs::rename(&staged, &self.root)
        })();
        if result.is_err() {
            let _cleanup = fs::remove_dir_all(&staged);
        }
        result?;
        Ok(self.root.clone())
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn entries(root: &Path) -> io::Result<Vec<fs::DirEntry>> {
    let mut entries = fs::read_dir(root)?.collect::<io::Result<Vec<_>>>()?;
    entries.retain(|entry| entry.file_name() != ".git");
    entries.sort_by_key(fs::DirEntry::file_name);
    Ok(entries)
}

fn copy_tree(source: &Path, target: &Path) -> io::Result<()> {
    fs::create_dir_all(target)?;
    for entry in entries(source)? {
        let from = entry.path();
        let to = target.join(entry.file_name());
        let metadata = from.metadata()?;
        if metadata.is_dir() && !entry.file_type()?.is_symlink() {
            copy_tree(&from, &to)?;
        } else if metadata.is_file() {
            // File symlinks become independent files so patching cannot write through them.
            fs::copy(&from, &to)?;
        } else {
            return Err(invalid(
                "unsupported special file or directory symlink in source",
            ));
        }
    }
    Ok(())
}

fn fingerprint(source: &Path) -> io::Result<String> {
    fn visit(root: &Path, hash: &mut DefaultHasher) -> io::Result<()> {
        for entry in entries(root)? {
            entry.file_name().hash(hash);
            let metadata = entry.path().metadata()?;
            if metadata.is_dir() && !entry.file_type()?.is_symlink() {
                0_u8.hash(hash);
                visit(&entry.path(), hash)?;
            } else if metadata.is_file() {
                1_u8.hash(hash);
                metadata.len().hash(hash);
                let mut file = File::open(entry.path())?;
                let mut bytes = [0_u8; 8192];
                loop {
                    let count = file.read(&mut bytes)?;
                    if count == 0 {
                        break;
                    }
                    hash.write(&bytes[..count]);
                }
            } else {
                return Err(invalid(
                    "unsupported special file or directory symlink in source",
                ));
            }
        }
        Ok(())
    }
    let mut hash = DefaultHasher::new();
    visit(source, &mut hash)?;
    Ok(format!("{:016x}", hash.finish()))
}

fn patch_decoder(root: &Path) -> io::Result<()> {
    let file = root.join("clickhouse/base/compressed.cpp");
    let original = fs::read_to_string(&file)?;
    let mut patched = original.clone();
    if !patched.contains("static_cast<uint32_t>(decoded) != original") {
        let old = "if (LZ4_decompress_safe((const char*)tmp.data() + HEADER_SIZE, \
                   (char*)data_.data(), static_cast<int>(compressed - HEADER_SIZE), original) < \
                   0) {";
        if patched.matches(old).count() != 1 {
            return Err(invalid(
                "unrecognized LZ4 decoder; cannot verify exact-size validation",
            ));
        }
        patched =
            patched.replace(
                old,
                "const auto decoded = LZ4_decompress_safe((const char*)tmp.data() + \
                 HEADER_SIZE,\n            (char*)data_.data(), static_cast<int>(compressed - \
                 HEADER_SIZE), original);\n        if (decoded < 0 || \
                 static_cast<uint32_t>(decoded) != original) {",
            );
    }
    if !patched.contains("res != original") {
        let old = "throw CompressionError(\"can't decompress ZSTD-encoded data, ZSTD error: \" + \
                   std::string(ZSTD_getErrorName(res)));\n        } else {";
        if patched.matches(old).count() != 1 {
            return Err(invalid(
                "unrecognized ZSTD decoder; cannot verify exact-size validation",
            ));
        }
        patched = patched.replace(old, "throw CompressionError(\"can't decompress ZSTD-encoded data, ZSTD error: \" + std::string(ZSTD_getErrorName(res)));\n        } else if (res != original) {\n            throw CompressionError(\"decompressed ZSTD size does not match header\");\n        } else {");
    }
    if patched != original {
        let temporary = file.with_extension("cpp.new");
        fs::write(&temporary, patched)?;
        fs::rename(temporary, file)?;
    }
    Ok(())
}
