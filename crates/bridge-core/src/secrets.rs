//! Reading secrets from files.
//!
//! Secrets (the bridge's Ethereum key, database keys, the Signal store
//! passphrase) live only in files, never in the config or in environment
//! variables, and those files must not be readable by group or others.

use std::fs;
use std::path::Path;

use anyhow::Context;

/// Read a secret file, trimmed of surrounding whitespace.
///
/// # Errors
/// Missing or empty file, or (on Unix) a file group/others can access.
pub fn read_secret_file(path: &Path) -> anyhow::Result<String> {
    check_private(path)?;
    let text = fs::read_to_string(path).with_context(|| format!("reading secret {}", path.display()))?;
    let trimmed = text.trim();
    anyhow::ensure!(!trimmed.is_empty(), "secret file {} is empty", path.display());
    Ok(trimmed.to_owned())
}

/// Read a hex secret of exactly `len` bytes (an optional `0x` prefix is allowed).
///
/// # Errors
/// As [`read_secret_file`], plus malformed hex or a wrong length.
pub fn read_hex_secret(path: &Path, len: usize) -> anyhow::Result<Vec<u8>> {
    let text = read_secret_file(path)?;
    let body = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")).unwrap_or(&text);
    anyhow::ensure!(
        body.len() == len * 2 && body.bytes().all(|b| b.is_ascii_hexdigit()),
        "secret file {} must contain {len} bytes as hex ({} hex characters)",
        path.display(),
        len * 2
    );
    Ok((0..len).map(|i| u8::from_str_radix(&body[i * 2..i * 2 + 2], 16).expect("validated hex")).collect())
}

#[cfg(unix)]
fn check_private(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let meta = fs::metadata(path).with_context(|| format!("secret file {} not found", path.display()))?;
    anyhow::ensure!(meta.is_file(), "secret {} is not a regular file", path.display());
    let mode = meta.permissions().mode();
    anyhow::ensure!(
        mode & 0o077 == 0,
        "secret file {} is accessible by group/others (mode {:o}); run: chmod 600 {}",
        path.display(),
        mode & 0o777,
        path.display()
    );
    Ok(())
}

#[cfg(not(unix))]
fn check_private(path: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(path.is_file(), "secret file {} not found", path.display());
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    fn write(dir: &Path, name: &str, body: &str, mode: u32) -> std::path::PathBuf {
        let path = dir.join(name);
        fs::write(&path, body).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    #[test]
    fn enforces_permissions_and_format() {
        let dir = std::env::temp_dir().join(format!("bridge-secrets-test-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let ok = write(&dir, "ok", &format!("0x{}\n", "ab".repeat(32)), 0o600);
        assert_eq!(read_hex_secret(&ok, 32).unwrap(), vec![0xab; 32]);
        let open = write(&dir, "open", "secret", 0o644);
        assert!(read_secret_file(&open).is_err(), "world-readable secret rejected");
        let short = write(&dir, "short", "abcd", 0o600);
        assert!(read_hex_secret(&short, 32).is_err());
        let empty = write(&dir, "empty", "  \n", 0o600);
        assert!(read_secret_file(&empty).is_err());
        fs::remove_dir_all(dir).unwrap();
    }
}
