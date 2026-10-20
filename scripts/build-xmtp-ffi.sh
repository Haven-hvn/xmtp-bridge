#!/usr/bin/env bash
# Build libxmtp_ffi (the C-ABI static library behind the `xmtp` crate) from
# source, at exactly the revision Cargo.toml pins, into
# .deps/qntx-xmtp/xmtp-ffi/target/release — where .cargo/config.toml points
# XMTP_FFI_DIR. With that set, xmtp-sys never downloads a prebuilt binary.
#
# Usage: scripts/build-xmtp-ffi.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
repo_url="https://github.com/qntx-labs/xmtp"
dest="$root/.deps/qntx-xmtp"

# The single source of truth for the revision is the workspace Cargo.toml.
rev="$(sed -n 's/^xmtp = { git = "https:\/\/github.com\/qntx-labs\/xmtp", rev = "\([0-9a-f]\{40\}\)".*/\1/p' "$root/Cargo.toml")"
if [[ -z "$rev" ]]; then
  echo "could not find the pinned qntx-labs/xmtp rev in Cargo.toml" >&2
  exit 1
fi

if [[ ! -d "$dest/.git" ]]; then
  mkdir -p "$(dirname "$dest")"
  git init --quiet "$dest"
  git -C "$dest" remote add origin "$repo_url"
fi
git -C "$dest" fetch --quiet --depth 1 origin "$rev"
git -C "$dest" checkout --quiet --detach "$rev"
actual="$(git -C "$dest" rev-parse HEAD)"
if [[ "$actual" != "$rev" ]]; then
  echo "checked out $actual, expected $rev" >&2
  exit 1
fi

echo "building libxmtp_ffi at $rev (this compiles libxmtp; expect several minutes)"
cd "$dest/xmtp-ffi"
# xmtp-ffi carries its own rust-toolchain.toml and Cargo.lock.
cargo build --release --locked

lib="$dest/xmtp-ffi/target/release/libxmtp_ffi.a"
if [[ ! -f "$lib" ]]; then
  echo "expected $lib after the build" >&2
  exit 1
fi
echo "ok: $lib"
