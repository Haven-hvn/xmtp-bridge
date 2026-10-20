//! xmtp-sys links a prebuilt Rust `staticlib` that carries its own copy of
//! Rust's std, which collides with this binary's (e.g. `rust_eh_personality`)
//! on GNU and MSVC linkers. The xmtp crate's README prescribes these flags for
//! any binary that links it. Only this worker binary links xmtp-sys; the
//! Signal side runs in a separate process and never shares these symbols.
//!
//! macOS needs nothing: ld64 resolves the archive without duplicates (verified
//! with Xcode's linker, which also reports `-multiply_defined` as obsolete).
fn main() {
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.contains("linux") {
        println!("cargo:rustc-link-arg=-Wl,--allow-multiple-definition");
    } else if target.contains("windows") && target.contains("msvc") {
        println!("cargo:rustc-link-arg=/FORCE:MULTIPLE");
    }
}
