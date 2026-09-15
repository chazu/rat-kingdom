//! Stamps the commit this tree was built from into `RK_BUILD_SHA`, so the
//! standalone `rk-mcp` binary can say which build it *is* — see
//! `crates/rk-cli/build.rs` for the full rationale (this is that same script,
//! duplicated rather than shared, because it is exactly the kind of tiny
//! per-executable leaf logic this ticket moved *out* of the widely-depended
//! `rk-core` in the first place; sharing it back through a crate would
//! reintroduce the same invalidation-fanout problem for whichever of the two
//! executables didn't change).
//!
//! `rk-mcp` is a second, independent entry point into the same daemon
//! protocol as `rk` (an MCP stdio bridge run directly by an MCP host, not a
//! subcommand of `rk`), so it needs its own build-identity stamp and its own
//! `rk_core::version::init_build_sha` call at the top of its `main` — the
//! `rk` binary's call does not cover this separate process.

use std::path::Path;
use std::process::Command;

fn main() {
    // An explicit override wins, for builds from a source tarball (no `.git`)
    // that still know their provenance — a packager can pass the sha in.
    println!("cargo:rerun-if-env-changed=RK_BUILD_SHA");
    if let Ok(sha) = std::env::var("RK_BUILD_SHA") {
        println!("cargo:rustc-env=RK_BUILD_SHA={}", sanitize(&sha));
        return;
    }

    for path in rerun_paths() {
        println!("cargo:rerun-if-changed={path}");
    }

    let sha = git(&["rev-parse", "--short=12", "HEAD"]).unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=RK_BUILD_SHA={}", sanitize(&sha));
}

/// The git files whose contents decide the answer above.
///
/// Emitting *any* `rerun-if-changed` opts out of cargo's default "rerun when a
/// package file changes", which is the point: this script's output depends on
/// git state alone, and rerunning it on every source edit would recompile
/// rk-mcp — and so relink the `rk-mcp` binary — for no change in output.
///
/// `--git-path` is used rather than hand-built `.git/...` strings because in a
/// linked worktree `.git` is a *file* and HEAD lives under
/// `<common>/worktrees/<name>/`; git resolves that, we would get it wrong.
fn rerun_paths() -> Vec<String> {
    let mut paths = Vec::new();
    let Some(head) = git(&["rev-parse", "--git-path", "HEAD"]) else {
        // Not a git checkout: emit nothing and let cargo fall back to its
        // default file-change heuristic.
        return paths;
    };
    paths.push(head);
    // The branch ref moves on commit; HEAD itself does not (it holds a symref).
    if let Some(reference) = git(&["rev-parse", "--symbolic-full-name", "HEAD"]) {
        if let Some(path) = git(&["rev-parse", "--git-path", &reference]) {
            paths.push(path);
        }
    }
    // A packed ref has no loose file, so watch the pack too.
    if let Some(packed) = git(&["rev-parse", "--git-path", "packed-refs"]) {
        paths.push(packed);
    }
    // Cargo treats a missing path as "rerun every time", which would defeat the
    // whole point for the loose-ref-vs-packed-refs pair (exactly one exists).
    paths.retain(|path| Path::new(path).exists());
    paths
}

fn git(args: &[&str]) -> Option<String> {
    // Read at runtime, not baked in via `env!`, so a cached build-script
    // binary keeps resolving the right checkout after the crate directory
    // moves (a linked worktree relocated or promoted from a shared cache).
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").ok()?;
    let out = Command::new("git")
        .args(args)
        .current_dir(manifest_dir)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8(out.stdout).ok()?.trim().to_string();
    (!text.is_empty()).then_some(text)
}

/// Keep the stamp to characters that survive a `cargo:rustc-env` line and read
/// unambiguously inside a `<semver>+<sha>` version string.
fn sanitize(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '.' || *c == '-')
        .take(32)
        .collect();
    if cleaned.is_empty() {
        "unknown".into()
    } else {
        cleaned
    }
}
