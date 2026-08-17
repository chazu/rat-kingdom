//! Stamps the commit this tree was built from into `RK_BUILD_SHA`, so a binary
//! can say which build it *is* and not merely which semver it claims.
//!
//! The workspace version has been `0.1.0` since the first commit and will stay
//! there for a long while, so comparing `CARGO_PKG_VERSION` across a socket
//! answers nothing: a daemon started before a merge and a CLI built after it
//! report the identical string. The commit does distinguish them, which is what
//! makes the CLI<->daemon handshake in `rk_core::version` able to notice that a
//! running daemon predates the code the operator just installed.
//!
//! Deliberately *not* a dirty-tree flag: this script only reruns when the git
//! files listed below change, and editing a source file touches none of them,
//! so a `-dirty` suffix computed here would go stale the moment it mattered.
//! Uncommitted differences between two builds are therefore invisible to the
//! handshake — commit before deploying if you want the check to be exact.

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
/// rk-core — and so relink the whole workspace — for no change in output.
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
    let out = Command::new("git")
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
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
