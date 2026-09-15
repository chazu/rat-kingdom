//! Tiny, hermetic Cargo fixture proving the shape of the fix in
//! `crates/rk-cli/build.rs` / `crates/rk-core/src/version.rs`: a two-crate
//! project structured exactly like `rk-core` (shared lib) + `rk-cli` (the
//! executable, whose own `build.rs` stamps the commit) should NOT recompile
//! the shared lib for a commit-only change, but SHOULD recompile it when its
//! own source changes; the app must always pick up a fresh commit stamp,
//! including after its directory is relocated (the `CARGO_MANIFEST_DIR`
//! cache-relocation bug this ticket also reproduced).
//!
//! This never touches the real workspace or its shared `target/` — it
//! generates a throwaway two-crate project in a tempdir with its own
//! isolated `--target-dir`, so it stays fast and cannot race or be raced by
//! a peer's build in the real `cargo-target-cache`.
//!
//! Whether a crate was actually recompiled is read directly off cargo's own
//! build output (`Compiling <crate>` on a cache miss, silence on a cache
//! hit) rather than inferred from artifact mtimes or content hashes — the
//! latter would be flaky here: a source edit that only changes a comment
//! still invalidates cargo's fingerprint and forces a real `rustc`
//! invocation, but can legitimately produce byte-identical output.

use std::fs;
use std::path::Path;
use std::process::Command;

fn cargo() -> String {
    std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string())
}

fn git(dir: &Path, args: &[&str]) {
    let status = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.com")
        .env("GIT_COMMITTER_NAME", "fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.com")
        .status()
        .unwrap_or_else(|e| panic!("spawn git {args:?} in {dir:?}: {e}"));
    assert!(status.success(), "git {args:?} in {dir:?} failed");
}

/// Writes the shared-lib + app project. `shared_lib_marker` lets the caller
/// force a shared-lib source change between builds.
fn write_project(root: &Path, shared_lib_marker: &str) {
    fs::create_dir_all(root.join("shared_lib/src")).unwrap();
    fs::create_dir_all(root.join("app/src")).unwrap();

    fs::write(
        root.join("Cargo.toml"),
        r#"[workspace]
members = ["shared_lib", "app"]
resolver = "2"
"#,
    )
    .unwrap();

    fs::write(
        root.join("shared_lib/Cargo.toml"),
        r#"[package]
name = "shared_lib"
version = "0.1.0"
edition = "2021"
"#,
    )
    .unwrap();
    fs::write(
        root.join("shared_lib/src/lib.rs"),
        format!("// marker: {shared_lib_marker}\npub fn hello() -> &'static str {{ \"hi\" }}\n"),
    )
    .unwrap();

    fs::write(
        root.join("app/Cargo.toml"),
        r#"[package]
name = "app"
version = "0.1.0"
edition = "2021"
build = "build.rs"

[dependencies]
shared_lib = { path = "../shared_lib" }
"#,
    )
    .unwrap();
    fs::write(
        root.join("app/src/main.rs"),
        "fn main() { println!(\"{} {}\", env!(\"RK_BUILD_SHA\"), shared_lib::hello()); }\n",
    )
    .unwrap();

    // Mirrors crates/rk-cli/build.rs: resolves HEAD via git, reading
    // CARGO_MANIFEST_DIR at *runtime* (not baked in via `env!`) so a later
    // relocation of this directory does not strand the stamp against a path
    // that no longer holds this checkout.
    fs::write(
        root.join("app/build.rs"),
        r#"use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/refs");
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let out = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .current_dir(&manifest_dir)
        .output()
        .ok();
    let sha = out
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=RK_BUILD_SHA={sha}");
}
"#,
    )
    .unwrap();
}

fn commit_all(root: &Path, message: &str) {
    git(root, &["add", "-A"]);
    git(root, &["commit", "-q", "-m", message]);
}

fn head_sha(root: &Path) -> String {
    let out = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .current_dir(root)
        .output()
        .unwrap();
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

/// Runs `cargo build` and returns its stderr, where cargo's progress/compile
/// log (`Compiling <crate>` on a cache miss) lives.
fn build(root: &Path, target_dir: &Path) -> String {
    let out = Command::new(cargo())
        .args(["build", "--target-dir"])
        .arg(target_dir)
        .current_dir(root)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "fixture cargo build failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn was_compiled(build_log: &str, crate_name: &str) -> bool {
    build_log.contains(&format!("Compiling {crate_name} "))
}

fn run_app(target_dir: &Path) -> String {
    let out = Command::new(target_dir.join("debug/app")).output().unwrap();
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn commit_only_change_leaves_the_shared_lib_untouched_but_restamps_the_app() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("proj");
    let target_dir = tmp.path().join("target");
    fs::create_dir_all(&root).unwrap();

    write_project(&root, "v1");
    git(&root, &["init", "-q"]);
    commit_all(&root, "initial");
    let sha_a = head_sha(&root);

    let first = build(&root, &target_dir);
    assert!(
        was_compiled(&first, "shared_lib") && was_compiled(&first, "app"),
        "the first build must compile both crates from scratch: {first}"
    );

    // A commit-only change: no source touched, just a new commit (as an
    // ordinary merge or unrelated file would produce).
    fs::write(root.join("NOTES.md"), "unrelated change\n").unwrap();
    commit_all(&root, "commit-only change, no source touched");
    let sha_b = head_sha(&root);
    assert_ne!(sha_a, sha_b, "the fixture commit must actually move HEAD");

    let second = build(&root, &target_dir);
    assert!(
        !was_compiled(&second, "shared_lib"),
        "shared_lib must NOT be recompiled for a commit-only change — this is \
         exactly the invalidation this ticket removes: {second}"
    );
    assert!(
        was_compiled(&second, "app"),
        "app's own build.rs output changed (new RK_BUILD_SHA), so app itself \
         must still be rebuilt: {second}"
    );

    let stdout = run_app(&target_dir);
    assert!(
        stdout.contains(&sha_b),
        "the app must restamp itself with the new commit: {stdout}"
    );
}

#[test]
fn a_real_shared_lib_source_change_still_rebuilds_its_dependent() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("proj");
    let target_dir = tmp.path().join("target");
    fs::create_dir_all(&root).unwrap();

    write_project(&root, "v1");
    git(&root, &["init", "-q"]);
    commit_all(&root, "initial");
    build(&root, &target_dir);

    // A real change to the shared lib's own source.
    write_project(&root, "v2");
    commit_all(&root, "shared_lib source change");
    let log = build(&root, &target_dir);

    assert!(
        was_compiled(&log, "shared_lib"),
        "a genuine shared_lib source edit must still trigger a rebuild — \
         the fix removes commit-stamp-only invalidation, not real \
         invalidation: {log}"
    );
    assert!(
        was_compiled(&log, "app"),
        "app depends on shared_lib, so it must relink too: {log}"
    );
}

#[test]
fn relocating_the_checkout_does_not_strand_the_stamp_on_the_old_path() {
    let tmp = tempfile::tempdir().unwrap();
    let root_a = tmp.path().join("checkout-a");
    let target_dir = tmp.path().join("target");
    fs::create_dir_all(&root_a).unwrap();

    write_project(&root_a, "v1");
    git(&root_a, &["init", "-q"]);
    commit_all(&root_a, "initial");
    let sha = head_sha(&root_a);

    // First build compiles (and caches) app's build-script executable while
    // the checkout lives at `checkout-a`.
    build(&root_a, &target_dir);

    // Relocate the whole checkout — the linked-worktree-move / cache
    // -relocation scenario from the ticket's reproduced bug. The cached
    // build-script binary is reused (nothing about its own source changed),
    // but it must still resolve HEAD correctly because it reads
    // `CARGO_MANIFEST_DIR` fresh from the environment on every invocation
    // rather than a path baked in at its own compile time.
    let root_b = tmp.path().join("checkout-b");
    fs::rename(&root_a, &root_b).unwrap();

    build(&root_b, &target_dir);

    let stdout = run_app(&target_dir);
    assert!(
        stdout.contains(&sha),
        "after relocation the app must still report the real HEAD sha, not \
         fall back to 'unknown': {stdout}"
    );
    assert!(
        !stdout.contains("unknown"),
        "a stranded compile-time CARGO_MANIFEST_DIR would silently report \
         'unknown' here: {stdout}"
    );
}
