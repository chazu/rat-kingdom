//! Regression coverage for `DiskConfig::shared_cargo_target`
//! (TKT-01M0EXYHV1GR9Z75QSS42HXBVK): proves, with a real `cargo build`
//! subprocess rather than an assertion about our own code, that sharing one
//! `CARGO_TARGET_DIR` across two differently-pathed checkouts of the same
//! workspace corrupts a build with **zero concurrency involved** — the
//! second build silently reuses the first checkout's compiled artifact
//! instead of recompiling — and that per-checkout target dirs (the current
//! default) do not.
//!
//! Uses a minimal two-crate fixture workspace instead of this repo's real
//! workspace: a full `cargo build -p rk-cli` per checkout takes 30-60s,
//! which is what actually confirmed this bug (two real `git worktree`
//! checkouts of this repo at different commits, shared `CARGO_TARGET_DIR`,
//! `crates/rk-cli/src/main.rs` failed to compile against a stale `rk-core`
//! that predated a field it referenced) — far too slow to carry in
//! `cargo test --workspace`. This fixture reproduces the identical cargo
//! mechanism (a local path crate, in a `[workspace]`, recompiled from a
//! differently-pathed checkout with different content) in well under a
//! second, and was verified by hand against the same failure signature
//! before being written up as a test.
//!
//! A single-crate (non-workspace) fixture does *not* reproduce this — only
//! a `[workspace]` layout does, matching this repo's own structure.

use std::path::Path;
use std::process::{Command, Output};

/// `answer_is_new`: whether the fixture's lib exports `answer()` and the bin
/// calls it. The two checkouts differ only in this, mirroring two worktrees
/// at different commits of the same repo.
fn write_fixture_checkout(dir: &Path, answer_is_new: bool) {
    std::fs::create_dir_all(dir.join("lib/src")).unwrap();
    std::fs::create_dir_all(dir.join("bin/src")).unwrap();

    std::fs::write(
        dir.join("Cargo.toml"),
        "[workspace]\nresolver = \"2\"\nmembers = [\"lib\", \"bin\"]\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("lib/Cargo.toml"),
        "[package]\nname = \"fixture-lib\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("bin/Cargo.toml"),
        "[package]\nname = \"fixture-bin\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n\
         [dependencies]\nfixture-lib = { path = \"../lib\" }\n",
    )
    .unwrap();

    let (lib_body, bin_body) = if answer_is_new {
        (
            "pub fn answer() -> i32 { 42 }\n",
            "fn main() { println!(\"{}\", fixture_lib::answer()); }\n",
        )
    } else {
        ("\n", "fn main() { println!(\"0\"); }\n")
    };
    std::fs::write(dir.join("lib/src/lib.rs"), lib_body).unwrap();
    std::fs::write(dir.join("bin/src/main.rs"), bin_body).unwrap();
}

fn cargo_build(checkout: &Path, target_dir: &Path) -> Output {
    Command::new(env!("CARGO"))
        .arg("build")
        .arg("--quiet")
        .current_dir(checkout)
        .env("CARGO_TARGET_DIR", target_dir)
        // Isolate from whatever invoked this test — no RK_* spawn identity,
        // no inherited CARGO_TARGET_DIR override.
        .env_remove("RK_AGENT")
        .output()
        .expect("run cargo build")
}

fn bin_output(target_dir: &Path) -> String {
    let out = Command::new(target_dir.join("debug/fixture-bin"))
        .output()
        .expect("run fixture-bin");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[test]
fn sharing_a_target_dir_across_checkouts_serves_a_stale_binary() {
    let root = tempfile::tempdir().unwrap();
    let checkout_a = root.path().join("checkout-a");
    let checkout_b = root.path().join("checkout-b");
    write_fixture_checkout(&checkout_a, false);
    write_fixture_checkout(&checkout_b, true);

    let shared_target = root.path().join("shared-target");
    let out_a = cargo_build(&checkout_a, &shared_target);
    assert!(
        out_a.status.success(),
        "checkout-a build failed: {}",
        String::from_utf8_lossy(&out_a.stderr)
    );

    // No overlap in time with checkout-a's build above — this is not a race.
    let out_b = cargo_build(&checkout_b, &shared_target);
    assert!(
        out_b.status.success(),
        "checkout-b build failed outright: {}",
        String::from_utf8_lossy(&out_b.stderr)
    );

    // checkout-b's own source calls `answer()` and should print "42". If
    // this reads "0", cargo silently linked checkout-a's stale `fixture-lib`
    // instead of recompiling checkout-b's — the exact defect
    // `DiskConfig::shared_cargo_target` now defaults off to avoid.
    assert_eq!(
        bin_output(&shared_target),
        "0",
        "expected the known cargo hazard (stale cross-checkout artifact reuse under a \
         shared CARGO_TARGET_DIR) to reproduce here. If this now reads \"42\", cargo's \
         behavior changed upstream and DiskConfig::shared_cargo_target's doc comment / \
         default needs re-evaluating, not just this assertion."
    );
}

#[test]
fn per_checkout_target_dirs_do_not_corrupt_each_other() {
    let root = tempfile::tempdir().unwrap();
    let checkout_a = root.path().join("checkout-a");
    let checkout_b = root.path().join("checkout-b");
    write_fixture_checkout(&checkout_a, false);
    write_fixture_checkout(&checkout_b, true);

    let target_a = checkout_a.join("target");
    let target_b = checkout_b.join("target");

    let out_a = cargo_build(&checkout_a, &target_a);
    assert!(
        out_a.status.success(),
        "{}",
        String::from_utf8_lossy(&out_a.stderr)
    );
    let out_b = cargo_build(&checkout_b, &target_b);
    assert!(
        out_b.status.success(),
        "{}",
        String::from_utf8_lossy(&out_b.stderr)
    );

    assert_eq!(bin_output(&target_a), "0");
    assert_eq!(
        bin_output(&target_b),
        "42",
        "per-checkout target dirs must not corrupt each other"
    );
}
