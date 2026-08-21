//! TKT-182 guard: no integration test may take its RPC identity from the
//! ambient environment.
//!
//! `Client::connect` resolves the caller from `RK_AGENT`/`RK_AUTH_TOKEN`, which
//! is right for `rk` and wrong for a test. A rat's spawn env sets both, and
//! test processes inherit them, so a test that connects ambiently speaks as
//! that rat — authenticating against the *live* fleet's derived token rather
//! than the temp layout's, and refused outright for every operator-only method
//! (`workflow.run`, `agent.spawn`, `ticket.update`, ...).
//!
//! The failure is invisible in an operator shell and total inside a rat, which
//! is the worst possible shape: `cargo test --workspace` cannot go green where
//! the completion protocol tells every rat to run it, so a rat cannot tell its
//! own breakage from the environment's. Tests must name their caller —
//! `Client::connect_as_operator`, or `Client::connect_as` for a specific agent.
//!
//! This is a source guard rather than a runtime one on purpose: the bug is that
//! ambient identity *reads correct* at the call site, so it has to be caught
//! where it is written.

use std::path::{Path, PathBuf};

/// This file necessarily quotes the pattern it bans.
const SELF: &str = "no_ambient_identity.rs";

/// Ambient-identity constructs that must not appear in an integration test.
const BANNED: &[(&str, &str)] = &[
    (
        "Client::connect(",
        "takes its caller from $RK_AGENT; use Client::connect_as_operator(..) \
         or Client::connect_as(.., agent)",
    ),
    (
        r#"env::var("RK_AGENT")"#,
        "reads the ambient agent identity; pass the name in explicitly",
    ),
    (
        r#"env::var("RK_AUTH_TOKEN")"#,
        "reads the ambient agent token; derive it from the test's own Layout",
    ),
];

fn workspace_root() -> PathBuf {
    // Resolved at runtime, not baked in via `env!("CARGO_MANIFEST_DIR")`: a
    // shared CARGO_TARGET_DIR can serve this binary unrecompiled to a
    // worktree other than the one it was compiled in
    // (TKT-01M0F0GHDPGA24X1TB24A0PZD0). Cargo sets a test binary's cwd to its
    // package's manifest directory on every run, so walking up from
    // `current_dir()` is always correct for the current process.
    let cwd = std::env::current_dir().expect("test process must have a current directory");
    cwd.ancestors()
        .find(|dir| dir.join("Cargo.toml").is_file() && dir.join("crates").is_dir())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| {
            panic!(
                "could not find the rat-kingdom workspace above runtime directory {}",
                cwd.display()
            )
        })
}

/// Every `crates/*/tests/*.rs` in the workspace.
fn integration_test_sources(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let crates = std::fs::read_dir(root.join("crates")).expect("crates dir");
    for krate in crates.flatten() {
        let tests = krate.path().join("tests");
        let Ok(entries) = std::fs::read_dir(&tests) else {
            continue; // a crate with no integration tests
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.ends_with(".rs") && name != SELF {
                found.push(path);
            }
        }
    }
    found.sort();
    found
}

#[test]
fn integration_tests_never_take_their_identity_from_the_environment() {
    let root = workspace_root();
    let sources = integration_test_sources(&root);
    assert!(
        // Well below the real count; this only catches a walk that found
        // nothing because the layout moved, which would pass vacuously.
        sources.len() > 10,
        "expected to scan the workspace's integration tests, found {}",
        sources.len()
    );

    let mut offences = Vec::new();
    for path in &sources {
        let text = std::fs::read_to_string(path).expect("read test source");
        for (line_no, line) in text.lines().enumerate() {
            for (pattern, why) in BANNED {
                if line.contains(pattern) {
                    let rel = path.strip_prefix(&root).unwrap_or(path);
                    offences.push(format!(
                        "{}:{}: `{pattern}` {why}",
                        rel.display(),
                        line_no + 1
                    ));
                }
            }
        }
    }

    assert!(
        offences.is_empty(),
        "integration tests must name their RPC caller explicitly (TKT-182):\n  {}",
        offences.join("\n  ")
    );
}
