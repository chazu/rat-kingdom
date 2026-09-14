//! Command-level regression coverage for `mise.toml`'s `[tasks.verify-full]`
//! recipe (TKT-dagom-lajub-hijug): proves the actual command used in each
//! phase still rejects a broken fixture, so a future edit to the recipe
//! cannot silently turn a phase into a no-op (this is exactly how the
//! nextest switch could otherwise drop doctest coverage — nextest does not
//! run doctests at all, see https://nexte.st/docs/running/).
//!
//! Every fixture is a throwaway single-file crate built in its own tempdir
//! with its own `CARGO_TARGET_DIR` — never the workspace itself — to keep
//! this fast, isolated from the shared per-repo cargo target directory, and
//! free of any need to rebuild the real workspace per case.

use std::fs;
use std::path::Path;
use std::process::Command;

fn write_fixture(dir: &Path, lib_rs: &str) {
    fs::create_dir_all(dir.join("src")).expect("create fixture src dir");
    fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"verify_full_fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .expect("write fixture Cargo.toml");
    fs::write(dir.join("src/lib.rs"), lib_rs).expect("write fixture lib.rs");
}

/// Runs `program args...` in `dir` with an isolated `CARGO_TARGET_DIR`,
/// returning whether it exited successfully.
fn run(dir: &Path, program: &str, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .current_dir(dir)
        .env("CARGO_TARGET_DIR", dir.join("target"))
        .status()
        .unwrap_or_else(|e| panic!("failed to spawn `{program} {args:?}`: {e}"))
        .success()
}

const VALID: &str = "\
/// Adds two numbers.
///
/// ```
/// assert_eq!(verify_full_fixture::add(2, 2), 4);
/// ```
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[cfg(test)]
mod tests {
    use super::add;

    #[test]
    fn adds() {
        assert_eq!(add(2, 2), 4);
    }
}
";

const FMT_BROKEN: &str = "\
/// Adds two numbers.
///
/// ```
/// assert_eq!(verify_full_fixture::add(2, 2), 4);
/// ```
pub fn add(a:i32,b:i32)->i32{
    a+b
}

#[cfg(test)]
mod tests {
    use super::add;

    #[test]
    fn adds() {
        assert_eq!(add(2, 2), 4);
    }
}
";

const CLIPPY_BROKEN: &str = "\
/// Adds two numbers.
///
/// ```
/// assert_eq!(verify_full_fixture::add(2, 2), 4);
/// ```
pub fn add(a: i32, b: i32) -> i32 {
    return a + b;
}

#[cfg(test)]
mod tests {
    use super::add;

    #[test]
    fn adds() {
        assert_eq!(add(2, 2), 4);
    }
}
";

const UNIT_TEST_BROKEN: &str = "\
/// Adds two numbers.
///
/// ```
/// assert_eq!(verify_full_fixture::add(2, 2), 4);
/// ```
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[cfg(test)]
mod tests {
    use super::add;

    #[test]
    fn adds() {
        assert_eq!(add(2, 2), 5);
    }
}
";

const DOCTEST_BROKEN: &str = "\
/// Adds two numbers.
///
/// ```
/// assert_eq!(verify_full_fixture::add(2, 2), 5);
/// ```
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

#[cfg(test)]
mod tests {
    use super::add;

    #[test]
    fn adds() {
        assert_eq!(add(2, 2), 4);
    }
}
";

#[test]
fn fmt_check_rejects_unformatted_source_but_passes_clean_source() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fixture(dir.path(), VALID);
    assert!(
        run(dir.path(), "cargo", &["fmt", "--all", "--check"]),
        "verify-full's `cargo fmt --all --check` must pass already-formatted source"
    );

    write_fixture(dir.path(), FMT_BROKEN);
    assert!(
        !run(dir.path(), "cargo", &["fmt", "--all", "--check"]),
        "verify-full's `cargo fmt --all --check` must reject unformatted source"
    );
}

#[test]
fn clippy_deny_warnings_rejects_a_lint_violation_but_passes_clean_source() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fixture(dir.path(), VALID);
    assert!(
        run(
            dir.path(),
            "cargo",
            &["clippy", "--all-targets", "--", "-D", "warnings"]
        ),
        "verify-full's clippy step must pass lint-clean source"
    );

    write_fixture(dir.path(), CLIPPY_BROKEN);
    assert!(
        !run(
            dir.path(),
            "cargo",
            &["clippy", "--all-targets", "--", "-D", "warnings"]
        ),
        "verify-full's clippy step must reject a clippy::needless_return violation"
    );
}

#[test]
fn nextest_rejects_a_failing_unit_test_but_passes_a_passing_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fixture(dir.path(), VALID);
    assert!(
        run(
            dir.path(),
            "cargo",
            &["nextest", "run", "--no-fail-fast", "--test-threads", "4"]
        ),
        "verify-full's `cargo nextest run` must pass a passing unit test"
    );

    write_fixture(dir.path(), UNIT_TEST_BROKEN);
    assert!(
        !run(
            dir.path(),
            "cargo",
            &["nextest", "run", "--no-fail-fast", "--test-threads", "4"]
        ),
        "verify-full's `cargo nextest run` must reject a failing unit test"
    );
}

#[test]
fn workspace_doc_tests_reject_a_failing_doctest_but_pass_a_passing_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fixture(dir.path(), VALID);
    assert!(
        run(
            dir.path(),
            "cargo",
            &["test", "--doc", "--", "--test-threads", "4"]
        ),
        "verify-full's `cargo test --doc` must pass a passing doctest"
    );

    write_fixture(dir.path(), DOCTEST_BROKEN);
    assert!(
        !run(
            dir.path(),
            "cargo",
            &["test", "--doc", "--", "--test-threads", "4"]
        ),
        "verify-full's `cargo test --doc` must reject a failing doctest — the exact \
         category `cargo nextest run` silently skips, which is why this step exists"
    );
}
