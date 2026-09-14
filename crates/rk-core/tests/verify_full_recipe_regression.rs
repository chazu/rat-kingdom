//! Command-level regression coverage for the shared verify-full recipe
//! (scripts/verify-full.sh, wired into mise.toml's `[tasks.verify-full]` —
//! TKT-dagom-lajub-hijug; CI adoption is a separate protected-path change
//! tracked as TKT-sojuz-bogij-bapip). Runs the real script file against a
//! tiny broken fixture crate for each phase, rather than a
//! hand-copied command list, so deleting or reordering a phase in the
//! maintained recipe fails this test instead of leaving it green. Also
//! proves an earlier phase's failure actually stops the pipeline
//! (`set -euo pipefail` in the script) before a later phase runs, by
//! checking for the absence of a later phase's distinctive output.
//!
//! Every fixture is a throwaway crate (one lib file plus one integration
//! test file) built in its own tempdir with its own `CARGO_TARGET_DIR` —
//! never the workspace itself — to keep this fast and isolated from the
//! shared per-repo cargo target directory.
//!
//! [`OwnedProcessGroup`] is the one bit of process-control machinery this
//! file needs and no more: a bounded runner has to be able to kill a whole
//! subtree, not just the direct child, or a hung/backgrounded grandchild can
//! make the "timeout" path itself hang. It is deliberately not a reusable
//! abstraction — just enough RAII to make this file's own bounded runs safe.

use std::fs::File;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// Walks up from the test's current directory to find the workspace root,
/// identified by a `mise.toml` alongside the workspace `Cargo.toml`.
/// Deliberately runtime-resolved rather than baked from
/// `env!("CARGO_MANIFEST_DIR")` — see mise_verify_env.rs's `workspace_root`,
/// which does the same for the same reason (a byte-identical test binary can
/// be reused from a different, possibly reaped, worktree under a shared
/// `CARGO_TARGET_DIR`).
fn workspace_root() -> PathBuf {
    let cwd = std::env::current_dir().expect("test process must have a current directory");
    cwd.ancestors()
        .find(|dir| dir.join("Cargo.toml").is_file() && dir.join("mise.toml").is_file())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| {
            panic!(
                "could not find the rat-kingdom workspace above runtime directory {}",
                cwd.display()
            )
        })
}

fn write_fixture(dir: &Path, lib_rs: &str, it_rs: &str) {
    std::fs::create_dir_all(dir.join("src")).expect("create fixture src dir");
    std::fs::create_dir_all(dir.join("tests")).expect("create fixture tests dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"verify_full_fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .expect("write fixture Cargo.toml");
    std::fs::write(dir.join("src/lib.rs"), lib_rs).expect("write fixture lib.rs");
    std::fs::write(dir.join("tests/it.rs"), it_rs).expect("write fixture integration test");
}

struct RunResult {
    success: bool,
    combined_output: String,
}

enum RunOutcome {
    Completed(RunResult),
    TimedOut,
}

/// Owns a child spawned into its own process group (`process_group(0)`
/// makes its pgid equal its own pid) and guarantees the whole group is
/// killed and reaped on drop — on the normal-exit path, the timeout path,
/// and an early return or panic in between. A script that backgrounds a
/// descendant and exits (`foo &`) leaves that descendant in the same group
/// by default (job control that would split it into its own group is an
/// interactive-shell feature, off in a non-interactive script), so killing
/// the leader alone is not enough to guarantee cleanup.
struct OwnedProcessGroup {
    child: Child,
    pgid: i32,
}

impl OwnedProcessGroup {
    fn spawn(dir: &Path, program: &Path, stdout: File, stderr: File) -> Self {
        let child = Command::new(program)
            .current_dir(dir)
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .process_group(0)
            .spawn()
            .unwrap_or_else(|e| panic!("failed to start {}: {e}", program.display()));
        let pgid = child.id() as i32;
        Self { child, pgid }
    }
}

/// Bound for [`OwnedProcessGroup::drop`]'s post-SIGKILL confirmation poll:
/// generous relative to how fast a kernel actually tears down a killed
/// process, so this only ever matters if something is genuinely stuck.
const GROUP_TEARDOWN_TIMEOUT: Duration = Duration::from_secs(5);

impl Drop for OwnedProcessGroup {
    fn drop(&mut self) {
        // SAFETY: kill(2) with a negative pid signals every process in that
        // process group; ESRCH (the group is already gone) is expected on
        // the common path where everything already exited on its own and is
        // not an error worth surfacing here.
        unsafe {
            libc::kill(-self.pgid, libc::SIGKILL);
        }
        let _ = self.child.wait();
        // SIGKILL delivery and process teardown are asynchronous — the
        // syscall returning does not mean every group member has actually
        // been removed from the process table yet (confirmed empirically:
        // an immediate liveness check on a just-killed descendant can still
        // observe it "alive" for a brief window). Poll until the group is
        // confirmed empty instead of trusting the signal alone, bounded so a
        // genuinely stuck process can't hang teardown forever.
        let deadline = Instant::now() + GROUP_TEARDOWN_TIMEOUT;
        while Instant::now() < deadline {
            // SAFETY: signal 0 sends no signal, only probes whether any
            // process in the group still exists and is signalable by us.
            if unsafe { libc::kill(-self.pgid, 0) } == -1 {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

/// Runs `program` in `dir`, bounded by `timeout`. Output is redirected to
/// plain files rather than pipes and read back only after the process
/// (group) is confirmed dead, so a chatty phase or a descendant that
/// outlives the leader while holding stdout/stderr open can never make this
/// function block on a pipe that never reaches EOF — the historical failure
/// mode of a piped-plus-joined-thread runner. The process group is always
/// killed before returning, on every path, via [`OwnedProcessGroup`]'s drop.
fn run_bounded(dir: &Path, program: &Path, timeout: Duration) -> RunOutcome {
    let stdout_path = dir.join(".verify-full-stdout.log");
    let stderr_path = dir.join(".verify-full-stderr.log");
    let stdout_file = File::create(&stdout_path).expect("create stdout log file");
    let stderr_file = File::create(&stderr_path).expect("create stderr log file");
    let mut group = OwnedProcessGroup::spawn(dir, program, stdout_file, stderr_file);

    let start = Instant::now();
    let status = loop {
        if let Some(status) = group.child.try_wait().expect("poll child status") {
            break Some(status);
        }
        if start.elapsed() > timeout {
            break None;
        }
        thread::sleep(Duration::from_millis(25));
    };
    drop(group);

    let read_log = |path: &Path| std::fs::read_to_string(path).unwrap_or_default();
    let mut combined_output = read_log(&stdout_path);
    combined_output.push_str(&read_log(&stderr_path));

    match status {
        Some(status) => RunOutcome::Completed(RunResult {
            success: status.success(),
            combined_output,
        }),
        None => RunOutcome::TimedOut,
    }
}

/// Unwraps a [`RunOutcome`], panicking with a message that clearly names a
/// timeout as an infra problem rather than letting it read as an assertion
/// failure on `success`/`combined_output`.
fn expect_completed(outcome: RunOutcome, context: &str, timeout: Duration) -> RunResult {
    match outcome {
        RunOutcome::Completed(r) => r,
        RunOutcome::TimedOut => panic!(
            "{context}: did not exit within {timeout:?} — treat this as an infra hang to \
             investigate separately, not a command failure"
        ),
    }
}

/// Generous relative to the fixture's real cost (a single-function crate
/// with a warm registry cache finishes in low single-digit seconds).
const RECIPE_TIMEOUT: Duration = Duration::from_secs(180);

fn run_verify_full(dir: &Path) -> RunOutcome {
    let script = workspace_root().join("scripts/verify-full.sh");
    run_bounded(dir, &script, RECIPE_TIMEOUT)
}

fn is_alive(pid: i32) -> bool {
    // SAFETY: signal 0 sends no signal, it only checks whether `pid` exists
    // and is signalable by us — the standard portable liveness probe.
    unsafe { libc::kill(pid, 0) == 0 }
}

/// Polls [`is_alive`] until it reports dead or `timeout` elapses. A process
/// just sent SIGKILL is not necessarily gone from the process table the
/// instant the syscall returns (confirmed empirically), so a single
/// point-in-time liveness check is not a reliable "still running" signal —
/// this bounds how long we tolerate that asynchronous teardown before
/// treating it as a real failure to clean up.
fn wait_until_dead(pid: i32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while is_alive(pid) {
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(10));
    }
    true
}

const VALID_LIB: &str = "\
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

const VALID_IT: &str = "\
#[test]
fn integration_adds() {
    assert_eq!(verify_full_fixture::add(2, 2), 4);
}
";

const FMT_BROKEN_LIB: &str = "\
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

const CLIPPY_BROKEN_LIB: &str = "\
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

const INTEGRATION_BROKEN_IT: &str = "\
#[test]
fn integration_adds() {
    assert_eq!(verify_full_fixture::add(2, 2), 5);
}
";

const DOCTEST_BROKEN_LIB: &str = "\
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
fn verify_full_passes_a_fully_valid_fixture() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fixture(dir.path(), VALID_LIB, VALID_IT);
    let result = expect_completed(run_verify_full(dir.path()), "valid fixture", RECIPE_TIMEOUT);
    assert!(
        result.success,
        "verify-full.sh must pass a fixture with clean fmt/clippy and passing \
         unit/integration/doc tests. Output:\n{}",
        result.combined_output
    );
    assert!(
        result
            .combined_output
            .contains("Checking verify_full_fixture"),
        "expected the clippy phase to actually run on a healthy fixture. Output:\n{}",
        result.combined_output
    );
}

#[test]
fn fmt_violation_rejects_and_stops_before_build() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fixture(dir.path(), FMT_BROKEN_LIB, VALID_IT);
    let result = expect_completed(
        run_verify_full(dir.path()),
        "fmt-broken fixture",
        RECIPE_TIMEOUT,
    );
    assert!(
        !result.success,
        "unformatted source must fail verify-full.sh's fmt phase"
    );
    assert!(
        !result
            .combined_output
            .contains("Compiling verify_full_fixture"),
        "a failed fmt phase must stop the recipe (set -euo pipefail) before the build phase \
         ever runs. Output:\n{}",
        result.combined_output
    );
}

#[test]
fn clippy_violation_rejects_after_running_every_earlier_phase() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fixture(dir.path(), CLIPPY_BROKEN_LIB, VALID_IT);
    let result = expect_completed(
        run_verify_full(dir.path()),
        "clippy-broken fixture",
        RECIPE_TIMEOUT,
    );
    assert!(
        !result.success,
        "a clippy::needless_return violation must fail verify-full.sh"
    );
    assert!(
        result.combined_output.contains("needless_return"),
        "expected the failure to actually be the needless_return lint, not something else. \
         Output:\n{}",
        result.combined_output
    );
}

#[test]
fn integration_test_failure_rejects_and_stops_before_doctests() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fixture(dir.path(), VALID_LIB, INTEGRATION_BROKEN_IT);
    let result = expect_completed(
        run_verify_full(dir.path()),
        "integration-test-broken fixture",
        RECIPE_TIMEOUT,
    );
    assert!(
        !result.success,
        "a failing integration test must fail verify-full.sh's nextest phase"
    );
    assert!(
        !result
            .combined_output
            .contains("Doc-tests verify_full_fixture"),
        "a failed nextest phase must stop the recipe before the doctest phase ever runs. \
         Output:\n{}",
        result.combined_output
    );
}

#[test]
fn doctest_failure_rejects_and_stops_before_clippy() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fixture(dir.path(), DOCTEST_BROKEN_LIB, VALID_IT);
    let result = expect_completed(
        run_verify_full(dir.path()),
        "doctest-broken fixture",
        RECIPE_TIMEOUT,
    );
    assert!(
        !result.success,
        "a failing doctest must fail verify-full.sh's doctest phase — the exact category \
         `cargo nextest run` silently skips, which is why this step exists"
    );
    assert!(
        !result
            .combined_output
            .contains("Checking verify_full_fixture"),
        "a failed doctest phase must stop the recipe before the clippy phase ever runs. \
         Output:\n{}",
        result.combined_output
    );
}

#[test]
fn leader_exit_reaps_a_backgrounded_grandchild_before_the_timeout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let script_path = dir.path().join("backgrounds-a-child.sh");
    let pid_file = dir.path().join("child.pid");
    // A synthetic script, not verify-full.sh: it exists purely to exercise
    // OwnedProcessGroup's cleanup guarantee against a leader that exits
    // immediately while a grandchild it backgrounded (and never waited on)
    // is still alive in the same process group — exactly the shape of bug
    // that a piped-plus-joined-thread runner would hang on.
    std::fs::write(
        &script_path,
        format!(
            "#!/usr/bin/env bash\nsleep 100 &\necho $! > {}\nexit 0\n",
            pid_file.display()
        ),
    )
    .expect("write synthetic script");
    let mut perms = std::fs::metadata(&script_path)
        .expect("stat script")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&script_path, perms).expect("chmod script");

    let bound = Duration::from_secs(10);
    let start = Instant::now();
    let result = expect_completed(
        run_bounded(dir.path(), &script_path, bound),
        "process-group cleanup fixture",
        bound,
    );
    let elapsed = start.elapsed();

    assert!(
        result.success,
        "the leader script itself exits 0 immediately"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "the leader exits almost immediately; a working runner must not wait anywhere near \
         the backgrounded child's 100s sleep or the {bound:?} bound. Took {elapsed:?}"
    );

    let child_pid: i32 = std::fs::read_to_string(&pid_file)
        .expect("read backgrounded child's pid")
        .trim()
        .parse()
        .expect("parse child pid");
    // run_bounded already waited out its own drop-time teardown poll before
    // returning, so this should already be dead; wait_until_dead's bound
    // here is a short grace window for the kernel's own asynchronous
    // teardown, not a retry of anything this test's own code controls.
    assert!(
        wait_until_dead(child_pid, Duration::from_secs(2)),
        "the backgrounded grandchild (pid {child_pid}) must be reaped as part of the process \
         group when the leader exits, well before the {bound:?} bound — not left running past \
         this test"
    );
}
