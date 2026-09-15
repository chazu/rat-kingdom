//! Shared out-of-process execution primitive: spawn a program, hand it a
//! payload on stdin, bound its runtime, kill it if it overruns.
//!
//! Every rk integration point that hands work to an operator-configured
//! program — [`notify::sinks::CommandSink`](crate::notify::sinks::CommandSink)
//! today, castle/repo lifecycle hooks alongside it — goes through this one
//! path, so a wedge/leak fix lands once instead of once per caller.

use std::collections::BTreeMap;
use std::io::Write;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

/// Mark every file descriptor above stderr that isn't already
/// close-on-exec as close-on-exec, immediately before `exec` — the fix for
/// TKT-bikuz-kumuz-zutit's leaked-pipe hang.
///
/// On a platform without `pipe2` (macOS, where this was diagnosed), a pipe
/// created for a child's captured stdout/stderr is born without
/// close-on-exec: the OS hands back the fds via a plain `pipe()`, and only
/// *after that* does the caller `fcntl(F_SETFD, FD_CLOEXEC)` each end
/// (`std::sys::pipe::unix::pipe`). If some OTHER thread's `fork`/`exec`
/// lands in that window, its child inherits a live copy of the still-open
/// pipe fd, whether or not that pipe has anything to do with the process
/// being launched. `TKT-bikuz-kumuz-zutit` caught this for real: 32
/// concurrent agent spawns raced a `git` invocation's captured-output pipe
/// against a fake harness's own launch, and the harness's shelled-out
/// `rk done` child ended up holding a duplicate of git's pipe write end
/// open. Git exited and went defunct; the daemon's blocking read of the
/// pipe's read end for EOF never returned, because that unrelated harness
/// child was still alive and still holding the write end.
///
/// A lock serializing our own spawns against each other was the first
/// approach tried here, but it only closes the window for call sites that
/// opt in — any other pipe-creating spawn anywhere in the process (a
/// third-party crate, a future call site nobody remembered to wire up)
/// still races every guarded one. Scrubbing the CHILD's fd table instead is
/// complete without an audit: it acts on an inherited descriptor regardless
/// of which unrelated operation raced its creation, because it acts on the
/// receiving end, not the creating end.
///
/// Delegates to [`close_fds::set_fds_cloexec`] rather than a hand-rolled
/// scan. Two hand-rolled drafts were tried and rejected here first: a
/// numeric range bounded by `RLIMIT_NOFILE` costs one `fcntl` per fd number
/// in the range regardless of how many fds are actually open — measured at
/// ~210ms per spawn against a host with a large soft limit and an unlimited
/// hard limit, on what is a per-`git`-call hot path — and it is wrong
/// besides, since unprivileged code can lower either limit below an
/// already-open fd without closing it, so the "bound" it scans to doesn't
/// actually bound where open fds can be. A parent-side `/dev/fd` snapshot,
/// captured before this `Command`'s own `fork`, fixed both of those but
/// reopened the exact race this function exists to close for anything
/// opened between that snapshot and the fork itself. `close_fds` avoids all
/// three problems: its enumeration runs from *inside* `pre_exec` — after
/// this spawn's own fork, not before it — using `close_range` on Linux and
/// `getdirentries` on macOS (both proportional to fds actually open, not to
/// any rlimit), so there is no separate snapshot to go stale and no
/// rlimit-sized scan. Its own docs describe it as async-signal-safe on
/// Linux, macOS/iOS, the BSDs, and Solaris/Illumos, and `set_fds_cloexec`
/// specifically (unlike its `closefrom`/`close_open_fds` siblings, which
/// close outright and carry a `# Safety` warning about concurrent fd use)
/// only ever *sets* `FD_CLOEXEC` — it is not `unsafe`, and it cannot
/// interfere with a fd that's already marked.
///
/// This runs as a `pre_exec` closure. Confirmed against the shipped
/// `std::sys::process::unix::unix::do_exec` (rustc 1.95.0): the child's own
/// stdin/stdout/stderr are already `dup2`'d onto fds 0/1/2 *before*
/// `pre_exec` closures run, and `pre_exec` runs last, immediately before
/// `execve` — so this cannot disturb the streams the caller wired up, and
/// nothing this codebase spawns passes any fd beyond 0/1/2 on purpose.
/// Because `set_fds_cloexec` only ever sets the flag, never closes: std's
/// own exec-error-reporting pipe (created before `fork`, deliberately
/// marked `FD_CLOEXEC` already so it vanishes silently on a *successful*
/// exec, but must stay open and valid up to the exec attempt itself to
/// carry a failure) is left alone regardless of whether it happens to be
/// above fd 2 — an earlier draft that closed unconditionally instead of
/// marking broke exactly this, and the missing-executable/permission-denied
/// tests below exist to catch a regression back to that.
#[cfg(unix)]
pub fn close_extra_fds(cmd: &mut Command) {
    use std::os::unix::process::CommandExt;
    unsafe {
        cmd.pre_exec(|| {
            close_fds::set_fds_cloexec(3, &[]);
            Ok(())
        });
    }
}

/// How often a bounded wait polls the child. Small enough that a fast script
/// does not visibly stall the caller, large enough not to spin.
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Spawn `program` with `args` on argv and `envs` merged into the
/// environment, write `stdin_payload` to its stdin, then wait up to `timeout`
/// before killing it. Returns the child's exit status — a non-zero status is
/// not itself an `Err`; callers that treat a non-zero exit as failure check
/// `ExitStatus::success()` themselves.
///
/// Output is always discarded (`Stdio::null()`): a piped stream left
/// undrained while polling `try_wait` deadlocks the moment the child fills
/// its pipe buffer, which is exactly the hang the timeout exists to prevent,
/// arriving through the back door. A program that wants its diagnostics kept
/// should log them itself.
///
/// Writing `stdin_payload` is best-effort: a program that ignores stdin
/// closes the pipe, and a broken-pipe write error here is normal, not
/// reported.
pub fn run_piped(
    program: &str,
    args: &[String],
    envs: &BTreeMap<String, String>,
    stdin_payload: &[u8],
    timeout: Duration,
) -> crate::Result<ExitStatus> {
    let mut cmd = Command::new(program);
    cmd.args(args)
        .envs(envs)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    close_extra_fds(&mut cmd);
    let mut child = cmd
        .spawn()
        .map_err(|e| crate::Error::other(format!("could not run `{program}`: {e}")))?;

    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(stdin_payload);
    }

    wait_bounded(&mut child, timeout).map_err(|e| crate::Error::other(format!("`{program}` {e}")))
}

/// Wait for `child`, killing it past `timeout`. `std::process::Child` has no
/// timed wait, and an unbounded one on a reactor-driven dispatch path is how
/// a wedged program stalls dispatch for everything behind it.
fn wait_bounded(child: &mut Child, timeout: Duration) -> crate::Result<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(crate::Error::other(format!(
                "timed out after {}s and was killed",
                timeout.as_secs()
            )));
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn script(dir: &std::path::Path, name: &str, body: &str) -> String {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[cfg(unix)]
    #[test]
    fn run_piped_hands_over_argv_env_and_stdin() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        let program = script(
            dir.path(),
            "collect",
            &format!(
                r#"{{ echo "argv1=$1"; echo "env=$RK_TEST_VAR"; echo "stdin=$(cat)"; }} > {}"#,
                out.display()
            ),
        );
        let envs = BTreeMap::from([("RK_TEST_VAR".to_string(), "hi".to_string())]);
        let status = run_piped(
            &program,
            &["hello".to_string()],
            &envs,
            b"payload",
            Duration::from_secs(5),
        )
        .unwrap();
        assert!(status.success());
        let got = std::fs::read_to_string(&out).unwrap();
        assert!(got.contains("argv1=hello"), "{got}");
        assert!(got.contains("env=hi"), "{got}");
        assert!(got.contains("stdin=payload"), "{got}");
    }

    #[cfg(unix)]
    #[test]
    fn run_piped_reports_a_nonzero_exit_via_status_not_err() {
        let dir = tempfile::tempdir().unwrap();
        let program = script(dir.path(), "broken", "exit 3");
        let status =
            run_piped(&program, &[], &BTreeMap::new(), b"", Duration::from_secs(5)).unwrap();
        assert!(!status.success());
        assert_eq!(status.code(), Some(3));
    }

    #[test]
    fn a_missing_program_is_an_error_not_a_panic() {
        let err = run_piped(
            "/nonexistent/rk-exec-nowhere",
            &[],
            &BTreeMap::new(),
            b"",
            Duration::from_secs(5),
        )
        .expect_err("an uninstalled program reports failure, it does not panic");
        assert!(err.to_string().contains("could not run"), "{err}");
    }

    /// Opens a real, deliberately non-close-on-exec pipe — the exact state a
    /// pipe is briefly in between `pipe()` and `fcntl(F_SETFD, FD_CLOEXEC)`
    /// on a platform without `pipe2` (macOS, where TKT-bikuz-kumuz-zutit was
    /// diagnosed) — and confirms it as an OS fact, not an assumption: a
    /// plain child spawned while it is open really does inherit it.
    #[cfg(unix)]
    fn leaky_pipe() -> (i32, i32) {
        let mut fds = [0i32; 2];
        assert_eq!(
            unsafe { libc::pipe(fds.as_mut_ptr()) },
            0,
            "pipe(2) failed: {}",
            std::io::Error::last_os_error()
        );
        (fds[0], fds[1])
    }

    /// Real OS-process check: does a freshly spawned child see `fd` open?
    /// `/dev/fd/<n>` exists iff the calling process currently holds that fd —
    /// this is exactly the mechanism `lsof` used to catch the original bug.
    #[cfg(unix)]
    fn child_sees_fd(cmd: &mut Command, fd: i32) -> bool {
        let out = cmd
            .arg("-c")
            .arg(format!("test -e /dev/fd/{fd}"))
            .status()
            .expect("failed to invoke sh");
        out.success()
    }

    #[cfg(unix)]
    #[test]
    fn an_unguarded_leaked_fd_really_does_survive_a_concurrent_spawn() {
        let (leak_r, leak_w) = leaky_pipe();
        assert!(
            child_sees_fd(&mut Command::new("sh"), leak_w),
            "test setup invalid: a plain non-cloexec fd must be visible to a concurrently spawned child"
        );
        unsafe {
            libc::close(leak_r);
            libc::close(leak_w);
        }
    }

    /// The actual fix, proven with the same real fd/real child/real
    /// `/dev/fd` methodology as the control test above: a child launched
    /// through [`close_extra_fds`] must NOT see an unrelated descriptor that
    /// was open in the parent when it was spawned, while its own intended
    /// stdio still works normally.
    #[cfg(unix)]
    #[test]
    fn close_extra_fds_hides_an_unrelated_descriptor_from_the_child() {
        let (leak_r, leak_w) = leaky_pipe();

        let mut cmd = Command::new("sh");
        close_extra_fds(&mut cmd);
        let leaked = child_sees_fd(&mut cmd, leak_w);

        unsafe {
            libc::close(leak_r);
            libc::close(leak_w);
        }
        assert!(
            !leaked,
            "a guarded child observed a descriptor it was never given"
        );
    }

    /// Deterministic barrier proving the guard acts at the actual `exec`
    /// boundary, not against a snapshot taken when the `Command` was
    /// configured: configure the guard FIRST, only THEN open the
    /// descriptor, and only THEN launch. A parent-side pre-scan (an earlier
    /// draft of this function) is built and captured at the
    /// `close_extra_fds` call and cannot see an fd that doesn't exist yet;
    /// it would fail this test. The real fix enumerates from inside
    /// `pre_exec`, strictly after this exact spawn's own `fork` and after
    /// the descriptor below already exists, so ordering it this way is not
    /// a weaker case than the test above — it's the one that would catch a
    /// regression back to snapshot-before-fork.
    #[cfg(unix)]
    #[test]
    fn close_extra_fds_catches_a_descriptor_opened_after_the_command_was_configured() {
        let mut cmd = Command::new("sh");
        close_extra_fds(&mut cmd);
        let (leak_r, leak_w) = leaky_pipe();

        let leaked = child_sees_fd(&mut cmd, leak_w);

        unsafe {
            libc::close(leak_r);
            libc::close(leak_w);
        }
        assert!(
            !leaked,
            "a guarded child observed a descriptor opened after the command was configured, \
             not just before it — the guard must act at exec time, not at configure time"
        );
    }

    /// Reproduces the exact gap an rlimit-bounded numeric scan has: an fd
    /// already open, then the child's `RLIMIT_NOFILE` dropped below that
    /// fd's number before the scan runs — ordinary, privilege-free
    /// `setrlimit`, not a pathological condition, and one that can lower
    /// EITHER the soft or the hard limit (unprivileged code can always
    /// lower its own hard limit too). A scan bounded by any current rlimit
    /// value would stop short of the leaked fd and miss it entirely.
    /// [`open_fds_above`] doesn't consult `RLIMIT_NOFILE` at all — it reads
    /// which fds are actually open — so it isn't affected by this rlimit
    /// change regardless of which limit it touches or how low. The rlimit
    /// change happens in a `pre_exec` closure chained BEFORE
    /// `close_extra_fds`'s own, so it runs in the forked child only — this
    /// test's own process (and its concurrently running sibling tests)
    /// never has its rlimit touched.
    #[cfg(unix)]
    #[test]
    fn close_extra_fds_survives_a_lowered_soft_limit_above_an_open_fd() {
        let (leak_r, leak_w) = leaky_pipe();
        // Computed from the actual leaked fd rather than a hardcoded
        // constant: however many fds this test binary happens to already
        // have open, the new soft limit must land strictly below `leak_w`.
        let lowered_cur = (leak_w - 1).max(3) as libc::rlim_t;

        let mut cmd = Command::new("sh");
        unsafe {
            use std::os::unix::process::CommandExt as _;
            cmd.pre_exec(move || {
                // Lower only the soft limit; leave the hard limit (whatever
                // it already is) untouched. Setting `rlim_max` to a made-up
                // value here would fail with EPERM if it happened to be
                // below whatever this process actually inherited.
                let mut lim: libc::rlimit = std::mem::zeroed();
                if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                lim.rlim_cur = lowered_cur;
                if libc::setrlimit(libc::RLIMIT_NOFILE, &lim) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        close_extra_fds(&mut cmd);
        let leaked = child_sees_fd(&mut cmd, leak_w);

        unsafe {
            libc::close(leak_r);
            libc::close(leak_w);
        }
        assert!(
            !leaked,
            "a descriptor opened above a since-lowered soft limit must still be caught"
        );
    }

    /// A numeric-range scan bounded by `RLIMIT_NOFILE` cost ~210ms per
    /// spawn on a host with a large soft limit and an unlimited hard
    /// limit — measured against an earlier draft of this function, on a
    /// per-`git`-call hot path. [`close_extra_fds`] must stay proportional
    /// to the actual (small) number of open fds, not to any rlimit value;
    /// this bounds a guarded spawn at a small constant multiple of an
    /// unguarded one rather than pinning an exact number, since the
    /// baseline itself is host- and load-dependent.
    #[cfg(unix)]
    #[test]
    fn close_extra_fds_spawn_cost_stays_proportional_to_open_fds_not_rlimit() {
        let plain_started = Instant::now();
        for _ in 0..20 {
            Command::new("true").status().unwrap();
        }
        let plain = plain_started.elapsed();

        let guarded_started = Instant::now();
        for _ in 0..20 {
            let mut cmd = Command::new("true");
            close_extra_fds(&mut cmd);
            cmd.status().unwrap();
        }
        let guarded = guarded_started.elapsed();

        assert!(
            guarded < plain * 10 + Duration::from_millis(200),
            "close_extra_fds must not turn every spawn into a large, rlimit-sized scan: \
             plain={plain:?} guarded={guarded:?}"
        );
    }

    /// Intended streams/control must still work after guarding: stdin,
    /// stdout, and the exit status all have to survive `close_extra_fds`
    /// untouched, or the fix would trade one production incident for
    /// another.
    #[cfg(unix)]
    #[test]
    fn close_extra_fds_preserves_stdio_and_exit_status() {
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg("read line; printf 'echo:%s' \"$line\"; exit 7")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        close_extra_fds(&mut cmd);
        let mut child = cmd.spawn().unwrap();
        use std::io::Write as _;
        child.stdin.take().unwrap().write_all(b"hello\n").unwrap();
        let out = child.wait_with_output().unwrap();
        assert_eq!(out.status.code(), Some(7));
        assert_eq!(String::from_utf8_lossy(&out.stdout), "echo:hello");
    }

    /// The exact regression a naive "close everything" implementation hit:
    /// a missing program must still fail `spawn()` itself with a normal
    /// `NotFound` error, not silently succeed and let the child abort. This
    /// is std's own exec-error-reporting pipe surviving `close_extra_fds`.
    #[cfg(unix)]
    #[test]
    fn close_extra_fds_still_reports_a_missing_program_as_spawn_err() {
        let mut cmd = Command::new("/nonexistent/rk-exec-nowhere-missing");
        close_extra_fds(&mut cmd);
        let err = cmd
            .spawn()
            .expect_err("a missing program must fail spawn(), not abort the child");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound, "{err}");
    }

    /// Same failure-reporting guarantee for a non-executable target: a
    /// permission failure at exec must still surface as a normal `spawn()`
    /// error through std's error pipe, not an aborted child.
    #[cfg(unix)]
    #[test]
    fn close_extra_fds_still_reports_a_non_executable_target_as_spawn_err() {
        let dir = tempfile::tempdir().unwrap();
        let not_executable = dir.path().join("not-executable");
        std::fs::write(&not_executable, b"not a script").unwrap();
        // Deliberately no execute bit.
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&not_executable, std::fs::Permissions::from_mode(0o600)).unwrap();

        let mut cmd = Command::new(&not_executable);
        close_extra_fds(&mut cmd);
        let err = cmd
            .spawn()
            .expect_err("a non-executable target must fail spawn(), not abort the child");
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied, "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn a_wedged_program_is_killed_at_its_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let program = script(dir.path(), "hang", "sleep 60");
        let started = Instant::now();
        let err = run_piped(&program, &[], &BTreeMap::new(), b"", Duration::from_secs(1))
            .expect_err("a hung child must not win");
        assert!(err.to_string().contains("timed out"), "{err}");
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the caller is not held hostage by a wedged program"
        );
    }
}
