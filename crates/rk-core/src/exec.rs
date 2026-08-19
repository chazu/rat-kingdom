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
    let mut child = Command::new(program)
        .args(args)
        .envs(envs)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
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
