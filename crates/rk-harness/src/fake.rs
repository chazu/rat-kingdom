//! A scriptable harness for integration tests and dry runs.
//!
//! Runs the command in `RK_FAKE_HARNESS_CMD` (default: a bash one-liner that
//! echoes a canned Claude-style stream), parsed with the same parser as the
//! Claude adapter — so daemon plumbing is exercised end-to-end without
//! spending tokens.

use crate::{claude, runner, ControlEnvelope, Harness, HarnessCaps, HarnessSession, LaunchSpec};
use tokio::process::Command;

pub struct FakeHarness;

const DEFAULT_SCRIPT: &str = r#"
echo '{"type":"system","subtype":"init","session_id":"fake-session-1"}'
read -r _first_message
echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"fake rat reporting for duty"}],"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"fake work complete","session_id":"fake-session-1","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

impl Harness for FakeHarness {
    fn kind(&self) -> &'static str {
        "fake"
    }

    fn caps(&self) -> HarnessCaps {
        HarnessCaps {
            steer: true,
            interrupt: true,
            resume: false,
            reports_cost_usd: true,
            native_budget: false,
        }
    }

    fn launch(&self, spec: &LaunchSpec) -> rk_core::Result<HarnessSession> {
        // Per-launch script via spec.env beats the process env var (which is
        // racy when parallel tests share the process).
        let script = spec
            .env
            .get("RK_FAKE_HARNESS_CMD")
            .cloned()
            .or_else(|| std::env::var("RK_FAKE_HARNESS_CMD").ok())
            .unwrap_or_else(|| DEFAULT_SCRIPT.to_string());
        let mut cmd = Command::new("bash");
        cmd.args(["-c", &script]);
        cmd.current_dir(&spec.cwd);
        cmd.envs(&spec.env);
        cmd.env("RK_FAKE_PROMPT", &spec.prompt);
        // Expose the composed system prompt (role priming) to the fake script,
        // symmetric with RK_FAKE_PROMPT — lets tests assert on what a spawned
        // rat is actually primed with (e.g. injected standing conventions).
        cmd.env(
            "RK_FAKE_SYSTEM_PROMPT",
            spec.system_prompt.as_deref().unwrap_or_default(),
        );

        let mut session = runner::launch(runner::Wiring {
            command: cmd,
            parse: Box::new(claude::parse_event_line),
            steer_line: Some(|envelope: &ControlEnvelope| {
                serde_json::json!({"type": "rk_control", "control": envelope}).to_string()
            }),
            resume: None,
        })?;

        let prompt = spec.prompt.clone();
        let control = session.control.clone();
        tokio::spawn(async move {
            let _ = control.steer(&prompt).await;
        });
        session.pid = session.pid.or(None);
        Ok(session)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HarnessEvent;
    use std::time::Duration;

    #[tokio::test]
    async fn fake_harness_runs_the_full_event_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let spec = LaunchSpec {
            prompt: "do the thing".into(),
            cwd: dir.path().to_path_buf(),
            ..Default::default()
        };
        let mut session = FakeHarness.launch(&spec).unwrap();

        let mut started = false;
        let mut completed = false;
        let mut exited = false;
        while let Some(event) = session.events.recv().await {
            match event {
                HarnessEvent::Started { session_id } => {
                    assert_eq!(session_id.as_deref(), Some("fake-session-1"));
                    started = true;
                }
                HarnessEvent::Completed {
                    result,
                    is_error,
                    cost_usd,
                    ..
                } => {
                    assert_eq!(result, "fake work complete");
                    assert!(!is_error);
                    assert_eq!(cost_usd, Some(0.001));
                    completed = true;
                }
                HarnessEvent::Exited { code } => {
                    assert_eq!(code, Some(0));
                    exited = true;
                }
                _ => {}
            }
        }
        assert!(started && completed && exited);
    }

    #[tokio::test]
    async fn interrupt_terminates_a_hung_fake() {
        let dir = tempfile::tempdir().unwrap();
        // A fake that never finishes; script via spec.env (no process-global
        // state, so parallel tests cannot race).
        let mut env = std::collections::HashMap::new();
        env.insert("RK_FAKE_HARNESS_CMD".to_string(), "sleep 300".to_string());
        let mut session = FakeHarness
            .launch(&LaunchSpec {
                cwd: dir.path().to_path_buf(),
                env,
                ..Default::default()
            })
            .unwrap();

        session.control.kill().await.unwrap();
        let mut saw_exit = false;
        while let Some(event) = session.events.recv().await {
            if let HarnessEvent::Exited { code } = event {
                assert_ne!(code, Some(0), "killed, not clean exit");
                saw_exit = true;
            }
        }
        assert!(saw_exit);
    }

    /// A starved/misconfigured harness that writes nothing to stdout (no
    /// `Started`/`Completed` — exactly the silent zero-token death this exists
    /// to diagnose) still surfaces what it said on stderr.
    #[tokio::test]
    async fn stderr_lines_are_captured_as_events() {
        let dir = tempfile::tempdir().unwrap();
        let mut env = std::collections::HashMap::new();
        env.insert(
            "RK_FAKE_HARNESS_CMD".to_string(),
            "echo 'rate limited, retrying' >&2; echo 'boom, auth expired' >&2".to_string(),
        );
        let mut session = FakeHarness
            .launch(&LaunchSpec {
                cwd: dir.path().to_path_buf(),
                env,
                ..Default::default()
            })
            .unwrap();

        let mut stderr_lines = Vec::new();
        let mut exited = false;
        while let Some(event) = session.events.recv().await {
            match event {
                HarnessEvent::Stderr { text } => stderr_lines.push(text),
                HarnessEvent::Exited { .. } => exited = true,
                _ => {}
            }
        }
        // A real shell can interleave incidental stderr (e.g. a locale
        // warning) around the fixture's own lines, so assert the fixture
        // lines are present and in order rather than the whole vector being
        // an exact match.
        let first = stderr_lines
            .iter()
            .position(|l| l == "rate limited, retrying")
            .expect("first fixture line present");
        let second = stderr_lines
            .iter()
            .position(|l| l == "boom, auth expired")
            .expect("second fixture line present");
        assert!(
            first < second,
            "fixture lines out of order: {stderr_lines:?}"
        );
        assert!(exited);
    }

    /// TKT-bikuz-kumuz-zutit's production-boundary regression: `FakeHarness::
    /// launch` is the exact same entry point (`Harness::launch` ->
    /// `runner::launch`) every real harness (Claude, Codex, jcode, maki)
    /// goes through, so this exercises the actual wiring rather than
    /// `rk_core::exec::close_extra_fds` in isolation. A pipe deliberately
    /// left open (no `FD_CLOEXEC`) in THIS process stands in for the
    /// original incident's `git` invocation, caught mid-`pipe()`-then-
    /// `fcntl()` by a concurrent, unrelated spawn — here, this exact
    /// harness launch. The spawned bash process must not see it.
    #[cfg(unix)]
    #[tokio::test]
    async fn fake_harness_launch_does_not_inherit_an_unrelated_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("fd-check");

        let mut fds = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let (leak_r, leak_w) = (fds[0], fds[1]);

        let mut env = std::collections::HashMap::new();
        env.insert(
            "RK_FAKE_HARNESS_CMD".to_string(),
            format!(
                "if test -e /dev/fd/{leak_w}; then echo leaked > {marker}; else echo clean > {marker}; fi",
                marker = shell_escape(&marker),
            ),
        );
        let mut session = FakeHarness
            .launch(&LaunchSpec {
                cwd: dir.path().to_path_buf(),
                env,
                ..Default::default()
            })
            .unwrap();
        while session.events.recv().await.is_some() {}

        unsafe {
            libc::close(leak_r);
            libc::close(leak_w);
        }

        let seen = std::fs::read_to_string(&marker).unwrap();
        assert_eq!(
            seen.trim(),
            "clean",
            "the real FakeHarness::launch path leaked an unrelated parent descriptor into the spawned harness"
        );
    }

    /// Companion to the leak test above, reproducing the ORIGINAL incident's
    /// exact shape rather than a scenario the fix would pass trivially: a
    /// real `git` child's captured stdout, wired to a pipe this test still
    /// owns a write-end copy of, with an unrelated harness launched WHILE
    /// that copy is still open — the only moment a leak into the harness
    /// could actually happen. Launching the harness first (an earlier draft
    /// of this test did) creates no such window: the harness has already
    /// fully exec'd before the pipe even exists, so it passes whether or not
    /// the fix works. The harness is parked on a release-file busy-wait
    /// (not a fixed `sleep`) so "still alive at the EOF check" is verified
    /// via its actual pid, not inferred from a timing coincidence, and it is
    /// released and reaped in every outcome — including a failed assertion
    /// — so this test can't leave it running past itself.
    #[cfg(unix)]
    #[tokio::test]
    async fn captured_output_reaches_eof_while_an_unrelated_harness_stays_alive() {
        use std::os::unix::io::FromRawFd;

        let dir = tempfile::tempdir().unwrap();
        let repo_dir = dir.path().join("repo");
        std::fs::create_dir(&repo_dir).unwrap();
        for args in [
            &["init", "-b", "main"][..],
            &["config", "user.email", "rat@example.com"],
            &["config", "user.name", "Rat"],
        ] {
            assert!(std::process::Command::new("git")
                .arg("-C")
                .arg(&repo_dir)
                .args(args)
                .status()
                .unwrap()
                .success());
        }
        std::fs::write(repo_dir.join("f"), "x").unwrap();
        for args in [&["add", "."][..], &["commit", "-m", "init"]] {
            assert!(std::process::Command::new("git")
                .arg("-C")
                .arg(&repo_dir)
                .args(args)
                .status()
                .unwrap()
                .success());
        }

        let mut fds = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let (r, w) = (fds[0], fds[1]);

        // A real `git` child, its stdout wired directly to `w` — the exact
        // captured-output shape `git_in` produces (see rk-git), reproduced
        // with the real binary. `close_extra_fds` here mirrors production;
        // it must not disturb the explicit stdout wiring below (fd 1 after
        // its own dup2), only fds it didn't set up on purpose.
        let w_for_git = unsafe { libc::dup(w) };
        assert!(w_for_git >= 0);
        let mut git_cmd = std::process::Command::new("git");
        git_cmd.args(["-C", repo_dir.to_str().unwrap(), "log", "--oneline"]);
        unsafe {
            git_cmd.stdout(std::process::Stdio::from_raw_fd(w_for_git));
        }
        rk_core::exec::close_extra_fds(&mut git_cmd);
        let mut git_child = git_cmd.spawn().unwrap();
        // `Command` itself still owns `w_for_git` after `spawn()`: for a
        // `Stdio::Fd` above fd 2, std passes it to the child as a bare raw
        // fd number to `dup2` (`ChildStdio::Explicit`), never taking
        // ownership, so `git_cmd`'s own `stdout` field keeps it open in
        // THIS process until `git_cmd` is dropped. Left alive, that is a
        // second, self-inflicted writer on `w` that would keep it "open"
        // long after the real `w` is closed below — not a leak into any
        // other process, just this test failing to release its own handle.
        drop(git_cmd);

        // The harness is launched HERE, while this test still holds its own
        // copy of `w` open — the only window in which a leak into the
        // harness's spawned bash process could occur.
        let release = dir.path().join("release");
        let diag = dir.path().join("diag");
        let mut env = std::collections::HashMap::new();
        env.insert(
            "RK_FAKE_HARNESS_CMD".to_string(),
            format!(
                "if test -e /dev/fd/{w}; then echo leaked > {diag}; else echo clean > {diag}; fi; while [ ! -f {release} ]; do sleep 0.05; done",
                diag = shell_escape(&diag),
                release = shell_escape(&release)
            ),
        );
        let mut session = FakeHarness
            .launch(&LaunchSpec {
                cwd: dir.path().to_path_buf(),
                env,
                ..Default::default()
            })
            .unwrap();
        let harness_pid = session.pid.expect("fake harness reports its pid") as i32;

        // Wait for the harness's own /dev/fd check (a direct assertion,
        // independent of the EOF inference below) to actually run before
        // moving on.
        for _ in 0..40 {
            if diag.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Now close this test's own copy of `w` and let git exit (closing
        // its copy too). If the harness leaked a copy, `w` still has a live
        // writer and the read below never sees EOF.
        unsafe { libc::close(w) };
        git_child.wait().unwrap();

        let read_result = tokio::time::timeout(Duration::from_secs(10), async {
            let mut file = unsafe { std::fs::File::from_raw_fd(r) };
            tokio::task::spawn_blocking(move || {
                use std::io::Read;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).map(|_| buf)
            })
            .await
            .unwrap()
        })
        .await;

        // Confirmed still alive at the moment of the check, not inferred
        // from a `sleep` that might coincidentally still be running: the
        // harness is parked on a release file this test hasn't written yet.
        let alive_at_check = still_running(harness_pid).is_some();

        // Release and reap the harness in every outcome, success or panic
        // below, before any assertion can leave it running past this test.
        let _ = std::fs::write(&release, "go");
        let _ = session.control.kill().await;
        while session.events.recv().await.is_some() {}

        assert!(
            alive_at_check,
            "the harness must still have been running at the moment of the EOF check, or this proves nothing about a still-alive leaker"
        );
        assert_eq!(
            std::fs::read_to_string(&diag).unwrap_or_default().trim(),
            "clean",
            "the harness's own /dev/fd check must confirm it never saw the leaked descriptor"
        );
        let output = read_result.expect(
            "captured output must reach EOF promptly even while an unrelated harness is \
             confirmed still alive — a leaked descriptor would hang this read forever",
        );
        assert!(
            String::from_utf8_lossy(&output.unwrap()).contains("init"),
            "sanity: the real git child's output must still have come through correctly"
        );
    }

    /// A chatty child writing far more stderr than the event channel's
    /// capacity (256) as fast as possible must never be allowed to block on
    /// the channel filling up: that would stop the drain task from calling
    /// `next_line`, which stops draining the OS pipe, which blocks the
    /// child's own `write(2)` — the exact hang class stderr capture must
    /// never introduce. The child signals completion by touching a sentinel
    /// file *before* this test ever reads `session.events`, proving it ran
    /// to exit without our cooperation.
    #[tokio::test]
    async fn chatty_stderr_does_not_block_the_child() {
        let dir = tempfile::tempdir().unwrap();
        let sentinel = dir.path().join("done");
        let mut env = std::collections::HashMap::new();
        env.insert(
            "RK_FAKE_HARNESS_CMD".to_string(),
            format!(
                "i=0; while [ $i -lt 5000 ]; do echo \"stderr line $i padded xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\" >&2; i=$((i+1)); done; touch {}; exit 7",
                shell_escape(&sentinel)
            ),
        );
        let mut session = FakeHarness
            .launch(&LaunchSpec {
                cwd: dir.path().to_path_buf(),
                env,
                ..Default::default()
            })
            .unwrap();

        // Deliberately do not touch `session.events` yet: a fully absent
        // consumer is the worst case for channel backpressure. If stderr
        // forwarding ever blocks on a full channel, the child blocks on its
        // own stderr pipe and the sentinel never appears.
        let appeared = tokio::time::timeout(Duration::from_secs(5), async {
            while !sentinel.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .is_ok();
        assert!(
            appeared,
            "child never finished writing stderr and exiting — stderr \
             forwarding is blocking the channel and stalling the child"
        );

        // Now drain: the published tail (last events before Exited) must
        // hold the LAST lines, not the first — proving the bounded local
        // backlog drops oldest, not newest, when it can't keep up live.
        let mut last_stderr = None;
        let mut exited_code = None;
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(event) = session.events.recv().await {
                match event {
                    HarnessEvent::Stderr { text } => last_stderr = Some(text),
                    HarnessEvent::Exited { code } => {
                        exited_code = Some(code);
                        break;
                    }
                    _ => {}
                }
            }
        })
        .await
        .is_ok();
        assert!(drained, "draining remaining events timed out");
        assert_eq!(exited_code, Some(Some(7)));
        assert_eq!(
            last_stderr.as_deref(),
            Some("stderr line 4999 padded xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
        );
    }

    /// Simulates the leak this exists to close: an ungraceful runtime
    /// teardown (daemon shutdown, aborted supervisor task, or — as here — a
    /// test process's own runtime dropping) with the harness child's session
    /// still live and never explicitly killed. `kill_on_drop` alone only
    /// reaches the `bash -c` wrapper's own pid; a grandchild it backgrounds
    /// is untouched unless the whole process group is signalled, which is
    /// what `runner::ProcessGroupGuard` now guarantees on every drop path.
    #[test]
    fn dropped_session_kills_the_whole_process_group_not_just_the_wrapper() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("grandchild.pid");
        let mut env = std::collections::HashMap::new();
        env.insert(
            "RK_FAKE_HARNESS_CMD".to_string(),
            format!("sleep 600 & echo $! > {}; wait", shell_escape(&pid_file)),
        );

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let _session = FakeHarness
                .launch(&LaunchSpec {
                    cwd: dir.path().to_path_buf(),
                    env,
                    ..Default::default()
                })
                .unwrap();
            // Give the fake harness a moment to background the grandchild and
            // write its pid, then let this scope end — `_session` drops here,
            // but the task inside `runner::launch` that actually owns `child`
            // is detached (its JoinHandle was discarded), so this alone does
            // NOT reproduce the leak; the runtime drop below does.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while !pid_file.exists() {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "grandchild pid file never appeared"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        // Deliberately no `session.control.kill()`/`interrupt()` call: this
        // must exercise the implicit drop path, not the explicit signal path.
        // Dropping the runtime forcibly drops every task still running on
        // it, including the detached task that owns `child`.
        drop(runtime);

        let pid_text = std::fs::read_to_string(&pid_file).unwrap();
        let grandchild_pid: i32 = pid_text.trim().parse().unwrap();
        // The kill signal lands on the process group synchronously (in
        // `ProcessGroupGuard::drop`), but *reaping* the now-orphaned
        // grandchild is the job of whatever adopts it (init/launchd) once
        // its real parent (the `bash -c` wrapper) dies too — that can lag
        // an arbitrary amount under full-workspace test-suite CPU
        // contention, so a fixed sleep is inherently racy. Poll instead,
        // bounded, and treat a zombie (signal delivered, not yet reaped) as
        // proof the kill landed rather than waiting on OS bookkeeping that
        // this code has no control over.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match still_running(grandchild_pid) {
                None => break,
                Some(stat) if std::time::Instant::now() >= deadline => panic!(
                    "grandchild `sleep` (pid {grandchild_pid}) survived runtime \
                     teardown: still running (ps STAT {stat:?}) after a 10s poll bound"
                ),
                Some(_) => std::thread::sleep(Duration::from_millis(25)),
            }
        }
    }

    /// Polls `ps` rather than raw `kill(pid, 0)`: signal-0 liveness checks
    /// return success for a zombie exactly as they do for a running process,
    /// which can't distinguish "still executing" from "killed but not yet
    /// reaped by its new parent". Returns `None` once the pid is gone from
    /// the process table *or* parked in zombie (`Z`) state — both only
    /// happen after the kernel has already delivered and processed the
    /// terminating signal. `Some(stat)` means it is genuinely still
    /// running and the caller should keep polling.
    fn still_running(pid: i32) -> Option<String> {
        let output = std::process::Command::new("ps")
            .args(["-o", "stat=", "-p", &pid.to_string()])
            .output()
            .expect("failed to invoke `ps`");
        if !output.status.success() {
            return None; // `ps` found no such process: already reaped.
        }
        let stat = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if stat.is_empty() || stat.starts_with('Z') {
            None
        } else {
            Some(stat)
        }
    }

    /// Minimal single-quoting for embedding a path in the fake harness's bash
    /// one-liner (paths here are always `tempfile::tempdir()` output).
    fn shell_escape(path: &std::path::Path) -> String {
        format!("'{}'", path.display().to_string().replace('\'', r"'\''"))
    }
}
