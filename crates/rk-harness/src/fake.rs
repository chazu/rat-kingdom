//! A scriptable harness for integration tests and dry runs.
//!
//! Runs the command in `RK_FAKE_HARNESS_CMD` (default: a bash one-liner that
//! echoes a canned Claude-style stream), parsed with the same parser as the
//! Claude adapter — so daemon plumbing is exercised end-to-end without
//! spending tokens.

use crate::{claude, runner, Harness, HarnessCaps, HarnessSession, LaunchSpec};
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
            parse: claude::parse_event_line,
            steer_line: Some(|text| serde_json::json!({"type": "user", "text": text}).to_string()),
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
        assert_eq!(last_stderr.as_deref(), Some("stderr line 4999 padded xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"));
    }
}

/// Minimal single-quoting for embedding a path in the fake harness's bash
/// one-liner (test-only; paths here are always `tempfile::tempdir()` output).
#[cfg(test)]
fn shell_escape(path: &std::path::Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', r"'\''"))
}
