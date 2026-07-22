//! axe adapter (jrswab/axe): one-shot Unix-philosophy executor.
//!
//! axe has no sessions, no steering, no resume — supervise as a plain
//! subprocess: `axe run <agent> -p <prompt> --json --workdir <cwd>`. The
//! single JSON result on stdout carries the reply and token metadata; exit
//! code 4 is axe's native budget-exceeded signal (the only harness with
//! built-in budget enforcement).
//!
//! The axe agent definition (TOML) selects model/provider; which agent to run
//! comes from `RK_AXE_AGENT` in the launch env (default: "rat" — define
//! `~/.config/axe/agents/rat.toml`). Role instructions are prepended to the
//! prompt, as axe exposes no separate system channel. Override the binary
//! with `RK_AXE_BIN` (used by tests to substitute a scripted stand-in).

use crate::{
    Harness, HarnessCaps, HarnessEvent, HarnessSession, LaunchSpec, SessionControl, TokenUsage,
};
use serde_json::Value;
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::mpsc;

pub const BUDGET_EXCEEDED_EXIT: i32 = 4;

pub struct AxeHarness;

impl Harness for AxeHarness {
    fn kind(&self) -> &'static str {
        "axe"
    }

    fn caps(&self) -> HarnessCaps {
        HarnessCaps {
            steer: false,
            interrupt: true,
            resume: false,
            reports_cost_usd: false,
            native_budget: true,
        }
    }

    fn launch(&self, spec: &LaunchSpec) -> rk_core::Result<HarnessSession> {
        let bin = spec
            .env
            .get("RK_AXE_BIN")
            .cloned()
            .or_else(|| std::env::var("RK_AXE_BIN").ok())
            .unwrap_or_else(|| "axe".into());
        let agent = spec
            .env
            .get("RK_AXE_AGENT")
            .cloned()
            .or_else(|| std::env::var("RK_AXE_AGENT").ok())
            .unwrap_or_else(|| "rat".into());

        let full_prompt = match &spec.system_prompt {
            Some(system) => format!("{system}\n\n---\n\n{}", spec.prompt),
            None => spec.prompt.clone(),
        };

        let mut cmd = Command::new(bin);
        cmd.args(["run", &agent, "-p", &full_prompt, "--json"]);
        cmd.args(["--workdir", &spec.cwd.to_string_lossy()]);
        if let Some(budget) = spec.env.get("RK_AXE_MAX_TOKENS") {
            cmd.args(["--max-tokens", budget]);
        }
        cmd.current_dir(&spec.cwd);
        cmd.envs(&spec.env);
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0)
            .kill_on_drop(true);

        let mut child = cmd.spawn()?;
        let pid = child.id();
        let mut stdout = child
            .stdout
            .take()
            .ok_or_else(|| rk_core::Error::other("child stdout unavailable"))?;

        let (event_tx, events) = mpsc::channel::<HarnessEvent>(16);
        let control = SessionControl::signal_only(pid);

        tokio::spawn(async move {
            let _ = event_tx
                .send(HarnessEvent::Started { session_id: None })
                .await;
            let mut buf = String::new();
            let _ = stdout.read_to_string(&mut buf).await;
            let code = child.wait().await.ok().and_then(|s| s.code());

            let parsed = parse_result(&buf);
            let (result, usage) = match parsed {
                Some((text, usage)) => (text, usage),
                None => (buf.trim().to_string(), TokenUsage::default()),
            };
            let is_error = code != Some(0);
            let result = if code == Some(BUDGET_EXCEEDED_EXIT) {
                format!("BUDGET EXCEEDED (axe exit 4): {result}")
            } else {
                result
            };
            let _ = event_tx
                .send(HarnessEvent::Completed {
                    result,
                    is_error,
                    usage,
                    cost_usd: None,
                    session_id: None,
                })
                .await;
            let _ = event_tx.send(HarnessEvent::Exited { code }).await;
        });

        Ok(HarnessSession {
            events,
            control,
            pid,
        })
    }
}

/// Parse axe's `--json` output tolerantly: the reply text under
/// result/response/output/text, token counts wherever they appear.
fn parse_result(stdout: &str) -> Option<(String, TokenUsage)> {
    let v: Value = serde_json::from_str(stdout.trim()).ok()?;
    let text = ["result", "response", "output", "text"]
        .iter()
        .find_map(|k| v[k].as_str())
        .unwrap_or_default()
        .to_string();
    let tokens = &v["tokens"];
    let meta = &v["metadata"];
    let pick = |keys: &[&Value], field: &str| -> u64 {
        keys.iter().find_map(|obj| obj[field].as_u64()).unwrap_or(0)
    };
    let sources: [&Value; 3] = [tokens, meta, &v];
    let usage = TokenUsage {
        input: pick(&sources, "input_tokens").max(pick(&sources, "input")),
        output: pick(&sources, "output_tokens").max(pick(&sources, "output")),
        cache_read: 0,
        cache_creation: 0,
    };
    Some((text, usage))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn parse_result_reads_common_shapes() {
        let (text, usage) = parse_result(
            r#"{"result":"done","metadata":{"input_tokens":120,"output_tokens":40,"model":"claude-x"}}"#,
        )
        .unwrap();
        assert_eq!(text, "done");
        assert_eq!(usage.input, 120);
        assert_eq!(usage.output, 40);

        let (text2, usage2) =
            parse_result(r#"{"response":"ok","tokens":{"input":10,"output":5}}"#).unwrap();
        assert_eq!(text2, "ok");
        assert_eq!(usage2.total(), 15);

        assert!(parse_result("not json").is_none());
    }

    fn stand_in(script: &str) -> LaunchSpec {
        let mut env = HashMap::new();
        // The stand-in ignores axe's argv and just runs the script body.
        env.insert("RK_AXE_BIN".into(), "/bin/bash".into());
        env.insert("RK_AXE_SCRIPT".into(), script.into());
        LaunchSpec {
            prompt: "task".into(),
            cwd: std::env::temp_dir(),
            env,
            ..Default::default()
        }
    }

    // bash invoked as `bash run rat -p <prompt> --json --workdir <dir>` errors;
    // instead use bash -c via a wrapper: RK_AXE_BIN=/bin/bash won't work
    // directly, so tests exercise launch via a tiny script file.
    #[tokio::test]
    async fn budget_exit_and_json_result_flow_through() {
        let dir = tempfile::tempdir().unwrap();
        let fake_axe = dir.path().join("fake-axe");
        std::fs::write(
            &fake_axe,
            "#!/bin/bash\necho '{\"result\":\"partial work\",\"metadata\":{\"input_tokens\":900,\"output_tokens\":100}}'\nexit 4\n",
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake_axe, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut spec = stand_in("");
        spec.env
            .insert("RK_AXE_BIN".into(), fake_axe.to_string_lossy().into_owned());
        spec.cwd = dir.path().to_path_buf();

        let mut session = AxeHarness.launch(&spec).unwrap();
        let mut completed = None;
        let mut exit_code = None;
        while let Some(event) = session.events.recv().await {
            match event {
                HarnessEvent::Completed {
                    result,
                    is_error,
                    usage,
                    ..
                } => completed = Some((result, is_error, usage)),
                HarnessEvent::Exited { code } => exit_code = code,
                _ => {}
            }
        }
        let (result, is_error, usage) = completed.expect("completed event");
        assert!(result.starts_with("BUDGET EXCEEDED"));
        assert!(is_error);
        assert_eq!(usage.input, 900);
        assert_eq!(exit_code, Some(BUDGET_EXCEEDED_EXIT));
    }
}
