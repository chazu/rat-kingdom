//! OpenAI Codex CLI adapter: `codex exec --json` (JSONL event stream).
//!
//! Codex exec mode has no mid-turn steering. The adapter provides trusted
//! steering by interrupting at a process turn boundary and resuming the same
//! session with a structurally distinct control prompt. Two protocol
//! impedance mismatches are absorbed by a per-session post-processor:
//!
//! - usage arrives as *session-cumulative* totals on `turn.completed`; the
//!   ledger wants deltas, so successive totals are differenced per session.
//! - there is no terminal `result` event; a clean exit after the final
//!   `agent_message` is the completion signal, so `Completed` is synthesized
//!   from the last assistant text when the process exits 0 first.

use crate::{
    runner, ControlEnvelope, Harness, HarnessCaps, HarnessEvent, HarnessSession, LaunchSpec,
    TokenUsage,
};
use serde_json::Value;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::mpsc;

pub struct CodexHarness;

/// Codex's `shell_environment_policy` filters which env vars reach commands
/// the agent's shell tool executes, independent of `--sandbox`/
/// `--dangerously-bypass-approvals-and-sandbox`. Left at its default, it
/// drops `RK_AGENT`/`RK_HOME`/`RK_AUTH_TOKEN`, so `rk` calls the agent makes
/// from inside the sandbox lose the daemon credentials the harness set on
/// the codex process itself and fail authentication.
fn env_policy_args() -> Vec<String> {
    vec!["-c".into(), "shell_environment_policy.inherit=all".into()]
}

fn permission_args(permission_mode: Option<&str>) -> Vec<String> {
    match permission_mode {
        Some("read-only") => vec!["--sandbox".into(), "read-only".into()],
        Some("workspace-write") => vec!["--sandbox".into(), "workspace-write".into()],
        // The worker contract is non-interactive: a command that needs human
        // approval cannot complete, and the daemon socket sits outside the
        // worktree sandbox. Keep approval and sandbox bypass explicit rather
        // than relying on `codex exec` defaults or user configuration.
        Some("bypassPermissions") | Some("danger-full-access") | None => {
            vec!["--dangerously-bypass-approvals-and-sandbox".into()]
        }
        // The supervisor rejects unsupported modes. Direct LaunchSpec callers
        // retain the old full-access fallback instead of silently narrowing.
        Some(_) => vec!["--dangerously-bypass-approvals-and-sandbox".into()],
    }
}

impl Harness for CodexHarness {
    fn kind(&self) -> &'static str {
        "codex"
    }

    fn caps(&self) -> HarnessCaps {
        HarnessCaps {
            steer: true,
            interrupt: true,
            resume: true,
            reports_cost_usd: false,
            native_budget: false,
        }
    }

    fn launch(&self, spec: &LaunchSpec) -> rk_core::Result<HarnessSession> {
        // Codex has no separate system-prompt channel in exec mode; prepend
        // role instructions to the prompt.
        let full_prompt = match &spec.system_prompt {
            Some(system) => format!("{system}\n\n---\n\n{}", spec.prompt),
            None => spec.prompt.clone(),
        };
        let binary = spec
            .env
            .get("RK_CODEX_BIN")
            .cloned()
            .or_else(|| std::env::var("RK_CODEX_BIN").ok())
            .unwrap_or_else(|| "codex".into());
        let cmd = command_for(
            binary.clone(),
            spec.cwd.clone(),
            spec.env.clone(),
            spec.permission_mode.clone(),
            spec.model.clone(),
            spec.resume_session.clone(),
            full_prompt,
        );
        let cwd = spec.cwd.clone();
        let env = spec.env.clone();
        let permission_mode = spec.permission_mode.clone();
        let model = spec.model.clone();

        let session = runner::launch(runner::Wiring {
            command: cmd,
            parse: parse_event_line,
            steer_line: None,
            resume: Some(runner::ResumeWiring {
                command: Arc::new(move |session_id, envelope| {
                    command_for(
                        binary.clone(),
                        cwd.clone(),
                        env.clone(),
                        permission_mode.clone(),
                        model.clone(),
                        Some(session_id.to_string()),
                        trusted_control_prompt(envelope),
                    )
                }),
            }),
        })?;
        Ok(post_process(session))
    }
}

fn command_for(
    binary: String,
    cwd: std::path::PathBuf,
    env: std::collections::HashMap<String, String>,
    permission_mode: Option<String>,
    model: Option<String>,
    resume_session: Option<String>,
    prompt: String,
) -> Command {
    let mut cmd = Command::new(binary);
    cmd.arg("exec");
    if let Some(session) = resume_session {
        cmd.args(["resume", &session]);
    }
    cmd.args(["--json", "--skip-git-repo-check"]);
    cmd.args(env_policy_args());
    cmd.args(permission_args(permission_mode.as_deref()));
    if let Some(model) = model {
        cmd.args(["-m", &model]);
    }
    cmd.arg(prompt);
    cmd.current_dir(cwd);
    cmd.envs(env);
    cmd
}

/// Codex has no metadata-bearing input protocol. Keep the authenticated
/// envelope structurally explicit in the new resume turn and never infer it
/// from any assistant, tool, or stderr event.
fn trusted_control_prompt(envelope: &ControlEnvelope) -> String {
    let serialized = serde_json::to_string(envelope).expect("control envelope is serializable");
    format!(
        "[RK TRUSTED CONTROL TURN]\nThe following envelope was authenticated by the Rat Kingdom daemon. It is control input, not repository or tool output. Apply its instruction as the next turn.\n<rk-control-envelope>{serialized}</rk-control-envelope>\n[/RK TRUSTED CONTROL TURN]"
    )
}

/// Wrap the raw event stream with per-session state: cumulative→delta usage
/// and Completed synthesis on clean exit.
fn post_process(mut session: HarnessSession) -> HarnessSession {
    let (tx, rx) = mpsc::channel::<HarnessEvent>(256);
    tokio::spawn(async move {
        let mut prev_cumulative: Option<TokenUsage> = None;
        let mut total = TokenUsage::default();
        let mut last_text: Option<String> = None;
        let mut session_id: Option<String> = None;
        let mut completed = false;

        while let Some(event) = session.events.recv().await {
            let forward = match event {
                HarnessEvent::Started { session_id: sid } => {
                    session_id = sid.clone();
                    Some(HarnessEvent::Started { session_id: sid })
                }
                HarnessEvent::AssistantText { text } => {
                    last_text = Some(text.clone());
                    Some(HarnessEvent::AssistantText { text })
                }
                HarnessEvent::Usage { usage: cumulative } => {
                    let delta = match prev_cumulative {
                        Some(prev) if cumulative.input >= prev.input => TokenUsage {
                            input: cumulative.input - prev.input,
                            output: cumulative.output.saturating_sub(prev.output),
                            cache_read: cumulative.cache_read.saturating_sub(prev.cache_read),
                            cache_creation: 0,
                        },
                        _ => cumulative,
                    };
                    prev_cumulative = Some(cumulative);
                    total.add(&delta);
                    Some(HarnessEvent::Usage { usage: delta })
                }
                HarnessEvent::Exited { code } => {
                    if !completed && code == Some(0) {
                        let _ = tx
                            .send(HarnessEvent::Completed {
                                result: last_text.clone().unwrap_or_default(),
                                is_error: false,
                                usage: total,
                                cost_usd: None,
                                session_id: session_id.clone(),
                            })
                            .await;
                    }
                    Some(HarnessEvent::Exited { code })
                }
                HarnessEvent::Completed { .. } => {
                    completed = true;
                    Some(event)
                }
                other => Some(other),
            };
            if let Some(event) = forward {
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        }
    });
    HarnessSession {
        events: rx,
        control: session.control,
        pid: session.pid,
    }
}

fn usage_from(value: &Value) -> TokenUsage {
    TokenUsage {
        input: value["input_tokens"].as_u64().unwrap_or(0),
        output: value["output_tokens"].as_u64().unwrap_or(0)
            + value["reasoning_output_tokens"].as_u64().unwrap_or(0),
        cache_read: value["cached_input_tokens"].as_u64().unwrap_or(0),
        cache_creation: 0,
    }
}

/// Raw line parser: cumulative usage passes through untouched; the session
/// post-processor owns differencing.
pub(crate) fn parse_event_line(line: &str) -> Vec<HarnessEvent> {
    let Ok(v) = serde_json::from_str::<Value>(line) else {
        return Vec::new();
    };
    let mut events = Vec::new();
    match v["type"].as_str() {
        Some("thread.started") => events.push(HarnessEvent::Started {
            session_id: v["thread_id"].as_str().map(String::from),
        }),
        Some("turn.completed") if v["usage"].is_object() => {
            events.push(HarnessEvent::Usage {
                usage: usage_from(&v["usage"]),
            });
        }
        Some("item.completed") => {
            let item = &v["item"];
            match item["item_type"].as_str().or(item["type"].as_str()) {
                Some("agent_message") => {
                    if let Some(text) = item["text"].as_str() {
                        events.push(HarnessEvent::AssistantText {
                            text: text.to_string(),
                        });
                    }
                }
                Some("command_execution") => events.push(HarnessEvent::ToolUse {
                    name: "command".into(),
                }),
                Some("mcp_tool_call") => events.push(HarnessEvent::ToolUse {
                    name: item["tool"].as_str().unwrap_or("mcp").to_string(),
                }),
                _ => {}
            }
        }
        Some("error") => events.push(HarnessEvent::Retry {
            attempt: 0,
            error: v["message"].as_str().unwrap_or("unknown").to_string(),
        }),
        _ => {}
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    #[test]
    fn env_policy_preserves_rat_credentials_for_sandboxed_shell_commands() {
        assert_eq!(
            env_policy_args(),
            vec!["-c", "shell_environment_policy.inherit=all"]
        );
    }

    #[test]
    fn codex_supports_trusted_resume_steering() {
        assert!(CodexHarness.caps().steer);
        assert!(CodexHarness.caps().resume);
    }

    #[test]
    fn autonomous_modes_bypass_codex_approvals_and_sandbox() {
        for mode in [None, Some("bypassPermissions"), Some("danger-full-access")] {
            assert_eq!(
                permission_args(mode),
                vec!["--dangerously-bypass-approvals-and-sandbox"]
            );
        }
        assert_eq!(
            permission_args(Some("read-only")),
            vec!["--sandbox", "read-only"]
        );
    }

    #[test]
    fn thread_started_maps_to_started() {
        let events = parse_event_line(r#"{"type":"thread.started","thread_id":"0198-abc"}"#);
        assert!(matches!(
            &events[..],
            [HarnessEvent::Started { session_id: Some(s) }] if s == "0198-abc"
        ));
    }

    #[test]
    fn parser_passes_cumulative_usage_through() {
        let events = parse_event_line(
            r#"{"type":"turn.completed","usage":{"input_tokens":1000,"cached_input_tokens":200,"output_tokens":50,"reasoning_output_tokens":10}}"#,
        );
        let [HarnessEvent::Usage { usage }] = &events[..] else {
            panic!("expected usage");
        };
        assert_eq!(usage.input, 1000);
        assert_eq!(usage.output, 60, "reasoning counts as output");
        assert_eq!(usage.cache_read, 200);
    }

    #[test]
    fn agent_message_and_command_items() {
        let msg = parse_event_line(
            r#"{"type":"item.completed","item":{"item_type":"agent_message","text":"all done"}}"#,
        );
        assert!(matches!(&msg[..], [HarnessEvent::AssistantText { text }] if text == "all done"));
        let cmd = parse_event_line(
            r#"{"type":"item.completed","item":{"item_type":"command_execution","command":"ls"}}"#,
        );
        assert!(matches!(&cmd[..], [HarnessEvent::ToolUse { name }] if name == "command"));
    }

    /// Post-processor behavior via a scripted child: cumulative usage becomes
    /// deltas, and a clean exit synthesizes Completed from the last message.
    #[tokio::test]
    async fn post_processor_differences_usage_and_synthesizes_completed() {
        let script = r#"
echo '{"type":"thread.started","thread_id":"t-1"}'
echo '{"type":"turn.completed","usage":{"input_tokens":100,"cached_input_tokens":0,"output_tokens":10,"reasoning_output_tokens":0}}'
echo '{"type":"item.completed","item":{"item_type":"agent_message","text":"step one"}}'
echo '{"type":"turn.completed","usage":{"input_tokens":250,"cached_input_tokens":40,"output_tokens":30,"reasoning_output_tokens":5}}'
echo '{"type":"item.completed","item":{"item_type":"agent_message","text":"final answer"}}'
"#;
        let mut cmd = Command::new("bash");
        cmd.args(["-c", script]);
        let session = runner::launch(runner::Wiring {
            command: cmd,
            parse: parse_event_line,
            steer_line: None,
            resume: None,
        })
        .unwrap();
        let mut session = post_process(session);

        let mut usages = Vec::new();
        let mut completed = None;
        while let Some(event) = session.events.recv().await {
            match event {
                HarnessEvent::Usage { usage } => usages.push(usage),
                HarnessEvent::Completed {
                    result,
                    usage,
                    session_id,
                    ..
                } => completed = Some((result, usage, session_id)),
                _ => {}
            }
        }
        assert_eq!(usages.len(), 2);
        assert_eq!(usages[0].input, 100);
        assert_eq!(usages[1].input, 150, "differenced");
        assert_eq!(usages[1].output, 25);

        let (result, total, session_id) = completed.expect("synthesized Completed");
        assert_eq!(result, "final answer");
        assert_eq!(total.input, 250, "total = sum of deltas");
        assert_eq!(session_id.as_deref(), Some("t-1"));
    }

    #[tokio::test]
    async fn trusted_control_interrupts_busy_turn_and_resumes_once() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("codex-fake");
        let log = dir.path().join("argv.log");
        fs::write(
            &binary,
            r#"#!/bin/sh
printf '%s\n' "$*" >> "$RK_CODEX_LOG"
if [ "$2" = "resume" ]; then
  echo '{"type":"thread.started","thread_id":"session-rat"}'
  echo '{"type":"item.completed","item":{"item_type":"agent_message","text":"control applied"}}'
  exit 0
fi
echo '{"type":"thread.started","thread_id":"session-rat"}'
trap 'exit 130' INT
while :; do sleep 1; done
"#,
        )
        .unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();

        let mut env = HashMap::new();
        env.insert("RK_CODEX_BIN".into(), binary.to_string_lossy().into_owned());
        env.insert("RK_CODEX_LOG".into(), log.to_string_lossy().into_owned());
        let mut session = CodexHarness
            .launch(&LaunchSpec {
                prompt: "keep working".into(),
                cwd: dir.path().to_path_buf(),
                env,
                ..Default::default()
            })
            .unwrap();

        let mut started = false;
        while !started {
            let event = tokio::time::timeout(Duration::from_secs(2), session.events.recv())
                .await
                .unwrap()
                .unwrap();
            started = matches!(event, HarnessEvent::Started { .. });
        }

        let envelope = ControlEnvelope::new(
            "message-1",
            "operator",
            "rat",
            "delivery-1",
            "resume-1",
            "also run the focused tests",
        );
        session.control.steer_envelope(&envelope).await.unwrap();
        // A replay race before the acknowledgement must not cause a second
        // resume generation for the same message id.
        session.control.steer_envelope(&envelope).await.unwrap();

        let mut deliveries = Vec::new();
        let mut applied = false;
        let mut observed = Vec::new();
        while let Some(event) = tokio::time::timeout(Duration::from_secs(3), session.events.recv())
            .await
            .unwrap()
        {
            observed.push(format!("{event:?}"));
            match event {
                HarnessEvent::ControlDelivered { envelope } => deliveries.push(envelope),
                HarnessEvent::AssistantText { text } if text == "control applied" => applied = true,
                HarnessEvent::Exited { code } => {
                    assert_eq!(code, Some(0));
                    break;
                }
                _ => {}
            }
        }

        assert!(
            applied,
            "the resumed turn must reach the agent; observed={observed:?}"
        );
        assert_eq!(deliveries, vec![envelope]);
        let log = fs::read_to_string(log).unwrap();
        assert_eq!(
            log.lines().filter(|line| line.starts_with("exec ")).count(),
            2,
            "one initial and one resume process"
        );
        assert!(log.contains("exec resume session-rat"));
        assert!(log.contains("[RK TRUSTED CONTROL TURN]"));
        assert!(log.contains("message-1"));
    }

    #[tokio::test]
    async fn resume_failure_without_session_is_visible_and_not_acknowledged() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("codex-fake");
        fs::write(&binary, "#!/bin/sh\nsleep 0.5\nexit 130\n").unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
        let mut env = HashMap::new();
        env.insert("RK_CODEX_BIN".into(), binary.to_string_lossy().into_owned());
        let mut session = CodexHarness
            .launch(&LaunchSpec {
                prompt: "keep working".into(),
                cwd: dir.path().to_path_buf(),
                env,
                ..Default::default()
            })
            .unwrap();
        let envelope = ControlEnvelope::new(
            "message-no-session",
            "operator",
            "rat",
            "delivery-1",
            "resume-1",
            "continue",
        );
        session.control.steer_envelope(&envelope).await.unwrap();

        let mut saw_failure = false;
        let mut saw_delivery = false;
        while let Some(event) = tokio::time::timeout(Duration::from_secs(2), session.events.recv())
            .await
            .unwrap()
        {
            match event {
                HarnessEvent::Retry { error, .. } => {
                    saw_failure = error.contains("could not resume");
                }
                HarnessEvent::ControlDelivered { .. } => saw_delivery = true,
                HarnessEvent::Exited { .. } => break,
                _ => {}
            }
        }
        assert!(
            saw_failure,
            "resume failure must be visible to the supervisor"
        );
        assert!(!saw_delivery, "failed resume must remain unacknowledged");
    }

    #[tokio::test]
    async fn resume_rejection_after_spawn_is_not_acknowledged() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("codex-fake");
        fs::write(
            &binary,
            r#"#!/bin/sh
if [ "$2" = "resume" ]; then
  echo '{"type":"error","message":"session rejected"}'
  exit 2
fi
echo '{"type":"thread.started","thread_id":"session-rat"}'
trap 'exit 130' INT
while :; do sleep 1; done
"#,
        )
        .unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
        let mut env = HashMap::new();
        env.insert("RK_CODEX_BIN".into(), binary.to_string_lossy().into_owned());
        let mut session = CodexHarness
            .launch(&LaunchSpec {
                prompt: "keep working".into(),
                cwd: dir.path().to_path_buf(),
                env,
                ..Default::default()
            })
            .unwrap();

        let started = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(event) = session.events.recv().await {
                if matches!(event, HarnessEvent::Started { .. }) {
                    break;
                }
            }
        })
        .await;
        assert!(started.is_ok(), "initial Codex session did not start");

        let envelope = ControlEnvelope::new(
            "message-rejected",
            "operator",
            "rat",
            "delivery-1",
            "resume-1",
            "continue",
        );
        session.control.steer_envelope(&envelope).await.unwrap();

        let mut saw_failure = false;
        let mut saw_delivery = false;
        while let Some(event) = tokio::time::timeout(Duration::from_secs(2), session.events.recv())
            .await
            .unwrap()
        {
            match event {
                HarnessEvent::Retry { .. } => saw_failure = true,
                HarnessEvent::ControlDelivered { .. } => saw_delivery = true,
                HarnessEvent::Exited { .. } => break,
                _ => {}
            }
        }
        assert!(saw_failure, "a rejected resume must be visible");
        assert!(
            !saw_delivery,
            "spawning a resume child must not acknowledge a rejected session"
        );
    }

    /// A resumed process can complete the protocol handshake (emit
    /// `thread.started`) and then immediately reject the session and exit,
    /// with no further event proving control was actually applied. Started
    /// alone must not acknowledge the envelope, or a durable ack would be
    /// persisted for a control that never took effect, permanently
    /// suppressing replay.
    #[tokio::test]
    async fn resume_started_then_immediate_exit_is_not_acknowledged() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("codex-fake");
        fs::write(
            &binary,
            r#"#!/bin/sh
if [ "$2" = "resume" ]; then
  echo '{"type":"thread.started","thread_id":"session-rat"}'
  exit 1
fi
echo '{"type":"thread.started","thread_id":"session-rat"}'
trap 'exit 130' INT
while :; do sleep 1; done
"#,
        )
        .unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
        let mut env = HashMap::new();
        env.insert("RK_CODEX_BIN".into(), binary.to_string_lossy().into_owned());
        let mut session = CodexHarness
            .launch(&LaunchSpec {
                prompt: "keep working".into(),
                cwd: dir.path().to_path_buf(),
                env,
                ..Default::default()
            })
            .unwrap();

        let started = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(event) = session.events.recv().await {
                if matches!(event, HarnessEvent::Started { .. }) {
                    break;
                }
            }
        })
        .await;
        assert!(started.is_ok(), "initial Codex session did not start");

        let envelope = ControlEnvelope::new(
            "message-started-then-exit",
            "operator",
            "rat",
            "delivery-1",
            "resume-1",
            "continue",
        );
        session.control.steer_envelope(&envelope).await.unwrap();

        let mut saw_failure = false;
        let mut saw_delivery = false;
        while let Some(event) = tokio::time::timeout(Duration::from_secs(2), session.events.recv())
            .await
            .unwrap()
        {
            match event {
                HarnessEvent::Retry { .. } => saw_failure = true,
                HarnessEvent::ControlDelivered { .. } => saw_delivery = true,
                HarnessEvent::Exited { .. } => break,
                _ => {}
            }
        }
        assert!(
            saw_failure,
            "a resume that starts then immediately exits must be visible as a failure"
        );
        assert!(
            !saw_delivery,
            "Started alone must not acknowledge a resume that never proves control was applied"
        );
    }

    /// A resumed process can complete the handshake, emit a tool-call event
    /// (Codex resume can replay command_execution/mcp_tool_call/turn.completed
    /// activity buffered from the session's prior history), and then reject
    /// the session and exit — all without the model ever engaging with the
    /// newly injected control turn. Neither Started nor a lookalike
    /// ToolUse/Usage event may acknowledge the envelope, or a durable ack
    /// would be persisted for a control that was never actually applied.
    #[tokio::test]
    async fn resume_started_then_tool_only_then_rejected_is_not_acknowledged() {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("codex-fake");
        fs::write(
            &binary,
            r#"#!/bin/sh
if [ "$2" = "resume" ]; then
  echo '{"type":"thread.started","thread_id":"session-rat"}'
  echo '{"type":"item.completed","item":{"item_type":"command_execution","command":"echo replayed"}}'
  echo '{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":0,"output_tokens":1,"reasoning_output_tokens":0}}'
  echo '{"type":"error","message":"session rejected"}'
  exit 2
fi
echo '{"type":"thread.started","thread_id":"session-rat"}'
trap 'exit 130' INT
while :; do sleep 1; done
"#,
        )
        .unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
        let mut env = HashMap::new();
        env.insert("RK_CODEX_BIN".into(), binary.to_string_lossy().into_owned());
        let mut session = CodexHarness
            .launch(&LaunchSpec {
                prompt: "keep working".into(),
                cwd: dir.path().to_path_buf(),
                env,
                ..Default::default()
            })
            .unwrap();

        let started = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(event) = session.events.recv().await {
                if matches!(event, HarnessEvent::Started { .. }) {
                    break;
                }
            }
        })
        .await;
        assert!(started.is_ok(), "initial Codex session did not start");

        let envelope = ControlEnvelope::new(
            "message-tool-then-reject",
            "operator",
            "rat",
            "delivery-1",
            "resume-1",
            "continue",
        );
        session.control.steer_envelope(&envelope).await.unwrap();

        let mut saw_failure = false;
        let mut saw_delivery = false;
        let mut saw_tool = false;
        while let Some(event) = tokio::time::timeout(Duration::from_secs(2), session.events.recv())
            .await
            .unwrap()
        {
            match event {
                HarnessEvent::ToolUse { .. } => saw_tool = true,
                HarnessEvent::Retry { .. } => saw_failure = true,
                HarnessEvent::ControlDelivered { .. } => saw_delivery = true,
                HarnessEvent::Exited { .. } => break,
                _ => {}
            }
        }
        assert!(saw_tool, "the replayed tool event must still surface");
        assert!(
            saw_failure,
            "a resume that starts, emits tool activity, then rejects must be visible as a failure"
        );
        assert!(
            !saw_delivery,
            "a lookalike tool/usage event must not acknowledge a resume that never proved control was applied"
        );
    }
}
