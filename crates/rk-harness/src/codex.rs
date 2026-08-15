//! OpenAI Codex CLI adapter: `codex exec --json` (JSONL event stream).
//!
//! Codex exec mode has no mid-turn steering; steering surfaces as
//! unsupported, and the orchestrator falls back to resume-with-guidance
//! (`codex exec resume <session>`). Two protocol impedance mismatches are
//! absorbed by a per-session post-processor:
//!
//! - usage arrives as *session-cumulative* totals on `turn.completed`; the
//!   ledger wants deltas, so successive totals are differenced per session.
//! - there is no terminal `result` event; a clean exit after the final
//!   `agent_message` is the completion signal, so `Completed` is synthesized
//!   from the last assistant text when the process exits 0 first.

use crate::{runner, Harness, HarnessCaps, HarnessEvent, HarnessSession, LaunchSpec, TokenUsage};
use serde_json::Value;
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
            steer: false,
            interrupt: true,
            resume: true,
            reports_cost_usd: false,
            native_budget: false,
        }
    }

    fn launch(&self, spec: &LaunchSpec) -> rk_core::Result<HarnessSession> {
        let mut cmd = Command::new("codex");
        cmd.arg("exec");
        if let Some(session) = &spec.resume_session {
            cmd.args(["resume", session]);
        }
        cmd.args(["--json", "--skip-git-repo-check"]);
        cmd.args(env_policy_args());
        cmd.args(permission_args(spec.permission_mode.as_deref()));
        if let Some(model) = &spec.model {
            cmd.args(["-m", model]);
        }
        // Codex has no separate system-prompt channel in exec mode; prepend
        // role instructions to the prompt.
        let full_prompt = match &spec.system_prompt {
            Some(system) => format!("{system}\n\n---\n\n{}", spec.prompt),
            None => spec.prompt.clone(),
        };
        cmd.arg(&full_prompt);
        cmd.current_dir(&spec.cwd);
        cmd.envs(&spec.env);

        let session = runner::launch(runner::Wiring {
            command: cmd,
            parse: parse_event_line,
            steer_line: None,
        })?;
        Ok(post_process(session))
    }
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

    #[test]
    fn env_policy_preserves_rat_credentials_for_sandboxed_shell_commands() {
        assert_eq!(
            env_policy_args(),
            vec!["-c", "shell_environment_policy.inherit=all"]
        );
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
}
