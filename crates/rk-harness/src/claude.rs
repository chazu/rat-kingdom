//! Claude Code adapter: headless stream-json in both directions.
//!
//! Launch shape:
//! `claude -p --output-format stream-json --input-format stream-json --verbose`
//! with the initial prompt delivered as the first stdin user message and role
//! instructions via `--append-system-prompt` — no TUI, no readiness probes.

use crate::{runner, Harness, HarnessCaps, HarnessEvent, HarnessSession, LaunchSpec, TokenUsage};
use serde_json::{json, Value};
use tokio::process::Command;

pub struct ClaudeHarness;

impl Harness for ClaudeHarness {
    fn kind(&self) -> &'static str {
        "claude"
    }

    fn caps(&self) -> HarnessCaps {
        HarnessCaps {
            steer: true,
            interrupt: true,
            resume: true,
            reports_cost_usd: true,
            native_budget: false,
        }
    }

    fn launch(&self, spec: &LaunchSpec) -> rk_core::Result<HarnessSession> {
        let mut cmd = Command::new("claude");
        cmd.args([
            "-p",
            "--output-format",
            "stream-json",
            "--input-format",
            "stream-json",
            "--verbose",
        ]);
        if let Some(system) = &spec.system_prompt {
            cmd.args(["--append-system-prompt", system]);
        }
        if let Some(mode) = &spec.permission_mode {
            cmd.args(["--permission-mode", mode]);
        }
        if let Some(model) = &spec.model {
            cmd.args(["--model", model]);
        }
        if let Some(session) = &spec.resume_session {
            cmd.args(["--resume", session]);
        }
        cmd.current_dir(&spec.cwd);
        cmd.envs(&spec.env);

        let mut session = runner::launch(runner::Wiring {
            command: cmd,
            parse: parse_event_line,
            steer_line: Some(user_message_line),
        })?;

        // The initial prompt is just the first steer message.
        let prompt = spec.prompt.clone();
        let control = session.control.clone();
        tokio::spawn(async move {
            let _ = control.steer(&prompt).await;
        });

        session.pid = session.pid.or(None);
        Ok(session)
    }
}

/// Wrap a plain-text message as a stream-json user message line.
fn user_message_line(text: &str) -> String {
    json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": [{"type": "text", "text": text}],
        },
    })
    .to_string()
}

fn usage_from(value: &Value) -> TokenUsage {
    TokenUsage {
        input: value["input_tokens"].as_u64().unwrap_or(0),
        output: value["output_tokens"].as_u64().unwrap_or(0),
        cache_read: value["cache_read_input_tokens"].as_u64().unwrap_or(0),
        cache_creation: value["cache_creation_input_tokens"].as_u64().unwrap_or(0),
    }
}

/// Map one stream-json line to normalized events. Unknown lines are ignored —
/// forward compatibility over strictness (the `capabilities` array in
/// `system/init` is the place to detect protocol growth).
pub(crate) fn parse_event_line(line: &str) -> Vec<HarnessEvent> {
    let Ok(v) = serde_json::from_str::<Value>(line) else {
        return Vec::new();
    };
    let mut events = Vec::new();
    match v["type"].as_str() {
        Some("system") => match v["subtype"].as_str() {
            Some("init") => events.push(HarnessEvent::Started {
                session_id: v["session_id"].as_str().map(String::from),
            }),
            Some("api_retry") => events.push(HarnessEvent::Retry {
                attempt: v["attempt"].as_u64().unwrap_or(0),
                error: v["error"].as_str().unwrap_or("unknown").to_string(),
            }),
            _ => {}
        },
        Some("assistant") => {
            let message = &v["message"];
            if let Some(content) = message["content"].as_array() {
                for block in content {
                    match block["type"].as_str() {
                        Some("text") => {
                            if let Some(text) = block["text"].as_str() {
                                events.push(HarnessEvent::AssistantText {
                                    text: text.to_string(),
                                });
                            }
                        }
                        Some("tool_use") => events.push(HarnessEvent::ToolUse {
                            name: block["name"].as_str().unwrap_or("?").to_string(),
                        }),
                        _ => {}
                    }
                }
            }
            if message["usage"].is_object() {
                events.push(HarnessEvent::Usage {
                    usage: usage_from(&message["usage"]),
                });
            }
        }
        Some("result") => events.push(HarnessEvent::Completed {
            result: v["result"].as_str().unwrap_or_default().to_string(),
            is_error: v["is_error"].as_bool().unwrap_or(false),
            usage: usage_from(&v["usage"]),
            cost_usd: v["total_cost_usd"].as_f64(),
            session_id: v["session_id"].as_str().map(String::from),
        }),
        _ => {}
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_line_yields_started_with_session() {
        let line = r#"{"type":"system","subtype":"init","session_id":"abc-123","model":"claude-x","capabilities":["interrupt_receipt_v1"]}"#;
        let events = parse_event_line(line);
        assert!(matches!(
            &events[..],
            [HarnessEvent::Started { session_id: Some(s) }] if s == "abc-123"
        ));
    }

    #[test]
    fn assistant_line_yields_text_tools_and_usage() {
        let line = r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"working on it"},{"type":"tool_use","name":"Bash","id":"t1","input":{}}],"usage":{"input_tokens":100,"output_tokens":20,"cache_read_input_tokens":5000,"cache_creation_input_tokens":10}}}"#;
        let events = parse_event_line(line);
        assert_eq!(events.len(), 3);
        assert!(
            matches!(&events[0], HarnessEvent::AssistantText { text } if text == "working on it")
        );
        assert!(matches!(&events[1], HarnessEvent::ToolUse { name } if name == "Bash"));
        assert!(matches!(
            &events[2],
            HarnessEvent::Usage { usage } if usage.cache_read == 5000 && usage.total() == 5130
        ));
    }

    #[test]
    fn result_line_yields_completed_with_cost() {
        let line = r#"{"type":"result","subtype":"success","is_error":false,"result":"done: merged","session_id":"abc-123","total_cost_usd":0.4523,"usage":{"input_tokens":2000,"output_tokens":900,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}"#;
        let events = parse_event_line(line);
        let [HarnessEvent::Completed {
            result,
            is_error,
            cost_usd,
            session_id,
            usage,
        }] = &events[..]
        else {
            panic!("expected Completed, got {events:?}");
        };
        assert_eq!(result, "done: merged");
        assert!(!is_error);
        assert_eq!(*cost_usd, Some(0.4523));
        assert_eq!(session_id.as_deref(), Some("abc-123"));
        assert_eq!(usage.output, 900);
    }

    #[test]
    fn retry_and_junk_lines() {
        let retry = parse_event_line(
            r#"{"type":"system","subtype":"api_retry","attempt":2,"max_retries":10,"error":"rate_limit"}"#,
        );
        assert!(matches!(
            &retry[..],
            [HarnessEvent::Retry { attempt: 2, .. }]
        ));
        assert!(parse_event_line("not json").is_empty());
        assert!(parse_event_line(r#"{"type":"stream_event"}"#).is_empty());
    }

    #[test]
    fn steer_message_is_valid_stream_json() {
        let line = user_message_line("please also run the tests");
        let v: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["type"], "user");
        assert_eq!(
            v["message"]["content"][0]["text"],
            "please also run the tests"
        );
        assert!(!line.contains('\n'), "must be a single line");
    }
}
