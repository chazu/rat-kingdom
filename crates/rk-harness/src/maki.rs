//! Maki adapter: headless SDK-mode stream-json in both directions.
//!
//! Launch shape:
//! `maki --print --input-format stream-json --output-format stream-json
//! --no-plugins --no-commands --disallowed-tools Task,Memory` with the
//! initial prompt delivered as the first stdin `user` record and role
//! instructions via `--append-system-prompt` — no TUI, no readiness probes.
//! Maki's SDK transport is Claude Code-compatible: `system/init`,
//! `assistant`, `system/api_retry`, and `result` records share Claude's
//! shape (verified against the installed 0.5.2 binary).
//!
//! Maki deserializes only `message.content` from an inbound `user` record —
//! the `rk_control` side-band metadata Claude's adapter carries alongside it
//! is silently dropped, so mid-session steering has no daemon-verifiable
//! trust boundary on Maki's side yet. `caps().steer` stays `false` to avoid
//! advertising that trust, even though the same control channel is still
//! how every launch delivers its one unavoidable message: the initial
//! prompt, which stream-json input mode has no CLI-argument equivalent for.

use crate::{
    runner, ControlEnvelope, Harness, HarnessCaps, HarnessEvent, HarnessSession, LaunchSpec,
    TokenUsage,
};
use serde_json::{json, Value};
use tokio::process::Command;

pub struct MakiHarness;

/// Native subagent dispatch (`Task`) and the plugin memory store (`Memory`)
/// both act outside this rat's registry/worktree boundary. `--no-plugins`
/// only stops user/project `init.lua` from loading — these two tools ship
/// with the Lua host itself and load regardless, so they are denied
/// explicitly on every launch (verified: `--disallowed-tools` removes them
/// from the advertised tool list in `system/init`).
const DENIED_TOOLS: &str = "Task,Memory";

fn permission_args(permission_mode: Option<&str>) -> Vec<String> {
    match permission_mode {
        Some("bypassPermissions") | Some("danger-full-access") => {
            vec!["--dangerously-skip-permissions".into()]
        }
        Some(mode) => vec!["--permission-mode".into(), mode.into()],
        None => Vec::new(),
    }
}

fn launch_args(spec: &LaunchSpec) -> Vec<String> {
    let mut args: Vec<String> = [
        "--print",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--no-plugins",
        "--no-commands",
        "--disallowed-tools",
        DENIED_TOOLS,
    ]
    .into_iter()
    .map(String::from)
    .collect();
    if let Some(system) = &spec.system_prompt {
        args.push("--append-system-prompt".into());
        args.push(system.clone());
    }
    args.extend(permission_args(spec.permission_mode.as_deref()));
    if let Some(model) = &spec.model {
        args.push("--model".into());
        args.push(model.clone());
    }
    if let Some(session) = &spec.resume_session {
        args.push("--session".into());
        args.push(session.clone());
    }
    args
}

impl Harness for MakiHarness {
    fn kind(&self) -> &'static str {
        "maki"
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
        let binary = spec
            .env
            .get("RK_MAKI_BIN")
            .cloned()
            .or_else(|| std::env::var("RK_MAKI_BIN").ok())
            .unwrap_or_else(|| "maki".into());
        let mut cmd = Command::new(binary);
        cmd.args(launch_args(spec));
        cmd.current_dir(&spec.cwd);
        cmd.envs(&spec.env);

        let session = runner::launch(runner::Wiring {
            command: cmd,
            parse: parse_event_line,
            steer_line: Some(control_message_line),
            resume: None,
        })?;
        let session = crate::watch_pre_work_transport_failure("maki", session);

        // Stream-json input mode has no CLI-argument prompt: the initial
        // task is delivered as the first control message, same as Claude.
        let prompt = spec.prompt.clone();
        let control = session.control.clone();
        tokio::spawn(async move {
            let _ = control.steer(&prompt).await;
        });

        Ok(session)
    }
}

/// Wrap a control envelope as a stream-json user message line. Maki only
/// reads `message.content`; the `rk_control` metadata rides along anyway
/// (Maki ignores unrecognized top-level keys, verified against the
/// installed binary) so the wire shape stays identical to Claude's and
/// costs nothing if Maki ever starts honoring it.
fn control_message_line(envelope: &ControlEnvelope) -> String {
    json!({
        "type": "user",
        "message": {
            "role": "user",
            "content": [{"type": "text", "text": envelope.text}],
        },
        "metadata": {"rk_control": envelope},
        "rk_control": envelope,
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

/// Map one stream-json line to normalized events. Unknown lines are ignored
/// — forward compatibility over strictness, same policy as the Claude
/// adapter.
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
            // Maki serializes unpriced/failed turns (e.g. OAuth-backed
            // models, or a request that errored before any billing) as
            // `total_cost_usd: 0`. Treating that zero as an authoritative
            // cost would let it overwrite RK's incremental estimate and
            // could defeat a USD budget cap, so only a strictly positive
            // self-reported cost is trusted; token usage is always kept.
            cost_usd: v["total_cost_usd"].as_f64().filter(|c| *c > 0.0),
            session_id: v["session_id"].as_str().map(String::from),
        }),
        _ => {}
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TransportClass;
    use std::collections::HashMap;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    /// Spawn a fake `maki` binary (via `RK_MAKI_BIN`) that behaves per
    /// `script`, and drain its events with a bound so a hung fixture cannot
    /// hang the test suite.
    async fn run_fake(script: &str) -> Vec<HarnessEvent> {
        run_fake_with_spec(
            script,
            LaunchSpec {
                prompt: "do the task".into(),
                ..Default::default()
            },
        )
        .await
    }

    async fn run_fake_with_spec(script: &str, mut spec: LaunchSpec) -> Vec<HarnessEvent> {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("maki-fake");
        fs::write(&binary, format!("#!/bin/sh\n{script}\n")).unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
        spec.env
            .insert("RK_MAKI_BIN".into(), binary.to_string_lossy().into_owned());
        spec.cwd = dir.path().to_path_buf();
        let mut session = MakiHarness.launch(&spec).unwrap();
        let mut events = Vec::new();
        while let Some(event) = tokio::time::timeout(Duration::from_secs(5), session.events.recv())
            .await
            .expect("fixture must not hang")
        {
            let exited = matches!(event, HarnessEvent::Exited { .. });
            events.push(event);
            if exited {
                break;
            }
        }
        events
    }

    fn transport_failure(events: &[HarnessEvent]) -> Option<&crate::TransportOutcome> {
        events.iter().find_map(|e| match e {
            HarnessEvent::TransportFailure { outcome } => Some(outcome),
            _ => None,
        })
    }

    #[tokio::test]
    async fn pre_work_certificate_failure_is_classified_before_exit() {
        let events = run_fake("echo 'unable to get local issuer certificate' >&2; exit 1").await;
        let outcome = transport_failure(&events).expect("must classify a transport failure");
        assert_eq!(outcome.provider, "maki");
        assert_eq!(outcome.class, TransportClass::Certificate);
        assert!(outcome.retryable);
        let failure_idx = events
            .iter()
            .position(|e| matches!(e, HarnessEvent::TransportFailure { .. }))
            .unwrap();
        let exited_idx = events
            .iter()
            .position(|e| matches!(e, HarnessEvent::Exited { .. }))
            .unwrap();
        assert!(failure_idx < exited_idx);
    }

    #[tokio::test]
    async fn pre_work_authentication_failure_is_classified_as_not_retryable() {
        let events = run_fake("echo '401 Unauthorized: invalid api key' >&2; exit 1").await;
        let outcome = transport_failure(&events).expect("must classify");
        assert_eq!(outcome.class, TransportClass::Authentication);
        assert!(!outcome.retryable);
    }

    #[tokio::test]
    async fn pre_work_unavailable_failure_is_classified() {
        let events = run_fake("echo '503 Service Unavailable' >&2; exit 1").await;
        let outcome = transport_failure(&events).expect("must classify");
        assert_eq!(outcome.class, TransportClass::Unavailable);
        assert!(outcome.retryable);
    }

    #[tokio::test]
    async fn pre_work_generic_transport_failure_is_classified() {
        let events = run_fake("echo 'connect ECONNRESET 1.2.3.4:443' >&2; exit 1").await;
        let outcome = transport_failure(&events).expect("must classify");
        assert_eq!(outcome.class, TransportClass::Generic);
        assert!(outcome.retryable);
    }

    #[tokio::test]
    async fn ordinary_pre_work_failure_is_not_classified_as_transport() {
        let events = run_fake("echo 'error: unrecognized flag --bogus' >&2; exit 2").await;
        assert!(
            transport_failure(&events).is_none(),
            "an unrelated CLI error must not be misclassified as a transport failure"
        );
    }

    #[tokio::test]
    async fn transport_vocabulary_after_started_is_not_classified() {
        let events = run_fake(
            r#"echo '{"type":"system","subtype":"init","session_id":"s-1"}'
echo 'note: certificate rotation scheduled for next week' >&2
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s-1","total_cost_usd":0.01,"usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'"#,
        )
        .await;
        assert!(
            transport_failure(&events).is_none(),
            "a Started generation must never be reclassified as a pre-work transport failure"
        );
        assert!(events
            .iter()
            .any(|e| matches!(e, HarnessEvent::Started { .. })));
        assert!(events.iter().any(|e| matches!(
            e,
            HarnessEvent::Completed {
                is_error: false,
                ..
            }
        )));
    }

    #[test]
    fn init_line_yields_started_with_session() {
        let line = r#"{"type":"system","subtype":"init","session_id":"CeoFm4BBMyofnkysbhxaH","model":"anthropic/claude-fable-5.1","permissionMode":"bypassPermissions"}"#;
        let events = parse_event_line(line);
        assert!(matches!(
            &events[..],
            [HarnessEvent::Started { session_id: Some(s) }] if s == "CeoFm4BBMyofnkysbhxaH"
        ));
    }

    #[test]
    fn assistant_line_yields_text_tools_and_usage() {
        let line = r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"working on it"},{"type":"tool_use","id":"toolu_1","name":"Read","input":{"path":"test.txt"}}],"usage":{"input_tokens":100,"output_tokens":20,"cache_read_input_tokens":5000,"cache_creation_input_tokens":10}}}"#;
        let events = parse_event_line(line);
        assert_eq!(events.len(), 3);
        assert!(
            matches!(&events[0], HarnessEvent::AssistantText { text } if text == "working on it")
        );
        assert!(matches!(&events[1], HarnessEvent::ToolUse { name } if name == "Read"));
        assert!(matches!(
            &events[2],
            HarnessEvent::Usage { usage } if usage.cache_read == 5000 && usage.total() == 5130
        ));
    }

    /// The `user`/`tool_result` echo Maki writes back to stdout (observed
    /// against the real binary) must be ignored like any other unknown
    /// record — it is conversation history, not a normalized event.
    #[test]
    fn tool_result_echo_and_junk_are_ignored() {
        assert!(parse_event_line(
            r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"1: hello"}]}}"#
        )
        .is_empty());
        assert!(parse_event_line("not json").is_empty());
        assert!(parse_event_line(r#"{"type":"stream_event"}"#).is_empty());
        assert!(parse_event_line(r#"{"no_type_field":true}"#).is_empty());
    }

    #[test]
    fn result_line_yields_completed_with_positive_cost() {
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

    /// An unpriced or failed-before-billing turn serializes
    /// `total_cost_usd: 0` — this must never be trusted as a real cost (it
    /// would overwrite RK's incremental estimate and could defeat a budget
    /// cap), while token usage is still preserved.
    #[test]
    fn zero_cost_result_maps_to_no_cost_but_keeps_usage() {
        let line = r#"{"type":"result","subtype":"error_during_execution","is_error":true,"result":"API error (402): insufficient credit","session_id":"s-1","total_cost_usd":0.0,"usage":{"input_tokens":0,"output_tokens":0,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}"#;
        let events = parse_event_line(line);
        let [HarnessEvent::Completed {
            is_error, cost_usd, ..
        }] = &events[..]
        else {
            panic!("expected Completed, got {events:?}");
        };
        assert!(is_error);
        assert_eq!(*cost_usd, None);
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
    }

    #[test]
    fn steer_message_is_valid_stream_json() {
        let envelope = ControlEnvelope::new(
            "msg-1",
            "operator",
            "Whisker",
            "2026-08-21T12:00:00Z",
            "spawn-1",
            "please also run the tests",
        );
        let line = control_message_line(&envelope);
        let v: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["type"], "user");
        assert_eq!(
            v["message"]["content"][0]["text"],
            "please also run the tests"
        );
        assert_eq!(v["rk_control"]["schema"], "rk.control.v1");
        assert_eq!(v["rk_control"]["message_id"], "msg-1");
        assert_eq!(v["metadata"]["rk_control"]["sender"], "operator");
        assert!(!line.contains('\n'), "must be a single line");
    }

    #[test]
    fn autonomous_modes_bypass_all_maki_permission_checks() {
        for mode in ["bypassPermissions", "danger-full-access"] {
            assert_eq!(
                permission_args(Some(mode)),
                vec!["--dangerously-skip-permissions"]
            );
        }
        assert_eq!(
            permission_args(Some("acceptEdits")),
            vec!["--permission-mode", "acceptEdits"]
        );
        assert_eq!(permission_args(None), Vec::<String>::new());
    }

    #[test]
    fn launch_always_denies_native_task_and_memory_tools() {
        let spec = LaunchSpec {
            prompt: "do the task".into(),
            permission_mode: Some("bypassPermissions".into()),
            model: Some("anthropic/claude-fable-5.1".into()),
            resume_session: Some("sess-1".into()),
            ..Default::default()
        };
        let args = launch_args(&spec);
        let idx = args
            .iter()
            .position(|a| a == "--disallowed-tools")
            .expect("--disallowed-tools must always be present");
        assert_eq!(args[idx + 1], "Task,Memory");
    }

    #[tokio::test]
    async fn caps_do_not_advertise_trusted_mid_session_steering_or_self_reported_cost() {
        let caps = MakiHarness.caps();
        assert!(!caps.steer);
        assert!(!caps.reports_cost_usd);
        assert!(caps.interrupt);
        assert!(caps.resume);
    }

    #[tokio::test]
    async fn launch_delivers_exact_argv_and_the_initial_prompt_as_first_stream_json_message() {
        let dir = tempfile::tempdir().unwrap();
        let fake_maki = dir.path().join("fake-maki");
        let args_file = dir.path().join("args");
        let stdin_file = dir.path().join("stdin");
        fs::write(
            &fake_maki,
            format!(
                r#"#!/bin/bash
printf '%s\036' "$@" > "{args}"
IFS= read -r line
printf '%s\n' "$line" > "{stdin}"
echo '{{"type":"system","subtype":"init","session_id":"maki-session-1"}}'
echo '{{"type":"assistant","message":{{"role":"assistant","content":[{{"type":"text","text":"working"}}],"usage":{{"input_tokens":10,"output_tokens":2,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}}}'
echo '{{"type":"result","subtype":"success","is_error":false,"result":"working","session_id":"maki-session-1","total_cost_usd":0.01,"usage":{{"input_tokens":10,"output_tokens":2,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
"#,
                args = args_file.to_string_lossy(),
                stdin = stdin_file.to_string_lossy(),
            ),
        )
        .unwrap();
        fs::set_permissions(&fake_maki, fs::Permissions::from_mode(0o755)).unwrap();

        let mut env = HashMap::new();
        env.insert(
            "RK_MAKI_BIN".into(),
            fake_maki.to_string_lossy().into_owned(),
        );
        let spec = LaunchSpec {
            prompt: "do the task".into(),
            system_prompt: Some("be a rat".into()),
            cwd: dir.path().to_path_buf(),
            env,
            model: Some("anthropic/claude-fable-5.1".into()),
            resume_session: Some("old-session".into()),
            permission_mode: Some("danger-full-access".into()),
        };
        let mut session = MakiHarness.launch(&spec).unwrap();

        let mut text = Vec::new();
        let mut completed = None;
        let mut control_delivered = 0;
        while let Some(event) = tokio::time::timeout(Duration::from_secs(5), session.events.recv())
            .await
            .expect("fixture must not hang")
        {
            match event {
                HarnessEvent::AssistantText { text: chunk } => text.push(chunk),
                HarnessEvent::Completed {
                    result, session_id, ..
                } => completed = Some((result, session_id)),
                HarnessEvent::ControlDelivered { .. } => control_delivered += 1,
                HarnessEvent::Exited { .. } => break,
                _ => {}
            }
        }

        assert_eq!(text, ["working"]);
        let (result, session_id) = completed.expect("completed");
        assert_eq!(result, "working");
        assert_eq!(session_id.as_deref(), Some("maki-session-1"));
        assert_eq!(
            control_delivered, 1,
            "the initial prompt is one delivered control message"
        );

        let bytes = fs::read(&args_file).unwrap();
        let args: Vec<_> = bytes
            .split(|byte| *byte == 0x1e)
            .filter(|arg| !arg.is_empty())
            .map(|arg| String::from_utf8(arg.to_vec()).unwrap())
            .collect();
        assert_eq!(
            args,
            [
                "--print",
                "--input-format",
                "stream-json",
                "--output-format",
                "stream-json",
                "--no-plugins",
                "--no-commands",
                "--disallowed-tools",
                "Task,Memory",
                "--append-system-prompt",
                "be a rat",
                "--dangerously-skip-permissions",
                "--model",
                "anthropic/claude-fable-5.1",
                "--session",
                "old-session",
            ]
        );

        let stdin_line = fs::read_to_string(&stdin_file).unwrap();
        let stdin_json: Value = serde_json::from_str(stdin_line.trim()).unwrap();
        assert_eq!(stdin_json["type"], "user");
        assert_eq!(stdin_json["message"]["content"][0]["text"], "do the task");
    }

    #[tokio::test]
    async fn nonzero_exit_surfaces_as_exited_with_code() {
        let events = run_fake("exit 7").await;
        assert!(matches!(
            events.last(),
            Some(HarnessEvent::Exited { code: Some(7) })
        ));
    }
}
