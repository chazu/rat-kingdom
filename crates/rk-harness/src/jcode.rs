//! jcode adapter: one-shot `run --ndjson` event stream.
//!
//! jcode has no separate system-prompt argument in run mode, so Rat Kingdom's
//! role instructions are prepended to the task. Its native swarm and auto-poke
//! loops are disabled for managed rats: worktree isolation, delegation, and
//! completion belong to the Rat Kingdom supervisor.

use crate::{runner, Harness, HarnessCaps, HarnessEvent, HarnessSession, LaunchSpec, TokenUsage};
use serde_json::Value;
use tokio::process::Command;
use tokio::sync::mpsc;

pub struct JcodeHarness;

impl Harness for JcodeHarness {
    fn kind(&self) -> &'static str {
        "jcode"
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
        let bin = spec
            .env
            .get("RK_JCODE_BIN")
            .cloned()
            .or_else(|| std::env::var("RK_JCODE_BIN").ok())
            .unwrap_or_else(|| "jcode".into());
        let mut cmd = Command::new(bin);
        cmd.args(["--no-update", "--quiet"]);
        cmd.args(["-C", &spec.cwd.to_string_lossy()]);
        if let Some(model) = &spec.model {
            cmd.args(["--model", model]);
        }
        if let Some(session) = &spec.resume_session {
            cmd.args(["--resume", session]);
        }
        if spec.permission_mode.as_deref() == Some("read-only") {
            cmd.args([
                "--disable-base-tools",
                "--tools",
                rk_core::JCODE_READ_ONLY_TOOLS,
            ]);
        }
        cmd.args(["run", "--ndjson"]);

        let full_prompt = match &spec.system_prompt {
            Some(system) => format!("{system}\n\n---\n\n{}", spec.prompt),
            None => spec.prompt.clone(),
        };
        cmd.arg(full_prompt);
        cmd.current_dir(&spec.cwd);
        cmd.envs(&spec.env);
        // These are authority boundaries, not user preferences. A jcode swarm
        // would share this rat's worktree outside the registry, while auto-poke
        // can continue after the rat has fulfilled `rk done`.
        cmd.env("JCODE_SWARM_ENABLED", "0");
        cmd.env("JCODE_RUN_AUTO_POKE", "0");

        let session = runner::launch(runner::Wiring {
            command: cmd,
            parse: parse_event_line,
            steer_line: None,
        })?;
        Ok(post_process(session))
    }
}

fn usage_from_tokens(value: &Value) -> TokenUsage {
    TokenUsage {
        input: value["input"].as_u64().unwrap_or(0),
        output: value["output"].as_u64().unwrap_or(0),
        cache_read: value["cache_read_input"].as_u64().unwrap_or(0),
        cache_creation: value["cache_creation_input"].as_u64().unwrap_or(0),
    }
}

fn usage_from_done(value: &Value) -> TokenUsage {
    TokenUsage {
        input: value["input_tokens"].as_u64().unwrap_or(0),
        output: value["output_tokens"].as_u64().unwrap_or(0),
        cache_read: value["cache_read_input_tokens"].as_u64().unwrap_or(0),
        cache_creation: value["cache_creation_input_tokens"].as_u64().unwrap_or(0),
    }
}

/// Map jcode's public `run --ndjson` records onto normalized events.
///
/// `text_replace` is deliberately left to the authoritative `done.text`: the
/// normalized protocol has no replace operation, and replaying the replacement
/// as a delta would duplicate transcript text.
pub(crate) fn parse_event_line(line: &str) -> Vec<HarnessEvent> {
    let Ok(v) = serde_json::from_str::<Value>(line) else {
        return Vec::new();
    };
    match v["type"].as_str() {
        Some("start") => vec![HarnessEvent::Started {
            session_id: v["session_id"].as_str().map(String::from),
        }],
        Some("text_delta") => v["text"]
            .as_str()
            .map(|text| {
                vec![HarnessEvent::AssistantText {
                    text: text.to_string(),
                }]
            })
            .unwrap_or_default(),
        Some("tool_start") => vec![HarnessEvent::ToolUse {
            name: v["name"].as_str().unwrap_or("?").to_string(),
        }],
        Some("tokens") => vec![HarnessEvent::Usage {
            usage: usage_from_tokens(&v),
        }],
        Some("done") => vec![HarnessEvent::Completed {
            result: v["text"].as_str().unwrap_or_default().to_string(),
            is_error: false,
            usage: usage_from_done(&v["usage"]),
            cost_usd: None,
            session_id: v["session_id"].as_str().map(String::from),
        }],
        Some("error") => vec![HarnessEvent::Retry {
            attempt: 0,
            error: v["message"].as_str().unwrap_or("unknown").to_string(),
        }],
        _ => Vec::new(),
    }
}

async fn flush_text(
    tx: &mpsc::Sender<HarnessEvent>,
    pending: &mut String,
) -> Result<(), mpsc::error::SendError<HarnessEvent>> {
    if pending.is_empty() {
        return Ok(());
    }
    tx.send(HarnessEvent::AssistantText {
        text: std::mem::take(pending),
    })
    .await
}

/// Coalesce token-sized text deltas for useful bounded transcripts, accumulate
/// per-request usage, and guarantee a terminal result even on CLI failure.
fn post_process(mut session: HarnessSession) -> HarnessSession {
    let (tx, rx) = mpsc::channel::<HarnessEvent>(256);
    tokio::spawn(async move {
        let mut pending_text = String::new();
        let mut full_text = String::new();
        let mut total = TokenUsage::default();
        let mut session_id: Option<String> = None;
        let mut last_error: Option<String> = None;
        let mut completed = false;

        while let Some(event) = session.events.recv().await {
            match event {
                HarnessEvent::Started { session_id: sid } => {
                    session_id = sid.clone();
                    if tx
                        .send(HarnessEvent::Started { session_id: sid })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                HarnessEvent::AssistantText { text } => {
                    pending_text.push_str(&text);
                    full_text.push_str(&text);
                }
                HarnessEvent::ToolUse { name } => {
                    if flush_text(&tx, &mut pending_text).await.is_err()
                        || tx.send(HarnessEvent::ToolUse { name }).await.is_err()
                    {
                        break;
                    }
                }
                HarnessEvent::Usage { usage } => {
                    total.add(&usage);
                    if flush_text(&tx, &mut pending_text).await.is_err()
                        || tx.send(HarnessEvent::Usage { usage }).await.is_err()
                    {
                        break;
                    }
                }
                HarnessEvent::Retry { attempt, error } => {
                    last_error = Some(error.clone());
                    if flush_text(&tx, &mut pending_text).await.is_err()
                        || tx
                            .send(HarnessEvent::Retry { attempt, error })
                            .await
                            .is_err()
                    {
                        break;
                    }
                }
                HarnessEvent::Completed {
                    result,
                    is_error,
                    usage,
                    cost_usd,
                    session_id: completed_session,
                } => {
                    if flush_text(&tx, &mut pending_text).await.is_err() {
                        break;
                    }
                    completed = true;
                    if completed_session.is_some() {
                        session_id = completed_session;
                    }
                    let usage = if total.total() > 0 { total } else { usage };
                    if tx
                        .send(HarnessEvent::Completed {
                            result,
                            is_error,
                            usage,
                            cost_usd,
                            session_id: session_id.clone(),
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                HarnessEvent::Stderr { text } => {
                    if flush_text(&tx, &mut pending_text).await.is_err()
                        || tx.send(HarnessEvent::Stderr { text }).await.is_err()
                    {
                        break;
                    }
                }
                HarnessEvent::Exited { code } => {
                    if flush_text(&tx, &mut pending_text).await.is_err() {
                        break;
                    }
                    if !completed {
                        let is_error = code != Some(0);
                        let result = if is_error {
                            last_error.clone().unwrap_or_else(|| full_text.clone())
                        } else {
                            full_text.clone()
                        };
                        if tx
                            .send(HarnessEvent::Completed {
                                result,
                                is_error,
                                usage: total,
                                cost_usd: None,
                                session_id: session_id.clone(),
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    let _ = tx.send(HarnessEvent::Exited { code }).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn parser_maps_public_ndjson_records() {
        let started = parse_event_line(
            r#"{"type":"start","session_id":"session-rat","provider":"openai","model":"gpt"}"#,
        );
        assert!(matches!(
            &started[..],
            [HarnessEvent::Started { session_id: Some(id) }] if id == "session-rat"
        ));

        let tool = parse_event_line(r#"{"type":"tool_start","id":"1","name":"bash"}"#);
        assert!(matches!(&tool[..], [HarnessEvent::ToolUse { name }] if name == "bash"));

        let tokens = parse_event_line(
            r#"{"type":"tokens","input":100,"output":20,"cache_read_input":30,"cache_creation_input":5}"#,
        );
        let [HarnessEvent::Usage { usage }] = &tokens[..] else {
            panic!("expected usage");
        };
        assert_eq!(
            *usage,
            TokenUsage {
                input: 100,
                output: 20,
                cache_read: 30,
                cache_creation: 5,
            }
        );

        let done = parse_event_line(
            r#"{"type":"done","session_id":"session-rat","text":"finished","usage":{"input_tokens":100,"output_tokens":20}}"#,
        );
        assert!(matches!(
            &done[..],
            [HarnessEvent::Completed { result, session_id: Some(id), .. }]
                if result == "finished" && id == "session-rat"
        ));
        assert!(parse_event_line(r#"{"type":"text_replace","text":"revised"}"#).is_empty());
    }

    #[tokio::test]
    async fn launch_uses_ndjson_resume_and_managed_lifecycle_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let fake_jcode = dir.path().join("fake-jcode");
        let args_file = dir.path().join("args");
        std::fs::write(
            &fake_jcode,
            r#"#!/bin/bash
test "$JCODE_SWARM_ENABLED" = 0 || exit 41
test "$JCODE_RUN_AUTO_POKE" = 0 || exit 42
printf '%s\036' "$@" > "$RK_JCODE_ARGS_FILE"
echo '{"type":"start","session_id":"jcode-session-1","provider":"openai","model":"gpt-test"}'
echo '{"type":"text_delta","text":"working "}'
echo '{"type":"tool_start","id":"tool-1","name":"bash"}'
echo '{"type":"tokens","input":10,"output":2,"cache_read_input":1,"cache_creation_input":0}'
echo '{"type":"text_delta","text":"done"}'
echo '{"type":"tokens","input":20,"output":3,"cache_read_input":4,"cache_creation_input":0}'
echo '{"type":"done","session_id":"jcode-session-1","text":"working done","usage":{"input_tokens":20,"output_tokens":3,"cache_read_input_tokens":4}}'
"#,
        )
        .unwrap();
        std::fs::set_permissions(&fake_jcode, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut env = HashMap::new();
        env.insert(
            "RK_JCODE_BIN".into(),
            fake_jcode.to_string_lossy().into_owned(),
        );
        env.insert(
            "RK_JCODE_ARGS_FILE".into(),
            args_file.to_string_lossy().into_owned(),
        );
        let spec = LaunchSpec {
            prompt: "do the task".into(),
            system_prompt: Some("be a rat".into()),
            cwd: dir.path().to_path_buf(),
            env,
            model: Some("gpt-test".into()),
            resume_session: Some("old-session".into()),
            permission_mode: Some("danger-full-access".into()),
        };
        let mut session = JcodeHarness.launch(&spec).unwrap();

        let mut text = Vec::new();
        let mut tools = Vec::new();
        let mut completed = None;
        let mut exit = None;
        while let Some(event) = session.events.recv().await {
            match event {
                HarnessEvent::AssistantText { text: chunk } => text.push(chunk),
                HarnessEvent::ToolUse { name } => tools.push(name),
                HarnessEvent::Completed {
                    result,
                    usage,
                    session_id,
                    ..
                } => completed = Some((result, usage, session_id)),
                HarnessEvent::Exited { code } => exit = code,
                _ => {}
            }
        }

        assert_eq!(text, ["working ", "done"]);
        assert_eq!(tools, ["bash"]);
        let (result, usage, session_id) = completed.expect("completed");
        assert_eq!(result, "working done");
        assert_eq!(
            usage,
            TokenUsage {
                input: 30,
                output: 5,
                cache_read: 5,
                cache_creation: 0,
            }
        );
        assert_eq!(session_id.as_deref(), Some("jcode-session-1"));
        assert_eq!(exit, Some(0));

        let bytes = std::fs::read(args_file).unwrap();
        let args: Vec<_> = bytes
            .split(|byte| *byte == 0x1e)
            .filter(|arg| !arg.is_empty())
            .map(|arg| String::from_utf8(arg.to_vec()).unwrap())
            .collect();
        assert_eq!(
            args,
            [
                "--no-update",
                "--quiet",
                "-C",
                dir.path().to_str().unwrap(),
                "--model",
                "gpt-test",
                "--resume",
                "old-session",
                "run",
                "--ndjson",
                "be a rat\n\n---\n\ndo the task",
            ]
        );
    }

    #[tokio::test]
    async fn read_only_launch_exposes_only_assessment_tools() {
        let dir = tempfile::tempdir().unwrap();
        let fake_jcode = dir.path().join("fake-jcode");
        let args_file = dir.path().join("args");
        std::fs::write(
            &fake_jcode,
            r#"#!/bin/bash
printf '%s\036' "$@" > "$RK_JCODE_ARGS_FILE"
echo '{"type":"done","text":"assessed"}'
"#,
        )
        .unwrap();
        std::fs::set_permissions(&fake_jcode, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut env = HashMap::new();
        env.insert(
            "RK_JCODE_BIN".into(),
            fake_jcode.to_string_lossy().into_owned(),
        );
        env.insert(
            "RK_JCODE_ARGS_FILE".into(),
            args_file.to_string_lossy().into_owned(),
        );
        let mut session = JcodeHarness
            .launch(&LaunchSpec {
                prompt: "assess".into(),
                cwd: dir.path().to_path_buf(),
                env,
                permission_mode: Some("read-only".into()),
                ..Default::default()
            })
            .unwrap();
        while session.events.recv().await.is_some() {}

        let bytes = std::fs::read(args_file).unwrap();
        let args: Vec<_> = bytes
            .split(|byte| *byte == 0x1e)
            .filter(|arg| !arg.is_empty())
            .map(|arg| String::from_utf8(arg.to_vec()).unwrap())
            .collect();
        assert_eq!(
            args,
            [
                "--no-update",
                "--quiet",
                "-C",
                dir.path().to_str().unwrap(),
                "--disable-base-tools",
                "--tools",
                rk_core::JCODE_READ_ONLY_TOOLS,
                "run",
                "--ndjson",
                "assess",
            ]
        );
        for forbidden in ["bash", "write", "edit", "apply_patch"] {
            assert!(!rk_core::JCODE_READ_ONLY_TOOLS
                .split(',')
                .any(|tool| tool == forbidden));
        }
    }

    #[tokio::test]
    async fn failed_cli_run_reports_the_ndjson_error() {
        let dir = tempfile::tempdir().unwrap();
        let fake_jcode = dir.path().join("fake-jcode");
        std::fs::write(
            &fake_jcode,
            "#!/bin/bash\necho '{\"type\":\"start\",\"session_id\":\"failed-session\"}'\necho '{\"type\":\"error\",\"message\":\"provider unavailable\"}'\nexit 7\n",
        )
        .unwrap();
        std::fs::set_permissions(&fake_jcode, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut env = HashMap::new();
        env.insert(
            "RK_JCODE_BIN".into(),
            fake_jcode.to_string_lossy().into_owned(),
        );
        let mut session = JcodeHarness
            .launch(&LaunchSpec {
                cwd: dir.path().to_path_buf(),
                env,
                ..Default::default()
            })
            .unwrap();

        let mut completed = None;
        while let Some(event) = session.events.recv().await {
            if let HarnessEvent::Completed {
                result, is_error, ..
            } = event
            {
                completed = Some((result, is_error));
            }
        }
        assert_eq!(completed, Some(("provider unavailable".to_string(), true)));
    }
}
