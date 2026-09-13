//! Claude Code adapter: headless stream-json in both directions.
//!
//! Launch shape:
//! `claude -p --output-format stream-json --input-format stream-json --verbose`
//! with the initial prompt delivered as the first stdin user message and role
//! instructions via `--append-system-prompt` — no TUI, no readiness probes.

use crate::{
    runner, ControlEnvelope, Harness, HarnessCaps, HarnessEvent, HarnessSession, LaunchSpec,
    TokenUsage,
};
use serde_json::{json, Value};
use tokio::process::Command;

pub struct ClaudeHarness;

/// Anthropic-hosted account connectors (Gmail/Calendar/Drive) are bound to
/// whatever Claude account is authenticated on the host, not to this rat's
/// task. They carry live read/write access to the operator's real inbox,
/// calendar, and files and are not declared in any project/user mcp.json, so
/// there is no config-side lever to scope them — deny them at the CLI level
/// on every spawn instead. Server names match the `mcp__<server>__<tool>`
/// tool-name prefixes these connectors register under.
const DENIED_MCP_SERVERS: &[&str] = &[
    "mcp__claude_ai_Gmail",
    "mcp__claude_ai_Google_Calendar",
    "mcp__claude_ai_Google_Drive",
];

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
        "-p",
        "--output-format",
        "stream-json",
        "--input-format",
        "stream-json",
        "--verbose",
        "--disallowedTools",
    ]
    .into_iter()
    .map(String::from)
    .collect();
    args.extend(DENIED_MCP_SERVERS.iter().map(|s| s.to_string()));
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
        args.push("--resume".into());
        args.push(session.clone());
    }
    args
}

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
        let binary = spec
            .env
            .get("RK_CLAUDE_BIN")
            .cloned()
            .or_else(|| std::env::var("RK_CLAUDE_BIN").ok())
            .unwrap_or_else(|| "claude".into());
        let mut cmd = Command::new(binary);
        cmd.args(launch_args(spec));
        cmd.current_dir(&spec.cwd);
        cmd.envs(&spec.env);

        let session = runner::launch(runner::Wiring {
            command: cmd,
            parse: Box::new(dedup_usage_parser()),
            steer_line: Some(control_message_line),
            resume: None,
        })?;
        let mut session = crate::watch_pre_work_transport_failure("claude", session);

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

/// Wrap a trusted control envelope as a stream-json user message line. The
/// metadata is carried beside the text so repository/tool output can never
/// manufacture an equivalent control frame.
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

/// The Anthropic API reports total cache-write tokens flat
/// (`cache_creation_input_tokens`) AND, alongside it, the TTL split that made
/// up that total (`cache_creation.ephemeral_{5m,1h}_input_tokens`). Older
/// server versions and fixtures may only carry the flat field — those `.as_u64()`
/// calls fall back to `0`, leaving the split unknown and the flat total priced
/// at the legacy/unknown rate (see `ModelPrice::cost`), not silently dropped.
fn usage_from(value: &Value) -> TokenUsage {
    TokenUsage {
        input: value["input_tokens"].as_u64().unwrap_or(0),
        output: value["output_tokens"].as_u64().unwrap_or(0),
        cache_read: value["cache_read_input_tokens"].as_u64().unwrap_or(0),
        cache_creation: value["cache_creation_input_tokens"].as_u64().unwrap_or(0),
        cache_creation_5m: value["cache_creation"]["ephemeral_5m_input_tokens"]
            .as_u64()
            .unwrap_or(0),
        cache_creation_1h: value["cache_creation"]["ephemeral_1h_input_tokens"]
            .as_u64()
            .unwrap_or(0),
    }
}

/// Map one stream-json line to normalized events. Unknown lines are ignored —
/// forward compatibility over strictness (the `capabilities` array in
/// `system/init` is the place to detect protocol growth).
pub(crate) fn parse_event_line(line: &str) -> Vec<HarnessEvent> {
    parse_event_line_with_message_id(line).0
}

/// Same mapping as [`parse_event_line`], plus the assistant message id the
/// line belongs to (when the line is an `assistant` record). Split out so
/// [`dedup_usage_parser`] can key on the id without re-parsing the line's
/// JSON a second time; `parse_event_line` itself stays a plain, stateless fn
/// — the `fake` adapter's test harness reuses it directly and must keep
/// emitting one `Usage` per line, unchanged.
fn parse_event_line_with_message_id(line: &str) -> (Vec<HarnessEvent>, Option<String>) {
    let Ok(v) = serde_json::from_str::<Value>(line) else {
        return (Vec::new(), None);
    };
    let message_id = v["message"]["id"].as_str().map(String::from);
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
    (events, message_id)
}

/// Per-launch usage dedup for the Claude stream-json protocol. One assistant
/// turn can be split across several stdout lines — one per content block
/// (text, tool_use, ...) — and the CLI repeats that turn's full usage
/// snapshot on every one of those lines. Counting `Usage` once per line (as
/// [`parse_event_line`] does on its own) multiplies a turn's real usage by
/// its content-block count. This wrapper forwards every event untouched
/// except a repeated `Usage` for a `message.id` already seen, which it
/// drops; `AssistantText`/`ToolUse`/everything else still comes through for
/// every line, so no observed output is lost. The final `result` line's
/// `Completed { usage, cost_usd, .. }` is a separate accounting scope (the
/// session-level total, reported once by the CLI already) and is never
/// touched here.
///
/// The seen-id set is a plain `HashSet`, not an evicting cache: an eviction
/// policy would let a forgotten id's usage be recorded again on a later
/// repeat, silently re-inflating the exact total this exists to fix. Its
/// size is bounded by one launch's distinct assistant turns, not by session
/// length, so it does not need one. Per-launch scope also matters in the
/// other direction — this closure is built fresh inside
/// [`ClaudeHarness::launch`] for every call, so a resumed or freshly spawned
/// generation always starts with an empty set and never inherits another
/// launch's seen ids.
///
/// A line whose assistant message carries no `id` (or that fails to parse
/// one) is never deduped — with nothing to key on, under-counting real usage
/// would be the worse failure mode, so the conservative default is to keep
/// every such `Usage` event rather than risk dropping one that was never
/// actually a repeat.
fn dedup_usage_parser() -> impl FnMut(&str) -> Vec<HarnessEvent> + Send {
    let mut seen_usage_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    move |line: &str| {
        let (mut events, message_id) = parse_event_line_with_message_id(line);
        let carries_usage = events
            .iter()
            .any(|event| matches!(event, HarnessEvent::Usage { .. }));
        // Only mark an id "seen" once a Usage for it has actually been kept —
        // never on bare id-sighting — so a message whose first line happens
        // to omit usage can't poison a later line that carries the real
        // snapshot into being wrongly treated as a repeat.
        if carries_usage {
            if let Some(id) = message_id {
                if !seen_usage_ids.insert(id) {
                    events.retain(|event| !matches!(event, HarnessEvent::Usage { .. }));
                }
            }
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TransportClass;
    use std::collections::HashMap;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    /// Spawn a fake `claude` binary (via `RK_CLAUDE_BIN`) that behaves per
    /// `script`, and drain its events with a bound so a hung fixture cannot
    /// hang the test suite.
    async fn run_fake(script: &str) -> Vec<HarnessEvent> {
        let dir = tempfile::tempdir().unwrap();
        let binary = dir.path().join("claude-fake");
        fs::write(&binary, format!("#!/bin/sh\n{script}\n")).unwrap();
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o755)).unwrap();
        let mut env = HashMap::new();
        env.insert(
            "RK_CLAUDE_BIN".into(),
            binary.to_string_lossy().into_owned(),
        );
        let mut session = ClaudeHarness
            .launch(&LaunchSpec {
                prompt: "do the task".into(),
                cwd: dir.path().to_path_buf(),
                env,
                ..Default::default()
            })
            .unwrap();
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
        assert_eq!(outcome.provider, "claude");
        assert_eq!(outcome.class, TransportClass::Certificate);
        assert!(outcome.retryable);
        // Order: the failure must be visible before Exited, never after.
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

    /// An ordinary pre-`Started` launch failure (bad flag, misconfiguration)
    /// that carries none of the known transport signals must NOT be
    /// classified — it is left to ordinary failure handling, unchanged.
    #[tokio::test]
    async fn ordinary_pre_work_failure_is_not_classified_as_transport() {
        let events = run_fake("echo 'error: unrecognized flag --bogus' >&2; exit 2").await;
        assert!(
            transport_failure(&events).is_none(),
            "an unrelated CLI error must not be misclassified as a transport failure"
        );
    }

    /// Once the harness has actually started (a real session, real work
    /// under way), a later stderr line that happens to contain transport
    /// vocabulary must not be classified — healthy success and ordinary
    /// task-failure behavior after work has begun is unchanged.
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
    fn assistant_line_splits_cache_creation_by_ttl() {
        let line = r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"x"}],"usage":{"input_tokens":2,"output_tokens":183,"cache_read_input_tokens":10019,"cache_creation_input_tokens":54704,"cache_creation":{"ephemeral_5m_input_tokens":1000,"ephemeral_1h_input_tokens":53704}}}}"#;
        let events = parse_event_line(line);
        let [HarnessEvent::AssistantText { .. }, HarnessEvent::Usage { usage }] = &events[..]
        else {
            panic!("expected text + usage, got {events:?}");
        };
        assert_eq!(usage.cache_creation, 54704, "flat total unchanged");
        assert_eq!(usage.cache_creation_5m, 1000);
        assert_eq!(usage.cache_creation_1h, 53704);
        // The TTL split is a decomposition of cache_creation, not additional
        // tokens: total() must not double-count it.
        assert_eq!(usage.total(), 2 + 183 + 10019 + 54704);
    }

    /// Real-world shape observed from live Claude Code 2.1.270 streams: every
    /// cache write lands in the 1h bucket, none in 5m. The flat total must
    /// still equal the (5m + 1h) split exactly, so `ModelPrice::cost` treats
    /// none of it as TTL-unknown.
    #[test]
    fn assistant_line_handles_cache_creation_exclusively_1h() {
        let line = r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"x"}],"usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":2059,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":2059}}}}"#;
        let events = parse_event_line(line);
        let [_, HarnessEvent::Usage { usage }] = &events[..] else {
            panic!("expected text + usage, got {events:?}");
        };
        assert_eq!(usage.cache_creation_5m, 0);
        assert_eq!(usage.cache_creation_1h, 2059);
        assert_eq!(usage.cache_creation, 2059);
    }

    /// A line with only the legacy flat field (no `cache_creation` object at
    /// all — older CLI versions, or any fixture predating the TTL split) must
    /// still parse: the split stays at its zero default and the full amount
    /// is treated as TTL-unknown by pricing, never dropped.
    #[test]
    fn assistant_line_without_ttl_split_leaves_it_zero() {
        let line = r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"x"}],"usage":{"input_tokens":100,"output_tokens":20,"cache_read_input_tokens":5000,"cache_creation_input_tokens":10}}}"#;
        let events = parse_event_line(line);
        let [_, HarnessEvent::Usage { usage }] = &events[..] else {
            panic!("expected text + usage, got {events:?}");
        };
        assert_eq!(usage.cache_creation, 10);
        assert_eq!(usage.cache_creation_5m, 0);
        assert_eq!(usage.cache_creation_1h, 0);
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

    /// The bug this ticket fixes: the real Claude CLI splits one assistant
    /// turn across multiple stream-json lines (one per content block) and
    /// repeats that turn's full usage snapshot on every line. Without dedup,
    /// `parse_event_line` alone (what `dedup_usage_parser` wraps) would
    /// report Usage twice here — this asserts that duplicated-fixture
    /// behavior directly, so the case this ticket exists to fix is pinned
    /// even though `parse_event_line` itself must stay stateless and
    /// unchanged for `fake.rs`.
    #[test]
    fn undeduped_parser_double_counts_a_repeated_message_id() {
        let text_line = r#"{"type":"assistant","message":{"id":"msg-dup","content":[{"type":"text","text":"first"}],"usage":{"input_tokens":2,"output_tokens":183,"cache_read_input_tokens":10019,"cache_creation_input_tokens":54704}}}"#;
        let tool_line = r#"{"type":"assistant","message":{"id":"msg-dup","content":[{"type":"tool_use","name":"Bash","id":"t1","input":{}}],"usage":{"input_tokens":2,"output_tokens":183,"cache_read_input_tokens":10019,"cache_creation_input_tokens":54704}}}"#;
        let usage_count = |events: &[HarnessEvent]| {
            events
                .iter()
                .filter(|e| matches!(e, HarnessEvent::Usage { .. }))
                .count()
        };
        let mut all = parse_event_line(text_line);
        all.extend(parse_event_line(tool_line));
        assert_eq!(
            usage_count(&all),
            2,
            "documents the pre-fix bug: the stateless parser has no way to know these two lines belong to the same turn"
        );
    }

    #[test]
    fn dedup_parser_collapses_repeated_message_id_usage_but_keeps_all_content() {
        let text_line = r#"{"type":"assistant","message":{"id":"msg-dup","content":[{"type":"text","text":"first"}],"usage":{"input_tokens":2,"output_tokens":183,"cache_read_input_tokens":10019,"cache_creation_input_tokens":54704}}}"#;
        let tool_line = r#"{"type":"assistant","message":{"id":"msg-dup","content":[{"type":"tool_use","name":"Bash","id":"t1","input":{}}],"usage":{"input_tokens":2,"output_tokens":183,"cache_read_input_tokens":10019,"cache_creation_input_tokens":54704}}}"#;

        let mut parse = dedup_usage_parser();
        let first = parse(text_line);
        let second = parse(tool_line);

        assert!(
            first
                .iter()
                .any(|e| matches!(e, HarnessEvent::AssistantText { text } if text == "first")),
            "text block must still come through"
        );
        assert_eq!(
            first
                .iter()
                .filter(|e| matches!(e, HarnessEvent::Usage { .. }))
                .count(),
            1,
            "first sighting of the id must keep its Usage"
        );
        assert!(
            second
                .iter()
                .any(|e| matches!(e, HarnessEvent::ToolUse { name } if name == "Bash")),
            "tool block must still come through"
        );
        assert!(
            !second
                .iter()
                .any(|e| matches!(e, HarnessEvent::Usage { .. })),
            "repeat sighting of the same id must drop its duplicate Usage"
        );
    }

    #[test]
    fn dedup_parser_counts_distinct_message_ids_separately() {
        let first_line = r#"{"type":"assistant","message":{"id":"msg-a","content":[{"type":"text","text":"a"}],"usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}"#;
        let second_line = r#"{"type":"assistant","message":{"id":"msg-b","content":[{"type":"text","text":"b"}],"usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}"#;

        let mut parse = dedup_usage_parser();
        let first = parse(first_line);
        let second = parse(second_line);

        assert!(first
            .iter()
            .any(|e| matches!(e, HarnessEvent::Usage { .. })));
        assert!(
            second
                .iter()
                .any(|e| matches!(e, HarnessEvent::Usage { .. })),
            "a different message id is a distinct turn, not a repeat"
        );
    }

    #[test]
    fn dedup_parser_never_inherits_state_across_a_fresh_launch() {
        let line = r#"{"type":"assistant","message":{"id":"msg-shared","content":[{"type":"text","text":"x"}],"usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}"#;

        let mut first_launch = dedup_usage_parser();
        assert!(first_launch(line)
            .iter()
            .any(|e| matches!(e, HarnessEvent::Usage { .. })));
        assert!(
            !first_launch(line)
                .iter()
                .any(|e| matches!(e, HarnessEvent::Usage { .. })),
            "same parser, same id repeated: usage must be dropped"
        );

        // A fresh call to dedup_usage_parser() — what ClaudeHarness::launch
        // does on every invocation — starts a brand new seen-id set. The
        // same message id that was already a "repeat" to `first_launch` must
        // still be counted fresh here.
        let mut second_launch = dedup_usage_parser();
        assert!(
            second_launch(line)
                .iter()
                .any(|e| matches!(e, HarnessEvent::Usage { .. })),
            "a new launch's parser must not inherit a previous launch's seen ids"
        );
    }

    #[test]
    fn dedup_parser_never_drops_usage_when_message_id_is_missing() {
        let line = r#"{"type":"assistant","message":{"content":[{"type":"text","text":"no id here"}],"usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}"#;

        let mut parse = dedup_usage_parser();
        for _ in 0..3 {
            let events = parse(line);
            assert!(
                events
                    .iter()
                    .any(|e| matches!(e, HarnessEvent::Usage { .. })),
                "with no id to key on, the conservative choice is to never drop a Usage event"
            );
        }
    }

    #[test]
    fn dedup_parser_leaves_the_final_result_usage_and_cost_untouched() {
        let text_line = r#"{"type":"assistant","message":{"id":"msg-dup","content":[{"type":"text","text":"work"}],"usage":{"input_tokens":2,"output_tokens":183,"cache_read_input_tokens":10019,"cache_creation_input_tokens":54704}}}"#;
        let tool_line = r#"{"type":"assistant","message":{"id":"msg-dup","content":[{"type":"tool_use","name":"Bash","id":"t1","input":{}}],"usage":{"input_tokens":2,"output_tokens":183,"cache_read_input_tokens":10019,"cache_creation_input_tokens":54704}}}"#;
        let result_line = r#"{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"abc-123","total_cost_usd":0.4523,"usage":{"input_tokens":2000,"output_tokens":900,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}"#;

        let mut parse = dedup_usage_parser();
        let _ = parse(text_line);
        let _ = parse(tool_line);
        let result_events = parse(result_line);

        let [HarnessEvent::Completed {
            cost_usd, usage, ..
        }] = &result_events[..]
        else {
            panic!("expected Completed, got {result_events:?}");
        };
        assert_eq!(*cost_usd, Some(0.4523));
        assert_eq!(usage.output, 900);
    }

    /// End-to-end through the real runner: a fake `claude` binary emits one
    /// assistant turn as two stream-json lines sharing a message id (mirroring
    /// the actual CLI's per-content-block framing) each carrying the full
    /// turn usage, then a distinct second turn, then the result line. Exercises
    /// `ClaudeHarness::launch` -> `runner::launch` -> the boxed dedup closure,
    /// not just the pure parser function.
    #[tokio::test]
    async fn real_subprocess_stream_deduplicates_usage_through_the_runner() {
        let events = run_fake(
            r#"echo '{"type":"system","subtype":"init","session_id":"s-1"}'
read -r _first_message
echo '{"type":"assistant","message":{"id":"msg-1","content":[{"type":"text","text":"working"}],"usage":{"input_tokens":2,"output_tokens":183,"cache_read_input_tokens":10019,"cache_creation_input_tokens":54704}}}'
echo '{"type":"assistant","message":{"id":"msg-1","content":[{"type":"tool_use","name":"Bash","id":"t1","input":{}}],"usage":{"input_tokens":2,"output_tokens":183,"cache_read_input_tokens":10019,"cache_creation_input_tokens":54704}}}'
echo '{"type":"assistant","message":{"id":"msg-2","content":[{"type":"text","text":"done thinking"}],"usage":{"input_tokens":5,"output_tokens":10,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s-1","total_cost_usd":0.4523,"usage":{"input_tokens":2000,"output_tokens":900,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'"#,
        )
        .await;

        let text_count = events
            .iter()
            .filter(|e| matches!(e, HarnessEvent::AssistantText { .. }))
            .count();
        let tool_count = events
            .iter()
            .filter(|e| matches!(e, HarnessEvent::ToolUse { .. }))
            .count();
        let usage_events: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                HarnessEvent::Usage { usage } => Some(usage),
                _ => None,
            })
            .collect();

        assert_eq!(text_count, 2, "both AssistantText blocks must still arrive");
        assert_eq!(tool_count, 1, "the ToolUse block must still arrive");
        assert_eq!(
            usage_events.len(),
            2,
            "msg-1's usage counted once despite two lines, plus msg-2's own usage"
        );
        assert_eq!(usage_events[0].output, 183, "msg-1's usage, kept once");
        assert_eq!(usage_events[1].output, 10, "msg-2's distinct usage");

        let completed = events.iter().find_map(|e| match e {
            HarnessEvent::Completed {
                cost_usd, usage, ..
            } => Some((*cost_usd, usage.output)),
            _ => None,
        });
        assert_eq!(
            completed,
            Some((Some(0.4523), 900)),
            "the final result's own usage/cost is untouched by turn-level dedup"
        );
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
    fn autonomous_modes_bypass_all_claude_permission_checks() {
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
    }

    #[test]
    fn launch_always_denies_account_connectors() {
        let spec = LaunchSpec {
            prompt: "do the task".into(),
            permission_mode: Some("bypassPermissions".into()),
            model: Some("claude-x".into()),
            resume_session: Some("sess-1".into()),
            ..Default::default()
        };
        let args = launch_args(&spec);
        let idx = args
            .iter()
            .position(|a| a == "--disallowedTools")
            .expect("--disallowedTools must always be present");
        for (offset, server) in DENIED_MCP_SERVERS.iter().enumerate() {
            assert_eq!(args[idx + 1 + offset], *server);
        }
    }
}
