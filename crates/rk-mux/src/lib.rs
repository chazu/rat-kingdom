//! herdr integration: run rats in herdr panes so humans can watch, attach,
//! and take over — while control still flows through the daemon and the
//! tuplespace, never through keystroke scraping.
//!
//! Shell-out client (the herdr CLI mirrors its socket API 1:1 and shields us
//! from pre-1.0 protocol churn). Everything degrades gracefully: no herdr, no
//! attach surface, headless spawns unaffected.

use rk_core::notify::{EscalationNotice, NotificationSink};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};
use tracing::debug;

pub struct HerdrMux;

/// Identity of one detected agent generation, anchored by terminal and agent
/// kind. `session_id` stores either `agent-session:<value>` or the conservative
/// `revision:<number>` fallback. The field name is retained for state-file
/// compatibility, and the selected fence mode persists until re-registration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentIdentity {
    pub terminal_id: String,
    pub pane_id: String,
    pub session_id: String,
    pub agent: String,
    pub cwd: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentState {
    pub identity: AgentIdentity,
    pub status: String,
    pub focused: bool,
}

impl HerdrMux {
    /// Is a herdr server reachable?
    pub fn available() -> bool {
        Command::new("herdr")
            .args(["status", "server"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Start `argv` as a named agent in a new herdr pane. The name doubles as
    /// the herdr target for send/attach/close.
    pub fn start_agent(
        name: &str,
        cwd: &Path,
        env: &HashMap<String, String>,
        argv: &[String],
    ) -> rk_core::Result<String> {
        if argv.is_empty() {
            return Err(rk_core::Error::other("cannot start an empty agent argv"));
        }
        let mut create = vec![
            "workspace".to_string(),
            "create".to_string(),
            "--cwd".to_string(),
            cwd.to_string_lossy().to_string(),
            "--label".to_string(),
            name.to_string(),
            "--no-focus".to_string(),
        ];
        for (key, value) in env {
            create.push("--env".into());
            create.push(format!("{key}={value}"));
        }
        let created = run_herdr_owned(&create)?;
        let value: Value = serde_json::from_str(&created)
            .map_err(|e| rk_core::Error::other(format!("invalid herdr workspace response: {e}")))?;
        let workspace = find_string_key(&value, "workspace_id").ok_or_else(|| {
            rk_core::Error::other("herdr workspace response omitted workspace_id")
        })?;
        let pane = Self::snapshot()
            .and_then(|snapshot| {
                snapshot["result"]["snapshot"]["panes"]
                    .as_array()?
                    .iter()
                    .find(|p| p["workspace_id"].as_str() == Some(&workspace))
                    .and_then(|p| p["pane_id"].as_str().map(String::from))
            })
            .ok_or_else(|| rk_core::Error::other("new herdr workspace has no pane"))?;
        let started = retry_while_pane_busy(
            || Self::start_in_pane(name, &argv[0], &pane, &argv[1..]),
            PANE_READY_TIMEOUT,
            PANE_READY_POLL,
        );
        if let Err(error) = started {
            let _ = run_herdr(&["workspace", "close", &workspace]);
            return Err(error);
        }
        debug!(name, "started herdr pane");
        Ok(pane)
    }

    /// Submit one complete prompt atomically. Herdr owns readiness and Enter;
    /// splitting those operations reintroduces the timing race this API exists
    /// to avoid.
    ///
    pub fn send(target: &str, text: &str) -> rk_core::Result<()> {
        let target = &Self::agent_target(target);
        run_herdr(&["agent", "prompt", target, text])?;
        Ok(())
    }

    /// Resolve a stored identity field to a target Herdr accepts today.
    ///
    /// Herdr 0.8 stopped resolving `terminal_id` for `agent` subcommands
    /// (`agent_not_found`), while `pane_id` and the agent name still resolve.
    /// Registrations persist `terminal_id`, so map it back through the
    /// snapshot; pass anything already resolvable straight through.
    fn agent_target(target: &str) -> String {
        match Self::snapshot()
            .as_ref()
            .and_then(|snapshot| Self::agent_entry(snapshot, target))
            .and_then(|entry| entry["pane_id"].as_str().map(String::from))
        {
            Some(pane) => pane,
            None => target.to_string(),
        }
    }

    /// Submit and wait for a settled semantic state. Used for lifecycle
    /// commands where "text reached the pane" is not enough evidence.
    pub fn send_wait(target: &str, text: &str, timeout_ms: u64) -> rk_core::Result<()> {
        let timeout = timeout_ms.clamp(1_000, 300_000).to_string();
        run_herdr(&[
            "agent",
            "prompt",
            &Self::agent_target(target),
            text,
            "--wait",
            "--until",
            "idle",
            "--until",
            "done",
            "--timeout",
            &timeout,
        ])?;
        Ok(())
    }

    /// Resolve an operator-supplied label/pane/terminal/session to the exact
    /// terminal + agent-generation identity persisted by the King loop.
    pub fn identify(target: &str) -> rk_core::Result<AgentIdentity> {
        let snapshot =
            Self::snapshot().ok_or_else(|| rk_core::Error::other("cannot read herdr snapshot"))?;
        let entry = Self::agent_entry(&snapshot, target)
            .ok_or_else(|| rk_core::Error::other(format!("no herdr agent for {target}")))?;
        identity_from_entry(entry)
    }

    /// State for an exact registered generation. A new agent in the old pane
    /// is not silently treated as the same King.
    pub fn exact_state(identity: &AgentIdentity) -> Option<AgentState> {
        let snapshot = Self::snapshot()?;
        Self::exact_state_from_snapshot(&snapshot, identity)
    }

    fn exact_state_from_snapshot(snapshot: &Value, identity: &AgentIdentity) -> Option<AgentState> {
        // Names and labels may alias another terminal; they are lookup aids
        // for registration, never proof of an already registered identity.
        let entry = snapshot["result"]["snapshot"]["agents"]
            .as_array()?
            .iter()
            .find(|entry| entry["terminal_id"].as_str() == Some(&identity.terminal_id))?;
        if entry["agent"].as_str() != Some(&identity.agent) {
            return None;
        }
        let fence = if identity.session_id.starts_with("agent-session:") {
            reported_session_fence(entry)?
        } else if identity.session_id.starts_with("revision:") {
            revision_fence(entry)?
        } else {
            return None;
        };
        if fence != identity.session_id {
            return None;
        }
        let mut current = identity_from_entry(entry).ok()?;
        // Late session reporting must not silently upgrade a legacy identity.
        current.session_id = fence;
        Some(AgentState {
            identity: current,
            status: entry["agent_status"].as_str().unwrap_or("unknown").into(),
            focused: entry["focused"].as_bool().unwrap_or(false),
        })
    }

    /// Start a fresh harness generation in an existing shell pane.
    pub fn start_in_pane(
        name: &str,
        harness: &str,
        pane: &str,
        args: &[String],
    ) -> rk_core::Result<AgentIdentity> {
        let mut command = vec![
            "agent".to_string(),
            "start".to_string(),
            name.to_string(),
            "--kind".to_string(),
            harness.to_string(),
            "--pane".to_string(),
            pane.to_string(),
        ];
        if !args.is_empty() {
            command.push("--".into());
            command.extend(args.iter().cloned());
        }
        run_herdr_owned(&command)?;
        Self::identify(pane)
    }

    /// Exit one exact agent generation and start a new one in its pane.
    pub fn replace_agent(
        identity: &AgentIdentity,
        name: &str,
        harness: &str,
        args: &[String],
        timeout_ms: u64,
    ) -> rk_core::Result<AgentIdentity> {
        let current = Self::exact_state(identity).ok_or_else(|| {
            rk_core::Error::other("registered King generation is no longer present")
        })?;
        let timeout = timeout_ms.clamp(1_000, 300_000).to_string();
        // Target the pane: Herdr 0.8 no longer resolves `terminal_id` here.
        run_herdr(&[
            "agent",
            "prompt",
            &current.identity.pane_id,
            "/exit",
            "--wait",
            "--until",
            "done",
            "--timeout",
            &timeout,
        ])?;
        Self::start_in_pane(name, harness, &current.identity.pane_id, args)
    }

    /// Herdr's semantic state for the pane: idle|working|blocked|done|unknown.
    pub fn agent_status(target: &str) -> Option<String> {
        let snapshot = Self::snapshot()?;
        Self::agent_entry(&snapshot, target)
            .and_then(|a| a["agent_status"].as_str().map(String::from))
    }

    /// Close the agent's pane.
    pub fn close(target: &str) -> rk_core::Result<()> {
        let pane = Self::find_pane(target)
            .ok_or_else(|| rk_core::Error::other(format!("no herdr pane for {target}")))?;
        run_herdr(&["pane", "close", &pane])?;
        Ok(())
    }

    /// Desktop/in-app notification via herdr.
    pub fn notify(title: &str, body: &str) {
        let _ = run_herdr(&["notification", "show", title, "--body", body]);
    }

    /// The argv a human uses to attach interactively (exec'd by `rk attach`).
    pub fn attach_argv(target: &str) -> Vec<String> {
        vec![
            "herdr".into(),
            "agent".into(),
            "attach".into(),
            target.into(),
        ]
    }

    fn find_pane(target: &str) -> Option<String> {
        let snapshot = Self::snapshot()?;
        Self::agent_entry(&snapshot, target).and_then(|a| a["pane_id"].as_str().map(String::from))
    }

    fn snapshot() -> Option<Value> {
        let out = Command::new("herdr")
            .args(["api", "snapshot"])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        serde_json::from_slice::<Value>(&out.stdout).ok()
    }

    /// Match an agent entry by herdr name/label/terminal id.
    fn agent_entry<'a>(snapshot: &'a Value, target: &str) -> Option<&'a Value> {
        let agents = snapshot["result"]["snapshot"]["agents"].as_array()?;
        // Persisted terminal targets must also win during command routing,
        // even if another agent's name or label happens to equal that ID.
        agents
            .iter()
            .find(|entry| entry["terminal_id"].as_str() == Some(target))
            .or_else(|| {
                agents.iter().find(|a| {
                    [
                        &a["name"],
                        &a["label"],
                        &a["terminal_id"],
                        &a["pane_id"],
                        &a["agent_session"]["value"],
                    ]
                    .iter()
                    .any(|f| f.as_str() == Some(target))
                })
            })
    }
}

fn identity_from_entry(entry: &Value) -> rk_core::Result<AgentIdentity> {
    let required = |key: &str| {
        entry[key]
            .as_str()
            .map(String::from)
            .ok_or_else(|| rk_core::Error::other(format!("herdr agent omitted {key}")))
    };
    Ok(AgentIdentity {
        terminal_id: required("terminal_id")?,
        pane_id: required("pane_id")?,
        session_id: generation_fence(entry)?,
        agent: required("agent")?,
        cwd: required("cwd")?,
    })
}

/// Choose a fence when registering. Herdr's revision changes with ordinary
/// metadata updates, so prefer the harness session when it is already known.
/// Revision-only registrations remain conservative until explicitly replaced.
fn generation_fence(entry: &Value) -> rk_core::Result<String> {
    reported_session_fence(entry)
        .or_else(|| revision_fence(entry))
        .ok_or_else(|| rk_core::Error::other("herdr agent omitted session and revision"))
}

fn reported_session_fence(entry: &Value) -> Option<String> {
    entry["agent_session"]["value"]
        .as_str()
        .filter(|session| !session.trim().is_empty())
        .map(|session| format!("agent-session:{session}"))
}

fn revision_fence(entry: &Value) -> Option<String> {
    entry["revision"]
        .as_u64()
        .map(|revision| format!("revision:{revision}"))
}

fn find_string_key(value: &Value, key: &str) -> Option<String> {
    match value {
        Value::Object(map) => map
            .get(key)
            .and_then(Value::as_str)
            .map(String::from)
            .or_else(|| map.values().find_map(|v| find_string_key(v, key))),
        Value::Array(values) => values.iter().find_map(|v| find_string_key(v, key)),
        _ => None,
    }
}

/// The herdr desktop push as a [`NotificationSink`] — the default (and, before
/// the sink registry, the only) escalation channel.
///
/// Renders exactly what the hardwired call rendered: `HerdrMux::notify(title,
/// body)` with the notice's own title/body. Unlike [`HerdrMux::notify`], which
/// swallows everything, this reports failure so the registry can decline to
/// write a dedup marker and retry the notice on a later escalation.
pub struct HerdrSink;

impl NotificationSink for HerdrSink {
    fn kind(&self) -> &str {
        rk_core::config::HERDR_SINK_KIND
    }

    fn deliver(&self, notice: &EscalationNotice) -> rk_core::Result<()> {
        run_herdr(&[
            "notification",
            "show",
            &notice.title(),
            "--body",
            &notice.body(),
        ])?;
        Ok(())
    }
}

/// Herdr refuses `agent start` until the pane's shell has reached its
/// interactive prompt. A freshly created workspace answers `agent_pane_busy`
/// for the first seconds while the login shell runs its startup files (longer
/// on a cold cache after a reboot), so the very first attempt is expected to
/// lose that race. Bound the wait rather than fail the spawn.
const PANE_READY_TIMEOUT: Duration = Duration::from_secs(30);
const PANE_READY_POLL: Duration = Duration::from_millis(250);

/// Herdr reports the not-yet-ready shell as a structured `agent_pane_busy`
/// error on stderr, which `run_herdr` folds into the error text.
fn is_pane_busy(error: &rk_core::Error) -> bool {
    error.to_string().contains("agent_pane_busy")
}

/// Repeat `attempt` while it fails only because the pane's shell is not ready
/// yet. Any other error, a success, or the deadline ends the loop; the final
/// busy error is returned unchanged so the caller still sees Herdr's reason.
fn retry_while_pane_busy<T>(
    mut attempt: impl FnMut() -> rk_core::Result<T>,
    timeout: Duration,
    poll: Duration,
) -> rk_core::Result<T> {
    let deadline = Instant::now() + timeout;
    loop {
        match attempt() {
            Err(error) if is_pane_busy(&error) && Instant::now() < deadline => {
                debug!("herdr pane shell not ready yet; retrying agent start");
                std::thread::sleep(poll);
            }
            other => return other,
        }
    }
}

fn run_herdr(args: &[&str]) -> rk_core::Result<String> {
    let out = Command::new("herdr")
        .args(args)
        .output()
        .map_err(|e| rk_core::Error::other(format!("herdr not runnable: {e}")))?;
    if !out.status.success() {
        return Err(rk_core::Error::other(format!(
            "herdr {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn run_herdr_owned(args: &[String]) -> rk_core::Result<String> {
    let refs = args.iter().map(String::as_str).collect::<Vec<_>>();
    run_herdr(&refs)
}

/// Interactive (TUI) argv for a harness kind — used for attach-mode spawns,
/// where the human may take over the session.
pub fn interactive_argv(
    harness: &str,
    system_prompt: Option<&str>,
    model: Option<&str>,
    permission_mode: Option<&str>,
) -> rk_core::Result<Vec<String>> {
    let mut argv: Vec<String> = match harness {
        "claude" => vec!["claude".into()],
        "codex" => vec!["codex".into()],
        "jcode" => vec!["jcode".into(), "--no-update".into()],
        other => {
            return Err(rk_core::Error::other(format!(
                "harness '{other}' has no interactive mode (attach supports claude, codex, jcode)"
            )))
        }
    };
    match harness {
        "claude" => {
            if let Some(prompt) = system_prompt {
                argv.push("--append-system-prompt".into());
                argv.push(prompt.into());
            }
            if let Some(model) = model {
                argv.push("--model".into());
                argv.push(model.into());
            }
            match permission_mode {
                Some("bypassPermissions") | Some("danger-full-access") => {
                    argv.push("--dangerously-skip-permissions".into());
                }
                Some(mode) => {
                    argv.push("--permission-mode".into());
                    argv.push(mode.into());
                }
                None => {}
            }
        }
        "codex" => {
            if let Some(model) = model {
                argv.push("-m".into());
                argv.push(model.into());
            }
            match permission_mode {
                Some("read-only") => {
                    argv.push("--sandbox".into());
                    argv.push("read-only".into());
                }
                Some("workspace-write") => {
                    argv.push("--sandbox".into());
                    argv.push("workspace-write".into());
                }
                _ => argv.push("--dangerously-bypass-approvals-and-sandbox".into()),
            }
        }
        "jcode" => {
            if let Some(model) = model {
                argv.push("--model".into());
                argv.push(model.into());
            }
        }
        _ => unreachable!(),
    }
    Ok(argv)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn busy() -> rk_core::Error {
        rk_core::Error::other(
            r#"herdr agent start king --kind codex --pane w1R:p1 failed: {"error":{"code":"agent_pane_busy","message":"agent target pane w1R:p1 is not an available shell"},"id":"cli:agent:start"}"#,
        )
    }

    #[test]
    fn agent_start_waits_out_a_pane_whose_shell_is_still_starting() {
        let mut attempts = 0;
        let started = retry_while_pane_busy(
            || {
                attempts += 1;
                if attempts < 3 {
                    Err(busy())
                } else {
                    Ok("w1R:p1")
                }
            },
            Duration::from_secs(5),
            Duration::from_millis(1),
        )
        .unwrap();
        assert_eq!(started, "w1R:p1");
        assert_eq!(attempts, 3);
    }

    #[test]
    fn agent_start_does_not_retry_other_herdr_failures() {
        let mut attempts = 0;
        let error = retry_while_pane_busy(
            || -> rk_core::Result<()> {
                attempts += 1;
                Err(rk_core::Error::other(
                    "herdr agent start failed: agent_not_found",
                ))
            },
            Duration::from_secs(5),
            Duration::from_millis(1),
        )
        .unwrap_err();
        assert_eq!(attempts, 1);
        assert!(error.to_string().contains("agent_not_found"), "{error}");
    }

    #[test]
    fn agent_start_gives_up_with_the_busy_reason_after_the_deadline() {
        let mut attempts = 0;
        let error = retry_while_pane_busy(
            || -> rk_core::Result<()> {
                attempts += 1;
                Err(busy())
            },
            Duration::from_millis(20),
            Duration::from_millis(1),
        )
        .unwrap_err();
        assert!(attempts > 1, "expected repeated attempts, got {attempts}");
        assert!(error.to_string().contains("agent_pane_busy"), "{error}");
    }

    #[test]
    fn interactive_argv_shapes() {
        let claude = interactive_argv("claude", Some("be a rat"), Some("haiku"), None).unwrap();
        assert_eq!(claude[0], "claude");
        assert!(claude.contains(&"--append-system-prompt".to_string()));
        assert!(
            !claude.contains(&"-p".to_string()),
            "interactive, not headless"
        );

        let codex = interactive_argv("codex", None, Some("gpt-5.5-codex"), None).unwrap();
        assert_eq!(codex[0], "codex");
        assert!(codex.contains(&"--dangerously-bypass-approvals-and-sandbox".to_string()));

        let jcode = interactive_argv(
            "jcode",
            Some("delivered with the first prompt"),
            Some("gpt-5.5"),
            Some("danger-full-access"),
        )
        .unwrap();
        assert_eq!(jcode, ["jcode", "--no-update", "--model", "gpt-5.5"]);

        let claude = interactive_argv("claude", None, None, Some("bypassPermissions")).unwrap();
        assert!(claude.contains(&"--dangerously-skip-permissions".to_string()));
        assert!(!claude.contains(&"--permission-mode".to_string()));

        assert!(interactive_argv("fake", None, None, None).is_err());
    }

    #[test]
    fn agent_entry_matches_by_name_terminal_pane_or_generation_not_agent_kind() {
        let snapshot: Value = serde_json::from_str(
            r#"{"result":{"snapshot":{"agents":[
                {"name":"Whisker","agent":"claude","agent_status":"working","pane_id":"w1:p2","terminal_id":"term_1"},
                {"agent":"codex","agent_status":"idle","pane_id":"w1:p3","terminal_id":"term_2"}
            ]}}}"#,
        )
        .unwrap();
        let by_name = HerdrMux::agent_entry(&snapshot, "Whisker").unwrap();
        assert_eq!(by_name["pane_id"], "w1:p2");
        let by_term = HerdrMux::agent_entry(&snapshot, "term_2").unwrap();
        assert_eq!(by_term["agent_status"], "idle");
        assert!(HerdrMux::agent_entry(&snapshot, "codex").is_none());
        assert!(HerdrMux::agent_entry(&snapshot, "Nibbles").is_none());
    }

    fn reported_agent() -> Value {
        serde_json::json!({
            "terminal_id": "term_1", "pane_id": "w1:p1", "revision": 7,
            "agent": "codex", "cwd": "/repo", "agent_status": "idle",
            "focused": false, "agent_session": {"value": "sess_abc"}
        })
    }

    fn exact_agent(entry: &Value, identity: &AgentIdentity) -> Option<AgentState> {
        HerdrMux::exact_state_from_snapshot(
            &serde_json::json!({"result": {"snapshot": {"agents": [entry]}}}),
            identity,
        )
    }

    #[test]
    fn session_identity_survives_metadata_updates_and_state_round_trip() {
        let mut entry = reported_agent();
        let registered = identity_from_entry(&entry).unwrap();
        assert_eq!(registered.session_id, "agent-session:sess_abc");
        let persisted = serde_json::to_vec(&registered).unwrap();
        let restored = serde_json::from_slice(&persisted).unwrap();
        entry["revision"] = serde_json::json!(1070);
        entry["agent_status"] = serde_json::json!("working");
        entry["focused"] = serde_json::json!(true);
        entry["cwd"] = serde_json::json!("/other/repo");
        entry["pane_id"] = serde_json::json!("w2:p1");
        let state = exact_agent(&entry, &restored).unwrap();
        assert_eq!(state.identity.session_id, registered.session_id);
        assert_eq!(state.identity.cwd, "/other/repo");
        assert_eq!(state.identity.pane_id, "w2:p1");
        assert_eq!(state.status, "working");
        assert!(state.focused);
    }

    #[test]
    fn session_identity_rejects_replacement_and_missing_session_metadata() {
        let entry = reported_agent();
        let registered = identity_from_entry(&entry).unwrap();
        for (field, value) in [("terminal_id", "term_2"), ("agent", "claude")] {
            let mut replaced = entry.clone();
            replaced[field] = serde_json::json!(value);
            assert!(exact_agent(&replaced, &registered).is_none(), "{field}");
        }
        for value in [
            serde_json::json!("sess_replacement"),
            serde_json::json!(""),
            serde_json::json!("  "),
            serde_json::json!(42),
            Value::Null,
        ] {
            let mut replaced = entry.clone();
            replaced["agent_session"]["value"] = value;
            assert!(exact_agent(&replaced, &registered).is_none());
        }
        let mut missing = entry;
        missing.as_object_mut().unwrap().remove("agent_session");
        assert!(exact_agent(&missing, &registered).is_none());
    }

    #[test]
    fn exact_identity_does_not_accept_terminal_aliases() {
        let entry = reported_agent();
        let registered = identity_from_entry(&entry).unwrap();
        let mut other = entry.clone();
        other["terminal_id"] = serde_json::json!("term_other");
        other["label"] = serde_json::json!(registered.terminal_id);
        assert!(exact_agent(&other, &registered).is_none());
        let snapshot = serde_json::json!({"result": {"snapshot": {"agents": [other, entry]}}});
        assert_eq!(
            HerdrMux::exact_state_from_snapshot(&snapshot, &registered)
                .unwrap()
                .identity
                .terminal_id,
            registered.terminal_id
        );
    }

    #[test]
    fn fence_modes_preserve_opaque_session_ids_and_reject_unknown_formats() {
        let mut entry = reported_agent();
        entry["agent_session"]["value"] = serde_json::json!("revision:7");
        let mut registered = identity_from_entry(&entry).unwrap();
        assert_eq!(registered.session_id, "agent-session:revision:7");
        entry["revision"] = serde_json::json!(8);
        assert!(exact_agent(&entry, &registered).is_some());
        for unknown in ["revision:07", "sess_abc", "unknown:7", "agent-session:"] {
            registered.session_id = unknown.into();
            assert!(exact_agent(&entry, &registered).is_none(), "{unknown}");
        }
    }

    #[test]
    fn revision_identity_retains_its_mode_when_session_appears_late() {
        // Verbatim shape of a live `claude` agent under herdr 0.8.2: healthy,
        // interactive, and carrying no `agent_session` key at all.
        let unreported: Value = serde_json::from_str(
            r#"{"agent":"claude","agent_status":"idle","cwd":"/repo","focused":false,
                "interactive_ready":true,"name":"king","pane_id":"w8:p1","revision":1,
                "state_change_seq":3,"tab_id":"w8:t1","terminal_id":"term_2",
                "workspace_id":"w8"}"#,
        )
        .unwrap();
        let identity = identity_from_entry(&unreported).unwrap();
        assert_eq!(identity.session_id, "revision:1");
        assert_eq!(identity.terminal_id, "term_2");
        assert_eq!(identity.agent, "claude");

        let persisted = serde_json::to_vec(&identity).unwrap();
        let restored = serde_json::from_slice(&persisted).unwrap();
        let mut reported = unreported.clone();
        reported["agent_session"] = serde_json::json!({"value": "late_session"});
        let state = exact_agent(&reported, &restored).unwrap();
        assert_eq!(state.identity.session_id, "revision:1");
        // Explicit registration can select the stronger fence.
        assert_eq!(
            identity_from_entry(&reported).unwrap().session_id,
            "agent-session:late_session"
        );
        // The fallback remains conservative when revision changes, whether
        // that change is a replacement or merely new metadata.
        reported["revision"] = serde_json::json!(3);
        assert!(exact_agent(&reported, &restored).is_none());

        let neither: Value =
            serde_json::from_str(r#"{"terminal_id":"term_3","pane_id":"w1:p1"}"#).unwrap();
        assert!(generation_fence(&neither).is_err());
    }
}
