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
use tracing::debug;

pub struct HerdrMux;

/// Stable identity of one detected agent generation. `terminal_id` anchors the
/// pane while `session_id` fences a restarted agent in that same terminal.
///
/// `agent_session` is nullable in the Herdr API schema: it is populated only
/// when the harness itself reports a session (`herdr pane
/// report-agent-session`), which Claude Code does not do. When it is absent we
/// fence on the pane's `revision` counter instead — a required field that
/// Herdr increments when a new agent generation takes over the pane.
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
        let created = match run_herdr_owned(&create) {
            Ok(created) => created,
            // Herdr <0.8 exposed `agent start NAME --cwd ... -- ARGV`
            // directly. Keep that compatibility path after the current API
            // fails so older installations and deterministic fake-Herdr test
            // harnesses remain usable.
            Err(current_api_error) => {
                return Self::start_agent_legacy(name, cwd, env, argv).map_err(|legacy_error| {
                    rk_core::Error::other(format!(
                        "current Herdr start failed ({current_api_error}); legacy start failed ({legacy_error})"
                    ))
                });
            }
        };
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
        if let Err(error) = Self::start_in_pane(name, &argv[0], &pane, &argv[1..]) {
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
    /// Herdr 0.8 dropped `agent send`, so the split send-then-Enter path is
    /// only reachable on older servers. Report the original `agent prompt`
    /// failure when the fallback is unavailable too: the fallback's own usage
    /// error says nothing about why the prompt did not land.
    pub fn send(target: &str, text: &str) -> rk_core::Result<()> {
        let target = &Self::agent_target(target);
        let prompt_error = match run_herdr(&["agent", "prompt", target, text]) {
            Ok(_) => return Ok(()),
            Err(error) => error,
        };
        if run_herdr(&["agent", "send", target, text]).is_err() {
            return Err(prompt_error);
        }
        let pane = Self::find_pane(target)
            .ok_or_else(|| rk_core::Error::other(format!("no herdr pane for {target}")))?;
        run_herdr(&["pane", "send-keys", &pane, "enter"])?;
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

    fn start_agent_legacy(
        name: &str,
        cwd: &Path,
        env: &HashMap<String, String>,
        argv: &[String],
    ) -> rk_core::Result<String> {
        let mut command = vec!["agent".into(), "start".into(), name.into()];
        command.push("--cwd".into());
        command.push(cwd.to_string_lossy().to_string());
        for (key, value) in env {
            command.push("--env".into());
            command.push(format!("{key}={value}"));
        }
        command.push("--no-focus".into());
        command.push("--".into());
        command.extend(argv.iter().cloned());
        run_herdr_owned(&command)?;
        Ok(name.to_string())
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
        let entry = Self::agent_entry(&snapshot, &identity.terminal_id)?;
        let current = identity_from_entry(entry).ok()?;
        if current.session_id != identity.session_id {
            return None;
        }
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
        if Self::exact_state(identity).is_none() {
            return Err(rk_core::Error::other(
                "registered King generation is no longer present",
            ));
        }
        let timeout = timeout_ms.clamp(1_000, 300_000).to_string();
        // Target the pane: Herdr 0.8 no longer resolves `terminal_id` here.
        run_herdr(&[
            "agent",
            "prompt",
            &identity.pane_id,
            "/exit",
            "--wait",
            "--until",
            "done",
            "--timeout",
            &timeout,
        ])?;
        Self::start_in_pane(name, harness, &identity.pane_id, args)
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
        snapshot["result"]["snapshot"]["agents"]
            .as_array()?
            .iter()
            .find(|a| {
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

/// Fence one agent generation within a pane.
///
/// Prefer the harness-reported `agent_session.value`: it survives Herdr server
/// restarts and identifies the harness session itself. Herdr leaves it null
/// for harnesses that do not report one, so fall back to the pane's required
/// `revision` counter, which Herdr increments when a new generation takes over
/// the pane. Either way a replacement agent in the same pane fences out.
fn generation_fence(entry: &Value) -> rk_core::Result<String> {
    if let Some(session) = entry["agent_session"]["value"].as_str() {
        return Ok(session.to_string());
    }
    entry["revision"]
        .as_u64()
        .map(|revision| format!("revision:{revision}"))
        .ok_or_else(|| {
            rk_core::Error::other("herdr agent omitted both agent_session.value and revision")
        })
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

    /// A harness-reported session is the preferred fence, but Herdr's schema
    /// declares `agent_session` nullable and Claude Code never reports one.
    /// Requiring it made `rk king spawn` fail outright against a healthy
    /// agent, so an omitted session falls back to the pane `revision`.
    #[test]
    fn generation_fence_prefers_agent_session_then_falls_back_to_revision() {
        let reported: Value = serde_json::from_str(
            r#"{"terminal_id":"term_1","pane_id":"w1:p1","revision":7,
                "agent":"codex","cwd":"/repo","agent_session":{"value":"sess_abc"}}"#,
        )
        .unwrap();
        assert_eq!(generation_fence(&reported).unwrap(), "sess_abc");

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

        // A replacement generation in the same pane must not read as the same
        // King: Herdr bumps `revision` when a new agent takes the pane over.
        let mut replaced = unreported.clone();
        replaced["revision"] = serde_json::json!(3);
        assert_ne!(
            identity_from_entry(&replaced).unwrap().session_id,
            identity.session_id
        );

        let neither: Value =
            serde_json::from_str(r#"{"terminal_id":"term_3","pane_id":"w1:p1"}"#).unwrap();
        assert!(generation_fence(&neither).is_err());
    }
}
