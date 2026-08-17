//! herdr integration: run rats in herdr panes so humans can watch, attach,
//! and take over — while control still flows through the daemon and the
//! tuplespace, never through keystroke scraping.
//!
//! Shell-out client (the herdr CLI mirrors its socket API 1:1 and shields us
//! from pre-1.0 protocol churn). Everything degrades gracefully: no herdr, no
//! attach surface, headless spawns unaffected.

use rk_core::notify::{EscalationNotice, NotificationSink};
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use tracing::debug;

pub struct HerdrMux;

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
        let mut cmd = Command::new("herdr");
        cmd.args(["agent", "start", name]);
        cmd.args(["--cwd", &cwd.to_string_lossy()]);
        for (key, value) in env {
            cmd.args(["--env", &format!("{key}={value}")]);
        }
        cmd.arg("--no-focus");
        cmd.arg("--");
        cmd.args(argv);
        let out = cmd
            .output()
            .map_err(|e| rk_core::Error::other(format!("herdr not runnable: {e}")))?;
        if !out.status.success() {
            return Err(rk_core::Error::other(format!(
                "herdr agent start failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        debug!(name, "started herdr pane");
        Ok(name.to_string())
    }

    /// Type a message into the agent's pane (herdr owns timing — no sleeps
    /// here) followed by Enter via `pane run` semantics: `agent send` writes
    /// literal text, so we send text then the Enter key.
    pub fn send(target: &str, text: &str) -> rk_core::Result<()> {
        run_herdr(&["agent", "send", target, text])?;
        let pane = Self::find_pane(target)
            .ok_or_else(|| rk_core::Error::other(format!("no herdr pane for {target}")))?;
        run_herdr(&["pane", "send-keys", &pane, "enter"])?;
        Ok(())
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
                [&a["name"], &a["label"], &a["terminal_id"], &a["agent"]]
                    .iter()
                    .any(|f| f.as_str() == Some(target))
            })
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
        assert_eq!(
            jcode,
            ["jcode", "--no-update", "--model", "gpt-5.5"]
        );

        let claude = interactive_argv(
            "claude",
            None,
            None,
            Some("bypassPermissions"),
        )
        .unwrap();
        assert!(claude.contains(&"--dangerously-skip-permissions".to_string()));
        assert!(!claude.contains(&"--permission-mode".to_string()));

        assert!(interactive_argv("axe", None, None, None).is_err());
    }

    #[test]
    fn agent_entry_matches_by_name_or_terminal() {
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
        assert!(HerdrMux::agent_entry(&snapshot, "Nibbles").is_none());
    }
}
