//! The built-in [`NotificationSink`](super::NotificationSink) implementations —
//! the ones that need nothing but `rk-core`, so they are registered by
//! [`SinkFactory::builtin`](super::SinkFactory::builtin) and are selectable from
//! `[[notify.sinks]]` on any castle.
//!
//! Between them they cover the two shapes a channel comes in:
//!
//! * [`LogSink`] — in-process, zero options. The escalation goes out through
//!   `tracing` at its own severity, so a headless castle (no desktop, no herdr)
//!   still has escalations in whatever the daemon's log is shipped to.
//! * [`CommandSink`] — out-of-process, one required option. Runs an operator's
//!   program with the notice on argv, in the environment, and as JSON on stdin.
//!   This is the escape hatch that makes the config-only promise real: a chat
//!   webhook, a phone push, an SMS gateway, `notify-send`, or a script that
//!   drives `rk` is a `[[notify.sinks]]` table, not a patch to this crate.

use super::{EscalationNotice, NotificationSink, Severity};
use crate::config::SinkConfig;
use crate::exec::run_piped;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

/// The `kind` [`LogSink`] answers to.
pub const LOG_SINK_KIND: &str = "log";

/// The `kind` [`CommandSink`] answers to.
pub const COMMAND_SINK_KIND: &str = "command";

/// Emit the notice into the daemon's own `tracing` output at its severity.
///
/// Not a substitute for a push — a log line does not interrupt anyone — but it
/// is the one channel that cannot fail to be installed, which makes it the
/// useful second sink on a headless castle and the honest default to reach for
/// when `herdr` is not there.
pub struct LogSink;

impl NotificationSink for LogSink {
    fn kind(&self) -> &str {
        LOG_SINK_KIND
    }

    fn deliver(&self, notice: &EscalationNotice) -> crate::Result<()> {
        // Severity is chosen by the source, so it maps straight onto a level
        // rather than being flattened to one — an operator filtering the log at
        // `warn` should still see a critical escalation.
        match notice.severity {
            Severity::Critical => error!(
                tuple = %notice.tuple_id, class = %notice.class, scope = %notice.scope,
                "escalation: {}", notice.body()
            ),
            Severity::Warn => warn!(
                tuple = %notice.tuple_id, class = %notice.class, scope = %notice.scope,
                "escalation: {}", notice.body()
            ),
            Severity::Info => info!(
                tuple = %notice.tuple_id, class = %notice.class, scope = %notice.scope,
                "escalation: {}", notice.body()
            ),
        }
        Ok(())
    }
}

/// How long a [`CommandSink`] child gets before it is killed, when the table
/// does not say.
const DEFAULT_TIMEOUT_SECS: u64 = 10;

/// Run an operator-configured program per notice.
///
/// Options (`[notify.sinks.options]`):
///
/// * `command` (**required**) — the program to run. Not a shell line: it is
///   exec'd directly, so anything needing pipes, globbing or quoting belongs in
///   a script the option points at. That is deliberate — a shell string here
///   would make every notice field an injection site.
/// * `timeout_secs` — bound on the child, default
///   [`DEFAULT_TIMEOUT_SECS`]. A child that overruns is killed and reported as a
///   failure; the escalation is already durable and on `rk inbox`, so the
///   reactor cycle must never be the thing that waits.
///
/// The notice reaches the child three ways, so a one-line script and a
/// structured consumer are both cheap:
///
/// * **argv** — `<command> <title> <body>`, the same shape as `notify-send` and
///   the herdr call this generalises.
/// * **env** — `RK_NOTICE_TITLE`, `_BODY`, `_TEXT`, `_CLASS`, `_SEVERITY`,
///   `_SCOPE`, `_SUBJECT`, `_TUPLE`, `_ACTION`, plus every ref as
///   `RK_NOTICE_REF_<KEY>`.
/// * **stdin** — the whole [`EscalationNotice`] as JSON.
#[derive(Debug)]
pub struct CommandSink {
    program: String,
    timeout: Duration,
}

impl CommandSink {
    /// Read a `[[notify.sinks]]` table. Fails when `command` is absent rather
    /// than registering a sink that would report success while delivering
    /// nowhere — a misconfigured channel the operator believes in is worse than
    /// one that refused to start and said so.
    pub fn from_config(config: &SinkConfig) -> crate::Result<Self> {
        let program = config.option("command").ok_or_else(|| {
            crate::Error::Config(format!(
                "sink `{}` of kind `{COMMAND_SINK_KIND}` needs an options.command to run",
                config.name()
            ))
        })?;
        let timeout = match config.option("timeout_secs") {
            Some(raw) => raw.trim().parse::<u64>().map_err(|e| {
                crate::Error::Config(format!(
                    "sink `{}`: options.timeout_secs `{raw}` is not a whole number of seconds: {e}",
                    config.name()
                ))
            })?,
            None => DEFAULT_TIMEOUT_SECS,
        };
        Ok(Self {
            program: program.to_string(),
            timeout: Duration::from_secs(timeout),
        })
    }

    fn env(notice: &EscalationNotice) -> BTreeMap<String, String> {
        let mut env = BTreeMap::from([
            ("RK_NOTICE_TUPLE".to_string(), notice.tuple_id.clone()),
            ("RK_NOTICE_CLASS".to_string(), notice.class.clone()),
            (
                "RK_NOTICE_SEVERITY".to_string(),
                notice.severity.to_string(),
            ),
            ("RK_NOTICE_SCOPE".to_string(), notice.scope.clone()),
            ("RK_NOTICE_SUBJECT".to_string(), notice.subject.clone()),
            ("RK_NOTICE_TEXT".to_string(), notice.text.clone()),
            ("RK_NOTICE_TITLE".to_string(), notice.title()),
            ("RK_NOTICE_BODY".to_string(), notice.body()),
        ]);
        if let Some(action) = &notice.suggested_action {
            env.insert("RK_NOTICE_ACTION".to_string(), action.clone());
        }
        for (key, value) in &notice.refs {
            if let Some(key) = ref_env_key(key) {
                env.insert(key, value.clone());
            }
        }
        env
    }
}

/// `branch` → `RK_NOTICE_REF_BRANCH`. Anything not `[A-Z0-9]` becomes `_`, and a
/// key with no usable character at all is dropped rather than colliding on
/// `RK_NOTICE_REF_`.
fn ref_env_key(raw: &str) -> Option<String> {
    let sanitized: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    sanitized
        .chars()
        .any(|c| c != '_')
        .then(|| format!("RK_NOTICE_REF_{sanitized}"))
}

impl NotificationSink for CommandSink {
    fn kind(&self) -> &str {
        COMMAND_SINK_KIND
    }

    fn deliver(&self, notice: &EscalationNotice) -> crate::Result<()> {
        let payload = serde_json::to_vec(notice)?;
        let args = [notice.title(), notice.body()];
        let status = run_piped(
            &self.program,
            &args,
            &Self::env(notice),
            &payload,
            self.timeout,
        )?;
        if !status.success() {
            return Err(crate::Error::other(format!(
                "`{}` exited {}",
                self.program,
                status
                    .code()
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "by signal".into())
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notice() -> EscalationNotice {
        EscalationNotice::new(
            "01AAA",
            "steward-escalation",
            Severity::Critical,
            "myrepo",
            "TKT-7",
            "needs a human merge decision",
        )
        .with_action("rk land myrepo")
        .with_ref("branch", "rat/x/y")
    }

    #[test]
    fn a_log_sink_needs_no_options_and_cannot_fail() {
        let sink = LogSink;
        assert_eq!(sink.kind(), "log");
        assert!(sink.deliver(&notice()).is_ok());
    }

    #[test]
    fn a_command_sink_without_a_command_option_refuses_to_build() {
        let err = CommandSink::from_config(&SinkConfig::of_kind(COMMAND_SINK_KIND))
            .expect_err("no options.command must not build a sink that delivers nowhere");
        assert!(err.to_string().contains("options.command"), "{err}");

        let err = CommandSink::from_config(
            &SinkConfig::of_kind(COMMAND_SINK_KIND)
                .with_option("command", "/bin/true")
                .with_option("timeout_secs", "soon"),
        )
        .expect_err("a non-numeric timeout is a config error, not a silent default");
        assert!(err.to_string().contains("timeout_secs"), "{err}");
    }

    #[test]
    fn ref_keys_become_env_names_or_are_dropped() {
        assert_eq!(ref_env_key("branch").unwrap(), "RK_NOTICE_REF_BRANCH");
        assert_eq!(
            ref_env_key("work-tree.id").unwrap(),
            "RK_NOTICE_REF_WORK_TREE_ID"
        );
        assert_eq!(ref_env_key("--"), None, "no usable character: dropped");
    }

    #[test]
    fn env_carries_every_notice_field_including_refs() {
        let env = CommandSink::env(&notice());
        assert_eq!(env["RK_NOTICE_TITLE"], "steward escalation — TKT-7");
        assert_eq!(
            env["RK_NOTICE_BODY"],
            "needs a human merge decision\n→ rk land myrepo"
        );
        assert_eq!(env["RK_NOTICE_SEVERITY"], "critical");
        assert_eq!(env["RK_NOTICE_ACTION"], "rk land myrepo");
        assert_eq!(env["RK_NOTICE_REF_BRANCH"], "rat/x/y");
    }

    #[cfg(unix)]
    fn script(dir: &std::path::Path, name: &str, body: &str) -> String {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[cfg(unix)]
    #[test]
    fn a_command_sink_hands_the_notice_over_three_ways() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("delivered");
        let program = script(
            dir.path(),
            "push",
            &format!(
                r#"{{ echo "argv1=$1"; echo "env=$RK_NOTICE_SEVERITY/$RK_NOTICE_REF_BRANCH"; echo "stdin=$(cat)"; }} > {}"#,
                out.display()
            ),
        );

        let sink = CommandSink::from_config(
            &SinkConfig::of_kind(COMMAND_SINK_KIND).with_option("command", &program),
        )
        .unwrap();
        sink.deliver(&notice()).unwrap();

        let got = std::fs::read_to_string(&out).unwrap();
        assert!(got.contains("argv1=steward escalation — TKT-7"), "{got}");
        assert!(got.contains("env=critical/rat/x/y"), "{got}");
        assert!(
            got.contains(r#"stdin={"tuple_id":"01AAA""#),
            "the whole notice is on stdin as JSON: {got}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_failing_command_is_an_error_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let program = script(dir.path(), "broken", "exit 3");
        let sink = CommandSink::from_config(
            &SinkConfig::of_kind(COMMAND_SINK_KIND).with_option("command", &program),
        )
        .unwrap();
        let err = sink.deliver(&notice()).expect_err("exit 3 is a failure");
        assert!(err.to_string().contains("exited 3"), "{err}");
    }

    #[test]
    fn a_missing_program_is_an_error_not_a_panic() {
        let sink = CommandSink::from_config(
            &SinkConfig::of_kind(COMMAND_SINK_KIND)
                .with_option("command", "/nonexistent/rk-notify-nowhere"),
        )
        .unwrap();
        let err = sink
            .deliver(&notice())
            .expect_err("an uninstalled channel reports failure, it does not panic");
        assert!(err.to_string().contains("could not run"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn a_wedged_command_is_killed_at_its_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let program = script(dir.path(), "hang", "sleep 60");
        let sink = CommandSink::from_config(
            &SinkConfig::of_kind(COMMAND_SINK_KIND)
                .with_option("command", &program)
                .with_option("timeout_secs", "1"),
        )
        .unwrap();

        let started = Instant::now();
        let err = sink
            .deliver(&notice())
            .expect_err("a hung child must not win");
        assert!(err.to_string().contains("timed out"), "{err}");
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the reactor cycle is not held hostage by a wedged notifier"
        );
    }
}
