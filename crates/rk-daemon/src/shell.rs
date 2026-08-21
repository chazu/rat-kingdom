//! Shared shell-invocation helper for repository- and workflow-declared
//! commands.
//!
//! Every place RK runs a check command it did not author itself — a named
//! check contract (`.rk/checks.cue`), a workflow `run` step's raw command —
//! goes through [`pipefail_command`] so a pipe stage inside that command
//! (`cargo test | tee out.log`) cannot mask a failing exit status behind a
//! successful downstream consumer's own exit code. Without this, `sh -c`'s
//! default (no `pipefail`) reports only the LAST stage's exit status, so a
//! red suite piped through a green `tee`/`tail` reads as clean.

/// Build the argv RK hands to the process spawner for `command`: `bash -c`
/// wrapping `command` with `set -o pipefail` in effect first.
///
/// `bash`, not `sh`: on Linux `sh` is commonly `dash`, which has no
/// `pipefail` builtin and would fail the option outright — every check
/// declaring a plain command (`cargo test --workspace`, no pipe at all)
/// would break, not just the piped ones. `bash` already backs every harness
/// launcher in this codebase, so it costs nothing new to require.
pub(crate) fn pipefail_command(command: &str) -> (&'static str, String) {
    ("bash", format!("set -o pipefail\n{command}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn wraps_with_bash_and_pipefail() {
        let (program, script) = pipefail_command("cargo test");
        assert_eq!(program, "bash");
        assert!(script.starts_with("set -o pipefail\n"));
        assert!(script.ends_with("cargo test"));
    }

    #[test]
    fn masked_failure_in_a_pipe_is_not_masked_under_pipefail() {
        // Without pipefail, `sh -c "false | cat"` reports `cat`'s exit
        // status (0) — exactly the masking this wrapper exists to prevent.
        let (program, script) = pipefail_command("false | cat");
        let status = Command::new(program).arg("-c").arg(&script).status().unwrap();
        assert!(
            !status.success(),
            "pipefail_command must surface the failing pipe stage, not the consumer's success"
        );
    }

    #[test]
    fn a_clean_pipeline_still_reports_success() {
        let (program, script) = pipefail_command("true | cat");
        let status = Command::new(program).arg("-c").arg(&script).status().unwrap();
        assert!(status.success());
    }

    #[test]
    fn a_plain_command_with_no_pipe_is_unaffected() {
        let (program, script) = pipefail_command("exit 3");
        let status = Command::new(program).arg("-c").arg(&script).status().unwrap();
        assert_eq!(status.code(), Some(3));
    }
}
