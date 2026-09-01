//! Thin, success-checked wrappers over one-shot `git` invocations, for callers
//! that need a raw command in a directory rather than a [`crate::Repo`]
//! lifecycle operation.
//!
//! Every daemon module that shells out to git used to carry its own private
//! copy of these four helpers, each with a slightly different failure
//! message and environment. This is the one copy: `LC_ALL=C` so parsed output
//! is locale-stable, stderr preferred (and stdout used as the fallback,
//! because `git merge` and friends report on stdout) when composing the
//! error, and the directory named in it so a failure in one of several
//! worktrees can be placed.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

fn command(dir: &Path, args: &[&str]) -> Command {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(dir).args(args).env("LC_ALL", "C");
    cmd
}

fn checked(dir: &Path, args: &[&str], output: Output) -> rk_core::Result<Output> {
    if output.status.success() {
        return Ok(output);
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let detail = if stderr.is_empty() {
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if stdout.is_empty() {
            format!("exited {}", output.status.code().unwrap_or(-1))
        } else {
            stdout
        }
    } else {
        stderr
    };
    Err(rk_core::Error::other(format!(
        "git {} failed in {}: {detail}",
        args.join(" "),
        dir.display()
    )))
}

/// Run `git <args>` in `dir`, returning the captured output on success and a
/// descriptive error (command, directory, git's own diagnostic) otherwise.
pub fn git_output(dir: &Path, args: &[&str]) -> rk_core::Result<Output> {
    let output = command(dir, args).output()?;
    checked(dir, args, output)
}

/// [`git_output`], keeping only the success/failure outcome.
pub fn git_ok(dir: &Path, args: &[&str]) -> rk_core::Result<()> {
    git_output(dir, args).map(|_| ())
}

/// [`git_output`], returning trimmed stdout — the shape of every
/// `rev-parse`/`log --format`/`status --porcelain` read.
pub fn git_text(dir: &Path, args: &[&str]) -> rk_core::Result<String> {
    let output = git_output(dir, args)?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Whether `git <args>` exits zero in `dir`; a git that cannot be run at all
/// counts as a failure rather than an error, which is what a `show-ref
/// --verify --quiet` style probe wants.
pub fn git_succeeds(dir: &Path, args: &[&str]) -> bool {
    command(dir, args)
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

/// Run `git <args>` in `dir` with `input` piped to its stdin — `git apply -`
/// and other commands that read their payload from the pipe.
pub fn git_with_stdin(dir: &Path, args: &[&str], input: &str) -> rk_core::Result<()> {
    let mut child = command(dir, args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or_else(|| rk_core::Error::other("git stdin was not piped"))?
        .write_all(input.as_bytes())?;
    let output = child.wait_with_output()?;
    checked(dir, args, output).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_repo() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        git_ok(dir.path(), &["init", "-q", "-b", "main"]).unwrap();
        git_ok(dir.path(), &["config", "user.email", "r@x"]).unwrap();
        git_ok(dir.path(), &["config", "user.name", "R"]).unwrap();
        std::fs::write(dir.path().join("README.md"), "# x\n").unwrap();
        git_ok(dir.path(), &["add", "."]).unwrap();
        git_ok(dir.path(), &["commit", "-q", "-m", "init"]).unwrap();
        dir
    }

    #[test]
    fn text_is_trimmed_stdout() {
        let dir = scratch_repo();
        assert_eq!(
            git_text(dir.path(), &["branch", "--show-current"]).unwrap(),
            "main"
        );
    }

    #[test]
    fn failure_names_command_directory_and_diagnostic() {
        let dir = scratch_repo();
        let err = git_text(dir.path(), &["rev-parse", "--verify", "no-such-ref"])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("git rev-parse --verify no-such-ref failed in"),
            "{err}"
        );
        assert!(err.contains(&dir.path().display().to_string()), "{err}");
        assert!(err.contains("fatal"), "{err}");
    }

    #[test]
    fn succeeds_is_a_quiet_probe() {
        let dir = scratch_repo();
        assert!(git_succeeds(
            dir.path(),
            &["show-ref", "--verify", "--quiet", "refs/heads/main"]
        ));
        assert!(!git_succeeds(
            dir.path(),
            &["show-ref", "--verify", "--quiet", "refs/heads/nope"]
        ));
        assert!(!git_succeeds(
            Path::new("/definitely/not/a/dir"),
            &["status"]
        ));
    }

    #[test]
    fn with_stdin_feeds_the_pipe() {
        let dir = scratch_repo();
        let patch = "--- a/README.md\n+++ b/README.md\n@@ -1 +1,2 @@\n # x\n+more\n";
        git_with_stdin(dir.path(), &["apply", "-"], patch).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("README.md")).unwrap(),
            "# x\nmore\n"
        );
        let err = git_with_stdin(dir.path(), &["apply", "-"], "garbage")
            .unwrap_err()
            .to_string();
        assert!(err.contains("git apply - failed in"), "{err}");
    }
}
