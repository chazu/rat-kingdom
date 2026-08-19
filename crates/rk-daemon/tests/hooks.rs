//! Castle/repo lifecycle hooks (TKT-01M0BV4Z1Z48ENFE37PWWP846P), end to end
//! over the live daemon's reactor loop: spawn a real fake-harness rat, let it
//! actually complete (a real `harness_result` tuple, a real transcript file),
//! and confirm the hook dispatcher the reactor runs on every cycle reacts to
//! it — a castle-level hook and a repo-level hook both fire for the same
//! event (additive fan-in, the "repo extends castle" half of the acceptance
//! criteria), a failing hook alongside them never affects the agent's own
//! completion, and its failure is durably (but rate-capped) announced.
//!
//! "Rats cannot register hooks" is not exercised here because there is
//! nothing to exercise: no RPC method writes `<home>/hooks/*.cue` or
//! `<repo>/.rk/hooks.cue` (see `Layout::hooks_dir` and `Reactor::hook_files`)
//! — the same architectural absence `.rk/triggers.cue` already relies on.

mod fixture;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.com"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# scratch\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "gnawed by $RK_AGENT for task $RK_TASK" > gnawed.txt
git add gnawed.txt >/dev/null 2>&1
git -c user.email=rat@x -c user.name=Rat commit -q -m "rat work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"fake-hooks"}'
rk_done "work done"
echo '{"type":"result","subtype":"success","is_error":false,"result":"committed gnawed.txt","session_id":"fake-hooks","total_cost_usd":0.002,"usage":{"input_tokens":50,"output_tokens":25,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

#[cfg(unix)]
fn recorder_script(dir: &Path, name: &str, out: &Path) -> String {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    std::fs::write(
        &path,
        format!(
            r#"#!/bin/sh
{{
    echo "argv1=$1"
    echo "event=$RK_HOOK_EVENT"
    echo "name=$RK_HOOK_NAME"
    echo "scope=$RK_HOOK_SCOPE"
    echo "agent=$RK_HOOK_AGENT"
    echo "transcript=$RK_HOOK_TRANSCRIPT_PATH"
    echo "stdin=$(cat)"
}} > "{}"
"#,
            out.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path.to_string_lossy().into_owned()
}

fn write_cue(path: &Path, source: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, source).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn castle_and_repo_hooks_fire_on_completion_and_a_failing_hook_stays_isolated() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();

    // Castle-level hook: fires for every repo's agent_completed.
    let castle_out = home.path().join("castle-hook-out.txt");
    let castle_program = recorder_script(home.path(), "castle-hook.sh", &castle_out);
    write_cue(
        &layout.hooks_dir().join("archive.cue"),
        &format!(
            r#"hooks: [{{name: "archive-castle", events: ["agent_completed"], command: "{castle_program}"}}]"#
        ),
    );

    // A castle-level hook whose program does not exist: proves a failing
    // hook never blocks the agent's own completion or the other hooks.
    write_cue(
        &layout.hooks_dir().join("broken.cue"),
        r#"hooks: [{name: "broken-castle", events: ["agent_completed"], command: "/nonexistent/rk-hook-nowhere"}]"#,
    );

    // Repo-level hook: same event, additive alongside the castle hook (the
    // "repo extends castle" half of the acceptance criteria).
    let repo_out = home.path().join("repo-hook-out.txt");
    let repo_program = recorder_script(home.path(), "repo-hook.sh", &repo_out);
    write_cue(
        &repo_dir.path().join(".rk").join("hooks.cue"),
        &format!(
            r#"hooks: [{{name: "notify-repo", events: ["agent_completed"], command: "{repo_program}"}}]"#
        ),
    );

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // The event tuple's `scope` is the git-discovered repo name
    // (`rk_git::Repo::discover(path).name()`, the directory's basename), not
    // the registry's `name` param — align them so the repo-local hook's
    // scope check has something to match.
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "gnaw-hooks",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    let mut completed = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            completed = true;
            break;
        }
    }
    assert!(completed, "agent never completed");
    std::env::remove_var("RK_FAKE_HARNESS_CMD");

    // Both hooks (castle + repo) fire, even though a third, broken hook is
    // also configured for the same event: a failing hook does not stop the
    // reactor from dispatching to the others.
    let mut castle_seen = false;
    let mut repo_seen = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        castle_seen = castle_out.exists();
        repo_seen = repo_out.exists();
        if castle_seen && repo_seen {
            break;
        }
    }
    assert!(castle_seen, "castle-level hook never ran");
    assert!(repo_seen, "repo-level hook never ran");

    let castle_content = std::fs::read_to_string(&castle_out).unwrap();
    assert!(
        castle_content.contains("event=agent_completed"),
        "{castle_content}"
    );
    assert!(
        castle_content.contains("argv1=agent_completed"),
        "{castle_content}"
    );
    assert!(
        castle_content.contains(&format!("agent={name}")),
        "{castle_content}"
    );
    assert!(
        castle_content.contains(r#""identity":"harness_result""#),
        "the full event tuple is on stdin as JSON: {castle_content}"
    );
    assert!(
        castle_content.contains(&format!(r#""agent":"{name}""#)),
        "{castle_content}"
    );

    // The transcript path env var points at this exact generation's
    // transcript file location (`<agent>.<generation-stamp>.jsonl` under
    // `agent-logs/`) — the deliberate handle an archive hook ships. This
    // minimal fake harness narrates no Text/Tool events, so the file itself
    // may be empty or not yet flushed; what matters is the hook received the
    // real per-generation path, not a placeholder.
    let transcript_line = castle_content
        .lines()
        .find(|l| l.starts_with("transcript="))
        .expect("transcript line present");
    let transcript_path = transcript_line.trim_start_matches("transcript=");
    assert!(!transcript_path.is_empty(), "{castle_content}");
    assert!(
        transcript_path.contains(&format!("{name}.")) && transcript_path.ends_with(".jsonl"),
        "transcript path should be this generation's own agent-logs file: {transcript_path}"
    );

    let repo_content = std::fs::read_to_string(&repo_out).unwrap();
    assert!(
        repo_content.contains(&format!("scope={repo_name}")),
        "{repo_content}"
    );

    // The broken hook's failure is durable and visible (rate-capped
    // announcement), but never touched the agent's completed state above.
    let mut obstacle_seen = false;
    for _ in 0..50 {
        let obstacles = client
            .call(
                "space.scan",
                json!({"category": "obstacle", "identity": "hook_command_failed"}),
            )
            .await
            .unwrap();
        if !obstacles["tuples"].as_array().unwrap().is_empty() {
            obstacle_seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(obstacle_seen, "broken hook's failure was never announced");
}

/// A hook scoped to a DIFFERENT repo than the one that emitted the event
/// never fires — `hook.repo` (or a repo-local file's own repo) is a real
/// filter, not a hint.
#[cfg(unix)]
#[tokio::test]
async fn a_repo_scoped_hook_does_not_fire_for_a_different_repo() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();

    let out = home.path().join("scoped-hook-out.txt");
    let program = recorder_script(home.path(), "scoped-hook.sh", &out);
    write_cue(
        &layout.hooks_dir().join("scoped.cue"),
        &format!(
            r#"hooks: [{{name: "scoped-elsewhere", events: ["agent_completed"], command: "{program}", repo: "some-other-repo"}}]"#
        ),
    );

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // The event tuple's `scope` is the git-discovered repo name
    // (`rk_git::Repo::discover(path).name()`, the directory's basename), not
    // the registry's `name` param — align them so the repo-local hook's
    // scope check has something to match.
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "gnaw-scoped",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    let mut completed = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            completed = true;
            break;
        }
    }
    assert!(completed, "agent never completed");
    std::env::remove_var("RK_FAKE_HARNESS_CMD");

    // Give the reactor several cycles to prove the absence, not just the
    // first one.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !out.exists(),
        "a hook scoped to a different repo must never fire"
    );
}
