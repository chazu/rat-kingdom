//! `rk log`, end to end through the daemon RPC (TKT-25).
//!
//! The bounded ring, tail, and follow-broadcast are unit-tested in
//! `agent_log`; this proves the wiring at the supervisor boundary: assistant
//! text and tool calls the supervisor used to DROP are now persisted per-agent
//! and served back by `agent.log`. Plus (TKT-158) that a name which named two
//! rats serves each of them separately.

use chrono::{DateTime, TimeDelta, Utc};
mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::agents::{AgentRecord, AgentState, Registry};
use rk_daemon::Daemon;
use serde_json::json;
use std::time::Duration;
use support::connect;

/// A fake rat that narrates: one prose chunk, one tool call, then completes.
/// These `assistant`/`tool_use` events are exactly what `handle_event` used to
/// throw away.
///
/// A clean turn that never calls `rk done` now parks the agent as `Paused`
/// (awaiting resume) rather than `Completed`, so `agent never completed`
/// below would fire. Declare done via `fixture::with_rk_done` before the
/// result line, exactly as a real primed rat does.
fn chatty_fake() -> String {
    fixture::with_rk_done(
        r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"fake-log"}'
echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"planning the gnaw"}]}}'
echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","name":"Bash","id":"t1","input":{}}]}}'
rk_done "done"
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"fake-log","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#,
    )
}

#[tokio::test]
async fn supervisor_persists_transcript_and_log_serves_it() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", chatty_fake());
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "gnaw-log",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();

    // Wait for completion so all events have been pumped through handle_event.
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

    // The transcript captured both the prose chunk and the tool call. Real
    // environments can emit incidental harness stderr (e.g. a shell locale
    // warning) that the fixture script never asked for, and it is not
    // guaranteed to land before or after the fixture's own lines — so locate
    // the fixture's entries by content/tag instead of assuming a fixed
    // position or count.
    let log = client
        .call("agent.log", json!({"name": name}))
        .await
        .unwrap();
    let entries = log["entries"].as_array().unwrap();
    let non_stderr: Vec<_> = entries.iter().filter(|e| e["kind"] != "stderr").collect();
    assert_eq!(
        non_stderr.len(),
        2,
        "text + tool call persisted (ignoring incidental stderr): {entries:?}"
    );
    assert_eq!(non_stderr[0]["kind"], "text");
    assert_eq!(non_stderr[0]["text"], "planning the gnaw");
    assert_eq!(non_stderr[1]["kind"], "tool");
    assert_eq!(non_stderr[1]["name"], "Bash");
    assert!(non_stderr[0]["ts"].is_string(), "each entry is timestamped");

    // `tail` bounds the result to the most-recent entries: verify it returns
    // exactly the true last entry, whatever kind that happens to be, rather
    // than assuming it is the fixture's tool call (incidental stderr could
    // legitimately be the last line captured).
    let tailed = client
        .call("agent.log", json!({"name": name, "tail": 1}))
        .await
        .unwrap();
    let tail = tailed["entries"].as_array().unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(
        &tail[0],
        entries.last().unwrap(),
        "tail returns the true last entry"
    );

    // An unknown agent is an empty transcript, not an error.
    let empty = client
        .call("agent.log", json!({"name": "ghost"}))
        .await
        .unwrap();
    assert!(empty["entries"].as_array().unwrap().is_empty());
    assert_eq!(empty["generations"], 0, "nothing has carried that name");

    // The reply says which rat of that name this is (TKT-158). One generation is
    // the normal case, and the client stays quiet about it.
    assert_eq!(log["generations"], 1);
    assert_eq!(log["generation"], 1);
    assert_eq!(
        log["created_at"].as_str(),
        spawned["agent"]["created_at"].as_str(),
        "the generation reported is the record's created_at"
    );

    // Asking for a generation that never existed is a parameter error, not a
    // silent fallback to a different rat's transcript.
    let bad = client
        .call("agent.log", json!({"name": name, "generation": 2}))
        .await;
    assert!(bad.is_err(), "generation 2 of a one-generation name");

    // The transcript is filed under the generation, not the bare name.
    let logs = home.path().join("agent-logs");
    let files: Vec<String> = std::fs::read_dir(&logs)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(files.len(), 1, "one transcript file: {files:?}");
    assert!(
        files[0].starts_with(&format!("{name}.")) && files[0] != format!("{name}.jsonl"),
        "transcript must be generation-keyed, got {:?}",
        files[0]
    );

    // E4: an exact spawn id resolves the same generation directly, without
    // going through name (+ordinal) resolution at all.
    let spawn = spawned["agent"]["spawn"].as_str().unwrap().to_string();
    let by_spawn = client
        .call("agent.log", json!({"name": spawn}))
        .await
        .unwrap();
    assert_eq!(by_spawn["entries"], log["entries"], "same transcript");
    assert_eq!(
        by_spawn["agent"].as_str(),
        Some(name.as_str()),
        "the reply discloses which name the spawn id resolved to"
    );

    // An unknown spawn id is a parameter error, not a silent empty transcript.
    let bad_spawn = client
        .call(
            "agent.log",
            json!({"name": rk_core::id::SpawnId::new().to_string()}),
        )
        .await;
    assert!(bad_spawn.is_err(), "no agent carries that spawn id");

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// The historical shape TKT-158 exists for: one name, two unrelated rats, and a
/// single legacy `<name>.jsonl` holding both their transcripts back to back.
/// `agent.log` must serve each rat its own lines and neither rat the other's.
///
/// The fixture is built through `Registry` rather than by spawning, because
/// spawning can no longer produce it — TKT-146 stopped recycling names. This is
/// the on-disk state 24 names were left in before that fix.
#[tokio::test]
async fn a_name_that_named_two_rats_serves_each_generation_separately() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    let now = Utc::now();
    let older_at = now - TimeDelta::try_days(2).unwrap();
    let newer_at = now - TimeDelta::try_hours(1).unwrap();

    {
        let mut reg = Registry::load(&home.path().join("agents.json")).unwrap();
        reg.insert(terminal_record("Gouda", older_at)).unwrap();
        // Archiving is what freed the name back then; it is also what keeps the
        // older generation discoverable now.
        reg.archive(now).unwrap();
        reg.insert(terminal_record("Gouda", newer_at)).unwrap();
    }
    // The pre-generation file layout: both rats' lines in one name-keyed file.
    write_legacy_transcript(
        home.path(),
        "Gouda",
        &[
            (older_at + TimeDelta::try_seconds(5).unwrap(), "first rat"),
            (
                older_at + TimeDelta::try_seconds(9).unwrap(),
                "first rat again",
            ),
            (newer_at + TimeDelta::try_seconds(5).unwrap(), "second rat"),
        ],
    );

    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // A bare name serves the NEWEST rat, and says the name is ambiguous.
    let newest = client
        .call("agent.log", json!({"name": "Gouda"}))
        .await
        .unwrap();
    assert_eq!(newest["generations"], 2, "two rats carried this name");
    assert_eq!(newest["generation"], 2, "the newest is the default");
    let entries = newest["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1, "only the second rat's line");
    assert_eq!(entries[0]["text"], "second rat");

    // ...and the older rat's transcript is reachable, de-interleaved out of the
    // same legacy file by its generation window.
    let oldest = client
        .call("agent.log", json!({"name": "Gouda", "generation": 1}))
        .await
        .unwrap();
    assert_eq!(oldest["generation"], 1);
    let entries = oldest["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2, "both of the first rat's lines");
    assert_eq!(entries[0]["text"], "first rat");
    assert_eq!(entries[1]["text"], "first rat again");
    assert_eq!(
        oldest["created_at"].as_str().unwrap(),
        older_at.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true),
        "and it is labelled with the older rat's spawn instant"
    );

    // `tail` bounds the selected generation, not the file.
    let tailed = client
        .call(
            "agent.log",
            json!({"name": "Gouda", "generation": 1, "tail": 1}),
        )
        .await
        .unwrap();
    let entries = tailed["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["text"], "first rat again");

    // A generation that never existed is an error, not a quiet fallback to a
    // different rat's transcript.
    assert!(client
        .call("agent.log", json!({"name": "Gouda", "generation": 3}))
        .await
        .is_err());
}

/// A settled record, as a rat that finished two days ago would be left.
fn terminal_record(name: &str, created_at: DateTime<Utc>) -> AgentRecord {
    AgentRecord {
        name: name.into(),
        spawn: None,
        role: "rat".into(),
        coordination: None,
        harness: "fake".into(),
        permission_mode: None,
        model: None,
        repo_root: "/tmp/repo".into(),
        repo_name: "repo".into(),
        task: Some(format!("task-for-{created_at}")),
        branch: None,
        fork_point: None,
        worktree: None,
        target_branch: "main".into(),
        parent: None,
        workflow_instance: None,
        review: None,
        coordinator: None,
        session_id: None,
        attach_target: None,
        pid: None,
        merge_commit: None,
        state: AgentState::Dismissed,
        result: None,
        progress: None,
        crashed: false,
        stderr_tail: None,
        usage: Default::default(),
        cost_usd: 0.0,
        created_at,
        updated_at: created_at,
        archived_at: None,
        liveness: Default::default(),
        transport_outage: None,
        recovery: None,
    }
}

/// Write the pre-TKT-158 file layout by hand: `agent-logs/<name>.jsonl`.
fn write_legacy_transcript(home: &std::path::Path, agent: &str, lines: &[(DateTime<Utc>, &str)]) {
    let dir = home.join("agent-logs");
    std::fs::create_dir_all(&dir).unwrap();
    let body: String = lines
        .iter()
        .map(|(ts, text)| format!("{}\n", json!({"ts": ts, "kind": "text", "text": text})))
        .collect();
    std::fs::write(dir.join(format!("{agent}.jsonl")), body).unwrap();
}

fn scratch_repo(dir: &std::path::Path) {
    let git = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}");
    };
    git(&["init", "-b", "main"]);
    git(&["config", "user.email", "rat@example.com"]);
    git(&["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# scratch\n").unwrap();
    git(&["add", "."]);
    git(&["commit", "-m", "init"]);
}
