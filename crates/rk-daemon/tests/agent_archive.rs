//! TKT-136 end to end over the socket: `rk prune` offloads settled terminal
//! agent records out of the default views without losing their history.
//!
//! The registry never dropped a record, so `rk list`/`rk top` grew without
//! bound. These tests pin the contract of the fix: only Completed/Failed/
//! Dismissed records may leave the default view, live and Orphaned records
//! never do, cost/usage/lineage survives the move, and the whole thing
//! round-trips back through `agent.unarchive`.

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::start_daemon;

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
    support::install_passing_landing_checks(dir);
}

/// ONE fake harness for the whole binary, branching on the task name: a
/// `hang-*` task stays `running` forever (the live-agent guardrail needs a real
/// running rat), anything else commits real work and reports a Claude-style
/// success with a non-zero cost worth preserving through an archive.
///
/// Deliberately one script rather than two: `RK_FAKE_HARNESS_CMD` is a
/// process-global, so tests in this binary running in parallel would clobber
/// each other's script — the flake class behind TKT-88. Selecting behaviour
/// from the per-spawn task instead makes the env var write-once and racefree.
///
/// Both branches narrate one line of prose, so every rat here leaves a
/// transcript on disk for `--reap-logs` to have an opinion about.
///
/// A clean turn that never calls `rk done` now parks the agent as `Paused`
/// (awaiting resume) rather than `Completed`, so every `wait_for_state(...,
/// "completed")` below would time out. The `*` branch declares done via
/// `fixture::with_rk_done` before its result line, exactly as a real primed
/// rat does; `hang-*` deliberately never does, since it models a rat that is
/// still running.
fn fake() -> String {
    fixture::with_rk_done(
        r#"
read -r _prompt
case "$RK_TASK" in
  hang-*)
    echo '{"type":"system","subtype":"init","session_id":"fake-hang"}'
    echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"settling in"}]}}'
    sleep 300
    ;;
  *)
    echo "gnawed by $RK_AGENT for task $RK_TASK" > gnawed.txt
    git add gnawed.txt >/dev/null 2>&1
    git -c user.email=rat@x -c user.name=Rat commit -q -m "rat work: $RK_TASK"
    echo '{"type":"system","subtype":"init","session_id":"fake-archive"}'
    echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"gnawed it"}]}}'
    rk_done "committed gnawed.txt"
    echo '{"type":"result","subtype":"success","is_error":false,"result":"committed gnawed.txt","session_id":"fake-archive","total_cost_usd":0.002,"usage":{"input_tokens":50,"output_tokens":25,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
    ;;
esac
"#,
    )
}

async fn spawn(client: &mut Client, repo: &Path, task: &str, extra: Value) -> String {
    let mut params = json!({
        "repo": repo.to_string_lossy(),
        "task": task,
        "harness": "fake",
    });
    let map = params.as_object_mut().unwrap();
    for (k, v) in extra.as_object().cloned().unwrap_or_default() {
        map.insert(k, v);
    }
    let spawned = client.call("agent.spawn", params).await.unwrap();
    spawned["agent"]["name"].as_str().unwrap().to_string()
}

async fn wait_for_state(client: &mut Client, name: &str, want: &str) {
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == want {
            return;
        }
    }
    panic!("{name} never reached state {want}");
}

async fn list(client: &mut Client, params: Value) -> Vec<Value> {
    client.call("agent.list", params).await.unwrap()["agents"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// Spend the budget rollup attributes to one workflow instance.
async fn instance_spend(client: &mut Client, instance: &str) -> Option<f64> {
    client.call("budget.rollup", json!({})).await.unwrap()["instances"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .find(|i| i["instance"] == instance)
        .and_then(|i| i["spent_usd"].as_f64())
}

fn names(agents: &[Value]) -> Vec<String> {
    let mut n: Vec<String> = agents
        .iter()
        .map(|a| a["name"].as_str().unwrap_or("?").to_string())
        .collect();
    n.sort();
    n
}

/// The whole capability in one pass: a dry run previews without mutating, the
/// real archive moves ONLY the settled terminal record (the running rat stays
/// put), the default `agent.list` hides it while `include_archived` /
/// `archived_only` surface it, its cost/usage/lineage survives, and
/// `agent.unarchive` puts it back.
#[tokio::test]
async fn archive_hides_terminal_records_and_round_trips() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fake());
    let layout = Layout::at(home.path());
    let mut client = start_daemon(&layout).await;

    // A rat that finishes and is left standing (state Completed). Carries a
    // parent and a workflow instance so we can prove lineage survives, and
    // stays undismissed so it keeps counting toward the instance rollup.
    let done = spawn(
        &mut client,
        repo_dir.path(),
        "archive-1",
        json!({"parent": "KingRat", "workflow_instance": "wf-archive"}),
    )
    .await;
    wait_for_state(&mut client, &done, "completed").await;

    // ...and one that was dismissed, so both terminal states are exercised.
    let gone = spawn(&mut client, repo_dir.path(), "archive-2", json!({})).await;
    wait_for_state(&mut client, &gone, "completed").await;
    let dismissed = client
        .call("agent.dismiss", json!({"name": gone}))
        .await
        .unwrap();
    assert_eq!(
        dismissed["merged"], false,
        "dismiss must preserve the branch"
    );

    // Spend recorded before the archive, to compare against afterwards.
    let spend_before = instance_spend(&mut client, "wf-archive")
        .await
        .expect("instance spend recorded");
    assert!(spend_before > 0.0);

    // A rat still working — the guardrail's subject.
    let live = spawn(&mut client, repo_dir.path(), "hang-archive-3", json!({})).await;
    let terminal = names(&[json!({"name": &done}), json!({"name": &gone})]);
    let everyone = names(&[
        json!({"name": &done}),
        json!({"name": &gone}),
        json!({"name": &live}),
    ]);

    // Dry run: reports the terminal rats, touches nothing.
    let preview = client
        .call("agent.archive", json!({"all": true, "dry_run": true}))
        .await
        .unwrap();
    assert_eq!(preview["dry_run"], true);
    assert_eq!(
        names(preview["agents"].as_array().unwrap()),
        terminal,
        "preview must list exactly the eligible records"
    );
    assert_eq!(
        names(&list(&mut client, json!({})).await),
        everyone,
        "a dry run must not mutate the registry"
    );

    // The real thing.
    let archived = client
        .call("agent.archive", json!({"all": true}))
        .await
        .unwrap();
    assert_eq!(archived["count"], 2);
    assert_eq!(names(archived["agents"].as_array().unwrap()), terminal);

    // Default view: only the running rat. The live rat is never archived.
    assert_eq!(
        names(&list(&mut client, json!({})).await),
        vec![live.clone()]
    );
    // Opt in and the archived records are back in view, flagged as archived.
    assert_eq!(
        names(&list(&mut client, json!({"include_archived": true})).await),
        everyone
    );
    let only_archived = list(&mut client, json!({"archived_only": true})).await;
    assert_eq!(names(&only_archived), terminal);
    assert!(
        only_archived.iter().all(|a| a["archived_at"].is_string()),
        "archived rows carry their archived_at stamp"
    );

    // History survived intact: cost, usage, and lineage.
    let status = client
        .call("agent.status", json!({"name": done}))
        .await
        .unwrap();
    assert_eq!(status["agent"]["cost_usd"], 0.002);
    assert_eq!(status["agent"]["usage"]["input"], 50);
    assert_eq!(status["agent"]["usage"]["output"], 25);
    assert_eq!(status["agent"]["parent"], "KingRat");
    assert_eq!(status["agent"]["workflow_instance"], "wf-archive");
    assert_eq!(status["agent"]["state"], "completed");

    // ...and so did the budget rollup it feeds.
    assert_eq!(
        instance_spend(&mut client, "wf-archive").await,
        Some(spend_before),
        "archiving must not move a budget number"
    );

    // Round trip.
    client
        .call("agent.unarchive", json!({"name": done}))
        .await
        .unwrap();
    assert_eq!(
        names(&list(&mut client, json!({})).await),
        names(&[json!({"name": &done}), json!({"name": &live})]),
        "unarchive restores the record to the default view"
    );
    assert_eq!(
        names(&list(&mut client, json!({"archived_only": true})).await),
        vec![gone.clone()],
        "the record we did not restore stays archived"
    );

    let _ = client.call("agent.dismiss", json!({"name": live})).await;
}

/// `--before` is a real threshold, not a formality: a record that went terminal
/// moments ago stays in the live view until it ages past the window.
#[tokio::test]
async fn before_threshold_spares_fresh_records() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fake());
    let layout = Layout::at(home.path());
    let mut client = start_daemon(&layout).await;

    let name = spawn(&mut client, repo_dir.path(), "fresh-1", json!({})).await;
    wait_for_state(&mut client, &name, "completed").await;

    // Default window (7d): a rat that finished seconds ago is far too fresh.
    let archived = client.call("agent.archive", json!({})).await.unwrap();
    assert_eq!(archived["count"], 0, "a fresh record must not be archived");
    assert_eq!(
        names(&list(&mut client, json!({})).await),
        vec![name.clone()]
    );

    // A zero-length window is "everything up to now" — the same cutoff `--all`
    // uses — and now the record is eligible.
    let archived = client
        .call("agent.archive", json!({"before": "0s"}))
        .await
        .unwrap();
    assert_eq!(archived["count"], 1);
    assert!(list(&mut client, json!({})).await.is_empty());

    // A malformed spec is a clean parameter error, never a silent full sweep.
    assert!(
        client
            .call("agent.archive", json!({"before": "7"}))
            .await
            .is_err(),
        "a bare number is ambiguous and must be rejected"
    );
}

/// Live and Orphaned records are structurally ineligible. Orphaned matters
/// most: its worktree/branch/session are preserved for `rk respawn`, so
/// archiving one would retire a rat that is still meant to come back.
#[tokio::test]
async fn live_and_orphaned_records_are_never_archived() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fake());
    let layout = Layout::at(home.path());
    let mut client = start_daemon(&layout).await;

    let live = spawn(&mut client, repo_dir.path(), "hang-orphan-1", json!({})).await;
    wait_for_state(&mut client, &live, "running").await;

    // Even the most aggressive sweep leaves a running rat alone.
    let archived = client
        .call("agent.archive", json!({"all": true}))
        .await
        .unwrap();
    assert_eq!(archived["count"], 0, "a running rat must never be archived");

    // Restart the daemon against the same home: the running rat becomes
    // Orphaned, the state `rk respawn` exists for.
    client.call("stop", json!({})).await.ok();
    let mut client = start_daemon(&layout).await;
    let status = client
        .call("agent.status", json!({"name": live}))
        .await
        .unwrap();
    assert_eq!(status["agent"]["state"], "orphaned");

    let archived = client
        .call("agent.archive", json!({"all": true}))
        .await
        .unwrap();
    assert_eq!(
        archived["count"], 0,
        "an orphaned rat must never be archived"
    );
    assert_eq!(names(&list(&mut client, json!({})).await), vec![live]);
}

/// `--reap-git` reclaims the worktree and branch of an archived rat whose work
/// already landed, and refuses to touch one whose branch is still unmerged —
/// that branch holds the only copy of the work.
#[tokio::test]
async fn reap_git_reclaims_merged_branches_and_refuses_unmerged_ones() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fake());
    let layout = Layout::at(home.path());
    let mut client = start_daemon(&layout).await;

    // Two rats that finish but are never dismissed, so both keep their
    // worktree and branch — exactly the leftovers an operator prunes by hand.
    let landed = spawn(&mut client, repo_dir.path(), "reap-1", json!({})).await;
    wait_for_state(&mut client, &landed, "completed").await;
    let stranded = spawn(&mut client, repo_dir.path(), "reap-2", json!({})).await;
    wait_for_state(&mut client, &stranded, "completed").await;

    // Registry rows key the agent as `name`; the reap report keys it `agent`.
    let pick = |rows: &[Value], key: &str, want: &str| -> Value {
        rows.iter()
            .find(|r| r[key] == want)
            .cloned()
            .unwrap_or_else(|| panic!("no row for {want} in {rows:?}"))
    };
    let agents = list(&mut client, json!({})).await;
    let landed_rec = pick(&agents, "name", &landed);
    let stranded_rec = pick(&agents, "name", &stranded);
    let landed_branch = landed_rec["branch"].as_str().unwrap().to_string();
    let stranded_branch = stranded_rec["branch"].as_str().unwrap().to_string();
    let stranded_worktree =
        std::path::PathBuf::from(stranded_rec["worktree"].as_str().unwrap().to_string());
    let landed_worktree =
        std::path::PathBuf::from(landed_rec["worktree"].as_str().unwrap().to_string());
    assert!(landed_worktree.exists() && stranded_worktree.exists());

    // Only the first rat's work reaches main.
    git(
        repo_dir.path(),
        &["merge", "--no-ff", "-m", "land", &landed_branch],
    );

    let result = client
        .call("agent.archive", json!({"all": true, "reap_git": true}))
        .await
        .unwrap();
    assert_eq!(
        result["count"], 2,
        "both terminal records archive either way"
    );
    let reaped = result["reaped"].as_array().cloned().unwrap_or_default();
    let row = |name: &str| pick(&reaped, "agent", name);

    assert_eq!(
        row(&landed)["reaped"],
        json!(true),
        "merged branch should be reclaimed: {}",
        row(&landed)["reason"]
    );
    assert!(!landed_worktree.exists(), "merged rat's worktree removed");

    assert_eq!(
        row(&stranded)["reaped"],
        json!(false),
        "an unmerged branch must be refused"
    );
    assert!(
        row(&stranded)["reason"]
            .as_str()
            .unwrap_or("")
            .contains("not merged"),
        "reason should say why: {}",
        row(&stranded)["reason"]
    );
    assert!(
        stranded_worktree.exists(),
        "an unmerged rat's worktree must be left standing"
    );

    let branches = Command::new("git")
        .arg("-C")
        .arg(repo_dir.path())
        .args(["branch", "--list", "--format=%(refname:short)"])
        .output()
        .unwrap();
    let branches = String::from_utf8_lossy(&branches.stdout).to_string();
    assert!(
        !branches.lines().any(|b| b == landed_branch),
        "merged branch deleted, got: {branches}"
    );
    assert!(
        branches.lines().any(|b| b == stranded_branch),
        "unmerged branch preserved, got: {branches}"
    );
}

/// O12 (docs/2026-08-18-drain-probe-log.md): `--reap-artifacts` reclaims a
/// terminal agent's regenerable build artifacts (this repo declares `target`
/// through its activated `.rk/repo.cue` `reap.artifactPaths` — the daemon
/// itself has no built-in notion of what any language's build directory is
/// called) REGARDLESS of merge state — unlike `--reap-git`, an unmerged
/// branch's build output is exactly as regenerable as a merged one's. A
/// running agent's artifacts are never touched, and only the named artifact
/// path is removed: the worktree, branch, and every other file survive.
#[tokio::test]
async fn reap_artifacts_reclaims_terminal_worktrees_regardless_of_merge_state() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());
    // STACK NEUTRALITY (TKT-P3b): the daemon's own artifact_paths default is
    // now empty, so this repo must declare its own `target` reap path
    // through its activated `.rk/repo.cue` policy for `--reap-artifacts` to
    // have anything to do.
    std::fs::create_dir_all(repo_dir.path().join(".rk")).unwrap();
    std::fs::write(
        repo_dir.path().join(".rk/repo.cue"),
        r#"repo: { reap: { artifactPaths: ["target"] } }"#,
    )
    .unwrap();
    git(repo_dir.path(), &["add", ".rk"]);
    git(repo_dir.path(), &["commit", "-m", "policy: reap target/"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", fake());
    let layout = Layout::at(home.path());
    let mut client = start_daemon(&layout).await;
    let repo_name = repo_dir.path().file_name().unwrap().to_string_lossy();
    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    // A live agent — its worktree must never be touched by any reap pass.
    let live = spawn(&mut client, repo_dir.path(), "hang-artifacts-1", json!({})).await;
    wait_for_state(&mut client, &live, "running").await;

    // A terminal agent whose branch is NEVER merged — the exact case
    // `reap_git` refuses and leaves standing, but whose `target/` is exactly
    // as regenerable as a merged branch's.
    let stranded = spawn(&mut client, repo_dir.path(), "artifacts-1", json!({})).await;
    wait_for_state(&mut client, &stranded, "completed").await;

    let pick = |rows: &[Value], key: &str, want: &str| -> Value {
        rows.iter()
            .find(|r| r[key] == want)
            .cloned()
            .unwrap_or_else(|| panic!("no row for {want} in {rows:?}"))
    };
    let agents = list(&mut client, json!({})).await;
    let live_worktree =
        std::path::PathBuf::from(pick(&agents, "name", &live)["worktree"].as_str().unwrap());
    let stranded_rec = pick(&agents, "name", &stranded);
    let stranded_worktree = std::path::PathBuf::from(stranded_rec["worktree"].as_str().unwrap());
    let stranded_branch = stranded_rec["branch"].as_str().unwrap().to_string();

    // Simulate a cargo build in both worktrees.
    for wt in [&live_worktree, &stranded_worktree] {
        std::fs::create_dir_all(wt.join("target/debug")).unwrap();
        std::fs::write(wt.join("target/debug/build-marker"), b"binary").unwrap();
        // A sibling file that must never be touched by an artifact-only reap.
        std::fs::write(wt.join("keepme.txt"), b"source, not artifact").unwrap();
    }

    let result = client
        .call(
            "agent.archive",
            json!({"all": true, "reap_artifacts": true}),
        )
        .await
        .unwrap();
    assert_eq!(result["count"], 1, "the running agent must not be archived");

    let reaped = result["reaped_artifacts"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let row = pick(&reaped, "agent", &stranded);
    assert_eq!(row["reaped"], json!(true), "{}", row["reason"]);
    assert!(
        row["reason"].as_str().unwrap_or("").contains("target"),
        "reason names what was removed: {}",
        row["reason"]
    );

    assert!(
        !stranded_worktree.join("target").exists(),
        "the terminal (though unmerged) agent's target/ must be reaped"
    );
    assert!(
        stranded_worktree.join("keepme.txt").exists(),
        "artifact reap must never touch source files"
    );
    assert!(
        stranded_worktree.exists(),
        "artifact reap must never remove the worktree itself"
    );
    let branches = Command::new("git")
        .arg("-C")
        .arg(repo_dir.path())
        .args(["branch", "--list", "--format=%(refname:short)"])
        .output()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&branches.stdout)
            .lines()
            .any(|b| b == stranded_branch),
        "artifact reap must never delete the branch"
    );

    assert!(
        live_worktree.join("target").exists(),
        "a live agent's build artifacts must never be touched"
    );
}

/// `--reap-logs` (TKT-162) reclaims the last artifact an archived rat leaves
/// behind: its `agent-logs/` transcript. Each file is a bounded ring, but the
/// COUNT grew once per rat forever.
///
/// The contract has three halves. A record that archives loses its own
/// generation's file. A record that does not archive keeps its transcript,
/// whatever else is being swept. And the legacy name-keyed file is never
/// touched — it can still hold a generation nobody is archiving, which is the
/// hazard that kept this from being wired up before transcripts were keyed on
/// a generation.
#[tokio::test]
async fn reap_logs_deletes_archived_transcripts_and_spares_retained_ones() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var("RK_FAKE_HARNESS_CMD", fake());
    let layout = Layout::at(home.path());
    let mut client = start_daemon(&layout).await;

    // One rat that settles (archivable) and one still running (structurally
    // never archivable). Both narrate, so both have a transcript.
    let settled = spawn(&mut client, repo_dir.path(), "reap-log-1", json!({})).await;
    wait_for_state(&mut client, &settled, "completed").await;
    let running = spawn(&mut client, repo_dir.path(), "hang-reap-log", json!({})).await;
    wait_for_state(&mut client, &running, "running").await;

    let logs = home.path().join("agent-logs");
    let files = || -> Vec<String> {
        std::fs::read_dir(&logs)
            .map(|d| {
                d.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default()
    };
    // Generation-keyed: `<name>.<stamp>.jsonl`. Match on the prefix rather than
    // recomputing the stamp, so the test does not restate the naming rule.
    let transcript_of = |name: &str| -> Option<String> {
        files()
            .into_iter()
            .find(|f| f.starts_with(&format!("{name}.")) && f != &format!("{name}.jsonl"))
    };

    // The running rat's prose arrives asynchronously; wait for both files.
    let mut both = false;
    for _ in 0..100 {
        if transcript_of(&settled).is_some() && transcript_of(&running).is_some() {
            both = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(both, "both rats should have transcripts, got {:?}", files());
    let settled_file = logs.join(transcript_of(&settled).unwrap());
    let running_file = logs.join(transcript_of(&running).unwrap());

    // A pre-TKT-158 file under the settled rat's bare name. It may hold a
    // generation that is NOT being archived, so a reap must leave it alone.
    let legacy = logs.join(format!("{settled}.jsonl"));
    std::fs::write(&legacy, "{\"ts\":\"2020-01-01T00:00:00Z\",\"kind\":\"text\",\"text\":\"an older rat of this name\"}\n").unwrap();

    let result = client
        .call("agent.archive", json!({"all": true, "reap_logs": true}))
        .await
        .unwrap();
    assert_eq!(result["count"], 1, "only the settled rat archives");

    let reaped_logs = result["reaped_logs"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(reaped_logs.len(), 1, "one row per archived record");
    assert_eq!(reaped_logs[0]["agent"], json!(settled));
    assert_eq!(
        reaped_logs[0]["reaped"],
        json!(true),
        "reason: {}",
        reaped_logs[0]["reason"]
    );

    assert!(
        !settled_file.exists(),
        "the archived rat's transcript should be gone"
    );
    assert!(
        running_file.exists(),
        "a rat that did not archive keeps its transcript"
    );
    assert!(
        legacy.exists(),
        "the legacy name-keyed file is never reaped"
    );

    // The two reap passes are independent switches: asking for logs must not
    // quietly start deleting branches and worktrees too.
    assert!(
        result["reaped"]
            .as_array()
            .is_none_or(|rows| rows.is_empty()),
        "no git reap was requested, got {}",
        result["reaped"]
    );
}
