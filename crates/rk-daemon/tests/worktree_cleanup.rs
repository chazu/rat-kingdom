//! TKT-01M04N6W4X47KMXDA6MH0WPH8H end to end: guaranteed worktree cleanup on
//! instance terminalization, the orphan-sweep safety checks, and the
//! disk-pressure spawn guard.
//!
//! Root cause of the 2026-08-16 incident: steward/workflow failure paths skip
//! their own `dismiss` step, so a terminal agent's worktree (and its
//! multi-GB cargo `target/`) can persist indefinitely — 104 of them, 298 GB,
//! drove the disk to 97% full and the daemon started failing writes. These
//! tests pin the contract of the fix: a terminalizing workflow instance
//! reclaims every spawned agent's worktree even when the workflow's own CUE
//! never ran a `dismiss` step; an unattended reclaim never force-deletes a
//! worktree with uncommitted changes; the periodic sweep actually runs
//! unattended; and a spawn is refused (with an inbox obstacle) once free disk
//! drops below the configured floor.

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Once;
use std::time::Duration;
use support::connect;

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
    support::install_default_repository_policy(dir);
}

/// One process-global fake harness for the whole binary (mirrors
/// agent_archive.rs / fleet_budget.rs — `RK_FAKE_HARNESS_CMD` is a process
/// env var, so install the identical script once and never rewrite it while
/// parallel tests are spawning agents, TKT-88). Branches on `$RK_TASK`:
///
/// - `dirty-*`: writes an UNCOMMITTED file, then reports success. The branch
///   never diverges from base (trivially "merged"), but the worktree itself
///   carries uncommitted changes — the case `reap_git`'s dirty-check exists
///   for.
/// - `noop-*`: does nothing (no writes, no commits) — a branch identical to
///   base, so it is both trivially merged AND clean.
/// - `hang-*`: stays `running` forever — the live-agent guardrail needs a
///   real running rat to prove against (mirrors agent_archive.rs's FAKE).
/// - anything else: commits real work, then reports success — ordinary
///   "finished but never dismissed" leftovers.
///
/// Every completing branch declares `rk done` before its result line: a
/// clean turn that never does now parks the agent as `Paused` (awaiting
/// resume) rather than `Completed`, which every non-`hang-*` wait here is
/// keyed on. `hang-*` deliberately never does, since it models a rat that is
/// still running.
fn fake() -> String {
    fixture::with_rk_done(
        r#"
read -r _prompt
case "$RK_TASK" in
  dirty-*)
    echo "uncommitted" > dirty.txt
    echo '{"type":"system","subtype":"init","session_id":"fake-dirty"}'
    rk_done "left dirty"
    echo '{"type":"result","subtype":"success","is_error":false,"result":"left dirty","session_id":"fake-dirty","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
    ;;
  noop-*)
    echo '{"type":"system","subtype":"init","session_id":"fake-noop"}'
    rk_done "nothing to do"
    echo '{"type":"result","subtype":"success","is_error":false,"result":"nothing to do","session_id":"fake-noop","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
    ;;
  hang-*)
    if [[ "$RK_TASK" == hang-sweep-artifacts-* ]]; then
      mkdir -p target/debug
      echo binary > target/debug/build-marker
    fi
    echo '{"type":"system","subtype":"init","session_id":"fake-hang"}'
    echo '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"settling in"}]}}'
    sleep 300
    ;;
  *)
    if [[ "$RK_TASK" == sweep-artifacts-1 ]]; then
      mkdir -p target/debug
      echo binary > target/debug/build-marker
    fi
    echo "gnawed by $RK_AGENT for task $RK_TASK" > gnawed.txt
    git add gnawed.txt >/dev/null 2>&1
    git -c user.email=rat@x -c user.name=Rat commit -q -m "rat work: $RK_TASK"
    echo '{"type":"system","subtype":"init","session_id":"fake-work"}'
    rk_done "committed gnawed.txt"
    echo '{"type":"result","subtype":"success","is_error":false,"result":"committed gnawed.txt","session_id":"fake-work","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
    ;;
esac
"#,
    )
}

fn install_fake() {
    static FAKE_HARNESS: Once = Once::new();
    FAKE_HARNESS.call_once(|| std::env::set_var("RK_FAKE_HARNESS_CMD", fake()));
}

async fn spawn_record(client: &mut Client, repo: &Path, task: &str, extra: Value) -> Value {
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
    spawned["agent"].clone()
}

async fn spawn(client: &mut Client, repo: &Path, task: &str, extra: Value) -> String {
    spawn_record(client, repo, task, extra).await["name"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn wait_for_state(client: &mut Client, name: &str, want: &str) {
    for _ in 0..200 {
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

/// Poll `agent.list` until `name` appears. `agent.status` (what
/// `wait_for_state` reads) and `agent.list` are backed by separate reads, so
/// a state transition confirmed via `wait_for_state` is not guaranteed to be
/// visible to `agent.list` yet — asserting single-shot presence right after
/// `wait_for_state` races that visibility gap (TKT-01M0CY0SRKS6MGCT2NT79BZY6N).
async fn wait_for_list_record(client: &mut Client, name: &str) -> Value {
    for _ in 0..200 {
        let agents = list(client, json!({"include_archived": true})).await;
        if let Some(rec) = agents.iter().find(|a| a["name"] == name) {
            return rec.clone();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no record for {name} ever appeared in agent.list");
}

/// A workflow that spawns one rat, waits, and checks the result — but,
/// unlike `solo-task.cue`, deliberately has NO `dismiss` step. This is the
/// exact shape of the root-cause failure mode: a workflow whose own CUE never
/// reaches cleanup.
const NO_DISMISS: &str = r#"
workflow: {
    name: "no-dismiss"
    steps: [
        {type: "spawn", role: "rat", task: {title: "finalize-sweep-1"}, harness: "fake"},
        {type: "wait", timeout: "30s"},
        {type: "evaluate", expect: {is_error: false}},
    ]
}
"#;

/// Same shape as `NO_DISMISS`, but the spawned task's title starts with
/// `dirty-` so the fake harness (see `FAKE` above) leaves an uncommitted file
/// in the worktree instead of committing. Exercises the finalize-time sweep's
/// dirty-worktree guard (TKT-01M05A2GRAHMMD2RB8CTMNP7RY rework 2).
const DIRTY_NO_DISMISS: &str = r#"
workflow: {
    name: "dirty-no-dismiss"
    steps: [
        {type: "spawn", role: "rat", task: {title: "dirty-finalize-1"}, harness: "fake"},
        {type: "wait", timeout: "30s"},
        {type: "evaluate", expect: {is_error: false}},
    ]
}
"#;

fn init_workflow_repo() -> tempfile::TempDir {
    let repo = tempfile::tempdir().unwrap();
    scratch_repo(repo.path());
    let wf_dir = repo.path().join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("no-dismiss.cue"), NO_DISMISS).unwrap();
    std::fs::write(wf_dir.join("dirty-no-dismiss.cue"), DIRTY_NO_DISMISS).unwrap();
    repo
}

async fn run_workflow(client: &mut Client, repo: &Path, name: &str) -> String {
    let started = client
        .call(
            "workflow.run",
            json!({"name": name, "repo": repo.to_string_lossy(), "params": {}}),
        )
        .await
        .unwrap();
    started["instance"]["id"].as_str().unwrap().to_string()
}

/// Wait for the instance to reach EITHER terminal status. The guaranteed-
/// cleanup sweep runs on both (`finalize` fires for `Completed` and `Failed`
/// alike), and whether this particular fixture's `evaluate` step is satisfied
/// is not the point of this test — only that the instance terminalizes
/// without ever running a `dismiss` step.
async fn wait_workflow_terminal(client: &mut Client, id: &str) -> String {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        let s = status["instance"]["status"]
            .as_str()
            .unwrap_or("")
            .to_string();
        if s == "completed" || s == "failed" {
            return s;
        }
    }
    panic!("instance {id} never reached a terminal status");
}

/// TKT item 1 (GUARANTEED CLEANUP): a workflow instance that completes
/// without ever running a `dismiss`/`dismiss_all` step must still leave its
/// spawned agent dismissed and its worktree reclaimed — the finalize-time
/// safety net, not the per-arm CUE step.
#[tokio::test]
async fn finalize_dismisses_agents_the_workflow_never_dismissed() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = init_workflow_repo();

    install_fake();
    let layout = Layout::at(home.path());
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    // Bare/test daemons default this off (TKT-01M04N6W4X47KMXDA6MH0WPH8H
    // rework: left on unconditionally, every workflow-based e2e test paid
    // for a synchronous git reclaim at finalize time, adding enough load
    // under a full parallel `cargo test --workspace` run to tip unrelated
    // tests' fixed polling timeouts over the edge). This is the one test
    // that exercises the guarantee, so opt back in explicitly.
    daemon.set_worktree_sweep_config(rk_core::config::WorktreeSweepConfig {
        enabled: false,
        finalize_cleanup_enabled: true,
        ..rk_core::config::WorktreeSweepConfig::default()
    });
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    let instance = run_workflow(&mut client, repo_dir.path(), "no-dismiss").await;
    wait_workflow_terminal(&mut client, &instance).await;

    // The spawned agent's worktree must be reclaimed even though the
    // workflow's own steps never dismissed it. Poll: finalize persists the
    // instance's terminal status BEFORE running the cleanup sweep, so
    // `workflow.status` can read "completed" a moment before the sweep
    // actually finishes dismissing the agent.
    let mut worktree: Option<PathBuf> = None;
    let mut final_state: Option<String> = None;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let agents = list(&mut client, json!({})).await;
        let Some(rec) = agents.iter().find(|a| a["workflow_instance"] == instance) else {
            continue;
        };
        worktree = rec["worktree"].as_str().map(PathBuf::from);
        final_state = rec["state"].as_str().map(str::to_string);
        if final_state.as_deref() == Some("dismissed") {
            break;
        }
    }
    assert_eq!(
        final_state.as_deref(),
        Some("dismissed"),
        "finalize's cleanup sweep must dismiss an agent its workflow never dismissed"
    );
    let worktree = worktree.expect("agent record carried a worktree path");
    assert!(
        !worktree.exists(),
        "finalize's cleanup sweep must reclaim the worktree: {worktree:?} still exists"
    );
}

/// Rework 2 (TKT-01M05A2GRAHMMD2RB8CTMNP7RY): the finalize-time cleanup sweep
/// (`dismiss_orphaned_instance_agents`) must apply the SAME dirty-worktree
/// guard `reap_git` already applies to the periodic sweep, instead of
/// unconditionally force-removing via `dismiss`. An instance that
/// terminalizes with a spawned agent still holding uncommitted edits must
/// leave that worktree standing and surface an obstacle naming it — the
/// salvage window budget-killed rats depend on must not close just because
/// the reclaim happened through finalize instead of the periodic sweep.
#[tokio::test]
async fn finalize_sweep_parks_a_dirty_worktree() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = init_workflow_repo();

    install_fake();
    let layout = Layout::at(home.path());
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_worktree_sweep_config(rk_core::config::WorktreeSweepConfig {
        enabled: false,
        finalize_cleanup_enabled: true,
        ..rk_core::config::WorktreeSweepConfig::default()
    });
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    let instance = run_workflow(&mut client, repo_dir.path(), "dirty-no-dismiss").await;
    wait_workflow_terminal(&mut client, &instance).await;

    // Same poll shape as the clean-case test above: finalize persists the
    // instance's terminal status before the sweep runs, so wait for the
    // sweep's own effect (the agent record settling) rather than trusting
    // `workflow.status` alone.
    let mut worktree: Option<PathBuf> = None;
    let mut agent_name: Option<String> = None;
    let mut final_state: Option<String> = None;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let agents = list(&mut client, json!({})).await;
        let Some(rec) = agents.iter().find(|a| a["workflow_instance"] == instance) else {
            continue;
        };
        worktree = rec["worktree"].as_str().map(PathBuf::from);
        agent_name = rec["name"].as_str().map(str::to_string);
        final_state = rec["state"].as_str().map(str::to_string);
        if final_state.as_deref() == Some("dismissed") {
            break;
        }
    }
    assert_eq!(
        final_state.as_deref(),
        Some("dismissed"),
        "finalize's cleanup sweep must still dismiss the agent record, only sparing the worktree"
    );
    let worktree = worktree.expect("agent record carried a worktree path");
    let agent_name = agent_name.expect("agent record carried a name");
    assert!(
        worktree.join("dirty.txt").exists(),
        "fake harness should have left an uncommitted file, and the sweep must not have removed it: {worktree:?}"
    );

    let obstacles = client
        .call("space.scan", json!({"category": "obstacle"}))
        .await
        .unwrap();
    let tuples = obstacles["tuples"].as_array().cloned().unwrap_or_default();
    let found = tuples.iter().any(|t| {
        t["payload"]["type"] == json!("worktree_parked_dirty")
            && t["payload"]["agent"] == json!(agent_name)
    });
    assert!(
        found,
        "expected a worktree_parked_dirty obstacle naming {agent_name}: {tuples:?}"
    );
}

/// TKT item 2 (ORPHAN SWEEP safety): `agent.archive --reap-git` must never
/// force-remove a worktree that still carries uncommitted changes, even when
/// its branch is trivially "merged" (never diverged from base). Uncommitted
/// edits are not captured by any commit, so a merged branch cannot vouch for
/// them.
#[tokio::test]
async fn reap_git_leaves_a_dirty_worktree_standing() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    install_fake();
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    let name = spawn(&mut client, repo_dir.path(), "dirty-1", json!({})).await;
    wait_for_state(&mut client, &name, "completed").await;

    let agents = list(&mut client, json!({})).await;
    let rec = agents.iter().find(|a| a["name"] == name).unwrap();
    let worktree = PathBuf::from(rec["worktree"].as_str().unwrap());
    assert!(worktree.exists());
    assert!(
        worktree.join("dirty.txt").exists(),
        "fake harness should have left an uncommitted file"
    );

    let result = client
        .call("agent.archive", json!({"all": true, "reap_git": true}))
        .await
        .unwrap();
    assert_eq!(result["count"], 1);
    let reaped = result["reaped"].as_array().cloned().unwrap_or_default();
    let row = reaped
        .iter()
        .find(|r| r["agent"] == name)
        .cloned()
        .unwrap_or_else(|| panic!("no reap row for {name} in {reaped:?}"));
    assert_eq!(
        row["reaped"],
        json!(false),
        "a dirty worktree must never be force-removed"
    );
    assert!(
        row["reason"].as_str().unwrap_or("").contains("uncommitted"),
        "reason should say why: {}",
        row["reason"]
    );
    assert!(
        worktree.exists(),
        "a dirty worktree must be left standing, not force-removed"
    );
}

/// TKT item 2 (ORPHAN SWEEP, periodic): the `[worktree_sweep]` daemon loop
/// actually reclaims a leaked worktree unattended — no `rk prune` call from
/// the test at all.
#[tokio::test]
async fn periodic_sweep_reclaims_a_leaked_worktree() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    install_fake();
    let layout = Layout::at(home.path());
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_worktree_sweep_config(rk_core::config::WorktreeSweepConfig {
        enabled: true,
        interval_secs: 1,
        after_days: 0,
        // Out of scope for this test (no workflow instance involved, only a
        // direct `agent.spawn`); left off to keep the test's assertion
        // attributable to the periodic loop alone.
        finalize_cleanup_enabled: false,
        ..rk_core::config::WorktreeSweepConfig::default()
    });
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    // A clean, trivially-merged (never diverged from base) leftover — exactly
    // the leaked-worktree scenario the sweep exists to reclaim.
    let spawned = spawn_record(&mut client, repo_dir.path(), "noop-1", json!({})).await;
    let name = spawned["name"].as_str().unwrap().to_string();
    let worktree = PathBuf::from(spawned["worktree"].as_str().unwrap());
    wait_for_state(&mut client, &name, "completed").await;

    assert!(worktree.exists());

    // Never dismissed or pruned by the test — only the periodic sweep touches it.
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if !worktree.exists() {
            return;
        }
    }
    panic!("periodic worktree sweep never reclaimed {worktree:?}");
}

/// O12 (docs/2026-08-18-drain-probe-log.md), periodic-sweep half: the
/// `[worktree_sweep]` loop reaps a terminal agent's regenerable build
/// artifacts (`target/`) within one sweep interval REGARDLESS of merge
/// state, while a live agent's `target/` is left completely untouched. A
/// probe day accumulated 231 GB of terminal rats' `target/` dirs because the
/// sweep only ever reclaimed worktrees wholesale for MERGED branches; an
/// unmerged branch's build output is exactly as regenerable and must not
/// have to wait on a merge that may never come.
#[tokio::test]
async fn periodic_sweep_reaps_terminal_artifacts_regardless_of_merge_state() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    install_fake();
    let layout = Layout::at(home.path());
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_worktree_sweep_config(rk_core::config::WorktreeSweepConfig {
        enabled: true,
        interval_secs: 1,
        after_days: 0,
        finalize_cleanup_enabled: false,
        // Operator-set fallback: this repo is unregistered (no activated
        // `.rk/repo.cue`), so `reap_artifacts` falls back to this list. The
        // shipped default is empty — see
        // `periodic_sweep_reaps_artifacts_immediately_under_default_after_days`
        // for the repo-policy-driven path this exists alongside.
        artifact_paths: vec!["target".to_string()],
        ..rk_core::config::WorktreeSweepConfig::default()
    });
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    let live = spawn(
        &mut client,
        repo_dir.path(),
        "hang-sweep-artifacts-1",
        json!({}),
    )
    .await;
    wait_for_state(&mut client, &live, "running").await;

    // Commits real work, so the branch diverges from base and is never
    // merged by anything in this test — the case `reap_git` refuses.
    let stranded = spawn(&mut client, repo_dir.path(), "sweep-artifacts-1", json!({})).await;
    wait_for_state(&mut client, &stranded, "completed").await;

    // `include_archived`: this test's own `after_days: 0` also makes the
    // record eligible for the SAME sweep's archive half (worktree_sweep_once
    // reaps artifacts immediately, then separately archives anything past the
    // cutoff), so by the time this reads back the record it may already have
    // been archived — orthogonal to what this test is actually verifying.
    //
    // Poll rather than a single-shot list(): `wait_for_state` above only
    // confirms `agent.status` observed "completed", not that `agent.list`
    // has caught up to the same record yet.
    let live_rec = wait_for_list_record(&mut client, &live).await;
    let live_worktree = PathBuf::from(live_rec["worktree"].as_str().unwrap());
    let stranded_rec = wait_for_list_record(&mut client, &stranded).await;
    let stranded_worktree = PathBuf::from(stranded_rec["worktree"].as_str().unwrap());
    let stranded_branch = stranded_rec["branch"].as_str().unwrap().to_string();

    // Never dismissed or pruned by the test — only the periodic sweep touches it.
    let mut reaped = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if !stranded_worktree.join("target").exists() {
            reaped = true;
            break;
        }
    }
    assert!(
        reaped,
        "periodic sweep never reaped {stranded_worktree:?}/target within the interval"
    );
    assert!(
        stranded_worktree.exists(),
        "artifact reap must never remove the (unmerged) worktree itself"
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
        "artifact reap must never delete an unmerged branch"
    );
    assert!(
        live_worktree.join("target").exists(),
        "a live agent's build artifacts must never be touched by the periodic sweep"
    );
}

/// Rework of TKT-01M0BH4ZW5DDF8F6J3TTBCFH70 item 1: artifact reap must not
/// wait on `after_days`. The test above pins the mechanism with
/// `after_days: 0`, which can't distinguish "reaped immediately" from
/// "reaped once the (already-elapsed) cutoff passed" — it would pass even if
/// artifact reap were still wired through the age-gated `archive_agents`
/// path. This one runs the SHIPPED DEFAULT `WorktreeSweepConfig` (whose
/// `after_days` is 3, and whose `artifact_paths` is now EMPTY — TKT-P3b
/// stack neutrality: the daemon has no built-in notion of what any
/// language's build directory is called) and proves a `target/` dir is still
/// gone well within one sweep interval, driven entirely by the repo's own
/// activated `.rk/repo.cue` `reap.artifactPaths` — the exact default-config
/// gap the reviewer flagged: a newly terminal agent's build artifacts must
/// not stand for `after_days` before the first sweep even looks at them.
#[tokio::test]
async fn periodic_sweep_reaps_artifacts_immediately_under_default_after_days() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());
    std::fs::create_dir_all(repo_dir.path().join(".rk")).unwrap();
    std::fs::write(
        repo_dir.path().join(".rk/repo.cue"),
        r#"repo: { reap: { artifactPaths: ["target"] } }"#,
    )
    .unwrap();
    git(repo_dir.path(), &["add", ".rk"]);
    git(repo_dir.path(), &["commit", "-m", "policy: reap target/"]);

    install_fake();
    let layout = Layout::at(home.path());
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_worktree_sweep_config(rk_core::config::WorktreeSweepConfig {
        enabled: true,
        interval_secs: 1,
        finalize_cleanup_enabled: false,
        // Deliberately NOT overridden: this is the shipped default (3 days,
        // empty artifact_paths) — the reap below must come entirely from the
        // repo's own registered policy, not this daemon-wide fallback.
        ..rk_core::config::WorktreeSweepConfig::default()
    });
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    let repo_name = repo_dir.path().file_name().unwrap().to_string_lossy();
    let added = client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();
    assert!(
        added["repo"]["activated_policy"]["digest"]
            .as_str()
            .is_some_and(|d| d.len() == 64),
        "registration must activate the repo's reap.artifactPaths policy: {added}"
    );

    let name = spawn(
        &mut client,
        repo_dir.path(),
        "sweep-artifacts-default-1",
        json!({}),
    )
    .await;
    wait_for_state(&mut client, &name, "completed").await;

    let agents = list(&mut client, json!({})).await;
    let rec = agents.iter().find(|a| a["name"] == name).unwrap();
    let worktree = PathBuf::from(rec["worktree"].as_str().unwrap());

    std::fs::create_dir_all(worktree.join("target/debug")).unwrap();
    std::fs::write(worktree.join("target/debug/build-marker"), b"binary").unwrap();

    let mut reaped = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if !worktree.join("target").exists() {
            reaped = true;
            break;
        }
    }
    assert!(
        reaped,
        "periodic sweep never reaped {worktree:?}/target under the default (after_days: 3) config \
         within the interval — artifact reap must not be gated by the archiving cutoff"
    );
    assert!(
        worktree.exists(),
        "artifact reap must never remove the worktree itself"
    );
}

/// STACK NEUTRALITY (binding, TKT-P3b): the shipped `WorktreeSweepConfig`
/// default is empty and a repo that never activates a `reap.artifactPaths`
/// policy of its own gets no artifact reaping at all — the daemon must never
/// guess a build directory name on a repo's behalf. Pins the "reap nothing"
/// half of the default that `periodic_sweep_reaps_artifacts_immediately_under_default_after_days`
/// pins the opposite (policy-declared) half of.
#[tokio::test]
async fn periodic_sweep_reaps_nothing_for_a_repo_with_no_declared_artifact_paths() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    install_fake();
    let layout = Layout::at(home.path());
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_worktree_sweep_config(rk_core::config::WorktreeSweepConfig {
        enabled: true,
        interval_secs: 1,
        after_days: 0,
        finalize_cleanup_enabled: false,
        // Shipped default: no fallback paths, and this repo is never
        // registered so it has no activated policy either.
        ..rk_core::config::WorktreeSweepConfig::default()
    });
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    let name = spawn(
        &mut client,
        repo_dir.path(),
        "sweep-artifacts-no-policy-1",
        json!({}),
    )
    .await;
    wait_for_state(&mut client, &name, "completed").await;

    let agents = list(&mut client, json!({})).await;
    let rec = agents.iter().find(|a| a["name"] == name).unwrap();
    let worktree = PathBuf::from(rec["worktree"].as_str().unwrap());

    std::fs::create_dir_all(worktree.join("target/debug")).unwrap();
    std::fs::write(worktree.join("target/debug/build-marker"), b"binary").unwrap();

    // Several sweep ticks to give a wrongly-firing reap a chance to act.
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert!(
        worktree.join("target").exists(),
        "with no declared reap.artifactPaths anywhere, the shipped-default sweep must reap nothing"
    );
}

/// Rework of TKT-01M0BH4ZW5DDF8F6J3TTBCFH70 item 2: an `artifact_paths` entry
/// that resolves to the worktree root itself (`.`) must be rejected the same
/// way an absolute path or a `..` segment already is — `reap_artifacts`'s
/// path-safety check previously only caught empty/absolute/`..` paths, so a
/// misconfigured `.` would `remove_dir_all` the ENTIRE worktree (source,
/// git state, everything), not just a regenerable build directory.
#[tokio::test]
async fn periodic_sweep_rejects_artifact_path_that_resolves_to_worktree_root() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    install_fake();
    let layout = Layout::at(home.path());
    let mut daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_worktree_sweep_config(rk_core::config::WorktreeSweepConfig {
        enabled: true,
        interval_secs: 1,
        after_days: 0,
        finalize_cleanup_enabled: false,
        artifact_paths: vec![".".to_string()],
        ..rk_core::config::WorktreeSweepConfig::default()
    });
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    let spawned = spawn_record(
        &mut client,
        repo_dir.path(),
        "sweep-artifacts-root-1",
        json!({}),
    )
    .await;
    let name = spawned["name"].as_str().unwrap().to_string();
    let worktree = PathBuf::from(spawned["worktree"].as_str().unwrap());
    wait_for_state(&mut client, &name, "completed").await;

    assert!(worktree.exists());
    assert!(
        worktree.join("gnawed.txt").exists(),
        "fake harness should have committed a source file"
    );

    // Give the periodic sweep several ticks to (wrongly) act on the unsafe
    // path before asserting nothing was touched.
    tokio::time::sleep(Duration::from_millis(400)).await;

    assert!(
        worktree.exists(),
        "an artifact_paths entry resolving to the worktree root must never delete the worktree"
    );
    assert!(
        worktree.join("gnawed.txt").exists(),
        "an artifact_paths entry resolving to the worktree root must never touch source/git state: {worktree:?}"
    );
    assert!(
        worktree.join(".git").exists(),
        "worktree git state must be untouched by a rejected artifact path"
    );
}

/// TKT item 3 (DISK PRESSURE GUARD): a spawn is refused before it creates a
/// new worktree once free disk is below the configured floor, and the
/// refusal surfaces as an inbox obstacle rather than running the disk to zero
/// and failing deep inside an io path.
#[tokio::test]
async fn spawn_refused_when_disk_floor_breached() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    // An impossible floor: guaranteed to exceed any real disk's free space,
    // so the refusal is deterministic regardless of the test machine.
    daemon.set_min_free_disk_gb(1_000_000_000);
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    let refused = client
        .call(
            "agent.spawn",
            json!({"repo": repo_dir.path().to_string_lossy(), "task": "disk-1", "harness": "fake"}),
        )
        .await;
    assert!(
        refused.is_err(),
        "spawn should be refused once the disk floor is breached, got {refused:?}"
    );
    let msg = format!("{}", refused.unwrap_err());
    assert!(
        msg.contains("min_free_gb") || msg.contains("disk"),
        "error names the disk floor: {msg}"
    );

    let obstacles = client
        .call("space.scan", json!({"category": "obstacle"}))
        .await
        .unwrap();
    let kinds: Vec<String> = obstacles["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["payload"]["type"].as_str().unwrap_or("").to_string())
        .collect();
    assert!(
        kinds.contains(&"disk_pressure".to_string()),
        "disk-pressure obstacle posted: {kinds:?}"
    );
}

/// O12 (docs/2026-08-18-drain-probe-log.md): a disk-floor refusal must not be
/// silent. The `rk inbox` obstacle row above existed before this ticket and
/// still needed an operator to go looking — this pins the fix, that the
/// refusal ALSO goes out through the same `RecoveryAnnouncer` machinery every
/// other automated recovery action uses (respawn, kill-process-group), so it
/// reaches the configured notification sinks. The first refusal in a rolling
/// hour announces normally; a second refusal in the same window is rate-held
/// (raised severity, "HELD" in the text) rather than silently dropped —
/// "silence is earned later, not shipped now" applies here exactly as much
/// as it does to `respawn_sweep`.
#[tokio::test]
async fn disk_floor_refusal_announces_through_the_recovery_sinks() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    daemon.set_min_free_disk_gb(1_000_000_000);
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    for _ in 0..2 {
        let _ = client
            .call(
                "agent.spawn",
                json!({"repo": repo_dir.path().to_string_lossy(), "task": "disk-announce-1", "harness": "fake"}),
            )
            .await;
    }

    let events = client
        .call(
            "space.scan",
            json!({"category": "event", "identity": "recovery_action"}),
        )
        .await
        .unwrap();
    let rows: Vec<Value> = events["tuples"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|t| t["payload"]["action_kind"] == "disk-floor")
        .collect();
    assert_eq!(
        rows.len(),
        2,
        "both refusals announce (silence is never the fallback): {rows:?}"
    );

    let first_text = rows[0]["payload"]["notice"]["text"].as_str().unwrap_or("");
    assert!(
        first_text.contains("GB"),
        "the announcement names free/floor GB: {first_text}"
    );
    assert_eq!(
        rows[0]["payload"]["held"],
        json!(false),
        "the first refusal in the rolling hour must not be held"
    );
    assert_eq!(
        rows[1]["payload"]["held"],
        json!(true),
        "a second refusal within the same rolling hour must be rate-held, not silent"
    );
    let second_text = rows[1]["payload"]["notice"]["text"].as_str().unwrap_or("");
    assert!(
        second_text.contains("rate cap hit"),
        "a held escalation still explains why: {second_text}"
    );
}
