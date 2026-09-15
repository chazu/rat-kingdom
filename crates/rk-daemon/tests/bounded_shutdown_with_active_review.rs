//! TKT-karut-jaraf-hivur: a genuinely live two-`Daemon`-process reproduction
//! of the marker-held-reviewer shutdown hang, and proof of its fix.
//!
//! Before this ticket, `LandingPipeline::await_primary_verdict`'s poll loop
//! (`crates/rk-daemon/src/landing.rs`) had no way to notice a graceful
//! `stop` — it watched only the verdict pattern and the reviewer's liveness
//! — so a genuinely live, still-running reviewer held `Server::run`'s
//! shutdown `background_tasks.join_next()` open for up to
//! `GateConfig::review_max_wait` (production default 45 minutes), not any
//! bound related to the accept loop it had already closed. This mirrors
//! `live_landing_restart.rs`'s comment on the same consumer loop ("the
//! landing consumer loop's shutdown signal is only checked BETWEEN
//! cycles"), but drives it through review specifically, with a genuine
//! graceful `stop` RPC rather than `handle.abort()`.
//!
//! The fixture, closely modeled on `live_landing_restart.rs`'s own
//! reactor-driven end-to-end shape (`action: "land"` on `harness_result`,
//! not `LandingPipeline` internals, and not the SYNCHRONOUS `repo.land` /
//! `submit_manual` RPC, which blocks its own caller until the candidate
//! reaches a terminal outcome and so cannot be used to observe an
//! intermediate `awaiting_review` state at all): spawn a real fake-harness
//! rat whose diff is large enough to require review, let the reactor
//! enqueue it, hold its dispatched reviewer at a marker (a fake-harness
//! script blocked on a file this test never creates until told to), spawn a
//! second fake-harness rat (doc-only, needs no review) whose candidate must
//! remain queued behind the first (`drain_key`'s per-key single-consumer
//! invariant), issue a genuine graceful `stop`, and prove the old daemon's
//! `run()` future actually resolves within a small bound instead of hanging
//! behind the still-live reviewer. A second `Daemon::new` over the same
//! on-disk home then resumes: the SAME `review_instance_id` accepts a
//! directly-injected verdict (standing in for the reviewer eventually
//! reporting in — the marker-held process itself is orphaned by design,
//! exactly as a real daemon exit would leave it, so this test does not
//! depend on it reconnecting), both candidates land, and no duplicate
//! reviewer generation is ever spawned.

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

/// Same shape as `live_landing_restart.rs`'s own copy: the daemon-native
/// landing pipeline's completion feed.
const LANDING_TRIGGER: &str = r#"
triggers: [
    {
        name:   "bounded-shutdown-landing-on-completion"
        action: "land"
        match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}
        maxFires: 20
    },
]
"#;

/// Minimal review-only workflow, global (`layout.workflows_dir()`), using the
/// `fake` harness. `reviewTimeout` is deliberately long (well past anything
/// this test does) — this is the WORKFLOW instance's own `wait` step
/// deadline, distinct from `GateConfig::review_max_wait`; too short a value
/// here would time the workflow instance out from under a deliberately
/// long-held marker and misroute this fixture into `ReviewerDied` instead
/// of proving the live-reviewer shutdown bound this ticket is about.
const REVIEW_WORKFLOW: &str = r#"
package workflow
workflow: {
    name: "candidate-review"
    params: {
        taskId:        {type: "string", required: false, default: "unknown"}
        branch:        {type: "string", required: true}
        repo:          {type: "string", required: false, default: "unknown"}
        target:        {type: "string", required: false, default: "main"}
        headSha:       {type: "string", required: false, default: ""}
        reviewTimeout: {type: "string", required: false, default: "10m"}
    }
    agents: {default: {harness: "fake"}, reviewer: {harness: "fake"}}
    steps: [
        {type: "spawn", role: "reviewer", agent: "reviewer", branch: _input.branch,
         task: {title: "review-" + _input.taskId, description: "hold at marker"}},
        {type: "wait", timeout: _input.reviewTimeout},
        {type: "evaluate", expect: {is_error: false}},
    ]
}
"#;

/// One fake-harness script for all three spawned generations this fixture
/// needs, branching on `$RK_ROLE`/`$RK_TASK` so a single `RK_FAKE_HARNESS_CMD`
/// covers all of them:
/// - `reviewer`: blocks on a marker file this test controls, bounded to
///   ~30s so an orphaned process from a failed assertion still exits on its
///   own. Never writes the `review` verdict artifact itself — this fixture
///   injects that directly (module doc) — its only job is to stay
///   observably `Running` on command, reproducing the "marker-held
///   reviewer" the production incident described.
/// - task `candidate-a-substantial`: a 100-line non-doc file change — large
///   enough to classify `small` (not `trivial`), which is what routes
///   `process_entry` through review at all.
/// - anything else (`candidate-b-doc`): a doc-only change, which never
///   needs review.
const FAKE: &str = r#"
read -r _prompt
if [ "$RK_ROLE" = "reviewer" ]; then
  i=0
  while [ ! -f "$RK_HOME/release-reviewer" ] && [ "$i" -lt 600 ]; do
    sleep 0.05
    i=$((i+1))
  done
  echo '{"type":"system","subtype":"init","session_id":"marker-reviewer"}'
  rk_done "review complete"
  echo '{"type":"result","subtype":"success","is_error":false,"result":"reviewed","session_id":"marker-reviewer","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
elif [ "$RK_TASK" = "candidate-a-substantial" ]; then
  for i in $(seq 1 100); do echo "fn generated_$i() {}" >> src.rs; done
  git add src.rs >/dev/null 2>&1
  git -c user.email=r@x -c user.name=R commit -q -m "feat: substantial candidate needing review"
  echo '{"type":"system","subtype":"init","session_id":"impl-a"}'
  rk_done "work done"
  echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"impl-a","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
else
  mkdir -p docs
  echo "queued second candidate" > docs/note.md
  git add docs/note.md >/dev/null 2>&1
  git -c user.email=r@x -c user.name=R commit -q -m "docs: second queued candidate"
  echo '{"type":"system","subtype":"init","session_id":"impl-b"}'
  rk_done "work done"
  echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"impl-b","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
fi
"#;

fn git(dir: &Path, args: &[&str]) -> String {
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
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_passing_landing_checks(dir);
}

/// Mirrors the daemon's private `landing::review_instance_id`
/// (`crates/rk-daemon/src/landing.rs`): the exact commit-keyed identity a
/// review verdict artifact must carry (`review_attempt`) to be read as the
/// live attempt's answer instead of triggering a fresh reviewer spawn.
fn review_instance_id(
    repo_name: &str,
    branch: &str,
    head_sha: &str,
    target: &str,
    task: &str,
) -> String {
    use sha2::Digest;
    let digest =
        sha2::Sha256::digest(format!("{repo_name}@{branch}@{head_sha}@{target}@{task}").as_bytes());
    format!("landing-review-{}", hex::encode(&digest[..16]))
}

async fn wait_agent_completed(client: &mut Client, name: &str) {
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        match status["agent"]["state"].as_str() {
            Some("completed") => return,
            Some("failed") => panic!("rat failed instead of completing: {status}"),
            _ => {}
        }
    }
    panic!("rat {name} never completed");
}

async fn scan(client: &mut Client, scope: &str, identity: &str) -> Vec<Value> {
    client
        .call(
            "space.scan",
            json!({"category": "event", "scope": scope, "identity": identity}),
        )
        .await
        .unwrap()["tuples"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

async fn queue_entry(client: &mut Client, scope: &str, branch: &str) -> Option<Value> {
    scan(client, scope, "landing_queue_entry")
        .await
        .into_iter()
        .find(|t| t["payload"]["branch"] == branch)
}

fn ref_contains(repo: &Path, rev: &str, path: &str) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["cat-file", "-e", &format!("{rev}:{path}")])
        .output()
        .is_ok_and(|output| output.status.success())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_stop_exits_promptly_behind_a_marker_held_reviewer_and_resumes_cleanly() {
    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(FAKE));

    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let layout = Layout::at(home.path());
    layout.ensure().unwrap();
    std::fs::create_dir_all(layout.triggers_dir()).unwrap();
    std::fs::write(layout.triggers_dir().join("landing.cue"), LANDING_TRIGGER).unwrap();
    std::fs::create_dir_all(layout.workflows_dir()).unwrap();
    std::fs::write(
        layout.workflows_dir().join("candidate-review.cue"),
        REVIEW_WORKFLOW,
    )
    .unwrap();

    // Daemon A: genuinely on-disk (`Daemon::new`, not `new_in_memory`) — a
    // second `Daemon::new` below must inherit this durable state, not start
    // fresh, for the resumption half of this test to mean anything.
    let config = rk_core::config::Config::default();
    let daemon_a = Daemon::new(layout.clone(), &config).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;

    client
        .call(
            "repo.add",
            json!({"name": &repo_name, "path": repo_dir.path().to_string_lossy()}),
        )
        .await
        .unwrap();

    let spawned_a = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "candidate-a-substantial",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let agent_a = spawned_a["agent"]["name"].as_str().unwrap().to_string();
    let branch_a = spawned_a["agent"]["branch"].as_str().unwrap().to_string();
    wait_agent_completed(&mut client, &agent_a).await;

    // Poll until the reactor's trigger enqueued this completion and the
    // background landing pipeline genuinely passed gates and dispatched a
    // reviewer — the marker-held reviewer is only real once this is true.
    let mut awaiting_entry = None;
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if let Some(entry) = queue_entry(&mut client, &repo_name, &branch_a).await {
            if entry["payload"]["status"] == "awaiting_review" {
                awaiting_entry = Some(entry);
                break;
            }
        }
    }
    let awaiting_entry = awaiting_entry
        .expect("candidate-a never reached awaiting_review before the marker-held reviewer could be proven live");
    let head_sha_a = awaiting_entry["payload"]["head_sha"]
        .as_str()
        .unwrap()
        .to_string();
    let task_a = awaiting_entry["payload"]["task"]
        .as_str()
        .unwrap()
        .to_string();

    // Spawn the second candidate only now, while the reviewer is holding —
    // `drain_key`'s single-consumer-per-key loop cannot even claim it until
    // the in-flight `process_entry` for candidate-a returns, so this stays
    // genuinely `queued` for as long as the reviewer holds.
    let spawned_b = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo_dir.path().to_string_lossy(),
                "task": "candidate-b-doc",
                "harness": "fake",
            }),
        )
        .await
        .unwrap();
    let agent_b = spawned_b["agent"]["name"].as_str().unwrap().to_string();
    let branch_b = spawned_b["agent"]["branch"].as_str().unwrap().to_string();
    wait_agent_completed(&mut client, &agent_b).await;

    let mut b_queued = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if let Some(entry) = queue_entry(&mut client, &repo_name, &branch_b).await {
            if entry["payload"]["status"] == "queued" {
                b_queued = true;
                break;
            }
        }
    }
    assert!(
        b_queued,
        "candidate-b must remain queued in the landing queue behind the marker-held reviewer"
    );

    // The graceful stop itself: NOT `handle_a.abort()` — this is the exact
    // RPC a real `rk daemon stop`/rollover sends, so a bound proven here is
    // a bound `Server::run`'s real shutdown path actually honors.
    let stop_started = tokio::time::Instant::now();
    client.call("stop", json!({})).await.unwrap();

    // Before this ticket's fix, `background_tasks.join_next()` in
    // `Server::run` would block behind `await_primary_verdict`'s poll loop
    // for up to `GateConfig::review_max_wait` (production default 45
    // minutes) — the reviewer above is still genuinely `Running`, blocked on
    // a marker file this test has not created. A bound far below that
    // ceiling, proven against the REAL `run()` future (not an internal
    // `LandingPipeline` call), is the whole point of this fixture.
    let join_result = tokio::time::timeout(Duration::from_secs(15), handle_a)
        .await
        .expect(
            "Daemon::run must physically exit within a bounded time despite the live \
             marker-held reviewer — this is the TKT-karut-jaraf-hivur regression",
        );
    join_result.unwrap().unwrap();
    let stop_elapsed = stop_started.elapsed();
    assert!(
        stop_elapsed < Duration::from_secs(10),
        "graceful stop took {stop_elapsed:?}, which is no longer bounded well clear of the \
         15s timeout margin above"
    );

    // The durable state a restart must resume from: candidate-a — the one
    // whose reviewer was genuinely live and racing the shutdown signal at
    // the moment `stop` was called — is still exactly `awaiting_review`; no
    // forced/fake settlement was invented for it just to make shutdown
    // fast. (Candidate-b is deliberately NOT asserted on here: it was never
    // in flight when `stop` was issued, so whether the pre-existing
    // outer-loop shutdown race let one more harmless cycle drain it before
    // the accept loop's `join_next` returned is orthogonal to this ticket's
    // fix, which is specifically about a review wait already in progress.)
    {
        let space = rk_space::Space::open(&layout.db_path()).unwrap();
        let queued = space
            .scan(
                &rk_core::tuple::Pattern::category(rk_core::tuple::Category::Event)
                    .identity("landing_queue_entry"),
            )
            .unwrap();
        let a = queued
            .iter()
            .find(|t| t.payload["branch"] == branch_a)
            .expect("candidate-a must survive the graceful stop untouched");
        assert_eq!(a.payload["status"], "awaiting_review");
        let settled = space
            .scan(
                &rk_core::tuple::Pattern::category(rk_core::tuple::Category::Event)
                    .identity("landing_review_ceiling_settled"),
            )
            .unwrap();
        assert!(
            settled.is_empty(),
            "a graceful stop must never fabricate a review-ceiling settlement just to exit fast"
        );
    }

    // Daemon B: fresh `Daemon::new` over the SAME on-disk home.
    let daemon_b = Daemon::new(layout.clone(), &config).unwrap();
    let handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    // Stand in for the marker-held reviewer eventually reporting its
    // verdict (the process itself is orphaned by design — a graceful stop
    // must not have waited on it, which is exactly what was just proven —
    // so this fixture does not depend on it reconnecting). The instance id
    // is the SAME one `dispatch_review` computed before the stop; injecting
    // a verdict under it is only meaningful if the restarted pipeline is
    // still asking about that exact attempt, which is what
    // `review_instance_id` being deterministic guarantees.
    let instance_id = review_instance_id(&repo_name, &branch_a, &head_sha_a, "main", &task_a);
    client
        .call(
            "space.out",
            json!({
                "category": "artifact", "scope": &repo_name, "identity": "review",
                "payload": {
                    "task": &task_a, "recommendation": "APPROVE",
                    "notes": "resumed after graceful stop", "branch": &branch_a,
                    "head_sha": head_sha_a, "target": "main",
                    "review_attempt": instance_id,
                },
            }),
        )
        .await
        .unwrap();

    let mut both_landed = false;
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        if ref_contains(repo_dir.path(), "main", "src.rs")
            && ref_contains(repo_dir.path(), "main", "docs/note.md")
        {
            both_landed = true;
            break;
        }
    }
    assert!(
        both_landed,
        "both the resumed reviewed candidate and the queued second candidate must land \
         after the restart"
    );

    // No duplicate reviewer generation: `dispatch_review`'s repeat call for
    // the SAME `instance_id` after the restart must resolve to the already-
    // dispatched instance, never spawn a second one.
    let agents = client
        .call("agent.list", json!({"include_archived": true}))
        .await
        .unwrap();
    let reviewer_count = agents["agents"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["role"] == "reviewer" && a["repo_name"] == repo_name)
        .count();
    assert_eq!(
        reviewer_count, 1,
        "exactly one reviewer generation must ever have been dispatched for candidate-a"
    );

    handle_b.abort();
    let _ = handle_b.await;
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
