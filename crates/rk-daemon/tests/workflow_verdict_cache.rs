//! Phase 2 of the steward remediation (memory/steward-investigation): a
//! commit-keyed verdict cache so a retry against an UNCHANGED branch tip does
//! not re-pay for a reviewer.
//!
//! Depends on TKT-01M036N1RT74H6NPRH5FMM8A6T (Phase 0, already landed), which
//! put `head_sha` into completion payloads and review artifacts, and on the
//! `forCommit`/`onTimeout: "continue"` `read` predicate added alongside this
//! file, which lets a step lift ANY prior verdict for an exact commit —
//! regardless of who wrote it — without failing the instance when nothing is
//! cached yet.
//!
//! Reduced to the seam under test, the same way `workflow_verdict_binding.rs`
//! reduces the real steward: a leading cache probe, then a `when` whose `""`
//! (miss) arm spawns a reviewer exactly as before, and whose `default` (hit)
//! arm routes on the cached recommendation WITHOUT spawning anything. The
//! reviewer script counts its own invocations into a file under `$RK_HOME`,
//! so "did not spawn a reviewer" is a measurement, not an inference from the
//! instance's final state.

mod fixture;

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

/// Every test in this file drives `RK_FAKE_HARNESS_CMD`, a process-global env
/// var — and unlike the earlier reduced-workflow tests (which all share one
/// identical fake script), the shipped-steward e2e test below installs a
/// DIFFERENT script. Concurrent test threads racing to set it would let one
/// test's harness content leak into another's run, so every fn in this file
/// takes this lock first (the convention `automated_landing.rs`/
/// `workflow_checks.rs`/`review_tiering.rs` already use for the same reason).
static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
    String::from_utf8_lossy(&out.stdout).to_string()
}

/// A reviewer that parks until the test releases it, so the test can plant a
/// SHA-tagged verdict and learn the spawned agent's name before the harness
/// reports back — the same barrier `workflow_verdict_binding.rs` uses, needed
/// only by the cache-MISS test below (the hit tests never spawn a reviewer at
/// all, so there is nothing to barrier).
///
/// Counts its own invocation into `$RK_HOME/reviewer-invocations` — the
/// direct measurement that the expensive reviewer agent did or did not run,
/// rather than inferring it from routing alone.
const BARRIERED_REVIEWER: &str = r#"
read -r _prompt
echo x >> "$RK_HOME/reviewer-invocations"
i=0
while [ ! -f "$RK_HOME/go" ] && [ "$i" -lt 600 ]; do sleep 0.05; i=$((i+1)); done
echo '{"type":"system","subtype":"init","session_id":"cache-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"reviewed","session_id":"cache-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// The steward's Phase 2 shape, reduced to the seam under test: probe the
/// verdict cache for `_input.headSha`; a miss (`""`) spawns a reviewer, waits,
/// and routes on the verdict IT wrote (`fromAgent: true`, unchanged from
/// before the cache); a hit (`default`) skips straight to routing on the
/// cached recommendation — no spawn, no wait.
const WORKFLOW: &str = r#"
workflow: {
    name: "verdict-cache-test"
    params: {
        headSha: {type: "string", required: false, default: ""}
        branch: {type: "string", required: false, default: ""}
    }
    agents: {
        default: {harness: "fake", model: "sonnet"}
    }
    steps: [
        {
            type:      "read"
            category:  "artifact"
            identity:  "review"
            forCommit: _input.headSha
            forBranch: _input.branch
            field:     "recommendation"
            into:      "cachedVerdict"
            timeout:   "1s"
            onTimeout: "continue"
        },
        {
            type: "when"
            var:  "cachedVerdict"
            cases: {
                "": [
                    {type: "spawn", role: "reviewer", task: {title: "review-the-branch", description: "Review it"}},
                    {type: "wait", timeout: "60s"},
                    {type: "evaluate", expect: {is_error: false}},
                    {
                        type:      "read"
                        category:  "artifact"
                        identity:  "review"
                        fromAgent: true
                        field:     "recommendation"
                        into:      "verdict"
                        timeout:   "30s"
                    },
                    {
                        type: "when"
                        var:  "verdict"
                        cases: {
                            "APPROVE": [{type: "dismiss", noMerge: true}]
                            "REWORK": [
                                {type: "dismiss", noMerge: true},
                                {type: "stop", reason: "routed fresh REWORK"},
                            ]
                        }
                        default: [
                            {type: "dismiss", noMerge: true},
                            {type: "stop", reason: "routed nothing"},
                        ]
                    },
                ]
            }
            default: [
                {
                    type: "when"
                    var:  "cachedVerdict"
                    cases: {
                        "APPROVE": []
                        "REWORK": [{type: "stop", reason: "routed cached REWORK"}]
                    }
                    default: [{type: "stop", reason: "routed cached unknown"}]
                },
            ]
        },
    ]
}
"#;

fn init_repo(dir: &Path) -> String {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    support::install_default_repository_policy(dir);
    let wf_dir = dir.join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("verdict-cache-test.cue"), WORKFLOW).unwrap();
    dir.file_name().unwrap().to_string_lossy().to_string()
}

async fn run_workflow(
    client: &mut Client,
    repo_dir: &Path,
    head_sha: &str,
    branch: &str,
) -> String {
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "verdict-cache-test",
                "repo": repo_dir.to_string_lossy(),
                "params": {"headSha": head_sha, "branch": branch},
            }),
        )
        .await
        .unwrap();
    started["instance"]["id"].as_str().unwrap().to_string()
}

async fn instance(client: &mut Client, id: &str) -> serde_json::Value {
    client
        .call("workflow.status", json!({"name": id}))
        .await
        .unwrap()["instance"]
        .clone()
}

/// Poll until the instance settles; returns the whole terminal record.
async fn await_instance(client: &mut Client, id: &str) -> serde_json::Value {
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let inst = instance(client, id).await;
        match inst["status"].as_str().unwrap_or("") {
            "completed" | "failed" => return inst,
            _ => {}
        }
    }
    panic!("workflow instance {id} never settled");
}

/// The reviewer this instance spawned, once `spawn` has recorded it.
async fn await_reviewer(client: &mut Client, id: &str) -> String {
    for _ in 0..200 {
        let inst = instance(client, id).await;
        if let Some(agent) = inst["context"]["active_agent"].as_str() {
            return agent.to_string();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("instance {id} never spawned a reviewer");
}

fn release_reviewers(home: &Path) {
    std::fs::write(home.join("go"), "").unwrap();
}

/// Record a verdict exactly as `rk out artifact <repo> review` would, carrying
/// `head_sha`+`branch` the way steward.cue's reviewer prompt does
/// (TKT-01M036N1RT74H6NPRH5FMM8A6T, and the rework of
/// TKT-01M036NWEG0H019BJ16G59RZVP that added `branch`).
async fn plant_verdict(
    client: &mut Client,
    scope: &str,
    agent: &str,
    recommendation: &str,
    head_sha: &str,
    branch: &str,
) {
    let spawn = client
        .call("agent.status", json!({"name": agent}))
        .await
        .ok()
        .and_then(|status| status["agent"]["spawn"].as_str().map(str::to_string));
    let mut payload = json!({
        "agent": agent,
        "task": "review-the-branch",
        "recommendation": recommendation,
        "notes": format!("verdict for {agent}"),
        "head_sha": head_sha,
        "branch": branch,
    });
    if let Some(spawn) = spawn {
        payload["spawn"] = json!(spawn);
    }
    client
        .call(
            "space.out",
            json!({
                "category": "artifact",
                "scope": scope,
                "identity": "review",
                "payload": payload,
            }),
        )
        .await
        .unwrap();
}

/// How many times the reviewer harness actually ran — the direct cost
/// measurement the cache exists to avoid paying twice.
fn reviewer_invocation_count(home: &Path) -> usize {
    std::fs::read_to_string(home.join("reviewer-invocations"))
        .map(|s| s.lines().filter(|l| !l.is_empty()).count())
        .unwrap_or(0)
}

/// A retry against a branch tip ALREADY reviewed by someone else consumes
/// that cached APPROVE and completes without ever spawning a reviewer.
#[tokio::test]
async fn same_sha_retry_consumes_cached_approve_without_spawning_a_reviewer() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path());

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(BARRIERED_REVIEWER),
    );
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    // A prior run's reviewer (any agent, any run) already recorded APPROVE
    // for this exact commit on this exact branch.
    plant_verdict(
        &mut client,
        &repo_name,
        "earlier-reviewer",
        "APPROVE",
        "sha-abc123",
        "rat/x/work",
    )
    .await;

    let id = run_workflow(&mut client, repo_dir.path(), "sha-abc123", "rat/x/work").await;
    let done = await_instance(&mut client, &id).await;

    assert_eq!(
        done["context"]["vars"]["cachedVerdict"],
        json!("APPROVE"),
        "the cache probe must lift the prior run's verdict: {done:?}"
    );
    assert_eq!(
        done["status"], "completed",
        "a cached APPROVE must complete the run exactly like a fresh one, got: {done:?}"
    );
    assert_eq!(
        reviewer_invocation_count(home.path()),
        0,
        "a cache hit must never spawn the reviewer harness"
    );
}

/// A retry against a branch tip whose only recorded verdict belongs to a
/// DIFFERENT commit is a cache miss: it falls through to spawning a fresh
/// reviewer, exactly as steward did before the cache existed.
#[tokio::test]
async fn a_different_commit_is_a_cache_miss_and_spawns_a_fresh_reviewer() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path());

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(BARRIERED_REVIEWER),
    );
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    // A verdict exists, but for a stale commit — it must NOT satisfy a probe
    // for the new tip.
    plant_verdict(
        &mut client,
        &repo_name,
        "earlier-reviewer",
        "APPROVE",
        "sha-OLD",
        "rat/x/work",
    )
    .await;

    let id = run_workflow(&mut client, repo_dir.path(), "sha-NEW", "rat/x/work").await;
    let reviewer = await_reviewer(&mut client, &id).await;

    // This run's own reviewer records ITS verdict for the new tip; release
    // only after it is in the space, exactly as the fromAgent binding test
    // does, so the `read` cannot race the plant.
    plant_verdict(
        &mut client,
        &repo_name,
        &reviewer,
        "APPROVE",
        "sha-NEW",
        "rat/x/work",
    )
    .await;
    release_reviewers(home.path());

    let done = await_instance(&mut client, &id).await;

    assert_eq!(
        done["context"]["vars"]["cachedVerdict"],
        json!(null),
        "a miss must lift null, not silently match the stale commit's verdict: {done:?}"
    );
    assert_eq!(
        done["context"]["vars"]["verdict"],
        json!("APPROVE"),
        "the fresh reviewer's own verdict must still be read after the miss: {done:?}"
    );
    assert_eq!(done["status"], "completed", "got: {done:?}");
    assert_eq!(
        reviewer_invocation_count(home.path()),
        1,
        "a cache miss must spawn exactly one fresh reviewer"
    );
}

/// Safety property: a cached REWORK is honored exactly like a fresh one —
/// the instance routes to the rework arm WITHOUT spawning a reviewer to shop
/// for a better verdict.
#[tokio::test]
async fn cached_rework_routes_to_rework_without_spawning_a_reviewer() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path());

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(BARRIERED_REVIEWER),
    );
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    plant_verdict(
        &mut client,
        &repo_name,
        "earlier-reviewer",
        "REWORK",
        "sha-abc123",
        "rat/x/work",
    )
    .await;

    let id = run_workflow(&mut client, repo_dir.path(), "sha-abc123", "rat/x/work").await;
    let done = await_instance(&mut client, &id).await;

    assert_eq!(
        done["context"]["vars"]["cachedVerdict"],
        json!("REWORK"),
        "got: {done:?}"
    );
    assert_eq!(
        done["status"], "failed",
        "a cached REWORK must route to the rework arm, same as a fresh one, got: {done:?}"
    );
    assert!(
        done["error"]
            .as_str()
            .unwrap_or("")
            .contains("routed cached REWORK"),
        "the rework arm's reason should name that it came from the cache, got: {done:?}"
    );
    assert_eq!(
        reviewer_invocation_count(home.path()),
        0,
        "a cached REWORK must not spawn a reviewer to re-litigate the verdict"
    );
}

/// The rework's regression (TKT-01M036NWEG0H019BJ16G59RZVP rework, reported
/// against Templeton-7's REWORK review): two branches cut from the same point
/// share a tip commit before either gains a new commit of its own. A verdict
/// recorded for branch A's tip must NOT satisfy a cache probe for branch B at
/// that identical sha — branch A's diff-against-target and branch B's may be
/// completely different despite the shared HEAD, so reusing A's verdict on B
/// would route a merge decision on a diff nobody reviewed.
#[tokio::test]
async fn a_different_branch_at_the_same_sha_is_a_cache_miss_and_spawns_a_fresh_reviewer() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path());

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(BARRIERED_REVIEWER),
    );
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo_dir.path()).await;

    // Branch A's reviewer already recorded APPROVE for the shared tip.
    plant_verdict(
        &mut client,
        &repo_name,
        "earlier-reviewer",
        "APPROVE",
        "sha-shared",
        "rat/branch-a/work",
    )
    .await;

    // Branch B probes the SAME sha — must miss, since it names a different
    // branch, and fall through to spawning its own reviewer.
    let id = run_workflow(
        &mut client,
        repo_dir.path(),
        "sha-shared",
        "rat/branch-b/work",
    )
    .await;
    let reviewer = await_reviewer(&mut client, &id).await;

    plant_verdict(
        &mut client,
        &repo_name,
        &reviewer,
        "APPROVE",
        "sha-shared",
        "rat/branch-b/work",
    )
    .await;
    release_reviewers(home.path());

    let done = await_instance(&mut client, &id).await;

    assert_eq!(
        done["context"]["vars"]["cachedVerdict"],
        json!(null),
        "a different branch at the same sha must miss, not reuse the other branch's verdict: \
         {done:?}"
    );
    assert_eq!(
        done["context"]["vars"]["verdict"],
        json!("APPROVE"),
        "branch B's own fresh reviewer's verdict must still be read after the miss: {done:?}"
    );
    assert_eq!(done["status"], "completed", "got: {done:?}");
    assert_eq!(
        reviewer_invocation_count(home.path()),
        1,
        "a cross-branch miss at a shared sha must spawn exactly one fresh reviewer"
    );
}
