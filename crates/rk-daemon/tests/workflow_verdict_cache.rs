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

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

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

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
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
    client
        .call(
            "space.out",
            json!({
                "category": "artifact",
                "scope": scope,
                "identity": "review",
                "payload": {
                    "agent": agent,
                    "task": "review-the-branch",
                    "recommendation": recommendation,
                    "notes": format!("verdict for {agent}"),
                    "head_sha": head_sha,
                    "branch": branch,
                },
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

/// A rat that does nothing but call `rk done` immediately — the gate-holder's
/// entire prescribed job (its task description literally says "do nothing
/// else"). No barrier needed: the cache-hit path spawns at most one agent.
const INSTANT_DONE_FAKE: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"shipped-e2e-fake"}'
rk_done "gate hold done"
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"shipped-e2e-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// Minimal named checks the shipped `_gates` resolve at runtime, mirroring
/// this repo's own `.rk/checks.cue`: the real protected-paths/diff-scope
/// commands, and a trivial stand-in for `verify` — this test repo is a
/// throwaway git tree, not a Rust project, so there is nothing real to build.
const CHECKS: &str = r#"
checks: [
    {name: "steward-protected-paths", command: "target=$RK_CHECK_TARGET; ! git diff --name-only \"$target\"...HEAD | grep -qE \"$RK_CHECK_PROTECTED_PATHS\"", timeout: "30s"},
    {name: "steward-diff-scope", command: "target=$RK_CHECK_TARGET; files=$(git diff --name-only \"$target\"...HEAD | wc -l | tr -d ' '); lines=$(git diff --numstat \"$target\"...HEAD | awk '{a=$1;b=$2;if(a==\"-\")a=0;if(b==\"-\")b=0;s+=a+b} END{print s+0}'); { [ \"$RK_CHECK_MAX_DIFF_FILES\" -eq 0 ] || [ \"$files\" -le \"$RK_CHECK_MAX_DIFF_FILES\" ]; } && { [ \"$RK_CHECK_MAX_DIFF_LINES\" -eq 0 ] || [ \"$lines\" -le \"$RK_CHECK_MAX_DIFF_LINES\" ]; }", timeout: "30s"},
    {name: "verify", command: "true", timeout: "30s"},
]
"#;

/// End-to-end proof of the SHIPPED cache-hit path (rework item 2, decided by
/// the operator: a cache hit skips the reviewer but keeps the gate-holder).
/// Unlike every other test in this file, which exercises a REDUCED stand-in
/// workflow, this one loads the real `examples/workflows/steward.cue` — the
/// exact gap Templeton-7's REWORK named ("the new e2e hit test uses a reduced
/// workflow without that arm and cannot catch this"). Proves, against the
/// real file: no reviewer-profile (LLM judgment) spawn happens on a hit, the
/// gate-holder spawn IS present, the real named gates execute, and the cached
/// recommendation still lands the branch.
#[tokio::test]
async fn shipped_steward_cache_hit_spawns_gateholder_not_reviewer_and_lands() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;

    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    // The "already-completed rat's work": a real branch with one unprotected
    // commit, exactly what steward chains a reviewer/gate-holder onto.
    git(repo_dir.path(), &["checkout", "-b", "rat/e2e/tkt-1"]);
    std::fs::write(repo_dir.path().join("feature.txt"), "hello\n").unwrap();
    git(repo_dir.path(), &["add", "feature.txt"]);
    git(repo_dir.path(), &["commit", "-m", "feature work"]);
    let head_sha = git(repo_dir.path(), &["rev-parse", "HEAD"])
        .trim()
        .to_string();
    git(repo_dir.path(), &["checkout", "main"]);

    let rk_dir = repo_dir.path().join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(rk_dir.join("checks.cue"), CHECKS).unwrap();

    // The shipped definition, harness swapped to `fake` so the test never
    // shells out to a real LLM CLI — same technique as `gated_merge.rs`'s
    // real-`examples/workflows/*.cue` e2e tests. The distinct `model` strings
    // ("gpt-5.6-luna" for reviewer, "haiku" for gateholder) survive the swap
    // untouched, so `agent.list` below can still tell which profile spawned.
    let steward_src = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("examples")
            .join("workflows")
            .join("steward.cue"),
    )
    .unwrap()
    .replace("\"codex\"", "\"fake\"")
    .replace("\"claude\"", "\"fake\"");
    let layout = Layout::at(home.path());
    // Global, not repo-local: `land` trusts only a "steward" definition
    // loaded from the operator-managed global workflow directory
    // (automated_landing.rs's `only_the_managed_global_steward_may_land...`).
    std::fs::create_dir_all(layout.workflows_dir()).unwrap();
    std::fs::write(layout.workflows_dir().join("steward.cue"), steward_src).unwrap();

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        fixture::with_rk_done(INSTANT_DONE_FAKE),
    );
    let daemon = Daemon::new_in_memory(layout.clone(), "shipped-e2e".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // A cached APPROVE already on record for this exact branch+commit — the
    // hit the cache-probe rework is meant to serve.
    plant_verdict(
        &mut client,
        &repo_name,
        "earlier-reviewer",
        "APPROVE",
        &head_sha,
        "rat/e2e/tkt-1",
    )
    .await;

    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "steward",
                "repo": repo_dir.path().to_string_lossy(),
                "params": {
                    "taskId": "tkt-1",
                    "branch": "rat/e2e/tkt-1",
                    "repo": repo_name,
                    "target": "main",
                    "headSha": head_sha,
                    "diffClass": "large",
                    "reviewTimeout": "30s",
                    "gateTimeout": "30s",
                },
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();
    let done = await_instance(&mut client, &id).await;
    assert_eq!(
        done["status"], "completed",
        "the cached APPROVE must complete the run through the real gates: {done:?}"
    );

    // GATE-HOLDER SPAWN PRESENT, NO REVIEWER-PROFILE SPAWN: the two agent
    // profiles carry distinct models in the shipped definition, so which one
    // ran is a direct measurement, not an inference from routing.
    let agents = client.call("agent.list", json!({})).await.unwrap();
    let models: Vec<Option<String>> = agents["agents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["model"].as_str().map(String::from))
        .collect();
    assert!(
        models.iter().any(|m| m.as_deref() == Some("haiku")),
        "a cache hit must still spawn the gate-holder profile to host the gates: {models:?}"
    );
    assert!(
        !models.iter().any(|m| m.as_deref() == Some("gpt-5.6-luna")),
        "a cache hit must never spawn the expensive reviewer profile: {models:?}"
    );

    // GATES ACTUALLY EXECUTED: only reachable via the real named
    // steward-protected-paths/steward-diff-scope/verify checks passing —
    // `_gates` fail-closed otherwise, and the branch would still be held.
    assert!(
        git(repo_dir.path(), &["log", "main", "--oneline"]).contains("feature work"),
        "the cached recommendation must still land the branch onto main after real gates pass"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
