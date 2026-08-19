//! Review tiering (TKT-01M036N1RT74H6NPRH5FMM8A6T): the steward skips its
//! reviewer spawn for a diff classified "doc-only"/"trivial", replacing it
//! with a near-zero gate-holder — same deterministic gates, unconditional
//! land, no LLM verdict. `TIERED_STEWARD` mirrors the shape of
//! examples/workflows/steward.cue's tiering (a `list.Concat`/`if` selection
//! over `_input.diffClass` at CUE load time, a shared `_gate`, distinct
//! `reviewer`/`gateholder` agent profiles), trimmed to a fast fake-harness
//! run rather than the real named checks.

mod fixture;

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "tiered work" > steward.txt
git add steward.txt >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"tier-fake"}'
rk_done "work done"
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"tier-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

// Mirrors examples/workflows/steward.cue's tiering shape at the size a fast
// fake-harness test needs: the deterministic `_gate` and the land tail are
// shared between both tiers via `list.Concat`; only the spawned agent profile
// (and, in the real file, the verdict routing) differs.
const TIERED_STEWARD: &str = r#"
import "list"

workflow: {
    name: "steward"
    params: {
        diffClass: {type: "string", required: false, default: "large"}
    }
    agents: {
        default:    {harness: "fake", model: "default-model"}
        reviewer:   {harness: "fake", model: "reviewer-model"}
        gateholder: {harness: "fake", model: "gateholder-model"}
    }
    _reduced: _input.diffClass == "doc-only" || _input.diffClass == "trivial"
    _gate: [
        {type: "run", command: "true", timeout: "30s"},
        {type: "evaluate", expect: {exit: 0}},
    ]
    _tail: [
        {type: "dismiss", noMerge: true},
        {type: "land", branch: "{{ctx.activeBranch}}", target: "main"},
        {type: "evaluate", expect: {delivered: true}},
    ]
    _reviewArm: list.Concat([
        [
            {type: "spawn", role: "reviewer", agent: "reviewer", task: {title: "review"}},
            {type: "wait", timeout: "60s"},
            {type: "evaluate", expect: {is_error: false}},
        ],
        _gate,
        _tail,
    ])
    _reducedArm: list.Concat([
        [
            {type: "spawn", role: "reviewer", agent: "gateholder", task: {title: "gatehold"}},
            {type: "wait", timeout: "60s"},
            {type: "evaluate", expect: {is_error: false}},
        ],
        _gate,
        _tail,
    ])
    steps: list.Concat([
        if _reduced {_reducedArm},
        if !_reduced {_reviewArm},
    ])
}
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
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

async fn run_and_wait(
    client: &mut Client,
    repo: &Path,
    params: serde_json::Value,
) -> serde_json::Value {
    let started = client
        .call(
            "workflow.run",
            json!({"name": "steward", "repo": repo.to_string_lossy(), "params": params}),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap();
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("workflow.status", json!({"name": id}))
            .await
            .unwrap();
        if status["instance"]["status"] != "running" {
            return status;
        }
    }
    panic!("workflow did not settle");
}

/// (a) A completion classified "doc-only" takes the reduced tier: the spawned
/// agent runs on the gate-holder's (cheap) model, never the reviewer's — no
/// reviewer-profile spawn happens — the shared deterministic gate still runs
/// (a failing gate would fail the instance, not merely skip review), and the
/// branch still lands on main.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn doc_only_diff_class_skips_reviewer_but_still_gates_and_lands() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));

    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    std::fs::create_dir_all(layout.workflows_dir()).unwrap();
    std::fs::write(layout.workflows_dir().join("steward.cue"), TIERED_STEWARD).unwrap();
    let daemon = Daemon::new_in_memory(layout.clone(), "tiered".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let result = run_and_wait(&mut client, repo.path(), json!({"diffClass": "doc-only"})).await;
    assert_eq!(result["instance"]["status"], "completed", "{result}");

    let agents = client.call("agent.list", json!({})).await.unwrap();
    let models: Vec<Option<String>> = agents["agents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["model"].as_str().map(String::from))
        .collect();
    assert!(
        models
            .iter()
            .any(|m| m.as_deref() == Some("gateholder-model")),
        "the reduced tier must spawn the gate-holder profile: {models:?}"
    );
    assert!(
        !models
            .iter()
            .any(|m| m.as_deref() == Some("reviewer-model")),
        "the reduced tier must never spawn the reviewer profile: {models:?}"
    );

    // Land still gated: the branch actually merged onto main, past the shared
    // deterministic gate (a gate failure would have failed the instance above
    // instead of reaching `land`).
    assert!(
        git(repo.path(), &["show", "main:steward.txt"]).contains("tiered work"),
        "reduced-tier work must still land on main after the gate passes"
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// (b) A completion payload with no diffClass at all (a legacy completion, or
/// any tier other than doc-only/trivial) takes the unchanged review tier —
/// the param default ("large") is fail-closed toward review.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_diff_class_defaults_to_the_review_tier() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));

    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    init_repo(repo.path());
    let layout = Layout::at(home.path());
    std::fs::create_dir_all(layout.workflows_dir()).unwrap();
    std::fs::write(layout.workflows_dir().join("steward.cue"), TIERED_STEWARD).unwrap();
    let daemon = Daemon::new_in_memory(layout.clone(), "tiered-default".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // No diffClass param at all — exactly what a completion predating this
    // feature (or the reactor's null-templated-param omission) produces.
    let result = run_and_wait(&mut client, repo.path(), json!({})).await;
    assert_eq!(result["instance"]["status"], "completed", "{result}");

    let agents = client.call("agent.list", json!({})).await.unwrap();
    let models: Vec<Option<String>> = agents["agents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["model"].as_str().map(String::from))
        .collect();
    assert!(
        models
            .iter()
            .any(|m| m.as_deref() == Some("reviewer-model")),
        "a missing diffClass must default to the review tier: {models:?}"
    );
    assert!(
        !models
            .iter()
            .any(|m| m.as_deref() == Some("gateholder-model")),
        "a missing diffClass must never take the reduced tier: {models:?}"
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
