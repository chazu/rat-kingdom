//! TKT-161 regression: a `read` step must lift the verdict THIS instance's
//! reviewer wrote, never a concurrent instance's.
//!
//! The failure this pins down: `(category, scope, identity)` is not an identity.
//! `steward.cue` lifts its merge decision with
//! `{type: "read", category: "artifact", identity: "review", …}`, and the
//! reactor fires a steward per rat completion — so two stewards on ONE repo is
//! the designed steady state, not an edge case. Both reads match the same
//! pattern, "newest match wins" hands both of them whichever reviewer finished
//! last, and the `when` behind it routes an APPROVE — a land onto main — on a
//! stranger's review of code this instance never looked at. Same shape as
//! TKT-146 (a read satisfied by a record that is not the one being waited on),
//! so it takes the same cure: bind to the author plus its generation floor.
//!
//! The test runs two instances of one workflow against one repo, with each
//! reviewer's verdict differing (APPROVE vs REWORK) and the verdicts routing to
//! visibly different ends. The peer's verdict is planted LAST, so it is the
//! newest match and an unbound read prefers it. The second test runs that exact
//! mutation — the same fixture with `fromAgent` off — and asserts the instance
//! routes on the STRANGER's verdict, so the bug is reproduced here rather than
//! only described.
//!
//! Scope note: this pins the READ side, planting each `review` artifact with the
//! `"agent"` stamp the writer would carry. The WRITE side — `rk out` stamping
//! `RK_AGENT` into any object payload that does not already name an agent — is
//! pinned by the unit tests on `stamp_author` in `rk-cli/src/space_cmds.rs`.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

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
        if let Ok(c) = Client::connect(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

/// A reviewer that parks until the test releases it.
///
/// The barrier IS the fixture. Both instances have to be sitting at their
/// `wait` while the test learns both reviewer names and plants both verdicts,
/// so that when either `read` finally runs there are two competing `review`
/// artifacts in the scope — the concurrency the bug needs. A `sleep` would do
/// that too, right up until a loaded machine slips one instance's read in
/// before the other's plant and the test quietly stops testing anything. The
/// go-file lives under `$RK_HOME`, which is a fresh tempdir per test, so the
/// two tests in this binary cannot release each other's reviewers.
///
/// The 30s cap keeps a broken run failing rather than hanging; it is under the
/// workflow's own 60s `wait` timeout, so the wait still reports the failure.
const BARRIERED_REVIEWER: &str = r#"
read -r _prompt
i=0
while [ ! -f "$RK_HOME/go" ] && [ "$i" -lt 600 ]; do sleep 0.05; i=$((i+1)); done
echo "reviewed by $RK_AGENT" > "review-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "review: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"verdict-fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"reviewed","session_id":"verdict-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// The steward's shape, reduced to the seam under test: spawn a reviewer, wait,
/// lift its verdict, route. APPROVE completes the run (the stand-in for `land`,
/// which needs no real merge to make the routing observable); REWORK stops it
/// with a reason naming the arm, so the arm each instance took is legible from
/// `workflow.status` either way.
///
/// `{{FROM_AGENT}}` is substituted per run so the mutation — the whole bug —
/// is expressible without editing this file.
const WORKFLOW: &str = r#"
workflow: {
    name: "verdict-binding-test"
    agents: {
        default: {harness: "fake", model: "sonnet"}
    }
    steps: [
        {type: "spawn", role: "reviewer", task: {title: "review-the-branch", description: "Review it"}},
        {type: "wait", timeout: "60s"},
        {type: "evaluate", expect: {is_error: false}},
        {
            type: "read"
            category: "artifact"
            identity: "review"
            fromAgent: {{FROM_AGENT}}
            field: "recommendation"
            into: "verdict"
            timeout: "30s"
        },
        {
            type: "when"
            var: "verdict"
            cases: {
                "APPROVE": [{type: "dismiss", noMerge: true}]
                "REWORK": [
                    {type: "dismiss", noMerge: true},
                    {type: "stop", reason: "routed REWORK"},
                ]
            }
            default: [
                {type: "dismiss", noMerge: true},
                {type: "stop", reason: "routed nothing"},
            ]
        },
    ]
}
"#;

fn init_repo(dir: &Path, from_agent: bool) -> String {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    let wf_dir = dir.join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(
        wf_dir.join("verdict-binding-test.cue"),
        WORKFLOW.replace("{{FROM_AGENT}}", &from_agent.to_string()),
    )
    .unwrap();
    dir.file_name().unwrap().to_string_lossy().to_string()
}

async fn run_workflow(client: &mut Client, repo_dir: &Path) -> String {
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "verdict-binding-test",
                "repo": repo_dir.to_string_lossy(),
                "params": {},
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

/// Poll until the instance settles; returns the whole terminal record, so the
/// caller can read both its status and the verdict it lifted.
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

/// Open the barrier: every parked reviewer proceeds to report, and both
/// instances march on to their `read` with the full set of verdicts already in
/// the space.
fn release_reviewers(home: &Path) {
    std::fs::write(home.join("go"), "").unwrap();
}

/// Record one reviewer's verdict, exactly as `rk out artifact <repo> review`
/// would from that reviewer's session.
///
/// Separated by a couple of milliseconds because "newest wins" is only defined
/// ACROSS milliseconds: a `RecordId` is a ULID whose sub-millisecond ordering
/// comes from a random suffix, so two verdicts written inside one millisecond
/// sort arbitrarily and the fixture would stop expressing a plant ORDER at all.
/// Real reviewers finish seconds apart; only this test writes fast enough to
/// care.
async fn plant_verdict(client: &mut Client, scope: &str, agent: &str, recommendation: &str) {
    tokio::time::sleep(Duration::from_millis(2)).await;
    client
        .call(
            "space.out",
            json!({
                "category": "artifact",
                "scope": scope,
                "identity": "review",
                // The stamp `rk out` now adds for any spawn-session write.
                "payload": {
                    "agent": agent,
                    "task": "review-the-branch",
                    "recommendation": recommendation,
                    "notes": format!("verdict for {agent}"),
                },
            }),
        )
        .await
        .unwrap();
}

/// Two instances in flight on one repo, each reviewer reaching a different
/// verdict. Each instance must route on its OWN reviewer — even though the
/// peer's verdict is the newest tuple in the scope and would win an unbound
/// read.
#[tokio::test]
async fn concurrent_instances_each_route_on_their_own_reviewers_verdict() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path(), true);

    std::env::set_var("RK_FAKE_HARNESS_CMD", BARRIERED_REVIEWER);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // Both stewards in flight at once, exactly as the reactor produces them.
    let first = run_workflow(&mut client, repo_dir.path()).await;
    let second = run_workflow(&mut client, repo_dir.path()).await;
    let first_reviewer = await_reviewer(&mut client, &first).await;
    let second_reviewer = await_reviewer(&mut client, &second).await;
    assert_ne!(
        first_reviewer, second_reviewer,
        "the two instances must have spawned distinct reviewers"
    );

    // First instance's reviewer approves; the peer's asks for rework. Plant the
    // PEER's last so it is the newest `review` in the scope — the tuple an
    // unbound "newest wins" read hands to BOTH instances. Both are in the space
    // before either reviewer is released, so neither read can be decided by
    // whichever instance happened to reach it first.
    plant_verdict(&mut client, &repo_name, &first_reviewer, "APPROVE").await;
    plant_verdict(&mut client, &repo_name, &second_reviewer, "REWORK").await;
    release_reviewers(home.path());

    let first_done = await_instance(&mut client, &first).await;
    let second_done = await_instance(&mut client, &second).await;

    // The heart of it: the first instance lifted its own reviewer's APPROVE.
    // Under the bug it lifted the peer's REWORK and failed on the rework arm —
    // and in the real steward it is the mirror case that bites, an instance
    // landing a branch on main because a stranger's reviewer said APPROVE.
    assert_eq!(
        first_done["context"]["vars"]["verdict"],
        json!("APPROVE"),
        "instance {first} routed on a verdict its reviewer {first_reviewer} did not write: \
         {first_done:?}"
    );
    assert_eq!(
        first_done["status"], "completed",
        "the APPROVE arm should complete the run, got: {first_done:?}"
    );

    // ...and the peer lifted its own REWORK, rather than being dragged onto the
    // first instance's APPROVE. Both directions, so neither assertion can be
    // satisfied by a read that simply prefers one arm.
    assert_eq!(
        second_done["context"]["vars"]["verdict"],
        json!("REWORK"),
        "instance {second} routed on a verdict its reviewer {second_reviewer} did not write: \
         {second_done:?}"
    );
    assert_eq!(
        second_done["status"], "failed",
        "the REWORK arm should stop the run, got: {second_done:?}"
    );
    assert!(
        second_done["error"]
            .as_str()
            .unwrap_or("")
            .contains("routed REWORK"),
        "the REWORK arm's reason should name the arm taken, got: {second_done:?}"
    );
}

/// The mutation, run as a test rather than described in a comment: with
/// `fromAgent` off, the SAME fixture routes the first instance onto its peer's
/// verdict. This is the pre-fix behaviour, and it is what the default `read`
/// still does — an unbound read remains available, so the fix has to be opted
/// into where identity matters, and that is worth pinning rather than assuming.
#[tokio::test]
async fn an_unbound_read_still_takes_the_newest_strangers_verdict() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_name = init_repo(repo_dir.path(), false);

    std::env::set_var("RK_FAKE_HARNESS_CMD", BARRIERED_REVIEWER);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let first = run_workflow(&mut client, repo_dir.path()).await;
    let second = run_workflow(&mut client, repo_dir.path()).await;
    let first_reviewer = await_reviewer(&mut client, &first).await;
    let second_reviewer = await_reviewer(&mut client, &second).await;

    plant_verdict(&mut client, &repo_name, &first_reviewer, "APPROVE").await;
    plant_verdict(&mut client, &repo_name, &second_reviewer, "REWORK").await;
    release_reviewers(home.path());

    let first_done = await_instance(&mut client, &first).await;

    // The defect, reproduced: this instance's reviewer said APPROVE and the
    // instance routed REWORK, because the newest `review` in the repo belonged
    // to somebody else. Swap the plant order and it would route the other way —
    // which in `steward.cue` is a merge to main on an unread branch.
    assert_eq!(
        first_done["context"]["vars"]["verdict"],
        json!("REWORK"),
        "an unbound read should still take the newest match, got: {first_done:?}"
    );
    assert_eq!(
        first_done["status"], "failed",
        "and route accordingly, got: {first_done:?}"
    );
}
