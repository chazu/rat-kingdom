//! TKT-172 regression: the `read` behind an approval gate must lift the decision
//! made for THIS instance, never a concurrent instance's.
//!
//! The sibling of TKT-161, one key over. There the unbound predicate was keyed by
//! agent name; here it is keyed by workflow instance, and the shape is the same:
//! `(event, <repo>, workflow_approval)` is shared by every gated instance running
//! on one repo. The approval GATE already matches the decision to its own
//! instance — it waits on `"instance":"<id>"` — but the READ that lifts
//! `approved` out from behind it did not, so "newest match wins" handed both
//! instances whichever decision landed last. An operator who approves A and
//! rejects B gets one of two silent wrong answers: B merges on A's approval, or
//! A is held on B's rejection. The fail-closed timeout makes it worse still,
//! since a timing-out instance synthesises an `{approved: false}` a live peer can
//! then route on.
//!
//! Two instances park at an approval gate on one repo, one is approved and one
//! rejected, and each must route on its own decision — proved in git, since the
//! two arms differ in whether the branch reaches main. The second test runs the
//! mutation (`fromInstance` off) and asserts the approved instance is dragged
//! onto its peer's rejection, so the defect is reproduced here rather than only
//! described.

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

fn git_out(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
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

/// The rat commits a file named after itself, so which instance's work reached
/// main is readable straight out of `git ls-tree`.
const WORKING_FAKE: &str = r#"
read -r _prompt
echo "work by $RK_AGENT" > "work-$RK_AGENT.txt"
git add . >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"approval-fake"}'
rk_done "work done"   # a rat that never declares done fails (TKT-175)
echo '{"type":"result","subtype":"success","is_error":false,"result":"did the work","session_id":"approval-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// `gated-merge.cue`'s shape, reduced to the seam under test and with one step
/// added: a barrier between the gate and the read.
///
/// The barrier IS the fixture. Both instances have to clear their gate and then
/// STOP, so that when either `read` finally runs there are two competing
/// `workflow_approval` events in the scope — the contention the bug needs. A
/// gate wakes the instant its own decision lands, so without the barrier the
/// first instance's read would ordinarily run before the second decision is even
/// written, and the test would quietly stop testing anything. A `sleep` would
/// paper over that on an idle machine and lose the race on a loaded one; a file
/// the test itself creates does not. `__BARRIER__` is substituted with a path
/// under this test's own `$RK_HOME` tempdir, so the two tests in this binary
/// cannot release each other's instances.
///
/// The 30s cap keeps a broken run failing rather than hanging, and the
/// `evaluate` behind it turns a barrier that never opened into a loud failure
/// instead of a silently degraded test.
///
/// `{{FROM_INSTANCE}}` is substituted per run so the mutation — the whole bug —
/// is expressible without editing this file.
const WORKFLOW: &str = r#"
workflow: {
    name: "approval-binding-test"
    agents: {
        default: {harness: "fake", model: "sonnet"}
    }
    steps: [
        {type: "spawn", role: "rat", task: {title: "do-the-thing", description: "Do the risky thing"}},
        {type: "wait", timeout: "60s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "gate", gateType: "approval", timeout: "60s"},
        {
            type: "run"
            command: "i=0; while [ ! -f '__BARRIER__' ] && [ $i -lt 600 ]; do sleep 0.05; i=$((i+1)); done; [ -f '__BARRIER__' ]"
            timeout: "60s"
        },
        {type: "evaluate", expect: {exit: 0}},
        {
            type: "read"
            category: "event"
            identity: "workflow_approval"
            fromInstance: {{FROM_INSTANCE}}
            field: "approved"
            into: "approved"
            timeout: "30s"
        },
        {
            type: "when"
            var: "approved"
            cases: {
                "true": [{type: "dismiss"}]
                "false": [{type: "dismiss", noMerge: true}]
            }
            default: [
                {type: "dismiss", noMerge: true},
                {type: "stop", reason: "unrecognized approval decision"},
            ]
        },
    ]
}
"#;

/// Index of the approval `gate` in the fixture above.
const GATE_STEP: u64 = 3;

fn init_repo(dir: &Path, barrier: &Path, from_instance: bool) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
    let wf_dir = dir.join(".rk").join("workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    let src = WORKFLOW
        .replace("__BARRIER__", &barrier.to_string_lossy())
        .replace("{{FROM_INSTANCE}}", &from_instance.to_string());
    std::fs::write(wf_dir.join("approval-binding-test.cue"), src).unwrap();
}

async fn instance(client: &mut Client, id: &str) -> serde_json::Value {
    client
        .call("workflow.status", json!({"name": id}))
        .await
        .unwrap()["instance"]
        .clone()
}

/// Start a run and block until it parks at the approval gate, so no decision is
/// ever written to a gate that is not yet listening.
async fn run_to_gate(client: &mut Client, repo: &Path) -> String {
    let started = client
        .call(
            "workflow.run",
            json!({
                "name": "approval-binding-test",
                "repo": repo.to_string_lossy(),
                "params": {},
            }),
        )
        .await
        .unwrap();
    let id = started["instance"]["id"].as_str().unwrap().to_string();

    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let inst = instance(client, &id).await;
        assert_ne!(
            inst["status"], "failed",
            "run {id} failed before the gate: {}",
            inst["error"]
        );
        if inst["status"] == "running" && inst["current_step"] == GATE_STEP {
            return id;
        }
    }
    panic!("instance {id} never parked at the approval gate");
}

/// The rat this instance spawned — its branch is the one the decision routes.
async fn agent_of(client: &mut Client, id: &str) -> String {
    instance(client, id).await["context"]["active_agent"]
        .as_str()
        .expect("instance parked at the gate must still hold its rat")
        .to_string()
}

/// Record one human decision, exactly as `rk approve`/`rk reject` would.
///
/// Separated by a couple of milliseconds because "newest wins" is only defined
/// ACROSS milliseconds: a `RecordId` is a ULID whose sub-millisecond ordering
/// comes from a random suffix, so two decisions written inside one millisecond
/// sort arbitrarily and the fixture would stop expressing a write ORDER at all.
/// Real operators decide seconds apart; only this test writes fast enough to
/// care.
async fn decide(client: &mut Client, id: &str, approved: bool) {
    tokio::time::sleep(Duration::from_millis(2)).await;
    client
        .call(
            "workflow.approve",
            json!({"instance": id, "approved": approved, "by": "operator"}),
        )
        .await
        .unwrap();
}

/// Open the barrier: both instances proceed to their `read`, with the full set
/// of decisions already in the space.
fn release(barrier: &Path) {
    std::fs::write(barrier, "").unwrap();
}

async fn await_settled(client: &mut Client, id: &str) -> serde_json::Value {
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let inst = instance(client, id).await;
        match inst["status"].as_str().unwrap_or("") {
            "completed" | "failed" => return inst,
            _ => {}
        }
    }
    panic!("workflow instance {id} never settled");
}

/// Two instances parked at an approval gate on one repo: approve one, reject the
/// other, and each must route on its OWN decision — even though the peer's
/// rejection is the newest `workflow_approval` in the scope and would win an
/// unbound read.
#[tokio::test]
async fn concurrent_instances_each_route_on_their_own_approval_decision() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let barrier = home.path().join("go");
    init_repo(repo_dir.path(), &barrier, true);

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    // Both gated runs in flight at once — two operators' worth of pending work
    // on one repo, which is the ordinary state of a busy castle, not an edge
    // case.
    let approved = run_to_gate(&mut client, repo_dir.path()).await;
    let rejected = run_to_gate(&mut client, repo_dir.path()).await;
    let approved_rat = agent_of(&mut client, &approved).await;
    let rejected_rat = agent_of(&mut client, &rejected).await;
    assert_ne!(
        approved_rat, rejected_rat,
        "the two instances must have spawned distinct rats"
    );

    // Approve the first, reject the second. The REJECTION goes last so it is the
    // newest decision in the scope — the tuple an unbound "newest wins" read
    // hands to BOTH instances. Both are in the space before the barrier opens,
    // so neither read can be decided by which instance happened to get there
    // first.
    decide(&mut client, &approved, true).await;
    decide(&mut client, &rejected, false).await;
    release(&barrier);

    let approved_done = await_settled(&mut client, &approved).await;
    let rejected_done = await_settled(&mut client, &rejected).await;

    // The heart of it: the approved instance lifted its OWN approval. Under the
    // bug it lifted the peer's rejection and held a branch its operator had
    // already cleared to land.
    assert_eq!(
        approved_done["context"]["vars"]["approved"],
        json!(true),
        "instance {approved} routed on a decision that was not made for it: {approved_done:?}"
    );
    // ...and the rejected instance lifted its own rejection rather than being
    // dragged onto the peer's approval. Both directions, so neither assertion
    // can be satisfied by a read that simply prefers one arm.
    assert_eq!(
        rejected_done["context"]["vars"]["approved"],
        json!(false),
        "instance {rejected} routed on a decision that was not made for it: {rejected_done:?}"
    );
    // A veto is a normal outcome, so both runs end cleanly either way.
    assert_eq!(approved_done["status"], "completed", "{approved_done:?}");
    assert_eq!(rejected_done["status"], "completed", "{rejected_done:?}");

    // The consequence, in git: exactly the approved rat's work is on main, and
    // exactly the rejected rat's branch survives unmerged for a human. This is
    // the assertion that would have caught the defect in production — the ctx
    // var above is the mechanism, this is the damage.
    let main = git_out(repo_dir.path(), &["ls-tree", "--name-only", "main"]);
    assert!(
        main.contains(&format!("work-{approved_rat}.txt")),
        "the approved rat's work should have merged: {main}"
    );
    assert!(
        !main.contains(&format!("work-{rejected_rat}.txt")),
        "the rejected rat's work must NOT have merged: {main}"
    );
    // Branch names slugify the agent, so match on the slug rather than the
    // display name.
    let branches = git_out(repo_dir.path(), &["branch", "--list", "rat/*"]);
    assert!(
        !branches.contains(&approved_rat.to_lowercase()),
        "the merged branch should be deleted: {branches}"
    );
    assert!(
        branches.contains(&rejected_rat.to_lowercase()),
        "the rejected branch should be preserved unmerged: {branches}"
    );
    // RK_FAKE_HARNESS_CMD is left set: the sibling test in this binary runs in
    // parallel and shares the same value, so unsetting it here could race its
    // spawns. It is scoped to this test process and harmless to leave.
}

/// The mutation, run as a test rather than described in a comment: with
/// `fromInstance` off, the SAME fixture drags the APPROVED instance onto its
/// peer's rejection. This is the pre-fix behaviour, and it is what a hand-written
/// `read` still does — the unbound form remains available, so the fix has to be
/// opted into where identity matters, and that is worth pinning rather than
/// assuming.
#[tokio::test]
async fn an_unbound_approval_read_still_takes_the_newest_strangers_decision() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let barrier = home.path().join("go");
    init_repo(repo_dir.path(), &barrier, false);

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let approved = run_to_gate(&mut client, repo_dir.path()).await;
    let rejected = run_to_gate(&mut client, repo_dir.path()).await;
    let approved_rat = agent_of(&mut client, &approved).await;

    decide(&mut client, &approved, true).await;
    decide(&mut client, &rejected, false).await;
    release(&barrier);

    let approved_done = await_settled(&mut client, &approved).await;

    // The defect, reproduced: this instance's operator approved it, and it
    // routed the reject arm — because the newest `workflow_approval` in the repo
    // was meant for somebody else. Swap the decision order and it routes the
    // other way, which is the direction that actually merges: a branch landing
    // on main on a stranger's approval.
    assert_eq!(
        approved_done["context"]["vars"]["approved"],
        json!(false),
        "an unbound read should still take the newest decision, got: {approved_done:?}"
    );
    let main = git_out(repo_dir.path(), &["ls-tree", "--name-only", "main"]);
    assert!(
        !main.contains(&format!("work-{approved_rat}.txt")),
        "and hold the approved work back accordingly: {main}"
    );
    // See the sibling test: RK_FAKE_HARNESS_CMD is intentionally left set.
}
