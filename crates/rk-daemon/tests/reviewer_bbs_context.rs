//! TKT-hifam-dosop-rugod: a native reviewer's synthetic spawn task (e.g.
//! `candidate-review-TKT-...`) never resolves to a ticket, so its BBS
//! briefing always surfaces zero entries even when the ticket it is
//! reviewing has directly relevant evidence. `LandingPolicy::
//! reviewed_ticket_bbs_context` (default off) redirects the briefing query
//! onto the daemon-owned `ReviewContext.task` instead, while leaving the
//! reviewer's own task/spawn/telemetry identity untouched. These fixtures
//! drive real `agent.spawn` RPCs (not `Supervisor::bbs_briefing` directly) so
//! the whole path — policy resolution, `PrimeContext` rendering into the
//! actual prompt, and BBS exposure telemetry — is exercised end to end.

mod fixture;
mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::Command;
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

fn git_out(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.com"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# scratch\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

/// Same shape as `install_default_repository_policy`, but with
/// `landing.reviewedTicketBbsContext: true` — written and committed BEFORE
/// `repo.add` so this fresh, previously-unregistered repo activates it
/// immediately at registration (see `handle_repo_add`), with no separate
/// `rk repo onboard activate` needed. Changing the flag on an
/// ALREADY-registered repo is a different, digest-fenced journey — see the
/// field's doc comment on `LandingPolicy`.
fn install_reviewed_ticket_bbs_context_policy(repo: &Path) {
    let rk_dir = repo.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(
        rk_dir.join("repo.cue"),
        r#"repo: {
    delivery: {target: "agent-base", mode: "merge", remote: "origin", remoteBranch: "{{branch}}", deleteSource: true}
    landing: {reviewedTicketBbsContext: true}
}
"#,
    )
    .unwrap();
    git(repo, &["add", ".rk/repo.cue"]);
    git(
        repo,
        &["commit", "-m", "test: activate reviewedTicketBbsContext"],
    );
}

/// Fake harness that captures the system prompt it was primed with into a
/// committed file, so the test can read back exactly what the agent
/// received — same pattern as `convention_priming.rs`'s `capture_prime`.
fn capture_prime() -> String {
    fixture::with_rk_done(
        r#"
read -r _prompt
printf '%s' "$RK_FAKE_SYSTEM_PROMPT" > primed.txt
git add primed.txt >/dev/null 2>&1
git -c user.email=rat@x -c user.name=Rat commit -q -m "capture prime"
echo '{"type":"system","subtype":"init","session_id":"fake-prime"}'
rk_done "captured prime"
echo '{"type":"result","subtype":"success","is_error":false,"result":"captured prime","session_id":"fake-prime","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#,
    )
}

async fn spawn_and_wait(
    client: &mut Client,
    repo: &Path,
    task: &str,
    role: &str,
    review: Option<Value>,
) -> (String, String) {
    let mut params = json!({
        "repo": repo.to_string_lossy(),
        "task": task,
        "role": role,
        "harness": "fake",
    });
    if let Some(review) = review {
        params["review"] = review;
    }
    let spawned = client.call("agent.spawn", params).await.unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    let branch = spawned["agent"]["branch"].as_str().unwrap().to_string();
    let mut completed = false;
    for _ in 0..250 {
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(completed, "agent {name} never completed");
    (name, branch)
}

async fn exposure_for(client: &mut Client, scope: &str, agent_name: &str) -> Value {
    let rows = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": scope, "identity": "bbs-exposure-spawn"}),
        )
        .await
        .unwrap();
    rows["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["payload"]["agent"] == agent_name)
        .unwrap_or_else(|| panic!("no bbs-exposure-spawn recorded for {agent_name}"))
        .clone()
}

/// Baseline: reproduces the production defect. With the setting at its
/// default (off), a reviewer's briefing is queried against its own
/// synthetic task — which never resolves to a ticket — so it never sees the
/// reviewed ticket's evidence even though that evidence exists.
#[tokio::test]
async fn policy_off_by_default_reviewer_briefing_misses_reviewed_ticket_evidence() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    scratch_repo(repo.path());
    support::install_default_repository_policy(repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", capture_prime());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo.path()).await;
    let scope = repo
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "flaky retry needs a caveat", "scope": scope}),
        )
        .await
        .unwrap();
    let original_ticket = ticket["ticket"]["identity"].as_str().unwrap().to_string();
    client
        .call(
            "space.out",
            json!({
                "category": "artifact", "scope": scope, "identity": "finding",
                "payload": {"task": original_ticket, "summary": "ACCEPTANCE CAVEAT: only safe under single-writer load"},
            }),
        )
        .await
        .unwrap();

    let synthetic_task = format!("candidate-review-{original_ticket}");
    let (name, branch) = spawn_and_wait(
        &mut client,
        repo.path(),
        &synthetic_task,
        "reviewer",
        Some(json!({
            "branch": "feature", "headSha": "d".repeat(40), "target": "main",
            "task": original_ticket, "attempt": "attempt-policy-off",
        })),
    )
    .await;

    let primed = git_out(repo.path(), &["show", &format!("{branch}:primed.txt")]);
    assert!(
        !primed.contains("ACCEPTANCE CAVEAT"),
        "defect reproduction: the default-off setting must still miss the \
         reviewed ticket's evidence:\n{primed}"
    );

    let exposure = exposure_for(&mut client, &scope, &name).await;
    assert_eq!(exposure["payload"]["task"], synthetic_task);
    assert_eq!(exposure["payload"]["consumer_task"], synthetic_task);
    assert_eq!(exposure["payload"]["reviewed_ticket_bbs_context"], false);

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// With the setting on, a reviewer's briefing is redirected onto the ticket
/// it is actually reviewing, so relevant evidence becomes visible — while
/// exposure telemetry keeps the query task (the reviewed ticket) and the
/// consumer's own true task distinct, an ordinary (non-reviewer) worker is
/// unaffected, and a foreign-repo review binding neither leaks nor errors
/// the spawn.
#[tokio::test]
async fn policy_on_exposes_reviewed_ticket_evidence_without_leaking_foreign_scope() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    scratch_repo(repo.path());
    install_reviewed_ticket_bbs_context_policy(repo.path());
    let other_repo = tempfile::tempdir().unwrap();
    scratch_repo(other_repo.path());
    support::install_default_repository_policy(other_repo.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", capture_prime());

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo.path()).await;
    support::register_repo(&mut client, other_repo.path()).await;
    let scope = repo
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let other_scope = other_repo
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();

    let ticket = client
        .call(
            "ticket.new",
            json!({"title": "flaky retry needs a caveat", "scope": scope}),
        )
        .await
        .unwrap();
    let original_ticket = ticket["ticket"]["identity"].as_str().unwrap().to_string();
    client
        .call(
            "space.out",
            json!({
                "category": "artifact", "scope": scope, "identity": "finding",
                "payload": {"task": original_ticket, "summary": "ACCEPTANCE CAVEAT: only safe under single-writer load"},
            }),
        )
        .await
        .unwrap();

    let foreign_ticket = client
        .call(
            "ticket.new",
            json!({"title": "unrelated other-repo work", "scope": other_scope}),
        )
        .await
        .unwrap();
    let foreign_ticket = foreign_ticket["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string();
    client
        .call(
            "space.out",
            json!({
                "category": "artifact", "scope": other_scope, "identity": "finding",
                "payload": {"task": foreign_ticket, "summary": "OTHER REPO SECRET evidence"},
            }),
        )
        .await
        .unwrap();

    // (1) A reviewer whose review binding names the REAL, same-repo ticket
    // sees its evidence, and telemetry keeps query/consumer task distinct.
    let synthetic_task = format!("candidate-review-{original_ticket}");
    let (reviewer_name, reviewer_branch) = spawn_and_wait(
        &mut client,
        repo.path(),
        &synthetic_task,
        "reviewer",
        Some(json!({
            "branch": "feature", "headSha": "d".repeat(40), "target": "main",
            "task": original_ticket, "attempt": "attempt-policy-on",
        })),
    )
    .await;
    let reviewer_primed =
        git_out(repo.path(), &["show", &format!("{reviewer_branch}:primed.txt")]);
    assert!(
        reviewer_primed.contains("ACCEPTANCE CAVEAT"),
        "reviewed-ticket evidence must reach the reviewer's briefing:\n{reviewer_primed}"
    );
    let reviewer_exposure = exposure_for(&mut client, &scope, &reviewer_name).await;
    assert_eq!(
        reviewer_exposure["payload"]["task"], original_ticket,
        "the BBS query itself targeted the reviewed ticket"
    );
    assert_eq!(
        reviewer_exposure["payload"]["consumer_task"], synthetic_task,
        "telemetry must keep the reviewer's OWN task distinct from the reviewed ticket it queried"
    );
    assert_eq!(
        reviewer_exposure["payload"]["reviewed_ticket_bbs_context"], true
    );
    assert_eq!(
        reviewer_exposure["payload"]["agent"], reviewer_name,
        "consumer identity stays the true reviewer, never the reviewed ticket"
    );

    // (2) A reviewer whose review binding names a ticket in a DIFFERENT
    // repo's scope must not leak that repo's evidence, and must not error
    // the spawn — bbs::brief's existing cross-repo guard degrades this to no
    // briefing at all, exactly like any other unavailable briefing.
    let mismatched_synthetic_task = format!("candidate-review-{foreign_ticket}");
    let (_mismatched_name, mismatched_branch) = spawn_and_wait(
        &mut client,
        repo.path(),
        &mismatched_synthetic_task,
        "reviewer",
        Some(json!({
            "branch": "feature", "headSha": "e".repeat(40), "target": "main",
            "task": foreign_ticket, "attempt": "attempt-policy-on-foreign",
        })),
    )
    .await;
    let mismatched_primed = git_out(
        repo.path(),
        &["show", &format!("{mismatched_branch}:primed.txt")],
    );
    assert!(
        !mismatched_primed.contains("OTHER REPO SECRET"),
        "a review binding naming a foreign repo's ticket must never leak its evidence:\n{mismatched_primed}"
    );

    // (3) An ordinary (non-reviewer) worker with no ReviewContext is
    // structurally unaffected by the setting: it is still briefed on its
    // own task.
    let (_worker_name, worker_branch) = spawn_and_wait(
        &mut client,
        repo.path(),
        &original_ticket,
        "rat",
        None,
    )
    .await;
    let worker_primed = git_out(
        repo.path(),
        &["show", &format!("{worker_branch}:primed.txt")],
    );
    assert!(
        worker_primed.contains("ACCEPTANCE CAVEAT"),
        "an ordinary worker on its own real task is unaffected by the setting:\n{worker_primed}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
