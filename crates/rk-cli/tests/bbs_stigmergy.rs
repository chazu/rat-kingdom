//! Real-CLI, real-daemon journey for S1 (findings, reuse receipts, operator
//! assessments): an early finding, another worker's receipt, and an operator
//! verdict, plus the authority/lifecycle regressions the design demands —
//! hostile forgery, cross-repo/task rejection, non-operator `bbs.assess`,
//! retry idempotency, and daemon-restart survival without duplication.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::Path;
use std::process::{Command, Output};
use std::time::Duration;

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git").arg("-C").arg(dir).args(args).output().unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn cli(layout: &Layout, agent: Option<&str>, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rk"));
    command.args(args);
    for key in rk_core::review::STRIPPED_RK_SPAWN_ENV {
        command.env_remove(key);
    }
    command.env("RK_HOME", layout.home());
    if let Some(agent) = agent {
        command.env("RK_AGENT", agent);
    }
    command.output().unwrap()
}

fn success(output: Output) -> Value {
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    serde_json::from_slice(&output.stdout).unwrap()
}

fn failure(output: Output) -> String {
    assert!(!output.status.success(), "expected failure, got: {output:?}");
    String::from_utf8_lossy(&output.stderr).to_string()
}

async fn start(layout: &Layout) -> (Client, tokio::task::JoinHandle<rk_core::Result<()>>) {
    let daemon = Daemon::new(layout.clone(), &rk_core::config::Config::default()).unwrap();
    let handle = tokio::spawn(daemon.run());
    for _ in 0..250 {
        if let Ok(client) = Client::connect_as_operator(layout).await {
            return (client, handle);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("daemon did not start");
}

const RESULT_LINE: &str = r#"echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"wf-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'"#;
const FAKE_HARNESS: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"wf-fake"}'
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn early_finding_to_receipt_to_assessment_survives_restart_and_rejects_hostile_writes() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    std::fs::create_dir_all(repo_dir.path().join(".rk")).unwrap();
    std::fs::write(repo_dir.path().join(".rk/repo.cue"), "repo: {}\n").unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    std::env::set_var("RK_FAKE_HARNESS_CMD", format!("{FAKE_HARNESS}{RESULT_LINE}\n"));

    let layout = Layout::at(home.path());
    let (mut client, handle) = start(&layout).await;
    client
        .call("repo.add", json!({"name": "myrepo", "path": repo_dir.path()}))
        .await
        .unwrap();

    // Two distinct repo-scoped tasks: alice's real assignment, and a second
    // task she must NOT be able to claim a finding/receipt against.
    let mine = client
        .call("ticket.new", json!({"title": "parser work", "scope": "myrepo"}))
        .await
        .unwrap()["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string();
    let not_mine = client
        .call("ticket.new", json!({"title": "unrelated work", "scope": "myrepo"}))
        .await
        .unwrap()["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string();

    // A consuming task for a SECOND genuinely supervised agent (bob), kept
    // distinct from alice's `mine` so the finding and its receipt carry two
    // actual, different native `SpawnId`s and task identities — the exact
    // source-to-consumer generation proof the offline report must replay.
    let consumer_task = client
        .call("ticket.new", json!({"title": "consumer work", "scope": "myrepo"}))
        .await
        .unwrap()["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string();

    async fn spawn_and_wait(layout: &Layout, client: &mut Client, ticket: &str) -> String {
        let spawned = success(cli(
            layout,
            None,
            &["--json", "spawn", "--ticket", ticket, "--harness", "fake"],
        ));
        let name = spawned["name"].as_str().unwrap().to_string();
        for _ in 0..250 {
            let status = client.call("agent.status", json!({"name": name})).await.unwrap();
            if status["state"] == "completed" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        name
    }

    // Two genuinely supervised agents (real `agent.spawn`, real registry
    // records) so the "publish/reuse must reference your own assigned task"
    // check has real `AgentRecord.task`/`spawn` to validate against — not
    // just the bare, unsupervised identity (`carol`, below) this test also
    // exercises for legacy/operator compatibility.
    let alice = spawn_and_wait(&layout, &mut client, &mine).await;
    let bob = spawn_and_wait(&layout, &mut client, &consumer_task).await;

    // Evidence: one ordinary artifact in-repo, one in a foreign scope.
    let ev = client
        .call(
            "space.out",
            json!({"category":"artifact","scope":"myrepo","identity":"ev1","payload":{"summary":"repro script"}}),
        )
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let foreign_ev = client
        .call(
            "space.out",
            json!({"category":"artifact","scope":"otherrepo","identity":"ev-foreign","payload":{}}),
        )
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let publish_args = |task: &str, revision: &str, evidence: &str| -> Vec<String> {
        vec![
            "--json".into(), "bbs".into(), "publish".into(),
            "Delimiters must be escaped".into(),
            "--repo".into(), "myrepo".into(),
            "--task".into(), task.into(),
            "--area".into(), "src/parser.rs".into(),
            "--revision".into(), revision.into(),
            "--evidence".into(), evidence.into(),
            "--limitations".into(), "only checked ASCII input".into(),
        ]
    };
    let sha = "a".repeat(40);
    let args_owned = publish_args(&mine, &sha, &ev);
    let args: Vec<&str> = args_owned.iter().map(String::as_str).collect();
    let finding = success(cli(&layout, Some(&alice), &args));
    let f = finding["id"].as_str().unwrap().to_string();
    assert_eq!(finding["kind"], "finding");

    // Retry is idempotent: same content, same tuple.
    let retry = success(cli(&layout, Some(&alice), &args));
    assert_eq!(retry["id"], f);
    assert_eq!(retry["written"], false);

    // Hostile: cross-repo evidence is rejected.
    let cross_repo_owned = publish_args(&mine, &sha, &foreign_ev);
    let cross_repo_args: Vec<&str> = cross_repo_owned.iter().map(String::as_str).collect();
    assert!(!cli(&layout, Some(&alice), &cross_repo_args).status.success());

    // Hostile: a supervised agent cannot publish against a task she was not
    // assigned — this is the real `AgentRecord.task` path, not a bare caller.
    let cross_task_owned = publish_args(&not_mine, &sha, &ev);
    let cross_task_args: Vec<&str> = cross_task_owned.iter().map(String::as_str).collect();
    let err = failure(cli(&layout, Some(&alice), &cross_task_args));
    assert!(err.contains("assigned task"), "{err}");

    // Bob — a SECOND genuinely supervised worker, assigned to his OWN task —
    // records a reuse receipt against alice's finding. This exercises the
    // documented `--text` flag (not a positional), matching the accepted
    // design's `rk bbs reuse SOURCE --outcome ... --text TEXT --evidence
    // ARTIFACT` syntax exactly.
    let reuse_args = [
        "--json", "bbs", "reuse", &f, "--task", &consumer_task, "--outcome", "used",
        "--text", "Applied it to my case", "--evidence", &ev,
    ];
    let receipt = success(cli(&layout, Some(&bob), &reuse_args));
    let r = receipt["id"].as_str().unwrap().to_string();
    assert_eq!(receipt["kind"], "reuse");
    let retry_receipt = success(cli(&layout, Some(&bob), &reuse_args));
    assert_eq!(retry_receipt["id"], r);
    assert_eq!(retry_receipt["written"], false);

    // The concrete source-to-consumer generation proof: the finding and its
    // receipt carry two ACTUAL, DISTINCT native `SpawnId`s and task
    // identities — not synthesized ones — which is exactly what the offline
    // report must later replay to attribute reuse across real generations.
    let shown_f = success(cli(&layout, Some("carol"), &["--json", "bbs", "show", &f]));
    let shown_r = success(cli(&layout, Some("carol"), &["--json", "bbs", "show", &r]));
    let finding_spawn = shown_f["tuple"]["payload"]["spawn"].as_str().unwrap().to_string();
    let receipt_spawn = shown_r["tuple"]["payload"]["spawn"].as_str().unwrap().to_string();
    assert_ne!(
        finding_spawn, receipt_spawn,
        "the publisher's and consumer's native SpawnIds must be genuinely distinct"
    );
    assert_eq!(shown_f["tuple"]["payload"]["task"], mine);
    assert_eq!(shown_r["tuple"]["payload"]["task"], consumer_task);
    assert_ne!(mine, consumer_task);

    // Everything past this point is a bare, unbound identity ("carol", never
    // spawned) — kept separate from alice/bob to preserve legacy/operator
    // compatibility (record=None) for hostile and hostile-adjacent checks
    // that don't need a real generation to be meaningful.

    // Hostile: reuse SOURCE cannot be another receipt.
    let bad_source = [
        "--json", "bbs", "reuse", &r, "--task", &consumer_task, "--outcome", "used",
        "--text", "Trying to reuse a receipt", "--evidence", &ev,
    ];
    assert!(!cli(&layout, Some("carol"), &bad_source).status.success());

    // Hostile: reuse's TASK must belong to the source's own repository. A
    // plain unresolvable string falls back to itself (like `ask` does), so
    // this needs a REAL ticket in a different repo scope to be a genuine
    // cross-repo claim rather than an unresolved alias.
    let other_repo_task = client
        .call("ticket.new", json!({"title": "other repo work", "scope": "otherrepo"}))
        .await
        .unwrap()["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string();
    let cross_repo_task_args = [
        "--json", "bbs", "reuse", &f, "--task", &other_repo_task,
        "--outcome", "used", "--text", "cross-repo task claim", "--evidence", &ev,
    ];
    let cross_repo_task_err = failure(cli(&layout, Some("carol"), &cross_repo_task_args));
    assert!(cross_repo_task_err.contains("different repository"), "{cross_repo_task_err}");

    // Two DIFFERENT consuming tasks genuinely reusing the same source the
    // same way must get two DISTINCT receipts, not collide into one retry —
    // the receipt's logical identity includes its consuming task. Run as the
    // unbound identity so an operator-style caller (no assigned task to
    // enforce) is exactly what is exercised here.
    let second_task = client
        .call("ticket.new", json!({"title": "a second consumer", "scope": "myrepo"}))
        .await
        .unwrap()["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string();
    let reuse_for_second_task = [
        "--json", "bbs", "reuse", &f, "--task", &second_task, "--outcome", "used",
        "--text", "Applied it to my case", "--evidence", &ev,
    ];
    let receipt_for_second_task = success(cli(&layout, Some("carol"), &reuse_for_second_task));
    assert_ne!(
        receipt_for_second_task["id"], r,
        "a different consuming task must get its own receipt, not the first task's"
    );
    let shown_second = success(cli(
        &layout, Some("carol"),
        &["--json", "bbs", "show", receipt_for_second_task["id"].as_str().unwrap()],
    ));
    assert_eq!(shown_second["tuple"]["payload"]["task"], second_task);

    // Hostile: a forged Session-lifecycle artifact merely CLAIMING
    // `"bbs_kind":"finding"` (never produced by `bbs.publish`, which always
    // writes Furniture) must not be accepted as a typed finding source, and
    // neither must a non-string `bbs_kind`.
    let forged_finding = client
        .call(
            "space.out",
            json!({"category":"artifact","scope":"myrepo","identity":"forged-session-finding","payload":{"bbs_kind":"finding","text":"not a real finding"}}),
        )
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let reuse_of_forged_finding = [
        "--json", "bbs", "reuse", &forged_finding, "--task", &consumer_task, "--outcome", "used",
        "--text", "trying to reuse a forged finding", "--evidence", &ev,
    ];
    assert!(!cli(&layout, Some("carol"), &reuse_of_forged_finding).status.success());
    let non_string_kind = client
        .call(
            "space.out",
            json!({"category":"artifact","scope":"myrepo","identity":"non-string-kind","payload":{"bbs_kind":123}}),
        )
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let reuse_of_non_string_kind = [
        "--json", "bbs", "reuse", &non_string_kind, "--task", &consumer_task, "--outcome", "used",
        "--text", "trying to reuse a non-string bbs_kind", "--evidence", &ev,
    ];
    assert!(!cli(&layout, Some("carol"), &reuse_of_non_string_kind).status.success());

    // Ordinary workers cannot assess or self-grant authority.
    let assess_args = [
        "--json", "bbs", "assess", &r, "--verdict", "verified", "--reason",
        "Confirmed independently", "--evidence", &ev,
    ];
    let denied = failure(cli(&layout, Some(&bob), &assess_args));
    assert!(denied.contains("not authorized for bbs.assess"), "{denied}");

    // Operator assesses.
    let assessment = success(cli(&layout, None, &assess_args));
    assert_eq!(assessment["kind"], "assessment");

    // Hostile forged raw writes must be refused, mirroring the existing
    // bbs-question/answer/accept forgery guard.
    let mut carol = Client::connect_as(&layout, "carol").await.unwrap();
    assert!(carol
        .call(
            "space.out",
            json!({"category":"artifact","scope":"myrepo","identity":"forged-finding","payload":{"bbs_kind":"finding","text":"fake"}})
        )
        .await
        .is_err());
    assert!(carol
        .call(
            "space.out",
            json!({"category":"artifact","scope":"myrepo","identity":format!("bbs-reuse-{r}"),"payload":{}})
        )
        .await
        .is_err());

    // `bbs show` threads BOTH receipts (bob's assessed one and carol's
    // second-task one) and the current assessment onto the finding.
    let find_receipt = |shown: &Value, id: &str| -> Value {
        shown["reuse"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["receipt"]["id"] == id)
            .unwrap()
            .clone()
    };
    let shown = success(cli(&layout, Some("carol"), &["--json", "bbs", "show", &f]));
    assert_eq!(shown["reuse"].as_array().unwrap().len(), 2);
    let bobs_entry = find_receipt(&shown, &r);
    assert_eq!(bobs_entry["current_assessment"]["payload"]["verdict"], "verified");

    // Restart: everything survives, re-publishing/re-reusing stays idempotent
    // against the durable record rather than a fresh duplicate, and the
    // consumer's retry identity (bound to her real SpawnId) is unaffected —
    // the same generation's record still resolves to the same receipt.
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
    let (mut client, handle) = start(&layout).await;
    let shown_after_restart = success(cli(&layout, Some("carol"), &["--json", "bbs", "show", &f]));
    assert_eq!(shown_after_restart["reuse"].as_array().unwrap().len(), 2);
    let bobs_entry_after_restart = find_receipt(&shown_after_restart, &r);
    assert_eq!(
        bobs_entry_after_restart["current_assessment"]["payload"]["verdict"],
        "verified"
    );
    let post_restart_publish = success(cli(&layout, Some(&alice), &args));
    assert_eq!(post_restart_publish["id"], f);
    assert_eq!(post_restart_publish["written"], false);
    let post_restart_reuse = success(cli(&layout, Some(&bob), &reuse_args));
    assert_eq!(post_restart_reuse["id"], r);
    assert_eq!(post_restart_reuse["written"], false);
    let shown_r_after_restart = success(cli(&layout, Some("carol"), &["--json", "bbs", "show", &r]));
    assert_eq!(
        shown_r_after_restart["tuple"]["payload"]["spawn"].as_str().unwrap(),
        receipt_spawn,
        "the consumer's native SpawnId is durable across a restart"
    );

    // A finding stays discoverable, but the receipt/assessment records that
    // crowd nothing out of `bbs brief`.
    let brief = success(cli(
        &layout,
        Some("carol"),
        &["--json", "bbs", "brief", "--repo", "myrepo", "--task", &mine],
    ));
    let entries = brief["entries"].as_array().unwrap();
    assert!(entries.iter().any(|e| e["id"] == f), "the finding must be discoverable");
    assert!(!entries.iter().any(|e| e["id"] == r), "a reuse receipt must not appear in discovery");
    assert!(
        !entries.iter().any(|e| e["id"] == assessment["id"]),
        "an assessment must not appear in discovery"
    );

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}
