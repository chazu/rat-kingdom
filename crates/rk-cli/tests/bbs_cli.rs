//! Agent-facing BBS journeys through the real CLI and an isolated daemon.
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::process::{Command, Output};
use std::time::Duration;

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
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn question_answer_acceptance_survives_restart_and_checks_requester() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    let (mut client, handle) = start(&layout).await;
    let ask = [
        "--json",
        "bbs",
        "ask",
        "Which delimiter?",
        "--repo",
        "repo",
        "--task",
        "parser",
    ];
    let question = success(cli(&layout, Some("alice"), &ask));
    let q = question["id"].as_str().unwrap();
    assert_eq!(success(cli(&layout, Some("alice"), &ask))["id"], q);
    let answer = success(cli(
        &layout,
        Some("bob"),
        &["--json", "bbs", "answer", q, "Use a newline"],
    ));
    let a = answer["id"].as_str().unwrap();
    let open = success(cli(&layout, Some("alice"), &["--json", "bbs", "show", q]));
    assert_eq!(open["status"], "open");
    assert_eq!(open["replies"].as_array().unwrap().len(), 1);
    assert_eq!(open["question"]["lifecycle"], "furniture");
    assert!(open["question"]["strength"].is_null());
    let rejected = cli(
        &layout,
        Some("bob"),
        &["--json", "bbs", "accept", q, a, "Used the delimiter"],
    );
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("requester"));
    let other = success(cli(
        &layout,
        Some("alice"),
        &[
            "--json",
            "bbs",
            "ask",
            "What encoding?",
            "--repo",
            "repo",
            "--task",
            "parser",
        ],
    ));
    assert!(!cli(
        &layout,
        Some("alice"),
        &[
            "--json",
            "bbs",
            "accept",
            other["id"].as_str().unwrap(),
            a,
            "Wrong thread"
        ]
    )
    .status
    .success());
    // Two requester connections racing different answers cannot both accept.
    let race_question = success(cli(
        &layout,
        Some("alice"),
        &[
            "--json",
            "bbs",
            "ask",
            "Which framing?",
            "--repo",
            "repo",
            "--task",
            "parser",
        ],
    ));
    let race_q = race_question["id"].as_str().unwrap();
    let answer_one = success(cli(
        &layout,
        Some("bob"),
        &["--json", "bbs", "answer", race_q, "Length prefix"],
    ));
    let answer_two = success(cli(
        &layout,
        Some("carol"),
        &["--json", "bbs", "answer", race_q, "Delimiter"],
    ));
    let mut first_requester = Client::connect_as(&layout, "alice").await.unwrap();
    let mut second_requester = Client::connect_as(&layout, "alice").await.unwrap();
    let (one, two) = tokio::join!(
        first_requester.call(
            "bbs.accept",
            json!({"question":race_q,"answer":answer_one["id"],"text":"Use length prefix"})
        ),
        second_requester.call(
            "bbs.accept",
            json!({"question":race_q,"answer":answer_two["id"],"text":"Use delimiter"})
        )
    );
    assert_ne!(one.is_ok(), two.is_ok());
    let mut bob = Client::connect_as(&layout, "bob").await.unwrap();
    assert!(bob.call("space.out",json!({"category":"artifact","scope":"repo","identity":"forged","payload":{"bbs_kind":"acceptance","question":q,"answer":a}})).await.is_err());
    assert!(bob.call("space.out",json!({"category":"artifact","scope":"repo","identity":format!("bbs-accept-{q}"),"payload":{}})).await.is_err());
    assert!(bob
        .call(
            "space.take",
            json!({"category":"need","identity":open["question"]["identity"],"timeout_ms":1})
        )
        .await
        .unwrap()["tuple"]
        .is_null());
    // A generic resolver must not bypass requester acceptance or erase history.
    bob.call("space.out",json!({"category":"artifact","scope":"repo","identity":"fake-resolution","payload":{"resolves":q}})).await.unwrap();
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
    drop(bob);
    let (mut client, handle) = start(&layout).await;
    assert_eq!(
        success(cli(&layout, Some("alice"), &["--json", "bbs", "show", q]))["status"],
        "open"
    );
    let contribution = client.call("space.out",json!({"category":"artifact","scope":"repo","identity":"parser-change","payload":{"summary":"Parser consumes newline records","commit":"abc123"}})).await.unwrap();
    let args = [
        "--json",
        "bbs",
        "accept",
        q,
        a,
        "Changed the parser",
        "--contribution",
        contribution["id"].as_str().unwrap(),
    ];
    let accepted = success(cli(&layout, Some("alice"), &args));
    assert_eq!(
        success(cli(&layout, Some("alice"), &args))["id"],
        accepted["id"]
    );
    let thread = success(cli(&layout, Some("alice"), &["--json", "bbs", "show", a]));
    assert_eq!(thread["status"], "accepted");
    assert_eq!(
        success(cli(
            &layout,
            Some("bob"),
            &["--json", "bbs", "answer", q, "Use a newline"]
        ))["id"],
        a
    );
    assert!(!cli(
        &layout,
        Some("bob"),
        &["--json", "bbs", "answer", q, "A different answer"]
    )
    .status
    .success());
    let readable = cli(&layout, Some("alice"), &["bbs", "show", q]);
    assert!(readable.status.success());
    let readable = String::from_utf8_lossy(&readable.stdout);
    assert!(
        readable.contains("(accepted)")
            && readable.contains("Use a newline")
            && readable.contains("Changed the parser")
    );
    assert_eq!(thread["acceptance"]["payload"]["answer"], a);
    assert_eq!(
        thread["acceptance"]["payload"]["contribution"],
        contribution["id"]
    );
    let brief = success(cli(
        &layout,
        Some("alice"),
        &[
            "--json", "bbs", "brief", "--repo", "repo", "--task", "parser",
        ],
    ));
    assert!(!brief["entries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["id"] == q));
    let inbox = client
        .call("inbox.list", json!({"repo":"repo"}))
        .await
        .unwrap();
    assert!(!inbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["subject"] == open["question"]["identity"]));
    assert!(!cli(
        &layout,
        Some("alice"),
        &["--json", "bbs", "accept", q, a, "Different outcome"]
    )
    .status
    .success());
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
    let (mut client, handle) = start(&layout).await;
    assert_eq!(
        success(cli(&layout, Some("alice"), &["--json", "bbs", "show", q]))["status"],
        "accepted"
    );
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn briefing_selects_dependencies_bounds_noise_and_refreshes() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    let (mut client, handle) = start(&layout).await;
    for (id, payload) in [
        (
            "TKT-1",
            json!({"title":"Implement parser", "depends_on":["TKT-2"]}),
        ),
        ("TKT-2", json!({"title":"Define grammar"})),
    ] {
        client
            .call(
                "space.out",
                json!({"category":"task","scope":"repo","identity":id,"payload":payload}),
            )
            .await
            .unwrap();
    }
    let artifact = client.call("space.out",json!({"category":"artifact","scope":"repo","identity":"grammar-contract","instance":"alice","payload":{"task":"TKT-2","summary":"Use newline delimiters","branch":"rat/alice/grammar","commit":"abc123"}})).await.unwrap();
    for n in 0..30 {
        client.call("space.out",json!({"category":"artifact","scope":"repo","identity":format!("noise-{n}"),"payload":{"task":"TKT-10","summary":"Unrelated history"}})).await.unwrap();
    }
    client.call("space.out",json!({"category":"artifact","scope":"other","identity":"foreign","payload":{"task":"TKT-2"}})).await.unwrap();
    let first = success(cli(
        &layout,
        Some("bob"),
        &[
            "--json", "bbs", "brief", "--repo", "repo", "--task", "TKT-1", "--limit", "1",
        ],
    ));
    assert_eq!(first["entries"].as_array().unwrap().len(), 1);
    assert_eq!(first["entries"][0]["id"], artifact["id"]);
    assert_eq!(first["entries"][0]["branch"], "rat/alice/grammar");
    let focused = success(cli(
        &layout,
        Some("bob"),
        &[
            "--json",
            "bbs",
            "brief",
            "--repo",
            "repo",
            "--task",
            "TKT-1",
            "--area",
            "unmatched/path",
        ],
    ));
    assert!(focused["entries"].as_array().unwrap().is_empty());
    let shown = success(cli(
        &layout,
        Some("bob"),
        &["--json", "bbs", "show", artifact["id"].as_str().unwrap()],
    ));
    assert_eq!(
        shown["tuple"]["payload"]["summary"],
        "Use newline delimiters"
    );
    client.call("space.out",json!({"category":"need","scope":"repo","identity":"question","instance":"alice","payload":{"task":"TKT-2","text":"Does the delimiter allow escaping?"}})).await.unwrap();
    let cursor = first["cursor"].to_string();
    let refreshed = success(cli(
        &layout,
        Some("bob"),
        &[
            "--json", "bbs", "brief", "--repo", "repo", "--task", "TKT-1", "--since", &cursor,
        ],
    ));
    assert!(refreshed["entries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["category"] == "need" && e["changed"] == true));
    assert!(refreshed["entries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["id"] == artifact["id"] && e["changed"] == false));
    let text = cli(
        &layout,
        Some("bob"),
        &["bbs", "brief", "--repo", "repo", "--task", "TKT-1"],
    );
    assert!(text.status.success());
    assert!(String::from_utf8_lossy(&text.stdout).contains("rk bbs show"));
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}
