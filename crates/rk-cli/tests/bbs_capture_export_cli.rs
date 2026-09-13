//! Real-`rk`, real-daemon acceptance for the S2 capture surfaces the branch
//! only had in-process unit coverage for: the `brief` exposure surface, the
//! `open` records `bbs show` leaves behind, empty-versus-missing capture, the
//! nonfatal-capture contract under a store that genuinely refuses the write,
//! forged telemetry, and the bounded `bbs.export`/`rk bbs export` wire —
//! including whether a pinned boundary actually holds one snapshot across
//! pages.
//!
//! The unit tests assert the predicates (`is_telemetry`,
//! `RESERVED_IDENTITY_PREFIXES`). These assert the behaviour a consumer
//! actually gets over the wire.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use rk_space::Space;
use serde_json::{json, Value};
use std::path::Path;
use std::process::{Command, Output};
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

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    std::fs::create_dir_all(dir.join(".rk")).unwrap();
    std::fs::write(dir.join(".rk/repo.cue"), "repo: {}\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

/// Drive the real `rk` binary, with the full spawn-identity environment
/// stripped so this rat's own ambient identity cannot authenticate the call.
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

fn failure(output: Output) -> String {
    assert!(
        !output.status.success(),
        "expected failure, got: {output:?}"
    );
    String::from_utf8_lossy(&output.stderr).to_string()
}

/// A real daemon over a `Space` the test keeps a handle on, so the telemetry
/// write fault can be injected into a genuinely running daemon.
async fn start(layout: &Layout, space: Space) -> Client {
    let daemon = Daemon::with_space_for_tests(
        layout.clone(),
        "test-castle".into(),
        "fake".into(),
        rk_ledger::Budget::default(),
        space,
    )
    .unwrap();
    tokio::spawn(daemon.run());
    for _ in 0..250 {
        if let Ok(client) = Client::connect_as_operator(layout).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("daemon did not start");
}

const FAKE_HARNESS: &str = r#"
read -r _prompt
echo '{"type":"system","subtype":"init","session_id":"fake"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

async fn ticket(client: &mut Client, title: &str) -> String {
    client
        .call("ticket.new", json!({"title": title, "scope": "myrepo"}))
        .await
        .unwrap()["ticket"]["identity"]
        .as_str()
        .unwrap()
        .to_string()
}

/// A genuinely supervised agent, so exposure/open records bind to a real
/// `AgentRecord` generation rather than a bare caller name.
async fn spawn(layout: &Layout, client: &mut Client, task: &str) -> (String, String) {
    let spawned = success(cli(
        layout,
        None,
        &["--json", "spawn", "--ticket", task, "--harness", "fake"],
    ));
    let name = spawned["name"].as_str().unwrap().to_string();
    for _ in 0..250 {
        let status = client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap();
        if status["agent"]["spawn"].as_str().is_some() {
            return (
                name.clone(),
                status["agent"]["spawn"].as_str().unwrap().to_string(),
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{name} never reported a generation");
}

/// Every daemon-authored record of one `bbs_kind`, read as the operator.
async fn telemetry(client: &mut Client, kind: &str) -> Vec<Value> {
    let scanned = client
        .call(
            "space.scan",
            json!({"category": "event", "scope": "myrepo"}),
        )
        .await
        .unwrap();
    scanned["tuples"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|t| t["payload"]["bbs_kind"] == kind)
        .map(|t| t["payload"].clone())
        .collect()
}

/// Surface 4 of 4 (`brief`), the `open` records, empty-versus-missing capture,
/// the no-metadata-in-peer-briefings rule, forged telemetry, and the nonfatal
/// contract — all over the real CLI against a real daemon.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn brief_and_show_capture_exactly_what_was_served_and_never_leak_into_a_briefing() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", FAKE_HARNESS);

    let layout = Layout::at(home.path());
    let space = Space::open(&layout.db_path()).unwrap();
    let mut client = start(&layout, space.clone()).await;
    client
        .call(
            "repo.add",
            json!({"name": "myrepo", "path": repo_dir.path()}),
        )
        .await
        .unwrap();

    let authors_task = ticket(&mut client, "parser work").await;
    let consumer_task = ticket(&mut client, "consumer work").await;
    let quiet_task = ticket(&mut client, "quietzz").await;
    let (alice, alice_spawn) = spawn(&layout, &mut client, &authors_task).await;
    let (bob, bob_spawn) = spawn(&layout, &mut client, &consumer_task).await;
    assert_ne!(alice_spawn, bob_spawn);

    let evidence = client
        .call(
            "space.out",
            json!({"category":"artifact","scope":"myrepo","identity":"ev1","payload":{"summary":"repro"}}),
        )
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let finding = success(cli(
        &layout,
        Some(&alice),
        &[
            "--json",
            "bbs",
            "publish",
            "Delimiters must be escaped",
            "--repo",
            "myrepo",
            "--task",
            &authors_task,
            "--area",
            "src/parser.rs",
            "--revision",
            &"a".repeat(40),
            "--evidence",
            &evidence,
            "--limitations",
            "ASCII only",
        ],
    ))["id"]
        .as_str()
        .unwrap()
        .to_string();

    // --- Surface 4 of 4: an explicit `rk bbs brief` by an authenticated
    // consumer. The record must name the exact sources prepared and the exact
    // consumer GENERATION, not just the name.
    let briefing = success(cli(
        &layout,
        Some(&bob),
        &[
            "--json",
            "bbs",
            "brief",
            "--repo",
            "myrepo",
            "--task",
            &consumer_task,
            "--area",
            "src/parser.rs",
        ],
    ));
    assert_eq!(briefing["telemetry"], "recorded");
    assert!(briefing["exposure"].as_str().is_some());

    let exposures = telemetry(&mut client, "exposure").await;
    let brief_record = exposures
        .iter()
        .find(|e| e["surface"] == "brief")
        .expect("an explicit brief must record a `brief` exposure");
    assert_eq!(brief_record["agent"], bob.as_str());
    assert_eq!(brief_record["spawn"], bob_spawn.as_str());
    assert_eq!(brief_record["bound"], "agent");
    assert_eq!(brief_record["repo"], "myrepo");
    let entries = brief_record["entries"].as_array().unwrap();
    assert_eq!(
        brief_record["prepared"].as_u64().unwrap() as usize,
        entries.len(),
        "the count and the enumeration must agree: {brief_record:?}"
    );
    let entry = entries
        .iter()
        .find(|e| e["source"] == finding.as_str())
        .unwrap_or_else(|| {
            panic!("alice's finding must be among the prepared sources: {entries:?}")
        });
    assert_eq!(entry["kind"], "finding");
    assert_eq!(
        entry["reason"], "shared area",
        "the record must carry the EXACT reason the source was selected, not \
         merely that it was: {entry:?}"
    );
    assert_eq!(entry["category"], "artifact");
    assert!(brief_record["omitted"].as_u64().is_some());

    // Empty is a record with zero entries; MISSING is the absence of a record.
    // A task nothing was ever prepared for has no exposure at all, which is
    // what makes `prepared: 0` meaningful rather than indistinguishable.
    assert!(
        !exposures.iter().any(|e| e["task"] == quiet_task.as_str()),
        "a task that was never briefed must leave NO exposure record"
    );
    let empty = success(cli(
        &layout,
        Some(&bob),
        &[
            "--json",
            "bbs",
            "brief",
            "--repo",
            "myrepo",
            "--task",
            &quiet_task,
        ],
    ));
    assert_eq!(empty["telemetry"], "recorded");
    let quiet = telemetry(&mut client, "exposure")
        .await
        .into_iter()
        .find(|e| e["task"] == quiet_task.as_str() && e["surface"] == "brief")
        .expect("briefing it once DOES leave an empty record");
    assert_eq!(quiet["prepared"], 0);
    assert_eq!(quiet["entries"], json!([]));

    // --- `open`: requested/served, never comprehended. A re-read stays
    // visible as a re-read, under ONE dedup key pairing source with consumer
    // generation.
    for _ in 0..2 {
        let shown = success(cli(
            &layout,
            Some(&bob),
            &["--json", "bbs", "show", &finding],
        ));
        assert_eq!(shown["telemetry"], "recorded");
    }
    let opens = telemetry(&mut client, "open").await;
    let mine: Vec<&Value> = opens
        .iter()
        .filter(|o| o["source"] == finding.as_str() && o["agent"] == bob.as_str())
        .collect();
    assert_eq!(
        mine.len(),
        2,
        "two reads are two retained records: {opens:?}"
    );
    assert_eq!(mine[0]["dedup_key"], mine[1]["dedup_key"]);
    assert_eq!(
        mine[0]["dedup_key"],
        format!("{finding}:{bob_spawn}").as_str(),
        "the dedup key is (source, consumer generation)"
    );
    assert_eq!(mine[0]["source_kind"], "finding");
    assert_eq!(mine[0]["spawn"], bob_spawn.as_str());
    assert_eq!(mine[0]["semantics"], "requested");
    assert_ne!(
        mine[0]["spawn"],
        alice_spawn.as_str(),
        "the consumer generation is the reader's, not the author's"
    );

    // --- Measurement metadata must never reach a peer-facing surface. By now
    // the store holds exposure, open and native records for this repo; a real
    // briefing must still contain none of them.
    let peer = success(cli(
        &layout,
        Some(&alice),
        &[
            "--json",
            "bbs",
            "brief",
            "--repo",
            "myrepo",
            "--task",
            &authors_task,
        ],
    ));
    let shown_entries = peer["entries"].as_array().unwrap();
    assert!(
        !shown_entries.is_empty(),
        "a vacuous briefing would prove nothing about exclusion: {peer:?}"
    );
    for entry in shown_entries {
        let source = entry["id"].as_str().unwrap();
        let shown = success(cli(&layout, None, &["--json", "bbs", "show", source]));
        let kind = shown["tuple"]["payload"]["bbs_kind"].as_str().unwrap_or("");
        assert!(
            ![
                "exposure",
                "open",
                "telemetry_gap",
                "agent_final_usage",
                "agent_exit"
            ]
            .contains(&kind),
            "telemetry leaked into a peer briefing as {source} ({kind})"
        );
    }
    // And the records provably exist by now, so the exclusion above is a real
    // filter rather than an empty store.
    assert!(!telemetry(&mut client, "exposure").await.is_empty());
    assert!(!telemetry(&mut client, "open").await.is_empty());

    // --- Forged telemetry: read authorization is not authority to author the
    // measurement records about those reads.
    let furniture = failure(cli(
        &layout,
        Some(&bob),
        &[
            "out",
            "event",
            "myrepo",
            "bbs-agent-exit",
            "--payload",
            r#"{"bbs_kind":"agent_exit","exit_code":0}"#,
            "--lifecycle",
            "furniture",
        ],
    ));
    assert!(
        furniture.to_lowercase().contains("forbidden") || furniture.contains("cannot write"),
        "{furniture}"
    );
    // ...and stripping the lifecycle does not help: the reserved identity and
    // the `bbs_kind` payload are each refused on their own.
    let plain = failure(cli(
        &layout,
        Some(&bob),
        &[
            "out",
            "event",
            "myrepo",
            "bbs-exposure-brief",
            "--payload",
            r#"{"bbs_kind":"exposure","prepared":99}"#,
        ],
    ));
    assert!(plain.to_lowercase().contains("forbidden"), "{plain}");
    assert!(
        !telemetry(&mut client, "exposure")
            .await
            .iter()
            .any(|e| e["prepared"] == 99),
        "no forged exposure may have landed"
    );

    // --- Nonfatal capture. The store now refuses telemetry records outright;
    // the read it describes must still succeed, report its own coverage gap,
    // and leave a durable `telemetry_gap` naming what went missing.
    space.fail_bbs_telemetry_writes_for_tests(true);
    let degraded = success(cli(
        &layout,
        Some(&bob),
        &["--json", "bbs", "show", &finding],
    ));
    assert_eq!(
        degraded["telemetry"], "failed",
        "a capture failure is REPORTED on the response, not raised"
    );
    assert!(degraded["open"].is_null());
    assert_eq!(
        degraded["tuple"]["id"],
        finding.as_str(),
        "the read still served its answer in full"
    );
    let still_briefed = success(cli(
        &layout,
        Some(&bob),
        &[
            "--json",
            "bbs",
            "brief",
            "--repo",
            "myrepo",
            "--task",
            &consumer_task,
        ],
    ));
    assert_eq!(still_briefed["telemetry"], "failed");
    assert!(
        still_briefed["entries"].as_array().is_some(),
        "the briefing is still computed and returned: {still_briefed:?}"
    );
    let gaps = telemetry(&mut client, "telemetry_gap").await;
    assert!(
        gaps.iter().any(|g| g["missing"] == "bbs-open"),
        "the missing record must be named, so the gap is known-missing rather \
         than mistaken for a known-negative: {gaps:?}"
    );
    assert!(gaps.iter().any(|g| g["context"]["surface"] == "show"));
    space.fail_bbs_telemetry_writes_for_tests(false);
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

/// The bounded export wire: the exact `order` enum, one snapshot pinned across
/// pages, a future boundary refused rather than clamped, reference closure
/// reported honestly, and a worker fenced to its own repository.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn export_pins_one_snapshot_across_pages_and_refuses_a_future_boundary() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());
    std::env::set_var("RK_FAKE_HARNESS_CMD", FAKE_HARNESS);

    let layout = Layout::at(home.path());
    let space = Space::open(&layout.db_path()).unwrap();
    let mut client = start(&layout, space).await;
    for name in ["myrepo", "otherrepo"] {
        client
            .call("repo.add", json!({"name": name, "path": repo_dir.path()}))
            .await
            .unwrap();
    }
    let task = ticket(&mut client, "export work").await;
    let (alice, _) = spawn(&layout, &mut client, &task).await;

    // A finding whose evidence is NOT in the repository's own scope, so the
    // closure has a reference it must report rather than export.
    let foreign = client
        .call(
            "space.out",
            json!({"category":"artifact","scope":"otherrepo","identity":"ev-foreign","payload":{}}),
        )
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let local = client
        .call(
            "space.out",
            json!({"category":"artifact","scope":"myrepo","identity":"ev-local","payload":{}}),
        )
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    success(cli(
        &layout,
        Some(&alice),
        &[
            "--json",
            "bbs",
            "publish",
            "local evidence",
            "--repo",
            "myrepo",
            "--task",
            &task,
            "--area",
            "src/a.rs",
            "--revision",
            &"b".repeat(40),
            "--evidence",
            &local,
            "--limitations",
            "none",
        ],
    ));

    // --- Page 1. The ordering claim is the point of the surface.
    let page1 = success(cli(
        &layout,
        None,
        &[
            "--json", "bbs", "export", "--repo", "myrepo", "--limit", "2",
        ],
    ));
    assert_eq!(
        page1["order"], "persistence_sequence",
        "a consumer that does not see exactly this must treat order as unknown"
    );
    assert_eq!(
        page1["order_provenance"], "tuple_persistence_events.commit_sequence ascending",
        "the SQL detail is kept OUT of the wire enum so a column rename cannot \
         silently change the contract"
    );
    assert_eq!(page1["source"], "space.persistence_page");
    assert_eq!(page1["kind"], "bbs.export");
    assert_eq!(
        page1["truncated"], true,
        "limit 2 cannot have drained the repo"
    );
    assert_eq!(
        page1["coverage"]["complete"], false,
        "a truncated page is never complete"
    );
    let boundary = page1["boundary"].as_u64().unwrap();
    assert_eq!(page1["pinned_boundary"], boundary);
    let cursor = page1["next_cursor"].as_u64().unwrap();
    let sequences: Vec<u64> = page1["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["commit_sequence"].as_u64().unwrap())
        .collect();
    assert_eq!(sequences.len(), 2);
    assert!(sequences[0] < sequences[1], "ascending: {sequences:?}");
    assert!(sequences.iter().all(|s| *s <= boundary));

    // --- A concurrent write lands BETWEEN pages. This is the whole reason a
    // caller-pinned boundary exists.
    let mid = client
        .call(
            "space.out",
            json!({"category":"artifact","scope":"myrepo","identity":"written-between-pages","payload":{}}),
        )
        .await
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let ids_of = |page: &Value| -> Vec<String> {
        page["tuples"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["id"].as_str().unwrap().to_string())
            .collect()
    };
    let after = cursor.to_string();
    let pinned = success(cli(
        &layout,
        None,
        &[
            "--json",
            "bbs",
            "export",
            "--repo",
            "myrepo",
            "--limit",
            "2000",
            "--after",
            &after,
            "--boundary",
            &boundary.to_string(),
        ],
    ));
    assert_eq!(
        pinned["boundary"], boundary,
        "the snapshot is held, not recaptured"
    );
    assert!(
        !ids_of(&pinned).contains(&mid),
        "a row written after the pinned boundary must not appear in a later page \
         of the SAME snapshot"
    );

    // Without the pin the second page captures a FRESH boundary, and the
    // concurrent write appears mid-paging — the failure the pin prevents.
    let unpinned = success(cli(
        &layout,
        None,
        &[
            "--json", "bbs", "export", "--repo", "myrepo", "--limit", "2000", "--after", &after,
        ],
    ));
    assert!(unpinned["boundary"].as_u64().unwrap() > boundary);
    assert!(
        ids_of(&unpinned).contains(&mid),
        "proof the pin was doing real work, not passing trivially"
    );

    // --- A boundary ahead of the store is REFUSED, not clamped: clamping would
    // silently answer a different question than the one asked.
    let future = failure(cli(
        &layout,
        None,
        &[
            "--json",
            "bbs",
            "export",
            "--repo",
            "myrepo",
            "--boundary",
            &(boundary + 10_000).to_string(),
        ],
    ));
    assert!(
        future.contains("ahead of the store"),
        "a future boundary must be refused: {future}"
    );

    // --- Reference closure: the foreign-scope evidence is reported, never
    // exported into another repository's capture.
    // `bbs publish` refuses cross-repo evidence outright (that fence is proven
    // in `bbs_stigmergy.rs`), so the operator writes the finding shape directly
    // through the same RPC. The export closure cannot tell the two apart, and
    // it is the closure under test here — not publish.
    client
        .call(
            "space.out",
            json!({"category":"artifact","scope":"myrepo","identity":"bbs-finding-names-foreign",
                   "lifecycle":"furniture",
                   "payload":{"bbs_kind":"finding","evidence":[foreign]}}),
        )
        .await
        .unwrap();
    let full = success(cli(
        &layout,
        None,
        &[
            "--json", "bbs", "export", "--repo", "myrepo", "--limit", "2000",
        ],
    ));
    assert_eq!(full["truncated"], false);
    let missing = full["coverage"]["missing_references"].as_array().unwrap();
    assert!(
        missing.iter().any(|m| m == &json!(foreign)),
        "a foreign-scope reference is REPORTED, never exported: {missing:?}"
    );
    assert_eq!(
        full["coverage"]["complete"], false,
        "`complete` must be false while any reference is unresolved"
    );
    assert!(
        !ids_of(&full).contains(&foreign)
            && !full["references"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["id"] == foreign.as_str()),
        "the foreign tuple itself must not cross the scope fence"
    );
    assert_eq!(full["coverage"]["scope"], "myrepo");
    assert_eq!(full["coverage"]["reference_depth"], 4);

    // --- A worker may capture only its own repository.
    let fenced = failure(cli(
        &layout,
        Some(&alice),
        &["--json", "bbs", "export", "--repo", "otherrepo"],
    ));
    assert!(fenced.contains("assigned repository"), "{fenced}");
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
