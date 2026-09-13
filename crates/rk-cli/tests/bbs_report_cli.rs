//! `rk bbs report` through the real CLI binary — deliberately with NO daemon
//! running: the offline evidence report must render from saved manifest/
//! tuple/review JSON alone (docs/2026-09-12-stigmergy-evidence-and-trial.md,
//! S3: TKT-tavik-kifos-lozuf).

use serde_json::{json, Value};
use std::io::Write;
use std::process::{Command, Output};

fn write_json(dir: &std::path::Path, name: &str, value: &Value) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(serde_json::to_string_pretty(value).unwrap().as_bytes())
        .unwrap();
    path
}

fn cli_in_home(home: &std::path::Path, args: &[&str], agent: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rk"));
    command.args(args);
    for key in rk_core::review::STRIPPED_RK_SPAWN_ENV {
        command.env_remove(key);
    }
    // An empty RK_HOME with no daemon in it. Falling back to the developer's
    // real ~/.rat-kingdom would let a live daemon answer a connection this
    // command must never open, so the no-daemon invariant would go untested.
    command.env("RK_HOME", home);
    if let Some(agent) = agent {
        command.env("RK_AGENT", agent);
    }
    command.output().unwrap()
}

fn cli(args: &[&str]) -> Output {
    let home = tempfile::tempdir().unwrap();
    cli_in_home(home.path(), args, None)
}

fn manifest() -> Value {
    json!({
        "schema_version": 1,
        "experiment_id": "rk-stigmergy-cli-test",
        "repos": ["repo"],
        "batches": [{"id": "batch-1", "arm": "real", "repo": "repo"}],
        "eligible_pairs": [{
            "id": "p1",
            "source": "src-1",
            "consumer_task": "TKT-1",
            "consumer_generation": "gen-1",
            "repo": "repo",
            "batch": "batch-1"
        }]
    })
}

fn tuples_envelope() -> Value {
    json!({
        "tuples": [
            {
                "id": "src-1", "category": "artifact", "scope": "repo",
                "identity": "finding-1", "instance": "author", "lifecycle": "furniture",
                "created_at": "2026-01-01T00:00:00Z",
                "payload": {
                    "schema_version": 1, "bbs_kind": "finding", "agent": "author",
                    "spawn": "author-gen", "task": "TKT-source",
                    "text": "interface constraint", "areas": ["src/x.rs"],
                    "revision": "abc123", "evidence": ["ev-1"], "limitations": "none"
                }
            },
            {
                "id": "ev-1", "category": "artifact", "scope": "repo",
                "identity": "ev-1", "instance": "author",
                "created_at": "2026-01-01T00:00:00Z", "payload": {}
            },
            {
                "id": "r1", "category": "artifact", "scope": "repo",
                "identity": "reuse-1", "instance": "consumer", "lifecycle": "furniture",
                "created_at": "2026-01-02T00:00:00Z",
                "payload": {
                    "schema_version": 1, "bbs_kind": "reuse", "agent": "consumer",
                    "spawn": "gen-1", "task": "TKT-1", "source": "src-1",
                    "outcome": "used", "text": "used it", "evidence": ["ev-2"]
                }
            },
            {
                "id": "ev-2", "category": "artifact", "scope": "repo",
                "identity": "ev-2", "instance": "consumer",
                "created_at": "2026-01-02T00:00:00Z", "payload": {}
            },
            {
                "id": "a1", "category": "artifact", "scope": "repo",
                "identity": "assessment-1", "instance": "operator", "lifecycle": "furniture",
                "created_at": "2026-01-03T00:00:00Z",
                "payload": {
                    "schema_version": 1, "bbs_kind": "assessment", "agent": "operator",
                    "spawn": null, "task": "TKT-source", "receipt": "r1",
                    "verdict": "verified", "reason": "matches delivered work",
                    "evidence": ["ev-3"]
                }
            },
            {
                "id": "ev-3", "category": "artifact", "scope": "repo",
                "identity": "ev-3", "instance": "operator",
                "created_at": "2026-01-03T00:00:00Z", "payload": {}
            }
        ],
        "truncated": false
    })
}

fn reviews() -> Value {
    json!([{
        "pair": "p1",
        "coverage": {"status": "prepared", "evidence": "manual-check"},
        "relayed_by_operator": false
    }])
}

#[test]
fn offline_report_needs_no_daemon_and_computes_expected_counts() {
    let dir = tempfile::tempdir().unwrap();
    let manifest_path = write_json(dir.path(), "manifest.json", &manifest());
    let tuples_path = write_json(dir.path(), "tuples.json", &tuples_envelope());
    let reviews_path = write_json(dir.path(), "reviews.json", &reviews());

    let out = cli(&[
        "--json",
        "bbs",
        "report",
        "--manifest",
        manifest_path.to_str().unwrap(),
        "--tuples",
        tuples_path.to_str().unwrap(),
        "--reviews",
        reviews_path.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let report: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(report["eligible"], 1);
    assert_eq!(report["claimed"], 1);
    assert_eq!(report["assessed"], 1);
    assert_eq!(report["outcome_classes"]["used"], 1);
    assert_eq!(report["outcome_classes"]["verified"], 1);
    assert_eq!(report["mechanism"]["effects"], 1);
    // No author_terminal_evidence was supplied, so this effect does not
    // also count toward the author-exit sub-goal.
    assert_eq!(report["mechanism"]["author_exit_effects"], 0);
    assert_eq!(report["excluded"].as_array().unwrap().len(), 0);
}

#[test]
fn same_inputs_produce_deterministic_repeated_output() {
    let dir = tempfile::tempdir().unwrap();
    let manifest_path = write_json(dir.path(), "manifest.json", &manifest());
    let tuples_path = write_json(dir.path(), "tuples.json", &tuples_envelope());
    let reviews_path = write_json(dir.path(), "reviews.json", &reviews());
    let args = [
        "--json",
        "bbs",
        "report",
        "--manifest",
        manifest_path.to_str().unwrap(),
        "--tuples",
        tuples_path.to_str().unwrap(),
        "--reviews",
        reviews_path.to_str().unwrap(),
    ];
    let first = cli(&args);
    let second = cli(&args);
    assert!(first.status.success());
    assert!(second.status.success());
    assert_eq!(first.stdout, second.stdout);
}

#[test]
fn writes_output_file_in_addition_to_stdout() {
    let dir = tempfile::tempdir().unwrap();
    let manifest_path = write_json(dir.path(), "manifest.json", &manifest());
    let tuples_path = write_json(dir.path(), "tuples.json", &tuples_envelope());
    let reviews_path = write_json(dir.path(), "reviews.json", &reviews());
    let output_path = dir.path().join("report.json");

    let out = cli(&[
        "--json",
        "bbs",
        "report",
        "--manifest",
        manifest_path.to_str().unwrap(),
        "--tuples",
        tuples_path.to_str().unwrap(),
        "--reviews",
        reviews_path.to_str().unwrap(),
        "--output",
        output_path.to_str().unwrap(),
    ]);
    assert!(out.status.success());
    let written: Value =
        serde_json::from_str(&std::fs::read_to_string(&output_path).unwrap()).unwrap();
    let stdout: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(written, stdout);
}

#[test]
fn human_readable_output_summarizes_the_report() {
    let dir = tempfile::tempdir().unwrap();
    let manifest_path = write_json(dir.path(), "manifest.json", &manifest());
    let tuples_path = write_json(dir.path(), "tuples.json", &tuples_envelope());
    let reviews_path = write_json(dir.path(), "reviews.json", &reviews());

    let out = cli(&[
        "bbs",
        "report",
        "--manifest",
        manifest_path.to_str().unwrap(),
        "--tuples",
        tuples_path.to_str().unwrap(),
        "--reviews",
        reviews_path.to_str().unwrap(),
    ]);
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("Stigmergy evidence report"));
    assert!(text.contains("eligible=1"));
    assert!(text.contains("mechanism goal"));
}

#[test]
fn rejects_manifest_with_unsupported_schema_version() {
    let dir = tempfile::tempdir().unwrap();
    let mut bad_manifest = manifest();
    bad_manifest["schema_version"] = json!(99);
    let manifest_path = write_json(dir.path(), "manifest.json", &bad_manifest);
    let tuples_path = write_json(dir.path(), "tuples.json", &tuples_envelope());
    let reviews_path = write_json(dir.path(), "reviews.json", &reviews());

    let out = cli(&[
        "--json",
        "bbs",
        "report",
        "--manifest",
        manifest_path.to_str().unwrap(),
        "--tuples",
        tuples_path.to_str().unwrap(),
        "--reviews",
        reviews_path.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("schema_version"));
}

#[test]
fn rejects_tuples_file_missing_the_tuples_array() {
    let dir = tempfile::tempdir().unwrap();
    let manifest_path = write_json(dir.path(), "manifest.json", &manifest());
    let tuples_path = write_json(dir.path(), "tuples.json", &json!({"not_tuples": []}));
    let reviews_path = write_json(dir.path(), "reviews.json", &reviews());

    let out = cli(&[
        "--json",
        "bbs",
        "report",
        "--manifest",
        manifest_path.to_str().unwrap(),
        "--tuples",
        tuples_path.to_str().unwrap(),
        "--reviews",
        reviews_path.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
}

#[test]
fn accepts_a_bare_tuple_array_as_well_as_the_scan_envelope() {
    let dir = tempfile::tempdir().unwrap();
    let manifest_path = write_json(dir.path(), "manifest.json", &manifest());
    let bare_array = tuples_envelope()["tuples"].clone();
    let tuples_path = write_json(dir.path(), "tuples.json", &bare_array);
    let reviews_path = write_json(dir.path(), "reviews.json", &reviews());

    let out = cli(&[
        "--json",
        "bbs",
        "report",
        "--manifest",
        manifest_path.to_str().unwrap(),
        "--tuples",
        tuples_path.to_str().unwrap(),
        "--reviews",
        reviews_path.to_str().unwrap(),
    ]);
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let report: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(report["tuples_order"], "unknown");
    assert_eq!(report["claimed"], 1);
}

/// The acceptance criterion the report exists for: no daemon connection is
/// required to render saved evidence. Both spellings of "no daemon" are
/// checked, because they fail differently — as a rat (`RK_AGENT` set) a
/// connect errors outright, while unset it silently spawns a whole detached
/// daemon (space.db/rk.sock/rk.pid/castle.key/auth.token/worktrees) as a side
/// effect of a pure offline aggregation.
#[test]
fn renders_offline_against_a_fresh_home_without_creating_daemon_artifacts() {
    for agent in [None, Some("Whisker-15")] {
        let dir = tempfile::tempdir().unwrap();
        let manifest_path = write_json(dir.path(), "manifest.json", &manifest());
        let tuples_path = write_json(dir.path(), "tuples.json", &tuples_envelope());
        let reviews_path = write_json(dir.path(), "reviews.json", &reviews());

        let home = tempfile::tempdir().unwrap();
        let out = cli_in_home(
            home.path(),
            &[
                "--json",
                "bbs",
                "report",
                "--manifest",
                manifest_path.to_str().unwrap(),
                "--tuples",
                tuples_path.to_str().unwrap(),
                "--reviews",
                reviews_path.to_str().unwrap(),
            ],
            agent,
        );
        assert!(
            out.status.success(),
            "agent={agent:?} stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let report: Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(report["eligible"], 1);

        let leaked: Vec<String> = std::fs::read_dir(home.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert!(
            leaked.is_empty(),
            "agent={agent:?} left daemon state in RK_HOME: {leaked:?}"
        );
    }
}
