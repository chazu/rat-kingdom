#[cfg(test)]
mod product_to_code_ticket_graph {
    use rk_core::paths::Layout;
    use rk_daemon::{Client, Daemon};
    use serde_json::Value;
    use std::{
        fs,
        path::Path,
        process::{Command, Output},
        time::Duration,
    };

    fn fixture(name: &str) -> String {
        format!(
            "{}/tests/fixtures/product_to_code/{name}",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    fn git(root: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .env("LC_ALL", "C")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn repository() -> tempfile::TempDir {
        let dir = tempfile::Builder::new()
            .prefix("rk-ticket-graph-cli")
            .tempdir()
            .unwrap();
        git(dir.path(), &["init", "-b", "main"]);
        git(dir.path(), &["config", "user.email", "test@example.com"]);
        git(dir.path(), &["config", "user.name", "Test"]);
        fs::write(dir.path().join("README.md"), "# Fixture\n").unwrap();
        git(dir.path(), &["add", "."]);
        git(dir.path(), &["commit", "-m", "initial"]);
        dir
    }

    async fn daemon_fixture() -> (
        tempfile::TempDir,
        tempfile::TempDir,
        tokio::task::JoinHandle<rk_core::Result<()>>,
    ) {
        let home = tempfile::tempdir().unwrap();
        let repo = repository();
        let layout = Layout::at(home.path());
        let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        let handle = tokio::spawn(daemon.run());
        let mut client = connect(&layout).await;
        client
            .call(
                "repo.add",
                serde_json::json!({"name": "fixture", "path": repo.path().to_string_lossy()}),
            )
            .await
            .unwrap();
        (home, repo, handle)
    }

    async fn connect(layout: &Layout) -> Client {
        for _ in 0..100 {
            if let Ok(client) = Client::connect_as_operator(layout).await {
                return client;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("daemon did not start");
    }

    fn run(args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_rk"))
            .args(args)
            .env_remove("RK_AGENT")
            .env_remove("RK_AUTH_TOKEN")
            .output()
            .unwrap()
    }

    fn run_with_layout(layout: &Layout, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_rk"))
            .args(args)
            .env("RK_HOME", layout.home())
            .env_remove("RK_AGENT")
            .env_remove("RK_AUTH_TOKEN")
            .env_remove("RK_TASK")
            .env_remove("RK_REPO")
            .env_remove("RK_ROLE")
            .env_remove("RK_BRANCH")
            .env_remove("RK_WORKTREE")
            .output()
            .unwrap()
    }

    fn json_success(output: Output) -> Value {
        assert!(
            output.status.success(),
            "stderr: {}\nstdout: {}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    fn json_failure(output: Output) -> Value {
        assert!(
            !output.status.success(),
            "stdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    #[test]
    fn test_graph_validate_accepts_acyclic_graph_with_existing_acceptance_refs() {
        let value = json_success(run(&[
            "--json",
            "product-to-code",
            "graph",
            "validate",
            "--graph",
            &fixture("ticket_graph_valid.json"),
            "--initiative",
            &fixture("initiative_minimal.json"),
        ]));

        assert_eq!(value["valid"], true);
        assert_eq!(value["graph_id"], "GRAPH-product-to-code");
        assert_eq!(value["errors"], serde_json::json!([]));
        assert_eq!(value["warnings"], serde_json::json!([]));
        assert_eq!(
            value["topological_order"],
            serde_json::json!(["NODE-contracts", "NODE-tests"])
        );
    }

    #[test]
    fn test_graph_validate_rejects_missing_dependency_node() {
        let graph_path = std::env::temp_dir().join(format!(
            "rk-ticket-graph-missing-node-{}.json",
            std::process::id()
        ));
        fs::write(&graph_path, r#"{
      "id":"GRAPH-invalid",
      "initiative_id":"INIT-product-to-code",
      "nodes":[
        {"id":"NODE-contracts","title":"Contracts","description":"Contracts","acceptance_criterion_ids":["AC-1"]},
        {"id":"NODE-tests","title":"Tests","description":"Tests","acceptance_criterion_ids":["AC-2"]}
      ],
      "edges":[{"from":"NODE-contracts","to":"NODE-missing","relationship":"blocks"}]
    }"#).unwrap();

        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "graph",
            "validate",
            "--graph",
            graph_path.to_str().unwrap(),
            "--initiative",
            &fixture("initiative_minimal.json"),
        ]));
        let errors = value["errors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            errors.contains("edge to references unknown node NODE-missing"),
            "{errors}"
        );
    }

    #[test]
    fn test_graph_validate_rejects_dependency_cycle() {
        let graph_path =
            std::env::temp_dir().join(format!("rk-ticket-graph-cycle-{}.json", std::process::id()));
        fs::write(
            &graph_path,
            r#"{
      "id":"GRAPH-invalid",
      "initiative_id":"INIT-product-to-code",
      "nodes":[
        {"id":"NODE-a","title":"A","description":"A","acceptance_criterion_ids":["AC-1"]},
        {"id":"NODE-b","title":"B","description":"B","acceptance_criterion_ids":["AC-2"]}
      ],
      "edges":[
        {"from":"NODE-a","to":"NODE-b","relationship":"blocks"},
        {"from":"NODE-b","to":"NODE-a","relationship":"blocks"}
      ]
    }"#,
        )
        .unwrap();

        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "graph",
            "validate",
            "--graph",
            graph_path.to_str().unwrap(),
            "--initiative",
            &fixture("initiative_minimal.json"),
        ]));
        let errors = value["errors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            errors.contains("cycle path NODE-a -> NODE-b -> NODE-a"),
            "{errors}"
        );
        assert_eq!(
            value["cycle_path"],
            serde_json::json!(["NODE-a", "NODE-b", "NODE-a"])
        );
    }

    #[test]
    fn test_graph_validate_rejects_unknown_acceptance_criterion_ref() {
        let graph_path = std::env::temp_dir().join(format!(
            "rk-ticket-graph-unknown-criterion-{}.json",
            std::process::id()
        ));
        fs::write(&graph_path, r#"{
      "id":"GRAPH-invalid",
      "initiative_id":"INIT-product-to-code",
      "nodes":[
        {"id":"NODE-contracts","title":"Contracts","description":"Contracts","acceptance_criterion_ids":["AC-1", "AC-404"]},
        {"id":"NODE-tests","title":"Tests","description":"Tests","acceptance_criterion_ids":["AC-2"]}
      ],
      "edges":[{"from":"NODE-contracts","to":"NODE-tests","relationship":"blocks"}]
    }"#).unwrap();

        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "graph",
            "validate",
            "--graph",
            graph_path.to_str().unwrap(),
            "--initiative",
            &fixture("initiative_minimal.json"),
        ]));
        let errors = value["errors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            errors.contains("unknown acceptance criterion AC-404"),
            "{errors}"
        );
    }

    #[test]
    fn test_graph_dry_run_is_read_only_and_lists_exact_mutations() {
        let temp_state = tempfile::tempdir().unwrap();
        let before = fs::read_dir(temp_state.path()).unwrap().count();
        let value = json_success(run(&[
            "--json",
            "product-to-code",
            "graph",
            "dry-run",
            "--graph",
            &fixture("ticket_graph_valid.json"),
            "--initiative",
            &fixture("initiative_minimal.json"),
        ]));
        let after = fs::read_dir(temp_state.path()).unwrap().count();

        assert_eq!(before, after);
        assert_eq!(value["graph_id"], "GRAPH-product-to-code");
        assert_eq!(value["creates"].as_array().unwrap().len(), 2);
        assert_eq!(value["updates"], serde_json::json!([]));
        assert_eq!(value["dependencies"].as_array().unwrap().len(), 1);
        assert_eq!(value["dispatches"], serde_json::json!([]));
        assert_eq!(value["blocked"], serde_json::json!([]));
        assert_eq!(
            value["creates"][0]["stable_graph_node_id"],
            "NODE-contracts"
        );
        assert_eq!(
            value["dependencies"][0]["dependency_graph_node_id"],
            "NODE-contracts"
        );
        assert_eq!(
            value["dependencies"][0]["blocked_graph_node_id"],
            "NODE-tests"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_graph_propose_apply_uses_phase2_daemon_canonical_proposal() {
        let (home, _repo, handle) = daemon_fixture().await;
        let layout = Layout::at(home.path());

        let value = json_success(run_with_layout(
            &layout,
            &[
                "--json",
                "product-to-code",
                "graph",
                "propose-apply",
                "--graph",
                &fixture("ticket_graph_valid.json"),
                "--initiative",
                &fixture("initiative_minimal.json"),
                "--repo",
                "fixture",
            ],
        ));

        assert_eq!(value["kind"], "ticket_graph.apply");
        assert_eq!(value["submitted_to_daemon"], true);
        assert_eq!(value["proposal_id"], value["digest"]);
        assert_eq!(value["proposal_id"].as_str().unwrap().len(), 64);
        assert_eq!(value["canonical_action"]["kind"], "ticket_graph.apply");
        assert_eq!(
            value["canonical_action"]["graph"]["nodes"][0]["id"],
            "NODE-contracts"
        );
        assert!(value["canonical_action"].get("topological_order").is_none());
        assert!(value["canonical_action"].get("mutations").is_none());
        assert_eq!(
            value["canonical_action"]["apply_plan"]["topological_order"],
            serde_json::json!(["NODE-contracts", "NODE-tests"])
        );
        assert_eq!(
            value["canonical_action"]["apply_plan"]["creates"][0]["stable_graph_node_id"],
            "NODE-contracts"
        );
        assert!(value["approval_instructions"]
            .as_str()
            .unwrap()
            .contains("rk factory approve"));
        assert!(value["approval_instructions"]
            .as_str()
            .unwrap()
            .contains(value["proposal_id"].as_str().unwrap()));
        assert!(value["authority_boundary"]
            .as_str()
            .unwrap()
            .contains("factory.propose_action"));
        assert!(value["authority_boundary"]
            .as_str()
            .unwrap()
            .contains("local CLI did not apply"));

        handle.abort();
    }
}
