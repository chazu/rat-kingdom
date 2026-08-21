#[cfg(test)]
mod product_to_code_workflow {
    use rk_core::paths::Layout;
    use rk_daemon::{Client, Daemon};
    use serde_json::{json, Value};
    use std::{
        fs,
        path::Path,
        process::{Command, Output},
        time::Duration,
    };

    // Resolved at runtime, not baked in via `env!("CARGO_MANIFEST_DIR")`: a
    // shared CARGO_TARGET_DIR can serve this binary unrecompiled to a
    // worktree other than the one it was compiled in
    // (TKT-01M0F0GHDPGA24X1TB24A0PZD0). Cargo sets a test binary's cwd to its
    // package's manifest directory on every run, so `current_dir()` is
    // always correct for the current process.
    fn crate_root() -> std::path::PathBuf {
        std::env::current_dir().expect("test process must have a current directory")
    }

    fn workspace_root() -> std::path::PathBuf {
        crate_root()
            .ancestors()
            .find(|dir| dir.join("Cargo.toml").is_file() && dir.join("crates").is_dir())
            .map(Path::to_path_buf)
            .expect("could not find the rat-kingdom workspace above the test's runtime directory")
    }

    fn fixture(name: &str) -> String {
        format!(
            "{}/tests/fixtures/product_to_code/{name}",
            crate_root().display()
        )
    }

    fn workspace_file(relative: &str) -> std::path::PathBuf {
        workspace_root().join(relative)
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
            .prefix("rk-p2c-workflow-cli")
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

    async fn connect(layout: &Layout) -> Client {
        for _ in 0..100 {
            if let Ok(client) = Client::connect_as_operator(layout).await {
                return client;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("daemon did not start");
    }

    fn read_fixture_json(name: &str) -> Value {
        serde_json::from_str(&fs::read_to_string(fixture(name)).unwrap()).unwrap()
    }

    /// Spawn an in-memory daemon, register the fixture repo, and drive the
    /// canonical Phase 2 ticket_graph.apply propose -> approve -> execute path
    /// so a graph-node-id -> TKT-id mapping exists for dispatch proposals.
    async fn daemon_with_applied_graph() -> (
        tempfile::TempDir,
        tempfile::TempDir,
        tokio::task::JoinHandle<rk_core::Result<()>>,
        Value,
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
                json!({"name": "fixture", "path": repo.path().to_string_lossy()}),
            )
            .await
            .unwrap();
        let action = json!({
            "repo": "fixture",
            "graph": read_fixture_json("ticket_graph_valid.json"),
            "initiative": read_fixture_json("initiative_minimal.json"),
        });
        let proposed = client
            .call(
                "factory.propose_action",
                json!({"kind": "ticket_graph.apply", "action": action}),
            )
            .await
            .unwrap();
        client
            .call(
                "factory.approve_action",
                json!({
                    "proposal_id": proposed["proposal"]["id"],
                    "digest": proposed["proposal"]["digest"],
                }),
            )
            .await
            .unwrap();
        let executed = client
            .call(
                "factory.execute_action",
                json!({
                    "proposal_id": proposed["proposal"]["id"],
                    "digest": proposed["proposal"]["digest"],
                    "kind": "ticket_graph.apply",
                    "action": action,
                }),
            )
            .await
            .unwrap();
        let mapping = executed["result"]["graph_node_to_ticket_id"].clone();
        (home, repo, handle, mapping)
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

    fn errors_joined(value: &Value) -> String {
        value["errors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|error| error.as_str().unwrap())
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn test_workflow_propose_validates_research_before_graph_apply() {
        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "workflow",
            "propose",
            "--initiative",
            &fixture("initiative_minimal.json"),
            "--research",
            &fixture("architecture_research_no_substance.json"),
            "--graph",
            &fixture("ticket_graph_valid.json"),
            "--evidence-dir",
            &fixture("evidence_dispatch_partial"),
        ]));

        assert_eq!(value["stage"], "research");
        assert_eq!(value["submitted_to_daemon"], false);
        let errors = errors_joined(&value);
        assert!(
            errors.contains("architecture_decisions must contain at least one"),
            "{errors}"
        );
    }

    #[test]
    fn test_workflow_propose_rejects_graph_with_cycle_before_dispatch() {
        let graph_path =
            std::env::temp_dir().join(format!("rk-p2c-workflow-cycle-{}.json", std::process::id()));
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
            "workflow",
            "propose",
            "--initiative",
            &fixture("initiative_minimal.json"),
            "--research",
            &fixture("architecture_research_valid.json"),
            "--graph",
            graph_path.to_str().unwrap(),
            "--evidence-dir",
            &fixture("evidence_dispatch_partial"),
        ]));

        assert_eq!(value["stage"], "graph");
        assert_eq!(value["submitted_to_daemon"], false);
        let errors = errors_joined(&value);
        assert!(errors.contains("cycle path"), "{errors}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_workflow_propose_blocks_nodes_without_impact_evidence() {
        let (home, _repo, handle, _mapping) = daemon_with_applied_graph().await;
        let layout = Layout::at(home.path());

        let value = json_success(run_with_layout(
            &layout,
            &[
                "--json",
                "product-to-code",
                "workflow",
                "propose",
                "--initiative",
                &fixture("initiative_minimal.json"),
                "--research",
                &fixture("architecture_research_valid.json"),
                "--graph",
                &fixture("ticket_graph_valid.json"),
                "--evidence-dir",
                &fixture("evidence_dispatch_partial"),
                "--repo",
                "fixture",
            ],
        ));

        let blocked = value["blocked"].as_array().unwrap();
        assert_eq!(blocked.len(), 1, "{value}");
        assert_eq!(blocked[0]["graph_node_id"], "NODE-tests");
        assert!(blocked[0]["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason.as_str().unwrap().contains("impact evidence")));
        let dispatches = value["dispatches"].as_array().unwrap();
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0]["graph_node_id"], "NODE-contracts");
        assert!(!blocked[0]["graph_node_id"]
            .as_str()
            .unwrap()
            .starts_with("TKT-"));

        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_workflow_propose_includes_implement_featureset_dispatches_for_unblocked_nodes() {
        let (home, _repo, handle, mapping) = daemon_with_applied_graph().await;
        let layout = Layout::at(home.path());

        let value = json_success(run_with_layout(
            &layout,
            &[
                "--json",
                "product-to-code",
                "workflow",
                "propose",
                "--initiative",
                &fixture("initiative_minimal.json"),
                "--research",
                &fixture("architecture_research_valid.json"),
                "--graph",
                &fixture("ticket_graph_valid.json"),
                "--evidence-dir",
                &fixture("evidence_dispatch_partial"),
                "--repo",
                "fixture",
            ],
        ));

        assert_eq!(value["kind"], "product_to_code.dispatch");
        assert_eq!(value["submitted_to_daemon"], true);
        assert_eq!(value["proposal_id"], value["digest"]);
        assert_eq!(value["proposal_id"].as_str().unwrap().len(), 64);
        assert_eq!(
            value["canonical_action"]["kind"],
            "product_to_code.dispatch"
        );

        let dispatches = value["canonical_action"]["dispatches"].as_array().unwrap();
        assert_eq!(dispatches.len(), 1);
        let dispatch = &dispatches[0];
        assert_eq!(dispatch["workflow"], "implement-featureset");
        assert_eq!(dispatch["graph_node_id"], "NODE-contracts");
        let expected_ticket = mapping["NODE-contracts"].as_str().unwrap();
        assert!(expected_ticket.starts_with("TKT-"));
        assert_eq!(dispatch["ticket_id"], expected_ticket);
        assert_eq!(dispatch["params"]["taskId"], expected_ticket);
        assert!(!dispatch["params"]["taskDescription"]
            .as_str()
            .unwrap()
            .is_empty());
        assert!(value["approval_instructions"]
            .as_str()
            .unwrap()
            .contains("rk factory approve --proposal-file"));
        assert!(value["approval_instructions"]
            .as_str()
            .unwrap()
            .contains("rk factory execute-action --proposal-file"));
        assert!(value["authority_boundary"]
            .as_str()
            .unwrap()
            .contains("local CLI did not dispatch"));

        handle.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_workflow_propose_rejects_consumed_apply_for_different_graph_revision() {
        let (home, _repo, handle, _mapping) = daemon_with_applied_graph().await;
        let layout = Layout::at(home.path());
        let mut graph = read_fixture_json("ticket_graph_valid.json");
        graph["nodes"][0]["description"] = json!("Changed after the graph apply");
        let graph_path = home.path().join("changed-graph.json");
        fs::write(&graph_path, serde_json::to_vec_pretty(&graph).unwrap()).unwrap();

        let value = json_failure(run_with_layout(
            &layout,
            &[
                "--json",
                "product-to-code",
                "workflow",
                "propose",
                "--initiative",
                &fixture("initiative_minimal.json"),
                "--research",
                &fixture("architecture_research_valid.json"),
                "--graph",
                graph_path.to_str().unwrap(),
                "--evidence-dir",
                &fixture("evidence_dispatch_partial"),
                "--repo",
                "fixture",
            ],
        ));

        assert_eq!(value["stage"], "graph_apply");
        assert!(errors_joined(&value).contains("exact graph revision"));
        handle.abort();
    }

    #[test]
    fn test_product_to_code_workflow_definition_lists_research_graph_apply_implement_and_verify_steps(
    ) {
        let definition =
            fs::read_to_string(workspace_file("examples/workflows/product-to-code.cue")).unwrap();

        // Composition order: initiative + research validation, graph validation,
        // approved graph apply proposal, implement-featureset dispatch for
        // unblocked nodes, delivery blocked until independent verification.
        assert!(definition.contains("research validate"), "{definition}");
        assert!(definition.contains("graph validate"), "{definition}");
        assert!(definition.contains("graph propose-apply"), "{definition}");
        assert!(definition.contains("workflow propose"), "{definition}");
        assert!(definition.contains("implement-featureset"), "{definition}");
        assert!(definition.contains("independent-verifier"), "{definition}");
        let research = definition.find("research validate").unwrap();
        let graph_validate = definition.find("graph validate").unwrap();
        let graph_apply = definition.find("graph propose-apply").unwrap();
        let dispatch = definition.find("workflow propose").unwrap();
        let verify = definition.find("independent-verifier").unwrap();
        assert!(research < graph_validate);
        assert!(graph_validate < graph_apply);
        assert!(graph_apply < dispatch);
        assert!(dispatch < verify);
    }

    #[test]
    fn test_cli_workflow_propose_is_thin_wiring_only() {
        let source = fs::read_to_string(crate_root().join("src/product_to_code_cmds.rs")).unwrap();

        // The CLI emits and submits the typed proposal via the Phase 2 daemon
        // path and owns no mutation, approval, or dispatch shortcut.
        assert!(source.contains("factory.propose_action"));
        assert!(source.contains("product_to_code.dispatch"));
        assert!(!source.contains("approved_id"));
        assert!(!source.contains("--approved-id"));
        assert!(!source.contains("factory.execute_action"));
        assert!(!source.contains("factory.approve_action"));
        assert!(!source.contains("\"workflow.run\""));
        assert!(!source.contains("ticket.new"));
    }
}
