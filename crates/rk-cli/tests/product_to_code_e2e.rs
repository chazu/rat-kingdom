//! End-to-end acceptance for the product-to-code lifecycle.
//!
//! These tests drive the real `rk` binary across the offline lifecycle stages
//! (research validation, graph validation, graph dry-run, evidence gates, and
//! the independent verifier report) and the daemon-backed proposal stages
//! (approved `ticket_graph.apply`, then the typed `product_to_code.dispatch`
//! proposal). Nothing here approves or executes a mutation: the CLI only ever
//! emits or submits typed proposals, and every daemon-backed assertion checks
//! that no local approval or apply shortcut exists.

#[cfg(test)]
mod product_to_code_e2e {
    use rk_core::paths::Layout;
    use rk_daemon::{Client, Daemon};
    use serde_json::{json, Value};
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

    fn errors_text(value: &Value) -> String {
        value["errors"].to_string()
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
            .prefix("rk-p2c-e2e")
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
    /// canonical Phase 2 `ticket_graph.apply` propose -> approve -> execute path
    /// so a graph-node-id -> minted TKT-id mapping exists for dispatch
    /// proposals. Returns the home dir, repo dir, daemon handle, and mapping.
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

    // -- Offline lifecycle stages -----------------------------------------

    /// Research validates, the graph validates, the graph dry-run mints exactly
    /// the plan without mutating anything, and the daemon-backed graph apply and
    /// workflow dispatch produce typed proposals only.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_e2e_product_to_code_happy_path_produces_apply_and_dispatch_proposals() {
        // Stage 1: research validates offline.
        let research = json_success(run(&[
            "--json",
            "product-to-code",
            "research",
            "validate",
            "--artifact",
            &fixture("architecture_research_valid.json"),
            "--initiative",
            &fixture("initiative_minimal.json"),
        ]));
        assert_eq!(research["valid"], true);

        // Stage 2: the graph validates offline.
        let graph = json_success(run(&[
            "--json",
            "product-to-code",
            "graph",
            "validate",
            "--graph",
            &fixture("ticket_graph_valid.json"),
            "--initiative",
            &fixture("initiative_minimal.json"),
        ]));
        assert_eq!(graph["valid"], true);

        // Stage 3: the graph dry-run previews the mint plan without mutation.
        let dry = json_success(run(&[
            "--json",
            "product-to-code",
            "graph",
            "dry-run",
            "--graph",
            &fixture("ticket_graph_valid.json"),
            "--initiative",
            &fixture("initiative_minimal.json"),
            "--repo",
            "fixture",
        ]));
        assert_eq!(dry["daemon_connected"], false);
        assert_eq!(dry["creates"].as_array().unwrap().len(), 2);
        for create in dry["creates"].as_array().unwrap() {
            assert!(
                !create["stable_graph_node_id"]
                    .as_str()
                    .unwrap()
                    .starts_with("TKT-"),
                "dry-run must not mint TKT ids: {create}"
            );
        }

        // Stage 4/5: apply the graph through the canonical Phase 2 boundary, then
        // propose dispatch. The workflow propose command emits a typed
        // product_to_code.dispatch proposal only.
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
        let dispatches = value["canonical_action"]["dispatches"].as_array().unwrap();
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0]["graph_node_id"], "NODE-contracts");
        let minted = mapping["NODE-contracts"].as_str().unwrap();
        assert!(minted.starts_with("TKT-"));
        assert_eq!(dispatches[0]["ticket_id"], minted);
        // No local approve/apply shortcut is offered.
        assert!(value["approval_instructions"]
            .as_str()
            .unwrap()
            .contains("rk factory approve"));

        handle.abort();
    }

    /// A cyclic graph fails validation, so neither an apply nor a dispatch
    /// proposal is possible.
    #[test]
    fn test_e2e_rejects_cycle_before_any_proposal() {
        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "graph",
            "validate",
            "--graph",
            &fixture("e2e/graph_cycle.json"),
            "--initiative",
            &fixture("initiative_minimal.json"),
        ]));
        assert_eq!(value["valid"], false);
        assert!(errors_text(&value).contains("cycle path"), "{value}");

        // The apply proposal path (propose-apply) also refuses the cyclic graph.
        let apply = json_failure(run(&[
            "--json",
            "product-to-code",
            "graph",
            "propose-apply",
            "--graph",
            &fixture("e2e/graph_cycle.json"),
            "--initiative",
            &fixture("initiative_minimal.json"),
            "--repo",
            "fixture",
        ]));
        assert_eq!(apply["valid"], false);
        assert!(apply["submitted_to_daemon"].as_bool() != Some(true));
    }

    /// A graph edge referencing an unknown node fails validation before any
    /// proposal is generated.
    #[test]
    fn test_e2e_rejects_missing_dependency_before_any_proposal() {
        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "graph",
            "validate",
            "--graph",
            &fixture("e2e/graph_missing_dependency.json"),
            "--initiative",
            &fixture("initiative_minimal.json"),
        ]));
        assert_eq!(value["valid"], false);
        let errors = errors_text(&value);
        assert!(
            errors.contains("unknown node") || errors.contains("references unknown"),
            "{errors}"
        );
    }

    /// A graph node without generic impact evidence is listed in `blocked` and
    /// produces no implement dispatch for that node.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_e2e_blocks_dispatch_without_impact_evidence() {
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
        let blocked = value["canonical_action"]["blocked"].as_array().unwrap();
        assert_eq!(blocked.len(), 1, "{value}");
        assert_eq!(blocked[0]["graph_node_id"], "NODE-tests");
        assert!(!blocked[0]["graph_node_id"]
            .as_str()
            .unwrap()
            .starts_with("TKT-"));
        let dispatches = value["canonical_action"]["dispatches"].as_array().unwrap();
        assert!(dispatches
            .iter()
            .all(|dispatch| dispatch["graph_node_id"] != "NODE-tests"));

        handle.abort();
    }

    /// When the initiative declares browser acceptance applicable, the delivery
    /// gate fails without browser acceptance evidence.
    #[test]
    fn test_e2e_delivery_gate_requires_browser_acceptance_when_applicable() {
        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "delivery-gate",
            "--ticket",
            &fixture("ticket_browser_applicable.json"),
            "--verification-report",
            &fixture("verification_browser_missing.json"),
            "--evidence-dir",
            &fixture("evidence_test_review"),
        ]));
        assert_eq!(value["valid"], false);
        let errors = errors_text(&value);
        assert!(errors.contains("browser_acceptance"), "{errors}");
        assert!(errors.contains("AC-1"), "{errors}");
    }

    /// A valid independent verifier report maps every acceptance criterion to
    /// evidence or to an explicit gap.
    #[test]
    fn test_e2e_independent_report_maps_all_acceptance_criteria() {
        let value = json_success(run(&[
            "--json",
            "product-to-code",
            "verify-report",
            "validate",
            "--report",
            &fixture("verification_complete.json"),
            "--initiative",
            &fixture("initiative_verification.json"),
            "--evidence-dir",
            &fixture("evidence_verification"),
        ]));
        assert_eq!(value["valid"], true);
        // Every initiative criterion is accounted for by the report.
        let initiative = read_fixture_json("initiative_verification.json");
        for criterion in initiative["acceptance_criteria"].as_array().unwrap() {
            let id = criterion["id"].as_str().unwrap();
            assert!(
                value.to_string().contains(id),
                "criterion {id} missing from report validation output: {value}"
            );
        }
    }
}
