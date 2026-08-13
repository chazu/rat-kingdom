//! Phase 5 CLI acceptance for the read-only factory self-optimization commands
//! `rk --json factory scorecards` and `rk --json factory recommend`.
//!
//! Drives the real `rk` binary against an in-memory daemon. Asserts the global
//! `--json` envelope, the Markdown renderer sections, unobserved source
//! families (never zero failures), and that mutating flags are rejected. No
//! command here approves, dispatches, or mutates any state.

#[cfg(test)]
mod factory_analytics_cli {
    use rk_core::paths::Layout;
    use rk_daemon::{Client, Daemon};
    use serde_json::{json, Value};
    use std::{
        process::{Command, Output},
        time::Duration,
    };

    fn run_with_layout(layout: &Layout, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_rk"))
            .args(args)
            .env("RK_HOME", layout.home())
            .env_remove("RK_AGENT")
            .env_remove("RK_AUTH_TOKEN")
            .env_remove("RK_TASK")
            .env_remove("RK_REPO")
            .env_remove("RK_ROLE")
            .output()
            .unwrap()
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

    async fn daemon() -> (
        tempfile::TempDir,
        Layout,
        tokio::task::JoinHandle<rk_core::Result<()>>,
    ) {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let d = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        let handle = tokio::spawn(d.run());
        let _ = connect(&layout).await;
        (home, layout, handle)
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scorecards_json_uses_global_flag_and_read_only_envelope() {
        let (_home, layout, handle) = daemon().await;
        let result = json_success(run_with_layout(
            &layout,
            &["--json", "factory", "scorecards", "--repo", "rat-kingdom"],
        ));
        assert_eq!(result["schema_version"], json!(1));
        assert_eq!(result["repo"], json!("rat-kingdom"));
        assert!(result["scorecards"].is_array());
        assert!(result["availability"].is_array());
        let mut client = connect(&layout).await;
        client.call("stop", json!({})).await.unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recommend_markdown_has_recommendations_and_suppressed_sections() {
        let (_home, layout, handle) = daemon().await;
        let output = run_with_layout(&layout, &["factory", "recommend", "--repo", "rat-kingdom"]);
        assert!(output.status.success());
        let text = String::from_utf8_lossy(&output.stdout);
        assert!(
            text.contains("# Factory Scorecards"),
            "missing title: {text}"
        );
        assert!(text.contains("## Source Counts"));
        assert!(text.contains("## Recommendations"));
        assert!(text.contains("## Suppressed"));
        let active = text
            .split("## Recommendations")
            .nth(1)
            .unwrap()
            .split("## Suppressed")
            .next()
            .unwrap();
        let suppressed = text.split("## Suppressed").nth(1).unwrap();
        assert!(
            active.contains("(no advisory recommendations)"),
            "suppressed/empty-advice records must not render active recommendations:\n{text}"
        );
        assert!(
            suppressed.contains("(none)"),
            "daemon wire schema with no suppressions must render no suppressed records:\n{text}"
        );
        // Advisory language, no mutation controls.
        for banned in ["--apply", "dispatch", "rewrite-policy", "update-workflow"] {
            assert!(!text.contains(banned), "markdown must not contain {banned}");
        }
        let mut client = connect(&layout).await;
        client.call("stop", json!({})).await.unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scorecards_markdown_shows_unobserved_families_not_zero() {
        let (_home, layout, handle) = daemon().await;
        let output = run_with_layout(&layout, &["factory", "scorecards", "--repo", "rat-kingdom"]);
        assert!(output.status.success());
        let text = String::from_utf8_lossy(&output.stdout);
        // Unavailable Phase 3/4 families render as available=false, and a
        // warning names them, so they never look like zero failures.
        assert!(
            text.contains("Phase3VerifiedDelivery: active=0 archived=0")
                && text.contains("Phase3VerifiedDelivery has no structured RK store")
                && text.contains("available=false"),
            "unexpected markdown:\n{text}"
        );
        assert!(text.contains("## Warnings"));
        assert!(text.contains("Phase4CiSignal"));
        let mut client = connect(&layout).await;
        client.call("stop", json!({})).await.unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn factory_commands_reject_mutating_flags() {
        let (_home, layout, handle) = daemon().await;
        for flag in ["--apply", "--dispatch", "--rewrite-policy", "--approve"] {
            let output = run_with_layout(
                &layout,
                &["factory", "scorecards", "--repo", "rat-kingdom", flag],
            );
            assert!(
                !output.status.success(),
                "mutating flag {flag} must be rejected by arg parsing"
            );
        }

        let invalid_group = run_with_layout(
            &layout,
            &[
                "factory",
                "scorecards",
                "--repo",
                "rat-kingdom",
                "--group-by",
                "nonsense",
            ],
        );
        assert!(!invalid_group.status.success());

        let missing_repo = run_with_layout(&layout, &["factory", "scorecards"]);
        assert!(!missing_repo.status.success());

        let mut client = connect(&layout).await;
        client.call("stop", json!({})).await.unwrap();
        handle.await.unwrap().unwrap();
    }
}
