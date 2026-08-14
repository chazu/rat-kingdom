//! Acceptance coverage for the Rust-native Factory Foreman dashboard.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::{process::Command, time::Duration};

fn run_with_layout(layout: &Layout, args: &[&str]) -> std::process::Output {
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

async fn stop_if_running(layout: &Layout) -> bool {
    for _ in 0..100 {
        if let Ok(mut client) = Client::connect_as_operator(layout).await {
            client.call("stop", json!({})).await.unwrap();
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dashboard_auto_starts_daemon_and_renders_native_factory_state() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    assert!(Client::connect_as_operator(&layout).await.is_err());

    let output = run_with_layout(
        &layout,
        &[
            "factory",
            "dashboard",
            "--repo",
            "rat-kingdom",
            "--event-limit",
            "5",
        ],
    );
    let daemon_started = stop_if_running(&layout).await;

    assert!(
        output.status.success(),
        "stderr: {}\nstdout: {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(daemon_started, "dashboard must auto-start the daemon");

    let text = String::from_utf8_lossy(&output.stdout);
    for heading in [
        "# Factory Dashboard",
        "## Approvals",
        "## Workflow Runs",
        "## Agents",
        "## Tickets",
        "## Inbox",
        "## Budget",
        "## Recent Events",
    ] {
        assert!(text.contains(heading), "missing {heading}:\n{text}");
    }
    assert!(text.contains("Repository: `rat-kingdom`"), "{text}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dashboard_includes_the_native_operator_inbox() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());

    let mut client = loop {
        if let Ok(client) = Client::connect_as_operator(&layout).await {
            break client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    client
        .call(
            "space.out",
            json!({
                "category": "need",
                "scope": "rat-kingdom",
                "identity": "human-review",
                "payload": {"text": "Review the blocked factory run"}
            }),
        )
        .await
        .unwrap();

    let output = run_with_layout(
        &layout,
        &["factory", "dashboard", "--repo", "rat-kingdom"],
    );
    assert!(
        output.status.success(),
        "stderr: {}\nstdout: {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("human-review"), "{text}");
    assert!(text.contains("Review the blocked factory run"), "{text}");

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dashboard_resolves_repository_paths_for_the_native_inbox() {
    let home = tempfile::tempdir().unwrap();
    let repository = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());

    let mut client = loop {
        if let Ok(client) = Client::connect_as_operator(&layout).await {
            break client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    client
        .call(
            "repo.add",
            json!({"name": "rat-kingdom", "path": repository.path()}),
        )
        .await
        .unwrap();
    client
        .call(
            "space.out",
            json!({
                "category": "need",
                "scope": "rat-kingdom",
                "identity": "path-scoped-review",
                "payload": {"text": "Resolve the registered repository path"}
            }),
        )
        .await
        .unwrap();

    let repository_path = repository.path().to_str().unwrap();
    let output = run_with_layout(
        &layout,
        &["factory", "dashboard", "--repo", repository_path],
    );
    assert!(
        output.status.success(),
        "stderr: {}\nstdout: {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("path-scoped-review"), "{text}");
    assert!(
        text.contains("Resolve the registered repository path"),
        "{text}"
    );

    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}
