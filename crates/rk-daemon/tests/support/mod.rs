//! Shared support for daemon-spinning integration tests.
//!
//! Lives in a subdirectory so cargo does not compile it as a test target of its
//! own; each test binary picks it up with `mod support;`.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use std::time::Duration;

/// Install the stack-neutral named checks every gated landing test repo must
/// explicitly own. The commands are intentionally trivial: individual tests
/// exercise workflow behavior, while this registry proves the landing queue
/// resolved policy by name instead of inventing a raw command fallback.
#[allow(dead_code)]
pub fn install_passing_landing_checks(repo: &std::path::Path) {
    let rk_dir = repo.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(
        rk_dir.join("checks.cue"),
        r#"checks: [
    {name: "steward-protected-paths", command: "true", timeout: "30s"},
    {name: "steward-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "true", timeout: "30s"},
]
"#,
    )
    .unwrap();
    let run = |args: &[&str]| {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(repo)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    };
    run(&["add", ".rk/checks.cue"]);
    run(&["commit", "-m", "test: register landing checks"]);
}

/// Poll for a daemon at `layout` to come up and connect as operator.
///
/// Was previously copy-pasted into ~40 test files with varying retry budgets
/// (50/100/200 iterations at 20ms — a hardcoded ~1s to ~4s). Several files had
/// already widened their local copy past the original ~1s under
/// parallel-test-process load, so this shared version standardizes on the
/// most generous of the budgets already proven necessary rather than the
/// tightest.
#[allow(dead_code)]
pub async fn connect(layout: &Layout) -> Client {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

/// Start a daemon against `layout` and connect to it, retrying the whole
/// start (not just the reconnect) on a transient loss of the singleton
/// lock — reproduced under parallel `cargo test` load, where this same
/// process can be running several other tests' daemons concurrently and one
/// of them can still be a few OS scheduler ticks from fully releasing its
/// `flock` when this one tries to bind. A plain reconnect loop can never
/// recover from that: once `Daemon::run()` loses the race for the lock it
/// returns immediately without ever listening, so nothing will ever answer
/// the socket no matter how long `connect` polls it.
#[allow(dead_code)]
pub async fn start_daemon(layout: &Layout) -> Client {
    for _ in 0..20 {
        let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        let handle = tokio::spawn(daemon.run());
        // A daemon that wins the bind runs its accept loop forever, so this
        // handle deliberately never resolves in the success case — the
        // timeout is just a generous grace window to catch the failure case,
        // which in every observed instance resolves in well under 50ms.
        match tokio::time::timeout(Duration::from_millis(200), handle).await {
            Err(_) => return connect(layout).await, // still running: bind succeeded
            Ok(_) => tokio::time::sleep(Duration::from_millis(50)).await, // fast exit: retry
        }
    }
    panic!("daemon repeatedly lost the singleton-lock race against {layout:?}");
}
