//! Native CLI/RPC acceptance for the `bbs-discovery-ranking` setting (P8/P11
//! first slice; docs/2026-09-14-bbs-discovery-ranking-setting.md). The unit
//! tests in `rk-daemon/src/bbs.rs` and `rk-daemon/src/bbs_discovery.rs` prove
//! the ranking/registry logic in isolation; this file proves the real RPC
//! surface a `rk bbs discovery` operator command and a `bbs.brief` caller
//! actually see, including persistence across a daemon restart. Title and
//! post bodies mirror the finite public CLI probe
//! (p11-public-discovery-probe-v1.py, candidate evidence
//! 01M2GZDA9YRS8E9AFYRSG99QQS) so the same real behavior is exercised both
//! here and independently by that script.

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::time::Duration;
use support::connect;

async fn add_repo(client: &mut Client, name: &str, path: &std::path::Path) {
    client
        .call(
            "repo.add",
            json!({"name": name, "path": path.to_string_lossy()}),
        )
        .await
        .unwrap();
}

/// A minimal resolvable ticket: `bbs::brief` only needs a `Category::Task`
/// tuple with the right identity and a `title`, not the full `ticket.new`
/// validation path.
async fn make_task(client: &mut Client, repo: &str, id: &str, title: &str) {
    client
        .call(
            "space.out",
            json!({
                "category": "task",
                "scope": repo,
                "identity": id,
                "payload": {"title": title, "status": "open"},
            }),
        )
        .await
        .unwrap();
}

/// Returns the tuple's actual generated id (`space.out`'s `"id"` field) —
/// NOT the `identity` slug passed in, which `bbs.brief` entries never carry.
async fn make_post(client: &mut Client, repo: &str, identity: &str, summary: &str) -> String {
    let result = client
        .call(
            "space.out",
            json!({
                "category": "artifact",
                "scope": repo,
                "identity": identity,
                "payload": {"summary": summary},
            }),
        )
        .await
        .unwrap();
    result["id"].as_str().unwrap().to_string()
}

async fn brief(client: &mut Client, repo: &str, task: &str) -> Value {
    client
        .call("bbs.brief", json!({"repo": repo, "task": task}))
        .await
        .unwrap()
}

fn entry_ids(briefing: &Value) -> Vec<String> {
    briefing["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap().to_string())
        .collect()
}

async fn wait_socket_gone(layout: &Layout) {
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !layout.socket_path().exists() {
            return;
        }
    }
    panic!("daemon did not release its socket on shutdown");
}

/// Off parity, an enabled target repo vs. an unchanged second repo, a
/// relevant older finding surviving alongside newer generic-word noise,
/// disable returning to baseline, malformed scope/mode rejection, and
/// restart behavior — the required journey's finite native test list, all
/// driven through the real daemon RPC surface.
#[tokio::test]
async fn native_discovery_setting_journey() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    let repo_a = tempfile::tempdir().unwrap();
    let repo_b = tempfile::tempdir().unwrap();

    let daemon_a = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle_a = tokio::spawn(daemon_a.run());
    let mut client = connect(&layout).await;

    add_repo(&mut client, "alpha", repo_a.path()).await;
    add_repo(&mut client, "beta", repo_b.path()).await;

    let title = "Reconcile verification budget after report";
    let mut noise = std::collections::HashMap::new();
    let mut old_finding = std::collections::HashMap::new();
    for repo in ["alpha", "beta"] {
        make_task(&mut client, repo, &format!("TKT-{repo}-fixture"), title).await;
        // Generic-word-only noise: shares only "report"/"source", words that
        // remain on the reviewed exclusion list.
        let id = make_post(
            &mut client,
            repo,
            "noise",
            "Landing report during a different source review",
        )
        .await;
        noise.insert(repo, id);
        // A relevant OLDER finding sharing only "verification" — a
        // load-bearing domain word deliberately NOT excluded, so noise
        // reduction must not erase it either.
        let id = make_post(
            &mut client,
            repo,
            "old-finding",
            "verification concurrency must remain bounded to avoid resource starvation",
        )
        .await;
        old_finding.insert(repo, id);
    }
    // Newer boilerplate noise in "alpha" only, minted after the old finding
    // above, sharing only generic words ("current"/"report"/"after").
    let newer_noise = make_post(
        &mut client,
        "alpha",
        "newer-noise",
        "Current report after the review",
    )
    .await;

    // --- Absent configuration: nothing has ever been set. ---
    let show = client
        .call("bbs.discovery.show", json!({"repo": "alpha"}))
        .await
        .unwrap();
    assert_eq!(show["mode"], "off");
    assert_eq!(show["revision"], 0);
    assert!(show["updated_at"].is_null());

    // --- Off parity: both repos see identical baseline selection/variant. ---
    let alpha_before = brief(&mut client, "alpha", "TKT-alpha-fixture").await;
    let beta_before = brief(&mut client, "beta", "TKT-beta-fixture").await;
    assert_eq!(alpha_before["ranking_variant"], "baseline");
    assert_eq!(alpha_before["ranking_config_revision"], 0);
    assert_eq!(alpha_before["ranking_config_status"], "default_absent");
    for (repo, briefing) in [("alpha", &alpha_before), ("beta", &beta_before)] {
        let ids = entry_ids(briefing);
        assert!(ids.contains(&noise[repo]), "baseline {repo} selects noise");
        assert!(
            ids.contains(&old_finding[repo]),
            "baseline {repo} selects old-finding"
        );
    }
    assert!(entry_ids(&alpha_before).contains(&newer_noise));

    // --- Malformed scope: an unregistered repo is rejected. ---
    let err = client
        .call(
            "bbs.discovery.set",
            json!({"repo": "not-registered", "mode": "on"}),
        )
        .await;
    assert!(err.is_err(), "unregistered repo scope must be rejected");

    // --- Malformed mode: shadow/cohort/bogus are rejected explicitly. ---
    for mode in ["shadow", "cohort", "bogus"] {
        let err = client
            .call("bbs.discovery.set", json!({"repo": "alpha", "mode": mode}))
            .await;
        assert!(err.is_err(), "mode '{mode}' must be rejected");
    }

    // --- Enable "alpha" only. ---
    let set = client
        .call("bbs.discovery.set", json!({"repo": "alpha", "mode": "on"}))
        .await
        .unwrap();
    assert_eq!(set["mode"], "on");
    assert_eq!(set["revision"], 1);
    assert_eq!(set["rollover_required"], false);

    // --- Enabled target repo vs. unchanged second repo, no restart. ---
    let alpha_on = brief(&mut client, "alpha", "TKT-alpha-fixture").await;
    let beta_still_baseline = brief(&mut client, "beta", "TKT-beta-fixture").await;
    assert_eq!(alpha_on["ranking_variant"], "observed-generic-word-filter");
    assert_eq!(alpha_on["ranking_config_revision"], 1);
    assert_eq!(alpha_on["ranking_config_status"], "explicit");
    let alpha_on_ids = entry_ids(&alpha_on);
    assert!(
        !alpha_on_ids.contains(&noise["alpha"]),
        "older generic-word-only noise is dropped once alpha is on"
    );
    assert!(
        !alpha_on_ids.contains(&newer_noise),
        "newer boilerplate noise is dropped too — it is no more specific than the old noise"
    );
    assert!(
        alpha_on_ids.contains(&old_finding["alpha"]),
        "the relevant older, single-topic finding survives noise reduction"
    );
    assert_eq!(beta_still_baseline["ranking_variant"], "baseline");
    assert!(
        entry_ids(&beta_still_baseline).contains(&noise["beta"]),
        "the unchanged second repo is completely unaffected"
    );

    let shown_on = client
        .call("bbs.discovery.show", json!({"repo": "alpha"}))
        .await
        .unwrap();
    assert_eq!(shown_on["mode"], "on");
    assert_eq!(shown_on["revision"], 1);
    assert!(shown_on["updated_at"].is_string());

    // --- Disable returns exactly to baseline. ---
    let disabled = client
        .call("bbs.discovery.set", json!({"repo": "alpha", "mode": "off"}))
        .await
        .unwrap();
    assert_eq!(disabled["mode"], "off");
    assert_eq!(disabled["revision"], 2);
    let alpha_off_again = brief(&mut client, "alpha", "TKT-alpha-fixture").await;
    assert_eq!(alpha_off_again["ranking_variant"], "baseline");
    // The revision is reported honestly even though behavior matches the
    // never-configured baseline: an explicit disable record still exists.
    assert_eq!(alpha_off_again["ranking_config_revision"], 2);
    assert_eq!(entry_ids(&alpha_off_again), entry_ids(&alpha_before));

    // Re-enable so the restart check below has something durable to prove.
    client
        .call("bbs.discovery.set", json!({"repo": "alpha", "mode": "on"}))
        .await
        .unwrap();

    // --- Restart: the discovery registry is file-backed and independent of
    // the (in-memory, wiped-on-restart) tuple store; it must survive. ---
    client.call("stop", json!({})).await.ok();
    wait_socket_gone(&layout).await;
    let _ = handle_a.await;

    let daemon_b = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle_b = tokio::spawn(daemon_b.run());
    let mut client = connect(&layout).await;

    let after_restart = client
        .call("bbs.discovery.show", json!({"repo": "alpha"}))
        .await
        .unwrap();
    assert_eq!(
        after_restart["mode"], "on",
        "the setting persists across a daemon restart with no re-configuration"
    );
    assert_eq!(after_restart["revision"], 3);

    // --- Malformed configuration file: `show`/`set` surface the error
    // honestly; `bbs.brief` degrades to baseline instead of failing the
    // read (the same "a failed capture never fails the read" discipline
    // already applied to exposure/telemetry capture). ---
    std::fs::write(home.path().join("bbs-discovery.json"), "not json").unwrap();
    let show_err = client
        .call("bbs.discovery.show", json!({"repo": "alpha"}))
        .await;
    assert!(
        show_err.is_err(),
        "a malformed registry file is surfaced, not silently ignored"
    );
    // The in-memory tuple store does not survive the restart above; the repo
    // registration does (it is file-backed too), so only the task needs
    // recreating for `bbs.brief` to have something to resolve.
    make_task(&mut client, "alpha", "TKT-alpha-fixture", title).await;
    let degraded = brief(&mut client, "alpha", "TKT-alpha-fixture").await;
    assert_eq!(
        degraded["ranking_variant"], "baseline",
        "bbs.brief falls back to baseline rather than failing on an unreadable config"
    );
    assert_eq!(
        degraded["ranking_config_status"], "unreadable_fallback",
        "the fallback is reported as UNKNOWN, never as a confirmed absent setting"
    );

    // The exposure envelope carries the same honest identity: build, the
    // applied variant, and the config status that produced it.
    let exposures: Vec<Value> = client
        .call("space.scan", json!({"category": "event", "scope": "alpha"}))
        .await
        .unwrap()["tuples"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|t| t["payload"]["bbs_kind"] == "exposure" && t["payload"]["surface"] == "brief")
        .collect();
    let last = exposures.last().expect("a brief exposure was recorded");
    assert!(last["payload"]["build"].is_string());
    assert_eq!(last["payload"]["ranking_variant"], "baseline");
    assert_eq!(
        last["payload"]["ranking_config_status"],
        "unreadable_fallback"
    );
}
