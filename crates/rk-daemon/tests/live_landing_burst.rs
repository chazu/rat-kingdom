//! P3-T4 follow-up item (2), TKT-01M05GJA753JXH9PNEBKYEF0KP: a
//! completion-burst-under-load stress test through the LIVE daemon.
//!
//! `crates/rk-daemon/src/landing.rs`'s own unit tests already prove
//! `LandingQueue` serializes a burst on one `(repo,target)` key
//! (`burst_of_completions_on_one_key_never_runs_gates_concurrently`) and that
//! distinct keys drain concurrently
//! (`distinct_keys_drain_concurrently_within_one_run_cycle`) — but both call
//! `LandingPipeline` methods directly, in-process. `LandingPipeline` is
//! `pub(crate)`, so an external `tests/*.rs` file cannot construct one
//! (the same reason TKT-01M05GJA6JZX6G8NHQQJ98KXPA's live restart test was
//! deferred — see docs/proposals/daemon-native-landing-pipeline.md §6). What
//! neither proves is the live wiring end to end: many rats completing at
//! once over real RPC, the reactor's `action: "land"` trigger dispatching
//! each `harness_result` onto the durable queue, and the daemon's own
//! landing-pipeline consumer loop (`server.rs`) draining it — with no
//! thundering (concurrent gate runs) on the shared key.
//!
//! This test spawns several fake-harness rats CONCURRENTLY (one RPC
//! connection per spawn, fired without waiting on each other) against the
//! SAME repo/target, each producing a small doc-only diff so it lands
//! straight through (no reviewer needed). The repo's `verify` named check —
//! run for real, once per candidate, by the live gate runner — records
//! whether it ever started while a sibling candidate's run was still in
//! flight, exactly as the in-process unit test does. If the live queue ever
//! let two gate runs for the same key overlap, this trips; if it always
//! serializes, the overlap log stays empty and every candidate still lands.

mod fixture;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

fn git(dir: &Path, args: &[&str]) -> String {
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
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn init_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "r@x"]);
    git(dir, &["config", "user.name", "R"]);
    std::fs::write(dir.join("README.md"), "# x\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(client) = Client::connect_as_operator(layout).await {
            return client;
        }
    }
    panic!("daemon did not come up");
}

/// Every burst candidate touches its own `docs/note-<task>.md` (doc-only —
/// `is_doc_path` matches on the `docs/` path segment, so this diff class
/// routes straight to `Supervisor::land` and never spawns a reviewer,
/// keeping the burst small and fast). `$RK_TASK` is a plain `burst<N>` token
/// (no spaces), safe to splice directly into the filename and commit
/// message.
const BURST_FAKE: &str = r#"
read -r _prompt
mkdir -p docs
echo "note $RK_TASK" > "docs/note-$RK_TASK.md"
git add "docs/note-$RK_TASK.md" >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "docs: note $RK_TASK"
echo '{"type":"system","subtype":"init","session_id":"burst-fake"}'
rk_done "note added"
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"burst-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

/// Installs the daemon-native landing pipeline's real completion feed
/// (`examples/triggers-landing-pipeline.cue`'s `steward-landing-on-completion`
/// trigger, repo-local) as a repo-registered `.rk/triggers.cue` — the
/// reactor resolves repo-local trigger files the same way it resolves the
/// global directory (`Reactor::trigger_files`).
const LANDING_TRIGGER: &str = r#"
triggers: [
	{
		name:   "steward-landing-on-completion"
		action: "land"
		match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}
		maxFires: 20
	},
]
"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_completions_on_one_key_serialize_through_the_live_daemon() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    init_repo(repo_dir.path());

    // Lives outside the repo/gate worktree entirely, so `reset_gate_worktree`'s
    // `git clean -fd` between candidates never touches it (mirrors
    // `landing.rs`'s own `burst_of_completions_on_one_key_never_runs_gates_
    // concurrently`).
    let barrier_dir = tempfile::tempdir().unwrap();
    let marker = barrier_dir.path().join("running");
    let overlap_log = barrier_dir.path().join("overlap.log");
    let checks = format!(
        r#"
checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "test -f \"{marker}\" && echo overlap >> \"{log}\"; touch \"{marker}\"; sleep 0.15; rm -f \"{marker}\"", timeout: "30s"}},
]
"#,
        marker = marker.display(),
        log = overlap_log.display(),
    );
    let rk_dir = repo_dir.path().join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(rk_dir.join("checks.cue"), &checks).unwrap();
    std::fs::write(rk_dir.join("triggers.cue"), LANDING_TRIGGER).unwrap();

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "burst-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;

    let repo_name = repo_dir
        .path()
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo_dir.path()}),
        )
        .await
        .unwrap();

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(BURST_FAKE));

    // Fired over N SEPARATE RPC connections, without awaiting one spawn
    // before issuing the next — a genuine concurrent burst of completions
    // hitting the daemon, not a sequential loop that happens to look
    // concurrent in the log.
    const N: usize = 6;
    let mut spawn_tasks = Vec::new();
    for i in 0..N {
        let layout = layout.clone();
        let repo_path = repo_dir.path().to_path_buf();
        spawn_tasks.push(tokio::spawn(async move {
            let mut c = Client::connect_as_operator(&layout).await.unwrap();
            c.call(
                "agent.spawn",
                json!({
                    "repo": repo_path.to_string_lossy(),
                    "task": format!("burst{i}"),
                    "harness": "fake",
                }),
            )
            .await
            .unwrap()
        }));
    }
    for t in spawn_tasks {
        t.await.unwrap();
    }

    // Wait for the durable queue to fully drain — the authoritative signal
    // that every candidate reached a terminal outcome (`LandingPipeline::
    // process_next` only removes an entry's tuple once `process_entry`
    // returns). The live daemon's reactor (dispatch) and landing-pipeline
    // (drain) loops both run on their own background ticks, so this is
    // polled rather than awaited directly. A candidate's file can already be
    // visible in `repo_dir`'s working tree mid-`land()`, before its queue
    // tuple is removed — polling file existence instead of queue-emptiness
    // would race that window, so the queue is the thing to wait on.
    let mut queue_empty = false;
    for _ in 0..600 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let queue = client
            .call(
                "space.scan",
                json!({"category": "event", "identity": "landing_queue_entry", "scope": repo_name}),
            )
            .await
            .unwrap();
        if queue["tuples"].as_array().unwrap().is_empty() {
            queue_empty = true;
            break;
        }
    }
    assert!(
        queue_empty,
        "landing queue did not fully drain within the wait budget"
    );

    let landed: Vec<bool> = (0..N)
        .map(|i| {
            repo_dir
                .path()
                .join(format!("docs/note-burst{i}.md"))
                .exists()
        })
        .collect();
    assert!(
        landed.iter().all(|&l| l),
        "not every burst candidate landed once the queue drained: {landed:?}"
    );

    let overlap = std::fs::read_to_string(&overlap_log).unwrap_or_default();
    assert!(
        overlap.is_empty(),
        "gate runs overlapped for the same (repo,target) key under a live-daemon RPC burst — \
         LandingQueue failed to serialize: {overlap}"
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
