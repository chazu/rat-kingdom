//! TKT-01M02EK9T3629624MS23BK7V40: exercise the first authenticated call from
//! a freshly launched harness while many spawns are racing one another.
//!
//! The fake harness runs the real `rk` binary immediately on process launch.
//! This is deliberately before the harness emits any protocol event or waits
//! for its prompt, so the call can race the supervisor's post-launch registry
//! update and the daemon's peer-origin lookup.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

fn git(dir: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn scratch_repo(dir: &Path) {
    git(dir, &["init", "-b", "main"]);
    git(dir, &["config", "user.email", "rat@example.com"]);
    git(dir, &["config", "user.name", "Rat"]);
    std::fs::write(dir.join("README.md"), "# startup race\n").unwrap();
    std::fs::create_dir_all(dir.join(".rk")).unwrap();
    std::fs::write(dir.join(".rk/repo.cue"), "repo: {}\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-m", "init"]);
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn first_call_harness(rk_bin: &str) -> String {
    let rk_bin = shell_quote(rk_bin);
    format!(
        r#"
set +e
{rk_bin} scan fact system > "$RK_WORKTREE/first-rk.out" 2>&1
status=$?
printf '%s\n' "$status" > "$RK_WORKTREE/first-rk.status"
{rk_bin} done "startup race probe" >/dev/null 2>&1 || true
echo '{{"type":"system","subtype":"init","session_id":"startup-race"}}'
echo '{{"type":"result","subtype":"success","is_error":false,"result":"first call attempted","session_id":"startup-race","total_cost_usd":0.001,"usage":{{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}}}'
"#
    )
}

/// Bounds a single RPC round trip. Multi-thread flavor (see the test's own
/// doc comment) keeps the polling loops below alive even while a git
/// subprocess call is wedged on another worker thread, but it says nothing
/// about a call that hangs INSIDE the daemon's own handling of one specific
/// RPC — that request's own `.await` would still never resolve, on any
/// runtime flavor. Every RPC in this test goes through this rather than a
/// bare `.call()`, so a daemon-side wedge fails this test with a specific,
/// attributable panic instead of blocking indefinitely regardless of which
/// half (test-side polling, or the daemon's handling of one call) is stuck.
async fn call_bounded(client: &mut Client, method: &str, params: serde_json::Value) -> Value {
    tokio::time::timeout(Duration::from_secs(20), client.call(method, params))
        .await
        .unwrap_or_else(|_| panic!("RPC `{method}` did not return within 20s: daemon-side handling is wedged, not just a test polling loop"))
        .unwrap()
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..1500 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(client) = Client::connect_as_operator(layout).await {
            return client;
        }
    }
    panic!("daemon did not come up");
}

async fn wait_for_markers(markers: &[PathBuf]) {
    for _ in 0..500 {
        if markers.iter().all(|path| path.exists()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let missing: Vec<_> = markers
        .iter()
        .filter(|path| !path.exists())
        .map(|path| path.display().to_string())
        .collect();
    panic!("first-call markers did not appear: {missing:?}");
}

/// Every process on the host with `pid` as an ancestor, found by walking
/// `ps`'s `pid`/`ppid` columns transitively from `pid` — cheap, portable
/// (no `/proc` dependency), and exact: it names only this process's own
/// descendant tree, never a process group, which the daemon deliberately
/// puts each harness child into its OWN copy of (`.process_group(0)` in
/// `rk-harness`'s launcher) specifically so a daemon-side signal never
/// reaches the wrong tree. Signalling "the process group" here would be
/// exactly backwards — and unlike a pid ancestry walk, could just as
/// easily land on an unrelated sibling nextest test's process group.
fn descendants_of(pid: u32) -> Vec<(u32, String)> {
    let Ok(output) = std::process::Command::new("ps")
        .args(["-Ao", "pid=,ppid=,comm="])
        .output()
    else {
        return Vec::new();
    };
    let rows: Vec<(u32, u32, String)> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut parts = line.trim().splitn(3, char::is_whitespace);
            let this_pid = parts.next()?.trim().parse().ok()?;
            let ppid = parts.next()?.trim().parse().ok()?;
            let comm = parts.next()?.trim().to_string();
            Some((this_pid, ppid, comm))
        })
        .collect();

    let mut descendants = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut frontier = vec![pid];
    while let Some(parent) = frontier.pop() {
        for (this_pid, ppid, comm) in &rows {
            if *ppid == parent && seen.insert(*this_pid) {
                descendants.push((*this_pid, comm.clone()));
                frontier.push(*this_pid);
            }
        }
    }
    descendants
}

/// A plain OS thread, outside the tokio runtime entirely, that kills this
/// test's own process after `bound` if it is still running. Every RPC in
/// this test is individually bounded by [`call_bounded`], but that alone
/// cannot bound tokio's own runtime *shutdown* — dropping a multi-thread
/// `Runtime` joins its blocking-pool threads (where `block_in_place`
/// hands off git subprocess calls), and a thread stuck in a genuinely
/// wedged syscall there blocks that join with nothing async-side left to
/// time out. This test builds to its own single-test binary, so exiting
/// the process here cannot collaterally kill an unrelated sibling test —
/// but exiting alone still leaks every descendant that caused the hang
/// (the fake harness's `bash`, its own `rk scan`/`rk done` children): a
/// dead parent does not take them with it, they're simply reparented and
/// left running. Kill this process's own descendant tree first — logging
/// it as evidence of exactly what was still alive — then exit.
fn spawn_watchdog(bound: Duration) {
    std::thread::spawn(move || {
        std::thread::sleep(bound);
        let descendants = descendants_of(std::process::id());
        eprintln!(
            "codex_auth_startup_race watchdog: still running after {bound:?} — wedged past \
             every per-call RPC timeout, or during runtime shutdown, which no in-test timeout \
             can bound. Owned descendants still alive: {descendants:?}. Terminating them and \
             this process so this reads as a failure, not an indefinitely occupied check."
        );
        for (pid, _) in &descendants {
            unsafe {
                libc::kill(*pid as libc::pid_t, libc::SIGKILL);
            }
        }
        std::process::exit(101);
    });
}

/// Multi-thread, not the `#[tokio::test]` default current-thread flavor:
/// `Supervisor::diff_summary_for` only routes its git subprocess call
/// through `block_in_place` when a multi-thread runtime is available,
/// falling back to running it inline otherwise. Inline means on this exact
/// OS thread — the same one every `tokio::time::sleep` in this test's
/// polling loops needs to be woken. On the current-thread flavor, a wedged
/// git subprocess call (e.g. TKT-bikuz-kumuz-zutit's leaked-pipe hang)
/// freezes that one thread solid, which starves the loop bounds below into
/// silently waiting forever instead of failing with the diagnostics they
/// carry — that is exactly how the original hang needed a manual process
/// kill instead of a failing test. Multi-thread lets `block_in_place` hand
/// the git call to its own thread, so the polling loops keep running and
/// their bounds are real wall-clock bounds again.
#[tokio::test(flavor = "multi_thread")]
async fn first_rk_call_survives_spawn_startup_race() {
    spawn_watchdog(Duration::from_secs(90));

    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    scratch_repo(repo_dir.path());

    std::env::set_var(
        "RK_FAKE_HARNESS_CMD",
        first_call_harness(env!("CARGO_BIN_EXE_rk")),
    );
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let _handle = tokio::spawn(daemon.run());
    let mut operator = connect(&layout).await;
    call_bounded(
        &mut operator,
        "repo.add",
        json!({"name": "startup-race", "path": repo_dir.path()}),
    )
    .await;

    // The child runs its first authenticated call before it emits any harness
    // event. Concurrent spawns maximize overlap between launch, registry PID
    // update, socket accept, and supervised_agents_for_peer().
    let mut spawn_calls = Vec::new();
    for index in 0..32 {
        let layout = layout.clone();
        let repo = repo_dir.path().to_string_lossy().to_string();
        spawn_calls.push(tokio::spawn(async move {
            let mut client = Client::connect_as_operator(&layout).await.unwrap();
            call_bounded(
                &mut client,
                "agent.spawn",
                json!({
                    "repo": repo,
                    "task": format!("codex-auth-race-{index}"),
                    "harness": "fake"
                }),
            )
            .await
        }));
    }

    let mut markers = Vec::new();
    for call in spawn_calls {
        let spawned = call.await.unwrap();
        let worktree = spawned["agent"]["worktree"]
            .as_str()
            .expect("spawn response includes the agent worktree");
        markers.push(Path::new(worktree).join("first-rk.status"));
    }

    wait_for_markers(&markers).await;
    for marker in markers {
        let status = tokio::fs::read_to_string(&marker).await.unwrap();
        let output = tokio::fs::read_to_string(marker.with_file_name("first-rk.out"))
            .await
            .unwrap();
        assert_eq!(
            status.trim(),
            "0",
            "first rk call failed at {}: {output}",
            marker.display()
        );
        assert!(
            !output.contains("FORBIDDEN") && !output.contains("forbidden:"),
            "first rk call was forbidden at {}: {output}",
            marker.display()
        );
    }

    // Drain the lifecycle so this test also proves the successful calls did
    // not merely leave harnesses wedged at the startup boundary. Bounded at
    // 500 * 20ms = 10s; past that this must fail loudly with the observed
    // states, not fall through silently — a harness left wedged here is the
    // failure this test exists to catch, and the loop previously expired
    // with no assertion at all.
    let mut last_agents = json!({});
    let mut all_terminal = false;
    for _ in 0..500 {
        let agents = call_bounded(&mut operator, "agent.list", json!({})).await;
        all_terminal = agents["agents"].as_array().unwrap().iter().all(|agent| {
            matches!(
                agent["state"].as_str(),
                Some("completed") | Some("failed") | Some("dismissed")
            )
        });
        last_agents = agents;
        if all_terminal {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        all_terminal,
        "not every spawned agent reached a terminal state within 10s: {last_agents}"
    );

    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
