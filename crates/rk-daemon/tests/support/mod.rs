//! Shared support for daemon-spinning integration tests.
//!
//! Lives in a subdirectory so cargo does not compile it as a test target of its
//! own; each test binary picks it up with `mod support;`.

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::Instant;

/// Register a scratch repository after its versioned CUE policy has been
/// installed. Production dispatch intentionally refuses unregistered repos.
#[allow(dead_code)]
pub async fn register_repo(client: &mut Client, repo: &Path) {
    let name = repo
        .file_name()
        .expect("scratch repo must have a final path component")
        .to_string_lossy();
    client
        .call(
            "repo.add",
            serde_json::json!({"name": name, "path": repo.to_string_lossy()}),
        )
        .await
        .unwrap();
}

/// Resolve the workspace root at RUNTIME instead of baking
/// `env!("CARGO_MANIFEST_DIR")` into the test binary at compile time.
///
/// Test binaries live under a shared `CARGO_TARGET_DIR` (`DiskConfig::
/// shared_cargo_target`); cargo's fingerprint does not cover the manifest
/// directory, so a binary compiled from one worktree is reused verbatim by a
/// byte-identical checkout in another. A path baked in via `env!` then
/// outlives the worktree that produced it, including one already reaped
/// (TKT-01M0F0GHDPGA24X1TB24A0PZD0). `std::env::current_dir()` is safe here
/// because cargo sets a test binary's working directory to its package's
/// manifest directory on every run, not at compile time.
#[allow(dead_code)]
pub fn workspace_root() -> PathBuf {
    let cwd = std::env::current_dir().expect("test process must have a current directory");
    cwd.ancestors()
        .find(|dir| dir.join("Cargo.toml").is_file() && dir.join("crates").is_dir())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| {
            panic!(
                "could not find the rat-kingdom workspace above runtime directory {}",
                cwd.display()
            )
        })
}

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
    {name: "landing-protected-paths", command: "true", timeout: "30s"},
    {name: "landing-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "true", timeout: "30s"},
]
"#,
    )
    .unwrap();
    if !rk_dir.join("repo.cue").is_file() {
        std::fs::write(
            rk_dir.join("repo.cue"),
            r#"repo: {
    delivery: {target: "agent-base", mode: "merge", remote: "origin", remoteBranch: "{{branch}}", deleteSource: true}
}
"#,
        )
        .unwrap();
    }
    let run = |args: &[&str]| {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(repo)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    };
    run(&["add", ".rk/checks.cue", ".rk/repo.cue"]);
    run(&[
        "commit",
        "-m",
        "test: register repository policy and landing checks",
    ]);
}

/// Install the minimal versioned repository policy needed for tests that
/// exercise dispatch but not landing gates.
#[allow(dead_code)]
pub fn install_default_repository_policy(repo: &std::path::Path) {
    let rk_dir = repo.join(".rk");
    std::fs::create_dir_all(&rk_dir).unwrap();
    std::fs::write(
        rk_dir.join("repo.cue"),
        r#"repo: {
    delivery: {target: "agent-base", mode: "merge", remote: "origin", remoteBranch: "{{branch}}", deleteSource: true}
}
"#,
    )
    .unwrap();
    let status = std::process::Command::new("git")
        .args(["add", ".rk/repo.cue"])
        .current_dir(repo)
        .status()
        .unwrap();
    assert!(status.success());
    let status = std::process::Command::new("git")
        .args(["commit", "-m", "test: register repository policy"])
        .current_dir(repo)
        .status()
        .unwrap();
    assert!(status.success());
}

/// Poll `attempt` at a fixed `interval` until it returns `Some` or a
/// monotonic `deadline` (measured from first call, via `tokio::time::Instant`
/// so it cannot be perturbed by wall-clock adjustments) elapses. Returns the
/// elapsed time on exhaustion so callers can report it.
///
/// Generic over the attempt so the deadline/exhaustion behavior can be
/// unit-tested against a fake, near-instant attempt closure instead of a real
/// daemon socket and instead of waiting out a production-sized deadline.
#[allow(dead_code)]
pub async fn poll_until<T, Fut>(
    deadline: Duration,
    interval: Duration,
    mut attempt: impl FnMut() -> Fut,
) -> Result<T, Duration>
where
    Fut: Future<Output = Option<T>>,
{
    let start = Instant::now();
    loop {
        if let Some(value) = attempt().await {
            return Ok(value);
        }
        let elapsed = start.elapsed();
        if elapsed >= deadline {
            return Err(elapsed);
        }
        tokio::time::sleep(interval).await;
    }
}

/// Poll for a daemon at `layout` to come up and connect as operator.
///
/// Was previously copy-pasted into ~40 test files with varying retry budgets
/// (50/100/200 iterations at 20ms — a hardcoded ~1s to ~4s). Several files had
/// already widened their local copy past the original ~1s under
/// parallel-test-process load, so this shared version standardized on the
/// most generous of the budgets already proven necessary rather than the
/// tightest — but under full-workspace `cargo test` process contention, even
/// that ~4s budget intermittently expired against a daemon that had already
/// won its bind and would have become connectable given more time
/// (TKT-01M0HJV6ZCREQTYSXGETEENY2F). The budget is now a monotonic 30s
/// deadline instead of a fixed iteration count, so it cannot expire early
/// just because individual polls ran slower under load.
#[allow(dead_code)]
pub async fn connect(layout: &Layout) -> Client {
    match poll_until(CONNECT_DEADLINE, CONNECT_POLL_INTERVAL, || async {
        Client::connect_as_operator(layout).await.ok()
    })
    .await
    {
        Ok(client) => client,
        Err(elapsed) => {
            panic!(
                "daemon did not come up: {elapsed:?} elapsed against a {CONNECT_DEADLINE:?} \
                 deadline"
            )
        }
    }
}

/// Why [`connect_or_report`]'s race ended without a client. Kept distinct
/// from the panic message itself so the racing logic (deadline exhaustion
/// vs. an already-finished handle) can be unit tested against a fake,
/// near-instant task instead of a real daemon socket and a
/// production-sized deadline — mirroring how [`poll_until`] is tested apart
/// from [`connect`].
#[allow(dead_code)]
#[derive(Debug)]
pub enum StartupFailure {
    TimedOut(Duration),
    DaemonExited(rk_core::Result<()>),
    DaemonJoinError(tokio::task::JoinError),
}

impl std::fmt::Display for StartupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartupFailure::TimedOut(elapsed) => write!(
                f,
                "daemon did not come up: {elapsed:?} elapsed against a {CONNECT_DEADLINE:?} \
                 deadline"
            ),
            StartupFailure::DaemonExited(Ok(())) => write!(
                f,
                "daemon task exited cleanly before its socket ever became connectable"
            ),
            StartupFailure::DaemonExited(Err(error)) => write!(
                f,
                "daemon failed to start: {error} — a stopped daemon, not a slow one"
            ),
            StartupFailure::DaemonJoinError(join_error) => write!(
                f,
                "daemon task panicked or was cancelled before its socket became connectable: \
                 {join_error}"
            ),
        }
    }
}

const CONNECT_DEADLINE: Duration = Duration::from_secs(30);
const CONNECT_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Diagnostic instrumentation, not a confirmed fix: races a connect attempt
/// against `handle`'s completion and the `deadline`, so that IF a daemon's
/// `run()` future has already resolved — lost the singleton-lock race,
/// failed its bind, or hit any other startup error — that actual result is
/// reported instead of only ever seeing "daemon did not come up" with no
/// cause. Whether that scenario (an early-finished `run()`) is what actually
/// produces the intermittent `restart_mid_queue_replays_fifo_order_*`
/// failure under TKT-mukos-pogim-lopis is NOT yet established — no run
/// observed under this instrumentation has yet captured a `DaemonExited` or
/// `DaemonJoinError` outcome. Treat the underlying cause as unknown until an
/// actual failing run under this instrumentation produces that evidence.
///
/// Each connect attempt is individually bounded via `tokio::select!` against
/// `handle` and a per-attempt timeout capped at `poll_interval` (or the
/// remaining budget, if smaller) — a stalled `connect`/auth handshake can
/// therefore never prevent this loop from re-observing `handle` or
/// `deadline`. A version of this that plainly `.await`ed the connect attempt
/// before ever checking `handle` or the elapsed time would not actually race
/// anything: a stall in that connect step would block both checks
/// indefinitely, which is not "bounded" in any meaningful sense.
///
/// The trailing `sleep(poll_interval)` is not just pacing: a `connect`
/// attempt against a socket nobody is listening on typically fails
/// synchronously (no real await point), so without an unconditional real
/// timer yield each iteration, a single-threaded runtime could keep this
/// loop always immediately ready and never actually hand control back to the
/// executor — starving `handle`'s task of a chance to be polled to
/// completion at all, which would misreport a genuinely stopped daemon as a
/// plain timeout. `poll_until`'s loop has the same unconditional-sleep shape
/// for the same reason.
///
/// `handle` is taken by `&mut` rather than by value so callers that still
/// need it afterwards (e.g. to `abort()` a daemon that DID come up) keep
/// ownership. It is safe to poll repeatedly via `&mut *handle` inside the
/// loop because a `select!` branch that resolves the handle to `Ready`
/// immediately returns from this function — the handle is never polled again
/// after it has yielded its result.
#[allow(dead_code)]
pub async fn try_connect_or_report(
    layout: &Layout,
    handle: &mut tokio::task::JoinHandle<rk_core::Result<()>>,
    deadline: Duration,
    poll_interval: Duration,
) -> Result<Client, StartupFailure> {
    let start = Instant::now();
    loop {
        tokio::select! {
            biased;
            join_result = &mut *handle => {
                return Err(match join_result {
                    Ok(result) => StartupFailure::DaemonExited(result),
                    Err(join_error) => StartupFailure::DaemonJoinError(join_error),
                });
            }
            connect_result = tokio::time::timeout(
                poll_interval,
                Client::connect_as_operator(layout),
            ) => {
                if let Ok(Ok(client)) = connect_result {
                    return Ok(client);
                }
                // Either the attempt timed out (bounded by `poll_interval`)
                // or connected and was refused/failed — either way, fall
                // through to the deadline check and retry below.
            }
        }

        let elapsed = start.elapsed();
        if elapsed >= deadline {
            return Err(StartupFailure::TimedOut(elapsed));
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// Panicking wrapper over [`try_connect_or_report`] using the same
/// production-sized budget as [`connect`]. See [`try_connect_or_report`] for
/// what this actually races and why.
#[allow(dead_code)]
pub async fn connect_or_report(
    layout: &Layout,
    handle: &mut tokio::task::JoinHandle<rk_core::Result<()>>,
) -> Client {
    match try_connect_or_report(layout, handle, CONNECT_DEADLINE, CONNECT_POLL_INTERVAL).await {
        Ok(client) => client,
        Err(failure) => panic!("{failure}"),
    }
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

/// Seed a real durable generation before starting a synthetic-completion daemon.
#[allow(dead_code)]
pub fn seed_landing_generation(
    home: &Path,
    repo: &Path,
    name: &str,
    task: &str,
    branch: &str,
    target: &str,
) {
    use rk_daemon::agents::{AgentRecord, AgentState, Registry};
    std::fs::create_dir_all(home).unwrap();
    let created_at = chrono::Utc::now();
    let record = AgentRecord {
        name: name.into(),
        spawn: Some(rk_core::id::SpawnId::new()),
        role: "rat".into(),
        coordination: None,
        harness: "fake".into(),
        permission_mode: None,
        model: None,
        repo_root: repo.canonicalize().unwrap(),
        repo_name: repo.file_name().unwrap().to_string_lossy().into_owned(),
        task: Some(task.into()),
        branch: Some(branch.into()),
        fork_point: None,
        worktree: None,
        target_branch: target.into(),
        parent: None,
        workflow_instance: None,
        review: None,
        coordinator: None,
        session_id: None,
        attach_target: None,
        pid: None,
        merge_commit: None,
        state: AgentState::Completed,
        result: None,
        progress: None,
        crashed: false,
        stderr_tail: None,
        usage: Default::default(),
        cost_usd: 0.0,
        created_at,
        updated_at: created_at,
        archived_at: None,
        liveness: Default::default(),
        transport_outage: None,
        recovery: None,
        recovery_receipt: None,
    };
    Registry::load(&home.join("agents.json"))
        .unwrap()
        .insert(record)
        .unwrap();
}
