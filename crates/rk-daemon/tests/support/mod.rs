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

/// Generic core of [`try_connect_or_report`]: races a *retrying* `attempt`
/// against `handle`'s completion and one absolute `deadline`. Generic over
/// `attempt` so this racing logic — an already-finished handle winning
/// immediately, a still-slow-but-eventually-successful attempt being allowed
/// to finish, the deadline firing when neither happens — can be unit tested
/// against a fake attempt closure instead of a real daemon socket and a
/// production-sized deadline, mirroring how [`poll_until`] is tested apart
/// from [`connect`].
///
/// `attempt` itself is NEVER individually capped short. A version of this
/// that wrapped each attempt in its own `poll_interval`-sized timeout would
/// cancel a legitimate attempt that is merely slow — a stalled connect or
/// disk-bound auth-token read under real host contention, exactly the
/// condition this instrumentation exists to survive — before it ever got a
/// chance to succeed, misreporting a healthy-but-slow daemon as unreachable.
/// `poll_interval` here is ONLY the backoff between FAILED attempts, never an
/// acceptance deadline on any single attempt in flight. The sole upper bound
/// on how long any one attempt may run is `deadline`, raced concurrently via
/// the third `select!` branch below — so a stall is still bounded overall,
/// just not truncated attempt-by-attempt.
///
/// `handle` is taken by `&mut` rather than by value so callers that still
/// need it afterwards (e.g. to `abort()` a daemon that DID come up) keep
/// ownership. It is safe to poll it inside `select!` because a branch that
/// resolves the handle to `Ready` returns from this function immediately —
/// the handle is never polled again after it has yielded its result.
#[allow(dead_code)]
pub async fn race_attempt_or_report<T, Fut>(
    handle: &mut tokio::task::JoinHandle<rk_core::Result<()>>,
    deadline: Duration,
    poll_interval: Duration,
    mut attempt: impl FnMut() -> Fut,
) -> Result<T, StartupFailure>
where
    Fut: Future<Output = Option<T>>,
{
    let start = Instant::now();
    // Wrapped in one `async` block rather than left as a bare `select!`
    // branch expression so the retry loop (attempt, and on failure a real
    // timer sleep) reads as the single logical unit it is; `select!` still
    // only ever polls this whole unit, never an individual attempt, against
    // `deadline`/`handle`.
    let retrying_attempt = async move {
        loop {
            if let Some(value) = attempt().await {
                return value;
            }
            // Not just pacing: a failed attempt against, e.g., a socket
            // nobody is listening on typically resolves synchronously (no
            // real await point). Without an unconditional real timer yield
            // here, a single-threaded runtime could keep this loop always
            // immediately ready and never hand control back to the
            // executor — starving `handle`'s task of a chance to be polled
            // to completion, which would misreport a genuinely stopped
            // daemon as a plain timeout. `poll_until`'s loop has the same
            // unconditional-sleep shape for the same reason.
            tokio::time::sleep(poll_interval).await;
        }
    };
    tokio::pin!(retrying_attempt);

    tokio::select! {
        biased;
        join_result = &mut *handle => Err(match join_result {
            Ok(result) => StartupFailure::DaemonExited(result),
            Err(join_error) => StartupFailure::DaemonJoinError(join_error),
        }),
        value = &mut retrying_attempt => Ok(value),
        _ = tokio::time::sleep(deadline) => Err(StartupFailure::TimedOut(start.elapsed())),
    }
}

/// Diagnostic instrumentation, not a confirmed fix: see
/// [`race_attempt_or_report`] for what this races and why. IF a daemon's
/// `run()` future has already resolved — lost the singleton-lock race,
/// failed its bind, or hit any other startup error — that actual result is
/// reported instead of only ever seeing "daemon did not come up" with no
/// cause. Whether that scenario (an early-finished `run()`) is what actually
/// produces the intermittent `restart_mid_queue_replays_fifo_order_*`
/// failure under TKT-mukos-pogim-lopis is NOT yet established — no run
/// observed under this instrumentation has yet captured a `DaemonExited` or
/// `DaemonJoinError` outcome. Treat the underlying cause as unknown until an
/// actual failing run under this instrumentation produces that evidence.
#[allow(dead_code)]
pub async fn try_connect_or_report(
    layout: &Layout,
    handle: &mut tokio::task::JoinHandle<rk_core::Result<()>>,
    deadline: Duration,
    poll_interval: Duration,
) -> Result<Client, StartupFailure> {
    race_attempt_or_report(handle, deadline, poll_interval, || async {
        Client::connect_as_operator(layout).await.ok()
    })
    .await
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

/// Shared retry core of [`start_daemon`]/[`restart_daemon_over`]: retry a
/// fresh `new_daemon` + spawn ONLY on the one specific, identified failure
/// this exists to survive — `acquire_singleton_lock`'s own "already holds
/// the lock" refusal (`server.rs`), observed under parallel `cargo test`
/// load, where this same process can be running several other tests'
/// daemons concurrently and one of them can still be a few OS scheduler
/// ticks from fully releasing its `flock` when this one tries to bind. A
/// plain reconnect loop can never recover from that: once `Daemon::run()`
/// loses the race for the lock it returns immediately without ever
/// listening, so nothing will ever answer the socket no matter how long
/// `connect` polls it.
///
/// This does NOT establish, and must not be read as establishing, exactly
/// WHY the lock is still held at that moment (a surviving descriptor, a
/// not-yet-dropped task, or a genuinely separate process could each produce
/// the identical symptom) — only that this specific, named refusal is
/// observed and is the one condition worth a bounded retry. Any OTHER early
/// exit — a bind failure, a config error, a panicked/cancelled task — is
/// propagated immediately via `panic!` instead of being silently retried
/// into an unhelpful "gave up after 20 attempts" message that would hide
/// the real cause. `new_daemon` is called fresh on every attempt because a
/// `Daemon` that failed to win the lock has already consumed itself
/// (`run(self)`) — there is no daemon left to retry, and every failed
/// attempt's task is already fully joined (not merely dropped) by the time
/// this loop inspects its result, since the `Ok(...)` arms below only match
/// once `&mut handle` has actually resolved. Only the FINAL, successful
/// handle is returned alongside its `Client` — the caller owns that
/// daemon's cleanup (e.g. `.abort()` at the end of a test that fakes a
/// restart), exactly as it would if it had spawned it directly.
#[allow(dead_code)]
async fn start_daemon_retrying(
    layout: &Layout,
    mut new_daemon: impl FnMut() -> Daemon,
) -> (Client, tokio::task::JoinHandle<rk_core::Result<()>>) {
    for _ in 0..20 {
        let daemon = new_daemon();
        let mut handle = tokio::spawn(daemon.run());
        // A daemon that wins the bind runs its accept loop forever, so this
        // handle deliberately never resolves in the success case — the
        // timeout is just a generous grace window to catch the failure case,
        // which in every observed instance resolves in well under 50ms.
        match tokio::time::timeout(Duration::from_millis(200), &mut handle).await {
            Err(_) => return (connect(layout).await, handle), // still running: bind succeeded
            Ok(Ok(Err(error))) if error.to_string().contains("already holds the lock") => {
                tokio::time::sleep(Duration::from_millis(50)).await; // identified transient: retry
            }
            Ok(Ok(Err(error))) => {
                panic!("daemon startup failed (not the identified lock-contention case): {error}")
            }
            Ok(Ok(Ok(()))) => {
                panic!("daemon task exited cleanly before it ever bound the socket")
            }
            Ok(Err(join_error)) => {
                panic!("daemon task panicked or was cancelled during startup: {join_error}")
            }
        }
    }
    panic!("daemon repeatedly lost the singleton-lock race against {layout:?}");
}

/// Start a daemon against `layout` (an in-memory `Space` — no durable state
/// survives a restart) and connect to it. See [`start_daemon_retrying`] for
/// what this retries and why. The successful daemon's task is intentionally
/// left unowned here (dropped with the runtime at test end), matching every
/// existing caller of this function, which never needed to clean it up
/// individually.
#[allow(dead_code)]
pub async fn start_daemon(layout: &Layout) -> Client {
    let (client, _handle) = start_daemon_retrying(layout, || {
        Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap()
    })
    .await;
    client
}

/// Same identified-transient-refusal retry as [`start_daemon`], but over a
/// genuinely ON-DISK `Daemon::new(layout, config)` — for a fixture that
/// needs durable state (the landing queue, the registry) to actually
/// survive the restart, which an in-memory `Space` cannot prove.
///
/// This is an IN-PROCESS durable-state fixture, faking a restart via
/// `handle.abort()`/`.await` on the outgoing daemon rather than a real
/// second OS process — it must not be read as physical-restart coverage
/// (`crates/rk-cli/tests/resumed_generation_successor_landing.rs` has the
/// genuine cross-process alternative when that distinction matters).
/// Returns the successful replacement daemon's own `JoinHandle` alongside
/// its `Client`, unlike [`start_daemon`]: a caller faking a restart
/// typically already owns and cleans up the OUTGOING daemon's handle
/// explicitly (`.abort()`/`.await`) and must do the same for this
/// replacement at the end of its own test, rather than leaving it running
/// unowned against a shared test binary's runtime.
#[allow(dead_code)]
pub async fn restart_daemon_over(
    layout: &Layout,
    config: &rk_core::config::Config,
) -> (Client, tokio::task::JoinHandle<rk_core::Result<()>>) {
    start_daemon_retrying(layout, || Daemon::new(layout.clone(), config).unwrap()).await
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
        current_attempt: None,
    };
    Registry::load(&home.join("agents.json"))
        .unwrap()
        .insert(record)
        .unwrap();
}
