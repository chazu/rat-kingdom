//! Daemon-native landing pipeline (Phase 3).
//!
//! T2 (`LandingQueue` + gate runner) grows here — see
//! docs/proposals/daemon-native-landing-pipeline.md §2.1/§2.2/§4. T1's
//! contribution to this module is its *interface*: a daemon-native consumer
//! in this crate can run a fully-resolved named check in an arbitrary
//! directory (a persistent gate worktree) through
//! [`crate::workflow_exec::WorkflowEngine::run_check_in`] without any agent
//! worktree or workflow context.
//!
//! This module adds the durable per-`(repo,target)` FIFO
//! ([`LandingQueue`], modeled on the Phase 1 trigger queue —
//! `Reactor::enqueue_fire`/`drain_queued_fires`) and the consumer
//! ([`LandingPipeline`]) that dequeues a candidate, runs the same three
//! gates `steward.cue`'s `_gates` block runs today (`steward-protected-paths`,
//! `steward-diff-scope`, the repo's named `verify` check) against T1's warm
//! worktree, and — for a `doc-only`/`trivial` diff, the tier that needs no
//! LLM judgment — routes straight to [`crate::supervisor::Supervisor::land`]
//! on a pass. A diff needing review is handed back as
//! [`LandingOutcome::NeedsReview`]: T3 wires the verdict-cache probe and
//! routing in at that point (see the T2→T3 interface note in the design
//! doc's §3) without touching anything above it.
//!
//! Restart-safety for an in-flight candidate (§2.6 of the design doc — a
//! queue entry marked `running_gates`/`awaiting_review` surviving a daemon
//! restart) is explicitly out of scope here: a queued entry is claimed
//! (deleted from the durable queue) at dequeue time, before its gates run.
//! That is deliberately simpler than the design doc's eventual shape, and
//! matches the ticket decomposition — T4 owns restart-safety proof and the
//! completion-feed cutover.
//!
//! Nothing outside this module's own tests constructs a [`LandingPipeline`]
//! yet: wiring one up to a live completion feed is T4's "cutover" (design
//! doc §4). `#![allow(dead_code)]` below is scoped to that gap, not a
//! blanket exemption — every item it covers is exercised by this module's
//! own test suite.
#![allow(dead_code)]

use crate::supervisor::Supervisor;
use crate::workflow_exec::{OnTimeout, ResolvedRun, WorkflowEngine};
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_space::Space;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::warn;

/// Identity of a durably-queued landing candidate (`Furniture`, scoped to the
/// repo it belongs to) — the T2 counterpart to the reactor's
/// `reactor_queued_fire` (`crates/rk-daemon/src/reactor.rs`).
const LANDING_QUEUE_IDENTITY: &str = "landing_queue_entry";

/// The two gates that guard every landing attempt regardless of tier —
/// `examples/workflows/steward.cue`'s `_gates` block, POLICY (#19) and
/// DIFF-SCOPE (#20). Named-check registry entries, not raw commands: a repo
/// must register them in `.rk/checks.cue` exactly as it does for the
/// workflow-driven steward today.
const PROTECTED_PATHS_CHECK: &str = "steward-protected-paths";
const DIFF_SCOPE_CHECK: &str = "steward-diff-scope";

/// Wall-clock bound for the two cheap policy/scope gates — matches
/// `_gates`' own `timeout: "2m"` in `examples/workflows/steward.cue`.
const POLICY_GATE_TIMEOUT: Duration = Duration::from_secs(2 * 60);

/// Fallback when a named check carries no `timeout` of its own (mirrors
/// `workflow_exec::DEFAULT_RUN_TIMEOUT`, private to that module).
const DEFAULT_CHECK_TIMEOUT: &str = "10m";

/// One landing candidate: a completed rat's branch, gated then routed toward
/// `Supervisor::land` (or, once T3 lands, a reviewer's verdict). Mirrors the
/// reactor's queued-fire tuple shape (`repo_name`/`repo_path` as two
/// distinct fields — the first is the tuple scope and ticket/artifact scope,
/// the second is the filesystem root `Repo::discover` and `Supervisor::land`
/// need) rather than inventing a new convention.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct LandingQueueEntry {
    pub(crate) repo_name: String,
    pub(crate) repo_path: String,
    pub(crate) branch: String,
    pub(crate) target: String,
    pub(crate) head_sha: String,
    pub(crate) diff_class: String,
    pub(crate) task: String,
    /// Enqueue order within `repo_name`, assigned by [`LandingQueue::enqueue`].
    /// `0` until then — never read before enqueue sets it.
    #[serde(default)]
    pub(crate) seq: u64,
}

/// A durable, per-`(repo,target)` FIFO of landing candidates — modeled
/// directly on the Phase 1 trigger queue (`Reactor::enqueue_fire` /
/// `drain_queued_fires`, `crates/rk-daemon/src/reactor.rs`) rather than a new
/// in-memory structure: one `Furniture` tuple per candidate, ordered by a
/// persisted monotonic sequence (one counter file per repo, mirroring
/// `<home>/reactor-queue-seq`), drained oldest-first by polling.
struct LandingQueue {
    space: Space,
    seq_dir: PathBuf,
    /// In-memory cache of the last sequence handed out per repo, so a burst
    /// of enqueues within one process lifetime doesn't reread its own file
    /// write back on every call. Reset (by construction) on daemon restart;
    /// the file is what survives.
    seq_cache: Mutex<std::collections::HashMap<String, u64>>,
}

impl LandingQueue {
    fn new(space: Space, layout: &Layout) -> Self {
        Self {
            space,
            seq_dir: layout.home().join("landing-queue-seq"),
            seq_cache: Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn seq_file(&self, repo_name: &str) -> PathBuf {
        self.seq_dir.join(sanitize_path_component(repo_name))
    }

    /// Next value in the durable per-repo enqueue-order counter. Loaded from
    /// disk lazily on first use per process, then cached and persisted
    /// forward on every call — same shape as `Reactor::next_queue_seq`.
    fn next_seq(&self, repo_name: &str) -> rk_core::Result<u64> {
        let mut cache = self.seq_cache.lock().unwrap_or_else(|p| p.into_inner());
        let current = match cache.get(repo_name) {
            Some(v) => *v,
            None => std::fs::read_to_string(self.seq_file(repo_name))
                .ok()
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap_or(0),
        };
        let next = current + 1;
        std::fs::create_dir_all(&self.seq_dir)?;
        std::fs::write(self.seq_file(repo_name), next.to_string())?;
        cache.insert(repo_name.to_string(), next);
        Ok(next)
    }

    fn enqueue(&self, mut entry: LandingQueueEntry) -> rk_core::Result<u64> {
        let seq = self.next_seq(&entry.repo_name)?;
        entry.seq = seq;
        let payload = serde_json::to_value(&entry)
            .map_err(|e| rk_core::Error::other(format!("landing queue entry: {e}")))?;
        let tuple = Tuple::new(
            Category::Event,
            entry.repo_name.clone(),
            LANDING_QUEUE_IDENTITY,
            "daemon",
            payload,
        )
        .with_lifecycle(Lifecycle::Furniture);
        self.space.out(tuple)?;
        Ok(seq)
    }

    /// Claim the oldest queued candidate for `(repo_name, target)`, deleting
    /// its tuple from the durable queue before returning it — single-consumer
    /// ownership transfer, matching the design doc's admission model (§2.1).
    /// See the module doc for why this claims at dequeue rather than after
    /// full processing.
    fn dequeue_next(
        &self,
        repo_name: &str,
        target: &str,
    ) -> rk_core::Result<Option<LandingQueueEntry>> {
        let mut pending = self.space.scan(
            &Pattern::category(Category::Event)
                .scope(repo_name)
                .identity(LANDING_QUEUE_IDENTITY),
        )?;
        pending.retain(|t| t.payload.get("target").and_then(Value::as_str) == Some(target));
        // Order by the durable enqueue sequence, not tuple id — a same-
        // millisecond RecordId suffix is random (see Reactor::drain_queued_fires).
        pending.sort_by_key(|t| {
            let seq = t.payload.get("seq").and_then(Value::as_u64).unwrap_or(0);
            (seq, t.id)
        });
        let Some(tuple) = pending.into_iter().next() else {
            return Ok(None);
        };
        let entry: LandingQueueEntry = serde_json::from_value(tuple.payload.clone())
            .map_err(|e| rk_core::Error::other(format!("landing queue entry: {e}")))?;
        self.space.delete(tuple.id)?;
        Ok(Some(entry))
    }

    /// Every distinct `(repo_name, target)` key with at least one candidate
    /// queued right now — what a polling consumer cycle iterates over.
    fn pending_keys(&self) -> rk_core::Result<Vec<(String, String)>> {
        let all = self
            .space
            .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))?;
        let mut keys = BTreeSet::new();
        for tuple in all {
            if let Some(target) = tuple.payload.get("target").and_then(Value::as_str) {
                keys.insert((tuple.scope.clone(), target.to_string()));
            }
        }
        Ok(keys.into_iter().collect())
    }
}

/// Filesystem-safe rendering of a repo name or branch for use as a path
/// component (queue seq files, gate worktree paths) — repo names are short
/// slugs in practice, but this stays safe if one ever isn't.
fn sanitize_path_component(raw: &str) -> String {
    raw.chars()
        .map(|c| if c == '/' || c == '\\' { '_' } else { c })
        .collect()
}

/// One resolved named check plus the env pairs and wall-clock bound it runs
/// with, in the order [`LandingPipeline::gate_plan`] wants them run.
type GatePlan = Vec<(rk_workflow::Check, Vec<(String, String)>, Duration)>;

/// The gate/tier tuning steward.cue exposes as workflow params
/// (`examples/workflows/steward.cue`'s `params` block) — same names, same
/// defaults, now owned by the daemon-native pipeline instead of CUE.
#[derive(Debug, Clone)]
pub(crate) struct GateConfig {
    /// The repo's real named check — the run gate's teeth. Looked up in
    /// `<repo>/.rk/checks.cue`.
    pub(crate) check_name: String,
    /// POLICY GUARDRAIL: an ERE matched against changed file paths.
    pub(crate) protected_paths: String,
    /// DIFF-SCOPE GUARDRAIL: 0 disables the budget.
    pub(crate) max_diff_files: u64,
    pub(crate) max_diff_lines: u64,
    /// Wall-clock bound for the run gate specifically (the two policy gates
    /// use their own fixed [`POLICY_GATE_TIMEOUT`]).
    pub(crate) gate_timeout: Duration,
}

impl Default for GateConfig {
    fn default() -> Self {
        Self {
            check_name: "verify".into(),
            protected_paths: r"(^|/)(\.github|\.rk|migrations)/".into(),
            max_diff_files: 50,
            max_diff_lines: 2000,
            gate_timeout: Duration::from_secs(60 * 60),
        }
    }
}

/// What became of one dequeued candidate.
#[derive(Debug)]
pub(crate) enum LandingOutcome {
    /// Gates passed and the candidate needed no LLM judgment (doc-only/trivial
    /// diff) — routed straight to `Supervisor::land`. Carries `land`'s own
    /// result JSON (`merged`, `delivered`, ...).
    Landed(Value),
    /// A gate failed or timed out. `run_check_in` already recorded the
    /// durable `gate-failure` artifact; the branch is simply left unmerged —
    /// nothing further happens to it here.
    GateHeld,
    /// Gates passed but the diff needs a reviewer's judgment before it can
    /// land. The T2→T3 hand-off point (design doc §3): T3 plugs the
    /// verdict-cache probe and APPROVE/REWORK/STOP routing in here.
    NeedsReview(LandingQueueEntry),
}

/// Daemon-native consumer: dequeues a candidate, runs its gates in a
/// persistent per-`(repo,target)` gate worktree, and routes a clean
/// doc-only/trivial pass straight to `Supervisor::land` — no agent spawn.
pub(crate) struct LandingPipeline {
    supervisor: Arc<Supervisor>,
    engine: Arc<WorkflowEngine>,
    layout: Layout,
    queue: LandingQueue,
    gates: GateConfig,
}

impl LandingPipeline {
    pub(crate) fn new(
        space: Space,
        supervisor: Arc<Supervisor>,
        engine: Arc<WorkflowEngine>,
        layout: Layout,
    ) -> Self {
        let queue = LandingQueue::new(space, &layout);
        Self {
            supervisor,
            engine,
            layout,
            queue,
            gates: GateConfig::default(),
        }
    }

    pub(crate) fn enqueue(&self, entry: LandingQueueEntry) -> rk_core::Result<u64> {
        self.queue.enqueue(entry)
    }

    /// Process exactly one candidate for `(repo_name, target)`, or `None` if
    /// the key has nothing queued. Single-consumer per key by construction:
    /// callers wanting to fully drain a key call this in a loop (see
    /// [`Self::drain_key`]).
    pub(crate) async fn process_next(
        &self,
        repo_name: &str,
        target: &str,
    ) -> rk_core::Result<Option<LandingOutcome>> {
        let Some(entry) = self.queue.dequeue_next(repo_name, target)? else {
            return Ok(None);
        };
        Ok(Some(self.process_entry(entry).await?))
    }

    async fn process_entry(&self, entry: LandingQueueEntry) -> rk_core::Result<LandingOutcome> {
        if !self.run_gates(&entry).await? {
            return Ok(LandingOutcome::GateHeld);
        }
        if matches!(entry.diff_class.as_str(), "doc-only" | "trivial") {
            let result = self
                .supervisor
                .land(
                    Path::new(&entry.repo_path),
                    &entry.branch,
                    &entry.target,
                    false,
                )
                .await?;
            return Ok(LandingOutcome::Landed(result));
        }
        Ok(LandingOutcome::NeedsReview(entry))
    }

    /// Drain every candidate currently queued for `(repo_name, target)`,
    /// oldest first, returning one outcome per candidate in processing order.
    pub(crate) async fn drain_key(
        &self,
        repo_name: &str,
        target: &str,
    ) -> rk_core::Result<Vec<LandingOutcome>> {
        let mut outcomes = Vec::new();
        while let Some(outcome) = self.process_next(repo_name, target).await? {
            outcomes.push(outcome);
        }
        Ok(outcomes)
    }

    /// One polling pass: discover every `(repo_name, target)` key with a
    /// candidate queued and drain each independently. Distinct keys never
    /// interleave with each other's ordering (each is drained to empty
    /// before the next starts), but nothing here serializes two DIFFERENT
    /// keys against each other — matching the design doc's "different target
    /// branches in the same repo merge concurrently" granularity.
    pub(crate) async fn run_cycle(&self) -> rk_core::Result<Vec<LandingOutcome>> {
        let mut outcomes = Vec::new();
        for (repo_name, target) in self.queue.pending_keys()? {
            outcomes.extend(self.drain_key(&repo_name, &target).await?);
        }
        Ok(outcomes)
    }

    fn gate_worktree_path(&self, repo_name: &str, target: &str) -> PathBuf {
        self.layout
            .home()
            .join("gate-worktrees")
            .join(sanitize_path_component(repo_name))
            .join(sanitize_path_component(target))
    }

    /// Run the same three gates `steward.cue`'s `_gates` block runs today
    /// (POLICY, DIFF-SCOPE, the repo's named `verify` check) against a
    /// persistent daemon-owned worktree reset to the candidate's tip.
    /// Returns `Ok(true)` only if every gate reported `verdict: "pass"`.
    async fn run_gates(&self, entry: &LandingQueueEntry) -> rk_core::Result<bool> {
        let repo_path = PathBuf::from(&entry.repo_path);
        let git_repo = {
            let repo_path = repo_path.clone();
            blocking(move || rk_git::Repo::discover(&repo_path)).await?
        };
        let gate_dir = self.gate_worktree_path(&entry.repo_name, &entry.target);
        {
            let git_repo = git_repo.clone();
            let gate_dir = gate_dir.clone();
            blocking(move || git_repo.ensure_gate_worktree(&gate_dir)).await?;
        }
        {
            let git_repo = git_repo.clone();
            let gate_dir = gate_dir.clone();
            let sha = entry.head_sha.clone();
            blocking(move || git_repo.reset_gate_worktree(&gate_dir, &sha)).await?;
        }

        let checks_file = repo_path.join(".rk").join("checks.cue");
        let plan = self.gate_plan(&checks_file, &entry.target)?;

        let id = format!("landing:{}", entry.branch);
        for (check, env, timeout) in plan {
            let resolved = ResolvedRun {
                command: check.command.clone(),
                cwd: check.cwd.clone(),
                // Left unset deliberately: the pipeline reads `verdict` off
                // the result instead of relying on run_check_in's inline
                // exit-gate Err path, so a failing gate is a clean Ok(false)
                // rather than a propagated error.
                expect_exit: None,
                timeout: check
                    .timeout
                    .clone()
                    .unwrap_or_else(|| DEFAULT_CHECK_TIMEOUT.into()),
                on_timeout: OnTimeout::Fail,
                environment_policy: check.environment_policy,
                retry_on_fail: 0,
            };
            let outcome = self
                .engine
                .run_check_in(
                    &id,
                    &entry.repo_name,
                    "daemon",
                    &gate_dir,
                    &resolved.command,
                    &resolved,
                    &env,
                    timeout,
                    None,
                )
                .await;
            match outcome {
                Ok(result) if result.get("verdict").and_then(Value::as_str) == Some("pass") => {}
                Ok(_) => return Ok(false),
                Err(e) => {
                    // Only reachable when `on_timeout: Fail` turns a blown
                    // budget into an Err — `record_gate_failure` already ran
                    // before it did. Any other run_check_in Err (a `sh` that
                    // could not even spawn) is an infra fault, not a verdict
                    // on the branch, but is treated the same way here:
                    // fail-closed, hold rather than land.
                    warn!(error = %e, check = %check.name, branch = %entry.branch, "landing pipeline: gate errored, holding branch");
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Resolve the three named checks (POLICY, DIFF-SCOPE, the run gate) into
    /// `(check, env, timeout)` triples in the order they must run — same
    /// registry lookup `WorkflowEngine::find_check` does, reimplemented here
    /// because that method is private to `workflow_exec` and this pipeline
    /// has no `run` step / `ctx.active_agent` to go through.
    fn gate_plan(&self, checks_file: &Path, target: &str) -> rk_core::Result<GatePlan> {
        if !checks_file.exists() {
            return Err(rk_core::Error::other(format!(
                "landing pipeline: no check registry at {}",
                checks_file.display()
            )));
        }
        let checks = rk_workflow::load_checks(checks_file)?;
        let find = |name: &str| {
            checks
                .iter()
                .find(|c| c.name == name)
                .cloned()
                .ok_or_else(|| {
                    rk_core::Error::other(format!(
                        "landing pipeline: no check named '{name}' in {}",
                        checks_file.display()
                    ))
                })
        };

        let protected_paths = find(PROTECTED_PATHS_CHECK)?;
        let diff_scope = find(DIFF_SCOPE_CHECK)?;
        let verify = find(&self.gates.check_name)?;

        Ok(vec![
            (
                protected_paths,
                vec![
                    ("RK_CHECK_TARGET".to_string(), target.to_string()),
                    (
                        "RK_CHECK_PROTECTED_PATHS".to_string(),
                        self.gates.protected_paths.clone(),
                    ),
                ],
                POLICY_GATE_TIMEOUT,
            ),
            (
                diff_scope,
                vec![
                    ("RK_CHECK_TARGET".to_string(), target.to_string()),
                    (
                        "RK_CHECK_MAX_DIFF_FILES".to_string(),
                        self.gates.max_diff_files.to_string(),
                    ),
                    (
                        "RK_CHECK_MAX_DIFF_LINES".to_string(),
                        self.gates.max_diff_lines.to_string(),
                    ),
                ],
                POLICY_GATE_TIMEOUT,
            ),
            (verify, Vec::new(), self.gates.gate_timeout),
        ])
    }
}

/// `Repo` git calls and `load_checks`'s `cue` shell-out are synchronous;
/// run them off the async runtime's worker threads. Mirrors
/// `supervisor::blocking_io`, private to that module.
async fn blocking<T, F>(f: F) -> rk_core::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> rk_core::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(|e| {
        rk_core::Error::other(format!("landing pipeline: blocking task failed: {e}"))
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tickets::Tickets;
    use rk_workflow::TierRouting;
    use std::collections::HashMap;
    use std::process::Command;

    fn git(dir: &Path, args: &[&str]) {
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
    }

    fn init_repo(dir: &Path) {
        git(dir, &["init", "-b", "main"]);
        git(dir, &["config", "user.email", "r@x"]);
        git(dir, &["config", "user.name", "R"]);
        std::fs::write(dir.join("README.md"), "# x\n").unwrap();
        git(dir, &["add", "."]);
        git(dir, &["commit", "-m", "init"]);
    }

    fn rev_parse(dir: &Path, rev: &str) -> String {
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(["rev-parse", rev])
            .output()
            .unwrap();
        assert!(out.status.success());
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// Commits `.rk/checks.cue` on whatever branch is currently checked out
    /// — deliberately tracked (`git add .` in a later commit would otherwise
    /// only add it to that ONE branch's tree, so switching back to `main`
    /// removes it entirely, since git drops a file that is tracked on the
    /// branch just left but absent from the branch being checked out into).
    fn write_checks(repo: &Path, src: &str) {
        let rk_dir = repo.join(".rk");
        std::fs::create_dir_all(&rk_dir).unwrap();
        std::fs::write(rk_dir.join("checks.cue"), src).unwrap();
        git(repo, &["add", ".rk/checks.cue"]);
        git(repo, &["commit", "-m", "add checks registry"]);
    }

    const ALL_PASS_CHECKS: &str = r#"
checks: [
    {name: "steward-protected-paths", command: "true", timeout: "30s"},
    {name: "steward-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "true", timeout: "30s"},
]
"#;

    const VERIFY_FAILS_CHECKS: &str = r#"
checks: [
    {name: "steward-protected-paths", command: "true", timeout: "30s"},
    {name: "steward-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "exit 3", timeout: "30s"},
]
"#;

    fn test_engine(
        layout: Layout,
        supervisor: Arc<Supervisor>,
        space: Space,
    ) -> Arc<WorkflowEngine> {
        let tickets = Arc::new(Tickets::new(space.clone(), "castle".into()));
        Arc::new(WorkflowEngine::new(
            layout,
            supervisor,
            space,
            tickets,
            HashMap::new(),
            TierRouting::default(),
            "fake".into(),
            false,
            true,
            false,
            Vec::new(),
            Vec::new(),
            0,
        ))
    }

    fn test_pipeline(home: &Path, space: Space) -> LandingPipeline {
        let layout = Layout::at(home);
        let tickets = Arc::new(Tickets::new(space.clone(), "castle".into()));
        let supervisor = Arc::new(
            Supervisor::new(
                layout.clone(),
                "castle".into(),
                "fake".into(),
                rk_ledger::Budget::default(),
                rk_ledger::FleetBudget::default(),
                space.clone(),
                tickets,
            )
            .unwrap(),
        );
        let engine = test_engine(layout.clone(), supervisor.clone(), space.clone());
        LandingPipeline::new(space, supervisor, engine, layout)
    }

    /// The T1->T2 interface, consumed from the module T2 lives in: build the
    /// resolved-run input shape by hand, point it at a bare directory (a
    /// stand-in for the persistent gate worktree), and run the extracted
    /// check runner with no agent and no workflow context.
    #[tokio::test]
    async fn landing_consumer_runs_a_check_in_a_bare_gate_dir() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let space = Space::open_in_memory().unwrap();
        let tickets = Arc::new(Tickets::new(space.clone(), "castle".into()));
        let supervisor = Arc::new(
            Supervisor::new(
                layout.clone(),
                "castle".into(),
                "fake".into(),
                rk_ledger::Budget::default(),
                rk_ledger::FleetBudget::default(),
                space.clone(),
                tickets.clone(),
            )
            .unwrap(),
        );
        let engine = WorkflowEngine::new(
            layout,
            supervisor,
            space,
            tickets,
            HashMap::new(),
            TierRouting::default(),
            "fake".into(),
            false,
            true,
            false,
            Vec::new(),
            Vec::new(),
            0,
        );

        let gate_dir = tempfile::tempdir().unwrap();
        std::fs::write(gate_dir.path().join("candidate.txt"), "tip\n").unwrap();
        let resolved = ResolvedRun {
            command: "cat candidate.txt".into(),
            cwd: None,
            expect_exit: Some(0),
            timeout: "5s".into(),
            on_timeout: OnTimeout::Fail,
            environment_policy: rk_workflow::CheckEnvironmentPolicy::StripRkSpawn,
            retry_on_fail: 0,
        };
        let result = engine
            .run_check_in(
                "landing-t1-interface",
                "/repo",
                "daemon",
                gate_dir.path(),
                &resolved.command,
                &resolved,
                &[],
                Duration::from_secs(5),
                None,
            )
            .await
            .unwrap();
        assert_eq!(result["verdict"], "pass");
        assert_eq!(result["exit"], 0);
        assert!(result["stdout"].as_str().unwrap().contains("tip"));
    }

    #[test]
    fn queue_orders_fifo_within_a_key_and_independent_across_keys() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let space = Space::open_in_memory().unwrap();
        let queue = LandingQueue::new(space, &layout);

        let entry = |repo: &str, target: &str, branch: &str| LandingQueueEntry {
            repo_name: repo.into(),
            repo_path: format!("/repos/{repo}"),
            branch: branch.into(),
            target: target.into(),
            head_sha: "deadbeef".into(),
            diff_class: "trivial".into(),
            task: "t".into(),
            seq: 0,
        };

        // Interleave two repos (independent keys) and, within "alpha", two
        // different targets (also independent keys) so a naive scan-without-
        // filter would misorder or cross-deliver.
        queue.enqueue(entry("alpha", "main", "b1")).unwrap();
        queue.enqueue(entry("beta", "main", "c1")).unwrap();
        queue.enqueue(entry("alpha", "release", "r1")).unwrap();
        queue.enqueue(entry("alpha", "main", "b2")).unwrap();
        queue.enqueue(entry("beta", "main", "c2")).unwrap();
        queue.enqueue(entry("alpha", "main", "b3")).unwrap();

        let alpha_main: Vec<String> =
            std::iter::from_fn(|| queue.dequeue_next("alpha", "main").unwrap())
                .map(|e| e.branch)
                .collect();
        assert_eq!(alpha_main, vec!["b1", "b2", "b3"]);

        let alpha_release: Vec<String> =
            std::iter::from_fn(|| queue.dequeue_next("alpha", "release").unwrap())
                .map(|e| e.branch)
                .collect();
        assert_eq!(alpha_release, vec!["r1"]);

        let beta_main: Vec<String> =
            std::iter::from_fn(|| queue.dequeue_next("beta", "main").unwrap())
                .map(|e| e.branch)
                .collect();
        assert_eq!(beta_main, vec!["c1", "c2"]);

        // Every key drained to empty; nothing left queued anywhere.
        assert!(queue.pending_keys().unwrap().is_empty());
    }

    #[tokio::test]
    async fn doc_only_completion_lands_with_zero_agent_spawns() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        write_checks(repo_dir.path(), ALL_PASS_CHECKS);
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::create_dir_all(repo_dir.path().join("docs")).unwrap();
        std::fs::write(repo_dir.path().join("docs").join("note.md"), "note\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "docs: add note"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "docs-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "feature".into(),
                target: "main".into(),
                head_sha,
                diff_class: "doc-only".into(),
                task: "add note".into(),
                seq: 0,
            })
            .unwrap();

        let outcomes = pipeline.drain_key("docs-repo", "main").await.unwrap();
        assert_eq!(outcomes.len(), 1);
        let LandingOutcome::Landed(result) = &outcomes[0] else {
            panic!("expected Landed, got {:?}", outcomes[0]);
        };
        assert_eq!(result["merged"], true, "result: {result}");

        let main_listing = Command::new("git")
            .arg("-C")
            .arg(repo_dir.path())
            .args(["ls-tree", "--name-only", "-r", "main"])
            .output()
            .unwrap();
        let listing = String::from_utf8_lossy(&main_listing.stdout);
        assert!(listing.contains("docs/note.md"), "listing: {listing}");

        // No agent was ever spawned to reach this outcome.
        assert!(space
            .scan(&Pattern::category(Category::Event).identity("agent_spawned"))
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn failing_gate_produces_gate_failure_artifact_and_holds_branch() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        write_checks(repo_dir.path(), VERIFY_FAILS_CHECKS);
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        let main_before = rev_parse(repo_dir.path(), "main");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "code-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "feature".into(),
                target: "main".into(),
                head_sha,
                diff_class: "doc-only".into(),
                task: "add src".into(),
                seq: 0,
            })
            .unwrap();

        let outcomes = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(outcomes[0], LandingOutcome::GateHeld));

        let failures = space
            .scan(
                &Pattern::category(Category::Artifact)
                    .scope("code-repo")
                    .identity("gate-failure"),
            )
            .unwrap();
        assert_eq!(failures.len(), 1, "failures: {failures:?}");
        assert_eq!(failures[0].payload["verdict"], "fail");

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_eq!(main_before, main_after, "branch must not have landed");
    }
}
