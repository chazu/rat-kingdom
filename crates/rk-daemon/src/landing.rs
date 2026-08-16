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
//! Restart-safety for an in-flight candidate (§2.6 of the design doc) is
//! T4's contribution: a claimed candidate is marked `running_gates` (or, once
//! a review is requested, `awaiting_review`) IN the durable queue tuple
//! itself rather than deleted — [`LandingQueue::claim_next`] transitions
//! status instead of removing the entry, and [`LandingQueue::remove`] only
//! runs once [`LandingPipeline::process_entry`] reaches a terminal outcome.
//! A restart's [`LandingPipeline::run_cycle`] poll re-discovers any entry
//! left `running_gates`/`awaiting_review` by a crashed prior process exactly
//! the same way it discovers a fresh `queued` one — [`LandingQueue::claim_next`]
//! does not filter on status — and reprocessing is safe because every step
//! downstream is independently idempotent: gates are a stateless
//! checkout+shell re-run, [`LandingPipeline::request_review`] resolves to the
//! SAME workflow instance on a repeat call (a stable id derived from the
//! candidate's work key, not a fresh random one), and a repeat
//! `Supervisor::land` on an already-merged branch is a clean CAS no-op
//! (design doc §1.1). See `crates/rk-daemon/tests/landing_pipeline_e2e.rs`
//! for the restart-mid-gate and restart-mid-review-wait proofs.
//!
//! `work_key = (repo, branch, head_sha)` dedup against a redelivered
//! completion (a reactor retry after a crash, an operator manually
//! re-triggering) is [`LandingPipeline::enqueue`]'s job: it probes a durable
//! `landing_processed` marker — written by [`LandingPipeline::process_entry`]
//! on every terminal outcome — before ever writing a new queue tuple, and
//! silently drops (`Ok(None)`) a work key already fully handled rather than
//! re-enqueueing it. `Supervisor::land`'s CAS already makes a literal
//! double-`land` call harmless; this dedup exists to also skip the
//! gate-run/review-request work a redelivery would otherwise repeat for no
//! reason.
//!
//! The completion feed itself — how a candidate gets from a rat's
//! `harness_result` to [`LandingPipeline::enqueue`] — is
//! `crate::reactor::Reactor`'s `action: "land"` trigger dispatch
//! (`crates/rk-daemon/src/reactor.rs`, design doc §2.1 option (a)): a
//! `#Trigger` with `action: "land"` reuses the reactor's existing
//! `(trigger, tuple)` dedup marker, rate cap, and cursor-based
//! restart-safety, and calls `LandingPipeline::enqueue` directly instead of
//! launching a workflow instance. `#![allow(dead_code)]` below covers the
//! items this module's own tests exercise directly without going through
//! that live wiring.
#![allow(dead_code)]

use crate::supervisor::Supervisor;
use crate::tickets::{NewTicket, Tickets};
use crate::workflow_exec::{OnTimeout, ResolvedRun, WorkflowEngine};
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_space::Space;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::warn;

/// Identity of a durably-queued landing candidate (`Furniture`, scoped to the
/// repo it belongs to) — the T2 counterpart to the reactor's
/// `reactor_queued_fire` (`crates/rk-daemon/src/reactor.rs`).
const LANDING_QUEUE_IDENTITY: &str = "landing_queue_entry";

/// Identity of the durable `work_key = (repo, branch, head_sha)` dedup
/// marker (`Furniture`, scoped to the repo), written by
/// [`LandingPipeline::process_entry`] on every terminal outcome. Probed by
/// [`LandingPipeline::enqueue`] before writing a new queue tuple, so a
/// redelivered completion for an already-fully-processed candidate is
/// dropped rather than reprocessed (design doc §2.6).
const LANDING_PROCESSED_IDENTITY: &str = "landing_processed";

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

/// Identity of a landing candidate's verdict artifact — Phase 2's
/// commit-keyed cache (`Pattern::for_commit`, §1.3 of the design doc),
/// written by the reviewer itself: `rk out artifact <repo> review --payload
/// {...}` (`examples/workflows/steward-review.cue`).
const REVIEW_ARTIFACT_IDENTITY: &str = "review";

/// Identity of the steward's escalation `need` tuple. Matches
/// `examples/workflows/steward.cue`'s `steward-report-stop`/
/// `steward-report-unknown-verdict`/`steward-report-timeout` named checks,
/// which all write `(need, <repo>, steward)` — `rk inbox` already ranks this
/// identity, so escalating through the identical shape keeps operator-facing
/// behavior unchanged even though the write is now a direct `Space::out`
/// call instead of a shelled-out `rk out need` (§1.5).
const STEWARD_NEED_IDENTITY: &str = "steward";

/// Name of the shrunk, review-only workflow definition (design doc §2.5) —
/// `examples/workflows/steward-review.cue`. [`LandingPipeline::request_review`]
/// invokes it programmatically on a verdict-cache miss; it is never
/// reactor-fired.
const REVIEW_WORKFLOW: &str = "steward-review";

/// One landing candidate: a completed rat's branch, gated then routed toward
/// `Supervisor::land` (or, once T3 lands, a reviewer's verdict). Mirrors the
/// reactor's queued-fire tuple shape (`repo_name`/`repo_path` as two
/// distinct fields — the first is the tuple scope and ticket/artifact scope,
/// the second is the filesystem root `Repo::discover` and `Supervisor::land`
/// need) rather than inventing a new convention.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
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
    /// In-flight progress marker (design doc §2.6), transitioned by
    /// [`LandingQueue::claim_next`]/[`LandingQueue::set_status`] — NOT an
    /// admission gate: [`LandingQueue::claim_next`] considers every status
    /// eligible, so a restart re-discovers a `RunningGates`/`AwaitingReview`
    /// entry a crashed prior process left behind exactly like a fresh
    /// `Queued` one. Purely diagnostic plus the record of "was this claimed
    /// at least once" for anyone reading the queue directly (`rk scan`).
    #[serde(default)]
    pub(crate) status: LandingEntryStatus,
}

/// See [`LandingQueueEntry::status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LandingEntryStatus {
    #[default]
    Queued,
    RunningGates,
    AwaitingReview,
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
        entry.status = LandingEntryStatus::Queued;
        self.write(&entry)?;
        Ok(seq)
    }

    /// Write (or overwrite, via delete-then-out — tuples have no in-place
    /// update) the durable tuple representing `entry`'s current state.
    fn write(&self, entry: &LandingQueueEntry) -> rk_core::Result<()> {
        let payload = serde_json::to_value(entry)
            .map_err(|e| rk_core::Error::other(format!("landing queue entry: {e}")))?;
        let tuple = Tuple::new(
            Category::Event,
            entry.repo_name.clone(),
            LANDING_QUEUE_IDENTITY,
            "daemon",
            payload,
        )
        .with_lifecycle(Lifecycle::Furniture);
        self.space.out(tuple)
    }

    /// Find the current durable tuple for `entry` — matched by its `seq`
    /// within `repo_name` scope, since `seq` is a monotonic per-repo counter
    /// ([`Self::next_seq`]) and therefore unique. `None` once the entry has
    /// been [`Self::remove`]d (or if it was never enqueued at all — a caller
    /// driving [`LandingPipeline::process_entry`] directly, bypassing the
    /// queue, has no durable tuple to find; every method here treats that as
    /// a harmless no-op rather than an error).
    fn find(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<Tuple>> {
        let pending = self.space.scan(
            &Pattern::category(Category::Event)
                .scope(&entry.repo_name)
                .identity(LANDING_QUEUE_IDENTITY),
        )?;
        Ok(pending
            .into_iter()
            .find(|t| t.payload.get("seq").and_then(Value::as_u64) == Some(entry.seq)))
    }

    /// Claim the oldest candidate for `(repo_name, target)` — regardless of
    /// its current [`LandingEntryStatus`] — and durably transition it to
    /// `RunningGates`. Deliberately does NOT delete the tuple: a candidate
    /// stays visible in the durable queue for the whole of its processing, so
    /// a crash mid-gate-run or mid-review-wait leaves it discoverable by the
    /// next [`Self::claim_next`] call rather than silently dropping it (see
    /// the module doc's restart-safety note). Single-consumer ownership
    /// transfer within one process is still exactly-once: nothing else in
    /// this process calls `claim_next` again for the same key until the
    /// candidate just claimed finishes processing.
    fn claim_next(
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
        let mut entry: LandingQueueEntry = serde_json::from_value(tuple.payload.clone())
            .map_err(|e| rk_core::Error::other(format!("landing queue entry: {e}")))?;
        self.space.delete(tuple.id)?;
        entry.status = LandingEntryStatus::RunningGates;
        self.write(&entry)?;
        Ok(Some(entry))
    }

    /// Durably transition an already-claimed `entry` to `status` (delete the
    /// current tuple, write a fresh one). A no-op if `entry` has no durable
    /// tuple right now (see [`Self::find`]'s doc).
    fn set_status(
        &self,
        entry: &LandingQueueEntry,
        status: LandingEntryStatus,
    ) -> rk_core::Result<()> {
        let Some(tuple) = self.find(entry)? else {
            return Ok(());
        };
        self.space.delete(tuple.id)?;
        let mut updated = entry.clone();
        updated.status = status;
        self.write(&updated)
    }

    /// Remove `entry`'s durable tuple — called once processing reaches a
    /// terminal [`LandingOutcome`]. A no-op if it has none (see
    /// [`Self::find`]'s doc).
    fn remove(&self, entry: &LandingQueueEntry) -> rk_core::Result<()> {
        let Some(tuple) = self.find(entry)? else {
            return Ok(());
        };
        self.space.delete(tuple.id)?;
        Ok(())
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

/// A durable workflow instance id, stable across repeat calls for the SAME
/// work key — the request-review counterpart to
/// `reactor::stable_workflow_instance_id`. Deriving it from `(repo, branch,
/// head_sha)` rather than a fresh random id per call is what makes
/// [`LandingPipeline::request_review`] safe to invoke twice for the same
/// candidate (a restart-driven reprocess): `run_owned_with_id` resolves the
/// second call to the first call's already-running (or already-finished)
/// instance instead of spawning a duplicate reviewer.
fn review_instance_id(entry: &LandingQueueEntry) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(
        format!("{}@{}@{}", entry.repo_name, entry.branch, entry.head_sha).as_bytes(),
    );
    format!("landing-review-{}", hex::encode(&digest[..16]))
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
    /// Wall-clock bound for a review request: how long
    /// [`LandingPipeline::request_review`] parks on the reviewer's verdict
    /// tuple before treating the candidate as a STOP-equivalent hold (design
    /// doc §2.3 step 3) — matches `examples/workflows/steward.cue`'s
    /// `reviewTimeout` param default.
    pub(crate) review_timeout: Duration,
}

impl Default for GateConfig {
    fn default() -> Self {
        Self {
            check_name: "verify".into(),
            protected_paths: r"(^|/)(\.github|\.rk|migrations)/".into(),
            max_diff_files: 50,
            max_diff_lines: 2000,
            gate_timeout: Duration::from_secs(60 * 60),
            review_timeout: Duration::from_secs(15 * 60),
        }
    }
}

/// What became of one dequeued candidate.
#[derive(Debug)]
pub(crate) enum LandingOutcome {
    /// Gates passed and the candidate either needed no LLM judgment
    /// (doc-only/trivial diff) or got an APPROVE (fresh or cached) — routed
    /// straight to `Supervisor::land`. Carries `land`'s own result JSON
    /// (`merged`, `delivered`, ...).
    Landed(Value),
    /// A gate failed or timed out. `run_check_in` already recorded the
    /// durable `gate-failure` artifact; the branch is simply left unmerged —
    /// nothing further happens to it here.
    GateHeld,
    /// The reviewer (fresh or cached) recommended REWORK: a follow-up ticket
    /// was filed directly (`Tickets::create`, §1.5) and the branch held
    /// unmerged — no `dismiss` is needed since no agent worktree exists for
    /// the candidate itself.
    ReworkFiled(Tuple),
    /// STOP, an unrecognized verdict, or a review that never produced one
    /// within `GateConfig::review_timeout` — all three get the same "genuine
    /// human judgment call" treatment (design doc §2.4): a `need` tuple was
    /// written directly (`Space::out`, §1.5) and the branch held unmerged.
    Escalated(Tuple),
}

/// Daemon-native consumer: dequeues a candidate, runs its gates in a
/// persistent per-`(repo,target)` gate worktree, and routes a clean
/// doc-only/trivial pass straight to `Supervisor::land` — no agent spawn.
pub(crate) struct LandingPipeline {
    supervisor: Arc<Supervisor>,
    engine: Arc<WorkflowEngine>,
    tickets: Arc<Tickets>,
    layout: Layout,
    queue: LandingQueue,
    /// Kept alongside `queue`'s own clone for the review-integration calls
    /// T3 adds (`Space::scan`/`rd`/`out`, §1.3/§1.5) — none of which go
    /// through the queue.
    space: Space,
    gates: GateConfig,
}

impl LandingPipeline {
    pub(crate) fn new(
        space: Space,
        supervisor: Arc<Supervisor>,
        engine: Arc<WorkflowEngine>,
        tickets: Arc<Tickets>,
        layout: Layout,
    ) -> Self {
        let queue = LandingQueue::new(space.clone(), &layout);
        Self {
            supervisor,
            engine,
            tickets,
            layout,
            queue,
            space,
            gates: GateConfig::default(),
        }
    }

    /// Enqueue a fresh completion as a landing candidate, guarded by the
    /// `work_key = (repo, branch, head_sha)` dedup (module doc, design doc
    /// §2.6): `Ok(None)` means this exact candidate was already fully
    /// processed (a `landing_processed` marker exists) and nothing was
    /// written — a redelivered completion tuple is dropped here rather than
    /// repeating gate/review work. `Ok(Some(seq))` is the fresh queue
    /// position, as before.
    pub(crate) fn enqueue(&self, entry: LandingQueueEntry) -> rk_core::Result<Option<u64>> {
        if self.already_processed(&entry)? {
            return Ok(None);
        }
        Ok(Some(self.queue.enqueue(entry)?))
    }

    /// Has `entry`'s exact `(repo, branch, head_sha)` already reached a
    /// terminal [`LandingOutcome`]? See [`Self::mark_processed`], the write
    /// side of this marker.
    fn already_processed(&self, entry: &LandingQueueEntry) -> rk_core::Result<bool> {
        let pattern = Pattern::for_commit(
            Category::Event,
            LANDING_PROCESSED_IDENTITY,
            &entry.branch,
            &entry.head_sha,
        )
        .scope(&entry.repo_name);
        Ok(!self.space.scan(&pattern)?.is_empty())
    }

    /// Durably record that `entry`'s work key reached a terminal outcome —
    /// the write side of [`Self::already_processed`]'s dedup probe. Called
    /// from every terminal return in [`Self::process_entry`], independent of
    /// whether `entry` arrived through the queue or a direct call (a caller
    /// bypassing the queue for testing still gets the same double-land
    /// protection on its next `enqueue`).
    fn mark_processed(
        &self,
        entry: &LandingQueueEntry,
        outcome: &LandingOutcome,
    ) -> rk_core::Result<()> {
        let outcome_str = match outcome {
            LandingOutcome::Landed(_) => "landed",
            LandingOutcome::GateHeld => "gate-held",
            LandingOutcome::ReworkFiled(_) => "rework-filed",
            LandingOutcome::Escalated(_) => "escalated",
        };
        let tuple = Tuple::new(
            Category::Event,
            entry.repo_name.clone(),
            LANDING_PROCESSED_IDENTITY,
            "daemon",
            json!({
                "branch": entry.branch,
                "target": entry.target,
                "head_sha": entry.head_sha,
                "task": entry.task,
                "outcome": outcome_str,
            }),
        )
        .with_lifecycle(Lifecycle::Furniture);
        self.space.out(tuple)
    }

    /// Process exactly one candidate for `(repo_name, target)`, or `None` if
    /// the key has nothing queued. Single-consumer per key by construction:
    /// callers wanting to fully drain a key call this in a loop (see
    /// [`Self::drain_key`]).
    ///
    /// `claim_next` transitions the durable entry to `RunningGates` rather
    /// than deleting it (restart-safety, module doc); this only removes it
    /// once [`Self::process_entry`] returns a terminal outcome. On an `Err`
    /// (an infra fault, not a verdict on the branch — a git call that failed,
    /// a `Supervisor::land` call that errored rather than cleanly reporting
    /// `merged: false`) the entry is deliberately left in place so the next
    /// poll cycle retries it instead of losing the candidate.
    pub(crate) async fn process_next(
        &self,
        repo_name: &str,
        target: &str,
    ) -> rk_core::Result<Option<LandingOutcome>> {
        let Some(entry) = self.queue.claim_next(repo_name, target)? else {
            return Ok(None);
        };
        let result = self.process_entry(&entry).await;
        if result.is_ok() {
            self.queue.remove(&entry)?;
        }
        result.map(Some)
    }

    async fn process_entry(&self, entry: &LandingQueueEntry) -> rk_core::Result<LandingOutcome> {
        if !self.run_gates(entry).await? {
            let outcome = LandingOutcome::GateHeld;
            self.mark_processed(entry, &outcome)?;
            return Ok(outcome);
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
            let outcome = LandingOutcome::Landed(result);
            self.mark_processed(entry, &outcome)?;
            return Ok(outcome);
        }
        let verdict = self.review_verdict(entry).await?;
        let outcome = self.route_verdict(entry, verdict.as_deref()).await?;
        self.mark_processed(entry, &outcome)?;
        Ok(outcome)
    }

    /// Resolve a recommendation for `entry`: a hit against Phase 2's
    /// commit-keyed verdict cache (§1.3/§2.3 step 2), or — on a miss — a
    /// fresh review request (§2.3 step 3). `None` means neither produced
    /// one: the review request timed out waiting on the verdict tuple,
    /// routed by the caller as a STOP-equivalent hold (design doc §2.3).
    async fn review_verdict(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<String>> {
        if let Some(cached) = self.cached_verdict(entry)? {
            return Ok(Some(cached));
        }
        self.request_review(entry).await
    }

    /// Non-blocking probe of Phase 2's commit-keyed verdict cache — ANY
    /// prior run's recommendation for this exact `(repo, branch, head_sha)`,
    /// regardless of who wrote it (§1.3). A hit is honored identically to a
    /// fresh verdict — never re-reviewed to shop for a better opinion.
    fn cached_verdict(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<String>> {
        let pattern = Pattern::for_commit(
            Category::Artifact,
            REVIEW_ARTIFACT_IDENTITY,
            &entry.branch,
            &entry.head_sha,
        )
        .scope(&entry.repo_name);
        Ok(self.space.scan(&pattern)?.into_iter().find_map(|t| {
            t.payload
                .get("recommendation")
                .and_then(Value::as_str)
                .map(str::to_string)
        }))
    }

    /// Spawn the shrunk review-only workflow (design doc §2.5,
    /// `examples/workflows/steward-review.cue`) chained onto the candidate
    /// branch, then park on the verdict tuple ITSELF rather than the
    /// workflow instance's own completion (§1.5's `watch_attached_completion`
    /// pattern) — a daemon restart loses nothing: the reviewer keeps working
    /// against its own worktree regardless of the pipeline's state, and the
    /// verdict tuple is durable even though this exact wait is not (§2.6).
    ///
    /// The workflow launch uses a STABLE instance id derived from `entry`'s
    /// work key (`review_instance_id`), not a fresh random one: a daemon
    /// restart re-claims the durable queue entry (still `AwaitingReview` from
    /// before the crash) and calls this again from scratch, and
    /// `run_owned_with_id` returns the EXISTING instance's snapshot instead
    /// of launching a second reviewer for a request already in flight — the
    /// same "never orphan a reviewer" guarantee the design doc's §2.6
    /// prescribes, but also never double-spawns one.
    async fn request_review(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<String>> {
        self.queue
            .set_status(entry, LandingEntryStatus::AwaitingReview)?;

        let mut params = HashMap::new();
        params.insert("taskId".to_string(), Value::String(entry.task.clone()));
        params.insert("branch".to_string(), Value::String(entry.branch.clone()));
        params.insert("repo".to_string(), Value::String(entry.repo_name.clone()));
        params.insert("target".to_string(), Value::String(entry.target.clone()));
        params.insert("headSha".to_string(), Value::String(entry.head_sha.clone()));
        params.insert(
            "reviewTimeout".to_string(),
            Value::String(format!("{}s", self.gates.review_timeout.as_secs())),
        );
        // The engine's `repo` argument is a filesystem path (it feeds
        // `Repo::discover` and repo-local definition resolution), unlike the
        // `repo` WORKFLOW PARAM above, which is the repo's scope name used
        // to address its verdict artifact.
        self.engine.run_owned_with_id(
            review_instance_id(entry),
            REVIEW_WORKFLOW,
            &entry.repo_path,
            params,
            None,
        )?;

        let pattern = Pattern::for_commit(
            Category::Artifact,
            REVIEW_ARTIFACT_IDENTITY,
            &entry.branch,
            &entry.head_sha,
        )
        .scope(&entry.repo_name);
        let verdict = self.space.rd(&pattern, self.gates.review_timeout).await?;
        Ok(verdict.and_then(|t| {
            t.payload
                .get("recommendation")
                .and_then(Value::as_str)
                .map(str::to_string)
        }))
    }

    /// Route a resolved (fresh or cached) recommendation — or `None` on a
    /// review timeout — via direct daemon calls: no shell, no agent auth
    /// token (§1.5/§2.4).
    async fn route_verdict(
        &self,
        entry: &LandingQueueEntry,
        verdict: Option<&str>,
    ) -> rk_core::Result<LandingOutcome> {
        match verdict {
            Some("APPROVE") => {
                let result = self
                    .supervisor
                    .land(
                        Path::new(&entry.repo_path),
                        &entry.branch,
                        &entry.target,
                        false,
                    )
                    .await?;
                Ok(LandingOutcome::Landed(result))
            }
            Some("REWORK") => Ok(LandingOutcome::ReworkFiled(
                self.file_rework_ticket(entry).await?,
            )),
            Some("STOP") => {
                let text = format!(
                    "steward: reviewer returned STOP for {} on {} — needs a human merge \
                     decision; branch held unmerged",
                    entry.task, entry.branch
                );
                Ok(LandingOutcome::Escalated(self.escalate(entry, text)?))
            }
            Some(other) => {
                let text = format!(
                    "steward: unrecognized review verdict '{other}' for {} on {} — branch held \
                     unmerged, needs a human",
                    entry.task, entry.branch
                );
                Ok(LandingOutcome::Escalated(self.escalate(entry, text)?))
            }
            None => {
                let text = format!(
                    "steward: review timed out for {} on {} — no verdict recorded within {}s; \
                     branch held unmerged",
                    entry.task,
                    entry.branch,
                    self.gates.review_timeout.as_secs()
                );
                Ok(LandingOutcome::Escalated(self.escalate(entry, text)?))
            }
        }
    }

    /// File the REWORK follow-up directly through `Tickets::create` (§1.5) —
    /// the same shape `steward-file-rework-ticket` produces today, minus the
    /// shell hop. The branch is left as-is: no agent worktree exists for the
    /// candidate itself, so there is nothing to dismiss.
    async fn file_rework_ticket(&self, entry: &LandingQueueEntry) -> rk_core::Result<Tuple> {
        self.tickets
            .create(NewTicket {
                title: format!("rework: {}", entry.task),
                body: Some(format!(
                    "Steward routed REWORK on branch {}. Read the reviewer notes: rk scan \
                     artifact {}",
                    entry.branch, entry.repo_name
                )),
                scope: Some(entry.repo_name.clone()),
                parent: None,
                priority: "normal".to_string(),
                labels: Vec::new(),
                depends_on: Vec::new(),
                created_by: Some("daemon".to_string()),
                coalesce_key: None,
            })
            .await
    }

    /// Write a `need` tuple directly (§1.5) — the STOP/unrecognized-verdict/
    /// review-timeout escalation shape `rk inbox` already ranks, unshelled.
    fn escalate(&self, entry: &LandingQueueEntry, text: String) -> rk_core::Result<Tuple> {
        let tuple = Tuple::new(
            Category::Need,
            entry.repo_name.clone(),
            STEWARD_NEED_IDENTITY,
            "daemon",
            json!({"agent": "steward", "task": entry.task, "text": text}),
        );
        self.space.out(tuple.clone())?;
        Ok(tuple)
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
    /// candidate queued (or left `RunningGates`/`AwaitingReview` by a crashed
    /// prior process — restart-safety, module doc) and drain each
    /// independently. Distinct keys never interleave with each other's
    /// ordering (each is drained to empty before the next starts), but
    /// nothing here serializes two DIFFERENT keys against each other —
    /// matching the design doc's "different target branches in the same repo
    /// merge concurrently" granularity.
    ///
    /// One key's failure does not abort the whole pass — logged and skipped,
    /// left for the next poll cycle to retry (this drives a live daemon
    /// loop, so one repo's transient fault must not stall every other repo's
    /// landing traffic).
    pub(crate) async fn run_cycle(&self) -> rk_core::Result<Vec<LandingOutcome>> {
        let mut outcomes = Vec::new();
        for (repo_name, target) in self.queue.pending_keys()? {
            match self.drain_key(&repo_name, &target).await {
                Ok(o) => outcomes.extend(o),
                Err(e) => warn!(
                    repo = %repo_name, target = %target, error = %e,
                    "landing pipeline: drain_key failed, will retry next cycle"
                ),
            }
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
    use rk_workflow::TierRouting;
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
        tickets: Arc<Tickets>,
    ) -> Arc<WorkflowEngine> {
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
                tickets.clone(),
            )
            .unwrap(),
        );
        let engine = test_engine(
            layout.clone(),
            supervisor.clone(),
            space.clone(),
            tickets.clone(),
        );
        LandingPipeline::new(space, supervisor, engine, tickets, layout)
    }

    /// Writes a minimal review-only workflow definition at the well-known
    /// resolved path (`<home>/workflows/steward-review.cue`) —
    /// [`REVIEW_WORKFLOW`]'s lookup name — using the `fake` harness so
    /// `request_review` can spawn a real (cheap, scripted) reviewer process
    /// without touching the shipped `examples/workflows/steward-review.cue`,
    /// which pins a real harness/model.
    fn write_review_workflow(layout: &Layout) {
        let dir = layout.workflows_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("steward-review.cue"),
            r#"
package workflow

workflow: {
	name: "steward-review"
	params: {
		taskId:        {type: "string", required: false, default: "unknown"}
		branch:        {type: "string", required: true}
		repo:          {type: "string", required: false, default: "rat-kingdom"}
		target:        {type: "string", required: false, default: "main"}
		headSha:       {type: "string", required: false, default: ""}
		reviewTimeout: {type: "string", required: false, default: "15m"}
	}
	agents: {
		default: {harness: "fake", model: "sonnet"}
	}
	steps: [
		{
			type:   "spawn"
			role:   "reviewer"
			branch: _input.branch
			task: {title: "review", description: "review it"}
		},
		{type: "wait", timeout: _input.reviewTimeout},
		{type: "evaluate", expect: {is_error: false}},
	]
}
"#,
        )
        .unwrap();
    }

    /// Poll until at least `want` `agent_spawned` events are visible, or
    /// panic after a generous budget — the direct measurement that a
    /// reviewer spawn actually happened (or didn't), matching this crate's
    /// existing `workflow_verdict_cache.rs` convention of counting spawns
    /// rather than inferring them from routing alone.
    async fn wait_for_spawn_count(space: &Space, want: usize) -> usize {
        for _ in 0..400 {
            let n = space
                .scan(&Pattern::category(Category::Event).identity("agent_spawned"))
                .unwrap()
                .len();
            if n >= want {
                return n;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for {want} agent_spawned event(s)");
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
            ..Default::default()
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

        // `claim_next` transitions status rather than deleting (T4
        // restart-safety), so a caller that wants the classic "drain to
        // empty" behavior must explicitly `remove` each claimed entry —
        // exactly what `LandingPipeline::process_next` does once processing
        // reaches a terminal outcome.
        let claim_all = |repo: &str, target: &str| {
            let mut branches = Vec::new();
            while let Some(e) = queue.claim_next(repo, target).unwrap() {
                assert_eq!(e.status, LandingEntryStatus::RunningGates);
                branches.push(e.branch.clone());
                queue.remove(&e).unwrap();
            }
            branches
        };

        assert_eq!(claim_all("alpha", "main"), vec!["b1", "b2", "b3"]);
        assert_eq!(claim_all("alpha", "release"), vec!["r1"]);
        assert_eq!(claim_all("beta", "main"), vec!["c1", "c2"]);

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
                ..Default::default()
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
                ..Default::default()
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

    /// Shared setup for the review-integration tests below: a repo with a
    /// "large" (review-needing) candidate branch, checks that always pass,
    /// and back on `main` when it returns. Returns `(repo_dir, head_sha,
    /// main_before)`.
    fn review_candidate_repo() -> (tempfile::TempDir, String, String) {
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        write_checks(repo_dir.path(), ALL_PASS_CHECKS);
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        let main_before = rev_parse(repo_dir.path(), "main");
        git(repo_dir.path(), &["checkout", "main"]);
        (repo_dir, head_sha, main_before)
    }

    fn review_candidate_entry(repo_dir: &Path, head_sha: &str) -> LandingQueueEntry {
        LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: head_sha.to_string(),
            diff_class: "large".into(),
            task: "add src".into(),
            ..Default::default()
        }
    }

    fn verdict_tuple(head_sha: &str, recommendation: &str) -> Tuple {
        Tuple::new(
            Category::Artifact,
            "code-repo",
            REVIEW_ARTIFACT_IDENTITY,
            "some-reviewer",
            json!({
                "task": "add src",
                "recommendation": recommendation,
                "notes": "notes",
                "head_sha": head_sha,
                "branch": "feature",
            }),
        )
    }

    fn no_spawns(space: &Space) {
        assert!(
            space
                .scan(&Pattern::category(Category::Event).identity("agent_spawned"))
                .unwrap()
                .is_empty(),
            "a verdict-cache hit must not spawn a reviewer"
        );
    }

    #[tokio::test]
    async fn cache_hit_skips_spawn_entirely() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();

        let space = Space::open_in_memory().unwrap();
        space.out(verdict_tuple(&head_sha, "APPROVE")).unwrap();

        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(review_candidate_entry(repo_dir.path(), &head_sha))
            .unwrap();

        let outcomes = pipeline.drain_key("code-repo", "main").await.unwrap();
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
        assert!(listing.contains("src.rs"), "listing: {listing}");

        no_spawns(&space);
    }

    #[tokio::test]
    async fn cached_rework_routes_to_ticket_without_spawning() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, main_before) = review_candidate_repo();

        let space = Space::open_in_memory().unwrap();
        space.out(verdict_tuple(&head_sha, "REWORK")).unwrap();

        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(review_candidate_entry(repo_dir.path(), &head_sha))
            .unwrap();

        let outcomes = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert_eq!(outcomes.len(), 1);
        let LandingOutcome::ReworkFiled(ticket) = &outcomes[0] else {
            panic!("expected ReworkFiled, got {:?}", outcomes[0]);
        };
        assert_eq!(ticket.payload["title"], "rework: add src");
        assert_eq!(ticket.scope, "code-repo");

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_eq!(main_before, main_after, "branch must not have landed");

        no_spawns(&space);
    }

    #[tokio::test]
    async fn cache_miss_spawns_one_reviewer_and_routes_on_late_verdict() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();

        let space = Space::open_in_memory().unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        pipeline
            .enqueue(review_candidate_entry(repo_dir.path(), &head_sha))
            .unwrap();

        let drain = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            async move { pipeline.drain_key("code-repo", "main").await }
        });

        // The pipeline is now parked on the verdict tuple, not the
        // reviewer's own instance completion — nothing routes until we
        // supply one.
        assert_eq!(wait_for_spawn_count(&space, 1).await, 1);

        space.out(verdict_tuple(&head_sha, "APPROVE")).unwrap();

        let outcomes = drain.await.unwrap().unwrap();
        assert_eq!(outcomes.len(), 1);
        let LandingOutcome::Landed(result) = &outcomes[0] else {
            panic!("expected Landed, got {:?}", outcomes[0]);
        };
        assert_eq!(result["merged"], true, "result: {result}");

        assert_eq!(
            space
                .scan(&Pattern::category(Category::Event).identity("agent_spawned"))
                .unwrap()
                .len(),
            1,
            "exactly one reviewer must have been spawned"
        );
    }

    #[tokio::test]
    async fn park_and_resume_survives_space_level_restart_with_late_verdict() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow(&layout);
        let (repo_dir, head_sha, main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        // "Before restart": this drives `process_entry` directly rather than
        // through the queue, to isolate the review-integration half from
        // T4's queue-level restart-safety (covered separately by
        // `crates/rk-daemon/tests/landing_pipeline_e2e.rs`): the pipeline
        // parks on the verdict tuple and never gets one before the simulated
        // crash.
        {
            let space = Space::open(&layout.db_path()).unwrap();
            let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
            let handle = tokio::spawn({
                let pipeline = Arc::clone(&pipeline);
                let entry = entry.clone();
                async move { pipeline.process_entry(&entry).await }
            });
            wait_for_spawn_count(&space, 1).await;
            handle.abort();
            let _ = handle.await;
        }

        // "After restart": a fresh `Space` reopened on the SAME durable
        // store. The reviewer — independent of the crashed daemon — finishes
        // late and writes its verdict; simulated here by writing directly,
        // standing in for the reviewer's own `rk out artifact` call landing
        // against the restarted daemon.
        let space = Space::open(&layout.db_path()).unwrap();
        space.out(verdict_tuple(&head_sha, "APPROVE")).unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());

        // The restarted pipeline does not resume the aborted `space.rd`
        // future — it reprocesses the same candidate and finds the verdict
        // through the identical durable pattern the cache probe uses
        // (§1.3/§2.6), so no second reviewer is ever spawned.
        let outcome = pipeline.process_entry(&entry).await.unwrap();
        let LandingOutcome::Landed(result) = &outcome else {
            panic!("expected Landed, got {outcome:?}");
        };
        assert_eq!(result["merged"], true, "result: {result}");

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_ne!(
            main_before, main_after,
            "branch must have landed after the restart"
        );

        assert_eq!(
            space
                .scan(&Pattern::category(Category::Event).identity("agent_spawned"))
                .unwrap()
                .len(),
            1,
            "the restarted pipeline must not spawn a second reviewer once the verdict is cached"
        );
    }
}
