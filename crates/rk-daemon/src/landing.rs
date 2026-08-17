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
//! (design doc §1.1). See the `restart_mid_gate_run_resumes_and_lands` and
//! `park_and_resume_survives_space_level_restart_with_late_verdict` tests
//! below for the restart-mid-gate and restart-mid-review-wait proofs.
//! `LandingQueue::claim_next`/`set_status` write the successor tuple BEFORE
//! deleting the predecessor — not delete-then-write — precisely so a daemon
//! crash landing in that narrow gap cannot lose the entry outright; a crash
//! there instead leaves two durable tuples sharing one `seq`, which
//! `LandingQueue::scan_current` heals on the next read by keeping the one
//! with the higher `rev`. See `crash_between_write_and_delete_survives_the_entry`.
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
use crate::workflow_exec::{InstanceStatus, OnTimeout, ResolvedRun, WorkflowEngine};
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_space::Space;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, warn};

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

/// Poll slice for the liveness-aware review wait (module doc): how often
/// [`LandingPipeline::request_review`] gives up on the current `rd` and
/// checks the review instance's liveness before resuming the wait. Short
/// enough that a genuinely dead reviewer's escalation lands within about one
/// slice of it going terminal, not the full `reviewTimeout`/`reviewMaxWait`
/// window. Shrunk under `cfg(test)` so the liveness-poll tests below exercise
/// several real iterations without each costing a real 60s of wall-clock.
#[cfg(not(test))]
const REVIEW_POLL_SLICE: Duration = Duration::from_secs(60);
#[cfg(test)]
const REVIEW_POLL_SLICE: Duration = Duration::from_millis(150);

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
    /// Monotonic per-entry transition counter (T4 restart-safety). Bumped by
    /// every [`LandingQueue::claim_next`]/[`LandingQueue::set_status`] write.
    /// Transitions are write-then-delete (the successor tuple lands durably
    /// BEFORE the predecessor is removed), not delete-then-write, so a crash
    /// in that gap can leave two durable tuples sharing one `seq` — `rev` is
    /// how [`LandingQueue::scan_current`] tells the fresh successor from the
    /// stale predecessor and heals the duplicate instead of exposing (or
    /// losing) the entry.
    #[serde(default)]
    pub(crate) rev: u64,
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
        entry.rev = 0;
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

    /// Every durable tuple for `repo_name` (optionally narrowed to
    /// `target`), with any crash-orphaned duplicate self-healed away.
    /// `claim_next`/`set_status` transition status by writing the successor
    /// tuple BEFORE deleting the predecessor (crash-safety, module doc): a
    /// crash landing in that gap leaves two tuples sharing one `seq`. This
    /// is where that gets resolved — the fresher one (higher `rev`, ties
    /// broken by tuple id) is kept as canonical, and the stale predecessor
    /// is deleted here as a side effect of the read. A reader that races
    /// this cleanup can see the entry reflected by either tuple for one
    /// scan; it can never see zero — that is the property the write-then-
    /// delete ordering exists to guarantee.
    fn scan_current(
        &self,
        repo_name: &str,
        target: Option<&str>,
    ) -> rk_core::Result<Vec<Tuple>> {
        let mut pending = self.space.scan(
            &Pattern::category(Category::Event)
                .scope(repo_name)
                .identity(LANDING_QUEUE_IDENTITY),
        )?;
        if let Some(target) = target {
            pending.retain(|t| t.payload.get("target").and_then(Value::as_str) == Some(target));
        }
        let mut by_seq: HashMap<u64, Tuple> = HashMap::new();
        let mut stale = Vec::new();
        for tuple in pending {
            let seq = tuple.payload.get("seq").and_then(Value::as_u64).unwrap_or(0);
            let rev = tuple.payload.get("rev").and_then(Value::as_u64).unwrap_or(0);
            match by_seq.remove(&seq) {
                None => {
                    by_seq.insert(seq, tuple);
                }
                Some(existing) => {
                    let existing_rev =
                        existing.payload.get("rev").and_then(Value::as_u64).unwrap_or(0);
                    let (winner, loser) = if (rev, tuple.id) > (existing_rev, existing.id) {
                        (tuple, existing)
                    } else {
                        (existing, tuple)
                    };
                    stale.push(loser.id);
                    by_seq.insert(seq, winner);
                }
            }
        }
        for id in stale {
            // Opportunistic: this heals an already-superseded duplicate, it
            // is not itself the transition, so a failure here just leaves
            // the cleanup to the next reader.
            let _ = self.space.delete(id);
        }
        Ok(by_seq.into_values().collect())
    }

    /// Find the current durable tuple for `entry` — matched by its `seq`
    /// within `repo_name` scope, since `seq` is a monotonic per-repo counter
    /// ([`Self::next_seq`]) and therefore unique. `None` once the entry has
    /// been [`Self::remove`]d (or if it was never enqueued at all — a caller
    /// driving [`LandingPipeline::process_entry`] directly, bypassing the
    /// queue, has no durable tuple to find; every method here treats that as
    /// a harmless no-op rather than an error).
    fn find(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<Tuple>> {
        Ok(self
            .scan_current(&entry.repo_name, None)?
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
        let mut pending = self.scan_current(repo_name, Some(target))?;
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
        entry.status = LandingEntryStatus::RunningGates;
        entry.rev = entry.rev.wrapping_add(1);
        // Write-then-delete (T4 crash-safety, module doc): the successor
        // tuple lands durably BEFORE the predecessor is removed, so a crash
        // in the gap leaves both readable (self-healed by `scan_current`)
        // rather than leaving neither.
        self.write(&entry)?;
        self.space.delete(tuple.id)?;
        Ok(Some(entry))
    }

    /// Durably transition an already-claimed `entry` to `status`: write the
    /// successor tuple, then delete the predecessor (crash-safe ordering,
    /// same as [`Self::claim_next`]). A no-op if `entry` has no durable
    /// tuple right now (see [`Self::find`]'s doc).
    fn set_status(
        &self,
        entry: &LandingQueueEntry,
        status: LandingEntryStatus,
    ) -> rk_core::Result<()> {
        let Some(tuple) = self.find(entry)? else {
            return Ok(());
        };
        let mut updated = entry.clone();
        updated.status = status;
        updated.rev = entry.rev.wrapping_add(1);
        self.write(&updated)?;
        self.space.delete(tuple.id)?;
        Ok(())
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
    /// [`LandingPipeline::request_review`] waits on the reviewer's verdict
    /// tuple before checking whether the reviewer is still alive (liveness-
    /// aware wait, module doc) — matches `examples/workflows/steward.cue`'s
    /// `reviewTimeout` param default. A reviewer still running past this
    /// point is NOT abandoned; see [`Self::review_max_wait`].
    pub(crate) review_timeout: Duration,
    /// Hard ceiling on the review wait: once a LIVE reviewer has run this
    /// long with no verdict, `request_review` stops waiting and escalates a
    /// "still running at ceiling" hold rather than waiting forever. A
    /// reviewer that goes terminal (crashes, budget death) without a verdict
    /// is never held to this ceiling — that case escalates immediately (see
    /// `ReviewWaitOutcome::ReviewerDied`).
    pub(crate) review_max_wait: Duration,
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
            review_max_wait: Duration::from_secs(45 * 60),
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
    /// durable `gate-failure` artifact, and a steward `need` row was written
    /// so the hold is visible in `rk inbox`; the branch is left unmerged.
    GateHeld,
    /// The reviewer (fresh or cached) recommended REWORK: a follow-up ticket
    /// was filed directly (`Tickets::create`, §1.5) and the branch held
    /// unmerged — no `dismiss` is needed since no agent worktree exists for
    /// the candidate itself.
    ReworkFiled(Tuple),
    /// STOP, an unrecognized verdict, a reviewer that went terminal without
    /// ever producing one, or a still-live reviewer that ran the wait out to
    /// `GateConfig::review_max_wait` — all get the same "genuine human
    /// judgment call" treatment (design doc §2.4, module doc): a `need` tuple
    /// was written directly (`Space::out`, §1.5) and the branch held unmerged.
    Escalated(Tuple),
    /// The work key already carried a terminal `landing_processed` marker
    /// when this entry was processed — the daemon crashed in the window
    /// between `mark_processed` and the queue-entry removal on a prior run.
    /// Every terminal side effect (need rows, rework tickets, the land
    /// itself) already happened then; this run only reconciles the queue.
    /// Carries the recorded outcome string ("landed", "gate-held", ...).
    Reconciled(String),
}

/// How [`LandingPipeline::request_review`]'s liveness-aware wait (module doc)
/// resolved — the three cases its escalation text must distinguish.
#[derive(Debug)]
enum ReviewWaitOutcome {
    /// A recommendation landed — fresh or from Phase 2's verdict cache.
    Verdict(String),
    /// The review instance went terminal (`Completed` or `Failed`) without
    /// ever producing a verdict tuple. Carries the instance's own captured
    /// failure context (`Instance::error`) for the escalation text, falling
    /// back to a generic note if the instance recorded none.
    ReviewerDied(String),
    /// The wait ran out `GateConfig::review_max_wait` with the reviewer
    /// still `Running` and no verdict — a live-at-ceiling hold, distinct from
    /// a dead reviewer.
    CeilingReached,
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
        }
    }

    /// Resolve this entry's repo-owned gate/review policy from its activated
    /// `.rk/repo.cue` (`RepositoryPolicy.landing`, digest-activated like
    /// `delivery` — `Supervisor::repository_policy`) — the daemon-native
    /// replacement for what `examples/workflows/steward.cue`'s mega-workflow
    /// used to expose as workflow params (`protectedPaths`, `maxDiffFiles`,
    /// `maxDiffLines`, `gateTimeout`, `reviewTimeout`). A repo registered
    /// without an activated policy falls back to `GateConfig::default()`'s
    /// values, matching `repository_policy`'s own legacy-translation
    /// fallback. `check_name` is not repo.cue-configurable (out of this
    /// ticket's scope): every repo's landing gate runs its named `verify`
    /// check.
    fn gate_config(&self, repo: &rk_git::Repo) -> GateConfig {
        let policy = self.supervisor.repository_policy(repo).landing;
        let defaults = GateConfig::default();
        GateConfig {
            check_name: defaults.check_name,
            protected_paths: policy.protected_paths,
            max_diff_files: policy.max_diff_files,
            max_diff_lines: policy.max_diff_lines,
            gate_timeout: crate::workflow_exec::parse_duration(&policy.gate_timeout)
                .unwrap_or(defaults.gate_timeout),
            review_timeout: crate::workflow_exec::parse_duration(&policy.review_timeout)
                .unwrap_or(defaults.review_timeout),
            review_max_wait: crate::workflow_exec::parse_duration(&policy.review_max_wait)
                .unwrap_or(defaults.review_max_wait),
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
    /// The recorded terminal outcome string for `entry`'s work key, when a
    /// `landing_processed` marker exists — the read side used both by the
    /// enqueue dedup and by `process_entry`'s crash-window reconciliation.
    fn processed_outcome(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<String>> {
        let pattern = Pattern::for_commit(
            Category::Event,
            LANDING_PROCESSED_IDENTITY,
            &entry.branch,
            &entry.head_sha,
        )
        .scope(&entry.repo_name);
        Ok(self.space.scan(&pattern)?.into_iter().find_map(|t| {
            t.payload
                .get("outcome")
                .and_then(Value::as_str)
                .map(str::to_string)
        }))
    }

    fn already_processed(&self, entry: &LandingQueueEntry) -> rk_core::Result<bool> {
        Ok(self.processed_outcome(entry)?.is_some())
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
            // A reconciled entry's marker already exists from the run that
            // performed the side effects; writing a second would corrupt the
            // one-marker-per-work-key invariant `already_processed` reads.
            LandingOutcome::Reconciled(_) => return Ok(()),
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
        // Crash-window reconciliation (review round 2): a crash between
        // `mark_processed` and the caller's queue removal leaves both the
        // marker and the queue entry. The marker is the truth — never repeat
        // terminal side effects (needs, rework tickets, the land itself);
        // just report what already happened so the caller removes the entry.
        if let Some(prior) = self.processed_outcome(entry)? {
            return Ok(LandingOutcome::Reconciled(prior));
        }
        let repo_path = PathBuf::from(&entry.repo_path);
        let git_repo = {
            let repo_path = repo_path.clone();
            blocking(move || rk_git::Repo::discover(&repo_path)).await?
        };
        let gates = self.gate_config(&git_repo);
        if !self.run_gates(entry, &git_repo, &gates).await? {
            // The durable gate-failure artifact carries the evidence; the
            // need row is what makes the hold VISIBLE in `rk inbox` — parity
            // with the CUE steward's escalation contract.
            self.escalate(
                entry,
                format!(
                    "steward: run gate FAILED for {} on {} — branch held unmerged; read the                      durable gate-failure artifact for the failing tests",
                    entry.task, entry.branch
                ),
            )?;
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
        let verdict = self.review_verdict(entry, &gates).await?;
        let outcome = self.route_verdict(entry, verdict, &gates).await?;
        self.mark_processed(entry, &outcome)?;
        Ok(outcome)
    }

    /// Resolve a recommendation for `entry`: a hit against Phase 2's
    /// commit-keyed verdict cache (§1.3/§2.3 step 2), or — on a miss — a
    /// fresh, liveness-aware review request (§2.3 step 3, module doc).
    async fn review_verdict(
        &self,
        entry: &LandingQueueEntry,
        gates: &GateConfig,
    ) -> rk_core::Result<ReviewWaitOutcome> {
        if let Some(cached) = self.cached_verdict(entry)? {
            return Ok(ReviewWaitOutcome::Verdict(cached));
        }
        self.request_review(entry, gates).await
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
    /// branch, then wait on the verdict tuple ITSELF rather than the
    /// workflow instance's own completion (§1.5's `watch_attached_completion`
    /// pattern) — a daemon restart loses nothing: the reviewer keeps working
    /// against its own worktree regardless of the pipeline's state, and the
    /// verdict tuple is durable even though this exact wait is not (§2.6).
    ///
    /// The wait is liveness-aware (module doc): rather than one fixed-length
    /// `rd`, it polls in [`REVIEW_POLL_SLICE`] slices up to
    /// `gates.review_max_wait`. A slice that times out with no verdict probes
    /// the review instance — still `Running` keeps waiting (a slow reviewer
    /// is not abandoned at `reviewTimeout`); gone terminal without a verdict
    /// stops waiting immediately, after one last race probe, and surfaces the
    /// instance's own captured failure context. Reaching the ceiling with a
    /// still-live reviewer is the only case that waits the full window.
    ///
    /// The `reviewTimeout` param handed to the workflow itself is
    /// `review_max_wait`, not `gates.review_timeout`: that param bounds the
    /// workflow's own internal `wait` step (§ the review-only workflow's
    /// `steps`), and if it stayed pinned to the base `reviewTimeout` a merely
    /// slow-but-alive reviewer would trip THAT step's deadline at exactly the
    /// point this wait is supposed to start tolerating it — indistinguishable
    /// from a genuine crash. A genuine crash is still detected promptly
    /// regardless of that bound: the workflow's `wait` step fails as soon as
    /// the agent's own liveness check reports it gone (`abandoned`,
    /// `workflow_exec::WorkflowEngine::await_result`), not at any timeout.
    ///
    /// The workflow launch uses a STABLE instance id derived from `entry`'s
    /// work key (`review_instance_id`), not a fresh random one: a daemon
    /// restart re-claims the durable queue entry (still `AwaitingReview` from
    /// before the crash) and calls this again from scratch, and
    /// `run_owned_with_id` returns the EXISTING instance's snapshot instead
    /// of launching a second reviewer for a request already in flight — the
    /// same "never orphan a reviewer" guarantee the design doc's §2.6
    /// prescribes, but also never double-spawns one.
    async fn request_review(
        &self,
        entry: &LandingQueueEntry,
        gates: &GateConfig,
    ) -> rk_core::Result<ReviewWaitOutcome> {
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
            Value::String(format!("{}s", gates.review_max_wait.as_secs())),
        );
        // The engine's `repo` argument is a filesystem path (it feeds
        // `Repo::discover` and repo-local definition resolution), unlike the
        // `repo` WORKFLOW PARAM above, which is the repo's scope name used
        // to address its verdict artifact.
        let instance_id = review_instance_id(entry);
        self.engine.run_owned_with_id(
            instance_id.clone(),
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

        let started = tokio::time::Instant::now();
        let deadline = started + gates.review_max_wait;
        let mut logged_past_base_timeout = false;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                // One last race probe: the verdict may have landed in the gap
                // between the previous slice's timeout and this check.
                if let Some(cached) = self.cached_verdict(entry)? {
                    return Ok(ReviewWaitOutcome::Verdict(cached));
                }
                return Ok(ReviewWaitOutcome::CeilingReached);
            }
            let slice = remaining.min(REVIEW_POLL_SLICE);
            if let Some(tuple) = self.space.rd(&pattern, slice).await? {
                let recommendation = tuple
                    .payload
                    .get("recommendation")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                return Ok(ReviewWaitOutcome::Verdict(recommendation));
            }
            if let Some(instance) = self.engine.status_any(&instance_id) {
                if instance.status != InstanceStatus::Running {
                    // Same race as above: probe once more before declaring
                    // the reviewer dead-without-a-verdict.
                    if let Some(cached) = self.cached_verdict(entry)? {
                        return Ok(ReviewWaitOutcome::Verdict(cached));
                    }
                    let context = instance.error.unwrap_or_else(|| {
                        "reviewer instance ended with no recorded error".to_string()
                    });
                    return Ok(ReviewWaitOutcome::ReviewerDied(context));
                }
            }
            if !logged_past_base_timeout && started.elapsed() >= gates.review_timeout {
                logged_past_base_timeout = true;
                info!(
                    repo = %entry.repo_name, branch = %entry.branch,
                    base_review_timeout_secs = gates.review_timeout.as_secs(),
                    ceiling_secs = gates.review_max_wait.as_secs(),
                    "landing pipeline: reviewer still alive past the base reviewTimeout, \
                     extending the wait toward the ceiling"
                );
            }
        }
    }

    /// Route a resolved (fresh or cached) recommendation, a dead reviewer, or
    /// a live-at-ceiling reviewer via direct daemon calls: no shell, no agent
    /// auth token (§1.5/§2.4).
    async fn route_verdict(
        &self,
        entry: &LandingQueueEntry,
        outcome: ReviewWaitOutcome,
        gates: &GateConfig,
    ) -> rk_core::Result<LandingOutcome> {
        let verdict = match outcome {
            ReviewWaitOutcome::Verdict(v) => v,
            ReviewWaitOutcome::ReviewerDied(context) => {
                let text = format!(
                    "steward: reviewer for {} on {} ended without producing a verdict — branch \
                     held unmerged. {context}",
                    entry.task, entry.branch
                );
                return Ok(LandingOutcome::Escalated(self.escalate(entry, text)?));
            }
            ReviewWaitOutcome::CeilingReached => {
                let text = format!(
                    "steward: reviewer still running at the {}s wait ceiling for {} on {} — \
                     branch held unmerged",
                    gates.review_max_wait.as_secs(),
                    entry.task,
                    entry.branch
                );
                return Ok(LandingOutcome::Escalated(self.escalate(entry, text)?));
            }
        };
        match verdict.as_str() {
            "APPROVE" => {
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
            "REWORK" => Ok(LandingOutcome::ReworkFiled(
                self.file_rework_ticket(entry).await?,
            )),
            "STOP" => {
                let text = format!(
                    "steward: reviewer returned STOP for {} on {} — needs a human merge \
                     decision; branch held unmerged",
                    entry.task, entry.branch
                );
                Ok(LandingOutcome::Escalated(self.escalate(entry, text)?))
            }
            other => {
                let text = format!(
                    "steward: unrecognized review verdict '{other}' for {} on {} — branch held \
                     unmerged, needs a human",
                    entry.task, entry.branch
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
    /// prior process — restart-safety, module doc) and drain each key
    /// CONCURRENTLY, one task per key — matching `MergeQueue`'s own promise
    /// that different target branches in the same repo merge concurrently
    /// (design doc §1.1), which this pipeline's admission model (§2.1) never
    /// narrowed. Fan-out is intentionally unbounded across keys: each key is
    /// already a natural, small admission unit (there is one only if
    /// something is genuinely queued for it), unlike WITHIN a key, where
    /// admission stays strictly single-consumer (§2.1, §5 open question 3) —
    /// `drain_key` still claims and finishes one candidate at a time for its
    /// own key, so a burst on ONE key still gate-runs serially even though
    /// this cycle now runs many keys side by side (see
    /// `burst_of_completions_on_one_key_never_runs_gates_concurrently` and
    /// `distinct_keys_drain_concurrently_within_one_run_cycle`). Prior to
    /// this, `run_cycle` drained keys one at a time in a single `for` loop,
    /// which meant a slow `verify` run (up to `GateConfig::gate_timeout`,
    /// 60 minutes by default) on one key silently stalled every other
    /// repo's/target's landing traffic for the rest of the cycle — a
    /// correctness gap against the stated concurrency promise, not just a
    /// stale comment, so it is fixed here rather than merely documented
    /// (T4 rework; see the design doc's T4 section for the writeup).
    ///
    /// One key's failure does not abort the whole pass — logged and skipped,
    /// left for the next poll cycle to retry (this drives a live daemon
    /// loop, so one repo's transient fault must not stall every other repo's
    /// landing traffic). A panicking drain task is treated the same way.
    pub(crate) async fn run_cycle(self: &Arc<Self>) -> rk_core::Result<Vec<LandingOutcome>> {
        let mut in_flight = tokio::task::JoinSet::new();
        for (repo_name, target) in self.queue.pending_keys()? {
            let pipeline = Arc::clone(self);
            in_flight.spawn(async move {
                let result = pipeline.drain_key(&repo_name, &target).await;
                (repo_name, target, result)
            });
        }
        let mut outcomes = Vec::new();
        while let Some(joined) = in_flight.join_next().await {
            match joined {
                Ok((_, _, Ok(o))) => outcomes.extend(o),
                Ok((repo_name, target, Err(e))) => warn!(
                    repo = %repo_name, target = %target, error = %e,
                    "landing pipeline: drain_key failed, will retry next cycle"
                ),
                Err(join_err) => warn!(
                    error = %join_err,
                    "landing pipeline: a key's drain task panicked, will retry next cycle"
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
    async fn run_gates(
        &self,
        entry: &LandingQueueEntry,
        git_repo: &rk_git::Repo,
        gates: &GateConfig,
    ) -> rk_core::Result<bool> {
        let repo_path = PathBuf::from(&entry.repo_path);
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
        let plan = self.gate_plan(&checks_file, &entry.target, gates)?;

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
    fn gate_plan(
        &self,
        checks_file: &Path,
        target: &str,
        gates: &GateConfig,
    ) -> rk_core::Result<GatePlan> {
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
        let verify = find(&gates.check_name)?;

        Ok(vec![
            (
                protected_paths,
                vec![
                    ("RK_CHECK_TARGET".to_string(), target.to_string()),
                    (
                        "RK_CHECK_PROTECTED_PATHS".to_string(),
                        gates.protected_paths.clone(),
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
                        gates.max_diff_files.to_string(),
                    ),
                    (
                        "RK_CHECK_MAX_DIFF_LINES".to_string(),
                        gates.max_diff_lines.to_string(),
                    ),
                ],
                POLICY_GATE_TIMEOUT,
            ),
            (verify, Vec::new(), gates.gate_timeout),
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
            false,
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
    ///
    /// A `timer` gate holds the instance `Running` for a couple of seconds
    /// after spawn, before the `wait`/`evaluate` steps that would otherwise
    /// let it complete near-instantly against the `fake` harness's canned,
    /// sub-second script. Without this, a test that injects its verdict tuple
    /// shortly after the spawn is detected would race the liveness-aware poll
    /// loop (module doc): a poll slice landing after the instance has already
    /// gone `Completed` — but before the test's `space.out` call — would read
    /// as a terminal-without-a-verdict reviewer and misfire
    /// `ReviewWaitOutcome::ReviewerDied` instead of honoring the late verdict.
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
		{type: "gate", gateType: "timer", duration: "2s"},
		{type: "wait", timeout: _input.reviewTimeout},
		{type: "evaluate", expect: {is_error: false}},
	]
}
"#,
        )
        .unwrap();
    }

    /// A review-only workflow whose reviewer spawns for real (the `fake`
    /// harness, same as [`write_review_workflow`]) but is immediately
    /// followed by a `stop` step — a deterministic stand-in for "the
    /// reviewer died before producing a verdict" that needs no genuinely
    /// crashing subprocess and no shared process-wide harness-script env var
    /// (which would race concurrently running unit tests elsewhere in this
    /// crate's test binary). `Step::Stop` fails the whole instance with the
    /// given reason as `Instance::error` (`workflow_exec::Step::Stop`
    /// handling) — exactly the captured-context shape a genuine crash
    /// produces, without needing one.
    fn write_broken_review_workflow(layout: &Layout) {
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
		{type: "stop", reason: "simulated reviewer crash: harness exited without reporting"},
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
            false,
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

    /// T4's crash-safety property (module doc): a daemon crash landing
    /// between the successor write and the predecessor delete of a status
    /// transition must never lose the candidate. Drives the two halves of
    /// that write-then-delete transition separately — writing the successor
    /// tuple via `queue.write` directly and deliberately skipping the
    /// predecessor's delete, which IS the crash gap — and asserts the entry
    /// is still exactly-once discoverable afterward, with no orphaned
    /// duplicate left behind.
    #[test]
    fn crash_between_write_and_delete_survives_the_entry() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let space = Space::open_in_memory().unwrap();
        let queue = LandingQueue::new(space.clone(), &layout);

        let mut entry = LandingQueueEntry {
            repo_name: "alpha".into(),
            repo_path: "/repos/alpha".into(),
            branch: "b1".into(),
            target: "main".into(),
            head_sha: "deadbeef".into(),
            diff_class: "trivial".into(),
            task: "t".into(),
            ..Default::default()
        };
        let seq = queue.enqueue(entry.clone()).unwrap();
        entry.seq = seq;
        assert!(
            queue.find(&entry).unwrap().is_some(),
            "predecessor tuple must exist right after enqueue"
        );

        // Drive claim_next's transition by hand, stopping after the write of
        // the successor -- this is exactly the gap a daemon crash could land
        // in. The predecessor's delete deliberately never runs.
        let mut successor = entry.clone();
        successor.status = LandingEntryStatus::RunningGates;
        successor.rev = entry.rev + 1;
        queue.write(&successor).unwrap();
        // <-- simulated crash: `queue.space.delete(predecessor.id)` never happens.

        // Both the stale Queued tuple and the fresh RunningGates tuple are
        // durably present right now. The entry must still be discoverable —
        // this is the property under test — not lost, and self-healing dedup
        // must resolve it to a single canonical claim rather than exposing it
        // (or losing it) twice.
        let recovered = queue
            .claim_next("alpha", "main")
            .unwrap()
            .expect("entry must survive the crash gap between write and delete");
        assert_eq!(recovered.branch, "b1");
        assert_eq!(recovered.seq, seq);
        assert_eq!(recovered.status, LandingEntryStatus::RunningGates);

        // The stale predecessor was cleaned up as part of the self-heal —
        // exactly one durable tuple for this entry, not two.
        let remaining = space
            .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
            .unwrap();
        assert_eq!(
            remaining.len(),
            1,
            "no duplicate/orphaned tuple must remain: {remaining:?}"
        );

        // Finishing processing (as `LandingPipeline::process_next` does on a
        // terminal outcome) empties the queue cleanly, proving the crash
        // never left a second, uncollectable copy behind.
        queue.remove(&recovered).unwrap();
        assert!(queue.claim_next("alpha", "main").unwrap().is_none());
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

        // The hold is VISIBLE where a human looks: a steward-parity need row
        // (agent/task/text) exists for the repo — the CUE steward's
        // escalation contract, kept by the pipeline (review round 2).
        let needs = space
            .scan(
                &Pattern::category(Category::Need)
                    .scope("code-repo")
                    .identity(STEWARD_NEED_IDENTITY),
            )
            .unwrap();
        assert_eq!(needs.len(), 1, "gate hold must write exactly one need row");
        assert_eq!(needs[0].payload["agent"], "steward");
        assert_eq!(needs[0].payload["task"], "add src");
        assert!(needs[0].payload["text"]
            .as_str()
            .unwrap()
            .contains("run gate FAILED"));

        // Crash-window reconciliation: the terminal marker exists but suppose
        // the queue entry survived (daemon died before removal). Re-processing
        // the same work key must NOT repeat side effects — same single need
        // row, no second marker — and must report itself as reconciled.
        let replay = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: rev_parse(repo_dir.path(), "feature"),
            diff_class: "doc-only".into(),
            task: "add src".into(),
            ..Default::default()
        };
        let reconciled = pipeline.process_entry(&replay).await.unwrap();
        match reconciled {
            LandingOutcome::Reconciled(prior) => assert_eq!(prior, "gate-held"),
            other => panic!("expected Reconciled, got {other:?}"),
        }
        let needs_after = space
            .scan(
                &Pattern::category(Category::Need)
                    .scope("code-repo")
                    .identity(STEWARD_NEED_IDENTITY),
            )
            .unwrap();
        assert_eq!(needs_after.len(), 1, "reconciliation must not duplicate the need row");

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

    /// `LandingPipeline::escalate` writes its `need` tuple directly
    /// (`Space::out`, §1.5 of the design doc) instead of going through a
    /// shelled-out `rk out need`, but the ROW `rk inbox` renders from it must
    /// be indistinguishable from the shape a workflow-driven steward's
    /// `steward-report-stop`/`steward-report-gate-failure`/
    /// `steward-report-timeout`/`steward-report-unknown-verdict` named checks
    /// (`.rk/checks.cue`) have always produced: `rk out need <repo> steward
    /// --field agent=steward --field task=<id> --field text=<text>`. Compares
    /// `inbox::build`'s output for a hand-built tuple in that exact
    /// historical shape against the tuple the pipeline actually escalates
    /// with on a STOP verdict.
    #[tokio::test]
    async fn escalation_row_matches_the_workflow_driven_steward_shape() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, main_before) = review_candidate_repo();

        let space = Space::open_in_memory().unwrap();
        space.out(verdict_tuple(&head_sha, "STOP")).unwrap();

        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(review_candidate_entry(repo_dir.path(), &head_sha))
            .unwrap();

        let outcomes = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert_eq!(outcomes.len(), 1);
        let LandingOutcome::Escalated(produced) = &outcomes[0] else {
            panic!("expected Escalated, got {:?}", outcomes[0]);
        };

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_eq!(main_before, main_after, "branch must not have landed");

        let historical = Tuple::new(
            Category::Need,
            "code-repo",
            STEWARD_NEED_IDENTITY,
            "daemon",
            json!({
                "agent": "steward",
                "task": "add src",
                "text": "steward: reviewer returned STOP for add src on feature — needs a \
                         human merge decision; branch held unmerged",
            }),
        );

        let empty_branches = crate::inbox::BranchEvents::default();
        let empty_ballots = crate::inbox::Ballots::default();
        let historical_rows = crate::inbox::build(
            &[],
            &[],
            &[],
            std::slice::from_ref(&historical),
            &empty_branches,
            &empty_ballots,
        );
        let produced_rows = crate::inbox::build(
            &[],
            &[],
            &[],
            std::slice::from_ref(produced),
            &empty_branches,
            &empty_ballots,
        );

        assert_eq!(historical_rows.len(), 1);
        assert_eq!(produced_rows.len(), 1);
        assert_eq!(historical_rows[0].kind, produced_rows[0].kind);
        assert_eq!(historical_rows[0].urgency, produced_rows[0].urgency);
        assert_eq!(historical_rows[0].subject, produced_rows[0].subject);
        assert_eq!(historical_rows[0].scope, produced_rows[0].scope);
        assert_eq!(historical_rows[0].action, produced_rows[0].action);
        assert!(
            produced_rows[0].detail.contains("STOP"),
            "detail: {}",
            produced_rows[0].detail
        );
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

    /// T4's queue-level restart-safety (design doc §2.6, module doc): a
    /// candidate crashed mid-gate-run is left `RunningGates` in the DURABLE
    /// queue tuple (not deleted), and a restarted pipeline's `run_cycle`
    /// (the same entrypoint the live daemon's polling loop calls) discovers
    /// and completes it — proven here via a genuine on-disk `Space` reopen,
    /// not an in-memory stand-in, and driven through `enqueue`/`run_cycle`
    /// (not `process_entry` directly), so it exercises `claim_next` picking
    /// up a non-`Queued` entry.
    #[tokio::test]
    async fn restart_mid_gate_run_resumes_and_lands() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        // A `verify` check with a real pause gives the "before restart" task
        // a window to be aborted WHILE the gate is genuinely still running,
        // not merely queued.
        write_checks(
            repo_dir.path(),
            r#"
checks: [
    {name: "steward-protected-paths", command: "true", timeout: "30s"},
    {name: "steward-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "sleep 0.4 && true", timeout: "30s"},
]
"#,
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::create_dir_all(repo_dir.path().join("docs")).unwrap();
        std::fs::write(repo_dir.path().join("docs").join("note.md"), "note\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "docs: add note"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        let main_before = rev_parse(repo_dir.path(), "main");
        git(repo_dir.path(), &["checkout", "main"]);

        let entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha,
            diff_class: "doc-only".into(),
            task: "add note".into(),
            ..Default::default()
        };

        // "Before restart": a real on-disk Space, claimed and mid-gate when
        // the hosting task is aborted (the crash).
        {
            let space = Space::open(&layout.db_path()).unwrap();
            let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
            pipeline.enqueue(entry.clone()).unwrap().unwrap();
            let handle = tokio::spawn({
                let pipeline = Arc::clone(&pipeline);
                async move { pipeline.run_cycle().await }
            });
            // Give claim_next time to run and the gate's `sleep 0.4` time to
            // genuinely be mid-flight, well before it would finish.
            tokio::time::sleep(Duration::from_millis(120)).await;
            handle.abort();
            let _ = handle.await;

            // The crash left the candidate durably RunningGates, not deleted.
            let pending = space
                .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
                .unwrap();
            assert_eq!(pending.len(), 1, "candidate must survive the crash");
            let status: LandingEntryStatus =
                serde_json::from_value(pending[0].payload["status"].clone()).unwrap();
            assert_eq!(status, LandingEntryStatus::RunningGates);
        }

        // "After restart": fresh Space handle over the SAME on-disk store.
        let space = Space::open(&layout.db_path()).unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        let outcomes = pipeline.run_cycle().await.unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(
            matches!(&outcomes[0], LandingOutcome::Landed(r) if r["merged"] == true),
            "expected Landed, got {:?}",
            outcomes[0]
        );

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_ne!(
            main_before, main_after,
            "branch must have landed after the restart"
        );
        assert!(
            space
                .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
                .unwrap()
                .is_empty(),
            "the queue entry must be removed once processing reaches a terminal outcome"
        );
    }

    /// T4's admission control (design doc §2.1's "single-consumer per key" +
    /// the T4 section's own "a burst of completions queues instead of
    /// thundering"): enqueueing several candidates onto the SAME
    /// `(repo, target)` key at once — the burst arrives before the consumer
    /// ever runs — must still gate-run them one at a time, never
    /// concurrently. Proven with a `verify` check that records whether it
    /// ever started while a sibling run's marker file was still present,
    /// which a concurrent (thundering) admission would trip.
    #[tokio::test]
    async fn burst_of_completions_on_one_key_never_runs_gates_concurrently() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());

        // Lives outside the repo/gate worktree entirely, so `reset_gate_
        // worktree`'s `git clean -fd` between candidates never touches it.
        let barrier_dir = tempfile::tempdir().unwrap();
        let marker = barrier_dir.path().join("running");
        let overlap_log = barrier_dir.path().join("overlap.log");
        let checks = format!(
            r#"
checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "test -f \"{marker}\" && echo overlap >> \"{log}\"; touch \"{marker}\"; sleep 0.1; rm -f \"{marker}\"", timeout: "30s"}},
]
"#,
            marker = marker.display(),
            log = overlap_log.display(),
        );
        write_checks(repo_dir.path(), &checks);

        const N: usize = 4;
        let mut candidates = Vec::new();
        for i in 0..N {
            let branch = format!("feature-{i}");
            git(repo_dir.path(), &["checkout", "-b", &branch]);
            std::fs::create_dir_all(repo_dir.path().join("docs")).unwrap();
            std::fs::write(
                repo_dir.path().join("docs").join(format!("note-{i}.md")),
                "note\n",
            )
            .unwrap();
            git(repo_dir.path(), &["add", "."]);
            git(repo_dir.path(), &["commit", "-m", &format!("docs: note {i}")]);
            let head_sha = rev_parse(repo_dir.path(), &branch);
            git(repo_dir.path(), &["checkout", "main"]);
            candidates.push((branch, head_sha));
        }

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        // Admit the whole burst before draining a single one -- every
        // candidate is already queued before the consumer starts.
        for (branch, head_sha) in &candidates {
            pipeline
                .enqueue(LandingQueueEntry {
                    repo_name: "code-repo".into(),
                    repo_path: repo_dir.path().display().to_string(),
                    branch: branch.clone(),
                    target: "main".into(),
                    head_sha: head_sha.clone(),
                    diff_class: "doc-only".into(),
                    task: format!("add {branch}"),
                    ..Default::default()
                })
                .unwrap();
        }

        let outcomes = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert_eq!(outcomes.len(), N);
        assert!(
            outcomes
                .iter()
                .all(|o| matches!(o, LandingOutcome::Landed(r) if r["merged"] == true)),
            "outcomes: {outcomes:?}"
        );

        let overlap = std::fs::read_to_string(&overlap_log).unwrap_or_default();
        assert!(
            overlap.is_empty(),
            "gate runs overlapped for the same key — admission is not bounded to one at a \
             time: {overlap}"
        );
    }

    /// The other half of T4's concurrency contract (design doc §1.1's
    /// `MergeQueue` promise, restated for `LandingQueue` in `run_cycle`'s doc
    /// comment): TWO DIFFERENT `(repo, target)` keys must drain concurrently
    /// within one `run_cycle`, not have one wait out the other's entire gate
    /// run first. Each key's `verify` check touches its own "reached" marker
    /// then busy-waits on a shared release flag the test controls — proof by
    /// direct observation that both are genuinely in flight at once, not
    /// inferred from timing.
    #[tokio::test]
    async fn distinct_keys_drain_concurrently_within_one_run_cycle() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        git(repo_dir.path(), &["branch", "release"]);

        let barrier_dir = tempfile::tempdir().unwrap();
        let release_flag = barrier_dir.path().join("release");
        let checks = format!(
            r#"
checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "touch \"{barrier}/reached-$$\"; while [ ! -f \"{release}\" ]; do sleep 0.02; done", timeout: "30s"}},
]
"#,
            barrier = barrier_dir.path().display(),
            release = release_flag.display(),
        );
        write_checks(repo_dir.path(), &checks);

        git(repo_dir.path(), &["checkout", "-b", "feature-main"]);
        std::fs::create_dir_all(repo_dir.path().join("docs")).unwrap();
        std::fs::write(repo_dir.path().join("docs").join("a.md"), "a\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "docs: a"]);
        let head_main = rev_parse(repo_dir.path(), "feature-main");
        git(repo_dir.path(), &["checkout", "main"]);

        git(repo_dir.path(), &["checkout", "release"]);
        git(repo_dir.path(), &["checkout", "-b", "feature-release"]);
        std::fs::create_dir_all(repo_dir.path().join("docs")).unwrap();
        std::fs::write(repo_dir.path().join("docs").join("b.md"), "b\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "docs: b"]);
        let head_release = rev_parse(repo_dir.path(), "feature-release");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "code-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "feature-main".into(),
                target: "main".into(),
                head_sha: head_main,
                diff_class: "doc-only".into(),
                task: "add a".into(),
                ..Default::default()
            })
            .unwrap();
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "code-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "feature-release".into(),
                target: "release".into(),
                head_sha: head_release,
                diff_class: "doc-only".into(),
                task: "add b".into(),
                ..Default::default()
            })
            .unwrap();

        let cycle = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            async move { pipeline.run_cycle().await }
        });

        // Wait until BOTH keys' verify gates are genuinely in flight at
        // once — the direct proof of concurrent draining.
        let mut waited = 0;
        loop {
            let reached = std::fs::read_dir(barrier_dir.path())
                .unwrap()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().starts_with("reached-"))
                .count();
            if reached >= 2 {
                break;
            }
            waited += 1;
            assert!(
                waited < 500,
                "timed out waiting for both keys' gates to be concurrently in flight \
                 (run_cycle is still serializing distinct keys)"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        std::fs::write(&release_flag, "").unwrap();
        let outcomes = cycle.await.unwrap().unwrap();
        assert_eq!(outcomes.len(), 2);
        assert!(
            outcomes
                .iter()
                .all(|o| matches!(o, LandingOutcome::Landed(r) if r["merged"] == true)),
            "outcomes: {outcomes:?}"
        );

        let listing = |rev: &str| {
            let out = Command::new("git")
                .arg("-C")
                .arg(repo_dir.path())
                .args(["ls-tree", "--name-only", "-r", rev])
                .output()
                .unwrap();
            String::from_utf8_lossy(&out.stdout).to_string()
        };
        assert!(listing("main").contains("docs/a.md"), "{}", listing("main"));
        assert!(
            listing("release").contains("docs/b.md"),
            "{}",
            listing("release")
        );
    }

    /// Liveness-aware review wait, case (a): a verdict that lands after the
    /// base `reviewTimeout` but before the `reviewMaxWait` ceiling, with the
    /// reviewer alive throughout, must be honored — not abandoned merely
    /// because the base window elapsed (the Templeton-7 specimen this
    /// behavior fixes, module doc).
    #[tokio::test]
    async fn slow_but_alive_reviewer_is_not_abandoned_before_ceiling() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow(&layout);
        let (repo_dir, head_sha, main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        let gates = GateConfig {
            review_timeout: Duration::from_millis(200),
            review_max_wait: Duration::from_secs(5),
            ..GateConfig::default()
        };

        let wait = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let gates = gates.clone();
            async move { pipeline.request_review(&entry, &gates).await }
        });

        wait_for_spawn_count(&space, 1).await;
        // The workflow's own timer gate holds the instance `Running` for 2s;
        // supplying the verdict at ~600ms is comfortably past the 200ms base
        // reviewTimeout but well before both the 2s gate and the 5s ceiling.
        tokio::time::sleep(Duration::from_millis(600)).await;
        space.out(verdict_tuple(&head_sha, "APPROVE")).unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("request_review must not hang")
            .unwrap()
            .unwrap();
        let ReviewWaitOutcome::Verdict(verdict) = outcome else {
            panic!("expected Verdict, got {outcome:?}");
        };
        assert_eq!(verdict, "APPROVE");

        let routed = pipeline
            .route_verdict(&entry, ReviewWaitOutcome::Verdict(verdict), &gates)
            .await
            .unwrap();
        assert!(
            matches!(&routed, LandingOutcome::Landed(r) if r["merged"] == true),
            "routed: {routed:?}"
        );
        let main_after = rev_parse(repo_dir.path(), "main");
        assert_ne!(main_before, main_after, "branch must have landed");
        assert!(
            space
                .scan(&Pattern::category(Category::Need).identity(STEWARD_NEED_IDENTITY))
                .unwrap()
                .is_empty(),
            "a live reviewer that produced a verdict before the ceiling must not escalate"
        );
    }

    /// Liveness-aware review wait, case (b): a reviewer that goes terminal
    /// without ever producing a verdict must not be held to the full wait
    /// window — it escalates fast (well before even a generous base
    /// `reviewTimeout`), carrying the instance's own captured failure
    /// context, and the escalation text must read as a dead reviewer, not a
    /// live-at-ceiling hold.
    #[tokio::test]
    async fn dead_reviewer_escalates_fast_with_death_context() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_broken_review_workflow(&layout);
        let (repo_dir, head_sha, main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        // Generous base/ceiling: the point under test is that death is
        // detected well before EITHER, not merely before the ceiling.
        let gates = GateConfig {
            review_timeout: Duration::from_secs(120),
            review_max_wait: Duration::from_secs(600),
            ..GateConfig::default()
        };

        let started = tokio::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            pipeline.request_review(&entry, &gates),
        )
        .await
        .expect(
            "a dead reviewer must escalate in well under 10s, nowhere near the 120s base \
             reviewTimeout",
        )
        .unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(10),
            "escalation took {elapsed:?}, expected well under the 120s base reviewTimeout"
        );

        let ReviewWaitOutcome::ReviewerDied(context) = outcome else {
            panic!("expected ReviewerDied, got {outcome:?}");
        };
        assert!(!context.trim().is_empty(), "death context must not be empty");

        let routed = pipeline
            .route_verdict(
                &entry,
                ReviewWaitOutcome::ReviewerDied(context.clone()),
                &gates,
            )
            .await
            .unwrap();
        let LandingOutcome::Escalated(need) = &routed else {
            panic!("expected Escalated, got {routed:?}");
        };
        let text = need.payload["text"].as_str().unwrap();
        assert!(
            text.contains(&context),
            "escalation text must carry the captured failure context: {text}"
        );
        assert!(
            !text.contains("ceiling"),
            "a dead reviewer must not read as the live-at-ceiling case: {text}"
        );

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_eq!(main_before, main_after, "branch must not have landed");
    }

    /// Liveness-aware review wait, case (c): a reviewer still alive when the
    /// wait reaches `reviewMaxWait` escalates as a hold, but the message must
    /// name the live-at-ceiling case rather than misreport it as a dead
    /// reviewer.
    #[tokio::test]
    async fn live_reviewer_at_ceiling_escalates_as_still_running() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow(&layout); // 2s timer gate before wait/evaluate
        let (repo_dir, head_sha, main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        // The ceiling (800ms) falls well inside the workflow's 2s timer
        // gate, so the instance is provably still `Running` — never a
        // verdict is ever written.
        let gates = GateConfig {
            review_timeout: Duration::from_millis(100),
            review_max_wait: Duration::from_millis(800),
            ..GateConfig::default()
        };

        let started = tokio::time::Instant::now();
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            pipeline.request_review(&entry, &gates),
        )
        .await
        .expect("must resolve at the ceiling")
        .unwrap();
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(800) && elapsed < Duration::from_secs(3),
            "expected to wait out roughly the 800ms ceiling, took {elapsed:?}"
        );
        assert!(
            matches!(outcome, ReviewWaitOutcome::CeilingReached),
            "expected CeilingReached, got {outcome:?}"
        );

        let routed = pipeline
            .route_verdict(&entry, ReviewWaitOutcome::CeilingReached, &gates)
            .await
            .unwrap();
        let LandingOutcome::Escalated(need) = &routed else {
            panic!("expected Escalated, got {routed:?}");
        };
        let text = need.payload["text"].as_str().unwrap();
        assert!(text.contains("ceiling"), "text: {text}");
        assert!(
            !text.to_lowercase().contains("died") && !text.to_lowercase().contains("crash"),
            "a live-at-ceiling hold must not read as a dead reviewer: {text}"
        );

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_eq!(main_before, main_after, "branch must not have landed");
    }
}
