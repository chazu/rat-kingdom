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
//! LLM judgment — advances the exact tested candidate on a pass. A diff
//! needing review is handed back as
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
//! exact candidate advancement is CAS-guarded and idempotent (design doc
//! §1.1). See the `restart_mid_gate_run_resumes_and_lands` and
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
//! re-enqueueing it. The landing CAS already makes a literal double-advance
//! harmless; this dedup exists to also skip the
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

use crate::landing_rework::{
    self, ReworkContext, ReworkPolicy, ReworkRoute, Withheld, REWORK_DISPATCH_IDENTITY,
};
use crate::supervisor::Supervisor;
use crate::tickets::{NewTicket, Tickets};
use crate::workflow_exec::{InstanceStatus, OnTimeout, ResolvedRun, WorkflowEngine};
use chrono::{DateTime, Utc};
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
/// `reactor_queued_fire` (`crates/rk-daemon/src/reactor.rs`). `pub(crate)` so
/// [`tasks_in_landing_queue`] can be called from `Server`/`Supervisor` —
/// mirrors [`LANDING_PROCESSED_IDENTITY`]'s visibility below.
pub(crate) const LANDING_QUEUE_IDENTITY: &str = "landing_queue_entry";

/// Evidence that a landed correction queued its reviewed parent for a fresh
/// pass against the parent's original target. The queue tuple is the durable
/// source of truth; this event makes the automatic hand-off inspectable.
const REWORK_RESUBMISSION_IDENTITY: &str = "landing_rework_resubmission";

/// Identity of the durable per-attempt evidence event for a gate
/// infrastructure-death retry (bounded fail-safe recovery). See
/// [`LandingPipeline::record_gate_infra_attempt`].
const GATE_INFRA_RETRY_IDENTITY: &str = "landing_gate_infra_retry";

/// Identity of the durable `work_key = (repo, branch, head_sha)` dedup
/// marker (`Furniture`, scoped to the repo), written by
/// [`LandingPipeline::process_entry`] on every terminal outcome. Probed by
/// [`LandingPipeline::enqueue`] before writing a new queue tuple, so a
/// redelivered completion for an already-fully-processed candidate is
/// dropped rather than reprocessed (design doc §2.6). `pub(crate)` so
/// `Server::ticket_reopen_sweep_at` can also probe it directly (by
/// `payload.task`, not by work key) before reopening an `in_progress`
/// ticket whose branch already landed — TKT-01M0C663BZ86SMA2PVMFP5QJ8D.
pub(crate) const LANDING_PROCESSED_IDENTITY: &str = "landing_processed";

/// Task ids (ticket identities, by fleet convention) with a branch currently
/// sitting anywhere in the landing pipeline — `Queued`, `RunningGates`, or
/// `AwaitingReview`. No status check is needed: a `landing_queue_entry`
/// tuple exists for exactly as long as its candidate is non-terminal —
/// [`LandingQueue::remove`] deletes it the moment [`LandingPipeline::process_entry`]
/// reaches a terminal [`LandingOutcome`] — so mere presence answers the
/// question. One unscoped scan, same shape as the `landing_processed` probe
/// the reopen sweep already runs (`Server::ticket_reopen_sweep_at`): a
/// ticket whose branch is still in flight here must be treated the same way
/// as one that already landed — left alone by the reopen sweep, and not
/// closed by an unrelated duplicate's dismiss (TKT-01M0CTC4DYBRX6P5X2NPEZF0EZ,
/// probes O8/O17).
pub(crate) fn tasks_in_landing_queue(space: &Space) -> std::collections::HashSet<String> {
    space
        .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
        .unwrap_or_default()
        .into_iter()
        .filter_map(|t| {
            t.payload
                .get("task")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .filter(|task| !task.is_empty())
        .collect()
}

/// One durable queue entry as read for depth/age telemetry (`rk status`/`rk
/// top`, `rk inbox`'s `landing-queue-stalled` row — probe O18). A plain
/// read-side projection of [`LandingQueueEntry`], not the entry itself, so a
/// dashboard consumer never depends on the queue's internal transition
/// fields (`rev`, `candidate_*`).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct LandingQueueSnapshotEntry {
    pub(crate) repo: String,
    pub(crate) target: String,
    pub(crate) branch: String,
    pub(crate) task: String,
    pub(crate) status: LandingEntryStatus,
    /// Seconds since [`LandingQueueEntry::enqueued_at`]. `0` for a legacy
    /// entry written before that field existed — under-reporting age rather
    /// than fabricating one.
    pub(crate) age_secs: i64,
}

/// Every candidate currently sitting in the landing queue, across every
/// repo/target, self-healed the same way [`LandingQueue::scan_current`]
/// heals a single key: a crash between `claim_next`/`set_status`'s
/// write-then-delete can leave two durable tuples sharing one `(scope,
/// seq)`, and only the one with the higher `rev` (ties broken by tuple id)
/// is live. This is a read-only projection — unlike `scan_current` it never
/// deletes the stale duplicate itself, since a `status`/`inbox` read has no
/// business mutating the queue; the next real queue operation on that key
/// heals it.
pub(crate) fn landing_queue_snapshot(space: &Space) -> Vec<LandingQueueSnapshotEntry> {
    let all = space
        .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
        .unwrap_or_default();
    let mut by_key: HashMap<(String, u64), Tuple> = HashMap::new();
    for tuple in all {
        let seq = tuple
            .payload
            .get("seq")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let rev = tuple
            .payload
            .get("rev")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let key = (tuple.scope.clone(), seq);
        let replace = match by_key.get(&key) {
            None => true,
            Some(existing) => {
                let existing_rev = existing
                    .payload
                    .get("rev")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                (rev, tuple.id) > (existing_rev, existing.id)
            }
        };
        if replace {
            by_key.insert(key, tuple);
        }
    }
    let now = Utc::now();
    by_key
        .into_values()
        .filter_map(|tuple| {
            let entry: LandingQueueEntry = serde_json::from_value(tuple.payload).ok()?;
            let age_secs = entry
                .enqueued_at
                .map(|enqueued_at| (now - enqueued_at).num_seconds().max(0))
                .unwrap_or(0);
            Some(LandingQueueSnapshotEntry {
                repo: entry.repo_name,
                target: entry.target,
                branch: entry.branch,
                task: entry.task,
                status: entry.status,
                age_secs,
            })
        })
        .collect()
}

/// Depth and oldest-entry age per `(repo, target)` landing-queue key — the
/// summary `status`/`rk top` render directly and `rk inbox`'s
/// `landing-queue-stalled` row is derived from (probe O18: "a slow queue is
/// indistinguishable from a dead one" without this).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct LandingQueueSummary {
    pub(crate) repo: String,
    pub(crate) target: String,
    pub(crate) depth: usize,
    pub(crate) oldest_age_secs: i64,
    pub(crate) oldest_branch: String,
    pub(crate) oldest_task: String,
}

pub(crate) fn landing_queue_summary(space: &Space) -> Vec<LandingQueueSummary> {
    let mut by_key: HashMap<(String, String), Vec<LandingQueueSnapshotEntry>> = HashMap::new();
    for entry in landing_queue_snapshot(space) {
        by_key
            .entry((entry.repo.clone(), entry.target.clone()))
            .or_default()
            .push(entry);
    }
    let mut summary: Vec<LandingQueueSummary> = by_key
        .into_iter()
        .map(|((repo, target), entries)| {
            // Oldest = largest age_secs; ties keep the first found, which is
            // fine — the summary reports the age, not a stable identity.
            let oldest = entries
                .iter()
                .max_by_key(|entry| entry.age_secs)
                .expect("entries is non-empty: only ever built by pushing at least one");
            LandingQueueSummary {
                repo,
                target,
                depth: entries.len(),
                oldest_age_secs: oldest.age_secs,
                oldest_branch: oldest.branch.clone(),
                oldest_task: oldest.task.clone(),
            }
        })
        .collect();
    // Deterministic order for `status`/`rk top` and unit-test assertions.
    summary.sort_by(|a, b| (&a.repo, &a.target).cmp(&(&b.repo, &b.target)));
    summary
}

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

/// Identity of the visibility event emitted when a candidate is about to
/// land on a target other than `"main"`. Mirrors
/// `Reactor::note_non_main_land_target`'s `reactor_non_main_land_target`
/// (`crates/rk-daemon/src/reactor.rs`), which only covers the `action:
/// "workflow"` firing path — this pipeline's `action: "land"` candidates
/// (`Reactor::fire_land_action`) never go through that path, and this
/// pipeline's own zero-agent-spawn fast paths (doc-only/trivial diff,
/// verdict-cache hit) never create a workflow instance for `rk workflow
/// list` to annotate either, so without this a non-main land here is
/// otherwise silent (TKT-01M0B71D9B51SV5AG95VR1A4ST).
const LANDING_NON_MAIN_TARGET_IDENTITY: &str = "landing_non_main_land_target";

/// Name of the shrunk, review-only workflow definition (design doc §2.5) —
/// `examples/workflows/steward-review.cue`. [`LandingPipeline::request_review`]
/// invokes it programmatically on a verdict-cache miss; it is never
/// reactor-fired.
const REVIEW_WORKFLOW: &str = "steward-review";

fn required_payload_str<'a>(
    payload: &'a Value,
    field: &str,
    source: &str,
) -> rk_core::Result<&'a str> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| rk_core::Error::other(format!("{source} missing {field}")))
}

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

/// One landing candidate: a completed rat's branch, prepared into an exact
/// merge object, gated, then either advanced or routed through review. Mirrors the
/// reactor's queued-fire tuple shape (`repo_name`/`repo_path` as two
/// distinct fields — the first is the tuple scope and ticket/artifact scope,
/// the second is the filesystem root `Repo::discover` and candidate preparation
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
    /// Exact merge object built before gates run. Persisted so a daemon
    /// rollover can retest and land the same parked object.
    #[serde(default)]
    pub(crate) candidate_sha: Option<String>,
    #[serde(default)]
    pub(crate) candidate_base: Option<String>,
    #[serde(default)]
    pub(crate) candidate_ref: Option<String>,
    /// Members of a prepared multi-branch candidate. Repeated on every row
    /// so any surviving transition can reconstruct the batch after rollover.
    #[serde(default)]
    pub(crate) batch_branches: Vec<String>,
    /// Operator/workflow submissions get the fast lane, but never bypass
    /// gates or exact-tree CAS. FIFO is preserved within each priority.
    #[serde(default)]
    pub(crate) operator_fast_lane: bool,
    #[serde(default)]
    pub(crate) keep_branch: bool,
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
    /// When this candidate first entered the queue — set once by
    /// [`LandingQueue::enqueue`] and left untouched by every status
    /// transition (`claim_next`/`set_status`/`persist` all rewrite the
    /// tuple but preserve this field via `entry.clone()`). Deliberately
    /// PRESERVED across [`LandingQueue::requeue_tail`] too, even though that
    /// resets `seq`/`rev`/`status`/candidate fields to look like a fresh
    /// entry: a candidate stuck in a stale-target requeue loop must keep
    /// aging from when it first arrived, not reset to zero on every retry —
    /// that reset is exactly what would hide a genuine wedge (probe O18).
    /// `None` only for a durable tuple written before this field existed.
    #[serde(default)]
    pub(crate) enqueued_at: Option<DateTime<Utc>>,
    /// Whether this exact prepared candidate has already spent its one
    /// automatic gate-infrastructure-death retry (bounded fail-safe recovery
    /// — see [`LandingPipeline::run_gates_at`]). Persisted durably and set
    /// BEFORE the retry attempt itself runs, so a daemon crash between
    /// spending the budget and the retry completing can never be replayed
    /// into a second retry: a restart's `claim_next` re-discovers this entry
    /// with the flag already `true` and treats any further infra death as an
    /// ordinary hold. Reset only by [`LandingQueue::requeue_tail`], which
    /// also clears the candidate identity — a rebuilt candidate against a
    /// new base is a genuinely fresh attempt and earns its own budget.
    /// Deliberately NOT reset by `LandingPipeline::bisect_batch` (see that
    /// function's doc) — a bisect is a same-pass continuation of the run
    /// that just spent this budget, not a new future attempt.
    #[serde(default)]
    pub(crate) gate_infra_retry_used: bool,
    /// The name of the check currently mid-retry, while `run_gates_at`
    /// awaits its outcome — `None` the rest of the time, including once that
    /// outcome is settled (ordinal-2 evidence recorded) either way. Persisted
    /// in the SAME write as `gate_infra_retry_used` flipping to `true`, so
    /// the two durable fields never disagree about whether a retry is
    /// in-flight. This is what makes the retry resumable rather than merely
    /// non-duplicable: a crash between spending the budget and the retry
    /// completing leaves this `Some(check_name)` durably, and a restart's
    /// next `run_gates_at` call — reaching that same check again — reads it
    /// as "resume the in-flight retry", running the check exactly once more
    /// and recording its outcome as the ordinal-2 attempt, rather than
    /// silently skipping straight to a hold with no ordinal-2 evidence at
    /// all (the gap TKT-01M0FXGQMA10JYCV9QCGEAK4TT's review caught).
    #[serde(default)]
    pub(crate) gate_infra_retry_check: Option<String>,
}

/// See [`LandingQueueEntry::status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LandingEntryStatus {
    #[default]
    Queued,
    RunningGates,
    AwaitingReview,
    Landing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnqueueDisposition {
    Queued(u64),
    Pending,
    Processed,
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
        // Only a genuinely fresh candidate gets a fresh timestamp — a
        // `requeue_tail` call passes a clone carrying its original
        // `enqueued_at` forward (see the field's doc comment) so re-queued
        // work keeps aging rather than resetting.
        if entry.enqueued_at.is_none() {
            entry.enqueued_at = Some(Utc::now());
        }
        self.write(&entry)?;
        Ok(seq)
    }

    fn contains_work_key(&self, entry: &LandingQueueEntry) -> rk_core::Result<bool> {
        Ok(self
            .scan_current(&entry.repo_name, None)?
            .into_iter()
            .any(|tuple| {
                let payload = &tuple.payload;
                payload.get("branch").and_then(Value::as_str) == Some(entry.branch.as_str())
                    && payload.get("target").and_then(Value::as_str) == Some(entry.target.as_str())
                    && payload.get("head_sha").and_then(Value::as_str)
                        == Some(entry.head_sha.as_str())
                    && payload.get("task").and_then(Value::as_str) == Some(entry.task.as_str())
            }))
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
    fn scan_current(&self, repo_name: &str, target: Option<&str>) -> rk_core::Result<Vec<Tuple>> {
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
            let seq = tuple
                .payload
                .get("seq")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let rev = tuple
                .payload
                .get("rev")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            match by_seq.remove(&seq) {
                None => {
                    by_seq.insert(seq, tuple);
                }
                Some(existing) => {
                    let existing_rev = existing
                        .payload
                        .get("rev")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
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
            let fast = t
                .payload
                .get("operator_fast_lane")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let seq = t.payload.get("seq").and_then(Value::as_u64).unwrap_or(0);
            (if fast { 0 } else { 1 }, seq, t.id)
        });
        let Some(tuple) = pending.into_iter().next() else {
            return Ok(None);
        };
        let mut entry: LandingQueueEntry = serde_json::from_value(tuple.payload.clone())
            .map_err(|e| rk_core::Error::other(format!("landing queue entry: {e}")))?;
        if entry.status != LandingEntryStatus::Landing {
            entry.status = LandingEntryStatus::RunningGates;
        }
        entry.rev = entry.rev.wrapping_add(1);
        // Write-then-delete (T4 crash-safety, module doc): the successor
        // tuple lands durably BEFORE the predecessor is removed, so a crash
        // in the gap leaves both readable (self-healed by `scan_current`)
        // rather than leaving neither.
        self.write(&entry)?;
        self.space.delete(tuple.id)?;
        Ok(Some(entry))
    }

    fn claim_batch(
        &self,
        repo_name: &str,
        target: &str,
        max: usize,
    ) -> rk_core::Result<Vec<LandingQueueEntry>> {
        let mut pending = self.scan_current(repo_name, Some(target))?;
        pending.sort_by_key(|tuple| {
            let fast = tuple
                .payload
                .get("operator_fast_lane")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let seq = tuple
                .payload
                .get("seq")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            (if fast { 0 } else { 1 }, seq, tuple.id)
        });
        let mut entries = Vec::new();
        for tuple in pending.into_iter().take(max) {
            let mut entry: LandingQueueEntry = serde_json::from_value(tuple.payload.clone())
                .map_err(|error| rk_core::Error::other(format!("landing queue entry: {error}")))?;
            if entry.status != LandingEntryStatus::Landing {
                entry.status = LandingEntryStatus::RunningGates;
            }
            entry.rev = entry.rev.wrapping_add(1);
            self.write(&entry)?;
            self.space.delete(tuple.id)?;
            entries.push(entry);
        }
        Ok(entries)
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

    /// Persist all current entry fields while transitioning status. Used
    /// after merge preparation because candidate identity is durable state.
    fn persist(
        &self,
        entry: &mut LandingQueueEntry,
        status: LandingEntryStatus,
    ) -> rk_core::Result<()> {
        let Some(tuple) = self.find(entry)? else {
            return Ok(());
        };
        entry.status = status;
        entry.rev = entry.rev.wrapping_add(1);
        self.write(entry)?;
        self.space.delete(tuple.id)?;
        Ok(())
    }

    /// Put a stale candidate back at the tail. The replacement is written
    /// before the claimed row is removed, so a crash cannot lose the work.
    fn requeue_tail(&self, entry: &LandingQueueEntry) -> rk_core::Result<u64> {
        let mut retry = entry.clone();
        retry.seq = 0;
        retry.rev = 0;
        retry.status = LandingEntryStatus::Queued;
        retry.candidate_sha = None;
        retry.candidate_base = None;
        retry.candidate_ref = None;
        retry.batch_branches.clear();
        retry.gate_infra_retry_used = false;
        retry.gate_infra_retry_check = None;
        self.enqueue(retry)
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
/// head_sha, target, task)` rather than a fresh random id per call is what makes
/// [`LandingPipeline::request_review`] safe to invoke twice for the same
/// candidate (a restart-driven reprocess): `run_owned_with_id` resolves the
/// second call to the first call's already-running (or already-finished)
/// instance instead of spawning a duplicate reviewer.
fn review_instance_id(entry: &LandingQueueEntry) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(
        format!(
            "{}@{}@{}@{}@{}",
            entry.repo_name, entry.branch, entry.head_sha, entry.target, entry.task
        )
        .as_bytes(),
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
    /// advanced through `Supervisor::land_prepared`. Carries its result JSON
    /// (`merged`, `delivered`, ...).
    Landed(Value),
    /// A gate failed or timed out. `run_check_in` already recorded the
    /// durable `gate-failure` artifact, and a steward `need` row was written
    /// so the hold is visible in `rk inbox`; the branch is left unmerged.
    GateHeld,
    /// Repository policy cannot resolve the complete named gate list. This
    /// is a visible fail-closed configuration state, never an implicit pass.
    NoGate(Tuple),
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
    /// The target moved after this exact merge object passed gates. Nothing
    /// landed; the work was re-enqueued at the tail for rebuild and retest.
    Requeued { seq: u64 },
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
/// doc-only/trivial pass straight to exact candidate advancement — no agent spawn.
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
    enqueue_lock: Mutex<()>,
    key_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

/// One decision [`LandingPipeline::gate_worktree_sweep_once`] made about a
/// specific `(repo, target)` gate worktree — returned (rather than just a
/// count) so `agent.archive`'s `reap_git`/`dry_run` path can report exactly
/// what it did or would do, mirroring `Supervisor::archive_agents`'s
/// `reaped` rows.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct GateWorktreeReclaim {
    pub(crate) repo: String,
    pub(crate) target: String,
    /// Why this worktree was chosen: `"age"` (unused past `max_age_days`) or
    /// `"cap"` (beyond `max_per_repo` most-recently-used targets for this
    /// repo).
    pub(crate) reason: &'static str,
    /// `false` for a `dry_run` pass, when a live queue entry made the key
    /// ineligible, or when the `git worktree remove` itself failed.
    pub(crate) reclaimed: bool,
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
            enqueue_lock: Mutex::new(()),
            key_locks: Mutex::new(HashMap::new()),
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
    /// processed under the SAME task identity (a `landing_processed` marker
    /// exists and its recorded task agrees with `entry.task`, or either side
    /// is blank) — nothing is written, and a redelivered completion tuple is
    /// dropped here rather than repeating gate/review work. `Ok(Some(seq))`
    /// is the fresh queue position, as before.
    ///
    /// A marker recording a DIFFERENT non-empty task is not a redelivery —
    /// it is the same branch/head being resubmitted under the wrong ticket
    /// (an operator typo, or a stale `--task` reused after the real one
    /// already landed and closed). That fails closed with an error instead
    /// of silently reporting `already_processed`, which would otherwise read
    /// as "nothing to do" while leaving the newly-named ticket untouched.
    fn enqueue_disposition(&self, entry: LandingQueueEntry) -> rk_core::Result<EnqueueDisposition> {
        let _guard = self.enqueue_lock.lock().unwrap_or_else(|p| p.into_inner());
        let Some(marker) = self.processed_marker(&entry)? else {
            if self.queue.contains_work_key(&entry)? {
                return Ok(EnqueueDisposition::Pending);
            }
            return Ok(EnqueueDisposition::Queued(self.queue.enqueue(entry)?));
        };
        let recorded_task = marker
            .payload
            .get("task")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !entry.task.is_empty() && !recorded_task.is_empty() && entry.task != recorded_task {
            return Err(rk_core::Error::other(format!(
                "{}@{} in {} was already landed under task {recorded_task} — refusing to \
                 resubmit the same branch/head under a different task {}",
                entry.branch, entry.head_sha, entry.repo_name, entry.task
            )));
        }
        Ok(EnqueueDisposition::Processed)
    }

    pub(crate) fn enqueue(&self, entry: LandingQueueEntry) -> rk_core::Result<Option<u64>> {
        Ok(match self.enqueue_disposition(entry)? {
            EnqueueDisposition::Queued(seq) => Some(seq),
            EnqueueDisposition::Pending | EnqueueDisposition::Processed => None,
        })
    }

    /// Reclaim parked merge objects not referenced by any durable queue row.
    /// Run once during daemon startup; live candidates survive, while the
    /// narrow prepare-before-persist crash window cannot leak refs forever.
    pub(crate) fn sweep_orphaned_candidate_refs(
        &self,
        registered_paths: impl IntoIterator<Item = PathBuf>,
    ) -> usize {
        let queued = self
            .space
            .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
            .unwrap_or_default();
        let live: BTreeSet<String> = queued
            .iter()
            .filter_map(|tuple| {
                tuple
                    .payload
                    .get("candidate_ref")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect();
        let mut paths: BTreeSet<PathBuf> = registered_paths.into_iter().collect();
        paths.extend(queued.iter().filter_map(|tuple| {
            tuple
                .payload
                .get("repo_path")
                .and_then(Value::as_str)
                .map(PathBuf::from)
        }));
        let mut reclaimed = 0;
        for path in paths {
            let Ok(repo) = rk_git::Repo::discover(&path) else {
                continue;
            };
            for candidate_ref in repo.candidate_refs().unwrap_or_default() {
                if !live.contains(&candidate_ref) && repo.discard_candidate(&candidate_ref).is_ok()
                {
                    reclaimed += 1;
                }
            }
        }
        reclaimed
    }

    fn key_lock(&self, repo_name: &str, target: &str) -> Arc<tokio::sync::Mutex<()>> {
        let key = format!("{repo_name}\0{target}");
        let mut locks = self.key_locks.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(
            locks
                .entry(key)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    /// Submit an operator/workflow landing into the same durable queue as
    /// automatic completions, then synchronously drive that key until this
    /// work key reaches a terminal result. Priority changes ordering only;
    /// it never skips preparation, named gates, review, or CAS.
    ///
    /// `task` must resolve to a non-empty identity — `Supervisor::land`
    /// passes `Supervisor::resolve_land_task`'s result, which already infers
    /// it from an agent record for an ordinary agent-bound branch. A branch
    /// with neither an agent record nor an explicit `--task` (a hand-built
    /// or recovery branch nobody bound) fails closed here rather than
    /// enqueueing with `task: ""` — that used to mean no ticket/spec for the
    /// reviewer, no delivery record, and nothing ever closed.
    pub(crate) async fn submit_manual(
        &self,
        repo_root: &Path,
        branch: &str,
        target: &str,
        keep_branch: bool,
        task: Option<String>,
    ) -> rk_core::Result<Value> {
        let Some(task) = task.filter(|t| !t.trim().is_empty()) else {
            return Err(rk_core::Error::other(format!(
                "cannot land {branch} onto {target}: no task identity — this branch carries no \
                 agent record and no --task was given; pass --task <ticket> to bind a hand-built \
                 or recovery submission, or land it from a ticket-dispatched agent so its task is \
                 inferred automatically"
            )));
        };
        let repo = rk_git::Repo::discover(repo_root)?;
        let repo_name = repo.name();
        let head_sha = repo.rev_parse(branch)?;
        let stat = repo.diff_stat(target, branch)?;
        let entry = LandingQueueEntry {
            repo_name: repo_name.clone(),
            repo_path: repo.root().to_string_lossy().to_string(),
            branch: branch.to_string(),
            target: target.to_string(),
            head_sha: head_sha.clone(),
            diff_class: crate::supervisor::classify_diff(&stat.files, stat.lines).to_string(),
            task,
            operator_fast_lane: true,
            keep_branch,
            ..Default::default()
        };
        match self.enqueue_disposition(entry.clone())? {
            EnqueueDisposition::Queued(_) | EnqueueDisposition::Pending => {}
            EnqueueDisposition::Processed => {
                return Ok(json!({
                    "branch": branch,
                    "target": target,
                    "already_processed": true,
                    "detail": "this exact branch/head landing was already processed",
                }));
            }
        }

        let lock = self.key_lock(&repo_name, target);
        let _guard = lock.lock().await;
        loop {
            let Some(claimed) = self.queue.claim_next(&repo_name, target)? else {
                if let Some(prior) = self.processed_outcome(&entry)? {
                    return Ok(json!({
                        "branch": branch,
                        "target": target,
                        "merged": prior == "landed",
                        "delivered": prior == "landed",
                        "status": prior,
                        "detail": "landing was completed while this same-key submitter waited",
                    }));
                }
                return Err(rk_core::Error::other(format!(
                    "landing queue lost operator submission {branch}@{head_sha}"
                )));
            };
            let ours = claimed.branch == branch && claimed.head_sha == head_sha;
            let outcome = self.process_entry(&claimed).await;
            if outcome.is_ok() {
                self.queue.remove(&claimed)?;
            }
            let outcome = outcome?;
            if !ours || matches!(outcome, LandingOutcome::Requeued { .. }) {
                continue;
            }
            return Ok(match outcome {
                LandingOutcome::Landed(result) => result,
                LandingOutcome::GateHeld => json!({
                    "branch": branch, "target": target, "merged": false,
                    "delivered": false, "status": "gate-held",
                }),
                LandingOutcome::NoGate(need) => json!({
                    "branch": branch, "target": target, "merged": false,
                    "delivered": false, "status": "no-gate", "need": need.id.to_string(),
                }),
                LandingOutcome::ReworkFiled(ticket) => json!({
                    "branch": branch, "target": target, "merged": false,
                    "delivered": false, "status": "rework-filed", "ticket": ticket.id.to_string(),
                }),
                LandingOutcome::Escalated(need) => json!({
                    "branch": branch, "target": target, "merged": false,
                    "delivered": false, "status": "escalated", "need": need.id.to_string(),
                }),
                LandingOutcome::Reconciled(prior) => json!({
                    "branch": branch, "target": target, "merged": prior == "landed",
                    "delivered": prior == "landed", "status": prior,
                }),
                LandingOutcome::Requeued { .. } => unreachable!(),
            });
        }
    }

    /// The durable `landing_processed` marker tuple for `entry`'s exact
    /// `(repo, branch, head_sha)` work key, if one exists — the base read
    /// both [`Self::enqueue`]'s dedup/mismatch probe and
    /// [`Self::processed_outcome`] build on. See [`Self::mark_processed`],
    /// the write side.
    fn processed_marker(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<Tuple>> {
        let pattern = Pattern::for_commit(
            Category::Event,
            LANDING_PROCESSED_IDENTITY,
            &entry.branch,
            &entry.head_sha,
        )
        .scope(&entry.repo_name);
        Ok(self.space.scan(&pattern)?.into_iter().next())
    }

    /// The recorded terminal outcome string for `entry`'s work key, when a
    /// `landing_processed` marker exists — the read side used both by
    /// `process_entry`'s crash-window reconciliation and `submit_manual`'s
    /// post-drain lookup.
    fn processed_outcome(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<String>> {
        Ok(self.processed_marker(entry)?.and_then(|t| {
            t.payload
                .get("outcome")
                .and_then(Value::as_str)
                .map(str::to_string)
        }))
    }

    /// Durably record that `entry`'s work key reached a terminal outcome —
    /// the write side of [`Self::processed_marker`]'s dedup/mismatch probe.
    /// Called from every terminal return in [`Self::process_entry`],
    /// independent of whether `entry` arrived through the queue or a direct
    /// call (a caller bypassing the queue for testing still gets the same
    /// double-land protection on its next `enqueue`).
    fn mark_processed(
        &self,
        entry: &LandingQueueEntry,
        outcome: &LandingOutcome,
    ) -> rk_core::Result<()> {
        let outcome_str = match outcome {
            LandingOutcome::Landed(_) => "landed",
            LandingOutcome::GateHeld => "gate-held",
            LandingOutcome::NoGate(_) => "no-gate",
            LandingOutcome::ReworkFiled(_) => "rework-filed",
            LandingOutcome::Escalated(_) => "escalated",
            LandingOutcome::Requeued { .. } => return Ok(()),
            // A reconciled entry's marker already exists from the run that
            // performed the side effects; writing a second would corrupt the
            // one-marker-per-work-key invariant `processed_marker` reads.
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
    /// a prepared landing call that errored rather than cleanly reporting
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
        let mut entry = entry.clone();
        let repo_path = PathBuf::from(&entry.repo_path);
        let git_repo = {
            let repo_path = repo_path.clone();
            blocking(move || rk_git::Repo::discover(&repo_path)).await?
        };
        if let Some(outcome) = self.recover_completed_land(&entry, &git_repo).await? {
            return Ok(outcome);
        }
        let gates = self.gate_config(&git_repo);
        let checks_file = repo_path.join(".rk").join("checks.cue");
        if let Err(error) = self.gate_plan(&checks_file, &entry.target, &gates) {
            let need = self.escalate(
                &entry,
                format!(
                    "steward: NO GATE for {} on {} — branch held unmerged: {error}",
                    entry.task, entry.branch
                ),
            )?;
            self.space.out(
                Tuple::new(
                    Category::Event,
                    entry.repo_name.clone(),
                    "landing_no_gate",
                    "daemon",
                    json!({
                        "branch": entry.branch,
                        "target": entry.target,
                        "task": entry.task,
                        "error": error.to_string(),
                        "state": "no-gate",
                    }),
                )
                .with_lifecycle(Lifecycle::Furniture),
            )?;
            let outcome = LandingOutcome::NoGate(need);
            self.mark_processed(&entry, &outcome)?;
            return Ok(outcome);
        }
        let candidate = if let (Some(commit), Some(base), Some(candidate_ref)) = (
            entry.candidate_sha.clone(),
            entry.candidate_base.clone(),
            entry.candidate_ref.clone(),
        ) {
            rk_git::PreparedMerge {
                commit,
                base,
                candidate_ref,
            }
        } else {
            let repo = git_repo.clone();
            let branch = entry.branch.clone();
            let target = entry.target.clone();
            match blocking(move || repo.prepare_merge(&branch, &target)).await? {
                rk_git::PrepareOutcome::Prepared(candidate) => {
                    entry.candidate_sha = Some(candidate.commit.clone());
                    entry.candidate_base = Some(candidate.base.clone());
                    entry.candidate_ref = Some(candidate.candidate_ref.clone());
                    self.queue
                        .persist(&mut entry, LandingEntryStatus::RunningGates)?;
                    candidate
                }
                rk_git::PrepareOutcome::Conflict { detail } => {
                    self.escalate(
                        &entry,
                        format!(
                            "steward: merge preparation FAILED for {} on {} — branch held unmerged: {detail}",
                            entry.task, entry.branch
                        ),
                    )?;
                    let outcome = LandingOutcome::GateHeld;
                    self.mark_processed(&entry, &outcome)?;
                    return Ok(outcome);
                }
            }
        };
        if !self
            .run_gates_at(&mut entry, &git_repo, &gates, &candidate.commit)
            .await?
        {
            git_repo.discard_candidate(&candidate.candidate_ref)?;
            // The durable gate-failure artifact carries the evidence; the
            // need row is what makes the hold VISIBLE in `rk inbox` — parity
            // with the CUE steward's escalation contract. A hold that
            // followed an exhausted infra-death retry says so explicitly —
            // "one precise human gate" distinct from an ordinary red check,
            // since the automatic recovery path already ran and lost.
            let text = if entry.gate_infra_retry_used {
                format!(
                    "steward: run gate FAILED for {} on {} after an automatic infrastructure-death retry was exhausted — branch held unmerged; read the durable gate-failure and landing_gate_infra_retry artifacts for the evidence",
                    entry.task, entry.branch
                )
            } else {
                format!(
                    "steward: run gate FAILED for {} on {} — branch held unmerged; read the                      durable gate-failure artifact for the failing tests",
                    entry.task, entry.branch
                )
            };
            self.escalate(&entry, text)?;
            let outcome = LandingOutcome::GateHeld;
            self.mark_processed(&entry, &outcome)?;
            return Ok(outcome);
        }
        if matches!(entry.diff_class.as_str(), "doc-only" | "trivial") {
            self.note_non_main_land_target(&entry);
            self.queue.set_status(&entry, LandingEntryStatus::Landing)?;
            let result = self
                .supervisor
                .land_prepared(
                    Path::new(&entry.repo_path),
                    &entry.branch,
                    &entry.target,
                    entry.keep_branch,
                    &candidate,
                )
                .await?;
            if result.get("stale").and_then(Value::as_bool) == Some(true) {
                return self.requeue_stale(&entry, &git_repo, &candidate, &result);
            }
            self.record_delivery(&entry, &result).await;
            let outcome = LandingOutcome::Landed(result);
            self.mark_processed(&entry, &outcome)?;
            return Ok(outcome);
        }
        let verdict = self.review_verdict(&entry, &gates).await?;
        let outcome = self
            .route_verdict_prepared(&entry, verdict, &gates, &git_repo, &candidate)
            .await?;
        if !matches!(
            outcome,
            LandingOutcome::Landed(_) | LandingOutcome::Requeued { .. }
        ) {
            git_repo.discard_candidate(&candidate.candidate_ref)?;
        }
        self.mark_processed(&entry, &outcome)?;
        Ok(outcome)
    }

    fn requeue_stale(
        &self,
        entry: &LandingQueueEntry,
        repo: &rk_git::Repo,
        candidate: &rk_git::PreparedMerge,
        result: &Value,
    ) -> rk_core::Result<LandingOutcome> {
        repo.discard_candidate(&candidate.candidate_ref)?;
        let seq = self.queue.requeue_tail(entry)?;
        let actual = result
            .get("actual_target_sha")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        self.space.out(
            Tuple::new(
                Category::Event,
                entry.repo_name.clone(),
                "landing_candidate_requeued",
                "daemon",
                json!({
                    "branch": entry.branch,
                    "target": entry.target,
                    "tested_sha": candidate.commit,
                    "expected_target_sha": candidate.base,
                    "actual_target_sha": actual,
                    "seq": seq,
                    "text": format!(
                        "target {} moved after gates; {} requeued at tail for rebuild and retest",
                        entry.target, entry.branch
                    ),
                }),
            )
            .with_lifecycle(Lifecycle::Furniture),
        )?;
        warn!(
            repo = %entry.repo_name,
            branch = %entry.branch,
            target = %entry.target,
            tested_sha = %candidate.commit,
            actual_target_sha = actual,
            seq,
            "landing pipeline: stale tested merge requeued at tail"
        );
        Ok(LandingOutcome::Requeued { seq })
    }

    async fn process_batch(
        &self,
        mut entries: Vec<LandingQueueEntry>,
    ) -> rk_core::Result<Vec<(LandingQueueEntry, LandingOutcome)>> {
        if entries.len() <= 1
            || entries
                .iter()
                .any(|entry| !matches!(entry.diff_class.as_str(), "doc-only" | "trivial"))
        {
            let mut outcomes = Vec::with_capacity(entries.len());
            for entry in entries {
                let outcome = self.process_entry(&entry).await?;
                outcomes.push((entry, outcome));
            }
            return Ok(outcomes);
        }

        let repo = rk_git::Repo::discover(Path::new(&entries[0].repo_path))?;
        let gates = self.gate_config(&repo);
        let checks_file = Path::new(&entries[0].repo_path).join(".rk").join("checks.cue");
        if self.gate_plan(&checks_file, &entries[0].target, &gates).is_err() {
            let mut outcomes = Vec::with_capacity(entries.len());
            for entry in entries {
                outcomes.push((entry.clone(), self.process_entry(&entry).await?));
            }
            return Ok(outcomes);
        }

        let branch_names: Vec<String> = entries.iter().map(|e| e.branch.clone()).collect();
        let recovered = entries.iter().find_map(|entry| {
            match (
                entry.candidate_sha.as_ref(),
                entry.candidate_base.as_ref(),
                entry.candidate_ref.as_ref(),
            ) {
                (Some(commit), Some(base), Some(candidate_ref))
                    if entry.batch_branches == branch_names =>
                {
                    Some(rk_git::PreparedMerge {
                        commit: commit.clone(),
                        base: base.clone(),
                        candidate_ref: candidate_ref.clone(),
                    })
                }
                _ => None,
            }
        });
        let candidate = if let Some(candidate) = recovered {
            candidate
        } else {
            let batch_repo = repo.clone();
            let branches = branch_names.clone();
            let target = entries[0].target.clone();
            match blocking(move || batch_repo.prepare_merge_batch(&branches, &target)).await? {
                rk_git::PrepareOutcome::Prepared(candidate) => candidate,
                rk_git::PrepareOutcome::Conflict { .. } => {
                    return self.bisect_batch(entries, None).await;
                }
            }
        };
        for entry in &mut entries {
            entry.candidate_sha = Some(candidate.commit.clone());
            entry.candidate_base = Some(candidate.base.clone());
            entry.candidate_ref = Some(candidate.candidate_ref.clone());
            entry.batch_branches = branch_names.clone();
            self.queue
                .persist(entry, LandingEntryStatus::RunningGates)?;
        }

        // The gate run mutates and durably persists `entries[0]`'s own
        // infra-retry-budget fields (and, on the infra-retry path, re-persists
        // its candidate identity too — see `run_gates_at`). It must run
        // against `entries[0]` ITSELF, not a clone taken before the candidate
        // loop above: a clone would still carry `candidate_sha: None` at this
        // point, and persisting that stale snapshot from inside the retry
        // path would clobber the candidate identity the loop just wrote,
        // corrupting restart recovery for the whole batch
        // (TKT-01M0FXGQMA10JYCV9QCGEAK4TT audit finding). Cloning only AFTER
        // this call — into `first` below — is also what keeps the retry
        // budget itself from silently diverging between `entries[0]`'s
        // in-memory copy and its durable tuple, which would otherwise let a
        // later `bisect_batch`/`mark_processed` pass over `entries` grant a
        // second retry the durable store had already spent.
        if !self
            .run_gates_at(&mut entries[0], &repo, &gates, &candidate.commit)
            .await?
        {
            return self.bisect_batch(entries, Some(&candidate)).await;
        }

        let first = entries[0].clone();
        self.note_non_main_land_target(&first);
        let mut result = self
            .supervisor
            .land_prepared(
                Path::new(&first.repo_path),
                &first.branch,
                &first.target,
                true,
                &candidate,
            )
            .await?;
        if result.get("stale").and_then(Value::as_bool) == Some(true) {
            let mut outcomes = Vec::with_capacity(entries.len());
            for entry in entries {
                let outcome = self.requeue_stale(&entry, &repo, &candidate, &result)?;
                outcomes.push((entry, outcome));
            }
            return Ok(outcomes);
        }

        let policy = self.supervisor.repository_policy(&repo);
        let mut all_deleted = true;
        if policy.delivery.delete_source {
            for entry in &entries {
                if entry.keep_branch {
                    all_deleted = false;
                    continue;
                }
                if repo.delete_branch(&entry.branch).is_err() {
                    all_deleted = false;
                }
            }
        } else {
            all_deleted = false;
        }
        result["branch_deleted"] = Value::Bool(all_deleted);
        result["batch_branches"] = json!(branch_names);
        result["batch_size"] = json!(entries.len());

        let mut outcomes = Vec::with_capacity(entries.len());
        for entry in entries {
            self.record_delivery(&entry, &result).await;
            let outcome = LandingOutcome::Landed(result.clone());
            self.mark_processed(&entry, &outcome)?;
            outcomes.push((entry, outcome));
        }
        Ok(outcomes)
    }

    async fn bisect_batch(
        &self,
        mut entries: Vec<LandingQueueEntry>,
        candidate: Option<&rk_git::PreparedMerge>,
    ) -> rk_core::Result<Vec<(LandingQueueEntry, LandingOutcome)>> {
        if let Some(candidate) = candidate {
            let repo = rk_git::Repo::discover(Path::new(&entries[0].repo_path))?;
            repo.discard_candidate(&candidate.candidate_ref)?;
        }
        // Deliberately does NOT reset `gate_infra_retry_used`/
        // `gate_infra_retry_check` here, unlike `LandingQueue::requeue_tail`.
        // A bisect is a same-pass continuation of the batch gate run that
        // just failed, not a genuinely new future landing attempt — an entry
        // whose budget the batch run already spent stays spent through the
        // split, so re-splitting a stubborn batch can never manufacture more
        // than the one retry the parent run was entitled to. Each entry's
        // OWN field values (not a stale clone's) are what get persisted here
        // — `entries[0]`'s retry fields are exactly what `run_gates_at` just
        // durably wrote in `process_batch`, and every other entry's is
        // whatever it already carried (untouched budget, since only
        // `entries[0]` drove the shared gate run) — so no clone can diverge
        // from its own durable tuple here (audit: TKT-01M0FXGQMA10JYCV9QCGEAK4TT).
        for entry in &mut entries {
            entry.candidate_sha = None;
            entry.candidate_base = None;
            entry.candidate_ref = None;
            entry.batch_branches.clear();
            self.queue
                .persist(entry, LandingEntryStatus::RunningGates)?;
        }
        if entries.len() == 1 {
            let entry = entries.pop().unwrap();
            let outcome = self.process_entry(&entry).await?;
            return Ok(vec![(entry, outcome)]);
        }
        let right = entries.split_off(entries.len() / 2);
        let mut outcomes = Box::pin(self.process_batch(entries)).await?;
        outcomes.extend(Box::pin(self.process_batch(right)).await?);
        Ok(outcomes)
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
    /// prior run's recommendation for this exact review context, regardless
    /// of who wrote it (§1.3). A hit is honored identically to a fresh verdict
    /// — never re-reviewed to shop for a better opinion.
    fn cached_verdict(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<String>> {
        let expected_review_attempt = review_instance_id(entry);
        let pattern = Pattern::for_commit(
            Category::Artifact,
            REVIEW_ARTIFACT_IDENTITY,
            &entry.branch,
            &entry.head_sha,
        )
        .scope(&entry.repo_name);
        Ok(self.space.scan(&pattern)?.into_iter().find_map(|t| {
            let payload = &t.payload;
            if payload.get("task").and_then(Value::as_str) == Some(entry.task.as_str())
                && payload.get("target").and_then(Value::as_str) == Some(entry.target.as_str())
                && payload.get("review_attempt").and_then(Value::as_str)
                    == Some(expected_review_attempt.as_str())
            {
                payload
                    .get("recommendation")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            } else {
                None
            }
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

        let instance_id = review_instance_id(entry);
        let mut params = HashMap::new();
        params.insert("taskId".to_string(), Value::String(entry.task.clone()));
        params.insert("branch".to_string(), Value::String(entry.branch.clone()));
        params.insert("repo".to_string(), Value::String(entry.repo_name.clone()));
        params.insert("target".to_string(), Value::String(entry.target.clone()));
        params.insert("headSha".to_string(), Value::String(entry.head_sha.clone()));
        params.insert(
            "reviewAttempt".to_string(),
            Value::String(instance_id.clone()),
        );
        params.insert(
            "reviewTimeout".to_string(),
            Value::String(format!("{}s", gates.review_max_wait.as_secs())),
        );
        let review = rk_core::review::ReviewContext {
            branch: entry.branch.clone(),
            head_sha: entry.head_sha.clone(),
            target: entry.target.clone(),
            task: entry.task.clone(),
            attempt: instance_id.clone(),
        };
        // The engine's `repo` argument is a filesystem path (it feeds
        // `Repo::discover` and repo-local definition resolution), unlike the
        // `repo` WORKFLOW PARAM above, which is the repo's scope name used
        // to address its verdict artifact.
        self.engine.run_review_owned_with_id(
            instance_id.clone(),
            REVIEW_WORKFLOW,
            &entry.repo_path,
            params,
            review,
        )?;

        let mut pattern = Pattern::category(Category::Artifact)
            .identity(REVIEW_ARTIFACT_IDENTITY)
            .scope(&entry.repo_name);
        pattern.payload_search = Some(format!("\"review_attempt\":\"{}\"", instance_id));

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
    async fn route_verdict_prepared(
        &self,
        entry: &LandingQueueEntry,
        outcome: ReviewWaitOutcome,
        gates: &GateConfig,
        git_repo: &rk_git::Repo,
        candidate: &rk_git::PreparedMerge,
    ) -> rk_core::Result<LandingOutcome> {
        let verdict = match outcome {
            ReviewWaitOutcome::Verdict(v) => v,
            ReviewWaitOutcome::ReviewerDied(context) => {
                return Ok(LandingOutcome::Escalated(self.review_human_gate(
                    entry,
                    git_repo,
                    "reviewer-died",
                    format!("the reviewer ended without producing a verdict: {context}"),
                    "inspect the failed review and either record a fresh verdict or make the land decision",
                )?));
            }
            ReviewWaitOutcome::CeilingReached => {
                return Ok(LandingOutcome::Escalated(self.review_human_gate(
                    entry,
                    git_repo,
                    "review-wait-exhausted",
                    format!(
                        "the reviewer was still running at the {}s hard wait ceiling",
                        gates.review_max_wait.as_secs()
                    ),
                    "inspect or stop the reviewer, then record a verdict or make the land decision",
                )?));
            }
        };
        match verdict.as_str() {
            "APPROVE" => {
                self.note_non_main_land_target(entry);
                self.queue.set_status(entry, LandingEntryStatus::Landing)?;
                let result = self
                    .supervisor
                    .land_prepared(
                        Path::new(&entry.repo_path),
                        &entry.branch,
                        &entry.target,
                        entry.keep_branch,
                        candidate,
                    )
                    .await?;
                if result.get("stale").and_then(Value::as_bool) == Some(true) {
                    return self.requeue_stale(entry, git_repo, candidate, &result);
                }
                self.record_delivery(entry, &result).await;
                Ok(LandingOutcome::Landed(result))
            }
            "REWORK" => self.route_rework(entry, git_repo).await,
            "STOP" => Ok(LandingOutcome::Escalated(self.review_human_gate(
                entry,
                git_repo,
                "reviewer-stop",
                "the reviewer returned STOP".into(),
                "decide whether to abandon the branch or explicitly override the STOP",
            )?)),
            other => Ok(LandingOutcome::Escalated(self.review_human_gate(
                entry,
                git_repo,
                "unknown-verdict",
                format!("the reviewer returned unrecognized verdict {other:?}"),
                "correct the review artifact to APPROVE, REWORK, or STOP, then resubmit",
            )?)),
        }
    }

    fn review_human_gate(
        &self,
        entry: &LandingQueueEntry,
        git_repo: &rk_git::Repo,
        code: &str,
        detail: String,
        decision: &str,
    ) -> rk_core::Result<Tuple> {
        let stat = git_repo.diff_stat(&entry.target, &entry.branch)?;
        let notes = self
            .review_artifact(entry)?
            .as_ref()
            .map(|artifact| landing_rework::notes(Some(artifact)))
            .filter(|notes| !notes.is_empty())
            .unwrap_or_else(|| "(none recorded)".to_string());
        self.escalate(
            entry,
            format!(
                "steward: review of {} for {} requires a human ({code}) — branch held unmerged.\n\
                 EVIDENCE: exact reviewed head {}; {detail}. Reviewer notes: {notes}\n\
                 DECISION NEEDED: {decision}\n\
                 BLAST RADIUS: {} file(s) / {} line(s) on {}, held back from {}. Nothing merged.\n\
                 RESOLVE WITH: rk land {} --repo {} --target {} --task {} --force --reason \
                 'human resolved {code}'",
                entry.branch,
                entry.task,
                entry.head_sha,
                stat.files.len(),
                stat.lines,
                entry.branch,
                entry.target,
                entry.branch,
                entry.repo_path,
                entry.target,
                entry.task,
            ),
        )
    }

    #[cfg(test)]
    async fn route_verdict(
        &self,
        entry: &LandingQueueEntry,
        outcome: ReviewWaitOutcome,
        gates: &GateConfig,
    ) -> rk_core::Result<LandingOutcome> {
        let repo = rk_git::Repo::discover(Path::new(&entry.repo_path))?;
        let candidate = match repo.prepare_merge(&entry.branch, &entry.target)? {
            rk_git::PrepareOutcome::Prepared(candidate) => candidate,
            rk_git::PrepareOutcome::Conflict { detail } => {
                return Err(rk_core::Error::other(detail));
            }
        };
        self.route_verdict_prepared(entry, outcome, gates, &repo, &candidate)
            .await
    }

    /// Write the durable delivery record onto `entry`'s ticket and close it
    /// (P1b). Called from every successful-land path in this module, which is
    /// the whole fix: nothing used to close a ticket after a land, so
    /// delivered work sat `in_progress` indefinitely while every later "is it
    /// delivered" question fell back to a live branch ref that the land had
    /// just deleted.
    ///
    /// Skipped — deliberately, not as an error — when the merge produced no
    /// merge commit (`merged: false`, a conflict the queue will surface) or
    /// when the branch was `content_free` (an empty branch is not a delivery).
    /// Best-effort: the merge already happened and is durable in git, so a
    /// failure to annotate the ticket is logged and never turned into a
    /// landing failure that would strand the queue entry.
    ///
    /// Stack-neutral by construction: the record is a merge commit sha plus
    /// the branch and target it landed on. No build tooling is consulted and
    /// no language convention is assumed.
    async fn record_delivery(&self, entry: &LandingQueueEntry, result: &Value) {
        if !entry.task.starts_with(crate::tickets::ID_PREFIX) {
            return;
        }
        if result.get("content_free").and_then(Value::as_bool) == Some(true) {
            info!(
                task = %entry.task,
                branch = %entry.branch,
                "land added nothing over target; not recording a delivery"
            );
            return;
        }
        let Some(merge_commit) = result
            .get("merge_commit")
            .and_then(Value::as_str)
            .filter(|c| !c.is_empty())
        else {
            return;
        };
        let record = crate::tickets::DeliveryRecord {
            merge_commit: merge_commit.to_string(),
            branch: entry.branch.clone(),
            target: entry.target.clone(),
            landed_at: Utc::now().to_rfc3339(),
        };
        match self.tickets.record_delivery(&entry.task, &record).await {
            Ok(_) => info!(
                task = %entry.task,
                merge_commit,
                target = %entry.target,
                "recorded delivery and closed ticket"
            ),
            Err(e) => warn!(
                task = %entry.task,
                merge_commit,
                error = %e,
                "landed but failed to record delivery on the ticket"
            ),
        }
        if let Err(error) = self.resubmit_reworked_parent(entry) {
            // The merge is durable; surface bookkeeping failure without denying it.
            let _ = self.escalate(
                entry,
                format!(
                    "steward: rework {} landed onto {}, but automatic parent resubmission failed: \
                     {error}. Re-submit with `rk land {} --repo {} --target <original-target> \
                     --task <original-ticket>`",
                    entry.task, entry.target, entry.target, entry.repo_path
                ),
            );
            warn!(
                task = %entry.task,
                target = %entry.target,
                error = %error,
                "rework landed but parent resubmission failed"
            );
        }
    }

    /// Recover after an APPROVE-authorized target advance but before delivery.
    async fn recover_completed_land(
        &self,
        entry: &LandingQueueEntry,
        repo: &rk_git::Repo,
    ) -> rk_core::Result<Option<LandingOutcome>> {
        if entry.status != LandingEntryStatus::Landing {
            return Ok(None);
        }
        if self.supervisor.repository_policy(repo).delivery.mode != rk_workflow::DeliveryMode::Merge
        {
            return Ok(None);
        }
        let (Some(commit), Some(base)) = (&entry.candidate_sha, &entry.candidate_base) else {
            return Ok(None);
        };
        if repo.rev_parse(&entry.target)? != *commit {
            return Ok(None);
        }
        let result = json!({
            "branch": entry.branch, "target": entry.target, "delivered": true,
            "merged": true, "merge_commit": commit, "content_free": commit == base,
            "recovered": true,
        });
        self.record_delivery(entry, &result).await;
        let outcome = LandingOutcome::Landed(result);
        self.mark_processed(entry, &outcome)?;
        Ok(Some(outcome))
    }

    /// Queue a corrected parent at its new head; enqueue dedupe makes replay safe.
    fn resubmit_reworked_parent(&self, entry: &LandingQueueEntry) -> rk_core::Result<()> {
        let marker = self
            .space
            .scan(
                &Pattern::category(Category::Event)
                    .identity(REWORK_DISPATCH_IDENTITY)
                    .scope(&entry.repo_name),
            )?
            .into_iter()
            .find(|marker| {
                marker.payload.get("rework_ticket").and_then(Value::as_str)
                    == Some(entry.task.as_str())
                    && marker.payload.get("branch").and_then(Value::as_str)
                        == Some(entry.target.as_str())
                    && matches!(
                        marker.payload.get("state").and_then(Value::as_str),
                        Some("dispatching" | "dispatched")
                    )
            });
        let Some(marker) = marker else {
            return Ok(());
        };
        let payload = &marker.payload;
        let original_branch = required_payload_str(payload, "branch", "rework dispatch marker")?;
        let original_target = required_payload_str(payload, "target", "rework dispatch marker")?;
        let original_task = required_payload_str(payload, "task", "rework dispatch marker")?;
        let repo = rk_git::Repo::discover(Path::new(&entry.repo_path))?;
        let head_sha = repo.rev_parse(original_branch)?;
        let stat = repo.diff_stat(original_target, original_branch)?;
        let parent = LandingQueueEntry {
            repo_name: entry.repo_name.clone(),
            repo_path: entry.repo_path.clone(),
            branch: original_branch.to_string(),
            target: original_target.to_string(),
            head_sha: head_sha.clone(),
            diff_class: crate::supervisor::classify_diff(&stat.files, stat.lines).to_string(),
            task: original_task.to_string(),
            ..Default::default()
        };
        let disposition = self.enqueue_disposition(parent)?;
        if let EnqueueDisposition::Queued(seq) = disposition {
            self.space.out(
                Tuple::new(
                    Category::Event,
                    entry.repo_name.clone(),
                    REWORK_RESUBMISSION_IDENTITY,
                    "daemon",
                    json!({
                        "dispatch_key": payload.get("dispatch_key"),
                        "rework_ticket": entry.task,
                        "rework_branch": entry.branch,
                        "branch": original_branch,
                        "target": original_target,
                        "task": original_task,
                        "head_sha": head_sha,
                        "seq": seq,
                        "state": "queued",
                    }),
                )
                .with_lifecycle(Lifecycle::Furniture),
            )?;
            info!(
                rework_ticket = %entry.task,
                branch = original_branch,
                target = original_target,
                head_sha,
                seq,
                "landed rework queued its corrected parent for fresh review"
            );
        }
        Ok(())
    }

    /// Resolve unattended-rework bounds from the activated repository policy.
    fn rework_policy(&self, repo: &rk_git::Repo) -> ReworkPolicy {
        ReworkPolicy::from_landing(&self.supervisor.repository_policy(repo).landing)
    }

    /// Chain-scoped markers. Counting by head would reset the attempt cap after
    /// every correction; exact-head replay is handled separately below.
    fn rework_dispatch_markers(&self, ctx: &ReworkContext) -> rk_core::Result<Vec<Tuple>> {
        let pattern = Pattern::category(Category::Event)
            .identity(REWORK_DISPATCH_IDENTITY)
            .scope(&ctx.repo);
        Ok(self
            .space
            .scan(&pattern)?
            .into_iter()
            .filter(|t| {
                t.payload.get("branch").and_then(Value::as_str) == Some(ctx.branch.as_str())
                    && t.payload.get("target").and_then(Value::as_str) == Some(ctx.target.as_str())
                    && t.payload.get("task").and_then(Value::as_str) == Some(ctx.task.as_str())
            })
            .collect())
    }

    fn marker_matches(marker: &Tuple, ctx: &ReworkContext) -> bool {
        let payload = &marker.payload;
        let key = ctx.dispatch_key();
        payload.get("dispatch_key").and_then(Value::as_str) == Some(key.as_str())
            || (payload.get("head_sha").and_then(Value::as_str) == Some(ctx.head_sha.as_str())
                && payload.get("rework_ticket").and_then(Value::as_str)
                    == Some(ctx.rework_ticket.as_str()))
    }

    fn record_rework_state(
        &self,
        entry: &LandingQueueEntry,
        ctx: &ReworkContext,
        attempt: u32,
        state: &str,
    ) -> rk_core::Result<Tuple> {
        let marker = Tuple::new(
            Category::Event,
            entry.repo_name.clone(),
            REWORK_DISPATCH_IDENTITY,
            "daemon",
            ctx.marker_payload(attempt, None, state),
        )
        .with_lifecycle(Lifecycle::Furniture);
        self.space.out(marker.clone())?;
        Ok(marker)
    }

    fn withhold_rework(
        &self,
        entry: &LandingQueueEntry,
        ctx: &ReworkContext,
        attempt: u32,
        withheld: &Withheld,
    ) -> rk_core::Result<()> {
        self.escalate(entry, ctx.escalation(withheld))?;
        self.record_rework_state(entry, ctx, attempt, withheld.code)?;
        Ok(())
    }

    fn rework_attempts_used(&self, ctx: &ReworkContext) -> rk_core::Result<u32> {
        let distinct: BTreeSet<String> = self
            .rework_dispatch_markers(ctx)?
            .into_iter()
            .filter(|marker| {
                matches!(
                    marker.payload.get("state").and_then(Value::as_str),
                    Some("dispatching" | "dispatched")
                )
            })
            .map(|marker| {
                marker
                    .payload
                    .get("dispatch_key")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| {
                        format!(
                            "{}\0{}",
                            marker
                                .payload
                                .get("head_sha")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                            marker
                                .payload
                                .get("rework_ticket")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                        )
                    })
            })
            .collect();
        Ok(distinct.len() as u32)
    }

    /// Exact reviewed-commit marker used for replay deduplication.
    fn rework_dispatch_marker(&self, ctx: &ReworkContext) -> rk_core::Result<Option<Tuple>> {
        Ok(self
            .rework_dispatch_markers(ctx)?
            .into_iter()
            .find(|marker| Self::marker_matches(marker, ctx)))
    }

    fn rework_dispatch_has_state(&self, ctx: &ReworkContext, state: &str) -> rk_core::Result<bool> {
        Ok(self
            .rework_dispatch_markers(ctx)?
            .into_iter()
            .any(|marker| {
                Self::marker_matches(&marker, ctx)
                    && marker.payload.get("state").and_then(Value::as_str) == Some(state)
            }))
    }

    /// Spawn's durable journal proves this exact dispatch crossed its commit point.
    fn rework_agent_was_journaled(&self, ctx: &ReworkContext) -> bool {
        self.supervisor.list_all().into_iter().any(|record| {
            record.role == "rat"
                && record.task.as_deref() == Some(ctx.rework_ticket.as_str())
                && record.target_branch == ctx.branch
                && record.fork_point.as_deref() == Some(ctx.head_sha.as_str())
        })
    }

    /// Cumulative chain spend, including terminal and archived agents.
    fn rework_chain_spend(&self, ctx: &ReworkContext) -> rk_core::Result<f64> {
        let rework_tickets: BTreeSet<String> = self
            .rework_dispatch_markers(ctx)?
            .into_iter()
            .filter_map(|marker| {
                marker
                    .payload
                    .get("rework_ticket")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect();
        let spent = self
            .supervisor
            .list_all()
            .iter()
            .filter(|a| {
                a.repo_name == ctx.repo
                    && a.role == "rat"
                    && a.task
                        .as_deref()
                        .is_some_and(|task| task == ctx.task || rework_tickets.contains(task))
            })
            .map(|a| a.cost_usd)
            .sum();
        Ok(spent)
    }

    /// File the ticket, then dispatch one exact-base correction or hold behind
    /// an evidence-rich gate. Both paths retain the durable ticket.
    async fn route_rework(
        &self,
        entry: &LandingQueueEntry,
        git_repo: &rk_git::Repo,
    ) -> rk_core::Result<LandingOutcome> {
        let review = self.review_artifact(entry)?;
        let stat = {
            let repo = git_repo.clone();
            let target = entry.target.clone();
            let branch = entry.branch.clone();
            blocking(move || repo.diff_stat(&target, &branch)).await?
        };
        let ticket = self.file_rework_ticket(entry).await?;
        let ctx = ReworkContext {
            repo: entry.repo_name.clone(),
            branch: entry.branch.clone(),
            head_sha: entry.head_sha.clone(),
            target: entry.target.clone(),
            task: entry.task.clone(),
            rework_ticket: ticket.identity.clone(),
            notes: landing_rework::notes(review.as_ref()),
            diff_files: stat.files.len() as u64,
            diff_lines: stat.lines,
        };

        // The coalesced ticket completes the dispatch key before side effects.
        if let Some(marker) = self.rework_dispatch_marker(&ctx)? {
            // Marker-before-spawn needs a journal check to distinguish success
            // from the interruption window.
            if marker.payload.get("state").and_then(Value::as_str) == Some("dispatching")
                && !self.rework_agent_was_journaled(&ctx)
                && !self.rework_dispatch_has_state(&ctx, "dispatch-interrupted")?
            {
                let attempt = marker
                    .payload
                    .get("attempt")
                    .and_then(Value::as_u64)
                    .and_then(|attempt| u32::try_from(attempt).ok())
                    .unwrap_or_default();
                let withheld = Withheld {
                    code: "dispatch-interrupted",
                    detail: format!(
                        "durable dispatch attempt {attempt} exists, but the supervisor registry \
                         contains no rat for rework ticket {} based on {} at {}; the daemon may \
                         have stopped between recording the marker and journaling the spawn",
                        ctx.rework_ticket, ctx.branch, ctx.head_sha
                    ),
                    decision: "confirm that no correction agent exists, then dispatch the \
                               recorded rework ticket exactly once or abandon it"
                        .into(),
                };
                self.withhold_rework(entry, &ctx, attempt, &withheld)?;
                warn!(
                    repo = %entry.repo_name, branch = %entry.branch,
                    head_sha = %entry.head_sha, ticket = %ctx.rework_ticket, attempt,
                    "landing pipeline: interrupted rework dispatch requires human recovery"
                );
            }
            info!(
                repo = %entry.repo_name, branch = %entry.branch, head_sha = %entry.head_sha,
                ticket = %ctx.rework_ticket,
                "landing pipeline: REWORK already routed for this exact chain; not re-dispatching"
            );
            return Ok(LandingOutcome::ReworkFiled(ticket));
        }

        let policy = self.rework_policy(git_repo);
        let attempts_used = self.rework_attempts_used(&ctx)?;
        let spent_usd = self.rework_chain_spend(&ctx)?;

        let route = landing_rework::route(&policy, review.as_ref(), attempts_used, spent_usd);
        let attempt = match route {
            ReworkRoute::Withhold(withheld) => {
                self.withhold_rework(entry, &ctx, attempts_used, &withheld)?;
                return Ok(LandingOutcome::ReworkFiled(ticket));
            }
            ReworkRoute::Dispatch { attempt } => attempt,
        };

        // Spawn cuts from branch tip, so never dispatch after the reviewed tip moves.
        let tip = {
            let repo = git_repo.clone();
            let branch = entry.branch.clone();
            blocking(move || repo.rev_parse(&branch)).await?
        };
        if tip != entry.head_sha {
            let withheld = Withheld {
                code: "reviewed-head-moved",
                detail: format!(
                    "the reviewed head {} is no longer {}'s tip (now {tip}), so a rework cut from \
                     the branch would start from work this verdict never reviewed",
                    entry.head_sha, entry.branch
                ),
                decision: "re-review the branch at its current tip, or reset it back to the \
                           reviewed head and let the rework dispatch"
                    .into(),
            };
            self.withhold_rework(entry, &ctx, attempts_used, &withheld)?;
            return Ok(LandingOutcome::ReworkFiled(ticket));
        }

        // Marker first: replay gates an interrupted spawn instead of duplicating it.
        self.record_rework_state(entry, &ctx, attempt, "dispatching")?;

        let params = crate::supervisor::SpawnParams {
            repo: entry.repo_path.clone(),
            task: ctx.rework_ticket.clone(),
            prompt: Some(ctx.prompt()),
            role: "rat".to_string(),
            // A correction always lands back on the reviewed branch.
            base: Some(entry.branch.clone()),
            // Ordinary correction spawn, not a machine-routed reviewer.
            review: None,
            coordination: None,
            harness: None,
            parent: None,
            model: None,
            permission_mode: None,
            profile: None,
            resolved_profile: None,
            attach: false,
            workflow_instance: None,
            coordinator: None,
            instance_max_usd: None,
        };
        match self.supervisor.spawn_async(params, 0).await {
            Ok(record) => {
                info!(
                    repo = %entry.repo_name, branch = %entry.branch, head_sha = %entry.head_sha,
                    agent = %record.name, ticket = %ctx.rework_ticket, attempt,
                    "landing pipeline: dispatched bounded rework agent from the reviewed branch"
                );
                if let Err(e) = self
                    .tickets
                    .update(
                        &ctx.rework_ticket,
                        crate::tickets::TicketChanges {
                            assignee: Some(record.name.clone()),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    warn!(
                        ticket = %ctx.rework_ticket, agent = %record.name, error = %e,
                        "landing pipeline: failed to record rework assignee"
                    );
                }
            }
            Err(e) => {
                // Refusal holds the branch and becomes a visible human gate.
                let withheld = Withheld {
                    code: "dispatch-refused",
                    detail: format!("the rework spawn was refused by the supervisor: {e}"),
                    decision: "clear whatever refused the dispatch (budget cap, WIP ceiling, \
                               paused dispatch), then dispatch the rework ticket"
                        .into(),
                };
                self.escalate(entry, ctx.escalation(&withheld))?;
                warn!(
                    repo = %entry.repo_name, branch = %entry.branch, error = %e,
                    "landing pipeline: bounded rework dispatch refused"
                );
            }
        }
        Ok(LandingOutcome::ReworkFiled(ticket))
    }

    /// Full exact-commit artifact used by the classifier.
    fn review_artifact(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<Value>> {
        let pattern = Pattern::for_commit(
            Category::Artifact,
            REVIEW_ARTIFACT_IDENTITY,
            &entry.branch,
            &entry.head_sha,
        )
        .scope(&entry.repo_name);
        Ok(self
            .space
            .scan(&pattern)?
            .into_iter()
            .next()
            .map(|t| t.payload))
    }

    /// File the historical steward-shaped follow-up directly and idempotently.
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
                // Redelivery resolves to the same follow-up ticket.
                coalesce_key: Some(landing_rework::ticket_coalesce_key(
                    &entry.repo_name,
                    &entry.branch,
                    &entry.head_sha,
                    &entry.target,
                    &entry.task,
                )),
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

    /// A candidate about to land on a target other than `"main"` is
    /// otherwise invisible in this pipeline: it never creates a workflow
    /// instance for `rk workflow list` to annotate, so an operator scanning
    /// `rk inbox`/`rk workflow list` cannot tell it apart from a landing to
    /// `main`. Mirrors `Reactor::note_non_main_land_target`'s shape (same
    /// scope, same `text`/`target`/`branch` fields) for the reactor's
    /// `action: "workflow"` path; this is the `action: "land"` counterpart,
    /// called from every landing route in this module.
    fn note_non_main_land_target(&self, entry: &LandingQueueEntry) {
        if entry.target.is_empty() || entry.target == "main" {
            return;
        }
        let text = format!(
            "landing pipeline will land {} on non-main target {}",
            entry.branch, entry.target
        );
        warn!(
            repo = %entry.repo_name,
            branch = %entry.branch,
            target = %entry.target,
            task = %entry.task,
            "landing pipeline landing onto a non-main target"
        );
        let _ = self.space.out(Tuple::new(
            Category::Event,
            entry.repo_name.clone(),
            LANDING_NON_MAIN_TARGET_IDENTITY,
            "daemon",
            json!({
                "text": text,
                "target": entry.target,
                "branch": entry.branch,
                "task": entry.task,
            }),
        ));
    }

    /// Drain every candidate currently queued for `(repo_name, target)`,
    /// oldest first, returning one outcome per candidate in processing order.
    pub(crate) async fn drain_key(
        &self,
        repo_name: &str,
        target: &str,
    ) -> rk_core::Result<Vec<LandingOutcome>> {
        let lock = self.key_lock(repo_name, target);
        let _guard = lock.lock().await;
        let mut outcomes = Vec::new();
        loop {
            let entries = self.queue.claim_batch(repo_name, target, 8)?;
            if entries.is_empty() {
                break;
            }
            let processed = self.process_batch(entries).await?;
            for (entry, outcome) in processed {
                self.queue.remove(&entry)?;
                outcomes.push(outcome);
            }
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
    async fn run_gates_at(
        &self,
        entry: &mut LandingQueueEntry,
        git_repo: &rk_git::Repo,
        gates: &GateConfig,
        tested_sha: &str,
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
            let sha = tested_sha.to_string();
            blocking(move || git_repo.reset_gate_worktree(&gate_dir, &sha)).await?;
        }
        // Record this reset for `gate_worktree_sweep_once`'s LRU ordering —
        // AFTER the reset succeeds, so a failed ensure/reset never marks a
        // worktree "just used" that was not actually touched.
        self.touch_gate_worktree_marker(&entry.repo_name, &entry.target);

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
                shared_cargo_target: check.shared_cargo_target,
            };

            // Resuming after a crash landed between spending the retry
            // budget and the retry attempt completing (`gate_infra_retry_check`'s
            // doc): this exact check is durably marked mid-retry from a PRIOR
            // process. Whatever THIS attempt does completes that retry — it
            // is ordinal 2, never a fresh ordinal-1 death — so it is settled
            // through the exact same path the inline retry below uses,
            // without ever granting a second retry.
            if entry.gate_infra_retry_check.as_deref() == Some(check.name.as_str()) {
                let retry_outcome = self
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
                if !self
                    .finish_infra_retry(entry, tested_sha, &check.name, retry_outcome)
                    .await?
                {
                    return Ok(false);
                }
                continue;
            }

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
                // An infra death (the child never reported its own exit code
                // — killed by a signal, or any other runner-loss shape) is
                // not a verdict on the branch, so it earns exactly one
                // automatic retry of this SAME check against this SAME
                // prepared candidate, bounded by the durable per-entry
                // budget. Everything else (a real "fail", a "timeout") falls
                // straight through to `Ok(_) => return Ok(false)` below —
                // never retried, held immediately.
                Ok(result)
                    if result.get("verdict").and_then(Value::as_str) == Some("infra")
                        && !entry.gate_infra_retry_used =>
                {
                    warn!(
                        check = %check.name, branch = %entry.branch,
                        "landing pipeline: gate check died to an infrastructure fault, retrying once"
                    );
                    // Spend the budget AND durably mark THIS check in-flight
                    // BEFORE anything else — including the ordinal-1 evidence
                    // write below — so a crash at any point from here onward
                    // resumes (via the `gate_infra_retry_check` branch above)
                    // as exactly one more attempt of this same check, never a
                    // duplicate ordinal-1 death or a second retry grant.
                    entry.gate_infra_retry_used = true;
                    entry.gate_infra_retry_check = Some(check.name.clone());
                    self.queue
                        .persist(entry, LandingEntryStatus::RunningGates)?;
                    self.record_gate_infra_attempt(entry, tested_sha, &check.name, 1, &result)?;
                    let retry_outcome = self
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
                    if !self
                        .finish_infra_retry(entry, tested_sha, &check.name, retry_outcome)
                        .await?
                    {
                        return Ok(false);
                    }
                }
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

    /// Settle a gate-infrastructure-death retry's outcome — the ordinal-2
    /// attempt, whether reached inline (the retry `run_gates_at` just
    /// launched itself) or by resuming one a prior process crashed mid-flight
    /// (`gate_infra_retry_check`'s doc). Records the durable per-attempt
    /// evidence (fail-closed: an evidence-write failure propagates as `Err`
    /// rather than letting the caller silently continue or land on
    /// unrecorded evidence — TKT-01M0FXGQMA10JYCV9QCGEAK4TT), clears the
    /// in-flight marker, and reports whether the gate run may continue
    /// (`Ok(true)`, the retry passed) or must hold (`Ok(false)`).
    async fn finish_infra_retry(
        &self,
        entry: &mut LandingQueueEntry,
        tested_sha: &str,
        check_name: &str,
        retry_outcome: rk_core::Result<Value>,
    ) -> rk_core::Result<bool> {
        let (result, passed) = match retry_outcome {
            Ok(result) => {
                let passed = result.get("verdict").and_then(Value::as_str) == Some("pass");
                (result, passed)
            }
            Err(e) => {
                warn!(error = %e, check = %check_name, branch = %entry.branch, "landing pipeline: gate retry errored, holding branch");
                (json!({"verdict": "error", "exit": -1}), false)
            }
        };
        self.record_gate_infra_attempt(entry, tested_sha, check_name, 2, &result)?;
        entry.gate_infra_retry_check = None;
        self.queue
            .persist(entry, LandingEntryStatus::RunningGates)?;
        Ok(passed)
    }

    /// Durable per-attempt evidence for a gate-infrastructure-death retry
    /// (bounded fail-safe recovery — see [`Self::run_gates_at`]): one event
    /// per attempt, so the full history (the original death, and the retry
    /// this pipeline allows) is inspectable even across a daemon restart
    /// between the two. `ordinal` is 1 for the original attempt that died, 2
    /// for the retry (fresh or resumed). This evidence is an acceptance
    /// invariant, not a diagnostic nicety — a write failure propagates as
    /// `Err` (fail-closed) instead of letting the gate run silently continue
    /// or land without a durable record of what the retry actually did.
    fn record_gate_infra_attempt(
        &self,
        entry: &LandingQueueEntry,
        tested_sha: &str,
        check_name: &str,
        ordinal: u32,
        result: &Value,
    ) -> rk_core::Result<()> {
        let verdict = result.get("verdict").and_then(Value::as_str).unwrap_or("");
        let disposition = match (ordinal, verdict) {
            (1, _) => "retrying",
            (_, "pass") => "retry_passed",
            _ => "retry_exhausted",
        };
        self.space.out(
            Tuple::new(
                Category::Event,
                entry.repo_name.clone(),
                GATE_INFRA_RETRY_IDENTITY,
                "daemon",
                json!({
                    "branch": entry.branch,
                    "target": entry.target,
                    "task": entry.task,
                    "candidate_sha": tested_sha,
                    "check": check_name,
                    "ordinal": ordinal,
                    "exit": result.get("exit").cloned().unwrap_or(Value::Null),
                    "signal": result.get("signal").cloned().unwrap_or(Value::Null),
                    "verdict": verdict,
                    "disposition": disposition,
                }),
            )
            .with_lifecycle(Lifecycle::Furniture),
        )
    }

    #[cfg(test)]
    async fn run_gates(
        &self,
        entry: &mut LandingQueueEntry,
        git_repo: &rk_git::Repo,
        gates: &GateConfig,
    ) -> rk_core::Result<bool> {
        let head_sha = entry.head_sha.clone();
        self.run_gates_at(entry, git_repo, gates, &head_sha).await
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

    /// Sibling marker file recording when `gate_worktree_path(repo, target)`
    /// was last reset for a landing attempt — `gate_worktree_sweep_once`'s
    /// LRU signal. Deliberately NOT inside the worktree itself:
    /// `reset_gate_worktree` runs `git clean -fd` on every reset, which
    /// would delete an untracked marker living inside the checkout, and a
    /// `verify` check running arbitrary repo-owned commands should never be
    /// able to touch retention bookkeeping.
    fn gate_worktree_marker_path(&self, repo_name: &str, target: &str) -> PathBuf {
        self.layout
            .home()
            .join("gate-worktrees")
            .join(sanitize_path_component(repo_name))
            .join(format!("{}.last-used", sanitize_path_component(target)))
    }

    fn read_gate_worktree_marker(&self, repo_name: &str, target: &str) -> Option<DateTime<Utc>> {
        let raw =
            std::fs::read_to_string(self.gate_worktree_marker_path(repo_name, target)).ok()?;
        DateTime::parse_from_rfc3339(raw.trim())
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    }

    /// Record that `(repo_name, target)`'s gate worktree was just reset for
    /// a landing attempt. Best-effort: a write failure here only skews
    /// `gate_worktree_sweep_once`'s LRU ordering, never the gate run itself,
    /// so it is logged and swallowed rather than propagated.
    fn touch_gate_worktree_marker(&self, repo_name: &str, target: &str) {
        let path = self.gate_worktree_marker_path(repo_name, target);
        if let Some(parent) = path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                warn!(error = %e, ?path, "landing pipeline: failed to create gate worktree marker dir");
                return;
            }
        }
        if let Err(e) = std::fs::write(&path, Utc::now().to_rfc3339()) {
            warn!(error = %e, ?path, "landing pipeline: failed to touch gate worktree last-used marker");
        }
    }

    /// Reclaim gate worktrees per
    /// docs/proposals/daemon-native-landing-pipeline.md §5 open question 4:
    /// enforce `cfg.max_age_days` (LRU by last landing attempt) and
    /// `cfg.max_per_repo` (a hard cap on how many target worktrees one repo
    /// may keep warm at once) over every `<home>/gate-worktrees/<repo>/
    /// <target>` directory found ON DISK — not `LandingQueue::pending_keys`,
    /// which only sees keys with a currently-queued candidate and would
    /// miss a target that finished landing days ago and has sat idle since.
    ///
    /// A key with any live `LandingQueue` entry (`queued`/`running_gates`/
    /// `awaiting_review`) is always skipped, dry run or not — the busy
    /// check this sweep leans on instead of a lock, matching
    /// `Supervisor::reap_git`'s own "only touch what is provably idle"
    /// posture for agent worktrees. `dry_run: true` computes the exact same
    /// eligible set without touching disk; every row comes back with
    /// `reclaimed: false`.
    pub(crate) fn gate_worktree_sweep_once(
        &self,
        cfg: &rk_core::config::GateWorktreeSweepConfig,
        dry_run: bool,
    ) -> Vec<GateWorktreeReclaim> {
        let root = self.layout.home().join("gate-worktrees");
        let Ok(repo_dirs) = std::fs::read_dir(&root) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for repo_entry in repo_dirs.flatten() {
            let repo_path = repo_entry.path();
            if !repo_path.is_dir() {
                continue;
            }
            let repo_name = repo_entry.file_name().to_string_lossy().to_string();
            out.extend(self.sweep_repo_gate_worktrees(&repo_name, &repo_path, cfg, dry_run));
        }
        out
    }

    fn sweep_repo_gate_worktrees(
        &self,
        repo_name: &str,
        repo_dir: &Path,
        cfg: &rk_core::config::GateWorktreeSweepConfig,
        dry_run: bool,
    ) -> Vec<GateWorktreeReclaim> {
        let Ok(entries) = std::fs::read_dir(repo_dir) else {
            return Vec::new();
        };
        // (target, last_used, worktree_path), sorted most-recently-used
        // first below. A target with no marker yet (raced with its own
        // first `run_gates`, or predates this feature) sorts as maximally
        // stale — eligible for the age rule, but still protected by the
        // busy check the same way a freshly-claimed candidate always is.
        let mut targets: Vec<(String, DateTime<Utc>, PathBuf)> = entries
            .flatten()
            .filter_map(|e| {
                let path = e.path();
                if !path.is_dir() {
                    return None; // skips sibling `*.last-used` marker files
                }
                let target = e.file_name().to_string_lossy().to_string();
                let last_used = self
                    .read_gate_worktree_marker(repo_name, &target)
                    .unwrap_or(DateTime::<Utc>::MIN_UTC);
                Some((target, last_used, path))
            })
            .collect();
        targets.sort_by_key(|t| std::cmp::Reverse(t.1));

        let now = Utc::now();
        let mut out = Vec::new();
        for (index, (target, last_used, path)) in targets.into_iter().enumerate() {
            let busy = match self.queue.scan_current(repo_name, Some(&target)) {
                Ok(live) => !live.is_empty(),
                // Scan failure: fail closed, treat as busy rather than risk
                // reclaiming a worktree a candidate might still be using.
                Err(_) => true,
            };
            if busy {
                continue;
            }
            let over_cap = cfg.max_per_repo > 0 && (index as u64) >= cfg.max_per_repo;
            let stale = cfg.max_age_days > 0
                && now.signed_duration_since(last_used)
                    > chrono::Duration::days(cfg.max_age_days as i64);
            if !over_cap && !stale {
                continue;
            }
            let reason = if over_cap { "cap" } else { "age" };
            let reclaimed = if dry_run {
                false
            } else {
                match rk_git::Repo::discover(&path).and_then(|repo| repo.remove_worktree(&path)) {
                    Ok(()) => {
                        let _ = std::fs::remove_file(
                            self.gate_worktree_marker_path(repo_name, &target),
                        );
                        info!(repo = %repo_name, target = %target, reason, "landing pipeline: reclaimed gate worktree");
                        true
                    }
                    Err(e) => {
                        warn!(error = %e, repo = %repo_name, target = %target, "landing pipeline: failed to reclaim gate worktree");
                        false
                    }
                }
            };
            out.push(GateWorktreeReclaim {
                repo: repo_name.to_string(),
                target,
                reason,
                reclaimed,
            });
        }
        out
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
    /// Deliberately omits the new declarative `review` block: the daemon-owned
    /// instance context must still bind the spawn when the globally installed
    /// workflow copy predates this feature.
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
            shared_cargo_target: false,
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

    #[test]
    fn snapshot_and_summary_report_depth_and_oldest_age_surviving_requeue() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let space = Space::open_in_memory().unwrap();
        let queue = LandingQueue::new(space.clone(), &layout);

        // A candidate that's been sitting for 5h, plus a fresh one right
        // behind it in the same (repo, target) key.
        let old_enqueued_at = Utc::now() - chrono::Duration::hours(5);
        let base = LandingQueueEntry {
            repo_name: "alpha".into(),
            repo_path: "/repos/alpha".into(),
            branch: "b1".into(),
            target: "main".into(),
            head_sha: "sha-old".into(),
            diff_class: "trivial".into(),
            task: "TKT-1".into(),
            enqueued_at: Some(old_enqueued_at),
            ..Default::default()
        };
        queue.enqueue(base.clone()).unwrap();
        queue
            .enqueue(LandingQueueEntry {
                branch: "b2".into(),
                head_sha: "sha-fresh".into(),
                task: "TKT-2".into(),
                enqueued_at: None,
                ..base.clone()
            })
            .unwrap();

        let summary = landing_queue_summary(&space);
        assert_eq!(summary.len(), 1, "one (repo, target) key");
        let q = &summary[0];
        assert_eq!(q.repo, "alpha");
        assert_eq!(q.target, "main");
        assert_eq!(q.depth, 2);
        // The 5h-old entry is the oldest even though it enqueued first and a
        // fresher one shares the key.
        assert!(q.oldest_age_secs >= 5 * 3600 - 5);
        assert_eq!(q.oldest_branch, "b1");

        // FIFO claims b1 first regardless of the age we set by hand.
        let claimed = queue.claim_next("alpha", "main").unwrap().unwrap();
        assert_eq!(claimed.branch, "b1");
        assert_eq!(claimed.enqueued_at, Some(old_enqueued_at));

        // A stale-target requeue resets seq/rev/status but must NOT reset
        // enqueued_at — otherwise a candidate stuck in a requeue loop would
        // look freshly-arrived on every cycle, hiding exactly the wedge
        // probe O18 wants surfaced.
        queue.requeue_tail(&claimed).unwrap();
        let after = landing_queue_snapshot(&space);
        let requeued = after.iter().find(|e| e.branch == "b1").unwrap();
        assert!(
            requeued.age_secs >= 5 * 3600 - 5,
            "requeue must not reset age, got {}",
            requeued.age_secs
        );
    }

    #[test]
    fn operator_fast_lane_precedes_automatic_fifo_without_reordering_its_class() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let space = Space::open_in_memory().unwrap();
        let queue = LandingQueue::new(space, &layout);
        let entry = |branch: &str, fast: bool| LandingQueueEntry {
            repo_name: "alpha".into(),
            repo_path: "/repos/alpha".into(),
            branch: branch.into(),
            target: "main".into(),
            head_sha: format!("sha-{branch}"),
            diff_class: "trivial".into(),
            task: "t".into(),
            operator_fast_lane: fast,
            ..Default::default()
        };

        queue.enqueue(entry("auto-1", false)).unwrap();
        queue.enqueue(entry("fast-1", true)).unwrap();
        queue.enqueue(entry("auto-2", false)).unwrap();
        queue.enqueue(entry("fast-2", true)).unwrap();

        let mut claimed = Vec::new();
        while let Some(next) = queue.claim_next("alpha", "main").unwrap() {
            claimed.push(next.branch.clone());
            queue.remove(&next).unwrap();
        }
        assert_eq!(claimed, ["fast-1", "fast-2", "auto-1", "auto-2"]);
    }

    #[test]
    fn orphaned_candidate_sweep_preserves_durable_refs_then_reclaims_them() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("feature.txt"), "candidate\n").unwrap();
        git(repo_dir.path(), &["add", "feature.txt"]);
        git(repo_dir.path(), &["commit", "-m", "add candidate"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);
        let repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let rk_git::PrepareOutcome::Prepared(candidate) =
            repo.prepare_merge("feature", "main").unwrap()
        else {
            panic!("expected prepared candidate");
        };

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space);
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: repo.name(),
                repo_path: repo.root().display().to_string(),
                branch: "feature".into(),
                target: "main".into(),
                head_sha,
                diff_class: "trivial".into(),
                task: "candidate sweep".into(),
                candidate_sha: Some(candidate.commit.clone()),
                candidate_base: Some(candidate.base),
                candidate_ref: Some(candidate.candidate_ref.clone()),
                ..Default::default()
            })
            .unwrap();

        assert_eq!(
            pipeline.sweep_orphaned_candidate_refs([repo.root().to_path_buf()]),
            0,
            "durably referenced candidate must survive startup sweep"
        );
        assert!(repo.rev_parse(&candidate.candidate_ref).is_ok());

        let queued = pipeline
            .queue
            .claim_next(&repo.name(), "main")
            .unwrap()
            .unwrap();
        pipeline.queue.remove(&queued).unwrap();
        assert_eq!(
            pipeline.sweep_orphaned_candidate_refs([repo.root().to_path_buf()]),
            1,
            "unreferenced candidate must be reclaimed"
        );
        assert!(repo.rev_parse(&candidate.candidate_ref).is_err());
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
        assert_eq!(
            result["tested_sha"], result["merge_commit"],
            "the landed commit must be the exact object that passed gates"
        );
        assert_eq!(
            rev_parse(repo_dir.path(), "main"),
            result["tested_sha"].as_str().unwrap()
        );
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

        // Landing on "main" must never produce a non-main-target visibility
        // event (TKT-01M0B71D9B51SV5AG95VR1A4ST).
        assert!(space
            .scan(&Pattern::category(Category::Event).identity(LANDING_NON_MAIN_TARGET_IDENTITY))
            .unwrap()
            .is_empty());
    }

    /// A crash after the exact target advance but before ticket recording
    /// resumes from the durable `landing` phase and closes the ticket.
    #[tokio::test]
    async fn advanced_landing_reconciles_the_ticket_and_terminal_marker() {
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
        let ticket = pipeline
            .tickets
            .create(crate::tickets::NewTicket {
                title: "add note".into(),
                body: None,
                scope: Some("docs-repo".into()),
                parent: None,
                priority: "normal".into(),
                labels: vec![],
                depends_on: vec![],
                created_by: None,
                coalesce_key: None,
            })
            .await
            .unwrap();
        pipeline
            .tickets
            .set_status(&ticket.identity, "in_progress")
            .await
            .unwrap();

        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "docs-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "feature".into(),
                target: "main".into(),
                head_sha,
                diff_class: "doc-only".into(),
                task: ticket.identity.clone(),
                ..Default::default()
            })
            .unwrap();

        let repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let candidate = match repo.prepare_merge("feature", "main").unwrap() {
            rk_git::PrepareOutcome::Prepared(candidate) => candidate,
            other => panic!("expected prepared merge, got {other:?}"),
        };
        let mut claimed = pipeline
            .queue
            .claim_next("docs-repo", "main")
            .unwrap()
            .unwrap();
        claimed.candidate_sha = Some(candidate.commit.clone());
        claimed.candidate_base = Some(candidate.base.clone());
        claimed.candidate_ref = Some(candidate.candidate_ref.clone());
        pipeline
            .queue
            .persist(&mut claimed, LandingEntryStatus::Landing)
            .unwrap();
        assert!(repo
            .advance_target_to("main", &candidate.commit, &candidate.base)
            .unwrap()
            .advanced());
        repo.discard_candidate(&candidate.candidate_ref).unwrap();
        repo.delete_branch("feature").unwrap();

        let outcomes = pipeline.drain_key("docs-repo", "main").await.unwrap();
        let LandingOutcome::Landed(result) = &outcomes[0] else {
            panic!("expected Landed, got {:?}", outcomes[0]);
        };
        assert_eq!(result["merged"], true, "result: {result}");

        let stored = pipeline.tickets.get(&ticket.identity).unwrap().unwrap();
        assert_eq!(
            stored.payload.get("status").and_then(Value::as_str),
            Some("closed"),
            "landed ticket must reach a terminal state without an operator"
        );
        let record = crate::tickets::delivery_of(&stored).expect("delivery record");
        assert_eq!(
            record.merge_commit,
            result["merge_commit"].as_str().unwrap()
        );
        assert_eq!(record.target, "main");

        // The acceptance case the old branch-ref inference got wrong: landing
        // DELETED the branch as part of the land, so any predicate reading the
        // ref would now say "not delivered". Assert the ref really is gone,
        // then assert the ticket still reads delivered from the record alone.
        let refs = Command::new("git")
            .arg("-C")
            .arg(repo_dir.path())
            .args(["branch", "--list", "feature"])
            .output()
            .unwrap();
        assert!(
            String::from_utf8_lossy(&refs.stdout).trim().is_empty(),
            "landing is expected to delete the branch; the record is what survives"
        );
        assert!(crate::tickets::is_delivered(&stored));
    }

    /// An empty branch is not a delivery: a duplicate rat dispatched onto a
    /// ticket whose real work already landed also "merges" cleanly, and that
    /// must not close the ticket on its behalf
    /// (TKT-01M0C663BZ86SMA2PVMFP5QJ8D).
    #[tokio::test]
    async fn an_empty_branch_land_records_no_delivery() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        write_checks(repo_dir.path(), ALL_PASS_CHECKS);
        // Branched from main and never committed anything: nothing to deliver.
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let ticket = pipeline
            .tickets
            .create(crate::tickets::NewTicket {
                title: "empty".into(),
                body: None,
                scope: Some("docs-repo".into()),
                parent: None,
                priority: "normal".into(),
                labels: vec![],
                depends_on: vec![],
                created_by: None,
                coalesce_key: None,
            })
            .await
            .unwrap();
        pipeline
            .tickets
            .set_status(&ticket.identity, "in_progress")
            .await
            .unwrap();

        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "docs-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "feature".into(),
                target: "main".into(),
                head_sha,
                diff_class: "doc-only".into(),
                task: ticket.identity.clone(),
                ..Default::default()
            })
            .unwrap();

        pipeline.drain_key("docs-repo", "main").await.unwrap();

        let stored = pipeline.tickets.get(&ticket.identity).unwrap().unwrap();
        assert!(
            !crate::tickets::is_delivered(&stored),
            "an empty branch must not read as a delivery"
        );
        assert_eq!(
            stored.payload.get("status").and_then(Value::as_str),
            Some("in_progress"),
            "an empty land must not close the ticket"
        );
    }

    /// A candidate landing on a target other than `"main"` — e.g. a
    /// rework/chained rat's own `--base`, the same inheritance the reactor's
    /// `note_non_main_land_target` test covers for the `action: "workflow"`
    /// path (`crates/rk-daemon/tests/reactor.rs`,
    /// `non_main_land_target_is_reported_main_is_not`) — must produce a
    /// visible `landing_non_main_land_target` event even though this
    /// zero-agent-spawn fast path never creates a workflow instance for `rk
    /// workflow list` to annotate (TKT-01M0B71D9B51SV5AG95VR1A4ST).
    #[tokio::test]
    async fn doc_only_completion_on_non_main_target_emits_visibility_event() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        write_checks(repo_dir.path(), ALL_PASS_CHECKS);
        git(repo_dir.path(), &["checkout", "-b", "base"]);
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::create_dir_all(repo_dir.path().join("docs")).unwrap();
        std::fs::write(repo_dir.path().join("docs").join("note.md"), "note\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "docs: add note"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "base"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "docs-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "feature".into(),
                target: "base".into(),
                head_sha,
                diff_class: "doc-only".into(),
                task: "add note".into(),
                ..Default::default()
            })
            .unwrap();

        let outcomes = pipeline.drain_key("docs-repo", "base").await.unwrap();
        assert_eq!(outcomes.len(), 1);
        let LandingOutcome::Landed(result) = &outcomes[0] else {
            panic!("expected Landed, got {:?}", outcomes[0]);
        };
        assert_eq!(result["merged"], true, "result: {result}");

        let events = space
            .scan(&Pattern::category(Category::Event).identity(LANDING_NON_MAIN_TARGET_IDENTITY))
            .unwrap();
        assert_eq!(
            events.len(),
            1,
            "a non-main target must produce exactly one visibility event"
        );
        assert_eq!(events[0].scope, "docs-repo");
        assert_eq!(events[0].payload["target"], "base");
        assert_eq!(events[0].payload["branch"], "feature");
        assert_eq!(events[0].payload["task"], "add note");
        assert_eq!(
            events[0].payload["text"],
            "landing pipeline will land feature on non-main target base"
        );
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
        assert_eq!(
            needs_after.len(),
            1,
            "reconciliation must not duplicate the need row"
        );

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
        let entry = review_candidate_entry(Path::new("."), head_sha);
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
                "target": "main",
                "review_attempt": review_instance_id(&entry),
            }),
        )
    }

    fn no_spawns(space: &Space) {
        assert!(
            tuples(space, Category::Event, "agent_spawned").is_empty(),
            "a verdict-cache hit must not spawn a reviewer"
        );
    }

    fn tuples(space: &Space, category: Category, identity: &str) -> Vec<Tuple> {
        space
            .scan(&Pattern::category(category).identity(identity))
            .unwrap()
    }

    /// Like `tuples`, but scoped to `code-repo` — for identities (dispatch
    /// markers, steward needs) that must not be conflated with another
    /// repo's tuples of the same category+identity.
    fn scoped_tuples(space: &Space, category: Category, identity: &str) -> Vec<Tuple> {
        space
            .scan(
                &Pattern::category(category)
                    .scope("code-repo")
                    .identity(identity),
            )
            .unwrap()
    }

    fn rework_context(head: &str, task: &str, rework_ticket: &str) -> ReworkContext {
        ReworkContext {
            repo: "code-repo".into(),
            branch: "feature".into(),
            head_sha: head.into(),
            target: "main".into(),
            task: task.into(),
            rework_ticket: rework_ticket.into(),
            notes: "notes".into(),
            diff_files: 1,
            diff_lines: 1,
        }
    }

    fn put_rework_marker(space: &Space, ctx: &ReworkContext, agent: Option<&str>, state: &str) {
        space
            .out(
                Tuple::new(
                    Category::Event,
                    "code-repo",
                    REWORK_DISPATCH_IDENTITY,
                    "daemon",
                    ctx.marker_payload(1, agent, state),
                )
                .with_lifecycle(Lifecycle::Furniture),
            )
            .unwrap();
    }

    async fn create_test_ticket(pipeline: &LandingPipeline, title: &str) -> Tuple {
        pipeline
            .tickets
            .create(NewTicket {
                title: title.into(),
                body: None,
                scope: Some("code-repo".into()),
                parent: None,
                priority: "normal".into(),
                labels: vec![],
                depends_on: vec![],
                created_by: Some("daemon".into()),
                coalesce_key: None,
            })
            .await
            .unwrap()
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

    #[test]
    fn verdict_cache_rejects_a_different_review_context() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);
        let space = Space::open_in_memory().unwrap();
        space
            .out(Tuple::new(
                Category::Artifact,
                "code-repo",
                REVIEW_ARTIFACT_IDENTITY,
                "some-reviewer",
                json!({
                    "task": "a different task",
                    "target": "release",
                    "recommendation": "APPROVE",
                    "head_sha": head_sha,
                    "branch": "feature",
                    "review_attempt": "landing-review-wrong-context",
                }),
            ))
            .unwrap();
        let pipeline = test_pipeline(home.path(), space);

        assert_eq!(
            pipeline.cached_verdict(&entry).unwrap(),
            None,
            "a verdict for the same branch tip but a different target/task is not reusable"
        );
    }

    #[test]
    fn verdict_cache_rejects_wrong_or_missing_review_attempt() {
        for review_attempt in [Some("landing-review-wrong-context"), None] {
            let home = tempfile::tempdir().unwrap();
            let (repo_dir, head_sha, _main_before) = review_candidate_repo();
            let entry = review_candidate_entry(repo_dir.path(), &head_sha);
            let space = Space::open_in_memory().unwrap();
            let mut verdict = verdict_tuple(&head_sha, "APPROVE");
            match review_attempt {
                Some(attempt) => verdict.payload["review_attempt"] = json!(attempt),
                None => {
                    verdict
                        .payload
                        .as_object_mut()
                        .unwrap()
                        .remove("review_attempt");
                }
            }
            space.out(verdict).unwrap();
            let pipeline = test_pipeline(home.path(), space);

            assert_eq!(
                pipeline.cached_verdict(&entry).unwrap(),
                None,
                "a verdict without the exact review attempt is not reusable: {review_attempt:?}"
            );
        }
    }

    /// The APPROVE routing arm (`LandingPipeline::route_verdict`) is a
    /// second, independent prepared-landing route from the doc-only
    /// fast path — this proves the non-main visibility event fires there
    /// too, not just on the fast path (TKT-01M0B71D9B51SV5AG95VR1A4ST).
    #[tokio::test]
    async fn approved_review_on_non_main_target_emits_visibility_event() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        write_checks(repo_dir.path(), ALL_PASS_CHECKS);
        git(repo_dir.path(), &["checkout", "-b", "base"]);
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "base"]);

        let entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "base".into(),
            head_sha,
            diff_class: "large".into(),
            task: "add src".into(),
            ..Default::default()
        };
        let space = Space::open_in_memory().unwrap();
        let mut verdict = verdict_tuple(&entry.head_sha, "APPROVE");
        verdict.payload["target"] = json!("base");
        verdict.payload["review_attempt"] = json!(review_instance_id(&entry));
        space.out(verdict).unwrap();

        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline.enqueue(entry).unwrap();

        let outcomes = pipeline.drain_key("code-repo", "base").await.unwrap();
        assert_eq!(outcomes.len(), 1);
        let LandingOutcome::Landed(result) = &outcomes[0] else {
            panic!("expected Landed, got {:?}", outcomes[0]);
        };
        assert_eq!(result["merged"], true, "result: {result}");

        let events = space
            .scan(&Pattern::category(Category::Event).identity(LANDING_NON_MAIN_TARGET_IDENTITY))
            .unwrap();
        assert_eq!(
            events.len(),
            1,
            "a non-main target must produce exactly one visibility event"
        );
        assert_eq!(events[0].scope, "code-repo");
        assert_eq!(events[0].payload["target"], "base");
        assert_eq!(events[0].payload["branch"], "feature");
    }

    #[tokio::test]
    async fn cached_rework_dispatches_exactly_once_and_replay_converges() {
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

        let spawns = tuples(&space, Category::Event, "agent_spawned");
        assert_eq!(spawns.len(), 1, "bounded REWORK must dispatch one agent");
        let markers = scoped_tuples(&space, Category::Event, REWORK_DISPATCH_IDENTITY);
        assert_eq!(markers.len(), 1, "one logical dispatch gets one marker");
        assert_eq!(markers[0].payload["state"], "dispatching");
        assert_eq!(markers[0].payload["branch"], "feature");
        assert_eq!(markers[0].payload["target"], "main");
        assert_eq!(markers[0].payload["task"], "add src");
        assert_eq!(markers[0].payload["rework_ticket"], ticket.identity);

        // Bypass the processed-work-key shortcut to exercise the dispatch
        // marker itself, as a restart replay of the routed verdict would.
        let repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let replay = pipeline
            .route_rework(&review_candidate_entry(repo_dir.path(), &head_sha), &repo)
            .await
            .unwrap();
        assert!(matches!(replay, LandingOutcome::ReworkFiled(_)));
        assert_eq!(
            tuples(&space, Category::Event, "agent_spawned").len(),
            1,
            "replayed routing must not spawn a second correction"
        );
        assert_eq!(
            scoped_tuples(&space, Category::Event, REWORK_DISPATCH_IDENTITY).len(),
            1,
            "replayed routing must not append another marker"
        );
        assert!(
            scoped_tuples(&space, Category::Need, STEWARD_NEED_IDENTITY).is_empty(),
            "a journaled correction agent must not be mistaken for an interrupted dispatch"
        );
    }

    #[tokio::test]
    async fn dispatching_marker_without_spawn_survives_restart_as_one_human_gate() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        layout.ensure().unwrap();
        let (repo_dir, head_sha, main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        {
            let space = Space::open(&layout.db_path()).unwrap();
            space.out(verdict_tuple(&head_sha, "REWORK")).unwrap();
            let pipeline = test_pipeline(home.path(), space.clone());
            let ticket = pipeline.file_rework_ticket(&entry).await.unwrap();
            let ctx = rework_context(&entry.head_sha, &entry.task, &ticket.identity);
            put_rework_marker(&space, &ctx, None, "dispatching");
        }

        // A fresh Space and pipeline stand in for a daemon restart after the
        // marker commit point but before Supervisor::spawn journaled an agent.
        let space = Space::open(&layout.db_path()).unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        for _ in 0..2 {
            let replay = pipeline.route_rework(&entry, &repo).await.unwrap();
            assert!(matches!(replay, LandingOutcome::ReworkFiled(_)));
        }

        assert_eq!(rev_parse(repo_dir.path(), "main"), main_before);
        assert!(
            tuples(&space, Category::Event, "agent_spawned").is_empty(),
            "an ambiguous restart must never duplicate the correction agent"
        );
        let needs = space
            .scan(&Pattern::category(Category::Need).scope("code-repo"))
            .unwrap();
        assert_eq!(needs.len(), 1, "replay must converge on one visible gate");
        let text = needs[0].payload["text"].as_str().unwrap();
        for required in [
            "dispatch-interrupted",
            "EVIDENCE: reviewer verdict REWORK",
            "DECISION NEEDED:",
            "BLAST RADIUS:",
            "RESOLVE WITH: rk spawn",
        ] {
            assert!(text.contains(required), "missing {required:?}: {text}");
        }
    }

    #[test]
    fn dispatch_status_replays_count_as_one_attempt() {
        let home = tempfile::tempdir().unwrap();
        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let ctx = rework_context("abc123", "TKT-original", "TKT-rework");
        for state in ["dispatching", "dispatched"] {
            put_rework_marker(&space, &ctx, None, state);
        }
        assert_eq!(pipeline.rework_attempts_used(&ctx).unwrap(), 1);
    }

    #[tokio::test]
    async fn landed_rework_resubmits_parent_once_then_ordinary_approve_lands_it() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        write_checks(repo_dir.path(), ALL_PASS_CHECKS);

        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn fixed() {}\n").unwrap();
        git(repo_dir.path(), &["add", "src.rs"]);
        git(repo_dir.path(), &["commit", "-m", "feat: original work"]);
        let reviewed_head = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "-b", "rat/rework"]);
        std::fs::create_dir_all(repo_dir.path().join("docs")).unwrap();
        std::fs::write(repo_dir.path().join("docs/fix.md"), "bounded correction\n").unwrap();
        git(repo_dir.path(), &["add", "docs/fix.md"]);
        git(
            repo_dir.path(),
            &["commit", "-m", "fix: reviewer correction"],
        );
        let rework_head = rev_parse(repo_dir.path(), "rat/rework");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let original = create_test_ticket(&pipeline, "original").await;
        let rework = create_test_ticket(&pipeline, "rework").await;
        let ctx = rework_context(&reviewed_head, &original.identity, &rework.identity);
        put_rework_marker(&space, &ctx, Some("Rat-Rework"), "dispatching");

        let intermediate = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "rat/rework".into(),
            target: "feature".into(),
            head_sha: rework_head,
            diff_class: "doc-only".into(),
            task: rework.identity.clone(),
            ..Default::default()
        };
        pipeline.enqueue(intermediate.clone()).unwrap();
        let landed = pipeline.drain_key("code-repo", "feature").await.unwrap();
        assert!(matches!(landed.as_slice(), [LandingOutcome::Landed(_)]));

        let parent_head = rev_parse(repo_dir.path(), "feature");
        let pending = pipeline
            .queue
            .scan_current("code-repo", Some("main"))
            .unwrap();
        assert_eq!(pending.len(), 1, "corrected parent must be queued once");
        assert_eq!(pending[0].payload["branch"], "feature");
        assert_eq!(pending[0].payload["target"], "main");
        assert_eq!(pending[0].payload["task"], original.identity);
        assert_eq!(pending[0].payload["head_sha"], parent_head);

        pipeline.resubmit_reworked_parent(&intermediate).unwrap();
        assert_eq!(
            pipeline
                .queue
                .scan_current("code-repo", Some("main"))
                .unwrap()
                .len(),
            1,
            "replayed intermediate delivery must not duplicate the parent"
        );
        assert_eq!(
            tuples(&space, Category::Event, REWORK_RESUBMISSION_IDENTITY).len(),
            1,
            "one logical parent hand-off gets one evidence event"
        );

        space
            .out(Tuple::new(
                Category::Artifact,
                "code-repo",
                REVIEW_ARTIFACT_IDENTITY,
                "fresh-reviewer",
                json!({
                    "task": original.identity,
                    "recommendation": "APPROVE",
                    "notes": "corrected branch is clean",
                    "head_sha": parent_head,
                    "branch": "feature",
                }),
            ))
            .unwrap();
        let final_outcome = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert!(matches!(
            final_outcome.as_slice(),
            [LandingOutcome::Landed(_)]
        ));
        let output = Command::new("git")
            .arg("-C")
            .arg(repo_dir.path())
            .args(["ls-tree", "--name-only", "-r", "main"])
            .output()
            .unwrap();
        assert!(output.status.success());
        let listing = String::from_utf8_lossy(&output.stdout);
        assert!(listing.contains("src.rs"), "{listing}");
        assert!(listing.contains("docs/fix.md"), "{listing}");
        let original_after = pipeline.tickets.get(&original.identity).unwrap().unwrap();
        let rework_after = pipeline.tickets.get(&rework.identity).unwrap().unwrap();
        assert_eq!(original_after.payload["status"], "closed");
        assert_eq!(rework_after.payload["status"], "closed");
    }

    #[tokio::test]
    async fn exhausted_rework_chain_holds_once_with_actionable_evidence() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, main_before) = review_candidate_repo();
        let space = Space::open_in_memory().unwrap();
        space.out(verdict_tuple(&head_sha, "REWORK")).unwrap();
        let prior = rework_context("prior-reviewed-head", "add src", "TKT-prior-rework");
        put_rework_marker(&space, &prior, Some("Prior-Rat"), "dispatching");
        let pipeline = test_pipeline(home.path(), space.clone());
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);
        pipeline.enqueue(entry.clone()).unwrap();
        let outcome = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert!(matches!(
            outcome.as_slice(),
            [LandingOutcome::ReworkFiled(_)]
        ));
        assert_eq!(rev_parse(repo_dir.path(), "main"), main_before);
        no_spawns(&space);

        let needs = scoped_tuples(&space, Category::Need, STEWARD_NEED_IDENTITY);
        assert_eq!(needs.len(), 1);
        let text = needs[0].payload["text"].as_str().unwrap();
        for required in [
            "attempts-exhausted",
            "EVIDENCE: reviewer verdict REWORK",
            "DECISION NEEDED:",
            "BLAST RADIUS:",
            "RESOLVE WITH: rk spawn",
        ] {
            assert!(text.contains(required), "missing {required:?}: {text}");
        }

        let repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        pipeline.route_rework(&entry, &repo).await.unwrap();
        assert_eq!(
            scoped_tuples(&space, Category::Need, STEWARD_NEED_IDENTITY).len(),
            1,
            "replay must converge on the existing human gate"
        );
    }

    #[tokio::test]
    async fn reviewer_declared_human_holds_without_dispatch() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, main_before) = review_candidate_repo();
        let space = Space::open_in_memory().unwrap();
        let mut verdict = verdict_tuple(&head_sha, "REWORK");
        verdict.payload["authority"] = json!("human");
        verdict.payload["notes"] = json!("operator must choose the compatibility policy");
        space.out(verdict).unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(review_candidate_entry(repo_dir.path(), &head_sha))
            .unwrap();
        let outcome = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert!(matches!(
            outcome.as_slice(),
            [LandingOutcome::ReworkFiled(_)]
        ));
        assert_eq!(rev_parse(repo_dir.path(), "main"), main_before);
        no_spawns(&space);
        let needs = space
            .scan(&Pattern::category(Category::Need).scope("code-repo"))
            .unwrap();
        assert_eq!(needs.len(), 1);
        let text = needs[0].payload["text"].as_str().unwrap();
        assert!(text.contains("reviewer-declared-human"), "{text}");
        assert!(text.contains("operator must choose"), "{text}");
        assert!(text.contains("RESOLVE WITH:"), "{text}");
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
        let detail = &produced_rows[0].detail;
        for required in [
            "reviewer-stop",
            "EVIDENCE: exact reviewed head",
            "Reviewer notes: notes",
            "DECISION NEEDED:",
            "BLAST RADIUS: 1 file(s) / 1 line(s)",
            "RESOLVE WITH: rk land feature",
            "--target main --task add src --force",
        ] {
            assert!(detail.contains(required), "missing {required:?}: {detail}");
        }
    }

    /// The REWORK counterpart to `escalation_row_matches_the_workflow_driven_
    /// steward_shape` above. A REWORK verdict never reaches `rk inbox` — no
    /// source `build` reads from scans `Category::Task` (tickets), so the
    /// follow-up ticket `file_rework_ticket` files has no row to compare
    /// against another row. What must still match, byte-for-byte, is the
    /// TICKET SHAPE itself: the pre-cutover `steward-file-rework-ticket`
    /// named check (removed in `.rk/checks.cue`/`examples/checks.cue`, Phase
    /// 4) ran exactly `rk ticket new "rework: $RK_CHECK_TASK_ID" --repo
    /// "$RK_CHECK_REPO" --body "Steward routed REWORK on branch
    /// $RK_CHECK_BRANCH. Read the reviewer notes: rk scan artifact
    /// $RK_CHECK_REPO"` — title, scope, and body are asserted against that
    /// exact historical template here, not just that *some* ticket got
    /// filed.
    #[tokio::test]
    async fn rework_ticket_matches_the_workflow_driven_steward_shape() {
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
        let LandingOutcome::ReworkFiled(produced) = &outcomes[0] else {
            panic!("expected ReworkFiled, got {:?}", outcomes[0]);
        };

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_eq!(main_before, main_after, "branch must not have landed");

        let historical_title = "rework: add src";
        let historical_body =
            "Steward routed REWORK on branch feature. Read the reviewer notes: rk scan \
             artifact code-repo";
        assert_eq!(produced.payload["title"], historical_title);
        assert_eq!(produced.payload["body"], historical_body);
        assert_eq!(produced.scope, "code-repo");
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
            // Poll until the candidate is durably claimed and mid-gate before
            // aborting — a fixed sleep is not a reliable proxy for "the
            // gate's `sleep 0.4` is genuinely mid-flight" under scheduler
            // contention from the rest of the workspace suite (TKT-01M0C8PJ7AQ7TQ4WV7SCYJ9Y7F).
            let poll_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                let pending = space
                    .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
                    .unwrap();
                if pending.len() == 1 {
                    let status: LandingEntryStatus =
                        serde_json::from_value(pending[0].payload["status"].clone()).unwrap();
                    if status == LandingEntryStatus::RunningGates {
                        break;
                    }
                }
                assert!(
                    tokio::time::Instant::now() < poll_deadline,
                    "candidate never reached RunningGates before the gate finished"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
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
        let gate_log = barrier_dir.path().join("gates.log");
        let checks = format!(
            r#"
checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "echo gate >> \"{gates}\"; test -f \"{marker}\" && echo overlap >> \"{log}\"; touch \"{marker}\"; sleep 0.1; rm -f \"{marker}\"", timeout: "30s"}},
]
"#,
            marker = marker.display(),
            log = overlap_log.display(),
            gates = gate_log.display(),
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
            git(
                repo_dir.path(),
                &["commit", "-m", &format!("docs: note {i}")],
            );
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
        assert_eq!(
            std::fs::read_to_string(&gate_log).unwrap().lines().count(),
            1,
            "the four compatible branches should share one gate run"
        );
    }

    #[tokio::test]
    async fn failed_batch_is_bisected_and_clean_siblings_still_land() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let gate_log = home.path().join("bisect-gates.log");
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "echo gate >> '{log}'; test ! -f docs/bad.md", timeout: "30s"}},
]
"#,
                log = gate_log.display()
            ),
        );

        let mut queued = Vec::new();
        for (branch, file) in [
            ("good-a", "good-a.md"),
            ("bad", "bad.md"),
            ("good-b", "good-b.md"),
        ] {
            git(repo_dir.path(), &["checkout", "-b", branch]);
            std::fs::create_dir_all(repo_dir.path().join("docs")).unwrap();
            std::fs::write(repo_dir.path().join("docs").join(file), "x\n").unwrap();
            git(repo_dir.path(), &["add", "."]);
            git(repo_dir.path(), &["commit", "-m", branch]);
            queued.push((branch.to_string(), rev_parse(repo_dir.path(), branch)));
            git(repo_dir.path(), &["checkout", "main"]);
        }

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space);
        for (branch, head_sha) in queued {
            pipeline
                .enqueue(LandingQueueEntry {
                    repo_name: "bisect-repo".into(),
                    repo_path: repo_dir.path().display().to_string(),
                    branch,
                    target: "main".into(),
                    head_sha,
                    diff_class: "doc-only".into(),
                    task: String::new(),
                    ..Default::default()
                })
                .unwrap();
        }
        let outcomes = pipeline.drain_key("bisect-repo", "main").await.unwrap();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, LandingOutcome::Landed(_)))
                .count(),
            2
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, LandingOutcome::GateHeld))
                .count(),
            1
        );
        let listing = Command::new("git")
            .arg("-C")
            .arg(repo_dir.path())
            .args(["ls-tree", "-r", "--name-only", "main"])
            .output()
            .unwrap();
        let listing = String::from_utf8_lossy(&listing.stdout);
        assert!(listing.contains("docs/good-a.md"));
        assert!(listing.contains("docs/good-b.md"));
        assert!(!listing.contains("docs/bad.md"));
        assert!(
            std::fs::read_to_string(gate_log).unwrap().lines().count() > 1,
            "a red combined batch must be bisected and retested"
        );
    }

    #[tokio::test]
    async fn stale_cas_requeues_at_tail_then_rebuilds_and_retests() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let checks = format!(
            r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "git -C '{repo}' update-ref refs/heads/main refs/heads/moving-target", timeout: "30s"}},
]
"#,
            repo = repo_dir.path().display()
        );
        write_checks(repo_dir.path(), &checks);
        git(repo_dir.path(), &["checkout", "-b", "moving-target"]);
        std::fs::write(repo_dir.path().join("sibling.txt"), "sibling\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(
            repo_dir.path(),
            &["commit", "-m", "sibling advances target"],
        );
        let moved_sha = rev_parse(repo_dir.path(), "moving-target");
        git(repo_dir.path(), &["checkout", "main"]);
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::create_dir_all(repo_dir.path().join("docs")).unwrap();
        std::fs::write(repo_dir.path().join("docs/note.md"), "note\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feature"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "stale-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "feature".into(),
                target: "main".into(),
                head_sha,
                diff_class: "doc-only".into(),
                task: String::new(),
                ..Default::default()
            })
            .unwrap();
        let outcomes = pipeline.drain_key("stale-repo", "main").await.unwrap();
        assert!(matches!(
            outcomes.first(),
            Some(LandingOutcome::Requeued { .. })
        ));
        assert!(matches!(outcomes.last(), Some(LandingOutcome::Landed(_))));
        assert!(rk_git::Repo::discover(repo_dir.path())
            .unwrap()
            .is_ancestor(&moved_sha, &rev_parse(repo_dir.path(), "main")));
        let announced = space
            .scan(
                &Pattern::category(Category::Event)
                    .scope("stale-repo")
                    .identity("landing_candidate_requeued"),
            )
            .unwrap();
        assert_eq!(announced.len(), 1, "stale retry must be visibly announced");
    }

    #[tokio::test]
    async fn missing_named_check_is_a_visible_no_gate_hold() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("note.md"), "note\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "note"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);
        let main_before = rev_parse(repo_dir.path(), "main");

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "no-gate-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "feature".into(),
                target: "main".into(),
                head_sha,
                diff_class: "doc-only".into(),
                task: "missing-policy".into(),
                ..Default::default()
            })
            .unwrap();
        let outcomes = pipeline.drain_key("no-gate-repo", "main").await.unwrap();
        assert!(matches!(outcomes.as_slice(), [LandingOutcome::NoGate(_)]));
        assert_eq!(rev_parse(repo_dir.path(), "main"), main_before);
        let rows = space
            .scan(
                &Pattern::category(Category::Event)
                    .scope("no-gate-repo")
                    .identity("landing_no_gate"),
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].payload["state"], "no-gate");
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

    #[tokio::test]
    async fn chained_non_main_reviewer_preserves_exact_binding_and_approve_resolves() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        git(repo_dir.path(), &["branch", "release"]);
        let mut entry = review_candidate_entry(repo_dir.path(), &head_sha);
        entry.target = "release".into();
        entry.task = "TKT-non-main".into();

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
        let reviewer = pipeline
            .supervisor
            .list()
            .into_iter()
            .find(|record| record.role == "reviewer")
            .expect("spawned reviewer record");
        assert_eq!(reviewer.target_branch, entry.branch, "reviewer is chained");
        let review = reviewer.review.expect("typed review binding");
        assert_eq!(review.branch, entry.branch);
        assert_eq!(review.head_sha, entry.head_sha);
        assert_eq!(review.target, "release");
        assert_eq!(review.task, "TKT-non-main");
        assert_eq!(review.attempt, review_instance_id(&entry));

        space
            .out(Tuple::new(
                Category::Artifact,
                &entry.repo_name,
                REVIEW_ARTIFACT_IDENTITY,
                reviewer.name,
                json!({
                    "recommendation": "APPROVE",
                    "branch": review.branch,
                    "head_sha": review.head_sha,
                    "target": review.target,
                    "task": review.task,
                    "review_attempt": review.attempt,
                }),
            ))
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .expect("bound APPROVE must not become no-verdict")
            .unwrap()
            .unwrap();
        assert!(
            matches!(outcome, ReviewWaitOutcome::Verdict(ref verdict) if verdict == "APPROVE"),
            "expected bound APPROVE, got {outcome:?}"
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
        assert!(
            !context.trim().is_empty(),
            "death context must not be empty"
        );

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

    /// Shared setup for the `gate_worktree_sweep_once` tests below: a real
    /// repo plus a pipeline, with `run_gates` used directly (not the full
    /// `drain_key`/land path) to create one or more real, git-registered
    /// gate worktrees without needing an actual `target` branch to exist.
    fn gate_sweep_fixture(home: &Path, repo_dir: &Path) -> (LandingPipeline, rk_git::Repo, String) {
        init_repo(repo_dir);
        write_checks(repo_dir, ALL_PASS_CHECKS);
        let head_sha = rev_parse(repo_dir, "main");
        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home, space);
        let git_repo = rk_git::Repo::discover(repo_dir).unwrap();
        (pipeline, git_repo, head_sha)
    }

    fn gate_sweep_entry(repo_dir: &Path, target: &str, head_sha: &str) -> LandingQueueEntry {
        LandingQueueEntry {
            repo_name: "myrepo".into(),
            repo_path: repo_dir.display().to_string(),
            branch: "feature".into(),
            target: target.into(),
            head_sha: head_sha.into(),
            diff_class: "trivial".into(),
            task: "t".into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn gate_worktree_sweep_reclaims_stale_worktree_past_max_age() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        let (pipeline, git_repo, head_sha) = gate_sweep_fixture(home.path(), repo_dir.path());
        let gates = GateConfig::default();
        let mut entry = gate_sweep_entry(repo_dir.path(), "main", &head_sha);
        assert!(pipeline.run_gates(&mut entry, &git_repo, &gates).await.unwrap());

        let gate_dir = pipeline.gate_worktree_path("myrepo", "main");
        assert!(gate_dir.is_dir());

        // Backdate the marker so it reads as long unused.
        let marker = pipeline.gate_worktree_marker_path("myrepo", "main");
        let stale = Utc::now() - chrono::Duration::days(30);
        std::fs::write(&marker, stale.to_rfc3339()).unwrap();

        let cfg = rk_core::config::GateWorktreeSweepConfig {
            max_age_days: 7,
            max_per_repo: 0,
            ..rk_core::config::GateWorktreeSweepConfig::default()
        };
        let reclaims = pipeline.gate_worktree_sweep_once(&cfg, false);
        assert_eq!(reclaims.len(), 1, "{reclaims:?}");
        assert_eq!(reclaims[0].reason, "age");
        assert!(reclaims[0].reclaimed);
        assert!(!gate_dir.exists(), "gate worktree should have been removed");
        assert!(
            !marker.exists(),
            "marker should have been removed alongside it"
        );
    }

    #[tokio::test]
    async fn gate_worktree_sweep_dry_run_reports_without_touching_disk() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        let (pipeline, git_repo, head_sha) = gate_sweep_fixture(home.path(), repo_dir.path());
        let gates = GateConfig::default();
        let mut entry = gate_sweep_entry(repo_dir.path(), "main", &head_sha);
        assert!(pipeline.run_gates(&mut entry, &git_repo, &gates).await.unwrap());

        let gate_dir = pipeline.gate_worktree_path("myrepo", "main");
        let marker = pipeline.gate_worktree_marker_path("myrepo", "main");
        std::fs::write(
            &marker,
            (Utc::now() - chrono::Duration::days(30)).to_rfc3339(),
        )
        .unwrap();

        let cfg = rk_core::config::GateWorktreeSweepConfig {
            max_age_days: 7,
            max_per_repo: 0,
            ..rk_core::config::GateWorktreeSweepConfig::default()
        };
        let reclaims = pipeline.gate_worktree_sweep_once(&cfg, true);
        assert_eq!(reclaims.len(), 1, "{reclaims:?}");
        assert!(!reclaims[0].reclaimed);
        assert!(gate_dir.exists(), "dry run must not touch disk");
        assert!(marker.exists(), "dry run must not touch disk");
    }

    #[tokio::test]
    async fn gate_worktree_sweep_enforces_max_per_repo_cap() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        let (pipeline, git_repo, head_sha) = gate_sweep_fixture(home.path(), repo_dir.path());
        let gates = GateConfig::default();

        for target in ["a", "b", "c"] {
            let mut entry = gate_sweep_entry(repo_dir.path(), target, &head_sha);
            assert!(pipeline.run_gates(&mut entry, &git_repo, &gates).await.unwrap());
            // Distinct, strictly increasing last-used timestamps even on
            // coarse filesystem/clock resolution.
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let cfg = rk_core::config::GateWorktreeSweepConfig {
            max_age_days: 0,
            max_per_repo: 2,
            ..rk_core::config::GateWorktreeSweepConfig::default()
        };
        let reclaims = pipeline.gate_worktree_sweep_once(&cfg, false);
        assert_eq!(reclaims.len(), 1, "{reclaims:?}");
        assert_eq!(reclaims[0].target, "a", "oldest of the three must go");
        assert_eq!(reclaims[0].reason, "cap");
        assert!(!pipeline.gate_worktree_path("myrepo", "a").exists());
        assert!(pipeline.gate_worktree_path("myrepo", "b").exists());
        assert!(pipeline.gate_worktree_path("myrepo", "c").exists());
    }

    #[tokio::test]
    async fn gate_worktree_sweep_never_touches_a_key_with_a_live_queue_entry() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        let (pipeline, git_repo, head_sha) = gate_sweep_fixture(home.path(), repo_dir.path());
        let gates = GateConfig::default();
        let mut entry = gate_sweep_entry(repo_dir.path(), "main", &head_sha);
        assert!(pipeline.run_gates(&mut entry, &git_repo, &gates).await.unwrap());

        // Backdate the marker so it would be evicted on age alone...
        let marker = pipeline.gate_worktree_marker_path("myrepo", "main");
        std::fs::write(
            &marker,
            (Utc::now() - chrono::Duration::days(30)).to_rfc3339(),
        )
        .unwrap();
        // ...but a live queue entry for this exact key must still protect it.
        pipeline.enqueue(entry).unwrap();

        let cfg = rk_core::config::GateWorktreeSweepConfig {
            max_age_days: 1,
            max_per_repo: 0,
            ..rk_core::config::GateWorktreeSweepConfig::default()
        };
        let reclaims = pipeline.gate_worktree_sweep_once(&cfg, false);
        assert!(reclaims.is_empty(), "{reclaims:?}");
        assert!(pipeline.gate_worktree_path("myrepo", "main").exists());
    }
}
