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
//! `work_key = (repo, branch, head_sha, target)` dedup against a redelivered
//! completion (a reactor retry after a crash, an operator manually
//! re-triggering) is [`LandingPipeline::enqueue`]'s job: it probes a durable
//! `landing_processed` marker — written by [`LandingPipeline::process_entry`]
//! on every terminal outcome — before ever writing a new queue tuple, and
//! silently drops (`Ok(None)`) a work key already fully handled rather than
//! re-enqueueing it. `target` is part of the key, not just `(repo, branch,
//! head_sha)`: the identical commit legitimately lands at different targets
//! (a nested workflow chains a step's branch onto a predecessor's, then an
//! operator retargets the same head at `main`), and each target is a
//! distinct, independently-audited candidate rather than a redelivery of the
//! other. The landing CAS already makes a literal double-advance
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

use crate::landing_conflict::{
    self, ConflictContext, ConflictEvidence, ConflictPolicy, CONFLICT_DISPATCH_IDENTITY,
};
use crate::landing_review_retry::{
    self, ReviewDeathContext, ReviewDeathPolicy, ReviewDeathRoute, REVIEW_DEATH_DISPATCH_IDENTITY,
};
use crate::landing_rework::{
    self, ReworkContext, ReworkPolicy, ReworkRoute, Withheld, REWORK_DISPATCH_IDENTITY,
};
use crate::supervisor::Supervisor;
use crate::tickets::{NewTicket, Tickets};
use crate::workflow_exec::{
    verification_proof_key, InstanceStatus, OnTimeout, ResolvedRun, RunProgress, WorkflowEngine,
};
use chrono::{DateTime, Utc};
use rk_core::paths::Layout;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_space::Space;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
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

/// [`REWORK_RESUBMISSION_IDENTITY`]'s counterpart for a landed merge-conflict
/// correction: evidence that it queued the conflicted branch for a fresh
/// gate run against its original target.
const CONFLICT_RESUBMISSION_IDENTITY: &str = "landing_conflict_rework_resubmission";

/// Identity of the durable per-attempt evidence event for a gate
/// infrastructure-death retry (bounded fail-safe recovery). See
/// [`LandingPipeline::record_gate_infra_attempt`].
const GATE_INFRA_RETRY_IDENTITY: &str = "landing_gate_infra_retry";

/// One durable timing/provenance record for a complete green daemon-owned
/// landing gate run. Failed checks already emit `gate-failure`; without the
/// successful counterpart operators cannot decompose landing latency or hand
/// a reviewer inspectable proof that the exact prepared candidate was tested.
const GATE_PASS_IDENTITY: &str = "landing_gate_pass";

/// Durable telemetry record (TKT-01M0QRZ7QT8CQD74GHRN81XFT5) written every
/// time a landing gate check skips its own execution because a durable
/// managed-verification proof already covered it — either the exact
/// candidate under test, or the pre-merge branch tip a rat's own `rk verify`
/// actually ran against (see [`LandingPipeline::reusable_verification_proof`]).
/// Separate from [`GATE_PASS_IDENTITY`] (written once per whole gate run)
/// so a peer can see proof-reuse per check without parsing that event's
/// `checks` list against `verification_proof`/`landing_gate_pass` history.
const VERIFICATION_PROOF_REUSE_IDENTITY: &str = "landing_verification_proof_reused";

/// Durable record of the gate plan [`LandingPipeline::gate_plan`] chose for
/// one candidate — written once per gate run, BEFORE any check executes, so
/// it exists even when a later check fails or the target moves out from
/// under the run. States the [`LandingEdgeClass`], the exact checks selected,
/// whether the full named check ran, and why — the "why a full check was
/// required or skipped" evidence the durable-events acceptance criterion
/// asks for. See [`LandingPipeline::run_gates_at`].
const LANDING_EDGE_PLAN_IDENTITY: &str = "landing_edge_plan";

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

/// Identity of the durable visibility event for an empty/no-op landing
/// candidate — a source head already containing zero commits beyond its
/// target when classified, either at admission
/// ([`LandingPipeline::enqueue_disposition`]) or after sitting queued while
/// the target caught up to it ([`LandingPipeline::process_entry`]). Written
/// alongside (never instead of) the [`LANDING_PROCESSED_IDENTITY`] dedup
/// marker so both "why" and "was this handled" are independently readable.
const LANDING_EMPTY_IDENTITY: &str = "landing_empty_candidate";

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
    /// Exact agent generation that produced this candidate. Operator-authored
    /// submissions remain `None` and cannot stand in for an agent handoff.
    pub(crate) source_spawn: Option<rk_core::id::SpawnId>,
    pub(crate) status: LandingEntryStatus,
    /// Seconds since [`LandingQueueEntry::enqueued_at`].
    pub(crate) age_secs: i64,
    /// Seconds since this entry entered the phase it is in RIGHT NOW
    /// ([`LandingQueueEntry::phase_entered_at`]) — total queue age MINUS
    /// whatever it spent in earlier phases. This is the only age a per-phase
    /// latency consumer may use: `age_secs` is deliberately cumulative
    /// (see [`LandingQueueEntry::enqueued_at`]), so reusing it as the
    /// elapsed time of the current phase reports every prior phase's wait
    /// against the phase that just started.
    pub(crate) phase_age_secs: i64,
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
            let enqueued_at = entry.enqueued_at?;
            let phase_entered_at = entry.phase_entered_at?;
            let age_secs = (now - enqueued_at).num_seconds().max(0);
            let phase_age_secs = (now - phase_entered_at).num_seconds().max(0);
            Some(LandingQueueSnapshotEntry {
                repo: entry.repo_name,
                target: entry.target,
                branch: entry.branch,
                task: entry.task,
                source_spawn: entry.source_spawn,
                status: entry.status,
                age_secs,
                phase_age_secs,
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
/// the retired steward mega-workflow's `_gates` block, POLICY (#19) and
/// DIFF-SCOPE (#20). Named-check registry entries, not raw commands: a repo
/// must register them in `.rk/checks.cue` exactly as it does for the
/// workflow-driven steward today.
const PROTECTED_PATHS_CHECK: &str = "steward-protected-paths";
const DIFF_SCOPE_CHECK: &str = "steward-diff-scope";

/// Wall-clock bound for the two cheap policy/scope gates — matches
/// that retired workflow's own `timeout: "2m"`.
const POLICY_GATE_TIMEOUT: Duration = Duration::from_secs(2 * 60);

/// Fallback when a named check carries no `timeout` of its own (mirrors
/// `workflow_exec::DEFAULT_RUN_TIMEOUT`, private to that module).
const DEFAULT_CHECK_TIMEOUT: &str = "10m";

/// Identity of a landing candidate's verdict artifact — Phase 2's
/// commit-keyed cache (`Pattern::for_commit`, §1.3 of the design doc),
/// written by the reviewer itself: `rk out artifact <repo> review --payload
/// {...}` (`examples/workflows/steward-review.cue`).
const REVIEW_ARTIFACT_IDENTITY: &str = "review";

/// Durable settlement marker for a review attempt whose landing-pipeline
/// wait hit `GateConfig::review_max_wait` (or was explicitly cancelled)
/// while the reviewer was still live — the ceiling counterpart to
/// [`REVIEW_DEATH_DISPATCH_IDENTITY`]'s dead-reviewer settlement. Written
/// exactly once per attempt by [`LandingPipeline::settle_review_ceiling`],
/// which probes for it first, so a daemon restart or a repeat routing pass
/// never double-dismisses the reviewer or double-releases capacity.
const REVIEW_CEILING_SETTLED_IDENTITY: &str = "landing_review_ceiling_settled";

/// Durable record that a verdict for an already-ceiling-settled review
/// attempt arrived anyway — retained as evidence (branch/head/attempt/
/// generation) without ever mutating the (already terminal) landing
/// decision. See [`LandingPipeline::retain_late_review_evidence`].
const LATE_REVIEW_EVIDENCE_IDENTITY: &str = "landing_late_review_evidence";

/// Durable record that [`LandingPipeline::reenqueue_after_ceiling`]
/// dispatched its one bounded fresh review attempt for a ceiling-settled
/// candidate. A second call for the same settled attempt finds this marker
/// and returns the SAME new attempt id rather than dispatching again.
const REVIEW_CEILING_REENQUEUE_IDENTITY: &str = "landing_review_ceiling_reenqueue";

/// [`crate::fault`] barrier name for the window inside
/// [`LandingPipeline::settle_review_ceiling`] after the live reviewer has
/// been dismissed but before [`REVIEW_CEILING_SETTLED_IDENTITY`] is durable.
/// Armed only by `tests/review_ceiling_crash_barrier.rs`.
const BARRIER_CEILING_PRE_MARKER: &str = "review-ceiling-pre-marker";

/// [`crate::fault`] barrier name for the mirror window: settlement durable,
/// caller not yet told. Armed only by `tests/review_ceiling_crash_barrier.rs`.
const BARRIER_CEILING_POST_MARKER: &str = "review-ceiling-post-marker";

/// Identity of the steward's escalation `need` tuple. Matches
/// the retired steward workflow's `steward-report-stop`/
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

/// Identity of the durable primary-vs-shadow comparison record written by
/// [`LandingPipeline::await_shadow_comparison`]. One per review request that
/// ran with shadow review enabled, scoped to the candidate's repo and keyed
/// (like the verdict artifact itself) on branch/head so
/// `Pattern::for_commit` finds it. Purely observational: nothing in this
/// pipeline ever reads it back to make a landing decision.
const SHADOW_COMPARISON_IDENTITY: &str = "review-shadow-comparison";

/// Suffix appended to a review request's stable instance id to derive the
/// shadow reviewer's own id and, with it, the shadow's `review_attempt`. The
/// distinct attempt is what keeps the two verdicts apart: both
/// [`LandingPipeline::cached_verdict`] and `request_review`'s `rd` pattern
/// match on the PRIMARY attempt, so a shadow verdict can never be routed on,
/// re-read as a cache hit by a later pass, or race the primary.
const SHADOW_INSTANCE_SUFFIX: &str = "-shadow";

/// A launched shadow reviewer, carried from
/// [`LandingPipeline::launch_shadow_review`] to the comparison record so the
/// record can name the model whose opinion it holds.
#[derive(Debug, Clone)]
pub(crate) struct ShadowReview {
    /// The shadow's instance id, which is also its `review_attempt` — the
    /// key its verdict artifact is bound to and the one this comparison
    /// polls on.
    attempt: String,
    model: String,
    harness: String,
}

/// The per-request data for [`LandingPipeline::await_shadow_comparison`],
/// bundled so the detached task's call site is a single struct literal
/// rather than the flat argument list that used to trip
/// `clippy::too_many_arguments` (the comparison also needs `Space`,
/// `Arc<WorkflowEngine>`, and `Arc<Supervisor>`, which stay separate params
/// since they're handles, not request data).
struct ShadowComparisonRequest {
    entry: LandingQueueEntry,
    shadow: ShadowReview,
    primary_attempt: String,
    primary_verdict: String,
    wait: Duration,
}

/// One check's settled outcome, bundled for
/// [`LandingPipeline::record_check_verification_span`] the same way
/// [`ShadowComparisonRequest`] is above — so the three per-check call sites
/// in `run_gates_at`'s check loop stay under `clippy::too_many_arguments`
/// without an `#[allow]`. `entry` (repo/target/task identity) stays a
/// separate parameter on that method since it is loop-invariant context,
/// not per-occurrence data.
struct CheckVerificationSpan<'a> {
    check_name: &'a str,
    attempt: u32,
    candidate: &'a str,
    full_check_required: bool,
    queue_wait_ms: Option<u64>,
    duration_ms: Option<u64>,
    /// Whether this check's pass came from a reused durable proof
    /// (TKT-01M0QRZ7QT8CQD74GHRN81XFT5) rather than actually executing the
    /// command in the gate worktree.
    proof_reused: bool,
}

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
    /// Exact source generation off the triggering `harness_result`. `None`
    /// is reserved for operator submissions not attributable to an agent.
    #[serde(default)]
    pub(crate) source_spawn: Option<rk_core::id::SpawnId>,
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
    /// `None` only before a fresh in-memory entry is enqueued; persisted rows
    /// without it are refused.
    #[serde(default)]
    pub(crate) enqueued_at: Option<DateTime<Utc>>,
    /// When this candidate entered the PHASE it is currently in — the
    /// per-phase counterpart to [`LandingQueueEntry::enqueued_at`], and the
    /// only clock a phase-latency consumer may read (see
    /// [`LandingQueueSnapshotEntry::phase_age_secs`]).
    ///
    /// Maintained exclusively by [`LandingQueueEntry::transition_to`], which
    /// resets it when — and only when — a status transition changes the
    /// mapped [`crate::span::Phase`]. The reset is deliberately phase-
    /// granular rather than status-granular: `Queued` and `RunningGates`
    /// BOTH map to `Phase::VerificationQueued`, so a candidate being claimed
    /// out of the queue must keep aging against the verification target it
    /// has already been accruing against — resetting there would hide a
    /// genuinely wedged verification lane exactly the way reusing total
    /// queue age fabricates a review breach.
    ///
    /// PRESERVED across [`LandingQueue::requeue_tail`] for the same reason
    /// `enqueued_at` is: a requeue re-enters `Queued`, still the
    /// verification phase, so a candidate stuck in a requeue loop keeps
    /// aging instead of zeroing its phase clock on every retry.
    ///
    /// `None` only before a fresh in-memory entry first transitions; persisted
    /// rows without it are refused.
    #[serde(default)]
    pub(crate) phase_entered_at: Option<DateTime<Utc>>,
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
    ///
    /// This marker is a hint that a retry MAY still be owed, never proof of
    /// it: the clear is a separate write from the ordinal-2 evidence, so a
    /// crash between them leaves it `Some` over an already-settled retry. The
    /// durable evidence is the authority — see
    /// [`LandingPipeline::settled_infra_retry`].
    #[serde(default)]
    pub(crate) gate_infra_retry_check: Option<String>,
}

impl LandingQueueEntry {
    fn validate_persisted(&self) -> rk_core::Result<()> {
        if self.enqueued_at.is_none() || self.phase_entered_at.is_none() {
            return Err(rk_core::Error::other(
                "landing queue entry predates the exact phase-clock schema; drain or clear it with the previous release before upgrading",
            ));
        }
        Ok(())
    }

    /// Move this entry to `status`, maintaining the per-phase clock
    /// [`LandingQueueEntry::phase_entered_at`] alongside it. EVERY status
    /// write goes through here (`enqueue`, `claim_next`, `claim_batch`,
    /// `set_status`, `persist`) so the clock cannot drift out of step with
    /// the status it describes.
    ///
    /// The clock restarts only when the transition crosses a
    /// [`crate::span::Phase`] boundary — see the field's doc for why that is
    /// phase-granular and not status-granular. A first-ever transition on an
    /// fresh entry that has no clock yet starts one. Persisted entries missing
    /// the clock are rejected before reaching this method.
    fn transition_to(&mut self, status: LandingEntryStatus, now: DateTime<Utc>) {
        if self.phase_entered_at.is_none() || status.phase() != self.status.phase() {
            self.phase_entered_at = Some(now);
        }
        self.status = status;
    }
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

impl LandingEntryStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            LandingEntryStatus::Queued => "queued",
            LandingEntryStatus::RunningGates => "running_gates",
            LandingEntryStatus::AwaitingReview => "awaiting_review",
            LandingEntryStatus::Landing => "landing",
        }
    }

    /// The task-to-main [`crate::span::Phase`] a candidate in this status is
    /// living in. Deliberately the ONE definition of that mapping: the
    /// phase-latency sweep (`crate::server`) labels its live probes with it,
    /// and [`LandingQueueEntry::transition_to`] decides whether to restart
    /// the per-phase clock with it. Two copies could disagree, and a
    /// disagreement is precisely the misattribution bug this exists to
    /// prevent — a phase boundary the sweep sees but the clock does not
    /// silently reintroduces prior-phase elapsed time.
    pub(crate) fn phase(self) -> crate::span::Phase {
        match self {
            // Both are the verification lane: `Queued` is waiting for a gate
            // slot, `RunningGates` is holding one. A candidate crossing
            // between them has not left the phase.
            LandingEntryStatus::Queued | LandingEntryStatus::RunningGates => {
                crate::span::Phase::VerificationQueued
            }
            LandingEntryStatus::AwaitingReview => crate::span::Phase::SemanticReview,
            LandingEntryStatus::Landing => crate::span::Phase::Merge,
        }
    }
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
        // Through `transition_to`, not a bare assignment, so the per-phase
        // clock is maintained here too: a fresh candidate starts one, and a
        // `requeue_tail` clone re-entering `Queued` from `RunningGates`
        // (same phase) carries its existing one forward untouched.
        entry.transition_to(LandingEntryStatus::Queued, Utc::now());
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
        entry.validate_persisted()?;
        if entry.status != LandingEntryStatus::Landing {
            entry.transition_to(LandingEntryStatus::RunningGates, Utc::now());
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
            entry.validate_persisted()?;
            if entry.status != LandingEntryStatus::Landing {
                entry.transition_to(LandingEntryStatus::RunningGates, Utc::now());
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
        updated.transition_to(status, Utc::now());
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
        entry.transition_to(status, Utc::now());
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
        // Status is deliberately left as-is for `enqueue` to transition:
        // its `transition_to(Queued, …)` needs the PRIOR status to decide
        // whether this requeue crosses a phase boundary (`RunningGates` ->
        // `Queued` does not; `Landing` -> `Queued` does). Zeroing it here
        // first would make every requeue look like a same-phase move and
        // carry a stale merge-phase clock into the verification lane.
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

/// The instance id for the Nth replacement reviewer after [`review_instance_id`]'s
/// primary died before a verdict (module doc's `ReviewWaitOutcome::ReviewerDied`
/// path). Distinct from the primary and from every other retry, so
/// `run_review_owned_with_id` can never resolve a retry dispatch back onto the
/// dead primary instance (or a sibling retry) — this is what makes a late
/// verdict from a dead generation structurally incapable of racing or
/// overriding a live replacement: each attempt's `rd` pattern in
/// [`LandingPipeline::await_primary_verdict`] filters on its OWN `review_attempt`,
/// which is this id, not a shared one.
fn review_retry_instance_id(entry: &LandingQueueEntry, retry_attempt: u32) -> String {
    format!("{}-retry{retry_attempt}", review_instance_id(entry))
}

/// The three nondeterministic inputs the review-death backoff path reads:
/// wall-clock now (which fixes the durable `not_before` a dispatch persists),
/// the uniform `[0.0, 1.0]` jitter draw
/// `landing_review_retry::retry_delay` scales by, and the real-time wait
/// itself. Gathered behind ONE injectable seam so a test can drive the real
/// dispatch path — [`LandingPipeline::route_review_death`]'s `Dispatch` arm
/// and [`LandingPipeline::await_review_retry_after_backoff`] — against a
/// frozen clock and a fixed draw, and then assert the EXACT schedule
/// persisted and the EXACT wait performed. Testing the pure
/// `landing_review_retry::retry_delay` helper alone can only prove the
/// arithmetic; only this seam can prove what the pipeline actually schedules
/// under a live repository policy, which is the property a repo operator is
/// really relying on.
///
/// Production wiring is the obvious one ([`Default`]): `Utc::now`,
/// `rand::random`, `tokio::time::sleep`. Nothing outside this trio is
/// seamed — every OTHER clock read in this module (queue enqueue stamps,
/// landed-at records, gate worktree ages) deliberately keeps using
/// `Utc::now` directly, so injecting a frozen clock here cannot distort
/// unrelated pipeline behavior in a test.
type RetrySleepFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type RetrySleeper = Box<dyn Fn(Duration) -> RetrySleepFuture + Send + Sync>;

pub(crate) struct RetrySchedule {
    now: Box<dyn Fn() -> DateTime<Utc> + Send + Sync>,
    jitter_unit: Box<dyn Fn() -> f64 + Send + Sync>,
    sleep: RetrySleeper,
}

impl RetrySchedule {
    fn now(&self) -> DateTime<Utc> {
        (self.now)()
    }

    fn jitter_unit(&self) -> f64 {
        (self.jitter_unit)()
    }

    async fn sleep(&self, wait: Duration) {
        (self.sleep)(wait).await
    }
}

impl Default for RetrySchedule {
    fn default() -> Self {
        Self {
            now: Box::new(Utc::now),
            jitter_unit: Box::new(rand::random::<f64>),
            sleep: Box::new(|wait| Box::pin(tokio::time::sleep(wait))),
        }
    }
}

/// The complete, repo-owned gate decision for one exact candidate and target.
/// Built once from `.rk/repo.cue`, `.rk/checks.cue`, and the candidate's
/// changed paths, then consumed without reading either policy file again.
type ResolvedGateCheck = (rk_workflow::Check, Vec<(String, String)>, Duration);

struct ResolvedGatePlan {
    checks: Vec<ResolvedGateCheck>,
    edge_class: LandingEdgeClass,
    full_check_required: bool,
    reason: String,
}

/// Classification of the one prepared-target advance implementation. Callers
/// still own mode-specific reporting, but none may call `land_prepared`
/// directly or reinterpret its stale/blocked flags independently.
enum TargetAdvance {
    Landed(Value),
    Stale(Value),
    Blocked(Value),
}

/// Which of the two landing-edge classes `GateConfig::protected_targets`
/// (`LandingPolicy::protected_targets`, `.rk/repo.cue`) puts a candidate's
/// `target` in — the switch between "run the full named check exactly once"
/// and "run only policy-selected focused checks", decided once per gate run
/// by [`LandingPipeline::gate_plan`] and recorded durably alongside the plan
/// (`LANDING_EDGE_PLAN_IDENTITY`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LandingEdgeClass {
    /// `target` is one of `GateConfig::protected_targets` — a final delivery
    /// destination. Runs the full `check_name` check, through the same
    /// prepared-candidate proof-key cache `verify_repo_check` gives a rat's
    /// own `verify.run` (never re-invented here — the existing
    /// `landing_gate_pass`-fallback reuse in
    /// `WorkflowEngine::lookup_verification_proof` already lets a reviewer's
    /// later `verify.run` on this exact candidate sha skip re-running it).
    ProtectedFinal,
    /// `target` is not a protected/final target — an inner child-to-parent
    /// edge. Runs only the checks `GateConfig::focused_checks` selects for
    /// this candidate's changed paths; never the full suite by default.
    Inner,
}

impl LandingEdgeClass {
    fn as_str(self) -> &'static str {
        match self {
            LandingEdgeClass::ProtectedFinal => "protected-final",
            LandingEdgeClass::Inner => "inner",
        }
    }
}

/// Whether POSIX ERE `pattern` matches any line of `paths` — evaluated
/// through `grep -E`, the same engine `steward-protected-paths`/
/// `steward-diff-scope` already use for their own patterns (their command
/// text in `.rk/checks.cue`), so a repo's `focusedChecks.paths` pattern
/// behaves identically to `protectedPaths`. A pattern that fails to even
/// spawn `grep` is treated as no match — fail-closed toward running FEWER
/// focused checks, never toward silently promoting to the full suite.
fn ere_matches_any(pattern: &str, paths: &[String]) -> bool {
    if pattern.trim().is_empty() || paths.is_empty() {
        return false;
    }
    let mut child = match std::process::Command::new("grep")
        .arg("-qE")
        .arg(pattern)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return false,
    };
    if let Some(mut stdin) = child.stdin.take() {
        use std::io::Write as _;
        let _ = stdin.write_all(paths.join("\n").as_bytes());
    }
    child.wait().map(|status| status.success()).unwrap_or(false)
}

/// Resolve `LandingPolicy::focused_checks` against one candidate's
/// `changed_paths`: every rule whose `paths` matches at least one changed
/// file (or that declares no `paths` at all, an unconditional catch-all)
/// contributes its `checks`, deduped in first-seen order. Returns the deduped
/// check names alongside one human-readable reason string per contributing
/// rule (`"<class> -> [<checks>]"`), for the durable edge-plan event's
/// `reason` field.
fn select_focused_checks(
    rules: &[rk_workflow::FocusedCheckRule],
    changed_paths: &[String],
) -> (Vec<String>, Vec<String>) {
    let mut selected = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut reasons = Vec::new();
    for rule in rules {
        let matches =
            rule.paths.is_empty() || rule.paths.iter().any(|p| ere_matches_any(p, changed_paths));
        if !matches {
            continue;
        }
        let mut added = Vec::new();
        for check_name in &rule.checks {
            if seen.insert(check_name.clone()) {
                selected.push(check_name.clone());
                added.push(check_name.clone());
            }
        }
        if !added.is_empty() {
            let label = if rule.class.is_empty() {
                "unlabeled rule"
            } else {
                rule.class.as_str()
            };
            reasons.push(format!("{label} -> [{}]", added.join(", ")));
        }
    }
    (selected, reasons)
}

/// The gate/tier tuning steward.cue exposes as workflow params
/// (formerly its `params` block) — same names, same
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
    /// aware wait, module doc) — matches the retired workflow's
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
    /// SHADOW REVIEW (`RepositoryPolicy.landing.shadowReviewModel`): when
    /// non-empty, [`LandingPipeline::request_review`] also launches a second,
    /// non-blocking reviewer on this model against the same candidate. Empty
    /// disables shadow review. See [`LandingPipeline::launch_shadow_review`].
    pub(crate) shadow_review_model: String,
    /// Harness for the shadow reviewer. Ignored when `shadow_review_model` is
    /// empty.
    pub(crate) shadow_review_harness: String,
    /// PROTECTED FINAL TARGETS (`LandingPolicy::protected_targets`): target
    /// branches this repo treats as protected/final delivery destinations.
    /// See [`LandingEdgeClass`].
    pub(crate) protected_targets: Vec<String>,
    /// FOCUSED CHECKS (`LandingPolicy::focused_checks`): the changed-path ->
    /// check-list rules an INNER edge (`target` not in `protected_targets`)
    /// selects from instead of running the full `check_name` check.
    pub(crate) focused_checks: Vec<rk_workflow::FocusedCheckRule>,
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
            protected_targets: vec!["main".into()],
            focused_checks: Vec::new(),
            // Deliberately OFF in this bare default, unlike every other field
            // here: `gate_config` never reads these two from `Default` (it
            // takes them straight off the resolved `LandingPolicy`, whose own
            // default IS "sonnet"/"claude"), so the only consumers of the
            // value below are unit tests constructing a `GateConfig` by hand —
            // which must get exactly ONE reviewer unless they opt in.
            shadow_review_model: String::new(),
            shadow_review_harness: String::new(),
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
    /// The candidate's source head already carried zero commits beyond its
    /// target when classified — an explicit no-op, never gated or reviewed
    /// (module doc, [`LANDING_EMPTY_IDENTITY`]). Carries the same
    /// `{branch, target, merged: false, delivered: false, status: "empty",
    /// reason, ...}` JSON shape callers already expect from an outcome
    /// value. Never advances the target, never creates a merge commit, and
    /// never records a delivery — diagnostic/operator ticket closure stays a
    /// separate explicit tracker action.
    Empty(Value),
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
    /// a dead reviewer. Carries the exact review-attempt id
    /// ([`review_instance_id`]/[`review_retry_instance_id`]) that hit the
    /// ceiling so the router can fence it (settle + release the still-live
    /// reviewer's capacity) — see [`LandingPipeline::settle_review_ceiling`].
    CeilingReached { instance_id: String },
    /// An operator explicitly cancelled this attempt out-of-band, via
    /// [`LandingPipeline::cancel_active_review`] (the `repo.land.cancel_review`
    /// RPC / `rk cancel-review`) — discovered by
    /// [`LandingPipeline::await_primary_verdict`]'s poll loop finding a
    /// ceiling-settlement marker for its own `instance_id` that it did not
    /// itself just write. `settle_review_ceiling` already ran by the time
    /// this is observed (the RPC calls it directly, synchronously, before
    /// this loop ever notices); the router's handling is idempotent so it
    /// is safe to call again.
    Cancelled { instance_id: String },
}

/// How [`LandingPipeline::route_review_death`] resolved one `ReviewerDied`
/// outcome — either a replacement reviewer was dispatched and awaited (its
/// own outcome may be a fresh `Verdict`, another `ReviewerDied`, or a
/// `CeilingReached`, all fed back through [`LandingPipeline::route_verdict_prepared`]'s
/// loop), or the retry ladder withheld and raised the one durable escalation.
#[derive(Debug)]
enum ReviewDeathOutcome {
    Retry(ReviewWaitOutcome),
    Escalated(Tuple),
}

/// What stopped [`LandingPipeline::run_gates_at`], distinct enough for its
/// callers to report an accurate escalation. `entry.gate_infra_retry_used`
/// alone cannot answer this: it stays `true` for the rest of the candidate's
/// gate run once ANY check has spent its retry, even if that retry PASSED
/// and a later, unrelated check then fails ordinarily — reading it at the
/// call site would falsely blame an exhausted retry for a plain gate
/// failure. `InfraRetryExhausted` is reported only at the exact point a
/// retry's own outcome is what stopped the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GateRunOutcome {
    /// Every check passed (`Ok(())` in `bool` terms).
    Pass,
    /// An ordinary check failure, timeout, or run error — never retried.
    Fail,
    /// The check that stopped the run is the one whose one-shot
    /// infrastructure-death retry just came back failing (inline or resumed
    /// after a crash).
    InfraRetryExhausted,
}

impl GateRunOutcome {
    fn passed(self) -> bool {
        matches!(self, GateRunOutcome::Pass)
    }
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
    /// Clock/jitter/sleep seam for review-death retry backoff — see
    /// [`RetrySchedule`]. Real in production; a test overrides it with
    /// [`LandingPipeline::with_retry_schedule`].
    retry_schedule: RetrySchedule,
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
            retry_schedule: RetrySchedule::default(),
        }
    }

    /// Replace the review-death backoff clock/jitter/sleep seam
    /// ([`RetrySchedule`]). Test-only: production always wants the real
    /// trio, and gating it behind `cfg(test)` keeps that a compile-time
    /// guarantee rather than a convention.
    #[cfg(test)]
    pub(crate) fn with_retry_schedule(mut self, schedule: RetrySchedule) -> Self {
        self.retry_schedule = schedule;
        self
    }

    /// Resolve this entry's repo-owned gate/review policy from its activated
    /// `.rk/repo.cue` (`RepositoryPolicy.landing`, digest-activated like
    /// `delivery` — `Supervisor::repository_policy`) — the daemon-native
    /// replacement for what the retired steward mega-workflow
    /// used to expose as workflow params (`protectedPaths`, `maxDiffFiles`,
    /// `maxDiffLines`, `gateTimeout`, `reviewTimeout`). A repo without an
    /// activated policy fails closed. `check_name` is not
    /// repo.cue-configurable: every repo's
    /// PROTECTED-FINAL edge (`protected_targets`, default `["main"]`) runs
    /// this same named `verify` check; an INNER edge instead runs whatever
    /// `focused_checks` selects (both repo.cue-configurable, see
    /// [`LandingEdgeClass`]).
    fn gate_config(&self, repo: &rk_git::Repo) -> rk_core::Result<GateConfig> {
        let policy = self.supervisor.repository_policy(repo)?.landing;
        let defaults = GateConfig::default();
        Ok(GateConfig {
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
            shadow_review_model: policy.shadow_review_model,
            shadow_review_harness: policy.shadow_review_harness,
            protected_targets: policy.protected_targets,
            focused_checks: policy.focused_checks,
        })
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
        let Some(marker) = self.admission_marker(&entry)? else {
            if let Some(target_head) = self.empty_candidate_at_admission(&entry) {
                let outcome = self.record_empty(&entry, &target_head)?;
                self.mark_processed(&entry, &outcome)?;
                return Ok(EnqueueDisposition::Processed);
            }
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
        source_spawn: Option<rk_core::id::SpawnId>,
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
            source_spawn,
            operator_fast_lane: true,
            keep_branch,
            ..Default::default()
        };
        match self.enqueue_disposition(entry.clone())? {
            EnqueueDisposition::Queued(_) | EnqueueDisposition::Pending => {}
            EnqueueDisposition::Processed => {
                // An empty/no-op candidate gets its own explicit status here
                // rather than the generic already-processed detail below —
                // acceptance requires a direct `rk land` on a branch with
                // nothing to land (or a replay of one already classified
                // that way) to report `status: "empty"`, `merged: false`,
                // `delivered: false`, not a vague "already processed" that
                // reads the same as a held or escalated branch.
                if let Some(event) = self.empty_event(&entry)? {
                    return Ok(json!({
                        "branch": branch,
                        "target": target,
                        "merged": false,
                        "delivered": false,
                        "status": "empty",
                        "reason": event.payload.get("reason").cloned().unwrap_or(Value::Null),
                        "already_processed": true,
                    }));
                }
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
                LandingOutcome::Empty(result) => result,
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
    /// `(repo, branch, head_sha, target)` work key, if one exists — the
    /// literal newest matching marker, trusted as-is regardless of whether
    /// the target has since moved. Used by [`Self::processed_outcome`]
    /// (process_entry's crash-window reconciliation, `submit_manual`'s
    /// post-drain lookup): both are asking "what became of THIS exact
    /// already-admitted attempt", not "should a NEW completion be admitted",
    /// so staleness does not apply — see [`Self::admission_marker`] for the
    /// probe that does. See [`Self::mark_processed`], the write side.
    ///
    /// `target` is filtered in Rust rather than folded into the
    /// `Pattern::for_commit` scan (which already spends both of
    /// [`Pattern`]'s substring predicate slots on `branch`+`head_sha`) —
    /// same shape as [`Self::cached_verdict`]/[`Self::review_artifact`]'s
    /// post-scan `target` check. Without it, a branch/head processed against
    /// one target reads back as `already_processed` for a *different*
    /// target: the same exact commit legitimately retargeted (e.g. a nested
    /// workflow branch held for a sub-target, then an operator lands that
    /// same head onto `main`) found no `landing_processed` event for `main`
    /// at all, yet `rk land` reported it already handled.
    ///
    /// `.last()` rather than the first match: normally there is exactly one
    /// marker per `(branch, head_sha, target)`, but [`Self::admission_marker`]
    /// letting a stale non-`landed` verdict be reprocessed can leave an older
    /// marker behind for the same key — the newest one is always the live
    /// answer.
    fn processed_marker(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<Tuple>> {
        let pattern = Pattern::for_commit(
            Category::Event,
            LANDING_PROCESSED_IDENTITY,
            &entry.branch,
            &entry.head_sha,
        )
        .scope(&entry.repo_name);
        Ok(self.space.scan(&pattern)?.into_iter().rfind(|t| {
            t.payload.get("target").and_then(Value::as_str) == Some(entry.target.as_str())
        }))
    }

    /// [`Self::processed_marker`], but additionally filtered to markers still
    /// CURRENT for admission purposes — the probe [`Self::enqueue_disposition`]
    /// actually gates a fresh completion on. A `landed` marker is always
    /// current: the merge already happened, is git-level idempotent to
    /// repeat (module doc — "the landing CAS already makes a literal
    /// double-advance harmless"), and this dedup exists only to skip
    /// redundant gate/review work on a redelivery, not to guard correctness;
    /// re-evaluating it after the target moves further would buy nothing and
    /// risks a second delivery record.
    ///
    /// A non-`landed` terminal outcome (gate-held, no-gate, rework-filed,
    /// escalated) instead recorded WHY the branch stayed unmerged against
    /// the target's tip AT THAT MOMENT — a protected-paths or diff-scope
    /// violation, a review REWORK, an escalation. Once the target has since
    /// moved (unrelated commits landed, a violation may no longer apply, a
    /// broken checks registry may have since been fixed), that verdict no
    /// longer describes the live ref: treating it as still current would
    /// permanently wedge the branch instead of giving it a fresh attempt
    /// against the ref as it actually stands now — so a moved target makes
    /// this probe report `None`, and [`Self::enqueue_disposition`] admits a
    /// fresh attempt.
    ///
    /// A marker written before `target_head` existed, or one whose
    /// comparison this probe cannot currently resolve (repo unreadable,
    /// target ref gone), is treated as current — sticky, matching this
    /// dedup's pre-existing behavior — rather than guessed at either way.
    fn admission_marker(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<Tuple>> {
        Ok(self.processed_marker(entry)?.filter(|marker| {
            if marker.payload.get("outcome").and_then(Value::as_str) == Some("landed") {
                return true;
            }
            let Some(recorded) = marker.payload.get("target_head").and_then(Value::as_str) else {
                return true;
            };
            match self.current_target_head(entry) {
                Some(current) => current == recorded,
                None => true,
            }
        }))
    }

    /// Best-effort current tip of `entry.target`, resolved fresh (not
    /// cached) so [`Self::admission_marker`] always compares against the
    /// live ref. `None` on any resolution failure — repo unreadable, target
    /// ref gone — rather than propagating an error into a dedup probe.
    fn current_target_head(&self, entry: &LandingQueueEntry) -> Option<String> {
        rk_git::Repo::discover(Path::new(&entry.repo_path))
            .and_then(|repo| repo.rev_parse(&entry.target))
            .ok()
    }

    /// `entry`'s source head already contained in `target`'s live tip — zero
    /// commits for this exact candidate to contribute. `None` when the
    /// target ref cannot be resolved, so an unresolvable target fails closed
    /// into the normal gated path rather than a silent short-circuit.
    fn empty_candidate_target_head(
        &self,
        entry: &LandingQueueEntry,
        repo: &rk_git::Repo,
    ) -> Option<String> {
        let target_head = repo.rev_parse(&entry.target).ok()?;
        repo.is_ancestor(&entry.head_sha, &target_head)
            .then_some(target_head)
    }

    /// [`Self::empty_candidate_target_head`] for [`Self::enqueue_disposition`],
    /// which has no [`rk_git::Repo`] handle yet — a fresh completion or
    /// operator `rk land` is classified before any candidate is ever
    /// prepared or queued.
    fn empty_candidate_at_admission(&self, entry: &LandingQueueEntry) -> Option<String> {
        let repo = rk_git::Repo::discover(Path::new(&entry.repo_path)).ok()?;
        self.empty_candidate_target_head(entry, &repo)
    }

    /// The durable [`LANDING_EMPTY_IDENTITY`] visibility event for `entry`'s
    /// exact work key, if one exists — read back by [`Self::submit_manual`]
    /// so a `Processed` disposition (fresh classification OR a replayed
    /// duplicate of one already classified this way) can report the
    /// explicit `"empty"` status instead of the generic already-processed
    /// detail. Same shape as [`Self::processed_marker`]: scan-then-filter on
    /// `target` in Rust, `.rfind` for the newest match.
    fn empty_event(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<Tuple>> {
        let pattern = Pattern::for_commit(
            Category::Event,
            LANDING_EMPTY_IDENTITY,
            &entry.branch,
            &entry.head_sha,
        )
        .scope(&entry.repo_name);
        Ok(self.space.scan(&pattern)?.into_iter().rfind(|t| {
            t.payload.get("target").and_then(Value::as_str) == Some(entry.target.as_str())
        }))
    }

    /// Durable, visible record of an empty/no-op landing candidate — the
    /// [`LANDING_EMPTY_IDENTITY`] event `rk scan`/`rk inbox` can surface,
    /// written alongside (not instead of) the [`Self::mark_processed`] dedup
    /// marker. Never advances the target, never lands a merge commit, and
    /// never records a delivery.
    fn record_empty(
        &self,
        entry: &LandingQueueEntry,
        target_head: &str,
    ) -> rk_core::Result<LandingOutcome> {
        let reason = format!(
            "{} onto {} carries no commits beyond the current target tip ({target_head}) — \
             nothing to land",
            entry.branch, entry.target
        );
        self.space.out(
            Tuple::new(
                Category::Event,
                entry.repo_name.clone(),
                LANDING_EMPTY_IDENTITY,
                "daemon",
                json!({
                    "repo": entry.repo_name,
                    "branch": entry.branch,
                    "target": entry.target,
                    "target_head": target_head,
                    "head_sha": entry.head_sha,
                    "task": entry.task,
                    "reason": reason,
                    "state": "empty",
                }),
            )
            .with_lifecycle(Lifecycle::Furniture),
        )?;
        Ok(LandingOutcome::Empty(json!({
            "branch": entry.branch,
            "target": entry.target,
            "merged": false,
            "delivered": false,
            "status": "empty",
            "reason": reason,
            "head_sha": entry.head_sha,
            "target_head": target_head,
        })))
    }

    /// The recorded terminal outcome string for `entry`'s work key, when a
    /// `landing_processed` marker exists — the read side `submit_manual`'s
    /// post-drain lookup uses to report what became of the caller's own
    /// just-submitted entry once another consumer has drained it. Reads the
    /// RAW [`Self::processed_marker`], not [`Self::admission_marker`]: at
    /// that call site the entry in question was JUST processed under the
    /// same key-exclusive lock this submission itself is holding, so the
    /// newest matching marker is unambiguously the answer for THIS
    /// submission — no staleness filter is meaningful there.
    /// [`Self::process_entry`]'s crash-window reconciliation used to share
    /// this helper but now goes through `admission_marker` directly instead
    /// — a claimed entry there can also be a fresh attempt just admitted
    /// past an old, now-superseded marker, which this raw form would
    /// otherwise short-circuit incorrectly.
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
    ///
    /// Stamps the target's current tip as `target_head` so a later probe can
    /// tell — for a non-`landed` outcome only, [`Self::admission_marker`] —
    /// whether this verdict still describes the live ref or was left behind
    /// by the target moving on. Best-effort: `None` (omitted as JSON `null`)
    /// when the tip cannot be resolved, which reads back as "still current"
    /// rather than as a false staleness signal.
    fn mark_processed(
        &self,
        entry: &LandingQueueEntry,
        outcome: &LandingOutcome,
    ) -> rk_core::Result<()> {
        let outcome_str = match outcome {
            LandingOutcome::Landed(_) => "landed",
            LandingOutcome::Empty(_) => "empty",
            LandingOutcome::GateHeld => "gate-held",
            LandingOutcome::NoGate(_) => "no-gate",
            LandingOutcome::ReworkFiled(_) => "rework-filed",
            LandingOutcome::Escalated(_) => "escalated",
            LandingOutcome::Requeued { .. } => return Ok(()),
            // A reconciled entry's marker already exists from the run that
            // performed the side effects; writing a second would corrupt the
            // one-current-marker-per-work-key invariant `processed_marker`
            // reads (module doc on `admission_marker`: a stale non-`landed`
            // predecessor may still be sitting there too, which is fine —
            // `.last()` picks this one, not it).
            LandingOutcome::Reconciled(_) => return Ok(()),
        };
        let target_head = self.current_target_head(entry);
        let tuple = Tuple::new(
            Category::Event,
            entry.repo_name.clone(),
            LANDING_PROCESSED_IDENTITY,
            "daemon",
            json!({
                "branch": entry.branch,
                "target": entry.target,
                "target_head": target_head,
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

    /// Terminal fail-closed transition for a candidate whose repo-owned CUE
    /// policy cannot produce a complete gate plan. Policy errors are verdicts
    /// on admissibility, not transient daemon faults, so retrying the queue
    /// forever would only hide the required human action.
    fn hold_no_gate(
        &self,
        entry: &LandingQueueEntry,
        error: String,
    ) -> rk_core::Result<LandingOutcome> {
        let need = self.escalate(
            entry,
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
                    "error": error,
                    "state": "no-gate",
                }),
            )
            .with_lifecycle(Lifecycle::Furniture),
        )?;
        let outcome = LandingOutcome::NoGate(need);
        self.mark_processed(entry, &outcome)?;
        Ok(outcome)
    }

    async fn process_entry(&self, entry: &LandingQueueEntry) -> rk_core::Result<LandingOutcome> {
        // Crash-window reconciliation (review round 2): a crash between
        // `mark_processed` and the caller's queue removal leaves both the
        // marker and the queue entry. The marker is the truth — never repeat
        // terminal side effects (needs, rework tickets, the land itself);
        // just report what already happened so the caller removes the entry.
        //
        // Deliberately `admission_marker`, not the raw `processed_outcome`:
        // a claimed entry can also be a candidate `admission_marker` just
        // ADMITTED because the target moved since a stale non-`landed`
        // marker was recorded — the raw marker still exists (only
        // superseded, never deleted) and would otherwise short-circuit this
        // fresh attempt straight back to `Reconciled`, silently skipping the
        // very gate re-run the move was supposed to trigger. A genuine
        // same-process crash-window marker is always current by construction
        // (mark_processed→crash→restart is too narrow a window for the
        // target to move in between), so this stays exactly as strict for
        // the case it exists to guard.
        if let Some(marker) = self.admission_marker(entry)? {
            let prior = marker
                .payload
                .get("outcome")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
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
        // Re-checked fresh here (not just at admission): a candidate can sit
        // `Queued` while the target catches up to its exact head through an
        // unrelated landing, or an already-queued entry can be discovered
        // for the first time after a restart — either way this must resolve
        // to the same terminal no-op before ever touching gates or review.
        // Placed after `recover_completed_land` deliberately: that call
        // already consumed the one legitimate case where the target
        // containing `entry.head_sha` means "my own prepared candidate just
        // landed" rather than "nothing to land" — reaching here means that
        // was ruled out (or entry.status is not `Landing` at all).
        if let Some(target_head) = self.empty_candidate_target_head(&entry, &git_repo) {
            if let Some(candidate_ref) = entry.candidate_ref.clone() {
                let _ = git_repo.discard_candidate(&candidate_ref);
            }
            let outcome = self.record_empty(&entry, &target_head)?;
            self.mark_processed(&entry, &outcome)?;
            return Ok(outcome);
        }
        let gates = self.gate_config(&git_repo)?;
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
                    let outcome = self.route_conflict(&entry, &git_repo, &detail).await?;
                    self.mark_processed(&entry, &outcome)?;
                    return Ok(outcome);
                }
            }
        };
        let gate_plan = match self
            .resolve_gate_plan_at(&entry, &git_repo, &gates, &candidate.commit)
            .await
        {
            Ok(plan) => plan,
            Err(error) => {
                git_repo.discard_candidate(&candidate.candidate_ref)?;
                return self.hold_no_gate(&entry, error.to_string());
            }
        };
        let gate_outcome = self
            .execute_gate_plan_at(&mut entry, &git_repo, gate_plan, &candidate.commit)
            .await?;
        if gate_outcome != GateRunOutcome::Pass {
            git_repo.discard_candidate(&candidate.candidate_ref)?;
            // The durable gate-failure artifact carries the evidence; the
            // need row is what makes the hold VISIBLE in `rk inbox` — parity
            // with the CUE steward's escalation contract. A hold that
            // followed an exhausted infra-death retry says so explicitly —
            // "one precise human gate" distinct from an ordinary red check,
            // since the automatic recovery path already ran and lost. Keyed
            // on `gate_outcome`, not `entry.gate_infra_retry_used`: that flag
            // stays `true` for the rest of this candidate's gate run even
            // after a retry PASSES, so reading it here would misreport a
            // later, unrelated ordinary failure as a retry exhaustion.
            let text = if gate_outcome == GateRunOutcome::InfraRetryExhausted {
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
            self.queue.set_status(&entry, LandingEntryStatus::Landing)?;
            let outcome = match self
                .advance_target(&entry, entry.keep_branch, &candidate)
                .await?
            {
                TargetAdvance::Landed(result) => self.finalize_landed(&entry, result).await?,
                TargetAdvance::Stale(result) => {
                    return self.requeue_stale(&entry, &git_repo, &candidate, &result);
                }
                TargetAdvance::Blocked(result) => {
                    LandingOutcome::Escalated(self.worktree_blocked_gate(&entry, &result)?)
                }
            };
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
        if let Some(recovered) = self.recover_completed_batch(&entries, &repo).await? {
            return Ok(recovered);
        }
        let gates = self.gate_config(&repo)?;

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

        let gate_plan = match self
            .resolve_gate_plan_at(&entries[0], &repo, &gates, &candidate.commit)
            .await
        {
            Ok(plan) => plan,
            Err(error) => {
                repo.discard_candidate(&candidate.candidate_ref)?;
                let error = error.to_string();
                let mut outcomes = Vec::with_capacity(entries.len());
                for entry in entries {
                    let outcome = self.hold_no_gate(&entry, error.clone())?;
                    outcomes.push((entry, outcome));
                }
                return Ok(outcomes);
            }
        };

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
            .execute_gate_plan_at(&mut entries[0], &repo, gate_plan, &candidate.commit)
            .await?
            .passed()
        {
            return self.bisect_batch(entries, Some(&candidate)).await;
        }

        let first = entries[0].clone();
        for entry in &entries {
            self.queue.set_status(entry, LandingEntryStatus::Landing)?;
        }
        let mut result = match self.advance_target(&first, true, &candidate).await? {
            TargetAdvance::Landed(result) => result,
            TargetAdvance::Stale(result) => {
                let mut outcomes = Vec::with_capacity(entries.len());
                for entry in entries {
                    let outcome = self.requeue_stale(&entry, &repo, &candidate, &result)?;
                    outcomes.push((entry, outcome));
                }
                return Ok(outcomes);
            }
            TargetAdvance::Blocked(result) => {
                let mut outcomes = Vec::with_capacity(entries.len());
                for entry in entries {
                    let outcome =
                        LandingOutcome::Escalated(self.worktree_blocked_gate(&entry, &result)?);
                    self.mark_processed(&entry, &outcome)?;
                    outcomes.push((entry, outcome));
                }
                return Ok(outcomes);
            }
        };

        let policy = self.supervisor.repository_policy(&repo)?;
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
            let outcome = self.finalize_landed(&entry, result.clone()).await?;
            self.mark_processed(&entry, &outcome)?;
            outcomes.push((entry, outcome));
        }
        Ok(outcomes)
    }

    /// Batch counterpart to [`Self::recover_completed_land`]. Batch landing
    /// advances one shared prepared commit and only then finalizes each member;
    /// after a crash or retry, running gates/CAS again would misclassify that
    /// already-landed commit as stale. Recover straight from the durable
    /// `Landing` rows whenever their exact shared candidate is contained in
    /// the target.
    async fn recover_completed_batch(
        &self,
        entries: &[LandingQueueEntry],
        repo: &rk_git::Repo,
    ) -> rk_core::Result<Option<Vec<(LandingQueueEntry, LandingOutcome)>>> {
        if entries.is_empty()
            || entries
                .iter()
                .any(|entry| entry.status != LandingEntryStatus::Landing)
        {
            return Ok(None);
        }
        let Some(commit) = entries[0].candidate_sha.as_deref() else {
            return Ok(None);
        };
        let Some(base) = entries[0].candidate_base.as_deref() else {
            return Ok(None);
        };
        if entries.iter().any(|entry| {
            entry.candidate_sha.as_deref() != Some(commit)
                || entry.candidate_base.as_deref() != Some(base)
        }) || !repo.is_ancestor(commit, &entries[0].target)
        {
            return Ok(None);
        }

        let policy = self.supervisor.repository_policy(repo)?;
        let mut all_deleted = true;
        if policy.delivery.delete_source {
            for entry in entries {
                if entry.keep_branch {
                    all_deleted = false;
                    continue;
                }
                if repo.branch_exists(&entry.branch) && repo.delete_branch(&entry.branch).is_err() {
                    all_deleted = false;
                }
            }
        } else {
            all_deleted = false;
        }

        let result = json!({
            "branch": entries[0].branch,
            "target": entries[0].target,
            "delivered": true,
            "merged": true,
            "merge_commit": commit,
            "content_free": commit == base,
            "recovered": true,
            "branch_deleted": all_deleted,
            "batch_branches": entries.iter().map(|entry| entry.branch.clone()).collect::<Vec<_>>(),
            "batch_size": entries.len(),
        });
        let mut outcomes = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Some(marker) = self.admission_marker(entry)? {
                let prior = marker
                    .payload
                    .get("outcome")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string();
                outcomes.push((entry.clone(), LandingOutcome::Reconciled(prior)));
                continue;
            }
            let outcome = self.finalize_landed(entry, result.clone()).await?;
            self.mark_processed(entry, &outcome)?;
            outcomes.push((entry.clone(), outcome));
        }
        Ok(Some(outcomes))
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
    ///
    /// Settlement is checked FIRST, before any cache read (audit gap fixed
    /// here: [`Self::route_review_death`] already refuses to re-decide a
    /// settled chain, but that guard is USELESS if this function reads a
    /// late verdict off a dead attempt and returns it as current before
    /// `route_review_death` ever runs). A restart between the settled
    /// marker's write and this candidate's queue removal
    /// (`Self::process_entry`'s `mark_processed` runs only at the very end)
    /// re-enters here first; without this check, a verdict artifact that a
    /// zombie dead-generation reviewer posts AFTER escalation would be read
    /// straight back as authoritative and acted on, silently overriding the
    /// human hold. Routing the settled case back through
    /// [`ReviewWaitOutcome::ReviewerDied`] reuses `route_review_death`'s own
    /// settled-marker short-circuit (already replay-safe, already tested)
    /// instead of duplicating its escalation logic here.
    async fn review_verdict(
        &self,
        entry: &LandingQueueEntry,
        gates: &GateConfig,
    ) -> rk_core::Result<ReviewWaitOutcome> {
        if let Some(settled) = self.review_death_settlement(entry)? {
            return Ok(ReviewWaitOutcome::ReviewerDied(format!(
                "review-death retry chain already settled ({}); a late verdict from the dead \
                 generation must not be read as current",
                settled
                    .payload
                    .get("state")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            )));
        }
        if let Some(cached) = self.cached_verdict(entry)? {
            return Ok(ReviewWaitOutcome::Verdict(cached));
        }
        let active_attempt = self.active_review_attempt(entry)?;
        if active_attempt != review_instance_id(entry) {
            // A daemon can restart after the retry marker is durable but
            // before its backoff schedule (`not_before`) has elapsed, or
            // while the replacement is still waiting for its verdict.
            // Resume that exact instance through the same backoff-aware
            // wait the fresh dispatch used — reading its PERSISTED
            // `not_before` back rather than drawing a fresh one, so a
            // restart can never reset or multiply the schedule.
            // `dispatch_review` is idempotent for the stable retry id and
            // never falls back to the dead primary.
            let not_before = self.review_death_not_before(entry, &active_attempt)?;
            return self
                .await_review_retry_after_backoff(entry, gates, &active_attempt, not_before)
                .await;
        }
        self.request_review(entry, gates).await
    }

    /// The review attempt id currently authoritative for `entry`: the
    /// highest-numbered review-death retry dispatched so far (module
    /// [`landing_review_retry`]), or the primary [`review_instance_id`] if
    /// none has been. Every verdict READ — this restart-safe cache probe, or
    /// a live [`Self::await_primary_verdict`] wait — scopes to exactly this
    /// id, never to an earlier one: once a later attempt has been
    /// dispatched, an earlier attempt is by definition a DEAD generation, so
    /// a verdict that arrives from it late — however it arrives — can never
    /// be read as authoritative again. This is what keeps a late verdict
    /// from a dead generation from racing or overriding its replacement.
    ///
    /// Settlement-aware (defense in depth alongside
    /// [`Self::review_verdict`]'s own settled-first check): once this
    /// candidate's review-death chain has reached a final withhold/escalate
    /// decision, NO attempt is active any more — the chain is done, held for
    /// a human — so this returns a sentinel id (`"{primary}-settled"`) that
    /// can never equal any real dispatched attempt's id
    /// ([`review_instance_id`]/[`review_retry_instance_id`] never produce a
    /// `-settled` suffix), guaranteeing [`Self::cached_verdict`] misses on
    /// every verdict artifact, dead or otherwise, for a settled candidate.
    fn active_review_attempt(&self, entry: &LandingQueueEntry) -> rk_core::Result<String> {
        let ctx = ReviewDeathContext {
            repo: entry.repo_name.clone(),
            repo_path: entry.repo_path.clone(),
            branch: entry.branch.clone(),
            head_sha: entry.head_sha.clone(),
            target: entry.target.clone(),
            task: entry.task.clone(),
        };
        if self.review_death_settled_marker(&ctx)?.is_some() {
            return Ok(format!("{}-settled", review_instance_id(entry)));
        }
        let latest_retry = self
            .review_death_dispatch_markers(&ctx)?
            .into_iter()
            .filter(|marker| {
                matches!(
                    marker.payload.get("state").and_then(Value::as_str),
                    Some("dispatching" | "dispatched")
                )
            })
            .filter_map(|marker| marker.payload.get("attempt").and_then(Value::as_u64))
            .max();
        let candidate = match latest_retry {
            Some(attempt) => review_retry_instance_id(entry, attempt as u32),
            None => review_instance_id(entry),
        };
        // A ceiling-settled attempt is a dead generation exactly like a
        // withheld review-death chain (module doc above): once
        // `settle_review_ceiling` has fenced it, nothing may read it as
        // current again — UNLESS an explicit, bounded
        // [`Self::reenqueue_after_ceiling`] has since superseded it with a
        // fresh attempt, in which case THAT attempt becomes authoritative
        // instead (the whole point of re-enqueuing: give the replacement a
        // real chance to be read as current, not fence it too).
        if let Some(reenqueue) = self.review_ceiling_reenqueue_marker(entry, &candidate)? {
            return required_payload_str(&reenqueue.payload, "new_attempt", "reenqueue marker")
                .map(str::to_string);
        }
        if self.review_ceiling_settlement(entry, &candidate)?.is_some() {
            return Ok(format!("{candidate}-ceiling-settled"));
        }
        Ok(candidate)
    }

    /// The fresh attempt [`Self::reenqueue_after_ceiling`] already dispatched
    /// for `settled_attempt`, if any — shared by that function's own
    /// idempotency check and [`Self::active_review_attempt`]'s un-fencing
    /// lookup, so the two can never disagree about whether a re-enqueue has
    /// happened.
    fn review_ceiling_reenqueue_marker(
        &self,
        entry: &LandingQueueEntry,
        settled_attempt: &str,
    ) -> rk_core::Result<Option<Tuple>> {
        let pattern = Pattern::category(Category::Event)
            .identity(REVIEW_CEILING_REENQUEUE_IDENTITY)
            .scope(&entry.repo_name);
        Ok(self.space.scan(&pattern)?.into_iter().find(|t| {
            t.payload.get("settled_attempt").and_then(Value::as_str) == Some(settled_attempt)
        }))
    }

    /// Thin `entry`-keyed wrapper over [`Self::review_death_settled_marker`]
    /// for [`Self::review_verdict`]'s settled-first check.
    fn review_death_settlement(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<Tuple>> {
        let ctx = ReviewDeathContext {
            repo: entry.repo_name.clone(),
            repo_path: entry.repo_path.clone(),
            branch: entry.branch.clone(),
            head_sha: entry.head_sha.clone(),
            target: entry.target.clone(),
            task: entry.task.clone(),
        };
        self.review_death_settled_marker(&ctx)
    }

    /// Non-blocking probe of Phase 2's commit-keyed verdict cache — ANY
    /// prior run's recommendation for this exact review context, regardless
    /// of who wrote it (§1.3), SCOPED to the currently active review attempt
    /// (see [`Self::active_review_attempt`]) so a restart can resume an
    /// in-flight or already-settled review-death retry without ever reading
    /// a dead generation's verdict back as current. A hit is honored
    /// identically to a fresh verdict — never re-reviewed to shop for a
    /// better opinion.
    fn cached_verdict(&self, entry: &LandingQueueEntry) -> rk_core::Result<Option<String>> {
        let expected_review_attempt = self.active_review_attempt(entry)?;
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
        let instance_id = review_instance_id(entry);
        self.dispatch_review(entry, gates, &instance_id)?;
        let shadow = self.launch_shadow_review(entry, gates, &instance_id);

        let outcome = self
            .await_primary_verdict(entry, gates, &instance_id)
            .await?;
        // Fire-and-forget: the comparison is observational, so it must never
        // add latency to (or fail) the landing decision that has already been
        // reached above.
        if let Some(shadow) = shadow {
            self.spawn_shadow_comparison(entry, shadow, &instance_id, &outcome, gates);
        }
        Ok(outcome)
    }

    /// Launch one review workflow instance under `instance_id` — the primary
    /// (see [`Self::request_review`]) or a review-death replacement (see
    /// [`Self::request_review_retry`]). `run_review_owned_with_id` resolves a
    /// repeat call for the SAME `instance_id` to the already-running (or
    /// already-finished) instance instead of spawning a duplicate — the same
    /// property [`review_instance_id`]'s doc comment relies on for restart
    /// safety, which is why neither caller needs its own crash-window
    /// dedup: re-entering this function for an id that already dispatched is
    /// itself idempotent.
    fn dispatch_review(
        &self,
        entry: &LandingQueueEntry,
        gates: &GateConfig,
        instance_id: &str,
    ) -> rk_core::Result<()> {
        self.queue
            .set_status(entry, LandingEntryStatus::AwaitingReview)?;
        let params = self.review_params(entry, gates, instance_id);
        let review = rk_core::review::ReviewContext {
            branch: entry.branch.clone(),
            head_sha: entry.head_sha.clone(),
            target: entry.target.clone(),
            task: entry.task.clone(),
            attempt: instance_id.to_string(),
        };
        // The engine's `repo` argument is a filesystem path (it feeds
        // `Repo::discover` and repo-local definition resolution), unlike the
        // `repo` WORKFLOW PARAM above, which is the repo's scope name used
        // to address its verdict artifact.
        self.engine.run_review_owned_with_id(
            instance_id.to_string(),
            REVIEW_WORKFLOW,
            &entry.repo_path,
            params,
            review,
        )?;
        Ok(())
    }

    /// Dispatch and await exactly one review-death replacement reviewer under
    /// `instance_id` (a [`review_retry_instance_id`]). No shadow reviewer: a
    /// retry is still the PRIMARY chain, just a later attempt at it — shadow
    /// review is an observational extra against the primary's own verdict,
    /// not something every replacement needs its own copy of.
    async fn request_review_retry(
        &self,
        entry: &LandingQueueEntry,
        gates: &GateConfig,
        instance_id: &str,
    ) -> rk_core::Result<ReviewWaitOutcome> {
        self.dispatch_review(entry, gates, instance_id)?;
        self.await_primary_verdict(entry, gates, instance_id).await
    }

    /// Workflow params for one review request — the PRIMARY reviewer's set.
    /// `priority`/`labels` are the candidate ticket's, threaded through so the
    /// review spawn participates in cost-tier routing (`tiers.rules`) exactly
    /// like a `for_each` worker fan-out does; `reviewerModel`/`reviewerHarness`
    /// are left empty here so the primary keeps whatever the tier table and
    /// the workflow's own `agents.reviewer` profile resolve to (see
    /// [`Self::launch_shadow_review`], which reuses this and overrides them).
    fn review_params(
        &self,
        entry: &LandingQueueEntry,
        gates: &GateConfig,
        attempt: &str,
    ) -> HashMap<String, Value> {
        let (priority, labels) = self.review_candidate_routing(entry);
        let mut params = HashMap::new();
        params.insert("taskId".to_string(), Value::String(entry.task.clone()));
        params.insert("branch".to_string(), Value::String(entry.branch.clone()));
        params.insert("repo".to_string(), Value::String(entry.repo_name.clone()));
        params.insert("target".to_string(), Value::String(entry.target.clone()));
        params.insert("headSha".to_string(), Value::String(entry.head_sha.clone()));
        params.insert("reviewAttempt".to_string(), Value::String(attempt.into()));
        params.insert(
            "reviewTimeout".to_string(),
            Value::String(format!("{}s", gates.review_max_wait.as_secs())),
        );
        params.insert("priority".to_string(), Value::String(priority));
        params.insert(
            "labels".to_string(),
            Value::Array(labels.into_iter().map(Value::String).collect()),
        );
        params.insert("reviewerModel".to_string(), Value::String(String::new()));
        params.insert("reviewerHarness".to_string(), Value::String(String::new()));
        params
    }

    /// The candidate ticket's `priority`/`labels` — the cost-tier routing
    /// predicate for the review spawn. `entry.task` is a ticket id for every
    /// candidate the reactor enqueues, but NOT for an operator `rk land
    /// --task` with free text, and a ticket can always have been closed and
    /// swept since; all of those degrade to `("", [])`, which matches no rule
    /// carrying an explicit `priority`/`label` and so leaves resolution
    /// exactly where it was before this wiring existed.
    fn review_candidate_routing(&self, entry: &LandingQueueEntry) -> (String, Vec<String>) {
        let Ok(Some(ticket)) = self.tickets.get(&entry.task) else {
            return (String::new(), Vec::new());
        };
        let priority = ticket
            .payload
            .get("priority")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let labels = ticket
            .payload
            .get("labels")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        (priority, labels)
    }

    /// Launch the second, NON-BLOCKING reviewer when the repo's landing policy
    /// configures one (`shadowReviewModel`), and return the handle the
    /// comparison record needs. `None` when shadow review is disabled.
    ///
    /// The shadow is the same review workflow against the same candidate
    /// branch/head, differing in exactly two ways: an inline
    /// `model`/`harness` override pinning it to the configured shadow model
    /// (an inline step override beats the tier table, unlike the primary's),
    /// and a distinct instance id / `reviewAttempt`. That distinct attempt is
    /// load-bearing — the primary's `rd` pattern and
    /// [`Self::cached_verdict`] both filter on the PRIMARY attempt, so the
    /// shadow's verdict is structurally incapable of being routed on or of
    /// being served as a cache hit to a later pass. The primary verdict stays
    /// the one and only authority.
    ///
    /// A launch failure is logged and swallowed: a broken shadow config must
    /// never take down the real review.
    fn launch_shadow_review(
        &self,
        entry: &LandingQueueEntry,
        gates: &GateConfig,
        primary_attempt: &str,
    ) -> Option<ShadowReview> {
        if gates.shadow_review_model.is_empty() {
            return None;
        }
        let attempt = format!("{primary_attempt}{SHADOW_INSTANCE_SUFFIX}");
        let mut params = self.review_params(entry, gates, &attempt);
        params.insert(
            "reviewerModel".to_string(),
            Value::String(gates.shadow_review_model.clone()),
        );
        params.insert(
            "reviewerHarness".to_string(),
            Value::String(gates.shadow_review_harness.clone()),
        );
        let review = rk_core::review::ReviewContext {
            branch: entry.branch.clone(),
            head_sha: entry.head_sha.clone(),
            target: entry.target.clone(),
            task: entry.task.clone(),
            attempt: attempt.clone(),
        };
        match self.engine.run_review_owned_with_id(
            attempt.clone(),
            REVIEW_WORKFLOW,
            &entry.repo_path,
            params,
            review,
        ) {
            Ok(_) => Some(ShadowReview {
                attempt,
                model: gates.shadow_review_model.clone(),
                harness: gates.shadow_review_harness.clone(),
            }),
            Err(e) => {
                warn!(
                    repo = %entry.repo_name, branch = %entry.branch,
                    shadow_model = %gates.shadow_review_model,
                    error = %e,
                    "landing pipeline: shadow reviewer failed to launch; the primary \
                     review is unaffected"
                );
                None
            }
        }
    }

    /// Detach the primary-vs-shadow comparison onto its own task so it can
    /// outlive the landing decision (the shadow is typically still running
    /// when the primary's verdict arrives) without holding the queue open.
    /// Nothing awaits the handle; a lost task on daemon shutdown costs one
    /// observational record and nothing else.
    fn spawn_shadow_comparison(
        &self,
        entry: &LandingQueueEntry,
        shadow: ShadowReview,
        primary_attempt: &str,
        outcome: &ReviewWaitOutcome,
        gates: &GateConfig,
    ) {
        let primary_verdict = match outcome {
            ReviewWaitOutcome::Verdict(v) => v.clone(),
            // The primary never produced one — there is nothing to compare
            // against, and the candidate is already headed for a human gate.
            _ => return,
        };
        let space = self.space.clone();
        let engine = Arc::clone(&self.engine);
        let supervisor = Arc::clone(&self.supervisor);
        let entry = entry.clone();
        let primary_attempt = primary_attempt.to_string();
        let wait = gates.review_max_wait;
        tokio::spawn(async move {
            let request = ShadowComparisonRequest {
                entry,
                shadow,
                primary_attempt,
                primary_verdict,
                wait,
            };
            if let Err(e) = Self::await_shadow_comparison(space, engine, supervisor, request).await
            {
                warn!(error = %e, "landing pipeline: shadow-review comparison not recorded");
            }
        });
    }

    /// Wait out the shadow reviewer (bounded by `wait`, off the landing path)
    /// and write the durable [`SHADOW_COMPARISON_IDENTITY`] artifact. Returns
    /// the tuple it wrote.
    ///
    /// Associated rather than a method so the detached task owns plain clones
    /// (`Space`, `Arc<WorkflowEngine>`) instead of the pipeline itself, and so
    /// tests can drive the wait deterministically.
    ///
    /// Records something in every case: agreement, disagreement, or a shadow
    /// that died / ran out the window without a verdict. `agreement` is a
    /// three-state string, never a bool, precisely so "the shadow never
    /// answered" cannot be silently counted as "the two models disagreed".
    async fn await_shadow_comparison(
        space: Space,
        engine: Arc<WorkflowEngine>,
        supervisor: Arc<Supervisor>,
        request: ShadowComparisonRequest,
    ) -> rk_core::Result<Tuple> {
        let ShadowComparisonRequest {
            entry,
            shadow,
            primary_attempt,
            primary_verdict,
            wait,
        } = request;
        let mut pattern = Pattern::category(Category::Artifact)
            .identity(REVIEW_ARTIFACT_IDENTITY)
            .scope(&entry.repo_name);
        pattern.payload_search = Some(format!("\"review_attempt\":\"{}\"", shadow.attempt));

        let deadline = tokio::time::Instant::now() + wait;
        let mut shadow_verdict = None;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let slice = remaining.min(REVIEW_POLL_SLICE);
            if let Some(tuple) = space.rd(&pattern, slice).await? {
                shadow_verdict = tuple
                    .payload
                    .get("recommendation")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                break;
            }
            if let Some(instance) = engine.status_any(&shadow.attempt) {
                if instance.status != InstanceStatus::Running {
                    // Same last-race probe the primary wait does: the verdict
                    // may have landed between the slice timing out and here.
                    shadow_verdict = space.scan(&pattern)?.into_iter().find_map(|t| {
                        t.payload
                            .get("recommendation")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    });
                    break;
                }
            }
        }

        let agreement = match shadow_verdict.as_deref() {
            None => "no-verdict",
            Some(v) if v == primary_verdict => "agree",
            Some(_) => "disagree",
        };
        // Each reviewer's own generation, found by its `review.attempt` —
        // the same join key `AgentRecord::review` persists at spawn time —
        // so the comparison carries the ACTUAL identity/model/spend of each
        // reviewer rather than merely the shadow's configured request. Not
        // found (already reaped, or an archived generation) degrades to
        // `null` rather than failing the whole comparison record.
        let records = supervisor.list_all();
        let find = |attempt: &str| {
            records
                .iter()
                .find(|r| r.review.as_ref().is_some_and(|rv| rv.attempt == attempt))
        };
        let primary_record = find(&primary_attempt);
        let shadow_record = find(&shadow.attempt);
        let tuple = Tuple::new(
            Category::Artifact,
            entry.repo_name.clone(),
            SHADOW_COMPARISON_IDENTITY,
            "daemon",
            json!({
                "task": entry.task,
                "branch": entry.branch,
                "head_sha": entry.head_sha,
                "target": entry.target,
                "review_attempt": primary_attempt,
                "shadow_attempt": shadow.attempt,
                "primary_identity": primary_record.map(|r| r.name.clone()),
                "primary_verdict": primary_verdict,
                "primary_model": primary_record.and_then(|r| r.model.clone()),
                "primary_spend_usd": primary_record.map(|r| r.cost_usd),
                "shadow_identity": shadow_record.map(|r| r.name.clone()),
                "shadow_verdict": shadow_verdict,
                "shadow_model": shadow.model,
                "shadow_harness": shadow.harness,
                "shadow_spend_usd": shadow_record.map(|r| r.cost_usd),
                "agreement": agreement,
                // Stated in the record itself so no later reader can mistake
                // this for a second opinion the pipeline acted on.
                "authoritative": "primary",
                "recorded_at": chrono::Utc::now().to_rfc3339(),
            }),
        )
        .with_lifecycle(Lifecycle::Furniture);
        space.out(tuple.clone())?;
        info!(
            repo = %entry.repo_name, branch = %entry.branch,
            primary_verdict = %primary_verdict,
            shadow_verdict = shadow_verdict.as_deref().unwrap_or("<none>"),
            shadow_model = %shadow.model,
            agreement,
            "landing pipeline: recorded primary-vs-shadow review comparison"
        );
        Ok(tuple)
    }

    /// The liveness-aware wait on the PRIMARY reviewer's verdict tuple — the
    /// loop described in [`Self::request_review`]'s doc, split out so the
    /// shadow launch above it reads as the one-line side effect it is.
    async fn await_primary_verdict(
        &self,
        entry: &LandingQueueEntry,
        gates: &GateConfig,
        instance_id: &str,
    ) -> rk_core::Result<ReviewWaitOutcome> {
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
                return Ok(ReviewWaitOutcome::CeilingReached {
                    instance_id: instance_id.to_string(),
                });
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
            // An operator's `cancel_active_review` settles out-of-band, from
            // a completely different call stack (an RPC handler, not this
            // loop) — the only way this loop can learn about it is by
            // polling for the settlement marker it left behind. Checked
            // before the liveness probe below so a cancellation that has
            // already dismissed the reviewer is reported as `Cancelled`,
            // never misread as an ordinary `ReviewerDied`.
            if self
                .review_ceiling_settlement(entry, instance_id)?
                .is_some()
            {
                return Ok(ReviewWaitOutcome::Cancelled {
                    instance_id: instance_id.to_string(),
                });
            }
            if let Some(instance) = self.engine.status_any(instance_id) {
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
        let mut outcome = outcome;
        let verdict = loop {
            match outcome {
                ReviewWaitOutcome::Verdict(v) => break v,
                ReviewWaitOutcome::ReviewerDied(context) => {
                    match self
                        .route_review_death(entry, gates, git_repo, &context)
                        .await?
                    {
                        ReviewDeathOutcome::Retry(next) => {
                            outcome = next;
                            continue;
                        }
                        ReviewDeathOutcome::Escalated(need) => {
                            return Ok(LandingOutcome::Escalated(need));
                        }
                    }
                }
                ReviewWaitOutcome::CeilingReached { instance_id } => {
                    self.settle_review_ceiling(entry, &instance_id, "review-wait-exhausted")
                        .await?;
                    return Ok(LandingOutcome::Escalated(self.review_human_gate(
                        entry,
                        git_repo,
                        "review-wait-exhausted",
                        format!(
                            "the reviewer was still running at the {}s hard wait ceiling",
                            gates.review_max_wait.as_secs()
                        ),
                        "inspect or stop the reviewer, then record a verdict or make the land decision",
                        Some(format!(
                            "to wait for a fresh review instead of forcing the land, rk \
                             reenqueue-review {} --repo {} --target {} --task {} --attempt {}",
                            entry.branch, entry.repo_path, entry.target, entry.task, instance_id
                        )),
                    )?));
                }
                ReviewWaitOutcome::Cancelled { instance_id } => {
                    // Idempotent: `cancel_active_review` already called this
                    // (that write is what the poll loop just discovered), so
                    // this is a no-op that returns the same marker.
                    self.settle_review_ceiling(entry, &instance_id, "operator-cancelled")
                        .await?;
                    return Ok(LandingOutcome::Escalated(self.review_human_gate(
                        entry,
                        git_repo,
                        "operator-cancelled",
                        "the review was explicitly cancelled by an operator".to_string(),
                        "decide whether to land as-is, reenqueue a fresh review, or abandon the branch",
                        Some(format!(
                            "to wait for a fresh review instead of forcing the land, rk \
                             reenqueue-review {} --repo {} --target {} --task {} --attempt {}",
                            entry.branch, entry.repo_path, entry.target, entry.task, instance_id
                        )),
                    )?));
                }
            }
        };
        match verdict.as_str() {
            "APPROVE" => {
                // Capture the review phase's own clock BEFORE the status
                // transition below moves the durable candidate into `Landing`
                // (`Phase::Merge`) — that transition resets
                // `phase_entered_at`, so reading it any later would silently
                // lose the review's elapsed time. The round is derived, not
                // assumed to be the first: an approval on resubmission after a
                // correction round would otherwise dedup away against that
                // round's span (see `Self::approved_review_attempt`).
                let _ = crate::span::record_phase_span(
                    &self.space,
                    &entry.repo_name,
                    "daemon",
                    &Self::timed_review_span(
                        &entry.task,
                        crate::span::Phase::SemanticReview,
                        self.approved_review_attempt(entry),
                        &entry.repo_name,
                        self.phase_started_at(entry),
                        Utc::now(),
                        "approved",
                    ),
                );
                self.queue.set_status(entry, LandingEntryStatus::Landing)?;
                match self
                    .advance_target(entry, entry.keep_branch, candidate)
                    .await?
                {
                    TargetAdvance::Landed(result) => self.finalize_landed(entry, result).await,
                    TargetAdvance::Stale(result) => {
                        self.requeue_stale(entry, git_repo, candidate, &result)
                    }
                    TargetAdvance::Blocked(result) => Ok(LandingOutcome::Escalated(
                        self.worktree_blocked_gate(entry, &result)?,
                    )),
                }
            }
            "REWORK" => self.route_rework(entry, git_repo).await,
            "STOP" => Ok(LandingOutcome::Escalated(self.review_human_gate(
                entry,
                git_repo,
                "reviewer-stop",
                "the reviewer returned STOP".into(),
                "decide whether to abandon the branch or explicitly override the STOP",
                None,
            )?)),
            other => Ok(LandingOutcome::Escalated(self.review_human_gate(
                entry,
                git_repo,
                "unknown-verdict",
                format!("the reviewer returned unrecognized verdict {other:?}"),
                "correct the review artifact to APPROVE, REWORK, or STOP, then resubmit",
                None,
            )?)),
        }
    }

    /// The durable ceiling-settlement marker for review `attempt` on
    /// `entry`, if [`Self::settle_review_ceiling`] has already run for it.
    /// `None` while the attempt is still open.
    fn review_ceiling_settlement(
        &self,
        entry: &LandingQueueEntry,
        attempt: &str,
    ) -> rk_core::Result<Option<Tuple>> {
        let pattern = Pattern::category(Category::Event)
            .identity(REVIEW_CEILING_SETTLED_IDENTITY)
            .scope(&entry.repo_name);
        Ok(self.space.scan(&pattern)?.into_iter().find(|t| {
            t.payload.get("task").and_then(Value::as_str) == Some(entry.task.as_str())
                && t.payload.get("branch").and_then(Value::as_str) == Some(entry.branch.as_str())
                && t.payload.get("target").and_then(Value::as_str) == Some(entry.target.as_str())
                && t.payload.get("attempt").and_then(Value::as_str) == Some(attempt)
        }))
    }

    /// Fence a review wait that reached its workflow ceiling
    /// (`ReviewWaitOutcome::CeilingReached`) — or was explicitly cancelled —
    /// with the reviewer still live (module doc, parent incident
    /// 2026-08-21: a Codex reviewer stayed `Running`/reconnecting well after
    /// its owning steward-review workflow had already timed out). Two
    /// things a live-at-ceiling hold must do that a dead-reviewer hold
    /// (`route_review_death`) does not need to, because there the reviewer
    /// is already gone:
    ///
    ///  - release the fleet capacity the still-live reviewer is holding,
    ///    via [`Supervisor::dismiss_live_instance_agents`] (the terminal-only
    ///    `dismiss_orphaned_instance_agents` cannot touch it — it filters to
    ///    `Completed`/`Failed` on purpose);
    ///  - settle the attempt exactly once: a repeat call for the SAME
    ///    `attempt` (a daemon restart replaying the same routing pass, or a
    ///    duplicate `route_verdict_prepared` call) finds the existing
    ///    marker and does nothing further, so a still-reconnecting harness
    ///    is never dismissed twice and the marker is never duplicated.
    ///
    /// A later verdict from `attempt` can still arrive (the harness may
    /// finish its in-flight turn before the kill lands, or was already past
    /// the point of no return) — that is handled separately by
    /// [`Self::retain_late_review_evidence`], never by this function.
    async fn settle_review_ceiling(
        &self,
        entry: &LandingQueueEntry,
        attempt: &str,
        reason: &str,
    ) -> rk_core::Result<Tuple> {
        if let Some(existing) = self.review_ceiling_settlement(entry, attempt)? {
            return Ok(existing);
        }
        let dismissed = self.supervisor.dismiss_live_instance_agents(attempt).await;
        // The one genuinely non-atomic window in this function: the reviewer
        // is already dismissed (irreversible — its OS process is gone) but
        // nothing durable records that the attempt was settled. A daemon
        // that dies here leaves the candidate still `awaiting_review` with
        // no reviewer behind it, which is exactly the state a successor must
        // converge out of without orphaning or duplicating anything. See
        // `crate::fault` for why this is a barrier and not a sleep.
        crate::fault::barrier(&self.layout, BARRIER_CEILING_PRE_MARKER).await;
        let released: Vec<&str> = dismissed
            .iter()
            .filter(|(_, ok)| *ok)
            .map(|(name, _)| name.as_str())
            .collect();
        let failed = dismissed.len() - released.len();
        let marker = Tuple::new(
            Category::Event,
            entry.repo_name.clone(),
            REVIEW_CEILING_SETTLED_IDENTITY,
            "daemon",
            json!({
                "branch": entry.branch,
                "head_sha": entry.head_sha,
                "target": entry.target,
                "task": entry.task,
                "attempt": attempt,
                "reason": reason,
                "released_agents": released,
                "settled_at": Utc::now().to_rfc3339(),
            }),
        )
        .with_lifecycle(Lifecycle::Furniture);
        self.space.out(marker.clone())?;
        // The mirror window: the settlement is now durable, but the caller
        // that asked for it (an operator's `repo.land.cancel_review`, or the
        // ceiling path's own routing pass) has not yet seen it succeed. A
        // daemon that dies here must leave a successor refusing the retry
        // the operator will naturally make, not settling a second time.
        crate::fault::barrier(&self.layout, BARRIER_CEILING_POST_MARKER).await;
        info!(
            repo = %entry.repo_name, branch = %entry.branch, attempt, reason,
            released = released.len(), failed,
            "landing pipeline: review ceiling settled; released reviewer capacity"
        );
        Ok(marker)
    }

    /// Capture a verdict that arrives for an already-ceiling-settled review
    /// attempt as durable evidence — branch, head, attempt, and the
    /// generation (the dismissed agent names [`Self::settle_review_ceiling`]
    /// recorded) — WITHOUT ever treating it as the landing decision: by the
    /// time settlement exists the candidate's own queue entry is already
    /// terminal (`mark_processed` already ran), so this never touches
    /// `LandingQueue` or re-decides anything. Idempotent per `(attempt,
    /// head_sha)` — a repeat call (a restart, a second late delivery of the
    /// same tuple) finds the evidence already recorded and does nothing.
    ///
    /// Deliberately scoped to a SPECIFIC (dead) `attempt` rather than
    /// [`Self::active_review_attempt`]'s current one — the entire point is
    /// to find a verdict [`Self::cached_verdict`] would correctly refuse to
    /// read as authoritative.
    pub(crate) fn retain_late_review_evidence(
        &self,
        entry: &LandingQueueEntry,
        attempt: &str,
    ) -> rk_core::Result<Option<Tuple>> {
        let Some(settlement) = self.review_ceiling_settlement(entry, attempt)? else {
            return Ok(None);
        };
        let already = Pattern::category(Category::Artifact)
            .identity(LATE_REVIEW_EVIDENCE_IDENTITY)
            .scope(&entry.repo_name);
        if self.space.scan(&already)?.into_iter().any(|t| {
            t.payload.get("attempt").and_then(Value::as_str) == Some(attempt)
                && t.payload.get("head_sha").and_then(Value::as_str)
                    == Some(entry.head_sha.as_str())
        }) {
            return Ok(None);
        }
        let mut pattern = Pattern::category(Category::Artifact)
            .identity(REVIEW_ARTIFACT_IDENTITY)
            .scope(&entry.repo_name);
        pattern.payload_search = Some(format!("\"review_attempt\":\"{attempt}\""));
        let Some(verdict) = self.space.scan(&pattern)?.into_iter().find(|t| {
            t.payload.get("task").and_then(Value::as_str) == Some(entry.task.as_str())
                && t.payload.get("head_sha").and_then(Value::as_str)
                    == Some(entry.head_sha.as_str())
                && t.payload.get("branch").and_then(Value::as_str) == Some(entry.branch.as_str())
                && t.payload.get("review_attempt").and_then(Value::as_str) == Some(attempt)
        }) else {
            return Ok(None);
        };
        let evidence = Tuple::new(
            Category::Artifact,
            entry.repo_name.clone(),
            LATE_REVIEW_EVIDENCE_IDENTITY,
            "daemon",
            json!({
                "branch": entry.branch,
                "head_sha": entry.head_sha,
                "target": entry.target,
                "task": entry.task,
                "attempt": attempt,
                "generation": settlement.payload.get("released_agents").cloned().unwrap_or(json!([])),
                "recommendation": verdict.payload.get("recommendation"),
                "retained_at": Utc::now().to_rfc3339(),
            }),
        )
        .with_lifecycle(Lifecycle::Furniture);
        self.space.out(evidence.clone())?;
        info!(
            repo = %entry.repo_name, branch = %entry.branch, attempt,
            recommendation = ?verdict.payload.get("recommendation"),
            "landing pipeline: retained a late review verdict as evidence; landing decision \
             unchanged"
        );
        Ok(Some(evidence))
    }

    /// Explicit, bounded re-enqueue: dispatch exactly one fresh review
    /// attempt for a candidate whose prior attempt was ceiling-settled
    /// ([`Self::settle_review_ceiling`]). Requires settlement to already
    /// exist — there is nothing to re-enqueue while the original wait is
    /// still live or was never fenced — and is idempotent per settled
    /// attempt: a second call finds the [`REVIEW_CEILING_REENQUEUE_IDENTITY`]
    /// marker this write leaves behind and returns the SAME new attempt id
    /// rather than dispatching a second replacement reviewer.
    ///
    /// The marker is written BEFORE dispatch, mirroring
    /// `route_review_death`'s "marker before dispatch" ordering: a crash in
    /// the gap just costs a resumed caller re-reading the same marker back
    /// (`dispatch_review` is itself idempotent per instance id), never a
    /// duplicate dispatch.
    pub(crate) async fn reenqueue_after_ceiling(
        &self,
        entry: &LandingQueueEntry,
        gates: &GateConfig,
        settled_attempt: &str,
    ) -> rk_core::Result<String> {
        let settlement = self
            .review_ceiling_settlement(entry, settled_attempt)?
            .ok_or_else(|| {
                rk_core::Error::other(format!(
                    "cannot re-enqueue review for {} on {}: attempt {settled_attempt} was never \
                     ceiling-settled",
                    entry.branch, entry.task
                ))
            })?;
        if let Some(existing) = self.review_ceiling_reenqueue_marker(entry, settled_attempt)? {
            return required_payload_str(&existing.payload, "new_attempt", "reenqueue marker")
                .map(str::to_string);
        }
        let new_attempt = format!("{settled_attempt}-reenqueue");
        self.space.out(
            Tuple::new(
                Category::Event,
                entry.repo_name.clone(),
                REVIEW_CEILING_REENQUEUE_IDENTITY,
                "daemon",
                json!({
                    "branch": entry.branch,
                    "head_sha": entry.head_sha,
                    "target": entry.target,
                    "task": entry.task,
                    "settled_attempt": settled_attempt,
                    "new_attempt": new_attempt,
                    "settled_reason": settlement.payload.get("reason"),
                }),
            )
            .with_lifecycle(Lifecycle::Furniture),
        )?;
        self.dispatch_review(entry, gates, &new_attempt)?;
        info!(
            repo = %entry.repo_name, branch = %entry.branch, settled_attempt, new_attempt,
            "landing pipeline: explicit re-enqueue dispatched one fresh review attempt"
        );
        Ok(new_attempt)
    }

    /// Operator-facing wrapper around [`Self::reenqueue_after_ceiling`] for
    /// the `repo.land.reenqueue` RPC (`rk reenqueue-review`). The RPC caller
    /// only has the branch/target/task identifiers and the settled attempt
    /// id an escalation text handed them — not a `LandingQueueEntry`, whose
    /// `head_sha` is recovered from the ceiling-settlement marker itself
    /// (mirroring [`Self::synthetic_conflict_entry`]) rather than requiring
    /// the caller to know it.
    pub(crate) async fn reenqueue_ceiling_settled_review(
        &self,
        repo_path: &Path,
        branch: &str,
        target: &str,
        task: &str,
        settled_attempt: &str,
    ) -> rk_core::Result<String> {
        let git_repo = rk_git::Repo::discover(repo_path)?;
        let repo_name = git_repo.name();
        let lookup = LandingQueueEntry {
            repo_name: repo_name.clone(),
            branch: branch.to_string(),
            target: target.to_string(),
            task: task.to_string(),
            ..Default::default()
        };
        let settlement = self
            .review_ceiling_settlement(&lookup, settled_attempt)?
            .ok_or_else(|| {
                rk_core::Error::other(format!(
                    "cannot re-enqueue review for {branch} on {repo_name}: attempt \
                     {settled_attempt} was never ceiling-settled"
                ))
            })?;
        let head_sha = settlement
            .payload
            .get("head_sha")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let entry = LandingQueueEntry {
            repo_name,
            repo_path: repo_path.display().to_string(),
            branch: branch.to_string(),
            target: target.to_string(),
            head_sha,
            task: task.to_string(),
            ..Default::default()
        };
        let gates = self.gate_config(&git_repo)?;
        self.reenqueue_after_ceiling(&entry, &gates, settled_attempt)
            .await
    }

    /// Find the durably-queued entry for `(branch, target, task)`, if the
    /// candidate is still in the queue in any status — used by
    /// [`Self::cancel_active_review`] to recover the real `head_sha` an
    /// operator RPC caller cannot know (they only have the identifiers a
    /// human can type). Unlike [`Self::reenqueue_ceiling_settled_review`],
    /// which recovers `head_sha` from an existing settlement marker, cancel
    /// runs BEFORE any settlement exists for this attempt, so the live
    /// queue entry is the only durable source left.
    fn queued_entry_for(
        &self,
        repo_name: &str,
        branch: &str,
        target: &str,
        task: &str,
    ) -> rk_core::Result<Option<LandingQueueEntry>> {
        for tuple in self.queue.scan_current(repo_name, Some(target))? {
            if tuple.payload.get("branch").and_then(Value::as_str) == Some(branch)
                && tuple.payload.get("task").and_then(Value::as_str) == Some(task)
            {
                let entry: LandingQueueEntry = serde_json::from_value(tuple.payload.clone())
                    .map_err(|e| rk_core::Error::other(format!("landing queue entry: {e}")))?;
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    /// Operator-facing cancellation for the `repo.land.cancel_review` RPC
    /// (`rk cancel-review`): fence the CURRENTLY active review attempt for
    /// `(branch, target, task)` through [`Self::settle_review_ceiling`] —
    /// the exact same durable settlement, live-capacity release, and
    /// exactly-once guarantee a ceiling timeout gets, just triggered
    /// explicitly instead of by the wall-clock deadline. A still-in-flight
    /// [`Self::await_primary_verdict`] poll loop for this same attempt
    /// discovers the settlement this call just wrote on its next slice and
    /// exits with `ReviewWaitOutcome::Cancelled`; its own call back into
    /// `settle_review_ceiling` is then a no-op that returns the SAME
    /// marker, never a second dismissal or a second write. A late verdict
    /// that still arrives afterward is retained as evidence
    /// ([`Self::retain_late_review_evidence`]), never treated as the
    /// landing decision.
    ///
    /// Refuses (rather than silently no-op or guess) in two cases:
    ///
    ///  - no candidate for `(branch, target, task)` is currently in the
    ///    durable queue at all — unlike [`Self::reenqueue_ceiling_settled_review`],
    ///    which can recover `head_sha` from an existing settlement marker,
    ///    cancel runs BEFORE any settlement exists, so the live queue entry
    ///    is the only durable source of the real `head_sha`. Guessing it
    ///    (e.g. defaulting to empty) would compute the WRONG attempt id —
    ///    [`review_instance_id`] hashes `head_sha` in — and silently settle
    ///    a phantom attempt that matches no live reviewer, returning success
    ///    while cancelling nothing;
    ///  - [`Self::active_review_attempt`] returns a `*-settled` sentinel
    ///    once the attempt is already fenced by either settlement path
    ///    (ceiling or review-death), which this rejects up front so a
    ///    repeat or late cancel call is never mistaken for having cancelled
    ///    anything.
    pub(crate) async fn cancel_active_review(
        &self,
        repo_path: &Path,
        branch: &str,
        target: &str,
        task: &str,
    ) -> rk_core::Result<Tuple> {
        let git_repo = rk_git::Repo::discover(repo_path)?;
        let repo_name = git_repo.name();
        let entry = self
            .queued_entry_for(&repo_name, branch, target, task)?
            .ok_or_else(|| {
                rk_core::Error::other(format!(
                    "cannot cancel review for {branch} on {repo_name}: no candidate currently \
                     in the landing queue for task {task}"
                ))
            })?;
        let attempt = self.active_review_attempt(&entry)?;
        if attempt.ends_with("-settled") {
            return Err(rk_core::Error::other(format!(
                "cannot cancel review for {branch} on {repo_name}: no active review attempt \
                 (already settled)"
            )));
        }
        self.settle_review_ceiling(&entry, &attempt, "operator-cancelled")
            .await
    }

    /// Live reconciliation: scan every durable `REVIEW_CEILING_SETTLED_IDENTITY`
    /// marker across all repos and retain any late-arriving verdict for it as
    /// durable evidence via [`Self::retain_late_review_evidence`]. Meant to
    /// run on the same periodic tick as [`Self::run_cycle`] (see `Server`'s
    /// landing background loop) — restart-safe and idempotent, since
    /// `retain_late_review_evidence` itself is idempotent per `(attempt,
    /// head_sha)`: re-scanning the same settled markers on every tick, or
    /// after a daemon restart, only ever picks up evidence not already
    /// retained. Never touches `LandingQueue` or re-decides a landing
    /// outcome — settlement markers name attempts whose candidate is already
    /// terminal.
    pub(crate) fn reconcile_late_review_evidence(&self) -> rk_core::Result<usize> {
        let markers = self
            .space
            .scan(&Pattern::category(Category::Event).identity(REVIEW_CEILING_SETTLED_IDENTITY))?;
        let mut retained = 0;
        for marker in markers {
            let (Some(branch), Some(target), Some(task), Some(attempt)) = (
                marker.payload.get("branch").and_then(Value::as_str),
                marker.payload.get("target").and_then(Value::as_str),
                marker.payload.get("task").and_then(Value::as_str),
                marker.payload.get("attempt").and_then(Value::as_str),
            ) else {
                continue;
            };
            let entry = LandingQueueEntry {
                repo_name: marker.scope.clone(),
                branch: branch.to_string(),
                target: target.to_string(),
                task: task.to_string(),
                head_sha: marker
                    .payload
                    .get("head_sha")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
                ..Default::default()
            };
            if self.retain_late_review_evidence(&entry, attempt)?.is_some() {
                retained += 1;
            }
        }
        Ok(retained)
    }

    fn review_human_gate(
        &self,
        entry: &LandingQueueEntry,
        git_repo: &rk_git::Repo,
        code: &str,
        detail: String,
        decision: &str,
        extra_resolve: Option<String>,
    ) -> rk_core::Result<Tuple> {
        let stat = git_repo.diff_stat(&entry.target, &entry.branch)?;
        let notes = self
            .review_artifact(entry)?
            .as_ref()
            .map(|artifact| landing_rework::notes(Some(artifact)))
            .filter(|notes| !notes.is_empty())
            .unwrap_or_else(|| "(none recorded)".to_string());
        let extra_resolve = extra_resolve
            .map(|line| format!("\nOR: {line}"))
            .unwrap_or_default();
        self.escalate(
            entry,
            format!(
                "steward: review of {} for {} requires a human ({code}) — branch held unmerged.\n\
                 EVIDENCE: exact reviewed head {}; {detail}. Reviewer notes: {notes}\n\
                 DECISION NEEDED: {decision}\n\
                 BLAST RADIUS: {} file(s) / {} line(s) on {}, held back from {}. Nothing merged.\n\
                 RESOLVE WITH: rk land {} --repo {} --target {} --task {} --force --reason \
                 'human resolved {code}'{extra_resolve}",
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

    /// A tested-and-approved candidate that could not land because `target`
    /// is checked out somewhere (an operator's or an agent's own worktree)
    /// and refused the fast-forward — a dirty index/working tree, or local
    /// edits `git merge --ff-only` cannot reconcile
    /// ([`rk_git::AdvanceOutcome::Blocked`]). The ref was never moved and
    /// `candidate` is still parked under its ref: this is the fail-closed
    /// recovery gate (rather than a silent ref move that would strand that
    /// checkout's index against the new HEAD, or an unattended reset that
    /// could discard genuine work).
    fn worktree_blocked_gate(
        &self,
        entry: &LandingQueueEntry,
        result: &Value,
    ) -> rk_core::Result<Tuple> {
        let worktree_path = result
            .get("worktree_path")
            .and_then(Value::as_str)
            .unwrap_or("(unknown)");
        let detail = result
            .get("detail")
            .and_then(Value::as_str)
            .unwrap_or("fast-forward refused");
        self.escalate(
            entry,
            format!(
                "steward: {} for {} passed gates and review but could not land onto {} \
                 — branch held unmerged, target ref untouched.\n\
                 EVIDENCE: {} is checked out at {worktree_path}, which refused a \
                 fast-forward onto the tested merge {}: {detail}\n\
                 DECISION NEEDED: inspect {worktree_path} for genuine uncommitted work \
                 (commit or stash it), then let this land retry — do not discard \
                 anything there without checking it first.\n\
                 BLAST RADIUS: nothing merged; the tested candidate is still parked and \
                 will be reused once the worktree is clean.\n\
                 RESOLVE WITH: once {worktree_path} is clean, rk land {} --repo {} \
                 --target {} --task {} --force --reason 'worktree at {worktree_path} resolved'",
                entry.branch,
                entry.task,
                entry.target,
                entry.target,
                result
                    .get("tested_sha")
                    .and_then(Value::as_str)
                    .unwrap_or("?"),
                entry.branch,
                entry.repo_path,
                entry.target,
                entry.task,
            ),
        )
    }

    /// Resolve one `ReviewWaitOutcome::ReviewerDied`: dispatch exactly one
    /// replacement reviewer against the SAME exact branch/head/target/task
    /// bound by `entry`, or withhold and raise the one durable human
    /// escalation. Bounded by the repository's activated
    /// [`ReviewDeathPolicy`] (attempt count and cumulative USD, both durable
    /// across restarts via [`REVIEW_DEATH_DISPATCH_IDENTITY`] markers) — the
    /// same fail-closed, evidence-rich shape as [`Self::route_rework`], just
    /// without a verdict to classify: a death carries no reviewer notes, so
    /// the only questions are policy ones (see `landing_review_retry` module
    /// doc).
    async fn route_review_death(
        &self,
        entry: &LandingQueueEntry,
        gates: &GateConfig,
        git_repo: &rk_git::Repo,
        death_context: &str,
    ) -> rk_core::Result<ReviewDeathOutcome> {
        let ctx = ReviewDeathContext {
            repo: entry.repo_name.clone(),
            repo_path: entry.repo_path.clone(),
            branch: entry.branch.clone(),
            head_sha: entry.head_sha.clone(),
            target: entry.target.clone(),
            task: entry.task.clone(),
        };
        // Replay guard, independent of `process_entry`'s outer work-key dedup
        // (module doc): a marker whose state is neither "dispatching" nor
        // "dispatched" is a withhold code already recorded for this EXACT
        // dispatch key — this chain already reached its final decision.
        // Re-deciding would re-escalate a second, duplicate `need` for the
        // same hold; converge on the existing one instead.
        if let Some(settled) = self.review_death_settled_marker(&ctx)? {
            info!(
                repo = %entry.repo_name, branch = %entry.branch, head_sha = %entry.head_sha,
                code = %settled.payload["state"],
                "landing pipeline: review-death chain already routed for this exact candidate; \
                 not re-escalating"
            );
            return Ok(ReviewDeathOutcome::Escalated(settled));
        }
        let attempts_used = self.review_death_attempts_used(&ctx)?;
        let reviewed_tip = match git_repo.rev_parse(&entry.branch) {
            Ok(tip) => Some(tip),
            Err(error) => {
                let withheld = landing_review_retry::Withheld {
                    code: "reviewed-head-unavailable",
                    detail: format!(
                        "the reviewed branch {} could not be resolved while preparing a retry: \
                         {error}",
                        entry.branch
                    ),
                    decision: "restore the reviewed branch or explicitly re-submit its current \
                               head for a fresh review, rather than retrying an unknown tree"
                        .into(),
                };
                let need = self.escalate(entry, ctx.escalation(&withheld, death_context))?;
                self.record_review_death_state(
                    entry,
                    &ctx,
                    attempts_used,
                    None,
                    withheld.code,
                    None,
                )?;
                return Ok(ReviewDeathOutcome::Escalated(need));
            }
        };
        if reviewed_tip.as_deref() != Some(entry.head_sha.as_str()) {
            let actual = reviewed_tip.as_deref().unwrap_or("<unavailable>");
            let withheld = landing_review_retry::Withheld {
                code: "reviewed-head-moved",
                detail: format!(
                    "the reviewed head {} is no longer {}'s tip (now {actual}), so the \
                     replacement would inspect work the dead reviewer never reviewed",
                    entry.head_sha, entry.branch
                ),
                decision: "re-review the branch at its current tip, or restore it to the exact \
                           reviewed head before retrying"
                    .into(),
            };
            let need = self.escalate(entry, ctx.escalation(&withheld, death_context))?;
            self.record_review_death_state(entry, &ctx, attempts_used, None, withheld.code, None)?;
            return Ok(ReviewDeathOutcome::Escalated(need));
        }
        let policy =
            ReviewDeathPolicy::from_landing(&self.supervisor.repository_policy(git_repo)?.landing);
        let spent_usd = self.review_death_chain_spend(entry);
        match landing_review_retry::route(&policy, attempts_used, spent_usd) {
            ReviewDeathRoute::Withhold(withheld) => {
                let need = self.escalate(entry, ctx.escalation(&withheld, death_context))?;
                self.record_review_death_state(
                    entry,
                    &ctx,
                    attempts_used,
                    None,
                    withheld.code,
                    None,
                )?;
                warn!(
                    repo = %entry.repo_name, branch = %entry.branch, head_sha = %entry.head_sha,
                    code = withheld.code, attempts_used,
                    "landing pipeline: review-death retry withheld; escalated to a human gate"
                );
                Ok(ReviewDeathOutcome::Escalated(need))
            }
            ReviewDeathRoute::Dispatch { attempt } => {
                let instance_id = review_retry_instance_id(entry, attempt);
                // Backoff (module doc on `landing_review_retry::ReviewDeathBackoffPolicy`):
                // chosen ONCE, right here, and made durable in the SAME
                // "dispatching" marker below — before restart-safety, this
                // was the only marker write on this path, and now the
                // schedule rides along with it rather than needing a
                // separate durable record. Every later reader (a restart's
                // resume through `Self::review_verdict`, or a duplicate
                // routing of the same dead review) reads this persisted
                // `not_before` back rather than drawing its own jitter, so
                // duplicates always converge on one schedule.
                let backoff_policy = landing_review_retry::ReviewDeathBackoffPolicy::from_landing(
                    &self.supervisor.repository_policy(git_repo)?.landing,
                );
                let delay = landing_review_retry::retry_delay(
                    &backoff_policy,
                    attempt,
                    self.retry_schedule.jitter_unit(),
                );
                let not_before = self.retry_schedule.now()
                    + chrono::Duration::from_std(delay)
                        .unwrap_or_else(|_| chrono::Duration::zero());
                // Marker before dispatch: a crash between this write and the
                // spawn call below just costs one budget slot on resume
                // (the next `route_review_death` counts this marker as
                // "used" and moves straight to the next attempt) rather than
                // risking a duplicate — `dispatch_review` is idempotent per
                // `instance_id` anyway, so resuming this exact attempt after
                // a crash is ALSO safe, just not required for correctness.
                self.record_review_death_state(
                    entry,
                    &ctx,
                    attempt,
                    Some(&instance_id),
                    "dispatching",
                    Some(not_before),
                )?;
                info!(
                    repo = %entry.repo_name, branch = %entry.branch, head_sha = %entry.head_sha,
                    attempt, instance_id = %instance_id, delay_secs = delay.as_secs(),
                    "landing pipeline: reviewer died before a verdict; scheduling a replacement \
                     reviewer against the same exact head"
                );
                let outcome = self
                    .await_review_retry_after_backoff(entry, gates, &instance_id, Some(not_before))
                    .await?;
                self.record_review_death_state(
                    entry,
                    &ctx,
                    attempt,
                    Some(&instance_id),
                    "dispatched",
                    None,
                )?;
                Ok(ReviewDeathOutcome::Retry(outcome))
            }
        }
    }

    /// The real-time wait still owed before `not_before`, measured against
    /// `now` — `None` for "no wait", whether because there is no schedule at
    /// all or because it has already elapsed. Kept as a pure function of its
    /// inputs — `now` is supplied by the caller from [`RetrySchedule`], the
    /// same split `landing_review_retry::retry_delay` makes for jitter — so
    /// the elapsed/not-yet-elapsed decision, the exact property
    /// [`Self::await_review_retry_after_backoff`] must get right, is
    /// deterministically unit-testable without spawning a task, a workflow,
    /// or touching the clock.
    fn remaining_backoff(
        not_before: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Option<Duration> {
        let not_before = not_before?;
        (not_before > now).then(|| (not_before - now).to_std().unwrap_or(Duration::ZERO))
    }

    /// Wait out a review-death retry's durable backoff schedule (module doc
    /// on `landing_review_retry::ReviewDeathBackoffPolicy`), if any, then
    /// dispatch/resume the replacement reviewer. `not_before` is `None` for
    /// a marker seeded before this policy existed, or absent entirely (the
    /// primary reviewer path never calls this) — either way that means "no
    /// wait", not an error. Sleeping here blocks only THIS candidate's own
    /// async task: `LandingPipeline::run_cycle` already runs every
    /// `(repo, target)` key on its own task, and within a key this candidate
    /// already holds exclusive processing (`drain_key`'s `key_lock`), so a
    /// pending backoff here was already going to hold that lock regardless —
    /// it never blocks an unrelated repo's or target's queue.
    async fn await_review_retry_after_backoff(
        &self,
        entry: &LandingQueueEntry,
        gates: &GateConfig,
        instance_id: &str,
        not_before: Option<DateTime<Utc>>,
    ) -> rk_core::Result<ReviewWaitOutcome> {
        if let Some(wait) = Self::remaining_backoff(not_before, self.retry_schedule.now()) {
            info!(
                repo = %entry.repo_name, branch = %entry.branch, instance_id = %instance_id,
                wait_secs = wait.as_secs(),
                "landing pipeline: waiting out the review-death retry backoff before \
                 dispatching the replacement reviewer"
            );
            self.retry_schedule.sleep(wait).await;
        }
        self.request_review_retry(entry, gates, instance_id).await
    }

    /// Every review-death retry marker for this exact candidate — scoped to
    /// the full `(repo, branch, head_sha, target, task)` dispatch key, unlike
    /// [`Self::rework_dispatch_markers`]'s branch/target/task scoping: a
    /// review-death chain never survives a moved head (a new head is a new
    /// candidate, entered fresh from [`LandingPipeline::process_entry`], not
    /// a continuation of this one), so there is no separate "exact-head
    /// replay" case to fold in.
    fn review_death_dispatch_markers(
        &self,
        ctx: &ReviewDeathContext,
    ) -> rk_core::Result<Vec<Tuple>> {
        let pattern = Pattern::category(Category::Event)
            .identity(REVIEW_DEATH_DISPATCH_IDENTITY)
            .scope(&ctx.repo);
        let key = ctx.dispatch_key();
        Ok(self
            .space
            .scan(&pattern)?
            .into_iter()
            .filter(|t| t.payload.get("dispatch_key").and_then(Value::as_str) == Some(key.as_str()))
            .collect())
    }

    fn record_review_death_state(
        &self,
        entry: &LandingQueueEntry,
        ctx: &ReviewDeathContext,
        attempt: u32,
        instance_id: Option<&str>,
        state: &str,
        not_before: Option<DateTime<Utc>>,
    ) -> rk_core::Result<Tuple> {
        let marker = Tuple::new(
            Category::Event,
            entry.repo_name.clone(),
            REVIEW_DEATH_DISPATCH_IDENTITY,
            "daemon",
            ctx.marker_payload(attempt, instance_id.unwrap_or_default(), state, not_before),
        )
        .with_lifecycle(Lifecycle::Furniture);
        self.space.out(marker.clone())?;
        Ok(marker)
    }

    /// The durable backoff schedule (module doc on
    /// `landing_review_retry::ReviewDeathBackoffPolicy`) chosen for
    /// `instance_id`'s dispatch, if this exact candidate has one recorded —
    /// `None` for the primary reviewer (never scheduled) or a marker seeded
    /// before this policy existed, both of which must read back as "no
    /// wait". Only the "dispatching" marker for an attempt ever carries
    /// `not_before` (`Self::route_review_death`'s Dispatch arm), so this
    /// scans every marker for `instance_id` rather than just the latest one.
    fn review_death_not_before(
        &self,
        entry: &LandingQueueEntry,
        instance_id: &str,
    ) -> rk_core::Result<Option<DateTime<Utc>>> {
        let ctx = ReviewDeathContext {
            repo: entry.repo_name.clone(),
            repo_path: entry.repo_path.clone(),
            branch: entry.branch.clone(),
            head_sha: entry.head_sha.clone(),
            target: entry.target.clone(),
            task: entry.task.clone(),
        };
        Ok(self
            .review_death_dispatch_markers(&ctx)?
            .into_iter()
            .filter(|marker| {
                marker.payload.get("instance_id").and_then(Value::as_str) == Some(instance_id)
            })
            .find_map(|marker| {
                marker
                    .payload
                    .get("not_before")
                    .and_then(Value::as_str)
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.with_timezone(&Utc))
            }))
    }

    /// The withhold-code marker already recorded for this EXACT dispatch
    /// key, if this candidate's review-death chain already reached its final
    /// decision — a state outside `{"dispatching", "dispatched"}`, i.e. one
    /// of [`landing_review_retry::route`]'s withhold codes. `None` while the
    /// chain is still open (no marker yet, or its most recent attempt is
    /// still in flight).
    fn review_death_settled_marker(
        &self,
        ctx: &ReviewDeathContext,
    ) -> rk_core::Result<Option<Tuple>> {
        Ok(self
            .review_death_dispatch_markers(ctx)?
            .into_iter()
            .find(|marker| {
                !matches!(
                    marker.payload.get("state").and_then(Value::as_str),
                    Some("dispatching" | "dispatched")
                )
            }))
    }

    /// Distinct retry ordinals actually dispatched (or in flight) for this
    /// exact candidate — the attempt-cap counter [`landing_review_retry::route`]
    /// checks. Counts `attempt` numbers, not marker tuples: `"dispatching"`
    /// and `"dispatched"` both mark the SAME attempt at two points in its
    /// life, so counting tuples would double-count every settled retry.
    fn review_death_attempts_used(&self, ctx: &ReviewDeathContext) -> rk_core::Result<u32> {
        let distinct: BTreeSet<u32> = self
            .review_death_dispatch_markers(ctx)?
            .into_iter()
            .filter(|marker| {
                matches!(
                    marker.payload.get("state").and_then(Value::as_str),
                    Some("dispatching" | "dispatched")
                )
            })
            .filter_map(|marker| marker.payload.get("attempt").and_then(Value::as_u64))
            .map(|attempt| attempt as u32)
            .collect();
        Ok(distinct.len() as u32)
    }

    /// Cumulative USD spent by the primary reviewer plus every review-death
    /// replacement in this candidate's chain — every agent whose own
    /// `review.attempt` is [`review_instance_id`]'s primary id or one of its
    /// [`review_retry_instance_id`] descendants. Joined by that attempt id
    /// (not the dispatch markers) so a terminated-and-archived reviewer's
    /// spend still counts, the same join `Self::await_shadow_comparison`
    /// relies on.
    fn review_death_chain_spend(&self, entry: &LandingQueueEntry) -> f64 {
        let primary = review_instance_id(entry);
        let retry_prefix = format!("{primary}-retry");
        self.supervisor
            .list_all()
            .iter()
            .filter(|record| {
                record.review.as_ref().is_some_and(|review| {
                    review.attempt == primary || review.attempt.starts_with(&retry_prefix)
                })
            })
            .map(|record| record.cost_usd)
            .sum()
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

    /// The sole prepared target-advance implementation. Every ordinary,
    /// workflow, automatic, and batch merge reaches this method after its
    /// CUE plan passes; mode-specific callers only decide how to report or
    /// requeue the classified result.
    async fn advance_target(
        &self,
        entry: &LandingQueueEntry,
        keep_branch: bool,
        candidate: &rk_git::PreparedMerge,
    ) -> rk_core::Result<TargetAdvance> {
        self.note_non_main_land_target(entry);
        let result = self
            .supervisor
            .land_prepared(
                Path::new(&entry.repo_path),
                &entry.branch,
                &entry.target,
                keep_branch,
                candidate,
            )
            .await?;
        if result.get("stale").and_then(Value::as_bool) == Some(true) {
            Ok(TargetAdvance::Stale(result))
        } else if result.get("blocked").and_then(Value::as_bool) == Some(true) {
            Ok(TargetAdvance::Blocked(result))
        } else {
            Ok(TargetAdvance::Landed(result))
        }
    }

    /// The sole successful-land finalization transition. Delivery facts,
    /// generation-fenced ticket closure, and the terminal landing outcome
    /// cannot drift apart because all successful paths pass through here.
    async fn finalize_landed(
        &self,
        entry: &LandingQueueEntry,
        result: Value,
    ) -> rk_core::Result<LandingOutcome> {
        self.record_delivery(entry, &result).await?;
        Ok(LandingOutcome::Landed(result))
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
    /// The merge already happened and is durable in git, so a failure here is
    /// propagated specifically to KEEP the durable `Landing` queue entry. A
    /// later pass recovers from that receipt and retries this idempotent
    /// finalization instead of marking an incomplete delivery processed.
    ///
    /// Stack-neutral by construction: the record is a merge commit sha plus
    /// the branch and target it landed on. No build tooling is consulted and
    /// no language convention is assumed.
    async fn record_delivery(
        &self,
        entry: &LandingQueueEntry,
        result: &Value,
    ) -> rk_core::Result<()> {
        if result.get("content_free").and_then(Value::as_bool) == Some(true) {
            info!(
                task = %entry.task,
                branch = %entry.branch,
                "land added nothing over target; not recording a delivery"
            );
            return Ok(());
        }
        let Some(merge_commit) = result
            .get("merge_commit")
            .and_then(Value::as_str)
            .filter(|c| !c.is_empty())
        else {
            return Ok(());
        };
        // A non-ticket candidate (a bare named-branch land the reactor picked
        // up with no `--task`) still owns an agent generation whose merge
        // pointer must be derived — only the ticket-side write is skipped.
        let is_ticket = entry.task.starts_with(crate::tickets::ID_PREFIX);
        let landed_at = Utc::now();
        let record = crate::tickets::DeliveryRecord {
            merge_commit: merge_commit.to_string(),
            branch: entry.branch.clone(),
            target: entry.target.clone(),
            landed_at: landed_at.to_rfc3339(),
        };
        if is_ticket {
            let _ = crate::span::record_phase_span(
                &self.space,
                &entry.repo_name,
                "daemon",
                &crate::span::PhaseSpan::new(&entry.task, crate::span::Phase::Merge)
                    .repo(&entry.repo_name)
                    .target(&entry.target)
                    .candidate(merge_commit)
                    .ended_at(landed_at),
            );
        }
        if let Err(error) = self
            .supervisor
            .finalize_delivery(
                std::path::Path::new(&entry.repo_path),
                &entry.repo_name,
                is_ticket.then_some(entry.task.as_str()),
                &record,
                entry.source_spawn,
            )
            .await
        {
            self.note_finalization_failure(entry, merge_commit, &error);
            warn!(task = %entry.task, merge_commit, error = %error, "landed but failed to finalize delivery; retaining landing receipt for replay");
            return Err(error);
        }
        info!(task = %entry.task, merge_commit, target = %entry.target, "recorded delivery");
        if !is_ticket {
            return Ok(());
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
        if let Err(error) = self.resubmit_conflict_reworked_parent(entry) {
            // The merge is durable; surface bookkeeping failure without denying it.
            let _ = self.escalate(
                entry,
                format!(
                    "steward: conflict correction {} landed onto {}, but automatic parent \
                     resubmission failed: {error}. Re-submit with `rk land {} --repo {} --target \
                     <original-target> --task <original-ticket>`",
                    entry.task, entry.target, entry.target, entry.repo_path
                ),
            );
            warn!(
                task = %entry.task,
                target = %entry.target,
                error = %error,
                "conflict correction landed but parent resubmission failed"
            );
        }
        Ok(())
    }

    /// Visibility for a retryable post-merge bookkeeping failure. The queue
    /// entry remains the authority; this event is deduplicated evidence for
    /// operators and `rk work`, not a second recovery ledger.
    fn note_finalization_failure(
        &self,
        entry: &LandingQueueEntry,
        merge_commit: &str,
        error: &rk_core::Error,
    ) {
        const IDENTITY: &str = "landing_finalization_failed";
        let prior = self
            .space
            .scan(
                &Pattern::category(Category::Event)
                    .scope(&entry.repo_name)
                    .identity(IDENTITY),
            )
            .unwrap_or_default()
            .into_iter()
            .any(|tuple| {
                tuple.payload["task"] == entry.task && tuple.payload["merge_commit"] == merge_commit
            });
        if prior {
            return;
        }
        let _ = self.space.out(
            Tuple::new(
                Category::Event,
                entry.repo_name.clone(),
                IDENTITY,
                "daemon",
                json!({
                    "task": entry.task,
                    "branch": entry.branch,
                    "target": entry.target,
                    "merge_commit": merge_commit,
                    "error": error.to_string(),
                    "text": format!(
                        "landing {} reached {} but delivery finalization failed: {error}",
                        entry.branch, merge_commit
                    ),
                }),
            )
            .with_lifecycle(Lifecycle::Furniture),
        );
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
        if self.supervisor.repository_policy(repo)?.delivery.mode
            != rk_workflow::DeliveryMode::Merge
        {
            return Ok(None);
        }
        let (Some(commit), Some(base)) = (&entry.candidate_sha, &entry.candidate_base) else {
            return Ok(None);
        };
        if !repo.is_ancestor(commit, &entry.target) {
            return Ok(None);
        }
        let result = json!({
            "branch": entry.branch, "target": entry.target, "delivered": true,
            "merged": true, "merge_commit": commit, "content_free": commit == base,
            "recovered": true,
        });
        let outcome = self.finalize_landed(entry, result).await?;
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
    fn rework_policy(&self, repo: &rk_git::Repo) -> rk_core::Result<ReworkPolicy> {
        Ok(ReworkPolicy::from_landing(
            &self.supervisor.repository_policy(repo)?.landing,
        ))
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
        round: u32,
        withheld: &Withheld,
    ) -> rk_core::Result<()> {
        self.escalate(entry, ctx.escalation(withheld))?;
        self.record_rework_state(entry, ctx, attempt, withheld.code)?;
        let hold_at = Utc::now();
        // Every `route_rework` call site that reaches a withhold does so
        // BEFORE its own "rework-requested" span write (the interrupted-
        // recovery, budget, and reviewed-head-moved routes all return early
        // above that point) — this is the only place those routes' review
        // phase gets closed out. The one exception (`dispatch-refused`,
        // reached AFTER that write) targets the SAME `(task, phase, round)`
        // key, so `record_phase_span`'s dedup makes this a harmless no-op
        // there, correctly leaving the original "rework-requested" span as
        // the review's terminal record rather than overwriting it. `round`
        // is the shared, cross-kind number `Self::correction_round` derives
        // — NOT `attempt`, which numbers only this chain's own kind and can
        // collide with a conflict-correction round's span on the same task
        // (see `Self::correction_round`'s doc).
        let _ = crate::span::record_phase_span(
            &self.space,
            &entry.repo_name,
            "daemon",
            &Self::timed_review_span(
                &entry.task,
                crate::span::Phase::SemanticReview,
                round,
                &entry.repo_name,
                self.phase_started_at(entry),
                hold_at,
                withheld.code,
            ),
        );
        let _ = crate::span::record_phase_span(
            &self.space,
            &entry.repo_name,
            "daemon",
            &crate::span::PhaseSpan::new(&entry.task, crate::span::Phase::AttentionHold)
                .attempt(attempt)
                .repo(&entry.repo_name)
                .authority(crate::span::Authority::Human)
                .terminal_reason(withheld.code),
        );
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

    /// Exact reviewed-commit marker used for replay deduplication. A single
    /// dispatch chain can carry both its opening "dispatching" marker and a
    /// later terminal one (`"dispatched"`/`"dispatch-refused"`/a withhold
    /// code); markers are read oldest-first, so a naive first-match would
    /// always return the transient "dispatching" one. Prefer whichever
    /// marker is terminal so a completed dispatch reads as complete instead
    /// of replaying into the interrupted-dispatch diagnosis.
    fn rework_dispatch_marker(&self, ctx: &ReworkContext) -> rk_core::Result<Option<Tuple>> {
        let matching: Vec<Tuple> = self
            .rework_dispatch_markers(ctx)?
            .into_iter()
            .filter(|marker| Self::marker_matches(marker, ctx))
            .collect();
        Ok(matching
            .iter()
            .find(|marker| {
                marker.payload.get("state").and_then(Value::as_str) != Some("dispatching")
            })
            .or_else(|| matching.first())
            .cloned())
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

    /// Start-of-phase clock for a `SemanticReview`/`Rework` span about to be
    /// recorded against `entry`: the durable landing-queue transition clock
    /// ([`LandingQueueEntry::phase_entered_at`]), read FRESH from the
    /// queue's current durable tuple rather than `entry`'s own in-memory
    /// copy. Every caller here holds an `entry` cloned at the top of
    /// [`Self::process_entry`], BEFORE [`Self::dispatch_review`] durably
    /// transitions the queue row to `AwaitingReview` — that transition
    /// resets `phase_entered_at` to the moment review actually began, but
    /// only in the DURABLE tuple, never in the caller's stale local copy.
    /// Re-reading it here is what makes elapsed time phase-local instead of
    /// inheriting whatever phase the candidate was in before review started.
    ///
    /// `None` when `entry` has no durable queue tuple at all — a synthetic
    /// conflict entry ([`Self::synthetic_conflict_entry`]), which never
    /// passed through [`LandingQueue::enqueue`]. The resulting span is
    /// left without a `started_at`, so [`crate::span::PhaseSpan::duration_ms`]
    /// comes out `None` rather than a fabricated value.
    fn phase_started_at(&self, entry: &LandingQueueEntry) -> Option<DateTime<Utc>> {
        self.queue
            .find(entry)
            .ok()
            .flatten()
            .and_then(|t| t.payload.get("phase_entered_at").cloned())
            .and_then(|v| serde_json::from_value(v).ok())
    }

    /// Build a `SemanticReview`/`Rework` span carrying real elapsed time
    /// whenever `started_at` is available, and no `duration_ms` at all when
    /// it is not — the shared shape every producer of these two phases below
    /// uses, so the timing logic lives in exactly one place. `authority` is
    /// always `Llm`: every current producer of these two phases is
    /// LLM-driven review or correction dispatch (see
    /// [`crate::span::Authority`]'s doc on the human/LLM split).
    fn timed_review_span(
        task: &str,
        phase: crate::span::Phase,
        attempt: u32,
        repo: &str,
        started_at: Option<DateTime<Utc>>,
        ended_at: DateTime<Utc>,
        terminal_reason: &str,
    ) -> crate::span::PhaseSpan {
        let mut span = crate::span::PhaseSpan::new(task, phase)
            .attempt(attempt)
            .repo(repo)
            .authority(crate::span::Authority::Llm)
            .terminal_reason(terminal_reason)
            .ended_at(ended_at);
        if let Some(started) = started_at {
            span = span.started_at(started);
        }
        span
    }

    /// Round number for the `SemanticReview` span an APPROVE verdict closes.
    ///
    /// [`crate::span::record_phase_span`] dedups on `(task, phase, attempt)`,
    /// and every bounded-correction round this pipeline dispatches already
    /// writes its own `SemanticReview` span numbered from 1
    /// ([`Self::route_rework`]'s `attempts_used + 1`,
    /// [`Self::dispatch_held_conflict`]'s `attempt`). The same `entry.task`
    /// persists across the correction/resubmit cycle, so a hardcoded `1` here
    /// would silently no-op against the first round's "rework-requested" span
    /// and leave the approval that actually landed the task with no durable
    /// record at all — the task would read as terminally rework-requested,
    /// exactly the telemetry gap this instrumentation exists to close.
    ///
    /// Derived from the durable dispatch markers those rounds number
    /// themselves from, NOT from the spans already recorded. An APPROVE can be
    /// re-routed from the verdict cache after a restart; counting spans would
    /// make each replay pick the next free attempt and write a duplicate,
    /// breaking the replay idempotency the rest of this instrumentation
    /// depends on. The markers are settled by the time an approval reads them,
    /// so every replay derives the same round.
    fn approved_review_attempt(&self, entry: &LandingQueueEntry) -> u32 {
        self.correction_rounds_used(entry)
            .unwrap_or(0)
            .saturating_add(1)
    }

    /// Distinct bounded-correction rounds — review rework and conflict
    /// correction alike — already dispatched for `entry`'s branch/target/task.
    ///
    /// Counted off the same durable markers and the same distinct-dispatch-key
    /// rule [`Self::rework_attempts_used`] and [`Self::conflict_attempts_used`]
    /// number their own rounds by, so an approval's round follows on from
    /// whichever kind of round preceded it. Both marker payloads carry
    /// `dispatch_key`/`branch`/`target`/`task`/`state`
    /// (`ReworkContext::marker_payload`, `ConflictContext::marker_payload`),
    /// so one filter serves both; keys are namespaced by identity because the
    /// two kinds number independently and could otherwise coincide.
    fn correction_rounds_used(&self, entry: &LandingQueueEntry) -> rk_core::Result<u32> {
        self.correction_rounds(entry, None)
    }

    /// Shared implementation behind [`Self::correction_rounds_used`] and
    /// [`Self::correction_round`]: counts distinct dispatched/dispatching
    /// bounded-correction rounds — rework and conflict alike — for `entry`,
    /// optionally excluding one exact `(identity, dispatch_key)` round from
    /// the count.
    fn correction_rounds(
        &self,
        entry: &LandingQueueEntry,
        exclude: Option<(&str, &str)>,
    ) -> rk_core::Result<u32> {
        let mut distinct: BTreeSet<String> = BTreeSet::new();
        for identity in [REWORK_DISPATCH_IDENTITY, CONFLICT_DISPATCH_IDENTITY] {
            let pattern = Pattern::category(Category::Event)
                .identity(identity)
                .scope(&entry.repo_name);
            for marker in self.space.scan(&pattern)? {
                let payload = &marker.payload;
                let field = |key: &str| payload.get(key).and_then(Value::as_str);
                if field("branch") != Some(entry.branch.as_str())
                    || field("target") != Some(entry.target.as_str())
                    || field("task") != Some(entry.task.as_str())
                    || !matches!(field("state"), Some("dispatching" | "dispatched"))
                {
                    continue;
                }
                let key = field("dispatch_key")
                    .map(str::to_string)
                    .unwrap_or_else(|| {
                        format!(
                            "{}\0{}",
                            field("head_sha").unwrap_or_default(),
                            field("rework_ticket").unwrap_or_default()
                        )
                    });
                if exclude == Some((identity, key.as_str())) {
                    continue;
                }
                distinct.insert(format!("{identity}\0{key}"));
            }
        }
        Ok(distinct.len() as u32)
    }

    /// The shared, cross-kind round number for one bounded-correction round
    /// (rework or conflict), for use as the `attempt` on the `SemanticReview`
    /// / `Rework` / `AttentionHold` spans that round writes.
    ///
    /// [`Self::rework_attempts_used`] and [`Self::conflict_attempts_used`]
    /// number their own kind's rounds independently, both starting at 1 — a
    /// task that interleaves one rework round and one conflict round would
    /// have both write their opening `SemanticReview` span at `attempt: 1`,
    /// and [`crate::span::record_phase_span`]'s dedup on `(task, phase,
    /// attempt)` would silently drop the second round's span. This derives
    /// the attempt from the same shared, cross-kind count
    /// [`Self::approved_review_attempt`] already uses for the APPROVE case,
    /// so rework and conflict rounds share one numbering line and can never
    /// collide.
    ///
    /// Excludes `identity`/`dispatch_key`'s own round from the count before
    /// adding 1, rather than requiring the caller only invoke this before
    /// that round's own marker is durably written: that makes the result
    /// stable regardless of whether this round's own `"dispatching"` marker
    /// already exists in the space — a fresh dispatch (marker not yet
    /// written) and a crash replay reproducing the same round for the same
    /// chain (marker already written) both read the identical number, which
    /// is what lets the replay path's span write hit the same `(task, phase,
    /// attempt)` dedup key the original attempt would have used.
    fn correction_round(
        &self,
        entry: &LandingQueueEntry,
        identity: &str,
        dispatch_key: &str,
    ) -> rk_core::Result<u32> {
        self.correction_rounds(entry, Some((identity, dispatch_key)))
            .map(|n| n.saturating_add(1))
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
        // Shared, cross-kind round number for every SemanticReview/Rework
        // span this chain's round writes below — see `Self::correction_round`.
        // Excludes this exact dispatch_key from the count, so it reads the
        // same whether this round's own marker is durably written yet or
        // not; safe to compute once, up front, and reuse through every
        // branch of this function.
        let round = self.correction_round(entry, REWORK_DISPATCH_IDENTITY, &ctx.dispatch_key())?;

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
                self.withhold_rework(entry, &ctx, attempt, round, &withheld)?;
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

        let policy = self.rework_policy(git_repo)?;
        let attempts_used = self.rework_attempts_used(&ctx)?;
        let spent_usd = self.rework_chain_spend(&ctx)?;

        let route = landing_rework::route(&policy, review.as_ref(), attempts_used, spent_usd);
        let attempt = match route {
            ReworkRoute::Withhold(withheld) => {
                self.withhold_rework(entry, &ctx, attempts_used, round, &withheld)?;
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
            self.withhold_rework(entry, &ctx, attempts_used, round, &withheld)?;
            return Ok(LandingOutcome::ReworkFiled(ticket));
        }

        // Marker first: replay gates an interrupted spawn instead of duplicating it.
        self.record_rework_state(entry, &ctx, attempt, "dispatching")?;
        // The review phase ends exactly here — reused below as the `Rework`
        // phase's own `started_at`, so the two spans bracket the same
        // instant rather than leaving a gap or an overlap between them.
        let review_ended_at = Utc::now();
        let _ = crate::span::record_phase_span(
            &self.space,
            &entry.repo_name,
            "daemon",
            &Self::timed_review_span(
                &entry.task,
                crate::span::Phase::SemanticReview,
                round,
                &entry.repo_name,
                self.phase_started_at(entry),
                review_ended_at,
                "rework-requested",
            ),
        );

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
                // Terminal marker: a redelivery must never read this dispatch
                // as interrupted just because the spawn journaled cleanly.
                self.record_rework_state(entry, &ctx, attempt, "dispatched")?;
                let _ = crate::span::record_phase_span(
                    &self.space,
                    &entry.repo_name,
                    "daemon",
                    &Self::timed_review_span(
                        &entry.task,
                        crate::span::Phase::Rework,
                        round,
                        &entry.repo_name,
                        Some(review_ended_at),
                        Utc::now(),
                        "dispatched",
                    ),
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
                self.withhold_rework(entry, &ctx, attempt, round, &withheld)?;
                warn!(
                    repo = %entry.repo_name, branch = %entry.branch, error = %e,
                    "landing pipeline: bounded rework dispatch refused"
                );
            }
        }
        Ok(LandingOutcome::ReworkFiled(ticket))
    }

    /// Resolve unattended-conflict-recovery bounds from the activated
    /// repository policy.
    fn conflict_policy(&self, repo: &rk_git::Repo) -> rk_core::Result<ConflictPolicy> {
        Ok(ConflictPolicy::from_landing(
            &self.supervisor.repository_policy(repo)?.landing,
        ))
    }

    /// Chain-scoped markers, mirroring [`Self::rework_dispatch_markers`] but
    /// namespaced to [`CONFLICT_DISPATCH_IDENTITY`] so a conflict chain and a
    /// review-rework chain on the same branch/target/task never share a budget.
    fn conflict_dispatch_markers(&self, ctx: &ConflictContext) -> rk_core::Result<Vec<Tuple>> {
        let pattern = Pattern::category(Category::Event)
            .identity(CONFLICT_DISPATCH_IDENTITY)
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

    fn conflict_marker_matches(marker: &Tuple, ctx: &ConflictContext) -> bool {
        let payload = &marker.payload;
        let key = ctx.dispatch_key();
        payload.get("dispatch_key").and_then(Value::as_str) == Some(key.as_str())
            || (payload.get("head_sha").and_then(Value::as_str) == Some(ctx.head_sha.as_str())
                && payload.get("rework_ticket").and_then(Value::as_str)
                    == Some(ctx.rework_ticket.as_str()))
    }

    fn record_conflict_state(
        &self,
        entry: &LandingQueueEntry,
        ctx: &ConflictContext,
        attempt: u32,
        state: &str,
    ) -> rk_core::Result<Tuple> {
        let marker = Tuple::new(
            Category::Event,
            entry.repo_name.clone(),
            CONFLICT_DISPATCH_IDENTITY,
            "daemon",
            ctx.marker_payload(attempt, None, state),
        )
        .with_lifecycle(Lifecycle::Furniture);
        self.space.out(marker.clone())?;
        Ok(marker)
    }

    fn withhold_conflict(
        &self,
        entry: &LandingQueueEntry,
        ctx: &ConflictContext,
        attempt: u32,
        round: u32,
        withheld: &Withheld,
    ) -> rk_core::Result<()> {
        self.escalate(entry, ctx.escalation(withheld))?;
        self.record_conflict_state(entry, ctx, attempt, withheld.code)?;
        // Mirrors `withhold_rework`: closes out the review/correction phase
        // for the withhold routes that never reach `dispatch_held_conflict`'s
        // own "conflict-correction-requested" write, and is a dedup no-op
        // (same `(task, phase, round)` key) for the one route that does.
        // `round` is the shared, cross-kind number `Self::correction_round`
        // derives, not `attempt`, which numbers only conflict-correction
        // rounds and can collide with a rework round's span on the same task.
        // `entry` here is usually the synthetic conflict entry
        // (`Self::synthetic_conflict_entry`), which carries no durable queue
        // clock, so `duration_ms` is left absent rather than fabricated.
        let _ = crate::span::record_phase_span(
            &self.space,
            &entry.repo_name,
            "daemon",
            &Self::timed_review_span(
                &entry.task,
                crate::span::Phase::SemanticReview,
                round,
                &entry.repo_name,
                self.phase_started_at(entry),
                Utc::now(),
                withheld.code,
            ),
        );
        let _ = crate::span::record_phase_span(
            &self.space,
            &entry.repo_name,
            "daemon",
            &crate::span::PhaseSpan::new(&entry.task, crate::span::Phase::AttentionHold)
                .attempt(attempt)
                .repo(&entry.repo_name)
                .authority(crate::span::Authority::Human)
                .terminal_reason(withheld.code),
        );
        Ok(())
    }

    fn conflict_attempts_used(&self, ctx: &ConflictContext) -> rk_core::Result<u32> {
        let distinct: BTreeSet<String> = self
            .conflict_dispatch_markers(ctx)?
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

    /// Exact conflicted-commit marker used for replay deduplication. Mirrors
    /// [`Self::rework_dispatch_marker`]'s terminal-preferring lookup: a
    /// chain's opening "dispatching" marker is always the oldest, so prefer
    /// whichever marker is terminal rather than first-match.
    fn conflict_dispatch_marker(&self, ctx: &ConflictContext) -> rk_core::Result<Option<Tuple>> {
        let matching: Vec<Tuple> = self
            .conflict_dispatch_markers(ctx)?
            .into_iter()
            .filter(|marker| Self::conflict_marker_matches(marker, ctx))
            .collect();
        Ok(matching
            .iter()
            .find(|marker| {
                marker.payload.get("state").and_then(Value::as_str) != Some("dispatching")
            })
            .or_else(|| matching.first())
            .cloned())
    }

    fn conflict_dispatch_has_state(
        &self,
        ctx: &ConflictContext,
        state: &str,
    ) -> rk_core::Result<bool> {
        Ok(self
            .conflict_dispatch_markers(ctx)?
            .into_iter()
            .any(|marker| {
                Self::conflict_marker_matches(&marker, ctx)
                    && marker.payload.get("state").and_then(Value::as_str) == Some(state)
            }))
    }

    /// Spawn's durable journal proves this exact dispatch crossed its commit point.
    fn conflict_agent_was_journaled(&self, ctx: &ConflictContext) -> bool {
        self.supervisor.list_all().into_iter().any(|record| {
            record.role == "rat"
                && record.task.as_deref() == Some(ctx.rework_ticket.as_str())
                && record.target_branch == ctx.branch
                && record.fork_point.as_deref() == Some(ctx.head_sha.as_str())
        })
    }

    /// Cumulative chain spend, including terminal and archived agents.
    fn conflict_chain_spend(&self, ctx: &ConflictContext) -> rk_core::Result<f64> {
        let correction_tickets: BTreeSet<String> = self
            .conflict_dispatch_markers(ctx)?
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
                        .is_some_and(|task| task == ctx.task || correction_tickets.contains(task))
            })
            .map(|a| a.cost_usd)
            .sum();
        Ok(spent)
    }

    /// File the ticket, then dispatch one exact-base correction agent or hold
    /// behind an evidence-rich gate. Both paths retain the durable ticket.
    /// Mirrors [`Self::route_rework`], the review-verdict counterpart: the
    /// two differ only in where authority is judged from (a reviewer's notes
    /// there, the conflict's own evidence here) and in the dedicated
    /// dispatch-marker identity and budget each chain uses.
    async fn route_conflict(
        &self,
        entry: &LandingQueueEntry,
        git_repo: &rk_git::Repo,
        detail: &str,
    ) -> rk_core::Result<LandingOutcome> {
        let stat = {
            let repo = git_repo.clone();
            let target = entry.target.clone();
            let branch = entry.branch.clone();
            blocking(move || repo.diff_stat(&target, &branch)).await?
        };
        let fork_point = {
            let repo = git_repo.clone();
            let target = entry.target.clone();
            let branch = entry.branch.clone();
            blocking(move || repo.merge_base(&branch, &target)).await?
        };
        let target_head = {
            let repo = git_repo.clone();
            let target = entry.target.clone();
            blocking(move || repo.rev_parse(&target)).await?
        };
        let ticket = self.file_conflict_rework_ticket(entry).await?;
        let ctx = ConflictContext {
            repo: entry.repo_name.clone(),
            repo_path: entry.repo_path.clone(),
            branch: entry.branch.clone(),
            head_sha: entry.head_sha.clone(),
            target: entry.target.clone(),
            target_head,
            fork_point,
            task: entry.task.clone(),
            rework_ticket: ticket.identity.clone(),
            conflict_detail: landing_conflict::bound_conflict_detail(detail),
            diff_files: stat.files.len() as u64,
            diff_lines: stat.lines,
        };
        // Shared, cross-kind round number for every SemanticReview span this
        // chain's round writes below — see `Self::correction_round`.
        let round =
            self.correction_round(entry, CONFLICT_DISPATCH_IDENTITY, &ctx.dispatch_key())?;

        // The coalesced ticket completes the dispatch key before side effects.
        if let Some(marker) = self.conflict_dispatch_marker(&ctx)? {
            // Marker-before-spawn needs a journal check to distinguish success
            // from the interruption window.
            if marker.payload.get("state").and_then(Value::as_str) == Some("dispatching")
                && !self.conflict_agent_was_journaled(&ctx)
                && !self.conflict_dispatch_has_state(&ctx, "dispatch-interrupted")?
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
                         contains no rat for correction ticket {} based on {} at {}; the daemon \
                         may have stopped between recording the marker and journaling the spawn",
                        ctx.rework_ticket, ctx.branch, ctx.head_sha
                    ),
                    decision: "confirm that no correction agent exists, then dispatch the \
                               recorded correction ticket exactly once or abandon it"
                        .into(),
                };
                self.withhold_conflict(entry, &ctx, attempt, round, &withheld)?;
                warn!(
                    repo = %entry.repo_name, branch = %entry.branch,
                    head_sha = %entry.head_sha, ticket = %ctx.rework_ticket, attempt,
                    "landing pipeline: interrupted conflict-correction dispatch requires human recovery"
                );
            }
            info!(
                repo = %entry.repo_name, branch = %entry.branch, head_sha = %entry.head_sha,
                ticket = %ctx.rework_ticket,
                "landing pipeline: CONFLICT already routed for this exact chain; not re-dispatching"
            );
            return Ok(LandingOutcome::ReworkFiled(ticket));
        }

        let landing_policy = self.supervisor.repository_policy(git_repo)?.landing;
        let protected_path_hit = ere_matches_any(&landing_policy.protected_paths, &stat.files);
        let evidence = ConflictEvidence {
            conflict_detail: &ctx.conflict_detail,
            protected_path_hit,
            diff_files: ctx.diff_files,
            diff_lines: ctx.diff_lines,
            max_diff_files: landing_policy.max_diff_files,
            max_diff_lines: landing_policy.max_diff_lines,
        };
        let policy = self.conflict_policy(git_repo)?;
        let attempts_used = self.conflict_attempts_used(&ctx)?;
        let spent_usd = self.conflict_chain_spend(&ctx)?;

        let route = landing_conflict::route(&policy, &evidence, attempts_used, spent_usd);
        let attempt = match route {
            ReworkRoute::Withhold(withheld) => {
                self.withhold_conflict(entry, &ctx, attempts_used, round, &withheld)?;
                return Ok(LandingOutcome::ReworkFiled(ticket));
            }
            ReworkRoute::Dispatch { attempt } => attempt,
        };

        // A correction cuts from branch tip, so never open an orchestrator
        // decision after the conflicted tip has already moved — the evidence
        // it would decide against is already stale.
        let tip = {
            let repo = git_repo.clone();
            let branch = entry.branch.clone();
            blocking(move || repo.rev_parse(&branch)).await?
        };
        if tip != entry.head_sha {
            let withheld = Withheld {
                code: "conflicted-head-moved",
                detail: format!(
                    "the conflicted head {} is no longer {}'s tip (now {tip}), so a correction \
                     cut from the branch would start from work this conflict was never \
                     evidenced against",
                    entry.head_sha, entry.branch
                ),
                decision: "re-attempt the merge at the branch's current tip, or reset it back to \
                           the conflicted head and let the correction dispatch"
                    .into(),
            };
            self.withhold_conflict(entry, &ctx, attempts_used, round, &withheld)?;
            return Ok(LandingOutcome::ReworkFiled(ticket));
        }

        // Authority::Orchestrator means a dispatch decision is SAFE to make
        // without a human, not that this daemon process may make it
        // unattended: TKT-01M0E8PNFQZ70F3ZFG3KCS39ZG's own contract requires
        // a live, fenced orchestrator lease over this repository before the
        // correction agent may be spawned. This process only collects
        // evidence and holds — `Server::execute_orchestrator` is the only
        // caller of `dispatch_held_conflict`, and it is only reachable once
        // `attention.decide` has fenced the call through
        // `crate::orchestrator_lease::LeaseStore`.
        self.hold_conflict_for_orchestrator_decision(entry, &ctx, attempt)?;
        Ok(LandingOutcome::ReworkFiled(ticket))
    }

    /// Record the durable evidence a conflict-correction dispatch decision
    /// needs, then wait: no `spawn_async` call happens on this path. Writes
    /// the marker `route_conflict`'s own replay guard (top of that function)
    /// already checks, in state [`landing_conflict::CONFLICT_STATE_AWAITING_DECISION`]
    /// rather than `"dispatching"` — that state is reserved for
    /// [`Self::dispatch_held_conflict`]'s own crash window, which this call
    /// never enters. Also emits a `branch_landed` event with the same
    /// `{merged: false, pr_opened: false}` shape every other dropped land in
    /// this daemon uses, so `crate::reconcile::conflict_held_landing` (and
    /// `rk inbox`'s own copy of the same check) surfaces this exact hold as
    /// an `Authority::Orchestrator` attention item with no separate
    /// violation-detection code required.
    fn hold_conflict_for_orchestrator_decision(
        &self,
        entry: &LandingQueueEntry,
        ctx: &ConflictContext,
        attempt: u32,
    ) -> rk_core::Result<()> {
        self.record_conflict_state(
            entry,
            ctx,
            attempt,
            landing_conflict::CONFLICT_STATE_AWAITING_DECISION,
        )?;
        self.space.out(Tuple::new(
            Category::Event,
            entry.repo_name.clone(),
            "branch_landed",
            "daemon",
            json!({
                "branch": ctx.branch,
                "target": ctx.target,
                "merged": false,
                "pr_opened": false,
                // Distinguishes THIS chain from a later, distinct conflict on
                // the same branch: `reconcile::conflict_held_landing` folds
                // this into the violation id, so a second conflict after this
                // one is corrected gets its own attention item and decision
                // record rather than replaying (or being permanently blocked
                // behind the cursor of) this chain's own terminal decision.
                "chain_key": ctx.dispatch_key(),
                "detail": format!(
                    "merge conflict held for a bounded Orchestrator-authority correction \
                     decision (attempt {attempt} of this repository's cap, ticket {}); resolve \
                     via attention.decide over a live orchestrator lease, or rk spawn --repo {} \
                     --ticket {} --base {}",
                    ctx.rework_ticket, ctx.repo, ctx.rework_ticket, ctx.branch
                ),
            }),
        ))?;
        let _ = crate::span::record_phase_span(
            &self.space,
            &entry.repo_name,
            "daemon",
            &crate::span::PhaseSpan::new(&entry.task, crate::span::Phase::AttentionHold)
                .attempt(attempt)
                .repo(&entry.repo_name)
                .authority(crate::span::Authority::Llm)
                .terminal_reason("awaiting-orchestrator-decision"),
        );
        info!(
            repo = %entry.repo_name, branch = %entry.branch, head_sha = %entry.head_sha,
            ticket = %ctx.rework_ticket, attempt,
            "landing pipeline: CONFLICT held for a leased orchestrator decision, not dispatching unattended"
        );
        Ok(())
    }

    /// The most recent [`CONFLICT_DISPATCH_IDENTITY`] marker for a
    /// (repo, branch) pair, regardless of its exact dispatch key — an
    /// orchestrator decision only knows the violation's subject (the branch
    /// name), not the chain's full identity, so this is how
    /// [`Self::dispatch_held_conflict`] locates the held chain to act on.
    /// Newest by tuple id, matching every other "latest wins" marker lookup
    /// in this module.
    /// How far one chain's own state machine has actually progressed
    /// (awaiting-decision -> dispatching -> a terminal state). Used instead
    /// of raw id order to pick the "current" marker WITHIN one chain's own
    /// markers: a chain's "dispatching" and terminal writes land
    /// microseconds apart within the SAME call (`record_conflict_state`
    /// then `withhold_conflict`), often inside one millisecond, and a
    /// ULID's sub-millisecond ordering is random (`RecordId`'s own doc
    /// comment) — id order alone picked the still-"dispatching" write over
    /// that same chain's own terminal one often enough to make a bare
    /// `max_by(id)` a genuinely flaky read, not a rare theoretical one. A
    /// bare "state != dispatching" filter is not a safe substitute either —
    /// it matches a chain's FIRST marker (the original awaiting-decision
    /// hold) just as readily as its terminal one.
    fn conflict_state_rank(marker: &Tuple) -> u8 {
        match marker.payload.get("state").and_then(Value::as_str) {
            Some(landing_conflict::CONFLICT_STATE_AWAITING_DECISION) => 0,
            Some("dispatching") => 1,
            _ => 2,
        }
    }

    /// The exact dispatch key a current marker belongs to. Missing identity is
    /// not reconstructed from content fields.
    fn conflict_marker_chain_key(marker: &Tuple) -> Option<&str> {
        marker.payload.get("dispatch_key").and_then(Value::as_str)
    }

    fn conflict_markers_for_branch(&self, repo: &str, branch: &str) -> rk_core::Result<Vec<Tuple>> {
        let pattern = Pattern::category(Category::Event)
            .identity(CONFLICT_DISPATCH_IDENTITY)
            .scope(repo);
        Ok(self
            .space
            .scan(&pattern)?
            .into_iter()
            .filter(|t| t.payload.get("branch").and_then(Value::as_str) == Some(branch))
            .collect())
    }

    /// The current marker for the EXACT chain `chain_key` names — never a
    /// different, possibly newer chain on the same branch. This is what
    /// [`Self::dispatch_held_conflict`] and [`Self::pending_conflict_attempt`]
    /// use whenever the caller (an already-decided `Violation`) knows which
    /// chain it means. There is no branch-latest fallback.
    fn conflict_marker_for_chain_key(
        &self,
        repo: &str,
        branch: &str,
        chain_key: &str,
    ) -> rk_core::Result<Option<Tuple>> {
        Ok(self
            .conflict_markers_for_branch(repo, branch)?
            .into_iter()
            .filter(|m| Self::conflict_marker_chain_key(m) == Some(chain_key))
            .max_by_key(|m| (Self::conflict_state_rank(m), m.id)))
    }

    /// The in-flight attempt number for a conflict chain awaiting an
    /// orchestrator decision, if one is held — `Server::orchestrator_attempt_hint`'s
    /// read-only lookup for the `CONFLICT_HELD_LANDING` kind, folded into
    /// the decision journal envelope before the actual dispatch mutation
    /// runs. Returns `None` for a branch with no held chain at all rather
    /// than guessing an attempt number. `chain_key` (from the violation's
    /// own evidence, when present) binds this to the EXACT chain the
    /// decision named; missing exact chain identity is not actionable.
    pub(crate) fn pending_conflict_attempt(
        &self,
        repo: &str,
        branch: &str,
        chain_key: Option<&str>,
    ) -> Option<u32> {
        let marker = self
            .conflict_marker_for_chain_key(repo, branch, chain_key?)
            .ok()??;
        marker
            .payload
            .get("attempt")
            .and_then(Value::as_u64)
            .and_then(|a| u32::try_from(a).ok())
    }

    /// Execute the bounded correction spawn a leased orchestrator decision
    /// has just authorized for a conflict this pipeline is holding. The ONLY
    /// caller is `Server::execute_orchestrator`, itself only reachable after
    /// `attention.decide`'s `Authority::Orchestrator` arm has fenced the
    /// call through a live lease and journaled the decision — this function
    /// performs the mutation that decision authorizes, never authenticates
    /// on its own. Idempotent: a chain whose marker has already advanced
    /// past [`landing_conflict::CONFLICT_STATE_AWAITING_DECISION`] (a prior
    /// call already dispatched, or a human resolved the branch by hand) is a
    /// no-op success, not a second spawn.
    /// `chain_key` (from the deciding `Violation`'s own evidence, when
    /// present) binds this dispatch to the EXACT chain that violation
    /// named — never a different, possibly newer chain that appeared on
    /// the same branch between when `attention.decide` authorized this call
    /// and this function's own independent marker read. Missing exact chain
    /// identity fails closed.
    pub(crate) async fn dispatch_held_conflict(
        &self,
        repo: &str,
        branch: &str,
        chain_key: Option<&str>,
    ) -> rk_core::Result<String> {
        let chain_key = chain_key.ok_or_else(|| {
            rk_core::Error::other(format!(
                "conflict correction for {repo}/{branch} requires an exact chain key"
            ))
        })?;
        let marker = self.conflict_marker_for_chain_key(repo, branch, chain_key)?;
        let Some(marker) = marker else {
            return Err(rk_core::Error::other(format!(
                "no conflict-correction chain for {repo}/{branch} has ever been held for a \
                 decision"
            )));
        };
        let state = marker
            .payload
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let ctx = ConflictContext::from_marker_payload(&marker.payload).ok_or_else(|| {
            rk_core::Error::other(format!(
                "conflict dispatch marker for {repo}/{branch} is missing a required field"
            ))
        })?;
        let attempt = marker
            .payload
            .get("attempt")
            .and_then(Value::as_u64)
            .and_then(|a| u32::try_from(a).ok())
            .unwrap_or(1);
        // Shared, cross-kind round number for every SemanticReview/Rework
        // span this chain's round writes below — see `Self::correction_round`.
        // Excludes this exact dispatch_key from the count, so it reads the
        // same whether this round's own "dispatching" marker is durably
        // written yet or not (a fresh dispatch vs. a crash replay of this
        // same call).
        let round = self.correction_round(
            &Self::synthetic_conflict_entry(&ctx),
            CONFLICT_DISPATCH_IDENTITY,
            &ctx.dispatch_key(),
        )?;

        // Every state but the one this call is meant to act on is handled
        // explicitly — never a blanket "not awaiting-decision, so already
        // done" — because that blanket check previously made the
        // `"dispatching"`-without-a-journaled-agent crash window below
        // UNREACHABLE: this match now runs BEFORE any journal check, so a
        // retry after a crash between recording `"dispatching"` and the
        // spawn actually journaling can still be told apart from a genuine
        // prior success, rather than short-circuiting to a false `Ok` that
        // `Server::execute_orchestrator`/`attention.decide` would then
        // journal as a resolved decision and advance the lease cursor past
        // — with zero worker ever dispatched.
        match state.as_str() {
            landing_conflict::CONFLICT_STATE_AWAITING_DECISION => {}
            "dispatched" => {
                return Ok(format!(
                    "conflict-correction chain for {repo}/{branch} already dispatched (ticket \
                     {}); not re-dispatching",
                    ctx.rework_ticket
                ));
            }
            "dispatching" if self.conflict_agent_was_journaled(&ctx) => {
                // The spawn DID succeed — only the terminal "dispatched"
                // marker write never completed. Converging on success here
                // is correct, not a guess: the supervisor's own durable
                // journal is the proof.
                return Ok(format!(
                    "conflict-correction chain for {repo}/{branch} already dispatched \
                     (journaled, ticket {}); not re-dispatching",
                    ctx.rework_ticket
                ));
            }
            "dispatching" => {
                // A crash between this call recording "dispatching" and the
                // spawn actually journaling looks identical to a fresh,
                // never-attempted hold — the same ambiguity
                // `route_conflict`'s own top-of-function guard resolves for
                // the original dispatch path. Fail closed with a truthful
                // interruption gate rather than guessing either way; guarded
                // so a repeated retry does not raise a second human gate for
                // the exact same interruption.
                let entry = Self::synthetic_conflict_entry(&ctx);
                let withheld = Withheld {
                    code: "dispatch-interrupted",
                    detail: format!(
                        "a durable dispatch attempt exists for correction ticket {} based on {} \
                         at {}, but the supervisor registry contains no rat for it; the daemon \
                         may have stopped between recording the marker and journaling the spawn",
                        ctx.rework_ticket, ctx.branch, ctx.head_sha
                    ),
                    decision: "confirm that no correction agent exists, then dispatch the \
                               recorded correction ticket exactly once or abandon it"
                        .into(),
                };
                if !self.conflict_dispatch_has_state(&ctx, "dispatch-interrupted")? {
                    self.withhold_conflict(&entry, &ctx, attempt, round, &withheld)?;
                }
                return Err(rk_core::Error::other(withheld.detail));
            }
            other => {
                // A terminal human gate already raised for this exact chain
                // (`dispatch-refused`, `conflicted-head-moved`,
                // `dispatch-interrupted`) — retrying must never silently
                // report success behind an unresolved gate. Refusing here
                // (rather than a blanket `Ok`) keeps the decision journal
                // entry `resolved: false, terminal: false`: retryable once a
                // human clears the gate, never permanently poisoned as
                // "already decided" and never falsely marked done.
                return Err(rk_core::Error::other(format!(
                    "conflict-correction chain for {repo}/{branch} is already terminally \
                     '{other}'; resolve the existing human gate before retrying"
                )));
            }
        }

        let git_repo = {
            let repo_path = PathBuf::from(&ctx.repo_path);
            blocking(move || rk_git::Repo::discover(&repo_path)).await?
        };
        let tip = {
            let repo = git_repo.clone();
            let branch = ctx.branch.clone();
            blocking(move || repo.rev_parse(&branch)).await?
        };
        let entry = Self::synthetic_conflict_entry(&ctx);
        if tip != ctx.head_sha {
            let withheld = Withheld {
                code: "conflicted-head-moved",
                detail: format!(
                    "the conflicted head {} is no longer {}'s tip (now {tip}), so a correction \
                     cut from the branch would start from work this conflict was never \
                     evidenced against",
                    ctx.head_sha, ctx.branch
                ),
                decision: "re-attempt the merge at the branch's current tip, or reset it back to \
                           the conflicted head and let the correction dispatch"
                    .into(),
            };
            self.withhold_conflict(&entry, &ctx, attempt, round, &withheld)?;
            return Err(rk_core::Error::other(withheld.detail));
        }

        // Marker first: replay gates an interrupted spawn instead of duplicating it.
        self.record_conflict_state(&entry, &ctx, attempt, "dispatching")?;
        // Same SemanticReview/Rework phase pair `route_rework` brackets its
        // own LLM-authority dispatch with (landing.rs's rework routing) —
        // this dispatch is orchestrator-authorized rather than
        // reviewer-verdict-driven, but `Authority` has no distinct
        // orchestrator variant (matching the choice already made for the
        // `AttentionHold` this same chain writes while awaiting that
        // decision, below in `hold_conflict_for_orchestrator_decision`), so
        // `Llm` is reused here too. `entry` is synthetic here, so
        // `phase_started_at` is `None` and `duration_ms` is left absent
        // rather than fabricated (see `Self::phase_started_at`'s doc).
        let review_ended_at = Utc::now();
        let _ = crate::span::record_phase_span(
            &self.space,
            &entry.repo_name,
            "daemon",
            &Self::timed_review_span(
                &entry.task,
                crate::span::Phase::SemanticReview,
                round,
                &entry.repo_name,
                self.phase_started_at(&entry),
                review_ended_at,
                "conflict-correction-requested",
            ),
        );

        let params = crate::supervisor::SpawnParams {
            repo: ctx.repo_path.clone(),
            task: ctx.rework_ticket.clone(),
            prompt: Some(ctx.prompt()),
            role: "rat".to_string(),
            // A correction always lands back on the conflicted (held) branch.
            base: Some(ctx.branch.clone()),
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
                    repo = %ctx.repo, branch = %ctx.branch, head_sha = %ctx.head_sha,
                    agent = %record.name, ticket = %ctx.rework_ticket, attempt,
                    "landing pipeline: orchestrator-authorized dispatch of bounded \
                     conflict-correction agent from the held branch"
                );
                // Terminal marker: a redelivery must never read this dispatch
                // as interrupted just because the spawn journaled cleanly.
                self.record_conflict_state(&entry, &ctx, attempt, "dispatched")?;
                let _ = crate::span::record_phase_span(
                    &self.space,
                    &entry.repo_name,
                    "daemon",
                    &Self::timed_review_span(
                        &entry.task,
                        crate::span::Phase::Rework,
                        round,
                        &entry.repo_name,
                        Some(review_ended_at),
                        Utc::now(),
                        "conflict-correction-dispatched",
                    ),
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
                        "landing pipeline: failed to record conflict-correction assignee"
                    );
                }
                Ok(format!(
                    "dispatched {} for {repo}/{branch} (ticket {})",
                    record.name, ctx.rework_ticket
                ))
            }
            Err(e) => {
                // Refusal holds the branch and becomes a visible human gate.
                let withheld = Withheld {
                    code: "dispatch-refused",
                    detail: format!("the correction spawn was refused by the supervisor: {e}"),
                    decision: "clear whatever refused the dispatch (budget cap, WIP ceiling, \
                               paused dispatch), then dispatch the correction ticket"
                        .into(),
                };
                self.withhold_conflict(&entry, &ctx, attempt, round, &withheld)?;
                warn!(
                    repo = %ctx.repo, branch = %ctx.branch, error = %e,
                    "landing pipeline: orchestrator-authorized conflict-correction dispatch refused"
                );
                Err(rk_core::Error::other(withheld.detail))
            }
        }
    }

    /// The minimal [`LandingQueueEntry`] `escalate`/`record_conflict_state`
    /// actually read (`repo_name`, `task`) — [`Self::dispatch_held_conflict`]
    /// only has a [`ConflictContext`] reconstructed from a durable marker,
    /// not the original queue entry, and rebuilding the whole entry from
    /// scratch is not worth a second struct just for these two call sites.
    fn synthetic_conflict_entry(ctx: &ConflictContext) -> LandingQueueEntry {
        LandingQueueEntry {
            repo_name: ctx.repo.clone(),
            repo_path: ctx.repo_path.clone(),
            branch: ctx.branch.clone(),
            target: ctx.target.clone(),
            head_sha: ctx.head_sha.clone(),
            task: ctx.task.clone(),
            ..Default::default()
        }
    }

    /// File the historical steward-shaped follow-up directly and idempotently.
    /// Distinct coalesce namespace from [`Self::file_rework_ticket`] so a
    /// conflict and a review-rework on the same branch/head never collapse
    /// onto the same follow-up ticket.
    async fn file_conflict_rework_ticket(
        &self,
        entry: &LandingQueueEntry,
    ) -> rk_core::Result<Tuple> {
        self.tickets
            .create(NewTicket {
                title: format!("conflict: {}", entry.task),
                body: Some(format!(
                    "Landing pipeline could not merge {} into {} — a merge conflict, not a \
                     review verdict. Read the durable dispatch marker: rk scan event {}",
                    entry.branch, entry.target, entry.repo_name
                )),
                scope: Some(entry.repo_name.clone()),
                parent: None,
                priority: "normal".to_string(),
                labels: Vec::new(),
                depends_on: Vec::new(),
                created_by: Some("daemon".to_string()),
                // Redelivery resolves to the same follow-up ticket.
                coalesce_key: Some(landing_conflict::ticket_coalesce_key(
                    &entry.repo_name,
                    &entry.branch,
                    &entry.head_sha,
                    &entry.target,
                    &entry.task,
                )),
            })
            .await
    }

    /// [`Self::resubmit_reworked_parent`]'s counterpart for a landed
    /// conflict-correction: queue the conflicted branch at its new head once
    /// its correction agent lands back onto it.
    fn resubmit_conflict_reworked_parent(&self, entry: &LandingQueueEntry) -> rk_core::Result<()> {
        let marker = self
            .space
            .scan(
                &Pattern::category(Category::Event)
                    .identity(CONFLICT_DISPATCH_IDENTITY)
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
        let original_branch = required_payload_str(payload, "branch", "conflict dispatch marker")?;
        let original_target = required_payload_str(payload, "target", "conflict dispatch marker")?;
        let original_task = required_payload_str(payload, "task", "conflict dispatch marker")?;
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
                    CONFLICT_RESUBMISSION_IDENTITY,
                    "daemon",
                    json!({
                        "dispatch_key": payload.get("dispatch_key"),
                        "rework_ticket": entry.task,
                        "correction_branch": entry.branch,
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
                "landed conflict correction queued its held parent for a fresh gate run"
            );
        }
        Ok(())
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

    /// Resolve the repo-owned CUE policy once for this exact prepared
    /// candidate. The returned value is data-only and can be executed later
    /// without rereading either policy file.
    async fn resolve_gate_plan_at(
        &self,
        entry: &LandingQueueEntry,
        git_repo: &rk_git::Repo,
        gates: &GateConfig,
        tested_sha: &str,
    ) -> rk_core::Result<ResolvedGatePlan> {
        let changed_paths = {
            let git_repo = git_repo.clone();
            let target = entry.target.clone();
            let sha = tested_sha.to_string();
            blocking(move || {
                Ok(git_repo
                    .diff_stat(&target, &sha)
                    .map(|stat| stat.files)
                    .unwrap_or_default())
            })
            .await?
        };
        let checks_file = PathBuf::from(&entry.repo_path)
            .join(".rk")
            .join("checks.cue");
        self.gate_plan(&checks_file, &entry.target, gates, &changed_paths)
    }

    /// Run the same three gates `steward.cue`'s `_gates` block runs today
    /// (POLICY, DIFF-SCOPE, the repo's named `verify` check) against a
    /// persistent daemon-owned worktree reset to the candidate's tip.
    /// Returns [`GateRunOutcome::Pass`] only if every gate reported
    /// `verdict: "pass"`; a caller that needs a plain pass/fail bool can
    /// use [`GateRunOutcome::passed`].
    async fn execute_gate_plan_at(
        &self,
        entry: &mut LandingQueueEntry,
        git_repo: &rk_git::Repo,
        gate_plan: ResolvedGatePlan,
        tested_sha: &str,
    ) -> rk_core::Result<GateRunOutcome> {
        let started = Instant::now();
        let mut passed_checks = Vec::new();
        // Best-effort per-check admission-queue wait (`RunProgress::queue_wait_ms`,
        // only ever `Some` for a check that opted into `sharedCargoTarget` AND
        // actually contended for the per-repo verification-admission slot) —
        // the durable-events acceptance criterion's "queue wait" field,
        // recorded alongside `duration_ms` on the final `landing_gate_pass`.
        let mut queue_wait_ms: Vec<(String, Option<u64>)> = Vec::new();
        // One `verification_proof_key` digest per passed check — the SAME
        // identity components (repo, candidate, check name, command,
        // toolchain, environment policy) `lookup_verification_proof`'s
        // primary exact-match cache already keys on. Stored on the final
        // `landing_gate_pass` event so that event's OWN fallback reuse path
        // can require a real digest match instead of trusting a bare check
        // name, which said nothing about whether the command/toolchain/
        // environment that actually ran still matches a later caller's.
        let mut check_proof_keys: Vec<(String, Option<String>)> = Vec::new();
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

        let ResolvedGatePlan {
            checks,
            edge_class,
            full_check_required,
            reason,
        } = gate_plan;

        let selected_checks: Vec<String> = checks
            .iter()
            .map(|(check, _, _)| check.name.clone())
            .collect();
        let proof_key = if full_check_required {
            checks.last().and_then(|(check, _, _)| {
                verification_proof_key(&entry.repo_name, tested_sha, check)
            })
        } else {
            None
        };
        self.space.out(
            Tuple::new(
                Category::Event,
                entry.repo_name.clone(),
                LANDING_EDGE_PLAN_IDENTITY,
                "daemon",
                json!({
                    "branch": entry.branch,
                    "target": entry.target,
                    "task": entry.task,
                    "candidate_sha": tested_sha,
                    "edge_class": edge_class.as_str(),
                    "selected_checks": selected_checks,
                    "full_check_required": full_check_required,
                    "proof_key": proof_key,
                    "reason": reason,
                }),
            )
            .with_lifecycle(Lifecycle::Furniture),
        )?;
        let _ = crate::span::record_phase_span(
            &self.space,
            &entry.repo_name,
            "daemon",
            &crate::span::PhaseSpan::new(&entry.task, crate::span::Phase::LandingPrep)
                .repo(&entry.repo_name)
                .target(&entry.target)
                .candidate(tested_sha)
                .proof_kind(if full_check_required {
                    "full-final"
                } else {
                    "focused-inner"
                }),
        );

        let id = format!("landing:{}", entry.branch);
        for (check_index, (check, env, timeout)) in checks.into_iter().enumerate() {
            // This check's position in the plan, not a rework-round counter:
            // stable across a crash-resume re-run of this same plan (so a
            // repeated earlier check dedupes against the span it already
            // wrote), but collides with a later landing round's plan over
            // the same task the same way the single aggregate span this
            // replaces always did (both default to the same low attempts) —
            // no regression, just decomposed to one span per check.
            let check_attempt = u32::try_from(check_index)
                .unwrap_or(u32::MAX)
                .saturating_add(1);
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
            let progress = Arc::new(Mutex::new(RunProgress::default()));
            let check_started = Instant::now();

            // Resuming after a crash landed between spending the retry
            // budget and the retry attempt completing (`gate_infra_retry_check`'s
            // doc): this exact check is durably marked mid-retry from a PRIOR
            // process. Whatever THIS attempt does completes that retry — it
            // is ordinal 2, never a fresh ordinal-1 death — so it is settled
            // through the exact same path the inline retry below uses,
            // without ever granting a second retry.
            if entry.gate_infra_retry_check.as_deref() == Some(check.name.as_str()) {
                // Close the crash window between spending the retry budget
                // and writing its ordinal-1 evidence (the fresh-death branch
                // below persists the marker durably BEFORE that write, on
                // purpose — see its comment). A crash landing exactly there
                // leaves this marker set with no ordinal-1 record at all;
                // reconstruct it now, before doing anything else, so the
                // evidence trail is never silently missing an attempt.
                self.ensure_infra_retry_ordinal1_evidence(
                    entry,
                    tested_sha,
                    &check.name,
                    &resolved.command,
                )?;
                // The crash can equally have landed in the OTHER window
                // `finish_infra_retry` opens: after the ordinal-2 evidence was
                // written but before the marker was cleared and persisted.
                // There the retry is genuinely spent and its outcome is
                // already durable, so re-running the check would both execute
                // an already-settled attempt and append a duplicate ordinal-2
                // event. Settlement is therefore idempotent — the durable
                // evidence, not the marker alone, decides whether anything is
                // left to run.
                if let Some(passed) = self.settled_infra_retry(entry, tested_sha, &check.name)? {
                    warn!(
                        check = %check.name, branch = %entry.branch, passed,
                        "landing pipeline: infra retry was already settled before the crash, resuming from its durable evidence"
                    );
                    self.clear_infra_retry_marker(entry)?;
                    if !passed {
                        return Ok(GateRunOutcome::InfraRetryExhausted);
                    }
                    // Never executed this attempt at all — resumed straight
                    // from durable evidence — so there is no queue wait to
                    // report for it.
                    self.record_check_verification_span(
                        entry,
                        CheckVerificationSpan {
                            check_name: &check.name,
                            attempt: check_attempt,
                            candidate: tested_sha,
                            full_check_required,
                            queue_wait_ms: None,
                            duration_ms: None,
                            proof_reused: false,
                        },
                    );
                    queue_wait_ms.push((check.name.clone(), None));
                    passed_checks.push(check.name.clone());
                    continue;
                }
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
                        Some(Arc::clone(&progress)),
                    )
                    .await;
                if !self
                    .finish_infra_retry(
                        entry,
                        tested_sha,
                        &check.name,
                        &resolved.command,
                        retry_outcome,
                    )
                    .await?
                {
                    return Ok(GateRunOutcome::InfraRetryExhausted);
                }
                let check_queue_wait_ms = progress.lock().unwrap().queue_wait_ms();
                self.record_check_verification_span(
                    entry,
                    CheckVerificationSpan {
                        check_name: &check.name,
                        attempt: check_attempt,
                        candidate: tested_sha,
                        full_check_required,
                        queue_wait_ms: check_queue_wait_ms,
                        duration_ms: u64::try_from(check_started.elapsed().as_millis()).ok(),
                        proof_reused: false,
                    },
                );
                queue_wait_ms.push((check.name.clone(), check_queue_wait_ms));
                check_proof_keys.push((
                    check.name.clone(),
                    verification_proof_key(&entry.repo_name, tested_sha, &check),
                ));
                passed_checks.push(check.name.clone());
                continue;
            }

            if let Some(reused) = self
                .reusable_verification_proof(entry, git_repo, tested_sha, &check)
                .await?
            {
                self.record_verification_proof_reuse(entry, tested_sha, &check, &reused);
                self.record_check_verification_span(
                    entry,
                    CheckVerificationSpan {
                        check_name: &check.name,
                        attempt: check_attempt,
                        candidate: tested_sha,
                        full_check_required,
                        queue_wait_ms: None,
                        duration_ms: None,
                        proof_reused: true,
                    },
                );
                queue_wait_ms.push((check.name.clone(), None));
                check_proof_keys.push((
                    check.name.clone(),
                    verification_proof_key(&entry.repo_name, tested_sha, &check),
                ));
                passed_checks.push(check.name.clone());
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
                    Some(Arc::clone(&progress)),
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
                    self.record_gate_infra_attempt(
                        entry,
                        tested_sha,
                        &check.name,
                        &resolved.command,
                        1,
                        &result,
                        false,
                    )?;
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
                            Some(Arc::clone(&progress)),
                        )
                        .await;
                    if !self
                        .finish_infra_retry(
                            entry,
                            tested_sha,
                            &check.name,
                            &resolved.command,
                            retry_outcome,
                        )
                        .await?
                    {
                        return Ok(GateRunOutcome::InfraRetryExhausted);
                    }
                }
                Ok(_) => return Ok(GateRunOutcome::Fail),
                Err(e) => {
                    // Only reachable when `on_timeout: Fail` turns a blown
                    // budget into an Err — `record_gate_failure` already ran
                    // before it did. Any other run_check_in Err (a `sh` that
                    // could not even spawn) is an infra fault, not a verdict
                    // on the branch, but is treated the same way here:
                    // fail-closed, hold rather than land.
                    warn!(error = %e, check = %check.name, branch = %entry.branch, "landing pipeline: gate errored, holding branch");
                    return Ok(GateRunOutcome::Fail);
                }
            }
            let check_queue_wait_ms = progress.lock().unwrap().queue_wait_ms();
            self.record_check_verification_span(
                entry,
                CheckVerificationSpan {
                    check_name: &check.name,
                    attempt: check_attempt,
                    candidate: tested_sha,
                    full_check_required,
                    queue_wait_ms: check_queue_wait_ms,
                    duration_ms: u64::try_from(check_started.elapsed().as_millis()).ok(),
                    proof_reused: false,
                },
            );
            queue_wait_ms.push((check.name.clone(), check_queue_wait_ms));
            check_proof_keys.push((
                check.name.clone(),
                verification_proof_key(&entry.repo_name, tested_sha, &check),
            ));
            passed_checks.push(check.name);
        }
        self.space.out(
            Tuple::new(
                Category::Event,
                entry.repo_name.clone(),
                GATE_PASS_IDENTITY,
                "daemon",
                json!({
                    "branch": entry.branch,
                    "target": entry.target,
                    "task": entry.task,
                    "head_sha": entry.head_sha,
                    "candidate_sha": tested_sha,
                    "checks": passed_checks,
                    "duration_ms": u64::try_from(started.elapsed().as_millis())
                        .unwrap_or(u64::MAX),
                    "edge_class": edge_class.as_str(),
                    "full_check_required": full_check_required,
                    "proof_key": proof_key,
                    "check_proof_keys": check_proof_keys
                        .iter()
                        .map(|(name, key)| (name.clone(), json!(key)))
                        .collect::<serde_json::Map<String, Value>>(),
                    "queue_wait_ms": queue_wait_ms
                        .iter()
                        .map(|(name, wait)| (name.clone(), json!(wait)))
                        .collect::<serde_json::Map<String, Value>>(),
                }),
            )
            .with_lifecycle(Lifecycle::Furniture),
        )?;
        Ok(GateRunOutcome::Pass)
    }

    /// Look for a durable, reusable managed-verification proof for one
    /// gate-plan check under this candidate's EXACT identity — repo,
    /// candidate/head sha, check name, command, toolchain, and environment
    /// policy (TKT-01M0QRZ7QT8CQD74GHRN81XFT5) — so an already-proven check
    /// is never re-run inside the gate worktree.
    ///
    /// Tries two shas, in order:
    /// 1. `tested_sha` itself (the prepared merge commit) — an exact hit
    ///    here covers a replayed/restarted gate run against the identical
    ///    candidate, and (via `lookup_verification_proof`'s own secondary
    ///    fallback) an earlier `landing_gate_pass` for this exact candidate.
    /// 2. `entry.head_sha` — the pre-merge branch tip a rat's own managed
    ///    `rk verify` actually executed against. Reusing a proof recorded
    ///    for `head_sha` is sound only when merging changed nothing
    ///    relative to it: `git merge --no-ff` ([`rk_git::Repo::prepare_merge`])
    ///    always builds a fresh commit distinct from either parent, so
    ///    `tested_sha` can never literally equal `head_sha`, but when
    ///    `entry.candidate_base` (the target tip this merge was built on)
    ///    is already an ancestor of `head_sha`, `head_sha` already contains
    ///    everything `base` could have added — the merge is a fast-forward
    ///    forced into a merge commit, and its tree is byte-for-byte
    ///    `head_sha`'s tree. If `base` is NOT an ancestor of `head_sha`,
    ///    `target` moved with content `head_sha` never saw, so this fails
    ///    closed (returns `None`, the caller runs the check fresh) rather
    ///    than risk reusing a proof for a tree the candidate does not
    ///    actually have — the ticket's "main movement changes the prepared
    ///    candidate" requirement.
    ///
    /// Never attempted for a multi-branch batch candidate
    /// (`entry.batch_branches` naming more than one branch): `entry.head_sha`
    /// there names only ONE of several merged branches, so even a clean
    /// ancestor check would say nothing about the OTHER branches' content
    /// folded into `tested_sha`.
    ///
    /// A cancelled managed run can never surface here: `verify_repo_check`
    /// only ever records `VERIFICATION_PROOF_IDENTITY` after a completed
    /// `"pass"` verdict, never for a cancelled/interrupted run
    /// (`workflow_exec.rs`'s `VERIFICATION_CANCELLED_IDENTITY` doc), so
    /// `lookup_verification_proof` structurally cannot return one.
    async fn reusable_verification_proof(
        &self,
        entry: &LandingQueueEntry,
        git_repo: &rk_git::Repo,
        tested_sha: &str,
        check: &rk_workflow::Check,
    ) -> rk_core::Result<Option<Value>> {
        if let Some(hit) =
            self.engine
                .lookup_verification_proof(&entry.repo_name, tested_sha, check)
        {
            return Ok(Some(hit));
        }
        if entry.batch_branches.len() > 1 || entry.head_sha == tested_sha {
            return Ok(None);
        }
        let Some(base) = entry.candidate_base.clone() else {
            return Ok(None);
        };
        let head_sha = entry.head_sha.clone();
        let ancestor_ok = {
            let git_repo = git_repo.clone();
            let head = head_sha.clone();
            blocking(move || Ok(git_repo.is_ancestor(&base, &head))).await?
        };
        if !ancestor_ok {
            return Ok(None);
        }
        Ok(self
            .engine
            .lookup_verification_proof(&entry.repo_name, &head_sha, check))
    }

    /// Durable telemetry for a landing-gate check that skipped execution via
    /// [`reusable_verification_proof`](Self::reusable_verification_proof).
    fn record_verification_proof_reuse(
        &self,
        entry: &LandingQueueEntry,
        tested_sha: &str,
        check: &rk_workflow::Check,
        proof: &Value,
    ) {
        let _ = self.space.out(
            Tuple::new(
                Category::Event,
                entry.repo_name.clone(),
                VERIFICATION_PROOF_REUSE_IDENTITY,
                "daemon",
                json!({
                    "branch": entry.branch,
                    "target": entry.target,
                    "task": entry.task,
                    "check": check.name,
                    "head_sha": entry.head_sha,
                    "tested_sha": tested_sha,
                    "proof_key": verification_proof_key(&entry.repo_name, tested_sha, check),
                    "reused_from": proof.get("reused_from").cloned().unwrap_or(Value::Null),
                }),
            )
            .with_lifecycle(Lifecycle::Furniture),
        );
    }

    /// Record one check's `Phase::VerificationQueued` span, replacing the
    /// single aggregate span this call site used to write once per landing
    /// entry after the whole gate loop finished (TKT-01M0P974EZZTPMGVP4S0E76NXH's
    /// first cut) with one span per check, so a peer reading the span
    /// substrate sees exactly which check(s) a candidate's admission wait
    /// and run time went to. `attempt` is the check's position in this
    /// gate plan (`check_attempt` at each call site), not a rework-round
    /// counter — see that call site's comment for why. `lane` carries the
    /// check name: the one field `record_phase_span`'s `(task, phase,
    /// attempt)` idempotency key does not itself vary by, so it is purely
    /// descriptive here, not a dedup discriminant.
    fn record_check_verification_span(
        &self,
        entry: &LandingQueueEntry,
        occurrence: CheckVerificationSpan<'_>,
    ) {
        let CheckVerificationSpan {
            check_name,
            attempt,
            candidate,
            full_check_required,
            queue_wait_ms,
            duration_ms,
            proof_reused,
        } = occurrence;
        let _ = crate::span::record_phase_span(
            &self.space,
            &entry.repo_name,
            "daemon",
            &crate::span::PhaseSpan::from_durations(
                &entry.task,
                crate::span::Phase::VerificationQueued,
                queue_wait_ms,
                duration_ms,
                Utc::now(),
            )
            .attempt(attempt)
            .repo(&entry.repo_name)
            .target(&entry.target)
            .candidate(candidate)
            .lane(check_name)
            .proof_kind(if full_check_required {
                "full-final"
            } else {
                "focused-inner"
            })
            .proof_reused(proof_reused),
        );
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
        command: &str,
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
        // Evidence FIRST, marker cleared second: the durable evidence is what
        // `settled_infra_retry` reads to recognise an already-settled retry, so
        // a crash between these two writes resumes as "nothing left to run"
        // rather than replaying a spent attempt. The reverse order would lose
        // the outcome entirely if the crash landed between them.
        self.record_gate_infra_attempt(entry, tested_sha, check_name, command, 2, &result, false)?;
        self.clear_infra_retry_marker(entry)?;
        Ok(passed)
    }

    /// Guarantee ordinal-1 evidence exists for a retry this process is about
    /// to resume after a crash. `run_gates_at`'s fresh-infra-death branch
    /// durably persists `gate_infra_retry_used`/`gate_infra_retry_check`
    /// BEFORE writing the ordinal-1 evidence event — deliberately, so a crash
    /// after the persist never grants a duplicate retry on restart. But that
    /// same ordering means a crash landing between the persist and the
    /// evidence write leaves the marker set with no ordinal-1 record at all.
    ///
    /// The original attempt's exact exit/signal died with the crashed
    /// process's memory and cannot be recovered — this reconstructs the
    /// event from what IS durable (queue seq, candidate SHA, check name and
    /// command, the "infra" classification implied by the marker existing at
    /// all, ordinal 1, disposition "retrying"), flagged `reconstructed: true`
    /// so a reader can tell it apart from a directly observed attempt.
    /// Idempotent — a no-op once real or previously-reconstructed ordinal-1
    /// evidence is durable — so a crash-loop resume never duplicates it, and
    /// it never re-runs the check itself.
    fn ensure_infra_retry_ordinal1_evidence(
        &self,
        entry: &LandingQueueEntry,
        tested_sha: &str,
        check_name: &str,
        command: &str,
    ) -> rk_core::Result<()> {
        if self.has_gate_infra_evidence(entry, tested_sha, check_name, 1)? {
            return Ok(());
        }
        warn!(
            check = %check_name, branch = %entry.branch,
            "landing pipeline: resuming an infra retry with no ordinal-1 evidence — reconstructing it from the durable marker"
        );
        self.record_gate_infra_attempt(
            entry,
            tested_sha,
            check_name,
            command,
            1,
            &json!({"verdict": "infra", "exit": Value::Null, "signal": Value::Null}),
            true,
        )
    }

    /// Whether durable evidence for `(branch, target, task, candidate_sha,
    /// check, seq)` already exists at exactly `ordinal` — the existence
    /// check [`Self::ensure_infra_retry_ordinal1_evidence`] uses to stay
    /// idempotent. Scoped identically to [`Self::settled_infra_retry`]; see
    /// that method's doc for why `seq` is part of the match.
    fn has_gate_infra_evidence(
        &self,
        entry: &LandingQueueEntry,
        tested_sha: &str,
        check_name: &str,
        ordinal: u64,
    ) -> rk_core::Result<bool> {
        let pattern = Pattern::category(Category::Event)
            .identity(GATE_INFRA_RETRY_IDENTITY)
            .scope(&entry.repo_name);
        Ok(self.space.scan(&pattern)?.into_iter().any(|t| {
            let p = &t.payload;
            p.get("ordinal").and_then(Value::as_u64) == Some(ordinal)
                && p.get("branch").and_then(Value::as_str) == Some(entry.branch.as_str())
                && p.get("target").and_then(Value::as_str) == Some(entry.target.as_str())
                && p.get("task").and_then(Value::as_str) == Some(entry.task.as_str())
                && p.get("candidate_sha").and_then(Value::as_str) == Some(tested_sha)
                && p.get("check").and_then(Value::as_str) == Some(check_name)
                && p.get("seq").and_then(Value::as_u64) == Some(entry.seq)
        }))
    }

    /// Whether the retry marked in-flight for `check_name` has ALREADY settled
    /// — i.e. its ordinal-2 evidence for this exact candidate is durably
    /// recorded, so only the marker clear was lost to the crash. `Some(passed)`
    /// carries the recorded outcome (the gate may continue iff the retry
    /// passed); `None` means the retry has not settled and must still run.
    ///
    /// Scoped to the exact `(branch, target, task, candidate_sha, check,
    /// seq)` this gate run is settling: evidence from an earlier candidate of
    /// the same branch is a different attempt with its own budget and must
    /// not be read as this one's outcome. `seq` (`LandingQueueEntry::seq`) is
    /// the durable queue-generation discriminator — `LandingQueue::requeue_tail`
    /// can rebuild a stale-target candidate back to the exact same
    /// `candidate_sha` it held before, and without `seq` that rebuilt
    /// generation would silently inherit the previous generation's spent
    /// ordinal-2 evidence and skip the bounded retry it is newly entitled to.
    /// Evidence recorded before this field existed carries no `seq` and so
    /// never matches here — the safe direction, since it only costs an extra
    /// (still-bounded) retry rather than reusing stale evidence.
    fn settled_infra_retry(
        &self,
        entry: &LandingQueueEntry,
        tested_sha: &str,
        check_name: &str,
    ) -> rk_core::Result<Option<bool>> {
        let pattern = Pattern::category(Category::Event)
            .identity(GATE_INFRA_RETRY_IDENTITY)
            .scope(&entry.repo_name);
        Ok(self.space.scan(&pattern)?.into_iter().find_map(|t| {
            let p = &t.payload;
            let matches = p.get("ordinal").and_then(Value::as_u64) == Some(2)
                && p.get("branch").and_then(Value::as_str) == Some(entry.branch.as_str())
                && p.get("target").and_then(Value::as_str) == Some(entry.target.as_str())
                && p.get("task").and_then(Value::as_str) == Some(entry.task.as_str())
                && p.get("candidate_sha").and_then(Value::as_str) == Some(tested_sha)
                && p.get("check").and_then(Value::as_str) == Some(check_name)
                && p.get("seq").and_then(Value::as_u64) == Some(entry.seq);
            matches.then(|| p.get("verdict").and_then(Value::as_str) == Some("pass"))
        }))
    }

    /// Drop the in-flight retry marker and persist it, leaving
    /// `gate_infra_retry_used` spent. Both settlement paths — a retry this
    /// process ran, and one it found already settled — end here, so the
    /// durable state after either is identical.
    fn clear_infra_retry_marker(&self, entry: &mut LandingQueueEntry) -> rk_core::Result<()> {
        entry.gate_infra_retry_check = None;
        self.queue.persist(entry, LandingEntryStatus::RunningGates)
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
    ///
    /// `reconstructed` is `true` only for an ordinal-1 event synthesized by
    /// [`Self::ensure_infra_retry_ordinal1_evidence`] after a crash destroyed
    /// the original attempt's in-memory result — `false` for every event
    /// recorded from a result this process actually observed (both ordinals
    /// on the normal path).
    #[allow(clippy::too_many_arguments)]
    fn record_gate_infra_attempt(
        &self,
        entry: &LandingQueueEntry,
        tested_sha: &str,
        check_name: &str,
        command: &str,
        ordinal: u32,
        result: &Value,
        reconstructed: bool,
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
                    // The durable per-repo enqueue sequence for THIS queue
                    // generation (`LandingQueueEntry::seq`) — constant across
                    // `claim_next`/`set_status`/`persist`, but reassigned
                    // fresh by `LandingQueue::requeue_tail` whenever a
                    // candidate is rebuilt. A requeue can rebuild the exact
                    // same `candidate_sha` (e.g. after a stale-target retry),
                    // so branch/target/task/candidate_sha/check alone cannot
                    // tell a fresh generation's ordinal-2 evidence apart from
                    // a prior generation's — `seq` is what does.
                    "seq": entry.seq,
                    "check": check_name,
                    // The RESOLVED command this attempt actually ran, not just
                    // the check's name: the evidence has to say what was
                    // executed, since a name alone cannot be replayed by hand
                    // or checked against the checks.cue of the day.
                    "command": command,
                    "ordinal": ordinal,
                    "exit": result.get("exit").cloned().unwrap_or(Value::Null),
                    "signal": result.get("signal").cloned().unwrap_or(Value::Null),
                    "verdict": verdict,
                    "disposition": disposition,
                    // `true` only for an ordinal-1 event this process never
                    // itself observed — reconstructed on resume from the
                    // durable marker after a crash lost the original result.
                    "reconstructed": reconstructed,
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
        let gate_plan = self
            .resolve_gate_plan_at(entry, git_repo, gates, &head_sha)
            .await?;
        Ok(self
            .execute_gate_plan_at(entry, git_repo, gate_plan, &head_sha)
            .await?
            .passed())
    }

    /// Test convenience for cases that exercise an exact prepared candidate.
    /// Production resolves the CUE plan in `process_entry`/`process_batch`
    /// and passes the immutable plan directly to `execute_gate_plan_at`.
    #[cfg(test)]
    async fn run_gates_at(
        &self,
        entry: &mut LandingQueueEntry,
        git_repo: &rk_git::Repo,
        gates: &GateConfig,
        tested_sha: &str,
    ) -> rk_core::Result<GateRunOutcome> {
        let gate_plan = self
            .resolve_gate_plan_at(entry, git_repo, gates, tested_sha)
            .await?;
        self.execute_gate_plan_at(entry, git_repo, gate_plan, tested_sha)
            .await
    }

    /// Resolve the protected-paths/diff-scope policy gates plus this edge's
    /// selected checks into `(check, env, timeout)` triples, in the order
    /// they must run — same registry lookup `WorkflowEngine::find_check`
    /// does, reimplemented here because that method is private to
    /// `workflow_exec` and this pipeline has no `run` step / `ctx.active_agent`
    /// to go through.
    ///
    /// `target`'s edge class (`GateConfig::protected_targets`) decides what
    /// follows the two policy gates: a PROTECTED-FINAL target always gets
    /// the full `gates.check_name` check (preserving the pre-existing
    /// behavior for every repo that never configures this policy — see
    /// `default_protected_targets`); an INNER target instead gets whatever
    /// `GateConfig::focused_checks` selects for `changed_paths`, which may be
    /// nothing at all — this pipeline never silently falls back to the full
    /// suite for an edge policy declined to name. Returns the plan alongside
    /// the [`LandingEdgeClass`], whether the full check ran, and a
    /// human-readable reason for both — [`LandingPipeline::run_gates_at`]
    /// records all four durably before executing anything.
    fn gate_plan(
        &self,
        checks_file: &Path,
        target: &str,
        gates: &GateConfig,
        changed_paths: &[String],
    ) -> rk_core::Result<ResolvedGatePlan> {
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

        let mut checks = vec![
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
        ];

        let is_protected_final = gates.protected_targets.iter().any(|t| t == target);
        let (edge_class, full_check_required, reason) = if is_protected_final {
            let verify = find(&gates.check_name)?;
            checks.push((verify, Vec::new(), gates.gate_timeout));
            (
                LandingEdgeClass::ProtectedFinal,
                true,
                format!(
                    "target `{target}` is a protected final target (protectedTargets); running \
                     the full `{}` check",
                    gates.check_name
                ),
            )
        } else {
            let (selected, reasons) = select_focused_checks(&gates.focused_checks, changed_paths);
            if selected.is_empty() {
                (
                    LandingEdgeClass::Inner,
                    false,
                    format!(
                        "target `{target}` is not a protected final target and no focusedChecks \
                         rule matched; running no check beyond protected-paths/diff-scope"
                    ),
                )
            } else {
                for check_name in &selected {
                    let check = find(check_name)?;
                    checks.push((
                        check,
                        vec![("RK_CHECK_TARGET".to_string(), target.to_string())],
                        gates.gate_timeout,
                    ));
                }
                (
                    LandingEdgeClass::Inner,
                    false,
                    format!(
                        "target `{target}` is not a protected final target; running \
                         policy-selected focused checks: {}",
                        reasons.join("; ")
                    ),
                )
            }
        };

        Ok(ResolvedGatePlan {
            checks,
            edge_class,
            full_check_required,
            reason,
        })
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

    /// Overwrites `checks.cue` WITHOUT committing — `LandingPipeline::gate_plan`
    /// reads this file straight off disk (`repo_path`, the registered repo's
    /// own working directory), not from the git tree of any tested candidate
    /// sha, so a caller can change what a check runs between two
    /// `run_gates_at` calls against the identical candidate without touching
    /// git history at all. Used to reproduce that exact gap.
    fn write_checks_uncommitted(repo: &Path, src: &str) {
        std::fs::write(repo.join(".rk").join("checks.cue"), src).unwrap();
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
        test_engine_routed(
            layout,
            supervisor,
            space,
            tickets,
            HashMap::new(),
            TierRouting::default(),
        )
    }

    fn test_engine_routed(
        layout: Layout,
        supervisor: Arc<Supervisor>,
        space: Space,
        tickets: Arc<Tickets>,
        global_agents: HashMap<String, rk_workflow::AgentProfile>,
        tiers: TierRouting,
    ) -> Arc<WorkflowEngine> {
        Arc::new(WorkflowEngine::new(
            layout,
            supervisor,
            space,
            tickets,
            global_agents,
            tiers,
            "fake".into(),
            false,
            true,
            false,
            0,
            false,
        ))
    }

    fn test_pipeline(home: &Path, space: Space) -> LandingPipeline {
        test_pipeline_routed(home, space, HashMap::new(), TierRouting::default())
    }

    /// [`test_pipeline`] with the daemon's GLOBAL agent profiles and cost-tier
    /// routing table populated — the two inputs `WorkflowEngine` consults when
    /// resolving a spawn, and therefore the only way to exercise reviewer tier
    /// routing on the real landing path rather than through a synthetic
    /// workflow that declares its own `tiers` block.
    fn test_pipeline_routed(
        home: &Path,
        space: Space,
        global_agents: HashMap<String, rk_workflow::AgentProfile>,
        tiers: TierRouting,
    ) -> LandingPipeline {
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
        let engine = test_engine_routed(
            layout.clone(),
            supervisor.clone(),
            space.clone(),
            tickets.clone(),
            global_agents,
            tiers,
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

    /// A review-only workflow that dies on its PRIMARY attempt (a `stop`
    /// step, same stand-in as [`write_broken_review_workflow`]) but takes the
    /// normal wait/evaluate path — same 2s timer gate as
    /// [`write_review_workflow`] — on every REVIEW-DEATH RETRY attempt,
    /// distinguished purely by `_input.reviewAttempt` carrying `-retry`
    /// (`review_retry_instance_id`'s own suffix). Lets a test drive the whole
    /// primary-dies-then-replacement-succeeds path against one static
    /// workflow definition, the same way `write_review_workflow_with_shadow_death`
    /// branches on `reviewerModel` to isolate the shadow arm.
    fn write_review_workflow_dies_on_primary_recovers_on_retry(layout: &Layout) {
        let dir = layout.workflows_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("steward-review.cue"),
            r#"
package workflow

import (
	"list"
	"strings"
)

workflow: {
	name: "steward-review"
	params: {
		taskId:        {type: "string", required: false, default: "unknown"}
		branch:        {type: "string", required: true}
		repo:          {type: "string", required: false, default: "rat-kingdom"}
		target:        {type: "string", required: false, default: "main"}
		headSha:       {type: "string", required: false, default: ""}
		reviewTimeout: {type: "string", required: false, default: "15m"}
		reviewAttempt: {type: "string", required: false, default: ""}
	}
	agents: {
		default: {harness: "fake", model: "sonnet"}
	}
	_isRetry: strings.Contains(_input.reviewAttempt, "-retry")
	_spawn: {
		type:   "spawn"
		role:   "reviewer"
		branch: _input.branch
		task: {title: "review", description: "review it"}
	}
	steps: list.Concat([
		[_spawn],
		if !_isRetry {[{type: "stop", reason: "simulated reviewer crash: harness exited without reporting"}]},
		if _isRetry {[
			{type: "gate", gateType: "timer", duration: "2s"},
			{type: "wait", timeout: _input.reviewTimeout},
			{type: "evaluate", expect: {is_error: false}},
		]},
	])
}
"#,
        )
        .unwrap();
    }

    /// [`write_review_workflow`] predates the P4a live-path wiring and never
    /// declares `priority`/`labels`/`reviewerModel`/`reviewerHarness` on its
    /// spawn step, so it cannot exercise either cost-tier routing or shadow
    /// review. This mirrors `examples/workflows/steward-review.cue`'s actual
    /// wiring of those four params onto the spawn step: `priority`/`labels`
    /// are the tier-routing predicate; `reviewerModel`/`reviewerHarness` are
    /// empty for the primary reviewer (leaving the tier table / `reviewer`
    /// profile to decide) and set only for the shadow, whose inline
    /// model/harness beats the tier table — the same precedence the real
    /// workflow relies on to pin the shadow to its configured model.
    fn write_routed_review_workflow(layout: &Layout) {
        let dir = layout.workflows_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("steward-review.cue"),
            r#"
package workflow

workflow: {
	name: "steward-review"
	params: {
		taskId:          {type: "string", required: false, default: "unknown"}
		branch:          {type: "string", required: true}
		repo:            {type: "string", required: false, default: "rat-kingdom"}
		target:          {type: "string", required: false, default: "main"}
		headSha:         {type: "string", required: false, default: ""}
		reviewTimeout:   {type: "string", required: false, default: "15m"}
		priority:        {type: "string", required: false, default: ""}
		labels:          {type: "list",   required: false, default: []}
		reviewerModel:   {type: "string", required: false, default: ""}
		reviewerHarness: {type: "string", required: false, default: ""}
	}
	agents: {
		default:  {harness: "fake", model: "sonnet"}
		reviewer: {harness: "fake", model: "reviewer-model"}
	}
	steps: [
		{
			type:   "spawn"
			role:   "reviewer"
			agent:  "reviewer"
			branch: _input.branch
			labels: _input.labels
			if _input.priority != "" {
				priority: _input.priority
			}
			if _input.reviewerModel != "" {
				model: _input.reviewerModel
			}
			if _input.reviewerHarness != "" {
				harness: _input.reviewerHarness
			}
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

    /// [`write_routed_review_workflow`] with one difference: a spawn carrying
    /// a non-empty `reviewerModel` — which only the SHADOW spawn ever sets
    /// (see [`LandingPipeline::launch_shadow_review`]) — hits `stop` instead
    /// of the normal wait/evaluate tail. A deterministic stand-in for "the
    /// shadow reviewer died" that leaves the PRIMARY arm (empty
    /// `reviewerModel`) completely unaffected, so a test using this fixture
    /// proves shadow death in isolation rather than killing both reviewers.
    fn write_review_workflow_with_shadow_death(layout: &Layout) {
        let dir = layout.workflows_dir();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("steward-review.cue"),
            r#"
package workflow

import "list"

workflow: {
	name: "steward-review"
	params: {
		taskId:          {type: "string", required: false, default: "unknown"}
		branch:          {type: "string", required: true}
		repo:            {type: "string", required: false, default: "rat-kingdom"}
		target:          {type: "string", required: false, default: "main"}
		headSha:         {type: "string", required: false, default: ""}
		reviewTimeout:   {type: "string", required: false, default: "15m"}
		priority:        {type: "string", required: false, default: ""}
		labels:          {type: "list",   required: false, default: []}
		reviewerModel:   {type: "string", required: false, default: ""}
		reviewerHarness: {type: "string", required: false, default: ""}
	}
	agents: {
		default:  {harness: "fake", model: "sonnet"}
		reviewer: {harness: "fake", model: "reviewer-model"}
	}
	_isShadow: _input.reviewerModel != ""
	_spawn: {
		type:   "spawn"
		role:   "reviewer"
		agent:  "reviewer"
		branch: _input.branch
		labels: _input.labels
		if _input.priority != "" {
			priority: _input.priority
		}
		if _input.reviewerModel != "" {
			model: _input.reviewerModel
		}
		if _input.reviewerHarness != "" {
			harness: _input.reviewerHarness
		}
		task: {title: "review", description: "review it"}
	}
	steps: list.Concat([
		[_spawn],
		if _isShadow {[{type: "stop", reason: "simulated shadow reviewer crash"}]},
		if !_isShadow {[
			{type: "gate", gateType: "timer", duration: "2s"},
			{type: "wait", timeout: _input.reviewTimeout},
			{type: "evaluate", expect: {is_error: false}},
		]},
	])
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
            source_spawn: Some(rk_core::id::SpawnId::new()),
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
        assert_eq!(requeued.source_spawn, base.source_spawn);
        assert!(
            requeued.age_secs >= 5 * 3600 - 5,
            "requeue must not reset age, got {}",
            requeued.age_secs
        );
    }

    /// The per-phase clock is a SEPARATE reading from total queue age, and
    /// the two must diverge exactly at a phase boundary: `phase_age_secs`
    /// restarts, `age_secs` keeps counting. Reusing the cumulative one as
    /// the current phase's elapsed time is what made the phase-latency
    /// sweep fire an instant false-positive review breach on a candidate
    /// that had merely spent a long-but-healthy wait in verification.
    #[test]
    fn phase_clock_restarts_across_a_phase_boundary_while_total_age_keeps_running() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let space = Space::open_in_memory().unwrap();
        let queue = LandingQueue::new(space.clone(), &layout);

        // A candidate that has been in the verification lane for 15m.
        let fifteen_min_ago = Utc::now() - chrono::Duration::minutes(15);
        let base = LandingQueueEntry {
            repo_name: "alpha".into(),
            repo_path: "/repos/alpha".into(),
            branch: "b1".into(),
            target: "main".into(),
            head_sha: "sha-old".into(),
            diff_class: "trivial".into(),
            task: "TKT-1".into(),
            enqueued_at: Some(fifteen_min_ago),
            phase_entered_at: Some(fifteen_min_ago),
            ..Default::default()
        };
        queue.enqueue(base).unwrap();

        let phase_age = |branch: &str| {
            landing_queue_snapshot(&space)
                .into_iter()
                .find(|e| e.branch == branch)
                .map(|e| (e.age_secs, e.phase_age_secs))
                .unwrap()
        };
        let (age, phase) = phase_age("b1");
        assert!(age >= 15 * 60 - 5 && phase >= 15 * 60 - 5);

        // Queued -> RunningGates is NOT a phase boundary (both are
        // `VerificationQueued`): the verification clock must keep running,
        // or a wedged gate lane would reset itself out of every target.
        let claimed = queue.claim_next("alpha", "main").unwrap().unwrap();
        assert_eq!(claimed.status, LandingEntryStatus::RunningGates);
        assert_eq!(claimed.phase_entered_at, Some(fifteen_min_ago));
        let (age, phase) = phase_age("b1");
        assert!(
            age >= 15 * 60 - 5 && phase >= 15 * 60 - 5,
            "same-phase transition must not reset the phase clock, got {phase}"
        );

        // RunningGates -> AwaitingReview IS a phase boundary
        // (`VerificationQueued` -> `SemanticReview`): review just started,
        // so its clock is ~0 even though the candidate is still 15m old.
        queue
            .set_status(&claimed, LandingEntryStatus::AwaitingReview)
            .unwrap();
        let (age, phase) = phase_age("b1");
        assert!(age >= 15 * 60 - 5, "total queue age keeps counting");
        assert!(
            phase < 60,
            "review clock must start at the transition, not inherit 15m of \
             verification wait, got {phase}"
        );
    }

    /// A requeue back into `Queued` from `RunningGates` stays inside the
    /// verification phase, so — exactly like `enqueued_at` — the phase clock
    /// survives it: a candidate stuck in a stale-target requeue loop must not
    /// look freshly-phased on every cycle, which is the wedge probe O18 wants
    /// surfaced rather than hidden.
    #[test]
    fn requeue_within_the_verification_phase_carries_the_phase_clock_forward() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let space = Space::open_in_memory().unwrap();
        let queue = LandingQueue::new(space.clone(), &layout);

        let fifteen_min_ago = Utc::now() - chrono::Duration::minutes(15);
        queue
            .enqueue(LandingQueueEntry {
                repo_name: "alpha".into(),
                repo_path: "/repos/alpha".into(),
                branch: "b1".into(),
                target: "main".into(),
                head_sha: "sha-old".into(),
                diff_class: "trivial".into(),
                task: "TKT-1".into(),
                enqueued_at: Some(fifteen_min_ago),
                phase_entered_at: Some(fifteen_min_ago),
                ..Default::default()
            })
            .unwrap();

        let claimed = queue.claim_next("alpha", "main").unwrap().unwrap();
        queue.requeue_tail(&claimed).unwrap();
        // The replacement is written before the claimed row is removed
        // (`requeue_tail`'s crash-safety ordering) — the caller retires the
        // claimed row afterwards, so do the same here or the snapshot sees
        // both generations of this branch.
        queue.remove(&claimed).unwrap();

        let requeued = landing_queue_snapshot(&space)
            .into_iter()
            .find(|e| e.branch == "b1")
            .unwrap();
        assert_eq!(requeued.status, LandingEntryStatus::Queued);
        assert!(
            requeued.phase_age_secs >= 15 * 60 - 5,
            "requeue must not reset the verification clock, got {}",
            requeued.phase_age_secs
        );
    }

    #[test]
    fn persisted_entry_without_a_phase_clock_is_refused() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let space = Space::open_in_memory().unwrap();
        let queue = LandingQueue::new(space.clone(), &layout);

        let payload = json!({
            "repo_name": "alpha",
            "repo_path": "/repos/alpha",
            "branch": "b1",
            "target": "main",
            "head_sha": "sha-old",
            "diff_class": "trivial",
            "task": "TKT-old",
            "seq": 1,
            "rev": 0,
            "status": "queued",
            "enqueued_at": Utc::now() - chrono::Duration::hours(5),
        });
        space
            .out(
                Tuple::new(
                    Category::Event,
                    "alpha".to_string(),
                    LANDING_QUEUE_IDENTITY,
                    "daemon",
                    payload,
                )
                .with_lifecycle(Lifecycle::Furniture),
            )
            .unwrap();

        assert!(landing_queue_snapshot(&space).is_empty());
        let error = queue.claim_next("alpha", "main").unwrap_err();
        assert!(error
            .to_string()
            .contains("predates the exact phase-clock schema"));
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
        let now = Utc::now();
        entry.enqueued_at = Some(now);
        entry.phase_entered_at = Some(now);
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

    /// `record_delivery`'s `Merge` span must carry `ended_at` — without it
    /// `task_to_main_ms` (`crates/rk-cli/src/critical_path.rs`) has no end
    /// anchor and stays `null` forever, even for a ticket that fully landed
    /// (TKT-01M0QZFFT9WFDTG0CS4GVD03QX). A ticket created with no unresolved
    /// dependency already gets a `TicketReady` span at creation
    /// (TKT-01M0QMT83E7YXH6ZXHMQG0VRS6), so landing it end to end is enough
    /// to prove `task_to_main_ms` computes.
    #[tokio::test]
    async fn merge_span_carries_ended_at_and_completes_task_to_main() {
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

        pipeline.drain_key("docs-repo", "main").await.unwrap();

        let stored = pipeline.tickets.get(&ticket.identity).unwrap().unwrap();
        assert!(
            crate::tickets::is_delivered(&stored),
            "expected a real delivery: {stored:?}"
        );
        let record = crate::tickets::delivery_of(&stored).expect("delivery record");

        let spans = crate::span::spans_for_task(&space, "docs-repo", &ticket.identity).unwrap();
        let merge = spans
            .iter()
            .find(|s| s["phase"] == "merge")
            .expect("a merge span must be recorded");
        let merge_ended_at: chrono::DateTime<chrono::Utc> = merge["ended_at"]
            .as_str()
            .expect("merge span must carry ended_at")
            .parse()
            .expect("ended_at must be a valid timestamp");
        let record_landed_at: chrono::DateTime<chrono::Utc> =
            record.landed_at.parse().expect("landed_at must parse");
        assert_eq!(
            merge_ended_at, record_landed_at,
            "the merge span's ended_at must match the delivery record's landed_at: {merge:?}"
        );

        // `rk-cli`'s `build_critical_path` anchors `task_to_main_ms` on
        // `ticket_ready.queued_at|started_at` and `merge.ended_at`; asserting
        // both are present here (without depending on rk-cli from rk-daemon)
        // proves that computation now has both endpoints to work with.
        let ticket_ready = spans
            .iter()
            .find(|s| s["phase"] == "ticket_ready")
            .expect("ticket creation with no unresolved dependency must stamp ticket_ready");
        assert!(
            ticket_ready["queued_at"].is_string() || ticket_ready["started_at"].is_string(),
            "ticket_ready span must carry a start anchor: {ticket_ready:?}"
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
    async fn failed_delivery_finalization_retains_receipt_and_recovers_after_target_moves() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        write_checks(repo_dir.path(), ALL_PASS_CHECKS);
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::create_dir_all(repo_dir.path().join("docs")).unwrap();
        std::fs::write(repo_dir.path().join("docs/note.md"), "note\n").unwrap();
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
                // A missing ticket forces finalization to fail only after the
                // prepared commit has durably advanced the target.
                task: "TKT-missing-finalization-probe".into(),
                keep_branch: true,
                ..Default::default()
            })
            .unwrap();

        let error = pipeline.drain_key("docs-repo", "main").await.unwrap_err();
        assert!(error.to_string().contains("no such ticket"), "{error}");
        let retained = pipeline
            .queue
            .scan_current("docs-repo", Some("main"))
            .unwrap();
        assert_eq!(
            retained.len(),
            1,
            "failed finalization must retain its receipt"
        );
        assert_eq!(retained[0].payload["status"], "landing");
        assert!(pipeline
            .processed_outcome(&LandingQueueEntry {
                repo_name: "docs-repo".into(),
                branch: "feature".into(),
                head_sha: retained[0].payload["head_sha"].as_str().unwrap().into(),
                ..Default::default()
            })
            .unwrap()
            .is_none());

        // A later target commit must not erase the evidence that the exact
        // prepared candidate already landed.
        std::fs::write(repo_dir.path().join("later.txt"), "later\n").unwrap();
        git(repo_dir.path(), &["add", "later.txt"]);
        git(repo_dir.path(), &["commit", "-m", "chore: advance target"]);
        let second = pipeline.drain_key("docs-repo", "main").await.unwrap_err();
        assert!(second.to_string().contains("no such ticket"), "{second}");
        assert_eq!(
            pipeline
                .queue
                .scan_current("docs-repo", Some("main"))
                .unwrap()
                .len(),
            1,
            "ancestry recovery must keep retrying finalization, not mark the candidate empty"
        );
        assert_eq!(
            space
                .scan(
                    &Pattern::category(Category::Event)
                        .scope("docs-repo")
                        .identity("landing_finalization_failed")
                )
                .unwrap()
                .len(),
            1,
            "retries must deduplicate operator visibility"
        );
    }

    #[tokio::test]
    async fn landed_batch_recovers_without_gating_or_advancing_again() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        // If recovery accidentally returns to the gate path, this check fails.
        write_checks(repo_dir.path(), VERIFY_FAILS_CHECKS);
        for (branch, file) in [("feature-a", "a.md"), ("feature-b", "b.md")] {
            git(repo_dir.path(), &["checkout", "main"]);
            git(repo_dir.path(), &["checkout", "-b", branch]);
            std::fs::write(repo_dir.path().join(file), format!("{branch}\n")).unwrap();
            git(repo_dir.path(), &["add", file]);
            git(
                repo_dir.path(),
                &["commit", "-m", &format!("docs: {branch}")],
            );
        }
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space);
        for branch in ["feature-a", "feature-b"] {
            pipeline
                .enqueue(LandingQueueEntry {
                    repo_name: "docs-repo".into(),
                    repo_path: repo_dir.path().display().to_string(),
                    branch: branch.into(),
                    target: "main".into(),
                    head_sha: rev_parse(repo_dir.path(), branch),
                    diff_class: "doc-only".into(),
                    task: format!("deliver-{branch}"),
                    keep_branch: true,
                    ..Default::default()
                })
                .unwrap();
        }
        let mut entries = pipeline.queue.claim_batch("docs-repo", "main", 8).unwrap();
        let repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let branches = entries
            .iter()
            .map(|entry| entry.branch.clone())
            .collect::<Vec<_>>();
        let rk_git::PrepareOutcome::Prepared(candidate) =
            repo.prepare_merge_batch(&branches, "main").unwrap()
        else {
            panic!("batch must prepare cleanly");
        };
        for entry in &mut entries {
            entry.candidate_sha = Some(candidate.commit.clone());
            entry.candidate_base = Some(candidate.base.clone());
            entry.candidate_ref = Some(candidate.candidate_ref.clone());
            entry.batch_branches = branches.clone();
            pipeline
                .queue
                .persist(entry, LandingEntryStatus::Landing)
                .unwrap();
        }
        let advanced = repo
            .advance_target_to("main", &candidate.commit, &candidate.base)
            .unwrap();
        assert!(matches!(advanced, rk_git::AdvanceOutcome::Advanced { .. }));
        std::fs::write(repo_dir.path().join("later.txt"), "later\n").unwrap();
        git(repo_dir.path(), &["add", "later.txt"]);
        git(
            repo_dir.path(),
            &["commit", "-m", "chore: advance after batch"],
        );

        let outcomes = pipeline.drain_key("docs-repo", "main").await.unwrap();
        assert_eq!(outcomes.len(), 2);
        assert!(outcomes
            .iter()
            .all(|outcome| matches!(outcome, LandingOutcome::Landed(_))));
        assert!(pipeline
            .queue
            .scan_current("docs-repo", Some("main"))
            .unwrap()
            .is_empty());
    }

    /// TKT-01M0EHFDGZQDZM0CF4E04G6JKA: an approved candidate landing onto a
    /// non-main target that is checked out in a live linked worktree (an
    /// agent sitting on its own branch, as in the Peanut-9 rework chain)
    /// with a GENUINE conflicting uncommitted edit must fail closed — ref
    /// untouched, edit untouched — and raise exactly one durable human
    /// recovery gate, not silently land behind the checkout.
    #[tokio::test]
    async fn approved_landing_blocked_by_a_dirty_checked_out_target_escalates_durably() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        write_checks(repo_dir.path(), ALL_PASS_CHECKS);

        // The rework target: a non-main branch checked out in its own live
        // (linked) worktree, exactly like an agent's worktree.
        git(repo_dir.path(), &["branch", "rat/peanut-9/tkt-rework"]);
        let agent_wt = tempfile::tempdir().unwrap();
        git(
            repo_dir.path(),
            &[
                "worktree",
                "add",
                agent_wt.path().to_str().unwrap(),
                "rat/peanut-9/tkt-rework",
            ],
        );
        // Genuine uncommitted edit in that worktree, on the same file the
        // incoming fix also touches — `--ff-only` cannot silently preserve it.
        std::fs::write(
            agent_wt.path().join("README.md"),
            "agent's in-flight edit\n",
        )
        .unwrap();

        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("README.md"), "# fixed\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "docs: fix readme"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "rework-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "feature".into(),
                target: "rat/peanut-9/tkt-rework".into(),
                head_sha,
                diff_class: "doc-only".into(),
                task: "fix readme".into(),
                ..Default::default()
            })
            .unwrap();

        let before = rev_parse(repo_dir.path(), "rat/peanut-9/tkt-rework");
        let outcomes = pipeline
            .drain_key("rework-repo", "rat/peanut-9/tkt-rework")
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        let LandingOutcome::Escalated(need) = &outcomes[0] else {
            panic!("expected Escalated, got {:?}", outcomes[0]);
        };
        assert_eq!(need.payload["agent"], "steward");
        let text = need.payload["text"].as_str().unwrap();
        assert!(
            text.contains("could not land"),
            "escalation must explain the blocked land: {text}"
        );
        assert!(
            text.contains(agent_wt.path().to_str().unwrap()),
            "escalation must name the blocked worktree: {text}"
        );

        // Fail closed: the ref never moved, and the agent's genuine edit
        // survives untouched — no automated reset or overwrite.
        let after = rev_parse(repo_dir.path(), "rat/peanut-9/tkt-rework");
        assert_eq!(
            before, after,
            "target ref must not move behind a dirty checkout"
        );
        assert_eq!(
            std::fs::read_to_string(agent_wt.path().join("README.md")).unwrap(),
            "agent's in-flight edit\n",
            "genuine local edits must never be overwritten or reset"
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

    /// Bounded fail-safe recovery for landing gate infrastructure death
    /// (TKT-01M0FXGQMA10JYCV9QCGEAK4TT): a check whose child is killed by a
    /// signal on its first invocation, but passes on the automatic retry,
    /// must land — and the retry must be visible as exactly two durable
    /// per-attempt evidence events.
    #[tokio::test]
    async fn infra_death_then_pass_retries_once_and_lands() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let attempt_log = home.path().join("infra-attempts.log");
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "echo x >> '{log}'; n=$(wc -l < '{log}'); if [ $n -eq 1 ]; then kill -9 $$; else exit 0; fi", timeout: "30s"}},
]
"#,
                log = attempt_log.display()
            ),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
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
        assert!(
            matches!(&outcomes[0], LandingOutcome::Landed(r) if r["merged"] == true),
            "outcome: {:?}",
            outcomes[0]
        );
        assert_eq!(
            std::fs::read_to_string(&attempt_log)
                .unwrap()
                .lines()
                .count(),
            2,
            "the check must run exactly twice — the death and the one retry"
        );

        let events = space
            .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
            .unwrap();
        assert_eq!(events.len(), 2, "events: {events:?}");
        let by_ordinal = |n: u64| {
            events
                .iter()
                .find(|e| e.payload["ordinal"].as_u64() == Some(n))
                .unwrap_or_else(|| panic!("no event with ordinal {n}: {events:?}"))
        };
        assert_eq!(by_ordinal(1).payload["disposition"], "retrying");
        assert_eq!(by_ordinal(1).payload["verdict"], "infra");
        assert_eq!(by_ordinal(2).payload["disposition"], "retry_passed");
        assert_eq!(by_ordinal(2).payload["verdict"], "pass");
        assert_eq!(by_ordinal(1).payload["check"], "verify");
        assert_eq!(
            by_ordinal(1).payload["candidate_sha"],
            by_ordinal(2).payload["candidate_sha"]
        );
    }

    /// The symmetric failure case: a check that always dies to an
    /// infrastructure fault gets exactly one retry, then holds — never a
    /// second retry — with both attempts durably recorded.
    #[tokio::test]
    async fn infra_death_exhausted_after_one_retry_holds_with_precise_evidence() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let attempt_log = home.path().join("infra-attempts.log");
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "echo x >> '{log}'; kill -9 $$", timeout: "30s"}},
]
"#,
                log = attempt_log.display()
            ),
        );
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
        assert_eq!(
            std::fs::read_to_string(&attempt_log)
                .unwrap()
                .lines()
                .count(),
            2,
            "exactly one retry — never a second"
        );

        let events = space
            .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
            .unwrap();
        assert_eq!(events.len(), 2, "events: {events:?}");
        let by_ordinal = |n: u64| {
            events
                .iter()
                .find(|e| e.payload["ordinal"].as_u64() == Some(n))
                .unwrap_or_else(|| panic!("no event with ordinal {n}: {events:?}"))
        };
        assert_eq!(by_ordinal(1).payload["disposition"], "retrying");
        assert_eq!(by_ordinal(2).payload["disposition"], "retry_exhausted");
        assert_eq!(by_ordinal(2).payload["verdict"], "infra");

        let needs = space
            .scan(
                &Pattern::category(Category::Need)
                    .scope("code-repo")
                    .identity(STEWARD_NEED_IDENTITY),
            )
            .unwrap();
        assert_eq!(needs.len(), 1);
        assert!(
            needs[0].payload["text"]
                .as_str()
                .unwrap()
                .contains("infrastructure-death retry was exhausted"),
            "text: {}",
            needs[0].payload["text"]
        );

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_eq!(main_before, main_after, "branch must not have landed");
    }

    /// The false-positive `landing.rs:1417-1424` was reporting (parent
    /// review TKT-01M0G97GXNHA4VPRRXVMA9T6C8): `entry.gate_infra_retry_used`
    /// stays `true` for the rest of a candidate's gate run once ANY check
    /// spends its retry, even after that retry PASSES. If a later, unrelated
    /// check then fails an ordinary (non-infra) way, the hold text must
    /// describe a plain gate failure — never claim an infrastructure-death
    /// retry was exhausted, since the retry that actually ran succeeded and
    /// has nothing to do with why the branch is held.
    #[tokio::test]
    async fn retry_pass_then_later_ordinary_failure_is_not_misreported_as_exhausted() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let attempt_log = home.path().join("infra-attempts.log");
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "echo x >> '{log}'; n=$(wc -l < '{log}'); if [ $n -eq 1 ]; then kill -9 $$; else exit 0; fi", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "exit 1", timeout: "30s"}},
]
"#,
                log = attempt_log.display()
            ),
        );
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

        // The retry on `steward-protected-paths` passed — the evidence for
        // it must say so.
        let events = space
            .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
            .unwrap();
        assert_eq!(events.len(), 2, "events: {events:?}");
        let by_ordinal = |n: u64| {
            events
                .iter()
                .find(|e| e.payload["ordinal"].as_u64() == Some(n))
                .unwrap_or_else(|| panic!("no event with ordinal {n}: {events:?}"))
        };
        assert_eq!(by_ordinal(1).payload["check"], "steward-protected-paths");
        assert_eq!(by_ordinal(2).payload["disposition"], "retry_passed");
        assert_eq!(by_ordinal(2).payload["verdict"], "pass");

        // A gate-failure artifact is also recorded for the transient infra
        // death itself (verdict "infra"); the one that must decide the hold
        // is `verify`'s ordinary failure — not the retried, now-passing
        // `steward-protected-paths` check.
        let failures = space
            .scan(
                &Pattern::category(Category::Artifact)
                    .scope("code-repo")
                    .identity("gate-failure"),
            )
            .unwrap();
        assert_eq!(failures.len(), 2, "failures: {failures:?}");
        let ordinary_failure = failures
            .iter()
            .find(|f| f.payload["verdict"] == "fail")
            .unwrap_or_else(|| panic!("no ordinary-fail artifact: {failures:?}"));
        assert_eq!(ordinary_failure.payload["command"], "exit 1");

        // The hold text is the crux of the fix: it must NOT blame an
        // exhausted infra retry for a plain, unrelated gate failure.
        let needs = space
            .scan(
                &Pattern::category(Category::Need)
                    .scope("code-repo")
                    .identity(STEWARD_NEED_IDENTITY),
            )
            .unwrap();
        assert_eq!(needs.len(), 1);
        let text = needs[0].payload["text"].as_str().unwrap();
        assert!(
            !text.contains("infrastructure-death retry was exhausted"),
            "text falsely blamed the passed retry for an unrelated ordinary failure: {text}"
        );
        assert!(text.contains("run gate FAILED"), "text: {text}");

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_eq!(main_before, main_after, "branch must not have landed");
    }

    /// An ordinary red check (a real, non-signal exit code) must never be
    /// retried — it holds immediately on the first attempt, with zero
    /// infra-retry evidence events.
    #[tokio::test]
    async fn ordinary_check_failure_is_held_without_infra_retry() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let attempt_log = home.path().join("attempts.log");
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "echo x >> '{log}'; exit 3", timeout: "30s"}},
]
"#,
                log = attempt_log.display()
            ),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
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
        assert!(matches!(outcomes[0], LandingOutcome::GateHeld));
        assert_eq!(
            std::fs::read_to_string(&attempt_log)
                .unwrap()
                .lines()
                .count(),
            1,
            "an ordinary failure must never be retried"
        );
        let events = space
            .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
            .unwrap();
        assert!(events.is_empty(), "events: {events:?}");
    }

    /// A genuine timeout (`onTimeout: fail`, the default) must never be
    /// retried either — it is a policy-declared bound, not an infrastructure
    /// fault, and the acceptance contract explicitly preserves it unretried.
    #[tokio::test]
    async fn timeout_holds_without_infra_retry() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        // `GateConfig::gate_timeout` (the durable bound `run_gates_at`
        // actually enforces for the "verify" check — a named check's own
        // `timeout:` field in checks.cue is metadata only, not what's wired
        // in as the gate's wall-clock bound) is set short directly below
        // rather than through checks.cue, which has no effect on it.
        write_checks(
            repo_dir.path(),
            r#"checks: [
    {name: "steward-protected-paths", command: "true", timeout: "30s"},
    {name: "steward-diff-scope", command: "true", timeout: "30s"},
    {name: "verify", command: "sleep 5", timeout: "30s"},
]
"#,
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig {
            gate_timeout: Duration::from_millis(300),
            ..GateConfig::default()
        };
        let mut entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha,
            diff_class: "doc-only".into(),
            task: "add src".into(),
            ..Default::default()
        };

        let passed = pipeline
            .run_gates(&mut entry, &git_repo, &gates)
            .await
            .unwrap();
        assert!(!passed, "a genuine timeout must hold the branch");
        assert!(
            !entry.gate_infra_retry_used,
            "a timeout must never spend the infra-retry budget"
        );
        let events = space
            .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
            .unwrap();
        assert!(events.is_empty(), "events: {events:?}");
        let failures = space
            .scan(
                &Pattern::category(Category::Artifact)
                    .scope("code-repo")
                    .identity("gate-failure"),
            )
            .unwrap();
        assert_eq!(failures.len(), 1, "failures: {failures:?}");
        assert_eq!(failures[0].payload["verdict"], "timeout");
    }

    /// Restart-safety for the retry itself, not just the surrounding gate
    /// run (TKT-01M0FXGQMA10JYCV9QCGEAK4TT review point 3): a crash landing
    /// AFTER the retry budget is durably spent but BEFORE the retry attempt
    /// finishes must resume as exactly the ordinal-2 completion of that same
    /// retry on restart — recording its final disposition — never a second
    /// ordinal-1 death and never a second retry grant.
    #[tokio::test]
    async fn restart_resumes_in_flight_infra_retry_without_granting_a_second_one() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let attempt_log = home.path().join("infra-attempts.log");
        // Invocation 1 (before restart): dies immediately. Invocation 2
        // (before restart, the retry): logs, then sleeps — giving the test a
        // window to abort mid-retry, simulating the crash — before dying.
        // Invocation 3 (after restart): dies immediately again, so a THIRD
        // invocation ever happening at all would prove a second retry was
        // wrongly granted.
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "echo x >> '{log}'; n=$(wc -l < '{log}'); if [ $n -eq 2 ]; then sleep 5; fi; kill -9 $$", timeout: "30s"}},
]
"#,
                log = attempt_log.display()
            ),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha,
            diff_class: "doc-only".into(),
            task: "add src".into(),
            ..Default::default()
        };

        // "Before restart": a real on-disk Space, claimed and mid-retry when
        // the hosting task is aborted (the crash).
        {
            let space = Space::open(&layout.db_path()).unwrap();
            let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
            pipeline.enqueue(entry.clone()).unwrap().unwrap();
            let handle = tokio::spawn({
                let pipeline = Arc::clone(&pipeline);
                async move { pipeline.run_cycle().await }
            });
            // Poll until the durable entry shows the retry budget spent AND
            // the exact check marked in-flight AND the retry's own script
            // invocation has actually started (its line landed in
            // `attempt_log`) — the durable marker alone is written BEFORE
            // the retry's child process is even spawned (deliberately, so a
            // crash before that spawn still resumes correctly), so waiting
            // on it alone races ahead of the retry starting and can abort
            // the task before invocation 2 ever runs, collapsing this test
            // to a 2-invocation resume instead of the intended 3-invocation,
            // mid-sleep crash.
            let poll_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                let pending = space
                    .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
                    .unwrap();
                let retry_invocation_started = std::fs::read_to_string(&attempt_log)
                    .map(|s| s.lines().count() >= 2)
                    .unwrap_or(false);
                if pending.len() == 1
                    && pending[0].payload["gate_infra_retry_used"].as_bool() == Some(true)
                    && pending[0].payload["gate_infra_retry_check"].as_str() == Some("verify")
                    && retry_invocation_started
                {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < poll_deadline,
                    "candidate never reached a durably in-flight retry before the retry finished"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            handle.abort();
            let _ = handle.await;

            let pending = space
                .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
                .unwrap();
            assert_eq!(pending.len(), 1, "candidate must survive the crash");
            assert_eq!(pending[0].payload["gate_infra_retry_used"], true);
            assert_eq!(pending[0].payload["gate_infra_retry_check"], "verify");

            // Only the ordinal-1 "retrying" event exists so far — the
            // aborted retry's own continuation (which would record
            // ordinal 2) never ran.
            let events = space
                .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
                .unwrap();
            assert_eq!(events.len(), 1, "events: {events:?}");
            assert_eq!(events[0].payload["ordinal"], 1);
        }

        // "After restart": fresh Space handle over the SAME on-disk store.
        let space = Space::open(&layout.db_path()).unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        let outcomes = pipeline.run_cycle().await.unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(
            matches!(&outcomes[0], LandingOutcome::GateHeld),
            "outcome: {:?}",
            outcomes[0]
        );

        // Exactly one more script invocation happened (n=3 total) — the
        // resumed check re-ran once and died again — never a fresh
        // ordinal-1 death plus its own new retry (which would need n=4).
        assert_eq!(
            std::fs::read_to_string(&attempt_log)
                .unwrap()
                .lines()
                .count(),
            3,
            "restart must resume the exact retry, not restart the whole cycle"
        );

        // Exactly one more event (ordinal 2, exhausted) — the budget was
        // never re-spent, and no second "retrying" (ordinal 1) event exists.
        let events = space
            .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
            .unwrap();
        assert_eq!(events.len(), 2, "events: {events:?}");
        assert_eq!(
            events.iter().filter(|e| e.payload["ordinal"] == 1).count(),
            1,
            "no duplicate ordinal-1 death: {events:?}"
        );
        let ordinal_2 = events
            .iter()
            .find(|e| e.payload["ordinal"] == 2)
            .unwrap_or_else(|| panic!("no ordinal-2 event: {events:?}"));
        assert_eq!(ordinal_2.payload["disposition"], "retry_exhausted");
    }

    /// Requeue-and-rebuild regression (landing-review-17987ae38cd4097d333c7cf22e89151e,
    /// TKT-01M0G97GXNHA4VPRRXVMA9T6C8): `LandingQueue::requeue_tail` hands a
    /// rebuilt candidate a fresh durable `seq` (the queue-generation
    /// discriminator), but a rebuild can land on the EXACT SAME candidate SHA
    /// a prior generation already spent its retry against.
    /// `settled_infra_retry` must not let the new generation's own in-flight
    /// retry read that prior generation's ordinal-2 evidence as if it were
    /// its own — doing so would skip the newly-requeued generation's newly
    /// entitled retry entirely and settle on a stale verdict instead.
    #[tokio::test]
    async fn requeued_generation_earns_its_own_retry_against_a_rebuilt_same_sha() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let attempt_log = home.path().join("infra-attempts.log");
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "echo x >> '{log}'; n=$(wc -l < '{log}'); if [ $n -eq 2 ]; then sleep 5; fi; kill -9 $$", timeout: "30s"}},
]
"#,
                log = attempt_log.display()
            ),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));

        let base_entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: head_sha.clone(),
            diff_class: "doc-only".into(),
            task: "add src".into(),
            ..Default::default()
        };

        // Generation 1: enqueued, claimed, and left with durable ordinal-2
        // evidence recording that it PASSED its retry against `head_sha` —
        // fabricated directly (not run through the pipeline) so its verdict
        // is deliberately the OPPOSITE of what generation 2's real check
        // below will do, making a wrongly-reused verdict observable.
        pipeline.enqueue(base_entry.clone()).unwrap().unwrap();
        let gen1 = pipeline
            .queue
            .claim_next("code-repo", "main")
            .unwrap()
            .unwrap();
        pipeline
            .record_gate_infra_attempt(
                &gen1,
                &head_sha,
                "verify",
                "echo x; kill -9 $$",
                2,
                &json!({"verdict": "pass", "exit": 0}),
                false,
            )
            .unwrap();

        // Requeue: a genuinely fresh generation (its own seq, budget, and
        // in-flight marker all reset) that goes on to rebuild the EXACT SAME
        // candidate SHA `head_sha` — the scenario the review flagged. Mirrors
        // `process_next`'s real orchestration: `requeue_tail` only ADDS the
        // new tuple, so the original claimed tuple (still durable, per
        // `claim_next`'s doc) must be explicitly removed the same way
        // `process_next` does once `process_entry` returns, or `claim_next`
        // below would just re-claim generation 1 again (lower seq, still
        // queued).
        let seq2 = pipeline.queue.requeue_tail(&gen1).unwrap();
        assert_ne!(
            seq2, gen1.seq,
            "requeue must hand out a fresh queue-generation seq"
        );
        pipeline.queue.remove(&gen1).unwrap();
        let gen2 = pipeline
            .queue
            .claim_next("code-repo", "main")
            .unwrap()
            .unwrap();
        assert_eq!(gen2.seq, seq2);
        assert!(!gen2.gate_infra_retry_used);

        // Drive generation 2's own real gate run: it dies (ordinal 1),
        // spends its own budget, and starts its own retry — which is
        // aborted mid-flight (the script's deliberate sleep) to leave
        // `gate_infra_retry_check` durably set with no ordinal-2 evidence of
        // generation 2's OWN yet, exactly the crash window
        // `settled_infra_retry` exists to recover.
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig::default();
        {
            let pipeline = Arc::clone(&pipeline);
            let git_repo = git_repo.clone();
            let gates = gates.clone();
            let mut gen2 = gen2.clone();
            let handle = tokio::spawn(async move {
                let _ = pipeline.run_gates(&mut gen2, &git_repo, &gates).await;
            });
            let poll_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                let pending = space
                    .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
                    .unwrap();
                let retry_invocation_started = std::fs::read_to_string(&attempt_log)
                    .map(|s| s.lines().count() >= 2)
                    .unwrap_or(false);
                if pending.len() == 1
                    && pending[0].payload["gate_infra_retry_used"].as_bool() == Some(true)
                    && pending[0].payload["gate_infra_retry_check"].as_str() == Some("verify")
                    && retry_invocation_started
                {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < poll_deadline,
                    "generation 2 never reached a durably in-flight retry before the retry finished"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            handle.abort();
            let _ = handle.await;
        }

        let pending = space
            .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
            .unwrap();
        assert_eq!(pending.len(), 1, "generation 2 must survive the crash");
        assert_eq!(pending[0].payload["seq"], seq2);
        let mut resumed: LandingQueueEntry =
            serde_json::from_value(pending[0].payload.clone()).unwrap();
        assert_eq!(resumed.gate_infra_retry_check.as_deref(), Some("verify"));

        // "Restart": resume generation 2's in-flight retry. With the fix,
        // its stale-evidence read must miss (different seq) and it must
        // actually run the check one more time — observing the real death
        // and holding, rather than silently inheriting generation 1's
        // fabricated "pass".
        let passed = pipeline
            .run_gates(&mut resumed, &git_repo, &gates)
            .await
            .unwrap();
        assert!(
            !passed,
            "generation 2 must settle from its OWN retry outcome, not generation 1's stale 'pass' evidence"
        );
        assert_eq!(
            std::fs::read_to_string(&attempt_log)
                .unwrap()
                .lines()
                .count(),
            3,
            "generation 2 must actually execute its resumed retry rather than skip it"
        );

        let events = space
            .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
            .unwrap();
        let gen2_ordinal2 = events
            .iter()
            .find(|e| e.payload["ordinal"] == 2 && e.payload["seq"] == seq2)
            .unwrap_or_else(|| {
                panic!("no ordinal-2 event stamped with generation 2's own seq: {events:?}")
            });
        assert_eq!(gen2_ordinal2.payload["verdict"], "infra");
        assert_ne!(
            gen2_ordinal2.payload["disposition"], "retry_passed",
            "must not carry generation 1's stale verdict"
        );
    }

    /// The second, narrower crash window in the same retry
    /// (TKT-01M0GC2A0BPSRK96A4EPZB2HGQ): `finish_infra_retry` records the
    /// ordinal-2 evidence and only THEN clears the in-flight marker, so a
    /// crash between those two writes leaves a durably-settled retry still
    /// marked in-flight. Resuming that state must NOT re-run the check (the
    /// one retry is spent) and must NOT append a second ordinal-2 event — it
    /// must settle from the recorded evidence alone, in both directions.
    ///
    /// Driven through `run_gates` at a known `tested_sha` so the pre-crash
    /// state is exact; the evidence itself is written by the real
    /// `record_gate_infra_attempt`, not hand-rolled, so the test cannot pass
    /// against a payload shape the production path no longer writes.
    #[tokio::test]
    async fn settled_infra_retry_resumes_from_evidence_without_rerunning_or_duplicating() {
        for (recorded_verdict, expect_pass) in [("pass", true), ("infra", false)] {
            let home = tempfile::tempdir().unwrap();
            let repo_dir = tempfile::tempdir().unwrap();
            init_repo(repo_dir.path());
            let attempt_log = home.path().join("infra-attempts.log");
            write_checks(
                repo_dir.path(),
                &format!(
                    r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "echo x >> '{log}'; exit 0", timeout: "30s"}},
]
"#,
                    log = attempt_log.display()
                ),
            );
            git(repo_dir.path(), &["checkout", "-b", "feature"]);
            std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
            git(repo_dir.path(), &["add", "."]);
            git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
            let head_sha = rev_parse(repo_dir.path(), "feature");
            git(repo_dir.path(), &["checkout", "main"]);

            let space = Space::open_in_memory().unwrap();
            let pipeline = test_pipeline(home.path(), space.clone());
            let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
            let gates = GateConfig::default();

            // The exact durable state a crash in that window leaves behind:
            // budget spent, marker still set, both the ordinal-1 and
            // ordinal-2 evidence already durably written for this candidate
            // (the crash landed after `finish_infra_retry`'s evidence write,
            // before its marker clear).
            let mut entry = LandingQueueEntry {
                repo_name: "code-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "feature".into(),
                target: "main".into(),
                head_sha: head_sha.clone(),
                diff_class: "doc-only".into(),
                task: "add src".into(),
                gate_infra_retry_used: true,
                gate_infra_retry_check: Some("verify".into()),
                ..Default::default()
            };
            pipeline
                .record_gate_infra_attempt(
                    &entry,
                    &head_sha,
                    "verify",
                    "echo x; exit 0",
                    1,
                    &json!({"verdict": "infra", "exit": null, "signal": 9}),
                    false,
                )
                .unwrap();
            pipeline
                .record_gate_infra_attempt(
                    &entry,
                    &head_sha,
                    "verify",
                    "echo x; exit 0",
                    2,
                    &json!({"verdict": recorded_verdict, "exit": 0}),
                    false,
                )
                .unwrap();

            let passed = pipeline
                .run_gates(&mut entry, &git_repo, &gates)
                .await
                .unwrap();
            assert_eq!(
                passed, expect_pass,
                "a settled {recorded_verdict} retry must decide the gate from its recorded verdict"
            );

            assert!(
                !attempt_log.exists(),
                "an already-settled retry must not re-run the check ({recorded_verdict})"
            );
            let events = space
                .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
                .unwrap();
            assert_eq!(
                events.len(),
                2,
                "ordinal-1 evidence already durable must not be reconstructed again, and settling must not duplicate ordinal-2 ({recorded_verdict}): {events:?}"
            );
            assert!(
                entry.gate_infra_retry_check.is_none(),
                "the marker must be cleared once the settled outcome is consumed"
            );
            assert!(
                entry.gate_infra_retry_used,
                "the budget stays spent — resuming must never hand back a retry"
            );
        }
    }

    /// The crash window at `run_gates_at`'s fresh-infra-death branch
    /// (parent review TKT-01M0G97GXNHA4VPRRXVMA9T6C8, landing.rs:2840-2851
    /// as reviewed): the budget-spent/in-flight marker is persisted durably
    /// BEFORE the ordinal-1 evidence event is written — deliberately, so a
    /// crash after the persist never grants a duplicate retry. A crash
    /// landing exactly between those two writes leaves the marker set with
    /// NO ordinal-1 evidence at all. Restart must reconstruct it (queue seq,
    /// candidate SHA, check name and command, "infra" classification,
    /// ordinal 1, disposition) rather than silently losing the record of the
    /// original death, and must resume as exactly one more execution of the
    /// check — never a duplicate.
    #[tokio::test]
    async fn restart_reconstructs_missing_ordinal1_evidence_across_the_marker_persist_crash_window()
    {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let attempt_log = home.path().join("infra-attempts.log");
        let verify_command = format!("echo x >> '{log}'; exit 0", log = attempt_log.display());
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "{cmd}", timeout: "30s"}},
]
"#,
                cmd = verify_command
            ),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig::default();

        // The exact durable state the crash window leaves behind: the
        // budget-spent flag and the in-flight marker persisted, but the
        // ordinal-1 evidence write that was supposed to follow never
        // happened — no event tuple exists at all for this check yet.
        let mut entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: head_sha.clone(),
            diff_class: "doc-only".into(),
            task: "add src".into(),
            gate_infra_retry_used: true,
            gate_infra_retry_check: Some("verify".into()),
            ..Default::default()
        };
        let events_before = space
            .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
            .unwrap();
        assert!(
            events_before.is_empty(),
            "precondition: no evidence at all before resume: {events_before:?}"
        );

        let outcome = pipeline
            .run_gates_at(&mut entry, &git_repo, &gates, &head_sha)
            .await
            .unwrap();
        assert_eq!(
            outcome,
            GateRunOutcome::Pass,
            "the resumed check ran once and passed"
        );

        let pass_events = space
            .scan(&Pattern::category(Category::Event).identity("landing_gate_pass"))
            .unwrap();
        assert_eq!(pass_events.len(), 1, "one timing record per green gate run");
        assert_eq!(pass_events[0].payload["branch"], "feature");
        assert_eq!(pass_events[0].payload["candidate_sha"], head_sha);
        assert_eq!(
            pass_events[0].payload["checks"],
            json!(["steward-protected-paths", "steward-diff-scope", "verify"])
        );
        assert!(pass_events[0].payload["duration_ms"].is_u64());

        // Exactly one execution — the resumed attempt itself, never a
        // duplicate of the (unrecoverable) original death.
        assert_eq!(
            std::fs::read_to_string(&attempt_log)
                .unwrap()
                .lines()
                .count(),
            1,
            "resuming must execute the check exactly once, not replay the lost original attempt"
        );

        let events = space
            .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
            .unwrap();
        assert_eq!(events.len(), 2, "events: {events:?}");
        let by_ordinal = |n: u64| {
            events
                .iter()
                .find(|e| e.payload["ordinal"].as_u64() == Some(n))
                .unwrap_or_else(|| panic!("no event with ordinal {n}: {events:?}"))
        };
        let ord1 = by_ordinal(1);
        assert_eq!(ord1.payload["check"], "verify");
        assert_eq!(ord1.payload["command"], verify_command);
        assert_eq!(ord1.payload["candidate_sha"], head_sha);
        assert_eq!(ord1.payload["seq"], entry.seq);
        assert_eq!(ord1.payload["verdict"], "infra");
        assert_eq!(ord1.payload["disposition"], "retrying");
        assert_eq!(
            ord1.payload["reconstructed"], true,
            "the synthesized ordinal-1 event must be flagged as reconstructed: {ord1:?}"
        );
        let ord2 = by_ordinal(2);
        assert_eq!(ord2.payload["disposition"], "retry_passed");
        assert_eq!(
            ord2.payload["reconstructed"], false,
            "the real, directly-observed ordinal-2 event must not be flagged reconstructed"
        );

        assert!(
            entry.gate_infra_retry_check.is_none(),
            "the marker must be cleared once resumption settles"
        );
        assert!(
            entry.gate_infra_retry_used,
            "the budget stays spent after resuming"
        );

        // Resuming again (a second crash-loop iteration) must not duplicate
        // the reconstructed ordinal-1 event now that it durably exists.
        pipeline
            .ensure_infra_retry_ordinal1_evidence(&entry, &head_sha, "verify", "echo x; exit 0")
            .unwrap();
        let events_after = space
            .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
            .unwrap();
        assert_eq!(
            events_after.len(),
            2,
            "a second reconstruction attempt must be a no-op: {events_after:?}"
        );
    }

    /// Evidence for an infra-retry attempt must name the exact command that
    /// ran, not only the check's name — the parent acceptance requires check
    /// name AND command, and a name alone cannot be replayed by hand.
    #[tokio::test]
    async fn infra_retry_evidence_carries_the_exact_check_command() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let attempt_log = home.path().join("infra-attempts.log");
        let verify_command = format!("echo x >> '{log}'; kill -9 $$", log = attempt_log.display());
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "{cmd}", timeout: "30s"}},
]
"#,
                cmd = verify_command
            ),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let mut entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha,
            diff_class: "doc-only".into(),
            task: "add src".into(),
            ..Default::default()
        };
        let passed = pipeline
            .run_gates(&mut entry, &git_repo, &GateConfig::default())
            .await
            .unwrap();
        assert!(!passed, "a check that always dies must hold the branch");

        let events = space
            .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
            .unwrap();
        assert_eq!(events.len(), 2, "events: {events:?}");
        for event in &events {
            assert_eq!(
                event.payload["check"], "verify",
                "check name missing: {event:?}"
            );
            assert_eq!(
                event.payload["command"], verify_command,
                "exact command missing from ordinal-{} evidence: {event:?}",
                event.payload["ordinal"]
            );
        }
    }

    /// Batch/bisect audit (TKT-01M0FXGQMA10JYCV9QCGEAK4TT review point 4): a
    /// batch whose shared gate run dies to an infrastructure fault, retries,
    /// and is STILL red bisects into per-branch sub-attempts. The already-
    /// spent retry budget for the ORIGINAL (now-discarded) batch candidate
    /// must not silently duplicate a retry for a sub-candidate cloned from
    /// it, and cloning `entries[0]` into `first` inside `process_batch` must
    /// never diverge from the durable tuple the retry path persists against.
    #[tokio::test]
    async fn batch_bisect_does_not_duplicate_a_spent_infra_retry() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let attempt_log = home.path().join("batch-infra-attempts.log");
        // Every invocation of the shared batch gate dies to an
        // infrastructure fault — deterministic red, so the batch always
        // bisects (all-or-nothing: no `docs/bad.md` needed here).
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "echo x >> '{log}'; kill -9 $$", timeout: "30s"}},
]
"#,
                log = attempt_log.display()
            ),
        );

        let mut queued = Vec::new();
        for (branch, file) in [("branch-a", "a.md"), ("branch-b", "b.md")] {
            git(repo_dir.path(), &["checkout", "-b", branch]);
            std::fs::create_dir_all(repo_dir.path().join("docs")).unwrap();
            std::fs::write(repo_dir.path().join("docs").join(file), "x\n").unwrap();
            git(repo_dir.path(), &["add", "."]);
            git(repo_dir.path(), &["commit", "-m", branch]);
            queued.push((branch.to_string(), rev_parse(repo_dir.path(), branch)));
            git(repo_dir.path(), &["checkout", "main"]);
        }

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
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
        assert_eq!(outcomes.len(), 2);
        assert!(
            outcomes
                .iter()
                .all(|o| matches!(o, LandingOutcome::GateHeld)),
            "outcomes: {outcomes:?}"
        );

        // The shared batch attempt (driven by `entries[0]`/branch-a, whose
        // durable tuple `process_batch` mutates and persists directly — the
        // clone-divergence bug this test guards against) spends its one
        // retry: 2 invocations. Bisecting to single entries then re-runs
        // each via plain `process_entry`, reading each entry's OWN durable
        // budget: branch-a's is already spent (per `bisect_batch`'s doc — a
        // same-pass continuation, not reset) so it holds on ONE more
        // invocation with no further retry; branch-b's was never touched by
        // the shared run (only `entries[0]` drove it) and is still fresh, so
        // it independently earns its own one retry against its own new
        // single-branch candidate SHA — 2 more invocations. Total: 2 + 1 + 2
        // = 5. The property under test is branch-a's 1, not 2: its already-
        // spent budget must never be duplicated into a second retry.
        assert_eq!(
            std::fs::read_to_string(&attempt_log)
                .unwrap()
                .lines()
                .count(),
            5,
            "the already-spent batch-driving entry's retry must not duplicate"
        );
        let events = space
            .scan(&Pattern::category(Category::Event).identity(GATE_INFRA_RETRY_IDENTITY))
            .unwrap();
        assert_eq!(
            events.len(),
            4,
            "2 from the shared batch retry + 2 from branch-b's own fresh-budget retry: {events:?}"
        );
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

    /// A deterministic [`RetrySchedule`]: a clock the test owns, a fixed
    /// jitter draw, and a `sleep` that RECORDS what it was asked to wait and
    /// advances that clock by exactly that much instead of blocking. The
    /// recorded waits are what make "the real dispatch path waited exactly
    /// the scheduled backoff" assertable without a wall-clock threshold, and
    /// advancing the clock keeps a later `remaining_backoff` read honest —
    /// after the wait, the schedule really has elapsed.
    #[derive(Clone)]
    struct FakeSchedule {
        now: Arc<Mutex<DateTime<Utc>>>,
        waits: Arc<Mutex<Vec<Duration>>>,
        jitter_unit: f64,
    }

    impl FakeSchedule {
        /// Frozen at `Utc::now()` with `jitter_unit` as every draw. A unit of
        /// `1.0` is the jitter ceiling and `0.0` the floor, so a test picks
        /// the exact end of the configured band it wants to prove.
        fn new(jitter_unit: f64) -> Self {
            Self {
                now: Arc::new(Mutex::new(Utc::now())),
                waits: Arc::new(Mutex::new(Vec::new())),
                jitter_unit,
            }
        }

        fn now(&self) -> DateTime<Utc> {
            *self.now.lock().unwrap()
        }

        fn waits(&self) -> Vec<Duration> {
            self.waits.lock().unwrap().clone()
        }

        fn schedule(&self) -> RetrySchedule {
            let now = Arc::clone(&self.now);
            let advancing = Arc::clone(&self.now);
            let waits = Arc::clone(&self.waits);
            let jitter_unit = self.jitter_unit;
            RetrySchedule {
                now: Box::new(move || *now.lock().unwrap()),
                jitter_unit: Box::new(move || jitter_unit),
                sleep: Box::new(move |wait| {
                    waits.lock().unwrap().push(wait);
                    let mut clock = advancing.lock().unwrap();
                    *clock += chrono::Duration::from_std(wait)
                        .unwrap_or_else(|_| chrono::Duration::zero());
                    Box::pin(std::future::ready(()))
                }),
            }
        }
    }

    /// Activate `policy` for `repo_path`, the way an operator's repository
    /// policy activation would. Registered against `Repo::discover`'s
    /// resolved root (not the raw temp path), because that is the key
    /// `Supervisor::repository_policy` looks up.
    fn activate_repository_policy(
        home: &Path,
        repo_path: &Path,
        policy: rk_workflow::RepositoryPolicy,
    ) {
        let root = rk_git::Repo::discover(repo_path)
            .unwrap()
            .root()
            .to_path_buf();
        let mut registry = crate::repos::RepoRegistry::load(&home.join("repos.json")).unwrap();
        registry
            .add(crate::repos::RepoRecord {
                name: "code-repo".into(),
                path: root,
                created_at: Utc::now(),
                host: None,
                activated_policy: Some(crate::repos::ActivatedRepositoryPolicy {
                    digest: "test-digest".into(),
                    policy,
                }),
            })
            .unwrap();
    }

    /// Activate `landing` as `repo_path`'s repository policy — the only way
    /// a test can exercise `route_review_death` under a policy other than
    /// `LandingPolicy::default()`.
    fn activate_landing_policy(home: &Path, repo_path: &Path, landing: rk_workflow::LandingPolicy) {
        activate_repository_policy(
            home,
            repo_path,
            rk_workflow::RepositoryPolicy {
                landing,
                ..rk_workflow::RepositoryPolicy::default()
            },
        );
    }

    /// The review-death backoff knobs under test, with everything else left
    /// at its shipped default.
    fn backoff_landing_policy(
        delay: &str,
        backoff_pct: u32,
        max_delay: &str,
        jitter_pct: u32,
    ) -> rk_workflow::LandingPolicy {
        rk_workflow::LandingPolicy {
            review_death_retry_delay: delay.into(),
            review_death_retry_backoff_pct: backoff_pct,
            review_death_retry_max_delay: max_delay.into(),
            review_death_retry_jitter_pct: jitter_pct,
            ..rk_workflow::LandingPolicy::default()
        }
    }

    /// The `not_before` persisted by the one `dispatching` marker on this
    /// space, or `None` if that marker recorded no schedule at all.
    fn dispatching_not_before(space: &Space) -> Option<DateTime<Utc>> {
        let marker = scoped_tuples(space, Category::Event, REVIEW_DEATH_DISPATCH_IDENTITY)
            .into_iter()
            .find(|m| m.payload["state"] == "dispatching")
            .expect("a dispatching marker must be recorded");
        marker.payload["not_before"]
            .as_str()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc))
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

    fn only_conflict_chain_key(space: &Space) -> String {
        let keys: std::collections::HashSet<String> =
            scoped_tuples(space, Category::Event, CONFLICT_DISPATCH_IDENTITY)
                .into_iter()
                .filter_map(|tuple| {
                    LandingPipeline::conflict_marker_chain_key(&tuple).map(str::to_string)
                })
                .collect();
        assert_eq!(keys.len(), 1, "expected one exact conflict chain: {keys:?}");
        keys.into_iter().next().unwrap()
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

    fn conflict_context(
        repo_path: &str,
        head: &str,
        task: &str,
        rework_ticket: &str,
    ) -> ConflictContext {
        ConflictContext {
            repo: "code-repo".into(),
            repo_path: repo_path.into(),
            branch: "feature".into(),
            head_sha: head.into(),
            target: "main".into(),
            target_head: "target-head-placeholder".into(),
            fork_point: "fork-point-placeholder".into(),
            task: task.into(),
            rework_ticket: rework_ticket.into(),
            conflict_detail: "CONFLICT (content): Merge conflict in src.rs".into(),
            diff_files: 1,
            diff_lines: 1,
        }
    }

    fn put_conflict_marker(space: &Space, ctx: &ConflictContext, agent: Option<&str>, state: &str) {
        space
            .out(
                Tuple::new(
                    Category::Event,
                    "code-repo",
                    CONFLICT_DISPATCH_IDENTITY,
                    "daemon",
                    ctx.marker_payload(1, agent, state),
                )
                .with_lifecycle(Lifecycle::Furniture),
            )
            .unwrap();
    }

    /// `main` and `feature` diverge on the same path from a shared fork
    /// point, so `prepare_merge("feature", "main")` cannot build a
    /// candidate. Returns `(repo_dir, head_sha, main_before)`.
    fn conflicting_repo_with_file(rel_path: &str) -> (tempfile::TempDir, String, String) {
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        write_checks(repo_dir.path(), ALL_PASS_CHECKS);
        let full = repo_dir.path().join(rel_path);
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(&full, "feature side\n").unwrap();
        git(repo_dir.path(), &["add", rel_path]);
        git(repo_dir.path(), &["commit", "-m", "feat: feature side"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(&full, "main side\n").unwrap();
        git(repo_dir.path(), &["add", rel_path]);
        git(repo_dir.path(), &["commit", "-m", "feat: main side"]);
        let main_before = rev_parse(repo_dir.path(), "main");
        (repo_dir, head_sha, main_before)
    }

    fn conflicting_repo() -> (tempfile::TempDir, String, String) {
        conflicting_repo_with_file("src.rs")
    }

    fn conflict_candidate_entry(repo_dir: &Path, head_sha: &str) -> LandingQueueEntry {
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

    /// AC1/AC4 proof: an Orchestrator-authority landing conflict resolves
    /// into exactly one durable, structured recovery item (repo/source/
    /// target/fork-point/exact-heads/bounded-evidence, all on the dispatch
    /// marker) and WAITS — no correction agent is dispatched unattended.
    /// Only once `dispatch_held_conflict` runs (standing in for
    /// `Server::execute_orchestrator`, itself only reachable after
    /// `attention.decide` has fenced the call through a live orchestrator
    /// lease) does the bounded correction agent spawn, cut from the held
    /// branch's own tip, without mutating either the branch or the target.
    #[tokio::test]
    async fn conflict_recovery_holds_for_an_orchestrator_decision_then_dispatches_one_bounded_correction(
    ) {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, main_before) = conflicting_repo();
        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let entry = conflict_candidate_entry(repo_dir.path(), &head_sha);
        pipeline.enqueue(entry.clone()).unwrap();

        let outcomes = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert_eq!(outcomes.len(), 1);
        let LandingOutcome::ReworkFiled(ticket) = &outcomes[0] else {
            panic!("expected ReworkFiled, got {:?}", outcomes[0]);
        };
        assert_eq!(ticket.payload["title"], "conflict: add src");
        assert_eq!(ticket.scope, "code-repo");

        assert_eq!(
            rev_parse(repo_dir.path(), "main"),
            main_before,
            "target must not have moved"
        );
        assert_eq!(
            rev_parse(repo_dir.path(), "feature"),
            head_sha,
            "the original conflicted branch must be untouched — the correction is a fresh agent"
        );

        assert!(
            tuples(&space, Category::Event, "agent_spawned").is_empty(),
            "Authority::Orchestrator must never dispatch unattended — nothing has fenced this \
             through a live lease yet"
        );
        let markers = scoped_tuples(&space, Category::Event, CONFLICT_DISPATCH_IDENTITY);
        assert_eq!(
            markers.len(),
            1,
            "holding for a decision writes exactly one marker, not a dispatching/dispatched pair"
        );
        let held = &markers[0];
        assert_eq!(
            held.payload["state"], "awaiting-orchestrator-decision",
            "{held:?}"
        );
        assert_eq!(held.payload["repo"], "code-repo");
        assert_eq!(
            held.payload["repo_path"],
            repo_dir.path().display().to_string()
        );
        assert_eq!(held.payload["source"], "feature");
        assert_eq!(held.payload["target"], "main");
        assert_eq!(held.payload["head_sha"], head_sha);
        assert_eq!(held.payload["target_head"], main_before);
        assert_eq!(held.payload["task"], "add src");
        assert_eq!(held.payload["rework_ticket"], ticket.identity);
        let fork_point = held.payload["fork_point"].as_str().unwrap();
        assert_ne!(
            fork_point, head_sha,
            "fork point must not be the source tip"
        );
        assert_ne!(
            fork_point, main_before,
            "fork point must not be the target tip"
        );
        assert!(
            held.payload["conflict_evidence"]
                .as_str()
                .unwrap()
                .contains("CONFLICT"),
            "{:?}",
            held.payload["conflict_evidence"]
        );

        // The hold must also be visible through the SAME `conflict-held-landing`
        // attention item every other convergence violation surfaces through —
        // no separate, bespoke visibility path.
        let lands = scoped_tuples(&space, Category::Event, "branch_landed");
        assert_eq!(lands.len(), 1, "{lands:?}");
        assert_eq!(lands[0].payload["branch"], "feature");
        assert_eq!(lands[0].payload["target"], "main");
        assert_eq!(lands[0].payload["merged"], false);
        assert_eq!(lands[0].payload["pr_opened"], false);

        // Standing in for `Server::execute_orchestrator`, reachable only
        // after `attention.decide` fenced the call through a live lease.
        let chain_key = only_conflict_chain_key(&space);
        let dispatch_detail = pipeline
            .dispatch_held_conflict("code-repo", "feature", Some(&chain_key))
            .await
            .unwrap();
        assert!(
            dispatch_detail.contains(&ticket.identity),
            "{dispatch_detail}"
        );

        let spawns = tuples(&space, Category::Event, "agent_spawned");
        assert_eq!(
            spawns.len(),
            1,
            "the authorized decision must dispatch exactly one agent"
        );
        let markers = scoped_tuples(&space, Category::Event, CONFLICT_DISPATCH_IDENTITY);
        assert_eq!(
            markers.len(),
            3,
            "the hold marker plus a dispatching marker and a terminal dispatched marker"
        );
        assert!(markers.iter().any(|m| m.payload["state"] == "dispatching"));
        let dispatched = markers
            .iter()
            .find(|m| m.payload["state"] == "dispatched")
            .expect("a successful spawn must record a terminal dispatched marker");
        assert_eq!(dispatched.payload["rework_ticket"], ticket.identity);

        // A second decision for the same chain must not double-dispatch.
        let replay = pipeline
            .dispatch_held_conflict("code-repo", "feature", Some(&chain_key))
            .await
            .unwrap();
        assert!(replay.contains("already"), "{replay}");
        assert_eq!(
            tuples(&space, Category::Event, "agent_spawned").len(),
            1,
            "a replayed decision must not spawn a second agent"
        );

        // The dispatch rides the same SemanticReview/Rework span pair
        // `route_rework` brackets its own bounded-rework dispatch with,
        // alongside the AttentionHold already written while this chain
        // awaited the orchestrator's decision — all three correlated on
        // the ORIGINAL ticket ("add src"), not the correction ticket, and
        // all under `Authority::Llm` (no distinct orchestrator variant).
        let spans = crate::span::spans_for_task(&space, "code-repo", "add src").unwrap();
        let review = spans
            .iter()
            .find(|s| s["phase"] == "semantic_review")
            .expect("dispatch_held_conflict must record a SemanticReview span");
        assert_eq!(review["authority"], "llm");
        assert_eq!(review["terminal_reason"], "conflict-correction-requested");
        // `entry` here is the synthetic conflict entry — no durable landing-
        // queue tuple, so no `phase_entered_at` clock exists to derive a
        // `started_at` from. `duration_ms` must be left absent (`null`),
        // never fabricated when evidence is unavailable.
        assert!(review["started_at"].is_null(), "{review:?}");
        assert!(review["duration_ms"].is_null(), "{review:?}");
        let rework = spans
            .iter()
            .find(|s| s["phase"] == "rework")
            .expect("dispatch_held_conflict must record a Rework span on a successful spawn");
        assert_eq!(rework["authority"], "llm");
        assert_eq!(rework["terminal_reason"], "conflict-correction-dispatched");
        // The Rework phase's own clock IS locally derivable (it starts
        // exactly when the SemanticReview phase's write completed), so it
        // must carry a real, non-negative duration even though the review
        // phase above could not.
        assert!(rework["started_at"].is_string(), "{rework:?}");
        assert!(rework["ended_at"].is_string(), "{rework:?}");
        let rework_duration = rework["duration_ms"]
            .as_i64()
            .expect("rework span must carry a real duration_ms");
        assert!(rework_duration >= 0, "{rework:?}");
        let hold = spans
            .iter()
            .find(|s| s["phase"] == "attention_hold")
            .expect("the earlier orchestrator-decision hold must have its own span");
        assert_eq!(hold["authority"], "llm");
        // The replayed (already-dispatched) call must not double either span.
        assert_eq!(
            spans
                .iter()
                .filter(|s| s["phase"] == "semantic_review")
                .count(),
            1
        );
        assert_eq!(spans.iter().filter(|s| s["phase"] == "rework").count(), 1);

        // Once the correction lands back onto `feature`, the held branch must
        // resubmit through the normal queue rather than land itself. Advance
        // `feature`'s tip first (as a landed correction commit would) so the
        // resubmitted work key differs from the conflicted one already
        // marked processed above.
        git(repo_dir.path(), &["checkout", "feature"]);
        std::fs::write(repo_dir.path().join("resolved.txt"), "fixed\n").unwrap();
        git(repo_dir.path(), &["add", "resolved.txt"]);
        git(repo_dir.path(), &["commit", "-m", "fix: resolve conflict"]);
        git(repo_dir.path(), &["checkout", "main"]);
        let correction = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "rat/correction".into(),
            target: "feature".into(),
            head_sha: "irrelevant-for-this-probe".into(),
            diff_class: "doc-only".into(),
            task: ticket.identity.clone(),
            ..Default::default()
        };
        pipeline
            .resubmit_conflict_reworked_parent(&correction)
            .unwrap();
        let pending = pipeline
            .queue
            .scan_current("code-repo", Some("main"))
            .unwrap();
        assert_eq!(
            pending.len(),
            1,
            "held branch must be requeued exactly once"
        );
        assert_eq!(pending[0].payload["branch"], "feature");
        assert_eq!(pending[0].payload["target"], "main");
        assert_eq!(pending[0].payload["task"], "add src");
    }

    /// AC5/human-gating proof: a conflict whose diff touches this
    /// repository's `protectedPaths` is held for a human, never dispatched.
    #[tokio::test]
    async fn protected_path_conflict_holds_for_a_human_instead_of_dispatching() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, main_before) =
            conflicting_repo_with_file("migrations/0001_schema.sql");
        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let entry = conflict_candidate_entry(repo_dir.path(), &head_sha);
        pipeline.enqueue(entry.clone()).unwrap();

        let outcome = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert!(matches!(
            outcome.as_slice(),
            [LandingOutcome::ReworkFiled(_)]
        ));
        assert_eq!(rev_parse(repo_dir.path(), "main"), main_before);
        assert_eq!(rev_parse(repo_dir.path(), "feature"), head_sha);
        no_spawns(&space);

        let needs = scoped_tuples(&space, Category::Need, STEWARD_NEED_IDENTITY);
        assert_eq!(needs.len(), 1);
        let text = needs[0].payload["text"].as_str().unwrap();
        for required in [
            "protected-path-impact",
            "EVIDENCE:",
            "DECISION NEEDED:",
            "BLAST RADIUS:",
            "RESOLVE WITH: rk spawn",
        ] {
            assert!(text.contains(required), "missing {required:?}: {text}");
        }
    }

    /// AC4/idempotent-duplicate-delivery proof: a redelivered completion for
    /// the exact same work key (the reactor's at-least-once retry, or an
    /// operator re-triggering the same event by hand) must reconcile against
    /// the already-processed outcome rather than spawn or land a second time.
    #[tokio::test]
    async fn duplicate_conflict_delivery_does_not_spawn_or_file_twice() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, main_before) = conflicting_repo();
        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let entry = conflict_candidate_entry(repo_dir.path(), &head_sha);

        let first = pipeline.process_entry(&entry).await.unwrap();
        assert!(matches!(first, LandingOutcome::ReworkFiled(_)));

        let second = pipeline.process_entry(&entry).await.unwrap();
        assert!(
            matches!(&second, LandingOutcome::Reconciled(prior) if prior == "rework-filed"),
            "a redelivered conflict must reconcile against the already-processed work key, got \
             {second:?}"
        );

        assert_eq!(rev_parse(repo_dir.path(), "main"), main_before);
        assert_eq!(rev_parse(repo_dir.path(), "feature"), head_sha);
        assert!(
            tuples(&space, Category::Event, "agent_spawned").is_empty(),
            "Authority::Orchestrator must never dispatch unattended, duplicate or not"
        );
        assert_eq!(
            scoped_tuples(&space, Category::Event, CONFLICT_DISPATCH_IDENTITY).len(),
            1,
            "the one hold gets its awaiting-orchestrator-decision marker; duplicate delivery \
             must not append a second"
        );
        let conflict_tickets = space
            .scan(&Pattern::category(Category::Task).scope("code-repo"))
            .unwrap()
            .into_iter()
            .filter(|t| t.payload["title"] == "conflict: add src")
            .count();
        assert_eq!(
            conflict_tickets, 1,
            "duplicate delivery must not file a second follow-up ticket"
        );
    }

    /// Restart proof: a marker recorded `dispatching` but never journaled by
    /// the supervisor (a daemon that stopped between the two) survives a
    /// restart as one human gate, not a duplicated spawn.
    #[tokio::test]
    async fn conflict_dispatch_interrupted_before_spawn_survives_restart_as_one_human_gate() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        layout.ensure().unwrap();
        let (repo_dir, head_sha, main_before) = conflicting_repo();
        let entry = conflict_candidate_entry(repo_dir.path(), &head_sha);

        {
            let space = Space::open(&layout.db_path()).unwrap();
            let pipeline = test_pipeline(home.path(), space.clone());
            let ticket = pipeline.file_conflict_rework_ticket(&entry).await.unwrap();
            let ctx = conflict_context(
                &entry.repo_path,
                &entry.head_sha,
                &entry.task,
                &ticket.identity,
            );
            put_conflict_marker(&space, &ctx, None, "dispatching");
        }

        // A fresh Space and pipeline stand in for a daemon restart after the
        // marker commit point but before Supervisor::spawn journaled an agent.
        let space = Space::open(&layout.db_path()).unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        for _ in 0..2 {
            let replay = pipeline
                .route_conflict(
                    &entry,
                    &repo,
                    "CONFLICT (content): Merge conflict in src.rs",
                )
                .await
                .unwrap();
            assert!(matches!(replay, LandingOutcome::ReworkFiled(_)));
        }

        assert_eq!(rev_parse(repo_dir.path(), "main"), main_before);
        assert_eq!(rev_parse(repo_dir.path(), "feature"), head_sha);
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
            "EVIDENCE:",
            "DECISION NEEDED:",
            "BLAST RADIUS:",
            "RESOLVE WITH: rk spawn",
        ] {
            assert!(text.contains(required), "missing {required:?}: {text}");
        }
    }

    /// [`refused_rework_spawn_reaches_a_terminal_state_not_stuck_dispatching`]'s
    /// CONFLICT counterpart, now exercised against the authorized-dispatch
    /// call directly (`route_conflict` itself never spawns any more): a
    /// refused correction spawn must record a terminal `dispatch-refused`
    /// marker rather than leaving the chain stuck at `dispatching`, which
    /// would otherwise misdiagnose the next decision as an interrupted
    /// daemon.
    #[tokio::test]
    async fn refused_conflict_spawn_reaches_a_terminal_state_not_stuck_dispatching() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, main_before) = conflicting_repo();
        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let entry = conflict_candidate_entry(repo_dir.path(), &head_sha);
        pipeline.enqueue(entry.clone()).unwrap();

        let outcomes = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(outcomes[0], LandingOutcome::ReworkFiled(_)));

        // Only pause dispatch once the chain is already held for a decision
        // — the hold itself must never touch the supervisor at all.
        pipeline.supervisor.set_dispatch_paused(true);
        let chain_key = only_conflict_chain_key(&space);
        let refusal = pipeline
            .dispatch_held_conflict("code-repo", "feature", Some(&chain_key))
            .await;
        assert!(refusal.is_err(), "{refusal:?}");

        assert_eq!(rev_parse(repo_dir.path(), "main"), main_before);
        assert_eq!(rev_parse(repo_dir.path(), "feature"), head_sha);
        assert!(
            tuples(&space, Category::Event, "agent_spawned").is_empty(),
            "a refused dispatch must not journal an agent"
        );

        let markers = scoped_tuples(&space, Category::Event, CONFLICT_DISPATCH_IDENTITY);
        assert_eq!(
            markers.len(),
            3,
            "the hold marker, then a dispatching marker, then a terminal refused one"
        );
        assert!(
            markers.iter().any(|m| m.payload["state"] == "dispatching"),
            "{markers:?}"
        );
        assert!(
            markers
                .iter()
                .any(|m| m.payload["state"] == "dispatch-refused"),
            "the refusal must be recorded as its own terminal state, not left dispatching: \
             {markers:?}"
        );

        let needs = scoped_tuples(&space, Category::Need, STEWARD_NEED_IDENTITY);
        assert_eq!(
            needs.len(),
            1,
            "a refused dispatch must raise one human gate"
        );
        let text = needs[0].payload["text"].as_str().unwrap();
        assert!(text.contains("dispatch-refused"), "{text}");

        // A retry against an already-refused chain must stay REFUSED, not
        // silently converge on a phantom `Ok`: `dispatch-refused` is a
        // terminal human gate exactly like `dispatch-interrupted`, and only
        // a human clearing it (not a repeated automated call) may unblock
        // the chain. See the `dispatch_held_conflict` `other` match arm.
        pipeline.supervisor.set_dispatch_paused(false);
        let replay = pipeline
            .dispatch_held_conflict("code-repo", "feature", Some(&chain_key))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            replay.contains("already terminally") && replay.contains("dispatch-refused"),
            "{replay}"
        );
        assert!(
            tuples(&space, Category::Event, "agent_spawned").is_empty(),
            "a redelivered decision must not silently dispatch behind the existing human gate"
        );
        assert_eq!(
            scoped_tuples(&space, Category::Need, STEWARD_NEED_IDENTITY).len(),
            1,
            "replay must converge on the existing human gate rather than raise a second one"
        );
        assert_eq!(
            scoped_tuples(&space, Category::Event, CONFLICT_DISPATCH_IDENTITY).len(),
            3,
            "a refused retry must not write a fresh marker on top of the existing terminal one"
        );
    }

    /// Retry-exhaustion proof: a branch that already spent this repository's
    /// one automatic conflict-correction attempt holds on its next conflict,
    /// with actionable evidence, and a replay converges on the same gate.
    #[tokio::test]
    async fn conflict_recovery_chain_exhausted_holds_once_with_actionable_evidence() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, main_before) = conflicting_repo();
        let space = Space::open_in_memory().unwrap();
        let prior = conflict_context(
            &repo_dir.path().display().to_string(),
            "prior-conflicted-head",
            "add src",
            "TKT-prior-correction",
        );
        put_conflict_marker(&space, &prior, Some("Prior-Rat"), "dispatching");
        let pipeline = test_pipeline(home.path(), space.clone());
        let entry = conflict_candidate_entry(repo_dir.path(), &head_sha);
        pipeline.enqueue(entry.clone()).unwrap();

        let outcome = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert!(matches!(
            outcome.as_slice(),
            [LandingOutcome::ReworkFiled(_)]
        ));
        assert_eq!(rev_parse(repo_dir.path(), "main"), main_before);
        assert_eq!(rev_parse(repo_dir.path(), "feature"), head_sha);
        no_spawns(&space);

        let needs = scoped_tuples(&space, Category::Need, STEWARD_NEED_IDENTITY);
        assert_eq!(needs.len(), 1);
        let text = needs[0].payload["text"].as_str().unwrap();
        for required in [
            "attempts-exhausted",
            "EVIDENCE:",
            "DECISION NEEDED:",
            "BLAST RADIUS:",
            "RESOLVE WITH: rk spawn",
        ] {
            assert!(text.contains(required), "missing {required:?}: {text}");
        }

        let repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        pipeline
            .route_conflict(
                &entry,
                &repo,
                "CONFLICT (content): Merge conflict in src.rs",
            )
            .await
            .unwrap();
        assert_eq!(
            scoped_tuples(&space, Category::Need, STEWARD_NEED_IDENTITY).len(),
            1,
            "replay must converge on the existing human gate"
        );
    }

    fn review_death_context(head: &str, task: &str) -> ReviewDeathContext {
        ReviewDeathContext {
            repo: "code-repo".into(),
            repo_path: "/repos/code-repo".into(),
            branch: "feature".into(),
            head_sha: head.into(),
            target: "main".into(),
            task: task.into(),
        }
    }

    fn put_review_death_marker(
        space: &Space,
        ctx: &ReviewDeathContext,
        attempt: u32,
        instance_id: &str,
        state: &str,
    ) {
        put_review_death_marker_with_schedule(space, ctx, attempt, instance_id, state, None);
    }

    fn put_review_death_marker_with_schedule(
        space: &Space,
        ctx: &ReviewDeathContext,
        attempt: u32,
        instance_id: &str,
        state: &str,
        not_before: Option<DateTime<Utc>>,
    ) {
        space
            .out(
                Tuple::new(
                    Category::Event,
                    "code-repo",
                    REVIEW_DEATH_DISPATCH_IDENTITY,
                    "daemon",
                    ctx.marker_payload(attempt, instance_id, state, not_before),
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

        // Clean approval: the review phase must get its own terminal
        // SemanticReview span too, not just the REWORK/withhold routes —
        // this is the "duplicate semantic-review time" evaluation's ordinary
        // path (docs/2026-08-23-tkt-01m0p974w01xt6njg10ymj0zed-live-tracer.md),
        // and it must carry a real duration derived from the durable
        // landing-queue phase clock set when `dispatch_review` moved this
        // candidate into `AwaitingReview`, captured BEFORE the subsequent
        // `Landing` transition would have reset it.
        let review_span = crate::span::spans_for_task(&space, "code-repo", "add src")
            .unwrap()
            .into_iter()
            .find(|s| s["phase"] == "semantic_review")
            .expect("a clean APPROVE must record a SemanticReview span");
        assert_eq!(review_span["terminal_reason"], "approved");
        assert_eq!(review_span["authority"], "llm");
        assert!(review_span["started_at"].is_string(), "{review_span:?}");
        assert!(review_span["ended_at"].is_string(), "{review_span:?}");
        let review_duration = review_span["duration_ms"]
            .as_i64()
            .expect("a clean approval must carry a real duration_ms, not null");
        assert!(review_duration >= 0, "{review_span:?}");
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
        activate_repository_policy(
            home.path(),
            repo_dir.path(),
            rk_workflow::RepositoryPolicy {
                delivery: rk_workflow::DeliveryPolicy {
                    target: "main".into(),
                    ..rk_workflow::DeliveryPolicy::default()
                },
                ..rk_workflow::RepositoryPolicy::default()
            },
        );
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
        assert_eq!(
            markers.len(),
            2,
            "one logical dispatch gets a dispatching marker and a terminal dispatched marker"
        );
        let dispatching = markers
            .iter()
            .find(|m| m.payload["state"] == "dispatching")
            .expect("dispatching marker must be recorded before the spawn");
        let dispatched = markers
            .iter()
            .find(|m| m.payload["state"] == "dispatched")
            .expect("a successful spawn must record a terminal dispatched marker");
        for marker in [dispatching, dispatched] {
            assert_eq!(marker.payload["branch"], "feature");
            assert_eq!(marker.payload["target"], "main");
            assert_eq!(marker.payload["task"], "add src");
            assert_eq!(marker.payload["rework_ticket"], ticket.identity);
        }

        // One LLM rework round: `route_rework` must record BOTH the
        // `SemanticReview` span that closed the review with a
        // "rework-requested" verdict AND the `Rework` span for the dispatch
        // it triggered, each carrying a real (non-fabricated) duration —
        // the review's from the durable landing-queue phase clock
        // (`dispatch_review`'s `AwaitingReview` transition, set when this
        // candidate was enqueued above), the rework's from bracketing the
        // dispatch itself.
        let review_span = crate::span::spans_for_task(&space, "code-repo", "add src")
            .unwrap()
            .into_iter()
            .find(|s| s["phase"] == "semantic_review")
            .expect("route_rework must record a SemanticReview span");
        assert_eq!(review_span["terminal_reason"], "rework-requested");
        assert!(
            review_span["started_at"].is_string(),
            "the durable landing-queue phase clock must supply a started_at: {review_span:?}"
        );
        assert!(review_span["ended_at"].is_string(), "{review_span:?}");
        let review_duration = review_span["duration_ms"]
            .as_i64()
            .expect("a review round with a known start must carry a real duration_ms");
        assert!(review_duration >= 0, "{review_span:?}");

        let rework_span = crate::span::spans_for_task(&space, "code-repo", "add src")
            .unwrap()
            .into_iter()
            .find(|s| s["phase"] == "rework")
            .expect("a successful dispatch must record a Rework span");
        assert_eq!(rework_span["terminal_reason"], "dispatched");
        let rework_duration = rework_span["duration_ms"]
            .as_i64()
            .expect("rework dispatch must carry a real duration_ms");
        assert!(rework_duration >= 0, "{rework_span:?}");

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
            2,
            "replayed routing must not append another marker"
        );
        assert!(
            scoped_tuples(&space, Category::Need, STEWARD_NEED_IDENTITY).is_empty(),
            "a journaled correction agent must not be mistaken for an interrupted dispatch"
        );

        // Restart/replay/dedup: the idempotent replay must neither duplicate
        // nor re-time either span — `record_phase_span`'s dedup on
        // `(task, phase, attempt)` makes the second write a no-op, so the
        // original timing survives untouched.
        let spans_after_replay =
            crate::span::spans_for_task(&space, "code-repo", "add src").unwrap();
        assert_eq!(
            spans_after_replay
                .iter()
                .filter(|s| s["phase"] == "semantic_review")
                .count(),
            1,
            "replay must not duplicate the SemanticReview span"
        );
        assert_eq!(
            spans_after_replay
                .iter()
                .filter(|s| s["phase"] == "rework")
                .count(),
            1,
            "replay must not duplicate the Rework span"
        );
        let review_after_replay = spans_after_replay
            .iter()
            .find(|s| s["phase"] == "semantic_review")
            .unwrap();
        assert_eq!(
            review_after_replay["duration_ms"], review_span["duration_ms"],
            "replay must not re-time the review span"
        );
    }

    /// The regression this pins: a task that goes through a REWORK round and
    /// is then APPROVEd on resubmission must still record the approval that
    /// actually landed it.
    ///
    /// `record_phase_span` dedups on `(task, phase, attempt)`, and `entry.task`
    /// persists across the whole rework/resubmit cycle, so an APPROVE that
    /// assumed it was always round 1 would silently no-op against the earlier
    /// round's "rework-requested" `SemanticReview` span — leaving the task
    /// permanently reading as terminally rework-requested with no record of
    /// the approval at all, exactly the telemetry gap this instrumentation
    /// exists to close.
    ///
    /// Both rounds are driven through the REAL routing path (`drain_key` ->
    /// `route_rework`, then `drain_key` -> `route_verdict_prepared`), NOT a
    /// hand-written `put_rework_marker`: the marker is what
    /// `approved_review_attempt` counts the round from, so a hand-written one
    /// would prove nothing about the collision this fixes.
    #[tokio::test]
    async fn approve_after_a_real_rework_round_records_its_own_review_span() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, main_before) = review_candidate_repo();

        let space = Space::open_in_memory().unwrap();
        space.out(verdict_tuple(&head_sha, "REWORK")).unwrap();

        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(review_candidate_entry(repo_dir.path(), &head_sha))
            .unwrap();

        // Round 1: a genuine REWORK verdict, routed for real. This is what
        // writes the attempt=1 `SemanticReview` span the approval below would
        // otherwise collide with.
        let reworked = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert!(
            matches!(reworked.as_slice(), [LandingOutcome::ReworkFiled(_)]),
            "expected a real REWORK round, got {reworked:?}"
        );
        assert_eq!(
            rev_parse(repo_dir.path(), "main"),
            main_before,
            "a rework round must not land the branch"
        );
        let rework_round = crate::span::spans_for_task(&space, "code-repo", "add src")
            .unwrap()
            .into_iter()
            .find(|s| s["phase"] == "semantic_review")
            .expect("route_rework must record the round it closed");
        assert_eq!(rework_round["terminal_reason"], "rework-requested");
        assert_eq!(
            rework_round["attempt"], 1,
            "the first rework round numbers itself 1: {rework_round:?}"
        );

        // The correction lands on the reviewed branch and the same task is
        // resubmitted — a fresh head, so a fresh review rather than a cache
        // hit on the REWORK verdict above.
        git(repo_dir.path(), &["checkout", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() { fixed() }\n").unwrap();
        git(repo_dir.path(), &["add", "src.rs"]);
        git(
            repo_dir.path(),
            &["commit", "-m", "fix: reviewer correction"],
        );
        let corrected_head = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);
        assert_ne!(corrected_head, head_sha);

        space
            .out(verdict_tuple(&corrected_head, "APPROVE"))
            .unwrap();
        pipeline
            .enqueue(review_candidate_entry(repo_dir.path(), &corrected_head))
            .unwrap();
        let approved = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert!(
            matches!(approved.as_slice(), [LandingOutcome::Landed(_)]),
            "the corrected branch must land, got {approved:?}"
        );

        // The approval's own span survives alongside the rework round's
        // instead of being dedup-dropped against it.
        let review_spans: Vec<Value> = crate::span::spans_for_task(&space, "code-repo", "add src")
            .unwrap()
            .into_iter()
            .filter(|s| s["phase"] == "semantic_review")
            .collect();
        assert_eq!(
            review_spans.len(),
            2,
            "both review rounds must be recorded, not collapsed: {review_spans:?}"
        );
        let approval = review_spans
            .iter()
            .find(|s| s["terminal_reason"] == "approved")
            .expect("the APPROVE that landed the task must record its own SemanticReview span");
        assert_eq!(
            approval["attempt"], 2,
            "an approval after one rework round is round 2: {approval:?}"
        );
        assert_eq!(approval["authority"], "llm");
        assert!(approval["started_at"].is_string(), "{approval:?}");
        assert!(approval["ended_at"].is_string(), "{approval:?}");
        let duration = approval["duration_ms"]
            .as_i64()
            .expect("the approval must carry a real duration_ms, not null");
        assert!(duration >= 0, "{approval:?}");
    }

    /// The bug this fixes: `rework_attempts_used` and `conflict_attempts_used`
    /// number their own kind's bounded-correction rounds independently, both
    /// starting at 1. A task that goes through one rework round and then one
    /// conflict-correction round would have both write their opening
    /// `SemanticReview` span at `attempt: 1`, and `record_phase_span`'s dedup
    /// on `(task, phase, attempt)` would silently drop the second round's
    /// span — the same telemetry gap
    /// `approve_after_a_real_rework_round_records_its_own_review_span` covers
    /// for the APPROVE-after-rework case, but for rework<->conflict
    /// interleaving. Both rounds are driven through the real routing paths
    /// (`drain_key` -> `route_rework`, then `route_conflict` ->
    /// `dispatch_held_conflict`), not hand-written markers, so this proves
    /// the fix through the actual attempt-derivation code
    /// (`Self::correction_round`).
    #[tokio::test]
    async fn interleaved_rework_then_conflict_rounds_record_distinct_review_spans() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, main_before) = review_candidate_repo();

        let space = Space::open_in_memory().unwrap();
        space.out(verdict_tuple(&head_sha, "REWORK")).unwrap();

        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(review_candidate_entry(repo_dir.path(), &head_sha))
            .unwrap();

        // Round 1: a real REWORK round, routed through the full pipeline.
        let reworked = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert!(
            matches!(reworked.as_slice(), [LandingOutcome::ReworkFiled(_)]),
            "expected a real REWORK round, got {reworked:?}"
        );

        // Round 2: a conflict-correction round on the SAME repo/branch/
        // target/task, routed and dispatched for real too. `route_conflict`
        // never inspects the working tree for an actual conflict itself —
        // the `detail` string is evidence it records, not something it
        // verifies — so the clean `review_candidate_repo` fixture is enough
        // to exercise this path on the exact same task the rework round
        // above used.
        let repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let conflict_entry = conflict_candidate_entry(repo_dir.path(), &head_sha);
        let held = pipeline
            .route_conflict(
                &conflict_entry,
                &repo,
                "CONFLICT (content): Merge conflict in src.rs",
            )
            .await
            .unwrap();
        assert!(matches!(held, LandingOutcome::ReworkFiled(_)));
        let chain_key = only_conflict_chain_key(&space);
        pipeline
            .dispatch_held_conflict("code-repo", "feature", Some(&chain_key))
            .await
            .unwrap();

        assert_eq!(
            rev_parse(repo_dir.path(), "main"),
            main_before,
            "neither round lands the branch"
        );

        let review_spans: Vec<Value> = crate::span::spans_for_task(&space, "code-repo", "add src")
            .unwrap()
            .into_iter()
            .filter(|s| s["phase"] == "semantic_review")
            .collect();
        assert_eq!(
            review_spans.len(),
            2,
            "both rounds must record their own SemanticReview span, not collapse onto one: \
             {review_spans:?}"
        );
        let rework_span = review_spans
            .iter()
            .find(|s| s["terminal_reason"] == "rework-requested")
            .expect("the rework round must record its own SemanticReview span");
        let conflict_span = review_spans
            .iter()
            .find(|s| s["terminal_reason"] == "conflict-correction-requested")
            .expect(
                "the conflict round must record its own SemanticReview span, not dedup-drop \
                 against the rework round's",
            );
        assert_eq!(rework_span["attempt"], 1, "{rework_span:?}");
        assert_eq!(
            conflict_span["attempt"], 2,
            "a conflict round after one rework round must share the rework round's numbering \
             line, not restart at 1: {conflict_span:?}"
        );
        assert_ne!(rework_span["attempt"], conflict_span["attempt"]);
    }

    /// The bug this fixes: a spawn refusal must leave the marker at a
    /// terminal `dispatch-refused` state, not stuck at `dispatching` forever
    /// — otherwise the NEXT redelivery reads the stuck marker as an
    /// interrupted daemon and raises the wrong diagnosis.
    #[tokio::test]
    async fn refused_rework_spawn_reaches_a_terminal_state_not_stuck_dispatching() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, main_before) = review_candidate_repo();

        let space = Space::open_in_memory().unwrap();
        space.out(verdict_tuple(&head_sha, "REWORK")).unwrap();

        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline.supervisor.set_dispatch_paused(true);
        pipeline
            .enqueue(review_candidate_entry(repo_dir.path(), &head_sha))
            .unwrap();

        let outcomes = pipeline.drain_key("code-repo", "main").await.unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(matches!(outcomes[0], LandingOutcome::ReworkFiled(_)));

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_eq!(main_before, main_after, "branch must not have landed");

        assert!(
            tuples(&space, Category::Event, "agent_spawned").is_empty(),
            "a refused dispatch must not journal an agent"
        );
        let markers = scoped_tuples(&space, Category::Event, REWORK_DISPATCH_IDENTITY);
        assert_eq!(
            markers.len(),
            2,
            "a refusal still writes the dispatching marker, then a terminal one"
        );
        assert!(
            markers.iter().any(|m| m.payload["state"] == "dispatching"),
            "{markers:?}"
        );
        assert!(
            markers
                .iter()
                .any(|m| m.payload["state"] == "dispatch-refused"),
            "the refusal must be recorded as its own terminal state, not left dispatching: \
             {markers:?}"
        );

        let needs = scoped_tuples(&space, Category::Need, STEWARD_NEED_IDENTITY);
        assert_eq!(
            needs.len(),
            1,
            "a refused dispatch must raise one human gate"
        );
        let text = needs[0].payload["text"].as_str().unwrap();
        assert!(text.contains("dispatch-refused"), "{text}");

        // Unpausing and redelivering must not misdiagnose the refusal as an
        // interrupted daemon (`dispatch-interrupted`) — the marker already
        // carries its own terminal state.
        pipeline.supervisor.set_dispatch_paused(false);
        let repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        pipeline
            .route_rework(&review_candidate_entry(repo_dir.path(), &head_sha), &repo)
            .await
            .unwrap();
        assert!(
            tuples(&space, Category::Event, "agent_spawned").is_empty(),
            "a redelivery must not silently dispatch behind the existing human gate"
        );
        assert_eq!(
            scoped_tuples(&space, Category::Need, STEWARD_NEED_IDENTITY).len(),
            1,
            "replay must converge on the existing human gate rather than raise a second one"
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

        // Terminal hold path: this route never reaches `route_rework`'s own
        // "rework-requested" span write (it returns from the `Withhold` arm
        // before that point), so `withhold_rework` is the ONLY place this
        // review round's SemanticReview span gets closed out.
        let review_span = crate::span::spans_for_task(&space, "code-repo", "add src")
            .unwrap()
            .into_iter()
            .find(|s| s["phase"] == "semantic_review")
            .expect("a withheld rework route must still close out the review phase");
        assert_eq!(review_span["terminal_reason"], "attempts-exhausted");
        assert!(review_span["started_at"].is_string(), "{review_span:?}");
        let review_duration = review_span["duration_ms"]
            .as_i64()
            .expect("a terminal hold must carry a real duration_ms");
        assert!(review_duration >= 0, "{review_span:?}");

        let repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        pipeline.route_rework(&entry, &repo).await.unwrap();
        assert_eq!(
            scoped_tuples(&space, Category::Need, STEWARD_NEED_IDENTITY).len(),
            1,
            "replay must converge on the existing human gate"
        );
        // Replay must not re-time the already-settled review span either.
        let review_span_after_replay = crate::span::spans_for_task(&space, "code-repo", "add src")
            .unwrap()
            .into_iter()
            .find(|s| s["phase"] == "semantic_review")
            .unwrap();
        assert_eq!(
            review_span_after_replay["duration_ms"], review_span["duration_ms"],
            "replay must not re-time the review span"
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

    /// A daemon restart while a review-death replacement is still waiting
    /// must resume the durable replacement attempt through `process_entry`.
    /// Re-entering the pipeline must neither revive the dead primary nor
    /// spend another retry slot before the active replacement can answer.
    #[tokio::test]
    async fn restart_resumes_in_flight_review_death_retry_through_process_entry() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow_dies_on_primary_recovers_on_retry(&layout);
        let (repo_dir, head_sha, main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);
        let gates = GateConfig {
            review_timeout: Duration::from_secs(120),
            review_max_wait: Duration::from_secs(600),
            ..GateConfig::default()
        };

        // The first daemon observes the primary death and dispatches the
        // replacement. Abort its wait after the dispatch marker is durable,
        // leaving the replacement workflow running as it would across a
        // process restart.
        let space = Space::open(&layout.db_path()).unwrap();
        // Both the original and the restarted daemon drive the shipped
        // default backoff through the deterministic seam — this test is
        // about resuming the marker's instance id, not about the wait.
        let clock = FakeSchedule::new(0.0);
        let pipeline = Arc::new(
            test_pipeline(home.path(), space.clone()).with_retry_schedule(clock.schedule()),
        );
        let primary = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let gates = gates.clone();
            async move { pipeline.request_review(&entry, &gates).await }
        });
        wait_for_spawn_count(&space, 1).await;
        let ReviewWaitOutcome::ReviewerDied(context) = primary.await.unwrap().unwrap() else {
            panic!("expected the primary reviewer to die");
        };

        let retry = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let gates = gates.clone();
            async move {
                pipeline
                    .route_verdict(&entry, ReviewWaitOutcome::ReviewerDied(context), &gates)
                    .await
            }
        });
        wait_for_spawn_count(&space, 2).await;
        assert!(
            scoped_tuples(&space, Category::Event, REVIEW_DEATH_DISPATCH_IDENTITY)
                .iter()
                .any(|marker| marker.payload["state"] == "dispatching"),
            "the restart must begin from the durable in-flight marker"
        );
        retry.abort();
        let _ = retry.await;

        // A fresh pipeline stands in for the restarted daemon. Its
        // process_entry path must dispatch/await the retry instance id that
        // the marker names, rather than calling request_review for primary.
        let restarted_space = Space::open(&layout.db_path()).unwrap();
        // Same clock: the first daemon already waited out the persisted
        // schedule, so the restart reads it back as elapsed and adds no
        // further wait — exactly what a real restart past `not_before` sees.
        let restarted = Arc::new(
            test_pipeline(home.path(), restarted_space.clone())
                .with_retry_schedule(clock.schedule()),
        );
        // A real daemon restart rehydrates every durable workflow instance
        // into the engine's in-memory map (`Server::run`, before any dispatch
        // loop touches the landing queue) — that is what makes
        // `dispatch_review`'s per-`instance_id` idempotency
        // (`WorkflowEngine::store_if_absent`) actually hold across a restart.
        // A bare, never-rehydrated engine has no record of the retry instance
        // the first daemon already dispatched, so it would treat the same id
        // as new and dispatch a duplicate — mirror the real startup sequence
        // here so this test exercises the actual restart invariant.
        restarted.engine.rehydrate();
        let process = tokio::spawn({
            let restarted = Arc::clone(&restarted);
            let entry = entry.clone();
            async move { restarted.process_entry(&entry).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        let retry_instance_id = review_retry_instance_id(&entry, 1);
        restarted_space
            .out(Tuple::new(
                Category::Artifact,
                &entry.repo_name,
                REVIEW_ARTIFACT_IDENTITY,
                "replacement-reviewer",
                json!({
                    "task": entry.task,
                    "recommendation": "APPROVE",
                    "notes": "clean after restart",
                    "head_sha": entry.head_sha,
                    "branch": entry.branch,
                    "target": entry.target,
                    "review_attempt": retry_instance_id,
                }),
            ))
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(10), process)
            .await
            .expect("the restarted pipeline must finish from the replacement verdict")
            .unwrap()
            .unwrap();
        assert!(
            matches!(&outcome, LandingOutcome::Landed(result) if result["merged"] == true),
            "expected the replacement verdict to land, got {outcome:?}"
        );
        assert_ne!(
            main_before,
            rev_parse(repo_dir.path(), "main"),
            "the restarted pipeline must land the reviewed branch"
        );
        assert_eq!(
            restarted_space
                .scan(&Pattern::category(Category::Event).identity("agent_spawned"))
                .unwrap()
                .len(),
            2,
            "restart recovery must not spawn a fresh primary or duplicate retry"
        );
    }

    /// P4a gap (1): reviewer tier routing never reached the live path — every
    /// review spawn resolved through the workflow's fixed `reviewer` profile
    /// regardless of the candidate ticket's own priority/labels. Proves the
    /// fix on the REAL landing path: a ticket carrying a priority a tier rule
    /// matches routes the reviewer spawn through that tier's profile, not the
    /// workflow's baseline `reviewer` profile.
    #[tokio::test]
    async fn real_review_path_routes_the_reviewer_through_ticket_priority_and_labels() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_routed_review_workflow(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        // The candidate ticket the routing predicate reads — `entry.task` is
        // the lookup key `review_candidate_routing` uses.
        space
            .out(Tuple::new(
                Category::Task,
                "code-repo",
                entry.task.clone(),
                "castle",
                json!({"priority": "urgent", "labels": ["security"]}),
            ))
            .unwrap();

        let tiers = TierRouting {
            rules: vec![rk_workflow::TierRule {
                priority: Some("urgent".into()),
                label: None,
                tier: "premium".into(),
            }],
        };
        let global_agents = HashMap::from([(
            "premium".to_string(),
            rk_workflow::AgentProfile {
                harness: Some("fake".into()),
                model: Some("premium-model".into()),
                permission_mode: None,
            },
        )]);
        let pipeline = Arc::new(test_pipeline_routed(
            home.path(),
            space.clone(),
            global_agents,
            tiers,
        ));
        let gates = GateConfig::default();

        let outcome = {
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let handle = tokio::spawn(async move { pipeline.request_review(&entry, &gates).await });
            wait_for_spawn_count(&space, 1).await;
            space.out(verdict_tuple(&head_sha, "APPROVE")).unwrap();
            handle.await.unwrap().unwrap()
        };
        let ReviewWaitOutcome::Verdict(v) = outcome else {
            panic!("expected Verdict, got {outcome:?}");
        };
        assert_eq!(v, "APPROVE");

        let reviewer = pipeline
            .supervisor
            .list_all()
            .into_iter()
            .find(|r| r.role == "reviewer")
            .expect("reviewer must have spawned");
        assert_eq!(
            reviewer.model.as_deref(),
            Some("premium-model"),
            "the candidate ticket's priority must route the reviewer spawn through \
             the tier table instead of the workflow's fixed reviewer profile"
        );
    }

    /// P4a gap (2): shadow review was schema-only. When the repo's policy
    /// configures `shadowReviewModel`, `request_review` must launch EXACTLY
    /// one secondary reviewer, bound to the same task/branch/head/target as
    /// the primary but under a distinct review attempt, and the two verdicts
    /// (recorded on the primary vs. carried in `ShadowReview`) must stay
    /// structurally distinct.
    #[tokio::test]
    async fn shadow_review_launches_exactly_one_secondary_reviewer_with_exact_binding() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_routed_review_workflow(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        let gates = GateConfig {
            shadow_review_model: "shadow-model".into(),
            shadow_review_harness: "fake".into(),
            ..GateConfig::default()
        };

        let primary_attempt = review_instance_id(&entry);
        let shadow_attempt = format!("{primary_attempt}{SHADOW_INSTANCE_SUFFIX}");

        let outcome = {
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let handle = tokio::spawn(async move { pipeline.request_review(&entry, &gates).await });
            // Exactly two spawns: the primary and the one shadow — never more.
            assert_eq!(wait_for_spawn_count(&space, 2).await, 2);
            space.out(verdict_tuple(&head_sha, "APPROVE")).unwrap();
            handle.await.unwrap().unwrap()
        };
        let ReviewWaitOutcome::Verdict(v) = outcome else {
            panic!("expected Verdict, got {outcome:?}");
        };
        assert_eq!(
            v, "APPROVE",
            "the primary verdict is what request_review returns"
        );

        // Neither more spawns happened waiting for the shadow to settle.
        assert_eq!(
            space
                .scan(&Pattern::category(Category::Event).identity("agent_spawned"))
                .unwrap()
                .len(),
            2,
            "exactly one primary and one shadow reviewer, never more"
        );

        let records = pipeline.supervisor.list_all();
        let primary = records
            .iter()
            .find(|r| r.review.as_ref().map(|rv| rv.attempt.as_str()) == Some(&primary_attempt))
            .expect("primary reviewer record must exist");
        let shadow = records
            .iter()
            .find(|r| r.review.as_ref().map(|rv| rv.attempt.as_str()) == Some(&shadow_attempt))
            .expect("shadow reviewer record must exist");

        // Exact binding: same task/branch/head/target, distinct attempt.
        let pr = primary.review.as_ref().unwrap();
        let sr = shadow.review.as_ref().unwrap();
        assert_eq!(pr.branch, sr.branch);
        assert_eq!(pr.head_sha, sr.head_sha);
        assert_eq!(pr.target, sr.target);
        assert_eq!(pr.task, sr.task);
        assert_ne!(pr.attempt, sr.attempt);
        assert_eq!(sr.attempt, shadow_attempt);

        // Distinct models: the shadow's inline override beats the tier
        // table / named profile, the primary keeps the workflow's own.
        assert_eq!(primary.model.as_deref(), Some("reviewer-model"));
        assert_eq!(shadow.model.as_deref(), Some("shadow-model"));

        // The comparison record is written off the landing path (fire and
        // forget) — supply the shadow's own verdict so the detached task
        // doesn't have to run out its full wait budget, then poll for it.
        space
            .out(Tuple::new(
                Category::Artifact,
                "code-repo",
                REVIEW_ARTIFACT_IDENTITY,
                "some-other-reviewer",
                json!({
                    "task": entry.task,
                    "recommendation": "APPROVE",
                    "notes": "shadow notes",
                    "head_sha": head_sha,
                    "branch": "feature",
                    "target": "main",
                    "review_attempt": shadow_attempt,
                }),
            ))
            .unwrap();

        let comparison = wait_for_comparison(&space, "code-repo").await;
        assert_eq!(comparison.payload["task"], "add src");
        assert_eq!(comparison.payload["branch"], "feature");
        assert_eq!(comparison.payload["head_sha"], head_sha);
        assert_eq!(comparison.payload["target"], "main");
        assert_eq!(comparison.payload["review_attempt"], primary_attempt);
        assert_eq!(comparison.payload["shadow_attempt"], shadow_attempt);
        assert_eq!(comparison.payload["primary_identity"], json!(primary.name));
        assert_eq!(comparison.payload["shadow_identity"], json!(shadow.name));
        assert_eq!(comparison.payload["primary_model"], "reviewer-model");
        assert_eq!(comparison.payload["shadow_model"], "shadow-model");
        assert_eq!(comparison.payload["primary_verdict"], "APPROVE");
        assert_eq!(comparison.payload["shadow_verdict"], "APPROVE");
        assert_eq!(comparison.payload["agreement"], "agree");
        assert_eq!(
            comparison.payload["authoritative"], "primary",
            "the shadow verdict must never be recorded as authoritative"
        );
        assert!(comparison.payload["primary_spend_usd"].is_number());
        assert!(comparison.payload["shadow_spend_usd"].is_number());
        assert!(
            comparison.payload["recorded_at"]
                .as_str()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .is_some(),
            "recorded_at must be a real timestamp: {comparison:?}"
        );
    }

    /// The shadow's verdict must never gate landing: a primary APPROVE lands
    /// the candidate even when the shadow disagrees, and the disagreement is
    /// recorded (not silently dropped or mistaken for agreement).
    #[tokio::test]
    async fn shadow_disagreement_is_recorded_and_never_blocks_the_primary_verdict() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_routed_review_workflow(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        let gates = GateConfig {
            shadow_review_model: "shadow-model".into(),
            shadow_review_harness: "fake".into(),
            ..GateConfig::default()
        };
        let primary_attempt = review_instance_id(&entry);
        let shadow_attempt = format!("{primary_attempt}{SHADOW_INSTANCE_SUFFIX}");

        let outcome = {
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let handle = tokio::spawn(async move { pipeline.request_review(&entry, &gates).await });
            assert_eq!(wait_for_spawn_count(&space, 2).await, 2);
            space.out(verdict_tuple(&head_sha, "APPROVE")).unwrap();
            handle.await.unwrap().unwrap()
        };
        let ReviewWaitOutcome::Verdict(v) = outcome else {
            panic!("expected Verdict, got {outcome:?}");
        };
        assert_eq!(
            v, "APPROVE",
            "the primary's verdict is authoritative regardless of what the shadow says"
        );

        space
            .out(Tuple::new(
                Category::Artifact,
                "code-repo",
                REVIEW_ARTIFACT_IDENTITY,
                "some-other-reviewer",
                json!({
                    "task": entry.task,
                    "recommendation": "REWORK",
                    "notes": "shadow disagrees",
                    "head_sha": head_sha,
                    "branch": "feature",
                    "target": "main",
                    "review_attempt": shadow_attempt,
                }),
            ))
            .unwrap();

        let comparison = wait_for_comparison(&space, "code-repo").await;
        assert_eq!(comparison.payload["primary_verdict"], "APPROVE");
        assert_eq!(comparison.payload["shadow_verdict"], "REWORK");
        assert_eq!(comparison.payload["agreement"], "disagree");
        assert_eq!(comparison.payload["authoritative"], "primary");
    }

    /// A shadow reviewer that dies without ever producing a verdict (crash,
    /// budget death) must not affect the primary at all: landing still
    /// proceeds off the primary's verdict, and the comparison records the
    /// three-state "no-verdict" rather than being silently skipped or
    /// mistaken for disagreement.
    #[tokio::test]
    async fn shadow_reviewer_death_is_recorded_without_affecting_the_primary() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow_with_shadow_death(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        let gates = GateConfig {
            shadow_review_model: "shadow-model".into(),
            shadow_review_harness: "fake".into(),
            review_max_wait: Duration::from_secs(30),
            ..GateConfig::default()
        };

        let started = tokio::time::Instant::now();
        let outcome = tokio::time::timeout(Duration::from_secs(15), async {
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let handle = tokio::spawn(async move { pipeline.request_review(&entry, &gates).await });
            assert_eq!(wait_for_spawn_count(&space, 2).await, 2);
            space.out(verdict_tuple(&head_sha, "APPROVE")).unwrap();
            handle.await.unwrap().unwrap()
        })
        .await
        .expect("a dead shadow must not stall the primary's own escalation path");
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "the primary's verdict must resolve without waiting on the dead shadow"
        );
        let ReviewWaitOutcome::Verdict(v) = outcome else {
            panic!("expected Verdict, got {outcome:?}");
        };
        assert_eq!(v, "APPROVE");

        let comparison = wait_for_comparison(&space, "code-repo").await;
        assert_eq!(comparison.payload["primary_verdict"], "APPROVE");
        assert!(
            comparison.payload["shadow_verdict"].is_null(),
            "a dead shadow must record no verdict, not a fabricated one: {comparison:?}"
        );
        assert_eq!(
            comparison.payload["agreement"], "no-verdict",
            "a shadow that never answered must be distinct from disagreement"
        );
    }

    /// A restarted daemon reprocessing the same candidate (the late-verdict
    /// replay path `park_and_resume_survives_space_level_restart_with_late_verdict`
    /// already proves for the primary alone) must not duplicate the SHADOW
    /// reviewer either, nor write a second comparison record.
    #[tokio::test]
    async fn restart_replay_does_not_duplicate_the_shadow_reviewer_or_its_comparison() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_routed_review_workflow(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);
        let gates = GateConfig {
            shadow_review_model: "shadow-model".into(),
            shadow_review_harness: "fake".into(),
            ..GateConfig::default()
        };
        let primary_attempt = review_instance_id(&entry);
        let shadow_attempt = format!("{primary_attempt}{SHADOW_INSTANCE_SUFFIX}");

        // "Before restart": park on both verdict tuples, then simulate a
        // crash before either arrives.
        {
            let space = Space::open(&layout.db_path()).unwrap();
            let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
            let handle = tokio::spawn({
                let pipeline = Arc::clone(&pipeline);
                let entry = entry.clone();
                let gates = gates.clone();
                async move { pipeline.request_review(&entry, &gates).await }
            });
            assert_eq!(wait_for_spawn_count(&space, 2).await, 2);
            handle.abort();
            let _ = handle.await;
        }

        // "After restart": both verdicts land late, against a fresh Space
        // handle over the same durable store and a brand-new pipeline/engine
        // — nothing carried over in memory from the aborted attempt.
        let space = Space::open(&layout.db_path()).unwrap();
        space.out(verdict_tuple(&head_sha, "APPROVE")).unwrap();
        space
            .out(Tuple::new(
                Category::Artifact,
                "code-repo",
                REVIEW_ARTIFACT_IDENTITY,
                "some-other-reviewer",
                json!({
                    "task": entry.task,
                    "recommendation": "APPROVE",
                    "notes": "late shadow verdict",
                    "head_sha": head_sha,
                    "branch": "feature",
                    "target": "main",
                    "review_attempt": shadow_attempt,
                }),
            ))
            .unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let outcome = pipeline.request_review(&entry, &gates).await.unwrap();
        let ReviewWaitOutcome::Verdict(v) = outcome else {
            panic!("expected Verdict, got {outcome:?}");
        };
        assert_eq!(v, "APPROVE");

        assert_eq!(
            space
                .scan(&Pattern::category(Category::Event).identity("agent_spawned"))
                .unwrap()
                .len(),
            2,
            "the restarted pipeline must not spawn a second primary or a second shadow"
        );

        let comparisons = wait_for_comparison_count(&space, "code-repo", 1).await;
        assert_eq!(
            comparisons, 1,
            "restart/replay must not duplicate the shadow comparison record"
        );
    }

    /// Repository policy defaults to authoritative-only review: no
    /// `shadowReviewModel` configured means `request_review` spawns exactly
    /// one reviewer and no comparison record is ever written. The acceptance
    /// bar this whole feature shipped under (`docs/2026-08-19-rk-phase-2-epic-rev3.md`
    /// P4a: "default unchanged until an explicit follow-up ticket flips it").
    #[tokio::test]
    async fn shadow_review_disabled_by_default_spawns_only_the_primary() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        // The policy-derived default, exactly as `LandingPipeline::gate_config`
        // resolves it for a repo with no activated landing policy.
        let gates = GateConfig::default();
        assert_eq!(
            gates.shadow_review_model, "",
            "shadow review must default to disabled"
        );

        let outcome = {
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let handle = tokio::spawn(async move { pipeline.request_review(&entry, &gates).await });
            wait_for_spawn_count(&space, 1).await;
            space.out(verdict_tuple(&head_sha, "APPROVE")).unwrap();
            handle.await.unwrap().unwrap()
        };
        let ReviewWaitOutcome::Verdict(v) = outcome else {
            panic!("expected Verdict, got {outcome:?}");
        };
        assert_eq!(v, "APPROVE");

        // Give any (incorrectly) launched shadow comparison task a moment to
        // land before asserting its absence.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            space
                .scan(&Pattern::category(Category::Event).identity("agent_spawned"))
                .unwrap()
                .len(),
            1,
            "the default policy must never launch a shadow reviewer"
        );
        assert!(
            space
                .scan(
                    &Pattern::category(Category::Artifact)
                        .scope("code-repo")
                        .identity(SHADOW_COMPARISON_IDENTITY)
                )
                .unwrap()
                .is_empty(),
            "no comparison record when shadow review is disabled"
        );
    }

    /// Poll until at least one `review-shadow-comparison` artifact is
    /// visible for `repo` — the detached comparison task
    /// ([`LandingPipeline::spawn_shadow_comparison`]) writes off the landing
    /// path, so tests must poll for it rather than assume it lands
    /// synchronously with the primary's verdict.
    async fn wait_for_comparison(space: &Space, repo: &str) -> Tuple {
        for _ in 0..400 {
            let found = space
                .scan(
                    &Pattern::category(Category::Artifact)
                        .scope(repo)
                        .identity(SHADOW_COMPARISON_IDENTITY),
                )
                .unwrap();
            if let Some(t) = found.into_iter().next() {
                return t;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for a review-shadow-comparison artifact");
    }

    /// Like [`wait_for_comparison`], but waits for an exact COUNT to settle
    /// (rather than returning on the first sighting) — the restart/replay
    /// duplicate-suppression test needs to see that the count never exceeds
    /// `want`, not merely that it eventually reaches it.
    async fn wait_for_comparison_count(space: &Space, repo: &str, want: usize) -> usize {
        let mut last = 0;
        for _ in 0..400 {
            last = space
                .scan(
                    &Pattern::category(Category::Artifact)
                        .scope(repo)
                        .identity(SHADOW_COMPARISON_IDENTITY),
                )
                .unwrap()
                .len();
            if last >= want {
                // Settle a little longer to catch a spurious duplicate
                // written just after the count first reached `want`.
                tokio::time::sleep(Duration::from_millis(200)).await;
                return space
                    .scan(
                        &Pattern::category(Category::Artifact)
                            .scope(repo)
                            .identity(SHADOW_COMPARISON_IDENTITY),
                    )
                    .unwrap()
                    .len();
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        last
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

    /// TKT-01M0PH88BX7T8BHTT5224SHFKZ's second acceptance leg: a `target`
    /// name match ALONE is not enough to keep a non-`landed` verdict
    /// current. A branch held `gate-held` against `main`'s tip at S1 must not
    /// stay wedged forever once `main` legitimately advances to S2 — the
    /// diff a redelivered completion carries could gate differently against
    /// the ref as it stands now, and permanently trusting the S1-era verdict
    /// would silently drop a since-fixed branch on the floor. Confirms both
    /// halves of `LandingPipeline::admission_marker`: freshly admitted after
    /// the move, and still deduped again once the target is stable.
    #[tokio::test]
    async fn moved_target_reprocesses_a_gate_held_verdict_instead_of_staying_wedged() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        write_checks(repo_dir.path(), VERIFY_FAILS_CHECKS);
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("work.txt"), "v1\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "work"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let base_entry = LandingQueueEntry {
            repo_name: "moved-target-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: head_sha.clone(),
            diff_class: "doc-only".into(),
            task: "moved-target-task".into(),
            ..Default::default()
        };

        pipeline.enqueue(base_entry.clone()).unwrap().unwrap();
        let first = pipeline
            .drain_key("moved-target-repo", "main")
            .await
            .unwrap();
        assert!(
            matches!(first.as_slice(), [LandingOutcome::GateHeld]),
            "{first:?}"
        );
        let main_at_first_hold = rev_parse(repo_dir.path(), "main");

        // A same-key resubmission while `main` has not moved is still an
        // ordinary redelivery: suppressed, no second gate run.
        assert_eq!(
            pipeline.enqueue(base_entry.clone()).unwrap(),
            None,
            "an unmoved target must still dedup the redelivery"
        );

        // `main` legitimately advances — unrelated work landing, nothing to
        // do with this held branch.
        std::fs::write(repo_dir.path().join("unrelated.txt"), "other work\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(
            repo_dir.path(),
            &["commit", "-m", "unrelated work advances main"],
        );
        let main_after_move = rev_parse(repo_dir.path(), "main");
        assert_ne!(main_at_first_hold, main_after_move);

        // The identical branch/head/target/task resubmitted post-move must
        // be admitted afresh, not read back as already_processed off the
        // stale S1-era gate-held marker.
        let reenqueued = pipeline.enqueue(base_entry.clone()).unwrap();
        assert!(
            reenqueued.is_some(),
            "a gate-held verdict recorded against the target's old tip must not permanently \
             wedge the branch once the target has moved"
        );
        let second = pipeline
            .drain_key("moved-target-repo", "main")
            .await
            .unwrap();
        assert!(
            matches!(second.as_slice(), [LandingOutcome::GateHeld]),
            "{second:?}"
        );

        let markers = space
            .scan(
                &Pattern::category(Category::Event)
                    .scope("moved-target-repo")
                    .identity(LANDING_PROCESSED_IDENTITY),
            )
            .unwrap();
        assert_eq!(
            markers.len(),
            2,
            "the post-move reprocessing must leave its own marker behind, not overwrite or skip \
             recording it: {markers:?}"
        );
        let target_heads: Vec<Option<&str>> = markers
            .iter()
            .map(|t| t.payload.get("target_head").and_then(Value::as_str))
            .collect();
        assert!(
            target_heads.contains(&Some(main_at_first_hold.as_str())),
            "{target_heads:?}"
        );
        assert!(
            target_heads.contains(&Some(main_after_move.as_str())),
            "{target_heads:?}"
        );

        // And now that the target is stable again (at S2), a further
        // redelivery goes back to being an ordinary dedup — not an infinite
        // reprocessing loop.
        assert_eq!(
            pipeline.enqueue(base_entry).unwrap(),
            None,
            "a stable post-move target must dedup again, not reprocess every redelivery forever"
        );
    }

    /// The `landed` half of the same acceptance leg: once a candidate has
    /// actually landed, the target moving FURTHER afterward must never
    /// reopen it — a `landed` marker is sticky regardless of `target_head`,
    /// unlike a `gate-held`/`rework-filed`/`escalated` one. Re-litigating an
    /// already-delivered merge on every later redelivery would risk a
    /// duplicate merge commit for zero benefit (module doc: the CAS already
    /// makes a repeat land harmless, but there is still no reason to redo
    /// the work).
    #[tokio::test]
    async fn moved_target_does_not_reopen_an_already_landed_verdict() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        write_checks(repo_dir.path(), ALL_PASS_CHECKS);
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("work.txt"), "v1\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "work"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let base_entry = LandingQueueEntry {
            repo_name: "moved-target-landed-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha,
            diff_class: "doc-only".into(),
            task: "moved-target-landed-task".into(),
            ..Default::default()
        };

        pipeline.enqueue(base_entry.clone()).unwrap().unwrap();
        let first = pipeline
            .drain_key("moved-target-landed-repo", "main")
            .await
            .unwrap();
        assert!(
            matches!(first.as_slice(), [LandingOutcome::Landed(_)]),
            "{first:?}"
        );
        let main_after_land = rev_parse(repo_dir.path(), "main");

        // `main` advances further with unrelated work, same as any busy
        // branch would between the real land and a later redelivered
        // completion.
        std::fs::write(repo_dir.path().join("unrelated.txt"), "other work\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(
            repo_dir.path(),
            &["commit", "-m", "unrelated work advances main again"],
        );
        let main_after_further_move = rev_parse(repo_dir.path(), "main");
        assert_ne!(main_after_land, main_after_further_move);

        assert_eq!(
            pipeline.enqueue(base_entry).unwrap(),
            None,
            "a landed verdict must stay suppressed even after the target moves further, not \
             reopen and risk a duplicate merge"
        );
        assert!(
            space
                .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
                .unwrap()
                .is_empty(),
            "the suppressed redelivery must never leave a live queue entry behind"
        );
        assert_eq!(
            rev_parse(repo_dir.path(), "main"),
            main_after_further_move,
            "no duplicate merge: main must not move again from the suppressed redelivery"
        );
    }

    /// task_done admission (`Reactor::fire_land_action`'s call shape,
    /// `LandingPipeline::enqueue`) of a candidate whose source head never
    /// diverged from its target: classified as an explicit no-op before it
    /// is ever queued — no `landing_queue_entry` tuple, no gate, no
    /// checks.cue read at all (deliberately absent here: reaching
    /// `gate_plan` would fail closed into `NoGate`, not `Empty`, so its
    /// absence proves the short-circuit ran first).
    #[tokio::test]
    async fn task_done_admission_of_an_empty_candidate_is_classified_before_queueing() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        git(repo_dir.path(), &["checkout", "main"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        let main_before = rev_parse(repo_dir.path(), "main");
        assert_eq!(
            head_sha, main_before,
            "feature must never have diverged from main"
        );

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let entry = LandingQueueEntry {
            repo_name: "empty-admission-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: head_sha.clone(),
            diff_class: "doc-only".into(),
            task: "empty-admission-task".into(),
            ..Default::default()
        };

        assert_eq!(
            pipeline.enqueue(entry).unwrap(),
            None,
            "an empty candidate must never occupy a queue slot"
        );
        assert!(
            space
                .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
                .unwrap()
                .is_empty(),
            "classifying as empty must never create a live queue entry"
        );
        let processed = space
            .scan(&Pattern::category(Category::Event).identity(LANDING_PROCESSED_IDENTITY))
            .unwrap();
        assert_eq!(processed.len(), 1);
        assert_eq!(
            processed[0].payload.get("outcome").and_then(Value::as_str),
            Some("empty")
        );
        let events = space
            .scan(&Pattern::category(Category::Event).identity(LANDING_EMPTY_IDENTITY))
            .unwrap();
        assert_eq!(events.len(), 1);
        let payload = &events[0].payload;
        assert_eq!(
            payload.get("repo").and_then(Value::as_str),
            Some("empty-admission-repo")
        );
        assert_eq!(
            payload.get("branch").and_then(Value::as_str),
            Some("feature")
        );
        assert_eq!(payload.get("target").and_then(Value::as_str), Some("main"));
        assert_eq!(
            payload.get("head_sha").and_then(Value::as_str),
            Some(head_sha.as_str())
        );
        assert_eq!(
            payload.get("task").and_then(Value::as_str),
            Some("empty-admission-task")
        );
        assert!(payload.get("reason").and_then(Value::as_str).is_some());
        assert_eq!(rev_parse(repo_dir.path(), "main"), main_before);
    }

    /// The direct `rk land` path (`repo.land` RPC, `LandingPipeline::submit_manual`):
    /// an empty candidate must report the SAME explicit `status: "empty"`,
    /// `merged: false`, `delivered: false` shape a caller gets back from a
    /// genuinely landed or held branch — not the generic
    /// `already_processed` detail a same-key redelivery of an ordinary
    /// outcome gets. A second call for the identical branch/head/target
    /// must replay the same explicit status rather than a vague
    /// already-processed marker with no distinguishing detail.
    #[tokio::test]
    async fn direct_land_of_an_empty_candidate_reports_an_explicit_empty_status() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        git(repo_dir.path(), &["checkout", "main"]);
        let main_before = rev_parse(repo_dir.path(), "main");

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());

        let result = pipeline
            .submit_manual(
                repo_dir.path(),
                "feature",
                "main",
                false,
                Some("direct-land-empty-task".into()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(result["status"], "empty");
        assert_eq!(result["merged"], false);
        assert_eq!(result["delivered"], false);
        assert!(result.get("reason").and_then(Value::as_str).is_some());

        // Replay: the same submission again must still report the explicit
        // empty status, not a generic already-processed detail with no
        // status field at all.
        let replay = pipeline
            .submit_manual(
                repo_dir.path(),
                "feature",
                "main",
                false,
                Some("direct-land-empty-task".into()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(replay["status"], "empty");
        assert_eq!(replay["merged"], false);
        assert_eq!(replay["delivered"], false);

        assert!(space
            .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
            .unwrap()
            .is_empty());
        assert_eq!(
            space
                .scan(&Pattern::category(Category::Event).identity(LANDING_EMPTY_IDENTITY))
                .unwrap()
                .len(),
            1,
            "the replay must dedup, not write a second empty event"
        );
        assert_eq!(rev_parse(repo_dir.path(), "main"), main_before);
    }

    /// An entry already sitting `Queued` — as it would after a restart, or
    /// as an entry admitted by an older daemon build before this
    /// short-circuit existed — must converge to the same terminal `Empty`
    /// outcome without ever reaching a gate. Inserted with the low-level
    /// `queue.enqueue` (bypassing `enqueue_disposition`'s own admission-time
    /// check) so this exercises `process_entry`'s independent check, not the
    /// admission one `task_done_admission_of_an_empty_candidate_is_classified_before_queueing`
    /// already covers. No checks.cue: reaching `gate_plan` would fail closed
    /// into `NoGate`, proving this outcome is `Empty` only because the gate
    /// was never attempted.
    #[tokio::test]
    async fn restart_discovers_an_already_queued_empty_candidate_and_lands_nothing() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        git(repo_dir.path(), &["checkout", "main"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        let main_before = rev_parse(repo_dir.path(), "main");

        let entry = LandingQueueEntry {
            repo_name: "restart-empty-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha,
            diff_class: "doc-only".into(),
            task: "restart-empty-task".into(),
            ..Default::default()
        };

        {
            let space = Space::open(&layout.db_path()).unwrap();
            let pipeline = test_pipeline(home.path(), space);
            pipeline.queue.enqueue(entry).unwrap();
        }

        // "After restart": fresh Space handle over the same on-disk store.
        let space = Space::open(&layout.db_path()).unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        let outcomes = pipeline.run_cycle().await.unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(
            matches!(&outcomes[0], LandingOutcome::Empty(_)),
            "expected Empty, got {:?}",
            outcomes[0]
        );
        assert!(
            space
                .scan(&Pattern::category(Category::Event).identity(LANDING_QUEUE_IDENTITY))
                .unwrap()
                .is_empty(),
            "the restart-discovered entry must be removed from the live queue"
        );
        assert_eq!(rev_parse(repo_dir.path(), "main"), main_before);
    }

    /// A candidate genuinely non-empty at admission can become empty while
    /// it sits `Queued` — the target catches all the way up to its exact
    /// head through an unrelated path (an operator's manual merge, another
    /// candidate carrying the identical commit). `process_entry` must
    /// re-check freshly against the live target rather than trusting the
    /// admission-time classification, and must converge to `Empty` without
    /// ever reaching a gate (no checks.cue here either, for the same reason
    /// as the restart test above).
    #[tokio::test]
    async fn target_advancing_to_the_source_head_while_queued_converges_to_empty() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("work.txt"), "v1\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "work"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let entry = LandingQueueEntry {
            repo_name: "advance-while-queued-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: head_sha.clone(),
            diff_class: "doc-only".into(),
            task: "advance-while-queued-task".into(),
            ..Default::default()
        };
        // Genuinely non-empty at admission: main does not yet contain
        // `feature`'s commit, so this is admitted onto the queue normally.
        assert!(pipeline.enqueue(entry).unwrap().is_some());

        // Advance main to include feature's exact head through an unrelated
        // path — not through this pipeline at all.
        git(
            repo_dir.path(),
            &["merge", "--no-ff", "feature", "-m", "external merge"],
        );
        let main_after_external_merge = rev_parse(repo_dir.path(), "main");
        assert_ne!(main_after_external_merge, head_sha);

        let outcomes = pipeline
            .drain_key("advance-while-queued-repo", "main")
            .await
            .unwrap();
        assert!(
            matches!(&outcomes[0], LandingOutcome::Empty(_)),
            "expected Empty once main already contains feature's head, got {:?}",
            outcomes[0]
        );
        assert_eq!(
            rev_parse(repo_dir.path(), "main"),
            main_after_external_merge,
            "no second merge: the pipeline must not touch main once it already contains the head"
        );
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
        // `release` must be a protected-final target too, not just `main` —
        // otherwise it is an INNER edge under the new focused-checks policy
        // (default `focusedChecks: []`) and never runs the barrier-gated
        // `verify` check this test depends on to prove genuine concurrency.
        activate_landing_policy(
            home.path(),
            repo_dir.path(),
            rk_workflow::LandingPolicy {
                protected_targets: vec!["main".into(), "release".into()],
                ..Default::default()
            },
        );

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
    /// window — it is detected fast (well before even a generous base
    /// `reviewTimeout`), carrying the instance's own captured failure
    /// context. Routing it (under the default review-death policy, one
    /// automatic retry) dispatches exactly one replacement reviewer at the
    /// SAME exact branch/head/target/task; the fixture's replacement also
    /// dies, so the chain exhausts its one attempt and escalates — with the
    /// escalation text reading as a dead reviewer (never the live-at-ceiling
    /// case) and carrying the exhaustion code.
    #[tokio::test]
    async fn dead_reviewer_escalates_fast_with_death_context() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_broken_review_workflow(&layout);
        let (repo_dir, head_sha, main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        // This test measures reviewer-death detection and escalation, not the
        // shipped retry delay. Drive the default 30s backoff through the fake
        // schedule so it advances deterministically without consuming wall
        // clock time.
        let clock = FakeSchedule::new(0.0);
        let pipeline = Arc::new(
            test_pipeline(home.path(), space.clone()).with_retry_schedule(clock.schedule()),
        );
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

        let routed = tokio::time::timeout(
            Duration::from_secs(10),
            pipeline.route_verdict(
                &entry,
                ReviewWaitOutcome::ReviewerDied(context.clone()),
                &gates,
            ),
        )
        .await
        .expect("the one automatic retry and its escalation must also resolve well under 10s")
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
            text.contains("attempts-exhausted"),
            "the one default retry attempt must be spent before escalating: {text}"
        );
        assert!(
            !text.contains("ceiling"),
            "a dead reviewer must not read as the live-at-ceiling case: {text}"
        );
        assert!(
            text.contains(&format!("--repo {}", entry.repo_path)),
            "human recovery must use the registered checkout path: {text}"
        );

        assert_eq!(
            tuples(&space, Category::Event, "agent_spawned").len(),
            2,
            "the dead primary plus exactly one replacement reviewer, no more"
        );
        assert_eq!(clock.waits(), vec![Duration::from_secs(30)]);
        let markers = scoped_tuples(&space, Category::Event, REVIEW_DEATH_DISPATCH_IDENTITY);
        let dispatching = markers
            .iter()
            .find(|m| m.payload["state"] == "dispatching")
            .expect("the retry dispatch must be recorded");
        assert_eq!(dispatching.payload["branch"], entry.branch);
        assert_eq!(dispatching.payload["head_sha"], entry.head_sha);
        assert_eq!(dispatching.payload["target"], entry.target);
        assert_eq!(dispatching.payload["task"], entry.task);
        assert_eq!(
            dispatching.payload["instance_id"],
            review_retry_instance_id(&entry, 1),
        );

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_eq!(main_before, main_after, "branch must not have landed");
    }

    /// The retry ladder's happy path: a primary that dies before a verdict
    /// is automatically replaced by exactly one reviewer at the SAME exact
    /// head, and when THAT reviewer produces a real verdict, routing lands
    /// on it normally — no human ever polls anything.
    #[tokio::test]
    async fn review_death_retry_lands_after_the_replacement_reviewer_approves() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow_dies_on_primary_recovers_on_retry(&layout);
        let (repo_dir, head_sha, main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        // The shipped default now paces the replacement (30s + jitter), so
        // this test — which is about the VERDICT, not the schedule — drives
        // the wait through the deterministic seam rather than sitting
        // through it.
        let clock = FakeSchedule::new(0.0);
        let pipeline = Arc::new(
            test_pipeline(home.path(), space.clone()).with_retry_schedule(clock.schedule()),
        );
        let gates = GateConfig {
            review_timeout: Duration::from_secs(120),
            review_max_wait: Duration::from_secs(600),
            ..GateConfig::default()
        };

        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            pipeline.request_review(&entry, &gates),
        )
        .await
        .expect("the dead primary must be detected fast")
        .unwrap();
        let ReviewWaitOutcome::ReviewerDied(context) = outcome else {
            panic!("expected ReviewerDied, got {outcome:?}");
        };

        let retry_instance_id = review_retry_instance_id(&entry, 1);
        let route = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let gates = gates.clone();
            async move {
                pipeline
                    .route_verdict(&entry, ReviewWaitOutcome::ReviewerDied(context), &gates)
                    .await
            }
        });

        // The replacement's own 2s timer gate holds it `Running`; supply its
        // verdict tagged to the RETRY instance id specifically.
        wait_for_spawn_count(&space, 2).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        space
            .out(Tuple::new(
                Category::Artifact,
                &entry.repo_name,
                REVIEW_ARTIFACT_IDENTITY,
                "replacement-reviewer",
                json!({
                    "task": entry.task,
                    "recommendation": "APPROVE",
                    "notes": "clean on re-review",
                    "head_sha": entry.head_sha,
                    "branch": entry.branch,
                    "target": entry.target,
                    "review_attempt": retry_instance_id,
                }),
            ))
            .unwrap();

        let routed = tokio::time::timeout(Duration::from_secs(10), route)
            .await
            .expect("the replacement's verdict must resolve the wait")
            .unwrap()
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
            "a retry that produces a verdict before its own ceiling must not escalate"
        );
    }

    /// Restart/replay safety: a marker already recording the one default
    /// retry attempt as dispatched (as a crash-then-resume, or a redelivered
    /// `ReviewerDied` completion event, would leave behind) must converge
    /// routing on the SAME single human escalation rather than dispatching a
    /// second replacement reviewer.
    #[tokio::test]
    async fn review_death_retry_seeded_marker_prevents_a_duplicate_dispatch_on_replay() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_broken_review_workflow(&layout);
        let (repo_dir, head_sha, main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        let ctx = review_death_context(&head_sha, &entry.task);
        let retry_instance_id = review_retry_instance_id(&entry, 1);
        put_review_death_marker(&space, &ctx, 1, &retry_instance_id, "dispatching");
        put_review_death_marker(&space, &ctx, 1, &retry_instance_id, "dispatched");

        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        let gates = GateConfig::default();

        for _ in 0..2 {
            let routed = pipeline
                .route_verdict(
                    &entry,
                    ReviewWaitOutcome::ReviewerDied("simulated crash".into()),
                    &gates,
                )
                .await
                .unwrap();
            assert!(matches!(routed, LandingOutcome::Escalated(_)));
        }

        assert!(
            tuples(&space, Category::Event, "agent_spawned").is_empty(),
            "the attempt budget was already spent by the seeded marker; replay must not dispatch"
        );
        let needs = scoped_tuples(&space, Category::Need, STEWARD_NEED_IDENTITY);
        assert_eq!(needs.len(), 1, "replay must converge on one visible gate");
        assert!(needs[0].payload["text"]
            .as_str()
            .unwrap()
            .contains("attempts-exhausted"));

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_eq!(main_before, main_after, "branch must not have landed");
    }

    /// The core race the retry ladder must close: once a replacement
    /// reviewer has been dispatched, a verdict that later arrives tagged
    /// with the DEAD (primary) generation's attempt id must never be read as
    /// authoritative again — [`LandingPipeline::cached_verdict`] must miss on
    /// it, scoped as it is to [`LandingPipeline::active_review_attempt`].
    #[test]
    fn late_verdict_from_a_dead_generation_cannot_override_the_active_replacement() {
        let home = tempfile::tempdir().unwrap();
        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let entry = review_candidate_entry(Path::new("."), "abc123");
        let primary_id = review_instance_id(&entry);
        let retry_id = review_retry_instance_id(&entry, 1);

        // A replacement has been dispatched: the retry is now active.
        let ctx = review_death_context("abc123", &entry.task);
        put_review_death_marker(&space, &ctx, 1, &retry_id, "dispatching");
        assert_eq!(pipeline.active_review_attempt(&entry).unwrap(), retry_id);

        // The dead primary's verdict arrives late, tagged with its OWN attempt id.
        space
            .out(Tuple::new(
                Category::Artifact,
                "code-repo",
                REVIEW_ARTIFACT_IDENTITY,
                "zombie-primary-reviewer",
                json!({
                    "task": entry.task,
                    "recommendation": "APPROVE",
                    "notes": "late",
                    "head_sha": entry.head_sha,
                    "branch": entry.branch,
                    "target": entry.target,
                    "review_attempt": primary_id,
                }),
            ))
            .unwrap();
        assert_eq!(
            pipeline.cached_verdict(&entry).unwrap(),
            None,
            "a verdict from a superseded attempt must never be read as current"
        );

        // The active replacement's own verdict, once it lands, IS read.
        space
            .out(Tuple::new(
                Category::Artifact,
                "code-repo",
                REVIEW_ARTIFACT_IDENTITY,
                "replacement-reviewer",
                json!({
                    "task": entry.task,
                    "recommendation": "REWORK",
                    "notes": "real",
                    "head_sha": entry.head_sha,
                    "branch": entry.branch,
                    "target": entry.target,
                    "review_attempt": retry_id,
                }),
            ))
            .unwrap();
        assert_eq!(
            pipeline.cached_verdict(&entry).unwrap(),
            Some("REWORK".to_string())
        );
    }

    /// Acceptance gap (independent audit, TKT-01M0J7J90HK8YS24RX51HR9ZQJ):
    /// once a review-death chain is SETTLED (withheld/escalated to a human
    /// gate — [`REVIEW_DEATH_DISPATCH_IDENTITY`]'s marker state outside
    /// `{"dispatching", "dispatched"}`), no attempt is active any more.
    /// [`LandingPipeline::active_review_attempt`] must not keep naming the
    /// dead retry as authoritative, and
    /// [`LandingPipeline::cached_verdict`] must miss on a verdict tagged to
    /// it — even one that arrives strictly AFTER settlement.
    #[test]
    fn a_settled_review_death_chain_makes_no_attempt_active_and_no_verdict_cached() {
        let home = tempfile::tempdir().unwrap();
        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let entry = review_candidate_entry(Path::new("."), "abc123");
        let retry_id = review_retry_instance_id(&entry, 1);

        // The one default retry was dispatched, died too, and the chain was
        // escalated — the exact durable state `route_review_death` leaves
        // behind once `max_review_death_attempts` (default 1) is spent.
        let ctx = review_death_context("abc123", &entry.task);
        put_review_death_marker(&space, &ctx, 1, &retry_id, "dispatching");
        put_review_death_marker(&space, &ctx, 1, &retry_id, "dispatched");
        put_review_death_marker(&space, &ctx, 1, "", "attempts-exhausted");

        assert_ne!(
            pipeline.active_review_attempt(&entry).unwrap(),
            retry_id,
            "a settled chain must not keep naming the dead retry as active"
        );

        // A late verdict from the dead retry arrives strictly AFTER settlement.
        space
            .out(Tuple::new(
                Category::Artifact,
                "code-repo",
                REVIEW_ARTIFACT_IDENTITY,
                "zombie-retry-reviewer",
                json!({
                    "task": entry.task,
                    "recommendation": "APPROVE",
                    "notes": "late, after escalation",
                    "head_sha": entry.head_sha,
                    "branch": entry.branch,
                    "target": entry.target,
                    "review_attempt": retry_id,
                }),
            ))
            .unwrap();
        assert_eq!(
            pipeline.cached_verdict(&entry).unwrap(),
            None,
            "a settled chain must never read a late verdict as current"
        );
    }

    /// The full acceptance gap, end to end: a daemon restart (or any
    /// redelivered completion) re-enters through
    /// [`LandingPipeline::review_verdict`] — production's real entry point,
    /// unlike the `route_verdict` test helper other tests here start from
    /// an already-resolved [`ReviewWaitOutcome`]. Before this fix,
    /// `review_verdict` read `cached_verdict` BEFORE ever checking
    /// settlement, so a late verdict tagged to the dead retry would be
    /// returned as current and acted on — landing the branch over an
    /// already-established human hold. Proves: settlement is checked first,
    /// the late verdict is never read even after being seeded, replay
    /// converges on the SAME single escalation with no second dispatch, and
    /// the branch never lands.
    #[tokio::test]
    async fn restart_replay_after_settlement_never_reads_or_lands_a_late_verdict() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        let ctx = review_death_context(&head_sha, &entry.task);
        let retry_id = review_retry_instance_id(&entry, 1);
        put_review_death_marker(&space, &ctx, 1, &retry_id, "dispatching");
        put_review_death_marker(&space, &ctx, 1, &retry_id, "dispatched");
        put_review_death_marker(&space, &ctx, 1, "", "attempts-exhausted");

        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        let gates = GateConfig::default();

        let resumed = pipeline.review_verdict(&entry, &gates).await.unwrap();
        assert!(
            matches!(resumed, ReviewWaitOutcome::ReviewerDied(_)),
            "a settled chain must route back to escalation, not a cached verdict: {resumed:?}"
        );

        // A late verdict artifact tagged to the dead retry, arriving after
        // settlement, must still not be read on a second replay.
        space
            .out(Tuple::new(
                Category::Artifact,
                &entry.repo_name,
                REVIEW_ARTIFACT_IDENTITY,
                "zombie-retry-reviewer",
                json!({
                    "task": entry.task,
                    "recommendation": "APPROVE",
                    "notes": "late, after escalation",
                    "head_sha": entry.head_sha,
                    "branch": entry.branch,
                    "target": entry.target,
                    "review_attempt": retry_id,
                }),
            ))
            .unwrap();
        let resumed_again = pipeline.review_verdict(&entry, &gates).await.unwrap();
        assert!(
            matches!(resumed_again, ReviewWaitOutcome::ReviewerDied(_)),
            "a late verdict after settlement must never be read as current: {resumed_again:?}"
        );

        // Driving the (correctly re-derived) ReviewerDied outcome through
        // the full routing path must converge on the SAME single
        // escalation — never a second dispatch, never a land.
        let routed = pipeline
            .route_verdict(&entry, resumed_again, &gates)
            .await
            .unwrap();
        assert!(matches!(routed, LandingOutcome::Escalated(_)));
        assert!(
            tuples(&space, Category::Event, "agent_spawned").is_empty(),
            "a settled chain must not dispatch a second replacement reviewer"
        );
        assert_eq!(
            main_before,
            rev_parse(repo_dir.path(), "main"),
            "a late verdict after settlement must never land the candidate"
        );
    }

    /// A replacement must never silently review a newer branch tip under the
    /// old review identity. A moved head is a new candidate and needs a fresh
    /// review rather than an automatic retry of the dead generation.
    #[tokio::test]
    async fn review_death_retry_holds_when_reviewed_head_moves() {
        let home = tempfile::tempdir().unwrap();
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);
        git(repo_dir.path(), &["checkout", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() { 1 + 1 }\n").unwrap();
        git(repo_dir.path(), &["add", "src.rs"]);
        git(
            repo_dir.path(),
            &["commit", "-m", "feat: move reviewed head"],
        );
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let routed = pipeline
            .route_verdict(
                &entry,
                ReviewWaitOutcome::ReviewerDied("primary stopped".into()),
                &GateConfig::default(),
            )
            .await
            .unwrap();
        let LandingOutcome::Escalated(need) = routed else {
            panic!("expected Escalated, got {routed:?}");
        };
        assert!(need.payload["text"]
            .as_str()
            .unwrap()
            .contains("reviewed-head-moved"));
        assert!(tuples(&space, Category::Event, "agent_spawned").is_empty());
        assert_eq!(
            scoped_tuples(&space, Category::Event, REVIEW_DEATH_DISPATCH_IDENTITY)
                .iter()
                .filter(|marker| marker.payload["state"] == "reviewed-head-moved")
                .count(),
            1
        );
    }

    /// Durable bounded backoff (TKT-01M0J7J90HK8YS24RX51HR9ZQJ): the
    /// schedule chosen for a review-death retry ([`landing_review_retry::retry_delay`])
    /// is persisted as `not_before` on the SAME "dispatching" marker the
    /// pre-backoff state machine already wrote, and every reader — a fresh
    /// dispatch, a restart's resume through [`LandingPipeline::review_verdict`],
    /// or a duplicate/redelivered routing of the same dead review — reads
    /// that ONE persisted value back through
    /// [`LandingPipeline::review_death_not_before`] rather than drawing its
    /// own jitter. This is what "duplicates share one schedule" means: there
    /// is exactly one writer (the fresh Dispatch decision) and every other
    /// caller is a read.
    #[test]
    fn review_death_not_before_reads_the_persisted_schedule_verbatim_for_every_reader() {
        let home = tempfile::tempdir().unwrap();
        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let entry = review_candidate_entry(Path::new("."), "abc123");
        let retry_id = review_retry_instance_id(&entry, 1);
        let ctx = review_death_context("abc123", &entry.task);

        // No marker yet (or a marker seeded before this policy existed):
        // "no wait", not an error.
        assert_eq!(
            pipeline.review_death_not_before(&entry, &retry_id).unwrap(),
            None
        );
        put_review_death_marker(&space, &ctx, 1, &retry_id, "dispatching");
        assert_eq!(
            pipeline.review_death_not_before(&entry, &retry_id).unwrap(),
            None
        );

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let scheduled = Utc::now() + chrono::Duration::seconds(42);
        put_review_death_marker_with_schedule(
            &space,
            &ctx,
            1,
            &retry_id,
            "dispatching",
            Some(scheduled),
        );
        for _ in 0..3 {
            assert_eq!(
                pipeline.review_death_not_before(&entry, &retry_id).unwrap(),
                Some(scheduled),
                "every reader must see the SAME persisted schedule, never a fresh draw"
            );
        }
        // The later "dispatched" marker (written once the wait/spawn
        // completes) never carries its own `not_before` — the schedule
        // stays readable off the earlier "dispatching" marker regardless.
        put_review_death_marker(&space, &ctx, 1, &retry_id, "dispatched");
        assert_eq!(
            pipeline.review_death_not_before(&entry, &retry_id).unwrap(),
            Some(scheduled)
        );
    }

    /// The exact elapsed/not-yet-elapsed split
    /// [`LandingPipeline::await_review_retry_after_backoff`] relies on,
    /// covered as a synchronous, deterministic unit test: no task spawn, no
    /// workflow dispatch, no real clock in the loop, so it cannot flake
    /// under scheduling contention the way an end-to-end timing assertion
    /// can (see the `restart_resume_dispatches_immediately_once_not_before_has_elapsed`
    /// doc comment below for why that mattered in practice).
    #[test]
    fn remaining_backoff_is_none_once_not_before_has_elapsed() {
        let now = Utc::now();
        assert_eq!(LandingPipeline::remaining_backoff(None, now), None);
        assert_eq!(
            LandingPipeline::remaining_backoff(Some(now), now),
            None,
            "a schedule exactly AT now has already elapsed, not still pending"
        );
        assert_eq!(
            LandingPipeline::remaining_backoff(Some(now - chrono::Duration::seconds(5)), now),
            None
        );
    }

    #[test]
    fn remaining_backoff_returns_the_exact_remainder_when_not_yet_elapsed() {
        let now = Utc::now();
        let not_before = now + chrono::Duration::milliseconds(700);
        assert_eq!(
            LandingPipeline::remaining_backoff(Some(not_before), now),
            Some(Duration::from_millis(700))
        );
    }

    /// Restart before `not_before`: resuming a review-death retry whose
    /// durable schedule has NOT yet elapsed must wait out the remainder
    /// before dispatching — a restart can never reset the wait to zero.
    #[tokio::test]
    async fn restart_resume_waits_out_the_remaining_backoff_before_not_before() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow_dies_on_primary_recovers_on_retry(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        let ctx = review_death_context(&head_sha, &entry.task);
        let retry_id = review_retry_instance_id(&entry, 1);
        let not_before = Utc::now() + chrono::Duration::milliseconds(700);
        put_review_death_marker_with_schedule(
            &space,
            &ctx,
            1,
            &retry_id,
            "dispatching",
            Some(not_before),
        );

        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        let gates = GateConfig {
            review_timeout: Duration::from_secs(120),
            review_max_wait: Duration::from_secs(600),
            ..GateConfig::default()
        };

        let started = tokio::time::Instant::now();
        let resume = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let gates = gates.clone();
            async move { pipeline.review_verdict(&entry, &gates).await }
        });
        wait_for_spawn_count(&space, 1).await;
        assert!(
            started.elapsed() >= Duration::from_millis(600),
            "a restart must wait out the remaining backoff, not dispatch immediately: {:?}",
            started.elapsed()
        );
        resume.abort();
        let _ = resume.await;
    }

    /// Restart after `not_before`: resuming a review-death retry whose
    /// durable schedule already elapsed (e.g. the daemon was down past the
    /// backoff window) dispatches without any further added wait — the
    /// backoff is a floor on when the retry may start, not a period the
    /// candidate must additionally sit through past that point.
    ///
    /// The EXACT elapsed-vs-not-yet-elapsed decision this guards is covered
    /// deterministically by `remaining_backoff_is_none_once_not_before_has_elapsed`
    /// above, so this end-to-end test only needs a wall-clock ceiling loose
    /// enough to absorb real scheduling contention (dispatch here runs the
    /// genuine spawn path — CUE load, instance persistence, a real `fake`-harness
    /// process — not a mock): under `mise run verify`'s full-workspace
    /// parallel run this was observed at ~550-570ms against a 500ms ceiling
    /// with zero added wait (TKT-01M0JBJGRZZNN0GK3K8RRJ4Y67), which is what
    /// made the tight ceiling flake. 5s stays two orders of magnitude below
    /// what a reintroduced backoff wait would look like while comfortably
    /// clearing that contention.
    #[tokio::test]
    async fn restart_resume_dispatches_immediately_once_not_before_has_elapsed() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow_dies_on_primary_recovers_on_retry(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        let ctx = review_death_context(&head_sha, &entry.task);
        let retry_id = review_retry_instance_id(&entry, 1);
        let not_before = Utc::now() - chrono::Duration::seconds(5);
        put_review_death_marker_with_schedule(
            &space,
            &ctx,
            1,
            &retry_id,
            "dispatching",
            Some(not_before),
        );

        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        let gates = GateConfig {
            review_timeout: Duration::from_secs(120),
            review_max_wait: Duration::from_secs(600),
            ..GateConfig::default()
        };

        let started = tokio::time::Instant::now();
        let resume = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let gates = gates.clone();
            async move { pipeline.review_verdict(&entry, &gates).await }
        });
        wait_for_spawn_count(&space, 1).await;
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "an already-elapsed schedule must not add any further wait: {:?}",
            started.elapsed()
        );
        resume.abort();
        let _ = resume.await;
    }

    /// An EXPLICITLY configured `reviewDeathRetryDelay: "0s"` — the opt-out
    /// from the shipped nonzero default — must preserve the pre-backoff
    /// immediate-dispatch behavior exactly: the fresh Dispatch decision
    /// records a `not_before` that is already due, and the replacement is
    /// spawned with no wait AT ALL (asserted through the seam's recorded
    /// waits, not a wall-clock threshold, so it cannot flake under load).
    #[tokio::test]
    async fn explicit_zero_delay_policy_dispatches_the_replacement_without_added_wait() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_broken_review_workflow(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        activate_landing_policy(
            home.path(),
            repo_dir.path(),
            backoff_landing_policy("0s", 200, "10m", 50),
        );
        // Jitter at its ceiling: an explicit zero must stay zero even when
        // every other knob is at its most inflating setting.
        let clock = FakeSchedule::new(1.0);
        let pipeline = Arc::new(
            test_pipeline(home.path(), space.clone()).with_retry_schedule(clock.schedule()),
        );
        let gates = GateConfig::default();

        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            pipeline.request_review(&entry, &gates),
        )
        .await
        .expect("the dead primary must be detected fast")
        .unwrap();
        let ReviewWaitOutcome::ReviewerDied(context) = outcome else {
            panic!("expected the primary reviewer to die");
        };

        let started = tokio::time::Instant::now();
        let route = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let gates = gates.clone();
            async move {
                pipeline
                    .route_verdict(&entry, ReviewWaitOutcome::ReviewerDied(context), &gates)
                    .await
            }
        });
        wait_for_spawn_count(&space, 2).await;
        // `route_verdict` (unlike `review_verdict`) also runs a real
        // `git prepare_merge` before it ever reaches the Dispatch arm, so
        // this threshold is generous rather than tight to the backoff
        // itself — the EXACT "no added wait" property is the seam assertion
        // below; this only keeps the end-to-end path honest.
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "an explicit zero-delay policy must dispatch without any added wait: {:?}",
            started.elapsed()
        );
        assert!(
            clock.waits().is_empty(),
            "an explicit zero-delay policy must not wait at all, waited {:?}",
            clock.waits()
        );

        let not_before = dispatching_not_before(&space)
            .expect("a zero-delay dispatch must still record its (already-due) schedule");
        assert_eq!(
            not_before,
            clock.now(),
            "a zero-delay schedule must be due at exactly the decision instant"
        );

        route.abort();
        let _ = route.await;
    }

    /// The SHIPPED default (no activated repository policy at all) must
    /// actually back off: `default_review_death_retry_delay`'s 30s, scaled
    /// by nothing at attempt 1, plus at most
    /// `default_review_death_retry_jitter_pct`'s 20%. This is the acceptance
    /// property the ticket asks for — bounded backoff is the default
    /// behavior, with immediate dispatch reserved for an explicitly
    /// configured zero — and it is asserted on the REAL dispatch path, off
    /// the durable marker, not on the pure helper's arithmetic.
    #[tokio::test]
    async fn shipped_default_policy_paces_the_replacement_within_its_jitter_band() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_broken_review_workflow(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        // Jitter at both ends of the band, on two independent runs of the
        // real dispatch path: the floor is the un-jittered backoff and the
        // ceiling is exactly `jitter_pct` above it. Nothing outside that
        // band is reachable, which is what "jitter within configured bounds"
        // means for an operator reading the policy.
        for (jitter_unit, expected) in [
            (0.0, Duration::from_secs(30)),
            (1.0, Duration::from_secs(36)),
        ] {
            let space = Space::open_in_memory().unwrap();
            let clock = FakeSchedule::new(jitter_unit);
            let pipeline = Arc::new(
                test_pipeline(home.path(), space.clone()).with_retry_schedule(clock.schedule()),
            );
            let gates = GateConfig::default();
            let decided_at = clock.now();

            let outcome = tokio::time::timeout(
                Duration::from_secs(5),
                pipeline.request_review(&entry, &gates),
            )
            .await
            .expect("the dead primary must be detected fast")
            .unwrap();
            let ReviewWaitOutcome::ReviewerDied(context) = outcome else {
                panic!("expected the primary reviewer to die");
            };
            let route = tokio::spawn({
                let pipeline = Arc::clone(&pipeline);
                let entry = entry.clone();
                let gates = gates.clone();
                async move {
                    pipeline
                        .route_verdict(&entry, ReviewWaitOutcome::ReviewerDied(context), &gates)
                        .await
                }
            });
            wait_for_spawn_count(&space, 2).await;

            let not_before = dispatching_not_before(&space)
                .expect("the shipped default must persist a nonzero schedule");
            assert_eq!(
                not_before - decided_at,
                chrono::Duration::from_std(expected).unwrap(),
                "jitter unit {jitter_unit} must persist exactly {expected:?} of backoff"
            );
            assert_eq!(
                clock.waits(),
                vec![expected],
                "the dispatch must wait out exactly the schedule it persisted"
            );

            route.abort();
            let _ = route.await;
        }
    }

    /// A repository that configures its own bounds gets exactly those
    /// bounds on the real dispatch path — including the `maxDelay` clamp,
    /// which jitter may not push past. Covered here against the durable
    /// marker and the seam's recorded wait, so the assertion is about what
    /// the pipeline scheduled, not about `retry_delay`'s arithmetic.
    #[tokio::test]
    async fn configured_policy_schedule_is_clamped_to_its_max_delay() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_broken_review_workflow(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);
        // 60s base with 50% jitter would be 90s, but the repo caps at 45s.
        activate_landing_policy(
            home.path(),
            repo_dir.path(),
            backoff_landing_policy("60s", 200, "45s", 50),
        );

        let space = Space::open_in_memory().unwrap();
        let clock = FakeSchedule::new(1.0);
        let pipeline = Arc::new(
            test_pipeline(home.path(), space.clone()).with_retry_schedule(clock.schedule()),
        );
        let gates = GateConfig::default();
        let decided_at = clock.now();

        let outcome = tokio::time::timeout(
            Duration::from_secs(5),
            pipeline.request_review(&entry, &gates),
        )
        .await
        .expect("the dead primary must be detected fast")
        .unwrap();
        let ReviewWaitOutcome::ReviewerDied(context) = outcome else {
            panic!("expected the primary reviewer to die");
        };
        let route = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let gates = gates.clone();
            async move {
                pipeline
                    .route_verdict(&entry, ReviewWaitOutcome::ReviewerDied(context), &gates)
                    .await
            }
        });
        wait_for_spawn_count(&space, 2).await;

        let not_before =
            dispatching_not_before(&space).expect("a configured delay must persist a schedule");
        assert_eq!(
            not_before - decided_at,
            chrono::Duration::seconds(45),
            "the repository's maxDelay is a hard ceiling jitter cannot exceed"
        );
        assert_eq!(clock.waits(), vec![Duration::from_secs(45)]);

        route.abort();
        let _ = route.await;
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
        let ReviewWaitOutcome::CeilingReached { instance_id } = &outcome else {
            panic!("expected CeilingReached, got {outcome:?}");
        };
        let instance_id = instance_id.clone();
        assert_eq!(instance_id, review_instance_id(&entry));

        // This fixture's `fake` harness finishes the reviewer's own turn in
        // under a second regardless — only the workflow's 2s timer gate (not
        // the agent) is what is still `Running` at the 800ms ceiling here.
        // Proving an actually-still-`Running` reviewer gets dismissed (not
        // just a completed one) is
        // `workflow_exec::tests::stale_instance_timeout_releases_a_still_live_owned_agent`,
        // which controls agent liveness directly; this test instead proves
        // the settlement-marker mechanics `settle_review_ceiling` owns:
        // settle exactly once, and a restart-replay of the same routing
        // pass never re-settles or re-escalates a duplicate.
        let routed = pipeline
            .route_verdict(&entry, outcome, &gates)
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

        let settlements = tuples(&space, Category::Event, REVIEW_CEILING_SETTLED_IDENTITY);
        assert_eq!(settlements.len(), 1, "the ceiling must settle exactly once");
        assert_eq!(settlements[0].payload["attempt"], instance_id);
        assert_eq!(settlements[0].payload["reason"], "review-wait-exhausted");

        // Daemon-restart-before-cleanup analogue: replaying the exact same
        // routing pass (a restart re-entering `route_verdict_prepared` for
        // an instance already settled) must not dismiss again or duplicate
        // the marker.
        let routed_again = pipeline
            .route_verdict(
                &entry,
                ReviewWaitOutcome::CeilingReached {
                    instance_id: instance_id.clone(),
                },
                &gates,
            )
            .await
            .unwrap();
        assert!(matches!(routed_again, LandingOutcome::Escalated(_)));
        assert_eq!(
            tuples(&space, Category::Event, REVIEW_CEILING_SETTLED_IDENTITY).len(),
            1,
            "settling twice for the same attempt must not duplicate the marker"
        );

        let main_after = rev_parse(repo_dir.path(), "main");
        assert_eq!(main_before, main_after, "branch must not have landed");
    }

    /// A verdict that arrives for a ceiling-settled attempt AFTER the
    /// ceiling has already fenced it — an APPROVE or a REWORK, it makes no
    /// difference to this path — must be retained as durable evidence
    /// (branch/head/attempt/generation) and never treated as the landing
    /// decision: `cached_verdict`/`active_review_attempt` must still refuse
    /// to read it as current, exactly like a review-death-settled chain
    /// already refuses a late verdict from its dead generation.
    #[tokio::test]
    async fn late_approve_and_rework_are_retained_as_evidence_without_mutating_the_decision() {
        let home = tempfile::tempdir().unwrap();
        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let entry = review_candidate_entry(Path::new("."), "abc123");
        let attempt = review_instance_id(&entry);

        pipeline
            .settle_review_ceiling(&entry, &attempt, "review-wait-exhausted")
            .await
            .unwrap();
        assert_ne!(
            pipeline.active_review_attempt(&entry).unwrap(),
            attempt,
            "a ceiling-settled attempt must not stay the active one"
        );

        // Nothing to retain yet.
        assert!(pipeline
            .retain_late_review_evidence(&entry, &attempt)
            .unwrap()
            .is_none());
        assert!(pipeline.cached_verdict(&entry).unwrap().is_none());

        // The fenced reviewer's late APPROVE arrives.
        space
            .out(Tuple::new(
                Category::Artifact,
                "code-repo",
                REVIEW_ARTIFACT_IDENTITY,
                "zombie-reviewer",
                json!({
                    "task": entry.task,
                    "recommendation": "APPROVE",
                    "notes": "late",
                    "head_sha": entry.head_sha,
                    "branch": entry.branch,
                    "target": entry.target,
                    "review_attempt": attempt,
                }),
            ))
            .unwrap();

        let evidence = pipeline
            .retain_late_review_evidence(&entry, &attempt)
            .unwrap()
            .expect("a late verdict for a settled attempt must be retained as evidence");
        assert_eq!(evidence.payload["attempt"], attempt);
        assert_eq!(evidence.payload["branch"], entry.branch);
        assert_eq!(evidence.payload["head_sha"], entry.head_sha);
        assert_eq!(evidence.payload["recommendation"], "APPROVE");
        assert!(evidence.payload["generation"].is_array());

        // The landing decision is untouched: the branch never landed, and
        // a fresh read still refuses to treat the late verdict as current.
        assert!(pipeline.cached_verdict(&entry).unwrap().is_none());

        // Idempotent: a redelivered completion (or a restart) re-driving
        // the same reconciliation must not duplicate the evidence record.
        assert!(pipeline
            .retain_late_review_evidence(&entry, &attempt)
            .unwrap()
            .is_none());
        assert_eq!(
            tuples(&space, Category::Artifact, LATE_REVIEW_EVIDENCE_IDENTITY).len(),
            1,
            "a late verdict arriving twice must be retained exactly once"
        );

        // A REWORK from a DIFFERENT (also dead) attempt must never be
        // conflated with this one's evidence — stale-attempt rejection: a
        // verdict tagged to an attempt nobody ever settled or asked about
        // is simply invisible to this reconciliation, not retained under
        // the wrong attempt.
        let other_attempt = format!("{attempt}-other");
        space
            .out(Tuple::new(
                Category::Artifact,
                "code-repo",
                REVIEW_ARTIFACT_IDENTITY,
                "another-zombie",
                json!({
                    "task": entry.task,
                    "recommendation": "REWORK",
                    "notes": "late, wrong attempt",
                    "head_sha": entry.head_sha,
                    "branch": entry.branch,
                    "target": entry.target,
                    "review_attempt": other_attempt,
                }),
            ))
            .unwrap();
        assert!(
            pipeline
                .retain_late_review_evidence(&entry, &other_attempt)
                .unwrap()
                .is_none(),
            "an attempt that was never ceiling-settled has nothing to retain evidence against"
        );
        assert_eq!(
            tuples(&space, Category::Artifact, LATE_REVIEW_EVIDENCE_IDENTITY).len(),
            1,
            "the unsettled attempt's verdict must not be retained as evidence for the settled one"
        );
    }

    /// Explicit operator cancellation of a review that is genuinely still
    /// in flight (mid-wait, well before the ceiling): `cancel_active_review`
    /// must settle through `settle_review_ceiling` (releasing the still-live
    /// reviewer's capacity), the concurrently-running `await_primary_verdict`
    /// poll loop must notice and resolve as `Cancelled` — not stall out to
    /// `CeilingReached` — and the router's own idempotent re-settle must not
    /// duplicate the marker. A late verdict that still arrives afterward is
    /// retained as evidence, never the landing decision, and a second
    /// cancel call is refused once the attempt is already settled.
    #[tokio::test]
    async fn cancel_active_review_settles_mid_wait_releases_capacity_and_fences_late_verdict() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow(&layout); // 2s timer gate keeps the reviewer Running
        let (repo_dir, head_sha, main_before) = review_candidate_repo();
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let entry = LandingQueueEntry {
            repo_name: git_repo.name(),
            ..review_candidate_entry(repo_dir.path(), &head_sha)
        };

        let space = Space::open_in_memory().unwrap();
        let pipeline = Arc::new(test_pipeline(home.path(), space.clone()));
        // `cancel_active_review` recovers `head_sha` from the durable queue
        // entry, exactly like a real candidate awaiting review would leave
        // behind (`dispatch_review`'s `set_status(AwaitingReview)`).
        pipeline.queue.enqueue(entry.clone()).unwrap();

        // A ceiling long enough that only an explicit cancel — not the
        // deadline — could plausibly resolve the wait within this test.
        let gates = GateConfig {
            review_timeout: Duration::from_millis(100),
            review_max_wait: Duration::from_secs(5),
            ..GateConfig::default()
        };

        let started = tokio::time::Instant::now();
        let request = tokio::spawn({
            let pipeline = Arc::clone(&pipeline);
            let entry = entry.clone();
            let gates = gates.clone();
            async move { pipeline.request_review(&entry, &gates).await }
        });

        // Let the reviewer actually launch and be observably `Running`
        // (inside the workflow's 2s timer gate) before cancelling it.
        wait_for_spawn_count(&space, 1).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let settlement = pipeline
            .cancel_active_review(repo_dir.path(), &entry.branch, &entry.target, &entry.task)
            .await
            .unwrap();
        assert_eq!(settlement.payload["reason"], "operator-cancelled");
        // `released_agents` reports whatever `dismiss_live_instance_agents`
        // found still live at the moment of settlement — proving it is
        // actually invoked (not just that a marker is written) is this
        // test's job; proving a genuinely still-running agent is torn down
        // is `workflow_exec::tests::stale_instance_timeout_releases_a_still_
        // live_owned_agent`'s (this fixture's own reviewer harness finishes
        // its turn in under a second regardless, per `write_review_workflow`'s
        // doc, so by 200ms there may be nothing live left to release).
        assert!(settlement.payload["released_agents"].is_array());

        let outcome = tokio::time::timeout(Duration::from_secs(3), request)
            .await
            .expect("cancel must interrupt the wait long before the 5s ceiling")
            .unwrap()
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "cancellation must be observed well inside the 5s ceiling"
        );
        let ReviewWaitOutcome::Cancelled { instance_id } = &outcome else {
            panic!("expected Cancelled, got {outcome:?}");
        };
        let instance_id = instance_id.clone();
        assert_eq!(instance_id, review_instance_id(&entry));

        let settlements = tuples(&space, Category::Event, REVIEW_CEILING_SETTLED_IDENTITY);
        assert_eq!(settlements.len(), 1, "cancel must settle exactly once");
        assert_eq!(settlements[0].payload["attempt"], instance_id);
        assert_eq!(settlements[0].payload["reason"], "operator-cancelled");

        // Route the discovered outcome the way `route_verdict_prepared`
        // would in production: its own call back into
        // `settle_review_ceiling` must be a no-op, never a second dismissal
        // or a duplicate marker.
        let routed = pipeline
            .route_verdict(&entry, outcome, &gates)
            .await
            .unwrap();
        let LandingOutcome::Escalated(need) = &routed else {
            panic!("expected Escalated, got {routed:?}");
        };
        let text = need.payload["text"].as_str().unwrap();
        assert!(text.contains("cancelled"), "text: {text}");
        assert_eq!(
            tuples(&space, Category::Event, REVIEW_CEILING_SETTLED_IDENTITY).len(),
            1,
            "the router's own settle call must not duplicate the marker"
        );

        // A late verdict from the cancelled generation is retained as
        // evidence, never treated as the landing decision.
        space
            .out(Tuple::new(
                Category::Artifact,
                entry.repo_name.clone(),
                REVIEW_ARTIFACT_IDENTITY,
                "cancelled-reviewer",
                json!({
                    "task": entry.task,
                    "recommendation": "APPROVE",
                    "notes": "arrived after cancellation",
                    "head_sha": entry.head_sha,
                    "branch": entry.branch,
                    "target": entry.target,
                    "review_attempt": instance_id,
                }),
            ))
            .unwrap();
        let evidence = pipeline
            .retain_late_review_evidence(&entry, &instance_id)
            .unwrap()
            .expect("a late verdict for a cancelled attempt must be retained as evidence");
        assert_eq!(evidence.payload["recommendation"], "APPROVE");
        assert!(
            pipeline.cached_verdict(&entry).unwrap().is_none(),
            "the late APPROVE must never be read back as the landing decision"
        );
        let main_after = rev_parse(repo_dir.path(), "main");
        assert_eq!(main_before, main_after, "branch must not have landed");

        // Stale-attempt rejection: a second cancel call for the same
        // candidate finds the attempt already settled and refuses, rather
        // than re-dismissing or re-settling.
        let repeat = pipeline
            .cancel_active_review(repo_dir.path(), &entry.branch, &entry.target, &entry.task)
            .await;
        assert!(
            repeat.is_err(),
            "cancelling an already-settled attempt must be refused"
        );
        assert_eq!(
            tuples(&space, Category::Event, REVIEW_CEILING_SETTLED_IDENTITY).len(),
            1,
            "a refused repeat cancel must not touch the settlement marker"
        );
    }

    /// `cancel_active_review` must refuse rather than guess when no
    /// candidate for `(branch, target, task)` is currently in the durable
    /// queue: without the real `head_sha` a fabricated entry would compute
    /// the wrong attempt id and silently settle a phantom that matches no
    /// live reviewer, returning success while cancelling nothing.
    #[tokio::test]
    async fn cancel_active_review_refuses_when_nothing_is_queued() {
        let home = tempfile::tempdir().unwrap();
        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let error = pipeline
            .cancel_active_review(repo_dir.path(), &entry.branch, &entry.target, &entry.task)
            .await
            .expect_err("nothing queued means nothing to cancel");
        assert!(
            error.to_string().contains("no candidate currently"),
            "error: {error}"
        );
        assert!(tuples(&space, Category::Event, REVIEW_CEILING_SETTLED_IDENTITY).is_empty());
    }

    /// The explicit, bounded re-enqueue action: requires a prior ceiling
    /// settlement, dispatches exactly one fresh review attempt, is
    /// idempotent on a second call (no duplicate reviewer), and makes the
    /// NEW attempt — not the dead one — the one `active_review_attempt`
    /// treats as current, so the replacement's own verdict is actually
    /// reachable.
    #[tokio::test]
    async fn reenqueue_after_ceiling_dispatches_exactly_one_fresh_attempt() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        let entry = review_candidate_entry(repo_dir.path(), &head_sha);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let gates = GateConfig::default();
        let attempt = review_instance_id(&entry);

        // Re-enqueueing before any settlement exists is refused: there is
        // nothing to re-enqueue while the original wait was never fenced.
        assert!(pipeline
            .reenqueue_after_ceiling(&entry, &gates, &attempt)
            .await
            .is_err());

        pipeline
            .settle_review_ceiling(&entry, &attempt, "review-wait-exhausted")
            .await
            .unwrap();

        let new_attempt = pipeline
            .reenqueue_after_ceiling(&entry, &gates, &attempt)
            .await
            .unwrap();
        assert_ne!(new_attempt, attempt);
        assert_eq!(
            pipeline.active_review_attempt(&entry).unwrap(),
            new_attempt,
            "the fresh attempt must become the one authoritative reads resolve to"
        );
        assert_eq!(
            wait_for_spawn_count(&space, 1).await,
            1,
            "exactly one fresh reviewer must be dispatched"
        );

        // Bounded to exactly once: a second call (a retried RPC, a replay)
        // returns the SAME new attempt id rather than dispatching another.
        let repeat = pipeline
            .reenqueue_after_ceiling(&entry, &gates, &attempt)
            .await
            .unwrap();
        assert_eq!(repeat, new_attempt);
        assert_eq!(
            tuples(&space, Category::Event, REVIEW_CEILING_REENQUEUE_IDENTITY).len(),
            1,
            "re-enqueueing twice for the same settled attempt must not duplicate the marker"
        );
        assert_eq!(
            tuples(&space, Category::Event, "agent_spawned").len(),
            1,
            "a repeat re-enqueue call must never spawn a second reviewer"
        );
    }

    /// The `repo.land.reenqueue` RPC's actual entry point:
    /// `reenqueue_ceiling_settled_review` must resolve a `LandingQueueEntry`
    /// and `GateConfig` from just a repo path plus branch/target/task/attempt
    /// — the caller-facing identifiers an escalation's `RESOLVE WITH:` text
    /// hands an operator — refuse before any settlement exists, and be
    /// idempotent per settled attempt exactly like the underlying
    /// `reenqueue_after_ceiling` it wraps.
    #[tokio::test]
    async fn reenqueue_ceiling_settled_review_resolves_entry_and_is_idempotent() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        write_review_workflow(&layout);
        let (repo_dir, head_sha, _main_before) = review_candidate_repo();
        // `reenqueue_ceiling_settled_review` derives `repo_name` from the
        // real repo path the same way `submit_manual` does (`rk_git::Repo::
        // name`, the tempdir's own leaf name) — override the fixture's
        // hardcoded "code-repo" so the lookup it performs actually matches.
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let entry = LandingQueueEntry {
            repo_name: git_repo.name(),
            ..review_candidate_entry(repo_dir.path(), &head_sha)
        };

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let attempt = review_instance_id(&entry);

        // No settlement yet: nothing to re-enqueue.
        assert!(pipeline
            .reenqueue_ceiling_settled_review(
                repo_dir.path(),
                &entry.branch,
                &entry.target,
                &entry.task,
                &attempt,
            )
            .await
            .is_err());

        pipeline
            .settle_review_ceiling(&entry, &attempt, "review-wait-exhausted")
            .await
            .unwrap();

        let new_attempt = pipeline
            .reenqueue_ceiling_settled_review(
                repo_dir.path(),
                &entry.branch,
                &entry.target,
                &entry.task,
                &attempt,
            )
            .await
            .unwrap();
        assert_ne!(new_attempt, attempt);
        assert_eq!(
            pipeline.active_review_attempt(&entry).unwrap(),
            new_attempt,
            "the fresh attempt dispatched via the RPC entry point must become authoritative"
        );
        assert_eq!(wait_for_spawn_count(&space, 1).await, 1);

        // Idempotent: a retried RPC call (or a duplicate CLI invocation)
        // returns the same fresh attempt id and never dispatches twice.
        let repeat = pipeline
            .reenqueue_ceiling_settled_review(
                repo_dir.path(),
                &entry.branch,
                &entry.target,
                &entry.task,
                &attempt,
            )
            .await
            .unwrap();
        assert_eq!(repeat, new_attempt);
        assert_eq!(tuples(&space, Category::Event, "agent_spawned").len(), 1);
    }

    /// The live reconciliation sweep (`Server`'s landing background loop,
    /// alongside `run_cycle`): a late verdict for a ceiling-settled attempt
    /// must be retained as durable evidence, the sweep must be idempotent
    /// (a second tick over the same marker retains nothing new), a fresh
    /// `LandingPipeline` instance re-scanning the SAME durable space (the
    /// daemon-restart case — nothing survives in memory) must not duplicate
    /// evidence either, and none of this may mutate the landing decision:
    /// the candidate stays exactly as terminal as `late_approve_and_rework_
    /// are_retained_as_evidence_without_mutating_the_decision` already
    /// proves for the underlying `retain_late_review_evidence`.
    #[tokio::test]
    async fn reconcile_late_review_evidence_sweep_is_idempotent_and_restart_safe() {
        let home = tempfile::tempdir().unwrap();
        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let entry = review_candidate_entry(Path::new("."), "abc123");
        let attempt = review_instance_id(&entry);

        pipeline
            .settle_review_ceiling(&entry, &attempt, "review-wait-exhausted")
            .await
            .unwrap();

        // Nothing to retain yet: the sweep is a no-op, and the decision is
        // still open (never landed, never cached).
        assert_eq!(pipeline.reconcile_late_review_evidence().unwrap(), 0);
        assert!(pipeline.cached_verdict(&entry).unwrap().is_none());

        space
            .out(Tuple::new(
                Category::Artifact,
                "code-repo",
                REVIEW_ARTIFACT_IDENTITY,
                "zombie-reviewer",
                json!({
                    "task": entry.task,
                    "recommendation": "APPROVE",
                    "notes": "late",
                    "head_sha": entry.head_sha,
                    "branch": entry.branch,
                    "target": entry.target,
                    "review_attempt": attempt,
                }),
            ))
            .unwrap();

        assert_eq!(
            pipeline.reconcile_late_review_evidence().unwrap(),
            1,
            "the sweep must retain exactly the one late verdict it just found"
        );
        assert_eq!(
            tuples(&space, Category::Artifact, LATE_REVIEW_EVIDENCE_IDENTITY).len(),
            1
        );
        // The decision itself is untouched by the sweep.
        assert!(pipeline.cached_verdict(&entry).unwrap().is_none());

        // Idempotent: the next tick over the same marker retains nothing new.
        assert_eq!(pipeline.reconcile_late_review_evidence().unwrap(), 0);
        assert_eq!(
            tuples(&space, Category::Artifact, LATE_REVIEW_EVIDENCE_IDENTITY).len(),
            1
        );

        // Restart-safe: a brand new `LandingPipeline` sharing only the
        // durable space (no in-memory state carried over) re-scanning the
        // same markers must find the evidence already recorded, not
        // duplicate it.
        let restarted = test_pipeline(home.path(), space.clone());
        assert_eq!(restarted.reconcile_late_review_evidence().unwrap(), 0);
        assert_eq!(
            tuples(&space, Category::Artifact, LATE_REVIEW_EVIDENCE_IDENTITY).len(),
            1,
            "a restart replaying the same durable markers must not duplicate evidence"
        );
        assert!(restarted.cached_verdict(&entry).unwrap().is_none());
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
        assert!(pipeline
            .run_gates(&mut entry, &git_repo, &gates)
            .await
            .unwrap());

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
        assert!(pipeline
            .run_gates(&mut entry, &git_repo, &gates)
            .await
            .unwrap());

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
            assert!(pipeline
                .run_gates(&mut entry, &git_repo, &gates)
                .await
                .unwrap());
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
        assert!(pipeline
            .run_gates(&mut entry, &git_repo, &gates)
            .await
            .unwrap());

        // Backdate the marker so it would be evicted on age alone...
        let marker = pipeline.gate_worktree_marker_path("myrepo", "main");
        std::fs::write(
            &marker,
            (Utc::now() - chrono::Duration::days(30)).to_rfc3339(),
        )
        .unwrap();
        // ...but a live queue entry for this exact key must still protect
        // it. Uses the low-level `queue.enqueue` rather than
        // `pipeline.enqueue`: this fixture's `head_sha` is `main`'s own tip
        // (a convenient stand-in, not a real divergent branch), which the
        // admission-time empty-candidate check would now correctly classify
        // as a no-op and refuse to queue — this test is about the sweep's
        // queue-awareness, not admission, so it plants the tuple directly.
        pipeline.queue.enqueue(entry).unwrap();

        let cfg = rk_core::config::GateWorktreeSweepConfig {
            max_age_days: 1,
            max_per_repo: 0,
            ..rk_core::config::GateWorktreeSweepConfig::default()
        };
        let reclaims = pipeline.gate_worktree_sweep_once(&cfg, false);
        assert!(reclaims.is_empty(), "{reclaims:?}");
        assert!(pipeline.gate_worktree_path("myrepo", "main").exists());
    }

    // --- TKT-01M0P2KM9YBHYDKA51XRAP0H20: policy-driven edge classification ---

    fn lint_focused_policy() -> rk_workflow::LandingPolicy {
        rk_workflow::LandingPolicy {
            protected_targets: vec!["main".into()],
            focused_checks: vec![rk_workflow::FocusedCheckRule {
                paths: Vec::new(),
                class: "lint".into(),
                checks: vec!["lint-check".into()],
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn nested_child_to_parent_to_main_runs_focused_then_full_check() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let marker_dir = tempfile::tempdir().unwrap();
        let lint_marker = marker_dir.path().join("lint-ran");
        let verify_marker = marker_dir.path().join("verify-ran");
        let checks = format!(
            r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "lint-check", command: "echo x >> '{lint}'", timeout: "30s"}},
    {{name: "verify", command: "echo x >> '{verify}'", timeout: "30s"}},
]
"#,
            lint = lint_marker.display(),
            verify = verify_marker.display(),
        );
        write_checks(repo_dir.path(), &checks);
        activate_landing_policy(home.path(), repo_dir.path(), lint_focused_policy());

        git(repo_dir.path(), &["checkout", "-b", "parent"]);
        git(repo_dir.path(), &["checkout", "-b", "child"]);
        std::fs::write(repo_dir.path().join("feature.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add feature"]);
        let child_head = rev_parse(repo_dir.path(), "child");
        git(repo_dir.path(), &["checkout", "parent"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "nested-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "child".into(),
                target: "parent".into(),
                head_sha: child_head,
                diff_class: "doc-only".into(),
                task: "add feature".into(),
                ..Default::default()
            })
            .unwrap();
        let outcomes = pipeline.drain_key("nested-repo", "parent").await.unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(
            matches!(outcomes[0], LandingOutcome::Landed(_)),
            "{:?}",
            outcomes[0]
        );
        assert_eq!(
            std::fs::read_to_string(&lint_marker)
                .unwrap()
                .lines()
                .count(),
            1,
            "the inner edge must run its policy-selected focused check"
        );
        assert!(
            !verify_marker.exists(),
            "the inner edge must never run the full check"
        );

        let plans = space
            .scan(&Pattern::category(Category::Event).identity(LANDING_EDGE_PLAN_IDENTITY))
            .unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].payload["edge_class"], "inner");
        assert_eq!(plans[0].payload["full_check_required"], false);
        assert_eq!(
            plans[0].payload["selected_checks"],
            json!([
                "steward-protected-paths",
                "steward-diff-scope",
                "lint-check"
            ])
        );

        // Promote the (now child-carrying) parent onto the protected final
        // target: this hop must run the full check, exactly once.
        let parent_head = rev_parse(repo_dir.path(), "parent");
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "nested-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "parent".into(),
                target: "main".into(),
                head_sha: parent_head,
                diff_class: "doc-only".into(),
                task: "promote parent".into(),
                ..Default::default()
            })
            .unwrap();
        let outcomes = pipeline.drain_key("nested-repo", "main").await.unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(
            matches!(outcomes[0], LandingOutcome::Landed(_)),
            "{:?}",
            outcomes[0]
        );
        assert_eq!(
            std::fs::read_to_string(&verify_marker)
                .unwrap()
                .lines()
                .count(),
            1,
            "the protected-final edge must run the full check exactly once"
        );

        let plans = space
            .scan(&Pattern::category(Category::Event).identity(LANDING_EDGE_PLAN_IDENTITY))
            .unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[1].payload["edge_class"], "protected-final");
        assert_eq!(plans[1].payload["full_check_required"], true);
        assert!(!plans[1].payload["proof_key"].is_null());
    }

    #[tokio::test]
    async fn direct_to_main_delivery_runs_the_full_check_exactly_once() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        write_checks(repo_dir.path(), ALL_PASS_CHECKS);
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig::default();
        let mut entry = LandingQueueEntry {
            repo_name: "direct-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha,
            diff_class: "doc-only".into(),
            task: "add src".into(),
            ..Default::default()
        };
        assert!(pipeline
            .run_gates(&mut entry, &git_repo, &gates)
            .await
            .unwrap());

        let plans = space
            .scan(&Pattern::category(Category::Event).identity(LANDING_EDGE_PLAN_IDENTITY))
            .unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].payload["edge_class"], "protected-final");
        assert_eq!(plans[0].payload["full_check_required"], true);
        assert_eq!(
            plans[0].payload["selected_checks"],
            json!(["steward-protected-paths", "steward-diff-scope", "verify"])
        );
        assert!(!plans[0].payload["proof_key"].is_null());

        // The task-to-main span substrate rides alongside these same two
        // events: a `landing_prep` span from the edge plan and one
        // `verification` span PER CHECK from the settled gate run (not one
        // aggregate span for the whole run), all correlated on the ticket
        // id and idempotent (a second `run_gates` over the same candidate
        // must not double any of them).
        let spans = crate::span::spans_for_task(&space, "direct-repo", "add src").unwrap();
        let phases: std::collections::BTreeSet<&str> =
            spans.iter().map(|s| s["phase"].as_str().unwrap()).collect();
        assert!(phases.contains("landing_prep"));
        assert!(phases.contains("verification"));
        assert_eq!(phases.len(), 2);

        let verification_spans: Vec<&Value> = spans
            .iter()
            .filter(|s| s["phase"] == "verification")
            .collect();
        assert_eq!(
            verification_spans.len(),
            3,
            "one span per check (steward-protected-paths, steward-diff-scope, verify), not one \
             aggregate span for the whole gate run: {verification_spans:?}"
        );
        let lanes: std::collections::BTreeSet<&str> = verification_spans
            .iter()
            .map(|s| s["lane"].as_str().unwrap())
            .collect();
        assert_eq!(
            lanes,
            std::collections::BTreeSet::from([
                "steward-protected-paths",
                "steward-diff-scope",
                "verify"
            ])
        );
        let attempts: std::collections::BTreeSet<u64> = verification_spans
            .iter()
            .map(|s| s["attempt"].as_u64().unwrap())
            .collect();
        assert_eq!(
            attempts,
            std::collections::BTreeSet::from([1, 2, 3]),
            "each check's span is keyed by its plan position: {verification_spans:?}"
        );
        assert!(
            verification_spans
                .iter()
                .all(|s| s["proof_kind"] == "full-final"),
            "{verification_spans:?}"
        );
    }

    #[tokio::test]
    async fn protected_path_touch_holds_an_inner_edge_the_same_as_a_final_one() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let checks = r#"checks: [
    {name: "steward-protected-paths", command: "target=$RK_CHECK_TARGET; ! git diff --name-only \"$target\"...HEAD | grep -qE \"$RK_CHECK_PROTECTED_PATHS\"", timeout: "30s"},
    {name: "steward-diff-scope", command: "true", timeout: "30s"},
    {name: "lint-check", command: "true", timeout: "30s"},
    {name: "verify", command: "true", timeout: "30s"},
]
"#;
        write_checks(repo_dir.path(), checks);
        activate_landing_policy(home.path(), repo_dir.path(), lint_focused_policy());

        git(repo_dir.path(), &["checkout", "-b", "parent"]);
        git(repo_dir.path(), &["checkout", "-b", "child"]);
        std::fs::create_dir_all(repo_dir.path().join("migrations")).unwrap();
        std::fs::write(
            repo_dir.path().join("migrations").join("x.sql"),
            "select 1;\n",
        )
        .unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "add migration"]);
        let head_sha = rev_parse(repo_dir.path(), "child");
        git(repo_dir.path(), &["checkout", "parent"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "escalation-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "child".into(),
                target: "parent".into(),
                head_sha,
                diff_class: "doc-only".into(),
                task: "add migration".into(),
                ..Default::default()
            })
            .unwrap();
        let outcomes = pipeline
            .drain_key("escalation-repo", "parent")
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(
            matches!(outcomes[0], LandingOutcome::GateHeld),
            "{:?}",
            outcomes[0]
        );

        let plans = space
            .scan(&Pattern::category(Category::Event).identity(LANDING_EDGE_PLAN_IDENTITY))
            .unwrap();
        assert_eq!(plans.len(), 1, "the plan is recorded before any check runs");
        assert_eq!(plans[0].payload["edge_class"], "inner");
        assert_eq!(plans[0].payload["full_check_required"], false);
        assert!(
            space
                .scan(&Pattern::category(Category::Event).identity(GATE_PASS_IDENTITY))
                .unwrap()
                .is_empty(),
            "a protected-path violation must never reach a green gate run"
        );
    }

    #[tokio::test]
    async fn focused_check_failure_holds_an_inner_edge_without_running_the_full_check() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let marker_dir = tempfile::tempdir().unwrap();
        let verify_marker = marker_dir.path().join("verify-ran");
        let checks = format!(
            r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "lint-check", command: "exit 1", timeout: "30s"}},
    {{name: "verify", command: "echo x >> '{verify}'", timeout: "30s"}},
]
"#,
            verify = verify_marker.display(),
        );
        write_checks(repo_dir.path(), &checks);
        activate_landing_policy(home.path(), repo_dir.path(), lint_focused_policy());

        git(repo_dir.path(), &["checkout", "-b", "parent"]);
        git(repo_dir.path(), &["checkout", "-b", "child"]);
        std::fs::write(repo_dir.path().join("feature.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add feature"]);
        let head_sha = rev_parse(repo_dir.path(), "child");
        git(repo_dir.path(), &["checkout", "parent"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "focused-fail-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "child".into(),
                target: "parent".into(),
                head_sha,
                diff_class: "doc-only".into(),
                task: "add feature".into(),
                ..Default::default()
            })
            .unwrap();
        let outcomes = pipeline
            .drain_key("focused-fail-repo", "parent")
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert!(
            matches!(outcomes[0], LandingOutcome::GateHeld),
            "{:?}",
            outcomes[0]
        );
        assert!(
            !verify_marker.exists(),
            "the full check must never run once the focused check already failed"
        );

        let plans = space
            .scan(&Pattern::category(Category::Event).identity(LANDING_EDGE_PLAN_IDENTITY))
            .unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0].payload["selected_checks"],
            json!([
                "steward-protected-paths",
                "steward-diff-scope",
                "lint-check"
            ])
        );
    }

    #[tokio::test]
    async fn target_movement_requeues_and_recomputes_a_fresh_edge_plan_and_proof_key() {
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
        git(repo_dir.path(), &["checkout", "main"]);
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("feature.txt"), "feature\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feature"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        pipeline
            .enqueue(LandingQueueEntry {
                repo_name: "movement-repo".into(),
                repo_path: repo_dir.path().display().to_string(),
                branch: "feature".into(),
                target: "main".into(),
                head_sha,
                diff_class: "doc-only".into(),
                task: "add feature".into(),
                ..Default::default()
            })
            .unwrap();
        let outcomes = pipeline.drain_key("movement-repo", "main").await.unwrap();
        assert!(matches!(
            outcomes.first(),
            Some(LandingOutcome::Requeued { .. })
        ));
        assert!(matches!(outcomes.last(), Some(LandingOutcome::Landed(_))));

        let plans = space
            .scan(&Pattern::category(Category::Event).identity(LANDING_EDGE_PLAN_IDENTITY))
            .unwrap();
        assert_eq!(
            plans.len(),
            2,
            "one edge plan per attempt, never reused across a target move: {plans:?}"
        );
        assert_eq!(plans[0].payload["edge_class"], "protected-final");
        assert_eq!(plans[1].payload["edge_class"], "protected-final");
        let candidate_a = plans[0].payload["candidate_sha"].as_str().unwrap();
        let candidate_b = plans[1].payload["candidate_sha"].as_str().unwrap();
        assert_ne!(
            candidate_a, candidate_b,
            "the retried attempt must test a freshly rebuilt candidate"
        );
        let proof_a = plans[0].payload["proof_key"].as_str().unwrap();
        let proof_b = plans[1].payload["proof_key"].as_str().unwrap();
        assert_ne!(
            proof_a, proof_b,
            "target movement must invalidate the prior attempt's proof key"
        );
    }

    #[tokio::test]
    async fn reviewer_verify_run_reuses_the_landing_gates_full_check_proof_without_rerunning() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let marker_dir = tempfile::tempdir().unwrap();
        let counter_file = marker_dir.path().join("verify-runs");
        let checks = format!(
            r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "echo x >> '{counter}'", timeout: "30s"}},
]
"#,
            counter = counter_file.display(),
        );
        write_checks(repo_dir.path(), &checks);
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig::default();
        let mut entry = LandingQueueEntry {
            repo_name: "replay-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: head_sha.clone(),
            diff_class: "doc-only".into(),
            task: "add src".into(),
            ..Default::default()
        };
        assert!(pipeline
            .run_gates(&mut entry, &git_repo, &gates)
            .await
            .unwrap());
        assert_eq!(
            std::fs::read_to_string(&counter_file)
                .unwrap()
                .lines()
                .count(),
            1,
            "the landing gate must run the full check exactly once"
        );

        // A reviewer's own `verify.run`, against the exact same prepared
        // candidate, must reuse that proof rather than duplicating it.
        let gate_dir = pipeline.gate_worktree_path("replay-repo", "main");
        let result = pipeline
            .engine
            .verify_repo_check(
                "reviewer-rat",
                &gate_dir,
                "replay-repo",
                "verify",
                None,
                "replay-request",
                None,
            )
            .await
            .unwrap();
        assert_eq!(result["reused"], true, "result: {result}");
        assert_eq!(
            std::fs::read_to_string(&counter_file)
                .unwrap()
                .lines()
                .count(),
            1,
            "the reviewer's verify.run must not re-execute the full check"
        );
    }

    /// The ticket's primary scenario (TKT-01M0QRZ7QT8CQD74GHRN81XFT5, live
    /// campaign evidence from Sooty-12/Ash-12/Dusty-12): a rat's own managed
    /// `rk verify` already produced a durable passing proof for its branch's
    /// own tip (`head_sha`) BEFORE the landing gate ever runs. The gate's
    /// own prepared candidate (`rk_git::Repo::prepare_merge`'s `--no-ff`
    /// merge commit) is, by construction, a different git object from
    /// `head_sha` — so this proves the landing gate bridges the two shas via
    /// the ancestor check, rather than merely hitting an exact-sha cache
    /// entry it wrote itself.
    #[tokio::test]
    async fn landing_gate_reuses_a_managed_verify_proof_when_main_has_not_moved() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let verify_log = home.path().join("verify.log");
        let verify_command = format!("echo x >> '{log}'; exit 0", log = verify_log.display());
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "{cmd}", timeout: "30s"}},
]
"#,
                cmd = verify_command
            ),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig::default();

        // The agent's own managed `rk verify`, run directly against its own
        // branch worktree at its clean tip — well before any landing gate
        // exists for this branch.
        let proof = pipeline
            .engine
            .verify_repo_check(
                "rat-1",
                repo_dir.path(),
                "code-repo",
                "verify",
                None,
                "verify-request-1",
                None,
            )
            .await
            .unwrap();
        assert_eq!(proof["verdict"], "pass");
        git(repo_dir.path(), &["checkout", "main"]);

        let candidate = match git_repo.prepare_merge("feature", "main").unwrap() {
            rk_git::PrepareOutcome::Prepared(candidate) => candidate,
            other => panic!("expected prepared merge, got {other:?}"),
        };
        assert_ne!(
            candidate.commit, head_sha,
            "a `--no-ff` merge always builds a fresh commit distinct from the branch tip"
        );

        let mut entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: head_sha.clone(),
            candidate_sha: Some(candidate.commit.clone()),
            candidate_base: Some(candidate.base.clone()),
            candidate_ref: Some(candidate.candidate_ref.clone()),
            diff_class: "feature".into(),
            task: "add src".into(),
            ..Default::default()
        };

        let outcome = pipeline
            .run_gates_at(&mut entry, &git_repo, &gates, &candidate.commit)
            .await
            .unwrap();
        assert_eq!(outcome, GateRunOutcome::Pass);
        assert_eq!(
            std::fs::read_to_string(&verify_log)
                .unwrap()
                .lines()
                .count(),
            1,
            "the landing gate must reuse the managed proof instead of re-running verify"
        );

        let reuse_events = space
            .scan(&Pattern::category(Category::Event).identity(VERIFICATION_PROOF_REUSE_IDENTITY))
            .unwrap();
        assert_eq!(reuse_events.len(), 1, "reuse events: {reuse_events:?}");
        assert_eq!(reuse_events[0].payload["check"], "verify");
        assert_eq!(reuse_events[0].payload["head_sha"], head_sha);
        assert_eq!(reuse_events[0].payload["tested_sha"], candidate.commit);
        assert_eq!(
            reuse_events[0].payload["reused_from"], "verification_proof",
            "must credit the rat's own managed verify, not an unrelated landing_gate_pass"
        );

        // The gate still records its usual green-run evidence, crediting
        // "verify" as passed — a reused check must look identical to a
        // freshly-run one to every OTHER consumer of that event.
        let pass_events = space
            .scan(&Pattern::category(Category::Event).identity(GATE_PASS_IDENTITY))
            .unwrap();
        assert_eq!(pass_events.len(), 1);
        assert_eq!(
            pass_events[0].payload["checks"],
            json!(["steward-protected-paths", "steward-diff-scope", "verify"])
        );
    }

    /// Fail-closed counterpart of the previous test: `main` (the merge
    /// target) advances with a commit the verified `head_sha` never saw
    /// BEFORE the landing gate builds its candidate. The prepared merge's
    /// `base` is therefore no longer an ancestor of `head_sha`, so the
    /// ticket's "main movement changes the prepared candidate" rule must
    /// hold — no reuse, the full check runs fresh against the real merge.
    #[tokio::test]
    async fn landing_gate_reruns_when_main_moved_past_the_verified_head() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let verify_log = home.path().join("verify.log");
        let verify_command = format!("echo x >> '{log}'; exit 0", log = verify_log.display());
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "{cmd}", timeout: "30s"}},
]
"#,
                cmd = verify_command
            ),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig::default();

        let proof = pipeline
            .engine
            .verify_repo_check(
                "rat-1",
                repo_dir.path(),
                "code-repo",
                "verify",
                None,
                "verify-request-1",
                None,
            )
            .await
            .unwrap();
        assert_eq!(proof["verdict"], "pass");

        // `main` moves with an unrelated change AFTER the branch's own
        // verify ran, and before landing ever builds a candidate.
        git(repo_dir.path(), &["checkout", "main"]);
        std::fs::write(repo_dir.path().join("other.rs"), "fn y() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(
            repo_dir.path(),
            &["commit", "-m", "feat: unrelated main commit"],
        );

        let candidate = match git_repo.prepare_merge("feature", "main").unwrap() {
            rk_git::PrepareOutcome::Prepared(candidate) => candidate,
            other => panic!("expected a clean auto-merge (disjoint files), got {other:?}"),
        };
        assert!(
            !git_repo.is_ancestor(&candidate.base, &head_sha),
            "precondition: the moved target must NOT be an ancestor of the verified head"
        );

        let mut entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: head_sha.clone(),
            candidate_sha: Some(candidate.commit.clone()),
            candidate_base: Some(candidate.base.clone()),
            candidate_ref: Some(candidate.candidate_ref.clone()),
            diff_class: "feature".into(),
            task: "add src".into(),
            ..Default::default()
        };

        let outcome = pipeline
            .run_gates_at(&mut entry, &git_repo, &gates, &candidate.commit)
            .await
            .unwrap();
        assert_eq!(outcome, GateRunOutcome::Pass);
        assert_eq!(
            std::fs::read_to_string(&verify_log)
                .unwrap()
                .lines()
                .count(),
            2,
            "main moved past what head_sha's proof covered, so verify must run again"
        );
        let reuse_events = space
            .scan(&Pattern::category(Category::Event).identity(VERIFICATION_PROOF_REUSE_IDENTITY))
            .unwrap();
        assert!(
            reuse_events.is_empty(),
            "no reuse may be credited once main has moved past the verified head: {reuse_events:?}"
        );
    }

    /// Even when `main` has not moved (the ancestor check alone would allow
    /// reuse), a check whose command changed since the managed verify ran
    /// must never reuse that stale proof: `verification_proof_key` folds
    /// the exact command text into its digest, so a changed command simply
    /// misses the cache.
    #[tokio::test]
    async fn landing_gate_reruns_when_the_checks_command_changed_since_verification() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let old_log = home.path().join("old-verify.log");
        let new_log = home.path().join("new-verify.log");
        let old_command = format!("echo x >> '{log}'; exit 0", log = old_log.display());
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "{cmd}", timeout: "30s"}},
]
"#,
                cmd = old_command
            ),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig::default();

        let proof = pipeline
            .engine
            .verify_repo_check(
                "rat-1",
                repo_dir.path(),
                "code-repo",
                "verify",
                None,
                "verify-request-1",
                None,
            )
            .await
            .unwrap();
        assert_eq!(proof["verdict"], "pass");
        git(repo_dir.path(), &["checkout", "main"]);

        // The repo's checks registry changes its "verify" command before
        // landing runs — a plain uncommitted working-tree edit, exactly
        // like a live `.rk/checks.cue` edit landing always reads fresh.
        let new_command = format!("echo x >> '{log}'; exit 0", log = new_log.display());
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "{cmd}", timeout: "30s"}},
]
"#,
                cmd = new_command
            ),
        );

        let candidate = match git_repo.prepare_merge("feature", "main").unwrap() {
            rk_git::PrepareOutcome::Prepared(candidate) => candidate,
            other => panic!("expected prepared merge, got {other:?}"),
        };

        let mut entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: head_sha.clone(),
            candidate_sha: Some(candidate.commit.clone()),
            candidate_base: Some(candidate.base.clone()),
            candidate_ref: Some(candidate.candidate_ref.clone()),
            diff_class: "feature".into(),
            task: "add src".into(),
            ..Default::default()
        };

        let outcome = pipeline
            .run_gates_at(&mut entry, &git_repo, &gates, &candidate.commit)
            .await
            .unwrap();
        assert_eq!(outcome, GateRunOutcome::Pass);
        assert_eq!(
            std::fs::read_to_string(&old_log).unwrap().lines().count(),
            1,
            "the old command must not run again"
        );
        assert_eq!(
            std::fs::read_to_string(&new_log).unwrap().lines().count(),
            1,
            "the new command must run fresh — its digest key never matches the old proof"
        );
    }

    /// A managed verification that was CANCELLED (agent dismissed/interrupted,
    /// RPC caller disconnected) before it settled never writes a
    /// `verification_proof` — only `verification_cancelled`
    /// (`workflow_exec.rs`'s `VERIFICATION_CANCELLED_IDENTITY` doc). This
    /// locks in that `lookup_verification_proof` cannot mistake the
    /// cancellation record for a reusable proof, by planting one directly
    /// (as `record_verification_cancellation` would) with no matching
    /// `verification_proof` ever written, and confirming the gate still
    /// executes the check fresh.
    #[tokio::test]
    async fn landing_gate_never_reuses_a_cancelled_managed_verification() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let verify_log = home.path().join("verify.log");
        let verify_command = format!("echo x >> '{log}'; exit 0", log = verify_log.display());
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "{cmd}", timeout: "30s"}},
]
"#,
                cmd = verify_command
            ),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig::default();

        // Plant a cancellation record for this exact head_sha/check — never
        // a `verification_proof` — the shape `record_verification_cancellation`
        // itself writes.
        space
            .out(
                Tuple::new(
                    Category::Event,
                    "code-repo".to_string(),
                    "verification_cancelled",
                    "daemon",
                    json!({
                        "agent": "rat-1",
                        "generation": Value::Null,
                        "request_key": "verify-request-1",
                        "proof_key": Value::Null,
                        "command": verify_command,
                        "queue_wait_ms": Value::Null,
                        "duration_ms": Value::Null,
                        "reason": "dismissed",
                    }),
                )
                .with_lifecycle(Lifecycle::Furniture),
            )
            .unwrap();

        let candidate = match git_repo.prepare_merge("feature", "main").unwrap() {
            rk_git::PrepareOutcome::Prepared(candidate) => candidate,
            other => panic!("expected prepared merge, got {other:?}"),
        };

        let mut entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: head_sha.clone(),
            candidate_sha: Some(candidate.commit.clone()),
            candidate_base: Some(candidate.base.clone()),
            candidate_ref: Some(candidate.candidate_ref.clone()),
            diff_class: "feature".into(),
            task: "add src".into(),
            ..Default::default()
        };

        let outcome = pipeline
            .run_gates_at(&mut entry, &git_repo, &gates, &candidate.commit)
            .await
            .unwrap();
        assert_eq!(outcome, GateRunOutcome::Pass);
        assert_eq!(
            std::fs::read_to_string(&verify_log)
                .unwrap()
                .lines()
                .count(),
            1,
            "a cancelled run leaves nothing reusable — the gate must execute the check itself"
        );
        let reuse_events = space
            .scan(&Pattern::category(Category::Event).identity(VERIFICATION_PROOF_REUSE_IDENTITY))
            .unwrap();
        assert!(
            reuse_events.is_empty(),
            "a cancellation record must never be credited as a reused proof: {reuse_events:?}"
        );
    }

    /// Restart/replay, and the ordinary baseline the ticket also requires:
    /// an ordinary completion-to-landing gate run executes its full check
    /// exactly once total, and a later replay of the SAME prepared candidate
    /// (a landing pipeline resuming after a restart, re-processing durable
    /// queue state) reuses that first run's own `landing_gate_pass` evidence
    /// instead of re-running — the pre-existing fallback branch of
    /// `lookup_verification_proof`, now actually reachable from
    /// `run_gates_at` for the first time.
    #[tokio::test]
    async fn landing_gate_runs_once_ordinarily_and_reuses_its_own_pass_on_replay() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let verify_log = home.path().join("verify.log");
        let verify_command = format!("echo x >> '{log}'; exit 0", log = verify_log.display());
        write_checks(
            repo_dir.path(),
            &format!(
                r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "{cmd}", timeout: "30s"}},
]
"#,
                cmd = verify_command
            ),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig::default();

        let candidate = match git_repo.prepare_merge("feature", "main").unwrap() {
            rk_git::PrepareOutcome::Prepared(candidate) => candidate,
            other => panic!("expected prepared merge, got {other:?}"),
        };

        let mut entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: head_sha.clone(),
            candidate_sha: Some(candidate.commit.clone()),
            candidate_base: Some(candidate.base.clone()),
            candidate_ref: Some(candidate.candidate_ref.clone()),
            diff_class: "feature".into(),
            task: "add src".into(),
            ..Default::default()
        };

        // Ordinary case: no proof exists anywhere yet, so the full suite
        // executes exactly once.
        let outcome = pipeline
            .run_gates_at(&mut entry, &git_repo, &gates, &candidate.commit)
            .await
            .unwrap();
        assert_eq!(outcome, GateRunOutcome::Pass);
        assert_eq!(
            std::fs::read_to_string(&verify_log)
                .unwrap()
                .lines()
                .count(),
            1,
            "an ordinary completion-to-landing run must execute the full suite exactly once"
        );

        // Replay: the daemon "restarts" and reprocesses the identical
        // durable candidate (same tested_sha) — must reuse, never re-run.
        let outcome = pipeline
            .run_gates_at(&mut entry, &git_repo, &gates, &candidate.commit)
            .await
            .unwrap();
        assert_eq!(outcome, GateRunOutcome::Pass);
        assert_eq!(
            std::fs::read_to_string(&verify_log)
                .unwrap()
                .lines()
                .count(),
            1,
            "replaying the same prepared candidate must not re-execute the check"
        );

        // Every check in the plan (the two cheap policy checks plus
        // "verify") was credited by the first run's own `landing_gate_pass`
        // for this exact candidate, so replay reuses all three.
        let reuse_events = space
            .scan(&Pattern::category(Category::Event).identity(VERIFICATION_PROOF_REUSE_IDENTITY))
            .unwrap();
        assert_eq!(reuse_events.len(), 3, "reuse events: {reuse_events:?}");
        assert!(
            reuse_events
                .iter()
                .all(|t| t.payload["reused_from"] == "landing_gate_pass"),
            "replay must credit the gate's own prior pass, not a managed verify proof: {reuse_events:?}"
        );

        // Exactly two `landing_gate_pass` records total — one per
        // `run_gates_at` call, whether the checks it credits ran fresh or
        // were reused — never fewer (a reused check must still count as
        // "passed this gate run" for every other consumer of this event).
        let pass_events = space
            .scan(&Pattern::category(Category::Event).identity(GATE_PASS_IDENTITY))
            .unwrap();
        assert_eq!(pass_events.len(), 2, "pass events: {pass_events:?}");
    }

    /// Resolution and execution are one admission decision: once the exact
    /// candidate's CUE plan has been built, a concurrent edit to the live
    /// registry cannot swap the command underneath that in-flight run. A
    /// later candidate resolves the new registry normally.
    #[tokio::test]
    async fn landing_gate_executes_the_single_resolved_cue_plan() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let resolved_log = home.path().join("resolved.log");
        let late_log = home.path().join("late.log");
        write_checks(
            repo_dir.path(),
            &checks_cue_with_verify(
                &format!("echo resolved >> '{}'", resolved_log.display()),
                "rust-stable",
                "inherit",
            ),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        let head_sha = rev_parse(repo_dir.path(), "feature");
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space);
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig::default();
        let candidate = match git_repo.prepare_merge("feature", "main").unwrap() {
            rk_git::PrepareOutcome::Prepared(candidate) => candidate,
            other => panic!("expected prepared merge, got {other:?}"),
        };
        let mut entry = LandingQueueEntry {
            repo_name: "single-resolution-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha,
            candidate_sha: Some(candidate.commit.clone()),
            candidate_base: Some(candidate.base.clone()),
            candidate_ref: Some(candidate.candidate_ref.clone()),
            diff_class: "feature".into(),
            task: "single-resolution-task".into(),
            ..Default::default()
        };

        let plan = pipeline
            .resolve_gate_plan_at(&entry, &git_repo, &gates, &candidate.commit)
            .await
            .unwrap();
        write_checks_uncommitted(
            repo_dir.path(),
            &checks_cue_with_verify(
                &format!("echo late >> '{}'", late_log.display()),
                "rust-stable",
                "inherit",
            ),
        );

        let outcome = pipeline
            .execute_gate_plan_at(&mut entry, &git_repo, plan, &candidate.commit)
            .await
            .unwrap();
        assert_eq!(outcome, GateRunOutcome::Pass);
        assert!(resolved_log.exists(), "the resolved command must run");
        assert!(
            !late_log.exists(),
            "execution must not reread and substitute the later CUE command"
        );
    }

    /// Builds a `checks.cue` registry identical to the replay test's own,
    /// except the "verify" check's command/toolchain/environmentPolicy are
    /// parameterized — so the three tests below can each vary exactly one
    /// `verification_proof_key` identity component while holding the other
    /// two fixed.
    fn checks_cue_with_verify(cmd: &str, toolchain: &str, env_policy: &str) -> String {
        format!(
            r#"checks: [
    {{name: "steward-protected-paths", command: "true", timeout: "30s"}},
    {{name: "steward-diff-scope", command: "true", timeout: "30s"}},
    {{name: "verify", command: "{cmd}", timeout: "30s", toolchain: "{toolchain}", environmentPolicy: "{env_policy}"}},
]
"#
        )
    }

    /// The bug this module's `check_proof_keys` field fixes
    /// (TKT-01M0QWJ1EGZ0E9PZZP0JA0SA2A): the `landing_gate_pass` fallback in
    /// `WorkflowEngine::lookup_verification_proof` used to match on nothing
    /// but `candidate_sha` + check NAME, so if the checks registry changed
    /// between two `run_gates_at` calls against the identical prepared
    /// candidate — possible because `gate_plan` reads `checks.cue` live off
    /// disk, not pinned to the candidate's own git tree — a replay could
    /// silently skip re-verifying a check whose actual command changed.
    /// Before the fix this test fails: the second run reuses the first
    /// run's stale `landing_gate_pass` for "verify" and the command below
    /// never executes a second time.
    #[tokio::test]
    async fn landing_gate_replay_reruns_verify_when_its_command_changed() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let verify_log = home.path().join("verify.log");
        write_checks(
            repo_dir.path(),
            &checks_cue_with_verify(
                &format!("echo x >> '{}'; exit 0", verify_log.display()),
                "rust-1.95.0",
                "inherit",
            ),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig::default();

        let candidate = match git_repo.prepare_merge("feature", "main").unwrap() {
            rk_git::PrepareOutcome::Prepared(candidate) => candidate,
            other => panic!("expected prepared merge, got {other:?}"),
        };

        let mut entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: rev_parse(repo_dir.path(), "feature"),
            candidate_sha: Some(candidate.commit.clone()),
            candidate_base: Some(candidate.base.clone()),
            candidate_ref: Some(candidate.candidate_ref.clone()),
            diff_class: "feature".into(),
            task: "add src".into(),
            ..Default::default()
        };

        let outcome = pipeline
            .run_gates_at(&mut entry, &git_repo, &gates, &candidate.commit)
            .await
            .unwrap();
        assert_eq!(outcome, GateRunOutcome::Pass);
        assert_eq!(
            std::fs::read_to_string(&verify_log)
                .unwrap()
                .lines()
                .count(),
            1,
            "first run must execute the check once"
        );

        // Mutate the registry ON DISK, same candidate sha, same log file —
        // only the "verify" command text changes (still appends to the same
        // log, so a re-execution is unambiguous either way).
        write_checks_uncommitted(
            repo_dir.path(),
            &checks_cue_with_verify(
                &format!("echo y >> '{}'; exit 0", verify_log.display()),
                "rust-1.95.0",
                "inherit",
            ),
        );

        let outcome = pipeline
            .run_gates_at(&mut entry, &git_repo, &gates, &candidate.commit)
            .await
            .unwrap();
        assert_eq!(outcome, GateRunOutcome::Pass);
        assert_eq!(
            std::fs::read_to_string(&verify_log)
                .unwrap()
                .lines()
                .count(),
            2,
            "a changed verify command must re-execute, never reuse a stale landing_gate_pass proof"
        );

        let reuse_events = space
            .scan(&Pattern::category(Category::Event).identity(VERIFICATION_PROOF_REUSE_IDENTITY))
            .unwrap();
        assert!(
            reuse_events.iter().all(|t| t.payload["check"] != "verify"),
            "verify must never be credited as reused once its command changed: {reuse_events:?}"
        );
        // The two unchanged cheap checks still reuse — the fix must not cost
        // the exact-match fast path anything.
        assert_eq!(
            reuse_events
                .iter()
                .filter(|t| t.payload["reused_from"] == "landing_gate_pass")
                .count(),
            2,
            "the two unrelated, unchanged checks must still reuse via landing_gate_pass: {reuse_events:?}"
        );
    }

    /// Same shape as the command-change test above, but only `toolchain`
    /// changes — `command` and `environmentPolicy` stay byte-identical.
    /// `verification_proof_key` folds toolchain into its digest, so this
    /// must also force a fresh run rather than a false-positive reuse.
    #[tokio::test]
    async fn landing_gate_replay_reruns_verify_when_its_toolchain_changed() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let verify_log = home.path().join("verify.log");
        let verify_command = format!("echo x >> '{}'; exit 0", verify_log.display());
        write_checks(
            repo_dir.path(),
            &checks_cue_with_verify(&verify_command, "rust-1.95.0", "inherit"),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig::default();

        let candidate = match git_repo.prepare_merge("feature", "main").unwrap() {
            rk_git::PrepareOutcome::Prepared(candidate) => candidate,
            other => panic!("expected prepared merge, got {other:?}"),
        };

        let mut entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: rev_parse(repo_dir.path(), "feature"),
            candidate_sha: Some(candidate.commit.clone()),
            candidate_base: Some(candidate.base.clone()),
            candidate_ref: Some(candidate.candidate_ref.clone()),
            diff_class: "feature".into(),
            task: "add src".into(),
            ..Default::default()
        };

        pipeline
            .run_gates_at(&mut entry, &git_repo, &gates, &candidate.commit)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&verify_log)
                .unwrap()
                .lines()
                .count(),
            1
        );

        write_checks_uncommitted(
            repo_dir.path(),
            &checks_cue_with_verify(&verify_command, "rust-1.96.0", "inherit"),
        );

        pipeline
            .run_gates_at(&mut entry, &git_repo, &gates, &candidate.commit)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&verify_log)
                .unwrap()
                .lines()
                .count(),
            2,
            "a changed toolchain must re-execute, never reuse a stale landing_gate_pass proof"
        );
        let reuse_events = space
            .scan(&Pattern::category(Category::Event).identity(VERIFICATION_PROOF_REUSE_IDENTITY))
            .unwrap();
        assert!(
            reuse_events.iter().all(|t| t.payload["check"] != "verify"),
            "verify must never be credited as reused once its toolchain changed: {reuse_events:?}"
        );
    }

    /// Same shape again, but only `environmentPolicy` changes.
    #[tokio::test]
    async fn landing_gate_replay_reruns_verify_when_its_environment_policy_changed() {
        let home = tempfile::tempdir().unwrap();
        let repo_dir = tempfile::tempdir().unwrap();
        init_repo(repo_dir.path());
        let verify_log = home.path().join("verify.log");
        let verify_command = format!("echo x >> '{}'; exit 0", verify_log.display());
        write_checks(
            repo_dir.path(),
            &checks_cue_with_verify(&verify_command, "rust-1.95.0", "inherit"),
        );
        git(repo_dir.path(), &["checkout", "-b", "feature"]);
        std::fs::write(repo_dir.path().join("src.rs"), "fn x() {}\n").unwrap();
        git(repo_dir.path(), &["add", "."]);
        git(repo_dir.path(), &["commit", "-m", "feat: add src"]);
        git(repo_dir.path(), &["checkout", "main"]);

        let space = Space::open_in_memory().unwrap();
        let pipeline = test_pipeline(home.path(), space.clone());
        let git_repo = rk_git::Repo::discover(repo_dir.path()).unwrap();
        let gates = GateConfig::default();

        let candidate = match git_repo.prepare_merge("feature", "main").unwrap() {
            rk_git::PrepareOutcome::Prepared(candidate) => candidate,
            other => panic!("expected prepared merge, got {other:?}"),
        };

        let mut entry = LandingQueueEntry {
            repo_name: "code-repo".into(),
            repo_path: repo_dir.path().display().to_string(),
            branch: "feature".into(),
            target: "main".into(),
            head_sha: rev_parse(repo_dir.path(), "feature"),
            candidate_sha: Some(candidate.commit.clone()),
            candidate_base: Some(candidate.base.clone()),
            candidate_ref: Some(candidate.candidate_ref.clone()),
            diff_class: "feature".into(),
            task: "add src".into(),
            ..Default::default()
        };

        pipeline
            .run_gates_at(&mut entry, &git_repo, &gates, &candidate.commit)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&verify_log)
                .unwrap()
                .lines()
                .count(),
            1
        );

        write_checks_uncommitted(
            repo_dir.path(),
            &checks_cue_with_verify(&verify_command, "rust-1.95.0", "strip_rk_spawn"),
        );

        pipeline
            .run_gates_at(&mut entry, &git_repo, &gates, &candidate.commit)
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&verify_log).unwrap().lines().count(),
            2,
            "a changed environment policy must re-execute, never reuse a stale landing_gate_pass proof"
        );
        let reuse_events = space
            .scan(&Pattern::category(Category::Event).identity(VERIFICATION_PROOF_REUSE_IDENTITY))
            .unwrap();
        assert!(
            reuse_events.iter().all(|t| t.payload["check"] != "verify"),
            "verify must never be credited as reused once its environment policy changed: {reuse_events:?}"
        );
    }
}
