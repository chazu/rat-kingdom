//! Agent registry: the supervision tree as first-class data.
//!
//! Every record carries its `parent` (the spawner) — completion routing walks
//! this structure, never payload fields (the predecessor's foreman-routing lesson).

use chrono::{DateTime, Utc};
use rk_core::review::ReviewContext;
use rk_harness::TokenUsage;
use rk_workflow::Coordination;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Spawning,
    Running,
    /// The harness returned control at a turn boundary *without* the agent
    /// declaring completion (`rk done`), and its process is still alive — so
    /// another turn can still follow (a steer, or the harness resuming itself).
    ///
    /// This is the "awaiting resume" state, and it is deliberately neither
    /// [`Completed`](AgentState::Completed) nor [`Failed`](AgentState::Failed).
    /// Before it existed, a rat that paused mid-task (waiting on a slow
    /// background check) was recorded `Completed`: drain freed its slot, the
    /// orphaned-ticket sweep saw no live owner and reclaimed its ticket, a
    /// duplicate rat was dispatched onto the same work, and — for a reviewer —
    /// the reaper that tore the "finished" agent down turned its withheld
    /// mid-flight turn into a published `is_error: true`, hard-failing a review
    /// that was proceeding correctly (probe O6/O8).
    ///
    /// A paused agent counts as LIVE ([`is_live`](AgentState::is_live)), which
    /// is the single predicate drain WIP accounting, the ticket-reopen sweep,
    /// and the workflow `wait` liveness gate all key on. It leaves this state
    /// on the next harness event (back to `Running`), at its `rk done`
    /// (`Completed`), or when the process goes away (`Failed`, via the `Exited`
    /// handler) — so a genuinely dead agent still terminalizes promptly, and
    /// only a *live* process holds a slot.
    ///
    /// The paused/failed boundary is decided by liveness alone, never by
    /// whether `rk done` was declared: a `Paused` record whose process then
    /// exits becomes `Failed` regardless (the `Exited` handler treats
    /// `Paused` as just another live state to terminalize), and a `Running`
    /// record that dies mid-turn is `Failed` too, `rk done` or not. A
    /// declared-but-dead generation and an undeclared-but-dead one land in
    /// the same place; only "is the process still there to resume" tells
    /// `Paused` apart from `Failed`.
    Paused,
    Completed,
    /// The process is gone and nothing will resume it. Reached from any live
    /// state ([`is_live`](AgentState::is_live)) whose harness never
    /// terminalized cleanly with `rk done` — see the discriminator note on
    /// [`Paused`](AgentState::Paused).
    Failed,
    /// Deliberately terminated by an operator or a hard budget guard. Unlike
    /// [`Failed`](AgentState::Failed), this is not a crash and is never
    /// eligible for automatic respawn. The preserved worktree/branch may
    /// still be resumed explicitly with `rk respawn`.
    Stopped,
    Dismissed,
    /// The daemon restarted while this agent was running; its process is gone
    /// but worktree/branch/session are preserved for respawn.
    Orphaned,
}

impl AgentState {
    /// Whether this agent still holds a slot: a process exists and more work
    /// can still come out of it.
    ///
    /// [`Paused`](AgentState::Paused) is live for the same reason `Running` is
    /// — its process is alive and its turn may resume. Every consumer of this
    /// predicate (drain WIP, the orphaned-ticket reopen sweep, the workflow
    /// `wait` gate, the burn/stuck sweep, cost rollups) wants that reading:
    /// a paused rat must not free a slot, lose its ticket to a duplicate, or
    /// have a result attributed to it that it never published.
    pub fn is_live(self) -> bool {
        matches!(
            self,
            AgentState::Spawning | AgentState::Running | AgentState::Paused
        )
    }

    /// Whether a record in this state may be archived out of the default views.
    ///
    /// Only the three *settled* terminal states qualify.
    /// `Spawning`/`Running`/`Paused` are live work, and `Orphaned` is
    /// deliberately excluded even though it is
    /// terminal-ish: its worktree/branch/session are preserved precisely so
    /// `rk respawn` (and the respawn sweep) can pick it back up.
    pub fn is_archivable(self) -> bool {
        matches!(
            self,
            AgentState::Completed
                | AgentState::Failed
                | AgentState::Stopped
                | AgentState::Dismissed
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRecord {
    pub name: String,
    /// Identity of this generation — the join key (`docs/2026-08-17-tkt-c1-generation-identity.md`).
    /// `None` on a pre-migration record; use [`AgentRecord::spawn_id`], never
    /// this field directly, so a reader always gets a stable id whether the
    /// record was minted with one or predates the type. Never rewritten into
    /// `agents.json` for a backfilled record — the accessor derives it fresh
    /// every load instead.
    #[serde(default)]
    pub spawn: Option<rk_core::id::SpawnId>,
    pub role: String,
    #[serde(default)]
    pub coordination: Option<Coordination>,
    pub harness: String,
    /// Effective permission mode used for this generation. Older records omit
    /// it and respawn falls back to the harness-specific worker default.
    #[serde(default)]
    pub permission_mode: Option<String>,
    /// Model requested at spawn (None = harness default; pricing then relies
    /// on harness-reported cost only).
    #[serde(default)]
    pub model: Option<String>,
    pub repo_root: PathBuf,
    pub repo_name: String,
    pub task: Option<String>,
    pub branch: Option<String>,
    /// Commit the branch was cut from. Paired with the branch head at handoff
    /// time, this proves the branch carried at least one commit even after a
    /// forge fast-forward makes target and branch refs identical.
    #[serde(default)]
    pub fork_point: Option<String>,
    pub worktree: Option<PathBuf>,
    /// Merge target on dismissal.
    pub target_branch: String,
    /// Spawning agent's name (None = spawned by a human).
    pub parent: Option<String>,
    /// Workflow instance that dispatched this agent (None = spawned outside a
    /// workflow, e.g. a bare `rk spawn`). Cost is summed per instance to
    /// enforce a workflow's `budget:` cap at dispatch time.
    #[serde(default)]
    pub workflow_instance: Option<String>,
    /// Exact candidate identity for a reviewer spawned by a review workflow.
    /// Persisted with the generation so respawn and artifact authorization
    /// retain the original binding.
    #[serde(default)]
    pub review: Option<ReviewContext>,
    /// Coordinator session owning the workflow that dispatched this agent.
    #[serde(default)]
    pub coordinator: Option<String>,
    pub session_id: Option<String>,
    /// herdr target when running attached in a pane (attach-mode spawn).
    #[serde(default)]
    pub attach_target: Option<String>,
    pub pid: Option<u32>,
    /// Merge commit the landing pipeline recorded for this agent's branch —
    /// the anchor `rk revert` revert-merges. Cleared once reverted, so a
    /// second revert errors instead of reverting the revert.
    #[serde(default)]
    pub merge_commit: Option<String>,
    pub state: AgentState,
    /// The process died (crashed, or was killed) while the agent was still
    /// live, so the harness never reported a verdict of its own — there is no
    /// `harness_result` for this generation and there never will be. Written
    /// once by the `Exited` handler over a live state; cleared by `respawn`,
    /// which starts a fresh attempt. See [`crashed_without_reporting`].
    ///
    /// [`crashed_without_reporting`]: AgentRecord::crashed_without_reporting
    #[serde(default)]
    pub crashed: bool,
    /// Bounded tail of this generation's harness stderr (see
    /// [`append_stderr_tail`]), kept alongside the record so a silent
    /// zero-token death still leaves a trace of why once the process is gone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_tail: Option<String>,
    pub result: Option<String>,
    /// Latest semantic checkpoint for this generation, if the agent has
    /// reported one. Stored with the registry so snapshots survive restart.
    #[serde(default)]
    pub progress: Option<AgentProgress>,
    pub usage: TokenUsage,
    pub cost_usd: f64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// When this record was moved to the archive store (`None` = live registry).
    /// Set by [`Registry::archive`] and cleared by [`Registry::unarchive`] —
    /// nothing else writes it, so it doubles as the "is this row archived?"
    /// marker in every rendered view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at: Option<DateTime<Utc>>,
    /// Durable liveness evidence for the supervisor's stuck sweep
    /// (`Supervisor::decide_sweep`), persisted alongside the rest of this
    /// generation's state so a daemon restart re-reads exactly what the
    /// previous process observed instead of granting a fresh grace window.
    /// See [`LivenessObservation`].
    #[serde(default)]
    pub liveness: LivenessObservation,
}

/// Evidence a stuck sweep uses to tell a genuinely wedged/dead generation
/// apart from one that is silent on the harness event stream but still doing
/// real work underneath it — a live verifier descendant (`cargo test`, an
/// `rk verify` CLI call blocked on the daemon), or bounded stdout/stderr/tool
/// output that is still advancing. Written outside of
/// [`Registry::update`](Registry::update) (via
/// [`Registry::update_quiet`](Registry::update_quiet)) so recording it never
/// itself resets the very silence clock (`AgentRecord::updated_at`) the sweep
/// measures — this can only ever EXCUSE a generation already past that bar,
/// never move the bar itself.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LivenessObservation {
    /// Session token ([`Supervisor`](crate::supervisor::Supervisor)'s
    /// `session_tokens`) this observation was recorded against. A respawn
    /// mints a fresh token for the same agent name/generation
    /// (`AgentRecord::spawn`/`created_at` is deliberately reused —
    /// see `session_tokens`' own doc comment), so a mismatch here means this
    /// evidence describes a predecessor process and must be read as absent,
    /// never as current proof of life.
    #[serde(default)]
    pub session: Option<rk_core::id::SpawnId>,
    /// Fingerprint of the last bounded output event (assistant text, tool
    /// use, or stderr) observed for this session, and when it last actually
    /// CHANGED — distinct from `updated_at`, which every event that touches
    /// the registry bumps regardless of whether anything new was said.
    #[serde(default)]
    pub output_fingerprint: u64,
    #[serde(default)]
    pub output_changed_at: Option<DateTime<Utc>>,
    /// Harness transport `Retry` events observed for this session since the
    /// last real forward-progress event (assistant text, tool use, or
    /// nonzero usage). A burst of these with nothing else moving is a
    /// reconnect loop, not liveness — it must not, by itself, excuse a
    /// generation from the stuck sweep.
    #[serde(default)]
    pub reconnect_events: u32,
    /// When the CURRENT stuck episode was first observed unhealthy. `None`
    /// = not currently in one. Persisted so the sweep's kill-grace clock
    /// survives a daemon restart instead of resetting to a fresh window.
    #[serde(default)]
    pub ceiling_started_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentProgress {
    pub summary: String,
    pub next: Option<String>,
    pub status: String,
    pub revision: u64,
    pub updated_at: DateTime<Utc>,
}

impl AgentRecord {
    /// Whether this agent left its run *without the harness ever reporting a
    /// verdict* — killed, crashed, or cut off mid-task (TKT-147).
    ///
    /// This is the "did this rat actually run?" question a workflow has to ask
    /// before it acts on a result attributed to the rat. `harness_result` is
    /// emitted only from a harness `Completed` event, so for a record in this
    /// shape there is no result of its own anywhere in the space — anything a
    /// `wait` hands downstream for it came from somewhere else, and a chain
    /// that reports success off it is a silent no-op (TKT-146's failure mode).
    ///
    /// [`crashed`](AgentRecord::crashed) is the authoritative signal, set at
    /// the one seam that knows (`HarnessEvent::Exited` over a live state). The
    /// second arm re-derives it for records written before that flag existed:
    /// a terminal-but-not-`Completed` record with no session id and not one
    /// token burned never got off the ground.
    pub fn crashed_without_reporting(&self) -> bool {
        self.crashed
            || (!self.state.is_live()
                && self.state != AgentState::Completed
                && self.state != AgentState::Stopped
                && self.session_id.is_none()
                && self.usage.total() == 0)
    }

    /// A one-line snippet of [`stderr_tail`](Self::stderr_tail) — its last
    /// non-empty line, capped so it stays legible folded into a `result`
    /// string or an inbox row rather than reproducing the whole tail there.
    /// `None` if nothing was ever captured.
    pub fn stderr_snippet(&self) -> Option<String> {
        const MAX_SNIPPET: usize = 200;
        let tail = self.stderr_tail.as_deref()?;
        let line = tail.lines().rev().find(|l| !l.trim().is_empty())?;
        Some(match line.char_indices().nth(MAX_SNIPPET) {
            Some((idx, _)) => format!("{}…", &line[..idx]),
            None => line.to_string(),
        })
    }

    /// Identity of one *generation* of a name. Names are not recycled (see
    /// [`Registry::reserve_name`]), so this exists for the one case that can
    /// still put two rows under one name: the archive/persist crash window,
    /// where `created_at` tells the archived copy from the live one.
    fn generation(&self) -> (&str, DateTime<Utc>) {
        (self.name.as_str(), self.created_at)
    }

    /// This generation's [`rk_core::id::SpawnId`] — the join key a consumer
    /// should key on instead of `name` (§2.2 of the design doc). Records
    /// minted after the migration carry one; a `None` (pre-migration) record
    /// gets a deterministic synthetic id derived from `created_at`, computed
    /// fresh on every call rather than rewritten into storage — see
    /// [`rk_core::id::SpawnId::synthetic_for`].
    pub fn spawn_id(&self) -> rk_core::id::SpawnId {
        self.spawn
            .unwrap_or_else(|| rk_core::id::SpawnId::synthetic_for(self.created_at))
    }
}

/// Cap on [`AgentRecord::stderr_tail`]: a chatty or looping harness must never
/// grow the record without bound, so only the most recent slice survives.
const STDERR_TAIL_CAP_BYTES: usize = 64 * 1024;

/// Append one stderr line to a bounded tail, keeping only the most recent
/// [`STDERR_TAIL_CAP_BYTES`] and snapping forward to the next line boundary so
/// the kept text never opens mid-line — same discipline as
/// [`agent_log::trim_to_tail`](crate::agent_log), applied in memory instead of
/// on disk.
pub fn append_stderr_tail(tail: &mut Option<String>, line: &str) {
    let buf = tail.get_or_insert_with(String::new);
    if !buf.is_empty() {
        buf.push('\n');
    }
    buf.push_str(line);
    if buf.len() > STDERR_TAIL_CAP_BYTES {
        // `buf.len() - CAP` is a raw byte offset that can land mid-character;
        // walk forward to the next valid boundary before slicing.
        let mut start = buf.len() - STDERR_TAIL_CAP_BYTES;
        while !buf.is_char_boundary(start) {
            start += 1;
        }
        let boundary = buf[start..]
            .find('\n')
            .map(|i| start + i + 1)
            .unwrap_or(start);
        buf.replace_range(..boundary, "");
    }
}

/// Default archive file name, kept beside `agents.json` in the same home.
const ARCHIVE_FILE: &str = "agents-archive.json";

/// JSON-file-backed registry. All mutation goes through [`Registry::update`],
/// which persists synchronously — the file is the daemon's restart memory.
///
/// Terminal records can be moved out of the live map into a second, append-only
/// archive store (`agents-archive.json`) by [`archive`](Registry::archive).
/// Archived records are *preserved, not deleted*: they keep their full
/// usage/cost/lineage and stay reachable through
/// [`list_archived`](Registry::list_archived) / [`list_all`](Registry::list_all)
/// / [`get_any`](Registry::get_any), and can be restored with
/// [`unarchive`](Registry::unarchive). They just stop inflating the default
/// views (`rk list`, `rk top`, `rk inbox`). Archiving does NOT free the name:
/// see [`reserve_name`].
///
/// [`reserve_name`]: Registry::reserve_name
pub struct Registry {
    path: PathBuf,
    archive_path: PathBuf,
    agents: HashMap<String, AgentRecord>,
    /// Archived terminal records. A `Vec`, not a map: an entry is keyed by
    /// GENERATION (`name`, `created_at`), which lets an archived record and a
    /// live one of the same name coexist across the archive/persist crash
    /// window (see [`Registry::is_shadowed`]).
    archived: Vec<AgentRecord>,
    /// Names handed out by [`Registry::reserve_name`] but not yet [`insert`]ed.
    /// Closes the spawn window where a name is chosen but its worktree/record
    /// don't exist yet, so concurrent spawns can't collide on it. In-memory
    /// only — a daemon restart starts with a clean slate (any half-spawned
    /// rats are already gone).
    ///
    /// [`insert`]: Registry::insert
    reserved: HashSet<String>,
    /// Outstanding fleet-WIP admission reservations taken by
    /// [`try_reserve_wip`](Registry::try_reserve_wip) and not yet resolved by
    /// [`release_wip`](Registry::release_wip). Closes the same kind of window
    /// `reserved` closes for names, but for the fleet-wide concurrency
    /// ceiling: two callers (a drain refill and a workflow `spawn` step, or
    /// two of either) that each read the live count before either's spawn
    /// lands in the registry must not both admit against the same free slot.
    /// In-memory only, same rationale as `reserved`.
    wip_reservations: usize,
    /// Outstanding per-`(repo, lane)` admission reservations taken by
    /// [`try_reserve_lane_wip`](Registry::try_reserve_lane_wip) and not yet
    /// resolved by [`release_lane_wip`](Registry::release_lane_wip). Same
    /// TOCTOU-closing role as `wip_reservations`, but scoped to one repo's
    /// one lane instead of the whole fleet, so one repository's implementation
    /// burst can never register against another repository's — or against the
    /// review lane's — count. Keyed by [`Lane::key`]. In-memory only, same
    /// rationale as `reserved`.
    lane_reservations: HashMap<String, usize>,
    /// Durable path for [`lane_waiters`](Self::lane_waiters) —
    /// `lane_waiters.json` beside `agents.json`.
    lane_waiters_path: PathBuf,
    /// Durably-recorded, FIFO-ordered waiters for a saturated lane, one entry
    /// per distinct `(repo, lane, key)` currently refused admission. See
    /// [`LaneWaiter`] and [`Registry::try_reserve_lane_wip`].
    lane_waiters: Vec<LaneWaiter>,
}

/// One durably-recorded waiter for a saturated capacity lane
/// (TKT-01M0P2KM83Y4MD5QYETR3JCKF2) — created the first time `key` is refused
/// admission to `(repo, lane)`, cleared once that same key is finally
/// admitted (or evicted as stale). `key` is a caller-supplied identifier for
/// one logical unit of work (e.g. `workflow:<instance>:<task>` for a fan-out
/// spawn, `<role>:<task>` for drain/operator dispatch) that stays stable
/// across that caller's own retries — without that stability, a caller
/// polling every few hundred milliseconds would mint a fresh queue entry on
/// every attempt instead of holding its place in line. Persisted to
/// `lane_waiters.json` (same atomic-write discipline as `agents.json`), so a
/// daemon restart neither loses a waiter's place in line nor duplicates it:
/// idempotent because insertion is keyed on `(repo, lane, key)` and a repeat
/// insert is a no-op.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LaneWaiter {
    repo: String,
    lane_tag: String,
    key: String,
    /// When this waiter was FIRST refused — never updated again. What
    /// [`Registry::lane_wait_stats`] reports as wait age: "how long has this
    /// request been waiting", not "is it still alive".
    requested_at: DateTime<Utc>,
    /// Refreshed every time this SAME key is refused again (an active
    /// retry) — what [`Registry::evict_stale_lane_waiters`] checks staleness
    /// against, deliberately separate from `requested_at`: an actively
    /// retrying waiter must keep its place in line no matter how long it has
    /// been waiting in total, only an ABANDONED one (no retry seen in
    /// [`LANE_WAIT_STALE_SECS`]) should ever be evicted.
    last_seen: DateTime<Utc>,
}

/// How long a durably-recorded waiter may go without a retry before it is
/// treated as abandoned (crashed caller, cancelled ticket, dismissed workflow
/// instance) and evicted — otherwise a dead waiter that never returns to
/// retry would permanently jam admission for every other waiter behind it,
/// even once capacity is free. Checked against `last_seen`, not
/// `requested_at` — see [`LaneWaiter`].
const LANE_WAIT_STALE_SECS: i64 = 600;

/// How often a still-queued waiter's `last_seen` heartbeat is actually
/// written to disk. A caller commonly retries every ~250ms
/// ([`crate::workflow_exec::FLEET_CAPACITY_POLL`]); persisting on every one
/// of those retries would mean a disk write several times a second per
/// waiting request. Throttling to once per this interval keeps the on-disk
/// staleness clock accurate to within one interval of the truth — more than
/// precise enough against a 10-minute eviction window — while bounding write
/// volume to a small, constant rate regardless of retry frequency.
const LANE_WAIT_HEARTBEAT_PERSIST_SECS: i64 = 60;

const LANE_WAITERS_FILE: &str = "lane_waiters.json";

/// Which capacity lane a spawn admits against
/// (TKT-01M0P2KM83Y4MD5QYETR3JCKF2), distinct from the pre-existing
/// fleet-wide `[drain] max_wip` ceiling ([`Registry::try_reserve_wip`]): that
/// ceiling has no repo dimension at all, so one repository's implementation
/// burst can exhaust it for every other repository. A lane reservation is
/// always additionally scoped to one repository, and `Review` is a wholly
/// separate counter from `Implementation` so a saturated implementation lane
/// can never starve a reviewer's admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Lane {
    /// Every role except `"reviewer"` — drain, workflow fan-out, and operator
    /// `agent.spawn` dispatch all admit against this.
    Implementation,
    /// `role == "reviewer"` only.
    Review,
}

impl Lane {
    /// The lane a spawn with this role admits against.
    pub(crate) fn for_role(role: &str) -> Self {
        if role == "reviewer" {
            Lane::Review
        } else {
            Lane::Implementation
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Lane::Implementation => "implementation",
            Lane::Review => "review",
        }
    }

    /// NUL can't appear in a repo name, so this is an unambiguous joiner —
    /// same convention as `supervisor::MergeQueue::key`.
    fn key(self, repo: &str) -> String {
        format!("{repo}\u{0}{}", self.tag())
    }
}

impl Registry {
    pub fn load(path: &Path) -> rk_core::Result<Self> {
        let agents = if path.exists() {
            let data = std::fs::read_to_string(path)?;
            serde_json::from_str(&data)?
        } else {
            HashMap::new()
        };
        let archive_path = path.with_file_name(ARCHIVE_FILE);
        let archived: Vec<AgentRecord> = if archive_path.exists() {
            let data = std::fs::read_to_string(&archive_path)?;
            serde_json::from_str(&data)?
        } else {
            Vec::new()
        };
        let lane_waiters_path = path.with_file_name(LANE_WAITERS_FILE);
        let lane_waiters: Vec<LaneWaiter> = if lane_waiters_path.exists() {
            let data = std::fs::read_to_string(&lane_waiters_path)?;
            serde_json::from_str(&data)?
        } else {
            Vec::new()
        };
        Ok(Self {
            path: path.to_path_buf(),
            archive_path,
            agents,
            archived,
            reserved: HashSet::new(),
            wip_reservations: 0,
            lane_reservations: HashMap::new(),
            lane_waiters_path,
            lane_waiters,
        })
    }

    /// Atomically pick a free rat name and reserve it against concurrent
    /// spawns. Callers hold the registry lock across this call and the
    /// eventual [`insert`](Registry::insert) (which frees the reservation);
    /// if the spawn fails before then, call [`release_name`] to free it.
    ///
    /// Consults the live map, the ARCHIVE, and the outstanding reservations.
    /// A rat name is an identity key, not a slot: it is stamped into durable
    /// tuples (`harness_result`, `task_done`), agent logs, branches and
    /// worktree paths that outlive the record by design, so `next_name`'s
    /// contract is that names are never recycled.
    ///
    /// TKT-136 briefly narrowed this to the live map, on the theory that
    /// archiving should return a name to the pool. It cannot: archiving does
    /// not retract the records the name is stamped into. A recycled name made
    /// every reader that keys on it ambiguous across time, which is TKT-146
    /// (a workflow's `wait` matched a two-day-old namesake's result and
    /// dismissed a one-second-old rat) and TKT-138 (`rk log` appends a reused
    /// name onto its predecessor's transcript). The pool needs no relief
    /// anyway — TKT-102 made `next_name` unbounded via generation suffixes
    /// (`Whisker-2`, `Whisker-3`, …), so uniqueness is free.
    ///
    /// [`release_name`]: Registry::release_name
    pub fn reserve_name(&mut self) -> String {
        let taken: Vec<&str> = self
            .agents
            .keys()
            .chain(self.reserved.iter())
            .map(String::as_str)
            .chain(self.archived.iter().map(|a| a.name.as_str()))
            .collect();
        let name = rk_core::names::next_name(taken);
        self.reserved.insert(name.clone());
        name
    }

    /// Release a name reserved by [`reserve_name`](Registry::reserve_name)
    /// when a spawn fails before the record is inserted.
    pub fn release_name(&mut self, name: &str) {
        self.reserved.remove(name);
    }

    /// Live rows plus outstanding fleet-WIP reservations — what
    /// [`try_reserve_wip`](Registry::try_reserve_wip) checks a cap against.
    pub fn live_or_reserved_wip(&self) -> usize {
        self.agents.values().filter(|r| r.state.is_live()).count() + self.wip_reservations
    }

    /// Atomically check the fleet-WIP ceiling and reserve one slot in the
    /// same critical section — this method is always called with the
    /// registry lock held (a caller reaches it only through
    /// [`Supervisor::spawn`](crate::supervisor::Supervisor::spawn)), so two
    /// concurrent admitters can never both observe the same free slot before
    /// either's spawn becomes a live row.
    ///
    /// `cap == 0` means this caller does not enforce a ceiling (manual/
    /// operator spawns, sub-spawns) — always admits, and reserves nothing.
    /// Every path that calls this with `cap != 0` and gets back `true` must
    /// eventually call [`release_wip`](Registry::release_wip) with the same
    /// `cap` exactly once: either explicitly on a failure path, or
    /// immediately once the spawn's registry row goes live (from then on the
    /// live row itself carries the count, matching how `insert` also frees a
    /// name reservation).
    pub fn try_reserve_wip(&mut self, cap: usize) -> bool {
        if cap == 0 {
            return true;
        }
        if self.live_or_reserved_wip() >= cap {
            return false;
        }
        self.wip_reservations += 1;
        true
    }

    /// Release a reservation taken by [`try_reserve_wip`](Registry::try_reserve_wip).
    /// `cap` must match the value passed to the paired `try_reserve_wip` call
    /// (`cap == 0` reserved nothing, so releases nothing).
    pub fn release_wip(&mut self, cap: usize) {
        if cap == 0 {
            return;
        }
        self.wip_reservations = self.wip_reservations.saturating_sub(1);
    }

    /// Live rows plus outstanding reservations for one `(repo, lane)` pair —
    /// what [`try_reserve_lane_wip`](Registry::try_reserve_lane_wip) checks a
    /// cap against. Scoped by `record.repo_name` and, within that repo, by
    /// whether the role matches the lane ([`Lane::for_role`]), so this can
    /// never be inflated by another repository's agents or by the other lane's.
    pub(crate) fn live_or_reserved_lane_wip(&self, repo: &str, lane: Lane) -> usize {
        self.agents
            .values()
            .filter(|r| r.repo_name == repo && r.state.is_live() && Lane::for_role(&r.role) == lane)
            .count()
            + self
                .lane_reservations
                .get(&lane.key(repo))
                .copied()
                .unwrap_or(0)
    }

    /// Atomically check one repository's lane ceiling AND its durable FIFO
    /// wait queue, and reserve one slot, in the same critical section — same
    /// atomicity contract as [`try_reserve_wip`](Registry::try_reserve_wip)
    /// (always called with the registry lock held, from inside
    /// [`Supervisor::spawn`](crate::supervisor::Supervisor::spawn)), but scoped
    /// to `(repo, lane)` instead of the whole fleet. `cap == 0` means this
    /// lane is unbounded for `repo` — always admits, reserves nothing, and
    /// never touches the wait queue.
    ///
    /// `key` identifies the caller's own logical request (stable across ITS
    /// OWN retries — see [`LaneWaiter`]). A free slot admits immediately when
    /// the wait queue for `(repo, lane)` is empty or `key` is already at its
    /// head (this caller has been waiting longest); otherwise it is held for
    /// whoever IS at the head, and this call is refused and durably queued
    /// behind them — real FIFO admission order, not just FIFO reporting,
    /// enforced for free by reusing the one lock every admission decision
    /// already serializes on.
    ///
    /// Every path that calls this with `cap != 0` and gets back `true` must
    /// eventually call [`release_lane_wip`](Registry::release_lane_wip) with
    /// the same `repo`/`lane`/`cap` exactly once, mirroring `try_reserve_wip`'s
    /// contract.
    pub(crate) fn try_reserve_lane_wip(
        &mut self,
        repo: &str,
        lane: Lane,
        cap: usize,
        key: &str,
    ) -> bool {
        if cap == 0 {
            return true;
        }
        self.evict_stale_lane_waiters(repo, lane);
        let blocked_behind_someone_else = self
            .lane_waiters
            .iter()
            .find(|w| w.repo == repo && w.lane_tag == lane.tag())
            .is_some_and(|head| head.key != key);
        if blocked_behind_someone_else || self.live_or_reserved_lane_wip(repo, lane) >= cap {
            self.record_lane_wait(repo, lane, key);
            return false;
        }
        // Fail CLOSED, not open, if this admission's matching queue-clear
        // cannot be made durable: admitting anyway while the on-disk queue
        // still names `key` as (or near) the head risks a phantom waiter
        // that outlives its own admission, wrongly blocking every real
        // waiter behind it until it ages out after `LANE_WAIT_STALE_SECS`.
        // Refusing here costs this one attempt (the in-memory queue is
        // untouched, so the very next retry gets an identical, correct
        // decision) — a bounded cost, unlike silently violating the
        // durable-ordering claim this whole mechanism exists to make.
        if let Err(e) = self.clear_lane_wait(repo, lane, key) {
            tracing::error!(
                repo,
                lane = lane.tag(),
                key,
                error = %e,
                "refusing admission: could not durably clear this waiter's lane_waiters.json \
                 record"
            );
            return false;
        }
        *self.lane_reservations.entry(lane.key(repo)).or_insert(0) += 1;
        true
    }

    /// Release a reservation taken by
    /// [`try_reserve_lane_wip`](Registry::try_reserve_lane_wip). `repo`/`lane`/
    /// `cap` must match the paired call (`cap == 0` reserved nothing, so
    /// releases nothing).
    pub(crate) fn release_lane_wip(&mut self, repo: &str, lane: Lane, cap: usize) {
        if cap == 0 {
            return;
        }
        if let Some(count) = self.lane_reservations.get_mut(&lane.key(repo)) {
            *count = count.saturating_sub(1);
        }
    }

    /// Durably record that `key` was refused admission to `(repo, lane)`. A
    /// repeat refusal for a key already queued is treated as a liveness
    /// heartbeat: `last_seen` is refreshed and persisted at most once every
    /// [`LANE_WAIT_HEARTBEAT_PERSIST_SECS`], to bound write volume
    /// under a caller that retries every ~250ms) instead of minting a second
    /// entry — the idempotence the durable-queue contract requires. A write
    /// failure here is logged but does not change this call's own outcome:
    /// the refusal was already correct regardless of durability, and the
    /// in-memory queue (which the very next admission decision reads) is
    /// unaffected either way — only cross-restart durability is at risk,
    /// which the log line surfaces rather than swallows.
    fn record_lane_wait(&mut self, repo: &str, lane: Lane, key: &str) {
        self.record_lane_wait_at(repo, lane, key, Utc::now());
    }

    /// Clock-injected implementation of [`record_lane_wait`](Self::record_lane_wait).
    /// Keeping `last_seen` at the last successfully persisted heartbeat is
    /// intentional: comparing against a timestamp refreshed on every 250ms
    /// retry would prevent the 60-second persistence threshold from ever
    /// being reached.
    fn record_lane_wait_at(&mut self, repo: &str, lane: Lane, key: &str, now: DateTime<Utc>) {
        if let Some(index) = self
            .lane_waiters
            .iter()
            .position(|w| w.repo == repo && w.lane_tag == lane.tag() && w.key == key)
        {
            if (now - self.lane_waiters[index].last_seen).num_seconds()
                < LANE_WAIT_HEARTBEAT_PERSIST_SECS
            {
                return;
            }
            let previous = self.lane_waiters[index].last_seen;
            self.lane_waiters[index].last_seen = now;
            if let Err(e) = self.persist_lane_waiters() {
                // Keep the old persisted timestamp in memory so the next
                // retry attempts the durable heartbeat again immediately.
                self.lane_waiters[index].last_seen = previous;
                tracing::error!(
                    repo, lane = lane.tag(), key, error = %e,
                    "failed to persist a lane wait heartbeat; retrying on the next refusal"
                );
            }
            return;
        }

        self.lane_waiters.push(LaneWaiter {
            repo: repo.to_string(),
            lane_tag: lane.tag().to_string(),
            key: key.to_string(),
            requested_at: now,
            last_seen: now,
        });
        if let Err(e) = self.persist_lane_waiters() {
            // Force the next retry to attempt persistence again rather than
            // waiting a full heartbeat interval after this failed first write.
            if let Some(inserted) = self.lane_waiters.last_mut() {
                inserted.last_seen = now
                    - chrono::TimeDelta::try_seconds(LANE_WAIT_HEARTBEAT_PERSIST_SECS)
                        .expect("positive heartbeat interval");
            }
            tracing::error!(
                repo, lane = lane.tag(), key, error = %e,
                "failed to persist a new lane wait record; retrying on the next refusal"
            );
        }
    }

    /// Clear `key`'s durable wait record for `(repo, lane)`, if any — a no-op
    /// (and cheap: no write, `Ok`) for the common case of a spawn admitted on
    /// its first attempt, which was never queued at all. Propagates a write
    /// failure to the caller rather than swallowing it: see
    /// [`try_reserve_lane_wip`](Self::try_reserve_lane_wip)'s fail-closed
    /// handling of this specific error.
    fn clear_lane_wait(&mut self, repo: &str, lane: Lane, key: &str) -> rk_core::Result<()> {
        let before = self.lane_waiters.len();
        self.lane_waiters
            .retain(|w| !(w.repo == repo && w.lane_tag == lane.tag() && w.key == key));
        if self.lane_waiters.len() != before {
            self.persist_lane_waiters()?;
        }
        Ok(())
    }

    /// Evict any waiter on `(repo, lane)` that has gone [`LANE_WAIT_STALE_SECS`]
    /// without a retry (`last_seen`, not `requested_at` — an actively
    /// retrying waiter keeps its place no matter how long it has waited in
    /// total) — an abandoned caller (crashed, ticket cancelled, workflow
    /// instance dismissed) that will never retry again must not permanently
    /// jam admission for everyone behind it.
    fn evict_stale_lane_waiters(&mut self, repo: &str, lane: Lane) {
        let now = Utc::now();
        let before = self.lane_waiters.len();
        self.lane_waiters.retain(|w| {
            !(w.repo == repo
                && w.lane_tag == lane.tag()
                && (now - w.last_seen).num_seconds() > LANE_WAIT_STALE_SECS)
        });
        if self.lane_waiters.len() != before {
            if let Err(e) = self.persist_lane_waiters() {
                tracing::error!(
                    repo, lane = lane.tag(), error = %e,
                    "failed to persist lane_waiters.json after evicting stale waiters"
                );
            }
        }
    }

    /// How many distinct requests are currently durably waiting on `(repo,
    /// lane)`, and the age in seconds of the oldest one — what
    /// `Supervisor::capacity_summary` surfaces as `waiting_count`/
    /// `oldest_wait_secs` for `rk top`/`rk status`/`rk digest`.
    pub(crate) fn lane_wait_stats(&self, repo: &str, lane: Lane) -> (usize, Option<i64>) {
        let now = Utc::now();
        let mut count = 0usize;
        let mut oldest_secs: Option<i64> = None;
        for w in &self.lane_waiters {
            if w.repo == repo && w.lane_tag == lane.tag() {
                count += 1;
                let age = (now - w.requested_at).num_seconds().max(0);
                oldest_secs = Some(oldest_secs.map_or(age, |o: i64| o.max(age)));
            }
        }
        (count, oldest_secs)
    }

    fn persist_lane_waiters(&self) -> rk_core::Result<()> {
        write_atomic(
            &self.lane_waiters_path,
            &serde_json::to_vec_pretty(&self.lane_waiters)?,
        )
    }

    /// Mark all live agents orphaned (called once at daemon startup).
    pub fn orphan_live_agents(&mut self) -> rk_core::Result<Vec<String>> {
        let mut orphaned = Vec::new();
        for record in self.agents.values_mut() {
            if record.state.is_live() {
                record.state = AgentState::Orphaned;
                record.pid = None;
                record.updated_at = Utc::now();
                orphaned.push(record.name.clone());
            }
        }
        self.persist()?;
        Ok(orphaned)
    }

    pub fn insert(&mut self, record: AgentRecord) -> rk_core::Result<()> {
        self.reserved.remove(&record.name);
        self.agents.insert(record.name.clone(), record);
        self.persist()
    }

    pub fn get(&self, name: &str) -> Option<&AgentRecord> {
        self.agents.get(name)
    }

    /// Live record for `name`, falling back to the newest archived generation.
    ///
    /// Read-only callers (`rk status`) use this so an archived rat's history
    /// stays queryable. Every MUTATING path (dismiss/revert/respawn/steer)
    /// deliberately keeps using [`get`](Registry::get), so an archived record
    /// reads as "no such agent" until it is explicitly unarchived.
    pub fn get_any(&self, name: &str) -> Option<&AgentRecord> {
        self.agents.get(name).or_else(|| {
            self.archived
                .iter()
                .filter(|a| a.name == name)
                .max_by_key(|a| a.created_at)
        })
    }

    /// Every record — live or archived — that ever carried `name`, oldest
    /// first: the full row, not just its `created_at`, so a caller can read
    /// [`AgentRecord::spawn_id`] off each generation instead of the bare
    /// timestamp [`generations_of`](Registry::generations_of) exposes.
    ///
    /// Normally one entry — a name is an identity key and
    /// [`reserve_name`](Registry::reserve_name) never recycles. Two entries mean
    /// the name genuinely named two rats, which the TKT-136 archiving window did
    /// to 24 names before TKT-146 closed it. Anything keyed on a name alone is
    /// ambiguous for exactly those names, so callers that must disambiguate
    /// (`rk log`) ask here instead of assuming.
    ///
    /// Deduped by `created_at`: the archive/persist crash window can leave one
    /// generation in both stores, and that is one rat, not two.
    pub fn records_of(&self, name: &str) -> Vec<&AgentRecord> {
        let mut records: Vec<&AgentRecord> = self
            .agents
            .get(name)
            .into_iter()
            .chain(self.archived.iter().filter(|a| a.name == name))
            .collect();
        records.sort_by_key(|a| a.created_at);
        records.dedup_by_key(|a| a.created_at);
        records
    }

    /// Every generation of `name` that ever existed, oldest first: the
    /// `created_at` of each row [`records_of`](Registry::records_of) returns.
    pub fn generations_of(&self, name: &str) -> Vec<DateTime<Utc>> {
        self.records_of(name)
            .into_iter()
            .map(|a| a.created_at)
            .collect()
    }

    /// The live registry, oldest first. Archived records are excluded — this is
    /// the default view every operator surface renders.
    pub fn list(&self) -> Vec<&AgentRecord> {
        let mut all: Vec<_> = self.agents.values().collect();
        all.sort_by_key(|a| a.created_at);
        all
    }

    /// Archived records only, oldest first.
    pub fn list_archived(&self) -> Vec<&AgentRecord> {
        let mut all: Vec<_> = self
            .archived
            .iter()
            .filter(|a| !self.is_shadowed(a))
            .collect();
        all.sort_by_key(|a| a.created_at);
        all
    }

    /// Live + archived, oldest first — the full lifetime history. Cost/usage
    /// rollups read this so archiving never moves a dollar figure.
    pub fn list_all(&self) -> Vec<&AgentRecord> {
        let mut all: Vec<_> = self.agents.values().collect();
        all.extend(self.archived.iter().filter(|a| !self.is_shadowed(a)));
        all.sort_by_key(|a| a.created_at);
        all
    }

    /// Terminal records eligible for archiving: state in
    /// {Completed, Failed, Dismissed} and last touched strictly before
    /// `before`. Live and `Orphaned` records are never eligible.
    pub fn archivable(&self, before: DateTime<Utc>) -> Vec<&AgentRecord> {
        let mut eligible: Vec<_> = self
            .agents
            .values()
            .filter(|a| a.state.is_archivable() && a.updated_at < before)
            .collect();
        eligible.sort_by_key(|a| a.created_at);
        eligible
    }

    /// Every still-live (not yet archived) record in a settled terminal state
    /// (Completed/Failed/Dismissed), with no age cutoff — the artifact-reap
    /// counterpart to [`archivable`](Self::archivable). `archivable` gates on
    /// `updated_at < before` because it decides eligibility for the archive
    /// move itself; a build-artifact reclaim has no such question to ask —
    /// a `target/` dir is exactly as regenerable one second after the agent
    /// settled as it is `after_days` later.
    pub fn terminal(&self) -> Vec<&AgentRecord> {
        self.agents
            .values()
            .filter(|a| a.state.is_archivable())
            .collect()
    }

    /// Move every [`archivable`](Registry::archivable) record into the archive
    /// store, returning the records as archived (with `archived_at` stamped).
    ///
    /// Writes the archive file BEFORE the live file: a crash between the two
    /// leaves the record in both stores, and [`is_shadowed`] resolves that in
    /// favour of the live copy — i.e. the archive silently no-ops rather than
    /// losing a record.
    ///
    /// [`is_shadowed`]: Registry::is_shadowed
    pub fn archive(&mut self, before: DateTime<Utc>) -> rk_core::Result<Vec<AgentRecord>> {
        let names: Vec<String> = self
            .archivable(before)
            .into_iter()
            .map(|a| a.name.clone())
            .collect();
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let now = Utc::now();
        let mut moved = Vec::new();
        for name in &names {
            let Some(record) = self.agents.get(name).cloned() else {
                continue;
            };
            let mut record = record;
            record.archived_at = Some(now);
            // Idempotent on the crash window above: a generation already in the
            // archive is not appended twice.
            if !self
                .archived
                .iter()
                .any(|a| a.generation() == record.generation())
            {
                self.archived.push(record.clone());
            }
            moved.push(record);
        }
        self.persist_archive()?;
        for record in &moved {
            self.agents.remove(&record.name);
        }
        self.persist()?;
        Ok(moved)
    }

    /// Restore the newest archived generation of `name` to the live registry.
    ///
    /// Errors if a live record already holds the name (archiving frees names
    /// for reuse, so this is a real collision, not a no-op).
    pub fn unarchive(&mut self, name: &str) -> rk_core::Result<Option<AgentRecord>> {
        if self.agents.contains_key(name) {
            return Err(rk_core::Error::other(format!(
                "cannot unarchive {name}: a live agent already holds that name"
            )));
        }
        let Some(index) = self
            .archived
            .iter()
            .enumerate()
            .filter(|(_, a)| a.name == name)
            .max_by_key(|(_, a)| a.created_at)
            .map(|(i, _)| i)
        else {
            return Ok(None);
        };
        let mut record = self.archived[index].clone();
        record.archived_at = None;
        self.agents.insert(name.to_string(), record.clone());
        // Live file first: a crash before the archive rewrite leaves the record
        // in both stores, where the live copy wins — never in neither.
        self.persist()?;
        self.archived.remove(index);
        self.persist_archive()?;
        Ok(Some(record))
    }

    /// Whether an archived entry is superseded by a live record of the same
    /// generation — the reconciliation rule for the crash window in
    /// [`archive`](Registry::archive) / [`unarchive`](Registry::unarchive).
    fn is_shadowed(&self, archived: &AgentRecord) -> bool {
        self.agents
            .get(&archived.name)
            .is_some_and(|live| live.generation() == archived.generation())
    }

    pub fn update<F>(&mut self, name: &str, mutate: F) -> rk_core::Result<Option<AgentRecord>>
    where
        F: FnOnce(&mut AgentRecord),
    {
        let Some(record) = self.agents.get_mut(name) else {
            return Ok(None);
        };
        mutate(record);
        record.updated_at = Utc::now();
        let snapshot = record.clone();
        self.persist()?;
        Ok(Some(snapshot))
    }

    /// Like [`update`](Self::update), but never touches `updated_at`, and
    /// only persists to disk when `mutate` reports it actually changed
    /// something (returns `true`).
    ///
    /// The stuck sweep's silence clock IS `updated_at`
    /// (`Supervisor::decide_sweep`) — recording supplementary liveness
    /// evidence (a bounded-output fingerprint, a reconnect-loop counter, the
    /// sweep's own persisted grace ceiling) through the bumping `update`
    /// would make observing that evidence itself reset the very silence it
    /// is meant to explain, silently widening the stuck bar for every write.
    /// The conditional persist matters for the same reason `resume_if_paused`
    /// avoids the registry entirely on `AssistantText`/`ToolUse`: those are
    /// the chattiest event paths in the harness pump, and this is the seam
    /// that now observes them for liveness evidence — an unconditional write
    /// would turn every token into a disk write.
    pub fn update_quiet<F>(&mut self, name: &str, mutate: F) -> rk_core::Result<Option<AgentRecord>>
    where
        F: FnOnce(&mut AgentRecord) -> bool,
    {
        let Some(record) = self.agents.get_mut(name) else {
            return Ok(None);
        };
        let changed = mutate(record);
        let snapshot = record.clone();
        if changed {
            self.persist()?;
        }
        Ok(Some(snapshot))
    }

    pub fn remove(&mut self, name: &str) -> rk_core::Result<Option<AgentRecord>> {
        let removed = self.agents.remove(name);
        if removed.is_some() {
            self.persist()?;
        }
        Ok(removed)
    }

    fn persist(&self) -> rk_core::Result<()> {
        write_atomic(&self.path, &serde_json::to_vec_pretty(&self.agents)?)
    }

    fn persist_archive(&self) -> rk_core::Result<()> {
        write_atomic(
            &self.archive_path,
            &serde_json::to_vec_pretty(&self.archived)?,
        )
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> rk_core::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Resolve an `rk prune --before` spec into an absolute cutoff: records last
/// touched strictly before it are eligible.
///
/// Accepts a relative duration (`30m`, `24h`, `7d`, `2w`, `90s`) or an absolute
/// date (`2026-07-24`, or a full RFC3339 timestamp). A bare number is rejected
/// rather than guessed at — "7" meaning seconds when the operator meant days
/// would silently archive the whole registry.
pub fn cutoff_from_spec(spec: &str, now: DateTime<Utc>) -> rk_core::Result<DateTime<Utc>> {
    let s = spec.trim();
    let invalid = || {
        rk_core::Error::other(format!(
            "invalid time spec: {s} \
             (want a duration like 30m/24h/7d/2w, a date like 2026-07-24, or an RFC3339 timestamp)"
        ))
    };
    // Duration form. Match on the last CHAR (not byte) so a multibyte suffix
    // can't split mid-codepoint; the units are single-byte ASCII, so once one
    // matches, trimming one byte is a valid boundary.
    let unit = s.chars().last().and_then(|c| match c {
        's' => Some(1i64),
        'm' => Some(60),
        'h' => Some(3_600),
        'd' => Some(86_400),
        'w' => Some(604_800),
        _ => None,
    });
    if let Some(mult) = unit {
        let n: i64 = s[..s.len() - 1].trim().parse().map_err(|_| invalid())?;
        if n < 0 {
            return Err(invalid());
        }
        let delta = n
            .checked_mul(mult)
            .and_then(chrono::TimeDelta::try_seconds)
            .ok_or_else(invalid)?;
        return now.checked_sub_signed(delta).ok_or_else(invalid);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|d| d.and_utc())
        .ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &str, state: AgentState) -> AgentRecord {
        AgentRecord {
            name: name.into(),
            spawn: None,
            role: "rat".into(),
            coordination: None,
            harness: "fake".into(),
            permission_mode: None,
            model: None,
            repo_root: "/tmp/repo".into(),
            repo_name: "repo".into(),
            task: Some(".rk-1".into()),
            branch: Some(format!("rat/{name}/rk-1")),
            fork_point: None,
            worktree: Some(format!("/tmp/wt/{name}").into()),
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            review: None,
            coordinator: None,
            session_id: None,
            attach_target: None,
            pid: Some(1234),
            merge_commit: None,
            state,
            crashed: false,
            stderr_tail: None,
            result: None,
            progress: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            archived_at: None,
            liveness: Default::default(),
        }
    }

    /// A terminal record that went quiet `age_secs` ago.
    fn aged(name: &str, state: AgentState, age_secs: i64) -> AgentRecord {
        let mut r = record(name, state);
        r.created_at = Utc::now() - chrono::TimeDelta::try_seconds(age_secs).unwrap();
        r.updated_at = r.created_at;
        r
    }

    fn names(records: &[&AgentRecord]) -> Vec<String> {
        records.iter().map(|r| r.name.clone()).collect()
    }

    /// A rat that ran: the harness gave it a session and it burned tokens.
    fn ran(name: &str, state: AgentState) -> AgentRecord {
        let mut r = record(name, state);
        r.session_id = Some("sess-1".into());
        r.usage.input = 100;
        r.usage.output = 50;
        r
    }

    /// An actively retrying waiter must refresh its durable heartbeat even
    /// when every individual retry arrives sooner than the write-throttle
    /// interval. Otherwise updating the in-memory timestamp on every retry
    /// makes the interval perpetually zero and a restart later misclassifies
    /// the active request as abandoned.
    #[test]
    fn active_lane_waiter_persists_heartbeat_across_a_long_wait() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agents.json");
        let start = Utc::now() - chrono::TimeDelta::try_minutes(11).unwrap();
        {
            let mut reg = Registry::load(&path).unwrap();
            reg.record_lane_wait_at("repo", Lane::Implementation, "active", start);
            for elapsed_secs in (30..=660).step_by(30) {
                reg.record_lane_wait_at(
                    "repo",
                    Lane::Implementation,
                    "active",
                    start + chrono::TimeDelta::try_seconds(elapsed_secs).unwrap(),
                );
            }
        }

        let mut reloaded = Registry::load(&path).unwrap();
        assert_eq!(reloaded.lane_waiters.len(), 1);
        assert!(
            (Utc::now() - reloaded.lane_waiters[0].last_seen).num_seconds() < 5,
            "the on-disk heartbeat must advance while sub-minute retries continue"
        );
        assert!(
            (Utc::now() - reloaded.lane_waiters[0].requested_at).num_seconds()
                >= LANE_WAIT_STALE_SECS,
            "the request has waited longer than the stale threshold in total"
        );

        reloaded.evict_stale_lane_waiters("repo", Lane::Implementation);
        assert_eq!(
            reloaded.lane_waiters.len(),
            1,
            "restart recovery must retain an old but actively retrying waiter"
        );
    }

    /// TKT-147: the predicate a workflow gate leans on to tell a rat that
    /// produced a verdict from one that was killed before it produced anything.
    #[test]
    fn crashed_without_reporting_separates_a_real_run_from_a_kill() {
        // The authoritative signal, as `HarnessEvent::Exited` writes it.
        let mut killed = ran("Nibbles", AgentState::Failed);
        killed.crashed = true;
        killed.result = Some("process exited (code None) without completing".into());
        assert!(killed.crashed_without_reporting());

        // It survives a later dismiss — exactly the TKT-146 record shape
        // (state=dismissed, crash-shaped result), which used to sail through
        // an `evaluate` as if the rat had worked.
        let mut dismissed = killed.clone();
        dismissed.state = AgentState::Dismissed;
        assert!(dismissed.crashed_without_reporting());

        // Records written before the flag existed still read correctly: no
        // session and not one token means it never got off the ground.
        let legacy = record("Pip", AgentState::Failed);
        assert!(!legacy.crashed, "the fallback arm is what is under test");
        assert!(legacy.crashed_without_reporting());

        // A rat that ran and failed *through the harness* reported a verdict
        // of its own; the gate must not touch it.
        assert!(!ran("Squeak", AgentState::Failed).crashed_without_reporting());
        assert!(!ran("Gnaw", AgentState::Completed).crashed_without_reporting());
        assert!(!ran("Remy", AgentState::Dismissed).crashed_without_reporting());
        assert!(
            !record("Stop", AgentState::Stopped).crashed_without_reporting(),
            "a deliberate stop is terminal but is not a crash"
        );
        // Nor a live one that simply has not reported yet.
        assert!(!record("Twitch", AgentState::Running).crashed_without_reporting());
    }

    #[test]
    fn stderr_tail_keeps_only_the_most_recent_lines() {
        let mut tail = None;
        // Push well past the 64 KiB cap with distinguishable lines (~10 bytes
        // each including the separator, so 10,000 lines is comfortably over).
        for i in 0..10_000 {
            append_stderr_tail(&mut tail, &format!("line {i}"));
        }
        let tail = tail.unwrap();
        assert!(
            tail.len() <= STDERR_TAIL_CAP_BYTES,
            "tail should be capped, got {} bytes",
            tail.len()
        );
        assert!(tail.ends_with("line 9999"), "newest line survives");
        assert!(!tail.contains("line 0\n"), "oldest lines are dropped");
        // Never opens mid-line: the first surviving line is whole.
        assert!(!tail.starts_with("ine "), "trim snapped to a line boundary");
    }

    #[test]
    fn stderr_tail_truncation_never_splits_a_multibyte_codepoint() {
        let mut tail = None;
        // Multi-byte filler so a naive byte-offset cap would panic mid-char.
        let filler = "日本語のログ行です".repeat(50);
        for _ in 0..50 {
            append_stderr_tail(&mut tail, &filler);
        }
        // No panic is the assertion; a valid `String` proves the boundary held.
        assert!(tail.unwrap().len() <= STDERR_TAIL_CAP_BYTES + filler.len());
    }

    #[test]
    fn stderr_snippet_is_the_last_nonblank_line_capped_and_ellipsized() {
        let mut r = record("Pip", AgentState::Failed);
        assert!(r.stderr_snippet().is_none(), "nothing captured yet");

        r.stderr_tail = Some("first error\nsecond, worse error\n\n".into());
        assert_eq!(r.stderr_snippet().as_deref(), Some("second, worse error"));

        r.stderr_tail = Some("x".repeat(500));
        let snippet = r.stderr_snippet().unwrap();
        assert!(snippet.ends_with('…'));
        assert_eq!(snippet.chars().count(), 201, "200 chars plus the ellipsis");
    }

    #[test]
    fn registry_persists_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agents.json");
        {
            let mut reg = Registry::load(&path).unwrap();
            reg.insert(record("Whisker", AgentState::Running)).unwrap();
            reg.update("Whisker", |r| r.cost_usd = 1.25).unwrap();
        }
        let reg = Registry::load(&path).unwrap();
        assert_eq!(reg.get("Whisker").unwrap().cost_usd, 1.25);
    }

    #[test]
    fn reserve_name_is_unique_until_inserted_or_released() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agents.json");
        let mut reg = Registry::load(&path).unwrap();

        // Two reservations without an intervening insert must differ — this is
        // the concurrent-spawn window that used to hand out duplicates.
        let first = reg.reserve_name();
        let second = reg.reserve_name();
        assert_ne!(first, second);

        // Inserting the first frees its reservation but the name is now in the
        // registry, so it still won't be reused.
        reg.insert(record(&first, AgentState::Running)).unwrap();
        let third = reg.reserve_name();
        assert_ne!(third, first);
        assert_ne!(third, second);

        // Releasing a reservation (spawn failed) returns the name to the pool.
        reg.release_name(&second);
        reg.release_name(&third);
        let reused = reg.reserve_name();
        assert_eq!(reused, second, "released name should be picked first again");
    }

    #[test]
    fn orphaning_marks_only_live_agents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agents.json");
        let mut reg = Registry::load(&path).unwrap();
        reg.insert(record("Whisker", AgentState::Running)).unwrap();
        reg.insert(record("Nibbles", AgentState::Dismissed))
            .unwrap();

        let orphaned = reg.orphan_live_agents().unwrap();
        assert_eq!(orphaned, vec!["Whisker".to_string()]);
        assert_eq!(reg.get("Whisker").unwrap().state, AgentState::Orphaned);
        assert!(reg.get("Whisker").unwrap().pid.is_none());
        assert_eq!(reg.get("Nibbles").unwrap().state, AgentState::Dismissed);
    }

    /// The core guardrail: only settled terminal records leave the live view.
    #[test]
    fn archive_moves_only_terminal_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load(&dir.path().join("agents.json")).unwrap();
        for (name, state) in [
            ("Whisker", AgentState::Running),
            ("Nibbles", AgentState::Spawning),
            ("Scurry", AgentState::Orphaned),
            ("Crumb", AgentState::Completed),
            ("Gnaw", AgentState::Failed),
            ("Pip", AgentState::Dismissed),
        ] {
            reg.insert(aged(name, state, 3_600)).unwrap();
        }

        let moved = reg.archive(Utc::now()).unwrap();
        let mut moved_names: Vec<_> = moved.iter().map(|r| r.name.clone()).collect();
        moved_names.sort();
        assert_eq!(moved_names, vec!["Crumb", "Gnaw", "Pip"]);

        let mut live = names(&reg.list());
        live.sort();
        assert_eq!(
            live,
            vec!["Nibbles", "Scurry", "Whisker"],
            "live and orphaned records must never be archived"
        );
        assert!(moved.iter().all(|r| r.archived_at.is_some()));
    }

    #[test]
    fn archive_respects_the_before_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load(&dir.path().join("agents.json")).unwrap();
        reg.insert(aged("Crumb", AgentState::Completed, 8 * 86_400))
            .unwrap();
        reg.insert(aged("Pip", AgentState::Dismissed, 3_600))
            .unwrap();

        let week_ago = cutoff_from_spec("7d", Utc::now()).unwrap();
        assert_eq!(names(&reg.archivable(week_ago)), vec!["Crumb"]);

        let moved = reg.archive(week_ago).unwrap();
        assert_eq!(names(&moved.iter().collect::<Vec<_>>()), vec!["Crumb"]);
        assert_eq!(names(&reg.list()), vec!["Pip"], "the fresh record stays");
    }

    /// Archiving preserves history: the record survives with its cost/usage and
    /// lineage intact, is reloadable from disk, and round-trips back.
    #[test]
    fn archived_records_keep_history_and_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agents.json");
        {
            let mut reg = Registry::load(&path).unwrap();
            let mut rec = aged("Crumb", AgentState::Completed, 3_600);
            rec.cost_usd = 4.25;
            rec.usage.input = 1_000;
            rec.usage.output = 500;
            rec.parent = Some("Whisker".into());
            rec.workflow_instance = Some("wf-abc".into());
            reg.insert(rec).unwrap();
            reg.archive(Utc::now()).unwrap();
        }

        // Reload from disk: the archive is its own file beside agents.json.
        let mut reg = Registry::load(&path).unwrap();
        assert!(reg.list().is_empty(), "default view is clean");
        assert_eq!(names(&reg.list_archived()), vec!["Crumb"]);
        assert_eq!(names(&reg.list_all()), vec!["Crumb"]);

        let archived = reg.get_any("Crumb").unwrap();
        assert_eq!(archived.cost_usd, 4.25);
        assert_eq!(archived.usage.total(), 1_500);
        assert_eq!(archived.parent.as_deref(), Some("Whisker"));
        assert_eq!(archived.workflow_instance.as_deref(), Some("wf-abc"));
        assert!(archived.archived_at.is_some());
        assert!(
            reg.get("Crumb").is_none(),
            "mutating lookups must not see an archived record"
        );

        let restored = reg.unarchive("Crumb").unwrap().unwrap();
        assert!(restored.archived_at.is_none());
        assert_eq!(restored.cost_usd, 4.25);
        assert_eq!(names(&reg.list()), vec!["Crumb"]);
        assert!(reg.list_archived().is_empty());
    }

    /// TKT-146 regression at the naming layer. A rat name is an identity key
    /// stamped into durable tuples, logs and branches that outlive its record,
    /// so archiving must NOT return it to the pool — a recycled name makes
    /// every reader that keys on it ambiguous across time.
    #[test]
    fn archiving_never_recycles_the_name() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load(&dir.path().join("agents.json")).unwrap();
        let first = reg.reserve_name();
        reg.insert(aged(&first, AgentState::Dismissed, 3_600))
            .unwrap();
        assert_ne!(reg.reserve_name(), first, "a live name stays taken");

        reg.archive(Utc::now()).unwrap();
        assert_eq!(reg.list_archived().len(), 1);
        let next = reg.reserve_name();
        assert_ne!(
            next, first,
            "an archived name stays taken: it is still stamped into durable records"
        );

        // The reservation survives a reload — the archive file, not just the
        // in-memory copy, is what keeps the name spent.
        reg.insert(record(&next, AgentState::Running)).unwrap();
        let mut reloaded = Registry::load(&dir.path().join("agents.json")).unwrap();
        let third = reloaded.reserve_name();
        assert_ne!(third, first, "reload must not resurrect an archived name");
        assert_ne!(third, next, "reload must not resurrect a live name");
    }

    /// The archive is keyed by generation, not by name, so a record present in
    /// BOTH files (the archive-then-crash window, before `agents.json` was
    /// rewritten) resolves in favour of the live copy instead of double-counting.
    /// `reserve_name` can no longer produce this state; a crash still can.
    #[test]
    fn a_live_record_shadows_its_archived_twin() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load(&dir.path().join("agents.json")).unwrap();
        let name = reg.reserve_name();
        let generation = aged(&name, AgentState::Dismissed, 3_600);
        reg.insert(generation.clone()).unwrap();
        reg.archive(Utc::now()).unwrap();

        // Simulate the crash window: the archive was written but `agents.json`
        // was not, so the SAME generation is back in the live map on reload.
        let mut live = generation;
        live.state = AgentState::Running;
        live.archived_at = None;
        reg.insert(live).unwrap();
        assert!(reg.list_archived().is_empty(), "the live copy shadows it");
        assert_eq!(reg.list_all().len(), 1, "no double-count in the rollups");
        assert_eq!(reg.get_any(&name).unwrap().state, AgentState::Running);
        assert!(
            reg.unarchive(&name).is_err(),
            "unarchiving onto a live name must refuse rather than clobber"
        );
    }

    /// `generations_of` is what lets a name-keyed reader (`rk log`) disambiguate
    /// the 24 names that carried two rats during the TKT-136 archiving window.
    #[test]
    fn generations_of_reports_every_rat_that_carried_a_name() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load(&dir.path().join("agents.json")).unwrap();

        assert!(
            reg.generations_of("Gouda").is_empty(),
            "a name nobody has carried has no generations"
        );

        // The historical shape: an older rat archived out of the live map, and a
        // newer one under the same name still live.
        let older = aged("Gouda", AgentState::Dismissed, 2 * 86_400);
        let older_created = older.created_at;
        reg.insert(older).unwrap();
        assert_eq!(reg.generations_of("Gouda"), vec![older_created]);
        reg.archive(Utc::now()).unwrap();

        let newer = record("Gouda", AgentState::Running);
        let newer_created = newer.created_at;
        reg.insert(newer).unwrap();

        assert_eq!(
            reg.generations_of("Gouda"),
            vec![older_created, newer_created],
            "both rats, oldest first — the archived one is not lost behind the live one"
        );
        assert!(
            reg.generations_of("Brie").is_empty(),
            "and a namesake's generations never leak onto another name"
        );
    }

    /// One rat sitting in both stores (the archive-then-crash window) is one
    /// generation, not two — otherwise `rk log` would offer a phantom choice.
    #[test]
    fn generations_of_dedupes_a_record_present_in_both_stores() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load(&dir.path().join("agents.json")).unwrap();
        let generation = aged("Cheddar", AgentState::Dismissed, 3_600);
        let created = generation.created_at;
        reg.insert(generation.clone()).unwrap();
        reg.archive(Utc::now()).unwrap();
        reg.insert(generation).unwrap();

        assert_eq!(reg.generations_of("Cheddar"), vec![created]);
    }

    #[test]
    fn cutoff_from_spec_parses_durations_and_dates() {
        let now = DateTime::parse_from_rfc3339("2026-07-24T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            cutoff_from_spec("7d", now).unwrap().to_rfc3339(),
            "2026-07-17T12:00:00+00:00"
        );
        assert_eq!(
            cutoff_from_spec("2h", now).unwrap().to_rfc3339(),
            "2026-07-24T10:00:00+00:00"
        );
        assert_eq!(
            cutoff_from_spec(" 1w ", now).unwrap().to_rfc3339(),
            "2026-07-17T12:00:00+00:00"
        );
        assert_eq!(
            cutoff_from_spec("2026-07-01", now).unwrap().to_rfc3339(),
            "2026-07-01T00:00:00+00:00"
        );
        assert_eq!(
            cutoff_from_spec("2026-07-01T06:30:00Z", now)
                .unwrap()
                .to_rfc3339(),
            "2026-07-01T06:30:00+00:00"
        );
        // A bare number is ambiguous, and a multibyte suffix must not panic.
        assert!(cutoff_from_spec("7", now).is_err());
        assert!(cutoff_from_spec("-3d", now).is_err());
        assert!(cutoff_from_spec("5m²", now).is_err());
        assert!(cutoff_from_spec("", now).is_err());
    }
}
