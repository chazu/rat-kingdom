//! The daemon server: accepts NDJSON requests on a Unix socket and dispatches
//! them. Hosts the tuplespace; `space.watch` upgrades a connection to a
//! server-push event stream.

use crate::coordinator::CoordinatorFilter;
use crate::factory_events::FactoryEventFilter;
use crate::ingest_auth::{IngestEventParams, IngestStateParams, SourcePrincipal};
use crate::proto::{codes, Request, Response};
use chrono::{DateTime, Utc};
use rk_core::action::{
    ActionProposal, ApprovalGrant, ApprovalStatus, FactoryAction, ProductToCodeBlockedNode,
    ProductToCodeDispatchAction, ProductToCodeDispatchExecutionResult,
    ProductToCodeDispatchedWorkflow, ProductToCodeWorkflowDispatch, TicketGraphAppliedEdge,
    TicketGraphApplyAction, TicketGraphApplyExecutionResult, TicketGraphApplyPreconditions,
    WorkflowRunAction,
};
use rk_core::id::RecordId;
use rk_core::paths::Layout;
use rk_core::product_to_code::contracts::{InitiativeContract, TicketGraph};
use rk_core::sdlc::SignalSourcePrincipal;
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple, SYSTEM_SCOPE};
use rk_harness::ControlEnvelope;
use rk_space::{CoordinatorEvent, Space};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{signal, Signal, SignalKind};
use tokio::sync::{broadcast, watch};
use tracing::{debug, info, warn};

const GC_INTERVAL: Duration = Duration::from_secs(60);
// Default lifetime for a pheromone trail (claim / obstacle / need) written
// without an explicit TTL — the hard-TTL backstop for strength decay — lives in
// rk-core so daemon-internal trail writers (supervisor, syncer) age on the same
// clock as this RPC boundary.
use rk_core::tuple::{DEFAULT_TRAIL_TTL, MAX_TRAIL_TTL};
/// Ceiling for blocking reads so a lost client cannot pin a connection task
/// forever; clients requesting more get clamped.
const MAX_BLOCK: Duration = Duration::from_secs(3600);
const DEFAULT_BLOCK: Duration = Duration::from_secs(5);
const MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Ceiling for an RPC *response*, separate from [`MAX_FRAME_BYTES`] (which
/// guards inbound request lines). A response is daemon-authored, not
/// client-supplied, so it gets more headroom than the request-side DoS guard
/// — but it still needs a ceiling: an unbounded aggregation endpoint
/// (`agent.list` with `include_archived` grows with the whole fleet's
/// lifetime history) can otherwise serialize a reply bigger than
/// `write_json_line` will accept, which used to surface as a bare `io::Error`
/// that dropped the connection with no wire response at all (`protocol:
/// daemon closed connection` client-side, no diagnosis possible).
const MAX_RESPONSE_FRAME_BYTES: usize = 16 * 1024 * 1024;
const MAX_SCAN_TUPLES: usize = 10_000;
/// Inbox is an aggregation endpoint, so cap both its source histories and its
/// final response. Newest-first source scans preserve the current state of
/// event reducers when old history is truncated.
const MAX_INBOX_ITEMS: usize = 2_048;
/// The caller id a human at a terminal authenticates as. `Client` sends this
/// whenever `RK_AGENT` is unset, and an empty caller means the same thing.
const OPERATOR_ACTOR: &str = "operator";

type FactVoteKey = (String, String, String);
type FactVoteState = (DateTime<Utc>, RecordId, String);
type RequestClock = fn() -> DateTime<Utc>;

fn ingest_state_filter(params: &IngestStateParams) -> (Option<String>, Option<String>) {
    if let Some(alert_key) = &params.alert_key {
        return (
            Some("production_alert".into()),
            Some(format!(
                "{}:{}:{}",
                params.environment.as_deref().unwrap_or_default(),
                params.service.as_deref().unwrap_or_default(),
                alert_key
            )),
        );
    }
    if params.environment.is_some() || params.service.is_some() {
        return (
            Some("deployment".into()),
            Some(format!(
                "{}:{}",
                params.environment.as_deref().unwrap_or_default(),
                params.service.as_deref().unwrap_or_default()
            )),
        );
    }
    if let Some(repo) = &params.repo {
        let _ = repo;
        return (Some("ci".into()), None);
    }
    (None, None)
}

/// Who may close a ballot: its proposer, or the operator (TKT-184).
///
/// Withdrawal is destructive-in-effect and unretractable in practice — it
/// permanently suppresses a promotion — so it is gated to the two parties with
/// standing. The proposer, because pulling your own proposal is the ordinary
/// case and needs no ceremony. The operator, because they are the only party who
/// is always reachable, and the ballot's author is usually a rat that has been
/// dead for days by the time anyone decides the proposal is going nowhere;
/// author-only would mean the common case has no one who can act.
///
/// A peer rat is deliberately NOT permitted. Endorsement is the fleet's
/// mechanism for disagreeing with a proposal — you decline to endorse it — and
/// letting any rat close any other's ballot would make a norm program where one
/// dissenter beats three endorsers. Quorum is the vote; this is not a veto.
///
/// An empty caller is the operator: `Client` sends `operator` when `RK_AGENT` is
/// unset, and pre-auth/local callers arrive blank, which the rest of the server
/// already reads as the operator.
fn may_withdraw(caller: &str, proposer: &str) -> bool {
    caller.is_empty() || caller == OPERATOR_ACTOR || caller == proposer
}

/// Correlates one `verify.run` call with the connection handling it
/// (TKT-01M0PA6C5WYRWS757R1SS2F2GR): `serve_conn` computes this before
/// dispatch to watch for the connection dying mid-call, and
/// `handle_verify_run` computes the SAME value to register under, so the two
/// always agree without threading an extra value through `dispatch`. `req.id`
/// alone is caller-controlled and not guaranteed unique fleet-wide, but this
/// only ever needs to be unique among calls currently in flight, and the
/// wire protocol allows exactly one in-flight request per connection.
fn verify_request_key(caller: &str, id: &str) -> String {
    format!("{caller}#{id}")
}

pub struct Daemon {
    layout: Layout,
    space: Space,
    /// The wire identity: this castle's Ed25519 actor id (`castle-<hex>`). Every
    /// daemon-authored tuple's `instance`, the sync author, and arbitration key
    /// on it — NEVER the display alias (TKT-124).
    castle: String,
    /// Operator-facing display string for this castle: the `castle_name` alias if
    /// set, else `castle` verbatim. Presentation-only — used in `status`/logs.
    castle_display: String,
    supervisor: Arc<crate::supervisor::Supervisor>,
    syncer: Option<Arc<crate::sync::Syncer>>,
    sync_interval: Duration,
    reactor_config: rk_core::config::ReactorConfig,
    /// Operator-configured escalation push channels (`[[notify.sinks]]`), handed
    /// to the reactor's sink registry. Default (no tables) reproduces the
    /// historical single herdr sink.
    notify_config: rk_core::config::NotifyConfig,
    scheduler_config: rk_core::config::SchedulerConfig,
    sweep_config: rk_core::config::SupervisorConfig,
    review_sweep_config: rk_core::config::ReviewSweepConfig,
    worktree_sweep_config: rk_core::config::WorktreeSweepConfig,
    /// Retention for the landing pipeline's persistent gate worktrees
    /// (`<home>/gate-worktrees/<repo>/<target>`) — see
    /// `crate::landing::LandingPipeline::gate_worktree_sweep_once`.
    gate_worktree_sweep_config: rk_core::config::GateWorktreeSweepConfig,
    /// Staleness threshold for the `landing-queue-stalled` `rk inbox` row
    /// (probe O18). See `crate::inbox::stalled_landing_queue_rows`.
    landing_queue_config: rk_core::config::LandingQueueConfig,
    /// B2 re-notify sweep: how often an unacked `recovery-action` escalation
    /// re-pushes. See `crate::recovery::renotify_sweep`.
    recovery_sweep_config: rk_core::config::RecoverySweepConfig,
    /// B8 stale-`Running`-instance hard timeout sweep. See
    /// `crate::workflow_exec::WorkflowEngine::stale_timeout_sweep_once`.
    instance_timeout_sweep_config: rk_core::config::InstanceTimeoutSweepConfig,
    /// Shared B2 announce-helper state across every automated recovery source
    /// this daemon runs (today: the B8 instance-timeout sweep). MUST be a
    /// single long-lived instance, not one built per sweep call: its rate-cap
    /// bookkeeping is an in-memory rolling window keyed by `RecoveryAction::kind`,
    /// and a fresh `RecoveryAnnouncer` per call would silently reset that
    /// window every tick, defeating the cap.
    recovery_announcer: crate::recovery::RecoveryAnnouncer,
    /// B9 orphaned-ticket sweep (seam 5): reopen an `in_progress` ticket whose
    /// assignee has no live agent record. See `ticket_reopen_sweep_once`.
    ticket_reopen_sweep_config: rk_core::config::TicketReopenSweepConfig,
    drain_config: rk_core::config::DrainConfig,
    evaporation_decay: f64,
    ingest_config: rk_core::config::IngestConfig,
    global_agents: std::collections::HashMap<String, rk_workflow::AgentProfile>,
    tier_routing: rk_workflow::TierRouting,
    default_harness: String,
    /// When set, workflow `run` steps may only invoke repo-registered named
    /// checks; raw inline commands are refused (TKT-30, `[policy]`).
    require_named_checks: bool,
    require_approval_for_landing: bool,
    automated_landing_workflows: Vec<String>,
    /// Fleet-wide default merge mode a repo is registered with when `rk repo
    /// add` names no explicit `--merge-mode` (`[policy] default_merge_mode`).
    default_merge_mode: rk_core::config::MergeMode,
    allowed_target_branches: Vec<String>,
    auth_token: String,
    engine: std::sync::OnceLock<Arc<crate::workflow_exec::WorkflowEngine>>,
    landing: std::sync::OnceLock<Arc<crate::landing::LandingPipeline>>,
    repos: std::sync::Mutex<crate::repos::RepoRegistry>,
    onboarding_sessions: std::sync::Mutex<crate::onboarding_sessions::OnboardingSessions>,
    /// Serializes the Git apply/commit/verification recovery window. Session
    /// persistence supplies restart safety; this lock prevents concurrent
    /// operator retries in one daemon from racing the same worktree.
    onboarding_apply_lock: tokio::sync::Mutex<()>,
    /// Serializes one daemon's approved ticket-graph execution. Durable
    /// checkpoints and idempotent ticket coalesce keys provide restart safety;
    /// this lock prevents concurrent operator retries racing those checkpoints.
    ticket_graph_apply_lock: tokio::sync::Mutex<()>,
    action_approvals: crate::action_approval::ActionApprovalStore,
    /// TKT-01M0E8PN9C41BWECGNW0990R3J: the durable orchestrator lease store
    /// (one lease per repo scope) an `attention.decide` orchestrator-authority
    /// call must hold before it may act.
    orchestrator_lease: crate::orchestrator_lease::LeaseStore,
    /// The authority-ladder policy (`[policy]` in `config.toml`), built once
    /// at startup — see `crate::authority::AuthorityPolicy` for why nothing
    /// mutates this at runtime.
    authority_policy: crate::authority::AuthorityPolicy,
    tickets: Arc<crate::tickets::Tickets>,
    coordinator_sessions: std::sync::Mutex<crate::coordinator::CoordinatorSessions>,
    /// Serializes read/append cycles for one agent's effective fact vote.
    fact_vote_lock: std::sync::Mutex<()>,
    started: Instant,
    shutdown_tx: watch::Sender<bool>,
    request_clock: RequestClock,
}

#[derive(Debug, Default)]
struct PeerOrigin {
    pid_observed: bool,
    supervised_agents: std::collections::HashSet<String>,
}

impl Daemon {
    #[cfg(test)]
    pub(crate) fn space_handle(&self) -> Space {
        self.space.clone()
    }

    pub fn new(layout: Layout, config: &rk_core::config::Config) -> rk_core::Result<Self> {
        layout.ensure()?;
        let space = Space::open(&layout.db_path())?;
        let global_agents: HashMap<String, rk_workflow::AgentProfile> = config
            .agents
            .iter()
            .map(|(name, p)| {
                (
                    name.clone(),
                    rk_workflow::AgentProfile {
                        harness: p.harness.clone(),
                        model: p.model.clone(),
                        permission_mode: p.permission_mode.clone(),
                    },
                )
            })
            .collect();
        let default_agent = global_agents.get("default").cloned().unwrap_or_default();
        let budget = rk_ledger::Budget {
            max_usd: config.budget.max_usd,
            max_tokens: config.budget.max_tokens,
            warn_at: config.budget.warn_at,
        };
        let fleet_budget = rk_ledger::FleetBudget {
            fleet_max_usd: config.budget.fleet_max_usd,
            repo_max_usd: config.budget.repo_max_usd,
            warn_at: config.budget.warn_at,
        };
        // Castle identity: the wire id is ALWAYS the stable, authenticated actor
        // id derived from this castle's Ed25519 key (TKT-59) — it signs every
        // replicated op and keys arbitration. A configured `castle_name` is a
        // PRESENTATION-ONLY alias (TKT-124), applied only at render time; it must
        // never become the wire id, or it would leak into signed records.
        let actor = rk_core::identity::CastleIdentity::load_or_create(&layout.castle_key_path())?
            .actor()
            .to_string();
        let display =
            rk_core::identity::CastleDisplay::new(actor.clone(), config.castle_name.clone());
        let castle_display = display.own().to_string();
        let mut daemon = Self::with_space_and_default_agent(
            layout,
            actor,
            config.harness.default.clone(),
            default_agent,
            budget,
            fleet_budget,
            space,
        )?;
        daemon.castle_display = castle_display;
        daemon.global_agents = global_agents;
        daemon.tier_routing = rk_workflow::TierRouting {
            rules: config
                .tiers
                .rules
                .iter()
                .map(|r| rk_workflow::TierRule {
                    priority: r.priority.clone(),
                    label: r.label.clone(),
                    tier: r.tier.clone(),
                })
                .collect(),
        };
        daemon.reactor_config = config.reactor.clone();
        daemon.notify_config = config.notify.clone();
        daemon.scheduler_config = config.scheduler.clone();
        daemon.sweep_config = config.supervisor.clone();
        if let Some(msg) = daemon.sweep_config.review_timeout_warning() {
            warn!(%msg, "supervisor config: stuck-sweep/review-timeout ordering invariant violated");
        }
        daemon.review_sweep_config = config.review_sweep.clone();
        daemon.worktree_sweep_config = config.worktree_sweep.clone();
        daemon.gate_worktree_sweep_config = config.gate_worktree_sweep.clone();
        daemon.landing_queue_config = config.landing_queue.clone();
        daemon.recovery_sweep_config = config.recovery_sweep.clone();
        daemon.instance_timeout_sweep_config = config.instance_timeout_sweep.clone();
        daemon.ticket_reopen_sweep_config = config.ticket_reopen_sweep.clone();
        daemon
            .supervisor
            .set_min_free_disk_gb(config.disk.min_free_gb);
        daemon
            .supervisor
            .set_reviewer_max_usd(config.budget.reviewer_max_usd);
        daemon
            .supervisor
            .set_max_load_per_cpu(config.machine.max_load_per_cpu);
        daemon
            .supervisor
            .set_shared_cargo_target(config.disk.shared_cargo_target);
        daemon.supervisor.set_verification_admission_limits(
            config.policy.verification_admission_limit,
            config
                .policy
                .verification_admission_limit_by_repo
                .clone()
                .into_iter()
                .collect(),
        );
        daemon
            .supervisor
            .set_done_kill_grace_secs(config.supervisor.done_kill_grace_secs);
        daemon.supervisor.set_sinks(
            crate::reactor::sink_factory()
                .registry(config.notify.resolved(config.reactor.notify_escalations)),
        );
        daemon.drain_config = config.drain.clone();
        daemon.evaporation_decay = config.evaporation.decay;
        daemon.ingest_config = config.ingest.clone();
        daemon.require_named_checks = config.policy.require_named_checks;
        daemon.require_approval_for_landing = config.policy.require_approval_for_landing;
        daemon.automated_landing_workflows = config.policy.automated_landing_workflows.clone();
        daemon.default_merge_mode = config.policy.default_merge_mode;
        daemon.allowed_target_branches = config.policy.allowed_target_branches.clone();
        daemon.authority_policy = crate::authority::AuthorityPolicy::from_config(&config.policy)?;
        if config.sync.enabled {
            let syncer = crate::sync::Syncer::new(
                &daemon.layout,
                &daemon.castle,
                config.sync.remote_url.as_deref(),
            )?;
            daemon.syncer = Some(Arc::new(syncer));
            daemon.sync_interval = Duration::from_secs(config.sync.interval_secs.max(5));
        }
        Ok(daemon)
    }

    #[doc(hidden)]
    pub fn new_in_memory(layout: Layout, castle: String) -> rk_core::Result<Self> {
        let space = Space::open_in_memory()?;
        Self::with_space(
            layout,
            castle,
            "fake".into(),
            rk_ledger::Budget::default(),
            rk_ledger::FleetBudget::default(),
            space,
        )
    }

    #[doc(hidden)]
    pub fn with_space_for_tests(
        layout: Layout,
        castle: String,
        default_harness: String,
        budget: rk_ledger::Budget,
        space: Space,
    ) -> rk_core::Result<Self> {
        Self::with_space(
            layout,
            castle,
            default_harness,
            budget,
            rk_ledger::FleetBudget::default(),
            space,
        )
    }

    /// Like [`with_space_for_tests`] but with an explicit fleet/repo cap, for
    /// exercising the pre-dispatch wallet kill-switch.
    #[doc(hidden)]
    pub fn with_fleet_budget_for_tests(
        layout: Layout,
        castle: String,
        default_harness: String,
        budget: rk_ledger::Budget,
        fleet_budget: rk_ledger::FleetBudget,
        space: Space,
    ) -> rk_core::Result<Self> {
        Self::with_space(layout, castle, default_harness, budget, fleet_budget, space)
    }

    #[doc(hidden)]
    pub fn set_sweep_config(&mut self, cfg: rk_core::config::SupervisorConfig) {
        // Propagated to the supervisor's own atomic too (not just stored for
        // the periodic sweep tick): `done_kill_grace_secs` is read from the
        // event-handling path in real time, same reasoning as
        // `min_free_disk_gb` — see `Supervisor::schedule_done_kill`.
        self.supervisor
            .set_done_kill_grace_secs(cfg.done_kill_grace_secs);
        self.sweep_config = cfg;
    }

    #[doc(hidden)]
    pub fn set_require_named_checks(&mut self, v: bool) {
        self.require_named_checks = v;
    }

    /// Test-only equivalent of `Daemon::new`'s
    /// `AuthorityPolicy::from_config(&config.policy)` — an in-memory/bare
    /// test daemon has no `config.toml` to load, so a test that needs a
    /// non-default authority-ladder policy (e.g. an orchestrator action
    /// allowlist entry) sets it directly. Panics on an invalid policy
    /// (a widening override), the same as a real daemon would fail to start.
    #[doc(hidden)]
    pub fn set_authority_policy_for_tests(&mut self, cfg: &rk_core::config::PolicyConfig) {
        self.authority_policy = crate::authority::AuthorityPolicy::from_config(cfg)
            .expect("test authority policy config must be valid");
    }

    #[doc(hidden)]
    pub fn set_drain_config(&mut self, cfg: rk_core::config::DrainConfig) {
        self.drain_config = cfg;
    }

    #[doc(hidden)]
    pub fn set_review_sweep_config(&mut self, cfg: rk_core::config::ReviewSweepConfig) {
        self.review_sweep_config = cfg;
    }

    #[doc(hidden)]
    pub fn set_worktree_sweep_config(&mut self, cfg: rk_core::config::WorktreeSweepConfig) {
        self.worktree_sweep_config = cfg;
    }

    #[doc(hidden)]
    pub fn set_recovery_sweep_config(&mut self, cfg: rk_core::config::RecoverySweepConfig) {
        self.recovery_sweep_config = cfg;
    }

    #[doc(hidden)]
    pub fn set_instance_timeout_sweep_config(
        &mut self,
        cfg: rk_core::config::InstanceTimeoutSweepConfig,
    ) {
        self.instance_timeout_sweep_config = cfg;
    }

    #[doc(hidden)]
    pub fn set_ticket_reopen_sweep_config(
        &mut self,
        cfg: rk_core::config::TicketReopenSweepConfig,
    ) {
        self.ticket_reopen_sweep_config = cfg;
    }

    #[doc(hidden)]
    pub fn set_min_free_disk_gb(&self, gb: u64) {
        self.supervisor.set_min_free_disk_gb(gb);
    }

    #[doc(hidden)]
    pub fn set_shared_cargo_target(&self, enabled: bool) {
        self.supervisor.set_shared_cargo_target(enabled);
    }

    #[doc(hidden)]
    pub fn set_request_clock_for_tests(&mut self, clock: fn() -> DateTime<Utc>) {
        self.request_clock = clock;
    }

    fn with_space(
        layout: Layout,
        castle: String,
        default_harness: String,
        budget: rk_ledger::Budget,
        fleet_budget: rk_ledger::FleetBudget,
        space: Space,
    ) -> rk_core::Result<Self> {
        Self::with_space_and_default_agent(
            layout,
            castle,
            default_harness,
            rk_workflow::AgentProfile::default(),
            budget,
            fleet_budget,
            space,
        )
    }

    fn with_space_and_default_agent(
        layout: Layout,
        castle: String,
        default_harness: String,
        default_agent: rk_workflow::AgentProfile,
        budget: rk_ledger::Budget,
        fleet_budget: rk_ledger::FleetBudget,
        space: Space,
    ) -> rk_core::Result<Self> {
        layout.ensure()?;
        let auth_token = layout.auth_token()?;
        // One Tickets instance, shared by the RPC handlers and the supervisor,
        // so ticket-lifecycle writes serialize on a single lock.
        let tickets = Arc::new(crate::tickets::Tickets::new(space.clone(), castle.clone()));
        let supervisor = Arc::new(crate::supervisor::Supervisor::new_with_agent_defaults(
            layout.clone(),
            castle.clone(),
            crate::supervisor::AgentDefaults::new(default_harness.clone(), default_agent),
            budget,
            fleet_budget,
            space.clone(),
            tickets.clone(),
        )?);
        let (shutdown_tx, _) = watch::channel(false);
        let repos = std::sync::Mutex::new(crate::repos::RepoRegistry::load(
            &layout.home().join("repos.json"),
        )?);
        let onboarding_sessions =
            std::sync::Mutex::new(crate::onboarding_sessions::OnboardingSessions::load(
                &layout.home().join("onboarding-sessions.json"),
            )?);
        let coordinator_sessions =
            std::sync::Mutex::new(crate::coordinator::CoordinatorSessions::load(
                &layout.home().join("coordinator-sessions.json"),
            )?);
        let action_approvals = crate::action_approval::ActionApprovalStore::load(
            layout.home().join("factory-actions.json"),
        )?;
        let orchestrator_lease = crate::orchestrator_lease::LeaseStore::load(
            layout.home().join("orchestrator-lease.json"),
        )?;
        Ok(Self {
            layout,
            space,
            // Default the display to the wire id; Daemon::new overrides it with
            // the configured alias. Test constructors keep id == display.
            castle_display: castle.clone(),
            castle,
            supervisor,
            syncer: None,
            sync_interval: Duration::from_secs(30),
            reactor_config: rk_core::config::ReactorConfig::default(),
            notify_config: rk_core::config::NotifyConfig::default(),
            scheduler_config: rk_core::config::SchedulerConfig::default(),
            sweep_config: rk_core::config::SupervisorConfig::default(),
            review_sweep_config: rk_core::config::ReviewSweepConfig::default(),
            // Disabled by default for bare/test constructors (mirrors the
            // Supervisor-level `min_free_disk_gb` default of 0): only
            // `Daemon::new`'s config-loading path enables the periodic sweep,
            // the finalize-time cleanup guarantee, and the disk-floor guard,
            // so existing e2e tests built on `new_in_memory`/`with_space_*`
            // are unaffected. `finalize_cleanup_enabled` is disabled here too
            // (not just `enabled`, the periodic-timer switch) — left on
            // unconditionally it made every workflow-based e2e test do extra
            // synchronous git reclaim at finalize time, adding enough load
            // under a full parallel `cargo test --workspace` run to tip
            // unrelated tests' fixed polling timeouts over the edge (rework
            // of TKT-01M04N6W4X47KMXDA6MH0WPH8H).
            worktree_sweep_config: rk_core::config::WorktreeSweepConfig {
                enabled: false,
                finalize_cleanup_enabled: false,
                ..rk_core::config::WorktreeSweepConfig::default()
            },
            // Same reasoning as `worktree_sweep_config` above: a bare/test
            // constructor must not grow a new periodic background loop that
            // existing e2e tests never asked for. `Daemon::new`'s
            // config-loading path is the only one that turns it on.
            gate_worktree_sweep_config: rk_core::config::GateWorktreeSweepConfig {
                enabled: false,
                ..rk_core::config::GateWorktreeSweepConfig::default()
            },
            // No periodic loop to gate — this is a pure read-time threshold
            // (`crate::inbox::stalled_landing_queue_rows`), so unlike the
            // sweep configs above a bare/test constructor can take the real
            // default safely.
            landing_queue_config: rk_core::config::LandingQueueConfig::default(),
            // Same reasoning as `worktree_sweep_config` above: the default is
            // `enabled: true`, but a bare/test constructor must not grow a new
            // periodic background loop that existing e2e tests never asked
            // for. `Daemon::new`'s config-loading path is the only one that
            // turns it on.
            recovery_sweep_config: rk_core::config::RecoverySweepConfig {
                enabled: false,
                ..rk_core::config::RecoverySweepConfig::default()
            },
            // Same reasoning as `recovery_sweep_config` above: a bare/test
            // constructor must not grow a new periodic background loop that
            // existing e2e tests never asked for.
            instance_timeout_sweep_config: rk_core::config::InstanceTimeoutSweepConfig {
                enabled: false,
                ..rk_core::config::InstanceTimeoutSweepConfig::default()
            },
            recovery_announcer: crate::recovery::RecoveryAnnouncer::new(),
            // Same reasoning again: `Daemon::new`'s config-loading path is the
            // only one that turns the periodic loop on for a real deployment.
            ticket_reopen_sweep_config: rk_core::config::TicketReopenSweepConfig {
                enabled: false,
                ..rk_core::config::TicketReopenSweepConfig::default()
            },
            drain_config: rk_core::config::DrainConfig::default(),
            evaporation_decay: rk_core::config::EvaporationConfig::default().decay,
            ingest_config: rk_core::config::IngestConfig::default(),
            global_agents: Default::default(),
            tier_routing: Default::default(),
            default_harness,
            require_named_checks: false,
            require_approval_for_landing: true,
            automated_landing_workflows: rk_core::config::PolicyConfig::default()
                .automated_landing_workflows,
            default_merge_mode: rk_core::config::MergeMode::default(),
            allowed_target_branches: rk_core::config::PolicyConfig::default()
                .allowed_target_branches,
            auth_token,
            engine: std::sync::OnceLock::new(),
            landing: std::sync::OnceLock::new(),
            repos,
            onboarding_sessions,
            onboarding_apply_lock: tokio::sync::Mutex::new(()),
            ticket_graph_apply_lock: tokio::sync::Mutex::new(()),
            action_approvals,
            orchestrator_lease,
            authority_policy: crate::authority::AuthorityPolicy::default(),
            tickets,
            coordinator_sessions,
            fact_vote_lock: std::sync::Mutex::new(()),
            started: Instant::now(),
            shutdown_tx,
            request_clock: Utc::now,
        })
    }

    /// Bind the socket (clearing a stale one if the previous daemon died) and
    /// serve until a `stop` request or SIGTERM/SIGINT arrives.
    pub async fn run(self) -> rk_core::Result<()> {
        self.layout.ensure()?;
        // Singleton gate: must win this before touching the socket or any
        // other daemon state. Held for the rest of `run()` via this binding;
        // dropped (lock released) on return, including on crash/kill.
        let _singleton_lock = acquire_singleton_lock(&self.layout)?;
        let sock = self.layout.socket_path();

        if sock.exists() {
            if UnixStream::connect(&sock).await.is_ok() {
                return Err(rk_core::Error::other(format!(
                    "daemon already running on {}",
                    sock.display()
                )));
            }
            // Connect failure alone does not prove staleness — THIS process
            // may be sandboxed away from the socket. Only reclaim it if the
            // recorded owner pid is actually dead.
            if let Some(pid) = read_pid(&self.layout) {
                if process_alive(pid) {
                    return Err(rk_core::Error::other(format!(
                        "daemon pid {pid} appears alive but its socket is unreachable \
                         from this process (sandbox?) — refusing to clobber {}",
                        sock.display()
                    )));
                }
            }
            debug!(path = %sock.display(), "removing stale socket");
            std::fs::remove_file(&sock)?;
        }

        let listener = UnixListener::bind(&sock)?;
        // A Unix socket's mode is otherwise inherited from the process umask.
        // The token is the primary credential, but filesystem permissions are
        // the first and cheapest boundary for local clients.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&sock, std::fs::Permissions::from_mode(0o600))?;
        }
        std::fs::write(self.layout.pid_file(), std::process::id().to_string())?;
        info!(socket = %sock.display(), pid = std::process::id(), castle = %self.castle_display, "daemon listening");
        // Only now that the bind is won may shared state be touched.
        self.supervisor.on_daemon_started();
        match self.onboarding_sessions.lock() {
            Ok(mut sessions) => {
                if let Err(error) = sessions.orphan_nonterminal() {
                    warn!(%error, "failed to orphan onboarding sessions");
                }
            }
            Err(_) => warn!("onboarding session registry lock poisoned"),
        }

        let daemon = Arc::new(self);
        let mut shutdown_rx = daemon.shutdown_tx.subscribe();

        // Every long-running background loop below (GC, sweeps, sync, reactor,
        // landing, scheduler, drain) is spawned into this `JoinSet` rather than
        // detached via a bare `tokio::spawn`, for two reasons. First, it makes
        // graceful shutdown deterministic: `run()` does not return until
        // `join_all` below has observed every loop actually exit, instead of
        // racing its own return against loops that are still mid-`select!` on
        // the same `shutdown_tx` signal. Second — the property a same-process
        // test relies on — a `JoinSet` aborts every task it still holds when
        // it is dropped, so aborting the outer `run()` future (e.g. a test
        // doing `handle.abort()` to simulate a crash) now tears down this
        // whole task tree instead of leaving these loops running as orphans
        // that can race a second `Daemon` constructed over the same on-disk
        // home (TKT-01M0G2VXS8PQYZN2X3ZWXDFC5B). A real crash needs none of
        // this — the OS kills every task in the process at once regardless —
        // so this has no effect on production behavior.
        let mut background_tasks: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();

        // Restore every durable workflow id before any reactor or scheduler task
        // can dispatch. Resume execution only after those consumers are listening
        // so completion events remain observable without opening a duplicate-ID
        // launch window during startup.
        let resumable_workflows = daemon.engine().rehydrate();

        // GC loop: decay pheromone trails and collect faded/expired tuples —
        // escalation/analytics live elsewhere.
        {
            let space = daemon.space.clone();
            let decay = daemon.evaporation_decay;
            let mut gc_shutdown = daemon.shutdown_tx.subscribe();
            background_tasks.spawn(async move {
                let mut tick = tokio::time::interval(GC_INTERVAL);
                loop {
                    tokio::select! {
                        _ = tick.tick() => match space.gc_expired(decay) {
                            Ok(0) => {}
                            Ok(n) => debug!(collected = n, "gc collected faded/expired tuples"),
                            Err(e) => warn!(error = %e, "gc failed"),
                        },
                        _ = gc_shutdown.changed() => break,
                    }
                }
            });
        }

        // Supervisor liveness/burn-rate sweep: catch rats hung mid-tool-call
        // (silent, so budget checks never see them) or running cost away with no
        // completion. Graduated: obstacle + steer, then kill after a grace pass.
        if daemon.sweep_config.enabled {
            let supervisor = Arc::clone(&daemon.supervisor);
            let cfg = daemon.sweep_config.clone();
            // Same `[[notify.sinks]]` config the reactor's escalations and the
            // B2 re-notify sweep resolve (`recovery_renotify_sweep_once`) —
            // sinks are stateless shell-outs (B1), so rebuilding the registry
            // fresh each tick is cheap and keeps this loop independent of
            // `Server` internals.
            let notify_config = daemon.notify_config.clone();
            let notify_escalations = daemon.reactor_config.notify_escalations;
            let mut sweep_shutdown = daemon.shutdown_tx.subscribe();
            let interval = Duration::from_secs(cfg.interval_secs.max(1));
            background_tasks.spawn(async move {
                let mut tick = tokio::time::interval(interval);
                // Consume the immediate first tick so freshly-spawned rats get a
                // full interval of grace before the first sweep looks at them.
                tick.tick().await;
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            let supervisor = Arc::clone(&supervisor);
                            let cfg = cfg.clone();
                            let notify_config = notify_config.clone();
                            let handle = tokio::runtime::Handle::current();
                            if let Err(e) = tokio::task::spawn_blocking(move || {
                                let _entered = handle.enter();
                                supervisor.sweep(&cfg);
                                // Self-healing respawn rides the same tick (TKT-53):
                                // relaunch crashed/orphaned rats with crash-loop
                                // backoff, castle-wide rate-capped and announced
                                // (strategic review B3). No-op unless
                                // [supervisor].respawn_enabled.
                                let sinks = crate::reactor::sink_factory()
                                    .registry(notify_config.resolved(notify_escalations));
                                supervisor.respawn_sweep(&cfg, &sinks);
                            })
                            .await
                            {
                                warn!(error = %e, "supervisor sweep task failed");
                            }
                        }
                        _ = sweep_shutdown.changed() => break,
                    }
                }
            });
        }

        // Fetch-driven awaiting-review clear (TKT-70). Periodically fetch+prune
        // each repo with an open PR and check whether the forge merged/deleted
        // the branch upstream — clearing the inbox row for a merge the operator
        // never pulled. Off by default (fetch is network + can hang) and coarse;
        // the fetch stays here, off the hot inbox read path, which only reads the
        // `pull_request_closed` events this loop emits.
        if daemon.review_sweep_config.enabled {
            let daemon_ref = Arc::clone(&daemon);
            let mut rs_shutdown = daemon.shutdown_tx.subscribe();
            let interval = Duration::from_secs(daemon.review_sweep_config.interval_secs.max(1));
            background_tasks.spawn(async move {
                let mut tick = tokio::time::interval(interval);
                // Consume the immediate first tick: give a freshly-opened PR a
                // full interval before the first fetch, and don't fetch on boot.
                tick.tick().await;
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            let d = Arc::clone(&daemon_ref);
                            match tokio::task::spawn_blocking(move || d.review_sweep_once()).await {
                                Ok(0) => {}
                                Ok(n) => debug!(closed = n, "review sweep cleared awaiting-review rows"),
                                Err(e) => warn!(error = %e, "review sweep task panicked"),
                            }
                        }
                        _ = rs_shutdown.changed() => break,
                    }
                }
            });
        }

        // Periodic worktree-leak sweep (`[worktree_sweep]`, TKT-01M04N6W4X47KMXDA6MH0WPH8H):
        // the automated, unattended counterpart to `rk prune --reap-git`. A
        // steward/workflow failure path that skips its own `dismiss` step
        // leaves a terminal agent's worktree (and its multi-GB cargo
        // `target/`) on disk indefinitely; this loop reclaims those on a
        // timer instead of waiting for an operator to run `rk prune` by hand.
        // Enabled by default (unlike the other sweeps here) because every
        // removal it performs is already gated safe by `Supervisor::reap_git`
        // (branch merged-or-gone AND worktree clean, or the worktree is left
        // standing) — see the 2026-08-16 104-worktree/298GB incident this
        // closes the gap on.
        if daemon.worktree_sweep_config.enabled {
            let daemon_ref = Arc::clone(&daemon);
            let mut ws_shutdown = daemon.shutdown_tx.subscribe();
            let interval = Duration::from_secs(daemon.worktree_sweep_config.interval_secs.max(1));
            background_tasks.spawn(async move {
                let mut tick = tokio::time::interval(interval);
                // Consume the immediate first tick: give a freshly-terminal
                // agent a full `after_days` window before the first sweep.
                tick.tick().await;
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            let d = Arc::clone(&daemon_ref);
                            match tokio::task::spawn_blocking(move || d.worktree_sweep_once()).await {
                                Ok(0) => {}
                                Ok(n) => info!(reclaimed = n, "worktree sweep reclaimed leaked worktrees"),
                                Err(e) => warn!(error = %e, "worktree sweep task panicked"),
                            }
                        }
                        _ = ws_shutdown.changed() => break,
                    }
                }
            });
        }

        // Periodic gate-worktree retention sweep (`[gate_worktree_sweep]`):
        // the persistent per-`(repo,target)` daemon-owned worktrees the
        // landing pipeline gates against (`landing.rs` §2.2) have no
        // dismiss-time cleanup the way an agent worktree does, so nothing
        // else ever reclaims one — see
        // docs/proposals/daemon-native-landing-pipeline.md §5 open question
        // 4. Every reclaim this loop performs is gated the same way
        // `worktree_sweep_once` gates agent reclaims: skipped outright while
        // the `(repo, target)` key has any live `LandingQueue` entry.
        if daemon.gate_worktree_sweep_config.enabled {
            let daemon_ref = Arc::clone(&daemon);
            let mut gws_shutdown = daemon.shutdown_tx.subscribe();
            let interval =
                Duration::from_secs(daemon.gate_worktree_sweep_config.interval_secs.max(1));
            background_tasks.spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.tick().await;
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            let d = Arc::clone(&daemon_ref);
                            match tokio::task::spawn_blocking(move || d.gate_worktree_sweep_once()).await {
                                Ok(0) => {}
                                Ok(n) => info!(reclaimed = n, "gate worktree sweep reclaimed stale worktrees"),
                                Err(e) => warn!(error = %e, "gate worktree sweep task panicked"),
                            }
                        }
                        _ = gws_shutdown.changed() => break,
                    }
                }
            });
        }

        // B2 re-notify sweep: an unacked `recovery-action` escalation
        // re-pushes at `first_renotify_secs`, then every
        // `repeat_renotify_secs`, up to `max_renotifies` times — after which
        // it stands as a passive `rk inbox` row with no further pushes. `rk
        // inbox ack <id>` is the only thing that stops it early.
        if daemon.recovery_sweep_config.enabled {
            let daemon_ref = Arc::clone(&daemon);
            let mut rc_shutdown = daemon.shutdown_tx.subscribe();
            let interval = Duration::from_secs(daemon.recovery_sweep_config.interval_secs.max(1));
            background_tasks.spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.tick().await;
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            let d = Arc::clone(&daemon_ref);
                            match tokio::task::spawn_blocking(move || d.recovery_renotify_sweep_once()).await {
                                Ok(0) => {}
                                Ok(n) => debug!(pushed = n, "recovery re-notify sweep pushed escalations"),
                                Err(e) => warn!(error = %e, "recovery re-notify sweep task panicked"),
                            }
                        }
                        _ = rc_shutdown.changed() => break,
                    }
                }
            });
        }

        // B8 stale-`Running`-instance hard timeout sweep: a Running instance
        // older than its effective timeout (workflow `staleTimeout:` override,
        // else `default_timeout_secs`) is marked failed, finalized, and
        // escalated through the B2 announce helper. Not spawn_blocking'd like
        // the sweeps above — it awaits `WorkflowEngine::stale_timeout_sweep_once`
        // directly (guaranteed-cleanup dismissal is already async), the same
        // shape as the landing pipeline loop below.
        if daemon.instance_timeout_sweep_config.enabled {
            let daemon_ref = Arc::clone(&daemon);
            let mut it_shutdown = daemon.shutdown_tx.subscribe();
            let interval =
                Duration::from_secs(daemon.instance_timeout_sweep_config.interval_secs.max(1));
            background_tasks.spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.tick().await;
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            match daemon_ref.stale_instance_timeout_sweep_once().await {
                                0 => {}
                                n => info!(timed_out = n, "stale-instance timeout sweep marked instances failed"),
                            }
                        }
                        _ = it_shutdown.changed() => break,
                    }
                }
            });
        }

        // B9 orphaned-ticket sweep (seam 5): an `in_progress` ticket whose
        // assignee has had no live agent record for `stale_after_secs`
        // reopens to `open` (drain-eligible again), announced through the B2
        // helper. Drain only refills from `status = open`, so without this an
        // errored rat's ticket is stuck `in_progress` forever. Runs directly
        // on this async task rather than `spawn_blocking` — same as the drain
        // loop below, which touches the same `Tickets`/`Space` methods this
        // does — because they are lock-based, not blocking I/O.
        if daemon.ticket_reopen_sweep_config.enabled {
            let daemon_ref = Arc::clone(&daemon);
            let mut tr_shutdown = daemon.shutdown_tx.subscribe();
            let interval =
                Duration::from_secs(daemon.ticket_reopen_sweep_config.interval_secs.max(1));
            background_tasks.spawn(async move {
                let mut tick = tokio::time::interval(interval);
                tick.tick().await;
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            match daemon_ref.ticket_reopen_sweep_once().await {
                                0 => {}
                                n => debug!(reopened = n, "ticket reopen sweep reopened orphaned tickets"),
                            }
                        }
                        _ = tr_shutdown.changed() => break,
                    }
                }
            });
        }

        // Multiplayer sync loop (git shell-outs are blocking → spawn_blocking).
        if let Some(syncer) = daemon.syncer.clone() {
            let space = daemon.space.clone();
            let interval = daemon.sync_interval;
            let mut sync_shutdown = daemon.shutdown_tx.subscribe();
            background_tasks.spawn(async move {
                let mut tick = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            let syncer = Arc::clone(&syncer);
                            let space = space.clone();
                            let result = tokio::task::spawn_blocking(move || {
                                syncer.run_cycle(&space)
                            })
                            .await;
                            match result {
                                Ok(Ok(stats)) => debug!(?stats, "sync cycle"),
                                Ok(Err(e)) => warn!(error = %e, "sync cycle failed"),
                                Err(e) => warn!(error = %e, "sync task panicked"),
                            }
                        }
                        _ = sync_shutdown.changed() => break,
                    }
                }
            });
        }

        // Install the merge-mode landing seam even when the background
        // reactor is disabled: explicit workflow/operator `land` calls still
        // enter and synchronously drive this queue.
        let daemon_landing = daemon.landing();
        let registered_paths: Vec<std::path::PathBuf> = daemon
            .repos
            .lock()
            .map(|repos| repos.list().into_iter().map(|repo| repo.path).collect())
            .unwrap_or_default();
        let reclaimed_candidates = daemon_landing.sweep_orphaned_candidate_refs(registered_paths);
        if reclaimed_candidates > 0 {
            info!(
                reclaimed_candidates,
                "reclaimed orphaned landing candidate refs"
            );
        }

        // Reactor loop: fire registered #Trigger workflows on matching tuples.
        // The lossy feed is only a wake signal; a durable cursor scan is the
        // source of truth, so no event is missed even when the feed drops it.
        if daemon.reactor_config.enabled {
            let reactor = Arc::new(
                crate::reactor::Reactor::new(
                    daemon.space.clone(),
                    daemon.engine(),
                    daemon.tickets.clone(),
                    // The live-session owner, so a promoted convention can be steered
                    // into already-running rats (TKT-34).
                    Some(Arc::clone(&daemon.supervisor)),
                    daemon.layout.clone(),
                    daemon.reactor_config.clone(),
                )
                // So an `action: "land"` trigger (P3-T4) has somewhere to
                // enqueue. Always wired when the reactor itself is enabled —
                // inert (nothing to dispatch) unless a repo actually installs
                // a "land" trigger.
                .with_landing(Arc::clone(&daemon_landing))
                // Escalation push channels. Empty config resolves to the
                // historical herdr-only registry, so an existing castle sees no
                // change; adding a `[[notify.sinks]]` table adds a channel with
                // no change at any escalation source.
                .with_sinks(&daemon.notify_config),
            );
            // Baseline the cursor so a fresh daemon does not react to the whole
            // pre-existing backlog on first boot.
            if let Err(e) = reactor.initialize_cursor() {
                warn!(error = %e, "reactor cursor init failed");
            }
            let mut feed = daemon.space.subscribe();
            let mut reactor_shutdown = daemon.shutdown_tx.subscribe();
            let interval = Duration::from_secs(daemon.reactor_config.interval_secs.max(1));
            // A cycle runs on a blocking thread (it shells out to `cue`), but its
            // dispatch calls `engine.run`, which `tokio::spawn`s the workflow — so
            // the blocking thread must enter the runtime context first.
            let handle = tokio::runtime::Handle::current();
            background_tasks.spawn(async move {
                let mut tick = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {}
                        recv = feed.recv() => match recv {
                            // Coalesce a burst: drain what is already queued so a
                            // single scan covers the whole batch.
                            Ok(_) => while feed.try_recv().is_ok() {},
                            // Dropped events are exactly why dispatch is scan-driven.
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        },
                        _ = reactor_shutdown.changed() => break,
                    }
                    let reactor = Arc::clone(&reactor);
                    let handle = handle.clone();
                    match tokio::task::spawn_blocking(move || {
                        let _guard = handle.enter();
                        reactor.run_cycle()
                    })
                    .await
                    {
                        Ok(Ok(0)) => {}
                        Ok(Ok(n)) => debug!(fired = n, "reactor cycle fired workflows"),
                        Ok(Err(e)) => warn!(error = %e, "reactor cycle failed"),
                        Err(e) => warn!(error = %e, "reactor task panicked"),
                    }
                }
            });

            // Landing pipeline consumer loop (P3-T4): drains the daemon-native
            // `LandingQueue` an `action: "land"` trigger feeds (design doc §2.1).
            // Same shape as the reactor loop above it — feed-wake plus a
            // fallback interval tick, since draining is "an instance completing
            // frees a slot without necessarily writing a tuple this exact scan
            // would match" polling, not event-driven (§2.1, mirroring the
            // reactor's own `drain_queued_fires` rationale). Gated on the SAME
            // `reactor_config.enabled` flag rather than a new config knob: with
            // no `action: "land"` trigger installed, `run_cycle` finds nothing
            // queued and is a cheap no-op.
            let landing = Arc::clone(&daemon_landing);
            let mut landing_feed = daemon.space.subscribe();
            let mut landing_shutdown = daemon.shutdown_tx.subscribe();
            let landing_interval = Duration::from_secs(daemon.reactor_config.interval_secs.max(1));
            background_tasks.spawn(async move {
                let mut tick = tokio::time::interval(landing_interval);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {}
                        recv = landing_feed.recv() => match recv {
                            Ok(_) => while landing_feed.try_recv().is_ok() {},
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        },
                        _ = landing_shutdown.changed() => break,
                    }
                    match landing.run_cycle().await {
                        Ok(outcomes) if outcomes.is_empty() => {}
                        Ok(outcomes) => {
                            debug!(processed = outcomes.len(), "landing pipeline cycle")
                        }
                        Err(e) => warn!(error = %e, "landing pipeline cycle failed"),
                    }
                }
            });
        }

        // Scheduler loop: fire registered #Schedule workflows on a cron cadence.
        // The TIME-axis sibling of the reactor — a purely clock-driven trigger.
        // A durable minute-cursor makes it catch-up-once after downtime, and each
        // schedule is single-flight so a slow run never stacks on itself.
        if daemon.scheduler_config.enabled {
            let scheduler = Arc::new(crate::scheduler::Scheduler::new(
                daemon.engine(),
                daemon.layout.clone(),
                daemon.scheduler_config.clone(),
                daemon.space.clone(),
                daemon.castle.clone(),
            ));
            // Baseline the cursor so a fresh daemon does not fire schedules for
            // minutes that elapsed before it started.
            if let Err(e) = scheduler.initialize_cursor() {
                warn!(error = %e, "scheduler cursor init failed");
            }
            let mut scheduler_shutdown = daemon.shutdown_tx.subscribe();
            // Must tick at least once a minute or a matching minute is skipped.
            let interval = Duration::from_secs(daemon.scheduler_config.interval_secs.clamp(1, 60));
            // A cycle runs on a blocking thread (it shells out to `cue`), and its
            // dispatch calls `engine.run`, which `tokio::spawn`s the workflow — so
            // the blocking thread must enter the runtime context first.
            let handle = tokio::runtime::Handle::current();
            background_tasks.spawn(async move {
                let mut tick = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {}
                        _ = scheduler_shutdown.changed() => break,
                    }
                    let scheduler = Arc::clone(&scheduler);
                    let handle = handle.clone();
                    match tokio::task::spawn_blocking(move || {
                        let _guard = handle.enter();
                        scheduler.run_cycle()
                    })
                    .await
                    {
                        Ok(Ok(0)) => {}
                        Ok(Ok(n)) => debug!(fired = n, "scheduler cycle fired workflows"),
                        Ok(Err(e)) => warn!(error = %e, "scheduler cycle failed"),
                        Err(e) => warn!(error = %e, "scheduler task panicked"),
                    }
                }
            });
        }

        // Continuous-drain loop: a WIP-limited fleet autoscaler. While fewer than
        // `max_wip` rats are live and the ready backlog is non-empty, claim the
        // highest-priority ready ticket and spawn a rat — the always-on refill
        // counterpart to a one-shot backlog-drain workflow. Off unless explicitly
        // enabled *and* given a positive cap (handing the dispatch loop to the
        // daemon is opt-in). Wakes on the tuple feed (a completion frees a slot)
        // with the interval as a fallback, mirroring the reactor.
        if daemon.drain_config.enabled && daemon.drain_config.max_wip > 0 {
            let drain = Arc::new(crate::drain::Drain::new(
                Arc::clone(&daemon.supervisor),
                daemon.tickets.clone(),
                daemon.layout.clone(),
                daemon.drain_config.clone(),
                daemon.tier_routing.clone(),
                daemon.global_agents.clone(),
                daemon.default_harness.clone(),
            ));
            let mut feed = daemon.space.subscribe();
            let mut drain_shutdown = daemon.shutdown_tx.subscribe();
            let interval = Duration::from_secs(daemon.drain_config.interval_secs.max(1));
            // Unlike the reactor/scheduler, a drain cycle shells out to nothing
            // (it claims tickets and spawns) so it runs directly in this async
            // task — the same context the RPC spawn path already uses.
            background_tasks.spawn(async move {
                let mut tick = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {}
                        recv = feed.recv() => match recv {
                            // Coalesce a burst so one refill covers the batch.
                            Ok(_) => while feed.try_recv().is_ok() {},
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        },
                        _ = drain_shutdown.changed() => break,
                    }
                    match drain.run_cycle().await {
                        Ok(0) => {}
                        Ok(n) => debug!(spawned = n, "drain cycle refilled fleet"),
                        Err(e) => warn!(error = %e, "drain cycle failed"),
                    }
                }
            });
        }

        // The maps were rehydrated before dispatch loops started. Now that
        // reactor/scheduler consumers are listening, resume in-flight instances.
        daemon.engine().resume_rehydrated(resumable_workflows);

        // Registered once, outside the accept loop: `shutdown_signal()` used to
        // be called fresh inside `tokio::select!` on every iteration, which
        // re-registers (and, on the branch not chosen, immediately drops and
        // re-registers) the SIGTERM/SIGINT listeners on every accepted
        // connection. Under worker-pool pressure (a busy accept loop) that is
        // needless per-connection syscall overhead, and it opens a real gap:
        // tokio only delivers a unix signal to a listener that is registered
        // at the moment the signal arrives, so a signal landing between one
        // iteration's listener being dropped and the next iteration's being
        // created is silently lost, leaving the daemon waiting on a second
        // signal to shut down. Holding the listeners for the lifetime of the
        // loop closes that gap.
        let mut term_signal = signal(SignalKind::terminate()).ok();
        let mut int_signal = signal(SignalKind::interrupt()).ok();

        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _)) => {
                            let peer_pid = stream
                                .peer_cred()
                                .ok()
                                .and_then(|credentials| credentials.pid())
                                .and_then(|pid| u32::try_from(pid).ok());
                            let origin = PeerOrigin {
                                pid_observed: peer_pid.is_some(),
                                supervised_agents: peer_pid
                                    .map(|pid| daemon.supervisor.supervised_agents_for_peer(pid))
                                    .unwrap_or_default(),
                            };
                            let daemon = Arc::clone(&daemon);
                            tokio::spawn(async move {
                                if let Err(e) = daemon.serve_conn(stream, origin).await {
                                    debug!(error = %e, "connection ended with error");
                                }
                            });
                        }
                        Err(e) => warn!(error = %e, "accept failed"),
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("shutdown requested");
                    break;
                }
                _ = wait_for_shutdown_signal(&mut term_signal, &mut int_signal) => {
                    info!("signal received, shutting down");
                    break;
                }
            }
        }

        // Make sure every background loop has actually been told to stop —
        // the OS-signal branch above breaks this loop directly without going
        // through `shutdown_tx`, and each loop below selects on that channel,
        // not the signal. `send` always notifies subscribers on this watch
        // channel even if the value is unchanged, so this is a harmless no-op
        // when a `stop` RPC already sent it.
        let _ = daemon.shutdown_tx.send(true);
        // Wait for every background loop to actually exit before returning —
        // see the `background_tasks` comment above for why this, rather than
        // a bare detached `tokio::spawn`, is what makes shutdown observable
        // and makes `run()`'s task tree abort cleanly as a unit. `join_next`
        // rather than `join_all`: a loop that panicked earlier (while running
        // detached, same as before this change) must not take the whole
        // shutdown down with it — warn and keep draining the set instead.
        while let Some(result) = background_tasks.join_next().await {
            if let Err(e) = result {
                warn!(error = %e, "background loop task panicked");
            }
        }

        // Remove the socket/pid files only if they are still OURS — a newer
        // daemon may have already bound a fresh socket at the same path, and
        // unlinking it would strand that daemon unreachable.
        let ours = std::fs::read_to_string(daemon.layout.pid_file())
            .ok()
            .and_then(|s| s.trim().parse::<u32>().ok())
            == Some(std::process::id());
        if ours {
            std::fs::remove_file(daemon.layout.socket_path()).ok();
            std::fs::remove_file(daemon.layout.pid_file()).ok();
        }
        Ok(())
    }

    fn engine(&self) -> Arc<crate::workflow_exec::WorkflowEngine> {
        Arc::clone(self.engine.get_or_init(|| {
            Arc::new(crate::workflow_exec::WorkflowEngine::new(
                self.layout.clone(),
                Arc::clone(&self.supervisor),
                self.space.clone(),
                Arc::clone(&self.tickets),
                self.global_agents.clone(),
                self.tier_routing.clone(),
                self.default_harness.clone(),
                self.require_named_checks,
                // A crashed rat may still be revived by the self-healing sweep;
                // a `wait` only gives up on one when it cannot be (TKT-147).
                self.sweep_config.respawn_enabled && self.sweep_config.respawn_max_attempts > 0,
                self.require_approval_for_landing,
                self.automated_landing_workflows.clone(),
                self.allowed_target_branches.clone(),
                // Shared with the continuous-drain autoscaler regardless of
                // whether its own refill loop is enabled: `max_wip` is the
                // fleet-wide concurrency ceiling either way, and 0 (the
                // default) keeps workflow spawns uncapped exactly as before
                // this admission control existed.
                self.drain_config.max_wip,
                // Finalize-time guaranteed-cleanup sweep (TKT-01M04N6W4X47KMXDA6MH0WPH8H):
                // a separate switch from `worktree_sweep_config.enabled` (the
                // periodic timer) — see the field doc.
                self.worktree_sweep_config.finalize_cleanup_enabled,
            ))
        }))
    }

    /// The daemon-native landing pipeline (P3-T4) an `action: "land"` trigger
    /// enqueues onto and the polling consumer loop drains — see the
    /// `landing loop` block in [`Self::run`]. Lazily built, same shape as
    /// [`Self::engine`], and sharing that same engine instance (it launches
    /// the review-only workflow on a verdict-cache miss, `landing.rs`'s
    /// `request_review`).
    fn landing(&self) -> Arc<crate::landing::LandingPipeline> {
        Arc::clone(self.landing.get_or_init(|| {
            let pipeline = Arc::new(crate::landing::LandingPipeline::new(
                self.space.clone(),
                Arc::clone(&self.supervisor),
                self.engine(),
                Arc::clone(&self.tickets),
                self.layout.clone(),
            ));
            self.supervisor.set_landing_pipeline(&pipeline);
            pipeline
        }))
    }

    async fn serve_conn(&self, stream: UnixStream, origin: PeerOrigin) -> std::io::Result<()> {
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::new(read);
        let mut buf = Vec::new();
        let mut noted_client_build = false;

        loop {
            buf.clear();
            loop {
                let available = tokio::time::timeout(Duration::from_secs(30), read.fill_buf())
                    .await
                    .map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::TimedOut, "request timeout")
                    })??;
                if available.is_empty() {
                    if buf.is_empty() {
                        return Ok(());
                    }
                    break;
                }
                let newline = available.iter().position(|byte| *byte == b'\n');
                let take = newline.map_or(available.len(), |position| position + 1);
                if buf.len() + take > MAX_FRAME_BYTES {
                    write_json_line(
                        &mut write,
                        &Response::err("", codes::FRAME_TOO_LARGE, "request exceeds 1 MiB"),
                    )
                    .await?;
                    return Ok(());
                }
                buf.extend_from_slice(&available[..take]);
                read.consume(take);
                if newline.is_some() {
                    break;
                }
            }
            let line = String::from_utf8_lossy(&buf);
            if line.trim().is_empty() {
                continue;
            }
            let parsed = serde_json::from_str::<Request>(&line);
            // The other half of the handshake. The daemon has no terminal to
            // warn into, but `daemon.log` is where anyone diagnosing "the
            // feature I merged isn't there" ends up, and one line naming both
            // builds turns that into a two-second answer. Once per connection:
            // a rat makes many calls and they all carry the same stamp.
            if let Ok(req) = &parsed {
                if let Some(client) = req.client_version.as_deref() {
                    if client != rk_core::version::BUILD_VERSION && !noted_client_build {
                        noted_client_build = true;
                        warn!(
                            client_build = client,
                            daemon_build = rk_core::version::BUILD_VERSION,
                            caller = %req.caller,
                            "caller is a different build than this daemon; `rk daemon rollover` onto it"
                        );
                    }
                }
            }
            let outcome = match parsed {
                Ok(req) if !self.authenticated(&req) => Outcome::Reply(Response::err(
                    req.id,
                    codes::UNAUTHORIZED,
                    "invalid daemon token",
                )),
                Ok(req) if !self.authorized(&req, &origin) => Outcome::Reply(Response::err(
                    req.id,
                    codes::FORBIDDEN,
                    format!("{} is not authorized for {}", req.caller, req.method),
                )),
                Ok(req) if req.method == "verify.run" => {
                    let key = verify_request_key(&req.caller, &req.id);
                    self.dispatch_watching_disconnect(req, &mut read, &key)
                        .await
                }
                Ok(req) => self.dispatch(req).await,
                Err(e) => Outcome::Reply(Response::err(
                    "",
                    codes::BAD_PARAMS,
                    format!("bad request: {e}"),
                )),
            };
            match outcome {
                Outcome::Reply(response) => {
                    write_response(&mut write, &response).await?;
                }
                Outcome::Watch { response, pattern } => {
                    write_response(&mut write, &response).await?;
                    return self.stream_watch(write, pattern).await;
                }
                Outcome::CoordinatorWatch {
                    response,
                    filter,
                    boundary,
                    rx,
                } => {
                    write_response(&mut write, &response).await?;
                    return self.stream_coordinator(write, filter, boundary, rx).await;
                }
                Outcome::FactoryEventsWatch {
                    response,
                    filter,
                    boundary,
                    rx,
                } => {
                    write_response(&mut write, &response).await?;
                    return self
                        .stream_factory_events(write, filter, boundary, rx)
                        .await;
                }
                Outcome::LogFollow { response, spawn } => {
                    write_response(&mut write, &response).await?;
                    return self.stream_log(write, spawn).await;
                }
            }
        }
    }

    /// Race `verify.run`'s dispatch against this connection dying — the
    /// RPC-disconnect half of TKT-01M0PA6C5WYRWS757R1SS2F2GR's cancellation
    /// binding: if the caller (an agent's own `rk verify`, or an operator's)
    /// is killed mid-call, its managed child process must not keep running
    /// under the daemon alone. Scoped to `verify.run` only, by the one call
    /// site above — every other method already completes fast enough that a
    /// lost caller costs nothing but an unread reply.
    ///
    /// The wire protocol is strictly one in-flight request per connection: a
    /// caller always awaits its response before sending again. So any byte
    /// this reads off the socket while `dispatch` is still pending is either
    /// the peer closing (`Ok(0)`) or a protocol violation this daemon does
    /// not support pipelining for — there is no legitimate next-request
    /// framing to preserve either way, so a non-zero read is simply ignored
    /// rather than risked as a would-be cancellation signal.
    async fn dispatch_watching_disconnect(
        &self,
        req: Request,
        read: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
        request_key: &str,
    ) -> Outcome {
        let dispatch_fut = self.dispatch(req);
        tokio::pin!(dispatch_fut);
        loop {
            tokio::select! {
                outcome = &mut dispatch_fut => return outcome,
                ready = read.get_ref().readable() => {
                    if ready.is_err() {
                        continue;
                    }
                    let mut probe = [0u8; 1];
                    if let Ok(0) = read.get_ref().try_read(&mut probe) {
                        self.supervisor.cancel_managed_verification_request(
                            request_key,
                            "caller_disconnect",
                        );
                        return dispatch_fut.await;
                    }
                }
            }
        }
    }

    fn authorized(&self, req: &Request, origin: &PeerOrigin) -> bool {
        let (allowed, reason) = self.authorize_reasoned(req, origin);
        if !allowed {
            // Server-side only: the wire response stays the generic
            // "forbidden: <caller> is not authorized for <method>" message
            // (see the dispatch loop above), so this does not hand a
            // misconfigured or malicious caller a signal about which check
            // rejected it. TKT-01M01EYN0132N30BWP8BXHXDR6: every arm used to
            // collapse into that one generic message, which made diagnosing
            // caller-side credential loss (e.g. a sandboxed harness stripping
            // RK_AUTH_TOKEN) guesswork from the daemon side.
            debug!(
                caller = %req.caller,
                method = %req.method,
                reason,
                "authorization denied"
            );
        }
        allowed
    }

    /// Same decision as `authorized`, paired with a short, non-sensitive tag
    /// naming which check failed (never a token or credential value).
    fn authorize_reasoned(&self, req: &Request, origin: &PeerOrigin) -> (bool, &'static str) {
        if req
            .caller
            .starts_with(crate::ingest_auth::SOURCE_CALLER_PREFIX)
        {
            return if self.ingest_principal(req).is_some()
                && matches!(req.method.as_str(), "ingest.event" | "ingest.state")
            {
                (true, "")
            } else {
                (false, "ingest_principal_or_method")
            };
        }
        let operator = req.caller == "operator" || req.caller.is_empty();
        // The bearer root token alone is not operator authority. A local
        // connection must have a kernel-observed pid, and a connection rooted
        // in exactly one supervised agent may claim only that agent. This
        // closes both `env -u RK_AGENT -u RK_AUTH_TOKEN rk ...` and
        // cross-agent token derivation from the same-user root credential.
        if !origin.pid_observed && operator {
            return (false, "operator_without_observed_pid");
        }
        if !origin.supervised_agents.is_empty()
            && (origin.supervised_agents.len() != 1
                || !origin.supervised_agents.contains(&req.caller))
        {
            return (false, "supervised_agents_mismatch");
        }
        if operator {
            return (true, "");
        }
        if req.auth != rk_core::paths::derive_agent_token(&self.auth_token, &req.caller) {
            return (false, "token_mismatch");
        }
        if let Some(record) = self.supervisor.status(&req.caller) {
            if crate::read_only_roles::is_read_only_role(&record.role) {
                return if crate::read_only_roles::method_allowed(&record.role, req) {
                    (true, "")
                } else {
                    (false, "read_only_role_method_not_allowed")
                };
            }
            if crate::supervisor::validate_role(&record.role).is_err() {
                return (false, "invalid_role");
            }
        }
        if !matches!(
            req.method.as_str(),
            "stop"
                | "daemon.pause_dispatch"
                | "daemon.resume_dispatch"
                | "agent.spawn"
                | "agent.respawn"
                | "agent.dismiss"
                | "agent.interrupt"
                | "agent.steer"
                | "agent.archive"
                | "agent.unarchive"
                | "agent.revert"
                | "repo.add"
                | "repo.land"
                | "repo.remove"
                | "repo.onboard.start"
                | "repo.onboard.propose"
                | "repo.onboard.approve"
                | "repo.onboard.decline"
                | "repo.onboard.apply"
                | "repo.onboard.activate"
                | "repo.onboard.decline_activation"
                | "repo.onboard.cleanup"
                | "repo.onboard.resume"
                | "repo.onboard.status"
                | "repo.onboard.report"
                | "workflow.run"
                | "factory.propose_action"
                | "factory.approve_action"
                | "factory.execute_action"
                | "workflow.approve"
                | "workflow.archive"
                | "workflow.unarchive"
                | "coordinator.snapshot"
                | "coordinator.watch"
                | "coordinator.register"
                | "coordinator.pending"
                | "coordinator.ack"
                | "sync.now"
                | "sync.peers"
                | "ticket.update"
                | "ticket.dep"
                | "ticket.reopen"
                | "reconcile.repair"
        ) {
            return (true, "");
        }

        // A foreman gets only the child-management subset. Each target is
        // checked again at dispatch time against the structural parent edge;
        // this broad authorization is only the first gate.
        let foreman_allowed = self.supervisor.is_foreman(&req.caller)
            && matches!(
                req.method.as_str(),
                "agent.spawn"
                    | "agent.respawn"
                    | "agent.dismiss"
                    | "agent.interrupt"
                    | "agent.steer"
            );
        // A groomer gets exactly one grant from this operator-only list: a
        // ticket.update whose wire shape proves it is a closure carrying
        // recorded evidence. ticket.dep and every other method here (spawn,
        // repo.add, workflow.run, ...) stay refused. handle_ticket_update
        // re-checks the shape and writes the audit event; this is only the
        // wire-level gate.
        let groomer_allowed = req.method == "ticket.update"
            && self.supervisor.is_groomer(&req.caller)
            && crate::read_only_roles::groomer_can_close_ticket(&req.params);
        let allowed = foreman_allowed || groomer_allowed;
        (allowed, if allowed { "" } else { "operator_only_method" })
    }

    /// Enforced capability profile for the onboarding role. This is
    /// intentionally much smaller than the ordinary rat surface: inspection
    /// reads, self progress, and the one completion event required by `rk
    /// done`. It is evaluated after peer-origin and token binding, so clearing
    /// ambient identity cannot select the operator arm above.
    fn authenticated(&self, req: &Request) -> bool {
        if req
            .caller
            .starts_with(crate::ingest_auth::SOURCE_CALLER_PREFIX)
        {
            return self.ingest_principal(req).is_some();
        }
        if req.caller == "operator" || req.caller.is_empty() {
            req.auth == self.auth_token
        } else {
            req.auth == rk_core::paths::derive_agent_token(&self.auth_token, &req.caller)
        }
    }

    fn ingest_principal(&self, req: &Request) -> Option<SourcePrincipal> {
        SourcePrincipal::from_request(
            &self.ingest_config,
            &self.auth_token,
            &req.caller,
            &req.auth,
        )
    }

    /// Push matching tuples as notification lines until the client goes away.
    async fn stream_watch(
        &self,
        mut write: tokio::net::unix::OwnedWriteHalf,
        pattern: Pattern,
    ) -> std::io::Result<()> {
        let mut rx = self.space.subscribe();
        loop {
            match rx.recv().await {
                Ok(tuple) if pattern.matches(&tuple) => {
                    let note = json!({"method": "tuple", "params": tuple});
                    write_json_line(&mut write, &note).await?;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    let note = json!({"method": "lagged", "params": {"missed": missed}});
                    write_json_line(&mut write, &note).await?;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }

    /// Push durable coordinator events after the snapshot/replay response.
    /// `rx` was subscribed before the durable backlog scan, so the feed is only
    /// a wake/continuation channel; journal sequences at or before `boundary`
    /// are skipped as already covered by the response.
    async fn stream_coordinator(
        &self,
        mut write: tokio::net::unix::OwnedWriteHalf,
        filter: CoordinatorFilter,
        boundary: Option<u64>,
        mut rx: broadcast::Receiver<CoordinatorEvent>,
    ) -> std::io::Result<()> {
        let mut cursor = boundary;
        loop {
            match rx.recv().await {
                Ok(coordinator_event)
                    if filter.matches_event(&coordinator_event.event)
                        && cursor.is_none_or(|seen| coordinator_event.cursor > seen) =>
                {
                    cursor = Some(coordinator_event.cursor);
                    let note = json!({
                        "method": "coordinator.event",
                        "params": {
                            "cursor": coordinator_event.cursor,
                            "event": coordinator_event.event,
                        },
                    });
                    write_json_line(&mut write, &note).await?;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    let note = json!({
                        "method": "lagged",
                        "params": {"missed": missed, "resync_required": true},
                    });
                    write_json_line(&mut write, &note).await?;
                    return Ok(());
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }

    async fn stream_factory_events(
        &self,
        mut write: tokio::net::unix::OwnedWriteHalf,
        filter: FactoryEventFilter,
        boundary: Option<u64>,
        mut rx: broadcast::Receiver<CoordinatorEvent>,
    ) -> std::io::Result<()> {
        let mut cursor = boundary;
        cursor = self
            .write_factory_durable_catchup(&mut write, &filter, cursor)
            .await?;
        loop {
            match rx.recv().await {
                Ok(coordinator_event)
                    if cursor.is_none_or(|seen| coordinator_event.cursor > seen) =>
                {
                    if let Some(event) = crate::factory_events::project(coordinator_event) {
                        if filter.matches(&event) {
                            cursor = Some(event.cursor);
                            write_json_line(
                                &mut write,
                                &json!({"method": "factory.event", "params": event}),
                            )
                            .await?;
                        }
                    }
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    let before = cursor;
                    cursor = self
                        .write_factory_durable_catchup(&mut write, &filter, cursor)
                        .await?;
                    write_json_line(
                        &mut write,
                        &json!({"method": "lagged", "params": {"missed": missed, "resync_required": before == cursor, "cursor": cursor}}),
                    )
                    .await?;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }

    async fn write_factory_durable_catchup(
        &self,
        write: &mut tokio::net::unix::OwnedWriteHalf,
        filter: &FactoryEventFilter,
        cursor: Option<u64>,
    ) -> std::io::Result<Option<u64>> {
        let mut catchup_filter = filter.clone();
        catchup_filter.after = cursor;
        let scanned = self
            .space
            .coordinator_events_after(cursor, catchup_filter.limit().saturating_add(1))
            .map_err(std::io::Error::other)?;
        let replay = crate::factory_events::replay(scanned, &catchup_filter);
        for event in replay.events {
            write_json_line(write, &json!({"method": "factory.event", "params": event})).await?;
        }
        if replay.truncated {
            write_json_line(
                write,
                &json!({"method":"factory.resync", "params":{"truncated": true, "resync_required": true, "boundary": replay.boundary}}),
            )
            .await?;
        }
        Ok(max_cursor(cursor, replay.boundary))
    }

    /// Collect the owned, read-only structured records the Phase 5 analytics
    /// adapter normalizes. This clones agent/workflow records out of their
    /// stores and hands the adapter no mutation-capable handles: the adapter
    /// cannot reach the registry, engine, tickets, approvals, queue, or
    /// dispatch. Repo and since/until windowing is applied here so the pure
    /// adapter only sees in-scope records.
    fn factory_analytics_inputs(
        &self,
        req: &crate::factory_analytics::FactoryAnalyticsRequest,
    ) -> crate::factory_analytics::AnalyticsInputs {
        let repo = req.repo.clone().unwrap_or_default();
        let in_window = |ms: i64| -> bool {
            req.since.is_none_or(|since| ms >= since) && req.until.is_none_or(|until| ms <= until)
        };
        // Always read active + archived immutable snapshots. The pure fact/
        // scorecard layer applies include_archived to metric numerators while
        // retaining archived source counts and archived-only availability.
        let mut runtime_unavailable = Vec::new();
        let mut read_warnings = Vec::new();
        let agents = self
            .supervisor
            .list_all()
            .into_iter()
            .filter(|agent| req.repo.as_deref().is_none_or(|r| agent.repo_name == r))
            .filter(|agent| in_window(agent.updated_at.timestamp_millis()))
            .collect();
        let instances = self
            .engine()
            .list_all()
            .into_iter()
            .filter(|instance| req.repo.as_deref().is_none_or(|r| instance.repo == r))
            .filter(|instance| {
                let observed_at = instance
                    .completed_at
                    .unwrap_or(instance.started_at)
                    .timestamp_millis();
                in_window(observed_at)
            })
            .collect();
        let tickets = match self.tickets.list(req.repo.clone(), None, None) {
            Ok(tickets) => tickets
                .into_iter()
                .filter(|ticket| in_window(ticket.created_at.timestamp_millis()))
                .collect(),
            Err(error) => {
                runtime_unavailable
                    .push(rk_core::factory::outcome_facts::OutcomeEvidenceKind::RecurrenceKey);
                read_warnings.push(format!(
                    "source_family_read_failed: RecurrenceKey unavailable: {error}"
                ));
                Vec::new()
            }
        };
        let approval_grants = match self.action_approvals.list_grants() {
            Ok(grants) => grants
                .into_iter()
                .filter(|grant| {
                    req.repo
                        .as_deref()
                        .is_none_or(|repo| grant.scope.repo.identity == repo)
                })
                .filter(|grant| in_window(grant.approved_at.timestamp_millis()))
                .collect(),
            Err(error) => {
                runtime_unavailable
                    .push(rk_core::factory::outcome_facts::OutcomeEvidenceKind::HumanGateDecision);
                read_warnings.push(format!(
                    "source_family_read_failed: HumanGateDecision unavailable: {error}"
                ));
                Vec::new()
            }
        };
        let sdlc_ci_facts = match self
            .space
            .scan(&Pattern::category(Category::Event).scope("ci"))
        {
            Ok(events) => events
                .into_iter()
                .filter(|event| {
                    req.repo.as_deref().is_none_or(|repo| {
                        event
                            .payload
                            .get("subject")
                            .and_then(Value::as_str)
                            .is_some_and(|subject| subject.starts_with(&format!("{repo}:")))
                    })
                })
                .filter(|event| {
                    let observed_at = event
                        .payload
                        .get("observed_at")
                        .and_then(Value::as_str)
                        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                        .map(|dt| dt.timestamp_millis())
                        .unwrap_or_else(|| event.created_at.timestamp_millis());
                    in_window(observed_at)
                })
                .collect(),
            Err(error) => {
                runtime_unavailable
                    .push(rk_core::factory::outcome_facts::OutcomeEvidenceKind::Phase4CiSignal);
                read_warnings.push(format!(
                    "source_family_read_failed: Phase4CiSignal unavailable: {error}"
                ));
                Vec::new()
            }
        };
        let revert_facts = match self
            .space
            .scan(&Pattern::category(Category::Fact).scope(repo.clone()))
        {
            Ok(facts) => facts
                .into_iter()
                .filter(|fact| fact.identity.starts_with("merge-reverted-"))
                .filter(|fact| in_window(fact.created_at.timestamp_millis()))
                .collect(),
            Err(error) => {
                runtime_unavailable
                    .push(rk_core::factory::outcome_facts::OutcomeEvidenceKind::StructuredRevert);
                read_warnings.push(format!(
                    "source_family_read_failed: StructuredRevert unavailable: {error}"
                ));
                Vec::new()
            }
        };
        let reviewer_verdicts = match self
            .space
            .scan(&Pattern::category(Category::Artifact).scope(repo.clone()))
        {
            Ok(artifacts) => artifacts
                .into_iter()
                .filter(|artifact| artifact.identity == "review")
                .filter(|artifact| in_window(artifact.created_at.timestamp_millis()))
                .collect(),
            Err(error) => {
                runtime_unavailable.push(
                    rk_core::factory::outcome_facts::OutcomeEvidenceKind::StructuredReviewerRework,
                );
                read_warnings.push(format!(
                    "source_family_read_failed: StructuredReviewerRework unavailable: {error}"
                ));
                Vec::new()
            }
        };
        crate::factory_analytics::AnalyticsInputs {
            repo,
            agents,
            instances,
            tickets,
            approval_grants,
            sdlc_ci_facts,
            revert_facts,
            reviewer_verdicts,
            runtime_unavailable,
            read_warnings,
        }
    }

    async fn factory_snapshot(&self, filter: &FactoryEventFilter) -> rk_core::Result<Value> {
        let coord = CoordinatorFilter {
            repo: filter.repo.clone(),
            coordinator: filter.coordinator.clone(),
            ..Default::default()
        };
        let agents: Vec<_> = if filter.include_archived {
            self.supervisor.list_all()
        } else {
            self.supervisor.list()
        }
        .into_iter()
        .filter(|agent| factory_matches_agent(filter, agent))
        .collect();
        let workflows: Vec<_> = if filter.include_archived {
            self.engine().list_all()
        } else {
            self.engine().list()
        }
        .into_iter()
        .filter(|workflow| factory_matches_workflow(filter, workflow))
        .collect();
        let tickets = self
            .tickets
            .list(filter.repo.clone(), None, None)
            .map(|tickets| json!(tickets))
            .unwrap_or_else(|_| json!([]));
        let all_proposals = self.action_approvals.list()?;
        let proposals: Vec<_> = all_proposals
            .iter()
            .filter(|proposal| factory_matches_proposal(filter, proposal))
            .cloned()
            .collect();
        let grants: Vec<_> = self
            .action_approvals
            .list_grants()?
            .into_iter()
            .filter(|grant| factory_matches_grant(filter, grant, &all_proposals))
            .collect();
        let approvals = json!({
            "proposals": proposals,
            "grants": grants,
        });
        let budget = factory_filtered_budget(self.supervisor.fleet_rollup(), filter, &workflows);
        let inbox = self.inbox_value(filter.repo.clone()).await?;
        let snapshot = crate::factory_events::snapshot_value(
            json!(agents),
            json!(workflows),
            tickets,
            inbox["items"].clone(),
            budget,
            approvals,
            json!({
                "cursor": self.latest_event_cursor(),
                "required": self
                    .syncer
                    .as_ref()
                    .is_some_and(|syncer| syncer.is_running()),
            }),
        );
        Ok(
            json!({"schema": crate::factory_events::SCHEMA, "snapshot": snapshot, "cursor": self.latest_event_cursor(), "coordinator": coord.coordinator}),
        )
    }

    fn factory_events_replay(&self, filter: &FactoryEventFilter) -> rk_core::Result<Value> {
        let scanned = self
            .space
            .coordinator_events_after(filter.after, filter.limit().saturating_add(1))?;
        Ok(serde_json::to_value(crate::factory_events::replay(
            scanned, filter,
        ))?)
    }

    fn prepare_factory_events_watch(
        &self,
        id: String,
        filter: FactoryEventFilter,
    ) -> rk_core::Result<Outcome> {
        let rx = self.space.subscribe_coordinator();
        let replay = self.factory_events_replay(&filter)?;
        let boundary = replay["boundary"].as_u64().or(filter.after);
        Ok(Outcome::FactoryEventsWatch {
            response: Response::ok(id, replay),
            filter,
            boundary,
            rx,
        })
    }

    fn emit_factory_event(&self, tuple: Tuple) {
        if let Err(error) = self.space.out_coordinator(tuple) {
            warn!(%error, "failed to emit factory event");
        }
    }

    fn coordinator_snapshot(&self, filter: &CoordinatorFilter) -> rk_core::Result<Value> {
        let events = self.space.coordinator_events_after(
            None,
            crate::coordinator::MAX_REPLAY_EVENTS.saturating_mul(4),
        )?;
        let snapshot = crate::coordinator::hierarchical_snapshot(
            &self.engine().list(),
            &self.supervisor.list(),
            &events,
            filter,
        );
        Ok(json!({
            "snapshot": snapshot,
            "cursor": self.latest_event_cursor(),
        }))
    }

    fn prepare_coordinator_watch(
        &self,
        id: String,
        filter: CoordinatorFilter,
    ) -> rk_core::Result<Outcome> {
        // Subscribe before taking either the cursor boundary or the durable
        // replay scan. Events written after this point are guaranteed to be in
        // either the replay result or the live receiver, then deduplicated by
        // journal sequence in stream_coordinator.
        let rx = self.space.subscribe_coordinator();
        let baseline = self.latest_event_cursor();
        let scanned = if filter.after.is_some() {
            self.space
                .coordinator_events_after(filter.after, crate::coordinator::MAX_REPLAY_EVENTS + 1)?
        } else {
            Vec::new()
        };
        let replay = crate::coordinator::replay(scanned, &filter);
        let boundary = max_cursor(baseline, replay.boundary);
        let events = self.space.coordinator_events_after(
            None,
            crate::coordinator::MAX_REPLAY_EVENTS.saturating_mul(4),
        )?;
        let snapshot = crate::coordinator::hierarchical_snapshot(
            &self.engine().list(),
            &self.supervisor.list(),
            &events,
            &filter,
        );
        Ok(Outcome::CoordinatorWatch {
            response: Response::ok(
                id,
                json!({
                    "snapshot": snapshot,
                    "cursor": boundary,
                    "events": replay
                        .events
                        .iter()
                        .map(|event| json!({"cursor": event.cursor, "event": event.event}))
                        .collect::<Vec<_>>(),
                    "truncated": replay.truncated,
                    "resync_required": replay.truncated,
                }),
            ),
            filter,
            boundary,
            rx,
        })
    }

    fn latest_event_cursor(&self) -> Option<u64> {
        self.space.coordinator_latest_sequence().ok().flatten()
    }

    fn handle_coordinator_register(&self, req: Request) -> Response {
        let params: CoordinatorRegisterParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        if params.coordinator.trim().is_empty() {
            return Response::err(req.id, codes::BAD_PARAMS, "coordinator is required");
        }
        let mut sessions = match self.coordinator_sessions.lock() {
            Ok(sessions) => sessions,
            Err(poisoned) => poisoned.into_inner(),
        };
        match sessions.register(&params.coordinator, params.after) {
            Ok(session) => Response::ok(req.id, json!({"session": session})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_coordinator_pending(&self, req: Request) -> Response {
        let mut filter: CoordinatorFilter = match parse_params(&req.params) {
            Ok(filter) => filter,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let Some(coordinator) = filter.coordinator.clone() else {
            return Response::err(req.id, codes::BAD_PARAMS, "coordinator is required");
        };
        let cursor = {
            let sessions = match self.coordinator_sessions.lock() {
                Ok(sessions) => sessions,
                Err(poisoned) => poisoned.into_inner(),
            };
            sessions
                .cursor(&coordinator)
                .unwrap_or(filter.after.unwrap_or(0))
        };
        filter.after = Some(cursor);
        match self.coordinator_pending(&filter) {
            Ok(result) => Response::ok(req.id, result),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_coordinator_ack(&self, req: Request) -> Response {
        let params: CoordinatorAckParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let mut sessions = match self.coordinator_sessions.lock() {
            Ok(sessions) => sessions,
            Err(poisoned) => poisoned.into_inner(),
        };
        match sessions.acknowledge(&params.coordinator, params.cursor) {
            Ok(session) => Response::ok(req.id, json!({"session": session})),
            Err(e) => Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
        }
    }

    fn coordinator_pending(&self, filter: &CoordinatorFilter) -> rk_core::Result<Value> {
        let scanned = self
            .space
            .coordinator_events_after(filter.after, crate::coordinator::MAX_REPLAY_EVENTS + 1)?;
        let replay = crate::coordinator::replay(scanned, filter);
        let snapshot = self.coordinator_snapshot(filter)?;
        Ok(json!({
            "snapshot": snapshot["snapshot"],
            "cursor": snapshot["cursor"],
            "events": replay.events.iter().map(|event| json!({"cursor": event.cursor, "event": event.event})).collect::<Vec<_>>(),
            "truncated": replay.truncated,
            "resync_required": replay.truncated,
            "ack_after": replay.boundary.or(snapshot["cursor"].as_u64()),
        }))
    }

    /// Push the resolved generation's new transcript entries as they land,
    /// until the client goes away. The backlog was already sent as the
    /// `agent.log` reply; this is the live tail (there may be a momentary
    /// overlap of one boundary entry).
    ///
    /// Matches on `SpawnId` alone (E5, docs/2026-08-17-tkt-c1-generation-identity.md)
    /// — it cannot collide with a namesake's, so no separate name check is
    /// needed. `spawn: None` (an unrecorded name) never matches, since nothing
    /// live can be writing under a name with no registry record.
    async fn stream_log(
        &self,
        mut write: tokio::net::unix::OwnedWriteHalf,
        spawn: Option<rk_core::id::SpawnId>,
    ) -> std::io::Result<()> {
        let mut rx = self.supervisor.log().subscribe();
        loop {
            match rx.recv().await {
                Ok(rec) if Some(rec.spawn) == spawn => {
                    let note = json!({"method": "log", "params": rec.entry});
                    write_json_line(&mut write, &note).await?;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                    let note = json!({"method": "lagged", "params": {"missed": missed}});
                    write_json_line(&mut write, &note).await?;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }

    async fn dispatch(&self, req: Request) -> Outcome {
        debug!(method = %req.method, id = %req.id, "dispatch");
        let id = req.id.clone();
        let reply = |r: Response| Outcome::Reply(r);
        match req.method.as_str() {
            "ping" => reply(Response::ok(id, json!("pong"))),
            "status" => reply(Response::ok(id, self.status())),
            "stop" => {
                let resp = Response::ok(id, json!({"stopping": true}));
                let _ = self.shutdown_tx.send(true);
                reply(resp)
            }
            // `rk daemon rollover`'s drain step: stop admitting new dispatch
            // and report who is still live so the CLI knows what it is
            // waiting on. Does not touch already-running agents.
            "daemon.pause_dispatch" => {
                self.supervisor.set_dispatch_paused(true);
                let live: Vec<String> = self
                    .supervisor
                    .list()
                    .into_iter()
                    .filter(|r| r.state.is_live())
                    .map(|r| r.name)
                    .collect();
                reply(Response::ok(
                    id,
                    json!({"paused": true, "live_agents": live}),
                ))
            }
            // Best-effort unwind if a rollover aborts before it reaches
            // `stop` — a fresh daemon process always starts unpaused anyway.
            "daemon.resume_dispatch" => {
                self.supervisor.set_dispatch_paused(false);
                reply(Response::ok(id, json!({"paused": false})))
            }
            "space.out" => reply(self.handle_out(req)),
            "ingest.event" => reply(self.handle_ingest_event(req)),
            "ingest.state" => reply(self.handle_ingest_state(req)),
            "space.withdraw" => reply(self.handle_withdraw(req)),
            "fact.vote" => reply(self.handle_fact_vote(req)),
            "space.scan" => reply(self.handle_scan(req)),
            "space.take" => reply(self.handle_blocking(req, true).await),
            "space.rd" => reply(self.handle_blocking(req, false).await),
            "space.watch" => match parse_params::<PatternParams>(&req.params) {
                Ok(p) => Outcome::Watch {
                    response: Response::ok(id, json!({"watching": true})),
                    pattern: p.pattern,
                },
                Err(e) => reply(Response::err(id, codes::BAD_PARAMS, e)),
            },
            "coordinator.snapshot" => match parse_params::<CoordinatorFilter>(&req.params) {
                Ok(filter) if filter_is_valid(&filter) => {
                    match self.coordinator_snapshot(&filter) {
                        Ok(snapshot) => reply(Response::ok(id, snapshot)),
                        Err(e) => reply(Response::err(id, codes::INTERNAL, e.to_string())),
                    }
                }
                Ok(_) => reply(Response::err(
                    id,
                    codes::BAD_PARAMS,
                    "a coordinator scope is required (instance, coordinator, subtree, or repo)",
                )),
                Err(e) => reply(Response::err(id, codes::BAD_PARAMS, e)),
            },
            "coordinator.register" => reply(self.handle_coordinator_register(req)),
            "coordinator.pending" => reply(self.handle_coordinator_pending(req)),
            "coordinator.ack" => reply(self.handle_coordinator_ack(req)),
            "coordinator.watch" => match parse_params::<CoordinatorFilter>(&req.params) {
                Ok(filter) if filter_is_valid(&filter) => {
                    match self.prepare_coordinator_watch(id, filter) {
                        Ok(outcome) => outcome,
                        Err(e) => reply(Response::err(req.id, codes::INTERNAL, e.to_string())),
                    }
                }
                Ok(_) => reply(Response::err(
                    id,
                    codes::BAD_PARAMS,
                    "a coordinator scope is required (instance, coordinator, subtree, or repo)",
                )),
                Err(e) => reply(Response::err(id, codes::BAD_PARAMS, e)),
            },
            "agent.spawn" => reply(self.handle_spawn(req).await),
            "agent.progress" => reply(self.handle_progress(req)),
            "agent.respawn" => reply(self.handle_respawn(req).await),
            "agent.list" => reply(match parse_params::<AgentListParams>(&req.params) {
                Ok(p) => {
                    let agents = if p.archived_only {
                        self.supervisor.list_archived()
                    } else if p.include_archived {
                        self.supervisor.list_all()
                    } else {
                        self.supervisor.list()
                    };
                    Response::ok(id, json!({ "agents": agents }))
                }
                Err(e) => Response::err(id, codes::BAD_PARAMS, e),
            }),
            "agent.archive" => reply(self.handle_agent_archive(req).await),
            "agent.unarchive" => {
                reply(self.handle_named(req, |sup, name| sup.unarchive_agent(&name)))
            }
            "budget.rollup" => reply(Response::ok(id, self.supervisor.fleet_rollup())),
            "inbox.list" => reply(self.handle_inbox(req).await),
            "inbox.ack" => reply(self.handle_inbox_ack(req)),
            "reconcile.report" => reply(self.handle_reconcile(req).await),
            "reconcile.repair" => reply(self.handle_reconcile_repair(req).await),
            "lease.acquire" => reply(self.handle_lease_acquire(req).await),
            "lease.renew" => reply(self.handle_lease_renew(req).await),
            "attention.next" => reply(self.handle_attention_next(req).await),
            "attention.decide" => reply(self.handle_attention_decide(req).await),
            "agent.status" => reply(self.handle_named(req, |sup, name| {
                sup.status(&name)
                    .map(|r| json!({"agent": r}))
                    .ok_or_else(|| rk_core::Error::other(format!("no such agent: {name}")))
            })),
            "agent.log" => {
                let params: LogParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                // E4: an exact SpawnId resolves directly, bypassing name
                // resolution entirely — the one form that can never be
                // ambiguous. Otherwise fall back to today's name (+ordinal)
                // resolution: a name can have named more than one rat (the
                // TKT-136 archiving window did that to 24 of them), so resolve
                // which generation is meant instead of keying the read on the
                // name alone. Default: the newest, which is what an operator
                // typing a bare name means.
                let (generations, selected) = match params.name.parse::<rk_core::id::SpawnId>() {
                    Ok(spawn) => match self.supervisor.find_generation_by_spawn(spawn) {
                        Some(g) => (self.supervisor.log_generations(&g.agent), g),
                        None => {
                            let msg = format!("no agent generation with spawn {spawn}");
                            return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, msg));
                        }
                    },
                    Err(_) => {
                        let generations = self.supervisor.log_generations(&params.name);
                        let selected = match params.generation {
                            Some(n) => match n.checked_sub(1).and_then(|i| generations.get(i)) {
                                Some(g) => g.clone(),
                                None => {
                                    let msg = format!(
                                            "{} has {} log generation(s); no generation {n} (1 = oldest)",
                                            params.name,
                                            generations.len()
                                        );
                                    return Outcome::Reply(Response::err(
                                        id,
                                        codes::BAD_PARAMS,
                                        msg,
                                    ));
                                }
                            },
                            None => generations.last().cloned().unwrap_or_else(|| {
                                crate::agent_log::Generation::unrecorded(&params.name)
                            }),
                        };
                        (generations, selected)
                    }
                };
                let ordinal = generations
                    .iter()
                    .position(|g| g.spawn == selected.spawn)
                    .map(|i| i + 1);
                let backlog = self.supervisor.log().read(&selected, params.tail);
                let response = Response::ok(
                    id,
                    json!({
                        "entries": backlog,
                        // The resolved agent name, how many rats have carried
                        // it, and which one this is (1 = oldest; 0 = no
                        // record at all), so the client can disclose that a
                        // name is ambiguous (and label a spawn-id lookup with
                        // the name it resolved to).
                        "agent": selected.agent,
                        "generations": generations.len(),
                        "generation": ordinal.unwrap_or(generations.len()),
                        "spawn": selected.spawn.map(|s| s.to_string()),
                        "created_at": selected.start,
                    }),
                );
                if params.follow {
                    Outcome::LogFollow {
                        response,
                        spawn: selected.spawn,
                    }
                } else {
                    reply(response)
                }
            }
            "agent.steer" => reply(self.handle_steer(req).await),
            "agent.interrupt" => {
                let params: NameParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                if let Err(e) = self.authorize_foreman_child(&req.caller, &params.name) {
                    return reply(Response::err(id, codes::FORBIDDEN, e.to_string()));
                }
                reply(match self.supervisor.interrupt(&params.name).await {
                    Ok(()) => Response::ok(id, json!({"interrupted": true})),
                    Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                })
            }
            "agent.dismiss" => {
                let params: DismissParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                if let Err(e) = self.authorize_foreman_child(&req.caller, &params.name) {
                    return reply(Response::err(id, codes::FORBIDDEN, e.to_string()));
                }
                reply(
                    match self.supervisor.dismiss(&params.name, params.no_merge).await {
                        Ok(v) => Response::ok(id, v),
                        Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                    },
                )
            }
            "agent.revert" => {
                let params: RevertParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                reply(
                    match self.supervisor.revert(&params.name, params.block).await {
                        Ok(v) => Response::ok(id, v),
                        Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                    },
                )
            }
            "workflow.run" => reply(self.handle_workflow_run(req).await),
            "verify.run" => reply(self.handle_verify_run(req).await),
            "factory.propose_action" => reply(self.handle_factory_propose_action(req)),
            "factory.approve_action" => reply(self.handle_factory_approve_action(req)),
            "factory.execute_action" => reply(self.handle_factory_execute_action(req).await),
            "factory.snapshot" => match parse_params::<FactoryEventFilter>(&req.params) {
                Ok(filter) => match self.factory_snapshot(&filter).await {
                    Ok(snapshot) => reply(Response::ok(id, snapshot)),
                    Err(e) => reply(Response::err(id, codes::INTERNAL, e.to_string())),
                },
                Err(e) => reply(Response::err(id, codes::BAD_PARAMS, e)),
            },
            "factory.events.replay" => match parse_params::<FactoryEventFilter>(&req.params) {
                Ok(filter) => match self.factory_events_replay(&filter) {
                    Ok(replay) => reply(Response::ok(id, replay)),
                    Err(e) => reply(Response::err(id, codes::INTERNAL, e.to_string())),
                },
                Err(e) => reply(Response::err(id, codes::BAD_PARAMS, e)),
            },
            "factory.events.watch" => match parse_params::<FactoryEventFilter>(&req.params) {
                Ok(filter) => match self.prepare_factory_events_watch(id, filter) {
                    Ok(outcome) => outcome,
                    Err(e) => reply(Response::err(req.id, codes::INTERNAL, e.to_string())),
                },
                Err(e) => reply(Response::err(id, codes::BAD_PARAMS, e)),
            },
            "factory.scorecards" => {
                match parse_params::<crate::factory_analytics::FactoryAnalyticsRequest>(&req.params)
                {
                    Ok(analytics) => match analytics.validate() {
                        Ok(()) => reply(Response::ok(
                            id,
                            crate::factory_analytics::scorecards_response(
                                &self.factory_analytics_inputs(&analytics),
                                &analytics,
                                (self.request_clock)(),
                            ),
                        )),
                        Err(e) => reply(Response::err(id, codes::BAD_PARAMS, e)),
                    },
                    Err(e) => reply(Response::err(id, codes::BAD_PARAMS, e)),
                }
            }
            "factory.recommend" => {
                match parse_params::<crate::factory_analytics::FactoryAnalyticsRequest>(&req.params)
                {
                    Ok(analytics) => match analytics.validate() {
                        Ok(()) => reply(Response::ok(
                            id,
                            crate::factory_analytics::recommend_response(
                                &self.factory_analytics_inputs(&analytics),
                                &analytics,
                                (self.request_clock)(),
                            ),
                        )),
                        Err(e) => reply(Response::err(id, codes::BAD_PARAMS, e)),
                    },
                    Err(e) => reply(Response::err(id, codes::BAD_PARAMS, e)),
                }
            }
            "workflow.list" => {
                let params: WorkflowListParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                let engine = self.engine();
                let instances = match (params.archived, params.all) {
                    (true, _) => engine.list_archived(),
                    (false, true) => engine.list_all(),
                    (false, false) => engine.list(),
                };
                reply(Response::ok(id, json!({ "instances": instances })))
            }
            "workflow.archive" => reply(self.handle_workflow_archive(req).await),
            "workflow.unarchive" => {
                let params: NameParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                reply(match self.engine().unarchive(&params.name) {
                    Ok(Some(instance)) => Response::ok(id, json!({ "instance": instance })),
                    Ok(None) => Response::err(
                        id,
                        codes::INTERNAL,
                        format!("no archived workflow instance: {}", params.name),
                    ),
                    Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                })
            }
            "workflow.status" => {
                let params: NameParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                reply(match self.engine().status_any(&params.name) {
                    Some(instance) => Response::ok(id, json!({"instance": instance})),
                    None => Response::err(
                        id,
                        codes::INTERNAL,
                        format!("no such instance: {}", params.name),
                    ),
                })
            }
            "workflow.timeline" => {
                let params: NameParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                let engine = self.engine();
                let name = params.name;
                let lookup_name = name.clone();
                let result =
                    tokio::task::spawn_blocking(move || engine.timeline(&lookup_name)).await;
                reply(match result {
                    Ok(Some((instance, steps))) => {
                        Response::ok(id, json!({"instance": instance, "steps": steps}))
                    }
                    Ok(None) => {
                        Response::err(id, codes::INTERNAL, format!("no such instance: {name}"))
                    }
                    Err(e) => Response::err(
                        id,
                        codes::INTERNAL,
                        format!("workflow timeline task failed: {e}"),
                    ),
                })
            }
            "workflow.approve" => {
                let params: WorkflowApproveParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                reply(
                    match self.engine().approve(
                        &params.instance,
                        params.approved,
                        &params.by,
                        params.reason,
                    ) {
                        Ok(()) => Response::ok(
                            id,
                            json!({"instance": params.instance, "approved": params.approved}),
                        ),
                        Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                    },
                )
            }
            "workflow.definitions" => {
                let params: WorkflowDefsParams = match parse_params(&req.params) {
                    Ok(p) => p,
                    Err(e) => return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, e)),
                };
                let engine = self.engine();
                let result =
                    tokio::task::spawn_blocking(move || engine.definitions(&params.repo)).await;
                reply(match result {
                    Ok(definitions) => Response::ok(id, json!({"definitions": definitions})),
                    Err(e) => Response::err(
                        id,
                        codes::INTERNAL,
                        format!("workflow definitions task failed: {e}"),
                    ),
                })
            }
            "sync.now" => {
                let Some(syncer) = self.syncer.clone() else {
                    return Outcome::Reply(Response::err(
                        id,
                        codes::INTERNAL,
                        "sync is not enabled ([sync] enabled = true in config.toml)",
                    ));
                };
                let space = self.space.clone();
                let result = tokio::task::spawn_blocking(move || syncer.run_cycle(&space)).await;
                reply(match result {
                    Ok(Ok(stats)) => Response::ok(id, json!(stats)),
                    Ok(Err(e)) => Response::err(id, codes::INTERNAL, e.to_string()),
                    Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                })
            }
            "sync.peers" => {
                let Some(syncer) = self.syncer.clone() else {
                    return Outcome::Reply(Response::err(
                        id,
                        codes::INTERNAL,
                        "sync is not enabled ([sync] enabled = true in config.toml)",
                    ));
                };
                let result = tokio::task::spawn_blocking(move || syncer.peers()).await;
                reply(match result {
                    Ok(Ok(peers)) => Response::ok(id, json!({"peers": peers})),
                    Ok(Err(e)) => Response::err(id, codes::INTERNAL, e.to_string()),
                    Err(e) => Response::err(id, codes::INTERNAL, e.to_string()),
                })
            }
            "repo.add" => reply(self.handle_repo_add(req).await),
            "repo.land" => {
                let params: RepoLandParams = match parse_params(&req.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, error));
                    }
                };
                // `land_force` bypasses the landing pipeline entirely (no
                // queue, no `Supervisor::resolve_land_task` validation, no
                // `landing_processed` marker) — it has nothing to carry a
                // `task` identity through. Silently dropping `--task` here
                // would look like it bound the ticket when nothing recorded
                // that; refuse the combination instead of pretending.
                if params.force && params.task.is_some() {
                    return Outcome::Reply(Response::err(
                        id,
                        codes::BAD_PARAMS,
                        "--force bypasses the landing pipeline and cannot carry --task identity \
                         — drop --task, or land without --force so the explicit task is \
                         validated and recorded",
                    ));
                }
                let result = if params.force {
                    self.supervisor
                        .land_force(
                            std::path::Path::new(&params.repo),
                            &params.branch,
                            &params.target,
                            params.keep_branch,
                            params.reason.as_deref().unwrap_or_default(),
                        )
                        .await
                } else {
                    self.supervisor
                        .land(
                            std::path::Path::new(&params.repo),
                            &params.branch,
                            &params.target,
                            params.keep_branch,
                            params.task,
                        )
                        .await
                };
                reply(match result {
                    Ok(value) => Response::ok(id, value),
                    Err(error) => Response::err(id, codes::INTERNAL, error.to_string()),
                })
            }
            "repo.list" => reply(match self.repos.lock() {
                Ok(reg) => Response::ok(id, json!({"repos": reg.list()})),
                Err(_) => Response::err(id, codes::INTERNAL, "repo registry lock poisoned"),
            }),
            "repo.get" => reply(self.handle_repo_get(req)),
            "repo.onboard.start" => reply(self.handle_onboarding_start(req).await),
            "repo.onboard.propose" => reply(self.handle_onboarding_propose(req)),
            "repo.onboard.approve" => reply(self.handle_onboarding_approve(req)),
            "repo.onboard.decline" => reply(self.handle_onboarding_decline(req)),
            "repo.onboard.apply" => reply(self.handle_onboarding_apply(req).await),
            "repo.onboard.activate" => reply(self.handle_onboarding_activate(req).await),
            "repo.onboard.decline_activation" => {
                reply(self.handle_onboarding_decline_activation(req).await)
            }
            "repo.onboard.cleanup" => reply(self.handle_onboarding_cleanup(req).await),
            "repo.onboard.resume" => reply(self.handle_onboarding_resume(req).await),
            "repo.onboard.status" => reply(self.handle_onboarding_status(req)),
            "repo.onboard.report" => reply(self.handle_onboarding_report(req)),
            "repo.onboard.inspect" => {
                let params: RepoInspectParams = match parse_params(&req.params) {
                    Ok(params) => params,
                    Err(error) => {
                        return Outcome::Reply(Response::err(id, codes::BAD_PARAMS, error));
                    }
                };
                let registered = match self.repos.lock() {
                    Ok(registry) => registry.list(),
                    Err(_) => {
                        return Outcome::Reply(Response::err(
                            id,
                            codes::INTERNAL,
                            "repo registry lock poisoned",
                        ));
                    }
                };
                let context = crate::onboarding::InspectContext {
                    default_harness: self.default_harness.clone(),
                    require_named_checks: self.require_named_checks,
                };
                let result = tokio::task::spawn_blocking(move || {
                    crate::onboarding::inspect(&params.target, &registered, &context)
                })
                .await;
                reply(match result {
                    Ok(report) => Response::ok(id, json!({"report": report})),
                    Err(error) => Response::err(
                        id,
                        codes::INTERNAL,
                        format!("repository inspection task failed: {error}"),
                    ),
                })
            }
            "ticket.new" => reply(self.handle_ticket_new(req).await),
            "ticket.list" => reply(self.handle_ticket_list(req)),
            "ticket.get" => reply(self.handle_ticket_get(req)),
            "ticket.update" => reply(self.handle_ticket_update(req).await),
            "ticket.dep" => reply(self.handle_ticket_dep(req).await),
            "ticket.reopen" => reply(self.handle_ticket_reopen(req).await),
            "ticket.ready" => reply(self.handle_ticket_ready(req)),
            other => reply(Response::err(
                id,
                codes::UNKNOWN_METHOD,
                format!("unknown method: {other}"),
            )),
        }
    }

    /// Resolve the optional inbox repository filter the same way for every
    /// read path. Callers may supply either a registered name or its path.
    fn resolve_inbox_repo(&self, requested: Option<String>) -> rk_core::Result<Option<String>> {
        requested
            .map(|filter| {
                let registry = self
                    .repos
                    .lock()
                    .map_err(|_| rk_core::Error::other("repo registry lock poisoned"))?;
                let by_name = registry.get(&filter);
                let by_path = std::fs::canonicalize(&filter)
                    .ok()
                    .and_then(|path| registry.get_by_path(&path));
                Ok::<_, rk_core::Error>(
                    by_name
                        .or(by_path)
                        .map(|record| record.name.clone())
                        .unwrap_or(filter),
                )
            })
            .transpose()
    }

    /// Union everything awaiting a human — failed/orphaned agents, failed or
    /// gate-parked workflow instances, obstacle and need tuples, and open PRs
    /// awaiting review — into one ranked triage list. Pure read-side
    /// aggregation; no new storage.
    async fn inbox_value(&self, requested_repo: Option<String>) -> rk_core::Result<Value> {
        let repo = self.resolve_inbox_repo(requested_repo)?;
        let agents = self
            .supervisor
            .list()
            .into_iter()
            .filter(|agent| repo.as_deref().is_none_or(|repo| agent.repo_name == repo))
            .collect::<Vec<_>>();
        let instances = self
            .engine()
            .list()
            .into_iter()
            .filter(|instance| repo.as_deref().is_none_or(|repo| instance.repo == repo))
            .collect::<Vec<_>>();
        let mut source_truncated = false;
        let mut scan = |pattern: Pattern| {
            let pattern = match repo.as_deref() {
                Some(repo) => pattern.scope(repo),
                None => pattern,
            };
            let mut tuples = self
                .space
                .scan_newest_limited(&pattern, MAX_SCAN_TUPLES.saturating_add(1));
            if let Ok(rows) = &mut tuples {
                if rows.len() > MAX_SCAN_TUPLES {
                    source_truncated = true;
                    rows.truncate(MAX_SCAN_TUPLES);
                }
            }
            tuples
        };
        let obstacles = match scan(Pattern::category(Category::Obstacle)) {
            Ok(t) => t,
            Err(e) => return Err(e),
        };
        let needs = match scan(Pattern::category(Category::Need)) {
            Ok(t) => t,
            Err(e) => return Err(e),
        };
        // Open PRs/MRs: a PR-mode landing emits a `pull_request_opened`
        // event, then the run completes — nothing else tracks the pushed branch.
        let pull_requests =
            match scan(Pattern::category(Category::Event).identity("pull_request_opened")) {
                Ok(t) => t,
                Err(e) => return Err(e),
            };
        // `pull_request_closed` events are emitted by the fetch-driven review
        // sweep (TKT-70): a background pass fetched the forge and saw the branch
        // merged/deleted upstream even though the operator never pulled, so the
        // LOCAL detection below could not see it. `build` folds their branches
        // into the same suppression. Reading the events is cheap and stays on
        // the hot path; the fetch that produces them does not.
        let pull_requests_closed =
            match scan(Pattern::category(Category::Event).identity("pull_request_closed")) {
                Ok(t) => t,
                Err(e) => return Err(e),
            };
        // Every `land` step records its own outcome as a `branch_landed` event.
        // A land that neither merged nor opened a PR left the branch standing
        // outside the target, and reports that as a clean `{merged: false}`
        // rather than an error — so unless the workflow definition happened to
        // carry an `evaluate {expect: {merged: true}}` after its `land`, the
        // drop is silent (TKT-171). `build` asserts the invariant here instead,
        // for every workflow.
        let lands = match scan(Pattern::category(Category::Event).identity("branch_landed")) {
            Ok(t) => t,
            Err(e) => return Err(e),
        };
        // Reduce the land events to the branches that are actually candidate
        // rows BEFORE the git check below. `branch_landed` accumulates one event
        // per land the fleet has ever performed and never shrinks, while the
        // drops are a handful; the git check is a subprocess per branch and this
        // is the hot read path behind `rk top`.
        let lands: Vec<Tuple> = crate::inbox::dropped_lands(&lands)
            .into_iter()
            .cloned()
            .collect();
        // Open ballots (TKT-167). A `Suggestion` promotes to a permanent
        // `Convention` at quorum and otherwise stays open indefinitely (durable
        // since TKT-168), and nothing else in the fleet announces a vote — so a
        // proposal is only ever endorsed by a peer who goes looking for one it
        // has no reason to suspect exists. Measured 2026-07-25: zero conventions
        // had ever reached quorum over 277 spawns. Surfacing the ballot here
        // puts the always-reachable endorser — the operator — in front of it.
        // The three scans are read-side only; `build` does the counting.
        // `Withdrawal` is the fourth: the other settled state, and the only one
        // that retires a row now that a ballot no longer decays (TKT-184).
        let mut ballot_tuples = |category| scan(Pattern::category(category));
        let (suggestions, endorsements, conventions, withdrawals) = match (
            ballot_tuples(Category::Suggestion),
            ballot_tuples(Category::Endorsement),
            ballot_tuples(Category::Convention),
            ballot_tuples(Category::Withdrawal),
        ) {
            (Ok(s), Ok(e), Ok(c), Ok(w)) => (s, e, c, w),
            (Err(e), _, _, _) | (_, Err(e), _, _) | (_, _, Err(e), _) | (_, _, _, Err(e)) => {
                return Err(e);
            }
        };
        // Both branch-shaped rows auto-clear once their branch is merged into
        // the target (or gone), and nothing emits a record when that happens —
        // no close event when a human merges a PR on the forge, and nothing at
        // all when a human hand-merges a dropped land. Detect it locally:
        // resolve each event's repo and ask git whether the branch has landed.
        // Local-only (no fetch, no forge API), so a row clears when the merge
        // reaches the local target — the operator's pull or a Direct-mode
        // fast-forward. The `pull_request_closed` events above close the same
        // gap for a forge merge the operator has NOT pulled (TKT-70).
        let cleared = match self.cleared_branches(&[&pull_requests, &lands]).await {
            Ok(cleared) => cleared,
            Err(e) => return Err(e),
        };
        // Automated recovery-action escalations (B2) and their acks. `rk
        // inbox` surfaces an unacked one via `recovery_action_rows`, a plain
        // function rather than a `build` input — see its doc comment for why.
        let recovery_actions = match scan(
            Pattern::category(Category::Event).identity(crate::recovery::RECOVERY_ACTION_IDENTITY),
        ) {
            Ok(t) => t,
            Err(e) => return Err(e),
        };
        let recovery_acks = match scan(
            Pattern::category(Category::Event).identity(crate::recovery::INBOX_ACK_IDENTITY),
        ) {
            Ok(t) => t,
            Err(e) => return Err(e),
        };
        let mut items = crate::inbox::build(
            &agents,
            &instances,
            &obstacles,
            &needs,
            &crate::inbox::BranchEvents {
                cleared,
                pull_requests: &pull_requests,
                pull_requests_closed: &pull_requests_closed,
                lands: &lands,
            },
            &crate::inbox::Ballots {
                suggestions: &suggestions,
                endorsements: &endorsements,
                conventions: &conventions,
                withdrawals: &withdrawals,
                // The reactor's own quorum, so the tally shown is the tally that
                // promotes — and a configured 0 (promotion disabled) raises no
                // rows rather than offering a vote that can never resolve.
                quorum: self.reactor_config.quorum as usize,
                now: chrono::Utc::now(),
            },
        );
        items.extend(crate::inbox::recovery_action_rows(
            &recovery_actions,
            &recovery_acks,
        ));
        // Landing-queue staleness (probe O18): computed live over the current
        // queue, same as the branch-shaped rows above — no ack, self-clears
        // the moment the oldest entry drains. Scoped to `repo` like every
        // other source above, so `rk inbox --repo X` never shows another
        // repo's queue.
        let landing_queue_summary: Vec<_> = crate::landing::landing_queue_summary(&self.space)
            .into_iter()
            .filter(|q| repo.as_deref().is_none_or(|repo| q.repo == repo))
            .collect();
        items.extend(crate::inbox::stalled_landing_queue_rows(
            &landing_queue_summary,
            self.landing_queue_config.stale_after_secs,
        ));
        items.sort_by_key(|b| std::cmp::Reverse(b.urgency));
        let mut response_truncated = source_truncated || items.len() > MAX_INBOX_ITEMS;
        items.truncate(MAX_INBOX_ITEMS);
        // A single tuple can carry a large operator-authored detail string, so
        // the item count cap alone does not prove the serialized response fits
        // the NDJSON frame. Drop lowest-priority tail rows until it does.
        while serde_json::to_vec(&json!({
            "items": &items,
            "truncated": response_truncated,
        }))
        .map(|bytes| bytes.len() > MAX_FRAME_BYTES)
        .unwrap_or(true)
        {
            if items.pop().is_none() {
                break;
            }
            response_truncated = true;
        }
        Ok(json!({"items": items, "truncated": response_truncated}))
    }

    /// Assemble the cross-ledger convergence report (`crate::reconcile`) for
    /// one repository: scan the durable views scoped to it, ask git the
    /// questions `reconcile::build` needs pre-answered, then hand everything
    /// to that pure function. Read-only throughout — every git call below is
    /// one of `rk_git::Repo`'s ancestry reads, never a mutation.
    async fn reconcile_value(&self, requested_repo: String) -> rk_core::Result<Value> {
        let report = self.reconcile_report(requested_repo).await?;
        Ok(crate::reconcile::to_json(&report))
    }

    /// The typed report `reconcile_value` renders to JSON — factored out so
    /// `crate::attention` (TKT-01M0E8PN9C41BWECGNW0990R3J) can consume the
    /// same live `Violation`s the operator-facing `reconcile.report` shows,
    /// rather than re-deriving a second, disconnected view of "what needs
    /// attention".
    async fn reconcile_report(
        &self,
        requested_repo: String,
    ) -> rk_core::Result<crate::reconcile::ConvergenceReport> {
        let repo = self
            .resolve_inbox_repo(Some(requested_repo))?
            .ok_or_else(|| rk_core::Error::other("repo is required"))?;

        let agents: Vec<crate::agents::AgentRecord> = self
            .supervisor
            .list_all()
            .into_iter()
            .filter(|a| a.repo_name == repo)
            .collect();

        let tickets = self.tickets.list(Some(repo.clone()), None, None)?;

        let lands_pattern = Pattern::category(Category::Event)
            .identity("branch_landed")
            .scope(repo.clone());
        let raw_lands = self
            .space
            .scan_newest_limited(&lands_pattern, MAX_SCAN_TUPLES)?;
        let lands: Vec<Tuple> = crate::inbox::dropped_lands(&raw_lands)
            .into_iter()
            .cloned()
            .collect();

        // The same hand-off carve-outs `Server::ticket_reopen_sweep_at` uses:
        // a ticket already recorded as landed, or still in flight through the
        // landing queue, is not abandoned even if its owning agent has gone
        // non-live. Unscoped scans, matching the sweep — ticket ids are
        // globally unique ULIDs, so intersecting against this repo's own
        // tickets below cannot pick up another repo's landed/queued ids.
        let landed_tickets: HashSet<String> = self
            .space
            .scan(
                &Pattern::category(Category::Event)
                    .identity(crate::landing::LANDING_PROCESSED_IDENTITY),
            )
            .unwrap_or_default()
            .into_iter()
            .filter(|t| t.payload.get("outcome").and_then(Value::as_str) == Some("landed"))
            .filter_map(|t| {
                t.payload
                    .get("task")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .filter(|task| !task.is_empty())
            .collect();
        let queued_tickets = crate::landing::tasks_in_landing_queue(&self.space);

        // Both branch-shaped self-clearing checks reuse the exact machinery
        // `rk inbox` uses: the dropped-land half of `cleared_branches`, and a
        // dedicated ancestry check per ticket's own delivery record.
        let cleared_branches = self.cleared_branches(&[&lands]).await?;

        let delivered_pairs: HashSet<(String, String)> = tickets
            .iter()
            .filter_map(crate::tickets::delivery_of)
            .filter(|d| !d.merge_commit.is_empty())
            .map(|d| (d.merge_commit, d.target))
            .collect();
        let is_ancestor = self
            .merge_commit_ancestry(&repo, delivered_pairs.into_iter().collect())
            .await?;

        let git = crate::reconcile::GitFacts {
            is_ancestor,
            cleared_branches,
        };

        let instances: Vec<crate::workflow_exec::Instance> = self
            .engine()
            .list_all()
            .into_iter()
            .filter(|i| i.repo == repo)
            .collect();

        Ok(crate::reconcile::build(
            &repo,
            &tickets,
            &agents,
            &lands,
            &landed_tickets,
            &queued_tickets,
            &instances,
            &git,
        ))
    }

    /// Assemble a repair plan for the two mechanically-repairable
    /// convergence violations (`crate::reconcile_repair`) and either preview
    /// it (`apply = false`) or execute it (`apply = true`). Reuses the same
    /// ticket/agent/carve-out scans `reconcile_value` performs, plus one
    /// extra round of durable-evidence git checks a read-only report never
    /// needs to answer: whether a delivered commit touches a protected path,
    /// and whether its landed branch has since diverged from what was
    /// recorded — both gathered fresh on every call, never cached, so a
    /// repair can never act on stale evidence.
    async fn reconcile_repair_value(
        &self,
        requested_repo: String,
        apply: bool,
    ) -> rk_core::Result<Value> {
        let repo = self
            .resolve_inbox_repo(Some(requested_repo))?
            .ok_or_else(|| rk_core::Error::other("repo is required"))?;

        let agents: Vec<crate::agents::AgentRecord> = self
            .supervisor
            .list_all()
            .into_iter()
            .filter(|a| a.repo_name == repo)
            .collect();

        let tickets = self.tickets.list(Some(repo.clone()), None, None)?;

        // The same hand-off carve-outs `reconcile_value`/`Server::ticket_reopen_sweep_at`
        // use — see `reconcile_value` for the full rationale.
        let landed_tickets: HashSet<String> = self
            .space
            .scan(
                &Pattern::category(Category::Event)
                    .identity(crate::landing::LANDING_PROCESSED_IDENTITY),
            )
            .unwrap_or_default()
            .into_iter()
            .filter(|t| t.payload.get("outcome").and_then(Value::as_str) == Some("landed"))
            .filter_map(|t| {
                t.payload
                    .get("task")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .filter(|task| !task.is_empty())
            .collect();
        let queued_tickets = crate::landing::tasks_in_landing_queue(&self.space);

        let delivered_pairs: HashSet<(String, String)> = tickets
            .iter()
            .filter_map(crate::tickets::delivery_of)
            .filter(|d| !d.merge_commit.is_empty())
            .map(|d| (d.merge_commit, d.target))
            .collect();
        let is_ancestor = self
            .merge_commit_ancestry(&repo, delivered_pairs.into_iter().collect())
            .await?;
        let (protected_touch, diverged) = self.repair_git_facts(&repo, &tickets).await?;

        let facts = crate::reconcile_repair::RepairFacts {
            git: crate::reconcile::GitFacts {
                is_ancestor,
                cleared_branches: HashSet::new(),
            },
            protected_touch,
            diverged,
        };

        let plan = crate::reconcile_repair::plan(
            &repo,
            &tickets,
            &agents,
            &landed_tickets,
            &queued_tickets,
            &facts,
        );
        let report = if apply {
            // Re-fetch agents right here, immediately before the write: the
            // slice `plan` was built from can be seconds old by the time we
            // get here (two intervening git/ancestry awaits above), and
            // `apply`'s stale-ownership re-check is only as fresh as what we
            // hand it — never reuse the `agents` snapshot planning used.
            let fresh_agents: Vec<crate::agents::AgentRecord> = self
                .supervisor
                .list_all()
                .into_iter()
                .filter(|a| a.repo_name == repo)
                .collect();
            crate::reconcile_repair::apply(
                plan,
                &crate::reconcile_repair::ApplyContext {
                    tickets: &self.tickets,
                    space: &self.space,
                    castle: &self.castle,
                    agents: &fresh_agents,
                },
            )
            .await?
        } else {
            crate::reconcile_repair::dry_run(plan)
        };
        Ok(crate::reconcile_repair::to_json(&report))
    }

    /// The two durable-evidence git checks `reconcile_repair_value` needs
    /// beyond ancestry: `merge_commit -> does its diff touch a protected
    /// path?` and `(scope, branch) -> has the branch diverged from what
    /// landed?`. An unregistered/unopenable repo, or a ticket with no
    /// delivery record at all, returns empty maps — "cannot check", read by
    /// `reconcile_repair::plan` as missing evidence, never as a clean bill.
    async fn repair_git_facts(
        &self,
        repo: &str,
        tickets: &[Tuple],
    ) -> rk_core::Result<(HashMap<String, bool>, HashMap<(String, String), bool>)> {
        let records: Vec<crate::tickets::DeliveryRecord> = tickets
            .iter()
            .filter_map(crate::tickets::delivery_of)
            .filter(|d| !d.merge_commit.is_empty())
            .collect();
        if records.is_empty() {
            return Ok((HashMap::new(), HashMap::new()));
        }
        let path = {
            let reg = self
                .repos
                .lock()
                .map_err(|_| rk_core::Error::other("repo registry lock poisoned"))?;
            reg.get(repo).map(|r| r.path.clone())
        };
        let Some(path) = path else {
            return Ok((HashMap::new(), HashMap::new()));
        };
        let scope = repo.to_string();
        let supervisor = Arc::clone(&self.supervisor);
        tokio::task::spawn_blocking(move || {
            let mut protected_touch = HashMap::new();
            let mut diverged = HashMap::new();
            let Ok(git_repo) = rk_git::Repo::discover(&path) else {
                return (protected_touch, diverged);
            };
            let protected_paths = supervisor
                .repository_policy(&git_repo)
                .landing
                .protected_paths;
            for record in records {
                if let Some(touch) =
                    touches_protected_path(&git_repo, &record.merge_commit, &protected_paths)
                {
                    protected_touch
                        .entry(record.merge_commit.clone())
                        .or_insert(touch);
                }
                if git_repo.branch_exists(&record.branch) {
                    if let Ok(tip) = git_repo.rev_parse(&record.branch) {
                        let still_ancestor = git_repo.is_ancestor(&tip, &record.merge_commit);
                        diverged.insert((scope.clone(), record.branch.clone()), !still_ancestor);
                    }
                }
            }
            (protected_touch, diverged)
        })
        .await
        .map_err(|e| rk_core::Error::other(format!("repair git facts panicked: {e}")))
    }

    /// `(merge_commit, target) -> is merge_commit an ancestor of target?` for
    /// every pair in `pairs`, resolved against the repo registered as `repo`.
    /// An unregistered or unopenable repo returns an empty map — "cannot
    /// check", which `reconcile::build` reads as no evidence rather than a
    /// contradiction — instead of erroring the whole report over one bad
    /// registration.
    async fn merge_commit_ancestry(
        &self,
        repo: &str,
        pairs: Vec<(String, String)>,
    ) -> rk_core::Result<HashMap<(String, String), bool>> {
        if pairs.is_empty() {
            return Ok(HashMap::new());
        }
        let path = {
            let reg = self
                .repos
                .lock()
                .map_err(|_| rk_core::Error::other("repo registry lock poisoned"))?;
            reg.get(repo).map(|r| r.path.clone())
        };
        let Some(path) = path else {
            return Ok(HashMap::new());
        };
        tokio::task::spawn_blocking(move || {
            let mut result = HashMap::new();
            let Ok(git_repo) = rk_git::Repo::discover(&path) else {
                return result;
            };
            for (commit, target) in pairs {
                let verdict = git_repo.is_ancestor(&commit, &target);
                result.insert((commit, target), verdict);
            }
            result
        })
        .await
        .map_err(|e| rk_core::Error::other(format!("git ancestry check panicked: {e}")))
    }

    async fn handle_inbox(&self, req: Request) -> Response {
        let params: InboxParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        match self.inbox_value(params.repo).await {
            Ok(value) => Response::ok(req.id, value),
            Err(error) => Response::err(req.id, codes::INTERNAL, error.to_string()),
        }
    }

    /// `inbox.ack` (B2) — durably close out a `recovery-action` inbox row so
    /// the re-notify sweep (`crate::recovery::renotify_sweep`) stops pushing
    /// it. Sink-agnostic by design: this is the one path a human `rk inbox
    /// ack` and a future rat-king sink both go through, mirroring
    /// `space.withdraw`'s own RPC-not-`space.out` shape — writing the ack
    /// directly here, rather than accepting an `Ack` tuple through
    /// `handle_out`, is what lets this stay idempotent (a re-run reports
    /// `already: true` instead of failing) without a caller having to scan
    /// first.
    fn handle_inbox_ack(&self, req: Request) -> Response {
        let params: InboxAckParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let id = params.id.trim().to_string();
        let record_id: RecordId = match id.parse() {
            Ok(id) => id,
            Err(_) => {
                return Response::err(
                    req.id,
                    codes::BAD_PARAMS,
                    format!("`{id}` is not a valid tuple id"),
                )
            }
        };
        let target = match self.space.get(record_id) {
            Ok(Some(t)) => t,
            Ok(None) => return Response::err(req.id, codes::BAD_PARAMS, format!("no tuple {id}")),
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        if target.category != Category::Event
            || target.identity != crate::recovery::RECOVERY_ACTION_IDENTITY
        {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!("{id} is not a recovery-action escalation"),
            );
        }
        let mut already =
            Pattern::category(Category::Event).identity(crate::recovery::INBOX_ACK_IDENTITY);
        already.payload_search = Some(format!("\"tuple\":\"{id}\""));
        match self.space.has_persistence_event_matching(&already) {
            Ok(true) => {
                return Response::ok(
                    req.id,
                    json!({"acked": id, "already": true, "written": false}),
                )
            }
            Ok(false) => {}
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
        let caller = req.caller.clone();
        let by = if caller.is_empty() {
            OPERATOR_ACTOR.to_string()
        } else {
            caller
        };
        let ack = Tuple::new(
            Category::Event,
            target.scope.clone(),
            crate::recovery::INBOX_ACK_IDENTITY,
            by.clone(),
            json!({"tuple": id, "acked_by": by}),
        )
        .with_lifecycle(Lifecycle::Furniture);
        match self.space.out(ack) {
            Ok(()) => Response::ok(
                req.id,
                json!({"acked": id, "already": false, "written": true, "by": by}),
            ),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    /// `reconcile.report` — the cross-ledger convergence report for one
    /// repository (`crate::reconcile`). Read-only: no tuple is written and no
    /// state changes, so repeated calls over unchanged state return identical
    /// output.
    async fn handle_reconcile(&self, req: Request) -> Response {
        let params: ReconcileParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        match self.reconcile_value(params.repo).await {
            Ok(value) => Response::ok(req.id, value),
            Err(error) => Response::err(req.id, codes::INTERNAL, error.to_string()),
        }
    }

    /// `lease.acquire` — TKT-01M0E8PN9C41BWECGNW0990R3J: a primed external
    /// orchestrator session's entry point. The SAME `holder` calling again
    /// (after a disconnect or a daemon restart) resumes its generation and
    /// cursor untouched; a DIFFERENT holder may only take over once the
    /// existing lease has expired.
    async fn handle_lease_acquire(&self, req: Request) -> Response {
        let params: LeaseAcquireParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let repo = match self.resolve_inbox_repo(Some(params.repo)) {
            Ok(Some(r)) => r,
            Ok(None) => return Response::err(req.id, codes::BAD_PARAMS, "repo is required"),
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
        };
        let ttl = params
            .ttl_secs
            .unwrap_or(self.authority_policy.lease_ttl_secs);
        match self
            .orchestrator_lease
            .acquire(&repo, &params.holder, ttl, (self.request_clock)())
        {
            Ok(lease) => Response::ok(req.id, json!(lease)),
            Err(e) => Response::err(req.id, codes::FORBIDDEN, e.to_string()),
        }
    }

    /// `lease.renew` — extend a held lease's TTL without disturbing its
    /// generation or cursor. Fenced identically to `attention.decide`: a
    /// stale generation (this holder has since been replaced) is refused.
    async fn handle_lease_renew(&self, req: Request) -> Response {
        let params: LeaseRenewParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let repo = match self.resolve_inbox_repo(Some(params.repo)) {
            Ok(Some(r)) => r,
            Ok(None) => return Response::err(req.id, codes::BAD_PARAMS, "repo is required"),
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
        };
        let ttl = params
            .ttl_secs
            .unwrap_or(self.authority_policy.lease_ttl_secs);
        match self.orchestrator_lease.renew(
            &repo,
            &params.holder,
            params.generation,
            ttl,
            (self.request_clock)(),
        ) {
            Ok(lease) => Response::ok(req.id, json!(lease)),
            Err(e) => Response::err(req.id, codes::FORBIDDEN, e.to_string()),
        }
    }

    /// `attention.next` — the next resumable attention item for one
    /// repository: the freshest `reconcile.report` for it, consumed against
    /// the repo's current lease cursor (or from the very start if no lease
    /// has ever been acquired). Read-only, like `reconcile.report` itself.
    async fn handle_attention_next(&self, req: Request) -> Response {
        let params: AttentionNextParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let report = match self.reconcile_report(params.repo).await {
            Ok(r) => r,
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        let cursor = match self.orchestrator_lease.current(&report.scope) {
            Ok(lease) => lease.and_then(|l| l.cursor),
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        let item =
            crate::attention::next_attention(&report, &self.authority_policy, cursor.as_deref());
        Response::ok(req.id, json!({"repo": report.scope, "item": item}))
    }

    /// `attention.decide` — TKT-01M0E8PN9C41BWECGNW0990R3J: resolve one
    /// attention item, dispatched by its effective authority.
    ///
    /// * `Human` — refused outright. Nothing is written, nothing is executed
    ///   — "pauses with no side effect" holds exactly because this arm never
    ///   reaches the journal write below.
    /// * `Mechanical` — the durable record already proves the fix
    ///   (`crate::attention::mechanical_action_for`); no lease, no LLM, no
    ///   rate cap.
    /// * `Orchestrator` — requires a live, fenced lease over this repo and a
    ///   kind the castle's `orchestrator_action_allowlist` names explicitly,
    ///   and is rate-capped fleet-wide through the SAME `RecoveryAnnouncer`
    ///   every other automated recovery source in this daemon uses, so a
    ///   rate-held decision is announced (visible in `rk inbox`) exactly
    ///   like a held mechanical recovery action, not silently dropped.
    ///
    /// Every path checks the durable decision journal FIRST: a violation id
    /// that already has a recorded decision returns it verbatim as a replay
    /// — the repair function is never called a second time, which is what
    /// makes replaying a fixture unable to repeat a mutation.
    async fn handle_attention_decide(&self, req: Request) -> Response {
        let params: AttentionDecideParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match self.attention_decide(params).await {
            Ok(value) => Response::ok(req.id, value),
            Err(AttentionDecideError::Refused(msg)) => Response::err(req.id, codes::FORBIDDEN, msg),
            Err(AttentionDecideError::BadParams(msg)) => {
                Response::err(req.id, codes::BAD_PARAMS, msg)
            }
            Err(AttentionDecideError::Internal(msg)) => Response::err(req.id, codes::INTERNAL, msg),
        }
    }

    async fn attention_decide(
        &self,
        params: AttentionDecideParams,
    ) -> Result<Value, AttentionDecideError> {
        let repo = self
            .resolve_inbox_repo(Some(params.repo))
            .map_err(|e| AttentionDecideError::Internal(e.to_string()))?
            .ok_or_else(|| AttentionDecideError::BadParams("repo is required".to_string()))?;

        // Consult the durable decision journal BEFORE rebuilding the report:
        // a mechanical/orchestrator repair can be self-clearing (the
        // violation it fixed no longer appears in a FRESH
        // `reconcile.report`), so checking "is this violation still live"
        // first would make a resumed/replayed `attention.decide` for an
        // already-terminal item look like "not found" instead of returning
        // the terminal decision it already recorded.
        if let Some(existing) = self
            .find_decision(&repo, &params.item)
            .map_err(|e| AttentionDecideError::Internal(e.to_string()))?
        {
            // Mirror the ORIGINAL decision's own shape rather than assuming
            // "found a record" means "resolved": a human gate that already
            // fired records `resolved: false, gated: true` (zero mutation,
            // still paused) and a replay must say the same thing again, not
            // claim the item was resolved just because a durable record for
            // it exists.
            let resolved = existing
                .get("resolved")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            let gated = existing
                .get("gated")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            return Ok(
                json!({"resolved": resolved, "replay": true, "gated": gated, "decision": existing}),
            );
        }

        let report = self
            .reconcile_report(repo.clone())
            .await
            .map_err(|e| AttentionDecideError::Internal(e.to_string()))?;
        let Some(violation) = report.violations.iter().find(|v| v.id == params.item) else {
            return Ok(json!({
                "resolved": false,
                "replay": false,
                "reason": "attention item not found: already resolved, or state has changed since it was surfaced",
            }));
        };
        let authority = self.authority_policy.effective_authority(violation);

        match authority {
            crate::reconcile::Authority::Human => {
                // Refused with ZERO mutation: no tuple is written on this arm
                // at all, so "pauses with no side effect" holds exactly and
                // a replayed call for the same violation id simply re-evaluates
                // (nothing was ever journaled to replay) and is refused again.
                let message = match crate::attention::human_gate_for(violation) {
                    Some(gate) => format!(
                        "{} is human-gated ({}): no automated action is permitted — {}. \
                         Blast radius: {}. Resolving action: {}",
                        violation.id,
                        violation.detail,
                        gate.requested_decision,
                        gate.blast_radius,
                        gate.resolving_action
                    ),
                    None => format!(
                        "{} is human-gated ({}): no automated action is permitted, and no \
                         human-gate template is registered for kind {}",
                        violation.id, violation.detail, violation.kind
                    ),
                };
                Err(AttentionDecideError::Refused(message))
            }
            crate::reconcile::Authority::Mechanical => {
                let Some(action) = crate::attention::mechanical_action_for(violation) else {
                    return Err(AttentionDecideError::BadParams(format!(
                        "no mechanical repair is registered for kind {}",
                        violation.kind
                    )));
                };
                // Durable INTENT written BEFORE the mutation: a crash between
                // `execute_mechanical` actually applying and the terminal
                // record below used to leave zero trace at all, so a resumed
                // caller had no way to tell "already attempted" from "never
                // tried" other than blindly calling the repair again. This
                // record makes that window observable. It cannot itself make
                // the repair exactly-once — that guarantee comes from
                // `execute_mechanical` calling `Tickets::set_status`, a CAS
                // that is safe to invoke twice — but it is what lets a
                // resumed decide converge on exactly one TERMINAL record
                // instead of silently retrying with no audit trail.
                self.record_decision(
                    &repo,
                    violation,
                    authority,
                    crate::attention::DECIDED_BY_MECHANICAL,
                    action,
                    None,
                    None,
                    "attempting",
                    false,
                    false,
                )
                .map_err(|e| AttentionDecideError::Internal(e.to_string()))?;
                let outcome = self.execute_mechanical(violation).await;
                let succeeded = outcome.is_ok();
                let outcome_str = match &outcome {
                    Ok(detail) => detail.clone(),
                    Err(e) => format!("error: {e}"),
                };
                let decision = self
                    .record_decision(
                        &repo,
                        violation,
                        authority,
                        crate::attention::DECIDED_BY_MECHANICAL,
                        action,
                        None,
                        None,
                        &outcome_str,
                        succeeded,
                        succeeded,
                    )
                    .map_err(|e| AttentionDecideError::Internal(e.to_string()))?;
                outcome.map_err(|e| AttentionDecideError::Internal(e.to_string()))?;
                Ok(json!({"resolved": true, "replay": false, "decision": decision}))
            }
            crate::reconcile::Authority::Orchestrator => {
                let holder = params.holder.ok_or_else(|| {
                    AttentionDecideError::BadParams("orchestrator decision requires holder".into())
                })?;
                let generation = params.generation.ok_or_else(|| {
                    AttentionDecideError::BadParams(
                        "orchestrator decision requires generation".into(),
                    )
                })?;
                if !self.authority_policy.orchestrator_may_act(&violation.kind) {
                    return Err(AttentionDecideError::Refused(format!(
                        "kind {} is not in the castle's orchestrator_action_allowlist",
                        violation.kind
                    )));
                }
                let Some(action) = crate::attention::orchestrator_action_for(violation) else {
                    return Err(AttentionDecideError::BadParams(format!(
                        "no orchestrator repair is registered for kind {}",
                        violation.kind
                    )));
                };
                let now = (self.request_clock)();
                self.orchestrator_lease
                    .renew(
                        &repo,
                        &holder,
                        generation,
                        self.authority_policy.lease_ttl_secs,
                        now,
                    )
                    .map_err(|e| AttentionDecideError::Refused(e.to_string()))?;

                let sinks = crate::reactor::sink_factory().registry(
                    self.notify_config
                        .resolved(self.reactor_config.notify_escalations),
                );
                let notice = rk_core::notify::EscalationNotice::new(
                    format!("{}@{}", violation.id, holder),
                    "orchestrator-action",
                    rk_core::notify::Severity::Warn,
                    repo.clone(),
                    violation.subject.clone(),
                    format!(
                        "orchestrator {holder} resolving {}: {}",
                        violation.id, violation.detail
                    ),
                );
                let announced = self
                    .recovery_announcer
                    .announce(
                        &self.space,
                        &sinks,
                        crate::recovery::RecoveryAction {
                            kind: "orchestrator-action".into(),
                            instance: holder.clone(),
                            notice,
                        },
                        self.authority_policy.rate_cap,
                    )
                    .map_err(|e| AttentionDecideError::Internal(e.to_string()))?;

                // Held (rate-capped) and a genuine execution failure are
                // both NON-terminal: neither actually mutated anything, so
                // neither may be recorded in a way that blocks a later
                // `attention.decide` call for this same violation from
                // trying again. Only `succeeded` may set `terminal: true` —
                // that is what actually makes "replaying a fixture cannot
                // repeat a mutation" true (nothing to repeat, because a
                // held/failed attempt is not journaled as done) while ALSO
                // not silently losing an orchestrator action that errored
                // (previously this recorded success and advanced the cursor
                // past it regardless of whether `execute_orchestrator`
                // actually returned `Ok`).
                let held = announced.held();
                let exec_result = if held {
                    None
                } else {
                    // Durable INTENT before the mutation — the same
                    // crash-safety reasoning as the mechanical arm above: a
                    // resumed caller sees this even if the daemon dies
                    // between `execute_orchestrator` applying and the
                    // terminal record below.
                    self.record_decision(
                        &repo,
                        violation,
                        authority,
                        &holder,
                        action,
                        params.budget_usd,
                        params.budget_tokens,
                        "attempting",
                        false,
                        false,
                    )
                    .map_err(|e| AttentionDecideError::Internal(e.to_string()))?;
                    Some(self.execute_orchestrator(violation).await)
                };
                let outcome_str = match &exec_result {
                    None => "held: fleet-wide orchestrator rate cap exceeded".to_string(),
                    Some(Ok(detail)) => detail.clone(),
                    Some(Err(e)) => format!("error: {e}"),
                };
                let succeeded = matches!(exec_result, Some(Ok(_)));
                let failed = matches!(exec_result, Some(Err(_)));
                let decision = self
                    .record_decision(
                        &repo,
                        violation,
                        authority,
                        &holder,
                        action,
                        params.budget_usd,
                        params.budget_tokens,
                        &outcome_str,
                        succeeded,
                        succeeded,
                    )
                    .map_err(|e| AttentionDecideError::Internal(e.to_string()))?;
                if succeeded {
                    self.orchestrator_lease
                        .advance_cursor(&repo, &holder, generation, &violation.id, now)
                        .map_err(|e| AttentionDecideError::Internal(e.to_string()))?;
                }
                if failed {
                    return Err(AttentionDecideError::Internal(outcome_str));
                }
                Ok(json!({
                    "resolved": succeeded,
                    "replay": false,
                    "held": held,
                    "decision": decision,
                }))
            }
        }
    }

    /// The one registered mechanical repair this tracer bullet wires up:
    /// `delivered-but-open`'s own doc comment names the fix — the delivery
    /// record is the durable proof, so the ticket's status is what is wrong.
    async fn execute_mechanical(&self, v: &crate::reconcile::Violation) -> rk_core::Result<String> {
        match v.kind.as_str() {
            crate::reconcile::kind::DELIVERED_BUT_OPEN => {
                self.tickets.set_status(&v.subject, "closed").await?;
                Ok(format!("{} set to closed", v.subject))
            }
            other => Err(rk_core::Error::other(format!(
                "no mechanical repair implemented for kind {other}"
            ))),
        }
    }

    /// The one registered orchestrator repair this tracer bullet wires up:
    /// hand the ticket back to the backlog via the SAME atomic CAS the B9
    /// orphaned-ticket sweep uses, so a live rat can redispatch it.
    async fn execute_orchestrator(
        &self,
        v: &crate::reconcile::Violation,
    ) -> rk_core::Result<String> {
        match v.kind.as_str() {
            crate::reconcile::kind::TERMINAL_ASSIGNEE_ACTIVE_WORK => {
                let reopened = self.tickets.reopen_if_in_progress(&v.subject).await?;
                Ok(format!("{} reopened: {reopened}", v.subject))
            }
            other => Err(rk_core::Error::other(format!(
                "no orchestrator repair implemented for kind {other}"
            ))),
        }
    }

    /// Durably record one attention decision (evidence, selected action,
    /// budget use, outcome) — this IS the acknowledgement the acceptance
    /// criteria describe. Called TWICE per real attempt: once as a
    /// non-`terminal` "attempting" intent before the mutation runs (so a
    /// crash between the mutation applying and this function's own SECOND,
    /// terminal call still leaves a durable trace instead of none at all),
    /// and once after, with the real outcome.
    ///
    /// `resolved`/`terminal` are governed by the SAME boolean by every
    /// current caller (a decision is only ever terminal when it actually
    /// resolved something), but they are kept as two separate fields because
    /// they answer different questions: `resolved` is what a caller's
    /// response should say happened; `terminal` is only consulted by
    /// `find_decision`, which is what makes replaying the same violation id
    /// return this record instead of acting again. A rate-held or genuinely
    /// failed attempt is NEITHER: `resolved: false` (nothing happened) and
    /// `terminal: false` (so a LATER `attention.decide` call — after the
    /// rate window passes, or simply retried — is free to try again rather
    /// than being permanently told "already decided" for an item nothing
    /// ever actually resolved).
    #[allow(clippy::too_many_arguments)]
    fn record_decision(
        &self,
        repo: &str,
        violation: &crate::reconcile::Violation,
        authority: crate::reconcile::Authority,
        decided_by: &str,
        action: &str,
        budget_usd: Option<f64>,
        budget_tokens: Option<u64>,
        outcome: &str,
        resolved: bool,
        terminal: bool,
    ) -> rk_core::Result<Value> {
        let payload = json!({
            "violation_id": violation.id,
            "kind": violation.kind,
            "scope": violation.scope,
            "subject": violation.subject,
            "authority": authority,
            "evidence": violation.evidence,
            "decided_by": decided_by,
            "action": action,
            "budget_usd": budget_usd,
            "budget_tokens": budget_tokens,
            "outcome": outcome,
            "resolved": resolved,
            "gated": false,
            "terminal": terminal,
            "decided_at": (self.request_clock)(),
        });
        let tuple = Tuple::new(
            Category::Event,
            repo,
            crate::attention::DECISION_IDENTITY,
            decided_by,
            payload.clone(),
        )
        // Permanent ledger entry, matching every other decision/escalation
        // record in this daemon (`recovery::RECOVERY_ACTION_IDENTITY`,
        // `action_approval`'s grants): the journal must not evaporate out
        // from under a caller replaying an old fixture.
        .with_lifecycle(Lifecycle::Furniture);
        self.space.out(tuple)?;
        Ok(payload)
    }

    /// The durably recorded TERMINAL decision for `violation_id`, if any —
    /// the idempotent-replay check every `attention.decide` arm consults
    /// before doing anything else. Deliberately skips a non-`terminal`
    /// record (an "attempting" intent, or a held/failed attempt): those
    /// exist for audit/crash-recovery visibility only and must never
    /// themselves count as "already decided", or a rate-capped or genuinely
    /// failed attempt would silently and permanently block every future
    /// retry of an item nothing ever actually resolved.
    /// `payload_search` narrows the scan; the exact field comparison after
    /// it guards against a substring match on a DIFFERENT violation id that
    /// happens to contain this one.
    fn find_decision(&self, repo: &str, violation_id: &str) -> rk_core::Result<Option<Value>> {
        let mut pattern = Pattern::category(Category::Event)
            .identity(crate::attention::DECISION_IDENTITY)
            .scope(repo.to_string());
        pattern.payload_search = Some(format!("\"violation_id\":\"{violation_id}\""));
        Ok(self
            .space
            .scan(&pattern)?
            .into_iter()
            .filter(|t| t.payload.get("violation_id").and_then(Value::as_str) == Some(violation_id))
            .find(|t| t.payload.get("terminal").and_then(Value::as_bool) == Some(true))
            .map(|t| t.payload))
    }

    /// `reconcile.repair` — dry-run or apply mechanical repair for the two
    /// convergence violations durable evidence alone proves and fixes
    /// (`crate::reconcile_repair`). Operator-only: unlike `reconcile.report`
    /// this can write tuples (a ticket status/assignee flip, and a durable
    /// journal/announcement event) when called with `apply: true`.
    async fn handle_reconcile_repair(&self, req: Request) -> Response {
        let params: ReconcileRepairParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        match self.reconcile_repair_value(params.repo, params.apply).await {
            Ok(value) => Response::ok(req.id, value),
            Err(error) => Response::err(req.id, codes::INTERNAL, error.to_string()),
        }
    }

    /// (scope, branch) pairs among the given branch-shaped events whose branch
    /// has since been merged into its target or deleted locally — the rows that
    /// have auto-cleared and must drop out of the inbox. Resolves each event's
    /// scope to a registered repo path and asks git; an unregistered scope or
    /// unopenable repo means "cannot tell", so the row stays (fails toward
    /// surfacing, never hiding).
    ///
    /// Both branch-shaped sources share this check, because they ask the same
    /// question of git — did this branch reach its target? — about the same
    /// `{branch, target}` payload shape: `pull_request_opened` events (a PR the
    /// human merged on the forge, TKT-67/70) and `branch_landed` events (a land
    /// that dropped its branch and was later resolved by any route, TKT-171).
    /// One git call per distinct branch covers both.
    async fn cleared_branches(
        &self,
        event_sets: &[&[Tuple]],
    ) -> rk_core::Result<HashSet<(String, String)>> {
        let events: Vec<(String, String, String, bool)> = event_sets
            .iter()
            .flat_map(|s| s.iter())
            .filter_map(|t| {
                let branch = t.payload.get("branch").and_then(|v| v.as_str())?;
                let target = t
                    .payload
                    .get("target")
                    .and_then(|v| v.as_str())
                    .unwrap_or("main");
                let content_proven = if t.identity == "pull_request_opened" {
                    match (
                        t.payload.get("fork_point").and_then(Value::as_str),
                        t.payload.get("head_sha").and_then(Value::as_str),
                    ) {
                        (Some(fork), Some(head)) => !fork.is_empty() && head != fork,
                        _ => false,
                    }
                } else {
                    t.payload.get("content_free").and_then(Value::as_bool) == Some(false)
                };
                Some((
                    t.scope.clone(),
                    branch.to_string(),
                    target.to_string(),
                    content_proven,
                ))
            })
            .collect();
        // Resolve scopes to paths once, under the registry lock, then release it
        // before shelling out to git. The actual Git calls run in a blocking
        // worker so a slow or locked repository cannot starve RPC handling.
        let paths = {
            let mut paths: HashMap<String, std::path::PathBuf> = HashMap::new();
            let reg = self
                .repos
                .lock()
                .map_err(|_| rk_core::Error::other("repo registry lock poisoned"))?;
            for (scope, _, _, _) in &events {
                if !paths.contains_key(scope) {
                    if let Some(rec) = reg.get(scope) {
                        paths.insert(scope.clone(), rec.path.clone());
                    }
                }
            }
            paths
        };
        tokio::task::spawn_blocking(move || cleared_branches_for_paths(events, paths))
            .await
            .map_err(|e| rk_core::Error::other(format!("inbox Git check task failed: {e}")))
    }

    /// One fetch-driven review-sweep cycle (TKT-70). For each repo carrying an
    /// open PR/MR whose branch has not already been closed, `git fetch --prune`
    /// the remote and ask whether the forge has since merged the branch into
    /// `<remote>/<target>` or deleted it — the case the local-only clear in
    /// [`cleared_pull_requests`](Daemon::cleared_pull_requests) misses because
    /// the operator never pulled. Each newly-resolved branch gets a durable
    /// `pull_request_closed` event, which `handle_inbox` folds into the
    /// awaiting-review suppression set. Returns the number of events emitted.
    ///
    /// Blocking (shells out to `git fetch`), so the caller runs it on a blocking
    /// thread; each fetch is hard-timeout-bounded. Idempotent: a branch already
    /// carrying a `pull_request_closed` event is skipped, so the durable event
    /// (and its rk-sync replication) is written once.
    fn review_sweep_once(&self) -> usize {
        let remote = self.review_sweep_config.remote.clone();
        let timeout = Duration::from_secs(self.review_sweep_config.fetch_timeout_secs.max(1));

        let open = match self
            .space
            .scan(&Pattern::category(Category::Event).identity("pull_request_opened"))
        {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "review sweep: scanning open PRs failed");
                return 0;
            }
        };
        if open.is_empty() {
            return 0;
        }
        let closed = self
            .space
            .scan(&Pattern::category(Category::Event).identity("pull_request_closed"))
            .unwrap_or_default();
        // (scope, branch) already resolved — never re-emit. The durable event
        // also replicates via rk-sync, so this guard keeps the sweep write-once.
        let mut already: HashSet<(String, String)> = HashSet::new();
        for t in &closed {
            if let Some(b) = t.payload.get("branch").and_then(|v| v.as_str()) {
                already.insert((t.scope.clone(), b.to_string()));
            }
        }
        // Still-open (scope, branch) -> target, deduped (a re-land repeats the
        // event for one branch; target is stable per branch).
        let mut pending: std::collections::HashMap<(String, String), (String, bool)> =
            std::collections::HashMap::new();
        for t in &open {
            let Some(branch) = t.payload.get("branch").and_then(|v| v.as_str()) else {
                continue;
            };
            let key = (t.scope.clone(), branch.to_string());
            if already.contains(&key) {
                continue;
            }
            let target = t
                .payload
                .get("target")
                .and_then(|v| v.as_str())
                .unwrap_or("main")
                .to_string();
            let content_proven = match (
                t.payload.get("fork_point").and_then(Value::as_str),
                t.payload.get("head_sha").and_then(Value::as_str),
            ) {
                (Some(fork), Some(head)) => !fork.is_empty() && head != fork,
                _ => false,
            };
            pending.insert(key, (target, content_proven));
        }
        if pending.is_empty() {
            return 0;
        }
        // Group by scope so each repo is fetched exactly once per cycle.
        let mut by_scope: std::collections::HashMap<String, Vec<(String, String)>> =
            std::collections::HashMap::new();
        for ((scope, branch), (target, content_proven)) in pending {
            if content_proven {
                by_scope.entry(scope).or_default().push((branch, target));
            }
        }
        // Resolve scopes to repo paths once, under the registry lock, then
        // release it before shelling out to git.
        let mut paths: std::collections::HashMap<String, std::path::PathBuf> =
            std::collections::HashMap::new();
        if let Ok(reg) = self.repos.lock() {
            for scope in by_scope.keys() {
                if let Some(rec) = reg.get(scope) {
                    paths.insert(scope.clone(), rec.path.clone());
                }
            }
        }

        let mut emitted = 0;
        for (scope, branches) in by_scope {
            // Unregistered scope or unopenable repo: cannot fetch, so the row
            // stays surfaced (fails toward surfacing, never hiding).
            let Some(path) = paths.get(&scope) else {
                continue;
            };
            let Ok(repo) = rk_git::Repo::discover(path) else {
                continue;
            };
            if let Err(e) = repo.fetch_prune(&remote, timeout) {
                // A failed/timed-out fetch leaves every row of this repo intact;
                // the next cycle retries. Never hide a row on a network hiccup.
                warn!(error = %e, scope = %scope, "review sweep: fetch failed");
                continue;
            }
            for (branch, target) in branches {
                if repo.remote_branch_merged_or_gone(&branch, &target, &remote) {
                    self.emit_event(
                        &scope,
                        "pull_request_closed",
                        json!({
                            "branch": branch,
                            "target": target,
                            "remote": remote,
                            "reason": "forge merged or deleted the branch",
                        }),
                    );
                    emitted += 1;
                }
            }
        }
        emitted
    }

    /// One pass of the periodic worktree-leak sweep (`[worktree_sweep]`):
    /// reclaim every still-live terminal agent's regenerable build artifacts
    /// immediately (no age cutoff — see
    /// [`reap_terminal_artifacts`](crate::supervisor::Supervisor::reap_terminal_artifacts)),
    /// then separately archive terminal agent records untouched for at least
    /// `after_days` and reclaim their git leftovers — worktree and local
    /// branch — wherever the branch has already landed or is gone. The
    /// automated counterpart to `rk prune --reap-git`/`--reap-artifacts`;
    /// every git removal is still gated by [`Supervisor::reap_git`]'s
    /// merged-or-gone-AND-clean-worktree checks, so this can run unattended
    /// without risking anyone's uncommitted or unmerged work. Returns the
    /// number of worktrees actually reclaimed (git only — artifact reaps are
    /// partial removals within a worktree, not a worktree reclaim).
    ///
    /// The artifact reap deliberately does NOT wait on `after_days`: that
    /// cutoff answers "has this record been idle long enough to archive",
    /// which has nothing to do with whether its `target/` dir is safe to
    /// delete — it always is, the moment the agent goes terminal. Gating
    /// artifact reap on the same cutoff (as the archive-time reap below still
    /// does, for records that only clear the cutoff there) is exactly the O12
    /// (2026-08-18 drain probe) bug: a newly terminal agent's `target/` stood
    /// for up to the default `after_days: 3` before the first sweep touched
    /// it.
    ///
    /// [`Supervisor::reap_git`]: crate::supervisor::Supervisor
    fn worktree_sweep_once(&self) -> usize {
        let artifact_reap = crate::supervisor::Reap {
            git: false,
            logs: false,
            // Always on: whether anything is actually removed for a given
            // repo is decided per-record inside `reap_artifacts` from that
            // repo's own activated policy, falling back to the operator-set
            // lists below (empty by default — STACK NEUTRALITY).
            artifacts: true,
            artifact_paths: self.worktree_sweep_config.artifact_paths.clone(),
            artifact_paths_by_repo: self.worktree_sweep_config.artifact_paths_by_repo.clone(),
        };
        self.supervisor.reap_terminal_artifacts(&artifact_reap);

        let cutoff = chrono::Utc::now()
            - chrono::Duration::days(self.worktree_sweep_config.after_days as i64);
        let reap = crate::supervisor::Reap {
            git: true,
            logs: false,
            artifacts: true,
            artifact_paths: self.worktree_sweep_config.artifact_paths.clone(),
            artifact_paths_by_repo: self.worktree_sweep_config.artifact_paths_by_repo.clone(),
        };
        match self.supervisor.archive_agents(cutoff, false, reap) {
            Ok(value) => value["reaped"]
                .as_array()
                .map(|rows| rows.iter().filter(|r| r["reaped"] == json!(true)).count())
                .unwrap_or(0),
            Err(e) => {
                warn!(error = %e, "worktree sweep: archive_agents failed");
                0
            }
        }
    }

    /// One pass of the periodic gate-worktree retention sweep
    /// (`[gate_worktree_sweep]`): reclaim `<home>/gate-worktrees/<repo>/
    /// <target>` directories per `self.gate_worktree_sweep_config`'s
    /// LRU/cap rules. The unattended, always-current-thresholds counterpart
    /// to `rk prune --reap-git`'s `gate_worktrees` extension (`handle_agent_archive`),
    /// which runs the identical reclaim on demand regardless of this switch.
    fn gate_worktree_sweep_once(&self) -> usize {
        self.landing()
            .gate_worktree_sweep_once(&self.gate_worktree_sweep_config, false)
            .iter()
            .filter(|r| r.reclaimed)
            .count()
    }

    /// B2 re-notify sweep body: re-push every unacked `recovery-action`
    /// escalation whose next scheduled re-notify is due. Builds its own
    /// [`rk_core::notify::SinkRegistry`] from the SAME `[[notify.sinks]]`
    /// config the reactor's first push used, so a re-notify reaches exactly
    /// the channel set the original announce did — sinks are stateless
    /// shell-outs (B1), so a second registry instance is free to build.
    fn recovery_renotify_sweep_once(&self) -> usize {
        let sinks = crate::reactor::sink_factory().registry(
            self.notify_config
                .resolved(self.reactor_config.notify_escalations),
        );
        let schedule = crate::recovery::RenotifySchedule {
            first: Duration::from_secs(self.recovery_sweep_config.first_renotify_secs.max(1)),
            repeat: Duration::from_secs(self.recovery_sweep_config.repeat_renotify_secs.max(1)),
            max: self.recovery_sweep_config.max_renotifies,
        };
        match crate::recovery::renotify_sweep(&self.space, &sinks, &schedule, chrono::Utc::now()) {
            Ok(n) => n,
            Err(e) => {
                warn!(error = %e, "recovery re-notify sweep failed");
                0
            }
        }
    }

    /// B8 stale-`Running`-instance hard timeout sweep body: delegates to
    /// [`crate::workflow_exec::WorkflowEngine::stale_timeout_sweep_once`],
    /// supplying the SAME sink set the reactor's own escalations use (mirrors
    /// [`recovery_renotify_sweep_once`](Self::recovery_renotify_sweep_once))
    /// and the daemon's single long-lived [`recovery_announcer`](Self::recovery_announcer)
    /// so the rate cap accumulates correctly across sweep ticks.
    async fn stale_instance_timeout_sweep_once(&self) -> usize {
        let sinks = crate::reactor::sink_factory().registry(
            self.notify_config
                .resolved(self.reactor_config.notify_escalations),
        );
        self.engine()
            .stale_timeout_sweep_once(
                chrono::Utc::now(),
                Duration::from_secs(
                    self.instance_timeout_sweep_config
                        .default_timeout_secs
                        .max(1),
                ),
                &self.recovery_announcer,
                &sinks,
                crate::recovery::RateCap::per_hour(
                    self.instance_timeout_sweep_config.rate_cap_per_hour,
                ),
            )
            .await
    }

    /// B9 orphaned-ticket sweep body (strategic review, seam 5): reopen every
    /// `in_progress` ticket whose assignee has had no LIVE agent record for
    /// `stale_after_secs`.
    ///
    /// "No live owner" is anchored on the MORE RECENT of the ticket's own
    /// last edit and the assignee's own last state transition, not on when
    /// the ticket was originally claimed — a rat that has been quietly
    /// working for an hour must not look "stale" the instant it dies, and a
    /// ticket whose `assignee` hasn't landed yet (the CLI sets `status`
    /// in_progress before recording `assignee` — `agent_cmds.rs`) must not
    /// look ownerless before that handoff has had a chance to complete.
    ///
    /// Reopen itself is the CAS [`crate::tickets::Tickets::reopen_if_in_progress`]
    /// (the mirror of `claim`'s `open` -> `in_progress`), so a ticket whose
    /// rat finishes racing this sweep's read is never clobbered back to
    /// `open` after it went `done`. The announce only fires once the reopen
    /// actually won — a ticket that moved on between the scan and the write
    /// produces no escalation, matching "announced" meaning "an action was
    /// taken", not "a ticket was looked at".
    async fn ticket_reopen_sweep_once(&self) -> usize {
        self.ticket_reopen_sweep_at(chrono::Utc::now()).await
    }

    /// Testable core of [`Self::ticket_reopen_sweep_once`]: `now` is injected
    /// rather than read from the clock, so a test can assert the 15-minute
    /// staleness bound without an actual 15-minute wait.
    async fn ticket_reopen_sweep_at(&self, now: DateTime<Utc>) -> usize {
        let stale_after =
            chrono::Duration::seconds(self.ticket_reopen_sweep_config.stale_after_secs as i64);
        let in_progress = match self
            .tickets
            .list(None, Some("in_progress".to_string()), None)
        {
            Ok(tickets) => tickets,
            Err(e) => {
                warn!(error = %e, "ticket reopen sweep: list failed");
                return 0;
            }
        };
        if in_progress.is_empty() {
            return 0;
        }
        // Landing-awareness (TKT-01M0C663BZ86SMA2PVMFP5QJ8D): a ticket whose
        // branch already landed must never be reopened just because its rat
        // went terminal without being dismissed — the async
        // steward-review flow leaves exactly that gap (O14: the harness's
        // own `rk done` finds the branch not yet merged and refuses to close
        // the ticket, so it sits `in_progress` until the landing pipeline
        // records delivery). Reopening it dispatches a duplicate rat onto
        // already-delivered work. `landing_processed` is
        // keyed by `(repo, branch, head_sha)` — the wrong shape for "does
        // this ticket have a landed outcome" — so read `payload.task`
        // instead, which the reactor's landing-trigger dispatch (`reactor.rs`)
        // populates from the completing rat's own `task` (== ticket id by
        // fleet convention). One unscoped scan up front, not one probe per
        // ticket: `landing_processed` is a `Furniture` event with no
        // per-ticket index, same tradeoff `build()`'s `branch_landed` scan
        // already makes for `rk inbox`.
        let landed_tickets: std::collections::HashSet<String> = self
            .space
            .scan(
                &Pattern::category(Category::Event)
                    .identity(crate::landing::LANDING_PROCESSED_IDENTITY),
            )
            .unwrap_or_default()
            .into_iter()
            .filter(|t| t.payload.get("outcome").and_then(Value::as_str) == Some("landed"))
            .filter_map(|t| {
                t.payload
                    .get("task")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .filter(|task| !task.is_empty())
            .collect();
        // Landing-awareness, part 2 (probes O8/O17, TKT-01M0CTC4DYBRX6P5X2NPEZF0EZ):
        // `landed_tickets` above only covers the terminal case. A ticket
        // whose rat went non-live (paused, killed, orphaned) WHILE its
        // branch is still queued for landing — `Queued`, `RunningGates`, or
        // `AwaitingReview` — has no live agent and no `landing_processed`
        // marker yet, so without this it sails through both guards and gets
        // reopened once `stale_after` elapses. Drain then dispatches a
        // duplicate rat onto work that is already in flight toward landing.
        let queued_tickets = crate::landing::tasks_in_landing_queue(&self.space);
        let sinks = crate::reactor::sink_factory().registry(
            self.notify_config
                .resolved(self.reactor_config.notify_escalations),
        );
        let announcer = crate::recovery::RecoveryAnnouncer::new();
        let mut reopened = 0usize;
        for ticket in in_progress {
            if landed_tickets.contains(&ticket.identity) {
                // Already delivered — leave it for the landing-driven ticket
                // transition to close rather than reopening onto a duplicate.
                continue;
            }
            if queued_tickets.contains(&ticket.identity) {
                // Branch is queued/gating/awaiting review — the owning rat
                // going non-live here is expected (it may have already
                // exited after handing off to the landing pipeline), not
                // abandonment. Leave it for the pipeline to reach a terminal
                // outcome (landed, or handed back for a real retry).
                continue;
            }
            let assignee = ticket
                .payload
                .get("assignee")
                .and_then(Value::as_str)
                .map(str::to_string);
            let agent = assignee
                .as_deref()
                .and_then(|name| self.supervisor.status(name))
                .or_else(|| {
                    // A ticket with no `assignee` is not necessarily ownerless: a
                    // drain claim writes `assignee` only after its spawn returns
                    // (drain.rs), so a ticket in the gap between claim and that
                    // write — or one left behind by a daemon predating that
                    // write — can still have a live rat working it. `task ==
                    // ticket id` is the same identity both the drain and the CLI
                    // spawn path (agent_cmds.rs) key on, so it is a reliable
                    // fallback match. Only tried when `assignee` is absent: a
                    // ticket that names a dead/gone assignee must not be
                    // rescued by an unrelated live agent that happens to share
                    // its task.
                    if assignee.is_some() {
                        return None;
                    }
                    self.supervisor.list().into_iter().find(|a| {
                        a.state.is_live() && a.task.as_deref() == Some(ticket.identity.as_str())
                    })
                });
            if agent.as_ref().is_some_and(|a| a.state.is_live()) {
                continue;
            }
            let ticket_updated_at = ticket
                .payload
                .get("updated_at")
                .and_then(Value::as_str)
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or(ticket.created_at);
            let owner_since = match &agent {
                Some(a) => a.updated_at.max(ticket_updated_at),
                None => ticket_updated_at,
            };
            if now - owner_since < stale_after {
                continue;
            }
            let ticket_id = ticket.identity.clone();
            let performed = match self.tickets.reopen_if_in_progress(&ticket_id).await {
                Ok(performed) => performed,
                Err(e) => {
                    warn!(ticket = %ticket_id, error = %e, "ticket reopen sweep: reopen failed");
                    continue;
                }
            };
            if !performed {
                // Moved on (claimed/done/closed) between the scan above and
                // this write — nothing to announce.
                continue;
            }
            let detail = match (&assignee, &agent) {
                (Some(name), Some(a)) => format!(
                    "no live owner for over {}m (assignee `{name}` is {:?})",
                    stale_after.num_minutes(),
                    a.state
                ),
                (Some(name), None) => format!(
                    "no live owner for over {}m (assignee `{name}` has no agent record)",
                    stale_after.num_minutes()
                ),
                (None, _) => format!("no assignee for over {}m", stale_after.num_minutes()),
            };
            let notice = rk_core::notify::EscalationNotice::new(
                "pending",
                "ticket-reopen",
                rk_core::notify::Severity::Warn,
                ticket.scope.clone(),
                ticket_id.clone(),
                format!("{ticket_id} reopened to `open`: {detail}"),
            )
            .with_ref("ticket", ticket_id.clone());
            let action = crate::recovery::RecoveryAction {
                kind: "ticket-reopen".to_string(),
                instance: "ticket-reopen-sweep".to_string(),
                notice,
            };
            if let Err(e) = announcer.announce(
                &self.space,
                &sinks,
                action,
                crate::recovery::RateCap::unlimited(),
            ) {
                warn!(ticket = %ticket_id, error = %e, "ticket reopen sweep: announce failed");
            }
            reopened += 1;
        }
        reopened
    }

    /// Append an `Event` tuple authored by this castle. Best-effort: a store
    /// error is logged, not propagated (event emission is never on a caller's
    /// critical path).
    fn emit_event(&self, scope: &str, identity: &str, payload: Value) {
        let tuple = Tuple::new(
            Category::Event,
            scope.to_string(),
            identity.to_string(),
            self.castle.clone(),
            payload,
        );
        if let Err(e) = self.space.out(tuple) {
            warn!(error = %e, identity, "failed to emit event tuple");
        }
    }

    async fn handle_repo_add(&self, req: Request) -> Response {
        let params: RepoAddParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let path = match std::fs::canonicalize(&params.path) {
            Ok(path) => path,
            Err(error) => {
                return Response::err(
                    req.id,
                    codes::BAD_PARAMS,
                    format!("cannot resolve repository path {}: {error}", params.path),
                );
            }
        };
        let policy_file = path.join(".rk").join("repo.cue");
        let activated_policy = if policy_file.is_file() {
            if params.merge_mode.is_some() || params.remote.is_some() {
                return Response::err(
                    req.id,
                    codes::BAD_PARAMS,
                    "a repository with .rk/repo.cue cannot also use --merge-mode or --remote; edit and activate the versioned policy instead",
                );
            }
            let policy_file_for_load = policy_file.clone();
            match tokio::task::spawn_blocking(move || {
                let (policy, digest) =
                    rk_workflow::load_repository_policy_with_digest(&policy_file_for_load)?;
                Ok::<_, rk_core::Error>(crate::repos::ActivatedRepositoryPolicy { digest, policy })
            })
            .await
            {
                Ok(Ok(activated)) => Some(activated),
                Ok(Err(error)) => {
                    return Response::err(req.id, codes::BAD_PARAMS, error.to_string());
                }
                Err(error) => {
                    return Response::err(
                        req.id,
                        codes::INTERNAL,
                        format!("repo policy inspection task failed: {error}"),
                    );
                }
            }
        } else {
            None
        };
        let remote = activated_policy
            .as_ref()
            .map(|approved| approved.policy.delivery.remote.clone())
            .or(params.remote);
        let remote_name = remote.as_deref().unwrap_or("origin");
        let path_for_remote = path.clone();
        let remote_name = remote_name.to_string();
        let host = match tokio::task::spawn_blocking(move || {
            repo_remote_url(&path_for_remote, &remote_name)
                .and_then(|url| crate::repos::infer_host(&url))
        })
        .await
        {
            Ok(host) => host,
            Err(e) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!("repo inspection task failed: {e}"),
                );
            }
        };
        let record = crate::repos::RepoRecord {
            name: params.name,
            path,
            created_at: chrono::Utc::now(),
            merge_mode: activated_policy
                .as_ref()
                .map(|approved| match approved.policy.delivery.mode {
                    rk_workflow::DeliveryMode::Pr => rk_core::config::MergeMode::Pr,
                    rk_workflow::DeliveryMode::Merge
                    | rk_workflow::DeliveryMode::MergePush
                    | rk_workflow::DeliveryMode::PushBranch => rk_core::config::MergeMode::Direct,
                })
                .or(params.merge_mode)
                .unwrap_or(self.default_merge_mode),
            remote,
            host,
            activated_policy,
        };
        let mut reg = match self.repos.lock() {
            Ok(r) => r,
            Err(_) => return Response::err(req.id, codes::INTERNAL, "repo registry lock poisoned"),
        };
        match reg.add(record.clone()) {
            Ok(()) => Response::ok(req.id, json!({"repo": record})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_repo_get(&self, req: Request) -> Response {
        let params: NameParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let reg = match self.repos.lock() {
            Ok(r) => r,
            Err(_) => return Response::err(req.id, codes::INTERNAL, "repo registry lock poisoned"),
        };
        match reg.get(&params.name) {
            Some(record) => Response::ok(req.id, json!({"repo": record})),
            None => Response::ok(req.id, json!({"repo": null})),
        }
    }

    async fn handle_onboarding_start(&self, req: Request) -> Response {
        let params: RepoOnboardingStartParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let harness = params
            .harness
            .clone()
            .unwrap_or_else(|| self.default_harness.clone());
        let registered = match self.repos.lock() {
            Ok(registry) => registry.list(),
            Err(_) => return Response::err(req.id, codes::INTERNAL, "repo registry lock poisoned"),
        };
        let target = params.target.clone();
        let context = crate::onboarding::InspectContext {
            default_harness: harness.clone(),
            require_named_checks: self.require_named_checks,
        };
        let assessment = match tokio::task::spawn_blocking(move || {
            crate::onboarding::inspect(&target, &registered, &context)
        })
        .await
        {
            Ok(assessment) => assessment,
            Err(error) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!("repository inspection task failed: {error}"),
                );
            }
        };
        let Some(repo_path) = assessment
            .identity
            .canonical_path
            .as_deref()
            .map(std::path::PathBuf::from)
        else {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                "repository identity did not resolve; run `rk repo onboard inspect` for findings",
            );
        };
        let path_for_git = repo_path.clone();
        let (repo_name, base_branch) = match tokio::task::spawn_blocking(move || {
            let repo = rk_git::Repo::discover(&path_for_git)?;
            Ok::<_, rk_core::Error>((repo.name(), repo.current_branch()?))
        })
        .await
        {
            Ok(Ok(identity)) => identity,
            Ok(Err(error)) => return Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
            Err(error) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!("repository identity task failed: {error}"),
                );
            }
        };
        let candidate = crate::onboarding_sessions::OnboardingSession::starting(
            params.target,
            repo_name,
            repo_path,
            base_branch,
            harness,
            params.model,
            params.attach,
            assessment,
            &self.layout.worktrees_dir(),
        );
        let (session, created) = {
            let mut sessions = match self.onboarding_sessions.lock() {
                Ok(sessions) => sessions,
                Err(_) => {
                    return Response::err(
                        req.id,
                        codes::INTERNAL,
                        "onboarding session registry lock poisoned",
                    );
                }
            };
            match sessions.insert_if_absent(candidate) {
                Ok(result) => result,
                Err(error) => return Response::err(req.id, codes::INTERNAL, error.to_string()),
            }
        };
        if !created {
            return match self.reconcile_onboarding_session(&session.id) {
                Ok(session) => Response::ok(req.id, onboarding_payload(&session, true)),
                Err(error) => Response::err(req.id, codes::FORBIDDEN, error.to_string()),
            };
        }

        match self.launch_onboarding_agent(&session, params.attach).await {
            Ok(record) => {
                let updated =
                    match self.update_onboarding_from_agent(&session.id, &record, params.attach) {
                        Ok(updated) => updated,
                        Err(error) => {
                            return Response::err(req.id, codes::INTERNAL, error.to_string());
                        }
                    };
                Response::ok(req.id, onboarding_payload(&updated, false))
            }
            Err(error) => {
                let _ = self.update_onboarding_failure(&session.id, error.to_string());
                Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!(
                        "onboarding session {} was journaled but launch failed: {error}",
                        session.id
                    ),
                )
            }
        }
    }

    async fn handle_onboarding_resume(&self, req: Request) -> Response {
        let params: RepoOnboardingResumeParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let session = match self.reconcile_onboarding_session(&params.session) {
            Ok(session) => session,
            Err(error) => return Response::err(req.id, codes::FORBIDDEN, error.to_string()),
        };
        if matches!(
            session.state,
            crate::onboarding_sessions::OnboardingSessionState::Running
                | crate::onboarding_sessions::OnboardingSessionState::Starting
                | crate::onboarding_sessions::OnboardingSessionState::Completed
        ) {
            return Response::ok(req.id, onboarding_payload(&session, true));
        }

        let resumed = if let Some(agent) = session.agent.as_deref() {
            if session.worktree.exists() {
                self.supervisor
                    .respawn_onboarding_async(agent.to_string(), params.attach)
                    .await
            } else {
                self.launch_onboarding_agent(&session, params.attach).await
            }
        } else {
            self.launch_onboarding_agent(&session, params.attach).await
        };
        match resumed {
            Ok(record) => {
                match self.update_onboarding_from_agent(&session.id, &record, params.attach) {
                    Ok(updated) => Response::ok(req.id, onboarding_payload(&updated, false)),
                    Err(error) => Response::err(req.id, codes::INTERNAL, error.to_string()),
                }
            }
            Err(error) => {
                let _ = self.update_onboarding_failure(&session.id, error.to_string());
                Response::err(req.id, codes::INTERNAL, error.to_string())
            }
        }
    }

    fn handle_onboarding_propose(&self, req: Request) -> Response {
        let params: RepoOnboardingProposeParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let session = match self.onboarding_sessions.lock() {
            Ok(sessions) => sessions.get(&params.session),
            Err(_) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    "onboarding session registry lock poisoned",
                );
            }
        };
        let Some(session) = session else {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!("no such onboarding session: {}", params.session),
            );
        };
        if req.caller != crate::client::OPERATOR
            && session.agent.as_deref() != Some(req.caller.as_str())
        {
            return Response::err(
                req.id,
                codes::FORBIDDEN,
                format!(
                    "{} does not own onboarding session {}",
                    req.caller, params.session
                ),
            );
        }
        let tree_revision =
            match crate::onboarding_proposals::onboarding_tree_revision(&session.worktree) {
                Ok(revision) => revision,
                Err(error) => {
                    return Response::err(req.id, codes::BAD_PARAMS, error.to_string());
                }
            };
        let result = self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| {
                sessions.propose(
                    &params.session,
                    params.proposal,
                    req.caller.clone(),
                    tree_revision,
                )
            });
        match result {
            Ok((proposal, created)) => {
                Response::ok(req.id, json!({"proposal": proposal, "created": created}))
            }
            Err(error) => Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        }
    }

    fn handle_onboarding_approve(&self, req: Request) -> Response {
        let params: RepoOnboardingDecisionParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        self.handle_onboarding_decision(
            req,
            params,
            crate::onboarding_proposals::OnboardingDecision::Approve,
        )
    }

    fn handle_onboarding_decline(&self, req: Request) -> Response {
        let params: RepoOnboardingDecisionParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        self.handle_onboarding_decision(
            req,
            params,
            crate::onboarding_proposals::OnboardingDecision::Decline,
        )
    }

    async fn handle_onboarding_apply(&self, req: Request) -> Response {
        let params: RepoOnboardingApplyParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let _apply_guard = self.onboarding_apply_lock.lock().await;
        let actor = format!("{}@{}", req.caller, self.castle);
        let (session, proposal) = match self.onboarding_sessions.lock() {
            Ok(sessions) => {
                let Some(session) = sessions.get(&params.session) else {
                    return Response::err(
                        req.id,
                        codes::BAD_PARAMS,
                        format!("no such onboarding session: {}", params.session),
                    );
                };
                let Some(proposal) = session
                    .proposals
                    .iter()
                    .find(|proposal| proposal.id == params.proposal)
                    .cloned()
                else {
                    return Response::err(
                        req.id,
                        codes::BAD_PARAMS,
                        format!(
                            "no such onboarding proposal in {}: {}",
                            params.session, params.proposal
                        ),
                    );
                };
                (session, proposal)
            }
            Err(_) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    "onboarding session registry lock poisoned",
                );
            }
        };
        if proposal.digest != params.digest {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!("stale proposal digest for {}", proposal.id),
            );
        }
        if !matches!(
            proposal.status,
            crate::onboarding_proposals::OnboardingProposalStatus::Approved
                | crate::onboarding_proposals::OnboardingProposalStatus::Applied
                | crate::onboarding_proposals::OnboardingProposalStatus::Failed
                | crate::onboarding_proposals::OnboardingProposalStatus::Verified
        ) {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!(
                    "proposal {} must be approved before apply; found {}",
                    proposal.id, proposal.status
                ),
            );
        }

        let apply_session = session.clone();
        let apply_proposal = proposal.clone();
        let apply_actor = actor.clone();
        let application = tokio::task::spawn_blocking(move || {
            crate::onboarding_apply::ensure_application(
                &apply_session,
                &apply_proposal,
                &apply_actor,
            )
        })
        .await;
        let application = match application {
            Ok(Ok(application)) => application,
            Ok(Err(error)) => {
                let detail = error.to_string();
                let _ = self
                    .onboarding_sessions
                    .lock()
                    .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
                    .and_then(|mut sessions| {
                        sessions.record_application_failure(
                            &params.session,
                            &params.proposal,
                            &params.digest,
                            actor,
                            detail.clone(),
                        )
                    });
                return Response::err(req.id, codes::BAD_PARAMS, detail);
            }
            Err(error) => {
                let detail = format!("onboarding application task failed: {error}");
                let _ = self
                    .onboarding_sessions
                    .lock()
                    .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
                    .and_then(|mut sessions| {
                        sessions.record_application_failure(
                            &params.session,
                            &params.proposal,
                            &params.digest,
                            actor,
                            detail.clone(),
                        )
                    });
                return Response::err(req.id, codes::INTERNAL, detail);
            }
        };
        if proposal.status == crate::onboarding_proposals::OnboardingProposalStatus::Verified {
            return Response::ok(
                req.id,
                json!({"proposal": proposal, "applied": false, "verified": false}),
            );
        }
        let (applied_proposal, applied) = match self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| {
                sessions.record_application(
                    &params.session,
                    &params.proposal,
                    &params.digest,
                    application,
                )
            }) {
            Ok(result) => result,
            Err(error) => return Response::err(req.id, codes::INTERNAL, error.to_string()),
        };

        let proposal = if let Some(contract) = applied_proposal.named_check.as_ref() {
            let attempt = applied_proposal.verification_results.len() as u32 + 1;
            let verification =
                match crate::onboarding_apply::verify(&session, &applied_proposal, &actor, attempt)
                    .await
                {
                    Ok(verification) => verification,
                    Err(error) => {
                        let now = chrono::Utc::now();
                        crate::onboarding_proposals::OnboardingVerification {
                            attempt,
                            actor: actor.clone(),
                            started_at: now,
                            finished_at: now,
                            check_name: contract.name.clone(),
                            command: contract.command.clone(),
                            cwd: contract.cwd.clone(),
                            expected_exit: contract.expect_exit,
                            timeout: contract.timeout.clone(),
                            environment_policy: contract.environment_policy,
                            toolchain: contract.toolchain.clone(),
                            exit_status: None,
                            timed_out: false,
                            passed: false,
                            output_summary: format!("verification setup failed: {error}"),
                            unresolved_risks: vec![
                            "verification could not execute; onboarding branch is not ready to land"
                                .into(),
                        ],
                        }
                    }
                };
            match self
                .onboarding_sessions
                .lock()
                .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
                .and_then(|mut sessions| {
                    sessions.record_verification(
                        &params.session,
                        &params.proposal,
                        &params.digest,
                        verification,
                    )
                }) {
                Ok(proposal) => proposal,
                Err(error) => {
                    return Response::err(req.id, codes::INTERNAL, error.to_string());
                }
            }
        } else {
            let attempt = applied_proposal.validation_results.len() as u32 + 1;
            let validation = match crate::onboarding_apply::validate_automation(
                &session,
                &applied_proposal,
                &actor,
                attempt,
            ) {
                Ok(validation) => validation,
                Err(error) => {
                    return Response::err(req.id, codes::INTERNAL, error.to_string());
                }
            };
            match self
                .onboarding_sessions
                .lock()
                .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
                .and_then(|mut sessions| {
                    sessions.record_validation(
                        &params.session,
                        &params.proposal,
                        &params.digest,
                        validation,
                    )
                }) {
                Ok(proposal) => proposal,
                Err(error) => {
                    return Response::err(req.id, codes::INTERNAL, error.to_string());
                }
            }
        };
        let passed =
            proposal.status == crate::onboarding_proposals::OnboardingProposalStatus::Verified;
        Response::ok(
            req.id,
            json!({"proposal": proposal, "applied": applied, "verified": passed}),
        )
    }

    async fn handle_onboarding_activate(&self, req: Request) -> Response {
        let params: RepoOnboardingApplyParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let _apply_guard = self.onboarding_apply_lock.lock().await;
        let actor = format!("{}@{}", req.caller, self.castle);
        let (session, proposal) = match self.onboarding_sessions.lock() {
            Ok(sessions) => {
                let Some(session) = sessions.get(&params.session) else {
                    return Response::err(
                        req.id,
                        codes::BAD_PARAMS,
                        format!("no such onboarding session: {}", params.session),
                    );
                };
                let Some(proposal) = session
                    .proposals
                    .iter()
                    .find(|proposal| proposal.id == params.proposal)
                    .cloned()
                else {
                    return Response::err(
                        req.id,
                        codes::BAD_PARAMS,
                        format!(
                            "no such onboarding proposal in {}: {}",
                            params.session, params.proposal
                        ),
                    );
                };
                (session, proposal)
            }
            Err(_) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    "onboarding session registry lock poisoned",
                );
            }
        };
        if proposal.digest != params.digest {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!("stale proposal digest for {}", proposal.id),
            );
        }
        let contract_session = session.clone();
        let contract_proposal = proposal.clone();
        let contract = match tokio::task::spawn_blocking(move || {
            crate::onboarding_activation::contract(&contract_session, &contract_proposal)
        })
        .await
        {
            Ok(Ok(contract)) => contract,
            Ok(Err(error)) => {
                return Response::err(req.id, codes::BAD_PARAMS, error.to_string());
            }
            Err(error) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!("onboarding activation preflight task failed: {error}"),
                );
            }
        };
        let (intent, intent_created) = match self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| {
                sessions.begin_activation(
                    &params.session,
                    &params.proposal,
                    &params.digest,
                    contract.operation_id.clone(),
                    actor,
                    contract.expected_base_commit.clone(),
                    contract.approved_commit.clone(),
                    contract.approved_tree_revision.clone(),
                    contract.target_digest.clone(),
                )
            }) {
            Ok(result) => result,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        };
        if intent.activation.as_ref().is_some_and(|activation| {
            activation.status == crate::onboarding_proposals::OnboardingActivationStatus::Activated
        }) {
            return Response::ok(
                req.id,
                json!({"proposal": intent, "activated": true, "changed": false}),
            );
        }

        let activates_repository_policy = proposal.automation_kind()
            == Some(crate::onboarding_proposals::OnboardingAutomationKind::RepositoryPolicy);
        let activation_repo_path = session.repo_path.clone();
        let activation_session = session;
        let activation_proposal = intent;
        let activation_contract = contract.clone();
        let evidence = tokio::task::spawn_blocking(move || {
            crate::onboarding_activation::ensure_activation(
                &activation_session,
                &activation_proposal,
                &activation_contract,
            )
        })
        .await;
        let evidence = match evidence {
            Ok(Ok(evidence)) => evidence,
            Ok(Err(error)) => {
                let detail = error.to_string();
                let _ = self
                    .onboarding_sessions
                    .lock()
                    .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
                    .and_then(|mut sessions| {
                        sessions.fail_activation(
                            &params.session,
                            &params.proposal,
                            &params.digest,
                            &contract.operation_id,
                            detail.clone(),
                        )
                    });
                return Response::err(req.id, codes::BAD_PARAMS, detail);
            }
            Err(error) => {
                let detail = format!("onboarding activation task failed: {error}");
                let _ = self
                    .onboarding_sessions
                    .lock()
                    .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
                    .and_then(|mut sessions| {
                        sessions.fail_activation(
                            &params.session,
                            &params.proposal,
                            &params.digest,
                            &contract.operation_id,
                            detail.clone(),
                        )
                    });
                return Response::err(req.id, codes::INTERNAL, detail);
            }
        };
        if activates_repository_policy {
            let policy_file = activation_repo_path.join(".rk").join("repo.cue");
            let policy_file_for_load = policy_file.clone();
            let activated = tokio::task::spawn_blocking(move || {
                let (policy, digest) =
                    rk_workflow::load_repository_policy_with_digest(&policy_file_for_load)?;
                Ok::<_, rk_core::Error>(crate::repos::ActivatedRepositoryPolicy { digest, policy })
            })
            .await;
            let activated = match activated {
                Ok(Ok(activated)) if activated.digest == contract.target_digest => activated,
                Ok(Ok(activated)) => {
                    let detail = format!(
                        "activated repository policy digest drifted: expected {}, found {}",
                        contract.target_digest, activated.digest
                    );
                    let _ = self
                        .onboarding_sessions
                        .lock()
                        .ok()
                        .and_then(|mut sessions| {
                            sessions
                                .fail_activation(
                                    &params.session,
                                    &params.proposal,
                                    &params.digest,
                                    &contract.operation_id,
                                    detail.clone(),
                                )
                                .ok()
                        });
                    return Response::err(req.id, codes::BAD_PARAMS, detail);
                }
                Ok(Err(error)) => {
                    return Response::err(req.id, codes::BAD_PARAMS, error.to_string());
                }
                Err(error) => {
                    return Response::err(
                        req.id,
                        codes::INTERNAL,
                        format!("repository policy activation task failed: {error}"),
                    );
                }
            };
            let mut repos = match self.repos.lock() {
                Ok(repos) => repos,
                Err(_) => {
                    return Response::err(
                        req.id,
                        codes::INTERNAL,
                        "repo registry lock poisoned during policy activation",
                    );
                }
            };
            if let Err(error) = repos.activate_policy_by_path(&activation_repo_path, activated) {
                return Response::err(req.id, codes::BAD_PARAMS, error.to_string());
            }
            drop(repos);
        }
        let (proposal, changed) = match self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| {
                sessions.finish_activation(
                    &params.session,
                    &params.proposal,
                    &params.digest,
                    &contract.operation_id,
                    evidence.registered_commit,
                    evidence.detail,
                )
            }) {
            Ok(result) => result,
            Err(error) => return Response::err(req.id, codes::INTERNAL, error.to_string()),
        };
        Response::ok(
            req.id,
            json!({
                "proposal": proposal,
                "activated": true,
                "changed": changed,
                "intent_created": intent_created,
            }),
        )
    }

    async fn handle_onboarding_decline_activation(&self, req: Request) -> Response {
        let params: RepoOnboardingDecisionParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let _apply_guard = self.onboarding_apply_lock.lock().await;
        let actor = format!("{}@{}", req.caller, self.castle);
        match self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| {
                sessions.decline_activation(
                    &params.session,
                    &params.proposal,
                    &params.digest,
                    actor,
                    params.reason,
                )
            }) {
            Ok((proposal, changed)) => Response::ok(
                req.id,
                json!({"proposal": proposal, "declined": true, "changed": changed}),
            ),
            Err(error) => Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        }
    }

    async fn handle_onboarding_cleanup(&self, req: Request) -> Response {
        let params: RepoOnboardingSessionParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let _apply_guard = self.onboarding_apply_lock.lock().await;
        let session = match self.reconcile_onboarding_session(&params.session) {
            Ok(session) => session,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        };
        if session.cleanup.is_some() {
            return Response::ok(
                req.id,
                json!({"session": session.status(), "cleaned": false}),
            );
        }
        let actor = format!("{}@{}", req.caller, self.castle);
        let cleanup_session = session.clone();
        let cleanup_actor = actor.clone();
        let cleanup = match tokio::task::spawn_blocking(move || {
            crate::onboarding_activation::ensure_cleanup(&cleanup_session, &cleanup_actor)
        })
        .await
        {
            Ok(Ok(cleanup)) => cleanup,
            Ok(Err(error)) => {
                return Response::err(req.id, codes::BAD_PARAMS, error.to_string());
            }
            Err(error) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!("onboarding cleanup task failed: {error}"),
                );
            }
        };
        match self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| sessions.record_cleanup(&params.session, cleanup))
        {
            Ok((session, changed)) => Response::ok(
                req.id,
                json!({"session": session.status(), "cleaned": changed}),
            ),
            Err(error) => Response::err(req.id, codes::INTERNAL, error.to_string()),
        }
    }

    fn handle_onboarding_decision(
        &self,
        req: Request,
        params: RepoOnboardingDecisionParams,
        decision: crate::onboarding_proposals::OnboardingDecision,
    ) -> Response {
        let worktree = match self.onboarding_sessions.lock() {
            Ok(sessions) => match sessions.get(&params.session) {
                Some(session) => session.worktree,
                None => {
                    return Response::err(
                        req.id,
                        codes::BAD_PARAMS,
                        format!("no such onboarding session: {}", params.session),
                    );
                }
            },
            Err(_) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    "onboarding session registry lock poisoned",
                );
            }
        };
        let tree_revision = match crate::onboarding_proposals::onboarding_tree_revision(&worktree) {
            Ok(revision) => revision,
            Err(error) => {
                return Response::err(req.id, codes::BAD_PARAMS, error.to_string());
            }
        };
        // The actor is derived only after RPC authentication. No request field
        // can claim a human identity; the castle-qualified operator channel is
        // the durable attribution available today.
        let actor = format!("{}@{}", req.caller, self.castle);
        let result = self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))
            .and_then(|mut sessions| {
                sessions.decide(
                    &params.session,
                    &params.proposal,
                    &params.digest,
                    &tree_revision,
                    decision,
                    actor,
                    params.reason,
                )
            });
        match result {
            Ok((proposal, changed)) => {
                Response::ok(req.id, json!({"proposal": proposal, "changed": changed}))
            }
            Err(error) => Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        }
    }

    fn handle_onboarding_status(&self, req: Request) -> Response {
        let params: RepoOnboardingSessionParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        match self.reconcile_onboarding_session(&params.session) {
            Ok(session) => Response::ok(req.id, json!({"session": session.status()})),
            Err(error) => Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        }
    }

    fn handle_onboarding_report(&self, req: Request) -> Response {
        let params: RepoOnboardingSessionParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        match self.reconcile_onboarding_session(&params.session) {
            Ok(session) => Response::ok(req.id, json!({"report": session.report()})),
            Err(error) => Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        }
    }

    async fn launch_onboarding_agent(
        &self,
        session: &crate::onboarding_sessions::OnboardingSession,
        attach: bool,
    ) -> rk_core::Result<crate::agents::AgentRecord> {
        let assessment = serde_json::to_string_pretty(&session.assessment)?;
        let (proposal_instruction, completion_instruction) = if session.harness == "jcode" {
            (
                "Include concrete proposed changes in the final assessment for the operator to \
                 review and journal; your restricted tool surface cannot call RK mutation APIs.",
                "Return the final assessment summary and stop; the one-shot jcode terminal event \
                 completes the session, so do not try to run `rk done`.",
            )
        } else {
            (
                "Journal concrete advice with `rk repo onboard propose`; that records a proposal \
                 but grants no approval or mutation authority.",
                "Finish with `rk done`; do not edit or commit anything.",
            )
        };
        self.supervisor
            .spawn_async(
                crate::supervisor::SpawnParams {
                    repo: session.repo_path.to_string_lossy().into_owned(),
                    task: session.id.clone(),
                    prompt: Some(format!(
                        "Assess this repository read-only for onboarding session {}. \
                     The daemon's deterministic starting assessment follows. Confirm \
                     evidence and report ambiguity. {proposal_instruction} \
                     {completion_instruction}\n\n{}",
                        session.id, assessment,
                    )),
                    role: crate::onboarding_sessions::ONBOARDER_ROLE.into(),
                    coordination: None,
                    harness: Some(session.harness.clone()),
                    parent: None,
                    base: Some(session.base_branch.clone()),
                    review: None,
                    model: session.model.clone(),
                    permission_mode: None,
                    attach,
                    workflow_instance: None,
                    coordinator: None,
                    instance_max_usd: None,
                    profile: None,
                    resolved_profile: None,
                },
                0,
            )
            .await
    }

    fn reconcile_onboarding_session(
        &self,
        id: &str,
    ) -> rk_core::Result<crate::onboarding_sessions::OnboardingSession> {
        let session = self
            .onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))?
            .get(id)
            .ok_or_else(|| rk_core::Error::other(format!("no such onboarding session: {id}")))?;
        let recovered = session
            .agent
            .as_deref()
            .and_then(|name| self.supervisor.status(name))
            .or_else(|| self.supervisor.onboarding_agent(id));
        let Some(recovered) = recovered else {
            return Ok(session);
        };
        let agent_name = recovered.name.clone();
        let agent = self
            .supervisor
            .reconcile_attached(&agent_name)?
            .or_else(|| self.supervisor.status(&agent_name))
            .ok_or_else(|| {
                rk_core::Error::other(format!(
                    "onboarding session {id} references missing agent {agent_name}"
                ))
            })?;
        if agent.role != crate::onboarding_sessions::ONBOARDER_ROLE {
            return Err(rk_core::Error::other(format!(
                "onboarding session {id} agent {} has downgraded role {:?}",
                agent.name, agent.role
            )));
        }
        self.update_onboarding_from_agent(id, &agent, session.attached)
    }

    fn update_onboarding_from_agent(
        &self,
        id: &str,
        agent: &crate::agents::AgentRecord,
        attached: bool,
    ) -> rk_core::Result<crate::onboarding_sessions::OnboardingSession> {
        let state = match agent.state {
            crate::agents::AgentState::Spawning => {
                crate::onboarding_sessions::OnboardingSessionState::Starting
            }
            // A paused agent is still a running session: its process is alive
            // and the next steer resumes it. Reporting it `Completed` here
            // would close an onboarding session mid-flight.
            crate::agents::AgentState::Running | crate::agents::AgentState::Paused => {
                crate::onboarding_sessions::OnboardingSessionState::Running
            }
            crate::agents::AgentState::Completed => {
                crate::onboarding_sessions::OnboardingSessionState::Completed
            }
            crate::agents::AgentState::Failed
            | crate::agents::AgentState::Stopped
            | crate::agents::AgentState::Dismissed => {
                crate::onboarding_sessions::OnboardingSessionState::Failed
            }
            crate::agents::AgentState::Orphaned => {
                crate::onboarding_sessions::OnboardingSessionState::Orphaned
            }
        };
        self.onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))?
            .update(id, |session| {
                session.agent = Some(agent.name.clone());
                session.branch = agent
                    .branch
                    .clone()
                    .unwrap_or_else(|| session.branch.clone());
                session.worktree = agent
                    .worktree
                    .clone()
                    .unwrap_or_else(|| session.worktree.clone());
                session.attached = attached;
                session.attach_target = agent.attach_target.clone();
                session.agent_result = agent.result.clone();
                session.state = state;
            })?
            .ok_or_else(|| rk_core::Error::other(format!("no such onboarding session: {id}")))
    }

    fn update_onboarding_failure(&self, id: &str, detail: String) -> rk_core::Result<()> {
        self.onboarding_sessions
            .lock()
            .map_err(|_| rk_core::Error::other("onboarding session registry lock poisoned"))?
            .update(id, |session| {
                session.state = crate::onboarding_sessions::OnboardingSessionState::Failed;
                session.agent_result = Some(detail);
            })?;
        Ok(())
    }

    async fn handle_ticket_new(&self, req: Request) -> Response {
        let mut params: crate::tickets::NewTicket = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        // Filing follow-up work is agent-safe, but the author identity is not a
        // caller-controlled field. Otherwise an agent could create a ticket
        // that presents itself as the operator or another castle.
        if req.caller != "operator" && !req.caller.is_empty() {
            params.created_by = Some(req.caller.clone());
        }
        match self.tickets.create(params).await {
            Ok(tuple) => Response::ok(req.id, json!({"ticket": tuple})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_ticket_list(&self, req: Request) -> Response {
        let params: TicketListParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match self
            .tickets
            .list(params.scope, params.status, params.parent)
        {
            Ok(tickets) => {
                let blocked = self.tickets.blocked_ids(&tickets).unwrap_or_default();
                Response::ok(req.id, json!({"tickets": tickets, "blocked": blocked}))
            }
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_ticket_get(&self, req: Request) -> Response {
        let params: TicketGetParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match self.tickets.get(&params.id) {
            Ok(ticket) => {
                let blockers = self.tickets.blockers(&params.id).ok().flatten();
                Response::ok(req.id, json!({"ticket": ticket, "blockers": blockers}))
            }
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    async fn handle_ticket_update(&self, req: Request) -> Response {
        let params: TicketUpdateParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        // Bind `done` to delivery (TKT-01M08HB566GFBZVMDKZ8DT1ES0 / strategic-
        // review C3): a steward or operator marking a merge-mode/push-branch
        // ticket done before its branch actually landed is exactly the
        // TKT-18/46/147 "approved but never merged" class — refuse it here,
        // with a pointed error, instead of letting the ticket claim done.
        if params.changes.status.as_deref() == Some("done") {
            if let Err(e) = self.supervisor.require_ticket_delivered(&params.id).await {
                return Response::err(req.id, codes::INTERNAL, e.to_string());
            }
        }
        // The wire-level allowlist (`read_only_roles::method_allowed`) already
        // proved a groomer's request is exactly {id, status: "closed",
        // reason: {reason, evidence}} before this handler runs. Re-check the
        // shape here anyway — belt and suspenders — so a future change to that
        // allowlist cannot silently widen groomer capability without also
        // breaking the audit trail this handler is responsible for writing.
        let groom_audit = if self.supervisor.is_groomer(&req.caller) {
            let Some(reason) = params.reason.as_ref() else {
                return Response::err(
                    req.id,
                    codes::FORBIDDEN,
                    "groomer ticket.update requires a reason payload",
                );
            };
            if params.changes.status.as_deref() != Some("closed") {
                return Response::err(
                    req.id,
                    codes::FORBIDDEN,
                    "groomer ticket.update may only close a ticket",
                );
            }
            let prior_status = match self.tickets.get(&params.id) {
                Ok(Some(ticket)) => ticket
                    .payload
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("open")
                    .to_string(),
                Ok(None) => {
                    return Response::err(
                        req.id,
                        codes::INTERNAL,
                        format!("no such ticket: {}", params.id),
                    )
                }
                Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
            };
            Some((prior_status, reason.reason.clone(), reason.evidence.clone()))
        } else {
            None
        };
        match self.tickets.update(&params.id, params.changes).await {
            Ok(ticket) => {
                if let Some((prior_status, reason, evidence)) = groom_audit {
                    self.emit_event(
                        &ticket.scope,
                        "ticket-groomed",
                        json!({
                            "ticket": params.id,
                            "prior_status": prior_status,
                            "new_status": "closed",
                            "reason": reason,
                            "evidence": evidence,
                            "groomer": req.caller,
                        }),
                    );
                }
                Response::ok(req.id, json!({"ticket": ticket}))
            }
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    async fn handle_ticket_dep(&self, req: Request) -> Response {
        let params: TicketDepParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let result = if params.remove {
            self.tickets.remove_dep(&params.id, &params.dep).await
        } else {
            self.tickets.add_dep(&params.id, &params.dep).await
        };
        match result {
            Ok(ticket) => Response::ok(req.id, json!({"ticket": ticket})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    /// Explicit operator-only recovery: move a `done` (or `closed`) ticket
    /// back to `open`/`blocked`. The state machine (`valid_transition`)
    /// refuses `done -> in_progress` and any backwards move out of `closed`
    /// on an ordinary `ticket.update` — this is the one door back, gated to
    /// operator/foreman-equivalent callers by the same `authorize_reasoned`
    /// list that covers `ticket.update`/`ticket.dep`, so an agent cannot
    /// demote its own ticket out from under a reviewer. Every reopen through
    /// this door is announced as a `ticket_reopened` event, mirroring the
    /// `ticket_closed` audit trail `Tickets::edit` already emits on the
    /// forward edge.
    async fn handle_ticket_reopen(&self, req: Request) -> Response {
        let params: TicketReopenParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let previous_status = match self.tickets.get(&params.id) {
            Ok(Some(t)) => t
                .payload
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("open")
                .to_string(),
            Ok(None) => {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!("no such ticket: {}", params.id),
                )
            }
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        let status = params.status.as_deref().unwrap_or("open");
        match self.tickets.reopen(&params.id, status).await {
            Ok(ticket) => {
                self.emit_event(
                    &ticket.scope,
                    "ticket_reopened",
                    json!({
                        "ticket": ticket.identity,
                        "from_status": previous_status,
                        "to_status": status,
                        "by": req.caller,
                    }),
                );
                Response::ok(req.id, json!({"ticket": ticket}))
            }
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_ticket_ready(&self, req: Request) -> Response {
        let params: TicketReadyParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match self.tickets.ready(params.scope) {
            Ok(tickets) => Response::ok(req.id, json!({"tickets": tickets})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    /// Route an `agent.spawn` through the SAME cost-tier → profile-layering
    /// path the drain and the workflow fan-out use, so a ticket dispatched by
    /// hand lands on the profile the fleet's `[[tiers.rules]]` chose for it
    /// rather than silently on `[agents.default]`. Before this, tier routing
    /// was consulted only on the drain/fan-out paths, so every operator spawn
    /// quietly bypassed the fleet's ratified cost policy.
    ///
    /// Precedence, most specific first — the same order as
    /// [`rk_workflow::resolve`], with one deliberate difference:
    ///
    /// 1. explicit `harness`/`model`/`permission_mode` on the request
    /// 2. an explicit `profile` — an operator naming a profile *replaces*
    ///    routing rather than layering under it. (In a workflow the tier beats
    ///    the step's static `agent:`, because the point of tiers is to override
    ///    a definition's baked-in defaults; a flag typed at dispatch time is a
    ///    live decision and must be the way to opt out.)
    /// 3. the tier a routing rule picked from the ticket's labels/priority
    /// 4. global `[agents.default]`, unchanged
    ///
    /// Returns the profile to layer under the request's own fields plus a
    /// human-readable `(name, source)` for the reply, or `None` when nothing
    /// routed — in which case resolution is byte-for-byte what it was before.
    fn route_spawn_profile(
        &self,
        params: &crate::supervisor::SpawnParams,
    ) -> rk_core::Result<Option<(rk_workflow::AgentProfile, String, &'static str)>> {
        let (name, source) = match params.profile.as_deref() {
            Some(explicit) => (explicit.to_string(), "profile"),
            None => {
                // The spawn's task IS the ticket id on every ticket-backed
                // dispatch (drain and `rk spawn --ticket` alike), so the same
                // labels/priority the drain routes on are recoverable here. A
                // free-form task has neither, and matches only a catch-all rule
                // — which is exactly what a catch-all means.
                let ticket = self.tickets.get(&params.task).ok().flatten();
                let (labels, priority) = match ticket.as_ref() {
                    Some(t) => crate::drain::tier_key(&t.payload),
                    None => (Vec::new(), ""),
                };
                match self.tier_routing.route(&labels, Some(priority)) {
                    Some(tier) => (tier.to_string(), "tier"),
                    None => return Ok(None),
                }
            }
        };
        // Reuse the workflow resolver so an unknown name errors here exactly as
        // it does in a spawn step or a drain cycle, instead of silently falling
        // back to the default profile and masking the typo.
        let (agent, tier) = match source {
            "profile" => (Some(name.as_str()), None),
            _ => (None, Some(name.as_str())),
        };
        let resolved = rk_workflow::resolve::resolve_fields(
            agent,
            tier,
            None,
            None,
            None,
            &HashMap::new(),
            &self.global_agents,
            &self.default_harness,
        )?;
        Ok(Some((
            rk_workflow::AgentProfile {
                harness: Some(resolved.harness),
                model: resolved.model,
                permission_mode: resolved.permission_mode,
            },
            name,
            source,
        )))
    }

    async fn handle_spawn(&self, req: Request) -> Response {
        let params: crate::supervisor::SpawnParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let mut params = if req.caller == "operator" || req.caller.is_empty() {
            params
        } else {
            match self.supervisor.prepare_foreman_spawn(&req.caller, params) {
                Ok(p) => p,
                Err(e) => return Response::err(req.id, codes::FORBIDDEN, e.to_string()),
            }
        };
        // Cost-tier routing, applied before the spawn so an unknown tier/profile
        // is refused rather than dispatched onto the wrong model.
        let routing = match self.route_spawn_profile(&params) {
            Ok(routed) => routed,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
        };
        let routing = routing.map(|(profile, name, source)| {
            info!(task = %params.task, profile = %name, source, "spawn routed to agent profile");
            params.resolved_profile = Some(profile);
            json!({"profile": name, "source": source})
        });
        // Workflow-spawned foremen inherit the instance cap for children they
        // dispatch. The workflow engine remains the source of that definition;
        // the child spawn must not silently become an uncapped side door.
        if params.instance_max_usd.is_none() {
            if let Some(instance) = params.workflow_instance.as_deref() {
                params.instance_max_usd = self.engine().instance_budget(instance);
            }
        }
        // Manual/foreman-driven spawns are not subject to the fleet-WIP
        // ceiling (0 = no cap enforced by this call) — only the drain
        // autoscaler and workflow `spawn` steps admit against it.
        // `routing` is echoed back so the resolved profile and *why* it was
        // chosen are visible at dispatch instead of being inferred later from
        // the agent record's model. `null` = nothing routed: global defaults.
        let routing = routing.unwrap_or(Value::Null);
        match self.supervisor.spawn_async(params, 0).await {
            Ok(record) => Response::ok(req.id, json!({"agent": record, "routing": routing})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_progress(&self, req: Request) -> Response {
        if req.caller.is_empty() || req.caller == "operator" {
            return Response::err(
                req.id,
                codes::FORBIDDEN,
                "progress must be reported by an authenticated agent",
            );
        }
        let params: ProgressParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match self.supervisor.record_progress(
            &req.caller,
            params.summary,
            params.next,
            params.status,
        ) {
            Ok(agent) => Response::ok(
                req.id,
                json!({"agent": agent.name, "progress": agent.progress}),
            ),
            Err(e) => Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
        }
    }

    async fn handle_respawn(&self, req: Request) -> Response {
        let params: NameParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        if let Err(e) = self.authorize_foreman_child(&req.caller, &params.name) {
            return Response::err(req.id, codes::FORBIDDEN, e.to_string());
        }
        let supervisor = Arc::clone(&self.supervisor);
        let name = params.name;
        let handle = tokio::runtime::Handle::current();
        let result = tokio::task::spawn_blocking(move || {
            let _entered = handle.enter();
            supervisor
                .respawn(&name)
                .map(|record| json!({"agent": record}))
        })
        .await;
        match result {
            Ok(Ok(value)) => Response::ok(req.id, value),
            Ok(Err(e)) => Response::err(req.id, codes::INTERNAL, e.to_string()),
            Err(e) => Response::err(req.id, codes::INTERNAL, format!("respawn task failed: {e}")),
        }
    }

    fn handle_factory_propose_action(&self, req: Request) -> Response {
        let params: FactoryProposeParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let factory_action = match params.kind.as_str() {
            "workflow.run" => match serde_json::from_value(params.action)
                .map_err(|e| rk_core::Error::other(e.to_string()))
                .and_then(|action| self.resolve_workflow_action(action))
            {
                Ok(action) => FactoryAction::WorkflowRun(action),
                Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
            },
            "ticket_graph.apply" => match self.resolve_ticket_graph_apply_action(params.action) {
                Ok(action) => FactoryAction::TicketGraphApply(action),
                Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
            },
            "product_to_code.dispatch" => {
                match self.resolve_product_to_code_dispatch_action(params.action) {
                    Ok(action) => FactoryAction::ProductToCodeDispatch(action),
                    Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
                }
            }
            _ => return Response::err(req.id, codes::BAD_PARAMS, "unsupported action kind"),
        };
        match self
            .action_approvals
            .propose_action(&req.caller, factory_action, params.ttl_seconds)
        {
            Ok(proposal) => {
                self.emit_factory_event(crate::factory_events::event_tuple(
                    &proposal.scope.repo.identity,
                    &req.caller,
                    "approval.changed",
                    "factory.propose_action",
                    json!({"proposal_id": proposal.id}),
                    "factory action approval proposed",
                    json!({"proposal_id": proposal.id, "digest": proposal.digest, "status": proposal.status, "coordinator": factory_action_coordinator(&proposal.action)}),
                ));
                Response::ok(
                    req.id,
                    json!({"proposal": proposal, "digest": proposal.digest}),
                )
            }
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_factory_approve_action(&self, req: Request) -> Response {
        let params: FactoryApproveParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match self
            .action_approvals
            .approve(&params.proposal_id, &params.digest, &req.caller)
        {
            Ok(approval) => {
                let coordinator = self.action_approvals.list().ok().and_then(|proposals| {
                    proposals
                        .into_iter()
                        .find(|proposal| proposal.id == approval.proposal_id)
                        .and_then(|proposal| {
                            factory_action_coordinator(&proposal.action).map(str::to_string)
                        })
                });
                self.emit_factory_event(crate::factory_events::event_tuple(
                    &approval.scope.repo.identity,
                    &req.caller,
                    "approval.changed",
                    "factory.approve_action",
                    json!({"proposal_id": approval.proposal_id}),
                    "workflow run approval approved",
                    json!({"proposal_id": approval.proposal_id, "digest": approval.digest, "status": approval.status, "coordinator": coordinator}),
                ));
                Response::ok(req.id, json!({"approval": approval}))
            }
            Err(e) => Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
        }
    }

    async fn handle_factory_execute_action(&self, req: Request) -> Response {
        let params: FactoryExecuteParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match params.kind.as_deref().unwrap_or("workflow.run") {
            "ticket_graph.apply" => {
                return self.handle_ticket_graph_apply_execute(req, params).await;
            }
            "product_to_code.dispatch" => {
                return self
                    .handle_product_to_code_dispatch_execute(req, params)
                    .await;
            }
            "workflow.run" => {}
            _ => return Response::err(req.id, codes::BAD_PARAMS, "unsupported action kind"),
        }
        let workflow_params: WorkflowRunParams = match serde_json::from_value(params.action) {
            Ok(action) => action,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
        };
        let action = match self.resolve_workflow_action(workflow_params) {
            Ok(action) => action,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
        };
        let grant = match self.action_approvals.begin_execute(
            &params.proposal_id,
            &params.digest,
            &req.caller,
            &action,
        ) {
            Ok(grant) => grant,
            Err(e) => return Response::err(req.id, codes::FORBIDDEN, e.to_string()),
        };
        if let Some(instance_id) = grant.instance_id.as_deref() {
            if let Some(instance) = self.engine().status_any(instance_id) {
                return Response::ok(req.id, json!({"instance": instance, "approval": grant}));
            }
        }
        if grant.status == ApprovalStatus::Consumed {
            return Response::err(req.id, codes::FORBIDDEN, "approval already consumed");
        }
        let Some(instance_id) = grant.instance_id.clone() else {
            return Response::err(
                req.id,
                codes::INTERNAL,
                "approval missing bound instance id",
            );
        };
        let engine = self.engine();
        let handle = tokio::runtime::Handle::current();
        let result = tokio::task::spawn_blocking(move || {
            let _entered = handle.enter();
            engine.run_owned_with_id(
                instance_id,
                &action.name,
                &action.repo_path,
                action.params.into_iter().collect(),
                action.coordinator,
            )
        })
        .await;
        match result {
            Ok(Ok(instance)) => {
                let approval = match self
                    .action_approvals
                    .finish_success(&params.proposal_id, &instance.id)
                {
                    Ok(approval) => approval,
                    Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
                };
                let coordinator = instance.coordinator.clone();
                self.emit_factory_event(crate::factory_events::event_tuple(
                    &approval.scope.repo.identity,
                    &req.caller,
                    "workflow.changed",
                    "workflow.run",
                    json!({"id": instance.id, "workflow": instance.workflow, "coordinator": instance.coordinator}),
                    "workflow run started",
                    json!({"id": instance.id, "status": instance.status, "proposal_id": params.proposal_id, "coordinator": instance.coordinator}),
                ));
                self.emit_factory_event(crate::factory_events::event_tuple(
                    &approval.scope.repo.identity,
                    &req.caller,
                    "approval.changed",
                    "factory.execute_action",
                    json!({"proposal_id": approval.proposal_id, "instance_id": instance.id, "coordinator": coordinator}),
                    "workflow run approval consumed",
                    json!({"proposal_id": approval.proposal_id, "digest": approval.digest, "status": approval.status, "instance_id": instance.id, "coordinator": coordinator}),
                ));
                Response::ok(req.id, json!({"instance": instance, "approval": approval}))
            }
            Ok(Err(e)) => {
                let _ = self
                    .action_approvals
                    .finish_failed(&params.proposal_id, &e.to_string());
                Response::err(req.id, codes::INTERNAL, e.to_string())
            }
            Err(e) => {
                let msg = format!("workflow task failed: {e}");
                let _ = self
                    .action_approvals
                    .finish_failed(&params.proposal_id, &msg);
                Response::err(req.id, codes::INTERNAL, msg)
            }
        }
    }

    async fn handle_ticket_graph_apply_execute(
        &self,
        req: Request,
        params: FactoryExecuteParams,
    ) -> Response {
        let submitted = match self.resolve_ticket_graph_apply_action(params.action) {
            Ok(action) => action,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
        };
        let proposal = match self.action_approvals.proposal(&params.proposal_id) {
            Ok(Some(proposal)) => proposal,
            Ok(None) => return Response::err(req.id, codes::FORBIDDEN, "unknown proposal"),
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        let stored = match proposal.action.clone() {
            FactoryAction::TicketGraphApply(action) => action,
            _ => {
                return Response::err(req.id, codes::FORBIDDEN, "proposal action kind mismatch");
            }
        };
        let submitted_action = FactoryAction::TicketGraphApply(submitted.clone());
        if submitted.repo != stored.repo
            || submitted.repo_identity != stored.repo_identity
            || submitted.repo_path != stored.repo_path
            || submitted.graph != stored.graph
            || submitted.initiative != stored.initiative
            || submitted.apply_plan != stored.apply_plan
        {
            let recomputed =
                crate::action_approval::recompute_proposal_digest(&proposal, &submitted_action)
                    .unwrap_or_else(|error| format!("<error:{error}>"));
            return Response::err(
                req.id,
                codes::FORBIDDEN,
                format!(
                    "action digest mismatch: expected={} provided={} recomputed={recomputed}",
                    proposal.digest, params.digest
                ),
            );
        }

        let factory_action = FactoryAction::TicketGraphApply(stored.clone());
        let grant = match self.action_approvals.begin_execute_action(
            &params.proposal_id,
            &params.digest,
            &req.caller,
            &factory_action,
        ) {
            Ok(grant) => grant,
            Err(e) => return Response::err(req.id, codes::FORBIDDEN, e.to_string()),
        };
        let Some(execution_id) = grant.execution_id.clone() else {
            return Response::err(req.id, codes::INTERNAL, "approval missing execution id");
        };
        let _guard = self.ticket_graph_apply_lock.lock().await;
        let ticket_guard = self.tickets.mutation_guard().await;
        let existing = match self
            .action_approvals
            .ticket_graph_result(&params.proposal_id)
        {
            Ok(result) => result,
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        if let Some(mut result) = existing
            .clone()
            .filter(|result| result.status == "completed")
        {
            let (reconciled, approval) = match self
                .action_approvals
                .finish_ticket_graph_success(&params.proposal_id, result.clone())
            {
                Ok(value) => value,
                Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
            };
            result = reconciled;
            result.idempotent_replay = true;
            return Response::ok(req.id, json!({"result": result, "approval": approval}));
        }
        let actual_preconditions =
            match self.ticket_graph_live_preconditions(&stored, &ticket_guard, &execution_id) {
                Ok(preconditions) => preconditions,
                Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
            };
        if actual_preconditions != stored.preconditions {
            let message = format!(
                "ticket graph CAS mismatch: expected repo_head={} ticket_store_digest={}, actual repo_head={} ticket_store_digest={}",
                stored.preconditions.repo_head,
                stored.preconditions.ticket_store_digest,
                actual_preconditions.repo_head,
                actual_preconditions.ticket_store_digest,
            );
            let _ = self
                .action_approvals
                .finish_failed(&params.proposal_id, &message);
            return Response::err(req.id, codes::FORBIDDEN, message);
        }
        let mut result = existing.unwrap_or_else(|| TicketGraphApplyExecutionResult {
            execution_id: execution_id.clone(),
            graph_id: stored.graph.id.clone(),
            graph_node_to_ticket_id: BTreeMap::new(),
            created_ticket_ids: Vec::new(),
            created_dependency_edges: Vec::new(),
            idempotent_replay: false,
            status: "executing".into(),
        });
        if result.graph_node_to_ticket_id.is_empty() {
            result = match self
                .action_approvals
                .checkpoint_ticket_graph_result(&params.proposal_id, result)
            {
                Ok(result) => result,
                Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
            };
        }

        let applied = self
            .apply_ticket_graph(&params.proposal_id, &stored, &ticket_guard, result)
            .await;
        match applied {
            Ok(mut result) => {
                result.status = "completed".into();
                let (result, approval) = match self
                    .action_approvals
                    .finish_ticket_graph_success(&params.proposal_id, result)
                {
                    Ok(value) => value,
                    Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
                };
                self.emit_factory_event(crate::factory_events::event_tuple(
                    &stored.repo_identity,
                    &req.caller,
                    "ticket.changed",
                    "ticket_graph.apply",
                    json!({"graph_id": stored.graph.id, "execution_id": execution_id}),
                    "approved ticket graph applied",
                    json!({"proposal_id": params.proposal_id, "status": result.status, "created_ticket_ids": result.created_ticket_ids}),
                ));
                Response::ok(req.id, json!({"result": result, "approval": approval}))
            }
            Err(e) => {
                let _ = self
                    .action_approvals
                    .finish_failed(&params.proposal_id, &e.to_string());
                Response::err(req.id, codes::INTERNAL, e.to_string())
            }
        }
    }

    fn ticket_graph_live_preconditions(
        &self,
        action: &TicketGraphApplyAction,
        ticket_guard: &crate::tickets::TicketMutationGuard<'_>,
        execution_id: &str,
    ) -> rk_core::Result<TicketGraphApplyPreconditions> {
        let created_by = format!("factory:{execution_id}");
        Ok(TicketGraphApplyPreconditions {
            repo_head: repository_head(std::path::Path::new(&action.repo_path))?,
            ticket_store_digest: self.tickets.snapshot_digest_excluding_created_by(
                ticket_guard,
                &action.repo_identity,
                &created_by,
            )?,
        })
    }

    async fn apply_ticket_graph(
        &self,
        proposal_id: &str,
        action: &TicketGraphApplyAction,
        ticket_guard: &crate::tickets::TicketMutationGuard<'_>,
        mut result: TicketGraphApplyExecutionResult,
    ) -> rk_core::Result<TicketGraphApplyExecutionResult> {
        for node_id in &action.apply_plan.topological_order {
            if result.graph_node_to_ticket_id.contains_key(node_id) {
                continue;
            }
            let create = action
                .apply_plan
                .creates
                .iter()
                .find(|create| create.stable_graph_node_id == *node_id)
                .ok_or_else(|| {
                    rk_core::Error::other(format!(
                        "ticket graph apply plan missing create for {node_id}"
                    ))
                })?;
            let labels = std::iter::once("product-to-code".to_string())
                .chain(std::iter::once(format!("graph:{}", action.graph.id)))
                .chain(std::iter::once(format!("node:{node_id}")))
                .chain(
                    create
                        .acceptance_criterion_ids
                        .iter()
                        .map(|criterion| format!("criterion:{criterion}")),
                )
                .collect();
            let (ticket, _created) = self.tickets.create_idempotent_locked(
                ticket_guard,
                crate::tickets::NewTicket {
                    title: create.title.clone(),
                    body: Some(create.description.clone()),
                    scope: Some(action.repo_identity.clone()),
                    parent: None,
                    priority: "normal".into(),
                    labels,
                    depends_on: Vec::new(),
                    created_by: Some(format!("factory:{}", result.execution_id)),
                    coalesce_key: Some(format!(
                        "factory:ticket-graph:{}:{node_id}",
                        result.execution_id
                    )),
                },
            )?;
            result
                .graph_node_to_ticket_id
                .insert(node_id.clone(), ticket.identity.clone());
            if !result.created_ticket_ids.contains(&ticket.identity) {
                result.created_ticket_ids.push(ticket.identity);
            }
            result = self
                .action_approvals
                .checkpoint_ticket_graph_result(proposal_id, result)?;
        }

        for dependency in &action.apply_plan.dependencies {
            let blocked_ticket_id = result
                .graph_node_to_ticket_id
                .get(&dependency.blocked_graph_node_id)
                .cloned()
                .ok_or_else(|| rk_core::Error::other("missing blocked ticket mapping"))?;
            let dependency_ticket_id = result
                .graph_node_to_ticket_id
                .get(&dependency.dependency_graph_node_id)
                .cloned()
                .ok_or_else(|| rk_core::Error::other("missing dependency ticket mapping"))?;
            let edge = TicketGraphAppliedEdge {
                blocked_ticket_id: blocked_ticket_id.clone(),
                dependency_ticket_id: dependency_ticket_id.clone(),
            };
            if result.created_dependency_edges.contains(&edge) {
                continue;
            }
            self.tickets
                .add_dep_locked(ticket_guard, &blocked_ticket_id, &dependency_ticket_id)
                .await?;
            result.created_dependency_edges.push(edge);
            result = self
                .action_approvals
                .checkpoint_ticket_graph_result(proposal_id, result)?;
        }
        Ok(result)
    }

    fn resolve_workflow_action(
        &self,
        params: WorkflowRunParams,
    ) -> rk_core::Result<WorkflowRunAction> {
        let submitted = std::path::PathBuf::from(&params.repo);
        let canonical_submitted = if submitted.exists() {
            submitted.canonicalize()?
        } else {
            submitted
        };
        let repos = self
            .repos
            .lock()
            .map_err(|_| rk_core::Error::other("repo registry lock poisoned"))?;
        let record = repos
            .get(&params.repo)
            .cloned()
            .or_else(|| repos.get_by_path(&canonical_submitted).cloned())
            .ok_or_else(|| {
                rk_core::Error::other(format!("repository is not registered: {}", params.repo))
            })?;
        let canonical_path = record.path.canonicalize().unwrap_or(record.path.clone());
        Ok(WorkflowRunAction {
            name: params.name,
            repo: record.name.clone(),
            repo_identity: record.name,
            repo_path: canonical_path.display().to_string(),
            params: params.params.into_iter().collect::<BTreeMap<_, _>>(),
            coordinator: params.coordinator,
        })
    }

    fn resolve_ticket_graph_apply_action(
        &self,
        params: serde_json::Value,
    ) -> rk_core::Result<TicketGraphApplyAction> {
        let params: TicketGraphApplyParams = serde_json::from_value(params).map_err(|e| {
            rk_core::Error::other(format!("invalid ticket_graph.apply action: {e}"))
        })?;
        params.initiative.validate()?;
        let repo = params.repo.as_str();
        let submitted = std::path::PathBuf::from(repo);
        let canonical_submitted = if submitted.exists() {
            submitted.canonicalize()?
        } else {
            submitted
        };
        let repos = self
            .repos
            .lock()
            .map_err(|_| rk_core::Error::other("repo registry lock poisoned"))?;
        let record = repos
            .get(repo)
            .cloned()
            .or_else(|| repos.get_by_path(&canonical_submitted).cloned())
            .ok_or_else(|| {
                rk_core::Error::other(format!("repository is not registered: {repo}"))
            })?;
        let canonical_path = record.path.canonicalize().unwrap_or(record.path.clone());
        let apply_plan = params
            .graph
            .apply_plan_for_initiative(&record.name, &params.initiative)?;
        drop(repos);
        let preconditions = TicketGraphApplyPreconditions {
            repo_head: repository_head(&canonical_path)?,
            ticket_store_digest: self.tickets.snapshot_digest(&record.name)?,
        };
        Ok(TicketGraphApplyAction {
            repo: record.name.clone(),
            repo_identity: record.name,
            repo_path: canonical_path.display().to_string(),
            graph: params.graph,
            initiative: params.initiative,
            apply_plan,
            preconditions,
        })
    }

    /// Resolve a submitted `product_to_code.dispatch` payload into the
    /// canonical typed action. The action references the prior approved
    /// `ticket_graph.apply` execution and resolves every dispatch's graph node
    /// id through that execution's minted graph-node-id -> TKT-id mapping.
    /// Blocked nodes are carried separately and are never dispatched.
    fn resolve_product_to_code_dispatch_action(
        &self,
        params: serde_json::Value,
    ) -> rk_core::Result<ProductToCodeDispatchAction> {
        let params: ProductToCodeDispatchParams = serde_json::from_value(params).map_err(|e| {
            rk_core::Error::other(format!("invalid product_to_code.dispatch action: {e}"))
        })?;
        params.initiative.validate()?;
        if params.graph.id != params.graph_id {
            return Err(rk_core::Error::other(format!(
                "submitted graph revision {} does not match graph_id {}",
                params.graph.id, params.graph_id
            )));
        }
        let graph_report = params
            .graph
            .validation_report_for_initiative(&params.initiative);
        if !graph_report.valid {
            return Err(rk_core::Error::other(format!(
                "submitted graph revision is invalid: {}",
                graph_report.errors.join("; ")
            )));
        }
        let repo = params.repo.as_str();
        let submitted = std::path::PathBuf::from(repo);
        let canonical_submitted = if submitted.exists() {
            submitted.canonicalize()?
        } else {
            submitted
        };
        let repos = self
            .repos
            .lock()
            .map_err(|_| rk_core::Error::other("repo registry lock poisoned"))?;
        let record = repos
            .get(repo)
            .cloned()
            .or_else(|| repos.get_by_path(&canonical_submitted).cloned())
            .ok_or_else(|| {
                rk_core::Error::other(format!("repository is not registered: {repo}"))
            })?;
        drop(repos);
        let canonical_path = record.path.canonicalize().unwrap_or(record.path.clone());

        // The dispatch is bound to one prior approved graph apply execution.
        let apply_proposal = self
            .action_approvals
            .proposal(&params.graph_apply_proposal_id)?
            .ok_or_else(|| {
                rk_core::Error::other(format!(
                    "unknown graph apply proposal {}",
                    params.graph_apply_proposal_id
                ))
            })?;
        let FactoryAction::TicketGraphApply(apply_action) = &apply_proposal.action else {
            return Err(rk_core::Error::other(format!(
                "proposal {} is not a ticket graph apply",
                params.graph_apply_proposal_id
            )));
        };
        let apply_repo_path = std::path::PathBuf::from(&apply_action.repo_path)
            .canonicalize()
            .unwrap_or_else(|_| std::path::PathBuf::from(&apply_action.repo_path));
        if apply_action.repo_identity != record.name || apply_repo_path != canonical_path {
            return Err(rk_core::Error::other(format!(
                "graph apply proposal {} belongs to a different repository",
                params.graph_apply_proposal_id
            )));
        }
        if apply_action.graph.id != params.graph_id {
            return Err(rk_core::Error::other(format!(
                "graph apply proposal {} applied graph {}, not {}",
                params.graph_apply_proposal_id, apply_action.graph.id, params.graph_id
            )));
        }
        if apply_action.initiative != params.initiative {
            return Err(rk_core::Error::other(format!(
                "graph apply proposal {} initiative revision does not match the submitted initiative",
                params.graph_apply_proposal_id
            )));
        }
        if apply_action.graph != params.graph {
            return Err(rk_core::Error::other(format!(
                "graph apply proposal {} graph revision does not match the submitted graph",
                params.graph_apply_proposal_id
            )));
        }
        let apply_result = self
            .action_approvals
            .ticket_graph_result(&params.graph_apply_proposal_id)?
            .filter(|result| result.status == "completed")
            .ok_or_else(|| {
                rk_core::Error::other(format!(
                    "graph apply proposal {} has no completed execution; approve and execute the graph apply first",
                    params.graph_apply_proposal_id
                ))
            })?;

        let graph = &apply_action.graph;
        let mut dispatches = Vec::new();
        for request in &params.dispatches {
            let ticket_id = apply_result
                .graph_node_to_ticket_id
                .get(&request.graph_node_id)
                .cloned()
                .ok_or_else(|| {
                    rk_core::Error::other(format!(
                        "graph node {} has no minted ticket in graph apply execution {}",
                        request.graph_node_id, apply_result.execution_id
                    ))
                })?;
            let description = graph
                .nodes
                .iter()
                .find(|node| node.id == request.graph_node_id)
                .map(|node| node.description.clone())
                .unwrap_or_else(|| request.task_description.clone());
            let mut workflow_params = BTreeMap::new();
            workflow_params.insert("taskId".to_string(), json!(ticket_id));
            workflow_params.insert("taskDescription".to_string(), json!(description));
            dispatches.push(ProductToCodeWorkflowDispatch {
                graph_node_id: request.graph_node_id.clone(),
                ticket_id,
                workflow: "implement-featureset".to_string(),
                params: workflow_params,
            });
        }
        for blocked in &params.blocked {
            if !apply_result
                .graph_node_to_ticket_id
                .contains_key(&blocked.graph_node_id)
            {
                return Err(rk_core::Error::other(format!(
                    "blocked graph node {} is not part of graph apply execution {}",
                    blocked.graph_node_id, apply_result.execution_id
                )));
            }
            if dispatches
                .iter()
                .any(|dispatch| dispatch.graph_node_id == blocked.graph_node_id)
            {
                return Err(rk_core::Error::other(format!(
                    "graph node {} cannot be both dispatched and blocked",
                    blocked.graph_node_id
                )));
            }
        }
        let preconditions = TicketGraphApplyPreconditions {
            repo_head: repository_head(&canonical_path)?,
            ticket_store_digest: self.tickets.snapshot_digest(&record.name)?,
        };
        Ok(ProductToCodeDispatchAction {
            repo: record.name.clone(),
            repo_identity: record.name,
            repo_path: canonical_path.display().to_string(),
            initiative: params.initiative,
            graph_id: params.graph_id,
            graph_apply_proposal_id: params.graph_apply_proposal_id,
            graph_apply_execution_id: apply_result.execution_id,
            graph_node_to_ticket_id: apply_result.graph_node_to_ticket_id,
            dispatches,
            blocked: params.blocked,
            preconditions,
        })
    }

    /// Execute an approved `product_to_code.dispatch`. Reuses the Phase 2
    /// canonical proposal validator (`begin_execute_action` +
    /// `recompute_proposal_digest`) for status/digest binding, rechecks CAS
    /// preconditions, and dispatches `implement-featureset` workflow runs for
    /// unblocked minted tickets only.
    async fn handle_product_to_code_dispatch_execute(
        &self,
        req: Request,
        params: FactoryExecuteParams,
    ) -> Response {
        let submitted = match self.resolve_product_to_code_dispatch_action(params.action) {
            Ok(action) => action,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
        };
        let proposal = match self.action_approvals.proposal(&params.proposal_id) {
            Ok(Some(proposal)) => proposal,
            Ok(None) => return Response::err(req.id, codes::FORBIDDEN, "unknown proposal"),
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        let stored = match proposal.action.clone() {
            FactoryAction::ProductToCodeDispatch(action) => action,
            _ => {
                return Response::err(req.id, codes::FORBIDDEN, "proposal action kind mismatch");
            }
        };
        let submitted_action = FactoryAction::ProductToCodeDispatch(submitted.clone());
        if submitted.repo != stored.repo
            || submitted.repo_identity != stored.repo_identity
            || submitted.repo_path != stored.repo_path
            || submitted.initiative != stored.initiative
            || submitted.graph_id != stored.graph_id
            || submitted.graph_apply_proposal_id != stored.graph_apply_proposal_id
            || submitted.graph_apply_execution_id != stored.graph_apply_execution_id
            || submitted.graph_node_to_ticket_id != stored.graph_node_to_ticket_id
            || submitted.dispatches != stored.dispatches
            || submitted.blocked != stored.blocked
        {
            let recomputed =
                crate::action_approval::recompute_proposal_digest(&proposal, &submitted_action)
                    .unwrap_or_else(|error| format!("<error:{error}>"));
            return Response::err(
                req.id,
                codes::FORBIDDEN,
                format!(
                    "action digest mismatch: expected={} provided={} recomputed={recomputed}",
                    proposal.digest, params.digest
                ),
            );
        }

        let factory_action = FactoryAction::ProductToCodeDispatch(stored.clone());
        let grant = match self.action_approvals.begin_execute_action(
            &params.proposal_id,
            &params.digest,
            &req.caller,
            &factory_action,
        ) {
            Ok(grant) => grant,
            Err(e) => return Response::err(req.id, codes::FORBIDDEN, e.to_string()),
        };
        let Some(execution_id) = grant.execution_id.clone() else {
            return Response::err(req.id, codes::INTERNAL, "approval missing execution id");
        };
        let _guard = self.ticket_graph_apply_lock.lock().await;
        let existing = match self
            .action_approvals
            .product_to_code_result(&params.proposal_id)
        {
            Ok(result) => result,
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        if let Some(mut result) = existing
            .clone()
            .filter(|result| result.status == "completed")
        {
            let (reconciled, approval) = match self
                .action_approvals
                .finish_product_to_code_success(&params.proposal_id, result.clone())
            {
                Ok(value) => value,
                Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
            };
            result = reconciled;
            result.idempotent_replay = true;
            return Response::ok(req.id, json!({"result": result, "approval": approval}));
        }
        let actual_preconditions = TicketGraphApplyPreconditions {
            repo_head: match repository_head(std::path::Path::new(&stored.repo_path)) {
                Ok(head) => head,
                Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
            },
            ticket_store_digest: match self.tickets.snapshot_digest(&stored.repo_identity) {
                Ok(digest) => digest,
                Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
            },
        };
        if actual_preconditions != stored.preconditions {
            let message = format!(
                "product_to_code dispatch CAS mismatch: expected repo_head={} ticket_store_digest={}, actual repo_head={} ticket_store_digest={}",
                stored.preconditions.repo_head,
                stored.preconditions.ticket_store_digest,
                actual_preconditions.repo_head,
                actual_preconditions.ticket_store_digest,
            );
            let _ = self
                .action_approvals
                .finish_failed(&params.proposal_id, &message);
            return Response::err(req.id, codes::FORBIDDEN, message);
        }
        let mut result = existing.unwrap_or_else(|| ProductToCodeDispatchExecutionResult {
            execution_id: execution_id.clone(),
            graph_id: stored.graph_id.clone(),
            dispatched: Vec::new(),
            blocked: stored.blocked.clone(),
            idempotent_replay: false,
            status: "executing".into(),
        });
        result = match self
            .action_approvals
            .checkpoint_product_to_code_result(&params.proposal_id, result)
        {
            Ok(result) => result,
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };

        let engine = self.engine();
        for dispatch in &stored.dispatches {
            if result
                .dispatched
                .iter()
                .any(|done| done.ticket_id == dispatch.ticket_id)
            {
                continue;
            }
            let instance_id = product_to_code_instance_id(&execution_id, &dispatch.ticket_id);
            let engine = Arc::clone(&engine);
            let workflow = dispatch.workflow.clone();
            let repo_path = stored.repo_path.clone();
            let workflow_params: HashMap<String, Value> =
                dispatch.params.clone().into_iter().collect();
            let handle = tokio::runtime::Handle::current();
            let launch_id = instance_id.clone();
            let launched = tokio::task::spawn_blocking(move || {
                let _entered = handle.enter();
                engine.run_owned_with_id(launch_id, &workflow, &repo_path, workflow_params, None)
            })
            .await;
            let instance = match launched {
                Ok(Ok(instance)) => instance,
                Ok(Err(e)) => {
                    let _ = self
                        .action_approvals
                        .finish_failed(&params.proposal_id, &e.to_string());
                    return Response::err(req.id, codes::INTERNAL, e.to_string());
                }
                Err(e) => {
                    let msg = format!("workflow dispatch task failed: {e}");
                    let _ = self
                        .action_approvals
                        .finish_failed(&params.proposal_id, &msg);
                    return Response::err(req.id, codes::INTERNAL, msg);
                }
            };
            result.dispatched.push(ProductToCodeDispatchedWorkflow {
                graph_node_id: dispatch.graph_node_id.clone(),
                ticket_id: dispatch.ticket_id.clone(),
                workflow: dispatch.workflow.clone(),
                instance_id: instance.id,
            });
            result = match self
                .action_approvals
                .checkpoint_product_to_code_result(&params.proposal_id, result)
            {
                Ok(result) => result,
                Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
            };
        }

        result.status = "completed".into();
        let (result, approval) = match self
            .action_approvals
            .finish_product_to_code_success(&params.proposal_id, result)
        {
            Ok(value) => value,
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        self.emit_factory_event(crate::factory_events::event_tuple(
            &stored.repo_identity,
            &req.caller,
            "workflow.changed",
            "product_to_code.dispatch",
            json!({"graph_id": stored.graph_id, "execution_id": execution_id}),
            "approved product-to-code dispatch executed",
            json!({"proposal_id": params.proposal_id, "status": result.status, "dispatched": result.dispatched, "blocked": result.blocked}),
        ));
        Response::ok(req.id, json!({"result": result, "approval": approval}))
    }

    async fn handle_workflow_run(&self, req: Request) -> Response {
        let params: WorkflowRunParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let engine = self.engine();
        let handle = tokio::runtime::Handle::current();
        let result = tokio::task::spawn_blocking(move || {
            let _entered = handle.enter();
            engine.run_owned(
                &params.name,
                &params.repo,
                params.params,
                params.coordinator,
            )
        })
        .await;
        match result {
            Ok(Ok(instance)) => Response::ok(req.id, json!({"instance": instance})),
            Ok(Err(e)) => Response::err(req.id, codes::INTERNAL, e.to_string()),
            Err(e) => Response::err(
                req.id,
                codes::INTERNAL,
                format!("workflow task failed: {e}"),
            ),
        }
    }

    /// `verify.run` — the managed alternative to a rat self-invoking a full
    /// verification suite directly (TKT-01M0HNESEECWWFQF8X6VH1XSJ6): resolves
    /// `params.check` (default `"verify"`) from the caller's own repo
    /// registry, and runs it through [`WorkflowEngine::verify_repo_check`] —
    /// the exact same bounded per-repo admission queue, env-stripped
    /// execution, and exact-exit provenance a landing gate or workflow `run`
    /// step already gets from `run_check_in`.
    ///
    /// The operator (or an unauthenticated/empty caller) runs against the
    /// repo's registered root checkout; any other caller must be a
    /// currently-supervised agent whose OWN `repo_name` matches `params.repo`
    /// — an agent cannot direct a verification run at a repo it is not
    /// working in, and runs in ITS OWN worktree (uncommitted branch work
    /// included), not a shared checkout.
    ///
    /// Bound to this exact call's lifecycle (TKT-01M0PA6C5WYRWS757R1SS2F2GR):
    /// `verify_request_key` correlates it with `serve_conn`'s
    /// [`dispatch_watching_disconnect`](Self::dispatch_watching_disconnect),
    /// which cancels it the moment this RPC connection dies mid-call.
    async fn handle_verify_run(&self, req: Request) -> Response {
        let params: VerifyRunParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let check_name = params.check.as_deref().unwrap_or("verify").to_string();
        // The agent generation this call is bound to for cancellation
        // (TKT-01M0PA6C5WYRWS757R1SS2F2GR): `None` for the operator, who has
        // no live agent record to fence a namesake against.
        let mut generation: Option<rk_core::id::SpawnId> = None;
        let dir = if req.caller.is_empty() || req.caller == crate::client::OPERATOR {
            match self.repos.lock() {
                Ok(registry) => match registry.get(&params.repo) {
                    Some(record) => record.path.clone(),
                    None => {
                        return Response::err(
                            req.id,
                            codes::BAD_PARAMS,
                            format!("no repo registered named '{}'", params.repo),
                        )
                    }
                },
                Err(_) => {
                    return Response::err(req.id, codes::INTERNAL, "repo registry lock poisoned")
                }
            }
        } else {
            let Some(record) = self.supervisor.status(&req.caller) else {
                return Response::err(
                    req.id,
                    codes::INTERNAL,
                    format!("no agent record for caller '{}'", req.caller),
                );
            };
            if record.repo_name != params.repo {
                return Response::err(
                    req.id,
                    codes::FORBIDDEN,
                    format!(
                        "{} may only verify.run its own repo ('{}'), not '{}'",
                        req.caller, record.repo_name, params.repo
                    ),
                );
            }
            generation = Some(record.spawn_id());
            match record.worktree {
                Some(worktree) => worktree,
                None => {
                    return Response::err(
                        req.id,
                        codes::INTERNAL,
                        format!("agent '{}' has no worktree", req.caller),
                    )
                }
            }
        };
        let caller_label = if req.caller.is_empty() {
            crate::client::OPERATOR
        } else {
            req.caller.as_str()
        };
        let request_key = verify_request_key(&req.caller, &req.id);
        match self
            .engine()
            .verify_repo_check(
                caller_label,
                &dir,
                &params.repo,
                &check_name,
                generation,
                &request_key,
            )
            .await
        {
            Ok(result) => Response::ok(req.id, result),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    /// `agent.archive` — offload settled terminal records out of the default
    /// views. The daemon owns `agents.json` and rewrites it on every mutation,
    /// so this has to be an RPC: an external edit would be clobbered by the
    /// next `Registry::persist`.
    async fn handle_agent_archive(&self, req: Request) -> Response {
        let params: AgentArchiveParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let now = chrono::Utc::now();
        // `all` means "everything eligible right now" — a cutoff of now, since
        // eligibility is `updated_at < cutoff`.
        let cutoff = if params.all {
            now
        } else {
            match crate::agents::cutoff_from_spec(params.before.as_deref().unwrap_or("7d"), now) {
                Ok(c) => c,
                Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
            }
        };
        let reap = crate::supervisor::Reap {
            git: params.reap_git,
            logs: params.reap_logs,
            artifacts: params.reap_artifacts,
            artifact_paths: if params.reap_artifacts {
                self.worktree_sweep_config.artifact_paths.clone()
            } else {
                Vec::new()
            },
            artifact_paths_by_repo: if params.reap_artifacts {
                self.worktree_sweep_config.artifact_paths_by_repo.clone()
            } else {
                HashMap::new()
            },
        };
        let supervisor = Arc::clone(&self.supervisor);
        let engine = self.engine();
        // `reap_git` doubles as "also reclaim gate worktrees": both are the
        // same kind of resource (a daemon-managed git worktree), so one flag
        // covers both rather than growing a second one an operator has to
        // learn. Thresholds always come from the live sweep config — a
        // manual `rk prune --reap-git` is not gated by
        // `gate_worktree_sweep_config.enabled`, the periodic-timer switch,
        // the same way `rk prune --reap-git` itself ignores `worktree_sweep
        // .enabled`.
        let landing = self.landing();
        let gate_worktree_cfg = self.gate_worktree_sweep_config.clone();
        let handle = tokio::runtime::Handle::current();
        let result = tokio::task::spawn_blocking(move || {
            let _entered = handle.enter();
            let mut value = supervisor.archive_agents(cutoff, params.dry_run, reap)?;
            // The same sweep clears the workflow side of the board (TKT-177).
            // An operator's "clear what's settled" is one gesture, and before
            // this a failed instance had no `rk` path off `rk inbox` at all.
            let selection = crate::workflow_exec::Selection::Before(cutoff);
            let instances = if params.dry_run {
                engine.archivable(&selection)?
            } else {
                engine.archive(&selection)?
            };
            value["instances"] = json!(instances);
            if params.reap_git {
                let reclaims = landing.gate_worktree_sweep_once(&gate_worktree_cfg, params.dry_run);
                value["gate_worktrees"] = json!(reclaims);
            }
            Ok::<_, rk_core::Error>(value)
        })
        .await;
        match result {
            Ok(Ok(value)) => Response::ok(req.id, value),
            Ok(Err(e)) => Response::err(req.id, codes::INTERNAL, e.to_string()),
            Err(e) => Response::err(req.id, codes::INTERNAL, format!("archive task failed: {e}")),
        }
    }

    /// `workflow.archive` — the targeted counterpart to the `agent.archive`
    /// sweep: prune named terminal instances (the `rk inbox` row action) or
    /// every one settled before a cutoff. The daemon owns the instance store
    /// and rewrites it on every mutation, so this has to be an RPC — moving the
    /// JSON aside by hand only works with the daemon stopped.
    async fn handle_workflow_archive(&self, req: Request) -> Response {
        let params: WorkflowArchiveParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let selection = if params.ids.is_empty() {
            let now = chrono::Utc::now();
            // `all` means "everything settled right now" — a cutoff of now,
            // since eligibility is `settled_at < cutoff`.
            let cutoff = if params.all {
                now
            } else {
                match crate::agents::cutoff_from_spec(params.before.as_deref().unwrap_or("7d"), now)
                {
                    Ok(c) => c,
                    Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e.to_string()),
                }
            };
            crate::workflow_exec::Selection::Before(cutoff)
        } else {
            crate::workflow_exec::Selection::Ids(params.ids)
        };
        let engine = self.engine();
        let result = tokio::task::spawn_blocking(move || {
            if params.dry_run {
                engine.archivable(&selection)
            } else {
                engine.archive(&selection)
            }
        })
        .await;
        match result {
            Ok(Ok(instances)) => Response::ok(
                req.id,
                json!({
                    "dry_run": params.dry_run,
                    "count": instances.len(),
                    "instances": instances,
                }),
            ),
            Ok(Err(e)) => Response::err(req.id, codes::INTERNAL, e.to_string()),
            Err(e) => Response::err(
                req.id,
                codes::INTERNAL,
                format!("workflow archive task failed: {e}"),
            ),
        }
    }

    fn authorize_foreman_child(&self, caller: &str, child: &str) -> rk_core::Result<()> {
        if caller == "operator" || caller.is_empty() {
            return Ok(());
        }
        self.supervisor.authorize_child(caller, child)
    }

    fn handle_named<F>(&self, req: Request, f: F) -> Response
    where
        F: FnOnce(&Arc<crate::supervisor::Supervisor>, String) -> rk_core::Result<Value>,
    {
        let params: NameParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        match f(&self.supervisor, params.name) {
            Ok(v) => Response::ok(req.id, v),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    async fn handle_steer(&self, req: Request) -> Response {
        let params: SteerParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        if let Err(e) = self.authorize_foreman_child(&req.caller, &params.name) {
            return Response::err(req.id, codes::FORBIDDEN, e.to_string());
        }
        let Some(record) = self.supervisor.status(&params.name) else {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!("no such agent: {}", params.name),
            );
        };
        let sender = if req.caller.is_empty() {
            OPERATOR_ACTOR
        } else {
            req.caller.as_str()
        };
        let Some(session_generation) = self.supervisor.session_generation(&record.name) else {
            return Response::err(
                req.id,
                codes::INTERNAL,
                format!("{} has no live session generation", record.name),
            );
        };
        let session_generation = session_generation.to_string();
        let envelope = ControlEnvelope::new(
            RecordId::new().to_string(),
            sender,
            &record.name,
            session_generation.clone(),
            session_generation.clone(),
            params.message,
        );
        if let Err(e) =
            crate::steer::enqueue(&self.space, &record.repo_name, &envelope, &self.castle)
        {
            return Response::err(req.id, codes::INTERNAL, e.to_string());
        }
        match self
            .supervisor
            .steer_envelope(&params.name, &envelope)
            .await
        {
            Ok(()) => Response::ok(
                req.id,
                json!({
                    "steered": true,
                    "message_id": envelope.message_id,
                    "delivery_generation": envelope.delivery_generation,
                    "resume_generation": envelope.resume_generation,
                }),
            ),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_out(&self, req: Request) -> Response {
        let params: OutParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let caller = req.caller.clone();
        let is_agent = caller != "operator" && !caller.is_empty();
        if is_agent && params.category == Category::Artifact && params.identity == "review" {
            if let Some(review) = self
                .supervisor
                .status(&caller)
                .and_then(|record| record.review)
            {
                if let Err(error) = validate_review_artifact(&params.payload, &review) {
                    return Response::err(req.id, codes::BAD_PARAMS, error);
                }
            }
        }
        let attention = if is_agent
            && matches!(params.category, Category::Obstacle | Category::Need)
            && self.supervisor.is_reporting_boundary(&caller)
        {
            Some((
                params.category.as_str().to_string(),
                params.scope.clone(),
                params.identity.clone(),
                params.payload.clone(),
            ))
        } else {
            None
        };
        if is_agent {
            if params.lifecycle == Some(Lifecycle::Furniture)
                || matches!(
                    params.category,
                    Category::Convention
                        | Category::Task
                        | Category::Available
                        // `Withdrawal` is on this list for a different reason
                        // than the rest: it is not that agents have no business
                        // closing a ballot — the proposer is exactly who should
                        // — but that a raw `out` carries no proof of WHOSE
                        // ballot it is. `handle_out` only checks that a tuple's
                        // `instance` is the caller, which a withdrawal keyed
                        // `identity = <sug-id>` satisfies trivially, so leaving
                        // it writable here would let any rat close any peer's
                        // proposal. `space.withdraw` is the only route, and it
                        // checks authorship against the Suggestion (TKT-184).
                        | Category::Withdrawal
                        | Category::FactVote
                )
            {
                return Response::err(
                    req.id,
                    codes::FORBIDDEN,
                    "agents cannot write furniture, convention, task, or available tuples \
                     (withdraw a ballot with `rk withdraw`, which checks authorship)",
                );
            }
            if params
                .instance
                .as_deref()
                .is_some_and(|instance| instance != caller)
            {
                return Response::err(
                    req.id,
                    codes::FORBIDDEN,
                    "agents may only write tuples for their own instance",
                );
            }
            if params.category == Category::Event {
                if params.identity != "task_done" {
                    return Response::err(
                        req.id,
                        codes::FORBIDDEN,
                        "agents may only write the task_done event",
                    );
                }
                if params.payload.get("agent").and_then(Value::as_str) != Some(caller.as_str()) {
                    return Response::err(
                        req.id,
                        codes::FORBIDDEN,
                        "task_done must identify the authenticated agent",
                    );
                }
            }
        }
        let mut tuple = Tuple::new(
            params.category,
            params.scope,
            params.identity,
            params.instance.unwrap_or_else(|| {
                if is_agent {
                    caller.clone()
                } else {
                    self.castle.clone()
                }
            }),
            params.payload,
        );
        let explicit_lifecycle = params.lifecycle.is_some() || params.ttl_secs.is_some();
        if let Some(lifecycle) = params.lifecycle {
            tuple = tuple.with_lifecycle(lifecycle);
        }
        if let Some(ttl_secs) = params.ttl_secs {
            if ttl_secs > MAX_TRAIL_TTL.as_secs() {
                return Response::err(
                    req.id,
                    codes::BAD_PARAMS,
                    format!(
                        "ttl_secs exceeds the maximum supported TTL of {} seconds",
                        MAX_TRAIL_TTL.as_secs()
                    ),
                );
            }
            tuple.lifecycle = Lifecycle::Ephemeral;
            tuple.expires_at = chrono::Utc::now().checked_add_signed(
                chrono::Duration::from_std(Duration::from_secs(ttl_secs))
                    .expect("MAX_TRAIL_TTL must fit chrono::Duration"),
            );
        }
        // Pheromone trails carry a decaying strength and default to an Ephemeral
        // lifetime so an abandoned one evaporates instead of lingering forever.
        if tuple.category.evaporates() {
            tuple.strength = Some(rk_core::tuple::FULL_STRENGTH);
            if !explicit_lifecycle {
                tuple.lifecycle = Lifecycle::Ephemeral;
                tuple.expires_at = chrono::Utc::now().checked_add_signed(
                    chrono::Duration::from_std(DEFAULT_TRAIL_TTL)
                        .expect("DEFAULT_TRAIL_TTL must fit chrono::Duration"),
                );
            }
        }
        // Evaporating writes reinforce an existing trail in place (refresh, no
        // duplicate); everything else is a plain append.
        let written = if tuple.category.evaporates() {
            self.space.reinforce(tuple)
        } else {
            self.space.out(tuple.clone()).map(|()| tuple)
        };
        match written {
            Ok(t) => {
                if let Some((category, scope, identity, payload)) = attention {
                    self.supervisor.publish_coordination_attention(
                        &caller, &category, &scope, &identity, &payload,
                    );
                }
                Response::ok(req.id, json!({"id": t.id, "written": true}))
            }
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    /// `space.withdraw` — close a losing ballot explicitly (TKT-184).
    ///
    /// This is its own RPC rather than a `space.out` of a `Withdrawal` because
    /// the act needs an authorisation that `handle_out` structurally cannot
    /// perform. `handle_out` authenticates a WRITER (a tuple's `instance` must
    /// be the caller); withdrawal has to authorise against a SUBJECT — the
    /// proposer recorded on a different tuple — and only a handler that reads
    /// the `Suggestion` first can do that. See [`may_withdraw`].
    ///
    /// Ordered so the cheap terminal answers come before the authorisation
    /// check, because they are answers the caller wants regardless of who they
    /// are: a promoted ballot cannot be withdrawn by anyone (its Convention is
    /// permanent and unretractable, so "withdrawn" would be a lie the space
    /// cannot honour), and an already-withdrawn one is a no-op that must report
    /// success — the resolving command on an inbox row has to be safe to run
    /// twice, and the operator re-running it after a `rk sync` pulled a peer's
    /// withdrawal should not read as a failure.
    fn handle_withdraw(&self, req: Request) -> Response {
        let params: WithdrawParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let sug_id = params.suggestion.trim().to_string();
        if sug_id.is_empty() {
            return Response::err(req.id, codes::BAD_PARAMS, "suggestion id must not be empty");
        }
        let ballot = |category| {
            self.space.scan(
                &Pattern::category(category)
                    .scope(SYSTEM_SCOPE)
                    .identity(sug_id.as_str()),
            )
        };

        let suggestions = match ballot(Category::Suggestion) {
            Ok(t) => t,
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        // The proposer is read off the Suggestion, never taken from the caller:
        // that tuple is the only durable record of whose ballot this is.
        let Some(suggestion) = suggestions.first() else {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!("no open suggestion {sug_id} on the system scope"),
            );
        };

        match ballot(Category::Convention) {
            Ok(c) if !c.is_empty() => {
                return Response::err(
                    req.id,
                    codes::FORBIDDEN,
                    format!(
                        "{sug_id} already promoted to a convention — a promoted norm is \
                         permanent and cannot be withdrawn"
                    ),
                );
            }
            Ok(_) => {}
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        }

        match ballot(Category::Withdrawal) {
            Ok(w) if !w.is_empty() => {
                return Response::ok(
                    req.id,
                    json!({"withdrawn": sug_id, "already": true, "written": false}),
                );
            }
            Ok(_) => {}
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        }

        let caller = req.caller.clone();
        if !may_withdraw(&caller, &suggestion.instance) {
            return Response::err(
                req.id,
                codes::FORBIDDEN,
                format!(
                    "only {proposer} (who proposed {sug_id}) or the operator may withdraw it",
                    proposer = suggestion.instance
                ),
            );
        }
        // An operator's caller id is `operator` or empty; record the former
        // either way so the ledger names an actor rather than a blank, and so
        // the operator reads as ONE withdrawer however many shells they use —
        // the same reason `rk endorse` votes as `operator`.
        let by = if caller.is_empty() {
            OPERATOR_ACTOR.to_string()
        } else {
            caller
        };
        let withdrawal = Tuple::new(
            Category::Withdrawal,
            SYSTEM_SCOPE,
            sug_id.as_str(),
            by.as_str(),
            json!({
                "suggestion": sug_id,
                "withdrawn_by": by,
                "proposer": suggestion.instance,
                "text": suggestion.payload.get("text").cloned().unwrap_or(Value::Null),
            }),
        )
        // Furniture for the same two reasons the Convention is: a closed ballot
        // must stay closed (an evaporating withdrawal would silently reopen the
        // row it retired, and re-arm the promotion it suppressed), and `in` must
        // not be able to consume it — otherwise `rk in withdrawal system <id>`
        // is an unauthorised reopen with no authorship check anywhere.
        // Furniture also replicates, so a ballot withdrawn in one castle does
        // not keep nagging — or promote — in another.
        .with_lifecycle(Lifecycle::Furniture);
        match self.space.out(withdrawal) {
            Ok(()) => Response::ok(
                req.id,
                json!({"withdrawn": sug_id, "already": false, "written": true, "by": by}),
            ),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    /// Cast, change, or retract the authenticated caller's vote on one Fact.
    /// Votes are append-only ledger tuples: the latest entry for
    /// `(fact, voter)` is effective, while the history preserves provenance.
    fn handle_fact_vote(&self, req: Request) -> Response {
        let params: FactVoteParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let fact_id = match params.fact.trim().parse::<RecordId>() {
            Ok(id) => id,
            Err(e) => {
                return Response::err(
                    req.id,
                    codes::BAD_PARAMS,
                    format!("fact must be a tuple id: {e}"),
                );
            }
        };
        let fact = match self.space.get(fact_id) {
            Ok(Some(tuple)) if tuple.category == Category::Fact => tuple,
            Ok(Some(_)) => {
                return Response::err(req.id, codes::BAD_PARAMS, "target tuple is not a fact");
            }
            Ok(None) => return Response::err(req.id, codes::BAD_PARAMS, "fact tuple not found"),
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        let vote = params.vote.trim().to_ascii_lowercase();
        if !matches!(vote.as_str(), "up" | "down" | "clear") {
            return Response::err(req.id, codes::BAD_PARAMS, "vote must be up, down, or clear");
        }
        let voter = if req.caller.is_empty() {
            OPERATOR_ACTOR.to_string()
        } else {
            req.caller.clone()
        };
        let _guard = match self.fact_vote_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let fact_key = fact_id.to_string();
        let mut vote_pattern = Pattern::category(Category::FactVote)
            .scope(fact.scope.clone())
            .identity(fact_key.clone());
        vote_pattern.instance = Some(voter.clone());
        let existing = match self.space.scan(&vote_pattern) {
            Ok(tuples) => tuples,
            Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
        };
        let current = existing
            .iter()
            .max_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.id.cmp(&right.id))
            })
            .and_then(|tuple| tuple.payload.get("vote"))
            .and_then(Value::as_str);
        let already = match vote.as_str() {
            "clear" => current.is_none() || current == Some("clear"),
            _ => current == Some(vote.as_str()),
        };
        if already {
            return Response::ok(
                req.id,
                json!({
                    "fact": fact_id,
                    "vote": vote,
                    "voter": voter,
                    "already": true,
                    "written": false,
                }),
            );
        }
        let tuple = Tuple::new(
            Category::FactVote,
            fact.scope,
            fact_key.clone(),
            voter.clone(),
            json!({"fact": fact_key, "vote": vote, "voter": voter}),
        );
        match self.space.out(tuple) {
            Ok(()) => Response::ok(
                req.id,
                json!({
                    "fact": fact_id,
                    "vote": vote,
                    "voter": voter,
                    "already": false,
                    "written": true,
                }),
            ),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_scan(&self, req: Request) -> Response {
        let params: ScanParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        // `--hot`, or any `--top N` cap, follows the strongest trail first;
        // otherwise the default oldest-first scan is unchanged.
        let requested_top = params.top;
        let limit = requested_top
            .unwrap_or(MAX_SCAN_TUPLES)
            .min(MAX_SCAN_TUPLES);
        let result = if params.hot || params.top.is_some() {
            self.space
                .scan_hot(&params.pattern, Some(limit.saturating_add(1)))
        } else {
            self.space
                .scan_limited(&params.pattern, limit.saturating_add(1))
        };
        match result {
            Ok(mut tuples) => {
                let truncated = tuples.len() > limit
                    || requested_top.is_some_and(|requested| requested > MAX_SCAN_TUPLES);
                tuples.truncate(limit);
                let fact_votes = if params.pattern.category == Some(Category::Fact) {
                    match self.fact_vote_counts(&tuples) {
                        Ok(counts) => Some(counts),
                        Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
                    }
                } else {
                    None
                };
                let mut result = json!({"tuples": tuples, "truncated": truncated});
                if let Some(counts) = fact_votes {
                    result["fact_votes"] = counts;
                }
                Response::ok(req.id, result)
            }
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    fn handle_ingest_event(&self, req: Request) -> Response {
        let Some(principal) = self.ingest_principal(&req) else {
            return Response::err(
                req.id,
                codes::FORBIDDEN,
                "unknown or disabled ingest source",
            );
        };
        let params: IngestEventParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        match crate::ingest_auth::validate_event(&principal, &params) {
            Ok(()) => {}
            Err(error) => return Response::err(req.id, codes::FORBIDDEN, error),
        }
        let source = match principal.configured_source() {
            Ok(source) => source,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error.to_string()),
        };
        let receipt = match self
            .space
            .accept_sdlc_signal(params.envelope, SignalSourcePrincipal::for_source(&source))
        {
            Ok(receipt) => receipt,
            Err(error) => return Response::err(req.id, codes::INTERNAL, error.to_string()),
        };
        Response::ok(req.id, json!({"accepted": true, "receipt": receipt}))
    }

    fn handle_ingest_state(&self, req: Request) -> Response {
        let Some(principal) = self.ingest_principal(&req) else {
            return Response::err(
                req.id,
                codes::FORBIDDEN,
                "unknown or disabled ingest source",
            );
        };
        let params: IngestStateParams = match parse_params(&req.params) {
            Ok(params) => params,
            Err(error) => return Response::err(req.id, codes::BAD_PARAMS, error),
        };
        let requested = params.limit.unwrap_or(principal.max_state_limit());
        if requested > principal.max_state_limit() {
            return Response::err(
                req.id,
                codes::BAD_PARAMS,
                format!("limit exceeds source cap {}", principal.max_state_limit()),
            );
        }
        let source = Some(principal.name.as_str());
        let (scope, subject) = ingest_state_filter(&params);
        match self
            .space
            .current_sdlc_facts(source, scope.as_deref(), subject.as_deref())
        {
            Ok(mut facts) => {
                if let Some(repo) = params.repo.as_deref() {
                    let prefix = format!("{repo}:");
                    facts.retain(|fact| {
                        fact.payload
                            .get("subject")
                            .and_then(Value::as_str)
                            .is_some_and(|s| s.starts_with(&prefix))
                    });
                }
                facts.truncate(requested);
                Response::ok(req.id, json!({"facts": facts, "truncated": false}))
            }
            Err(error) => Response::err(req.id, codes::INTERNAL, error.to_string()),
        }
    }

    async fn handle_blocking(&self, req: Request, destructive: bool) -> Response {
        let params: BlockingParams = match parse_params(&req.params) {
            Ok(p) => p,
            Err(e) => return Response::err(req.id, codes::BAD_PARAMS, e),
        };
        let timeout = params
            .timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(DEFAULT_BLOCK)
            .min(MAX_BLOCK);
        let result = if destructive {
            self.space.take(&params.pattern.pattern, timeout).await
        } else {
            self.space.rd(&params.pattern.pattern, timeout).await
        };
        match result {
            Ok(Some(tuple)) => {
                let fact_votes = if tuple.category == Category::Fact {
                    match self.fact_vote_counts(std::slice::from_ref(&tuple)) {
                        Ok(counts) => Some(counts),
                        Err(e) => return Response::err(req.id, codes::INTERNAL, e.to_string()),
                    }
                } else {
                    None
                };
                let mut result = json!({"tuple": tuple});
                if let Some(counts) = fact_votes {
                    result["fact_votes"] = counts;
                }
                Response::ok(req.id, result)
            }
            Ok(None) => Response::ok(req.id, json!({"tuple": null, "timed_out": true})),
            Err(e) => Response::err(req.id, codes::INTERNAL, e.to_string()),
        }
    }

    /// Reduce the append-only vote ledger to the latest effective vote from
    /// each voter, then return counts keyed by the fact id being read.
    fn fact_vote_counts(&self, facts: &[Tuple]) -> rk_core::Result<Value> {
        let targets: HashSet<(String, String)> = facts
            .iter()
            .map(|fact| (fact.scope.clone(), fact.id.to_string()))
            .collect();
        let votes = self.space.scan(&Pattern::category(Category::FactVote))?;
        let mut latest: HashMap<FactVoteKey, FactVoteState> = HashMap::new();
        for vote in votes {
            let key = (vote.scope, vote.identity, vote.instance);
            let state = vote
                .payload
                .get("vote")
                .and_then(Value::as_str)
                .unwrap_or("clear")
                .to_string();
            if latest.get(&key).is_none_or(|(current_at, current_id, _)| {
                vote.created_at > *current_at
                    || (vote.created_at == *current_at && vote.id > *current_id)
            }) {
                latest.insert(key, (vote.created_at, vote.id, state));
            }
        }
        let mut counts: HashMap<(String, String), (u64, u64)> = HashMap::new();
        for ((scope, fact_id, _voter), (_created_at, _id, state)) in latest {
            if !targets.contains(&(scope.clone(), fact_id.clone())) {
                continue;
            }
            let entry = counts.entry((scope, fact_id)).or_default();
            match state.as_str() {
                "up" => entry.0 += 1,
                "down" => entry.1 += 1,
                _ => {}
            }
        }
        let mut result = serde_json::Map::new();
        for fact in facts {
            let key = (fact.scope.clone(), fact.id.to_string());
            let (up, down) = counts.get(&key).copied().unwrap_or_default();
            result.insert(
                fact.id.to_string(),
                json!({"up": up, "down": down, "score": up as i64 - down as i64}),
            );
        }
        Ok(Value::Object(result))
    }

    fn status(&self) -> Value {
        json!({
            "version": env!("CARGO_PKG_VERSION"),
            // The version that can actually distinguish two daemons: `version`
            // above has read `0.1.0` since the first commit, so an operator
            // comparing it against a freshly installed binary learns nothing.
            "build_version": rk_core::version::BUILD_VERSION,
            "pid": std::process::id(),
            // Operator-facing: the friendly alias if configured, else the actor
            // id. The wire id (self.castle) is never exposed here as a name.
            "castle": self.castle_display,
            "uptime_secs": self.started.elapsed().as_secs(),
            "socket": self.layout.socket_path(),
            "tuples": self.space.count().unwrap_or(0),
            // Landing-queue depth and oldest-entry age per (repo, target) —
            // without this a slowly-draining queue and a wedged one are
            // indistinguishable from the outside (probe O18).
            "landing_queue": crate::landing::landing_queue_summary(&self.space),
        })
    }
}

fn cleared_branches_for_paths(
    events: Vec<(String, String, String, bool)>,
    paths: HashMap<String, std::path::PathBuf>,
) -> HashSet<(String, String)> {
    let mut cleared = HashSet::new();
    // Ask git once per distinct (scope, branch): the same branch commonly
    // carries several events (a retried land, a re-push), and the answer cannot
    // differ between them.
    let mut asked: HashSet<(String, String)> = HashSet::new();
    for (scope, branch, target, content_proven) in events {
        if !content_proven {
            continue;
        }
        let key = (scope.clone(), branch.clone());
        if !asked.insert(key.clone()) {
            continue;
        }
        let Some(path) = paths.get(&scope) else {
            continue;
        };
        let Ok(repo) = rk_git::Repo::discover(path) else {
            continue;
        };
        if repo.branch_merged_or_gone(&branch, &target) {
            cleared.insert(key);
        }
    }
    cleared
}

#[cfg(test)]
mod branch_clear_tests {
    use super::*;
    use std::process::Command;

    fn git(repo: &std::path::Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    #[test]
    fn review_clear_requires_durable_proof_that_the_branch_carried_work() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        git(repo, &["init", "-b", "main"]);
        git(repo, &["config", "user.email", "r@x"]);
        git(repo, &["config", "user.name", "R"]);
        std::fs::write(repo.join("f"), "base\n").unwrap();
        git(repo, &["add", "."]);
        git(repo, &["commit", "-m", "base"]);
        git(repo, &["branch", "empty", "main"]);

        let paths = HashMap::from([("repo".to_string(), repo.to_path_buf())]);
        let cleared = cleared_branches_for_paths(
            vec![("repo".into(), "empty".into(), "main".into(), false)],
            paths,
        );
        assert!(
            cleared.is_empty(),
            "an empty branch equals main but must stay surfaced without content proof"
        );

        git(repo, &["checkout", "-b", "work", "main"]);
        std::fs::write(repo.join("g"), "work\n").unwrap();
        git(repo, &["add", "."]);
        git(repo, &["commit", "-m", "work"]);
        git(repo, &["checkout", "main"]);
        git(repo, &["merge", "--ff-only", "work"]);
        let paths = HashMap::from([("repo".to_string(), repo.to_path_buf())]);
        let cleared = cleared_branches_for_paths(
            vec![("repo".into(), "work".into(), "main".into(), true)],
            paths,
        );
        assert!(cleared.contains(&("repo".into(), "work".into())));
    }
}

fn max_cursor(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, None) => left,
        (None, right) => right,
    }
}

fn factory_matches_repo(filter: &FactoryEventFilter, identity: &str, path: &str) -> bool {
    filter
        .repo
        .as_deref()
        .is_none_or(|repo| repo == identity || repo == path)
}

fn factory_matches_agent(filter: &FactoryEventFilter, agent: &crate::agents::AgentRecord) -> bool {
    factory_matches_repo(filter, &agent.repo_name, &agent.repo_root.to_string_lossy())
        && filter
            .coordinator
            .as_deref()
            .is_none_or(|coordinator| agent.coordinator.as_deref() == Some(coordinator))
}

fn factory_matches_workflow(
    filter: &FactoryEventFilter,
    workflow: &crate::workflow_exec::Instance,
) -> bool {
    factory_matches_repo(filter, &workflow.repo, &workflow.repo)
        && filter
            .coordinator
            .as_deref()
            .is_none_or(|coordinator| workflow.coordinator.as_deref() == Some(coordinator))
}

fn factory_matches_proposal(filter: &FactoryEventFilter, proposal: &ActionProposal) -> bool {
    factory_matches_repo(
        filter,
        &proposal.scope.repo.identity,
        &proposal.scope.repo.path,
    ) && filter.coordinator.as_deref().is_none_or(|coordinator| {
        matches!(
            &proposal.action,
            FactoryAction::WorkflowRun(action) if action.coordinator.as_deref() == Some(coordinator)
        )
    })
}

fn factory_action_coordinator(action: &FactoryAction) -> Option<&str> {
    match action {
        FactoryAction::WorkflowRun(action) => action.coordinator.as_deref(),
        FactoryAction::TicketGraphApply(_) | FactoryAction::ProductToCodeDispatch(_) => None,
    }
}

fn factory_matches_grant(
    filter: &FactoryEventFilter,
    grant: &ApprovalGrant,
    proposals: &[ActionProposal],
) -> bool {
    factory_matches_repo(filter, &grant.scope.repo.identity, &grant.scope.repo.path)
        && filter.coordinator.as_deref().is_none_or(|coordinator| {
            proposals
                .iter()
                .find(|proposal| proposal.id == grant.proposal_id)
                .is_some_and(|proposal| factory_matches_proposal(filter, proposal))
                || grant.requester == coordinator
        })
}

fn factory_filtered_budget(
    mut budget: Value,
    filter: &FactoryEventFilter,
    workflows: &[crate::workflow_exec::Instance],
) -> Value {
    if let Some(repo) = filter.repo.as_deref() {
        if let Some(repos) = budget.get_mut("repos").and_then(Value::as_array_mut) {
            repos.retain(|entry| entry.get("repo").and_then(Value::as_str) == Some(repo));
        }
    }
    if filter.repo.is_some() || filter.coordinator.is_some() {
        let allowed: HashSet<_> = workflows
            .iter()
            .map(|workflow| workflow.id.as_str())
            .collect();
        if let Some(instances) = budget.get_mut("instances").and_then(Value::as_array_mut) {
            instances.retain(|entry| {
                entry
                    .get("instance")
                    .and_then(Value::as_str)
                    .is_some_and(|instance| allowed.contains(instance))
            });
        }
    }
    budget
}

fn filter_is_valid(filter: &CoordinatorFilter) -> bool {
    filter.instance.is_some()
        || filter.coordinator.is_some()
        || filter.subtree.is_some()
        || filter.repo.is_some()
}

enum Outcome {
    Reply(Response),
    Watch {
        response: Response,
        pattern: Pattern,
    },
    CoordinatorWatch {
        response: Response,
        filter: CoordinatorFilter,
        boundary: Option<u64>,
        rx: broadcast::Receiver<CoordinatorEvent>,
    },
    FactoryEventsWatch {
        response: Response,
        filter: FactoryEventFilter,
        boundary: Option<u64>,
        rx: broadcast::Receiver<CoordinatorEvent>,
    },
    /// Reply with the backlog, then stream that generation's new log entries
    /// live.
    LogFollow {
        response: Response,
        /// The generation being followed. Only the newest generation of a name
        /// can still be writing, so following an older one correctly streams
        /// nothing rather than leaking a namesake's live output.
        spawn: Option<rk_core::id::SpawnId>,
    },
}

#[derive(Deserialize)]
struct LogParams {
    /// An agent name, or (E4) an exact `SpawnId` — `rk log` sends whichever the
    /// operator typed and lets the daemon tell them apart.
    name: String,
    /// Only the last N entries of the backlog (all if unset).
    #[serde(default)]
    tail: Option<usize>,
    /// Keep the connection open and push new entries as they land.
    #[serde(default)]
    follow: bool,
    /// Which generation of this name to read, 1-based oldest-first. Unset = the
    /// newest, i.e. the rat an operator means when they type a bare name.
    #[serde(default)]
    generation: Option<usize>,
}

#[derive(Deserialize)]
struct OutParams {
    category: Category,
    scope: String,
    identity: String,
    #[serde(default)]
    instance: Option<String>,
    #[serde(default)]
    payload: Value,
    #[serde(default)]
    lifecycle: Option<Lifecycle>,
    #[serde(default)]
    ttl_secs: Option<u64>,
}

fn validate_review_artifact(
    payload: &Value,
    review: &rk_core::review::ReviewContext,
) -> Result<(), String> {
    for (field, expected) in [
        ("branch", review.branch.as_str()),
        ("head_sha", review.head_sha.as_str()),
        ("target", review.target.as_str()),
        ("task", review.task.as_str()),
        ("review_attempt", review.attempt.as_str()),
    ] {
        let Some(actual) = payload.get(field) else {
            return Err(format!(
                "review artifact binding mismatch for {field}: expected '{expected}', got <missing>"
            ));
        };
        if actual.as_str() != Some(expected) {
            return Err(format!(
                "review artifact binding mismatch for {field}: expected '{expected}', got {actual}"
            ));
        }
    }
    Ok(())
}

/// `space.withdraw` params: the ballot to close. Everything else the record
/// needs — proposer, text, withdrawer — is read from the space and the
/// authenticated caller, never accepted from the wire, so a caller cannot
/// misattribute the close.
#[derive(Deserialize)]
struct WithdrawParams {
    suggestion: String,
}

#[derive(Deserialize)]
struct FactVoteParams {
    fact: String,
    vote: String,
}

#[derive(Deserialize, Default)]
struct PatternParams {
    #[serde(flatten)]
    pattern: Pattern,
}

/// `space.scan` params: a match pattern plus the optional hot-ranking sugar.
/// `hot` reorders by `category_weight × recency × strength` (strongest first);
/// `top` caps to the N strongest and implies `hot`.
#[derive(Deserialize, Default)]
struct ScanParams {
    #[serde(flatten)]
    pattern: Pattern,
    #[serde(default)]
    hot: bool,
    #[serde(default)]
    top: Option<usize>,
}

#[derive(Deserialize)]
struct FactoryProposeParams {
    kind: String,
    action: Value,
    #[serde(default)]
    ttl_seconds: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TicketGraphApplyParams {
    repo: String,
    graph: TicketGraph,
    initiative: InitiativeContract,
}

/// Submitted (pre-canonical) `product_to_code.dispatch` payload. Dispatch and
/// blocked entries reference stable graph node ids; the daemon resolves minted
/// TKT ids from the referenced approved graph apply execution.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProductToCodeDispatchParams {
    repo: String,
    initiative: InitiativeContract,
    graph: TicketGraph,
    graph_id: String,
    graph_apply_proposal_id: String,
    dispatches: Vec<ProductToCodeDispatchRequest>,
    #[serde(default)]
    blocked: Vec<ProductToCodeBlockedNode>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProductToCodeDispatchRequest {
    graph_node_id: String,
    #[serde(default)]
    task_description: String,
}

/// Deterministic per-ticket workflow instance id so concurrent execute retries
/// of one approved dispatch stay single-flight per ticket.
fn product_to_code_instance_id(execution_id: &str, ticket_id: &str) -> String {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"rk.product_to_code.dispatch.instance.v1\0");
    hasher.update(execution_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(ticket_id.as_bytes());
    let hex = hex::encode(hasher.finalize());
    format!("wf-{}", &hex[..32])
}

#[derive(Deserialize)]
struct FactoryApproveParams {
    proposal_id: String,
    digest: String,
}

#[derive(Deserialize)]
struct FactoryExecuteParams {
    proposal_id: String,
    digest: String,
    #[serde(default)]
    kind: Option<String>,
    action: Value,
}

#[derive(Deserialize)]
struct WorkflowRunParams {
    name: String,
    repo: String,
    #[serde(default)]
    params: std::collections::HashMap<String, Value>,
    /// Stable coordinator-session identity for owned-workflow monitoring.
    #[serde(default)]
    coordinator: Option<String>,
}

#[derive(Deserialize)]
struct VerifyRunParams {
    repo: String,
    /// Named check to run (`<repo>/.rk/checks.cue`). Defaults to `"verify"`,
    /// the conventional name for the full workspace suite.
    #[serde(default)]
    check: Option<String>,
}

#[derive(Deserialize)]
struct CoordinatorRegisterParams {
    coordinator: String,
    #[serde(default)]
    after: Option<u64>,
}

#[derive(Deserialize)]
struct CoordinatorAckParams {
    coordinator: String,
    cursor: u64,
}

#[derive(Deserialize)]
struct ProgressParams {
    summary: String,
    #[serde(default)]
    next: Option<String>,
    #[serde(default = "default_progress_status")]
    status: String,
}

fn default_progress_status() -> String {
    "working".into()
}

#[derive(Deserialize)]
struct WorkflowDefsParams {
    repo: String,
}

#[derive(Deserialize)]
struct WorkflowApproveParams {
    instance: String,
    approved: bool,
    by: String,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
struct NameParams {
    name: String,
}

#[derive(Default, Deserialize)]
struct InboxParams {
    #[serde(default)]
    repo: Option<String>,
}

#[derive(Deserialize)]
struct ReconcileParams {
    repo: String,
}

#[derive(Deserialize)]
struct ReconcileRepairParams {
    repo: String,
    /// `false` (the default) previews the plan with zero mutation; `true`
    /// executes every `Planned` item through the CAS repair writers.
    #[serde(default)]
    apply: bool,
}

#[derive(Deserialize)]
struct InboxAckParams {
    id: String,
}

#[derive(Deserialize)]
struct LeaseAcquireParams {
    repo: String,
    /// Stable identity of the orchestrator session acquiring the lease. The
    /// same string presented again is how a reconnect or a daemon restart
    /// resumes this session's generation and cursor.
    holder: String,
    ttl_secs: Option<i64>,
}

#[derive(Deserialize)]
struct LeaseRenewParams {
    repo: String,
    holder: String,
    generation: u64,
    ttl_secs: Option<i64>,
}

#[derive(Deserialize)]
struct AttentionNextParams {
    repo: String,
}

#[derive(Deserialize)]
struct AttentionDecideParams {
    repo: String,
    /// The attention item's `Violation::id`, as returned by `attention.next`.
    item: String,
    /// Required only when the item's effective authority is `Orchestrator`.
    holder: Option<String>,
    generation: Option<u64>,
    budget_usd: Option<f64>,
    budget_tokens: Option<u64>,
}

/// `attention.decide`'s three failure shapes, mapped to distinct wire error
/// codes by `handle_attention_decide`: a policy/authority refusal (403-shaped,
/// e.g. human-gated or lease fencing) is a materially different failure than
/// a malformed request or an internal error, and a caller (especially an
/// automated orchestrator loop) needs to tell them apart.
enum AttentionDecideError {
    Refused(String),
    BadParams(String),
    Internal(String),
}

/// `agent.list` view selector. Defaults keep the reply to the live registry so
/// every caller (`rk list`, `rk top`, `rk cost`) gets the current fleet unless
/// it opts into history.
#[derive(Deserialize, Default)]
#[serde(default)]
struct AgentListParams {
    /// Live + archived records.
    include_archived: bool,
    /// Archived records only (wins over `include_archived`).
    archived_only: bool,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct AgentArchiveParams {
    /// Cutoff: a duration (`7d`, `24h`) or a date. Defaults to `7d`.
    before: Option<String>,
    /// Archive every eligible record regardless of age (overrides `before`).
    all: bool,
    /// Report what would be archived without mutating the registry.
    dry_run: bool,
    /// Also reclaim each archived agent's worktree + local branch, but only
    /// when the branch has already landed.
    reap_git: bool,
    /// Also delete each archived agent's transcript file. One-way.
    reap_logs: bool,
    /// Also delete each archived agent's regenerable build-artifact paths
    /// (`[worktree_sweep] artifact_paths`/`artifact_paths_by_repo`, e.g.
    /// `target`) from its worktree — regardless of merge state, unlike
    /// `reap_git`.
    reap_artifacts: bool,
}

/// Which slice of the instance store `workflow.list` returns. Defaults (both
/// false) to the live store, so every existing caller is unchanged.
#[derive(Deserialize, Default)]
#[serde(default)]
struct WorkflowListParams {
    /// Pruned instances only.
    archived: bool,
    /// Live + pruned — the full run history. Ignored when `archived` is set.
    all: bool,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct WorkflowArchiveParams {
    /// Prune exactly these instance ids. Non-empty ids override the window:
    /// an unknown or still-running id is an error, never a silent no-op.
    ids: Vec<String>,
    /// Cutoff for the windowed form: a duration (`7d`, `24h`) or a date.
    /// Defaults to `7d`.
    before: Option<String>,
    /// Prune every settled instance regardless of age (overrides `before`).
    all: bool,
    /// Report what would be pruned without touching the store.
    dry_run: bool,
}

#[derive(Deserialize)]
struct SteerParams {
    name: String,
    message: String,
}

#[derive(Deserialize)]
struct DismissParams {
    name: String,
    #[serde(default)]
    no_merge: bool,
}

#[derive(Deserialize)]
struct RepoLandParams {
    repo: String,
    branch: String,
    #[serde(default = "default_main_branch")]
    target: String,
    #[serde(default)]
    keep_branch: bool,
    #[serde(default)]
    force: bool,
    reason: Option<String>,
    /// Explicit, operator-validated task identity for this submission.
    /// Overrides (and is cross-checked against) whatever
    /// `Supervisor::task_for_branch` would infer from an agent record — see
    /// `Supervisor::resolve_land_task`.
    #[serde(default)]
    task: Option<String>,
}

fn default_main_branch() -> String {
    "main".to_string()
}

#[derive(Deserialize)]
struct RevertParams {
    name: String,
    /// Reopen the agent's ticket as `blocked` instead of `open`.
    #[serde(default)]
    block: bool,
}

#[derive(Deserialize)]
struct BlockingParams {
    #[serde(flatten)]
    pattern: PatternParams,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

fn repository_head(path: &std::path::Path) -> rk_core::Result<String> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(path)
        .args(["rev-parse", "HEAD"])
        .env("LC_ALL", "C")
        .output()?;
    if !output.status.success() {
        return Err(rk_core::Error::other(format!(
            "cannot resolve repository HEAD for {}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let head = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if head.is_empty() {
        return Err(rk_core::Error::other("repository HEAD is empty"));
    }
    Ok(head)
}

/// Read a repo's configured URL for `remote` by shelling to git, so the host
/// can be inferred at registration time. Returns `None` when the path is not a
/// repo or has no such remote — host inference is best-effort, never fatal.
fn repo_remote_url(path: &std::path::Path, remote: &str) -> Option<String> {
    let out = std::process::Command::new("git")
        .args(["-C"])
        .arg(path)
        .args(["remote", "get-url", remote])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let url = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if url.is_empty() {
        None
    } else {
        Some(url)
    }
}

#[derive(Deserialize)]
struct RepoAddParams {
    name: String,
    path: String,
    /// Explicit merge mode; when absent the daemon's `[policy]` default applies.
    #[serde(default)]
    merge_mode: Option<rk_core::config::MergeMode>,
    /// Explicit remote name; when absent, `origin` is used at operation time.
    #[serde(default)]
    remote: Option<String>,
}

#[derive(Deserialize)]
struct RepoInspectParams {
    target: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepoOnboardingStartParams {
    target: String,
    #[serde(default)]
    harness: Option<String>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    attach: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepoOnboardingProposeParams {
    session: String,
    proposal: crate::onboarding_proposals::OnboardingProposalDraft,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepoOnboardingDecisionParams {
    session: String,
    proposal: String,
    digest: String,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepoOnboardingApplyParams {
    session: String,
    proposal: String,
    digest: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepoOnboardingResumeParams {
    session: String,
    #[serde(default)]
    attach: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RepoOnboardingSessionParams {
    session: String,
}

fn onboarding_payload(
    session: &crate::onboarding_sessions::OnboardingSession,
    reused: bool,
) -> Value {
    json!({
        "session": session.status(),
        "report": session.report(),
        "reused": reused,
    })
}

#[derive(Deserialize)]
struct TicketListParams {
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    parent: Option<String>,
}

#[derive(Deserialize)]
struct TicketGetParams {
    id: String,
}

#[derive(Deserialize)]
struct TicketDepParams {
    id: String,
    dep: String,
    #[serde(default)]
    remove: bool,
}

#[derive(Deserialize)]
struct TicketReadyParams {
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Deserialize)]
struct TicketUpdateParams {
    id: String,
    #[serde(flatten)]
    changes: crate::tickets::TicketChanges,
    /// Evidence for a groomer's closure — see `read_only_roles::method_allowed`.
    /// Ignored for any other caller.
    #[serde(default)]
    reason: Option<GroomReason>,
}

#[derive(Deserialize)]
struct GroomReason {
    reason: String,
    evidence: String,
}

#[derive(Deserialize)]
struct TicketReopenParams {
    id: String,
    /// Target status: "open" or "blocked" (defaults to "open"). Validated by
    /// `Tickets::reopen` itself.
    #[serde(default)]
    status: Option<String>,
}

fn parse_params<T: serde::de::DeserializeOwned>(params: &Value) -> Result<T, String> {
    serde_json::from_value(params.clone()).map_err(|e| e.to_string())
}

/// Does `commit`'s own diff (against its first parent) touch a path matched
/// by `protected_paths` (an ERE)? The same question `.rk/checks.cue`'s
/// `steward-protected-paths` check answers by hand for a landing candidate's
/// diff-scope range; this asks it about one already-landed commit instead,
/// via the exact same `grep -qE` semantics. `None` means the question could
/// not be answered (no parent commit, git or grep unavailable) — the caller
/// treats that as missing evidence, never as "does not touch".
fn touches_protected_path(
    repo: &rk_git::Repo,
    commit: &str,
    protected_paths: &str,
) -> Option<bool> {
    let parent = format!("{commit}^");
    let stat = repo.diff_stat(&parent, commit).ok()?;
    grep_matches(&stat.files, protected_paths)
}

fn grep_matches(files: &[String], pattern: &str) -> Option<bool> {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let mut child = Command::new("grep")
        .args(["-qE", pattern])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    if let Some(mut stdin) = child.stdin.take() {
        let _ = writeln!(stdin, "{}", files.join("\n"));
    }
    child.wait().ok().map(|status| status.success())
}

async fn write_json_line<W, T>(write: &mut W, value: &T) -> std::io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
    T: serde::Serialize,
{
    let mut out = serde_json::to_vec(value)?;
    if out.len() > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "response exceeds 1 MiB",
        ));
    }
    out.push(b'\n');
    write.write_all(&out).await
}

/// Write an RPC reply, downgrading to a `frame_too_large` error (still
/// carrying the request's `id`) instead of silently dropping the connection
/// when the reply itself is oversized. See [`MAX_RESPONSE_FRAME_BYTES`].
async fn write_response<W>(write: &mut W, response: &Response) -> std::io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut out = serde_json::to_vec(response)?;
    // NOTE: deliberately does not delegate to `write_json_line` for the
    // in-bounds case — that function enforces the older, smaller
    // `MAX_FRAME_BYTES` (the request-side cap), which would silently claw
    // back the headroom this function exists to grant.
    if out.len() > MAX_RESPONSE_FRAME_BYTES {
        let fallback = Response::err(
            response.id.clone(),
            codes::FRAME_TOO_LARGE,
            format!(
                "response too large ({} bytes, limit {MAX_RESPONSE_FRAME_BYTES}); narrow the request (e.g. drop --all/--archived, or filter by repo)",
                out.len()
            ),
        );
        out = serde_json::to_vec(&fallback)?;
    }
    out.push(b'\n');
    write.write_all(&out).await
}

/// Take the exclusive, kernel-held singleton lock for this `RK_HOME` before
/// touching the socket or any other daemon state.
///
/// `flock` is atomic (unlike the connect-then-check-pid probe below, which
/// has a TOCTOU window between the probe and `bind`) and is released by the
/// kernel the instant every fd referencing it closes — including on SIGKILL
/// — so a crashed daemon's lock is never actually "stale" and needs no
/// separate recovery path: the next daemon simply acquires it. A second LIVE
/// daemon against the same home gets a clean, immediate refusal naming the
/// holder's pid instead of contending with the first over the socket file
/// and the tuplespace WAL (TKT-01M04D394PQ8VS5N3V441D1MDD: multiple
/// concurrent daemons — stray old builds, a leaked test daemon, imprecise
/// kills — contending on one RK_HOME wedged the fleet under load).
///
/// The returned `File` must be kept alive for the daemon's lifetime; dropping
/// it (including implicitly, on process exit or crash) releases the lock.
fn acquire_singleton_lock(layout: &Layout) -> rk_core::Result<std::fs::File> {
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::unix::io::AsRawFd;

    let path = layout.lockfile_path();
    // Explicitly `truncate(false)`: the previous holder's pid must survive
    // the open so a losing contender can still read it off below.
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)?;

    // SAFETY: `file` owns a valid fd for the duration of this call, and
    // `flock` only touches the kernel's lock table entry for that fd.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
            // The holder writes its pid immediately after taking the flock,
            // but a contender can observe EWOULDBLOCK inside that tiny
            // window. Re-read briefly so refusals name the holder in
            // practice; the unknown-holder text below remains the honest
            // fallback for a holder that dies mid-write.
            let mut holder = String::new();
            for _ in 0..10 {
                holder.clear();
                let _ = file.seek(SeekFrom::Start(0));
                let _ = file.read_to_string(&mut holder);
                if !holder.trim().is_empty() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            let holder = holder.trim();
            return Err(rk_core::Error::other(if holder.is_empty() {
                format!(
                    "another rat-kingdom daemon already holds the lock at {} (holder pid \
                     unknown) — refusing to start a second daemon against this RK_HOME",
                    path.display()
                )
            } else {
                format!(
                    "another rat-kingdom daemon (pid {holder}) already holds the lock at {} \
                     — refusing to start a second daemon against this RK_HOME",
                    path.display()
                )
            }));
        }
        return Err(err.into());
    }

    // We hold the lock: record our pid so a contender can name us, and a
    // human can `kill` the right process directly from the refusal message.
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    write!(file, "{}", std::process::id())?;
    file.flush()?;
    Ok(file)
}

#[cfg(test)]
mod singleton_lock_tests {
    use super::*;

    #[test]
    fn second_acquisition_fails_while_first_is_held_and_names_the_holder() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        layout.ensure().unwrap();

        // flock is per open-file-description, not per process: two separate
        // `open()`s of the same path from this one process still conflict,
        // which is exactly what lets this run as a plain in-process test.
        let _held = acquire_singleton_lock(&layout).expect("first acquisition succeeds");
        let err = acquire_singleton_lock(&layout)
            .expect_err("second acquisition must fail while the first is held");
        let msg = err.to_string();
        assert!(
            msg.contains("already holds the lock"),
            "unexpected message: {msg}"
        );
        assert!(
            msg.contains(&std::process::id().to_string()),
            "refusal should name the holder pid: {msg}"
        );
    }

    /// Proves the lock needs no separate stale-lock recovery path: a real
    /// second OS process holds it, gets SIGKILLed (no destructors, no
    /// cleanup — an in-process `File` drop would prove nothing about this),
    /// and a fresh acquisition afterward succeeds immediately.
    #[test]
    fn lock_is_recovered_after_the_holder_is_killed() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        layout.ensure().unwrap();

        // Handshake marker: the child touches this AFTER it holds the lock,
        // so the parent never has to probe by transiently acquiring (a probe
        // acquisition could win the race against the child's attempt and
        // flake the test — the structural race review flagged).
        let marker = dir.path().join("child-holds-lock");
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            // Child: hold the lock and park until killed. Only sync,
            // fork-safe calls here — no tokio, no destructors on exit.
            // Retry the acquisition: the parent process itself briefly held
            // the lock in earlier test setup on some platforms, and a single
            // losing attempt must not abort the fixture.
            for _ in 0..100 {
                if let Ok(_guard) = acquire_singleton_lock(&layout) {
                    let _ = std::fs::write(&marker, b"held");
                    loop {
                        std::thread::sleep(std::time::Duration::from_secs(60));
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            unsafe { libc::_exit(1) }
        }

        // Parent: wait on the marker — no probe acquisitions.
        let mut child_holds_it = false;
        for _ in 0..500 {
            if marker.exists() {
                child_holds_it = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        if !child_holds_it {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
                let mut status = 0;
                libc::waitpid(pid, &mut status, 0);
            }
        }
        assert!(child_holds_it, "child never acquired the lock");

        // While the child demonstrably holds it, a refusal names the child.
        let err = acquire_singleton_lock(&layout)
            .expect_err("acquisition must fail while the child holds the lock");
        assert!(
            err.to_string().contains(&pid.to_string()),
            "refusal should name the child holder pid {pid}: {err}"
        );

        unsafe {
            libc::kill(pid, libc::SIGKILL);
            let mut status = 0;
            libc::waitpid(pid, &mut status, 0);
        }

        let recovered = acquire_singleton_lock(&layout);
        assert!(
            recovered.is_ok(),
            "lock was not recovered after the holder was killed: {:?}",
            recovered.err()
        );
    }
}

fn read_pid(layout: &Layout) -> Option<u32> {
    std::fs::read_to_string(layout.pid_file())
        .ok()?
        .trim()
        .parse()
        .ok()
}

fn process_alive(pid: u32) -> bool {
    // kill(pid, 0): 0 or EPERM = alive; ESRCH = gone.
    unsafe { kill_probe(pid as i32, 0) == 0 || last_errno_is_eperm() }
}

extern "C" {
    #[link_name = "kill"]
    fn kill_probe(pid: i32, sig: i32) -> i32;
}

fn last_errno_is_eperm() -> bool {
    std::io::Error::last_os_error().raw_os_error() == Some(1)
}

/// Awaits on listeners created once outside the accept loop (see the call
/// site in [`RkDaemon::run`]) rather than registering fresh SIGTERM/SIGINT
/// listeners on every loop iteration. A `None` listener means registration
/// failed at startup (e.g. the platform has no signal driver installed);
/// that branch simply never fires rather than panicking or busy-looping.
async fn wait_for_shutdown_signal(term: &mut Option<Signal>, int: &mut Option<Signal>) {
    match (term.as_mut(), int.as_mut()) {
        (Some(term), Some(int)) => {
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
            }
        }
        (Some(term), None) => {
            term.recv().await;
        }
        (None, Some(int)) => {
            int.recv().await;
        }
        (None, None) => std::future::pending().await,
    }
}

#[cfg(test)]
mod withdraw_authorisation_tests {
    //! TKT-184: who may close a ballot. Pinned as a pure predicate because the
    //! wire tests cannot reach the interesting case — a test client with no
    //! `RK_AGENT` authenticates as `operator`, which is authorised for every
    //! ballot, so the peer-rat denial would never be exercised end to end.
    use super::*;

    #[test]
    fn the_proposer_and_the_operator_may_withdraw() {
        assert!(may_withdraw("Camembert-2", "Camembert-2"), "the proposer");
        assert!(may_withdraw(OPERATOR_ACTOR, "Camembert-2"), "the operator");
        // A local/pre-auth caller arrives blank and is the operator, exactly as
        // `handle_out` and the ticket handlers already read it.
        assert!(may_withdraw("", "Camembert-2"), "a blank caller");
    }

    #[test]
    fn a_peer_rat_may_not_close_someone_elses_ballot() {
        // The load-bearing denial. Declining to endorse is how a rat disagrees
        // with a proposal; if any rat could withdraw any other's, one dissenter
        // would outrank three endorsers and quorum would stop meaning anything.
        assert!(!may_withdraw("Gruyere-2", "Camembert-2"));
        // Name prefixes are distinct rats — a namesake generation must not
        // inherit standing over its predecessor's ballot (the TKT-146 lesson).
        assert!(!may_withdraw("Camembert-2", "Camembert"));
        assert!(!may_withdraw("Camembert", "Camembert-2"));
    }

    /// A ballot proposed BY the operator stays withdrawable by the operator, and
    /// is not thereby opened to every rat.
    #[test]
    fn an_operator_authored_ballot_is_still_operator_only() {
        assert!(may_withdraw(OPERATOR_ACTOR, OPERATOR_ACTOR));
        assert!(!may_withdraw("Gruyere-2", OPERATOR_ACTOR));
    }
}

#[cfg(test)]
mod agent_fact_authorisation_tests {
    use super::*;

    #[test]
    fn an_agent_may_write_a_fact_for_its_own_instance() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Daemon::new_in_memory(Layout::at(dir.path()), "test-castle".into()).unwrap();
        let response = daemon.handle_out(Request {
            id: "fact-1".into(),
            method: "space.out".into(),
            auth: String::new(),
            caller: "Whisker".into(),
            client_version: None,
            params: json!({
                "category": "fact",
                "scope": "rat-kingdom",
                "identity": "observed-rate-limit",
                "payload": {"limit": 100}
            }),
        });

        assert!(
            response.error.is_none(),
            "agent fact was rejected: {response:?}"
        );
        let facts = daemon
            .space
            .scan(&Pattern::category(Category::Fact).identity("observed-rate-limit"))
            .unwrap();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].instance, "Whisker");
        assert_eq!(facts[0].payload["limit"], 100);
    }

    #[test]
    fn an_agent_may_not_impersonate_another_fact_author() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Daemon::new_in_memory(Layout::at(dir.path()), "test-castle".into()).unwrap();
        let response = daemon.handle_out(Request {
            id: "fact-2".into(),
            method: "space.out".into(),
            auth: String::new(),
            caller: "Whisker".into(),
            client_version: None,
            params: json!({
                "category": "fact",
                "scope": "rat-kingdom",
                "identity": "forged-observation",
                "instance": "Nibbles",
                "payload": {"forged": true}
            }),
        });

        assert_eq!(
            response.error.as_ref().map(|error| error.code.as_str()),
            Some(codes::FORBIDDEN)
        );
    }

    #[test]
    fn agents_can_vote_change_and_retract_facts_with_aggregate_counts() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = Daemon::new_in_memory(Layout::at(dir.path()), "test-castle".into()).unwrap();
        let fact = daemon.handle_out(Request {
            id: "fact-vote-source".into(),
            method: "space.out".into(),
            auth: String::new(),
            caller: "Whisker".into(),
            client_version: None,
            params: json!({
                "category": "fact",
                "scope": "rat-kingdom",
                "identity": "observed-rate-limit",
                "payload": {"limit": 100}
            }),
        });
        let fact_id = fact.result.unwrap()["id"].as_str().unwrap().to_string();

        let vote = |id: &str, caller: &str, value: &str| {
            daemon.handle_fact_vote(Request {
                id: id.into(),
                method: "fact.vote".into(),
                auth: String::new(),
                caller: caller.into(),
                client_version: None,
                params: json!({"fact": fact_id, "vote": value}),
            })
        };
        assert_eq!(
            vote("up-1", "Whisker", "up").result.unwrap()["written"],
            true
        );
        assert_eq!(
            vote("up-2", "Whisker", "up").result.unwrap()["already"],
            true
        );
        assert_eq!(
            vote("down-1", "Whisker", "down").result.unwrap()["written"],
            true
        );
        assert_eq!(
            vote("up-3", "Nibbles", "up").result.unwrap()["written"],
            true
        );

        let scan = daemon.handle_scan(Request {
            id: "scan-1".into(),
            method: "space.scan".into(),
            auth: String::new(),
            caller: "operator".into(),
            client_version: None,
            params: json!({"category": "fact"}),
        });
        assert_eq!(
            scan.result.as_ref().unwrap()["fact_votes"][&fact_id]["up"],
            1
        );
        assert_eq!(
            scan.result.as_ref().unwrap()["fact_votes"][&fact_id]["down"],
            1
        );
        assert_eq!(
            scan.result.as_ref().unwrap()["fact_votes"][&fact_id]["score"],
            0
        );

        assert_eq!(
            vote("clear-1", "Whisker", "clear").result.unwrap()["written"],
            true
        );
        assert_eq!(
            vote("clear-2", "Whisker", "clear").result.unwrap()["already"],
            true
        );
        let scan = daemon.handle_scan(Request {
            id: "scan-2".into(),
            method: "space.scan".into(),
            auth: String::new(),
            caller: "operator".into(),
            client_version: None,
            params: json!({"category": "fact"}),
        });
        assert_eq!(
            scan.result.as_ref().unwrap()["fact_votes"][&fact_id]["up"],
            1
        );
        assert_eq!(
            scan.result.as_ref().unwrap()["fact_votes"][&fact_id]["down"],
            0
        );
        assert_eq!(
            scan.result.as_ref().unwrap()["fact_votes"][&fact_id]["score"],
            1
        );

        let raw = daemon.handle_out(Request {
            id: "forged-vote".into(),
            method: "space.out".into(),
            auth: String::new(),
            caller: "Whisker".into(),
            client_version: None,
            params: json!({
                "category": "fact_vote",
                "scope": "rat-kingdom",
                "identity": fact_id,
                "instance": "Nibbles",
                "payload": {"vote": "up"}
            }),
        });
        assert_eq!(
            raw.error.as_ref().map(|error| error.code.as_str()),
            Some(codes::FORBIDDEN)
        );
    }
}

#[cfg(test)]
mod default_agent_profile_tests {
    use super::*;
    use rk_core::config::AgentProfileConfig;
    use std::process::Command;

    fn git(repo: &std::path::Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test]
    async fn config_default_profile_reaches_a_direct_spawn() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "-b", "main"]);
        git(&repo, &["config", "user.email", "rat@example.com"]);
        git(&repo, &["config", "user.name", "Rat"]);
        std::fs::write(repo.join("README.md"), "test\n").unwrap();
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "-m", "initial"]);

        let mut config = rk_core::config::Config::default();
        config.harness.default = "claude".into();
        config.agents.insert(
            "default".into(),
            AgentProfileConfig {
                harness: Some("fake".into()),
                model: Some("profile-model".into()),
                permission_mode: Some("workspace-write".into()),
            },
        );
        let daemon = Daemon::new(Layout::at(dir.path().join("rk-home")), &config).unwrap();
        let record = daemon
            .supervisor
            .spawn_async(
                crate::supervisor::SpawnParams {
                    repo: repo.display().to_string(),
                    task: "direct-defaults".into(),
                    prompt: Some("finish".into()),
                    role: "rat".into(),
                    coordination: None,
                    harness: None,
                    parent: None,
                    base: None,
                    review: None,
                    model: None,
                    permission_mode: None,
                    attach: false,
                    workflow_instance: None,
                    coordinator: None,
                    instance_max_usd: None,
                    profile: None,
                    resolved_profile: None,
                },
                0,
            )
            .await
            .unwrap();

        assert_eq!(record.harness, "fake");
        assert_eq!(record.model.as_deref(), Some("profile-model"));
        assert_eq!(record.permission_mode.as_deref(), Some("workspace-write"));
    }

    fn profile_cfg(model: &str) -> AgentProfileConfig {
        AgentProfileConfig {
            harness: Some("fake".into()),
            model: Some(model.into()),
            permission_mode: None,
        }
    }

    fn spawn_params(task: &str, profile: Option<&str>) -> crate::supervisor::SpawnParams {
        crate::supervisor::SpawnParams {
            repo: ".".into(),
            task: task.into(),
            prompt: None,
            role: "rat".into(),
            coordination: None,
            harness: None,
            parent: None,
            base: None,
            model: None,
            permission_mode: None,
            attach: false,
            workflow_instance: None,
            review: None,
            coordinator: None,
            instance_max_usd: None,
            profile: profile.map(String::from),
            resolved_profile: None,
        }
    }

    /// The acceptance for TKT-01M0CW1918D10C48C2J3TMRSFQ: a ticket must reach
    /// the same profile whichever dispatcher picks it up. Before this, tier
    /// routing ran only on the drain path, so `rk spawn --ticket` quietly
    /// resolved `[agents.default]` and the fleet's cost policy went unenforced
    /// for every hand-dispatched ticket.
    #[tokio::test]
    async fn operator_spawn_and_drain_resolve_the_same_tier() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = rk_core::config::Config::default();
        config.harness.default = "fake".into();
        config
            .agents
            .insert("default".into(), profile_cfg("opus-default"));
        config
            .agents
            .insert("sonnet-worker".into(), profile_cfg("sonnet"));
        config.agents.insert("premium".into(), profile_cfg("opus"));
        // Catch-all last, exactly as an operator would write it: everything is
        // a medium unless it is labelled `hard`.
        config.tiers.rules = vec![
            rk_core::config::TierRuleConfig {
                priority: None,
                label: Some("hard".into()),
                tier: "premium".into(),
            },
            rk_core::config::TierRuleConfig {
                priority: None,
                label: None,
                tier: "sonnet-worker".into(),
            },
        ];
        let daemon = Daemon::new(Layout::at(dir.path().join("rk-home")), &config).unwrap();

        let medium = daemon
            .tickets
            .create(
                serde_json::from_value(json!({"title": "medium work", "scope": "demo"})).unwrap(),
            )
            .await
            .unwrap();
        let hard = daemon
            .tickets
            .create(
                serde_json::from_value(
                    json!({"title": "hard work", "scope": "demo", "labels": ["hard"]}),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        // The drain's view of the same two tickets, resolved through its own
        // (pre-existing) path — this is the reference the operator path must
        // agree with, not a re-derivation of the expected answer.
        let drain = crate::drain::Drain::new(
            Arc::clone(&daemon.supervisor),
            Arc::clone(&daemon.tickets),
            Layout::at(dir.path().join("rk-home")),
            rk_core::config::DrainConfig::default(),
            daemon.tier_routing.clone(),
            daemon.global_agents.clone(),
            daemon.default_harness.clone(),
        );

        for ticket in [&medium, &hard] {
            let by_drain = drain.resolve_tier(ticket).unwrap();
            let (by_operator, name, source) = daemon
                .route_spawn_profile(&spawn_params(&ticket.identity, None))
                .unwrap()
                .expect("a catch-all rule must route every ticket");
            assert_eq!(source, "tier");
            assert_eq!(
                by_operator.model, by_drain.model,
                "{} routed to {name} by hand but to {:?} by the drain",
                ticket.identity, by_drain.model
            );
            assert_eq!(
                by_operator.harness.as_deref(),
                Some(by_drain.harness.as_str())
            );
        }
        // ...and the two tickets are genuinely on different tiers, so the
        // agreement above is not both paths collapsing to one default.
        let medium_model = daemon
            .route_spawn_profile(&spawn_params(&medium.identity, None))
            .unwrap()
            .unwrap()
            .0
            .model;
        assert_eq!(medium_model.as_deref(), Some("sonnet"));
        assert_eq!(
            daemon
                .route_spawn_profile(&spawn_params(&hard.identity, None))
                .unwrap()
                .unwrap()
                .0
                .model
                .as_deref(),
            Some("opus")
        );

        // `--profile` is the documented opt-out: it replaces the tier rather
        // than layering under it, so a catch-all cannot override an operator
        // who named a profile at dispatch time.
        let (overridden, name, source) = daemon
            .route_spawn_profile(&spawn_params(&medium.identity, Some("premium")))
            .unwrap()
            .unwrap();
        assert_eq!((name.as_str(), source), ("premium", "profile"));
        assert_eq!(overridden.model.as_deref(), Some("opus"));

        // A typo must be refused, not silently dispatched on the default.
        assert!(daemon
            .route_spawn_profile(&spawn_params(&medium.identity, Some("nope")))
            .is_err());
    }
}

#[cfg(test)]
mod display_alias_tests {
    //! TKT-124: the `castle_name` alias is presentation-only. These tests pin the
    //! load-bearing invariant that the alias is confined to the display field and
    //! NEVER becomes the wire identity (`self.castle`) — the string handed to the
    //! syncer and stamped on every daemon-authored tuple's `instance`. If a
    //! future refactor re-routes `castle_name` back into the wire id, the alias
    //! would leak into signed `SyncRecord`s; that regression fails here.
    use super::*;
    use rk_core::config::Config;

    fn daemon_with_alias(alias: Option<&str>) -> (tempfile::TempDir, Daemon) {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let config = Config {
            castle_name: alias.map(|s| s.to_string()),
            ..Config::default()
        };
        let daemon = Daemon::new(layout, &config).unwrap();
        (dir, daemon)
    }

    #[test]
    fn alias_is_the_display_but_never_the_wire_id() {
        let (_dir, daemon) = daemon_with_alias(Some("Nikaido"));
        // Wire id is the crypto actor, NOT the alias — so nothing the syncer
        // exports (author == self.castle) can carry "Nikaido".
        assert!(daemon.castle.starts_with("castle-"));
        assert_ne!(daemon.castle, "Nikaido");
        // Display + the status render path both show the friendly alias.
        assert_eq!(daemon.castle_display, "Nikaido");
        assert_eq!(daemon.status()["castle"], json!("Nikaido"));
    }

    #[test]
    fn unset_alias_shows_the_actor_id_unchanged() {
        let (_dir, daemon) = daemon_with_alias(None);
        assert!(daemon.castle.starts_with("castle-"));
        // No behaviour change when unset: display == wire id == status.
        assert_eq!(daemon.castle_display, daemon.castle);
        assert_eq!(daemon.status()["castle"], json!(daemon.castle));
    }

    /// The faithful end-to-end regression: with an alias configured AND sync on,
    /// read back the records this castle actually exported (its own notes ref)
    /// and assert the alias appears in NONE of them — every record carries only
    /// the crypto actor id.
    #[test]
    fn alias_never_appears_in_an_exported_sync_record() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let mut config = Config {
            castle_name: Some("Nikaido".into()),
            ..Config::default()
        };
        config.sync.enabled = true;
        let daemon = Daemon::new(layout, &config).unwrap();

        // Syncer::new wrote the castle-presence record at construction. Read it
        // back through a reader bound to the same identity/ref.
        let syncer = daemon
            .syncer
            .as_ref()
            .expect("sync enabled → syncer present");
        let identity =
            rk_core::identity::CastleIdentity::load_or_create(&daemon.layout.castle_key_path())
                .unwrap();
        let reader = rk_sync::NotesSync::new(syncer.repo_path(), identity);
        let records = reader.own_records().unwrap();

        assert!(!records.is_empty(), "expected at least the presence record");
        for r in &records {
            let json = serde_json::to_string(r).unwrap();
            assert!(
                !json.contains("Nikaido"),
                "presentation alias leaked into a SyncRecord: {json}"
            );
            assert!(
                r.actor.starts_with("castle-"),
                "record actor must be the crypto id, got {}",
                r.actor
            );
        }
    }
}

#[cfg(test)]
mod authorize_reasoned_tests {
    //! TKT-01M01EYN0132N30BWP8BXHXDR6: `authorized()` used to collapse every
    //! rejection into one generic bool, so a caller-side credential problem
    //! (e.g. a sandboxed harness losing RK_AUTH_TOKEN) was indistinguishable
    //! server-side from a role or supervision mismatch. These pin the reason
    //! tag for each arm so a future refactor can't silently re-collapse them.
    use super::*;
    use crate::agents::{AgentRecord, AgentState};
    use rk_core::config::Config;
    use rk_harness::TokenUsage;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn test_daemon() -> (tempfile::TempDir, Daemon) {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let daemon = Daemon::new(layout, &Config::default()).unwrap();
        (dir, daemon)
    }

    fn test_daemon_with_role(role: &str) -> (tempfile::TempDir, Daemon) {
        test_daemon_with_named_role("invalid-rat", role)
    }

    pub(super) fn test_daemon_with_named_role(
        name: &str,
        role: &str,
    ) -> (tempfile::TempDir, Daemon) {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        layout.ensure().unwrap();
        let now = chrono::Utc::now();
        let record = AgentRecord {
            name: name.into(),
            spawn: None,
            role: role.into(),
            coordination: None,
            harness: "fake".into(),
            permission_mode: None,
            model: None,
            repo_root: PathBuf::from("/tmp/repo"),
            repo_name: "repo".into(),
            task: Some("auth-reason-test".into()),
            branch: Some("rat/invalid-rat/auth-reason-test".into()),
            fork_point: None,
            worktree: Some(PathBuf::from("/tmp/worktree/invalid-rat")),
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            review: None,
            coordinator: None,
            session_id: Some("test-session".into()),
            attach_target: None,
            pid: None,
            merge_commit: None,
            state: AgentState::Running,
            crashed: false,
            stderr_tail: None,
            result: None,
            progress: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: now,
            updated_at: now,
            archived_at: None,
        };
        let mut records = HashMap::new();
        records.insert(record.name.clone(), record);
        std::fs::write(
            layout.home().join("agents.json"),
            serde_json::to_vec(&records).unwrap(),
        )
        .unwrap();
        let daemon = Daemon::new(layout, &Config::default()).unwrap();
        (dir, daemon)
    }

    fn req(caller: &str, method: &str, auth: &str) -> Request {
        Request {
            id: "1".into(),
            method: method.into(),
            auth: auth.into(),
            caller: caller.into(),
            client_version: None,
            params: Value::Null,
        }
    }

    #[test]
    fn wrong_token_is_tagged_token_mismatch() {
        let (_dir, daemon) = test_daemon();
        let request = req("some-rat", "space.scan", "not-the-real-token");
        let origin = PeerOrigin {
            pid_observed: true,
            supervised_agents: Default::default(),
        };
        let (allowed, reason) = daemon.authorize_reasoned(&request, &origin);
        assert!(!allowed);
        assert_eq!(reason, "token_mismatch");
    }

    #[test]
    fn operator_claim_without_observed_pid_is_tagged() {
        let (_dir, daemon) = test_daemon();
        let request = req("operator", "space.scan", "");
        let origin = PeerOrigin {
            pid_observed: false,
            supervised_agents: Default::default(),
        };
        let (allowed, reason) = daemon.authorize_reasoned(&request, &origin);
        assert!(!allowed);
        assert_eq!(reason, "operator_without_observed_pid");
    }

    #[test]
    fn caller_outside_the_supervised_set_is_tagged() {
        let (_dir, daemon) = test_daemon();
        let token = rk_core::paths::derive_agent_token(&daemon.auth_token, "some-rat");
        let request = req("some-rat", "space.scan", &token);
        let mut supervised = std::collections::HashSet::new();
        supervised.insert("a-different-rat".to_string());
        let origin = PeerOrigin {
            pid_observed: true,
            supervised_agents: supervised,
        };
        let (allowed, reason) = daemon.authorize_reasoned(&request, &origin);
        assert!(!allowed);
        assert_eq!(reason, "supervised_agents_mismatch");
    }

    #[test]
    fn invalid_role_is_tagged_with_a_controlled_registry_fixture() {
        let (_dir, daemon) = test_daemon_with_role("not-a-supported-role");
        let token = rk_core::paths::derive_agent_token(&daemon.auth_token, "invalid-rat");
        let request = req("invalid-rat", "space.scan", &token);
        let mut supervised = std::collections::HashSet::new();
        supervised.insert("invalid-rat".to_string());
        let origin = PeerOrigin {
            pid_observed: true,
            supervised_agents: supervised,
        };
        let (allowed, reason) = daemon.authorize_reasoned(&request, &origin);
        assert!(!allowed);
        assert_eq!(reason, "invalid_role");
    }

    #[test]
    fn correct_credentials_are_allowed_with_no_reason() {
        let (_dir, daemon) = test_daemon();
        let token = rk_core::paths::derive_agent_token(&daemon.auth_token, "some-rat");
        let request = req("some-rat", "space.scan", &token);
        let mut supervised = std::collections::HashSet::new();
        supervised.insert("some-rat".to_string());
        let origin = PeerOrigin {
            pid_observed: true,
            supervised_agents: supervised,
        };
        let (allowed, reason) = daemon.authorize_reasoned(&request, &origin);
        assert!(allowed);
        assert_eq!(reason, "");
    }

    fn groomer_origin() -> PeerOrigin {
        let mut supervised = std::collections::HashSet::new();
        supervised.insert("invalid-rat".to_string());
        PeerOrigin {
            pid_observed: true,
            supervised_agents: supervised,
        }
    }

    fn groomer_close_req(auth: &str, params: Value) -> Request {
        Request {
            id: "1".into(),
            method: "ticket.update".into(),
            auth: auth.into(),
            caller: "invalid-rat".into(),
            client_version: None,
            params,
        }
    }

    #[test]
    fn groomer_evidence_backed_closure_is_allowed_with_no_reason() {
        let (_dir, daemon) = test_daemon_with_role(crate::read_only_roles::GROOMER_ROLE);
        let token = rk_core::paths::derive_agent_token(&daemon.auth_token, "invalid-rat");
        let request = groomer_close_req(
            &token,
            json!({"id": "TKT-1", "status": "closed",
                "reason": {"reason": "stale-rework", "evidence": "TKT-2 done"}}),
        );
        let (allowed, reason) = daemon.authorize_reasoned(&request, &groomer_origin());
        assert!(allowed);
        assert_eq!(reason, "");
    }

    #[test]
    fn groomer_closure_without_evidence_is_operator_only() {
        let (_dir, daemon) = test_daemon_with_role(crate::read_only_roles::GROOMER_ROLE);
        let token = rk_core::paths::derive_agent_token(&daemon.auth_token, "invalid-rat");
        let request = groomer_close_req(&token, json!({"id": "TKT-1", "status": "closed"}));
        let (allowed, reason) = daemon.authorize_reasoned(&request, &groomer_origin());
        assert!(!allowed);
        assert_eq!(reason, "operator_only_method");
    }

    #[test]
    fn groomer_cannot_reopen_or_mark_done() {
        let (_dir, daemon) = test_daemon_with_role(crate::read_only_roles::GROOMER_ROLE);
        let token = rk_core::paths::derive_agent_token(&daemon.auth_token, "invalid-rat");
        for status in ["open", "done", "in_progress"] {
            let request = groomer_close_req(
                &token,
                json!({"id": "TKT-1", "status": status,
                    "reason": {"reason": "x", "evidence": "y"}}),
            );
            let (allowed, reason) = daemon.authorize_reasoned(&request, &groomer_origin());
            assert!(!allowed, "status {status} must be refused");
            assert_eq!(reason, "operator_only_method");
        }
    }

    #[test]
    fn groomer_cannot_reach_ticket_dep() {
        let (_dir, daemon) = test_daemon_with_role(crate::read_only_roles::GROOMER_ROLE);
        let token = rk_core::paths::derive_agent_token(&daemon.auth_token, "invalid-rat");
        let request = Request {
            id: "1".into(),
            method: "ticket.dep".into(),
            auth: token,
            caller: "invalid-rat".into(),
            client_version: None,
            params: json!({"id": "TKT-1", "dep": "TKT-2"}),
        };
        let (allowed, reason) = daemon.authorize_reasoned(&request, &groomer_origin());
        assert!(!allowed);
        assert_eq!(reason, "operator_only_method");
    }

    #[test]
    fn an_ordinary_rat_cannot_close_a_ticket_even_with_evidence() {
        let (_dir, daemon) = test_daemon_with_role("rat");
        let token = rk_core::paths::derive_agent_token(&daemon.auth_token, "invalid-rat");
        let request = groomer_close_req(
            &token,
            json!({"id": "TKT-1", "status": "closed",
                "reason": {"reason": "stale-rework", "evidence": "TKT-2 done"}}),
        );
        let (allowed, reason) = daemon.authorize_reasoned(&request, &groomer_origin());
        assert!(!allowed);
        assert_eq!(reason, "operator_only_method");
    }
}

#[cfg(test)]
mod review_artifact_binding_tests {
    use super::*;
    use rk_core::review::ReviewContext;

    fn review() -> ReviewContext {
        ReviewContext {
            branch: "rat/fidget-10/tkt-1".into(),
            head_sha: "0640835".into(),
            target: "release".into(),
            task: "TKT-1".into(),
            attempt: "landing-review-1".into(),
        }
    }

    #[test]
    fn authenticated_reviewer_rejects_an_incorrectly_bound_artifact_exactly() {
        let (_dir, daemon) =
            super::authorize_reasoned_tests::test_daemon_with_named_role("Brie-10", "reviewer");
        daemon
            .supervisor
            .lock_registry()
            .update("Brie-10", |record| record.review = Some(review()))
            .unwrap();

        let response = daemon.handle_out(Request {
            id: "wrong-review".into(),
            method: "space.out".into(),
            auth: String::new(),
            caller: "Brie-10".into(),
            client_version: None,
            params: json!({
                "category": "artifact",
                "scope": "repo",
                "identity": "review",
                "payload": {
                    "recommendation": "APPROVE",
                    "branch": "rat/fidget-10/steward-review-tkt-1",
                    "head_sha": "0640835",
                    "target": "release",
                    "task": "TKT-1",
                    "review_attempt": "landing-review-1"
                }
            }),
        });

        let error = response.error.expect("wrong binding must be rejected");
        assert_eq!(error.code, codes::BAD_PARAMS);
        assert_eq!(
            error.message,
            "review artifact binding mismatch for branch: expected \
             'rat/fidget-10/tkt-1', got \"rat/fidget-10/steward-review-tkt-1\""
        );
        assert!(
            daemon
                .space
                .scan(&Pattern::category(Category::Artifact).identity("review"))
                .unwrap()
                .is_empty(),
            "a rejected artifact must not enter the tuplespace"
        );
    }
}

#[cfg(test)]
mod groomer_ticket_update_tests {
    //! `handle_ticket_update`'s own shape check and audit event, exercised
    //! directly (bypassing the wire) the way `authorize_reasoned_tests` above
    //! exercises the auth gate. Both layers are meant to agree; these tests
    //! pin the handler's half — closing writes exactly one `ticket-groomed`
    //! event, and the handler itself refuses a malformed groomer request even
    //! though the auth gate would already have caught it first.
    use super::authorize_reasoned_tests::test_daemon_with_named_role;
    use super::*;
    use crate::read_only_roles::GROOMER_ROLE;
    use crate::tickets::NewTicket;

    fn ticket_update_req(caller: &str, params: Value) -> Request {
        Request {
            id: "1".into(),
            method: "ticket.update".into(),
            auth: String::new(),
            caller: caller.into(),
            client_version: None,
            params,
        }
    }

    #[tokio::test]
    async fn groomer_closure_writes_one_audit_event_and_closes_the_ticket() {
        let (_dir, daemon) = test_daemon_with_named_role("groomer-1", GROOMER_ROLE);
        let ticket = daemon
            .tickets
            .create(serde_json::from_value::<NewTicket>(json!({"title": "rework: TKT-x"})).unwrap())
            .await
            .unwrap();
        let id = ticket.identity.clone();

        let response = daemon
            .handle_ticket_update(ticket_update_req(
                "groomer-1",
                json!({"id": id, "status": "closed",
                    "reason": {"reason": "stale-rework", "evidence": "TKT-target done at abc123"}}),
            ))
            .await;
        assert!(response.error.is_none(), "{response:?}");

        let updated = daemon.tickets.get(&id).unwrap().unwrap();
        assert_eq!(updated.payload["status"], "closed");

        let events = daemon
            .space
            .scan(&Pattern::category(Category::Event).identity("ticket-groomed"))
            .unwrap();
        assert_eq!(events.len(), 1, "{events:?}");
        let payload = &events[0].payload;
        assert_eq!(payload["ticket"], id);
        assert_eq!(payload["prior_status"], "open");
        assert_eq!(payload["new_status"], "closed");
        assert_eq!(payload["reason"], "stale-rework");
        assert_eq!(payload["evidence"], "TKT-target done at abc123");
        assert_eq!(payload["groomer"], "groomer-1");
    }

    #[tokio::test]
    async fn groomer_request_without_reason_is_refused_by_the_handler_too() {
        let (_dir, daemon) = test_daemon_with_named_role("groomer-1", GROOMER_ROLE);
        let ticket = daemon
            .tickets
            .create(serde_json::from_value::<NewTicket>(json!({"title": "no evidence"})).unwrap())
            .await
            .unwrap();
        let id = ticket.identity.clone();

        let response = daemon
            .handle_ticket_update(ticket_update_req(
                "groomer-1",
                json!({"id": id, "status": "closed"}),
            ))
            .await;
        assert!(response.error.is_some());

        let untouched = daemon.tickets.get(&id).unwrap().unwrap();
        assert_eq!(untouched.payload["status"], "open");
        let events = daemon
            .space
            .scan(&Pattern::category(Category::Event).identity("ticket-groomed"))
            .unwrap();
        assert!(events.is_empty(), "{events:?}");
    }

    #[tokio::test]
    async fn non_groomer_closure_writes_no_audit_event() {
        let (_dir, daemon) = test_daemon_with_named_role("rat-1", "rat");
        let ticket = daemon
            .tickets
            .create(
                serde_json::from_value::<NewTicket>(json!({"title": "ordinary close"})).unwrap(),
            )
            .await
            .unwrap();
        let id = ticket.identity.clone();

        let response = daemon
            .handle_ticket_update(ticket_update_req(
                "operator",
                json!({"id": id, "status": "closed"}),
            ))
            .await;
        assert!(response.error.is_none(), "{response:?}");
        let events = daemon
            .space
            .scan(&Pattern::category(Category::Event).identity("ticket-groomed"))
            .unwrap();
        assert!(
            events.is_empty(),
            "an ordinary/operator closure must not be misattributed as a groomer audit: {events:?}"
        );
    }
}

#[cfg(test)]
mod factory_snapshot_resync_tests {
    use super::*;
    use crate::factory_events::FactoryEventFilter;
    use rk_core::config::Config;

    #[tokio::test]
    async fn factory_snapshot_reports_active_repo_resync() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let mut config = Config::default();
        config.sync.enabled = true;
        let daemon = Daemon::new(layout, &config).unwrap();
        let syncer = daemon.syncer.as_ref().unwrap().clone();
        syncer.set_running_for_test(true);

        let snapshot = daemon
            .factory_snapshot(&FactoryEventFilter::default())
            .await
            .unwrap();
        assert_eq!(snapshot["snapshot"]["repo_resync"]["required"], true);
    }
}

#[cfg(test)]
mod ticket_reopen_sweep_tests {
    //! B9 (strategic review, seam 5): an `in_progress` ticket whose assignee
    //! has had no live agent record for the stale window reopens to `open`,
    //! announced through the B2 recovery helper.
    use super::*;
    use crate::agents::{AgentRecord, AgentState};
    use crate::tickets::{NewTicket, TicketChanges};
    use rk_core::config::Config;
    use rk_harness::TokenUsage;
    use std::collections::HashMap;
    use std::path::PathBuf;

    /// Writes a fabricated `agents.json` directly (mirrors
    /// `authorize_reasoned_tests::test_daemon_with_role`) so the daemon's
    /// registry starts with a controllable agent state — no real spawn or
    /// process needed. `updated_at` is real "now", so the test controls
    /// staleness by injecting a future `now` into the sweep itself rather
    /// than by faking the record's timestamp.
    fn daemon_with_agent(name: &str, state: AgentState) -> (tempfile::TempDir, Daemon) {
        daemon_with_agent_task(name, state, "ticket-reopen-sweep-test")
    }

    /// Like [`daemon_with_agent`] but with a caller-chosen `task`, for the
    /// no-assignee fallback tests: a drain spawn keys `task` to the ticket id
    /// (`SpawnParams` in drain.rs), which is exactly what the sweep's
    /// fallback match uses in place of a missing `assignee`.
    fn daemon_with_agent_task(
        name: &str,
        state: AgentState,
        task: &str,
    ) -> (tempfile::TempDir, Daemon) {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        layout.ensure().unwrap();
        let now = chrono::Utc::now();
        let record = AgentRecord {
            name: name.into(),
            spawn: None,
            role: "rat".into(),
            coordination: None,
            harness: "fake".into(),
            permission_mode: None,
            model: None,
            repo_root: PathBuf::from("/tmp/repo"),
            repo_name: "repo".into(),
            task: Some(task.into()),
            branch: Some(format!("rat/{name}/task")),
            fork_point: None,
            worktree: Some(PathBuf::from(format!("/tmp/worktree/{name}"))),
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            review: None,
            coordinator: None,
            session_id: Some("test-session".into()),
            attach_target: None,
            pid: None,
            merge_commit: None,
            state,
            crashed: false,
            stderr_tail: None,
            result: None,
            progress: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: now,
            updated_at: now,
            archived_at: None,
        };
        let mut records = HashMap::new();
        records.insert(record.name.clone(), record);
        std::fs::write(
            layout.home().join("agents.json"),
            serde_json::to_vec(&records).unwrap(),
        )
        .unwrap();
        let daemon = Daemon::new(layout, &Config::default()).unwrap();
        (dir, daemon)
    }

    fn new_ticket() -> NewTicket {
        NewTicket {
            title: "orphan me".into(),
            body: None,
            scope: None,
            parent: None,
            priority: "normal".into(),
            labels: vec![],
            depends_on: vec![],
            created_by: None,
            coalesce_key: None,
        }
    }

    /// Create a ticket and drive it `in_progress` with the given assignee,
    /// mirroring `agent_cmds.rs`'s spawn path (status set, then assignee).
    async fn in_progress_ticket(daemon: &Daemon, assignee: Option<&str>) -> String {
        let ticket = daemon.tickets.create(new_ticket()).await.unwrap();
        daemon
            .tickets
            .update(
                &ticket.identity,
                TicketChanges {
                    status: Some("in_progress".into()),
                    assignee: assignee.map(String::from),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        ticket.identity
    }

    #[tokio::test]
    async fn a_live_owner_is_never_touched() {
        let (_dir, daemon) = daemon_with_agent("Live-1", AgentState::Running);
        let id = in_progress_ticket(&daemon, Some("Live-1")).await;

        // Even far beyond the stale window, a live owner must never be swept.
        let far_future = chrono::Utc::now() + chrono::Duration::hours(2);
        let reopened = daemon.ticket_reopen_sweep_at(far_future).await;

        assert_eq!(reopened, 0);
        let ticket = daemon.tickets.get(&id).unwrap().unwrap();
        assert_eq!(ticket.payload["status"], json!("in_progress"));
    }

    #[tokio::test]
    async fn a_dead_owner_reopens_after_the_stale_window_and_announces() {
        let (_dir, daemon) = daemon_with_agent("Dead-1", AgentState::Failed);
        let id = in_progress_ticket(&daemon, Some("Dead-1")).await;

        let past_window = chrono::Utc::now() + chrono::Duration::minutes(20);
        let reopened = daemon.ticket_reopen_sweep_at(past_window).await;

        assert_eq!(reopened, 1);
        let ticket = daemon.tickets.get(&id).unwrap().unwrap();
        assert_eq!(ticket.payload["status"], json!("open"));

        let events = daemon
            .space
            .scan(
                &Pattern::category(Category::Event)
                    .identity(crate::recovery::RECOVERY_ACTION_IDENTITY),
            )
            .unwrap();
        assert_eq!(
            events.len(),
            1,
            "expected exactly one announced recovery action"
        );
        assert_eq!(events[0].payload["action_kind"], json!("ticket-reopen"));
    }

    #[tokio::test]
    async fn a_dead_owner_within_the_grace_window_is_left_alone() {
        let (_dir, daemon) = daemon_with_agent("Dead-2", AgentState::Failed);
        let id = in_progress_ticket(&daemon, Some("Dead-2")).await;

        let within_window = chrono::Utc::now() + chrono::Duration::minutes(5);
        let reopened = daemon.ticket_reopen_sweep_at(within_window).await;

        assert_eq!(reopened, 0);
        let ticket = daemon.tickets.get(&id).unwrap().unwrap();
        assert_eq!(ticket.payload["status"], json!("in_progress"));
    }

    /// Covers the spawn-handoff race: `status` flips to `in_progress` before
    /// `assignee` is recorded (`agent_cmds.rs`). If the assignee never lands
    /// (the spawn itself failed before recording it), the ticket must not
    /// stay `in_progress` forever waiting for an owner that will never exist.
    #[tokio::test]
    async fn a_ticket_with_no_assignee_yet_still_eventually_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let daemon = Daemon::new(layout, &Config::default()).unwrap();
        let id = in_progress_ticket(&daemon, None).await;

        let past_window = chrono::Utc::now() + chrono::Duration::minutes(20);
        let reopened = daemon.ticket_reopen_sweep_at(past_window).await;

        assert_eq!(reopened, 1);
        let ticket = daemon.tickets.get(&id).unwrap().unwrap();
        assert_eq!(ticket.payload["status"], json!("open"));
    }

    /// B9-rework: a drain-claimed ticket has no `assignee` in the gap before
    /// the drain's post-spawn write lands (or on a daemon that predates that
    /// write), but its live rat's `task` still equals the ticket id. The
    /// sweep's fallback match must find that live owner and leave the ticket
    /// alone, even far past the stale window — otherwise every drain-owned
    /// live ticket gets reopened and double-dispatched on the first sweep.
    #[tokio::test]
    async fn a_null_assignee_ticket_with_a_live_task_match_is_never_touched() {
        // Placeholder task at creation time — the real ticket id does not
        // exist until after the ticket is created, so the fixture's task is
        // patched to match it below (mirrors a real drain: the agent is
        // spawned with `task = ticket.identity` from the start, but the test
        // fixture cannot know that id in advance).
        let (dir, daemon) = daemon_with_agent_task("Drain-Owned-1", AgentState::Running, "tbd");
        let id = in_progress_ticket(&daemon, None).await;

        let mut records: HashMap<String, AgentRecord> =
            serde_json::from_slice(&std::fs::read(dir.path().join("agents.json")).unwrap())
                .unwrap();
        for record in records.values_mut() {
            record.task = Some(id.clone());
        }
        std::fs::write(
            dir.path().join("agents.json"),
            serde_json::to_vec(&records).unwrap(),
        )
        .unwrap();
        let daemon = Daemon::new(Layout::at(dir.path()), &Config::default()).unwrap();

        let far_future = chrono::Utc::now() + chrono::Duration::hours(2);
        let reopened = daemon.ticket_reopen_sweep_at(far_future).await;

        assert_eq!(reopened, 0);
        let ticket = daemon.tickets.get(&id).unwrap().unwrap();
        assert_eq!(ticket.payload["status"], json!("in_progress"));
    }

    /// B9-rework: a null-assignee ticket with NO live agent whose task
    /// matches it (the genuinely orphaned case — e.g. the assignee's write
    /// never landed and its rat is gone) must still reopen after the stale
    /// window: the fallback match must not make every null-assignee ticket
    /// immortal, only ones a live rat can actually be traced to.
    #[tokio::test]
    async fn a_null_assignee_ticket_with_no_live_task_match_still_reopens() {
        let (_dir, daemon) = daemon_with_agent_task(
            "Unrelated-Live-1",
            AgentState::Running,
            "some-other-task-entirely",
        );
        let id = in_progress_ticket(&daemon, None).await;

        let past_window = chrono::Utc::now() + chrono::Duration::minutes(20);
        let reopened = daemon.ticket_reopen_sweep_at(past_window).await;

        assert_eq!(reopened, 1);
        let ticket = daemon.tickets.get(&id).unwrap().unwrap();
        assert_eq!(ticket.payload["status"], json!("open"));
    }

    /// A ticket that reaches `done` between the sweep's scan and its CAS
    /// write must never be clobbered back to `open` — the whole point of
    /// `reopen_if_in_progress` being a CAS rather than a blind set.
    #[tokio::test]
    async fn a_ticket_that_finished_racing_the_sweep_is_not_reopened() {
        let (_dir, daemon) = daemon_with_agent("Dead-3", AgentState::Failed);
        let id = in_progress_ticket(&daemon, Some("Dead-3")).await;

        // Simulate the rat's own `rk done` landing between the sweep's scan
        // and its write by moving the ticket to `done` directly, then
        // exercise the CAS primitive the sweep itself calls.
        daemon.tickets.set_status(&id, "done").await.unwrap();
        let performed = daemon.tickets.reopen_if_in_progress(&id).await.unwrap();

        assert!(!performed);
        let ticket = daemon.tickets.get(&id).unwrap().unwrap();
        assert_eq!(ticket.payload["status"], json!("done"));
    }

    /// TKT-01M0C663BZ86SMA2PVMFP5QJ8D: the O14 gap left by the async
    /// steward-review flow — `rk done` finds the branch not yet merged and
    /// refuses to close the ticket (the C3 delivery-mode gate), so the
    /// ticket sits `in_progress` with its rat gone terminal. The sweep must
    /// not treat that as ownerless-and-abandoned when a `landing_processed`
    /// event proves the branch landed after the rat went terminal —
    /// reopening it dispatches a duplicate rat onto already-delivered work.
    #[tokio::test]
    async fn a_ticket_whose_branch_already_landed_is_not_reopened() {
        let (_dir, daemon) = daemon_with_agent("Clover-Alike", AgentState::Completed);
        let id = in_progress_ticket(&daemon, Some("Clover-Alike")).await;

        let landed = Tuple::new(
            Category::Event,
            "some-repo".to_string(),
            "landing_processed".to_string(),
            "daemon".to_string(),
            json!({
                "branch": "rat/clover-alike/tkt",
                "target": "main",
                "head_sha": "ccad32f",
                "task": id,
                "outcome": "landed",
            }),
        )
        .with_lifecycle(Lifecycle::Furniture);
        daemon.space.out(landed).unwrap();

        let far_future = chrono::Utc::now() + chrono::Duration::hours(2);
        let reopened = daemon.ticket_reopen_sweep_at(far_future).await;

        assert_eq!(reopened, 0);
        let ticket = daemon.tickets.get(&id).unwrap().unwrap();
        assert_eq!(ticket.payload["status"], json!("in_progress"));
    }

    /// A ticket whose only `landing_processed` marker recorded a NON-landed
    /// terminal outcome (gate-held, rework-filed, escalated) must still
    /// reopen normally — landing-awareness is specifically about a landed
    /// branch, not about "this ticket's work key was ever processed".
    #[tokio::test]
    async fn a_ticket_with_a_non_landed_processing_marker_still_reopens() {
        let (_dir, daemon) = daemon_with_agent("GateHeld-1", AgentState::Failed);
        let id = in_progress_ticket(&daemon, Some("GateHeld-1")).await;

        let held = Tuple::new(
            Category::Event,
            "some-repo".to_string(),
            "landing_processed".to_string(),
            "daemon".to_string(),
            json!({
                "branch": "rat/gateheld-1/tkt",
                "target": "main",
                "head_sha": "deadbee",
                "task": id,
                "outcome": "gate-held",
            }),
        )
        .with_lifecycle(Lifecycle::Furniture);
        daemon.space.out(held).unwrap();

        let past_window = chrono::Utc::now() + chrono::Duration::minutes(20);
        let reopened = daemon.ticket_reopen_sweep_at(past_window).await;

        assert_eq!(reopened, 1);
        let ticket = daemon.tickets.get(&id).unwrap().unwrap();
        assert_eq!(ticket.payload["status"], json!("open"));
    }

    fn landing_queue_entry_tuple(task: &str, status: &str) -> Tuple {
        Tuple::new(
            Category::Event,
            "some-repo".to_string(),
            "landing_queue_entry".to_string(),
            "daemon".to_string(),
            json!({
                "repo_name": "some-repo",
                "repo_path": "/tmp/some-repo",
                "branch": "rat/queued-owner/tkt",
                "target": "main",
                "head_sha": "abc1234",
                "diff_class": "trivial",
                "task": task,
                "seq": 1,
                "status": status,
                "rev": 0,
            }),
        )
        .with_lifecycle(Lifecycle::Furniture)
    }

    /// Probes O8/O17 (TKT-01M0CTC4DYBRX6P5X2NPEZF0EZ): a ticket whose rat
    /// went non-live (paused, killed, orphaned) while its branch is still
    /// sitting in the daemon-native landing pipeline — `Queued`,
    /// `RunningGates`, or `AwaitingReview` — must not be reopened just
    /// because the stale window elapsed. Unlike `landing_processed`, this
    /// marker's mere PRESENCE (not any particular `status` value) is what
    /// matters: the entry only disappears on a terminal outcome.
    #[tokio::test]
    async fn a_ticket_whose_branch_is_queued_for_landing_is_not_reopened() {
        for status in ["queued", "running_gates", "awaiting_review"] {
            let (_dir, daemon) = daemon_with_agent("Queued-Owner", AgentState::Completed);
            let id = in_progress_ticket(&daemon, Some("Queued-Owner")).await;
            daemon
                .space
                .out(landing_queue_entry_tuple(&id, status))
                .unwrap();

            let far_future = chrono::Utc::now() + chrono::Duration::hours(2);
            let reopened = daemon.ticket_reopen_sweep_at(far_future).await;

            assert_eq!(reopened, 0, "status={status}");
            let ticket = daemon.tickets.get(&id).unwrap().unwrap();
            assert_eq!(
                ticket.payload["status"],
                json!("in_progress"),
                "status={status}"
            );
        }
    }

    /// A `landing_queue_entry` for a DIFFERENT task must not immunize this
    /// ticket — only its own branch's queue membership matters. This is also
    /// the "truly orphaned ticket still reopens" half of the acceptance
    /// criteria: presence of unrelated landing-pipeline traffic must not
    /// mask genuine abandonment.
    #[tokio::test]
    async fn a_ticket_with_an_unrelated_queue_entry_still_reopens() {
        let (_dir, daemon) = daemon_with_agent("Orphan-1", AgentState::Failed);
        let id = in_progress_ticket(&daemon, Some("Orphan-1")).await;
        daemon
            .space
            .out(landing_queue_entry_tuple(
                "some-other-ticket-entirely",
                "queued",
            ))
            .unwrap();

        let past_window = chrono::Utc::now() + chrono::Duration::minutes(20);
        let reopened = daemon.ticket_reopen_sweep_at(past_window).await;

        assert_eq!(reopened, 1);
        let ticket = daemon.tickets.get(&id).unwrap().unwrap();
        assert_eq!(ticket.payload["status"], json!("open"));
    }
}
