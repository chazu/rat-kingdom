//! `rk` — the rat-kingdom CLI.

mod agent_cmds;
mod attention_cmds;
mod critical_path;
mod factory_cmds;
mod factory_dashboard;
mod factory_skill;
mod ingest_cmds;
mod king_cmds;
mod observe;
mod product_to_code_cmds;
mod reconcile_cmds;
mod reconcile_repair_cmds;
mod repo_cmds;
mod space_cmds;
mod ticket_cmds;
mod top;
mod trigger_cmds;
mod work_cmds;
mod workflow_cmds;

use agent_cmds::print_pruned_instance;
use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use rk_core::config::Config;
use rk_core::id::SpawnId;
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};

#[derive(Parser)]
#[command(
    name = "rk",
    version,
    about = "rat-kingdom: orchestrate fleets of AI coding rats"
)]
struct Cli {
    /// Emit machine-readable JSON on stdout where applicable.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Verify the CLI and daemon are working (auto-starts the daemon).
    Ping,
    /// Manage the background daemon.
    Daemon {
        #[command(subcommand)]
        command: DaemonCommand,
    },
    /// Write a tuple to the space.
    Out(space_cmds::OutArgs),
    /// Destructively take a matching tuple (blocks until match or timeout).
    In(space_cmds::ReadArgs),
    /// Read a matching tuple without consuming it (blocks until match or timeout).
    Rd(space_cmds::ReadArgs),
    /// List all matching tuples (non-blocking; --hot/--top rank strongest-first).
    Scan(space_cmds::HotScanArgs),
    /// Stream tuples live as they are written.
    Watch(space_cmds::ScanArgs),
    /// Read or follow bounded coordinator attention and middle-rat rollups.
    Monitor(MonitorArgs),
    /// Signal task completion (sugar; env-autofilled).
    Done(space_cmds::DoneArgs),
    /// Report something blocking progress (sugar; env-autofilled).
    Obstacle(space_cmds::TextArgs),
    /// Publish a bounded semantic checkpoint for the current rat.
    Progress(agent_cmds::ProgressArgs),
    /// Ask the room for help (sugar; env-autofilled).
    Need(space_cmds::TextArgs),
    /// Advisory claim marking an area you're editing (evaporates on a TTL; sugar; env-autofilled).
    Claim(space_cmds::ClaimArgs),
    /// Propose a norm for the fleet; peers endorse it, quorum promotes it to a convention (sugar).
    Suggest(space_cmds::SuggestArgs),
    /// Endorse a suggestion by id (idempotent; sugar; env-autofilled).
    Endorse(space_cmds::EndorseArgs),
    /// Close a losing ballot by id (proposer or operator only; votes stay on the record).
    Withdraw(space_cmds::WithdrawArgs),
    /// Vote on the quality of an injected fact.
    Fact {
        #[command(subcommand)]
        command: FactCommand,
    },
    /// Spawn a rat to work on a task in an isolated worktree.
    Spawn(agent_cmds::SpawnArgs),
    /// List agents (live fleet by default; --all/--archived include archived).
    List(agent_cmds::ListArgs),
    /// Archive settled terminal agent records (completed/failed/dismissed) AND
    /// settled workflow instances out of the default views. Nothing is deleted
    /// — cost/usage/lineage survive and stay readable via `rk list --archived`
    /// and `rk workflow list --archived`.
    Prune(agent_cmds::PruneArgs),
    /// Restore an archived agent record to the live registry.
    Unarchive(agent_cmds::NameArg),
    /// Broad diagnostic triage, including rows that may require an open-ended
    /// decision or an external forge/git action.
    Inbox {
        #[command(subcommand)]
        command: Option<InboxCommand>,
    },
    /// Current operational work in one small view: build parity, live rats,
    /// ready tickets, and actionable attention. Historical detail stays in
    /// `rk digest`, `rk inbox`, `rk reconcile`, and `rk top`.
    Work(work_cmds::WorkArgs),
    /// Cross-ledger convergence report for one repository: read-only
    /// comparison of the ticket, agent, landing, and git views, surfacing
    /// contradictions between them (delivered-but-open tickets, terminal
    /// assignees still owning active work, conflict-held landing work,
    /// tracker claims git disagrees with).
    Reconcile(reconcile_cmds::ReportArgs),
    /// Dry-run (default) or apply mechanical repair for the two convergence
    /// violations durable evidence alone proves and fixes: delivered-but-open
    /// tickets and stale ownership (a terminal assignee still on record for
    /// open work). Every other violation stays report-only. Operator-only.
    ReconcileRepair(reconcile_repair_cmds::RepairArgs),
    /// Durable orchestrator lease over one repository's attention queue
    /// (TKT-01M0E8PN9C41BWECGNW0990R3J): acquire/renew, surviving disconnect,
    /// replacement, or a daemon restart.
    Lease {
        #[command(subcommand)]
        command: attention_cmds::LeaseCommand,
    },
    /// Consume and resolve `rk reconcile`'s violations as a resumable,
    /// authority-classified attention queue.
    Attention {
        #[command(subcommand)]
        command: attention_cmds::AttentionCommand,
    },
    /// Operate the dedicated Herdr-backed LLM operator delegate.
    King {
        #[command(subcommand)]
        command: king_cmds::KingCommand,
    },
    /// Live fleet dashboard: agents, workflows, budget, inbox (q to quit).
    Top {
        /// Refresh interval in seconds.
        #[arg(long, default_value_t = 2)]
        interval: u64,
        /// Include archived agent records in the agents pane.
        #[arg(long)]
        all: bool,
    },
    /// What the fleet did in the last interval — an async catch-up over the
    /// event feed, workflows, friction, spend, and inbox.
    Digest {
        /// Window to report on, e.g. 30m, 2h, 1d (bare number = minutes).
        #[arg(long, default_value = "1h")]
        since: String,
        /// Summarize the report into prose with a one-shot `claude -p` call
        /// (falls back to the raw digest if the binary is unavailable).
        #[arg(long)]
        llm: bool,
    },
    /// Show one agent's status, or (given a TKT- id) a ticket's task-to-main
    /// critical path: queue wait, execution duration, attempts, terminal
    /// reason, target/candidate, proof reuse, rework amplification, and
    /// human- vs LLM-gated time per phase.
    Status(agent_cmds::NameArg),
    /// Print an agent's transcript (assistant text, tool calls, retries); --follow to stream.
    Log(agent_cmds::LogArgs),
    /// Send mid-session guidance to a running agent.
    Steer(agent_cmds::SteerArgs),
    /// Gracefully interrupt a running agent.
    Interrupt(agent_cmds::NameArg),
    /// Dismiss an agent: stop it, preserve its branch, clean up its worktree.
    Dismiss(agent_cmds::DismissArgs),
    /// Submit a branch to the gated landing queue (or explicitly force an audited bypass).
    Land(agent_cmds::LandArgs),
    /// Dispatch one fresh review attempt for a candidate whose prior review
    /// was fenced at the landing pipeline's wait ceiling.
    ReenqueueReview(agent_cmds::ReenqueueReviewArgs),
    /// Explicitly cancel a candidate's currently active review attempt.
    CancelReview(agent_cmds::CancelReviewArgs),
    /// Undo a bad landing: revert an agent's recorded merge commit and reopen
    /// its ticket.
    Revert(agent_cmds::RevertArgs),
    /// Relaunch a failed/orphaned agent in its preserved worktree.
    Respawn(agent_cmds::NameArg),
    /// Resume an agent parked by a post-commit transport outage, optionally
    /// under a different harness. Surfaced by `rk inbox` as a
    /// `recovery-action` row.
    #[command(name = "continue-recovery")]
    ContinueRecovery(agent_cmds::RecoveryActionArgs),
    /// Decline to continue a parked post-commit recovery: leave the generation
    /// terminal instead of resuming it.
    #[command(name = "abandon-recovery")]
    AbandonRecovery(agent_cmds::RecoveryActionArgs),
    /// Attach interactively to an attach-mode rat's herdr pane.
    Attach(agent_cmds::NameArg),
    /// Per-agent and fleet token/cost rollup.
    Cost {
        /// Show the hierarchical fleet/repo budget rollup vs configured caps
        /// instead of the per-agent breakdown.
        #[arg(long)]
        fleet: bool,
    },
    /// Report primary-vs-shadow reviewer agreement over recorded comparisons.
    ShadowReview {
        /// Restrict the report to one repository.
        #[arg(long)]
        repo: Option<String>,
        /// Window to report on, e.g. 30m, 2h, 7d (bare number = minutes).
        #[arg(long, default_value = "7d")]
        since: String,
    },
    /// Multiplayer sync via git notes.
    Sync {
        #[command(subcommand)]
        command: SyncCommand,
    },
    /// List castles seen in the shared tuplespace.
    Peers,
    /// Print role instructions for the system. Defaults to the `operator` role
    /// unless RK_ROLE (set on spawned rats) indicates otherwise.
    Prime {
        /// Role to render: operator | onboarding | rat | reviewer | foreman | verifier | onboarder | diagnostician | groomer. Overrides RK_ROLE.
        #[arg(long)]
        role: Option<String>,
    },
    /// Prime this operator session to guide repository onboarding with the user.
    ///
    /// Exact sugar for `rk prime --role onboarding`; prints context only and
    /// does not launch an agent or mutate repository state.
    Onboard,
    /// Register and inspect repositories the system knows about.
    Repo {
        #[command(subcommand)]
        command: RepoCommand,
    },
    /// Create, inspect, and update tickets (durable work items).
    Ticket {
        #[command(subcommand)]
        command: TicketCommand,
    },
    /// Ingest canonical SDLC feedback events and read current facts.
    Ingest {
        #[command(subcommand)]
        command: ingest_cmds::IngestCommand,
    },
    /// Typed factory proposal, approval, and execution commands.
    Factory {
        #[command(subcommand)]
        command: factory_cmds::FactoryCommand,
    },
    /// Validate and render local product-to-code artifacts.
    ProductToCode {
        #[command(subcommand)]
        command: product_to_code_cmds::ProductToCodeCommand,
    },
    /// Run and inspect CUE-defined workflows.
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
    },
    /// Run a named repo check (`.rk/checks.cue`, default "verify") through
    /// the daemon's `verify.run` RPC — the managed alternative to
    /// self-invoking a full suite directly, so the run goes through the same
    /// bounded per-repo verification admission queue a landing gate or
    /// workflow `run` step gets (TKT-01M0HNESEECWWFQF8X6VH1XSJ6). Exits with
    /// the check's own exit code.
    Verify {
        /// Repo name to verify (defaults to $RK_REPO — the repo a spawned
        /// rat is working in). An agent caller may only verify its own repo;
        /// the operator may verify any registered repo.
        #[arg(long)]
        repo: Option<String>,
        /// Named check to run. Defaults to "verify".
        #[arg(long)]
        check: Option<String>,
    },
    /// Install and audit deployed `#Trigger` definitions (global and
    /// repo-local), so a source-of-truth check can catch a stale trigger left
    /// behind by a manual copy — e.g. after a swap like the daemon-native
    /// landing pipeline cutover (docs/proposals/daemon-native-landing-pipeline.md §6).
    Trigger {
        #[command(subcommand)]
        command: TriggerCommand,
    },
    /// Approve a workflow instance parked at an approval gate (lets it merge).
    Approve(WorkflowDecisionArgs),
    /// Reject a workflow instance parked at an approval gate (holds it unmerged).
    Reject(WorkflowDecisionArgs),
}

#[derive(Subcommand)]
enum FactCommand {
    /// Upvote, downvote, or retract your vote on a fact tuple.
    Vote(space_cmds::FactVoteArgs),
}

#[derive(clap::Args)]
struct WorkflowDecisionArgs {
    /// Workflow instance id (from `rk workflow list`).
    instance: String,
    /// Who is making the decision (defaults to $RK_AGENT, $USER, or "operator").
    #[arg(long)]
    by: Option<String>,
    /// Optional note recorded with the decision.
    #[arg(long)]
    reason: Option<String>,
}

#[derive(Subcommand)]
enum RepoCommand {
    /// Register a repository and activate its current `.rk/repo.cue`, if present.
    Add {
        /// Path to the repository.
        path: String,
        /// Name to register it under (defaults to the directory name).
        #[arg(long)]
        name: Option<String>,
    },
    /// List registered repositories.
    List,
    /// Show one repository, its active delivery policy, and its open tickets.
    Show {
        /// Registered repo name.
        name: String,
    },
    /// Read-only repository onboarding assessment.
    Onboard {
        #[command(subcommand)]
        command: RepoOnboardCommand,
    },
}

#[derive(Args)]
struct NamedCheckProposalArgs {
    /// Named check whose executable contract is carried by a `.rk/checks.cue` proposal.
    #[arg(long)]
    check_name: Option<String>,
    /// Exact repository-owned runner command.
    #[arg(long, requires = "check_name")]
    check_command: Option<String>,
    /// Exact working directory relative to the onboarding worktree.
    #[arg(long, requires = "check_name")]
    check_cwd: Option<String>,
    /// Exact expected exit status.
    #[arg(long, requires = "check_name")]
    check_expect_exit: Option<i64>,
    /// Exact wall-clock timeout.
    #[arg(long, requires = "check_name")]
    check_timeout: Option<String>,
    /// Environment contract: inherit or strip_rk_spawn.
    #[arg(long, requires = "check_name")]
    check_environment_policy: Option<String>,
    /// Repository-owned toolchain/runner description.
    #[arg(long, requires = "check_name")]
    check_toolchain: Option<String>,
}

#[derive(Subcommand)]
enum RepoOnboardCommand {
    /// Start or idempotently reuse a durable onboarding session.
    Start {
        /// Repository path or registered name.
        target: String,
        /// Harness kind; defaults to the daemon configuration.
        #[arg(long)]
        harness: Option<String>,
        /// Harness-native model identifier.
        #[arg(long)]
        model: Option<String>,
        /// Launch in a human-attachable herdr pane.
        #[arg(long)]
        attach: bool,
    },
    /// Inspect a path or registered name without launching an agent or writing state.
    Inspect {
        /// Repository path or registered name.
        target: String,
    },
    /// Journal one immutable, content-bound onboarding proposal.
    Propose {
        /// Stable onboarding session id.
        session: String,
        /// Proposal kind: repo_file (including `.rk/repo.cue`), castle_config,
        /// registration, workflow_activation, trigger_activation,
        /// schedule_activation, or hook_activation.
        #[arg(long)]
        kind: String,
        /// Human-readable proposal title.
        #[arg(long)]
        title: String,
        /// Evidence supporting the proposal; repeat for multiple entries.
        #[arg(long, required = true)]
        evidence: Vec<String>,
        /// Exact repository path or castle setting affected.
        #[arg(long)]
        target: String,
        /// Action: write_repo_file, change_castle_config,
        /// register_repository, activate_workflow, activate_trigger,
        /// activate_schedule, or activate_hook.
        #[arg(long)]
        action: String,
        /// Exact reviewable unified diff or configuration delta.
        #[arg(long)]
        diff: String,
        /// Risk: low, medium, or high.
        #[arg(long)]
        risk: String,
        /// Verification step; repeat for multiple entries.
        #[arg(long, required = true)]
        verification: Vec<String>,
        #[command(flatten)]
        named_check: Box<NamedCheckProposalArgs>,
    },
    /// Approve one exact proposal digest.
    Approve {
        /// Stable onboarding session id.
        session: String,
        /// Stable proposal id.
        proposal: String,
        /// Canonical digest shown by status/report.
        #[arg(long)]
        digest: String,
        /// Optional durable decision rationale.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Decline one exact proposal digest.
    Decline {
        /// Stable onboarding session id.
        session: String,
        /// Stable proposal id.
        proposal: String,
        /// Canonical digest shown by status/report.
        #[arg(long)]
        digest: String,
        /// Optional durable decision rationale.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Apply and validate one approved repository-file or automation proposal;
    /// execute its named check when the proposal carries one.
    Apply {
        /// Stable onboarding session id.
        session: String,
        /// Stable proposal id.
        proposal: String,
        /// Canonical digest shown by status/report.
        #[arg(long)]
        digest: String,
    },
    /// Explicitly activate one validated repository policy, workflow, trigger,
    /// or schedule by landing its exact approved commit into the registered
    /// checkout.
    Activate {
        /// Stable onboarding session id.
        session: String,
        /// Stable proposal id.
        proposal: String,
        /// Canonical digest shown by status/report.
        #[arg(long)]
        digest: String,
    },
    /// Refuse activation while retaining the validated onboarding branch and
    /// durable report.
    DeclineActivation {
        /// Stable onboarding session id.
        session: String,
        /// Stable proposal id.
        proposal: String,
        /// Canonical digest shown by status/report.
        #[arg(long)]
        digest: String,
        /// Optional durable decision rationale.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Remove a terminal session's clean worktree while retaining its branch
    /// and durable report.
    Cleanup {
        /// Stable onboarding session id.
        session: String,
    },
    /// Resume an orphaned or failed onboarding session.
    Resume {
        /// Stable onboarding session id.
        session: String,
        /// Resume in a human-attachable herdr pane.
        #[arg(long)]
        attach: bool,
    },
    /// Show durable onboarding session state.
    Status {
        /// Stable onboarding session id.
        session: String,
    },
    /// Show the durable onboarding assessment and terminal result.
    Report {
        /// Stable onboarding session id.
        session: String,
    },
}

#[derive(Subcommand)]
enum TicketCommand {
    /// File a new ticket.
    New(ticket_cmds::NewArgs),
    /// List tickets (filter by --repo, --status, --parent).
    List(ticket_cmds::ListArgs),
    /// Show one ticket and its sub-tickets.
    Show {
        /// Ticket id (for example, TKT-01J...).
        id: String,
    },
    /// Update a ticket's status or fields.
    Update(ticket_cmds::UpdateArgs),
    /// List open tickets whose dependencies are all satisfied (actionable now).
    Ready {
        /// Only tickets in this repo scope.
        #[arg(long)]
        repo: Option<String>,
    },
    /// Declare that one ticket depends on (is blocked by) another.
    Dep {
        /// The blocked ticket.
        id: String,
        /// The ticket it depends on.
        dep: String,
    },
    /// Remove a dependency edge.
    Undep {
        /// The blocked ticket.
        id: String,
        /// The dependency to remove.
        dep: String,
    },
    /// Explicitly move a `done` or `closed` ticket back to the backlog.
    /// Operator/foreman-authorized only — an agent caller is refused, and
    /// the move is announced as a `ticket_reopened` event. Ordinary `rk
    /// ticket update --status ...` can never do this: the state machine
    /// refuses `done -> in_progress` and any backwards move out of `closed`.
    Reopen(ticket_cmds::ReopenArgs),
    /// Record externally landed work with content-bound git evidence.
    Deliver(ticket_cmds::DeliverArgs),
}

#[derive(Subcommand)]
enum InboxCommand {
    /// Durably acknowledge an escalation so the B2 re-notify sweep stops
    /// pushing it. Idempotent — acking an already-acked id is a no-op.
    Ack {
        /// The tuple id shown in the row's `rk inbox ack <id>` action.
        id: String,
    },
}

#[derive(Subcommand)]
enum WorkflowCommand {
    /// Run a workflow by name (or .cue path).
    Run {
        name: String,
        /// Repository the workflow operates on.
        #[arg(long, default_value = ".")]
        repo: String,
        /// Workflow parameters as key=value (repeatable). Values are strings,
        /// coerced to each param's declared type (int/number/bool/list).
        #[arg(long = "param")]
        params: Vec<String>,
        /// Path to a JSON file (object of key→value) supplying params in bulk.
        /// Its values keep their JSON types; individual --param flags override
        /// matching keys.
        #[arg(long = "param-file")]
        param_file: Option<String>,
        /// Stable coordinator-session id that owns this workflow for monitoring.
        #[arg(long)]
        coordinator: Option<String>,
    },
    /// List workflow instances.
    List {
        /// Show pruned instances instead of live ones.
        #[arg(long)]
        archived: bool,
        /// Show live and pruned instances together (the full run history).
        #[arg(long)]
        all: bool,
    },
    /// Archive settled instances out of `rk workflow list` and `rk inbox`.
    /// Nothing is deleted: a pruned run still reads via `rk workflow status`
    /// and `rk workflow list --archived`, and `rk workflow unarchive` puts it
    /// back.
    Prune {
        /// Prune exactly these instance ids (from `rk inbox`). An unknown or
        /// still-running id is an error, not a silent no-op.
        ids: Vec<String>,
        /// Without ids: prune instances that settled before this point — a
        /// duration (30m, 24h, 7d, 2w) or a date (2026-07-24 / RFC3339).
        #[arg(long, default_value = "7d")]
        before: String,
        /// Without ids: prune every settled instance, regardless of age.
        #[arg(long)]
        all: bool,
        /// List what would be pruned without touching the store.
        #[arg(long)]
        dry_run: bool,
    },
    /// Restore one pruned instance to the live list.
    Unarchive { id: String },
    /// Show one instance.
    Status { id: String },
    /// Render an instance's step trace: every step labelled and marked
    /// done/current/pending, plus where the instance is parked.
    Timeline { id: String },
    /// Replay and follow one workflow's coordinator state until it completes
    /// or fails; --json emits an NDJSON stream of snapshot/event records.
    Watch {
        /// Workflow instance id (from `rk workflow run`).
        id: String,
        /// Resume event delivery after this durable coordinator journal cursor.
        #[arg(long)]
        after: Option<String>,
    },
    /// List available workflow definitions.
    Defs {
        #[arg(long, default_value = ".")]
        repo: String,
    },
    /// Install a .cue definition into the managed global or repo-local directory.
    Install {
        source: String,
        /// Install into this repository's `.rk/workflows/`; omit for global install.
        #[arg(long)]
        repo: Option<String>,
    },
    /// Compare deployed definitions with the repository's workflow sources.
    Drift {
        #[arg(long, default_value = ".")]
        repo: String,
        /// Override the source directory (defaults to `<repo>/examples/workflows`).
        #[arg(long)]
        source_dir: Option<String>,
    },
}

#[derive(Subcommand)]
enum TriggerCommand {
    /// Install a .cue definition into the managed global triggers directory,
    /// or a repository's `.rk/triggers.cue` (the reactor only ever reads
    /// that one fixed repo-local filename, so `--repo` always targets it
    /// regardless of the source file's own name).
    Install {
        source: String,
        /// Install into this repository's `.rk/triggers.cue`; omit for
        /// global install.
        #[arg(long)]
        repo: Option<String>,
    },
    /// Compare deployed trigger definitions with the repository's trigger
    /// sources, flagging a deployed copy that no longer matches any shipped
    /// source (stale/unmanaged) or matches under a different name (drifted).
    Drift {
        #[arg(long, default_value = ".")]
        repo: String,
        /// Override the source directory (defaults to `<repo>/examples`).
        #[arg(long)]
        source_dir: Option<String>,
    },
    /// Flag active triggers (global directory plus this repo's
    /// `.rk/triggers.cue`) whose `match` predicates are identical — two such
    /// triggers double-dispatch on every matching tuple, even when each
    /// deployed file is individually "clean" per `drift`.
    Conflicts {
        #[arg(long, default_value = ".")]
        repo: String,
    },
}

#[derive(Args)]
struct MonitorArgs {
    /// Stable coordinator-session owner to monitor.
    #[arg(long)]
    coordinator: Option<String>,
    /// Explicit middle-rat subtree to drill into.
    #[arg(long)]
    subtree: Option<String>,
    /// Repository diagnostic scope (not the coordinator default).
    #[arg(long)]
    repo: Option<String>,
    /// Resume durable coordinator delivery after this cursor.
    #[arg(long)]
    after: Option<u64>,
    /// Show all descendants instead of middle-rat rollups.
    #[arg(long)]
    all: bool,
    /// Follow live events after the initial snapshot.
    #[arg(long)]
    follow: bool,
    /// Perform one bounded read (the default; explicit for scripts).
    #[arg(long)]
    once: bool,
}

#[derive(Subcommand)]
enum SyncCommand {
    /// Run one sync cycle immediately.
    Now,
}

#[derive(Subcommand)]
enum DaemonCommand {
    /// Run the daemon in the foreground.
    Run,
    /// Show daemon status.
    Status,
    /// Stop the running daemon.
    Stop,
    /// One-command drain -> restart -> reconcile: stop admitting new
    /// dispatch, wait for live rats to finish (parking any still running at
    /// the deadline), restart the daemon binary onto whatever `rk` now
    /// resolves to, then respawn the rats this rollover parked.
    Rollover {
        /// Seconds to wait for live rats to finish naturally before parking
        /// them. Parked rats respawn after the restart either way, so 0
        /// parks immediately.
        #[arg(long, default_value_t = 120)]
        wait_secs: u64,
    },
}

/// Drive `rk daemon rollover` end to end: pause dispatch, wait out
/// `wait_secs` for live rats to finish naturally, stop the daemon (parking
/// whoever is still running — `on_daemon_started` marks them `Orphaned` on
/// the next boot, worktree/branch/session preserved), bring a fresh daemon
/// process up onto whatever `rk` now resolves to, then respawn exactly the
/// rats this run parked.
///
/// Deliberately narrower than the periodic self-healing sweep
/// (`respawn_enabled`): it only ever touches agents it captured as live
/// right before calling `stop`, and only if the restart actually orphaned
/// them (one that finished during the wait is `Completed`, not `Orphaned`,
/// and must not be resumed) — so it never disturbs an unrelated agent an
/// operator left `Orphaned` from an earlier incident, and it works
/// regardless of the `respawn_enabled` policy flag.
async fn daemon_rollover(layout: &Layout, wait_secs: u64, as_json: bool) -> Result<()> {
    let mut client = Client::connect(layout)
        .await
        .map_err(|_| anyhow::anyhow!("daemon is not running — nothing to roll over"))?;

    let mut live = match rollover_drain(&mut client, wait_secs, as_json).await {
        Ok(live) => live,
        Err(e) => {
            // Don't leave a live daemon stuck refusing dispatch over a
            // failure that happened before we ever got to `stop`.
            let _ = client.call("daemon.resume_dispatch", json!({})).await;
            return Err(e);
        }
    };

    if !live.is_empty() && !as_json {
        println!(
            "rollover: {} rat(s) still running after {wait_secs}s — parking them, they will respawn",
            live.len()
        );
    }

    client.call("stop", json!({})).await?;
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        if Client::connect(layout).await.is_err() {
            break;
        }
    }

    // Bring the new daemon up onto whatever binary `rk` now resolves to.
    // `connect_or_spawn` refuses to auto-start from inside an agent session
    // (RK_AGENT set) — this command is operator-only (see `authorize_reasoned`)
    // so that refusal, if hit, is itself the right answer.
    let mut client = Client::connect_or_spawn(layout).await?;

    // Reconcile: respawn only the rats parked above, and only the ones the
    // restart actually orphaned.
    let mut respawned = Vec::new();
    let mut failed = Vec::new();
    if !live.is_empty() {
        let agents = client.call("agent.list", json!({})).await?;
        let recoverable: std::collections::HashSet<SpawnId> = agents["agents"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter(|r| r["state"].as_str() == Some("orphaned"))
                    .filter_map(|r| r["spawn"].as_str()?.parse().ok())
                    .collect()
            })
            .unwrap_or_default();
        live.retain(|generation| recoverable.contains(&generation.spawn));
        for generation in &live {
            match client
                .call(
                    "agent.respawn",
                    json!({"name": generation.name, "spawn": generation.spawn}),
                )
                .await
            {
                Ok(_) => respawned.push(generation.name.clone()),
                Err(e) => failed.push((generation.name.clone(), e.to_string())),
            }
        }
    }

    if as_json {
        println!(
            "{}",
            json!({
                "rolled_over": true,
                "respawned": respawned,
                "respawn_failed": failed
                    .iter()
                    .map(|(n, e)| json!({"name": n, "error": e}))
                    .collect::<Vec<_>>(),
            })
        );
    } else {
        println!("rollover complete: new daemon up");
        if !respawned.is_empty() {
            println!("  respawned: {}", respawned.join(", "));
        }
        for (name, err) in &failed {
            println!("  respawn failed for {name}: {err}");
        }
    }
    Ok(())
}

/// The drain phase of [`daemon_rollover`]: pause dispatch, then poll live
/// agents down to zero or `wait_secs`, whichever comes first. Returns
/// whoever is still live at the end — the set about to be parked.
#[derive(Clone)]
struct RolloverGeneration {
    name: String,
    spawn: SpawnId,
}

fn rollover_generation(value: &Value) -> Option<RolloverGeneration> {
    Some(RolloverGeneration {
        name: value["name"].as_str()?.to_string(),
        spawn: value["spawn"].as_str()?.parse().ok()?,
    })
}

async fn rollover_drain(
    client: &mut Client,
    wait_secs: u64,
    as_json: bool,
) -> Result<Vec<RolloverGeneration>> {
    let pause = client.call("daemon.pause_dispatch", json!({})).await?;
    let mut live: Vec<RolloverGeneration> = pause["live_agents"]
        .as_array()
        .map(|a| a.iter().filter_map(rollover_generation).collect())
        .unwrap_or_default();

    if !as_json {
        println!(
            "rollover: dispatch paused, {} live rat(s) to drain",
            live.len()
        );
    }

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(wait_secs);
    while !live.is_empty() {
        let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
            break;
        };
        tokio::time::sleep(remaining.min(std::time::Duration::from_secs(2))).await;
        let agents = client.call("agent.list", json!({})).await?;
        let still_live: std::collections::HashSet<SpawnId> = agents["agents"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter(|r| matches!(r["state"].as_str(), Some("running" | "spawning")))
                    .filter_map(|r| r["spawn"].as_str()?.parse().ok())
                    .collect()
            })
            .unwrap_or_default();
        live.retain(|generation| still_live.contains(&generation.spawn));
        if !as_json {
            println!("rollover: waiting on {} live rat(s)...", live.len());
        }
    }

    Ok(live)
}

/// Emit a human approval decision for a workflow instance parked at an
/// approval gate. The daemon writes the `workflow_approval` event the blocked
/// gate is waiting on.
async fn decide(
    layout: &Layout,
    args: WorkflowDecisionArgs,
    approved: bool,
    as_json: bool,
) -> Result<()> {
    let by = args
        .by
        .or_else(|| std::env::var("RK_AGENT").ok())
        .or_else(|| std::env::var("USER").ok())
        .unwrap_or_else(|| "operator".to_string());
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "workflow.approve",
            json!({
                "instance": args.instance,
                "approved": approved,
                "by": by,
                "reason": args.reason,
            }),
        )
        .await?;
    if as_json {
        println!("{result}");
    } else {
        let verb = if approved { "approved" } else { "rejected" };
        println!("{verb} {}", args.instance);
    }
    Ok(())
}

const PRIME_ROLES: [&str; 9] = [
    "operator",
    "onboarding",
    "rat",
    "reviewer",
    "foreman",
    "verifier",
    "onboarder",
    "diagnostician",
    "groomer",
];

fn review_context_from_env() -> Option<rk_core::review::ReviewContext> {
    use rk_core::review::{
        ReviewContext, REVIEW_ATTEMPT_ENV, REVIEW_BRANCH_ENV, REVIEW_HEAD_ENV, REVIEW_TARGET_ENV,
        REVIEW_TASK_ENV,
    };
    Some(ReviewContext {
        branch: std::env::var(REVIEW_BRANCH_ENV).ok()?,
        head_sha: std::env::var(REVIEW_HEAD_ENV).ok()?,
        target: std::env::var(REVIEW_TARGET_ENV).ok()?,
        task: std::env::var(REVIEW_TASK_ENV).ok()?,
        attempt: std::env::var(REVIEW_ATTEMPT_ENV).ok()?,
    })
}

fn print_prime(role: String, json_output: bool) -> Result<()> {
    if !PRIME_ROLES.contains(&role.as_str()) {
        anyhow::bail!(
            "unknown role '{role}' (expected: {})",
            PRIME_ROLES.join(", ")
        );
    }
    let ctx = rk_core::prime::PrimeContext {
        agent: std::env::var("RK_AGENT").unwrap_or_default(),
        repo: std::env::var("RK_REPO").unwrap_or_default(),
        task: std::env::var("RK_TASK").ok(),
        branch: std::env::var("RK_BRANCH").ok(),
        base: std::env::var("RK_BASE").ok(),
        review: review_context_from_env(),
        parent: std::env::var("RK_PARENT").ok(),
        facts: Vec::new(),
        // `rk prime` inspects the template shape; live conventions are scanned
        // and injected by the supervisor at spawn time.
        conventions: Vec::new(),
        verification_checks: Vec::new(),
        harness_terminal_completion: false,
    };
    let text = rk_core::prime::render(&role, &ctx);
    if json_output {
        println!("{}", json!({ "role": role, "prime": text }));
    } else {
        print!("{text}");
    }
    Ok(())
}

fn init_tracing(config: &Config) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_env("RK_LOG")
        .unwrap_or_else(|_| EnvFilter::new(config.log.filter.clone()));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let layout = Layout::discover()?;
    let config = Config::load(&layout.config_file())?;
    init_tracing(&config);

    // Presentation-only alias resolver (TKT-124): maps this castle's own actor id
    // to its friendly `castle_name` in operator-facing output. Reads the actor
    // side-effect-free (never mints a key just to print a name); absent a key or
    // alias, every author renders verbatim.
    let castle_display = rk_core::identity::CastleDisplay::new(
        rk_core::identity::CastleIdentity::actor_at(&layout.castle_key_path()).unwrap_or_default(),
        config.castle_name.clone(),
    );

    match cli.command {
        Command::Ping => {
            let mut client = Client::connect_or_spawn(&layout).await?;
            let result = client.call("ping", json!({})).await?;
            if cli.json {
                println!("{}", json!({ "ping": result }));
            } else {
                println!("{}", result.as_str().unwrap_or("?"));
            }
        }
        Command::Daemon { command } => match command {
            DaemonCommand::Run => {
                Daemon::new(layout, &config)?.run().await?;
            }
            DaemonCommand::Status => match Client::connect(&layout).await {
                Ok(mut client) => {
                    let status = client.call("status", json!({})).await?;
                    if cli.json {
                        println!("{status}");
                    } else {
                        // Prefer the build version: `version` alone cannot tell
                        // a daemon started before a merge from one started
                        // after it, which is the question this line is asked.
                        // A daemon too old to report one falls back.
                        let build = status["build_version"]
                            .as_str()
                            .or_else(|| status["version"].as_str())
                            .unwrap_or("?");
                        println!(
                            "running: pid {} · castle {} · {} tuples · uptime {}s · v{build}",
                            status["pid"],
                            status["castle"].as_str().unwrap_or("?"),
                            status["tuples"],
                            status["uptime_secs"],
                        );
                        // Landing-queue depth and oldest-entry age per
                        // (repo, target) — a slow queue and a wedged one
                        // are otherwise indistinguishable (probe O18).
                        for q in status["landing_queue"].as_array().into_iter().flatten() {
                            let oldest = q["oldest_age_secs"].as_i64().unwrap_or(0).max(0) as u64;
                            println!(
                                "  landing {} → {}: {} queued, oldest ({}) waiting {}",
                                q["repo"].as_str().unwrap_or("?"),
                                q["target"].as_str().unwrap_or("?"),
                                q["depth"],
                                q["oldest_branch"].as_str().unwrap_or("?"),
                                top::human_secs(oldest),
                            );
                        }
                        // Per-repo implementation/verification/review lane
                        // capacity (TKT-01M0P2KM83Y4MD5QYETR3JCKF2) — only
                        // repos with an explicit override or a live agent are
                        // reported at all (see `Supervisor::capacity_summary`),
                        // so this stays silent for a fleet that hasn't opted in.
                        for (repo, lanes) in status["capacity"].as_object().into_iter().flatten() {
                            for lane in ["implementation", "review", "verification"] {
                                let Some(entry) = lanes.get(lane) else {
                                    continue;
                                };
                                let limit = entry["limit"].as_u64().unwrap_or(0);
                                if limit == 0 {
                                    continue;
                                }
                                let occupied = entry["occupied"]
                                    .as_u64()
                                    .or_else(|| entry["in_flight"].as_u64())
                                    .unwrap_or(0);
                                let reason = entry["waiting_reason"]
                                    .as_str()
                                    .map(|r| format!(" ({r})"))
                                    .unwrap_or_default();
                                let queue = match (
                                    entry["waiting_count"].as_u64(),
                                    entry["oldest_wait_secs"].as_i64(),
                                ) {
                                    (Some(n), Some(age)) if n > 0 => format!(
                                        ", {n} waiting (oldest {})",
                                        top::human_secs(age.max(0) as u64)
                                    ),
                                    _ => String::new(),
                                };
                                println!(
                                    "  capacity {repo} {lane}: {occupied}/{limit}{reason}{queue}"
                                );
                            }
                        }
                    }
                }
                Err(_) => {
                    if cli.json {
                        println!("{}", json!({ "running": false }));
                    } else {
                        println!("not running");
                    }
                    std::process::exit(1);
                }
            },
            DaemonCommand::Stop => {
                let mut client = Client::connect(&layout).await?;
                client.call("stop", json!({})).await?;
                // Wait for the old daemon to fully release the socket so an
                // immediate restart cannot race its shutdown cleanup.
                for _ in 0..50 {
                    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
                    if Client::connect(&layout).await.is_err() {
                        break;
                    }
                }
                if cli.json {
                    println!("{}", json!({ "stopped": true }));
                } else {
                    println!("stopped");
                }
            }
            DaemonCommand::Rollover { wait_secs } => {
                daemon_rollover(&layout, wait_secs, cli.json).await?;
            }
        },
        Command::Out(args) => space_cmds::out(&layout, args, cli.json).await?,
        Command::In(args) => {
            space_cmds::blocking_read(&layout, args, true, cli.json, &castle_display).await?
        }
        Command::Rd(args) => {
            space_cmds::blocking_read(&layout, args, false, cli.json, &castle_display).await?
        }
        Command::Scan(args) => space_cmds::scan(&layout, args, cli.json, &castle_display).await?,
        Command::Watch(args) => space_cmds::watch(&layout, args).await?,
        Command::Monitor(args) => {
            let mut params = serde_json::Map::new();
            if let Some(coordinator) = args.coordinator {
                params.insert("coordinator".into(), json!(coordinator));
                params.insert("scope".into(), json!("owned"));
            }
            if let Some(subtree) = args.subtree {
                params.insert("subtree".into(), json!(subtree));
                params.insert("scope".into(), json!("subtree"));
            }
            if let Some(repo) = args.repo {
                params.insert("repo".into(), json!(repo));
                if !params.contains_key("scope") {
                    params.insert("scope".into(), json!("repo"));
                }
            }
            if let Some(after) = args.after {
                params.insert("after".into(), json!(after));
            }
            params.insert(
                "depth".into(),
                json!(if args.all { "all" } else { "middle" }),
            );
            observe::monitor(&layout, json!(params), args.follow && !args.once, cli.json).await?;
        }
        Command::Done(args) => space_cmds::done(&layout, args, cli.json).await?,
        Command::Obstacle(args) => space_cmds::report(&layout, args, "obstacle", cli.json).await?,
        Command::Progress(args) => agent_cmds::progress(&layout, args, cli.json).await?,
        Command::Need(args) => space_cmds::report(&layout, args, "need", cli.json).await?,
        Command::Claim(args) => space_cmds::claim(&layout, args, cli.json).await?,
        Command::Suggest(args) => space_cmds::suggest(&layout, args, cli.json).await?,
        Command::Endorse(args) => space_cmds::endorse(&layout, args, cli.json).await?,
        Command::Withdraw(args) => space_cmds::withdraw(&layout, args, cli.json).await?,
        Command::Fact {
            command: FactCommand::Vote(args),
        } => space_cmds::fact_vote(&layout, args, cli.json).await?,
        Command::Spawn(args) => agent_cmds::spawn(&layout, args, cli.json).await?,
        Command::List(args) => agent_cmds::list(&layout, args, cli.json).await?,
        Command::Prune(args) => agent_cmds::prune(&layout, args, cli.json).await?,
        Command::Unarchive(args) => agent_cmds::unarchive(&layout, args, cli.json).await?,
        Command::Inbox { command } => match command {
            None => agent_cmds::inbox(&layout, cli.json).await?,
            Some(InboxCommand::Ack { id }) => agent_cmds::inbox_ack(&layout, id, cli.json).await?,
        },
        Command::Work(args) => work_cmds::run(&layout, args, cli.json).await?,
        Command::Reconcile(args) => reconcile_cmds::report(&layout, args, cli.json).await?,
        Command::ReconcileRepair(args) => {
            reconcile_repair_cmds::repair(&layout, args, cli.json).await?
        }
        Command::Lease { command } => match command {
            attention_cmds::LeaseCommand::Acquire(args) => {
                attention_cmds::lease_acquire(&layout, args, cli.json).await?
            }
            attention_cmds::LeaseCommand::Renew(args) => {
                attention_cmds::lease_renew(&layout, args, cli.json).await?
            }
        },
        Command::Attention { command } => match command {
            attention_cmds::AttentionCommand::Next(args) => {
                attention_cmds::attention_next(&layout, args, cli.json).await?
            }
            attention_cmds::AttentionCommand::Decide(args) => {
                attention_cmds::attention_decide(&layout, *args, cli.json).await?
            }
            attention_cmds::AttentionCommand::Invalidate(args) => {
                attention_cmds::attention_invalidate(&layout, args, cli.json).await?
            }
        },
        Command::King { command } => king_cmds::run(&layout, command, cli.json).await?,
        Command::Top { interval, all } => top::top(&layout, interval, all).await?,
        Command::Digest { since, llm } => observe::digest(&layout, &since, llm, cli.json).await?,
        Command::Status(args) => agent_cmds::status(&layout, args, cli.json).await?,
        Command::Log(args) => agent_cmds::log(&layout, args, cli.json).await?,
        Command::Steer(args) => agent_cmds::steer(&layout, args, cli.json).await?,
        Command::Interrupt(args) => agent_cmds::interrupt(&layout, args, cli.json).await?,
        Command::Dismiss(args) => agent_cmds::dismiss(&layout, args, cli.json).await?,
        Command::Land(args) => agent_cmds::land(&layout, args, cli.json).await?,
        Command::ReenqueueReview(args) => {
            agent_cmds::reenqueue_review(&layout, args, cli.json).await?
        }
        Command::CancelReview(args) => agent_cmds::cancel_review(&layout, args, cli.json).await?,
        Command::Revert(args) => agent_cmds::revert(&layout, args, cli.json).await?,
        Command::Respawn(args) => agent_cmds::respawn(&layout, args, cli.json).await?,
        Command::ContinueRecovery(args) => {
            agent_cmds::continue_recovery(&layout, args, cli.json).await?
        }
        Command::AbandonRecovery(args) => {
            agent_cmds::abandon_recovery(&layout, args, cli.json).await?
        }
        Command::Attach(args) => agent_cmds::attach(&layout, args).await?,
        Command::Cost { fleet } => {
            if fleet {
                agent_cmds::cost_fleet(&layout, cli.json).await?
            } else {
                agent_cmds::cost(&layout, cli.json).await?
            }
        }
        Command::ShadowReview { repo, since } => {
            observe::shadow_review_report(&layout, repo.as_deref(), &since, cli.json).await?
        }
        Command::Sync { command } => match command {
            SyncCommand::Now => {
                let mut client = Client::connect_or_spawn(&layout).await?;
                let stats = client.call("sync.now", json!({})).await?;
                if cli.json {
                    println!("{stats}");
                } else {
                    println!(
                        "synced: {} exported · {} imported · {} castles · pushed: {}",
                        stats["exported"], stats["imported"], stats["actors_seen"], stats["pushed"]
                    );
                }
            }
        },
        Command::Factory { command } => factory_cmds::run(&layout, command, cli.json).await?,
        Command::ProductToCode { command } => {
            let code = product_to_code_cmds::run(&layout, command, cli.json).await?;
            if code != 0 {
                std::process::exit(code);
            }
        }
        Command::Ingest { command } => ingest_cmds::run(&layout, command, cli.json).await?,
        Command::Workflow { command } => {
            let mut client = Client::connect_or_spawn(&layout).await?;
            match command {
                WorkflowCommand::Run {
                    name,
                    repo,
                    params,
                    param_file,
                    coordinator,
                } => {
                    let repo = std::fs::canonicalize(&repo)?;
                    let mut map = serde_json::Map::new();
                    // --param-file seeds the map with natively-typed JSON...
                    if let Some(path) = param_file {
                        let text = std::fs::read_to_string(&path)
                            .map_err(|e| anyhow::anyhow!("read --param-file {path}: {e}"))?;
                        match serde_json::from_str(&text).map_err(|e| {
                            anyhow::anyhow!("--param-file {path} is not valid JSON: {e}")
                        })? {
                            serde_json::Value::Object(obj) => map.extend(obj),
                            _ => {
                                return Err(anyhow::anyhow!(
                                    "--param-file {path} must contain a JSON object of key→value"
                                ));
                            }
                        }
                    }
                    // ...then individual --param flags override, always as strings
                    // (coerced to the declared type server-side at load time).
                    for pair in params {
                        let (k, v) = pair.split_once('=').ok_or_else(|| {
                            anyhow::anyhow!("--param must be key=value, got: {pair}")
                        })?;
                        map.insert(k.to_string(), json!(v));
                    }
                    let result = client
                        .call(
                            "workflow.run",
                            json!({"name": name, "repo": repo.to_string_lossy(), "params": map, "coordinator": coordinator}),
                        )
                        .await?;
                    if cli.json {
                        println!("{}", result["instance"]);
                    } else {
                        println!(
                            "started {} ({} steps)",
                            result["instance"]["id"].as_str().unwrap_or("?"),
                            result["instance"]["total_steps"]
                        );
                    }
                }
                WorkflowCommand::List { archived, all } => {
                    let result = client
                        .call("workflow.list", json!({"archived": archived, "all": all}))
                        .await?;
                    if cli.json {
                        println!("{}", result["instances"]);
                    } else {
                        let instances = result["instances"].as_array().cloned().unwrap_or_default();
                        for i in &instances {
                            // A pruned row is only reachable via --archived/--all,
                            // so mark it rather than letting it read as live.
                            let status = match i["archived_at"].as_str() {
                                Some(_) => format!("{}*", i["status"].as_str().unwrap_or("?")),
                                None => i["status"].as_str().unwrap_or("?").to_string(),
                            };
                            let target_suffix = workflow_target_suffix(i);
                            println!(
                                "{:14} {:12} {:10} step {}/{}{}",
                                i["id"].as_str().unwrap_or("?"),
                                i["workflow"].as_str().unwrap_or("?"),
                                status,
                                i["current_step"],
                                i["total_steps"],
                                target_suffix,
                            );
                        }
                        if instances.iter().any(|i| !i["archived_at"].is_null()) {
                            println!("(* pruned — `rk workflow unarchive <id>` restores one)");
                        }
                    }
                }
                WorkflowCommand::Prune {
                    ids,
                    before,
                    all,
                    dry_run,
                } => {
                    let window = if !ids.is_empty() {
                        format!("{} named", ids.len())
                    } else if all {
                        "all settled".to_string()
                    } else {
                        format!("settled before {before}")
                    };
                    let result = client
                        .call(
                            "workflow.archive",
                            json!({
                                "ids": ids,
                                "before": before,
                                "all": all,
                                "dry_run": dry_run,
                            }),
                        )
                        .await?;
                    if cli.json {
                        println!("{result}");
                    } else {
                        let instances = result["instances"].as_array().cloned().unwrap_or_default();
                        if instances.is_empty() {
                            println!("nothing to prune ({window})");
                        } else {
                            let verb = if dry_run { "would prune" } else { "pruned" };
                            println!("{verb} {} instance(s) ({window}):", instances.len());
                            for i in &instances {
                                print_pruned_instance(i);
                            }
                            if dry_run {
                                println!("(dry run — re-run without --dry-run to prune)");
                            }
                        }
                    }
                }
                WorkflowCommand::Unarchive { id } => {
                    let result = client
                        .call("workflow.unarchive", json!({"name": id}))
                        .await?;
                    if cli.json {
                        println!("{}", result["instance"]);
                    } else {
                        println!("unarchived {id}");
                    }
                }
                WorkflowCommand::Status { id } => {
                    let result = client.call("workflow.status", json!({"name": id})).await?;
                    let instance = &result["instance"];
                    println!("{instance}");
                    if !cli.json {
                        if let Some(target) = workflow_land_target(instance) {
                            println!("land target: {target}");
                        }
                    }
                }
                WorkflowCommand::Timeline { id } => {
                    let result = client
                        .call("workflow.timeline", json!({"name": id}))
                        .await?;
                    if cli.json {
                        println!("{result}");
                    } else {
                        observe::print_timeline(&result);
                    }
                }
                WorkflowCommand::Watch { id, after } => {
                    observe::watch_workflow(&layout, &id, after.as_deref(), cli.json).await?;
                }
                WorkflowCommand::Defs { repo } => {
                    let repo = std::fs::canonicalize(&repo)?;
                    let result = client
                        .call(
                            "workflow.definitions",
                            json!({"repo": repo.to_string_lossy()}),
                        )
                        .await?;
                    if cli.json {
                        println!("{}", result["definitions"]);
                    } else {
                        for d in result["definitions"]
                            .as_array()
                            .cloned()
                            .unwrap_or_default()
                        {
                            println!("{}", d.as_str().unwrap_or("?"));
                        }
                    }
                }
                WorkflowCommand::Install { source, repo } => {
                    let target = workflow_cmds::install(&layout, &source, repo.as_deref())?;
                    if cli.json {
                        println!("{}", json!({"installed": target}));
                    } else {
                        println!("installed {}", target.display());
                    }
                }
                WorkflowCommand::Drift { repo, source_dir } => {
                    let report = workflow_cmds::drift(&layout, &repo, source_dir.as_deref())?;
                    if cli.json {
                        println!("{}", serde_json::to_string(&report)?);
                    } else if report.rows.is_empty() {
                        println!("workflow drift: no deployed definitions found");
                    } else {
                        for row in &report.rows {
                            println!("{:<10} {}", row.status, row.target);
                        }
                        if report.drifted == 0 {
                            println!("workflow drift: clean");
                        } else {
                            println!(
                                "workflow drift: {} definition(s) need attention",
                                report.drifted
                            );
                        }
                    }
                    if report.drifted > 0 {
                        anyhow::bail!("workflow definitions are not synchronized");
                    }
                }
            }
        }
        Command::Verify { repo, check } => {
            let repo = repo
                .or_else(|| std::env::var("RK_REPO").ok())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!("--repo is required (or run inside a rat with $RK_REPO set)")
                })?;
            let mut client = Client::connect_or_spawn(&layout).await?;
            let mut params = serde_json::Map::new();
            params.insert("repo".into(), json!(repo));
            if let Some(check) = check {
                params.insert("check".into(), json!(check));
            }
            let result = client.call("verify.run", Value::Object(params)).await?;
            let exit = result["exit"].as_i64().unwrap_or(1);
            if cli.json {
                println!("{result}");
            } else {
                if let Some(stdout) = result["stdout"].as_str().filter(|s| !s.is_empty()) {
                    println!("{stdout}");
                }
                if let Some(stderr) = result["stderr"].as_str().filter(|s| !s.is_empty()) {
                    eprintln!("{stderr}");
                }
                println!(
                    "verify: {} (exit {exit})",
                    result["verdict"].as_str().unwrap_or("?"),
                );
            }
            if exit != 0 {
                std::process::exit(exit.clamp(1, 255) as i32);
            }
        }
        Command::Trigger { command } => match command {
            TriggerCommand::Install { source, repo } => {
                let target = trigger_cmds::install(&layout, &source, repo.as_deref())?;
                if cli.json {
                    println!("{}", json!({"installed": target}));
                } else {
                    println!("installed {}", target.display());
                }
            }
            TriggerCommand::Drift { repo, source_dir } => {
                let report = trigger_cmds::drift(&layout, &repo, source_dir.as_deref())?;
                if cli.json {
                    println!("{}", serde_json::to_string(&report)?);
                } else if report.rows.is_empty() {
                    println!("trigger drift: no deployed definitions found");
                } else {
                    for row in &report.rows {
                        println!("{:<10} {}", row.status, row.target);
                    }
                    if report.drifted == 0 {
                        println!("trigger drift: clean");
                    } else {
                        println!(
                            "trigger drift: {} definition(s) need attention",
                            report.drifted
                        );
                    }
                }
                if report.drifted > 0 {
                    anyhow::bail!("trigger definitions are not synchronized");
                }
            }
            TriggerCommand::Conflicts { repo } => {
                let report = trigger_cmds::conflicts(&layout, &repo)?;
                if cli.json {
                    println!("{}", serde_json::to_string(&report)?);
                } else if report.groups.is_empty() {
                    println!("trigger conflicts: none");
                } else {
                    for group in &report.groups {
                        println!("{}", serde_json::to_string(&group.matcher)?);
                        for trigger in &group.triggers {
                            println!("  {:<40} {}", trigger.name, trigger.file);
                        }
                    }
                    println!(
                        "trigger conflicts: {} predicate(s) shared by more than one trigger",
                        report.groups.len()
                    );
                }
                if !report.groups.is_empty() {
                    anyhow::bail!("multiple triggers share an identical match predicate");
                }
            }
        },
        Command::Approve(args) => decide(&layout, args, true, cli.json).await?,
        Command::Reject(args) => decide(&layout, args, false, cli.json).await?,
        Command::Peers => {
            let mut client = Client::connect_or_spawn(&layout).await?;
            let result = client.call("sync.peers", json!({})).await?;
            if cli.json {
                println!("{}", result["peers"]);
            } else {
                for p in result["peers"].as_array().cloned().unwrap_or_default() {
                    println!("{}", p.as_str().unwrap_or("?"));
                }
            }
        }
        Command::Prime { role } => {
            // Explicit --role wins; otherwise a spawned rat's RK_ROLE; otherwise
            // the operator (a session driving the fleet from outside).
            let role = role
                .or_else(|| std::env::var("RK_ROLE").ok())
                .unwrap_or_else(|| "operator".to_string());
            print_prime(role, cli.json)?;
        }
        Command::Onboard => print_prime("onboarding".into(), cli.json)?,
        Command::Repo { command } => match command {
            RepoCommand::Add { path, name } => {
                repo_cmds::add(&layout, path, name, cli.json).await?
            }
            RepoCommand::List => repo_cmds::list(&layout, cli.json).await?,
            RepoCommand::Show { name } => repo_cmds::show(&layout, name, cli.json).await?,
            RepoCommand::Onboard { command } => match command {
                RepoOnboardCommand::Start {
                    target,
                    harness,
                    model,
                    attach,
                } => {
                    repo_cmds::onboard_start(&layout, target, harness, model, attach, cli.json)
                        .await?
                }
                RepoOnboardCommand::Inspect { target } => {
                    repo_cmds::onboard_inspect(&layout, target, cli.json).await?
                }
                RepoOnboardCommand::Propose {
                    session,
                    kind,
                    title,
                    evidence,
                    target,
                    action,
                    diff,
                    risk,
                    verification,
                    named_check,
                } => {
                    let NamedCheckProposalArgs {
                        check_name,
                        check_command,
                        check_cwd,
                        check_expect_exit,
                        check_timeout,
                        check_environment_policy,
                        check_toolchain,
                    } = *named_check;
                    repo_cmds::onboard_propose(
                        &layout,
                        session,
                        kind,
                        title,
                        evidence,
                        target,
                        action,
                        diff,
                        risk,
                        verification,
                        check_name,
                        check_command,
                        check_cwd,
                        check_expect_exit,
                        check_timeout,
                        check_environment_policy,
                        check_toolchain,
                        cli.json,
                    )
                    .await?
                }
                RepoOnboardCommand::Approve {
                    session,
                    proposal,
                    digest,
                    reason,
                } => {
                    repo_cmds::onboard_decide(
                        &layout, session, proposal, digest, reason, true, cli.json,
                    )
                    .await?
                }
                RepoOnboardCommand::Decline {
                    session,
                    proposal,
                    digest,
                    reason,
                } => {
                    repo_cmds::onboard_decide(
                        &layout, session, proposal, digest, reason, false, cli.json,
                    )
                    .await?
                }
                RepoOnboardCommand::Apply {
                    session,
                    proposal,
                    digest,
                } => repo_cmds::onboard_apply(&layout, session, proposal, digest, cli.json).await?,
                RepoOnboardCommand::Activate {
                    session,
                    proposal,
                    digest,
                } => {
                    repo_cmds::onboard_activate(&layout, session, proposal, digest, cli.json)
                        .await?
                }
                RepoOnboardCommand::DeclineActivation {
                    session,
                    proposal,
                    digest,
                    reason,
                } => {
                    repo_cmds::onboard_decline_activation(
                        &layout, session, proposal, digest, reason, cli.json,
                    )
                    .await?
                }
                RepoOnboardCommand::Cleanup { session } => {
                    repo_cmds::onboard_cleanup(&layout, session, cli.json).await?
                }
                RepoOnboardCommand::Resume { session, attach } => {
                    repo_cmds::onboard_resume(&layout, session, attach, cli.json).await?
                }
                RepoOnboardCommand::Status { session } => {
                    repo_cmds::onboard_status(&layout, session, cli.json).await?
                }
                RepoOnboardCommand::Report { session } => {
                    repo_cmds::onboard_report(&layout, session, cli.json).await?
                }
            },
        },
        Command::Ticket { command } => match command {
            TicketCommand::New(args) => ticket_cmds::new(&layout, args, cli.json).await?,
            TicketCommand::List(args) => ticket_cmds::list(&layout, args, cli.json).await?,
            TicketCommand::Show { id } => ticket_cmds::show(&layout, id, cli.json).await?,
            TicketCommand::Update(args) => ticket_cmds::update(&layout, args, cli.json).await?,
            TicketCommand::Ready { repo } => ticket_cmds::ready(&layout, repo, cli.json).await?,
            TicketCommand::Dep { id, dep } => {
                ticket_cmds::dep(&layout, id, dep, false, cli.json).await?
            }
            TicketCommand::Undep { id, dep } => {
                ticket_cmds::dep(&layout, id, dep, true, cli.json).await?
            }
            TicketCommand::Reopen(args) => ticket_cmds::reopen(&layout, args, cli.json).await?,
            TicketCommand::Deliver(args) => ticket_cmds::deliver(&layout, args, cli.json).await?,
        },
    }

    Ok(())
}

/// Rendered suffix for `rk workflow list`: a `target` param other than "main"
/// means this instance is landing somewhere non-default — most commonly a
/// steward inheriting a chained/rework rat's own `--base` (docs/reactor.md,
/// "Land target inheritance"). Flag it so it isn't mistaken for a run headed
/// to main.
fn workflow_target_suffix(instance: &serde_json::Value) -> String {
    workflow_land_target(instance)
        .map(|target| format!(" target={target}"))
        .unwrap_or_default()
}

/// The effective non-default land target, if this workflow instance carries
/// one. This is shared by `workflow list` and `workflow status` so the two
/// human-facing views cannot drift on which target is worth calling out.
fn workflow_land_target(instance: &serde_json::Value) -> Option<&str> {
    match instance["params"]["target"].as_str() {
        Some(target) if !target.is_empty() && target != "main" => Some(target),
        _ => None,
    }
}

#[cfg(test)]
mod workflow_display_tests {
    use super::workflow_target_suffix;
    use serde_json::json;

    /// The non-main land target must be VISIBLE in `rk workflow list` — the
    /// steward trigger inherits a chained rat's base as its land target, and
    /// without this suffix such an instance reads identically to one landing
    /// on main (TKT-01M01DM0VXPD7VV09GX02YMEA1).
    #[test]
    fn non_main_target_renders_and_main_stays_silent() {
        let inherited = json!({"params": {"target": "rat/camembert-4/tkt-9"}});
        assert_eq!(
            workflow_target_suffix(&inherited),
            " target=rat/camembert-4/tkt-9"
        );
        assert_eq!(
            super::workflow_land_target(&inherited),
            Some("rat/camembert-4/tkt-9")
        );
        for silent in [
            json!({"params": {"target": "main"}}),
            json!({"params": {"target": ""}}),
            json!({"params": {}}),
        ] {
            assert_eq!(workflow_target_suffix(&silent), "");
            assert_eq!(super::workflow_land_target(&silent), None);
        }
    }
}
