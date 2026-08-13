//! `rk` — the rat-kingdom CLI.

mod agent_cmds;
mod factory_cmds;
mod observe;
mod product_to_code_cmds;
mod repo_cmds;
mod space_cmds;
mod ticket_cmds;
mod top;
mod workflow_cmds;

use agent_cmds::print_pruned_instance;
use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use rk_core::config::Config;
use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;

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
    /// One ranked triage list of everything awaiting a human, each row carrying
    /// the exact `rk` command that resolves it.
    Inbox,
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
    /// Show one agent's status.
    Status(agent_cmds::NameArg),
    /// Print an agent's transcript (assistant text, tool calls, retries); --follow to stream.
    Log(agent_cmds::LogArgs),
    /// Send mid-session guidance to a running agent.
    Steer(agent_cmds::SteerArgs),
    /// Gracefully interrupt a running agent.
    Interrupt(agent_cmds::NameArg),
    /// Dismiss an agent: stop it, merge its branch, clean up.
    Dismiss(agent_cmds::DismissArgs),
    /// Undo a bad auto-merge: revert a dismissed agent's landed merge commit
    /// and reopen its ticket.
    Revert(agent_cmds::RevertArgs),
    /// Relaunch a failed/orphaned agent in its preserved worktree.
    Respawn(agent_cmds::NameArg),
    /// Attach interactively to an attach-mode rat's herdr pane.
    Attach(agent_cmds::NameArg),
    /// Per-agent and fleet token/cost rollup.
    Cost {
        /// Show the hierarchical fleet/repo budget rollup vs configured caps
        /// instead of the per-agent breakdown.
        #[arg(long)]
        fleet: bool,
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
        /// Role to render: operator | onboarding | rat | reviewer | foreman | verifier | onboarder. Overrides RK_ROLE.
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
        /// Legacy fallback for repositories without `.rk/repo.cue`: `direct`
        /// or `pr`. Cannot be combined with a versioned repository policy.
        #[arg(long, value_parser = ["direct", "pr"])]
        merge_mode: Option<String>,
        /// Legacy fallback remote for repositories without `.rk/repo.cue`.
        /// Cannot be combined with a versioned repository policy.
        #[arg(long)]
        remote: Option<String>,
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
        /// registration, workflow_activation, trigger_activation, or
        /// schedule_activation.
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
        /// register_repository, activate_workflow, activate_trigger, or
        /// activate_schedule.
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

const PRIME_ROLES: [&str; 7] = [
    "operator",
    "onboarding",
    "rat",
    "reviewer",
    "foreman",
    "verifier",
    "onboarder",
];

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
                        println!(
                            "running: pid {} · castle {} · {} tuples · uptime {}s · v{}",
                            status["pid"],
                            status["castle"].as_str().unwrap_or("?"),
                            status["tuples"],
                            status["uptime_secs"],
                            status["version"].as_str().unwrap_or("?")
                        );
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
        Command::Inbox => agent_cmds::inbox(&layout, cli.json).await?,
        Command::Top { interval, all } => top::top(&layout, interval, all).await?,
        Command::Digest { since, llm } => observe::digest(&layout, &since, llm, cli.json).await?,
        Command::Status(args) => agent_cmds::status(&layout, args, cli.json).await?,
        Command::Log(args) => agent_cmds::log(&layout, args, cli.json).await?,
        Command::Steer(args) => agent_cmds::steer(&layout, args, cli.json).await?,
        Command::Interrupt(args) => agent_cmds::interrupt(&layout, args, cli.json).await?,
        Command::Dismiss(args) => agent_cmds::dismiss(&layout, args, cli.json).await?,
        Command::Revert(args) => agent_cmds::revert(&layout, args, cli.json).await?,
        Command::Respawn(args) => agent_cmds::respawn(&layout, args, cli.json).await?,
        Command::Attach(args) => agent_cmds::attach(&layout, args).await?,
        Command::Cost { fleet } => {
            if fleet {
                agent_cmds::cost_fleet(&layout, cli.json).await?
            } else {
                agent_cmds::cost(&layout, cli.json).await?
            }
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
            let code = product_to_code_cmds::run(command, cli.json)?;
            if code != 0 {
                std::process::exit(code);
            }
        }
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
                            println!(
                                "{:14} {:12} {:10} step {}/{}",
                                i["id"].as_str().unwrap_or("?"),
                                i["workflow"].as_str().unwrap_or("?"),
                                status,
                                i["current_step"],
                                i["total_steps"],
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
                    println!("{}", result["instance"]);
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
            RepoCommand::Add {
                path,
                name,
                merge_mode,
                remote,
            } => repo_cmds::add(&layout, path, name, merge_mode, remote, cli.json).await?,
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
        },
    }

    Ok(())
}
