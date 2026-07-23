//! `rk` — the rat-kingdom CLI.

mod agent_cmds;
mod repo_cmds;
mod space_cmds;
mod ticket_cmds;

use anyhow::Result;
use clap::{Parser, Subcommand};
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
    /// List all matching tuples (non-blocking).
    Scan(space_cmds::ScanArgs),
    /// Stream tuples live as they are written.
    Watch(space_cmds::ScanArgs),
    /// Signal task completion (sugar; env-autofilled).
    Done(space_cmds::DoneArgs),
    /// Report something blocking progress (sugar; env-autofilled).
    Obstacle(space_cmds::TextArgs),
    /// Ask the room for help (sugar; env-autofilled).
    Need(space_cmds::TextArgs),
    /// Advisory claim marking an area you're editing (evaporates on a TTL; sugar; env-autofilled).
    Claim(space_cmds::ClaimArgs),
    /// Propose a norm for the fleet; peers endorse it, quorum promotes it to a convention (sugar).
    Suggest(space_cmds::SuggestArgs),
    /// Endorse a suggestion by id (idempotent; sugar; env-autofilled).
    Endorse(space_cmds::EndorseArgs),
    /// Spawn a rat to work on a task in an isolated worktree.
    Spawn(agent_cmds::SpawnArgs),
    /// List all agents.
    List,
    /// One ranked triage list of everything awaiting a human, each row carrying
    /// the exact `rk` command that resolves it.
    Inbox,
    /// Show one agent's status.
    Status(agent_cmds::NameArg),
    /// Send mid-session guidance to a running agent.
    Steer(agent_cmds::SteerArgs),
    /// Gracefully interrupt a running agent.
    Interrupt(agent_cmds::NameArg),
    /// Dismiss an agent: stop it, merge its branch, clean up.
    Dismiss(agent_cmds::DismissArgs),
    /// Relaunch a failed/orphaned agent in its preserved worktree.
    Respawn(agent_cmds::NameArg),
    /// Attach interactively to an attach-mode rat's herdr pane.
    Attach(agent_cmds::NameArg),
    /// Per-agent and fleet token/cost rollup.
    Cost,
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
        /// Role to render: operator | rat | reviewer. Overrides RK_ROLE.
        #[arg(long)]
        role: Option<String>,
    },
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
    /// Register a repository (path is canonicalized; name defaults to its directory).
    Add {
        /// Path to the repository.
        path: String,
        /// Name to register it under (defaults to the directory name).
        #[arg(long)]
        name: Option<String>,
    },
    /// List registered repositories.
    List,
    /// Show one repository and its open tickets.
    Show {
        /// Registered repo name.
        name: String,
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
        /// Ticket id (e.g. TKT-12).
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
        /// Workflow parameters as key=value (repeatable).
        #[arg(long = "param")]
        params: Vec<String>,
    },
    /// List workflow instances.
    List,
    /// Show one instance.
    Status { id: String },
    /// List available workflow definitions.
    Defs {
        #[arg(long, default_value = ".")]
        repo: String,
    },
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
        Command::In(args) => space_cmds::blocking_read(&layout, args, true, cli.json).await?,
        Command::Rd(args) => space_cmds::blocking_read(&layout, args, false, cli.json).await?,
        Command::Scan(args) => space_cmds::scan(&layout, args, cli.json).await?,
        Command::Watch(args) => space_cmds::watch(&layout, args).await?,
        Command::Done(args) => space_cmds::done(&layout, args, cli.json).await?,
        Command::Obstacle(args) => space_cmds::report(&layout, args, "obstacle", cli.json).await?,
        Command::Need(args) => space_cmds::report(&layout, args, "need", cli.json).await?,
        Command::Claim(args) => space_cmds::claim(&layout, args, cli.json).await?,
        Command::Suggest(args) => space_cmds::suggest(&layout, args, cli.json).await?,
        Command::Endorse(args) => space_cmds::endorse(&layout, args, cli.json).await?,
        Command::Spawn(args) => agent_cmds::spawn(&layout, args, cli.json).await?,
        Command::List => agent_cmds::list(&layout, cli.json).await?,
        Command::Inbox => agent_cmds::inbox(&layout, cli.json).await?,
        Command::Status(args) => agent_cmds::status(&layout, args, cli.json).await?,
        Command::Steer(args) => agent_cmds::steer(&layout, args, cli.json).await?,
        Command::Interrupt(args) => agent_cmds::interrupt(&layout, args, cli.json).await?,
        Command::Dismiss(args) => agent_cmds::dismiss(&layout, args, cli.json).await?,
        Command::Respawn(args) => agent_cmds::respawn(&layout, args, cli.json).await?,
        Command::Attach(args) => agent_cmds::attach(&layout, args).await?,
        Command::Cost => agent_cmds::cost(&layout, cli.json).await?,
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
        Command::Workflow { command } => {
            let mut client = Client::connect_or_spawn(&layout).await?;
            match command {
                WorkflowCommand::Run { name, repo, params } => {
                    let repo = std::fs::canonicalize(&repo)?;
                    let mut map = serde_json::Map::new();
                    for pair in params {
                        let (k, v) = pair.split_once('=').ok_or_else(|| {
                            anyhow::anyhow!("--param must be key=value, got: {pair}")
                        })?;
                        map.insert(k.to_string(), json!(v));
                    }
                    let result = client
                        .call(
                            "workflow.run",
                            json!({"name": name, "repo": repo.to_string_lossy(), "params": map}),
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
                WorkflowCommand::List => {
                    let result = client.call("workflow.list", json!({})).await?;
                    if cli.json {
                        println!("{}", result["instances"]);
                    } else {
                        for i in result["instances"].as_array().cloned().unwrap_or_default() {
                            println!(
                                "{:14} {:12} {:10} step {}/{}",
                                i["id"].as_str().unwrap_or("?"),
                                i["workflow"].as_str().unwrap_or("?"),
                                i["status"].as_str().unwrap_or("?"),
                                i["current_step"],
                                i["total_steps"],
                            );
                        }
                    }
                }
                WorkflowCommand::Status { id } => {
                    let result = client.call("workflow.status", json!({"name": id})).await?;
                    println!("{}", result["instance"]);
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
            const ROLES: [&str; 3] = ["operator", "rat", "reviewer"];
            if !ROLES.contains(&role.as_str()) {
                anyhow::bail!("unknown role '{role}' (expected: {})", ROLES.join(", "));
            }
            let ctx = rk_core::prime::PrimeContext {
                agent: std::env::var("RK_AGENT").unwrap_or_default(),
                repo: std::env::var("RK_REPO").unwrap_or_default(),
                task: std::env::var("RK_TASK").ok(),
                branch: std::env::var("RK_BRANCH").ok(),
                parent: std::env::var("RK_PARENT").ok(),
            };
            let text = rk_core::prime::render(&role, &ctx);
            if cli.json {
                println!("{}", json!({ "role": role, "prime": text }));
            } else {
                print!("{text}");
            }
        }
        Command::Repo { command } => match command {
            RepoCommand::Add { path, name } => {
                repo_cmds::add(&layout, path, name, cli.json).await?
            }
            RepoCommand::List => repo_cmds::list(&layout, cli.json).await?,
            RepoCommand::Show { name } => repo_cmds::show(&layout, name, cli.json).await?,
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
