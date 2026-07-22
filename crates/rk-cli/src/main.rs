//! `rk` — the rat-kingdom CLI.

mod agent_cmds;
mod space_cmds;

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
    /// Advisory claim on a task (sugar; env-autofilled).
    Claim(space_cmds::ClaimArgs),
    /// Spawn a rat to work on a task in an isolated worktree.
    Spawn(agent_cmds::SpawnArgs),
    /// List all agents.
    List,
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
                Daemon::new(layout, config.castle_name(), config.harness.default.clone())?
                    .run()
                    .await?;
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
        Command::Spawn(args) => agent_cmds::spawn(&layout, args, cli.json).await?,
        Command::List => agent_cmds::list(&layout, cli.json).await?,
        Command::Status(args) => agent_cmds::status(&layout, args, cli.json).await?,
        Command::Steer(args) => agent_cmds::steer(&layout, args, cli.json).await?,
        Command::Interrupt(args) => agent_cmds::interrupt(&layout, args, cli.json).await?,
        Command::Dismiss(args) => agent_cmds::dismiss(&layout, args, cli.json).await?,
        Command::Respawn(args) => agent_cmds::respawn(&layout, args, cli.json).await?,
    }

    Ok(())
}
