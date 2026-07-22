//! `rk` — the rat-kingdom CLI.

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
                Daemon::new(layout).run().await?;
            }
            DaemonCommand::Status => match Client::connect(&layout).await {
                Ok(mut client) => {
                    let status = client.call("status", json!({})).await?;
                    if cli.json {
                        println!("{status}");
                    } else {
                        println!(
                            "running: pid {} · uptime {}s · v{}",
                            status["pid"],
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
    }

    Ok(())
}
