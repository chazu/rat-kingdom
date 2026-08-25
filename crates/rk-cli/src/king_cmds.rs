//! Operator-delegate King lifecycle commands.

use anyhow::Result;
use clap::{Args, Subcommand};
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::json;

#[derive(Subcommand)]
pub enum KingCommand {
    /// Bind RK to one exact Herdr terminal and agent generation.
    Register(RegisterArgs),
    /// Show durable registration, wakes, context lifecycle, and checkpoints.
    Status,
    /// Claim an opaque wake and pull a fresh bounded RK snapshot.
    Pull(PullArgs),
    /// Settle a claimed wake after handling its snapshot.
    Resolve(SettleArgs),
    /// Settle a claimed wake as deliberately handed to a human.
    Defer(SettleArgs),
    /// Persist an explicit bounded checkpoint.
    Checkpoint(CheckpointArgs),
    /// Pull one checkpoint plus current authoritative RK state.
    Restore(RestoreArgs),
    /// Run one control-loop cycle immediately (diagnostics/testing).
    Tick,
}

#[derive(Args)]
pub struct RegisterArgs {
    /// Herdr agent name, pane id, terminal id, or agent-session id.
    pub target: String,
    /// Stable operator-delegate identity used for King and repo leases.
    #[arg(long, default_value = "king")]
    pub holder: String,
    /// Display name used when RK starts a fresh post-hibernation session.
    #[arg(long, default_value = "King")]
    pub name: String,
}

#[derive(Args)]
pub struct PullArgs {
    pub wake: String,
    #[arg(long, default_value = "king")]
    pub holder: String,
}

#[derive(Args)]
pub struct SettleArgs {
    pub wake: String,
    #[arg(long, default_value = "king")]
    pub holder: String,
}

#[derive(Args)]
pub struct CheckpointArgs {
    /// Optional bounded operator-delegate note (maximum 4096 characters).
    #[arg(long)]
    pub notes: Option<String>,
}

#[derive(Args)]
pub struct RestoreArgs {
    pub checkpoint: String,
}

pub async fn run(layout: &Layout, command: KingCommand, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result =
        match command {
            KingCommand::Register(args) => {
                client
                    .call(
                        "king.register",
                        json!({"target": args.target, "holder": args.holder, "name": args.name}),
                    )
                    .await?
            }
            KingCommand::Status => client.call("king.status", json!({})).await?,
            KingCommand::Pull(args) => {
                client
                    .call(
                        "king.pull",
                        json!({"wake": args.wake, "holder": args.holder}),
                    )
                    .await?
            }
            KingCommand::Resolve(args) => client
                .call(
                    "king.settle",
                    json!({"wake": args.wake, "holder": args.holder, "disposition": "resolved"}),
                )
                .await?,
            KingCommand::Defer(args) => client
                .call(
                    "king.settle",
                    json!({"wake": args.wake, "holder": args.holder, "disposition": "deferred"}),
                )
                .await?,
            KingCommand::Checkpoint(args) => {
                client
                    .call("king.checkpoint", json!({"notes": args.notes}))
                    .await?
            }
            KingCommand::Restore(args) => {
                client
                    .call("king.restore", json!({"checkpoint": args.checkpoint}))
                    .await?
            }
            KingCommand::Tick => client.call("king.tick", json!({})).await?,
        };
    if as_json {
        println!("{result}");
    } else {
        println!("{}", serde_json::to_string_pretty(&result)?);
    }
    Ok(())
}
