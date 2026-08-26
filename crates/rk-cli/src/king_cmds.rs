//! Operator-delegate King lifecycle commands.

use anyhow::Result;
use clap::{Args, Subcommand};
use rk_core::paths::Layout;
use rk_daemon::Client;
use serde_json::json;

#[derive(Subcommand)]
pub enum KingCommand {
    /// Start and register the configured King in a new Herdr workspace.
    Spawn(SpawnArgs),
    /// Attach interactively to the registered King (`at` is an alias).
    #[command(alias = "at")]
    Attach,
    /// Stop the registered King and close his Herdr workspace.
    Dismiss,
    /// Checkpoint and replace the King with a fresh harness generation.
    Restart,
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
pub struct SpawnArgs {
    /// Stable operator-delegate identity used for King and repo leases.
    #[arg(long, default_value = "king")]
    pub holder: String,
    /// Display name for the dedicated Herdr workspace and agent.
    #[arg(long, default_value = "King")]
    pub name: String,
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
            KingCommand::Spawn(args) => {
                let cwd = std::env::current_dir()?;
                client
                    .call(
                        "king.spawn",
                        json!({
                            "cwd": cwd,
                            "holder": args.holder,
                            "name": args.name,
                        }),
                    )
                    .await?
            }
            KingCommand::Attach => return attach(&mut client).await,
            KingCommand::Dismiss => client.call("king.dismiss", json!({})).await?,
            KingCommand::Restart => client.call("king.restart", json!({})).await?,
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
        if result["spawned"].as_bool() == Some(true) {
            let registration = &result["registration"];
            println!(
                "spawned {} with {} in {}",
                registration["name"].as_str().unwrap_or("King"),
                result["harness"].as_str().unwrap_or("configured harness"),
                registration["identity"]["terminal_id"]
                    .as_str()
                    .unwrap_or("Herdr"),
            );
            if result["enabled"].as_bool() == Some(false) {
                eprintln!(
                    "warning: [king].enabled is false; enable it and roll the daemon to receive automatic wakes"
                );
            }
        } else if result["dismissed"].as_bool() == Some(true) {
            println!("dismissed King");
        } else if result["restarted"].as_bool() == Some(true) {
            println!(
                "restarted King from checkpoint {}{}",
                result["checkpoint"].as_str().unwrap_or("?"),
                if result["restore_injected"].as_bool() == Some(true) {
                    ""
                } else {
                    " (restore pending; run `rk king tick`)"
                }
            );
        } else {
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
    }
    Ok(())
}

async fn attach(client: &mut Client) -> Result<()> {
    let result = client.call("king.status", json!({})).await?;
    let Some(value) = result["state"]["registration"]["identity"].as_object() else {
        anyhow::bail!("no King is registered; use `rk king spawn`");
    };
    let identity: rk_mux::AgentIdentity = serde_json::from_value(value.clone().into())?;
    if rk_mux::HerdrMux::exact_state(&identity).is_none() {
        anyhow::bail!("registered King generation is absent; use `rk king spawn`");
    }
    use std::os::unix::process::CommandExt;
    // Attach by generation id, not terminal id: if Herdr replaces the process
    // after the exact-state check, it must fail closed instead of attaching the
    // human to an unregistered successor in the same pane.
    let argv = rk_mux::HerdrMux::attach_argv(&identity.session_id);
    let error = std::process::Command::new(&argv[0]).args(&argv[1..]).exec();
    Err(anyhow::anyhow!("failed to exec herdr attach: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        command: KingCommand,
    }

    #[test]
    fn at_is_an_alias_for_attach() {
        assert!(matches!(
            TestCli::try_parse_from(["rk-king", "at"]).unwrap().command,
            KingCommand::Attach
        ));
        assert!(matches!(
            TestCli::try_parse_from(["rk-king", "attach"])
                .unwrap()
                .command,
            KingCommand::Attach
        ));
    }
}
