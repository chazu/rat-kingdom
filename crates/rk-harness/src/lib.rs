//! Harness adapters: drive AI coding-agent CLIs over their structured
//! protocols. No terminal scraping, no keystroke injection, no sleeps —
//! completion and state are events, not pane contents.

pub mod claude;
pub mod codex;
pub mod fake;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::mpsc;

/// Token counts for one API call or one session, by class.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_creation: u64,
}

impl TokenUsage {
    pub fn add(&mut self, other: &TokenUsage) {
        self.input += other.input;
        self.output += other.output;
        self.cache_read += other.cache_read;
        self.cache_creation += other.cache_creation;
    }

    pub fn total(&self) -> u64 {
        self.input + self.output + self.cache_read + self.cache_creation
    }
}

/// Normalized events every adapter maps its native protocol onto.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HarnessEvent {
    /// Session established; `session_id` enables resume.
    Started { session_id: Option<String> },
    /// A chunk of assistant prose.
    AssistantText { text: String },
    /// The agent invoked a tool.
    ToolUse { name: String },
    /// Token usage for one API call (ledger feed).
    Usage { usage: TokenUsage },
    /// The harness is retrying an API error.
    Retry { attempt: u64, error: String },
    /// Terminal result for the run.
    Completed {
        result: String,
        is_error: bool,
        usage: TokenUsage,
        cost_usd: Option<f64>,
        session_id: Option<String>,
    },
    /// The child process exited (always the final event).
    Exited { code: Option<i32> },
}

/// What a given adapter can do; the orchestrator adapts per-capability.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct HarnessCaps {
    pub steer: bool,
    pub interrupt: bool,
    pub resume: bool,
    pub reports_cost_usd: bool,
    pub native_budget: bool,
}

/// Everything needed to launch a session.
#[derive(Debug, Clone, Default)]
pub struct LaunchSpec {
    /// Initial prompt (the task, rendered with context).
    pub prompt: String,
    /// Appended system prompt (role instructions).
    pub system_prompt: Option<String>,
    /// Working directory (the agent's worktree).
    pub cwd: PathBuf,
    /// Extra environment (RK_AGENT, RK_REPO, ...).
    pub env: HashMap<String, String>,
    /// Permission mode hint (adapter-specific mapping).
    pub permission_mode: Option<String>,
    /// Model override.
    pub model: Option<String>,
    /// Resume a previous session by id.
    pub resume_session: Option<String>,
}

/// A live session: an event stream plus control handles.
pub struct HarnessSession {
    pub events: mpsc::Receiver<HarnessEvent>,
    pub control: SessionControl,
    pub pid: Option<u32>,
}

/// Handles for steering/interrupting a running session. Cheap to clone.
#[derive(Clone)]
pub struct SessionControl {
    steer_tx: Option<mpsc::Sender<String>>,
    kill_tx: mpsc::Sender<KillSignal>,
}

#[derive(Debug, Clone, Copy)]
enum KillSignal {
    Interrupt,
    Kill,
}

impl SessionControl {
    /// Send mid-session guidance. Errors if this harness cannot steer.
    pub async fn steer(&self, message: &str) -> rk_core::Result<()> {
        let Some(tx) = &self.steer_tx else {
            return Err(rk_core::Error::other(
                "this harness does not support steering",
            ));
        };
        tx.send(message.to_string())
            .await
            .map_err(|_| rk_core::Error::other("session is no longer running"))
    }

    pub fn can_steer(&self) -> bool {
        self.steer_tx.is_some()
    }

    /// Graceful interrupt (SIGINT — abort the current turn / wind down).
    pub async fn interrupt(&self) -> rk_core::Result<()> {
        self.kill_tx
            .send(KillSignal::Interrupt)
            .await
            .map_err(|_| rk_core::Error::other("session is no longer running"))
    }

    /// Hard stop (SIGTERM; harnesses treat this as clean shutdown).
    pub async fn kill(&self) -> rk_core::Result<()> {
        self.kill_tx
            .send(KillSignal::Kill)
            .await
            .map_err(|_| rk_core::Error::other("session is no longer running"))
    }
}

/// A coding-agent CLI adapter.
pub trait Harness: Send + Sync {
    fn kind(&self) -> &'static str;
    fn caps(&self) -> HarnessCaps;
    fn launch(&self, spec: &LaunchSpec) -> rk_core::Result<HarnessSession>;
}

/// Resolve an adapter by kind name.
pub fn make_harness(kind: &str) -> rk_core::Result<Box<dyn Harness>> {
    match kind {
        "claude" => Ok(Box::new(claude::ClaudeHarness)),
        "codex" => Ok(Box::new(codex::CodexHarness)),
        "fake" => Ok(Box::new(fake::FakeHarness)),
        other => Err(rk_core::Error::other(format!(
            "unknown harness kind: {other} (available: claude, codex, fake)"
        ))),
    }
}

pub(crate) mod runner {
    //! Shared child-process plumbing: spawn, pump stdout lines through a
    //! per-adapter parser, forward steer messages to stdin, deliver signals.

    use super::*;
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::process::Command;
    use tracing::{debug, warn};

    pub struct Wiring {
        pub command: Command,
        /// Parse one stdout line into zero or more events.
        pub parse: fn(&str) -> Vec<HarnessEvent>,
        /// Map a steer message to a stdin line, if the adapter supports it.
        pub steer_line: Option<fn(&str) -> String>,
    }

    pub fn launch(mut wiring: Wiring) -> rk_core::Result<HarnessSession> {
        // Only pipe stdin for steerable adapters: a piped-but-idle stdin makes
        // some CLIs (codex exec) block waiting for EOF before starting.
        let stdin_mode = if wiring.steer_line.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        };
        wiring
            .command
            .stdin(stdin_mode)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            // Each harness child gets its OWN process group: some harnesses
            // (codex) signal their process group on cleanup, which must never
            // reach the daemon; and our signals should hit the child's whole
            // tree, not the daemon's.
            .process_group(0)
            .kill_on_drop(true);
        let mut child = wiring.command.spawn()?;
        let pid = child.id();

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| rk_core::Error::other("child stdout unavailable"))?;
        let mut stdin = child.stdin.take();

        let (event_tx, events) = mpsc::channel::<HarnessEvent>(256);
        let (steer_tx, mut steer_rx) = mpsc::channel::<String>(32);
        let (kill_tx, mut kill_rx) = mpsc::channel::<KillSignal>(4);

        let parse = wiring.parse;
        let steer_line = wiring.steer_line;

        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            loop {
                tokio::select! {
                    line = lines.next_line() => match line {
                        Ok(Some(line)) => {
                            for event in parse(&line) {
                                if event_tx.send(event).await.is_err() {
                                    break;
                                }
                            }
                        }
                        Ok(None) => break, // EOF
                        Err(e) => {
                            warn!(error = %e, "stdout read failed");
                            break;
                        }
                    },
                    msg = steer_rx.recv() => {
                        if let (Some(msg), Some(map), Some(sin)) =
                            (msg, steer_line, stdin.as_mut())
                        {
                            let mut line = map(&msg).into_bytes();
                            line.push(b'\n');
                            if let Err(e) = sin.write_all(&line).await {
                                warn!(error = %e, "steer write failed");
                            }
                        }
                    }
                    sig = kill_rx.recv() => {
                        match sig {
                            Some(KillSignal::Interrupt) => send_signal(pid, libc_sigint()),
                            Some(KillSignal::Kill) => send_signal(pid, libc_sigterm()),
                            None => {}
                        }
                    }
                }
            }
            let code = match child.wait().await {
                Ok(status) => status.code(),
                Err(e) => {
                    warn!(error = %e, "child wait failed");
                    None
                }
            };
            debug!(?code, "harness child exited");
            let _ = event_tx.send(HarnessEvent::Exited { code }).await;
        });

        Ok(HarnessSession {
            events,
            control: SessionControl {
                steer_tx: steer_line.map(|_| steer_tx),
                kill_tx,
            },
            pid,
        })
    }

    fn send_signal(pid: Option<u32>, sig: i32) {
        if let Some(pid) = pid {
            // Negative pid = the child's whole process group (it leads its
            // own group via process_group(0) above).
            // SAFETY: plain kill(2) on a process group we created.
            unsafe {
                libc_kill(-(pid as i32), sig);
            }
        }
    }

    extern "C" {
        #[link_name = "kill"]
        fn libc_kill(pid: i32, sig: i32) -> i32;
    }

    fn libc_sigint() -> i32 {
        2
    }

    fn libc_sigterm() -> i32 {
        15
    }
}
