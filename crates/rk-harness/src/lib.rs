//! Harness adapters: drive AI coding-agent CLIs over their structured
//! protocols. No terminal scraping, no keystroke injection, no sleeps —
//! completion and state are events, not pane contents.

pub mod claude;
pub mod codex;
pub mod fake;
pub mod jcode;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Versioned, daemon-authenticated control input.  This is deliberately a
/// different value from assistant/tool/stderr output: adapters must carry it
/// on their control input, and never reconstruct it from child output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlEnvelope {
    pub schema: String,
    pub message_id: String,
    pub sender: String,
    pub target: String,
    pub delivery_generation: String,
    pub resume_generation: String,
    pub text: String,
    #[serde(default = "default_durable_control")]
    pub durable: bool,
}

fn default_durable_control() -> bool {
    true
}

impl ControlEnvelope {
    pub fn new(
        message_id: impl Into<String>,
        sender: impl Into<String>,
        target: impl Into<String>,
        delivery_generation: impl Into<String>,
        resume_generation: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            schema: "rk.control.v1".into(),
            message_id: message_id.into(),
            sender: sender.into(),
            target: target.into(),
            delivery_generation: delivery_generation.into(),
            resume_generation: resume_generation.into(),
            text: text.into(),
            durable: true,
        }
    }

    /// Internal supervisor nudges still use the same typed wire shape, but
    /// are explicitly identified as daemon-originated rather than looking
    /// like an operator's prose.
    pub fn system(target: impl Into<String>, text: impl Into<String>) -> Self {
        let mut envelope = Self::new(
            rk_core::id::SpawnId::new().to_string(),
            "rk-daemon",
            target,
            "system",
            "system",
            text,
        );
        envelope.durable = false;
        envelope
    }
}

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
    /// A line the harness child wrote to stderr. Most harnesses put nothing
    /// here, but a starved/misconfigured one (rate limit, queueing, auth
    /// refresh, model unavailable) may produce zero protocol output and die
    /// silently — stderr is the only trace of why.
    Stderr { text: String },
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
    /// The adapter accepted a trusted control envelope for delivery to the
    /// harness. This acknowledgement is separate from child prose/tool
    /// output and is the durable audit boundary in the daemon.
    ControlDelivered { envelope: ControlEnvelope },
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
    steer_tx: Option<mpsc::Sender<ControlEnvelope>>,
    kill_tx: mpsc::Sender<KillSignal>,
}

#[derive(Debug, Clone, Copy)]
enum KillSignal {
    Interrupt,
    Kill,
    Hard,
}

impl SessionControl {
    /// Send mid-session guidance. Errors if this harness cannot steer.
    pub async fn steer(&self, message: &str) -> rk_core::Result<()> {
        self.steer_envelope(&ControlEnvelope::system("unknown", message))
            .await
    }

    /// Send a daemon-authenticated control envelope. The envelope remains
    /// typed through the adapter boundary; callers cannot smuggle it in via
    /// assistant text or tool output.
    pub async fn steer_envelope(&self, envelope: &ControlEnvelope) -> rk_core::Result<()> {
        let Some(tx) = &self.steer_tx else {
            return Err(rk_core::Error::other(
                "this harness does not support steering",
            ));
        };
        tx.send(envelope.clone())
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

    /// Unconditional stop (SIGKILL — no chance for the harness to intercept
    /// or clean up). Only for a process already confirmed lingering past a
    /// grace window given to exit on its own; `kill` (SIGTERM) is always the
    /// first resort.
    pub async fn hard_kill(&self) -> rk_core::Result<()> {
        self.kill_tx
            .send(KillSignal::Hard)
            .await
            .map_err(|_| rk_core::Error::other("session is no longer running"))
    }
}

pub(crate) const SIGINT: i32 = 2;
pub(crate) const SIGTERM: i32 = 15;
pub(crate) const SIGKILL: i32 = 9;

/// Signal a child's process group (children lead their own group via
/// `process_group(0)`).
pub(crate) fn send_group_signal(pid: Option<u32>, sig: i32) {
    if let Some(pid) = pid {
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
        "jcode" => Ok(Box::new(jcode::JcodeHarness)),
        other => Err(rk_core::Error::other(format!(
            "unknown harness kind: {other} (available: claude, codex, jcode, fake)"
        ))),
    }
}

pub(crate) mod runner {
    //! Shared child-process plumbing: spawn, pump stdout lines through a
    //! per-adapter parser, forward steer messages to stdin, deliver signals.

    use super::*;
    use std::collections::VecDeque;
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::process::{ChildStdout, Command};
    use tokio::sync::mpsc::error::TrySendError;
    use tracing::{debug, warn};

    /// Cap on the stderr drain's local fallback buffer: once the shared event
    /// channel is full, further lines land here (dropping the oldest) instead
    /// of blocking on it. Only a bounded tail can ever matter downstream
    /// (`append_stderr_tail` keeps a bounded suffix anyway), so this trades
    /// unreachable-in-practice middle lines for the guarantee that draining
    /// the child's stderr pipe never stalls.
    const STDERR_BACKLOG_CAP: usize = 256;

    pub struct Wiring {
        pub command: Command,
        /// Parse one stdout line into zero or more events.
        pub parse: fn(&str) -> Vec<HarnessEvent>,
        /// Map a steer message to a stdin line, if the adapter supports it.
        pub steer_line: Option<fn(&ControlEnvelope) -> String>,
        /// Optional safe-turn handoff for adapters whose live process has no
        /// control-input protocol. The runner interrupts the current
        /// generation, waits for it to persist its session, and starts the
        /// returned command as the next generation before acknowledging the
        /// envelope.
        pub resume: Option<ResumeWiring>,
    }

    pub type ResumeCommand = Arc<dyn Fn(&str, &ControlEnvelope) -> Command + Send + Sync>;

    pub struct ResumeWiring {
        pub command: ResumeCommand,
    }

    async fn drain_stdout(
        stdout: &mut Option<tokio::io::Lines<BufReader<ChildStdout>>>,
        parse: fn(&str) -> Vec<HarnessEvent>,
        event_tx: &mpsc::Sender<HarnessEvent>,
    ) -> bool {
        loop {
            let line = match stdout.as_mut() {
                Some(lines) => lines.next_line().await,
                None => return true,
            };
            match line {
                Ok(Some(line)) => {
                    for event in parse(&line) {
                        if event_tx.send(event).await.is_err() {
                            return false;
                        }
                    }
                }
                Ok(None) => {
                    *stdout = None;
                    return true;
                }
                Err(error) => {
                    let _ = event_tx
                        .send(HarnessEvent::Stderr {
                            text: format!("Codex control stdout drain failed: {error}"),
                        })
                        .await;
                    *stdout = None;
                    return true;
                }
            }
        }
    }

    /// Guarantees a harness child's WHOLE process group dies, not just the
    /// PID `kill_on_drop` reaches. The child is spawned with
    /// `.process_group(0)` (below) so grandchildren it backgrounds share its
    /// group; `child` itself is only ever dropped inside the detached task
    /// spawned by `launch`, so this guard travels with it into that task.
    /// Disarmed once `child.wait()` resolves — by then the child has exited
    /// on its own and the group is (or is about to be) empty, so a group-kill
    /// there would just race a legitimately finished process. Any other way
    /// that task's future stops running (an ungraceful daemon shutdown, a
    /// panicking/aborted supervisor task, or a tokio runtime tearing down
    /// with the task still pending) drops the guard still armed, reaching
    /// grandchildren `kill_on_drop` alone cannot (TKT-01M0BX1CT23QHZHMRXNRFD8QBV).
    struct ProcessGroupGuard(Option<u32>);

    impl ProcessGroupGuard {
        fn disarm(&mut self) {
            self.0 = None;
        }
    }

    impl Drop for ProcessGroupGuard {
        fn drop(&mut self) {
            crate::send_group_signal(self.0, crate::SIGKILL);
        }
    }

    pub fn launch(mut wiring: Wiring) -> rk_core::Result<HarnessSession> {
        if wiring.resume.is_some() {
            return launch_with_resume(wiring);
        }

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
            .stderr(Stdio::piped())
            // Each harness child gets its OWN process group: some harnesses
            // (codex) signal their process group on cleanup, which must never
            // reach the daemon; and our signals should hit the child's whole
            // tree, not the daemon's.
            .process_group(0)
            .kill_on_drop(true);
        let mut child = wiring.command.spawn()?;
        let pid = child.id();
        let mut group_guard = ProcessGroupGuard(pid);

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| rk_core::Error::other("child stdout unavailable"))?;
        let stderr = child.stderr.take();
        let mut stdin = child.stdin.take();

        let (event_tx, events) = mpsc::channel::<HarnessEvent>(256);
        let (steer_tx, mut steer_rx) = mpsc::channel::<ControlEnvelope>(32);
        let (kill_tx, mut kill_rx) = mpsc::channel::<KillSignal>(4);

        let parse = wiring.parse;
        let steer_line = wiring.steer_line;

        // Drained on its own task, mirroring stdout: stderr is diagnostic
        // exhaust, not protocol, so a slow/absent reader of `events` must never
        // back up the child's stderr pipe and stall it. The handle is joined
        // (below, bounded) before `Exited` is published, so a silent
        // zero-token death never races its own explanation onto the wire.
        //
        // Forwarding is non-blocking (`try_send`): the loop must keep calling
        // `next_line` regardless of channel state, because *that* read is what
        // drains the OS pipe and keeps the child's own `write(2)` from
        // blocking. A full channel spills into a small bounded local backlog
        // (dropping the oldest line, not the newest) instead, flushed once the
        // pipe hits EOF — by then the child is gone, so a blocking flush can
        // no longer stall it.
        let stderr_task = stderr.map(|stderr| {
            let stderr_tx = event_tx.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                let mut backlog: VecDeque<String> = VecDeque::new();
                'read: loop {
                    match lines.next_line().await {
                        Ok(Some(line)) => {
                            // FIFO: older spilled lines go first. Drain the
                            // backlog head opportunistically, and while ANY
                            // backlog remains the new line queues behind it —
                            // sending a newer line into a recovered channel
                            // slot would publish stderr out of order.
                            while let Some(text) = backlog.pop_front() {
                                match stderr_tx.try_send(HarnessEvent::Stderr { text }) {
                                    Ok(()) => {}
                                    Err(TrySendError::Full(HarnessEvent::Stderr { text })) => {
                                        backlog.push_front(text);
                                        break;
                                    }
                                    Err(TrySendError::Full(_)) => unreachable!(
                                        "try_send is only ever called with HarnessEvent::Stderr here"
                                    ),
                                    Err(TrySendError::Closed(_)) => break 'read,
                                }
                            }
                            if backlog.is_empty() {
                                match stderr_tx.try_send(HarnessEvent::Stderr { text: line }) {
                                    Ok(()) => {}
                                    Err(TrySendError::Full(HarnessEvent::Stderr { text })) => {
                                        backlog.push_back(text);
                                    }
                                    Err(TrySendError::Full(_)) => unreachable!(
                                        "try_send is only ever called with HarnessEvent::Stderr here"
                                    ),
                                    Err(TrySendError::Closed(_)) => break 'read,
                                }
                            } else {
                                if backlog.len() == STDERR_BACKLOG_CAP {
                                    backlog.pop_front();
                                }
                                backlog.push_back(line);
                            }
                        }
                        Ok(None) => break, // EOF
                        Err(e) => {
                            warn!(error = %e, "stderr read failed");
                            break;
                        }
                    }
                }
                // The child's stderr is gone (EOF or read error) by now, so a
                // blocking send here cannot stall it — only best-effort delivery
                // of whatever the fast path couldn't keep up with.
                for text in backlog {
                    if stderr_tx.send(HarnessEvent::Stderr { text }).await.is_err() {
                        break;
                    }
                }
            })
        });

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
                            } else if event_tx
                                .send(HarnessEvent::ControlDelivered { envelope: msg })
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                    sig = kill_rx.recv() => {
                        match sig {
                            Some(KillSignal::Interrupt) => crate::send_group_signal(pid, crate::SIGINT),
                            Some(KillSignal::Kill) => crate::send_group_signal(pid, crate::SIGTERM),
                            Some(KillSignal::Hard) => crate::send_group_signal(pid, crate::SIGKILL),
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
            group_guard.disarm();
            // Join the stderr drain before publishing `Exited`: it sends on a
            // clone of the same channel from an independent task, so without
            // this the final stderr line(s) can race `Exited` onto the wire
            // out of order — exactly defeating a silent zero-token death's
            // one trace of why. Bounded because an orphaned grandchild can
            // hold the pipe open past the parent's own exit; best effort past
            // the bound beats hanging the whole session on it.
            if let Some(stderr_task) = stderr_task {
                if tokio::time::timeout(Duration::from_millis(500), stderr_task)
                    .await
                    .is_err()
                {
                    warn!("stderr drain still running past exit; publishing without full tail");
                }
            }
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

    /// Run an adapter whose only trusted control boundary is a fresh resume
    /// turn. The current process is never fed the envelope as ordinary stdin:
    /// it is interrupted, allowed to exit at a turn boundary, and replaced by
    /// a command built by the adapter with the exact authenticated envelope.
    fn launch_with_resume(mut wiring: Wiring) -> rk_core::Result<HarnessSession> {
        let resume = wiring
            .resume
            .take()
            .expect("resume wiring checked before entering launch_with_resume");
        let mut command = wiring.command;
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        let mut pid = child.id();
        let group_guard = ProcessGroupGuard(pid);
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| rk_core::Error::other("child stdout unavailable"))?;
        let stderr = child.stderr.take();
        let (event_tx, events) = mpsc::channel::<HarnessEvent>(256);
        let (steer_tx, mut steer_rx) = mpsc::channel::<ControlEnvelope>(32);
        let (kill_tx, mut kill_rx) = mpsc::channel::<KillSignal>(4);
        let parse = wiring.parse;
        let resume_command = resume.command;

        tokio::spawn(async move {
            let mut stdout = Some(BufReader::new(stdout).lines());
            let mut stderr = stderr.map(|stream| BufReader::new(stream).lines());
            let mut session_id: Option<String> = None;
            let mut pending: Option<ControlEnvelope> = None;
            let mut interrupt_sent = false;
            let mut delivered = std::collections::HashSet::new();
            let mut group_guard = group_guard;

            loop {
                tokio::select! {
                    line = async {
                        match stdout.as_mut() {
                            Some(lines) => lines.next_line().await,
                            None => std::future::pending().await,
                        }
                    } => match line {
                        Ok(Some(line)) => {
                            for event in parse(&line) {
                                if let HarnessEvent::Started { session_id: Some(id) } = &event {
                                    session_id = Some(id.clone());
                                    if pending.is_some() && !interrupt_sent {
                                        send_group_signal(pid, SIGINT);
                                        interrupt_sent = true;
                                    }
                                }
                                if event_tx.send(event).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Ok(None) => stdout = None,
                        Err(error) => {
                            let _ = event_tx.send(HarnessEvent::Stderr {
                                text: format!("Codex control stdout read failed: {error}"),
                            }).await;
                        }
                    },
                    line = async {
                        match stderr.as_mut() {
                            Some(lines) => lines.next_line().await,
                            None => std::future::pending().await,
                        }
                    } => match line {
                        Ok(Some(text)) => {
                            if event_tx.send(HarnessEvent::Stderr { text }).await.is_err() {
                                return;
                            }
                        }
                        Ok(None) => stderr = None,
                        Err(error) => {
                            stderr = None;
                            let _ = event_tx.send(HarnessEvent::Stderr {
                                text: format!("Codex stderr read failed: {error}"),
                            }).await;
                        }
                    },
                    msg = steer_rx.recv() => {
                        let Some(msg) = msg else { continue };
                        if delivered.contains(&msg.message_id)
                            || pending.as_ref().is_some_and(|active| active.message_id == msg.message_id)
                        {
                            continue;
                        }
                        pending = Some(msg);
                        if session_id.is_some() && !interrupt_sent {
                            send_group_signal(pid, SIGINT);
                            interrupt_sent = true;
                        }
                    },
                    sig = kill_rx.recv() => match sig {
                        Some(KillSignal::Interrupt) => {
                            pending = None;
                            send_group_signal(pid, SIGINT);
                        }
                        Some(KillSignal::Kill) => {
                            pending = None;
                            send_group_signal(pid, SIGTERM);
                        }
                        Some(KillSignal::Hard) => {
                            pending = None;
                            send_group_signal(pid, SIGKILL);
                        }
                        None => {}
                    },
                    status = child.wait() => {
                        let code = match status {
                            Ok(status) => status.code(),
                            Err(error) => {
                                let _ = event_tx.send(HarnessEvent::Stderr {
                                    text: format!("Codex child wait failed: {error}"),
                                }).await;
                                None
                            }
                        };
                        group_guard.disarm();

                        let Some(envelope) = pending.take() else {
                            let _ = tokio::time::timeout(
                                Duration::from_millis(500),
                                drain_stdout(&mut stdout, parse, &event_tx),
                            )
                            .await;
                            let _ = event_tx.send(HarnessEvent::Exited { code }).await;
                            return;
                        };
                        let _ = tokio::time::timeout(
                            Duration::from_millis(500),
                            drain_stdout(&mut stdout, parse, &event_tx),
                        )
                        .await;
                        let Some(session_id) = session_id.clone() else {
                            let _ = event_tx.send(HarnessEvent::Stderr {
                                text: format!("Codex control {} could not resume: no session id was established", envelope.message_id),
                            }).await;
                            let _ = event_tx.send(HarnessEvent::Retry {
                                attempt: 0,
                                error: "Codex session could not resume because no session id was established".into(),
                            }).await;
                            let _ = event_tx.send(HarnessEvent::Exited { code }).await;
                            return;
                        };

                        let mut resumed = (resume_command)(&session_id, &envelope);
                        resumed
                            .stdin(Stdio::null())
                            .stdout(Stdio::piped())
                            .stderr(Stdio::piped())
                            .process_group(0)
                            .kill_on_drop(true);
                        let Ok(mut next_child) = resumed.spawn() else {
                            let _ = event_tx.send(HarnessEvent::Stderr {
                                text: format!("Codex control {} could not resume the session", envelope.message_id),
                            }).await;
                            let _ = event_tx.send(HarnessEvent::Retry {
                                attempt: 0,
                                error: "Codex session resume process could not be started".into(),
                            }).await;
                            let _ = event_tx.send(HarnessEvent::Exited { code }).await;
                            return;
                        };
                        let next_pid = next_child.id();
                        let next_stdout = match next_child.stdout.take() {
                            Some(stdout) => stdout,
                            None => {
                                let _ = event_tx.send(HarnessEvent::Stderr {
                                    text: format!("Codex control {} resumed without stdout", envelope.message_id),
                                }).await;
                                let _ = event_tx.send(HarnessEvent::Retry {
                                    attempt: 0,
                                    error: "Codex session resume stdout was unavailable".into(),
                                }).await;
                                let _ = event_tx.send(HarnessEvent::Exited { code }).await;
                                return;
                            }
                        };
                        stdout = Some(BufReader::new(next_stdout).lines());
                        stderr = next_child.stderr.take().map(|stream| BufReader::new(stream).lines());
                        child = next_child;
                        group_guard = ProcessGroupGuard(next_pid);
                        pid = next_pid;
                        interrupt_sent = false;
                        delivered.insert(envelope.message_id.clone());
                        if event_tx.send(HarnessEvent::ControlDelivered { envelope }).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        Ok(HarnessSession {
            events,
            control: SessionControl {
                steer_tx: Some(steer_tx),
                kill_tx,
            },
            pid,
        })
    }
}
