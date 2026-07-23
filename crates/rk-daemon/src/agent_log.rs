//! Per-agent event log: a bounded JSONL transcript of what a rat actually did
//! (assistant prose, tool calls, retries), so the operator can watch a run
//! without `--attach`ing. `handle_event` used to drop these events on the
//! floor; here they become a durable timeline instead.
//!
//! Files live at `<home>/agent-logs/<agent>.jsonl`, are byte-capped ring
//! buffers (a runaway rat cannot fill the disk), and stay strictly local — the
//! transcript is never a tuple and never touches `rk-sync`.

use chrono::{DateTime, Utc};
use rk_core::paths::Layout;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tokio::sync::broadcast;
use tracing::warn;

/// One transcript line: when it happened, and what happened.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub ts: DateTime<Utc>,
    #[serde(flatten)]
    pub event: LogEvent,
}

/// The subset of `HarnessEvent` worth persisting as a timeline — the three the
/// supervisor otherwise discards.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LogEvent {
    /// A chunk of assistant prose.
    Text { text: String },
    /// The agent invoked a tool.
    Tool { name: String },
    /// The harness retried an API error.
    Retry { attempt: u64, error: String },
}

/// A broadcast record tagging an entry with its agent so `--follow` clients can
/// filter the shared feed down to the one they asked for.
#[derive(Debug, Clone)]
pub struct LogRecord {
    pub agent: String,
    pub entry: LogEntry,
}

/// Once a transcript grows past `CAP_BYTES`, trim it back to (about) the last
/// `KEEP_BYTES`, so each agent's log is a bounded ring rather than an unbounded
/// append. Amortizes to O(1) per event: most appends just extend the file.
const CAP_BYTES: u64 = 512 * 1024;
const KEEP_BYTES: usize = 256 * 1024;

pub struct AgentLog {
    dir: PathBuf,
    tx: broadcast::Sender<LogRecord>,
    /// Serializes append+trim so a trim never races an append on the same file.
    write_lock: Mutex<()>,
}

impl AgentLog {
    pub fn new(layout: &Layout) -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            dir: layout.home().join("agent-logs"),
            tx,
            write_lock: Mutex::new(()),
        }
    }

    /// Subscribe to the live feed of every agent's entries (for `--follow`).
    pub fn subscribe(&self) -> broadcast::Receiver<LogRecord> {
        self.tx.subscribe()
    }

    /// Append one entry to `agent`'s transcript and publish it to followers.
    /// Best-effort: a disk failure is logged, never propagated — the transcript
    /// is a convenience, not a correctness dependency of the run.
    pub fn append(&self, agent: &str, event: LogEvent) {
        let entry = LogEntry {
            ts: Utc::now(),
            event,
        };
        // Broadcast first: a follower should see the entry even if the disk
        // write below fails.
        let _ = self.tx.send(LogRecord {
            agent: agent.to_string(),
            entry: entry.clone(),
        });

        let line = match serde_json::to_string(&entry) {
            Ok(s) => s,
            Err(_) => return,
        };
        let path = self.path_for(agent);
        let _guard = self.write_lock.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(e) = self.write_line(&path, &line) {
            warn!(agent, error = %e, "failed to append agent log");
        }
    }

    fn write_line(&self, path: &Path, line: &str) -> std::io::Result<()> {
        use std::io::Write;
        std::fs::create_dir_all(&self.dir)?;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        f.write_all(line.as_bytes())?;
        f.write_all(b"\n")?;
        let len = f.metadata()?.len();
        drop(f);
        if len > CAP_BYTES {
            trim_to_tail(path, KEEP_BYTES)?;
        }
        Ok(())
    }

    /// Read `agent`'s transcript oldest-first, optionally only the last `tail`
    /// entries. Malformed lines (e.g. a torn write) are skipped, not fatal.
    pub fn read(&self, agent: &str, tail: Option<usize>) -> Vec<LogEntry> {
        let data = match std::fs::read_to_string(self.path_for(agent)) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        let mut entries: Vec<LogEntry> = data
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();
        if let Some(n) = tail {
            if entries.len() > n {
                entries.drain(0..entries.len() - n);
            }
        }
        entries
    }

    fn path_for(&self, agent: &str) -> PathBuf {
        self.dir.join(format!("{}.jsonl", sanitize(agent)))
    }
}

/// Keep only the last `keep` bytes of a file, snapped forward to the next line
/// boundary so the trimmed file never opens on a half-line.
fn trim_to_tail(path: &Path, keep: usize) -> std::io::Result<()> {
    let data = std::fs::read(path)?;
    if data.len() <= keep {
        return Ok(());
    }
    let start = data.len() - keep;
    let boundary = data[start..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|i| start + i + 1)
        .unwrap_or(start);
    std::fs::write(path, &data[boundary..])
}

/// Filesystem-safe file stem. Agent names are rat names today, but be defensive
/// so a hostile/odd name can never traverse out of the log dir.
fn sanitize(agent: &str) -> String {
    agent
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_at(dir: &Path) -> AgentLog {
        AgentLog::new(&Layout::at(dir))
    }

    #[test]
    fn append_then_read_roundtrips_in_order() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        log.append("cinder", LogEvent::Text { text: "hi".into() });
        log.append("cinder", LogEvent::Tool { name: "Bash".into() });
        log.append(
            "cinder",
            LogEvent::Retry {
                attempt: 2,
                error: "overloaded".into(),
            },
        );

        let entries = log.read("cinder", None);
        assert_eq!(entries.len(), 3);
        assert!(matches!(&entries[0].event, LogEvent::Text { text } if text == "hi"));
        assert!(matches!(&entries[1].event, LogEvent::Tool { name } if name == "Bash"));
        assert!(matches!(&entries[2].event, LogEvent::Retry { attempt: 2, .. }));
    }

    #[test]
    fn read_tail_returns_most_recent() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        for i in 0..10 {
            log.append("r", LogEvent::Tool { name: i.to_string() });
        }
        let tail = log.read("r", Some(3));
        assert_eq!(tail.len(), 3);
        assert!(matches!(&tail[0].event, LogEvent::Tool { name } if name == "7"));
        assert!(matches!(&tail[2].event, LogEvent::Tool { name } if name == "9"));
    }

    #[test]
    fn missing_agent_reads_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        assert!(log.read("nobody", None).is_empty());
    }

    #[test]
    fn append_publishes_to_followers() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        let mut rx = log.subscribe();
        log.append("cinder", LogEvent::Tool { name: "Read".into() });
        let rec = rx.try_recv().expect("entry should be broadcast");
        assert_eq!(rec.agent, "cinder");
        assert!(matches!(rec.entry.event, LogEvent::Tool { name } if name == "Read"));
    }

    #[test]
    fn oversized_log_is_trimmed_to_a_bounded_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        // Each entry is ~1 KiB of prose; write well past the 512 KiB cap.
        let big = "x".repeat(1000);
        for _ in 0..1200 {
            log.append("whale", LogEvent::Text { text: big.clone() });
        }
        let path = tmp.path().join("agent-logs").join("whale.jsonl");
        let len = std::fs::metadata(&path).unwrap().len();
        assert!(len <= CAP_BYTES, "log should be capped, got {len} bytes");
        // The tail is still well-formed and the newest entry survived.
        let entries = log.read("whale", None);
        assert!(!entries.is_empty());
        assert!(entries.iter().all(|e| matches!(&e.event, LogEvent::Text { .. })));
    }

    #[test]
    fn odd_names_cannot_escape_the_log_dir() {
        assert_eq!(sanitize("../etc/passwd"), "___etc_passwd");
        assert_eq!(sanitize("Cinder-2"), "Cinder-2");
    }
}
