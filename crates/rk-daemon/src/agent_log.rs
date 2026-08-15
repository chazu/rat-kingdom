//! Per-agent event log: a bounded JSONL transcript of what a rat actually did
//! (assistant prose, tool calls, retries, harness stderr), so the operator can
//! watch a run without `--attach`ing. `handle_event` used to drop these events
//! on the floor; here they become a durable timeline instead.
//!
//! Files live at `<home>/agent-logs/<agent>.<generation>.jsonl`, are byte-capped
//! ring buffers (a runaway rat cannot fill the disk), and stay strictly local —
//! the transcript is never a tuple and never touches `rk-sync`.
//!
//! Each file is bounded but the *count* is not: one more file per rat, forever.
//! [`AgentLog::delete_for`] is how `rk prune --reap-logs` reclaims the file of a
//! record it archives; generation keying is what makes that safe (below).
//!
//! # Why the file is keyed on a generation, not a name
//!
//! A transcript is written under the *generation* that produced it — the
//! `(name, created_at)` pair the archive has been keyed on since TKT-136 — not
//! under the bare name. A name is normally an identity key (TKT-146 restored
//! that: [`Registry::reserve_name`] never recycles), so most of the time the two
//! keys agree. They came apart once: between the TKT-136 archiving window and
//! the TKT-146 fix, archiving briefly returned a name to the pool, and 24 names
//! ended up naming two unrelated rats. Keying the file on the name alone means
//! their transcripts share a file, so `rk log <name>` can only present one of
//! them, silently, as if it were the whole story.
//!
//! Two consequences worth knowing:
//!
//! - A write can no longer land in the wrong rat's file, whatever a future
//!   naming regression does — the path carries the spawn instant.
//! - Reads are fixed *retroactively*. [`AgentLog::read`] still reads the legacy
//!   `<agent>.jsonl` path, but windows it to `[start, end)` of the generation
//!   asked for. That works because generations of a name never overlap in time
//!   (the predecessor was terminal-and-archived before the successor spawned),
//!   so a timestamp alone says which rat wrote a line.
//!
//! [`Registry::reserve_name`]: crate::agents::Registry::reserve_name

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

/// The subset of `HarnessEvent` worth persisting as a timeline — the four the
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
    /// A line the harness child wrote to stderr — the only trace left of a
    /// run that produced zero protocol output (rate limit, queueing, auth
    /// refresh, model unavailable) before dying.
    Stderr { text: String },
}

/// A broadcast record tagging an entry with the generation that wrote it, so
/// `--follow` clients can filter the shared feed down to the one they asked for.
#[derive(Debug, Clone)]
pub struct LogRecord {
    pub agent: String,
    /// `created_at` of the writing agent's record — the generation key.
    pub generation: DateTime<Utc>,
    pub entry: LogEntry,
}

/// One generation of an agent name: which rat's transcript to read, and the
/// time window that isolates it inside a legacy name-keyed file.
///
/// Build these with [`Supervisor::log_generations`], which reads the bounds off
/// the registry (live + archive) rather than guessing.
///
/// [`Supervisor::log_generations`]: crate::supervisor::Supervisor::log_generations
#[derive(Debug, Clone)]
pub struct Generation {
    pub agent: String,
    /// The record's `created_at`: the generation key, and the inclusive lower
    /// bound of its entries in a legacy file. `None` = no registry record for
    /// this name at all, so there is nothing to window against and the legacy
    /// file is read whole.
    pub start: Option<DateTime<Utc>>,
    /// `created_at` of the NEXT generation of this name, if there is one: the
    /// exclusive upper bound. `None` = newest generation, unbounded above.
    pub end: Option<DateTime<Utc>>,
}

impl Generation {
    pub fn of(agent: &str, start: DateTime<Utc>, end: Option<DateTime<Utc>>) -> Self {
        Self {
            agent: agent.to_string(),
            start: Some(start),
            end,
        }
    }

    /// A name with no record in the registry or the archive — a hand-pruned
    /// `agents.json`, or a typo. Reads the legacy file unfiltered so a
    /// transcript that outlived its record is still legible.
    pub fn unrecorded(agent: &str) -> Self {
        Self {
            agent: agent.to_string(),
            start: None,
            end: None,
        }
    }

    /// Whether an entry stamped `ts` belongs to this generation.
    fn contains(&self, ts: DateTime<Utc>) -> bool {
        self.start.is_none_or(|start| ts >= start) && self.end.is_none_or(|end| ts < end)
    }
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

    /// Append one entry to the transcript of the `generation` of `agent` that is
    /// running now (its record's `created_at`), and publish it to followers.
    /// Best-effort: a disk failure is logged, never propagated — the transcript
    /// is a convenience, not a correctness dependency of the run.
    pub fn append(&self, agent: &str, generation: DateTime<Utc>, event: LogEvent) {
        let entry = LogEntry {
            ts: Utc::now(),
            event,
        };
        // Broadcast first: a follower should see the entry even if the disk
        // write below fails.
        let _ = self.tx.send(LogRecord {
            agent: agent.to_string(),
            generation,
            entry: entry.clone(),
        });

        let line = match serde_json::to_string(&entry) {
            Ok(s) => s,
            Err(_) => return,
        };
        let path = self.path_for(agent, generation);
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

    /// Read one generation's transcript oldest-first, optionally only the last
    /// `tail` entries. Malformed lines (e.g. a torn write) are skipped, not
    /// fatal.
    ///
    /// Reads the generation's own file *and* the generation's window of the
    /// legacy name-keyed file, merged by timestamp. Both exist together only for
    /// a rat that was mid-run when the daemon upgraded to generation keying;
    /// after that the legacy path is pure history.
    pub fn read(&self, generation: &Generation, tail: Option<usize>) -> Vec<LogEntry> {
        let mut entries = match generation.start {
            Some(start) => read_entries(&self.path_for(&generation.agent, start)),
            None => Vec::new(),
        };
        let mut legacy = read_entries(&self.legacy_path(&generation.agent));
        if !legacy.is_empty() {
            legacy.retain(|e| generation.contains(e.ts));
            entries.append(&mut legacy);
            entries.sort_by_key(|e| e.ts);
        }
        if let Some(n) = tail {
            if entries.len() > n {
                entries.drain(0..entries.len() - n);
            }
        }
        entries
    }

    /// Delete one generation's transcript file — the artifact `rk prune
    /// --reap-git` leaves behind once it has reclaimed the worktree and the
    /// branch, and what `rk prune --reap-logs` reclaims. `Ok(false)` means
    /// there was no file to remove (a rat that never narrated), which is a
    /// normal outcome, not a failure.
    ///
    /// Keyed on the same `(agent, generation)` pair [`append`](Self::append)
    /// writes under, and rebuilt through [`path_for`](Self::path_for) rather
    /// than matched by glob, so the only file this can ever unlink is the one
    /// this exact rat wrote. That is precisely what generation keying bought:
    /// under the old name-keyed layout, deleting an archived rat's file could
    /// have taken a live namesake's transcript with it.
    ///
    /// The legacy name-keyed file is deliberately never touched. It can still
    /// hold entries of a generation that is *not* being archived — that is the
    /// 24 two-rat names of the TKT-136 window — and its path alone cannot tell
    /// those apart.
    pub fn delete_for(&self, agent: &str, generation: DateTime<Utc>) -> std::io::Result<bool> {
        let path = self.path_for(agent, generation);
        // Share the append lock: an archived record is terminal so nothing
        // should still be writing, but a delete racing a trim is cheap to rule
        // out entirely.
        let _guard = self.write_lock.lock().unwrap_or_else(|e| e.into_inner());
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn path_for(&self, agent: &str, generation: DateTime<Utc>) -> PathBuf {
        self.dir
            .join(format!("{}.{}.jsonl", sanitize(agent), stamp(generation)))
    }

    /// Where transcripts lived before the file was keyed on a generation:
    /// read-only history, never appended to again.
    fn legacy_path(&self, agent: &str) -> PathBuf {
        self.dir.join(format!("{}.jsonl", sanitize(agent)))
    }
}

/// A generation stamp for a file name: sortable, second-and-millisecond precise,
/// and made only of characters [`sanitize`] would leave alone.
fn stamp(generation: DateTime<Utc>) -> String {
    generation.format("%Y%m%dT%H%M%S%3fZ").to_string()
}

/// Parse a JSONL transcript file, oldest first. A missing file is an empty
/// transcript, not an error — an agent may simply not have narrated yet.
fn read_entries(path: &Path) -> Vec<LogEntry> {
    let data = match std::fs::read_to_string(path) {
        Ok(d) => d,
        Err(_) => return Vec::new(),
    };
    data.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
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

    fn at(unix_secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(unix_secs, 0).unwrap()
    }

    /// The only generation of a name, which is the normal case.
    fn only(agent: &str, start: DateTime<Utc>) -> Generation {
        Generation::of(agent, start, None)
    }

    #[test]
    fn append_then_read_roundtrips_in_order() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        let gen = at(1000);
        log.append("cinder", gen, LogEvent::Text { text: "hi".into() });
        log.append("cinder", gen, LogEvent::Tool { name: "Bash".into() });
        log.append(
            "cinder",
            gen,
            LogEvent::Retry {
                attempt: 2,
                error: "overloaded".into(),
            },
        );

        let entries = log.read(&only("cinder", gen), None);
        assert_eq!(entries.len(), 3);
        assert!(matches!(&entries[0].event, LogEvent::Text { text } if text == "hi"));
        assert!(matches!(&entries[1].event, LogEvent::Tool { name } if name == "Bash"));
        assert!(matches!(&entries[2].event, LogEvent::Retry { attempt: 2, .. }));
    }

    #[test]
    fn read_tail_returns_most_recent() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        let gen = at(1000);
        for i in 0..10 {
            log.append("r", gen, LogEvent::Tool { name: i.to_string() });
        }
        let tail = log.read(&only("r", gen), Some(3));
        assert_eq!(tail.len(), 3);
        assert!(matches!(&tail[0].event, LogEvent::Tool { name } if name == "7"));
        assert!(matches!(&tail[2].event, LogEvent::Tool { name } if name == "9"));
    }

    #[test]
    fn missing_agent_reads_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        assert!(log.read(&only("nobody", at(1000)), None).is_empty());
        assert!(log.read(&Generation::unrecorded("nobody"), None).is_empty());
    }

    #[test]
    fn append_publishes_to_followers() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        let mut rx = log.subscribe();
        let gen = at(1000);
        log.append("cinder", gen, LogEvent::Tool { name: "Read".into() });
        let rec = rx.try_recv().expect("entry should be broadcast");
        assert_eq!(rec.agent, "cinder");
        // The generation rides along so a follower of one rat is never handed a
        // namesake's live output.
        assert_eq!(rec.generation, gen);
        assert!(matches!(rec.entry.event, LogEvent::Tool { name } if name == "Read"));
    }

    #[test]
    fn oversized_log_is_trimmed_to_a_bounded_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        let gen = at(1000);
        // Each entry is ~1 KiB of prose; write well past the 512 KiB cap.
        let big = "x".repeat(1000);
        for _ in 0..1200 {
            log.append("whale", gen, LogEvent::Text { text: big.clone() });
        }
        let path = tmp
            .path()
            .join("agent-logs")
            .join(format!("whale.{}.jsonl", stamp(gen)));
        let len = std::fs::metadata(&path).unwrap().len();
        assert!(len <= CAP_BYTES, "log should be capped, got {len} bytes");
        // The tail is still well-formed and the newest entry survived.
        let entries = log.read(&only("whale", gen), None);
        assert!(!entries.is_empty());
        assert!(entries
            .iter()
            .all(|e| matches!(&e.event, LogEvent::Text { .. })));
    }

    /// The point of generation keying, stated as a deletion: reaping one rat's
    /// transcript cannot touch a namesake's, and cannot touch the legacy
    /// name-keyed file that may still hold a generation nobody is archiving.
    #[test]
    fn delete_for_removes_only_that_generations_file() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        let (old, new) = (at(1000), at(2000));
        log.append("cinder", old, LogEvent::Text { text: "first".into() });
        log.append("cinder", new, LogEvent::Text { text: "second".into() });
        let legacy = tmp.path().join("agent-logs").join("cinder.jsonl");
        std::fs::write(&legacy, "{\"ts\":\"1970-01-01T00:16:40Z\",\"kind\":\"text\",\"text\":\"legacy\"}\n").unwrap();

        assert!(log.delete_for("cinder", old).unwrap(), "file was there");

        // The namesake's own file is untouched...
        assert!(matches!(
            &log.read(&Generation::of("cinder", new, None), None)[0].event,
            LogEvent::Text { text } if text == "second"
        ));
        // ...and so is the legacy file, whichever generation wrote into it.
        assert!(legacy.exists(), "legacy file must never be reaped");
    }

    #[test]
    fn delete_for_reports_a_rat_that_never_narrated() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        // No file, no directory even: absence is a normal outcome, not an error.
        assert!(!log.delete_for("silent", at(1000)).unwrap());
    }

    #[test]
    fn odd_names_cannot_escape_the_log_dir() {
        assert_eq!(sanitize("../etc/passwd"), "___etc_passwd");
        assert_eq!(sanitize("Cinder-2"), "Cinder-2");
    }

    #[test]
    fn a_generation_stamp_is_sortable_and_filesystem_safe() {
        // Alphanumeric, so it survives `sanitize` untouched, and it sorts in
        // chronological order — the `name.stamp.jsonl` stem stays legible by eye.
        assert_eq!(stamp(at(1_753_408_412)), "20250725T015332000Z");
        assert!(stamp(at(1000)) < stamp(at(2000)));
        assert_eq!(sanitize(&stamp(at(1000))), stamp(at(1000)));
        // `sanitize` never emits a dot, so the dot separating name from stamp is
        // unambiguous however odd the name is.
        assert!(!sanitize("Sable-2").contains('.'));
    }

    /// The bug this keying exists to prevent: two rats sharing a name must not
    /// share a transcript.
    #[test]
    fn two_generations_of_one_name_keep_separate_transcripts() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        let (first, second) = (at(1000), at(2000));

        log.append("Gouda", first, LogEvent::Text { text: "gen1".into() });
        log.append("Gouda", second, LogEvent::Text { text: "gen2".into() });

        let older = log.read(&Generation::of("Gouda", first, Some(second)), None);
        assert_eq!(older.len(), 1, "the older rat sees only its own line");
        assert!(matches!(&older[0].event, LogEvent::Text { text } if text == "gen1"));

        let newer = log.read(&only("Gouda", second), None);
        assert_eq!(newer.len(), 1, "and the newer rat only its own");
        assert!(matches!(&newer[0].event, LogEvent::Text { text } if text == "gen2"));
    }

    /// Retroactive half of the fix: a legacy `<name>.jsonl` written before the
    /// keying change can hold several generations back to back. Reads split it on
    /// the generation boundary, so the 24 historical name pairs become legible
    /// without rewriting a single byte of history.
    #[test]
    fn a_legacy_shared_file_is_split_on_the_generation_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        let (first, second) = (at(1000), at(2000));
        write_legacy(
            tmp.path(),
            "Brie",
            &[
                (at(1001), "gen1 opening"),
                (at(1500), "gen1 closing"),
                (at(2001), "gen2 opening"),
            ],
        );

        let older = log.read(&Generation::of("Brie", first, Some(second)), None);
        assert_eq!(older.len(), 2, "windowed to the first rat's run");
        assert!(matches!(&older[0].event, LogEvent::Text { text } if text == "gen1 opening"));
        assert!(matches!(&older[1].event, LogEvent::Text { text } if text == "gen1 closing"));

        let newer = log.read(&only("Brie", second), None);
        assert_eq!(newer.len(), 1, "and the second rat's");
        assert!(matches!(&newer[0].event, LogEvent::Text { text } if text == "gen2 opening"));

        // Entries predating every generation belong to nobody and are dropped,
        // not silently attributed to the oldest rat.
        write_legacy(tmp.path(), "Pip", &[(at(500), "orphan")]);
        assert!(log.read(&only("Pip", first), None).is_empty());
    }

    /// A name with no record at all (hand-pruned `agents.json`) still reads: the
    /// legacy file is returned whole rather than the transcript vanishing.
    #[test]
    fn an_unrecorded_name_reads_its_legacy_file_whole() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        write_legacy(tmp.path(), "Ghost", &[(at(10), "a"), (at(20), "b")]);
        let entries = log.read(&Generation::unrecorded("Ghost"), None);
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn stderr_lines_roundtrip_tagged_as_stderr() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        let gen = at(1000);
        log.append(
            "cinder",
            gen,
            LogEvent::Stderr {
                text: "rate limited, retrying in 30s".into(),
            },
        );
        let entries = log.read(&only("cinder", gen), None);
        assert_eq!(entries.len(), 1);
        assert!(
            matches!(&entries[0].event, LogEvent::Stderr { text } if text == "rate limited, retrying in 30s")
        );
    }

    /// The upgrade boundary: a rat mid-run when the daemon gained generation
    /// keying has a prefix in the legacy file and a suffix in its own. One read
    /// merges them in timestamp order.
    #[test]
    fn a_run_straddling_the_upgrade_merges_both_files() {
        let tmp = tempfile::tempdir().unwrap();
        let log = log_at(tmp.path());
        let gen = at(1000);
        write_legacy(tmp.path(), "Twitch", &[(at(1001), "before restart")]);
        log.append(
            "Twitch",
            gen,
            LogEvent::Text {
                text: "after restart".into(),
            },
        );

        let entries = log.read(&only("Twitch", gen), None);
        assert_eq!(entries.len(), 2, "both halves of the run");
        assert!(matches!(&entries[0].event, LogEvent::Text { text } if text == "before restart"));
        assert!(matches!(&entries[1].event, LogEvent::Text { text } if text == "after restart"));
        // `tail` applies to the merged transcript, not to one file's share of it.
        let tail = log.read(&only("Twitch", gen), Some(1));
        assert_eq!(tail.len(), 1);
        assert!(matches!(&tail[0].event, LogEvent::Text { text } if text == "after restart"));
    }

    /// Hand-write the pre-generation file layout: `<agent>.jsonl`, one JSON entry
    /// per line.
    fn write_legacy(home: &Path, agent: &str, entries: &[(DateTime<Utc>, &str)]) {
        let dir = home.join("agent-logs");
        std::fs::create_dir_all(&dir).unwrap();
        let body: String = entries
            .iter()
            .map(|(ts, text)| {
                let entry = LogEntry {
                    ts: *ts,
                    event: LogEvent::Text {
                        text: (*text).to_string(),
                    },
                };
                format!("{}\n", serde_json::to_string(&entry).unwrap())
            })
            .collect();
        std::fs::write(dir.join(format!("{}.jsonl", sanitize(agent))), body).unwrap();
    }
}
