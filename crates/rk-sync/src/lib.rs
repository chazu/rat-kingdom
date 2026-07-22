//! Multiplayer sync over git notes: per-actor single-writer refs, append-only
//! NDJSON records, union merge at read time, deterministic claim arbitration.
//!
//! The pattern (git-appraise/git-bug/Radicle convergence):
//! - each actor writes ONLY `refs/notes/rk/<actor>` → every push is a
//!   fast-forward; contention is impossible by construction.
//! - records are one self-contained JSON object per line, ULID-keyed, so
//!   `git notes merge --strategy=cat_sort_uniq` (and plain line-union) are
//!   idempotent and order-independent.
//! - readers fetch all actors' refs into `refs/notes/rk-remote/<actor>`
//!   (NON-mirroring — never `+refs/notes/rk/*:refs/notes/rk/*` with prune,
//!   which deletes local notes) and union everything into a local view.
//! - conflicts (two castles claiming one task) resolve identically on every
//!   machine: earliest (timestamp, actor) wins. No revocation messages.
//!
//! Notes annotate a single well-known anchor blob per scope, so records are
//! free-standing (commit-anchored facts can use real commits later). All git
//! operations shell out to system git: libgit2 lacks notes-merge strategies,
//! and gix lacks notes entirely.

use chrono::{DateTime, Utc};
use rk_core::id::RecordId;
use rk_core::tuple::Tuple;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use tracing::debug;

const NOTES_LOCAL_PREFIX: &str = "refs/notes/rk";
const NOTES_REMOTE_PREFIX: &str = "refs/notes/rk-remote";

/// One replicated record: a tuple op wrapped with origin metadata.
/// Serialized as a single NDJSON line — the unit of `cat_sort_uniq` union.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SyncRecord {
    /// Unique, time-ordered id (dedupe key; ULID timestamp = happened-at).
    pub id: RecordId,
    /// Writing actor (castle name).
    pub actor: String,
    pub written_at: DateTime<Utc>,
    pub op: SyncOp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum SyncOp {
    /// A tuple written to the space (durable lifecycles only — ephemeral
    /// tuples never touch git).
    Out { tuple: Tuple },
    /// A tuple consumed (destructive take) — replicated so other castles
    /// drop it from their materialized views.
    Take { tuple_id: RecordId },
}

/// The per-repo sync handle for one actor.
pub struct NotesSync {
    repo: PathBuf,
    actor: String,
}

impl NotesSync {
    pub fn new(repo: &Path, actor: &str) -> Self {
        Self {
            repo: repo.to_path_buf(),
            actor: actor.to_string(),
        }
    }

    fn local_ref(&self) -> String {
        format!("{NOTES_LOCAL_PREFIX}/{}", self.actor)
    }

    /// The well-known anchor object all free-standing records annotate.
    fn anchor(&self) -> rk_core::Result<String> {
        // A stable blob: hash of a constant string, created if missing.
        let out = git(
            &self.repo,
            &["hash-object", "-w", "--stdin"],
            Some("rk-anchor-v1\n"),
        )?;
        Ok(out.trim().to_string())
    }

    /// Append records to this actor's own notes ref (fast-forward-only by
    /// construction — nobody else writes this ref).
    pub fn append(&self, records: &[SyncRecord]) -> rk_core::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let anchor = self.anchor()?;
        let mut lines = String::new();
        for record in records {
            lines.push_str(&serde_json::to_string(record)?);
            lines.push('\n');
        }
        git(
            &self.repo,
            &[
                "notes",
                "--ref",
                &self.local_ref(),
                "append",
                "--no-separator",
                "-m",
                lines.trim_end(),
                &anchor,
            ],
            None,
        )?;
        debug!(count = records.len(), r#ref = %self.local_ref(), "appended sync records");
        Ok(())
    }

    /// Fetch every actor's ref into the remote-tracking namespace, then push
    /// our own ref (if it exists yet). Fetch-first: even when the push fails
    /// (network, auth), remote knowledge still lands locally. Never
    /// mirroring, never pruning local refs.
    pub fn sync_with_remote(&self, remote: &str) -> rk_core::Result<SyncStats> {
        git(
            &self.repo,
            &[
                "fetch",
                remote,
                &format!("+{NOTES_LOCAL_PREFIX}/*:{NOTES_REMOTE_PREFIX}/*"),
            ],
            None,
        )?;
        let have_local = git(
            &self.repo,
            &["rev-parse", "--verify", "--quiet", &self.local_ref()],
            None,
        )
        .is_ok();
        if have_local {
            git(
                &self.repo,
                &[
                    "push",
                    remote,
                    &format!("{}:{}", self.local_ref(), self.local_ref()),
                ],
                None,
            )
            .map_err(|e| rk_core::Error::other(format!("push failed: {e}")))?;
        }
        let actors = self.known_actors()?;
        Ok(SyncStats {
            actors_seen: actors.len(),
        })
    }

    /// Whether this actor's own notes ref exists yet.
    pub fn has_local_ref(&self) -> bool {
        git(
            &self.repo,
            &["rev-parse", "--verify", "--quiet", &self.local_ref()],
            None,
        )
        .is_ok()
    }

    /// All actors visible locally (own ref + fetched remote-tracking refs).
    pub fn known_actors(&self) -> rk_core::Result<BTreeSet<String>> {
        let out = git(
            &self.repo,
            &[
                "for-each-ref",
                "--format=%(refname)",
                NOTES_LOCAL_PREFIX,
                NOTES_REMOTE_PREFIX,
            ],
            None,
        )?;
        Ok(out
            .lines()
            .filter_map(|r| r.rsplit('/').next())
            .map(String::from)
            .collect())
    }

    /// Materialize the union of every actor's records, deduplicated by record
    /// id, ordered by (id) — i.e. happened-at order. This is the "merge at
    /// read time" step; no shared ref is ever written.
    pub fn materialize(&self) -> rk_core::Result<Vec<SyncRecord>> {
        let refs = git(
            &self.repo,
            &[
                "for-each-ref",
                "--format=%(refname)",
                NOTES_LOCAL_PREFIX,
                NOTES_REMOTE_PREFIX,
            ],
            None,
        )?;
        let mut by_id: BTreeMap<RecordId, SyncRecord> = BTreeMap::new();
        for notes_ref in refs.lines() {
            // Each note blob holds NDJSON lines (possibly many after appends).
            let list = match git(&self.repo, &["notes", "--ref", notes_ref, "list"], None) {
                Ok(l) => l,
                Err(_) => continue,
            };
            for entry in list.lines() {
                let Some(note_obj) = entry.split_whitespace().next() else {
                    continue;
                };
                let Ok(content) = git(&self.repo, &["cat-file", "blob", note_obj], None) else {
                    continue;
                };
                for line in content.lines() {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    if let Ok(record) = serde_json::from_str::<SyncRecord>(line) {
                        by_id.insert(record.id, record);
                    }
                }
            }
        }
        Ok(by_id.into_values().collect())
    }

    /// Apply the materialized log to a tuple view: Outs insert, Takes remove.
    /// Claim conflicts (same category/scope/identity from different actors)
    /// resolve earliest-(id, actor)-wins — identical on every castle.
    pub fn materialize_tuples(&self) -> rk_core::Result<Vec<Tuple>> {
        let records = self.materialize()?;
        let mut tuples: BTreeMap<RecordId, Tuple> = BTreeMap::new();
        let mut taken: BTreeSet<RecordId> = BTreeSet::new();
        for record in &records {
            match &record.op {
                SyncOp::Out { tuple } => {
                    tuples.insert(tuple.id, tuple.clone());
                }
                SyncOp::Take { tuple_id } => {
                    taken.insert(*tuple_id);
                }
            }
        }
        let mut result: Vec<Tuple> = tuples
            .into_values()
            .filter(|t| !taken.contains(&t.id))
            .collect();
        arbitrate_claims(&mut result);
        Ok(result)
    }
}

/// Deterministic claim arbitration: for tuples in the `claim` category with
/// the same (scope, identity), only the earliest (id, instance) survives.
/// ULIDs embed creation time, so this is earliest-claim-wins with a total
/// tiebreak — computed identically by every reader.
pub fn arbitrate_claims(tuples: &mut Vec<Tuple>) {
    use rk_core::tuple::Category;
    let mut winners: BTreeMap<(String, String), (RecordId, String)> = BTreeMap::new();
    for t in tuples.iter() {
        if t.category != Category::Claim {
            continue;
        }
        let key = (t.scope.clone(), t.identity.clone());
        let contender = (t.id, t.instance.clone());
        match winners.get(&key) {
            Some(current) if *current <= contender => {}
            _ => {
                winners.insert(key, contender);
            }
        }
    }
    tuples.retain(|t| {
        if t.category != Category::Claim {
            return true;
        }
        winners
            .get(&(t.scope.clone(), t.identity.clone()))
            .map(|(id, instance)| t.id == *id && t.instance == *instance)
            .unwrap_or(true)
    });
}

#[derive(Debug, Clone, Copy)]
pub struct SyncStats {
    pub actors_seen: usize,
}

fn git(dir: &Path, args: &[&str], stdin: Option<&str>) -> rk_core::Result<String> {
    use std::io::Write;
    use std::process::Stdio;
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(dir).args(args);
    cmd.stdin(if stdin.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|e| rk_core::Error::other(format!("git not runnable: {e}")))?;
    if let (Some(input), Some(mut pipe)) = (stdin, child.stdin.take()) {
        pipe.write_all(input.as_bytes())?;
    }
    let out = child.wait_with_output()?;
    if !out.status.success() {
        return Err(rk_core::Error::other(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rk_core::tuple::{Category, Tuple};
    use serde_json::json;

    fn setup_repo(dir: &Path) {
        run(dir, &["init", "-b", "main"]);
        run(dir, &["config", "user.email", "r@x"]);
        run(dir, &["config", "user.name", "R"]);
        std::fs::write(dir.join("f"), "x\n").unwrap();
        run(dir, &["add", "."]);
        run(dir, &["commit", "-m", "init"]);
    }

    fn run(dir: &Path, args: &[&str]) {
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn record(actor: &str, tuple: Tuple) -> SyncRecord {
        SyncRecord {
            id: RecordId::new(),
            actor: actor.into(),
            written_at: Utc::now(),
            op: SyncOp::Out { tuple },
        }
    }

    fn tuple(category: Category, identity: &str, instance: &str) -> Tuple {
        Tuple::new(category, "repo", identity, instance, json!({"n": 1}))
    }

    #[test]
    fn append_and_materialize_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        setup_repo(dir.path());
        let sync = NotesSync::new(dir.path(), "castle-a");

        let r1 = record("castle-a", tuple(Category::Event, "e1", "castle-a"));
        let r2 = record("castle-a", tuple(Category::Fact, "f1", "castle-a"));
        sync.append(std::slice::from_ref(&r1)).unwrap();
        sync.append(std::slice::from_ref(&r2)).unwrap();

        let all = sync.materialize().unwrap();
        assert_eq!(all.len(), 2);
        assert!(all.contains(&r1) && all.contains(&r2));
        // Idempotent: re-materialize sees the same set.
        assert_eq!(sync.materialize().unwrap().len(), 2);
    }

    #[test]
    fn takes_remove_tuples_from_the_view() {
        let dir = tempfile::tempdir().unwrap();
        setup_repo(dir.path());
        let sync = NotesSync::new(dir.path(), "castle-a");

        let t = tuple(Category::Need, "help", "castle-a");
        let tid = t.id;
        sync.append(&[record("castle-a", t)]).unwrap();
        assert_eq!(sync.materialize_tuples().unwrap().len(), 1);

        sync.append(&[SyncRecord {
            id: RecordId::new(),
            actor: "castle-b".into(),
            written_at: Utc::now(),
            op: SyncOp::Take { tuple_id: tid },
        }])
        .unwrap();
        assert_eq!(sync.materialize_tuples().unwrap().len(), 0);
    }

    /// The headline test: two castles, one bare remote, concurrent claims on
    /// the same task — both converge to the same single winner with no
    /// coordination beyond push/fetch.
    #[test]
    fn two_castles_converge_and_arbitrate_claims() {
        let remote_dir = tempfile::tempdir().unwrap();
        run(remote_dir.path(), &["init", "--bare"]);
        let a_dir = tempfile::tempdir().unwrap();
        let b_dir = tempfile::tempdir().unwrap();
        for dir in [a_dir.path(), b_dir.path()] {
            setup_repo(dir);
            run(
                dir,
                &[
                    "remote",
                    "add",
                    "origin",
                    &remote_dir.path().to_string_lossy(),
                ],
            );
        }

        let castle_a = NotesSync::new(a_dir.path(), "castle-a");
        let castle_b = NotesSync::new(b_dir.path(), "castle-b");

        // Both claim the same task while partitioned. castle-a's claim is
        // minted first (ULIDs are time-ordered).
        let claim_a = tuple(Category::Claim, "task-42", "castle-a");
        std::thread::sleep(std::time::Duration::from_millis(3));
        let claim_b = tuple(Category::Claim, "task-42", "castle-b");
        castle_a
            .append(&[record("castle-a", claim_a.clone())])
            .unwrap();
        castle_b
            .append(&[record("castle-b", claim_b.clone())])
            .unwrap();
        // Plus some unconflicted traffic.
        castle_b
            .append(&[record(
                "castle-b",
                tuple(Category::Obstacle, "o1", "castle-b"),
            )])
            .unwrap();

        // Rejoin: both sync (push own ref, fetch all).
        castle_a.sync_with_remote("origin").unwrap();
        castle_b.sync_with_remote("origin").unwrap();
        castle_a.sync_with_remote("origin").unwrap(); // a needs b's first push

        let view_a = castle_a.materialize_tuples().unwrap();
        let view_b = castle_b.materialize_tuples().unwrap();

        // Identical views on both castles.
        let ids = |v: &Vec<Tuple>| {
            let mut ids: Vec<String> = v.iter().map(|t| t.id.to_string()).collect();
            ids.sort();
            ids
        };
        assert_eq!(ids(&view_a), ids(&view_b), "views converged");

        // Exactly one claim on task-42 survives — the earliest (castle-a's) —
        // and the obstacle came through.
        let claims: Vec<&Tuple> = view_a
            .iter()
            .filter(|t| t.category == Category::Claim)
            .collect();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].instance, "castle-a");
        assert_eq!(claims[0].id, claim_a.id);
        assert!(view_a.iter().any(|t| t.category == Category::Obstacle));

        // Pushes never contend: each actor only ever pushed its own ref.
    }

    #[test]
    fn arbitration_is_deterministic_under_reordering() {
        let t1 = tuple(Category::Claim, "task-9", "zeta-castle");
        std::thread::sleep(std::time::Duration::from_millis(3));
        let t2 = tuple(Category::Claim, "task-9", "alpha-castle");
        // Earliest id wins even though its instance sorts later.
        let mut forward = vec![t1.clone(), t2.clone()];
        arbitrate_claims(&mut forward);
        let mut reversed = vec![t2, t1.clone()];
        arbitrate_claims(&mut reversed);
        assert_eq!(forward, reversed);
        assert_eq!(forward.len(), 1);
        assert_eq!(forward[0].instance, "zeta-castle");
    }
}
