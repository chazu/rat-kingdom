//! Daemon-side multiplayer sync: bridges the local tuplespace and the
//! git-notes replication layer (rk-sync).
//!
//! Sync runs through a dedicated state repo at `~/.rat-kingdom/sync/` (auto-
//! initialized; origin = `[sync] remote_url`), NOT through work repos — the
//! tuplespace is global and its scopes span repositories. Ephemeral tuples
//! never leave the local daemon.

use rk_core::id::RecordId;
use rk_core::identity::CastleIdentity;
use rk_core::paths::Layout;
use rk_core::tuple::{Lifecycle, Pattern, Tuple, DEFAULT_TRAIL_TTL};
use rk_space::Space;
use rk_sync::{NotesSync, SyncOp};
use std::path::PathBuf;
use std::process::Command;
use tracing::{debug, info, warn};

pub struct Syncer {
    sync_repo: PathBuf,
    cursor_file: PathBuf,
    presence_file: PathBuf,
    notes: NotesSync,
    castle: String,
    remote_configured: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CycleStats {
    pub exported: usize,
    /// Durable tuples that vanished locally and were exported as `Take` ops so
    /// their consumption replicates.
    pub takes: usize,
    pub imported: usize,
    /// Locally-present tuples dropped because a peer consumed them.
    pub removed: usize,
    /// Records pruned from our own notes ref by compaction.
    pub compacted: usize,
    pub actors_seen: usize,
    pub pushed: bool,
}

impl Syncer {
    /// Ensure the state repo exists (init + optional origin) and build the
    /// sync handle.
    pub fn new(layout: &Layout, castle: &str, remote_url: Option<&str>) -> rk_core::Result<Self> {
        // The signing identity is loaded from (or minted into) the RK home, so
        // every op this castle exports is Ed25519-signed and peers can verify
        // its origin. The `castle` string is the logical author label written
        // onto tuples; the identity's actor id is who physically signed/replicated
        // them — equal by default, distinct only under a `castle_name` override.
        let identity = CastleIdentity::load_or_create(&layout.castle_key_path())?;
        let sync_repo = layout.home().join("sync");
        if !sync_repo.join(".git").exists() {
            std::fs::create_dir_all(&sync_repo)?;
            run_git(&sync_repo, &["init", "-b", "main"])?;
            run_git(&sync_repo, &["config", "user.email", "rk@rat-kingdom"])?;
            run_git(&sync_repo, &["config", "user.name", "rat-kingdom"])?;
            std::fs::write(
                sync_repo.join("README.md"),
                "# rat-kingdom sync state\n\nTuplespace replication lives in refs/notes/rk/*.\n",
            )?;
            run_git(&sync_repo, &["add", "."])?;
            run_git(&sync_repo, &["commit", "-m", "rk sync state repo"])?;
            info!(path = %sync_repo.display(), "initialized sync state repo");
        }
        let mut remote_configured = false;
        if let Some(url) = remote_url {
            let existing = run_git(&sync_repo, &["remote", "get-url", "origin"]).ok();
            match existing {
                Some(current) if current.trim() == url => {}
                Some(_) => {
                    run_git(&sync_repo, &["remote", "set-url", "origin", url])?;
                }
                None => {
                    run_git(&sync_repo, &["remote", "add", "origin", url])?;
                }
            }
            remote_configured = true;
        }
        let notes = NotesSync::new(&sync_repo, identity);
        // Announce this castle the first time its ref is created, so peers
        // see it even before it exports any work.
        if !notes.has_local_ref() {
            notes.append(&[notes.record(SyncOp::Out {
                tuple: Tuple::new(
                    rk_core::tuple::Category::Fact,
                    rk_core::tuple::SYSTEM_SCOPE,
                    "castle_presence",
                    castle.to_string(),
                    serde_json::json!({"castle": castle, "since": chrono::Utc::now()}),
                ),
            })])?;
        }
        Ok(Self {
            cursor_file: layout.home().join("sync-cursor"),
            presence_file: layout.home().join("sync-presence"),
            notes,
            sync_repo,
            castle: castle.to_string(),
            remote_configured,
        })
    }

    pub fn repo_path(&self) -> &std::path::Path {
        &self.sync_repo
    }

    /// One full cycle: export new local durable tuples and any local
    /// consumptions (as `Take` ops) → push/fetch → import remotely-authored
    /// tuples (waking local waiters through the normal out path) and drop tuples
    /// a peer consumed → compact our own history.
    ///
    /// Take-detection is diff-based and lives here, not on the space's `take`
    /// path, because the syncer's seam is the tuplespace snapshot, not each
    /// consume: a durable tuple we exported (or imported) that has *disappeared*
    /// from the space since we last saw it was consumed locally, and its removal
    /// must replicate or `out_if_new` would resurrect it on the next import.
    /// This deliberately replicates every local removal of a replicated durable
    /// tuple — `take`, targeted `delete`, and TTL/decay GC alike — which is the
    /// correct fix for resurrection in all three cases (the log has no TTL, so a
    /// locally-expired tuple with a surviving `Out` would otherwise be re-added
    /// every cycle). Only a survivor still alive in the log is ever turned into
    /// a Take, so an already-consumed tuple is never re-emitted.
    pub fn run_cycle(&self, space: &Space) -> rk_core::Result<CycleStats> {
        let cursor = self.load_cursor();
        let all = space.scan(&Pattern::default())?;
        let durable_live: Vec<&Tuple> = all
            .iter()
            .filter(|t| t.lifecycle != Lifecycle::Ephemeral)
            .collect();
        let live_ids: std::collections::BTreeSet<RecordId> =
            durable_live.iter().map(|t| t.id).collect();

        // (1) Export our own newly-authored durable tuples (cursor-bounded).
        let ours: Vec<&Tuple> = durable_live
            .iter()
            .copied()
            .filter(|t| t.instance == self.castle && cursor.map(|c| t.id > c).unwrap_or(true))
            .collect();
        let mut records: Vec<_> = ours
            .iter()
            .map(|t| {
                self.notes.record(SyncOp::Out {
                    tuple: (*t).clone(),
                })
            })
            .collect();
        let exported = records.len();
        if let Some(max) = ours.iter().map(|t| t.id).max() {
            self.save_cursor(max)?;
        }

        // (2) Export takes: durable tuples we knew were live last cycle, still
        //     alive in the log, but now gone from the space → we consumed them.
        let prev_known = self.load_presence();
        let local_view = self.notes.read_log()?;
        let alive_in_log: std::collections::BTreeSet<RecordId> = local_view
            .tuples
            .keys()
            .copied()
            .filter(|id| !local_view.taken.contains(id))
            .collect();
        let mut takes = 0;
        for id in prev_known.difference(&live_ids) {
            if alive_in_log.contains(id) {
                records.push(self.notes.record(SyncOp::Take { tuple_id: *id }));
                takes += 1;
            }
        }
        self.notes.append(&records)?;

        // (3) Push our ref, fetch everyone else's.
        let mut pushed = false;
        let mut actors_seen = 1;
        if self.remote_configured {
            match self.notes.sync_with_remote("origin") {
                Ok(stats) => {
                    pushed = true;
                    actors_seen = stats.actors_seen;
                }
                Err(e) => {
                    // Sync failure is coordination-visible, not a debug log
                    // (the predecessor's silent-stall lesson) — but local export already
                    // happened, so nothing is lost.
                    warn!(error = %e, "remote sync failed");
                    // Evaporating + Ephemeral: a "cannot reach remote" signal is
                    // purely local (only this castle's operator can act on it),
                    // so it must not replicate — and as a durable Session tuple
                    // it would export as an undrained Out that re-imports on every
                    // peer once the remote returns. It re-emits each failed cycle
                    // while the partition lasts and evaporates once sync recovers.
                    space.out(
                        Tuple::new(
                            rk_core::tuple::Category::Obstacle,
                            rk_core::tuple::SYSTEM_SCOPE,
                            "sync_failure",
                            self.castle.clone(),
                            serde_json::json!({"error": e.to_string()}),
                        )
                        .into_trail(DEFAULT_TRAIL_TTL),
                    )?;
                }
            }
        }

        // (4) Import remote knowledge and apply remote consumptions.
        let view = self.notes.read_log()?;
        let mut imported = 0;
        for (id, tuple) in &view.tuples {
            if view.taken.contains(id) || tuple.instance == self.castle {
                continue; // consumed, or ours (already live locally)
            }
            if space.out_if_new(tuple.clone())? {
                imported += 1;
            }
        }
        // A tuple a peer consumed must leave our space too, or it lingers as a
        // ghost. `delete` is local removal (no event, no resurrection) and a
        // no-op when we already dropped it ourselves.
        let mut removed = 0;
        for id in &view.taken {
            if space.delete(*id)? {
                removed += 1;
            }
        }

        // Record the durable tuples live at end-of-cycle (imports included) so
        // next cycle can diff against it to spot a local consumption. Must run
        // AFTER import/delete, or a just-imported tuple would never be tracked
        // and its later take would go unnoticed.
        let end_live: std::collections::BTreeSet<RecordId> = space
            .scan(&Pattern::default())?
            .into_iter()
            .filter(|t| t.lifecycle != Lifecycle::Ephemeral)
            .map(|t| t.id)
            .collect();
        self.save_presence(&end_live)?;

        // (5) Bound the log.
        let compacted = self.notes.compact().unwrap_or(0);

        debug!(
            exported,
            takes, imported, removed, compacted, "sync cycle complete"
        );
        Ok(CycleStats {
            exported,
            takes,
            imported,
            removed,
            compacted,
            actors_seen,
            pushed,
        })
    }

    pub fn peers(&self) -> rk_core::Result<Vec<String>> {
        Ok(self.notes.known_actors()?.into_iter().collect())
    }

    /// This castle's authenticated actor id (its git-notes ref segment), derived
    /// from its Ed25519 public key.
    pub fn actor(&self) -> &str {
        self.notes.actor()
    }

    fn load_cursor(&self) -> Option<RecordId> {
        std::fs::read_to_string(&self.cursor_file)
            .ok()?
            .trim()
            .parse()
            .ok()
    }

    fn save_cursor(&self, id: RecordId) -> rk_core::Result<()> {
        std::fs::write(&self.cursor_file, id.to_string())?;
        Ok(())
    }

    /// The durable tuple ids present in our space at the end of the last cycle.
    /// Diffed against the current space to spot local consumptions. A missing or
    /// corrupt file yields an empty set, so a lost presence file only delays
    /// take-detection by a cycle — it never fabricates a spurious Take.
    fn load_presence(&self) -> std::collections::BTreeSet<RecordId> {
        std::fs::read_to_string(&self.presence_file)
            .map(|s| s.lines().filter_map(|l| l.trim().parse().ok()).collect())
            .unwrap_or_default()
    }

    fn save_presence(&self, ids: &std::collections::BTreeSet<RecordId>) -> rk_core::Result<()> {
        let mut buf = String::with_capacity(ids.len() * 27);
        for id in ids {
            buf.push_str(&id.to_string());
            buf.push('\n');
        }
        std::fs::write(&self.presence_file, buf)?;
        Ok(())
    }
}

fn run_git(dir: &std::path::Path, args: &[&str]) -> rk_core::Result<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .map_err(|e| rk_core::Error::other(format!("git not runnable: {e}")))?;
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
    use rk_core::tuple::Category;
    use serde_json::json;

    /// Two castles (two RK_HOMEs) share a bare remote; a tuple written on A
    /// appears in B's space after each side runs a cycle — and a blocked
    /// reader on B is woken by the imported tuple.
    #[tokio::test]
    async fn tuples_replicate_between_castles_and_wake_waiters() {
        let remote = tempfile::tempdir().unwrap();
        run_git(remote.path(), &["init", "--bare"]).unwrap();
        let url = remote.path().to_string_lossy().to_string();

        let home_a = tempfile::tempdir().unwrap();
        let home_b = tempfile::tempdir().unwrap();
        let layout_a = Layout::at(home_a.path());
        let layout_b = Layout::at(home_b.path());
        layout_a.ensure().unwrap();
        layout_b.ensure().unwrap();

        let space_a = Space::open_in_memory().unwrap();
        let space_b = Space::open_in_memory().unwrap();
        let syncer_a = Syncer::new(&layout_a, "castle-a", Some(&url)).unwrap();
        let syncer_b = Syncer::new(&layout_b, "castle-b", Some(&url)).unwrap();

        // A blocked reader on castle B waits for knowledge from castle A.
        let waiter = {
            let space = space_b.clone();
            tokio::spawn(async move {
                space
                    .rd(
                        &Pattern::category(Category::Fact).identity("shared-fact"),
                        std::time::Duration::from_secs(10),
                    )
                    .await
            })
        };

        space_a
            .out(Tuple::new(
                Category::Fact,
                "repo",
                "shared-fact",
                "castle-a",
                json!({"discovered": "rate limit is 100/s"}),
            ))
            .unwrap();
        // Ephemeral tuples must NOT replicate.
        space_a
            .out(
                Tuple::new(Category::Claim, "repo", "eph", "castle-a", json!({}))
                    .with_lifecycle(Lifecycle::Ephemeral),
            )
            .unwrap();

        let stats_a = syncer_a.run_cycle(&space_a).unwrap();
        assert_eq!(stats_a.exported, 1, "ephemeral excluded");
        // B imports A's shared fact plus A's castle-presence announcement.
        let stats_b = syncer_b.run_cycle(&space_b).unwrap();
        assert_eq!(stats_b.imported, 2);

        // The imported tuple woke B's blocked reader through the normal path.
        let woken = waiter.await.unwrap().unwrap().expect("waiter woken");
        assert_eq!(woken.payload["discovered"], "rate limit is 100/s");
        assert_eq!(woken.instance, "castle-a");

        // Re-running cycles is idempotent (no duplicates).
        let again_b = syncer_b.run_cycle(&space_b).unwrap();
        assert_eq!(again_b.imported, 0);
        assert_eq!(
            space_b
                .scan(&Pattern::category(Category::Fact).identity("shared-fact"))
                .unwrap()
                .len(),
            1
        );

        // Peers are visible on both sides after B pushes.
        let stats_a2 = syncer_a.run_cycle(&space_a).unwrap();
        assert!(stats_a2.actors_seen >= 2);
        // Peers are keyed by the pubkey-derived actor id, not the hostname.
        assert!(syncer_a
            .peers()
            .unwrap()
            .contains(&syncer_b.actor().to_string()));
    }

    /// A daemon-authored obstacle (supervisor liveness/budget signal, or the
    /// syncer's own `sync_failure`) is a LOCAL coordination trail: it names this
    /// castle's agents or its own connectivity, which only this castle's operator
    /// can resolve. Shaped by `into_trail` it is Ephemeral, so `run_cycle` never
    /// exports it and no peer ever imports it — closing TKT-92, where such a
    /// tuple defaulted to durable Session and replicated forever as an Out that
    /// nothing ever drained.
    #[tokio::test]
    async fn daemon_authored_obstacle_trail_never_replicates() {
        let remote = tempfile::tempdir().unwrap();
        run_git(remote.path(), &["init", "--bare"]).unwrap();
        let url = remote.path().to_string_lossy().to_string();

        let home_a = tempfile::tempdir().unwrap();
        let home_b = tempfile::tempdir().unwrap();
        let layout_a = Layout::at(home_a.path());
        let layout_b = Layout::at(home_b.path());
        layout_a.ensure().unwrap();
        layout_b.ensure().unwrap();

        let space_a = Space::open_in_memory().unwrap();
        let space_b = Space::open_in_memory().unwrap();
        let syncer_a = Syncer::new(&layout_a, "castle-a", Some(&url)).unwrap();
        let syncer_b = Syncer::new(&layout_b, "castle-b", Some(&url)).unwrap();

        // Exactly the shape the supervisor/syncer now write daemon-internal
        // obstacles: an evaporating trail via into_trail.
        space_a
            .out(
                Tuple::new(
                    Category::Obstacle,
                    rk_core::tuple::SYSTEM_SCOPE,
                    "sync_failure",
                    "castle-a",
                    json!({"error": "cannot reach remote"}),
                )
                .into_trail(rk_core::tuple::DEFAULT_TRAIL_TTL),
            )
            .unwrap();

        // A's cycle exports only its castle-presence announcement, never the
        // Ephemeral obstacle.
        let stats_a = syncer_a.run_cycle(&space_a).unwrap();
        assert_eq!(stats_a.exported, 0, "ephemeral obstacle is not exported");

        // B imports A's presence fact but sees no obstacle.
        syncer_b.run_cycle(&space_b).unwrap();
        assert!(
            space_b
                .scan(&Pattern::category(Category::Obstacle))
                .unwrap()
                .is_empty(),
            "daemon obstacle must not cross to a peer"
        );

        // And it stays absent across further cycles (no resurrection path via an
        // undrained Out).
        for _ in 0..3 {
            syncer_a.run_cycle(&space_a).unwrap();
            syncer_b.run_cycle(&space_b).unwrap();
        }
        assert!(
            space_b
                .scan(&Pattern::category(Category::Obstacle))
                .unwrap()
                .is_empty(),
            "obstacle never replicates on repeat cycles"
        );
    }

    /// A durable tuple authored on A, imported by B, then consumed on B, must
    /// (a) leave A's space when the Take replicates back, and (b) never
    /// resurrect on B — the resurrection bug this ticket closes. Compaction
    /// then drains both records without changing any view.
    #[tokio::test]
    async fn consumption_replicates_and_never_resurrects() {
        let remote = tempfile::tempdir().unwrap();
        run_git(remote.path(), &["init", "--bare"]).unwrap();
        let url = remote.path().to_string_lossy().to_string();

        let home_a = tempfile::tempdir().unwrap();
        let home_b = tempfile::tempdir().unwrap();
        let layout_a = Layout::at(home_a.path());
        let layout_b = Layout::at(home_b.path());
        layout_a.ensure().unwrap();
        layout_b.ensure().unwrap();

        let space_a = Space::open_in_memory().unwrap();
        let space_b = Space::open_in_memory().unwrap();
        let syncer_a = Syncer::new(&layout_a, "castle-a", Some(&url)).unwrap();
        let syncer_b = Syncer::new(&layout_b, "castle-b", Some(&url)).unwrap();

        // A posts a durable need and replicates it to B.
        let need = Tuple::new(Category::Need, "repo", "grab-me", "castle-a", json!({}));
        let need_id = need.id;
        space_a.out(need).unwrap();
        syncer_a.run_cycle(&space_a).unwrap();
        syncer_b.run_cycle(&space_b).unwrap(); // B imports the need
        assert_eq!(
            space_b
                .scan(&Pattern::category(Category::Need).identity("grab-me"))
                .unwrap()
                .len(),
            1,
            "B imported the need"
        );

        // B's agent consumes the need (destructive take).
        let taken = space_b
            .take(
                &Pattern::category(Category::Need).identity("grab-me"),
                std::time::Duration::from_secs(1),
            )
            .await
            .unwrap()
            .expect("B took the need");
        assert_eq!(taken.id, need_id);

        // B exports the Take; A imports it and drops the need from its space.
        let b_stats = syncer_b.run_cycle(&space_b).unwrap();
        assert_eq!(b_stats.takes, 1, "B replicated the consumption");
        let a_stats = syncer_a.run_cycle(&space_a).unwrap();
        assert_eq!(a_stats.removed, 1, "A dropped the consumed need");
        assert!(
            space_a
                .scan(&Pattern::category(Category::Need).identity("grab-me"))
                .unwrap()
                .is_empty(),
            "consumption propagated to the author"
        );

        // No resurrection on B across further cycles (the core bug).
        for _ in 0..3 {
            syncer_b.run_cycle(&space_b).unwrap();
            assert!(
                space_b
                    .scan(&Pattern::category(Category::Need).identity("grab-me"))
                    .unwrap()
                    .is_empty(),
                "consumed need must not resurrect on B"
            );
        }

        // Compaction eventually drains the need's Out+Take across both actors,
        // and the need still stays dead.
        for _ in 0..4 {
            syncer_a.run_cycle(&space_a).unwrap();
            syncer_b.run_cycle(&space_b).unwrap();
        }
        assert!(
            !syncer_a
                .notes
                .own_records()
                .unwrap()
                .iter()
                .any(|r| matches!(&r.op, SyncOp::Out { tuple } if tuple.id == need_id)),
            "A compacted away the consumed Out"
        );
        assert!(
            !syncer_b
                .notes
                .own_records()
                .unwrap()
                .iter()
                .any(|r| matches!(&r.op, SyncOp::Take { tuple_id } if *tuple_id == need_id)),
            "B compacted away the orphaned Take"
        );
        let absent = |space: &Space| {
            !space
                .scan(&Pattern::default())
                .unwrap()
                .iter()
                .any(|t| t.id == need_id)
        };
        assert!(absent(&space_a) && absent(&space_b), "dead on both castles");
    }
}
