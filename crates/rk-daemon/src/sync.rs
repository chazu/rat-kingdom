//! Daemon-side multiplayer sync: bridges the local tuplespace and the
//! git-notes replication layer (rk-sync).
//!
//! Sync runs through a dedicated state repo at `~/.rat-kingdom/sync/` (auto-
//! initialized; origin = `[sync] remote_url`), NOT through work repos — the
//! tuplespace is global and its scopes span repositories. Ephemeral tuples
//! never leave the local daemon.

use rk_core::id::RecordId;
use rk_core::paths::Layout;
use rk_core::tuple::{Lifecycle, Pattern, Tuple};
use rk_space::Space;
use rk_sync::{NotesSync, SyncOp, SyncRecord};
use std::path::PathBuf;
use std::process::Command;
use tracing::{debug, info, warn};

pub struct Syncer {
    sync_repo: PathBuf,
    cursor_file: PathBuf,
    notes: NotesSync,
    castle: String,
    remote_configured: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CycleStats {
    pub exported: usize,
    pub imported: usize,
    pub actors_seen: usize,
    pub pushed: bool,
}

impl Syncer {
    /// Ensure the state repo exists (init + optional origin) and build the
    /// sync handle.
    pub fn new(layout: &Layout, castle: &str, remote_url: Option<&str>) -> rk_core::Result<Self> {
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
        let notes = NotesSync::new(&sync_repo, castle);
        // Announce this castle the first time its ref is created, so peers
        // see it even before it exports any work.
        if !notes.has_local_ref() {
            notes.append(&[SyncRecord {
                id: RecordId::new(),
                actor: castle.to_string(),
                written_at: chrono::Utc::now(),
                op: SyncOp::Out {
                    tuple: Tuple::new(
                        rk_core::tuple::Category::Fact,
                        rk_core::tuple::SYSTEM_SCOPE,
                        "castle_presence",
                        castle.to_string(),
                        serde_json::json!({"castle": castle, "since": chrono::Utc::now()}),
                    ),
                },
            }])?;
        }
        Ok(Self {
            cursor_file: layout.home().join("sync-cursor"),
            notes,
            sync_repo,
            castle: castle.to_string(),
            remote_configured,
        })
    }

    pub fn repo_path(&self) -> &std::path::Path {
        &self.sync_repo
    }

    /// One full cycle: export new local durable tuples → push/fetch → import
    /// remotely-authored tuples into the space (waking local waiters through
    /// the normal out path).
    pub fn run_cycle(&self, space: &Space) -> rk_core::Result<CycleStats> {
        let cursor = self.load_cursor();
        let all = space.scan(&Pattern::default())?;
        let ours: Vec<&Tuple> = all
            .iter()
            .filter(|t| {
                t.instance == self.castle
                    && t.lifecycle != Lifecycle::Ephemeral
                    && cursor.map(|c| t.id > c).unwrap_or(true)
            })
            .collect();

        let records: Vec<SyncRecord> = ours
            .iter()
            .map(|t| SyncRecord {
                id: RecordId::new(),
                actor: self.castle.clone(),
                written_at: chrono::Utc::now(),
                op: SyncOp::Out {
                    tuple: (*t).clone(),
                },
            })
            .collect();
        self.notes.append(&records)?;
        if let Some(max) = ours.iter().map(|t| t.id).max() {
            self.save_cursor(max)?;
        }

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
                    // (imp's silent-stall lesson) — but local export already
                    // happened, so nothing is lost.
                    warn!(error = %e, "remote sync failed");
                    space.out(Tuple::new(
                        rk_core::tuple::Category::Obstacle,
                        rk_core::tuple::SYSTEM_SCOPE,
                        "sync_failure",
                        self.castle.clone(),
                        serde_json::json!({"error": e.to_string()}),
                    ))?;
                }
            }
        }

        let mut imported = 0;
        for tuple in self.notes.materialize_tuples()? {
            if tuple.instance == self.castle {
                continue; // ours already live locally
            }
            if space.out_if_new(tuple)? {
                imported += 1;
            }
        }
        debug!(exported = records.len(), imported, "sync cycle complete");
        Ok(CycleStats {
            exported: records.len(),
            imported,
            actors_seen,
            pushed,
        })
    }

    pub fn peers(&self) -> rk_core::Result<Vec<String>> {
        Ok(self.notes.known_actors()?.into_iter().collect())
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
        assert!(syncer_a.peers().unwrap().contains(&"castle-b".to_string()));
    }
}
