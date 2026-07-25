//! Agent registry: the supervision tree as first-class data.
//!
//! Every record carries its `parent` (the spawner) — completion routing walks
//! this structure, never payload fields (the predecessor's foreman-routing lesson).

use chrono::{DateTime, Utc};
use rk_harness::TokenUsage;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Spawning,
    Running,
    Completed,
    Failed,
    Dismissed,
    /// The daemon restarted while this agent was running; its process is gone
    /// but worktree/branch/session are preserved for respawn.
    Orphaned,
}

impl AgentState {
    pub fn is_live(self) -> bool {
        matches!(self, AgentState::Spawning | AgentState::Running)
    }

    /// Whether a record in this state may be archived out of the default views.
    ///
    /// Only the three *settled* terminal states qualify. `Spawning`/`Running`
    /// are live work, and `Orphaned` is deliberately excluded even though it is
    /// terminal-ish: its worktree/branch/session are preserved precisely so
    /// `rk respawn` (and the respawn sweep) can pick it back up.
    pub fn is_archivable(self) -> bool {
        matches!(
            self,
            AgentState::Completed | AgentState::Failed | AgentState::Dismissed
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentRecord {
    pub name: String,
    pub role: String,
    pub harness: String,
    /// Model requested at spawn (None = harness default; pricing then relies
    /// on harness-reported cost only).
    #[serde(default)]
    pub model: Option<String>,
    pub repo_root: PathBuf,
    pub repo_name: String,
    pub task: Option<String>,
    pub branch: Option<String>,
    pub worktree: Option<PathBuf>,
    /// Merge target on dismissal.
    pub target_branch: String,
    /// Spawning agent's name (None = spawned by a human).
    pub parent: Option<String>,
    /// Workflow instance that dispatched this agent (None = spawned outside a
    /// workflow, e.g. a bare `rk spawn`). Cost is summed per instance to
    /// enforce a workflow's `budget:` cap at dispatch time.
    #[serde(default)]
    pub workflow_instance: Option<String>,
    pub session_id: Option<String>,
    /// herdr target when running attached in a pane (attach-mode spawn).
    #[serde(default)]
    pub attach_target: Option<String>,
    pub pid: Option<u32>,
    /// Merge commit a Direct-mode dismiss landed on the target — the anchor
    /// `rk revert` revert-merges. Cleared once reverted, so a second revert
    /// errors instead of reverting the revert.
    #[serde(default)]
    pub merge_commit: Option<String>,
    pub state: AgentState,
    pub result: Option<String>,
    pub usage: TokenUsage,
    pub cost_usd: f64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// When this record was moved to the archive store (`None` = live registry).
    /// Set by [`Registry::archive`] and cleared by [`Registry::unarchive`] —
    /// nothing else writes it, so it doubles as the "is this row archived?"
    /// marker in every rendered view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at: Option<DateTime<Utc>>,
}

impl AgentRecord {
    /// Identity of one *generation* of a name. Names are not recycled (see
    /// [`Registry::reserve_name`]), so this exists for the one case that can
    /// still put two rows under one name: the archive/persist crash window,
    /// where `created_at` tells the archived copy from the live one.
    fn generation(&self) -> (&str, DateTime<Utc>) {
        (self.name.as_str(), self.created_at)
    }
}

/// Default archive file name, kept beside `agents.json` in the same home.
const ARCHIVE_FILE: &str = "agents-archive.json";

/// JSON-file-backed registry. All mutation goes through [`Registry::update`],
/// which persists synchronously — the file is the daemon's restart memory.
///
/// Terminal records can be moved out of the live map into a second, append-only
/// archive store (`agents-archive.json`) by [`archive`](Registry::archive).
/// Archived records are *preserved, not deleted*: they keep their full
/// usage/cost/lineage and stay reachable through
/// [`list_archived`](Registry::list_archived) / [`list_all`](Registry::list_all)
/// / [`get_any`](Registry::get_any), and can be restored with
/// [`unarchive`](Registry::unarchive). They just stop inflating the default
/// views (`rk list`, `rk top`, `rk inbox`). Archiving does NOT free the name:
/// see [`reserve_name`].
///
/// [`reserve_name`]: Registry::reserve_name
pub struct Registry {
    path: PathBuf,
    archive_path: PathBuf,
    agents: HashMap<String, AgentRecord>,
    /// Archived terminal records. A `Vec`, not a map: an entry is keyed by
    /// GENERATION (`name`, `created_at`), which lets an archived record and a
    /// live one of the same name coexist across the archive/persist crash
    /// window (see [`Registry::is_shadowed`]).
    archived: Vec<AgentRecord>,
    /// Names handed out by [`Registry::reserve_name`] but not yet [`insert`]ed.
    /// Closes the spawn window where a name is chosen but its worktree/record
    /// don't exist yet, so concurrent spawns can't collide on it. In-memory
    /// only — a daemon restart starts with a clean slate (any half-spawned
    /// rats are already gone).
    ///
    /// [`insert`]: Registry::insert
    reserved: HashSet<String>,
}

impl Registry {
    pub fn load(path: &Path) -> rk_core::Result<Self> {
        let agents = if path.exists() {
            let data = std::fs::read_to_string(path)?;
            serde_json::from_str(&data)?
        } else {
            HashMap::new()
        };
        let archive_path = path.with_file_name(ARCHIVE_FILE);
        let archived: Vec<AgentRecord> = if archive_path.exists() {
            let data = std::fs::read_to_string(&archive_path)?;
            serde_json::from_str(&data)?
        } else {
            Vec::new()
        };
        Ok(Self {
            path: path.to_path_buf(),
            archive_path,
            agents,
            archived,
            reserved: HashSet::new(),
        })
    }

    /// Atomically pick a free rat name and reserve it against concurrent
    /// spawns. Callers hold the registry lock across this call and the
    /// eventual [`insert`](Registry::insert) (which frees the reservation);
    /// if the spawn fails before then, call [`release_name`] to free it.
    ///
    /// Consults the live map, the ARCHIVE, and the outstanding reservations.
    /// A rat name is an identity key, not a slot: it is stamped into durable
    /// tuples (`harness_result`, `task_done`), agent logs, branches and
    /// worktree paths that outlive the record by design, so `next_name`'s
    /// contract is that names are never recycled.
    ///
    /// TKT-136 briefly narrowed this to the live map, on the theory that
    /// archiving should return a name to the pool. It cannot: archiving does
    /// not retract the records the name is stamped into. A recycled name made
    /// every reader that keys on it ambiguous across time, which is TKT-146
    /// (a workflow's `wait` matched a two-day-old namesake's result and
    /// dismissed a one-second-old rat) and TKT-138 (`rk log` appends a reused
    /// name onto its predecessor's transcript). The pool needs no relief
    /// anyway — TKT-102 made `next_name` unbounded via generation suffixes
    /// (`Whisker-2`, `Whisker-3`, …), so uniqueness is free.
    ///
    /// [`release_name`]: Registry::release_name
    pub fn reserve_name(&mut self) -> String {
        let taken: Vec<&str> = self
            .agents
            .keys()
            .chain(self.reserved.iter())
            .map(String::as_str)
            .chain(self.archived.iter().map(|a| a.name.as_str()))
            .collect();
        let name = rk_core::names::next_name(taken);
        self.reserved.insert(name.clone());
        name
    }

    /// Release a name reserved by [`reserve_name`](Registry::reserve_name)
    /// when a spawn fails before the record is inserted.
    pub fn release_name(&mut self, name: &str) {
        self.reserved.remove(name);
    }

    /// Mark all live agents orphaned (called once at daemon startup).
    pub fn orphan_live_agents(&mut self) -> rk_core::Result<Vec<String>> {
        let mut orphaned = Vec::new();
        for record in self.agents.values_mut() {
            if record.state.is_live() {
                record.state = AgentState::Orphaned;
                record.pid = None;
                record.updated_at = Utc::now();
                orphaned.push(record.name.clone());
            }
        }
        self.persist()?;
        Ok(orphaned)
    }

    pub fn insert(&mut self, record: AgentRecord) -> rk_core::Result<()> {
        self.reserved.remove(&record.name);
        self.agents.insert(record.name.clone(), record);
        self.persist()
    }

    pub fn get(&self, name: &str) -> Option<&AgentRecord> {
        self.agents.get(name)
    }

    /// Live record for `name`, falling back to the newest archived generation.
    ///
    /// Read-only callers (`rk status`) use this so an archived rat's history
    /// stays queryable. Every MUTATING path (dismiss/revert/respawn/steer)
    /// deliberately keeps using [`get`](Registry::get), so an archived record
    /// reads as "no such agent" until it is explicitly unarchived.
    pub fn get_any(&self, name: &str) -> Option<&AgentRecord> {
        self.agents.get(name).or_else(|| {
            self.archived
                .iter()
                .filter(|a| a.name == name)
                .max_by_key(|a| a.created_at)
        })
    }

    /// Every generation of `name` that ever existed, oldest first: the
    /// `created_at` of each live or archived record carrying it.
    ///
    /// Normally one entry — a name is an identity key and
    /// [`reserve_name`](Registry::reserve_name) never recycles. Two entries mean
    /// the name genuinely named two rats, which the TKT-136 archiving window did
    /// to 24 names before TKT-146 closed it. Anything keyed on a name alone is
    /// ambiguous for exactly those names, so callers that must disambiguate
    /// (`rk log`) ask here instead of assuming.
    ///
    /// Deduped by `created_at`: the archive/persist crash window can leave one
    /// generation in both stores, and that is one rat, not two.
    pub fn generations_of(&self, name: &str) -> Vec<DateTime<Utc>> {
        let mut generations: Vec<DateTime<Utc>> = self
            .agents
            .get(name)
            .into_iter()
            .chain(self.archived.iter().filter(|a| a.name == name))
            .map(|a| a.created_at)
            .collect();
        generations.sort_unstable();
        generations.dedup();
        generations
    }

    /// The live registry, oldest first. Archived records are excluded — this is
    /// the default view every operator surface renders.
    pub fn list(&self) -> Vec<&AgentRecord> {
        let mut all: Vec<_> = self.agents.values().collect();
        all.sort_by_key(|a| a.created_at);
        all
    }

    /// Archived records only, oldest first.
    pub fn list_archived(&self) -> Vec<&AgentRecord> {
        let mut all: Vec<_> = self
            .archived
            .iter()
            .filter(|a| !self.is_shadowed(a))
            .collect();
        all.sort_by_key(|a| a.created_at);
        all
    }

    /// Live + archived, oldest first — the full lifetime history. Cost/usage
    /// rollups read this so archiving never moves a dollar figure.
    pub fn list_all(&self) -> Vec<&AgentRecord> {
        let mut all: Vec<_> = self.agents.values().collect();
        all.extend(self.archived.iter().filter(|a| !self.is_shadowed(a)));
        all.sort_by_key(|a| a.created_at);
        all
    }

    /// Terminal records eligible for archiving: state in
    /// {Completed, Failed, Dismissed} and last touched strictly before
    /// `before`. Live and `Orphaned` records are never eligible.
    pub fn archivable(&self, before: DateTime<Utc>) -> Vec<&AgentRecord> {
        let mut eligible: Vec<_> = self
            .agents
            .values()
            .filter(|a| a.state.is_archivable() && a.updated_at < before)
            .collect();
        eligible.sort_by_key(|a| a.created_at);
        eligible
    }

    /// Move every [`archivable`](Registry::archivable) record into the archive
    /// store, returning the records as archived (with `archived_at` stamped).
    ///
    /// Writes the archive file BEFORE the live file: a crash between the two
    /// leaves the record in both stores, and [`is_shadowed`] resolves that in
    /// favour of the live copy — i.e. the archive silently no-ops rather than
    /// losing a record.
    ///
    /// [`is_shadowed`]: Registry::is_shadowed
    pub fn archive(&mut self, before: DateTime<Utc>) -> rk_core::Result<Vec<AgentRecord>> {
        let names: Vec<String> = self
            .archivable(before)
            .into_iter()
            .map(|a| a.name.clone())
            .collect();
        if names.is_empty() {
            return Ok(Vec::new());
        }
        let now = Utc::now();
        let mut moved = Vec::new();
        for name in &names {
            let Some(record) = self.agents.get(name).cloned() else {
                continue;
            };
            let mut record = record;
            record.archived_at = Some(now);
            // Idempotent on the crash window above: a generation already in the
            // archive is not appended twice.
            if !self
                .archived
                .iter()
                .any(|a| a.generation() == record.generation())
            {
                self.archived.push(record.clone());
            }
            moved.push(record);
        }
        self.persist_archive()?;
        for record in &moved {
            self.agents.remove(&record.name);
        }
        self.persist()?;
        Ok(moved)
    }

    /// Restore the newest archived generation of `name` to the live registry.
    ///
    /// Errors if a live record already holds the name (archiving frees names
    /// for reuse, so this is a real collision, not a no-op).
    pub fn unarchive(&mut self, name: &str) -> rk_core::Result<Option<AgentRecord>> {
        if self.agents.contains_key(name) {
            return Err(rk_core::Error::other(format!(
                "cannot unarchive {name}: a live agent already holds that name"
            )));
        }
        let Some(index) = self
            .archived
            .iter()
            .enumerate()
            .filter(|(_, a)| a.name == name)
            .max_by_key(|(_, a)| a.created_at)
            .map(|(i, _)| i)
        else {
            return Ok(None);
        };
        let mut record = self.archived[index].clone();
        record.archived_at = None;
        self.agents.insert(name.to_string(), record.clone());
        // Live file first: a crash before the archive rewrite leaves the record
        // in both stores, where the live copy wins — never in neither.
        self.persist()?;
        self.archived.remove(index);
        self.persist_archive()?;
        Ok(Some(record))
    }

    /// Whether an archived entry is superseded by a live record of the same
    /// generation — the reconciliation rule for the crash window in
    /// [`archive`](Registry::archive) / [`unarchive`](Registry::unarchive).
    fn is_shadowed(&self, archived: &AgentRecord) -> bool {
        self.agents
            .get(&archived.name)
            .is_some_and(|live| live.generation() == archived.generation())
    }

    pub fn update<F>(&mut self, name: &str, mutate: F) -> rk_core::Result<Option<AgentRecord>>
    where
        F: FnOnce(&mut AgentRecord),
    {
        let Some(record) = self.agents.get_mut(name) else {
            return Ok(None);
        };
        mutate(record);
        record.updated_at = Utc::now();
        let snapshot = record.clone();
        self.persist()?;
        Ok(Some(snapshot))
    }

    pub fn remove(&mut self, name: &str) -> rk_core::Result<Option<AgentRecord>> {
        let removed = self.agents.remove(name);
        if removed.is_some() {
            self.persist()?;
        }
        Ok(removed)
    }

    fn persist(&self) -> rk_core::Result<()> {
        write_atomic(&self.path, &serde_json::to_vec_pretty(&self.agents)?)
    }

    fn persist_archive(&self) -> rk_core::Result<()> {
        write_atomic(
            &self.archive_path,
            &serde_json::to_vec_pretty(&self.archived)?,
        )
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> rk_core::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Resolve an `rk prune --before` spec into an absolute cutoff: records last
/// touched strictly before it are eligible.
///
/// Accepts a relative duration (`30m`, `24h`, `7d`, `2w`, `90s`) or an absolute
/// date (`2026-07-24`, or a full RFC3339 timestamp). A bare number is rejected
/// rather than guessed at — "7" meaning seconds when the operator meant days
/// would silently archive the whole registry.
pub fn cutoff_from_spec(spec: &str, now: DateTime<Utc>) -> rk_core::Result<DateTime<Utc>> {
    let s = spec.trim();
    let invalid = || {
        rk_core::Error::other(format!(
            "invalid time spec: {s} \
             (want a duration like 30m/24h/7d/2w, a date like 2026-07-24, or an RFC3339 timestamp)"
        ))
    };
    // Duration form. Match on the last CHAR (not byte) so a multibyte suffix
    // can't split mid-codepoint; the units are single-byte ASCII, so once one
    // matches, trimming one byte is a valid boundary.
    let unit = s.chars().last().and_then(|c| match c {
        's' => Some(1i64),
        'm' => Some(60),
        'h' => Some(3_600),
        'd' => Some(86_400),
        'w' => Some(604_800),
        _ => None,
    });
    if let Some(mult) = unit {
        let n: i64 = s[..s.len() - 1].trim().parse().map_err(|_| invalid())?;
        if n < 0 {
            return Err(invalid());
        }
        let delta = n
            .checked_mul(mult)
            .and_then(chrono::TimeDelta::try_seconds)
            .ok_or_else(invalid)?;
        return now.checked_sub_signed(delta).ok_or_else(invalid);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&Utc));
    }
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|d| d.and_utc())
        .ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &str, state: AgentState) -> AgentRecord {
        AgentRecord {
            name: name.into(),
            role: "rat".into(),
            harness: "fake".into(),
            model: None,
            repo_root: "/tmp/repo".into(),
            repo_name: "repo".into(),
            task: Some(".rk-1".into()),
            branch: Some(format!("rat/{name}/rk-1")),
            worktree: Some(format!("/tmp/wt/{name}").into()),
            target_branch: "main".into(),
            parent: None,
            workflow_instance: None,
            session_id: None,
            attach_target: None,
            pid: Some(1234),
            merge_commit: None,
            state,
            result: None,
            usage: TokenUsage::default(),
            cost_usd: 0.0,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            archived_at: None,
        }
    }

    /// A terminal record that went quiet `age_secs` ago.
    fn aged(name: &str, state: AgentState, age_secs: i64) -> AgentRecord {
        let mut r = record(name, state);
        r.created_at = Utc::now() - chrono::TimeDelta::try_seconds(age_secs).unwrap();
        r.updated_at = r.created_at;
        r
    }

    fn names(records: &[&AgentRecord]) -> Vec<String> {
        records.iter().map(|r| r.name.clone()).collect()
    }

    #[test]
    fn registry_persists_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agents.json");
        {
            let mut reg = Registry::load(&path).unwrap();
            reg.insert(record("Whisker", AgentState::Running)).unwrap();
            reg.update("Whisker", |r| r.cost_usd = 1.25).unwrap();
        }
        let reg = Registry::load(&path).unwrap();
        assert_eq!(reg.get("Whisker").unwrap().cost_usd, 1.25);
    }

    #[test]
    fn reserve_name_is_unique_until_inserted_or_released() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agents.json");
        let mut reg = Registry::load(&path).unwrap();

        // Two reservations without an intervening insert must differ — this is
        // the concurrent-spawn window that used to hand out duplicates.
        let first = reg.reserve_name();
        let second = reg.reserve_name();
        assert_ne!(first, second);

        // Inserting the first frees its reservation but the name is now in the
        // registry, so it still won't be reused.
        reg.insert(record(&first, AgentState::Running)).unwrap();
        let third = reg.reserve_name();
        assert_ne!(third, first);
        assert_ne!(third, second);

        // Releasing a reservation (spawn failed) returns the name to the pool.
        reg.release_name(&second);
        reg.release_name(&third);
        let reused = reg.reserve_name();
        assert_eq!(reused, second, "released name should be picked first again");
    }

    #[test]
    fn orphaning_marks_only_live_agents() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agents.json");
        let mut reg = Registry::load(&path).unwrap();
        reg.insert(record("Whisker", AgentState::Running)).unwrap();
        reg.insert(record("Nibbles", AgentState::Dismissed))
            .unwrap();

        let orphaned = reg.orphan_live_agents().unwrap();
        assert_eq!(orphaned, vec!["Whisker".to_string()]);
        assert_eq!(reg.get("Whisker").unwrap().state, AgentState::Orphaned);
        assert!(reg.get("Whisker").unwrap().pid.is_none());
        assert_eq!(reg.get("Nibbles").unwrap().state, AgentState::Dismissed);
    }

    /// The core guardrail: only settled terminal records leave the live view.
    #[test]
    fn archive_moves_only_terminal_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load(&dir.path().join("agents.json")).unwrap();
        for (name, state) in [
            ("Whisker", AgentState::Running),
            ("Nibbles", AgentState::Spawning),
            ("Scurry", AgentState::Orphaned),
            ("Crumb", AgentState::Completed),
            ("Gnaw", AgentState::Failed),
            ("Pip", AgentState::Dismissed),
        ] {
            reg.insert(aged(name, state, 3_600)).unwrap();
        }

        let moved = reg.archive(Utc::now()).unwrap();
        let mut moved_names: Vec<_> = moved.iter().map(|r| r.name.clone()).collect();
        moved_names.sort();
        assert_eq!(moved_names, vec!["Crumb", "Gnaw", "Pip"]);

        let mut live = names(&reg.list());
        live.sort();
        assert_eq!(
            live,
            vec!["Nibbles", "Scurry", "Whisker"],
            "live and orphaned records must never be archived"
        );
        assert!(moved.iter().all(|r| r.archived_at.is_some()));
    }

    #[test]
    fn archive_respects_the_before_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load(&dir.path().join("agents.json")).unwrap();
        reg.insert(aged("Crumb", AgentState::Completed, 8 * 86_400))
            .unwrap();
        reg.insert(aged("Pip", AgentState::Dismissed, 3_600))
            .unwrap();

        let week_ago = cutoff_from_spec("7d", Utc::now()).unwrap();
        assert_eq!(names(&reg.archivable(week_ago)), vec!["Crumb"]);

        let moved = reg.archive(week_ago).unwrap();
        assert_eq!(names(&moved.iter().collect::<Vec<_>>()), vec!["Crumb"]);
        assert_eq!(names(&reg.list()), vec!["Pip"], "the fresh record stays");
    }

    /// Archiving preserves history: the record survives with its cost/usage and
    /// lineage intact, is reloadable from disk, and round-trips back.
    #[test]
    fn archived_records_keep_history_and_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agents.json");
        {
            let mut reg = Registry::load(&path).unwrap();
            let mut rec = aged("Crumb", AgentState::Completed, 3_600);
            rec.cost_usd = 4.25;
            rec.usage.input = 1_000;
            rec.usage.output = 500;
            rec.parent = Some("Whisker".into());
            rec.workflow_instance = Some("wf-abc".into());
            reg.insert(rec).unwrap();
            reg.archive(Utc::now()).unwrap();
        }

        // Reload from disk: the archive is its own file beside agents.json.
        let mut reg = Registry::load(&path).unwrap();
        assert!(reg.list().is_empty(), "default view is clean");
        assert_eq!(names(&reg.list_archived()), vec!["Crumb"]);
        assert_eq!(names(&reg.list_all()), vec!["Crumb"]);

        let archived = reg.get_any("Crumb").unwrap();
        assert_eq!(archived.cost_usd, 4.25);
        assert_eq!(archived.usage.total(), 1_500);
        assert_eq!(archived.parent.as_deref(), Some("Whisker"));
        assert_eq!(archived.workflow_instance.as_deref(), Some("wf-abc"));
        assert!(archived.archived_at.is_some());
        assert!(
            reg.get("Crumb").is_none(),
            "mutating lookups must not see an archived record"
        );

        let restored = reg.unarchive("Crumb").unwrap().unwrap();
        assert!(restored.archived_at.is_none());
        assert_eq!(restored.cost_usd, 4.25);
        assert_eq!(names(&reg.list()), vec!["Crumb"]);
        assert!(reg.list_archived().is_empty());
    }

    /// TKT-146 regression at the naming layer. A rat name is an identity key
    /// stamped into durable tuples, logs and branches that outlive its record,
    /// so archiving must NOT return it to the pool — a recycled name makes
    /// every reader that keys on it ambiguous across time.
    #[test]
    fn archiving_never_recycles_the_name() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load(&dir.path().join("agents.json")).unwrap();
        let first = reg.reserve_name();
        reg.insert(aged(&first, AgentState::Dismissed, 3_600))
            .unwrap();
        assert_ne!(reg.reserve_name(), first, "a live name stays taken");

        reg.archive(Utc::now()).unwrap();
        assert_eq!(reg.list_archived().len(), 1);
        let next = reg.reserve_name();
        assert_ne!(
            next, first,
            "an archived name stays taken: it is still stamped into durable records"
        );

        // The reservation survives a reload — the archive file, not just the
        // in-memory copy, is what keeps the name spent.
        reg.insert(record(&next, AgentState::Running)).unwrap();
        let mut reloaded = Registry::load(&dir.path().join("agents.json")).unwrap();
        let third = reloaded.reserve_name();
        assert_ne!(third, first, "reload must not resurrect an archived name");
        assert_ne!(third, next, "reload must not resurrect a live name");
    }

    /// The archive is keyed by generation, not by name, so a record present in
    /// BOTH files (the archive-then-crash window, before `agents.json` was
    /// rewritten) resolves in favour of the live copy instead of double-counting.
    /// `reserve_name` can no longer produce this state; a crash still can.
    #[test]
    fn a_live_record_shadows_its_archived_twin() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load(&dir.path().join("agents.json")).unwrap();
        let name = reg.reserve_name();
        let generation = aged(&name, AgentState::Dismissed, 3_600);
        reg.insert(generation.clone()).unwrap();
        reg.archive(Utc::now()).unwrap();

        // Simulate the crash window: the archive was written but `agents.json`
        // was not, so the SAME generation is back in the live map on reload.
        let mut live = generation;
        live.state = AgentState::Running;
        live.archived_at = None;
        reg.insert(live).unwrap();
        assert!(reg.list_archived().is_empty(), "the live copy shadows it");
        assert_eq!(reg.list_all().len(), 1, "no double-count in the rollups");
        assert_eq!(reg.get_any(&name).unwrap().state, AgentState::Running);
        assert!(
            reg.unarchive(&name).is_err(),
            "unarchiving onto a live name must refuse rather than clobber"
        );
    }

    /// `generations_of` is what lets a name-keyed reader (`rk log`) disambiguate
    /// the 24 names that carried two rats during the TKT-136 archiving window.
    #[test]
    fn generations_of_reports_every_rat_that_carried_a_name() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load(&dir.path().join("agents.json")).unwrap();

        assert!(
            reg.generations_of("Gouda").is_empty(),
            "a name nobody has carried has no generations"
        );

        // The historical shape: an older rat archived out of the live map, and a
        // newer one under the same name still live.
        let older = aged("Gouda", AgentState::Dismissed, 2 * 86_400);
        let older_created = older.created_at;
        reg.insert(older).unwrap();
        assert_eq!(reg.generations_of("Gouda"), vec![older_created]);
        reg.archive(Utc::now()).unwrap();

        let newer = record("Gouda", AgentState::Running);
        let newer_created = newer.created_at;
        reg.insert(newer).unwrap();

        assert_eq!(
            reg.generations_of("Gouda"),
            vec![older_created, newer_created],
            "both rats, oldest first — the archived one is not lost behind the live one"
        );
        assert!(
            reg.generations_of("Brie").is_empty(),
            "and a namesake's generations never leak onto another name"
        );
    }

    /// One rat sitting in both stores (the archive-then-crash window) is one
    /// generation, not two — otherwise `rk log` would offer a phantom choice.
    #[test]
    fn generations_of_dedupes_a_record_present_in_both_stores() {
        let dir = tempfile::tempdir().unwrap();
        let mut reg = Registry::load(&dir.path().join("agents.json")).unwrap();
        let generation = aged("Cheddar", AgentState::Dismissed, 3_600);
        let created = generation.created_at;
        reg.insert(generation.clone()).unwrap();
        reg.archive(Utc::now()).unwrap();
        reg.insert(generation).unwrap();

        assert_eq!(reg.generations_of("Cheddar"), vec![created]);
    }

    #[test]
    fn cutoff_from_spec_parses_durations_and_dates() {
        let now = DateTime::parse_from_rfc3339("2026-07-24T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            cutoff_from_spec("7d", now).unwrap().to_rfc3339(),
            "2026-07-17T12:00:00+00:00"
        );
        assert_eq!(
            cutoff_from_spec("2h", now).unwrap().to_rfc3339(),
            "2026-07-24T10:00:00+00:00"
        );
        assert_eq!(
            cutoff_from_spec(" 1w ", now).unwrap().to_rfc3339(),
            "2026-07-17T12:00:00+00:00"
        );
        assert_eq!(
            cutoff_from_spec("2026-07-01", now).unwrap().to_rfc3339(),
            "2026-07-01T00:00:00+00:00"
        );
        assert_eq!(
            cutoff_from_spec("2026-07-01T06:30:00Z", now)
                .unwrap()
                .to_rfc3339(),
            "2026-07-01T06:30:00+00:00"
        );
        // A bare number is ambiguous, and a multibyte suffix must not panic.
        assert!(cutoff_from_spec("7", now).is_err());
        assert!(cutoff_from_spec("-3d", now).is_err());
        assert!(cutoff_from_spec("5m²", now).is_err());
        assert!(cutoff_from_spec("", now).is_err());
    }
}
