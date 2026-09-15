//! Immutable paired RK/MCP release inventory; preparation does not activate a release.
//! The index records intent before building; the release id binds repo, source and recipe revision.
//! Commit the manifest digest durably before publishing the complete manifest, then mark Prepared.
//! Reads verify that registry digest, manifest identity and the exact paired binary hashes.
//! Smoke checks establish launch/protocol behavior, not runtime compatibility.
//! The recorded toolchain describes the original build; idempotent reuse does not re-probe the
//! host.

use crate::managed_verification::{
    collect_child_output, HostVerificationAdmission, ManagedChildMarker, RunOutcome,
};
use chrono::{DateTime, Utc};
use rk_core::paths::Layout;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Bumped whenever [`ReleaseManifest`]'s shape changes incompatibly. A stored
/// manifest declaring any other value is refused outright rather than guessed
/// at — see [`load_manifest`].
pub const SCHEMA_VERSION: u32 = 1;

/// Build rk-cli and rk-mcp together through the bounded operator recipe.
pub const RECIPE_PAIRED_RK_MCP: &str = "paired-rk-mcp";

/// Bump when recipe behavior changes; the revision is part of the release identity.
pub const RECIPE_REVISION: u32 = 1;

const BUILD_TIMEOUT: Duration = Duration::from_secs(20 * 60);
const SMOKE_TIMEOUT: Duration = Duration::from_secs(10);
const CARGO_BUILD_JOBS: u32 = 2;
const NICE_LEVEL: i32 = 10;
/// P4.1 (TKT-nibuv-gokun-sibin): bound on how long the build subprocess may
/// wait for a [`HostVerificationAdmission`] permit before `run_recipe` gives
/// up and reports the whole `prepare` call `Failed` — distinct from
/// [`BUILD_TIMEOUT`], which bounds the build itself only once admitted.
/// Generous relative to ordinary named-check admission waits: a release
/// build is an infrequent, operator-initiated action, not a per-commit gate,
/// so queuing behind checks/other builds for a while is an acceptable
/// tradeoff for a hard host-wide capacity ceiling.
const ADMISSION_WAIT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// The fixed admission identity for every `paired-rk-mcp` build. Never
/// derived from a repository's own `.rk/checks.cue` — a repo cannot relabel
/// its own release build as cheap. Now (P3.2, `TKT-nasif-danob-sirok`, on
/// `main`) a real `config.toml`-keyable weight/class lookup: an operator
/// sets `[policy] verification_admission_check_weight."release-build:paired-rk-mcp"]`
/// to give this build a heavier reservation than the default `1`.
const RELEASE_ADMISSION_IDENTITY: &str = "release-build:paired-rk-mcp";
/// Maximum retained failure-tail characters; truncation respects character boundaries.
const FAILURE_EVIDENCE_CHARS: usize = 4000;

const EXPECTED_BINARY_NAMES: [&str; 2] = ["rk", "rk-mcp"];
/// CLI help is side-effect-free; MCP instead receives an initialize request over stdin.
const RK_SMOKE_ARGV: &[&str] = &["--help"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseStatus {
    /// Durable intent recorded; the bounded build has not yet finished. A
    /// `Preparing` entry found with no [`crate::server::Server`]-held release
    /// lock actually in flight is stale — see [`effective_status`].
    Preparing,
    Prepared,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseIndexEntry {
    pub id: String,
    pub repo: String,
    pub recipe: String,
    pub recipe_revision: u32,
    pub input_key: String,
    pub requested_source: String,
    pub status: ReleaseStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub detail: Option<String>,
    /// Registry trust anchor covering every manifest field, rechecked on reads.
    #[serde(default)]
    pub manifest_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinaryArtifact {
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceProvenance {
    /// Exactly what the caller passed as `--candidate` (a branch, tag, or
    /// sha) — kept for operator readability; never used for identity.
    pub requested: String,
    pub resolved_commit: String,
    pub tree_sha: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckEvidence {
    pub name: String,
    pub command: String,
    pub exit_code: Option<i32>,
    pub passed: bool,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecipeBounds {
    pub cargo_build_jobs: u32,
    pub nice: i32,
    pub timeout_secs: u64,
    /// Describes process-local enforcement; no host-wide CPU or immutable-execution guarantee.
    pub enforcement_note: String,
    /// P4.1 (TKT-nibuv-gokun-sibin): host-wide admission bounds actually
    /// observed for this build. `None` means `[policy]
    /// release_build_admission_enabled` was off — the legacy unmanaged path,
    /// identical to every release prepared before this field existed.
    #[serde(default)]
    pub host_admission: Option<HostAdmissionBounds>,
}

/// P4.1 telemetry: this build's own aggregate host-verification-admission
/// wait/run bounds, distinct from [`RecipeBounds::timeout_secs`] (the build
/// execution bound alone). Recorded only when admission was enabled for this
/// prepare call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostAdmissionBounds {
    /// Fixed, never repo-supplied — see [`RELEASE_ADMISSION_IDENTITY`].
    pub recipe_identity: String,
    /// Aggregate units this build actually consumed — the EFFECTIVE
    /// configured weight for [`RELEASE_ADMISSION_IDENTITY`]
    /// (`HostVerificationAdmission::weight_for`, P3.2), default `1` when
    /// unconfigured. Acquired atomically in one `acquire()` call (P3.2's
    /// `acquire_many_owned` internally, never several sequential single-unit
    /// calls) — a heavier weight is never approximated by holding a partial
    /// reservation while awaiting the rest, which would be exactly the
    /// recursive/partial-hold pattern that can deadlock two heavy builds
    /// contending for the same tight aggregate cap.
    pub weight: u32,
    /// How long the build waited for a permit before it was granted.
    pub admission_wait_ms: u64,
    /// How long the build subprocess itself ran once admitted (spawn to
    /// exit), `None` if it never reached that point.
    pub build_run_ms: Option<u64>,
}

/// Keep confirmed absence distinct from an unavailable observation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum FileObservation {
    Present {
        sha256: String,
    },
    Absent,
    /// `git` itself failed to answer — never treated as `Absent` by any
    /// caller.
    Unavailable {
        reason: String,
    },
}

/// Effective build-environment facts this module actually observed, kept
/// distinct from what it does not verify (see `compatibility_checked`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigProvenance {
    pub cargo_build_jobs_env: String,
    pub cargo_incremental_env: String,
    /// Select mise from the resolved source tree. An unavailable observation aborts preparation.
    pub used_mise: bool,
    /// `.rk/repo.cue` at the resolved commit — a non-secret fingerprint of
    /// the effective repository delivery policy this exact source tree
    /// carries.
    pub repo_policy: FileObservation,
    /// `.rk/checks.cue` at the resolved commit.
    pub checks_registry: FileObservation,
    /// Read-only exact-key proof reference, never a check execution or proof about these binaries.
    /// None means the daemon found no cached proof for this repo/commit/check identity.
    pub known_verification: Option<serde_json::Value>,
    /// Always false: successful smoke checks do not establish runtime compatibility.
    pub compatibility_checked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseManifest {
    pub schema_version: u32,
    pub id: String,
    pub repo: String,
    pub recipe: String,
    pub recipe_revision: u32,
    pub source: SourceProvenance,
    /// `rustc --version` captured from the actual build environment, after
    /// the build succeeded. Empty if the recipe could not record it — treated
    /// as "unavailable", never inferred.
    pub toolchain: String,
    pub binaries: BTreeMap<String, BinaryArtifact>,
    pub recipe_bounds: RecipeBounds,
    pub config_provenance: ConfigProvenance,
    /// Observed launch/protocol smoke evidence, not full verification coverage.
    pub checks: Vec<CheckEvidence>,
    pub created_at: DateTime<Utc>,
}

/// The caller must freeze commit/tree once under its lock and use them for both proof lookup
/// and preparation; resolving a mutable ref twice could attach the wrong proof.
pub struct PrepareParams {
    pub repo_name: String,
    pub repo_path: PathBuf,
    /// Exactly what the caller passed as `--candidate` — kept for
    /// operator-readable provenance (`SourceProvenance::requested`), never
    /// used for identity or re-resolved.
    pub requested: String,
    pub resolved_commit: String,
    pub tree_sha: String,
    pub recipe: String,
    /// Optional proof reference for that same frozen repo/commit/check; never triggers
    /// verification.
    pub known_verification: Option<serde_json::Value>,
}

/// Resolve `candidate` (a branch, tag, or sha) to its exact commit and tree
/// sha in `repo_path`. Blocking (shells out to `git`); callers on an async
/// executor should wrap this in `spawn_blocking`.
pub fn resolve_candidate(repo_path: &Path, candidate: &str) -> rk_core::Result<(String, String)> {
    let repo = rk_git::Repo::discover(repo_path)?;
    let resolved = repo
        .rev_parse(&format!("{candidate}^{{commit}}"))
        .map_err(|e| {
            rk_core::Error::other(format!("cannot resolve candidate '{candidate}': {e}"))
        })?;
    let tree = repo.rev_parse(&format!("{resolved}^{{tree}}"))?;
    Ok((resolved, tree))
}

/// Load a usable named check at sha; absent or invalid definitions return None.
pub fn load_named_check(repo_path: &Path, sha: &str, name: &str) -> Option<rk_workflow::Check> {
    let BlobObservation::Present(bytes) = read_blob_at(repo_path, sha, ".rk/checks.cue") else {
        return None;
    };
    let text = String::from_utf8(bytes).ok()?;
    let checks = rk_workflow::load_checks_str(&text).ok()?;
    checks.into_iter().find(|c| c.name == name)
}

pub struct PrepareOutcome {
    pub entry: ReleaseIndexEntry,
    pub manifest: ReleaseManifest,
    /// True when this call reused an already-`Prepared` release instead of
    /// running the recipe again.
    pub already_prepared: bool,
}

pub struct ShowResult {
    pub entry: ReleaseIndexEntry,
    pub manifest: Option<ReleaseManifest>,
    /// False means content drift; None means no manifest exists to verify.
    pub content_verified: Option<bool>,
}

fn releases_dir(layout: &Layout) -> PathBuf {
    layout.home().join("releases")
}

fn registry_path(layout: &Layout) -> PathBuf {
    layout.home().join("releases.json")
}

fn staging_dir(layout: &Layout, repo_name: &str) -> PathBuf {
    layout.home().join("release-staging").join(repo_name)
}

/// Where a stale partial release directory is moved (never deleted) when a
/// retry needs the id's directory back — see the doc comment in `run_recipe`.
fn quarantine_dir(layout: &Layout, id: &str) -> PathBuf {
    layout
        .home()
        .join("releases-partial")
        .join(format!("{id}-{}", Utc::now().format("%Y%m%dT%H%M%S%.3fZ")))
}

/// Isolated scratch RK_HOME so candidate smoke checks cannot touch the live fleet.
fn smoke_home_dir(layout: &Layout, id: &str) -> PathBuf {
    layout
        .home()
        .join("release-staging")
        .join(format!("{id}-smoke-{}", std::process::id()))
}

/// JSON-file-backed registry, persisted synchronously on every mutation —
/// same discipline as [`crate::repos::RepoRegistry`].
pub struct ReleaseRegistry {
    path: PathBuf,
    entries: BTreeMap<String, ReleaseIndexEntry>,
}

impl ReleaseRegistry {
    pub fn load(path: &Path) -> rk_core::Result<Self> {
        let entries = if path.exists() {
            serde_json::from_str(&std::fs::read_to_string(path)?)?
        } else {
            BTreeMap::new()
        };
        Ok(Self {
            path: path.to_path_buf(),
            entries,
        })
    }

    pub fn get(&self, id: &str) -> Option<&ReleaseIndexEntry> {
        self.entries.get(id)
    }

    pub fn list(&self, repo: Option<&str>) -> Vec<ReleaseIndexEntry> {
        let mut all: Vec<_> = self
            .entries
            .values()
            .filter(|e| repo.is_none_or(|r| e.repo == r))
            .cloned()
            .collect();
        all.sort_by_key(|e| std::cmp::Reverse(e.created_at));
        all
    }

    pub fn upsert(&mut self, entry: ReleaseIndexEntry) -> rk_core::Result<()> {
        self.entries.insert(entry.id.clone(), entry);
        self.persist()
    }

    /// Persist the digest before publication: fsync the temporary file, rename, then sync its
    /// directory.
    fn persist(&self) -> rk_core::Result<()> {
        use std::io::Write;
        let Some(parent) = self.path.parent() else {
            return Err(rk_core::Error::other(
                "release registry path has no parent directory",
            ));
        };
        std::fs::create_dir_all(parent)?;
        let tmp = self.path.with_extension("json.tmp");
        {
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&serde_json::to_vec_pretty(&self.entries)?)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        sync_directory(parent)?;
        Ok(())
    }
}

/// A Preparing entry with no in-flight prepare lock is stale intent, reported as Unknown.
pub fn effective_status(entry: &ReleaseIndexEntry, lock_is_free: bool) -> ReleaseStatus {
    if entry.status == ReleaseStatus::Preparing && lock_is_free {
        ReleaseStatus::Unknown
    } else {
        entry.status
    }
}

fn compute_input_key(
    repo: &str,
    resolved_commit: &str,
    recipe: &str,
    recipe_revision: u32,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"rat-kingdom-release-input-v1\0");
    hasher.update(repo.as_bytes());
    hasher.update(b"\0");
    hasher.update(resolved_commit.as_bytes());
    hasher.update(b"\0");
    hasher.update(recipe.as_bytes());
    hasher.update(b"\0");
    hasher.update(recipe_revision.to_le_bytes());
    hex::encode(hasher.finalize())
}

fn release_id(input_key: &str) -> String {
    format!("rel-{}", &input_key[..20])
}

/// Hash the entire canonical manifest, including future fields; structs and BTreeMaps serialize
/// deterministically. The trusted digest lives in the registry, outside the manifest.
fn manifest_digest(manifest: &ReleaseManifest) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"rat-kingdom-release-manifest-v1\0");
    hasher.update(serde_json::to_vec(manifest).expect("ReleaseManifest always serializes"));
    hex::encode(hasher.finalize())
}

fn load_manifest(path: &Path) -> rk_core::Result<ReleaseManifest> {
    let bytes = std::fs::read(path)?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)?;
    let declared = value.get("schema_version").and_then(|v| v.as_u64());
    if declared != Some(u64::from(SCHEMA_VERSION)) {
        return Err(rk_core::Error::other(format!(
            "release manifest at {} declares unsupported schema_version {declared:?} \
             (this daemon supports {SCHEMA_VERSION}); refusing to trust its shape",
            path.display()
        )));
    }
    serde_json::from_value(value).map_err(|e| {
        rk_core::Error::other(format!(
            "release manifest at {} does not match schema v{SCHEMA_VERSION}: {e}",
            path.display()
        ))
    })
}

/// Require exactly the paired names and matching on-disk hashes/sizes. Identity and registry
/// digest checks belong to verify_content, not this binary-only check.
fn binaries_match_disk(release_dir: &Path, manifest: &ReleaseManifest) -> bool {
    let names: BTreeSet<&str> = manifest.binaries.keys().map(String::as_str).collect();
    let expected: BTreeSet<&str> = EXPECTED_BINARY_NAMES.into_iter().collect();
    if names != expected {
        return false;
    }
    for name in &names {
        if name.contains(['/', '\\']) || name.contains("..") {
            return false;
        }
    }
    for (name, artifact) in &manifest.binaries {
        let bytes = match std::fs::read(release_dir.join(name)) {
            Ok(b) => b,
            Err(_) => return false,
        };
        if bytes.len() as u64 != artifact.size_bytes {
            return false;
        }
        if hex::encode(Sha256::digest(&bytes)) != artifact.sha256 {
            return false;
        }
    }
    true
}

/// Verify the registry digest, matching manifest identity, and paired binary content.
fn verify_content(
    entry: &ReleaseIndexEntry,
    release_dir: &Path,
    manifest: &ReleaseManifest,
) -> bool {
    let Some(recorded_digest) = &entry.manifest_digest else {
        return false;
    };
    if manifest_digest(manifest) != *recorded_digest {
        return false;
    }
    if manifest.id != entry.id || manifest.repo != entry.repo || manifest.recipe != entry.recipe {
        return false;
    }
    binaries_match_disk(release_dir, manifest)
}

/// Return the last FAILURE_EVIDENCE_CHARS characters without splitting UTF-8.
fn tail(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let total = text.chars().count();
    if total <= FAILURE_EVIDENCE_CHARS {
        return text.into_owned();
    }
    text.chars().skip(total - FAILURE_EVIDENCE_CHARS).collect()
}

/// Sync directory entries; propagate failures except Unsupported. On unsupported filesystems,
/// atomic publication remains possible but prompt power-loss durability is not guaranteed.
fn sync_directory(dir: &Path) -> rk_core::Result<()> {
    let handle = std::fs::File::open(dir).map_err(|e| {
        rk_core::Error::other(format!(
            "failed to open directory {} to sync it durably: {e}",
            dir.display()
        ))
    })?;
    match handle.sync_all() {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::Unsupported => Ok(()),
        Err(e) => Err(rk_core::Error::other(format!(
            "failed to durably sync directory {}: {e}",
            dir.display()
        ))),
    }
}

fn upsert_entry(
    registry_path: &Path,
    id: &str,
    params: &PrepareParams,
    input_key: &str,
    status: ReleaseStatus,
    detail: Option<String>,
    manifest_digest: Option<String>,
) -> rk_core::Result<ReleaseIndexEntry> {
    let mut reg = ReleaseRegistry::load(registry_path)?;
    let now = Utc::now();
    let entry = match reg.get(id) {
        Some(existing) => ReleaseIndexEntry {
            status,
            updated_at: now,
            detail,
            manifest_digest: manifest_digest.or_else(|| existing.manifest_digest.clone()),
            ..existing.clone()
        },
        None => ReleaseIndexEntry {
            id: id.to_string(),
            repo: params.repo_name.clone(),
            recipe: params.recipe.clone(),
            recipe_revision: RECIPE_REVISION,
            input_key: input_key.to_string(),
            requested_source: params.requested.clone(),
            status,
            created_at: now,
            updated_at: now,
            detail,
            manifest_digest,
        },
    };
    reg.upsert(entry.clone())?;
    Ok(entry)
}

/// Shared production/test sequence: durably commit the digest while Preparing, publish the
/// manifest, then mark Prepared. Recovery trusts only that prior registry commitment.
fn publish_prepared_release(
    registry_path: &Path,
    id: &str,
    params: &PrepareParams,
    input_key: &str,
    release_dir: &Path,
    manifest: &ReleaseManifest,
) -> rk_core::Result<ReleaseIndexEntry> {
    let digest = manifest_digest(manifest);
    upsert_entry(
        registry_path,
        id,
        params,
        input_key,
        ReleaseStatus::Preparing,
        None,
        Some(digest.clone()),
    )?;
    write_manifest_new(&release_dir.join("manifest.json"), manifest)?;
    upsert_entry(
        registry_path,
        id,
        params,
        input_key,
        ReleaseStatus::Prepared,
        None,
        Some(digest),
    )
}

/// Prepare or reuse a release. The caller must serialize access to its shared staging worktree.
///
/// `admission`: `Some` when `[policy] release_build_admission_enabled` is on
/// — the build subprocess acquires one [`HostVerificationAdmission`] permit,
/// participating in the same aggregate host-wide cap every managed named
/// check already shares, before it spawns. `None` (the default) preserves
/// the unmanaged legacy path exactly.
pub(crate) async fn prepare(
    layout: &Layout,
    params: PrepareParams,
    admission: Option<&HostVerificationAdmission>,
) -> rk_core::Result<PrepareOutcome> {
    if params.recipe != RECIPE_PAIRED_RK_MCP {
        return Err(rk_core::Error::other(format!(
            "unsupported recipe '{}': only '{RECIPE_PAIRED_RK_MCP}' is available",
            params.recipe
        )));
    }

    // Use the caller-frozen identity for both build and proof reference.
    let repo_path = params.repo_path.clone();
    let resolved_commit = params.resolved_commit.clone();
    let tree_sha = params.tree_sha.clone();
    let mut config_provenance = {
        let repo_path = repo_path.clone();
        let resolved_commit = resolved_commit.clone();
        tokio::task::spawn_blocking(move || gather_config_provenance(&repo_path, &resolved_commit))
            .await
            .map_err(|e| rk_core::Error::other(format!("config provenance task failed: {e}")))??
    };
    config_provenance.known_verification = params.known_verification.clone();

    let input_key = compute_input_key(
        &params.repo_name,
        &resolved_commit,
        &params.recipe,
        RECIPE_REVISION,
    );
    let id = release_id(&input_key);
    let registry_path = registry_path(layout);
    let release_dir = releases_dir(layout).join(&id);
    let manifest_path = release_dir.join("manifest.json");

    let existing_entry = ReleaseRegistry::load(&registry_path)?.get(&id).cloned();

    // Recover published content only against a prior daemon-committed registry digest.
    if manifest_path.is_file() {
        let manifest = load_manifest(&manifest_path)?;
        let previously_prepared = existing_entry
            .as_ref()
            .is_some_and(|e| e.status == ReleaseStatus::Prepared);
        match &existing_entry {
            Some(entry) if verify_content(entry, &release_dir, &manifest) => {
                // This prior commitment covers both ordinary reuse and interrupted final index
                // updates.
                let entry = upsert_entry(
                    &registry_path,
                    &id,
                    &params,
                    &input_key,
                    ReleaseStatus::Prepared,
                    None,
                    entry.manifest_digest.clone(),
                )?;
                return Ok(PrepareOutcome {
                    entry,
                    manifest,
                    already_prepared: previously_prepared,
                });
            }
            Some(entry) if entry.status == ReleaseStatus::Prepared => {
                // Previously trusted and promoted, but no longer verifies
                // against its OWN recorded digest — tampered or corrupted.
                return Err(rk_core::Error::other(format!(
                    "release {id} content no longer matches its recorded manifest digest \
                     (tampered or corrupted); refusing to treat it as valid or rebuild over it"
                )));
            }
            _ => {
                // Preserve unattested content in quarantine and rebuild; self-consistency is not
                // provenance.
                let quarantine = quarantine_dir(layout, &id);
                if let Some(parent) = quarantine.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::rename(&release_dir, &quarantine)?;
            }
        }
    }

    // A missing manifest for a Prepared entry is corruption; never silently rebuild it.
    if let Some(entry) = &existing_entry {
        if entry.status == ReleaseStatus::Prepared {
            return Err(rk_core::Error::other(format!(
                "release {id} is recorded as prepared but its manifest is missing \
                 (deleted, or never durably published); refusing to silently rebuild \
                 under the same identity — this requires operator investigation"
            )));
        }
    }

    // Durable intent BEFORE the bounded build: a crash here leaves a
    // `Preparing` record, not silence.
    upsert_entry(
        &registry_path,
        &id,
        &params,
        &input_key,
        ReleaseStatus::Preparing,
        None,
        None,
    )?;

    match run_recipe(
        layout,
        &repo_path,
        &params.repo_name,
        &resolved_commit,
        &tree_sha,
        &params.requested,
        &id,
        &release_dir,
        config_provenance,
        admission,
    )
    .await
    {
        Ok(manifest) => {
            let entry = publish_prepared_release(
                &registry_path,
                &id,
                &params,
                &input_key,
                &release_dir,
                &manifest,
            )?;
            Ok(PrepareOutcome {
                entry,
                manifest,
                already_prepared: false,
            })
        }
        Err(e) => {
            upsert_entry(
                &registry_path,
                &id,
                &params,
                &input_key,
                ReleaseStatus::Failed,
                Some(e.to_string()),
                None,
            )?;
            Err(e)
        }
    }
}

/// Read a source-tree blob at an exact commit without modifying a worktree.
enum BlobObservation {
    Present(Vec<u8>),
    Absent,
    Unavailable(String),
}

/// Distinguish missing paths from Git/I/O failure: only the documented missing-path status
/// and diagnostic mean Absent; unavailable observations must remain explicit.
fn read_blob_at(repo_path: &Path, sha: &str, rel_path: &str) -> BlobObservation {
    let spec = format!("{sha}:{rel_path}");
    let mut cat_file_cmd = std::process::Command::new("git");
    cat_file_cmd
        .arg("-C")
        .arg(repo_path)
        .arg("cat-file")
        .arg("-e")
        .arg(&spec);
    rk_core::exec::close_extra_fds(&mut cat_file_cmd);
    match cat_file_cmd.output() {
        Ok(out) if out.status.code() == Some(0) => {}
        Ok(out)
            if out.status.code() == Some(128)
                && String::from_utf8_lossy(&out.stderr).contains("does not exist in") =>
        {
            return BlobObservation::Absent;
        }
        Ok(out) => {
            return BlobObservation::Unavailable(format!(
                "git cat-file -e {spec} exited {:?}: {}",
                out.status.code(),
                String::from_utf8_lossy(&out.stderr)
            ))
        }
        Err(e) => {
            return BlobObservation::Unavailable(format!(
                "git cat-file -e {spec} failed to run: {e}"
            ))
        }
    }
    let mut show_cmd = std::process::Command::new("git");
    show_cmd.arg("-C").arg(repo_path).arg("show").arg(&spec);
    rk_core::exec::close_extra_fds(&mut show_cmd);
    match show_cmd.output() {
        Ok(out) if out.status.success() => BlobObservation::Present(out.stdout),
        Ok(out) => BlobObservation::Unavailable(format!(
            "git show {spec} exited {:?} even though cat-file confirmed it exists: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        )),
        Err(e) => BlobObservation::Unavailable(format!("git show {spec} failed to run: {e}")),
    }
}

/// Determine mise use from the exact tree; unavailable observations have no safe default.
fn mise_present(repo_path: &Path, sha: &str) -> rk_core::Result<bool> {
    for name in ["mise.toml", ".mise.toml"] {
        match read_blob_at(repo_path, sha, name) {
            BlobObservation::Present(_) => return Ok(true),
            BlobObservation::Absent => continue,
            BlobObservation::Unavailable(reason) => {
                return Err(rk_core::Error::other(format!(
                    "could not determine whether {sha} carries {name}, so the build recipe \
                     (mise vs. plain cargo) cannot be safely selected: {reason}"
                )));
            }
        }
    }
    Ok(false)
}

fn file_observation(repo_path: &Path, sha: &str, rel_path: &str) -> FileObservation {
    match read_blob_at(repo_path, sha, rel_path) {
        BlobObservation::Present(bytes) => FileObservation::Present {
            sha256: hex::encode(Sha256::digest(&bytes)),
        },
        BlobObservation::Absent => FileObservation::Absent,
        BlobObservation::Unavailable(reason) => FileObservation::Unavailable { reason },
    }
}

/// Observe config at the frozen commit; the caller supplies any exact proof reference.
fn gather_config_provenance(
    repo_path: &Path,
    resolved_commit: &str,
) -> rk_core::Result<ConfigProvenance> {
    Ok(ConfigProvenance {
        cargo_build_jobs_env: CARGO_BUILD_JOBS.to_string(),
        cargo_incremental_env: "0".to_string(),
        used_mise: mise_present(repo_path, resolved_commit)?,
        repo_policy: file_observation(repo_path, resolved_commit, ".rk/repo.cue"),
        checks_registry: file_observation(repo_path, resolved_commit, ".rk/checks.cue"),
        known_verification: None,
        compatibility_checked: false,
    })
}

/// Bind the owned target directory and job limit explicitly so ambient Cargo settings cannot win.
fn build_script(use_mise: bool, target_dir: &Path) -> String {
    let target_dir = target_dir.display();
    if use_mise {
        format!(
            "set -e\n\
             export MISE_TRUSTED_CONFIG_PATHS=\"$PWD\"\n\
             nice -n {NICE_LEVEL} mise exec -- cargo build --release --target-dir '{target_dir}' \
             --jobs {CARGO_BUILD_JOBS} -p rk-cli -p rk-mcp\n\
             mise exec -- rustc --version > .rk-release-toolchain.txt\n"
        )
    } else {
        format!(
            "set -e\n\
             nice -n {NICE_LEVEL} cargo build --release --target-dir '{target_dir}' \
             --jobs {CARGO_BUILD_JOBS} -p rk-cli -p rk-mcp\n\
             rustc --version > .rk-release-toolchain.txt\n"
        )
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_recipe(
    layout: &Layout,
    repo_path: &Path,
    repo_name: &str,
    resolved_commit: &str,
    tree_sha: &str,
    requested: &str,
    id: &str,
    release_dir: &Path,
    config_provenance: ConfigProvenance,
    admission: Option<&HostVerificationAdmission>,
) -> rk_core::Result<ReleaseManifest> {
    let staging = staging_dir(layout, repo_name);
    {
        let repo_path = repo_path.to_path_buf();
        let staging = staging.clone();
        let resolved_commit = resolved_commit.to_string();
        tokio::task::spawn_blocking(move || -> rk_core::Result<()> {
            let repo = rk_git::Repo::discover(&repo_path)?;
            // Reuse the detached staging worktree and retain its ignored build cache.
            repo.ensure_gate_worktree(&staging)?;
            repo.reset_gate_worktree(&staging, &resolved_commit)?;
            Ok(())
        })
        .await
        .map_err(|e| rk_core::Error::other(format!("staging worktree task failed: {e}")))??;
    }

    // The recipe must write to the owned directory from which its binaries are read.
    let target_dir = staging.join("target");
    // Mise selection was already frozen from the selected source tree.
    let script = build_script(config_provenance.used_mise, &target_dir);

    // P4.1 (TKT-nibuv-gokun-sibin): acquire one bounded host-wide admission
    // permit, participating in the SAME aggregate cap every managed named
    // check already shares, before spawning the build subprocess. The
    // immutable selected source (`resolved_commit`/`tree_sha`, already
    // frozen in `PrepareParams` before `prepare` ever called this function)
    // is untouched while waiting — nothing here re-resolves or mutates it.
    // Held across the entire build execution below and released, via plain
    // RAII drop, on every exit path from this function (success, build
    // failure, timeout, or this future itself being dropped) — no explicit
    // release call, same convention `ManagedVerification::run` uses for its
    // own `_host_guard`.
    let admission_wait_started = Instant::now();
    let host_permit = match admission {
        Some(host_admission) => {
            match tokio::time::timeout(
                ADMISSION_WAIT_TIMEOUT,
                host_admission.acquire(RELEASE_ADMISSION_IDENTITY),
            )
            .await
            {
                Ok(permit) => permit,
                Err(_) => {
                    return Err(rk_core::Error::other(format!(
                        "release build did not acquire host verification admission capacity \
                         within {}s (aggregate cap saturated); the selected source \
                         {resolved_commit} was never built, not partially built",
                        ADMISSION_WAIT_TIMEOUT.as_secs()
                    )));
                }
            }
        }
        None => None,
    };
    let admission_wait_ms =
        u64::try_from(admission_wait_started.elapsed().as_millis()).unwrap_or(u64::MAX);

    let mut command = tokio::process::Command::new("sh");
    command
        .arg("-c")
        .arg(&script)
        .current_dir(&staging)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .env("CARGO_TARGET_DIR", &target_dir)
        .env("CARGO_BUILD_JOBS", CARGO_BUILD_JOBS.to_string())
        .env("CARGO_INCREMENTAL", "0")
        .process_group(0);
    // Guard concurrent captured-pipe inheritance; see rk_core::exec::close_extra_fds.
    rk_core::exec::close_extra_fds(command.as_std_mut());
    let build_started = Instant::now();
    let child = command.spawn().map_err(|e| {
        rk_core::Error::other(format!("release build: failed to spawn recipe: {e}"))
    })?;
    let _marker = child
        .id()
        .map(|pid| ManagedChildMarker::create(layout, pid));
    let outcome = collect_child_output(child, BUILD_TIMEOUT, "release build").await?;
    let build_run_ms = u64::try_from(build_started.elapsed().as_millis()).unwrap_or(u64::MAX);
    // The build itself has fully exited (or was killed on timeout, below) —
    // this permit's job is done; drop it before the (comparatively slow)
    // binary-copy/smoke-check steps that follow so a saturated aggregate
    // cap never waits on those too.
    drop(host_permit);
    match outcome {
        RunOutcome::TimedOut => {
            return Err(rk_core::Error::other(format!(
                "release build exceeded its {}s bound and was killed",
                BUILD_TIMEOUT.as_secs()
            )));
        }
        RunOutcome::Completed {
            status,
            stdout,
            stderr,
            ..
        } => {
            if !status.success() {
                return Err(rk_core::Error::other(format!(
                    "release build recipe exited {:?}\nstdout(tail): {}\nstderr(tail): {}",
                    status.code(),
                    tail(&stdout),
                    tail(&stderr)
                )));
            }
        }
    }

    let toolchain = std::fs::read_to_string(staging.join(".rk-release-toolchain.txt"))
        .unwrap_or_default()
        .trim()
        .to_string();

    let target_release = target_dir.join("release");
    if release_dir.exists() {
        // Preserve failed partial output for inspection before retrying this release id.
        let quarantine = quarantine_dir(layout, id);
        if let Some(parent) = quarantine.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::rename(release_dir, &quarantine)?;
    }
    std::fs::create_dir_all(release_dir)?;

    let mut binaries = BTreeMap::new();
    for name in EXPECTED_BINARY_NAMES {
        let src = target_release.join(name);
        let bytes = std::fs::read(&src).map_err(|e| {
            rk_core::Error::other(format!(
                "release recipe did not produce expected binary {}: {e}",
                src.display()
            ))
        })?;
        let sha256 = hex::encode(Sha256::digest(&bytes));
        let dest = release_dir.join(name);
        write_binary_durably(&dest, &bytes)?;
        binaries.insert(
            name.to_string(),
            BinaryArtifact {
                sha256,
                size_bytes: bytes.len() as u64,
            },
        );
    }
    // Sync binary directory entries before committing the manifest digest.
    sync_directory(release_dir)?;

    let smoke_home = smoke_home_dir(layout, id);
    std::fs::create_dir_all(&smoke_home)?;
    let mut checks = Vec::new();
    let smoke_result: rk_core::Result<()> = async {
        for name in EXPECTED_BINARY_NAMES {
            checks.push(run_smoke_check(layout, &release_dir.join(name), name, &smoke_home).await?);
        }
        Ok(())
    }
    .await;
    let _ = std::fs::remove_dir_all(&smoke_home);
    smoke_result?;

    let manifest = ReleaseManifest {
        schema_version: SCHEMA_VERSION,
        id: id.to_string(),
        repo: repo_name.to_string(),
        recipe: RECIPE_PAIRED_RK_MCP.to_string(),
        recipe_revision: RECIPE_REVISION,
        source: SourceProvenance {
            requested: requested.to_string(),
            resolved_commit: resolved_commit.to_string(),
            tree_sha: tree_sha.to_string(),
        },
        toolchain,
        binaries,
        recipe_bounds: RecipeBounds {
            cargo_build_jobs: CARGO_BUILD_JOBS,
            nice: NICE_LEVEL,
            timeout_secs: BUILD_TIMEOUT.as_secs(),
            enforcement_note: if admission.is_some() {
                "process-wide single-flight lock on release.prepare; participates in the \
                    HostVerificationAdmission aggregate cap (see host_admission below) at its \
                    own configured weight; still not a host-wide CPU quota and not an \
                    immutable-execution guarantee — nice/jobs bounds remain process-local only"
                    .to_string()
            } else {
                "process-wide single-flight lock on release.prepare; not a \
                    host-wide CPU quota, not the HostVerificationAdmission cap, and not an \
                    immutable-execution guarantee"
                    .to_string()
            },
            host_admission: admission.map(|host_admission| HostAdmissionBounds {
                recipe_identity: RELEASE_ADMISSION_IDENTITY.to_string(),
                weight: host_admission.weight_for(RELEASE_ADMISSION_IDENTITY),
                admission_wait_ms,
                build_run_ms: Some(build_run_ms),
            }),
        },
        config_provenance,
        checks,
        created_at: Utc::now(),
    };
    // The caller commits this manifest digest before publishing the manifest itself.
    Ok(manifest)
}

/// `rk-mcp`'s real minimal handshake, matching the operator recipe's own
/// smoke proof: one JSON-RPC `initialize` request, `id: 1`, empty object
/// params (`rk_mcp::handle_request` refuses a non-object `params`).
const MCP_INITIALIZE_REQUEST: &[u8] =
    br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;

async fn run_smoke_check(
    layout: &Layout,
    bin_path: &Path,
    name: &str,
    smoke_home: &Path,
) -> rk_core::Result<CheckEvidence> {
    let mut command = tokio::process::Command::new(bin_path);
    if name == "rk-mcp" {
        command.stdin(std::process::Stdio::piped());
    } else {
        command
            .args(RK_SMOKE_ARGV)
            .stdin(std::process::Stdio::null());
    }
    // Run arbitrary candidate bytes with a cleared environment and isolated RK_HOME.
    command
        .env_clear()
        .env("RK_HOME", smoke_home)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .process_group(0);
    if let Some(path) = std::env::var_os("PATH") {
        command.env("PATH", path);
    }
    // See `rk_core::exec::close_extra_fds`'s doc comment: this spawn also
    // captures stdout/stderr via `Stdio::piped()`, the same leaked-pipe
    // hang shape `run_recipe`'s build spawn is guarded against above.
    rk_core::exec::close_extra_fds(command.as_std_mut());
    let mut child = command.spawn().map_err(|e| {
        rk_core::Error::other(format!(
            "release smoke check for {name}: failed to spawn: {e}"
        ))
    })?;
    if name == "rk-mcp" {
        use tokio::io::AsyncWriteExt;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(MCP_INITIALIZE_REQUEST).await.map_err(|e| {
                rk_core::Error::other(format!(
                    "release smoke check for {name}: stdin write failed: {e}"
                ))
            })?;
            stdin.write_all(b"\n").await.ok();
            // Dropping the handle here closes the write half, giving the
            // child EOF on stdin once it has read this one line — exactly
            // how a real MCP client's shutdown looks to `rk_mcp::serve`.
        }
    }
    let _marker = child
        .id()
        .map(|pid| ManagedChildMarker::create(layout, pid));
    let label = format!("release smoke: {name}");
    let outcome = collect_child_output(child, SMOKE_TIMEOUT, &label).await?;
    let (exit_code, passed, stdout, stderr) = match outcome {
        RunOutcome::TimedOut => (None, false, Vec::new(), Vec::new()),
        RunOutcome::Completed {
            status,
            stdout,
            stderr,
            ..
        } => (status.code(), status.success(), stdout, stderr),
    };
    if !passed {
        return Err(rk_core::Error::other(format!(
            "release smoke check failed for {name}: exit {exit_code:?}\nstderr(tail): {}",
            tail(&stderr)
        )));
    }
    if name == "rk-mcp" {
        verify_mcp_initialize_response(&stdout)?;
    }
    Ok(CheckEvidence {
        name: format!("smoke:{name}"),
        command: if name == "rk-mcp" {
            format!("{name} <initialize over stdin>")
        } else {
            format!("{name} {}", RK_SMOKE_ARGV.join(" "))
        },
        exit_code,
        passed,
        observed_at: Utc::now(),
    })
}

/// Require a successful JSON-RPC initialize response; exit zero alone is insufficient.
fn verify_mcp_initialize_response(stdout: &[u8]) -> rk_core::Result<()> {
    let line = stdout
        .split(|&b| b == b'\n')
        .find(|line| !line.is_empty())
        .ok_or_else(|| rk_core::Error::other("mcp smoke: rk-mcp produced no response line"))?;
    let value: serde_json::Value = serde_json::from_slice(line).map_err(|e| {
        rk_core::Error::other(format!("mcp smoke: response is not valid JSON: {e}"))
    })?;
    if value.get("id").and_then(serde_json::Value::as_i64) != Some(1) {
        return Err(rk_core::Error::other(format!(
            "mcp smoke: response id did not match the request: {value}"
        )));
    }
    if let Some(error) = value.get("error") {
        return Err(rk_core::Error::other(format!(
            "mcp smoke: initialize returned an error: {error}"
        )));
    }
    let result = value
        .get("result")
        .ok_or_else(|| rk_core::Error::other("mcp smoke: response has neither result nor error"))?;
    if result
        .get("protocolVersion")
        .and_then(serde_json::Value::as_str)
        .is_none()
    {
        return Err(rk_core::Error::other(format!(
            "mcp smoke: result missing protocolVersion: {result}"
        )));
    }
    Ok(())
}

/// Write and fsync each binary before returning to the publication sequence.
fn write_binary_durably(path: &Path, bytes: &[u8]) -> rk_core::Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o755))?;
    }
    Ok(())
}

/// Fsync complete temporary content, then hard-link it into place without replacing an existing
/// manifest. Readers must never see a partially written file at the trusted path.
fn write_manifest_new(path: &Path, manifest: &ReleaseManifest) -> rk_core::Result<()> {
    use std::io::Write;
    let dir = path
        .parent()
        .ok_or_else(|| rk_core::Error::other("manifest path has no parent directory"))?;
    let bytes = serde_json::to_vec_pretty(manifest)?;
    let tmp = dir.join(format!(".manifest-{}.tmp", std::process::id()));
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&bytes)?;
        // Flush the complete file before publishing its directory entry.
        file.sync_all()?;
    }
    let result = std::fs::hard_link(&tmp, path);
    let _ = std::fs::remove_file(&tmp);
    result.map_err(|e| {
        rk_core::Error::other(format!(
            "failed to publish manifest at {}: {e}",
            path.display()
        ))
    })?;
    // Sync the published hard-link entry to complete the durability boundary.
    sync_directory(dir)?;
    Ok(())
}

/// Deterministic release id for `(repo, resolved_commit, recipe)` at the
/// current [`RECIPE_REVISION`], without touching the registry or building
/// anything. Lets a read-only caller (`release.status`) check whether a
/// given commit already has a recorded release without ever calling
/// [`prepare`] itself.
pub fn id_for(repo: &str, resolved_commit: &str, recipe: &str) -> String {
    release_id(&compute_input_key(
        repo,
        resolved_commit,
        recipe,
        RECIPE_REVISION,
    ))
}

pub fn list(layout: &Layout, repo: Option<&str>) -> rk_core::Result<Vec<ReleaseIndexEntry>> {
    Ok(ReleaseRegistry::load(&registry_path(layout))?.list(repo))
}

pub fn show(layout: &Layout, id: &str) -> rk_core::Result<Option<ShowResult>> {
    let reg = ReleaseRegistry::load(&registry_path(layout))?;
    let Some(entry) = reg.get(id).cloned() else {
        return Ok(None);
    };
    let release_dir = releases_dir(layout).join(id);
    let manifest_path = release_dir.join("manifest.json");
    if !manifest_path.is_file() {
        return Ok(Some(ShowResult {
            entry,
            manifest: None,
            content_verified: None,
        }));
    }
    let manifest = load_manifest(&manifest_path)?;
    let content_verified = verify_content(&entry, &release_dir, &manifest);
    Ok(Some(ShowResult {
        entry,
        manifest: Some(manifest),
        content_verified: Some(content_verified),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_input_produces_the_same_id() {
        let a = compute_input_key("repo", "abc123", RECIPE_PAIRED_RK_MCP, RECIPE_REVISION);
        let b = compute_input_key("repo", "abc123", RECIPE_PAIRED_RK_MCP, RECIPE_REVISION);
        assert_eq!(a, b);
        assert_eq!(release_id(&a), release_id(&b));
    }

    #[test]
    fn different_source_or_repo_or_recipe_or_revision_changes_the_id() {
        let base = compute_input_key("repo", "abc123", RECIPE_PAIRED_RK_MCP, RECIPE_REVISION);
        assert_ne!(
            base,
            compute_input_key(
                "other-repo",
                "abc123",
                RECIPE_PAIRED_RK_MCP,
                RECIPE_REVISION
            )
        );
        assert_ne!(
            base,
            compute_input_key("repo", "def456", RECIPE_PAIRED_RK_MCP, RECIPE_REVISION)
        );
        assert_ne!(
            base,
            compute_input_key("repo", "abc123", "other-recipe", RECIPE_REVISION)
        );
        assert_ne!(
            base,
            compute_input_key("repo", "abc123", RECIPE_PAIRED_RK_MCP, RECIPE_REVISION + 1)
        );
    }

    #[test]
    fn unsupported_schema_version_is_refused_explicitly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, r#"{"schema_version": 999}"#).unwrap();
        let err = load_manifest(&path).unwrap_err().to_string();
        assert!(err.contains("unsupported schema_version"), "{err}");
    }

    #[test]
    fn registry_persists_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("releases.json");
        let params = PrepareParams {
            repo_name: "r".into(),
            repo_path: "/tmp/r".into(),
            requested: "main".into(),
            resolved_commit: "abc123".into(),
            tree_sha: "def456".into(),
            recipe: RECIPE_PAIRED_RK_MCP.into(),
            known_verification: None,
        };
        let entry = upsert_entry(
            &path,
            "rel-abc",
            &params,
            "key-abc",
            ReleaseStatus::Preparing,
            None,
            None,
        )
        .unwrap();
        assert_eq!(entry.status, ReleaseStatus::Preparing);
        let reg = ReleaseRegistry::load(&path).unwrap();
        assert_eq!(reg.get("rel-abc").unwrap().repo, "r");
    }

    #[test]
    fn effective_status_downgrades_a_stale_preparing_entry_when_no_lock_is_held() {
        let params = PrepareParams {
            repo_name: "r".into(),
            repo_path: "/tmp/r".into(),
            requested: "main".into(),
            resolved_commit: "abc123".into(),
            tree_sha: "def456".into(),
            recipe: RECIPE_PAIRED_RK_MCP.into(),
            known_verification: None,
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("releases.json");
        let entry = upsert_entry(
            &path,
            "rel-abc",
            &params,
            "key",
            ReleaseStatus::Preparing,
            None,
            None,
        )
        .unwrap();
        assert_eq!(effective_status(&entry, true), ReleaseStatus::Unknown);
        assert_eq!(effective_status(&entry, false), ReleaseStatus::Preparing);
    }

    fn sample_manifest() -> ReleaseManifest {
        let mut binaries = BTreeMap::new();
        binaries.insert(
            "rk".to_string(),
            BinaryArtifact {
                sha256: "a".repeat(64),
                size_bytes: 10,
            },
        );
        binaries.insert(
            "rk-mcp".to_string(),
            BinaryArtifact {
                sha256: "b".repeat(64),
                size_bytes: 20,
            },
        );
        ReleaseManifest {
            schema_version: SCHEMA_VERSION,
            id: "rel-test".into(),
            repo: "r".into(),
            recipe: RECIPE_PAIRED_RK_MCP.into(),
            recipe_revision: RECIPE_REVISION,
            source: SourceProvenance {
                requested: "main".into(),
                resolved_commit: "abc".into(),
                tree_sha: "def".into(),
            },
            toolchain: "rustc 1.95.0".into(),
            binaries,
            recipe_bounds: RecipeBounds {
                cargo_build_jobs: CARGO_BUILD_JOBS,
                nice: NICE_LEVEL,
                timeout_secs: BUILD_TIMEOUT.as_secs(),
                enforcement_note: "note".into(),
                host_admission: None,
            },
            config_provenance: ConfigProvenance {
                cargo_build_jobs_env: "2".into(),
                cargo_incremental_env: "0".into(),
                used_mise: false,
                repo_policy: FileObservation::Absent,
                checks_registry: FileObservation::Absent,
                known_verification: None,
                compatibility_checked: false,
            },
            checks: Vec::new(),
            created_at: Utc::now(),
        }
    }

    /// Exercise the production publication helper at the digest-committed/manifest-missing fault
    /// boundary. Recovery must see Preparing, never a falsely Prepared release.
    #[test]
    fn publication_failure_after_a_committed_digest_leaves_no_false_prepared_state() {
        let dir = tempfile::tempdir().unwrap();
        let registry_path = dir.path().join("releases.json");
        let manifest = sample_manifest();
        let digest = manifest_digest(&manifest);
        let params = PrepareParams {
            repo_name: manifest.repo.clone(),
            repo_path: "/tmp/r".into(),
            requested: "main".into(),
            resolved_commit: manifest.source.resolved_commit.clone(),
            tree_sha: manifest.source.tree_sha.clone(),
            recipe: manifest.recipe.clone(),
            known_verification: None,
        };

        // Inject publication failure at the actual production destination.
        let release_dir = dir.path().join("releases").join(&manifest.id);
        let manifest_path = release_dir.join("manifest.json");
        let result = publish_prepared_release(
            &registry_path,
            &manifest.id,
            &params,
            "key",
            &release_dir,
            &manifest,
        );
        assert!(
            result.is_err(),
            "publishing into a missing directory must fail loudly, not silently succeed"
        );
        assert!(
            !manifest_path.exists(),
            "no partial manifest may be left at the trusted path"
        );

        // The first durable write must retain the digest while status is still Preparing.
        let reg = ReleaseRegistry::load(&registry_path).unwrap();
        let entry = reg.get(&manifest.id).unwrap();
        assert_eq!(entry.status, ReleaseStatus::Preparing);
        assert_eq!(entry.manifest_digest.as_deref(), Some(digest.as_str()));
    }

    #[test]
    fn verify_content_rejects_an_edited_manifest_field_even_without_touching_binaries() {
        let manifest = sample_manifest();
        let digest = manifest_digest(&manifest);
        let entry = ReleaseIndexEntry {
            id: manifest.id.clone(),
            repo: manifest.repo.clone(),
            recipe: manifest.recipe.clone(),
            recipe_revision: manifest.recipe_revision,
            input_key: "key".into(),
            requested_source: "main".into(),
            status: ReleaseStatus::Prepared,
            created_at: manifest.created_at,
            updated_at: manifest.created_at,
            detail: None,
            manifest_digest: Some(digest),
        };
        // No filesystem I/O possible (binaries don't exist on disk here), so
        // this only exercises the digest/identity gate, not the hash re-read.
        let mut tampered = manifest.clone();
        tampered.repo = "someone-elses-repo".into();
        assert!(!verify_content(
            &entry,
            Path::new("/nonexistent"),
            &tampered
        ));
    }

    #[test]
    fn verify_content_rejects_an_extra_or_path_traversal_binary_key() {
        let mut manifest = sample_manifest();
        let entry_digest = manifest_digest(&manifest);
        manifest.binaries.insert(
            "../evil".to_string(),
            BinaryArtifact {
                sha256: "c".repeat(64),
                size_bytes: 1,
            },
        );
        let entry = ReleaseIndexEntry {
            id: manifest.id.clone(),
            repo: manifest.repo.clone(),
            recipe: manifest.recipe.clone(),
            recipe_revision: manifest.recipe_revision,
            input_key: "key".into(),
            requested_source: "main".into(),
            status: ReleaseStatus::Prepared,
            created_at: manifest.created_at,
            updated_at: manifest.created_at,
            detail: None,
            manifest_digest: Some(entry_digest),
        };
        // Check the exact binary-set guard as well as the whole-manifest digest.
        assert!(!verify_content(
            &entry,
            Path::new("/nonexistent"),
            &manifest
        ));
    }

    #[test]
    fn tail_never_panics_on_a_multibyte_utf8_boundary() {
        let text = "é".repeat(FAILURE_EVIDENCE_CHARS + 10);
        let truncated = tail(text.as_bytes());
        assert_eq!(truncated.chars().count(), FAILURE_EVIDENCE_CHARS);
    }

    #[test]
    fn mcp_initialize_response_is_validated_for_shape() {
        let good = br#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05"}}"#;
        assert!(verify_mcp_initialize_response(good).is_ok());

        let wrong_id = br#"{"jsonrpc":"2.0","id":2,"result":{"protocolVersion":"x"}}"#;
        assert!(verify_mcp_initialize_response(wrong_id).is_err());

        let has_error = br#"{"jsonrpc":"2.0","id":1,"error":{"code":-1,"message":"no"}}"#;
        assert!(verify_mcp_initialize_response(has_error).is_err());

        let missing_field = br#"{"jsonrpc":"2.0","id":1,"result":{}}"#;
        assert!(verify_mcp_initialize_response(missing_field).is_err());

        assert!(verify_mcp_initialize_response(b"").is_err());
    }

    #[test]
    fn write_binary_durably_writes_exact_bytes_and_marks_it_executable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rk");
        write_binary_durably(&path, b"binary-content").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"binary-content");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o755, "{mode:o}");
        }
    }

    #[test]
    fn sync_directory_succeeds_on_an_ordinary_directory() {
        let dir = tempfile::tempdir().unwrap();
        assert!(sync_directory(dir.path()).is_ok());
    }

    /// A nonexistent directory forces a real sync failure independent of host permission
    /// privileges.
    #[test]
    fn sync_directory_propagates_a_real_open_failure() {
        let missing = Path::new("/nonexistent-rk-release-sync-directory-test-path");
        let err = sync_directory(missing).unwrap_err();
        assert!(err.to_string().contains("failed to open"), "{err}");
    }
}
