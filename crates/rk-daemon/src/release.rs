//! Immutable paired RK/MCP release preparation and inspection (P6.1).
//!
//! A release binds one registered repository's exact source commit to a pair
//! of content-hashed binaries (`rk`, `rk-mcp`) built together through a single
//! fixed, bounded recipe. Nothing here activates a release, stops the running
//! daemon, or treats a prepared release as installed — that is P7's job.
//!
//! Durable state is two plain files/dirs the daemon owns (mirroring
//! [`crate::repos`]'s `repos.json`, not the tuplespace, since both are
//! machine-local):
//!
//! - `<home>/releases.json`: an index of every attempted release
//!   ([`ReleaseIndexEntry`]), keyed by content-derived id. Written *before*
//!   the bounded build starts (`status: preparing`) so a crash mid-build
//!   leaves an inspectable record instead of silence.
//! - `<home>/releases/<id>/`: the frozen binaries plus `manifest.json`
//!   ([`ReleaseManifest`]), published atomically (temp file + hard link, never
//!   `create_new` + separate `write_all` — see [`write_manifest_new`]) and
//!   never overwritten once its digest is recorded in the registry entry.
//!
//! The release id is `rel-<sha256(repo, resolved source commit, recipe,
//! recipe_revision)>`, not a random identifier: preparing the same bound
//! input twice always resolves to the same id, which is what makes
//! idempotency a lookup instead of a separate dedup mechanism.
//! `recipe_revision` is bumped whenever this module changes what the named
//! recipe actually *does* (packages built, smoke protocol, bounds), so an old
//! manifest can never be silently reused as if it reflected new recipe
//! behavior merely because its name string didn't change.
//!
//! Binary bytes and check evidence are always produced by this module's own
//! execution, never accepted as caller-supplied claims — there is no field
//! anywhere in [`ReleaseManifest`] a caller can set to assert "this hash
//! matches" or "this check passed". [`verify_content`] re-derives everything
//! from disk (binary bytes, whole-manifest digest) before any inspection or
//! idempotent reuse trusts it; a mismatch is reported, never silently
//! accepted or overwritten.
//!
//! What this module explicitly does NOT claim (see
//! [`ConfigProvenance::compatibility_checked`] and the module-level docs
//! shipped alongside it): a prepared release's bounded CLI/MCP smoke checks
//! prove the binaries launch and speak their minimal protocol, not that they
//! are compatible with any particular runtime environment. An idempotent
//! reuse compares the exact bound input (repo, resolved source commit,
//! recipe, recipe revision) and never re-probes whether the *current* host
//! toolchain still matches what originally built the frozen binaries; the
//! manifest's `toolchain` field is the durable record of what did.

use crate::managed_verification::{collect_child_output, ManagedChildMarker, RunOutcome};
use chrono::{DateTime, Utc};
use rk_core::paths::Layout;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Bumped whenever [`ReleaseManifest`]'s shape changes incompatibly. A stored
/// manifest declaring any other value is refused outright rather than guessed
/// at — see [`load_manifest`].
pub const SCHEMA_VERSION: u32 = 1;

/// The only recipe this first slice supports: build the `rk-cli` and `rk-mcp`
/// packages together, producing the `rk`/`rk-mcp` binaries. Matches the
/// current operator build/install path (`scripts/install.sh`), extended with
/// the bounds this ticket requires.
pub const RECIPE_PAIRED_RK_MCP: &str = "paired-rk-mcp";

/// Bumped whenever this module changes what `RECIPE_PAIRED_RK_MCP` actually
/// does (packages built, smoke protocol, bounds) — folded into the id/input
/// key so a behavior change can never be silently reused under the same
/// recipe name. See the module doc for why a name string alone is not enough.
pub const RECIPE_REVISION: u32 = 1;

const BUILD_TIMEOUT: Duration = Duration::from_secs(20 * 60);
const SMOKE_TIMEOUT: Duration = Duration::from_secs(10);
const CARGO_BUILD_JOBS: u32 = 2;
const NICE_LEVEL: i32 = 10;
/// Bound on stdout/stderr retained in a failure message, mirroring
/// `managed_verification::GATE_EVIDENCE_LIMIT`'s reasoning: generous enough to
/// carry a compiler error, bounded so a runaway build cannot blow up the
/// response. Measured in chars, not bytes, so truncation is always char-
/// boundary safe (see [`tail`]).
const FAILURE_EVIDENCE_CHARS: usize = 4000;

const EXPECTED_BINARY_NAMES: [&str; 2] = ["rk", "rk-mcp"];
/// `rk`'s smoke argv: clap's built-in `--help` exits 0 with no side effects
/// and no daemon connection. `rk-mcp` takes no argv — its smoke check instead
/// sends one real `initialize` request over stdin (see `run_smoke_check`),
/// matching the operator recipe's own MCP smoke proof
/// (`p9-release-mcp-smoke-*` / `budget-release-mcp-smoke.json` artifacts).
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
    /// Present once `status == Prepared`: [`manifest_digest`] computed over
    /// the whole manifest at write time and re-checked on every read, so an
    /// in-place edit to `manifest.json` — a hash, an id, the schema version,
    /// anything — is detected as tampering rather than trusted.
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
    /// Explicit statement of what this bound does NOT provide, since the
    /// ticket requires distinguishing enforcement from aspiration: this is a
    /// single process-wide lock serializing `release.prepare` calls, not the
    /// P3.1 `HostVerificationAdmission` aggregate cap and not a host-wide CPU
    /// quota or immutable-execution guarantee.
    pub enforcement_note: String,
}

/// Effective build-environment facts this module actually observed, kept
/// distinct from what it does not verify (see `compatibility_checked`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigProvenance {
    pub cargo_build_jobs_env: String,
    pub cargo_incremental_env: String,
    /// Observed: whether the resolved source commit's tree carried a
    /// `mise.toml`/`.mise.toml` (read via `git show <sha>:<path>`, a pure
    /// function of the exact source, not of whatever the persistent staging
    /// worktree happened to have checked out before this call), which
    /// decided whether the recipe ran `cargo`/`rustc` directly or through
    /// `mise exec --`.
    pub used_mise: bool,
    /// sha256 of `.rk/repo.cue` at the resolved commit, when present — a
    /// non-secret fingerprint of the effective repository delivery policy
    /// this exact source tree carries. `None` means the tree has no such
    /// file at that commit, not that this wasn't checked.
    pub repo_policy_digest: Option<String>,
    /// sha256 of `.rk/checks.cue` at the resolved commit, when present.
    pub checks_registry_digest: Option<String>,
    /// A reference to an existing exact-key managed-verification proof for
    /// this resolved commit (`crate::managed_verification::
    /// lookup_verification_proof`/`verification_proof_key`), when this
    /// module is wired to look one up. Always `None` in this slice:
    /// `release::prepare` deliberately does not take the `Space`/
    /// `VerificationResources` plumbing that lookup needs, because the
    /// alternative path available today (`WorkflowEngine::verify_repo_check`)
    /// does not merely read a cache — on a miss it EXECUTES the named check,
    /// which could silently run a full `verify` suite as a hidden side
    /// effect of preparing a release. `None` here means "not looked up",
    /// never "checked and none exists"; wiring in the read-only cache lookup
    /// without that execution risk is a tracked, bounded follow-up.
    pub known_verification: Option<serde_json::Value>,
    /// Always `false` in this slice. Explicit rather than absent: the bounded
    /// smoke checks below prove the frozen binaries launch and speak their
    /// minimal protocol, not that they are compatible with any particular
    /// runtime environment, config, or host. Do not infer compatibility from
    /// its absence — this field states the limitation outright.
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
    /// Bounded CLI/MCP smoke evidence this module observed directly. This is
    /// NOT the full `verify` check suite — the manifest does not claim full
    /// correctness-check coverage, only that the frozen binaries launched and
    /// exited cleanly under a real minimal invocation of each.
    pub checks: Vec<CheckEvidence>,
    pub created_at: DateTime<Utc>,
}

/// A caller-supplied request to prepare (or idempotently return) one release.
pub struct PrepareParams {
    pub repo_name: String,
    pub repo_path: PathBuf,
    pub candidate: String,
    pub recipe: String,
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
    /// `Some(false)` means the manifest exists but its recorded digest or
    /// binary hashes no longer match what's on disk (tampered or corrupted).
    /// `None` means there is no manifest to check yet (still preparing, or
    /// failed before ever producing one).
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

/// A scratch `RK_HOME` for smoke-checking freshly built binaries, isolated
/// from the daemon's own production home. The candidate source is arbitrary
/// (that's the entire point of testing it); it must never run against the
/// live fleet's state even incidentally. Cleaned up best-effort after use.
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

    fn persist(&self) -> rk_core::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&self.entries)?)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

/// A `Preparing` entry is only "live" while some in-process call actually
/// holds the release-prepare lock. Called from a read path with that lock's
/// `try_lock` result: if the lock is free, nothing is building right now, so
/// a persisted `Preparing` status is stale intent left by a crash or an
/// errored attempt that never reached `Failed` — surfaced as `Unknown`
/// instead of implying an active build that no longer exists.
pub fn effective_status(entry: &ReleaseIndexEntry, lock_is_free: bool) -> ReleaseStatus {
    if entry.status == ReleaseStatus::Preparing && lock_is_free {
        ReleaseStatus::Unknown
    } else {
        entry.status
    }
}

fn compute_input_key(repo: &str, resolved_commit: &str, recipe: &str, recipe_revision: u32) -> String {
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

/// Whole-manifest tamper-evidence digest, recorded in the registry entry (not
/// inside the manifest itself, to avoid the bootstrapping problem of a digest
/// covering its own field) and recomputed on every read.
///
/// Hashes the manifest's own canonical serialized bytes rather than a
/// hand-picked field list: `serde_json`'s struct serialization is field-order
/// deterministic and `BTreeMap` serializes in sorted key order, so this is
/// stable across writes — and, unlike an enumerated field list, it cannot
/// silently stop covering a field that gets added later or was simply missed
/// (an earlier draft of this function hashed the fields individually and left
/// out `recipe_bounds.enforcement_note`, both `config_provenance` string
/// fields, per-check `observed_at`, and `created_at` — all editable without
/// detection). `expect` is safe: every field in `ReleaseManifest` is a plain
/// serializable type, so serialization cannot fail.
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

/// The binary set must be EXACTLY the paired names (no extra, no missing, no
/// path-traversal-shaped key — these join straight onto `release_dir`), and
/// each binary's bytes on disk must match its manifest-recorded hash/size.
/// `false` (not an error) is the expected shape of "content has drifted"; the
/// caller decides what that means. This alone does NOT check the manifest's
/// own identity fields or its digest — see [`verify_content`] for the
/// registry-trusted case and [`manifest_matches_requested_identity`] for the
/// no-prior-trust recovery case.
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

/// Full trust check for an on-disk release the registry already has a
/// recorded digest for: the digest covers every manifest field (so an edited
/// id/repo/hash/schema is caught), the manifest's identity fields must match
/// the registry entry's, and the binaries on disk must match.
fn verify_content(entry: &ReleaseIndexEntry, release_dir: &Path, manifest: &ReleaseManifest) -> bool {
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

/// Structural self-consistency check for a manifest found on disk with no
/// prior trusted registry digest to compare against (the crash-recovery
/// window between publishing `manifest.json` and updating the registry to
/// `Prepared` — see the doc comment in `prepare`). Requires the manifest to
/// claim to BE exactly this bound request before it is trusted enough to
/// adopt.
fn manifest_matches_requested_identity(
    manifest: &ReleaseManifest,
    id: &str,
    repo: &str,
    recipe: &str,
    resolved_commit: &str,
) -> bool {
    manifest.id == id
        && manifest.repo == repo
        && manifest.recipe == recipe
        && manifest.source.resolved_commit == resolved_commit
}

/// Char-boundary-safe tail of the last `FAILURE_EVIDENCE_CHARS` characters —
/// used only for human-readable failure messages, never for identity or
/// comparison. Operates on `char`s throughout (never re-slices the lossily-
/// decoded `&str` at a raw byte offset), so it cannot panic on a multi-byte
/// UTF-8 boundary the way a `text[text.len() - N..]` slice can.
fn tail(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let total = text.chars().count();
    if total <= FAILURE_EVIDENCE_CHARS {
        return text.into_owned();
    }
    text.chars().skip(total - FAILURE_EVIDENCE_CHARS).collect()
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
            requested_source: params.candidate.clone(),
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

/// Prepare (or idempotently return) one immutable paired release.
///
/// Callers MUST serialize concurrent calls (the daemon does this with a
/// process-wide lock — see `Server::release_prepare_lock`): this function
/// resets a single persistent staging worktree per repo, which is not safe
/// under concurrent use.
pub async fn prepare(layout: &Layout, params: PrepareParams) -> rk_core::Result<PrepareOutcome> {
    if params.recipe != RECIPE_PAIRED_RK_MCP {
        return Err(rk_core::Error::other(format!(
            "unsupported recipe '{}': only '{RECIPE_PAIRED_RK_MCP}' is available",
            params.recipe
        )));
    }

    let repo_path = params.repo_path.clone();
    let candidate = params.candidate.clone();
    let (resolved_commit, tree_sha, config_provenance) = {
        let repo_path = repo_path.clone();
        let candidate = candidate.clone();
        tokio::task::spawn_blocking(move || -> rk_core::Result<(String, String, ConfigProvenance)> {
            let repo = rk_git::Repo::discover(&repo_path)?;
            let resolved = repo
                .rev_parse(&format!("{candidate}^{{commit}}"))
                .map_err(|e| {
                    rk_core::Error::other(format!("cannot resolve candidate '{candidate}': {e}"))
                })?;
            let tree = repo.rev_parse(&format!("{resolved}^{{tree}}"))?;
            // Pure functions of the resolved commit, read via `git show`
            // rather than the (possibly not-yet-reset) persistent staging
            // worktree — this binds config provenance to the exact source
            // tree, not to whatever a prior prepare happened to leave
            // checked out.
            let used_mise = read_blob_at(&repo_path, &resolved, "mise.toml").is_some()
                || read_blob_at(&repo_path, &resolved, ".mise.toml").is_some();
            let repo_policy_digest =
                read_blob_at(&repo_path, &resolved, ".rk/repo.cue").map(|b| hex::encode(Sha256::digest(&b)));
            let checks_registry_digest =
                read_blob_at(&repo_path, &resolved, ".rk/checks.cue").map(|b| hex::encode(Sha256::digest(&b)));
            Ok((
                resolved,
                tree,
                ConfigProvenance {
                    cargo_build_jobs_env: CARGO_BUILD_JOBS.to_string(),
                    cargo_incremental_env: "0".to_string(),
                    used_mise,
                    repo_policy_digest,
                    checks_registry_digest,
                    known_verification: None,
                    compatibility_checked: false,
                },
            ))
        })
        .await
        .map_err(|e| rk_core::Error::other(format!("source resolution task failed: {e}")))??
    };

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

    // `manifest.json` can exist here even when the registry doesn't (yet)
    // say `Prepared` for this id: `run_recipe` publishes it durably BEFORE
    // this function updates the registry (see the `Ok(manifest)` arm below),
    // so a crash in that exact window leaves a fully valid, complete manifest
    // sitting next to a stale `Preparing`/absent registry entry. Adopting it
    // (once it verifies) is strictly better than quarantining perfectly good,
    // already-built content and paying for a rebuild — but ONLY once it
    // verifies: a manifest sitting at this path that does NOT verify is
    // exactly the "tampered/incomplete" case the ticket requires rejecting,
    // never silently rebuilt over.
    if manifest_path.is_file() {
        let manifest = load_manifest(&manifest_path)?;
        let trusted = match &existing_entry {
            Some(entry) if entry.manifest_digest.is_some() => {
                verify_content(entry, &release_dir, &manifest)
            }
            _ => {
                manifest_matches_requested_identity(
                    &manifest,
                    &id,
                    &params.repo_name,
                    &params.recipe,
                    &resolved_commit,
                ) && binaries_match_disk(&release_dir, &manifest)
            }
        };
        if trusted {
            let already_prepared = existing_entry
                .as_ref()
                .is_some_and(|e| e.status == ReleaseStatus::Prepared);
            let digest = manifest_digest(&manifest);
            let entry = upsert_entry(
                &registry_path,
                &id,
                &params,
                &input_key,
                ReleaseStatus::Prepared,
                None,
                Some(digest),
            )?;
            return Ok(PrepareOutcome {
                entry,
                manifest,
                already_prepared,
            });
        }
        return Err(rk_core::Error::other(format!(
            "release {id} has a manifest.json that does not verify against its recorded \
             identity/digest and on-disk binaries (tampered, corrupted, or an incomplete \
             write); refusing to treat it as valid or rebuild over it"
        )));
    }

    // No manifest at all. A registry entry claiming `Prepared` with no
    // manifest to back it is a content-integrity failure in its own right —
    // required rejection, never a silent rebuild under the same identity
    // (which would let a deleted/corrupted "prepared" release quietly come
    // back as if nothing had happened).
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
        &candidate,
        &id,
        &release_dir,
        config_provenance,
    )
    .await
    {
        Ok(manifest) => {
            let digest = manifest_digest(&manifest);
            let entry = upsert_entry(
                &registry_path,
                &id,
                &params,
                &input_key,
                ReleaseStatus::Prepared,
                None,
                Some(digest),
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

/// Read a file's content at an exact commit via `git show <sha>:<path>`,
/// without touching any worktree. `None` covers both "absent at that commit"
/// and any other git failure equally — callers only need "present with this
/// content" vs. "not usably present", never the distinction between them.
fn read_blob_at(repo_path: &Path, sha: &str, rel_path: &str) -> Option<Vec<u8>> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_path)
        .arg("show")
        .arg(format!("{sha}:{rel_path}"))
        .output()
        .ok()?;
    output.status.success().then_some(output.stdout)
}

fn build_script(use_mise: bool) -> String {
    if use_mise {
        format!(
            "set -e\n\
             export MISE_TRUSTED_CONFIG_PATHS=\"$PWD\"\n\
             nice -n {NICE_LEVEL} mise exec -- cargo build --release -p rk-cli -p rk-mcp\n\
             mise exec -- rustc --version > .rk-release-toolchain.txt\n"
        )
    } else {
        format!(
            "set -e\n\
             nice -n {NICE_LEVEL} cargo build --release -p rk-cli -p rk-mcp\n\
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
) -> rk_core::Result<ReleaseManifest> {
    let staging = staging_dir(layout, repo_name);
    {
        let repo_path = repo_path.to_path_buf();
        let staging = staging.clone();
        let resolved_commit = resolved_commit.to_string();
        tokio::task::spawn_blocking(move || -> rk_core::Result<()> {
            let repo = rk_git::Repo::discover(&repo_path)?;
            // A persistent, reused worktree (same mechanism as the landing
            // pipeline's gate worktree): `reset_gate_worktree` gives a clean
            // detached checkout of the exact candidate tree on every call
            // while leaving `target/` warm across prepares.
            repo.ensure_gate_worktree(&staging)?;
            repo.reset_gate_worktree(&staging, &resolved_commit)?;
            Ok(())
        })
        .await
        .map_err(|e| rk_core::Error::other(format!("staging worktree task failed: {e}")))??;
    }

    // `used_mise` was already decided from the resolved commit's tree (via
    // `git show`, in `prepare`) rather than re-derived from the staging
    // worktree here — a pure function of the exact source, not of whatever a
    // prior prepare happened to leave checked out.
    let script = build_script(config_provenance.used_mise);

    let mut command = tokio::process::Command::new("sh");
    command
        .arg("-c")
        .arg(&script)
        .current_dir(&staging)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .env("CARGO_BUILD_JOBS", CARGO_BUILD_JOBS.to_string())
        .env("CARGO_INCREMENTAL", "0")
        .process_group(0);
    let child = command
        .spawn()
        .map_err(|e| rk_core::Error::other(format!("release build: failed to spawn recipe: {e}")))?;
    let _marker = child
        .id()
        .map(|pid| ManagedChildMarker::create(layout, pid));
    let outcome = collect_child_output(child, BUILD_TIMEOUT, "release build").await?;
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

    let target_release = staging.join("target").join("release");
    if release_dir.exists() {
        // Only reachable when a prior attempt at this exact id failed after
        // partially writing binaries but before ever completing a manifest
        // (manifest.json existing would have short-circuited in `prepare`
        // above, and a completed manifest is never overwritten). Moved aside
        // rather than deleted: durable evidence of a failed/interrupted
        // attempt stays inspectable, matching the ticket's "keep durable
        // evidence and unreferenced partials inspectable" requirement.
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
        std::fs::write(&dest, &bytes)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o755))?;
        }
        binaries.insert(
            name.to_string(),
            BinaryArtifact {
                sha256,
                size_bytes: bytes.len() as u64,
            },
        );
    }

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
            enforcement_note: "process-wide single-flight lock on release.prepare; not a \
                host-wide CPU quota, not the P3.1 HostVerificationAdmission cap, and not an \
                immutable-execution guarantee"
                .to_string(),
        },
        config_provenance,
        checks,
        created_at: Utc::now(),
    };
    write_manifest_new(&release_dir.join("manifest.json"), &manifest)?;
    Ok(manifest)
}

/// `rk-mcp`'s real minimal handshake, matching the operator recipe's own
/// smoke proof: one JSON-RPC `initialize` request, `id: 1`, empty object
/// params (`rk_mcp::handle_request` refuses a non-object `params`).
const MCP_INITIALIZE_REQUEST: &[u8] = br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;

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
        command.args(RK_SMOKE_ARGV).stdin(std::process::Stdio::null());
    }
    // The candidate binary is arbitrary (that's the point of smoke-testing
    // it) and must never touch the daemon's own production home even
    // incidentally — a cleared env plus an isolated scratch `RK_HOME`, not
    // the daemon's live one.
    command
        .env_clear()
        .env("RK_HOME", smoke_home)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .process_group(0);
    if let Some(path) = std::env::var_os("PATH") {
        command.env("PATH", path);
    }
    let mut child = command.spawn().map_err(|e| {
        rk_core::Error::other(format!("release smoke check for {name}: failed to spawn: {e}"))
    })?;
    if name == "rk-mcp" {
        use tokio::io::AsyncWriteExt;
        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(MCP_INITIALIZE_REQUEST).await.map_err(|e| {
                rk_core::Error::other(format!("release smoke check for {name}: stdin write failed: {e}"))
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

/// Parse `rk-mcp`'s response line and require the real success shape
/// (matching `rk_mcp::handle_request`'s `"initialize"` arm): a JSON-RPC
/// envelope with `id: 1`, no `error`, and a `result.protocolVersion`. Exit
/// code 0 alone would also pass an `rk-mcp` that never actually spoke the
/// protocol (e.g. crashed after printing nothing) — this is the check the
/// review flagged as missing.
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
    if result.get("protocolVersion").and_then(serde_json::Value::as_str).is_none() {
        return Err(rk_core::Error::other(format!(
            "mcp smoke: result missing protocolVersion: {result}"
        )));
    }
    Ok(())
}

/// Publish `manifest.json` atomically and with fully-written content: write
/// the complete bytes to a temp file in the same directory first, then
/// [`std::fs::hard_link`] it into place. `hard_link` fails if the destination
/// already exists (unlike `rename`, which would silently replace it) and,
/// because the temp file was already fully written and `fsync`'d to the
/// filesystem's own durability semantics before the link, there is no window
/// where a reader can observe a partially written file at the trusted path —
/// unlike `create_new` followed by a separate `write_all`, where a crash
/// between those two calls leaves a truncated file sitting at the path future
/// reads trust as complete.
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
        // Actually flush to durable storage before the file is linked into
        // its trusted final path — without this, `write`'s data can still be
        // sitting in the OS page cache when a crash hits, and a reader after
        // reboot could see a `manifest.json` (via the hard link, still
        // "fully written" from a torn-read perspective) whose content never
        // made it to disk.
        file.sync_all()?;
    }
    let result = std::fs::hard_link(&tmp, path);
    let _ = std::fs::remove_file(&tmp);
    result.map_err(|e| {
        rk_core::Error::other(format!("failed to publish manifest at {}: {e}", path.display()))
    })
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
            compute_input_key("other-repo", "abc123", RECIPE_PAIRED_RK_MCP, RECIPE_REVISION)
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
            candidate: "main".into(),
            recipe: RECIPE_PAIRED_RK_MCP.into(),
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
            candidate: "main".into(),
            recipe: RECIPE_PAIRED_RK_MCP.into(),
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
            },
            config_provenance: ConfigProvenance {
                cargo_build_jobs_env: "2".into(),
                cargo_incremental_env: "0".into(),
                used_mise: false,
                repo_policy_digest: None,
                checks_registry_digest: None,
                known_verification: None,
                compatibility_checked: false,
            },
            checks: Vec::new(),
            created_at: Utc::now(),
        }
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
        assert!(!verify_content(&entry, Path::new("/nonexistent"), &tampered));
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
        // The extra key changes the true digest too, but this asserts the
        // exact-name-set gate independently by using the ORIGINAL digest
        // (as if only the key set had somehow been added without touching
        // the recorded digest) to prove that gate alone would still refuse.
        assert!(!verify_content(&entry, Path::new("/nonexistent"), &manifest));
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
}
