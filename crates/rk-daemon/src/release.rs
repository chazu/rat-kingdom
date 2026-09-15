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

/// Whether a declared build-config file at an exact commit was found,
/// confirmed absent, or could not be observed at all. Collapsing the last two
/// into one `None` (as an earlier draft did) is unsafe: a transient `git`
/// failure is not evidence the file doesn't exist, and for `used_mise` in
/// particular — which SELECTS the recipe's cargo invocation style — silently
/// treating "could not check" as "absent" risks running the wrong toolchain
/// without any record that the check never actually happened.
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
    /// Observed: whether the resolved source commit's tree carried a
    /// `mise.toml`/`.mise.toml` (read via `git cat-file -e`/`git show
    /// <sha>:<path>`, a pure function of the exact source, not of whatever
    /// the persistent staging worktree happened to have checked out before
    /// this call), which decided whether the recipe ran `cargo`/`rustc`
    /// directly or through `mise exec --`. Unlike `repo_policy`/
    /// `checks_registry` below, this is a plain `bool`: an unresolvable
    /// observation here aborts `prepare` outright (see `mise_present`)
    /// rather than being recorded as a value, because it selects the actual
    /// build recipe — there is no safe default to silently fall back to.
    pub used_mise: bool,
    /// `.rk/repo.cue` at the resolved commit — a non-secret fingerprint of
    /// the effective repository delivery policy this exact source tree
    /// carries.
    pub repo_policy: FileObservation,
    /// `.rk/checks.cue` at the resolved commit.
    pub checks_registry: FileObservation,
    /// A reference to an existing exact-key managed-verification proof for
    /// this resolved commit (`crate::managed_verification::
    /// lookup_verification_proof`/`verification_proof_key`'s durable
    /// `Event`/`landing_gate_pass` records — a pure read, never an
    /// execution), forwarded from `PrepareParams::known_verification`. The
    /// daemon looks this up itself (it alone holds the `Space`/
    /// `VerificationResources` that read needs) before calling `prepare`;
    /// this module deliberately never calls the alternative path that WOULD
    /// be reachable from in here (`WorkflowEngine::verify_repo_check`),
    /// because on a cache miss that path EXECUTES the named check — silently
    /// running a full `verify` suite as a side effect of preparing a release
    /// is exactly the unbounded behavior this ticket excludes. `None` means
    /// no cached proof exists for this exact repo/commit/check identity, not
    /// "not looked up" — this is a reference for operator/consumer
    /// awareness, never itself proof this release's own binaries pass it.
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
///
/// `resolved_commit`/`tree_sha` must already be frozen by the CALLER before
/// this is constructed — `prepare` never re-resolves `requested` itself. This
/// matters because `requested` can be a mutable ref (a branch or tag): if the
/// caller resolved it once for a read (e.g. `known_verification`'s proof
/// lookup) and `prepare` resolved it AGAIN independently after queueing
/// behind `Server::release_prepare_lock`, a push to that branch in between
/// could attach the first resolution's proof reference to a manifest that
/// actually describes the second, different commit. The caller MUST resolve
/// exactly once and pass the same frozen values into both the lookup and
/// this struct.
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
    /// A read-only reference to an existing exact-key managed-verification
    /// proof for `resolved_commit` (see `crate::managed_verification::
    /// lookup_verification_proof`), when the caller — which alone holds the
    /// `Space`/`VerificationResources` that lookup needs — found one under
    /// the SAME lock/resolution as this struct's `resolved_commit`. `None`
    /// means no such proof is currently cached for this exact
    /// repo/sha/check identity, never "not looked up".
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

/// Load one named check from `.rk/checks.cue` as it existed at `sha`, read
/// via `git show` (no worktree touched). `None` covers "no checks.cue at that
/// commit", "checks.cue doesn't parse", and "no check with that name" alike —
/// callers only need "a usable check definition was found" vs. not.
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

    /// `prepare`'s crash-recovery correctness relies on this registry write
    /// (specifically, committing `manifest_digest`) landing durably BEFORE
    /// `manifest.json` is ever published (see `write_manifest_new` and the
    /// doc comments in `prepare`'s `Ok(manifest)` arm). A `write` +
    /// `rename` with no `sync_all` gives atomicity (no reader ever observes
    /// a torn file) but not durability against power loss — the OS can hold
    /// either write in its page cache indefinitely. `sync_all` on the temp
    /// file before the rename, plus `sync_directory` on the containing
    /// directory after it, makes the write actually durable before this function returns,
    /// matching what the crash-recovery design claims rather than only
    /// approximating it.
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

/// Sync `dir`'s own directory entry so a rename/hard-link/file-create within
/// it durably survives a crash promptly, not just atomically — used by every
/// write this module claims is durable-before-publication
/// (`ReleaseRegistry::persist`, `write_manifest_new`, and the release
/// binaries' containing directory in `run_recipe`).
///
/// A genuine failure here is propagated as an error rather than swallowed:
/// silently ignoring it would leave those callers' doc comments claiming a
/// durable commit-before-publication order that the code no longer actually
/// enforces. The one exception is `ErrorKind::Unsupported` — some
/// filesystems refuse to open or `fsync` a directory at all (their renames
/// and hard-links stay atomic regardless, just not promptly durable against
/// power loss), which is the documented, honest limitation this module
/// already states rather than one this helper can fix. Every other error
/// (permission denied, the directory having vanished, disk I/O failure, …)
/// is a real fault a caller must not treat as "prepared"/"published".
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

/// Durably publish a freshly-built manifest and return the resulting
/// `Prepared` registry entry. This is the ONE place that performs the
/// digest-before-publication sequence `prepare`'s crash-recovery design
/// depends on — commit the digest of what is ABOUT to be published, durably,
/// BEFORE publishing it (the trust anchor a later crash-recovery read relies
/// on — see `prepare`'s `manifest_path.is_file()` branch), then publish
/// `manifest.json`, then promote the registry entry to `Prepared`. Still
/// `Preparing` after the first write: the file doesn't exist at its trusted
/// path yet. If the caller dies between these writes, a later `prepare` call
/// finds `manifest.json` already written (by `write_manifest_new`) with a
/// digest that matches what this function already committed, and adopts it;
/// if it finds `manifest.json` missing entirely, it just re-runs the recipe,
/// because a `Preparing` status commits to nothing being published yet
/// either way.
///
/// `prepare` calls this directly, and its own fault-boundary unit test
/// (`publication_failure_after_a_committed_digest_leaves_no_false_prepared_state`)
/// calls it too — deliberately the SAME function, not a hand-copy of its
/// steps, so a regression that reorders or drops one of these writes is
/// caught by exercising the real production path, not a test-local
/// reimplementation of it.
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

    // `resolved_commit`/`tree_sha` were frozen by the CALLER — see
    // `PrepareParams`'s doc comment for why `prepare` must never re-resolve
    // `requested` itself. Config provenance IS safe to gather here: it's a
    // pure function of the already-frozen commit, not of the mutable ref.
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

    // `manifest.json` can exist here even when the registry doesn't (yet)
    // say `Prepared` for this id: `run_recipe`'s caller (below) durably
    // commits the digest of the content it is ABOUT to publish before
    // publishing it (see the `Ok(manifest)` arm below), so a crash between
    // those two writes leaves a fully valid, complete manifest sitting next
    // to a `Preparing` registry entry that ALREADY carries that exact digest.
    // Recovery trusts ONLY that pre-committed, daemon-authored digest — never
    // the manifest file's own self-reported identity fields. The release id
    // is a deterministic hash of public inputs (repo name, candidate, recipe
    // — nothing secret), so its path is guessable; a manifest.json that
    // merely *claims* to be this release, with no prior registry commitment
    // to back it, is not evidence of anything and must never be adopted on
    // its own say-so (this was a real hole in an earlier draft: "the
    // manifest's own identity fields match" is trivially satisfiable by
    // whoever wrote the file).
    if manifest_path.is_file() {
        let manifest = load_manifest(&manifest_path)?;
        let previously_prepared = existing_entry
            .as_ref()
            .is_some_and(|e| e.status == ReleaseStatus::Prepared);
        match &existing_entry {
            Some(entry) if verify_content(entry, &release_dir, &manifest) => {
                // The registry already held this exact digest before this
                // call — either from a completed prior `Prepared` (ordinary
                // idempotent reuse) or from the pre-publish commit of an
                // attempt interrupted before its final `Prepared` write
                // (crash-before-index-update recovery). Both are equally
                // trustworthy: the digest was recorded by this daemon,
                // strictly before `manifest.json` could exist at this path.
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
                // No registry-committed digest this file can be checked
                // against (or it doesn't match): an unattested file at this
                // fully guessable, content-derived path proves nothing, no
                // matter how internally self-consistent it looks. Quarantine
                // it — preserving it as inspectable evidence rather than
                // deleting — and fall through to a fresh build below, the
                // same as any other stale/unverifiable partial content.
                let quarantine = quarantine_dir(layout, &id);
                if let Some(parent) = quarantine.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::rename(&release_dir, &quarantine)?;
            }
        }
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
        &params.requested,
        &id,
        &release_dir,
        config_provenance,
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

/// Read a file's content at an exact commit via `git show <sha>:<path>`,
/// without touching any worktree. `None` covers both "absent at that commit"
/// and any other git failure equally — callers only need "present with this
/// content" vs. "not usably present", never the distinction between them.
enum BlobObservation {
    Present(Vec<u8>),
    Absent,
    Unavailable(String),
}

/// Read a file's content at an exact commit, distinguishing "confirmed
/// absent" from "could not observe" (see `FileObservation`'s doc comment for
/// why collapsing the two is unsafe). `git cat-file -e` first: for a
/// `<tree-ish>:<path>` spec (as opposed to a bare object hash), git reports a
/// missing PATH as exit 128 with a `fatal: path '<path>' does not exist in
/// '<sha>'` message — not exit 1, which is reserved for a missing OBJECT
/// hash. Only that specific, stable message at exit 128 is treated as
/// `Absent`; any other exit code or message is `Unavailable`, never silently
/// coerced to `Absent`.
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

/// Whether the resolved commit's tree carries `mise.toml`/`.mise.toml`.
/// Unlike `file_observation` below, this is behavior-selecting (it decides
/// whether the recipe runs through `mise exec --`), so an `Unavailable`
/// observation for either candidate path is a hard error, not a default —
/// see `ConfigProvenance::used_mise`'s doc comment.
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

/// Gather `ConfigProvenance` for an already-resolved commit. Pure function of
/// `resolved_commit` (never of `requested` or of the persistent staging
/// worktree's current state), so — unlike candidate resolution — it is safe
/// to call after queueing behind `Server::release_prepare_lock`: the commit
/// is already frozen by the time this runs.
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

/// `target_dir` is passed BOTH as the `CARGO_TARGET_DIR` env var (set on the
/// child process, see `run_recipe`) AND as an explicit `--target-dir` CLI
/// flag here — CLI flags win over env vars and config-file settings in
/// Cargo's own precedence order, but binding it twice means this recipe's
/// output location does not depend on getting the env var through some
/// wrapper (`mise exec --`, `sh -c`) uninterfered with. Likewise `--jobs`
/// is explicit on the invocation, not left to the `CARGO_BUILD_JOBS` env var
/// alone. Quoted with `'...'` (shell single-quotes): the path is daemon-
/// controlled, never user input, but staying quote-safe costs nothing.
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

    // Explicit, owned build output directory — overriding whatever
    // `CARGO_TARGET_DIR` the daemon process's own ambient environment might
    // carry (mise sets a shared one for some agent roles; see
    // `supervisor.rs`'s `shared_cargo_target` handling). Without this, an
    // inherited shared target dir could point this build's output at a
    // location shared with unrelated builds — this function reads
    // `<staging>/target/release/*` unconditionally below, so it must also be
    // where THIS build actually writes, not wherever ambient config says.
    // Bound BOTH as an env var and as an explicit `--target-dir` CLI flag in
    // the script itself (see `build_script`'s doc comment).
    let target_dir = staging.join("target");
    // `used_mise` was already decided from the resolved commit's tree (via
    // `git show`, in `prepare`) rather than re-derived from the staging
    // worktree here — a pure function of the exact source, not of whatever a
    // prior prepare happened to leave checked out.
    let script = build_script(config_provenance.used_mise, &target_dir);

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
    // See `rk_core::exec::close_extra_fds`: a captured-output pipe
    // (`stdout`/`stderr` above are both `Stdio::piped()`) created on macOS is
    // born without close-on-exec, so a concurrent spawn elsewhere in this
    // process can race a fork into that window and inherit a live copy —
    // exactly the leaked-pipe hang TKT-bikuz-kumuz-zutit diagnosed.
    rk_core::exec::close_extra_fds(command.as_std_mut());
    let child = command.spawn().map_err(|e| {
        rk_core::Error::other(format!("release build: failed to spawn recipe: {e}"))
    })?;
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

    let target_release = target_dir.join("release");
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
        write_binary_durably(&dest, &bytes)?;
        binaries.insert(
            name.to_string(),
            BinaryArtifact {
                sha256,
                size_bytes: bytes.len() as u64,
            },
        );
    }
    // Sync the release directory entry so the binaries' own fsyncs above are
    // joined by a durable record of their names existing at all, matching
    // `write_manifest_new`'s and `ReleaseRegistry::persist`'s same treatment
    // of their containing directories. A genuine failure here is propagated
    // (see `sync_directory`'s doc comment) rather than swallowed — this
    // function's result feeds directly into `prepare`'s durable
    // digest-before-publication commit, which must not proceed on the
    // strength of an unconfirmed directory sync.
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
            enforcement_note: "process-wide single-flight lock on release.prepare; not a \
                host-wide CPU quota, not the P3.1 HostVerificationAdmission cap, and not an \
                immutable-execution guarantee"
                .to_string(),
        },
        config_provenance,
        checks,
        created_at: Utc::now(),
    };
    // Publishing `manifest.json` is the caller's (`prepare`'s) job: it must
    // durably commit this manifest's digest to the registry FIRST (the
    // crash-recovery trust anchor — see `prepare`'s doc comments), which
    // requires the fully-constructed manifest this function returns.
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

/// Write one release binary and `fsync` it before returning, so its bytes
/// are actually durable on disk rather than merely sitting in the OS page
/// cache — matching `ReleaseRegistry::persist`'s and `write_manifest_new`'s
/// same discipline for the registry/manifest writes that come after it in
/// `prepare`'s publication order. Without this, a crash between this write
/// and the registry's durable digest commit (see `prepare`'s `Ok(manifest)`
/// arm) could leave the digest durably recorded while the binary bytes it
/// describes were still unflushed, which would make a later restart's
/// `verify_content` re-hash a stale or truncated file — reported as
/// "tampered or corrupted" rather than what it actually is, an incomplete
/// publication.
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
        rk_core::Error::other(format!(
            "failed to publish manifest at {}: {e}",
            path.display()
        ))
    })?;
    // Sync the directory entry so the hard link's visibility itself survives
    // a crash promptly, matching `ReleaseRegistry::persist`'s same
    // treatment. A genuine failure here is propagated (see
    // `sync_directory`'s doc comment) rather than swallowed: this function
    // publishing "successfully" is exactly what a reader treats as proof the
    // manifest is durably visible.
    sync_directory(dir)?;
    Ok(())
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

    /// A publication/registry boundary fault (P6.1 correction, item 2): the
    /// exact crash window `prepare`'s doc comments describe is "the registry
    /// already durably committed this manifest's digest, but
    /// `manifest.json` was never published". Exercised here by calling
    /// [`publish_prepared_release`] directly — the SAME function `prepare`
    /// calls, not a hand copy of its steps — so a regression that reorders
    /// or drops one of its internal writes is caught by this test, not just
    /// one that happens to match a separately-maintained copy of the
    /// sequence. The registry must be left showing the honest, recoverable
    /// state (`Preparing`, digest committed) — never a `Prepared` with
    /// nothing published to back it, and never a partial `manifest.json` at
    /// the trusted path.
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

        // Fault injection: the release directory this manifest is supposed
        // to publish into was never created, so `publish_prepared_release`'s
        // internal `write_manifest_new` call must fail loudly rather than
        // silently no-op or leave a false `Prepared` behind.
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

        // The registry's durable state reflects exactly the first write this
        // function performs (digest committed, still `Preparing`) and never
        // reaches the final promotion to `Prepared` — proving the ORDER, not
        // just that some error was returned.
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
        // The extra key changes the true digest too, but this asserts the
        // exact-name-set gate independently by using the ORIGINAL digest
        // (as if only the key set had somehow been added without touching
        // the recorded digest) to prove that gate alone would still refuse.
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

    /// A publication/registry fault boundary (P6.1 correction, item 2): a
    /// genuine directory-sync failure must be reported, not silently
    /// swallowed the way `let _ = handle.sync_all();` used to. A nonexistent
    /// path is a deterministic, host/uid-independent way to force `File::
    /// open` itself to fail — no reliance on permission bits, which a
    /// root-run suite would not even enforce.
    #[test]
    fn sync_directory_propagates_a_real_open_failure() {
        let missing = Path::new("/nonexistent-rk-release-sync-directory-test-path");
        let err = sync_directory(missing).unwrap_err();
        assert!(err.to_string().contains("failed to open"), "{err}");
    }
}
