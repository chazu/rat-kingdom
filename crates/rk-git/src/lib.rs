//! Git lifecycle for agent isolation: worktree per rat, branch per task,
//! merge on dismiss. Shells out to system `git` — proven semantics, and the
//! same binary humans use when they inspect what the rats did.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tracing::debug;

/// Per-merge sequence, so two `merge_branch` calls running at once (e.g. a
/// `dismiss` into `main` and a `land` into `develop`) never collide on the same
/// temporary worktree path. Process id alone is shared by every thread/task, so
/// concurrent in-process merges would otherwise reuse one path and the second
/// `worktree add` would fail. Merges to the *same* target are additionally
/// serialized upstream by the daemon's merge queue.
static MERGE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Git's worktree administration mutates shared `.git/worktrees` metadata and
/// is not safe under concurrent `add`/`remove`/`prune` processes, even when the
/// worktree paths and target refs are distinct. Keep that short metadata
/// boundary process-serialized; the actual work performed inside each
/// worktree remains concurrent.
static WORKTREE_METADATA: Mutex<()> = Mutex::new(());

fn worktree_metadata_guard() -> MutexGuard<'static, ()> {
    WORKTREE_METADATA
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub const PROTECTED_BRANCHES: [&str; 4] = ["main", "master", "develop", "HEAD"];

/// Ref namespace parking [`Repo::prepare_merge`] candidates. A candidate merge
/// commit is unreachable from any branch for the whole gate run; parking it
/// under a ref is what keeps `git gc` from collecting the very tree the gate
/// is testing. Deliberately outside `refs/heads/` so candidates never appear
/// as branches to `git branch`, a push, or the daemon's own branch scans.
pub const CANDIDATE_REF_PREFIX: &str = "refs/rk/candidates/";

/// A repository root (the main checkout, not a worktree).
#[derive(Debug, Clone)]
pub struct Repo {
    root: PathBuf,
}

/// File list and total changed-line count for a diff range. See
/// [`Repo::diff_stat`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DiffStat {
    pub files: Vec<String>,
    pub lines: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MergeOutcome {
    pub merged: bool,
    /// The commit the target ref was advanced to (the merge commit, or the
    /// revert commit for [`Repo::revert_merge`]). `None` when nothing landed.
    pub commit: Option<String>,
    pub detail: String,
}

/// A merge commit that has been **built but not landed**: the exact tree a
/// gate should test, and the exact commit [`Repo::advance_target_to`] will
/// land if the gate passes.
///
/// This is the missing half of "test the merge, land the tested tree".
/// [`Repo::merge_branch`] always builds a *fresh* merge commit at land time,
/// so a gate that tested anything earlier — the branch tip, or a merge built
/// for the gate — cannot land what it tested; the commit that actually lands
/// carries a green receipt it never earned. Preparing the merge first makes
/// the tested sha and the landed sha the same object.
///
/// Purely git: no build system, language, or check runner is implied. What a
/// gate *does* with [`PreparedMerge::commit`] is the caller's policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedMerge {
    /// The candidate merge commit — the sha to test, and the sha to land.
    pub commit: String,
    /// The `target` tip this merge was built on. Doubles as the
    /// expected-parent guard for the compare-and-swap that lands it: if the
    /// target has moved off `base`, the candidate was tested against a tree
    /// that is no longer what landing would produce.
    pub base: String,
    /// Full ref name (under [`CANDIDATE_REF_PREFIX`]) keeping `commit`
    /// reachable until it lands or is discarded. Pass to
    /// [`Repo::discard_candidate`] when done.
    pub candidate_ref: String,
}

impl PreparedMerge {
    /// Whether merging changed nothing — `branch` was already contained in
    /// `target`, so git produced no merge commit and `commit == base`.
    /// Landing this is a no-op, not a delivery; callers gating on "did this
    /// deliver" should treat it as empty rather than as a successful merge.
    pub fn is_empty(&self) -> bool {
        self.commit == self.base
    }
}

/// Outcome of [`Repo::prepare_merge`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareOutcome {
    Prepared(PreparedMerge),
    /// The merge does not apply. Nothing was built and nothing was parked;
    /// not retryable against this pair of tips (though a later target tip may
    /// merge cleanly).
    Conflict {
        detail: String,
    },
}

/// Outcome of [`Repo::advance_target_to`] — the compare-and-swap land.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdvanceOutcome {
    /// The target ref now points at exactly `commit`.
    Advanced { commit: String },
    /// The target moved off the expected parent, so the swap was refused.
    /// **Nothing landed and nothing was lost**: the candidate is still parked
    /// under its ref. Retryable — rebuild the candidate on `actual` and gate
    /// again.
    Stale { expected: String, actual: String },
}

impl AdvanceOutcome {
    pub fn advanced(&self) -> bool {
        matches!(self, AdvanceOutcome::Advanced { .. })
    }

    /// Whether the caller may sensibly try again. A stale target is the one
    /// contended-but-healthy outcome; everything else surfaces as `Err`.
    pub fn retryable(&self) -> bool {
        matches!(self, AdvanceOutcome::Stale { .. })
    }

    /// The landed commit, or `None` when nothing landed.
    pub fn commit(&self) -> Option<&str> {
        match self {
            AdvanceOutcome::Advanced { commit } => Some(commit),
            AdvanceOutcome::Stale { .. } => None,
        }
    }

    pub fn detail(&self) -> String {
        match self {
            AdvanceOutcome::Advanced { commit } => format!("advanced to {commit}"),
            AdvanceOutcome::Stale { expected, actual } => {
                format!("target moved from {expected} to {actual}; candidate not landed")
            }
        }
    }
}

/// The remote host kind, inferred from an `origin` URL. Decides how a PR/MR
/// is opened over plain `git` — GitLab accepts merge-request push options,
/// GitHub only surfaces a compare URL on push.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Host {
    GitHub,
    GitLab,
    /// Any other host (self-hosted, unrecognized): treated like GitHub — push
    /// and surface whatever URL the remote prints; no PR is created for you.
    Unknown,
}

/// Outcome of pushing a branch and opening a pull/merge request over plain
/// `git`. Mirrors [`MergeOutcome`]: `opened` is the analogue of `merged`
/// (did the push/PR operation complete cleanly), and failure is a clean
/// `opened: false` with an explanatory `detail` — never a panic.
#[derive(Debug, Clone, PartialEq)]
pub struct PrOutcome {
    /// True iff the push (and, on GitLab, the merge-request push option)
    /// completed successfully.
    pub opened: bool,
    /// The PR/MR URL the remote printed, if one was surfaced. On GitLab this
    /// is the created merge request; on GitHub it is the compare URL a human
    /// clicks to open the PR.
    pub url: Option<String>,
    pub detail: String,
}

impl Repo {
    /// Open `path`, resolving through worktrees to the main repository root.
    pub fn discover(path: &Path) -> rk_core::Result<Self> {
        let common = git_in(
            path,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )?;
        let common = PathBuf::from(common.trim());
        let root = common
            .parent()
            .ok_or_else(|| rk_core::Error::other("cannot resolve repo root"))?
            .to_path_buf();
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Repo name = directory basename.
    pub fn name(&self) -> String {
        self.root
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "repo".into())
    }

    pub fn current_branch(&self) -> rk_core::Result<String> {
        Ok(self
            .git(&["rev-parse", "--abbrev-ref", "HEAD"])?
            .trim()
            .to_string())
    }

    pub fn branch_exists(&self, branch: &str) -> bool {
        self.git(&[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("refs/heads/{branch}"),
        ])
        .is_ok()
    }

    pub fn is_dirty(&self) -> rk_core::Result<bool> {
        Ok(!self.git(&["status", "--porcelain"])?.trim().is_empty())
    }

    /// True iff `commit` is an ancestor of (or equal to) `of` — i.e. `of`
    /// already contains `commit`. Both are any revision `git` can resolve
    /// (branch name, tag, sha). Returns false when either revision is
    /// unresolvable, so the caller cannot mistake "couldn't tell" for merged.
    pub fn is_ancestor(&self, commit: &str, of: &str) -> bool {
        // `merge-base --is-ancestor A B` exits 0 when A is an ancestor of B,
        // 1 when it is not, and non-0/1 on a bad revision — `git()` maps every
        // non-zero exit to Err, collapsing "not an ancestor" and "bad rev" into
        // the same false. That is the safe direction: unknown ⇒ not merged.
        self.git(&["merge-base", "--is-ancestor", commit, of])
            .is_ok()
    }

    /// Commit-count-aware delivery check: true iff `commit` was genuinely
    /// absorbed into `of` — `of` contains it AND has moved past it.
    ///
    /// Plain [`is_ancestor`](Repo::is_ancestor) alone also returns true when
    /// `commit` merely *equals* `of`'s history without ever diverging — an
    /// empty branch, cut from `of` and never touched, is trivially "an
    /// ancestor of" its own fork point forever, which reads a rat that
    /// committed nothing as delivered. Requiring `of` to also NOT be an
    /// ancestor of `commit` excludes that trivial-equal case while still
    /// recognizing a real merge: `rk` always lands with `--no-ff` (see
    /// [`merge_branch`](Repo::merge_branch)), so a genuine merge commit is
    /// always distinct from `commit`'s own tip, making this exact rather
    /// than a heuristic.
    fn advanced_past(&self, commit: &str, of: &str) -> bool {
        self.is_ancestor(commit, of) && !self.is_ancestor(of, commit)
    }

    /// Whether an awaiting-review branch has been dealt with on the forge:
    /// either merged into `target` (its tip is an ancestor of `target`) or
    /// gone (the local branch ref no longer exists).
    ///
    /// In PR mode the daemon KEEPS the branch after opening the PR (TKT-65), so
    /// a branch that has since vanished was deleted by a human — the forge's
    /// conventional post-merge cleanup — and the work either landed or was
    /// abandoned deliberately; either way nothing awaits review. A branch that
    /// still exists is cleared only once `target` actually contains it, which
    /// happens locally when the operator pulls the merge (or a Direct-mode
    /// fast-forward advances the target). No network, no forge API: a pure
    /// read over local refs, matching the use-git-directly PR-mode decision.
    ///
    /// NOT commit-count aware, deliberately: a genuine fast-forward (target
    /// had no other commits since the branch forked) leaves `target`
    /// pointing at exactly `branch`'s tip, which is indistinguishable here
    /// from a branch that never diverged at all — [`advanced_past`] would
    /// misread a real FF-merged PR as still-open. Callers that need to
    /// exclude the never-diverged case (crash-vs-delivered) use a check with
    /// more context than a bare ref pair — see
    /// [`branch_verified_merged`](Repo::branch_verified_merged), which is
    /// safe here only because its caller's merges are always `--no-ff`.
    pub fn branch_merged_or_gone(&self, branch: &str, target: &str) -> bool {
        if !self.branch_exists(branch) {
            return true;
        }
        self.is_ancestor(branch, target)
    }

    /// Whether `branch` has *verifiably* merged into `target`: it still
    /// exists and its tip is an ancestor of `target`. Unlike
    /// [`branch_merged_or_gone`](Repo::branch_merged_or_gone), a branch that
    /// no longer exists does NOT count as delivered here — that "gone ⇒
    /// dealt with" reading only holds where the daemon's own post-merge
    /// cleanup or a human's forge-side delete is the expected cause (PR/
    /// push-branch awaiting-review clearing). For merge-mode ticket-done
    /// gating we cannot tell that apart from a branch that vanished before
    /// ever landing (the exact "approved but never merged" class behind
    /// TKT-18/46/147), so an absent branch fails closed as "not delivered".
    /// Commit-count aware via [`advanced_past`](Repo::advanced_past): an
    /// empty branch (zero commits since its fork point) is not "verified
    /// merged" either — a rat that never committed must not read as done.
    pub fn branch_verified_merged(&self, branch: &str, target: &str) -> bool {
        self.branch_exists(branch) && self.advanced_past(branch, target)
    }

    /// Whether `branch` contains at least one commit after its recorded
    /// creation point. The fork commit is durable caller state; unlike a
    /// merge-base computed after the fact, it does not collapse to the branch
    /// tip after a legitimate fast-forward merge.
    pub fn branch_has_commits_since(&self, branch: &str, fork_point: &str) -> bool {
        let Ok(head) = self.rev_parse(branch) else {
            return false;
        };
        head != fork_point && self.is_ancestor(fork_point, branch)
    }

    /// Resolve `rev` (a branch name, sha, or any revision `git` accepts) to its
    /// full commit sha.
    ///
    /// Bounded: this and [`Repo::diff_stat`] run on daemon completion paths
    /// (`route_completion`'s diff summary), where an unbounded subprocess under
    /// extreme machine load blocked tokio workers until the whole daemon
    /// wedged (observed 2026-08-16, TKT-01M04D394PQ8VS5N3V441D1MDD). Local
    /// ref/diff reads take milliseconds; the generous bound only fires under
    /// pathology, and every caller already fails closed on `Err`.
    pub fn rev_parse(&self, rev: &str) -> rk_core::Result<String> {
        Ok(
            git_bounded(&self.root, &["rev-parse", rev], LOCAL_READ_TIMEOUT)?
                .trim()
                .to_string(),
        )
    }

    /// File list and total changed-line count for the `base...head` symmetric
    /// (merge-base) diff — the same range shape the `steward-diff-scope` check
    /// computes by hand in `.rk/checks.cue`. A binary file reports `-`/`-` in
    /// `--numstat`; those count as 0 lines, matching that check's `awk` script.
    pub fn diff_stat(&self, base: &str, head: &str) -> rk_core::Result<DiffStat> {
        let range = format!("{base}...{head}");
        let names = git_bounded(
            &self.root,
            &["diff", "--name-only", &range],
            LOCAL_READ_TIMEOUT,
        )?;
        let files: Vec<String> = names
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect();
        let numstat = git_bounded(
            &self.root,
            &["diff", "--numstat", &range],
            LOCAL_READ_TIMEOUT,
        )?;
        let mut lines: u64 = 0;
        for row in numstat.lines() {
            let mut cols = row.split_whitespace();
            let added: u64 = cols.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            let removed: u64 = cols.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            lines += added + removed;
        }
        Ok(DiffStat { files, lines })
    }

    /// Fetch `remote` and prune deleted remote-tracking branches, bounded by
    /// `timeout` so a hung network fetch cannot pin the caller. Non-interactive
    /// (`GIT_TERMINAL_PROMPT=0`), so a missing-credential prompt fails fast
    /// instead of blocking forever on stdin. `--quiet` keeps progress off the
    /// captured stderr so the bounded pipe cannot fill while we poll.
    ///
    /// This is the network half of the fetch-driven awaiting-review clear
    /// (TKT-70): refresh the remote-tracking refs so
    /// [`remote_branch_merged_or_gone`](Repo::remote_branch_merged_or_gone) can
    /// see a forge-side merge/delete the operator has not pulled locally.
    pub fn fetch_prune(&self, remote: &str, timeout: Duration) -> rk_core::Result<()> {
        if remote.trim().is_empty() {
            return Err(rk_core::Error::other("fetch_prune requires a remote"));
        }
        git_bounded(
            &self.root,
            &["fetch", "--prune", "--no-tags", "--quiet", remote],
            timeout,
        )
        .map(|_| ())
    }

    /// Remote-side analogue of [`branch_merged_or_gone`](Repo::branch_merged_or_gone):
    /// after a [`fetch_prune`](Repo::fetch_prune), decide whether the forge has
    /// dealt with `branch` — its remote-tracking ref `<remote>/<branch>` is gone
    /// (pruned after a forge-side delete) or has been merged into
    /// `<remote>/<target>`.
    ///
    /// Where `branch_merged_or_gone` reads the LOCAL target — which only
    /// advances when the operator pulls — this reads the remote-tracking refs a
    /// `fetch --prune` just refreshed, so it sees a human's forge merge with no
    /// local pull. Pure read over `refs/remotes/*`; run `fetch_prune` first. An
    /// unresolvable `<remote>/<target>` (never fetched) yields "not merged", so
    /// the awaiting-review row stays — the same fail-toward-surfacing direction
    /// as the local check. NOT commit-count aware, same reasoning as
    /// [`branch_merged_or_gone`](Repo::branch_merged_or_gone): a forge merge
    /// is very often a fast-forward, which is indistinguishable here from a
    /// branch that never diverged.
    pub fn remote_branch_merged_or_gone(&self, branch: &str, target: &str, remote: &str) -> bool {
        let remote_branch = format!("{remote}/{branch}");
        if !self.remote_ref_exists(&remote_branch) {
            return true;
        }
        self.is_ancestor(&remote_branch, &format!("{remote}/{target}"))
    }

    /// Whether a remote-tracking ref `<remote>/<name>` exists locally (i.e. the
    /// last fetch saw it and a prune has not removed it).
    fn remote_ref_exists(&self, remote_name: &str) -> bool {
        self.git(&[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("refs/remotes/{remote_name}"),
        ])
        .is_ok()
    }

    /// Create a worktree at `path` on new `branch` forked from `base`.
    pub fn create_worktree(&self, path: &Path, branch: &str, base: &str) -> rk_core::Result<()> {
        self.validate_branch_name(branch, "worktree branch")?;
        if PROTECTED_BRANCHES.contains(&branch) {
            return Err(rk_core::Error::other(format!(
                "refusing to use protected branch: {branch}"
            )));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _metadata = worktree_metadata_guard();
        // Cute rat names are drawn from a finite pool and reused, so a prior rat
        // of the same name can leave a worktree directory behind (a crash or a
        // failed cleanup) at exactly this path; `git worktree add` then fails
        // hard with "'<path>' already exists". The name was just reserved
        // atomically upstream, so no live rat holds it — reap the residue first.
        self.reap_stale_worktree(path);
        self.git(&[
            "worktree",
            "add",
            "-b",
            branch,
            &path.to_string_lossy(),
            base,
        ])?;
        debug!(?path, branch, base, "created worktree");
        Ok(())
    }

    /// Clear any leftover worktree at `path` so a fresh `worktree add` succeeds.
    /// Best-effort and never errors — a clean path is a no-op. First prune
    /// dangling registrations (dirs git still tracks but that are gone), then,
    /// if the directory is still present, ask git to remove a still-registered
    /// worktree and finally delete any residual directory outright. Only called
    /// with a freshly-reserved name's path, so a live rat cannot own it.
    fn reap_stale_worktree(&self, path: &Path) {
        let _ = self.git(&["worktree", "prune"]);
        if !path.exists() {
            return;
        }
        debug!(?path, "reaping stale worktree residue before add");
        // If git still tracks it as a worktree, this unregisters and removes it.
        let _ = self.git(&["worktree", "remove", "--force", &path.to_string_lossy()]);
        // Unregistered leftover (or the remove failed): delete the directory,
        // then prune again so git forgets any now-dangling registration.
        if path.exists() {
            let _ = std::fs::remove_dir_all(path);
            let _ = self.git(&["worktree", "prune"]);
        }
    }

    /// Remove a worktree (force: uncommitted changes in it are discarded —
    /// dismiss-with-merge commits first via the agent's own protocol).
    pub fn remove_worktree(&self, path: &Path) -> rk_core::Result<()> {
        let _metadata = worktree_metadata_guard();
        self.git(&["worktree", "remove", "--force", &path.to_string_lossy()])?;
        Ok(())
    }

    /// Discover the [`Repo`] a worktree at `path` belongs to and check whether
    /// that worktree itself has uncommitted changes (tracked edits or
    /// untracked files) — a plain `git status --porcelain` run directly
    /// against `path`, independent of which repo/branch it was forked from.
    ///
    /// Used before an unattended reclaim (rk-daemon's `Supervisor::reap_git`)
    /// force-removes a worktree: a branch being merged only proves its
    /// COMMITTED history landed, never anything left uncommitted in the
    /// working tree, so this is the check that stands between an automated
    /// sweep and silently destroying work no commit ever captured.
    pub fn worktree_is_dirty(path: &Path) -> rk_core::Result<bool> {
        Ok(!git_in(path, &["status", "--porcelain"])?.trim().is_empty())
    }

    pub fn prune_worktrees(&self) -> rk_core::Result<()> {
        let _metadata = worktree_metadata_guard();
        self.git(&["worktree", "prune"])?;
        Ok(())
    }

    /// Idempotent: create a detached, daemon-owned worktree at `path` if one
    /// isn't already registered there. Unlike [`create_worktree`](Repo::create_worktree)
    /// this never mints a branch — a plain detached checkout, the same *kind*
    /// as [`advance_via_worktree`](Repo::advance_via_worktree)'s temporary one,
    /// but meant to be created once and reused across many gate runs via
    /// [`reset_gate_worktree`](Repo::reset_gate_worktree) instead of being torn
    /// down after a single call. Detached avoids git's "a branch can't be
    /// checked out in two worktrees at once" restriction, so this worktree can
    /// hold a candidate branch's tip while that branch is still checked out
    /// (non-detached) elsewhere.
    pub fn ensure_gate_worktree(&self, path: &Path) -> rk_core::Result<()> {
        // Fast path only: the authoritative check repeats below UNDER the
        // metadata lock. Two concurrent ensures could otherwise both observe
        // absence here, and the second's `reap_stale_worktree` would tear
        // down the worktree the first had just created.
        if self.is_registered_worktree(path) {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _metadata = worktree_metadata_guard();
        if self.is_registered_worktree(path) {
            return Ok(());
        }
        self.reap_stale_worktree(path);
        self.git(&[
            "worktree",
            "add",
            "--detach",
            &path.to_string_lossy(),
            "HEAD",
        ])?;
        debug!(?path, "created gate worktree");
        Ok(())
    }

    /// Reset an existing gate worktree ([`ensure_gate_worktree`](Repo::ensure_gate_worktree))
    /// to `sha`'s tree, discarding whatever a prior gate run left behind:
    /// tracked-file edits (`checkout --force`) and untracked files
    /// (`clean -fd`). Deliberately `-fd`, not `-fdx`: an ignored build cache
    /// (e.g. `target/`) staying warm across resets is the entire point of a
    /// *persistent* worktree, not something a reset should wipe.
    pub fn reset_gate_worktree(&self, path: &Path, sha: &str) -> rk_core::Result<()> {
        git_in(path, &["checkout", "--detach", "--force", sha])?;
        git_in(path, &["clean", "-fd"])?;
        Ok(())
    }

    /// Whether `path` is already registered as a worktree of this repo (as
    /// opposed to a directory that merely exists there, e.g. stale residue).
    fn is_registered_worktree(&self, path: &Path) -> bool {
        let Ok(list) = self.git(&["worktree", "list", "--porcelain"]) else {
            return false;
        };
        let canon = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        list.lines()
            .filter_map(|line| line.strip_prefix("worktree "))
            .any(|p| std::fs::canonicalize(p).unwrap_or_else(|_| PathBuf::from(p)) == canon)
    }

    pub fn delete_branch(&self, branch: &str) -> rk_core::Result<()> {
        self.validate_local_branch(branch, "branch to delete")?;
        if PROTECTED_BRANCHES.contains(&branch) {
            return Err(rk_core::Error::other(format!(
                "refusing to delete protected branch: {branch}"
            )));
        }
        self.git(&["branch", "-D", branch])?;
        Ok(())
    }

    /// Merge `branch` into `target` without disturbing any checkout of
    /// `target`: run the merge in a temporary detached worktree, then
    /// fast-forward the target ref if it was not moved concurrently.
    pub fn merge_branch(&self, branch: &str, target: &str) -> rk_core::Result<MergeOutcome> {
        self.validate_local_branch(branch, "merge source")?;
        self.advance_via_worktree(
            target,
            "merge",
            &format!("branch {branch} left unmerged"),
            |tmp| {
                git_in(
                    tmp,
                    &[
                        "merge",
                        "--no-ff",
                        "-m",
                        &format!("merge {branch} into {target} [rk]"),
                        branch,
                    ],
                )
                .map(|_| ())
            },
            format!("merged {branch} into {target}"),
        )
    }

    /// Revert a previously-landed merge commit on `target` — the undo for
    /// [`merge_branch`](Repo::merge_branch). Creates a new commit that
    /// reverses the merge's tree changes (`git revert -m 1`, keeping the
    /// first parent — the target side), leaving history intact. Runs in a
    /// temporary detached worktree with the same compare-and-swap advance as
    /// the merge itself, so no live checkout is disturbed; a revert conflict
    /// or a concurrently-moved target is a clean `merged: false`, never an
    /// error.
    pub fn revert_merge(&self, commit: &str, target: &str) -> rk_core::Result<MergeOutcome> {
        if commit.trim().is_empty() {
            return Err(rk_core::Error::other(
                "revert_merge requires a merge commit",
            ));
        }
        self.advance_via_worktree(
            target,
            "revert",
            "nothing reverted",
            |tmp| git_in(tmp, &["revert", "--no-edit", "-m", "1", commit]).map(|_| ()),
            format!("reverted merge {commit} on {target}"),
        )
    }

    /// The shared engine behind [`merge_branch`](Repo::merge_branch) and
    /// [`revert_merge`](Repo::revert_merge): run `op` (a commit-producing git
    /// operation) in a temporary detached worktree of `target`, then advance
    /// the target ref to the new commit iff it was not moved concurrently.
    /// An `op` failure (conflict) and a moved target are clean
    /// `merged: false` outcomes; `aftermath` describes what the moved-target
    /// case leaves behind.
    fn advance_via_worktree(
        &self,
        target: &str,
        op_name: &str,
        aftermath: &str,
        op: impl FnOnce(&Path) -> rk_core::Result<()>,
        success_detail: String,
    ) -> rk_core::Result<MergeOutcome> {
        self.validate_local_branch(target, "merge target")?;
        self.in_temp_worktree(target, |tmp| {
            let target_before = self.git(&["rev-parse", &format!("refs/heads/{target}")])?;
            match op(tmp) {
                Ok(()) => {
                    let new_commit = git_in(tmp, &["rev-parse", "HEAD"])?;
                    let target_now = self.git(&["rev-parse", &format!("refs/heads/{target}")])?;
                    if target_now.trim() != target_before.trim() {
                        return Ok(MergeOutcome {
                            merged: false,
                            commit: None,
                            detail: format!("{target} moved during {op_name}; {aftermath}"),
                        });
                    }
                    self.advance_target(target, new_commit.trim(), target_before.trim())?;
                    Ok(MergeOutcome {
                        merged: true,
                        commit: Some(new_commit.trim().to_string()),
                        detail: success_detail,
                    })
                }
                Err(e) => Ok(MergeOutcome {
                    merged: false,
                    commit: None,
                    detail: format!("{op_name} conflict or failure: {e}"),
                }),
            }
        })
    }

    /// Run `op` in a throwaway detached worktree checked out at `rev`, tearing
    /// the worktree down afterwards whatever `op` did. Detached so it never
    /// conflicts with an existing checkout of the same branch.
    fn in_temp_worktree<T>(
        &self,
        rev: &str,
        op: impl FnOnce(&Path) -> rk_core::Result<T>,
    ) -> rk_core::Result<T> {
        let seq = MERGE_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp = self
            .root
            .join(".git")
            .join(format!("rk-merge-{}-{}", std::process::id(), seq));
        {
            let _metadata = worktree_metadata_guard();
            self.git(&["worktree", "add", "--detach", &tmp.to_string_lossy(), rev])?;
        }
        let result = op(&tmp);
        {
            let _metadata = worktree_metadata_guard();
            let _ = self.git(&["worktree", "remove", "--force", &tmp.to_string_lossy()]);
        }
        result
    }

    /// Build the merge of `branch` into `target` **without landing it**, and
    /// park the result so it survives until a gate has run on it.
    ///
    /// Together with [`advance_target_to`](Repo::advance_target_to) this is
    /// the "test the merge, land the tested tree" primitive:
    ///
    /// ```text
    /// prepare_merge(branch, target) -> PreparedMerge { commit, base }
    ///     run the repo's named checks against `commit`
    /// advance_target_to(target, commit, base) -> Advanced { commit }
    /// ```
    ///
    /// The landed sha is the sha the gate ran on, by construction — the merge
    /// is built once and never rebuilt. Contrast
    /// [`merge_branch`](Repo::merge_branch), which builds a fresh merge commit
    /// at land time: nothing a gate tested beforehand is what it lands.
    ///
    /// The candidate is built in a detached worktree pinned to the target tip
    /// read at entry, so a target that moves mid-build cannot silently change
    /// what was merged; the resulting [`PreparedMerge::base`] then fails the
    /// swap, which is the intended retryable outcome.
    ///
    /// Nothing here knows or cares what language the repo is written in.
    pub fn prepare_merge(&self, branch: &str, target: &str) -> rk_core::Result<PrepareOutcome> {
        self.validate_local_branch(branch, "merge source")?;
        self.validate_local_branch(target, "merge target")?;
        let base = self.rev_parse(&format!("refs/heads/{target}"))?;
        let message = format!("merge {branch} into {target} [rk]");
        // Built against `base` (the sha), not `target` (the name): pinning the
        // worktree to the sha we will guard on closes the window where the
        // target moves between the read and the checkout.
        let built = self.in_temp_worktree(&base, |tmp| {
            if let Err(e) = git_in(tmp, &["merge", "--no-ff", "-m", &message, branch]) {
                return Ok(Err(format!("merge conflict or failure: {e}")));
            }
            Ok(Ok(git_in(tmp, &["rev-parse", "HEAD"])?.trim().to_string()))
        })?;
        let commit = match built {
            Ok(commit) => commit,
            Err(detail) => return Ok(PrepareOutcome::Conflict { detail }),
        };
        // Park it before returning. Until this ref exists the merge commit is
        // reachable from nothing at all, and the temp worktree that held it is
        // already gone.
        let candidate_ref = format!("{CANDIDATE_REF_PREFIX}{commit}");
        self.git(&["update-ref", &candidate_ref, &commit])?;
        Ok(PrepareOutcome::Prepared(PreparedMerge {
            commit,
            base,
            candidate_ref,
        }))
    }

    /// Build several source branches onto one pinned target tip, producing a
    /// single parked candidate for one shared gate run. Branch order is
    /// caller-defined and therefore deterministic. A conflict abandons the
    /// whole candidate; callers may bisect the ordered slice and retry.
    pub fn prepare_merge_batch(
        &self,
        branches: &[String],
        target: &str,
    ) -> rk_core::Result<PrepareOutcome> {
        if branches.is_empty() {
            return Err(rk_core::Error::other("cannot prepare an empty merge batch"));
        }
        for branch in branches {
            self.validate_local_branch(branch, "merge source")?;
        }
        self.validate_local_branch(target, "merge target")?;
        let base = self.rev_parse(&format!("refs/heads/{target}"))?;
        let built = self.in_temp_worktree(&base, |tmp| {
            for branch in branches {
                let message = format!("merge {branch} into {target} [rk batch]");
                if let Err(error) = git_in(tmp, &["merge", "--no-ff", "-m", &message, branch]) {
                    return Ok(Err(format!(
                        "merge conflict or failure in {branch}: {error}"
                    )));
                }
            }
            Ok(Ok(git_in(tmp, &["rev-parse", "HEAD"])?.trim().to_string()))
        })?;
        let commit = match built {
            Ok(commit) => commit,
            Err(detail) => return Ok(PrepareOutcome::Conflict { detail }),
        };
        let candidate_ref = format!("{CANDIDATE_REF_PREFIX}{commit}");
        self.git(&["update-ref", &candidate_ref, &commit])?;
        Ok(PrepareOutcome::Prepared(PreparedMerge {
            commit,
            base,
            candidate_ref,
        }))
    }

    /// Compare-and-swap `target` onto an already-built commit: advance the ref
    /// to `commit` **iff** it still points at `expected`.
    ///
    /// This is the landing half of the pre-tested-merge protocol. It builds
    /// nothing — the tree it lands is exactly the object `commit` names, which
    /// is what makes "landed sha == tested sha" an invariant rather than a
    /// hope.
    ///
    /// A target that has moved off `expected` yields
    /// [`AdvanceOutcome::Stale`]: a clean, retryable outcome that lands
    /// nothing, loses nothing, and leaves the candidate parked for a rebuild.
    /// Errors are reserved for genuine faults and for misuse — notably a
    /// `commit` that does not descend from `expected`, which would silently
    /// discard whatever is on the target rather than build on it.
    pub fn advance_target_to(
        &self,
        target: &str,
        commit: &str,
        expected: &str,
    ) -> rk_core::Result<AdvanceOutcome> {
        self.validate_local_branch(target, "advance target")?;
        // Resolve both through git so a caller may pass any revision, and so
        // a nonexistent one is an error here rather than a confusing no-op.
        let commit = self.rev_parse(commit)?;
        let expected = self.rev_parse(expected)?;
        if commit != expected && !self.is_ancestor(&expected, &commit) {
            return Err(rk_core::Error::other(format!(
                "refusing to advance {target}: {commit} does not descend from {expected}"
            )));
        }
        let tip = self.rev_parse(&format!("refs/heads/{target}"))?;
        if tip != expected {
            return Ok(AdvanceOutcome::Stale {
                expected,
                actual: tip,
            });
        }
        if commit == expected {
            // Already there. Idempotent: a retry after a successful advance
            // whose caller lost the answer must not look like a failure.
            return Ok(AdvanceOutcome::Advanced { commit });
        }
        match self.advance_target(target, &commit, &expected) {
            Ok(()) => Ok(AdvanceOutcome::Advanced { commit }),
            Err(e) => {
                // The read above is not the guard — git's own old-value check
                // (`update-ref`'s third argument, or `merge --ff-only`'s
                // refusal) is, and it can reject a swap that looked fine
                // microseconds earlier. Re-read to tell a lost race from a
                // real fault instead of reporting every contention as an error.
                let now = self.rev_parse(&format!("refs/heads/{target}"))?;
                if now == expected {
                    return Err(e);
                }
                Ok(AdvanceOutcome::Stale {
                    expected,
                    actual: now,
                })
            }
        }
    }

    /// Drop a candidate's parking ref, letting git collect the commit if
    /// nothing else reaches it. Call after landing a candidate or abandoning
    /// one. Idempotent; refuses any ref outside [`CANDIDATE_REF_PREFIX`], so
    /// it can never be turned into a branch-deleting primitive.
    pub fn discard_candidate(&self, candidate_ref: &str) -> rk_core::Result<()> {
        if !candidate_ref.starts_with(CANDIDATE_REF_PREFIX) {
            return Err(rk_core::Error::other(format!(
                "not a candidate ref: {candidate_ref:?}"
            )));
        }
        let _ = self.git(&["update-ref", "-d", candidate_ref]);
        Ok(())
    }

    pub fn candidate_refs(&self) -> rk_core::Result<Vec<String>> {
        let output = self.git(&["for-each-ref", "--format=%(refname)", CANDIDATE_REF_PREFIX])?;
        Ok(output
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect())
    }

    /// Push `branch` to `remote`, setting upstream (`-u`). Returns git's
    /// combined output — remote messages (GitHub's compare URL, GitLab's MR
    /// URL) are printed on stderr, so both streams are captured. Uses the
    /// repo's already-configured credentials; no separate auth surface.
    pub fn push_branch(&self, branch: &str, remote: &str) -> rk_core::Result<String> {
        self.push_branch_as(branch, branch, remote)
    }

    /// Push a local branch to a separately named remote branch.
    pub fn push_branch_as(
        &self,
        branch: &str,
        remote_branch: &str,
        remote: &str,
    ) -> rk_core::Result<String> {
        if branch.trim().is_empty() || remote_branch.trim().is_empty() || remote.trim().is_empty() {
            return Err(rk_core::Error::other(
                "push_branch_as requires local branch, remote branch, and remote",
            ));
        }
        self.validate_local_branch(branch, "branch to push")?;
        self.validate_branch_name(remote_branch, "remote branch")?;
        let refspec = format!("{branch}:{remote_branch}");
        git_output(&self.root, &["push", "-u", remote, &refspec])
    }

    /// The [`Host`] kind of `remote`, inferred from its configured URL.
    /// Unresolvable remotes report [`Host::Unknown`] rather than erroring.
    pub fn remote_host(&self, remote: &str) -> Host {
        self.git(&["remote", "get-url", remote])
            .map(|u| infer_host(u.trim()))
            .unwrap_or(Host::Unknown)
    }

    /// Push `branch` and open a pull/merge request against `target`, using
    /// plain `git` only — no `gh`/`glab` dependency (operator decision).
    ///
    /// - **GitLab:** `git push -o merge_request.create -o
    ///   merge_request.target=<target> <remote> <branch>` — the push option
    ///   creates the MR server-side; the URL comes back on stderr.
    /// - **GitHub / unknown:** `git push -u <remote> <branch>` (no API via
    ///   push) and surface the compare URL git prints for a human to click.
    ///
    /// The host is inferred from the remote's URL. Mirrors [`merge_branch`]'s
    /// clean-failure contract: a push/auth/remote failure is a
    /// `PrOutcome { opened: false, .. }`, never a panic.
    ///
    /// [`merge_branch`]: Repo::merge_branch
    pub fn open_pull_request(&self, branch: &str, target: &str, remote: &str) -> PrOutcome {
        self.open_pull_request_as(branch, branch, target, remote)
    }

    /// Push `branch` as `remote_branch` and open a PR/MR against `target`.
    pub fn open_pull_request_as(
        &self,
        branch: &str,
        remote_branch: &str,
        target: &str,
        remote: &str,
    ) -> PrOutcome {
        if branch.trim().is_empty()
            || remote_branch.trim().is_empty()
            || target.trim().is_empty()
            || remote.trim().is_empty()
        {
            return PrOutcome {
                opened: false,
                url: None,
                detail:
                    "open_pull_request_as requires local branch, remote branch, target, and remote"
                        .into(),
            };
        }
        if let Err(e) = self
            .validate_local_branch(branch, "pull request source")
            .and_then(|_| self.validate_branch_name(remote_branch, "pull request remote source"))
            .and_then(|_| self.validate_local_branch(target, "pull request target"))
        {
            return PrOutcome {
                opened: false,
                url: None,
                detail: e.to_string(),
            };
        }
        let host = self.remote_host(remote);
        let target_opt = format!("merge_request.target={target}");
        let refspec = format!("{branch}:{remote_branch}");
        let args: Vec<&str> = match host {
            Host::GitLab => vec![
                "push",
                "-o",
                "merge_request.create",
                "-o",
                &target_opt,
                remote,
                &refspec,
            ],
            // GitHub has no create-PR-via-push; push and surface the compare URL.
            Host::GitHub | Host::Unknown => vec!["push", "-u", remote, &refspec],
        };
        match git_output(&self.root, &args) {
            Ok(out) => {
                let url = extract_pr_url(&out);
                let detail = match host {
                    Host::GitLab => "merge request created via push option".into(),
                    Host::GitHub => {
                        "branch pushed; open the pull request via the compare URL".into()
                    }
                    Host::Unknown => {
                        "branch pushed to an unrecognized host; open the PR manually".into()
                    }
                };
                PrOutcome {
                    opened: true,
                    url,
                    detail,
                }
            }
            Err(e) => PrOutcome {
                opened: false,
                url: None,
                detail: format!("push failed for {branch} -> {remote}/{remote_branch}: {e}"),
            },
        }
    }

    /// Advance `target` from `expected` to `merged` (a fast-forward — `merged`
    /// descends from `expected`).
    ///
    /// If the operator has `target` checked out in the main worktree, moving
    /// the ref alone leaves their index+working tree stale: the merged files
    /// live in the new HEAD but not on disk, so `git status` reports them as
    /// deleted. So when the root is on `target`, fast-forward it in place
    /// (`merge --ff-only`), which advances the ref *and* refreshes the working
    /// tree while preserving any non-conflicting local edits. If Git refuses
    /// that checkout update, return the error and leave the target ref alone;
    /// a bare ref move would create a checkout/ref split that lies to the
    /// operator about what is actually on disk.
    fn advance_target(&self, target: &str, merged: &str, expected: &str) -> rk_core::Result<()> {
        let root_on_target = self.current_branch().ok().as_deref() == Some(target);
        if root_on_target {
            self.git(&["merge", "--ff-only", merged])?;
            return Ok(());
        }
        self.git(&[
            "update-ref",
            &format!("refs/heads/{target}"),
            merged,
            expected,
        ])?;
        Ok(())
    }

    fn validate_branch_name(&self, branch: &str, role: &str) -> rk_core::Result<()> {
        if branch.trim().is_empty() || branch.starts_with('-') || branch.starts_with("refs/") {
            return Err(rk_core::Error::other(format!(
                "invalid {role} branch name: {branch:?}"
            )));
        }
        self.git(&["check-ref-format", "--branch", branch])?;
        Ok(())
    }

    fn validate_local_branch(&self, branch: &str, role: &str) -> rk_core::Result<()> {
        self.validate_branch_name(branch, role)?;
        let reference = format!("refs/heads/{branch}");
        self.git(["show-ref", "--verify", "--quiet", &reference].as_slice())
            .map(|_| ())
            .map_err(|_| rk_core::Error::other(format!("{role} does not exist: {branch}")))
    }

    fn git(&self, args: &[&str]) -> rk_core::Result<String> {
        git_in(&self.root, args)
    }
}

fn git_in(dir: &Path, args: &[&str]) -> rk_core::Result<String> {
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
            failure_reason(&out)
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

/// Why a git invocation failed, in one line. Prefers stderr and falls back to
/// stdout, because a failing `git merge` writes its whole diagnostic
/// ("CONFLICT (content): Merge conflict in …", "Automatic merge failed") to
/// STDOUT and leaves stderr empty. Reading stderr alone turned every conflict
/// into `merge conflict or failure: git merge … failed:` with nothing after the
/// colon — the outcome recorded on the `branch_landed` event and shown to the
/// operator, naming neither the conflicting files nor even that it was a
/// conflict (TKT-171).
///
/// Flattened onto one line — callers render it inline in an event payload and
/// an inbox row — and capped at [`REASON_LINES`], because a merge prints one
/// line per conflicting path and a hundred-file conflict must not become a
/// hundred-line "detail" string. A git that fails silently still reports its
/// exit code, so the reason is never empty.
fn failure_reason(out: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    // `git merge` reports conflicts on stdout with an empty stderr; everything
    // else reports on stderr. Prefer stderr, fall back to stdout.
    let text = if stderr.trim().is_empty() {
        stdout
    } else {
        stderr
    };
    let lines: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    if lines.is_empty() {
        return format!("exited {}", out.status.code().unwrap_or(-1));
    }
    let head = lines
        .iter()
        .take(REASON_LINES)
        .copied()
        .collect::<Vec<_>>()
        .join("; ");
    match lines.len().checked_sub(REASON_LINES) {
        Some(rest) if rest > 0 => format!("{head} (+{rest} more lines)"),
        _ => head,
    }
}

/// How many lines of a failing git's output [`failure_reason`] keeps.
const REASON_LINES: usize = 3;

/// Like [`git_in`], but returns stdout AND stderr on success. `git push`
/// writes progress and remote messages (PR/MR URLs) to stderr, so the plain
/// stdout-only capture would drop exactly the output we need.
fn git_output(dir: &Path, args: &[&str]) -> rk_core::Result<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .map_err(|e| rk_core::Error::other(format!("git not runnable: {e}")))?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    if !out.status.success() {
        return Err(rk_core::Error::other(format!(
            "git {} failed: {}",
            args.join(" "),
            stderr.trim()
        )));
    }
    Ok(format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        stderr
    ))
}

/// Like [`git_in`], but kills the child if it runs longer than `timeout`.
/// For network operations (`fetch`) that can hang indefinitely on an
/// unreachable remote or a credential prompt; a timeout is a clean `Err`, not a
/// pinned thread. Polls `try_wait` on a short interval — cheap for an operation
/// measured in seconds, and the captured output is bounded (`--quiet`) so the
/// pipe never fills while we poll.
/// Bound for local (no-network) git reads on daemon hot paths. See
/// [`Repo::rev_parse`] for why these are bounded at all.
const LOCAL_READ_TIMEOUT: Duration = Duration::from_secs(30);

fn git_bounded(dir: &Path, args: &[&str], timeout: Duration) -> rk_core::Result<String> {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| rk_core::Error::other(format!("git not runnable: {e}")))?;
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let out = child.wait_with_output().map_err(|e| {
                    rk_core::Error::other(format!("git {} output failed: {e}", args.join(" ")))
                })?;
                if !status.success() {
                    return Err(rk_core::Error::other(format!(
                        "git {} failed: {}",
                        args.join(" "),
                        String::from_utf8_lossy(&out.stderr).trim()
                    )));
                }
                return Ok(String::from_utf8_lossy(&out.stdout).to_string());
            }
            Ok(None) => {
                if start.elapsed() >= timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(rk_core::Error::other(format!(
                        "git {} timed out after {timeout:?}",
                        args.join(" ")
                    )));
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(rk_core::Error::other(format!(
                    "git {} wait failed: {e}",
                    args.join(" ")
                )));
            }
        }
    }
}

/// Infer the [`Host`] from a remote URL (`git@github.com:o/r.git`,
/// `https://gitlab.example.com/o/r.git`, …). Case-insensitive substring match
/// on the URL — enough to pick the push strategy; anything else is `Unknown`.
fn infer_host(remote_url: &str) -> Host {
    let u = remote_url.to_ascii_lowercase();
    if u.contains("gitlab") {
        Host::GitLab
    } else if u.contains("github") {
        Host::GitHub
    } else {
        Host::Unknown
    }
}

/// Pull the first PR/MR URL out of git's push output. GitHub prints a
/// `.../pull/new/<branch>` compare URL; GitLab prints a `.../merge_requests/N`
/// URL. Prefer a token that looks like a PR/MR link, else the first URL.
fn extract_pr_url(push_output: &str) -> Option<String> {
    let trim = |t: &str| {
        t.trim_matches(|c: char| c == '.' || c == ',' || c == ')' || c == '"' || c == '\'')
            .to_string()
    };
    let is_url = |t: &str| t.starts_with("http://") || t.starts_with("https://");
    let tokens = || push_output.split_whitespace();
    tokens()
        .find(|t| is_url(t) && (t.contains("merge_request") || t.contains("pull")))
        .or_else(|| tokens().find(|t| is_url(t)))
        .map(trim)
}

/// Branch name for an agent's task work.
pub fn agent_branch(agent: &str, task: &str) -> String {
    let clean = |s: &str| {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' {
                    c.to_ascii_lowercase()
                } else {
                    '-'
                }
            })
            .collect::<String>()
            .trim_matches('-')
            .to_string()
    };
    format!("rat/{}/{}", clean(agent), clean(task))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_repo() -> (tempfile::TempDir, Repo) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        run(path, &["init", "-b", "main"]);
        run(path, &["config", "user.email", "rat@example.com"]);
        run(path, &["config", "user.name", "Rat"]);
        std::fs::write(path.join("README.md"), "# scratch\n").unwrap();
        run(path, &["add", "."]);
        run(path, &["commit", "-m", "init"]);
        let repo = Repo::discover(path).unwrap();
        (dir, repo)
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

    /// Create a bare repo under `dir` named `<name>.git`, register it as a
    /// remote called `origin` in `repo`, and return the bare repo's path.
    /// The name lets `infer_host` see "github"/"gitlab" in the local path so
    /// the host-specific push strategy is exercised end to end.
    fn bare_remote(parent: &Path, name: &str, repo: &Repo, push_options: bool) -> PathBuf {
        let bare = parent.join(format!("{name}.git"));
        run(parent, &["init", "--bare", &bare.to_string_lossy()]);
        if push_options {
            run(&bare, &["config", "receive.advertisePushOptions", "true"]);
        }
        repo.git(&["remote", "add", "origin", &bare.to_string_lossy()])
            .unwrap();
        bare
    }

    fn commit_on_branch(dir: &Path, repo: &Repo, agent: &str, task: &str) -> String {
        let wt = dir.join(format!("wt-{agent}"));
        let branch = agent_branch(agent, task);
        repo.create_worktree(&wt, &branch, "main").unwrap();
        // Keep independently-created fixture commits distinct even when git
        // assigns them the same second-level timestamp. Otherwise two sibling
        // branches can receive the same commit SHA and make ancestry tests
        // report that landing one also landed the other.
        std::fs::write(wt.join(format!("feature-{agent}-{task}.txt")), "cheese\n").unwrap();
        run(&wt, &["add", "."]);
        run(&wt, &["commit", "-m", "add feature"]);
        branch
    }

    /// A branch cut from `main` that never received a commit — tip == fork
    /// point. The commit-count-awareness regression case: this must never
    /// read as "merged"/"delivered" just because its tip trivially satisfies
    /// `is_ancestor` against a target it never diverged from.
    fn empty_branch(dir: &Path, repo: &Repo, agent: &str, task: &str) -> String {
        let wt = dir.join(format!("wt-{agent}"));
        let branch = agent_branch(agent, task);
        repo.create_worktree(&wt, &branch, "main").unwrap();
        branch
    }

    /// The acceptance property: the sha a gate ran on is the sha that lands.
    /// Stands in for the gate with an assertion over the prepared commit's own
    /// tree — deliberately no build tooling, since the primitive must hold for
    /// a repo of any language.
    #[test]
    fn landed_sha_equals_the_sha_the_gate_ran_on() {
        let (dir, repo) = scratch_repo();
        let branch = commit_on_branch(dir.path(), &repo, "thistle", "cas");
        let PrepareOutcome::Prepared(candidate) = repo.prepare_merge(&branch, "main").unwrap()
        else {
            panic!("expected a clean merge");
        };
        assert!(!candidate.is_empty(), "branch had a commit to merge");
        assert_eq!(candidate.base, repo.rev_parse("refs/heads/main").unwrap());
        // Preparing does not land: main is untouched while the gate runs.
        assert_eq!(repo.rev_parse("main").unwrap(), candidate.base);

        // The "gate": read the tree a checkout of the candidate would give,
        // proving it really is the merged result. It is reachable purely
        // because prepare parked it — nothing else points at it yet.
        assert_eq!(
            repo.rev_parse(&candidate.candidate_ref).unwrap(),
            candidate.commit
        );
        let tested = repo.rev_parse(&candidate.commit).unwrap();
        let gate_saw = repo
            .git(&["ls-tree", "-r", "--name-only", &tested])
            .unwrap();
        assert!(
            gate_saw.contains("feature-thistle-cas.txt"),
            "gate saw: {gate_saw}"
        );
        assert!(gate_saw.contains("README.md"), "gate saw: {gate_saw}");

        let outcome = repo
            .advance_target_to("main", &candidate.commit, &candidate.base)
            .unwrap();
        assert_eq!(
            outcome,
            AdvanceOutcome::Advanced {
                commit: tested.clone()
            }
        );
        // The invariant.
        assert_eq!(repo.rev_parse("refs/heads/main").unwrap(), tested);
        assert!(repo.branch_verified_merged(&branch, "main"));

        repo.discard_candidate(&candidate.candidate_ref).unwrap();
        assert!(repo.rev_parse(&candidate.candidate_ref).is_err());
        // Discarding the parking ref does not unland the commit.
        assert_eq!(repo.rev_parse("refs/heads/main").unwrap(), tested);
    }

    #[test]
    fn advance_refuses_a_moved_target_and_stays_retryable() {
        let (dir, repo) = scratch_repo();
        let branch = commit_on_branch(dir.path(), &repo, "thistle", "stale");
        let PrepareOutcome::Prepared(candidate) = repo.prepare_merge(&branch, "main").unwrap()
        else {
            panic!("expected a clean merge");
        };

        // Someone else lands on main while our gate is running.
        let other = commit_on_branch(dir.path(), &repo, "nibble", "race");
        assert!(repo.merge_branch(&other, "main").unwrap().merged);
        let moved_to = repo.rev_parse("refs/heads/main").unwrap();
        assert_ne!(moved_to, candidate.base);

        let outcome = repo
            .advance_target_to("main", &candidate.commit, &candidate.base)
            .unwrap();
        assert_eq!(
            outcome,
            AdvanceOutcome::Stale {
                expected: candidate.base.clone(),
                actual: moved_to.clone(),
            }
        );
        assert!(outcome.retryable() && !outcome.advanced());
        assert_eq!(outcome.commit(), None);
        // Nothing landed: main still carries only the other rat's merge.
        assert_eq!(repo.rev_parse("refs/heads/main").unwrap(), moved_to);
        assert!(!repo.branch_verified_merged(&branch, "main"));
        // Nothing lost: the candidate is still parked, so a retry can rebuild.
        assert_eq!(
            repo.rev_parse(&candidate.candidate_ref).unwrap(),
            candidate.commit
        );

        // The retry: rebuild on the new tip and land that.
        let PrepareOutcome::Prepared(retry) = repo.prepare_merge(&branch, "main").unwrap() else {
            panic!("expected a clean merge");
        };
        assert_eq!(retry.base, moved_to);
        assert!(repo
            .advance_target_to("main", &retry.commit, &retry.base)
            .unwrap()
            .advanced());
        assert_eq!(repo.rev_parse("refs/heads/main").unwrap(), retry.commit);
    }

    #[test]
    fn advance_rejects_a_commit_that_would_discard_the_target() {
        let (dir, repo) = scratch_repo();
        let base = repo.rev_parse("refs/heads/main").unwrap();
        // A branch commit is not a descendant of main's tip once main moves on.
        let sibling = commit_on_branch(dir.path(), &repo, "thistle", "sibling");
        let other = commit_on_branch(dir.path(), &repo, "nibble", "onward");
        assert!(repo.merge_branch(&other, "main").unwrap().merged);
        let tip = repo.rev_parse("refs/heads/main").unwrap();

        let err = repo
            .advance_target_to("main", &sibling, &tip)
            .expect_err("a non-descendant would discard the target's history");
        assert!(err.to_string().contains("does not descend"));
        assert_eq!(repo.rev_parse("refs/heads/main").unwrap(), tip);

        // Advancing to the expected parent itself is a no-op, not an error:
        // the caller may retry a swap whose answer it lost.
        assert_eq!(
            repo.advance_target_to("main", &tip, &tip).unwrap(),
            AdvanceOutcome::Advanced { commit: tip }
        );
        // A guard naming a commit that never was still resolves-or-errors
        // rather than landing anything.
        assert!(repo.advance_target_to("main", &base, &base).is_ok());
    }

    #[test]
    fn prepare_merge_reports_conflict_without_touching_the_target() {
        let (dir, repo) = scratch_repo();
        // Two branches editing the same file at the same place.
        let wt = dir.path().join("wt-conflict");
        let branch = agent_branch("thistle", "conflict");
        repo.create_worktree(&wt, &branch, "main").unwrap();
        std::fs::write(wt.join("README.md"), "# theirs\n").unwrap();
        run(&wt, &["commit", "-am", "theirs"]);

        run(dir.path(), &["checkout", "main"]);
        std::fs::write(dir.path().join("README.md"), "# ours\n").unwrap();
        run(dir.path(), &["commit", "-am", "ours"]);
        let before = repo.rev_parse("refs/heads/main").unwrap();

        let outcome = repo.prepare_merge(&branch, "main").unwrap();
        assert!(matches!(outcome, PrepareOutcome::Conflict { .. }));
        assert_eq!(repo.rev_parse("refs/heads/main").unwrap(), before);
    }

    #[test]
    fn discard_candidate_refuses_refs_outside_its_namespace() {
        let (_dir, repo) = scratch_repo();
        let tip = repo.rev_parse("refs/heads/main").unwrap();
        assert!(repo.discard_candidate("refs/heads/main").is_err());
        assert_eq!(repo.rev_parse("refs/heads/main").unwrap(), tip);
        // Idempotent within its own namespace.
        repo.discard_candidate(&format!("{CANDIDATE_REF_PREFIX}{tip}"))
            .unwrap();
    }

    #[test]
    fn infer_host_reads_the_url() {
        assert_eq!(infer_host("git@github.com:o/r.git"), Host::GitHub);
        assert_eq!(infer_host("https://github.com/o/r.git"), Host::GitHub);
        assert_eq!(infer_host("git@gitlab.com:o/r.git"), Host::GitLab);
        assert_eq!(infer_host("https://gitlab.example.com/o/r"), Host::GitLab);
        assert_eq!(infer_host("git@bitbucket.org:o/r.git"), Host::Unknown);
        assert_eq!(infer_host(""), Host::Unknown);
    }

    #[test]
    fn extract_pr_url_finds_the_link() {
        let github = "remote: Create a pull request for 'rat/x/y' on GitHub by visiting:\n\
                      remote:   https://github.com/o/r/pull/new/rat/x/y\n";
        assert_eq!(
            extract_pr_url(github).as_deref(),
            Some("https://github.com/o/r/pull/new/rat/x/y")
        );
        let gitlab = "remote: View merge request for rat/x/y:\n\
                      remote:   https://gitlab.com/o/r/-/merge_requests/7\n";
        assert_eq!(
            extract_pr_url(gitlab).as_deref(),
            Some("https://gitlab.com/o/r/-/merge_requests/7")
        );
        // A push with no PR affordance surfaces nothing.
        assert_eq!(extract_pr_url("Everything up-to-date\n"), None);
    }

    #[test]
    fn push_branch_pushes_to_remote() {
        let (dir, repo) = scratch_repo();
        let bare = bare_remote(dir.path(), "plain-remote", &repo, false);
        let branch = commit_on_branch(dir.path(), &repo, "pip", "task-1");

        repo.push_branch(&branch, "origin").unwrap();

        // The branch now exists on the remote.
        let refs = git_in(&bare, &["rev-parse", &format!("refs/heads/{branch}")]);
        assert!(refs.is_ok(), "branch should exist on remote: {refs:?}");
    }

    #[test]
    fn push_branch_as_uses_the_configured_remote_name() {
        let (dir, repo) = scratch_repo();
        let bare = bare_remote(dir.path(), "plain-remote", &repo, false);
        let branch = commit_on_branch(dir.path(), &repo, "pip", "task-remote-name");

        repo.push_branch_as(&branch, "review/task-remote-name", "origin")
            .unwrap();

        assert!(git_in(&bare, &["rev-parse", "refs/heads/review/task-remote-name"]).is_ok());
        assert!(git_in(&bare, &["rev-parse", &format!("refs/heads/{branch}")]).is_err());
    }

    #[test]
    fn open_pr_github_pushes_and_reports_opened() {
        let (dir, repo) = scratch_repo();
        // "github" in the path makes infer_host pick the GitHub strategy.
        let bare = bare_remote(dir.path(), "github-remote", &repo, false);
        let branch = commit_on_branch(dir.path(), &repo, "nibbles", "task-2");
        assert_eq!(repo.remote_host("origin"), Host::GitHub);

        let outcome = repo.open_pull_request(&branch, "main", "origin");
        assert!(outcome.opened, "{}", outcome.detail);
        // A bare local remote prints no compare URL, so url is absent — but the
        // branch made it across.
        assert!(git_in(&bare, &["rev-parse", &format!("refs/heads/{branch}")]).is_ok());
    }

    #[test]
    fn open_pr_gitlab_sends_merge_request_push_options() {
        let (dir, repo) = scratch_repo();
        // "gitlab" in the path selects the MR-push-option strategy; the bare
        // remote must advertise push options or it rejects them.
        let bare = bare_remote(dir.path(), "gitlab-remote", &repo, true);
        let branch = commit_on_branch(dir.path(), &repo, "whisker", "task-3");
        assert_eq!(repo.remote_host("origin"), Host::GitLab);

        let outcome = repo.open_pull_request(&branch, "main", "origin");
        // The bare remote is not a real GitLab, so no MR is created, but the
        // push with options succeeds cleanly (opened) and the branch lands.
        assert!(outcome.opened, "{}", outcome.detail);
        assert!(git_in(&bare, &["rev-parse", &format!("refs/heads/{branch}")]).is_ok());
    }

    #[test]
    fn open_pr_missing_remote_fails_cleanly() {
        let (dir, repo) = scratch_repo();
        let branch = commit_on_branch(dir.path(), &repo, "dot", "task-4");

        // No remote configured at all — must not panic.
        let outcome = repo.open_pull_request(&branch, "main", "origin");
        assert!(!outcome.opened);
        assert!(outcome.url.is_none());
        assert!(!outcome.detail.is_empty());
    }

    #[test]
    fn open_pr_rejects_empty_args() {
        let (_dir, repo) = scratch_repo();
        let outcome = repo.open_pull_request("", "main", "origin");
        assert!(!outcome.opened);
        assert!(repo.push_branch("", "origin").is_err());
    }

    #[test]
    fn agent_branch_sanitizes() {
        assert_eq!(agent_branch("Whisker", ".rk-42"), "rat/whisker/rk-42");
        assert_eq!(agent_branch("Rat 2", "Fix Login!"), "rat/rat-2/fix-login");
    }

    #[test]
    fn worktree_create_work_merge_dismiss() {
        let (dir, repo) = scratch_repo();
        let wt = dir.path().join("wt-whisker");
        let branch = agent_branch("Whisker", "task-1");

        repo.create_worktree(&wt, &branch, "main").unwrap();
        assert!(wt.join("README.md").exists());

        // Agent does work and commits in its worktree.
        std::fs::write(wt.join("feature.txt"), "cheese\n").unwrap();
        run(&wt, &["add", "."]);
        run(&wt, &["commit", "-m", "add feature"]);

        // Dismiss: merge into main (which is checked out in the scratch repo —
        // the temp-worktree strategy must handle that).
        let outcome = repo.merge_branch(&branch, "main").unwrap();
        assert!(outcome.merged, "{}", outcome.detail);

        repo.remove_worktree(&wt).unwrap();
        repo.delete_branch(&branch).unwrap();

        // main now contains the work.
        let log = git_in(dir.path(), &["log", "--oneline", "main"]).unwrap();
        assert!(log.contains("add feature"));
        let files = git_in(dir.path(), &["ls-tree", "--name-only", "main"]).unwrap();
        assert!(files.contains("feature.txt"));
        // main is checked out at the root, so the merge must fast-forward the
        // operator's working tree too — the file is on disk and status is
        // clean (not a phantom "deleted" from a bare ref move).
        assert!(dir.path().join("feature.txt").exists());
        assert!(
            git_in(dir.path(), &["status", "--porcelain"])
                .unwrap()
                .trim()
                .is_empty(),
            "operator's working tree should be clean after auto-merge"
        );
    }

    #[test]
    fn ensure_gate_worktree_is_idempotent() {
        let (dir, repo) = scratch_repo();
        let gate = dir.path().join("gate-worktrees").join("main");

        repo.ensure_gate_worktree(&gate).unwrap();
        assert!(gate.join("README.md").exists());
        let list_before = repo.git(&["worktree", "list", "--porcelain"]).unwrap();

        // A second call finds the worktree already registered and is a no-op —
        // it must not error, re-add, or otherwise disturb the checkout.
        repo.ensure_gate_worktree(&gate).unwrap();
        let list_after = repo.git(&["worktree", "list", "--porcelain"]).unwrap();
        assert_eq!(list_before, list_after);
        assert_eq!(
            list_after
                .matches(&gate.to_string_lossy().to_string())
                .count(),
            1,
            "worktree must be registered exactly once"
        );
    }

    /// Two ensures racing must both succeed and leave exactly one registered,
    /// intact worktree. The un-locked fast-path check alone let the loser's
    /// `reap_stale_worktree` tear down the winner's fresh checkout; the
    /// authoritative recheck under the metadata lock closes that window.
    #[test]
    fn concurrent_ensure_gate_worktree_never_reaps_the_winner() {
        let (dir, repo) = scratch_repo();
        let gate = dir.path().join("gate-worktrees").join("main");
        let root = repo.root().to_path_buf();

        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let root = root.clone();
                    let gate = gate.clone();
                    scope.spawn(move || {
                        let repo = Repo::discover(&root).unwrap();
                        repo.ensure_gate_worktree(&gate)
                    })
                })
                .collect();
            for handle in handles {
                handle.join().unwrap().unwrap();
            }
        });

        assert!(
            gate.join("README.md").exists(),
            "checkout must survive the race"
        );
        let list = repo.git(&["worktree", "list", "--porcelain"]).unwrap();
        assert_eq!(
            list.matches(&gate.to_string_lossy().to_string()).count(),
            1,
            "exactly one registration after 4 concurrent ensures"
        );
    }

    #[test]
    fn reset_gate_worktree_discards_prior_state_but_keeps_ignored_files() {
        let (dir, repo) = scratch_repo();
        // A tracked .gitignore, so `target/` is ignored at every commit below.
        std::fs::write(dir.path().join(".gitignore"), "target/\n").unwrap();
        run(dir.path(), &["add", ".gitignore"]);
        run(dir.path(), &["commit", "-m", "add gitignore"]);

        let gate = dir.path().join("gate-worktrees").join("main");
        repo.ensure_gate_worktree(&gate).unwrap();

        // A second commit on main to reset onto.
        std::fs::write(dir.path().join("second.txt"), "second\n").unwrap();
        run(dir.path(), &["add", "second.txt"]);
        run(dir.path(), &["commit", "-m", "second commit"]);
        let second_sha = repo.rev_parse("main").unwrap();

        // Simulate a prior gate run's mess: an edit to a tracked file, an
        // untracked file, and a warm ignored build cache.
        std::fs::write(gate.join("README.md"), "tampered\n").unwrap();
        std::fs::write(gate.join("untracked.txt"), "junk\n").unwrap();
        std::fs::create_dir_all(gate.join("target")).unwrap();
        std::fs::write(gate.join("target").join("cache"), "warm\n").unwrap();

        repo.reset_gate_worktree(&gate, &second_sha).unwrap();

        assert_eq!(
            git_in(&gate, &["rev-parse", "HEAD"]).unwrap().trim(),
            second_sha
        );
        assert_eq!(
            std::fs::read_to_string(gate.join("README.md")).unwrap(),
            "# scratch\n",
            "tracked-file edit must be discarded by reset"
        );
        assert_eq!(
            std::fs::read_to_string(gate.join("second.txt")).unwrap(),
            "second\n"
        );
        assert!(
            !gate.join("untracked.txt").exists(),
            "untracked file must be discarded by reset"
        );
        assert_eq!(
            std::fs::read_to_string(gate.join("target").join("cache")).unwrap(),
            "warm\n",
            "an ignored build cache must survive reset (clean -fd, not -fdx)"
        );
    }

    #[test]
    fn revert_merge_undoes_a_landed_merge() {
        let (dir, repo) = scratch_repo();
        let wt = dir.path().join("wt-scabbers");
        let branch = agent_branch("Scabbers", "task-5");
        repo.create_worktree(&wt, &branch, "main").unwrap();

        std::fs::write(wt.join("bad.txt"), "regression\n").unwrap();
        run(&wt, &["add", "."]);
        run(&wt, &["commit", "-m", "bad work"]);

        let outcome = repo.merge_branch(&branch, "main").unwrap();
        assert!(outcome.merged, "{}", outcome.detail);
        let merge_commit = outcome
            .commit
            .expect("merge outcome carries the merge commit");
        repo.remove_worktree(&wt).unwrap();
        repo.delete_branch(&branch).unwrap();
        assert!(dir.path().join("bad.txt").exists());

        let revert = repo.revert_merge(&merge_commit, "main").unwrap();
        assert!(revert.merged, "{}", revert.detail);
        assert!(revert.commit.is_some());

        // The bad file is gone from main's tree AND the operator's checkout,
        // while history keeps both the merge and the revert.
        let files = git_in(dir.path(), &["ls-tree", "--name-only", "main"]).unwrap();
        assert!(!files.contains("bad.txt"));
        assert!(!dir.path().join("bad.txt").exists());
        let log = git_in(dir.path(), &["log", "--oneline", "main"]).unwrap();
        assert!(log.contains("Revert"));
        assert!(
            git_in(dir.path(), &["status", "--porcelain"])
                .unwrap()
                .trim()
                .is_empty(),
            "operator's working tree should be clean after revert"
        );
    }

    #[test]
    fn revert_merge_rejects_empty_commit() {
        let (_dir, repo) = scratch_repo();
        assert!(repo.revert_merge("", "main").is_err());
    }

    #[test]
    fn revert_merge_conflict_reports_not_merged() {
        let (dir, repo) = scratch_repo();
        let wt = dir.path().join("wt-templeton");
        let branch = agent_branch("Templeton", "task-6");
        repo.create_worktree(&wt, &branch, "main").unwrap();

        std::fs::write(wt.join("shared.txt"), "rat version\n").unwrap();
        run(&wt, &["add", "."]);
        run(&wt, &["commit", "-m", "rat work"]);
        let outcome = repo.merge_branch(&branch, "main").unwrap();
        assert!(outcome.merged, "{}", outcome.detail);
        let merge_commit = outcome.commit.unwrap();
        repo.remove_worktree(&wt).unwrap();
        repo.delete_branch(&branch).unwrap();

        // Main has since built on the merged file: the revert now conflicts.
        std::fs::write(dir.path().join("shared.txt"), "operator edit on top\n").unwrap();
        run(dir.path(), &["add", "."]);
        run(dir.path(), &["commit", "-m", "build on rat work"]);

        let revert = repo.revert_merge(&merge_commit, "main").unwrap();
        assert!(!revert.merged);
        assert!(revert.commit.is_none());
        assert!(revert.detail.contains("revert conflict or failure"));
        // The conflicted revert must leave main untouched.
        let files = git_in(dir.path(), &["ls-tree", "--name-only", "main"]).unwrap();
        assert!(files.contains("shared.txt"));
    }

    #[test]
    fn merge_preserves_uncommitted_operator_work() {
        let (dir, repo) = scratch_repo();
        let wt = dir.path().join("wt-pip");
        let branch = agent_branch("Pip", "task-3");
        repo.create_worktree(&wt, &branch, "main").unwrap();

        // Rat adds a new file on its branch.
        std::fs::write(wt.join("rat.txt"), "cheese\n").unwrap();
        run(&wt, &["add", "."]);
        run(&wt, &["commit", "-m", "rat work"]);

        // Operator has an uncommitted edit to an unrelated file at the root.
        std::fs::write(dir.path().join("scratch.txt"), "wip\n").unwrap();

        let outcome = repo.merge_branch(&branch, "main").unwrap();
        assert!(outcome.merged, "{}", outcome.detail);

        // The rat's file arrived AND the operator's uncommitted work survived.
        assert!(dir.path().join("rat.txt").exists());
        let scratch = std::fs::read_to_string(dir.path().join("scratch.txt")).unwrap();
        assert_eq!(scratch, "wip\n");
    }

    #[test]
    fn merge_refuses_a_conflicting_dirty_target_checkout() {
        let (dir, repo) = scratch_repo();
        let wt = dir.path().join("wt-scratch");
        let branch = agent_branch("Scratch", "task-4");
        repo.create_worktree(&wt, &branch, "main").unwrap();

        std::fs::write(wt.join("README.md"), "rat version\n").unwrap();
        run(&wt, &["add", "README.md"]);
        run(&wt, &["commit", "-m", "rat work"]);

        // The operator has an uncommitted edit to the same path in the live
        // target checkout. Updating refs behind its back would make HEAD and
        // the files on disk disagree, so the merge must fail closed.
        std::fs::write(dir.path().join("README.md"), "human work\n").unwrap();
        let before = git_in(dir.path(), &["rev-parse", "refs/heads/main"]).unwrap();
        let result = repo.merge_branch(&branch, "main");
        assert!(
            result.is_err(),
            "dirty target checkout must block ref advance"
        );
        let after = git_in(dir.path(), &["rev-parse", "refs/heads/main"]).unwrap();
        assert_eq!(after, before, "main ref must remain unchanged");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("README.md")).unwrap(),
            "human work\n"
        );
    }

    #[test]
    fn merge_conflict_reports_not_merged() {
        let (dir, repo) = scratch_repo();
        let wt = dir.path().join("wt-nibbles");
        let branch = agent_branch("Nibbles", "task-2");
        repo.create_worktree(&wt, &branch, "main").unwrap();

        // Diverge: same line changed on both sides.
        std::fs::write(wt.join("README.md"), "# rat version\n").unwrap();
        run(&wt, &["add", "."]);
        run(&wt, &["commit", "-m", "rat edit"]);
        std::fs::write(dir.path().join("README.md"), "# human version\n").unwrap();
        run(dir.path(), &["add", "."]);
        run(dir.path(), &["commit", "-m", "human edit"]);

        let outcome = repo.merge_branch(&branch, "main").unwrap();
        assert!(!outcome.merged);
        assert!(outcome.detail.contains("conflict"), "{}", outcome.detail);
        // The REASON must survive into the outcome, not just the word
        // "conflict" from our own wrapper text (TKT-171). `git merge` writes
        // its whole diagnostic to stdout and leaves stderr empty, so reading
        // stderr alone produced `... failed:` with nothing after the colon —
        // which is what the operator saw on the recorded `branch_landed` event
        // and had to reconstruct by hand.
        assert!(
            outcome.detail.contains("CONFLICT"),
            "detail must name the conflict git reported: {}",
            outcome.detail
        );
        assert!(
            outcome.detail.contains("README.md"),
            "detail must name the conflicting file: {}",
            outcome.detail
        );
        // Branch preserved for humans to resolve.
        assert!(repo.branch_exists(&branch));
    }

    #[test]
    fn failure_reason_prefers_stderr_and_caps_a_noisy_conflict() {
        use std::os::unix::process::ExitStatusExt;
        let out = |stdout: &str, stderr: &str| std::process::Output {
            status: std::process::ExitStatus::from_raw(256),
            stdout: stdout.as_bytes().to_vec(),
            stderr: stderr.as_bytes().to_vec(),
        };
        // Normal git: stderr carries the message.
        assert_eq!(
            failure_reason(&out("", "fatal: bad ref\n")),
            "fatal: bad ref"
        );
        // `git merge`: stderr empty, everything on stdout.
        assert_eq!(
            failure_reason(&out("CONFLICT (content): Merge conflict in a.rs\n", "")),
            "CONFLICT (content): Merge conflict in a.rs"
        );
        // stderr wins when both are present.
        assert_eq!(
            failure_reason(&out("noise\n", "real reason\n")),
            "real reason"
        );
        // A wide conflict is capped, and says so rather than truncating silently.
        let wide: String = (0..10)
            .map(|i| format!("Merge conflict in f{i}.rs\n"))
            .collect();
        let capped = failure_reason(&out(&wide, ""));
        assert!(capped.starts_with("Merge conflict in f0.rs; Merge conflict in f1.rs"));
        assert!(capped.ends_with("(+7 more lines)"), "{capped}");
        // A git that fails saying nothing at all still reports something.
        assert_eq!(failure_reason(&out("", "")), "exited 1");
    }

    #[test]
    fn concurrent_merges_to_distinct_targets_dont_collide() {
        // Two merges running at once into *different* targets must not reuse one
        // temporary worktree path. Before the per-merge sequence, both used
        // `.git/rk-merge-<pid>` and the second `worktree add` failed hard.
        let (dir, repo) = scratch_repo();
        run(dir.path(), &["branch", "develop"]);
        // Park the root on neither target so both merges take the bare ref-move
        // path (no shared index.lock) — the temp-path collision is what's tested.
        run(dir.path(), &["checkout", "-q", "-b", "scratch-head"]);

        let wt_a = dir.path().join("wt-a");
        let wt_b = dir.path().join("wt-b");
        let branch_a = agent_branch("A", "t");
        let branch_b = agent_branch("B", "t");
        repo.create_worktree(&wt_a, &branch_a, "main").unwrap();
        repo.create_worktree(&wt_b, &branch_b, "develop").unwrap();
        std::fs::write(wt_a.join("a.txt"), "a\n").unwrap();
        run(&wt_a, &["add", "."]);
        run(&wt_a, &["commit", "-m", "a"]);
        std::fs::write(wt_b.join("b.txt"), "b\n").unwrap();
        run(&wt_b, &["add", "."]);
        run(&wt_b, &["commit", "-m", "b"]);

        let (repo_a, repo_b) = (repo.clone(), repo.clone());
        let (ba, bb) = (branch_a.clone(), branch_b.clone());
        let h1 = std::thread::spawn(move || repo_a.merge_branch(&ba, "main").unwrap());
        let h2 = std::thread::spawn(move || repo_b.merge_branch(&bb, "develop").unwrap());
        let out_a = h1.join().unwrap();
        let out_b = h2.join().unwrap();

        assert!(out_a.merged, "merge into main: {}", out_a.detail);
        assert!(out_b.merged, "merge into develop: {}", out_b.detail);
        assert!(git_in(dir.path(), &["ls-tree", "--name-only", "main"])
            .unwrap()
            .contains("a.txt"));
        assert!(git_in(dir.path(), &["ls-tree", "--name-only", "develop"])
            .unwrap()
            .contains("b.txt"));
    }

    #[test]
    fn protected_branches_are_refused() {
        let (dir, repo) = scratch_repo();
        let wt = dir.path().join("wt-bad");
        assert!(repo.create_worktree(&wt, "main", "main").is_err());
        assert!(repo.delete_branch("master").is_err());
    }

    #[test]
    fn create_worktree_reaps_unregistered_leftover_dir() {
        // A prior rat of the same reused name crashed after its worktree was
        // unregistered (pruned) but before the directory was deleted, leaving
        // stale residue on disk. A fresh `worktree add` at that path must not
        // fail hard — the residue is reaped first.
        let (dir, repo) = scratch_repo();
        let wt = dir.path().join("worktrees").join("Nibbles");
        std::fs::create_dir_all(&wt).unwrap();
        std::fs::write(wt.join("junk.txt"), "leftover\n").unwrap();

        repo.create_worktree(&wt, "rat/Nibbles/task-9", "main")
            .unwrap();
        assert!(repo.branch_exists("rat/Nibbles/task-9"));
        assert_eq!(
            Repo::discover(&wt).unwrap().root().canonicalize().unwrap(),
            repo.root().canonicalize().unwrap()
        );
    }

    #[test]
    fn create_worktree_reaps_still_registered_worktree() {
        // Residue that git STILL tracks as a live worktree (the registration
        // under .git/worktrees survived): reserve reuses the name, so a fresh
        // add at the same path must reap the registered worktree too, not
        // collide with it.
        let (dir, repo) = scratch_repo();
        let wt = dir.path().join("worktrees").join("Scurry");
        repo.create_worktree(&wt, "rat/Scurry/old-task", "main")
            .unwrap();
        assert!(wt.exists());

        // Same path, new branch — the name was reused for a new task.
        repo.create_worktree(&wt, "rat/Scurry/new-task", "main")
            .unwrap();
        assert!(repo.branch_exists("rat/Scurry/new-task"));
        assert_eq!(
            Repo::discover(&wt).unwrap().root().canonicalize().unwrap(),
            repo.root().canonicalize().unwrap()
        );
    }

    #[test]
    fn discover_resolves_worktree_to_root() {
        let (dir, repo) = scratch_repo();
        let wt = dir.path().join("wt-x");
        repo.create_worktree(&wt, "rat/x/t", "main").unwrap();
        let from_wt = Repo::discover(&wt).unwrap();
        assert_eq!(
            from_wt.root().canonicalize().unwrap(),
            repo.root().canonicalize().unwrap()
        );
    }

    #[test]
    fn branch_merged_or_gone_tracks_the_pr_lifecycle() {
        let (dir, repo) = scratch_repo();
        let branch = commit_on_branch(dir.path(), &repo, "pip", "task-1");

        // Fresh PR branch ahead of main: not merged, still present.
        assert!(repo.branch_exists(&branch));
        assert!(!repo.is_ancestor(&branch, "main"));
        assert!(
            !repo.branch_merged_or_gone(&branch, "main"),
            "an open, unmerged PR branch must not auto-clear"
        );

        // Human merges the PR: the branch tip becomes an ancestor of main.
        let outcome = repo.merge_branch(&branch, "main").unwrap();
        assert!(outcome.merged, "{}", outcome.detail);
        assert!(repo.is_ancestor(&branch, "main"));
        assert!(
            repo.branch_merged_or_gone(&branch, "main"),
            "a branch merged into target auto-clears"
        );
    }

    #[test]
    fn branch_merged_or_gone_when_branch_deleted() {
        let (dir, repo) = scratch_repo();
        let branch = commit_on_branch(dir.path(), &repo, "nibbles", "task-2");
        // Human deletes the branch on the forge (PR mode keeps it otherwise),
        // reflected locally as a gone ref — treated as cleared even without an
        // ancestry check, since there is no ref left to compare.
        repo.remove_worktree(&dir.path().join("wt-nibbles"))
            .unwrap();
        repo.delete_branch(&branch).unwrap();
        assert!(!repo.branch_exists(&branch));
        assert!(repo.branch_merged_or_gone(&branch, "main"));
    }

    #[test]
    fn branch_verified_merged_false_for_a_branch_that_never_diverged() {
        let (dir, repo) = scratch_repo();
        let branch = empty_branch(dir.path(), &repo, "nibbles", "task-empty");
        assert!(
            !repo.branch_verified_merged(&branch, "main"),
            "an empty branch must not read as verifiably merged"
        );
    }

    #[test]
    fn recorded_fork_point_distinguishes_empty_from_fast_forward_merged() {
        let (dir, repo) = scratch_repo();
        let fork = repo.rev_parse("main").unwrap();
        let empty = empty_branch(dir.path(), &repo, "empty", "task-empty");
        assert!(!repo.branch_has_commits_since(&empty, &fork));

        let work = commit_on_branch(dir.path(), &repo, "worker", "task-work");
        assert!(repo.branch_has_commits_since(&work, &fork));
        run(repo.root(), &["merge", "--ff-only", &work]);
        assert_eq!(
            repo.rev_parse("main").unwrap(),
            repo.rev_parse(&work).unwrap()
        );
        assert!(
            repo.branch_has_commits_since(&work, &fork),
            "the durable fork remains distinct after target fast-forwards to head"
        );
    }

    #[test]
    fn is_ancestor_false_on_unknown_revision() {
        let (_dir, repo) = scratch_repo();
        // A bad revision must read as "not an ancestor", never merged.
        assert!(!repo.is_ancestor("rat/does-not-exist/tkt", "main"));
        assert!(!repo.is_ancestor("main", "no-such-target"));
    }

    #[test]
    fn remote_branch_merged_or_gone_sees_a_forge_merge_without_a_local_pull() {
        let (dir, repo) = scratch_repo();
        let bare = bare_remote(dir.path(), "plain-remote", &repo, false);
        let branch = commit_on_branch(dir.path(), &repo, "pip", "task-1");
        run(repo.root(), &["push", "origin", "main"]);
        repo.push_branch(&branch, "origin").unwrap();
        repo.fetch_prune("origin", Duration::from_secs(30)).unwrap();

        // Open PR: origin/<branch> exists and is not yet in origin/main.
        assert!(
            !repo.remote_branch_merged_or_gone(&branch, "main", "origin"),
            "an open, unmerged PR branch must not auto-clear"
        );

        // A human merges the PR on the forge — advance ONLY the bare's main
        // (via a throwaway clone), never the local main. The operator never
        // pulled, so the local check still cannot see it.
        let clone = dir.path().join("forge-clone");
        run(
            dir.path(),
            &["clone", &bare.to_string_lossy(), &clone.to_string_lossy()],
        );
        run(&clone, &["config", "user.email", "forge@example.com"]);
        run(&clone, &["config", "user.name", "Forge"]);
        run(
            &clone,
            &[
                "merge",
                "--no-ff",
                "-m",
                "merge PR",
                &format!("origin/{branch}"),
            ],
        );
        run(&clone, &["push", "origin", "main"]);

        repo.fetch_prune("origin", Duration::from_secs(30)).unwrap();
        assert!(
            repo.remote_branch_merged_or_gone(&branch, "main", "origin"),
            "a branch merged into origin/main auto-clears after a fetch"
        );
        // The whole point of the remote check: the LOCAL target never moved, so
        // the local-only detection still reports the branch open.
        assert!(
            !repo.branch_merged_or_gone(&branch, "main"),
            "local main did not advance — only the remote check sees the merge"
        );
    }

    #[test]
    fn remote_branch_merged_or_gone_when_branch_pruned() {
        let (dir, repo) = scratch_repo();
        let _bare = bare_remote(dir.path(), "plain-remote", &repo, false);
        let branch = commit_on_branch(dir.path(), &repo, "nibbles", "task-2");
        run(repo.root(), &["push", "origin", "main"]);
        repo.push_branch(&branch, "origin").unwrap();
        repo.fetch_prune("origin", Duration::from_secs(30)).unwrap();
        assert!(!repo.remote_branch_merged_or_gone(&branch, "main", "origin"));

        // Human deletes the branch on the forge (post-merge cleanup). A prune
        // drops the stale remote-tracking ref — cleared even without ancestry.
        run(repo.root(), &["push", "origin", "--delete", &branch]);
        repo.fetch_prune("origin", Duration::from_secs(30)).unwrap();
        assert!(repo.remote_branch_merged_or_gone(&branch, "main", "origin"));
    }

    #[test]
    fn fetch_prune_requires_a_remote() {
        let (_dir, repo) = scratch_repo();
        assert!(repo.fetch_prune("", Duration::from_secs(5)).is_err());
    }

    #[test]
    fn rev_parse_resolves_a_branch_to_its_sha() {
        let (dir, repo) = scratch_repo();
        let branch = commit_on_branch(dir.path(), &repo, "gouda", "task-rev");
        let sha = repo.rev_parse(&branch).unwrap();
        assert_eq!(sha.len(), 40, "expected a full sha, got {sha:?}");
        assert_eq!(sha, repo.git(&["rev-parse", &branch]).unwrap().trim());
    }

    #[test]
    fn diff_stat_counts_files_and_lines_vs_base() {
        let (dir, repo) = scratch_repo();
        let wt = dir.path().join("wt-diffstat");
        let branch = agent_branch("brie", "task-diffstat");
        repo.create_worktree(&wt, &branch, "main").unwrap();
        std::fs::write(wt.join("a.txt"), "one\ntwo\nthree\n").unwrap();
        std::fs::write(wt.join("b.txt"), "four\nfive\n").unwrap();
        run(&wt, &["add", "."]);
        run(&wt, &["commit", "-m", "add a and b"]);

        let stat = repo.diff_stat("main", &branch).unwrap();
        assert_eq!(stat.files, vec!["a.txt".to_string(), "b.txt".to_string()]);
        assert_eq!(stat.lines, 5); // 3 + 2 added lines, nothing removed

        // A no-op branch (tip == base) diffs empty rather than erroring.
        let stat = repo.diff_stat("main", "main").unwrap();
        assert!(stat.files.is_empty());
        assert_eq!(stat.lines, 0);
    }

    #[test]
    fn diff_stat_counts_binary_files_as_zero_lines() {
        let (dir, repo) = scratch_repo();
        let wt = dir.path().join("wt-diffstat-binary");
        let branch = agent_branch("edam", "task-diffstat-binary");
        repo.create_worktree(&wt, &branch, "main").unwrap();
        std::fs::write(wt.join("blob.bin"), [0u8, 159, 146, 150]).unwrap();
        run(&wt, &["add", "."]);
        run(&wt, &["commit", "-m", "add binary blob"]);

        let stat = repo.diff_stat("main", &branch).unwrap();
        assert_eq!(stat.files, vec!["blob.bin".to_string()]);
        assert_eq!(
            stat.lines, 0,
            "binary files report '-'/'-', not a line count"
        );
    }
}
