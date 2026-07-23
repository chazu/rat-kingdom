//! Git lifecycle for agent isolation: worktree per rat, branch per task,
//! merge on dismiss. Shells out to system `git` — proven semantics, and the
//! same binary humans use when they inspect what the rats did.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::debug;

/// Per-merge sequence, so two `merge_branch` calls running at once (e.g. a
/// `dismiss` into `main` and a `land` into `develop`) never collide on the same
/// temporary worktree path. Process id alone is shared by every thread/task, so
/// concurrent in-process merges would otherwise reuse one path and the second
/// `worktree add` would fail. Merges to the *same* target are additionally
/// serialized upstream by the daemon's merge queue.
static MERGE_SEQ: AtomicU64 = AtomicU64::new(0);

pub const PROTECTED_BRANCHES: [&str; 4] = ["main", "master", "develop", "HEAD"];

/// A repository root (the main checkout, not a worktree).
#[derive(Debug, Clone)]
pub struct Repo {
    root: PathBuf,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MergeOutcome {
    pub merged: bool,
    pub detail: String,
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

    /// Create a worktree at `path` on new `branch` forked from `base`.
    pub fn create_worktree(&self, path: &Path, branch: &str, base: &str) -> rk_core::Result<()> {
        if PROTECTED_BRANCHES.contains(&branch) {
            return Err(rk_core::Error::other(format!(
                "refusing to use protected branch: {branch}"
            )));
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
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

    /// Remove a worktree (force: uncommitted changes in it are discarded —
    /// dismiss-with-merge commits first via the agent's own protocol).
    pub fn remove_worktree(&self, path: &Path) -> rk_core::Result<()> {
        self.git(&["worktree", "remove", "--force", &path.to_string_lossy()])?;
        Ok(())
    }

    pub fn prune_worktrees(&self) -> rk_core::Result<()> {
        self.git(&["worktree", "prune"])?;
        Ok(())
    }

    pub fn delete_branch(&self, branch: &str) -> rk_core::Result<()> {
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
        let seq = MERGE_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp = self
            .root
            .join(".git")
            .join(format!("rk-merge-{}-{}", std::process::id(), seq));
        // Detached checkout of target — never conflicts with existing checkouts.
        self.git(&[
            "worktree",
            "add",
            "--detach",
            &tmp.to_string_lossy(),
            target,
        ])?;
        let result = (|| -> rk_core::Result<MergeOutcome> {
            let target_before = self.git(&["rev-parse", &format!("refs/heads/{target}")])?;
            let merge = git_in(
                &tmp,
                &[
                    "merge",
                    "--no-ff",
                    "-m",
                    &format!("merge {branch} into {target} [rk]"),
                    branch,
                ],
            );
            match merge {
                Ok(_) => {
                    let merged_commit = git_in(&tmp, &["rev-parse", "HEAD"])?;
                    let target_now = self.git(&["rev-parse", &format!("refs/heads/{target}")])?;
                    if target_now.trim() != target_before.trim() {
                        return Ok(MergeOutcome {
                            merged: false,
                            detail: format!(
                                "{target} moved during merge; branch {branch} left unmerged"
                            ),
                        });
                    }
                    self.advance_target(target, merged_commit.trim(), target_before.trim())?;
                    Ok(MergeOutcome {
                        merged: true,
                        detail: format!("merged {branch} into {target}"),
                    })
                }
                Err(e) => Ok(MergeOutcome {
                    merged: false,
                    detail: format!("merge conflict or failure: {e}"),
                }),
            }
        })();
        // Always clean up the temp worktree.
        let _ = self.git(&["worktree", "remove", "--force", &tmp.to_string_lossy()]);
        result
    }

    /// Push `branch` to `remote`, setting upstream (`-u`). Returns git's
    /// combined output — remote messages (GitHub's compare URL, GitLab's MR
    /// URL) are printed on stderr, so both streams are captured. Uses the
    /// repo's already-configured credentials; no separate auth surface.
    pub fn push_branch(&self, branch: &str, remote: &str) -> rk_core::Result<String> {
        if branch.trim().is_empty() || remote.trim().is_empty() {
            return Err(rk_core::Error::other(
                "push_branch requires a branch and a remote",
            ));
        }
        git_output(&self.root, &["push", "-u", remote, branch])
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
        if branch.trim().is_empty() || target.trim().is_empty() || remote.trim().is_empty() {
            return PrOutcome {
                opened: false,
                url: None,
                detail: "open_pull_request requires a branch, target, and remote".into(),
            };
        }
        let host = self.remote_host(remote);
        let target_opt = format!("merge_request.target={target}");
        let args: Vec<&str> = match host {
            Host::GitLab => vec![
                "push",
                "-o",
                "merge_request.create",
                "-o",
                &target_opt,
                remote,
                branch,
            ],
            // GitHub has no create-PR-via-push; push and surface the compare URL.
            Host::GitHub | Host::Unknown => vec!["push", "-u", remote, branch],
        };
        match git_output(&self.root, &args) {
            Ok(out) => {
                let url = extract_pr_url(&out);
                let detail = match host {
                    Host::GitLab => "merge request created via push option".into(),
                    Host::GitHub => "branch pushed; open the pull request via the compare URL".into(),
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
                detail: format!("push failed for {branch} -> {remote}: {e}"),
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
    /// tree while preserving any uncommitted local edits. When it isn't (no
    /// live checkout, or the operator has conflicting uncommitted work that a
    /// fast-forward would refuse to touch), fall back to a bare ref move.
    fn advance_target(&self, target: &str, merged: &str, expected: &str) -> rk_core::Result<()> {
        let root_on_target = self.current_branch().ok().as_deref() == Some(target);
        if root_on_target && self.git(&["merge", "--ff-only", merged]).is_ok() {
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
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

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
        std::fs::write(wt.join("feature.txt"), "cheese\n").unwrap();
        run(&wt, &["add", "."]);
        run(&wt, &["commit", "-m", "add feature"]);
        branch
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
        // Branch preserved for humans to resolve.
        assert!(repo.branch_exists(&branch));
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
}
