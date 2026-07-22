//! Git lifecycle for agent isolation: worktree per rat, branch per task,
//! merge on dismiss. Shells out to system `git` — proven semantics, and the
//! same binary humans use when they inspect what the rats did.

use std::path::{Path, PathBuf};
use std::process::Command;
use tracing::debug;

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
        let tmp = self
            .root
            .join(".git")
            .join(format!("rk-merge-{}", std::process::id()));
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
                    // Move the branch ref; any checked-out working tree of
                    // target will see it on next status/pull.
                    self.git(&[
                        "update-ref",
                        &format!("refs/heads/{target}"),
                        merged_commit.trim(),
                        target_before.trim(),
                    ])?;
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
        // The user's checkout of main was never touched mid-merge; their
        // working tree updates on next checkout/pull, but the ref moved:
        let files = git_in(dir.path(), &["ls-tree", "--name-only", "main"]).unwrap();
        assert!(files.contains("feature.txt"));
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
