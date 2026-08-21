//! Exact identity of the work a reviewer is judging.
//!
//! This is runtime-owned correlation data, not prose for an agent to recreate.

use serde::{Deserialize, Serialize};

pub const REVIEW_BRANCH_ENV: &str = "RK_REVIEW_BRANCH";
pub const REVIEW_HEAD_ENV: &str = "RK_REVIEW_HEAD";
pub const REVIEW_TARGET_ENV: &str = "RK_REVIEW_TARGET";
pub const REVIEW_TASK_ENV: &str = "RK_REVIEW_TASK";
pub const REVIEW_ATTEMPT_ENV: &str = "RK_REVIEW_ATTEMPT";

/// Supervised spawn identity: the variables `Supervisor::agent_env` sets on
/// every spawned agent (`RK_AGENT` and friends), independent of whether that
/// agent is reviewing anything.
pub const SPAWN_IDENTITY_ENV: &[&str] = &[
    "RK_AGENT",
    "RK_TASK",
    "RK_REPO",
    "RK_ROLE",
    "RK_HOME",
    "RK_BRANCH",
    "RK_WORKTREE",
    "RK_AUTH_TOKEN",
];

/// The complete set of environment variables a `strip_rk_spawn` isolation
/// must remove: supervised spawn identity PLUS the exact-review binding
/// (`RK_REVIEW_*`). A check, prompt, or manual command isolated from spawn
/// identity must also be isolated from an outer reviewer's review binding —
/// otherwise a nested process that itself exercises reviewer-role writes (a
/// repo's own test suite spawning a synthetic reviewer, for example) inherits
/// the real reviewer's `RK_REVIEW_TASK`/etc, which then mismatches its own
/// synthetic `RK_TASK` and gets rejected by the server's exact-review binding
/// check. Every renderer of a `strip_rk_spawn` command — daemon-executed
/// check, prompt fragment, or `mise.toml` task — must derive from exactly
/// this list rather than hand-roll its own, or the copies drift apart.
pub const STRIPPED_RK_SPAWN_ENV: &[&str] = &[
    "RK_AGENT",
    "RK_TASK",
    "RK_REPO",
    "RK_ROLE",
    "RK_HOME",
    "RK_BRANCH",
    "RK_WORKTREE",
    "RK_AUTH_TOKEN",
    REVIEW_BRANCH_ENV,
    REVIEW_HEAD_ENV,
    REVIEW_TARGET_ENV,
    REVIEW_TASK_ENV,
    REVIEW_ATTEMPT_ENV,
];

#[cfg(test)]
mod stripped_env_tests {
    use super::*;

    #[test]
    fn stripped_rk_spawn_env_is_spawn_identity_then_review_bindings() {
        let expected: Vec<&str> = SPAWN_IDENTITY_ENV
            .iter()
            .copied()
            .chain([
                REVIEW_BRANCH_ENV,
                REVIEW_HEAD_ENV,
                REVIEW_TARGET_ENV,
                REVIEW_TASK_ENV,
                REVIEW_ATTEMPT_ENV,
            ])
            .collect();
        assert_eq!(STRIPPED_RK_SPAWN_ENV, expected.as_slice());
    }
}

/// Complete binding carried from a review request through the spawned reviewer
/// and into its verdict artifact.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewContext {
    pub branch: String,
    pub head_sha: String,
    pub target: String,
    pub task: String,
    pub attempt: String,
}

impl ReviewContext {
    pub fn env_pairs(&self) -> [(&'static str, &str); 5] {
        [
            (REVIEW_BRANCH_ENV, &self.branch),
            (REVIEW_HEAD_ENV, &self.head_sha),
            (REVIEW_TARGET_ENV, &self.target),
            (REVIEW_TASK_ENV, &self.task),
            (REVIEW_ATTEMPT_ENV, &self.attempt),
        ]
    }
}
