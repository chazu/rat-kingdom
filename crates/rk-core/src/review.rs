//! Exact identity of the work a reviewer is judging.
//!
//! This is runtime-owned correlation data, not prose for an agent to recreate.

use serde::{Deserialize, Serialize};

pub const REVIEW_BRANCH_ENV: &str = "RK_REVIEW_BRANCH";
pub const REVIEW_HEAD_ENV: &str = "RK_REVIEW_HEAD";
pub const REVIEW_TARGET_ENV: &str = "RK_REVIEW_TARGET";
pub const REVIEW_TASK_ENV: &str = "RK_REVIEW_TASK";
pub const REVIEW_ATTEMPT_ENV: &str = "RK_REVIEW_ATTEMPT";

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
