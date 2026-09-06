//! Internal prepared-delivery outcomes. JSON is produced only for wire/event
//! consumers; pipeline decisions and required bookkeeping use these types.

use serde_json::{json, Value};
use std::path::PathBuf;

#[derive(Debug)]
pub(crate) enum TargetAdvance {
    Landed(LandedDelivery),
    Stale(StaleTarget),
    Blocked(BlockedTarget),
}

#[derive(Debug)]
pub(crate) struct StaleTarget {
    pub expected: String,
    pub actual: String,
}

#[derive(Debug)]
pub(crate) struct BlockedTarget {
    pub tested_sha: String,
    pub worktree_path: PathBuf,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Publication {
    Local,
    Pushed,
    PushPending,
}

/// A candidate whose target advance has succeeded. Construction requires the
/// exact tested commit; there is no optional commit or `merged: false` state.
#[derive(Debug, Clone)]
pub(crate) struct LandedDelivery {
    pub branch: String,
    pub target: String,
    merge_commit: String,
    pub content_free: bool,
    pub remote: String,
    pub publication: Publication,
    pub detail: String,
    pub branch_deleted: bool,
    pub batch_branches: Vec<String>,
    pub recovered: bool,
}

impl LandedDelivery {
    pub fn new(
        branch: &str,
        target: &str,
        commit: &str,
        content_free: bool,
    ) -> rk_core::Result<Self> {
        if commit.trim().is_empty() {
            return Err(rk_core::Error::other(
                "landed delivery requires an exact nonempty commit",
            ));
        }
        Ok(Self {
            branch: branch.into(),
            target: target.into(),
            merge_commit: commit.into(),
            content_free,
            remote: String::new(),
            publication: Publication::Local,
            detail: String::new(),
            branch_deleted: false,
            batch_branches: Vec::new(),
            recovered: false,
        })
    }

    pub fn merge_commit(&self) -> &str {
        &self.merge_commit
    }

    pub fn delivered(&self) -> bool {
        self.publication != Publication::PushPending
    }

    pub fn pushed(&self) -> bool {
        self.publication == Publication::Pushed
    }

    pub fn to_json(&self) -> Value {
        let mut value = json!({
            "branch": self.branch,
            "target": self.target,
            "remote": self.remote,
            "delivered": self.delivered(),
            "merged": true,
            "merge_commit": self.merge_commit,
            "tested_sha": self.merge_commit,
            "content_free": self.content_free,
            "pushed": self.pushed(),
            "pr_opened": false,
            "detail": self.detail,
            "branch_deleted": self.branch_deleted,
            "stale": false,
        });
        if !self.batch_branches.is_empty() {
            value["batch_size"] = json!(self.batch_branches.len());
            value["batch_branches"] = json!(self.batch_branches);
        }
        if self.recovered {
            value["recovered"] = json!(true);
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successful_delivery_cannot_omit_its_commit() {
        for commit in ["", "   ", "\n"] {
            assert!(LandedDelivery::new("feature", "main", commit, false).is_err());
        }
    }

    #[test]
    fn publication_distinguishes_a_local_merge_from_completed_push_delivery() {
        let mut delivery =
            LandedDelivery::new("feature", "release", "tested-commit", false).unwrap();
        delivery.publication = Publication::PushPending;
        let pending = delivery.to_json();
        assert_eq!(pending["merged"], true);
        assert_eq!(pending["delivered"], false);
        assert_eq!(pending["pushed"], false);
        assert_eq!(pending["tested_sha"], pending["merge_commit"]);
        delivery.publication = Publication::Pushed;
        assert_eq!(delivery.to_json()["delivered"], true);
        assert_eq!(delivery.to_json()["pushed"], true);
    }
}
