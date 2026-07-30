//! Explicit landing and recovery for validated onboarding automation.
//!
//! Repository-local workflows, triggers, and schedules are discovered from the
//! registered checkout. Staging and validation in the isolated onboarding
//! worktree are therefore inert. Activation starts only after a durable intent
//! is recorded, then fast-forwards the registered base checkout to the exact
//! approved application commit. Replays recognize that commit in the base
//! history and never repeat the side effect.

use crate::onboarding_apply::validate_automation_file;
use crate::onboarding_proposals::{
    repository_identity, OnboardingActivationStatus, OnboardingProposal, OnboardingProposalStatus,
};
use crate::onboarding_sessions::{
    activation_operation_id, proposal_summary, OnboardingCleanup, OnboardingSession,
    OnboardingSessionState,
};
use chrono::Utc;
use sha2::{Digest, Sha256};
use std::path::Path;
use std::process::{Command, Output};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationContract {
    pub operation_id: String,
    pub expected_base_commit: String,
    pub approved_commit: String,
    pub approved_tree_revision: String,
    pub target_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivationEvidence {
    pub registered_commit: String,
    pub detail: String,
}

pub fn contract(
    session: &OnboardingSession,
    proposal: &OnboardingProposal,
) -> rk_core::Result<ActivationContract> {
    proposal.validate_integrity()?;
    if proposal.status != OnboardingProposalStatus::Verified {
        return Err(rk_core::Error::other(format!(
            "proposal {} must be verified before activation; found {}",
            proposal.id, proposal.status
        )));
    }
    if proposal.automation_kind().is_none() {
        return Err(rk_core::Error::other(format!(
            "proposal {} is not a workflow, trigger, or schedule activation",
            proposal.id
        )));
    }
    if proposal
        .activation
        .as_ref()
        .is_some_and(|activation| activation.status == OnboardingActivationStatus::Declined)
    {
        return Err(rk_core::Error::other(format!(
            "proposal {} activation was declined",
            proposal.id
        )));
    }
    let application = proposal.application.as_ref().ok_or_else(|| {
        rk_core::Error::other(format!(
            "proposal {} has no staged application evidence",
            proposal.id
        ))
    })?;
    let expected_base_commit = git_text(
        &session.repo_path,
        &["rev-parse", &format!("{}^", application.commit)],
    )?;
    let parent_tree = git_text(
        &session.repo_path,
        &["rev-parse", &format!("{}^^{{tree}}", application.commit)],
    )?;
    if parent_tree != proposal.tree_revision {
        return Err(rk_core::Error::other(format!(
            "approved activation parent tree drifted: proposal {}, commit parent {parent_tree}",
            proposal.tree_revision
        )));
    }
    let approved_tree_revision = git_text(
        &session.repo_path,
        &["rev-parse", &format!("{}^{{tree}}", application.commit)],
    )?;
    if approved_tree_revision != application.tree_revision {
        return Err(rk_core::Error::other(format!(
            "approved activation tree drifted: recorded {}, commit {approved_tree_revision}",
            application.tree_revision
        )));
    }
    let committed_target = git_output(
        &session.repo_path,
        &[
            "show",
            &format!("{}:{}", application.commit, proposal.target_path),
        ],
    )?;
    let committed_digest = hex::encode(Sha256::digest(&committed_target.stdout));
    if committed_digest != application.target_digest {
        return Err(rk_core::Error::other(format!(
            "approved activation target drifted: recorded {}, commit {committed_digest}",
            application.target_digest
        )));
    }
    Ok(ActivationContract {
        operation_id: activation_operation_id(
            &session.id,
            &proposal.id,
            &proposal.digest,
            &application.commit,
        ),
        expected_base_commit,
        approved_commit: application.commit.clone(),
        approved_tree_revision,
        target_digest: application.target_digest.clone(),
    })
}

/// Advance the registered checkout, or reconcile a prior advance after a crash.
/// The base must still be the exact parent reviewed with the proposal. The one
/// exception is replay: if the approved commit is already in the base history
/// and the live target still has the approved digest, activation already
/// crossed its boundary and is recorded rather than repeated.
pub fn ensure_activation(
    session: &OnboardingSession,
    proposal: &OnboardingProposal,
    contract: &ActivationContract,
) -> rk_core::Result<ActivationEvidence> {
    if repository_identity(&session.repo_path) != proposal.repository_identity {
        return Err(rk_core::Error::other(format!(
            "registered repository identity drifted for {}",
            proposal.id
        )));
    }
    let branch_head = git_text(
        &session.repo_path,
        &["rev-parse", &format!("refs/heads/{}", session.branch)],
    )?;
    if branch_head != contract.approved_commit {
        return Err(rk_core::Error::other(format!(
            "onboarding branch moved after validation: approved {}, current {branch_head}",
            contract.approved_commit
        )));
    }
    if session.worktree.exists() {
        require_clean(&session.worktree)?;
        let staged_digest = file_digest(&session.worktree.join(&proposal.target_path))?;
        if staged_digest != contract.target_digest {
            return Err(rk_core::Error::other(format!(
                "staged automation content changed after validation: expected {}, current {staged_digest}",
                contract.target_digest
            )));
        }
    }
    let current_branch = git_text(&session.repo_path, &["branch", "--show-current"])?;
    if current_branch != session.base_branch {
        return Err(rk_core::Error::other(format!(
            "registered checkout must be on approved base branch {}; found {current_branch}",
            session.base_branch
        )));
    }
    require_clean(&session.repo_path)?;
    let before = git_text(&session.repo_path, &["rev-parse", "HEAD"])?;
    let already_landed = is_ancestor(&session.repo_path, &contract.approved_commit, &before);
    if !already_landed {
        if before != contract.expected_base_commit {
            return Err(rk_core::Error::other(format!(
                "registered base branch moved after approval: expected {}, current {before}",
                contract.expected_base_commit
            )));
        }
        git_ok(
            &session.repo_path,
            &["merge", "--ff-only", &contract.approved_commit],
        )?;
    }

    let registered_commit = git_text(&session.repo_path, &["rev-parse", "HEAD"])?;
    if !is_ancestor(
        &session.repo_path,
        &contract.approved_commit,
        &registered_commit,
    ) {
        return Err(rk_core::Error::other(format!(
            "activation did not land approved commit {}",
            contract.approved_commit
        )));
    }
    let target = session.repo_path.join(&proposal.target_path);
    let digest = file_digest(&target)?;
    if digest != contract.target_digest {
        return Err(rk_core::Error::other(format!(
            "registered automation content does not match approved digest: expected {}, current {digest}",
            contract.target_digest
        )));
    }
    validate_automation_file(
        &target,
        proposal
            .automation_kind()
            .expect("automation kind checked by contract"),
    )?;
    require_clean(&session.repo_path)?;
    Ok(ActivationEvidence {
        registered_commit,
        detail: if already_landed {
            "approved automation was already present; recovered activation without relanding".into()
        } else {
            format!(
                "fast-forwarded {} to content-bound commit {}",
                session.base_branch, contract.approved_commit
            )
        },
    })
}

/// Remove a terminal session's clean worktree while retaining its branch and
/// durable report. Keeping the branch makes cleanup recoverable and avoids
/// deleting the only Git copy of a verified-but-not-activated proposal.
pub fn ensure_cleanup(
    session: &OnboardingSession,
    actor: &str,
) -> rk_core::Result<OnboardingCleanup> {
    if !matches!(
        session.state,
        OnboardingSessionState::Completed | OnboardingSessionState::Failed
    ) {
        return Err(rk_core::Error::other(format!(
            "onboarding session {} is {:?}; long-lived or resumable sessions cannot be cleaned",
            session.id, session.state
        )));
    }
    let summary = proposal_summary(&session.proposals);
    if !summary.unresolved.is_empty() || !summary.staged.is_empty() {
        return Err(rk_core::Error::other(format!(
            "onboarding session {} still has staged or unresolved proposals",
            session.id
        )));
    }
    if session.worktree.exists() {
        require_clean(&session.worktree)?;
        git_ok(
            &session.repo_path,
            &["worktree", "remove", &session.worktree.to_string_lossy()],
        )?;
    } else {
        git_ok(&session.repo_path, &["worktree", "prune"])?;
    }
    Ok(OnboardingCleanup {
        actor: actor.to_string(),
        at: Utc::now(),
        worktree_removed: true,
        branch_retained: session.branch.clone(),
    })
}

fn require_clean(path: &Path) -> rk_core::Result<()> {
    let status = git_text(path, &["status", "--porcelain"])?;
    if status.is_empty() {
        Ok(())
    } else {
        Err(rk_core::Error::other(format!(
            "refusing onboarding operation with dirty checkout {}: {}",
            path.display(),
            status.lines().collect::<Vec<_>>().join(", ")
        )))
    }
}

fn is_ancestor(path: &Path, ancestor: &str, descendant: &str) -> bool {
    Command::new("git")
        .arg("-C")
        .arg(path)
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .env("LC_ALL", "C")
        .status()
        .is_ok_and(|status| status.success())
}

fn file_digest(path: &Path) -> rk_core::Result<String> {
    let bytes = std::fs::read(path)
        .map_err(|error| rk_core::Error::other(format!("read {}: {error}", path.display())))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn git_ok(path: &Path, args: &[&str]) -> rk_core::Result<()> {
    git_output(path, args).map(|_| ())
}

fn git_text(path: &Path, args: &[&str]) -> rk_core::Result<String> {
    let output = git_output(path, args)?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_output(path: &Path, args: &[&str]) -> rk_core::Result<Output> {
    let output = Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .env("LC_ALL", "C")
        .output()?;
    if output.status.success() {
        Ok(output)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let detail = if stderr.is_empty() {
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        } else {
            stderr
        };
        Err(rk_core::Error::other(format!(
            "git {} failed in {}: {detail}",
            args.join(" "),
            path.display()
        )))
    }
}
