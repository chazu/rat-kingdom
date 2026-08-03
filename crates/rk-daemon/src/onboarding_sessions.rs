//! Durable guided-onboarding session records.
//!
//! The supervisor remains the authority for harness/process state. This store
//! gives that process a stable repository-scoped identity and preserves the
//! assessment, branch, worktree, and linked agent across CLI disconnects and
//! daemon restarts.

use crate::onboarding::AssessmentReport;
use crate::onboarding_proposals::{
    repository_identity, OnboardingActivation, OnboardingActivationStatus,
    OnboardingActivationTransition, OnboardingApplication, OnboardingDecision, OnboardingProposal,
    OnboardingProposalDraft, OnboardingProposalStatus, OnboardingProposalTransition,
    OnboardingValidation, OnboardingVerification,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const ONBOARDER_ROLE: &str = "onboarder";
pub const SESSION_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingSessionState {
    Starting,
    Running,
    Completed,
    Failed,
    Orphaned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingSession {
    pub schema_version: u32,
    pub id: String,
    pub target: String,
    pub repo_name: String,
    pub repo_path: PathBuf,
    pub base_branch: String,
    pub branch: String,
    pub worktree: PathBuf,
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub attached: bool,
    pub state: OnboardingSessionState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attach_target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_result: Option<String>,
    pub assessment: AssessmentReport,
    #[serde(default)]
    pub proposals: Vec<OnboardingProposal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup: Option<OnboardingCleanup>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingSessionStatus {
    pub schema_version: u32,
    pub id: String,
    pub target: String,
    pub repo_name: String,
    pub repo_path: PathBuf,
    pub base_branch: String,
    pub branch: String,
    pub worktree: PathBuf,
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub attached: bool,
    pub state: OnboardingSessionState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attach_target: Option<String>,
    pub proposals: Vec<OnboardingProposal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup: Option<OnboardingCleanup>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingReport {
    pub schema_version: u32,
    pub session: OnboardingSessionStatus,
    pub assessment: AssessmentReport,
    pub proposals: Vec<OnboardingProposal>,
    pub summary: OnboardingProposalSummary,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup: Option<OnboardingCleanup>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_result: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingCleanup {
    pub actor: String,
    pub at: DateTime<Utc>,
    pub worktree_removed: bool,
    pub branch_retained: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingProposalSummary {
    pub staged: Vec<String>,
    pub verified: Vec<String>,
    pub activated: Vec<String>,
    pub declined: Vec<String>,
    pub failed: Vec<String>,
    pub unresolved: Vec<String>,
}

impl OnboardingSession {
    #[allow(clippy::too_many_arguments)]
    pub fn starting(
        target: String,
        repo_name: String,
        repo_path: PathBuf,
        base_branch: String,
        harness: String,
        model: Option<String>,
        attached: bool,
        assessment: AssessmentReport,
        worktrees_dir: &Path,
    ) -> Self {
        let id = session_id(&repo_path);
        let now = Utc::now();
        Self {
            schema_version: SESSION_SCHEMA_VERSION,
            branch: onboarding_branch(&id),
            worktree: onboarding_worktree(worktrees_dir, &repo_name, &id),
            id,
            target,
            repo_name,
            repo_path,
            base_branch,
            harness,
            model,
            attached,
            state: OnboardingSessionState::Starting,
            agent: None,
            attach_target: None,
            agent_result: None,
            assessment,
            proposals: Vec::new(),
            cleanup: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn status(&self) -> OnboardingSessionStatus {
        OnboardingSessionStatus {
            schema_version: self.schema_version,
            id: self.id.clone(),
            target: self.target.clone(),
            repo_name: self.repo_name.clone(),
            repo_path: self.repo_path.clone(),
            base_branch: self.base_branch.clone(),
            branch: self.branch.clone(),
            worktree: self.worktree.clone(),
            harness: self.harness.clone(),
            model: self.model.clone(),
            attached: self.attached,
            state: self.state,
            agent: self.agent.clone(),
            attach_target: self.attach_target.clone(),
            proposals: self.proposals.clone(),
            cleanup: self.cleanup.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }

    pub fn report(&self) -> OnboardingReport {
        OnboardingReport {
            schema_version: SESSION_SCHEMA_VERSION,
            session: self.status(),
            assessment: self.assessment.clone(),
            proposals: self.proposals.clone(),
            summary: proposal_summary(&self.proposals),
            cleanup: self.cleanup.clone(),
            agent_result: self.agent_result.clone(),
        }
    }
}

/// One stable session per canonical repository path. The path is already the
/// identity resolved by the read-only assessment, so aliases and the caller's
/// current directory cannot mint duplicate sessions.
pub fn session_id(repo_path: &Path) -> String {
    let mut digest = Sha256::new();
    digest.update(b"rat-kingdom-onboarding-session\0");
    digest.update(repo_path.as_os_str().as_encoded_bytes());
    let hex = hex::encode(digest.finalize());
    format!("onb-{}", &hex[..20])
}

pub fn onboarding_branch(id: &str) -> String {
    format!("onboarding/{id}")
}

pub fn onboarding_worktree(worktrees_dir: &Path, repo_name: &str, id: &str) -> PathBuf {
    worktrees_dir.join(repo_name).join("onboarding").join(id)
}

/// Synchronously persisted because each transition is a recovery boundary.
pub struct OnboardingSessions {
    path: PathBuf,
    sessions: BTreeMap<String, OnboardingSession>,
}

impl OnboardingSessions {
    pub fn load(path: &Path) -> rk_core::Result<Self> {
        let sessions = if path.exists() {
            serde_json::from_slice(&std::fs::read(path)?)?
        } else {
            BTreeMap::new()
        };
        Ok(Self {
            path: path.to_path_buf(),
            sessions,
        })
    }

    pub fn get(&self, id: &str) -> Option<OnboardingSession> {
        self.sessions.get(id).cloned()
    }

    /// Insert the durable starting journal unless this repository-derived id
    /// already exists. The bool distinguishes the one caller that owns launch
    /// from idempotent concurrent/repeated starts.
    pub fn insert_if_absent(
        &mut self,
        session: OnboardingSession,
    ) -> rk_core::Result<(OnboardingSession, bool)> {
        if let Some(existing) = self.sessions.get(&session.id) {
            return Ok((existing.clone(), false));
        }
        self.sessions.insert(session.id.clone(), session.clone());
        self.persist()?;
        Ok((session, true))
    }

    pub fn update(
        &mut self,
        id: &str,
        f: impl FnOnce(&mut OnboardingSession),
    ) -> rk_core::Result<Option<OnboardingSession>> {
        let Some(session) = self.sessions.get_mut(id) else {
            return Ok(None);
        };
        f(session);
        session.updated_at = Utc::now();
        let updated = session.clone();
        self.persist()?;
        Ok(Some(updated))
    }

    /// Journal immutable proposal content. The content digest derives the id,
    /// so submitting the same canonical proposal again is idempotent while an
    /// impossible prefix collision fails closed.
    pub fn propose(
        &mut self,
        id: &str,
        draft: OnboardingProposalDraft,
        proposer: String,
        tree_revision: String,
    ) -> rk_core::Result<(OnboardingProposal, bool)> {
        let session = self
            .sessions
            .get_mut(id)
            .ok_or_else(|| rk_core::Error::other(format!("no such onboarding session: {id}")))?;
        let proposal = OnboardingProposal::new(
            session.id.clone(),
            repository_identity(&session.repo_path),
            tree_revision,
            draft,
            proposer,
        )?;
        if let Some(existing) = session
            .proposals
            .iter()
            .find(|candidate| candidate.id == proposal.id)
        {
            existing.validate_integrity()?;
            if existing.digest != proposal.digest {
                return Err(rk_core::Error::other(format!(
                    "proposal id collision for {}",
                    proposal.id
                )));
            }
            return Ok((existing.clone(), false));
        }
        session.proposals.push(proposal.clone());
        session.updated_at = Utc::now();
        self.persist()?;
        Ok((proposal, true))
    }

    /// Record the one human decision for an exact proposal digest and tree.
    /// Same-decision retries return the original record unchanged; the opposite
    /// decision cannot overwrite the first durable choice.
    #[allow(clippy::too_many_arguments)]
    pub fn decide(
        &mut self,
        session_id: &str,
        proposal_id: &str,
        digest: &str,
        observed_tree_revision: &str,
        decision: OnboardingDecision,
        actor: String,
        reason: Option<String>,
    ) -> rk_core::Result<(OnboardingProposal, bool)> {
        let actor = required_actor(actor)?;
        let session = self.sessions.get_mut(session_id).ok_or_else(|| {
            rk_core::Error::other(format!("no such onboarding session: {session_id}"))
        })?;
        let proposal = session
            .proposals
            .iter_mut()
            .find(|proposal| proposal.id == proposal_id)
            .ok_or_else(|| {
                rk_core::Error::other(format!(
                    "no such onboarding proposal in {session_id}: {proposal_id}"
                ))
            })?;
        proposal.validate_integrity()?;
        if proposal.digest != digest {
            return Err(rk_core::Error::other(format!(
                "stale proposal digest for {proposal_id}: reviewed {digest}, current {}",
                proposal.digest
            )));
        }
        if proposal.tree_revision != observed_tree_revision {
            return Err(rk_core::Error::other(format!(
                "stale onboarding tree for {proposal_id}: proposed {}, current {observed_tree_revision}",
                proposal.tree_revision
            )));
        }
        let next = decision.status();
        if proposal.status == next
            || (decision == OnboardingDecision::Approve
                && matches!(
                    proposal.status,
                    OnboardingProposalStatus::Applied
                        | OnboardingProposalStatus::Verified
                        | OnboardingProposalStatus::Failed
                )
                && proposal.decision_actor.is_some())
        {
            return Ok((proposal.clone(), false));
        }
        if proposal.status != OnboardingProposalStatus::Proposed {
            return Err(rk_core::Error::other(format!(
                "proposal {proposal_id} is already {}; the first decision is final",
                proposal.status
            )));
        }

        let now = Utc::now();
        proposal.status = next;
        proposal.decision_actor = Some(actor.clone());
        proposal.decision_at = Some(now);
        proposal.decision_reason = reason.and_then(nonempty);
        proposal.transitions.push(OnboardingProposalTransition {
            from: Some(OnboardingProposalStatus::Proposed),
            to: next,
            actor,
            at: now,
            detail: proposal.decision_reason.clone(),
        });
        let updated = proposal.clone();
        session.updated_at = now;
        self.persist()?;
        Ok((updated, true))
    }

    /// CAS lifecycle seam for the application/verification slices. A retry of
    /// an already-recorded transition is idempotent, so an interrupted caller
    /// cannot double-apply merely by replaying its journal step.
    #[allow(clippy::too_many_arguments)]
    pub fn transition_proposal(
        &mut self,
        session_id: &str,
        proposal_id: &str,
        digest: &str,
        expected: OnboardingProposalStatus,
        next: OnboardingProposalStatus,
        actor: String,
        detail: Option<String>,
    ) -> rk_core::Result<(OnboardingProposal, bool)> {
        if !expected.allows(next) {
            return Err(rk_core::Error::other(format!(
                "invalid onboarding proposal transition: {expected} -> {next}"
            )));
        }
        let actor = required_actor(actor)?;
        let session = self.sessions.get_mut(session_id).ok_or_else(|| {
            rk_core::Error::other(format!("no such onboarding session: {session_id}"))
        })?;
        let proposal = session
            .proposals
            .iter_mut()
            .find(|proposal| proposal.id == proposal_id)
            .ok_or_else(|| {
                rk_core::Error::other(format!(
                    "no such onboarding proposal in {session_id}: {proposal_id}"
                ))
            })?;
        proposal.validate_integrity()?;
        if proposal.digest != digest {
            return Err(rk_core::Error::other(format!(
                "stale proposal digest for {proposal_id}"
            )));
        }
        if proposal
            .transitions
            .iter()
            .any(|transition| transition.from == Some(expected) && transition.to == next)
        {
            return Ok((proposal.clone(), false));
        }
        if proposal.status != expected {
            return Err(rk_core::Error::other(format!(
                "proposal {proposal_id} CAS expected {expected}, found {}",
                proposal.status
            )));
        }

        let now = Utc::now();
        proposal.status = next;
        let detail = detail.and_then(nonempty);
        if next == OnboardingProposalStatus::Failed {
            proposal.failure = detail.clone();
        }
        proposal.transitions.push(OnboardingProposalTransition {
            from: Some(expected),
            to: next,
            actor,
            at: now,
            detail,
        });
        let updated = proposal.clone();
        session.updated_at = now;
        self.persist()?;
        Ok((updated, true))
    }

    /// Persist the exact commit produced by applying an approved repo-file
    /// proposal. A matching replay is idempotent. A previously failed
    /// verification may return to `applied` only when the immutable application
    /// record still matches, allowing the named check to be retried without
    /// writing the patch or commit twice.
    pub fn record_application(
        &mut self,
        session_id: &str,
        proposal_id: &str,
        digest: &str,
        application: OnboardingApplication,
    ) -> rk_core::Result<(OnboardingProposal, bool)> {
        let session = self.sessions.get_mut(session_id).ok_or_else(|| {
            rk_core::Error::other(format!("no such onboarding session: {session_id}"))
        })?;
        let proposal = proposal_mut(session, proposal_id, digest)?;
        if let Some(existing) = &proposal.application {
            if existing != &application {
                return Err(rk_core::Error::other(format!(
                    "proposal {proposal_id} application evidence drifted"
                )));
            }
            if proposal.status != OnboardingProposalStatus::Failed {
                return Ok((proposal.clone(), false));
            }
        } else if !matches!(
            proposal.status,
            OnboardingProposalStatus::Approved | OnboardingProposalStatus::Failed
        ) {
            return Err(rk_core::Error::other(format!(
                "proposal {proposal_id} cannot record application from {}",
                proposal.status
            )));
        }

        let previous = proposal.status;
        let now = Utc::now();
        proposal.status = OnboardingProposalStatus::Applied;
        proposal.failure = None;
        proposal.application = Some(application.clone());
        proposal.transitions.push(OnboardingProposalTransition {
            from: Some(previous),
            to: OnboardingProposalStatus::Applied,
            actor: application.actor,
            at: now,
            detail: Some(format!("committed {}", application.commit)),
        });
        let updated = proposal.clone();
        session.updated_at = now;
        self.persist()?;
        Ok((updated, true))
    }

    /// Preserve an application/preflight failure before returning it to the
    /// caller. Failed proposals remain retryable; the immutable digest and any
    /// successful application evidence still constrain every retry.
    pub fn record_application_failure(
        &mut self,
        session_id: &str,
        proposal_id: &str,
        digest: &str,
        actor: String,
        detail: String,
    ) -> rk_core::Result<OnboardingProposal> {
        let actor = required_actor(actor)?;
        let session = self.sessions.get_mut(session_id).ok_or_else(|| {
            rk_core::Error::other(format!("no such onboarding session: {session_id}"))
        })?;
        let proposal = proposal_mut(session, proposal_id, digest)?;
        if !matches!(
            proposal.status,
            OnboardingProposalStatus::Approved
                | OnboardingProposalStatus::Applied
                | OnboardingProposalStatus::Failed
                | OnboardingProposalStatus::Verified
        ) {
            return Err(rk_core::Error::other(format!(
                "proposal {proposal_id} cannot fail application from {}",
                proposal.status
            )));
        }
        let previous = proposal.status;
        let now = Utc::now();
        proposal.status = OnboardingProposalStatus::Failed;
        proposal.failure = Some(detail.clone());
        proposal.transitions.push(OnboardingProposalTransition {
            from: Some(previous),
            to: OnboardingProposalStatus::Failed,
            actor,
            at: now,
            detail: Some(detail),
        });
        let updated = proposal.clone();
        session.updated_at = now;
        self.persist()?;
        Ok(updated)
    }

    /// Append one exact check execution result and advance the proposal to its
    /// terminal result. The result contains the command, toolchain,
    /// environment policy, exit/output summary, and unresolved risks shown in
    /// the final onboarding report.
    pub fn record_verification(
        &mut self,
        session_id: &str,
        proposal_id: &str,
        digest: &str,
        verification: OnboardingVerification,
    ) -> rk_core::Result<OnboardingProposal> {
        let session = self.sessions.get_mut(session_id).ok_or_else(|| {
            rk_core::Error::other(format!("no such onboarding session: {session_id}"))
        })?;
        let proposal = proposal_mut(session, proposal_id, digest)?;
        if proposal.status != OnboardingProposalStatus::Applied {
            return Err(rk_core::Error::other(format!(
                "proposal {proposal_id} cannot verify from {}",
                proposal.status
            )));
        }
        let next = if verification.passed {
            OnboardingProposalStatus::Verified
        } else {
            OnboardingProposalStatus::Failed
        };
        let now = Utc::now();
        proposal.status = next;
        proposal.failure = (!verification.passed).then(|| {
            format!(
                "check {} did not pass: {}",
                verification.check_name, verification.output_summary
            )
        });
        proposal.verification_results.push(verification.clone());
        proposal.transitions.push(OnboardingProposalTransition {
            from: Some(OnboardingProposalStatus::Applied),
            to: next,
            actor: verification.actor,
            at: now,
            detail: Some(verification.output_summary),
        });
        let updated = proposal.clone();
        session.updated_at = now;
        self.persist()?;
        Ok(updated)
    }

    /// Append schema validation evidence for a staged workflow, trigger, or
    /// schedule. Validation is deliberately separate from activation: this
    /// transition changes only the onboarding journal and branch.
    pub fn record_validation(
        &mut self,
        session_id: &str,
        proposal_id: &str,
        digest: &str,
        validation: OnboardingValidation,
    ) -> rk_core::Result<OnboardingProposal> {
        let session = self.sessions.get_mut(session_id).ok_or_else(|| {
            rk_core::Error::other(format!("no such onboarding session: {session_id}"))
        })?;
        let proposal = proposal_mut(session, proposal_id, digest)?;
        if proposal.status != OnboardingProposalStatus::Applied {
            return Err(rk_core::Error::other(format!(
                "proposal {proposal_id} cannot validate from {}",
                proposal.status
            )));
        }
        let next = if validation.passed {
            OnboardingProposalStatus::Verified
        } else {
            OnboardingProposalStatus::Failed
        };
        let now = Utc::now();
        proposal.status = next;
        proposal.failure = (!validation.passed).then(|| validation.output_summary.clone());
        proposal.validation_results.push(validation.clone());
        proposal.transitions.push(OnboardingProposalTransition {
            from: Some(OnboardingProposalStatus::Applied),
            to: next,
            actor: validation.actor,
            at: now,
            detail: Some(validation.output_summary),
        });
        let updated = proposal.clone();
        session.updated_at = now;
        self.persist()?;
        Ok(updated)
    }

    /// Persist activation intent before the registered checkout is advanced.
    /// An already-active or in-flight matching operation is replayed without
    /// minting another intent. A failed matching operation starts one new
    /// attempt while preserving its earlier transition evidence.
    #[allow(clippy::too_many_arguments)]
    pub fn begin_activation(
        &mut self,
        session_id: &str,
        proposal_id: &str,
        digest: &str,
        operation_id: String,
        actor: String,
        expected_base_commit: String,
        approved_commit: String,
        approved_tree_revision: String,
        target_digest: String,
    ) -> rk_core::Result<(OnboardingProposal, bool)> {
        let actor = required_actor(actor)?;
        let session = self.sessions.get_mut(session_id).ok_or_else(|| {
            rk_core::Error::other(format!("no such onboarding session: {session_id}"))
        })?;
        let proposal = proposal_mut(session, proposal_id, digest)?;
        if proposal.status != OnboardingProposalStatus::Verified {
            return Err(rk_core::Error::other(format!(
                "proposal {proposal_id} must be verified before activation; found {}",
                proposal.status
            )));
        }
        if proposal.automation_kind().is_none() {
            return Err(rk_core::Error::other(format!(
                "proposal {proposal_id} is not an automation activation proposal"
            )));
        }
        let matches_contract = |activation: &OnboardingActivation| {
            activation.operation_id == operation_id
                && activation.expected_base_commit == expected_base_commit
                && activation.approved_commit == approved_commit
                && activation.approved_tree_revision == approved_tree_revision
                && activation.target_digest == target_digest
        };
        if let Some(existing) = proposal.activation.as_ref() {
            if !matches_contract(existing) {
                return Err(rk_core::Error::other(format!(
                    "proposal {proposal_id} activation contract drifted"
                )));
            }
            match existing.status {
                OnboardingActivationStatus::Activated | OnboardingActivationStatus::Activating => {
                    return Ok((proposal.clone(), false));
                }
                OnboardingActivationStatus::Declined => {
                    return Err(rk_core::Error::other(format!(
                        "proposal {proposal_id} activation was declined"
                    )));
                }
                OnboardingActivationStatus::Failed => {}
            }
        }

        let now = Utc::now();
        match proposal.activation.as_mut() {
            Some(activation) => {
                let previous = activation.status;
                activation.status = OnboardingActivationStatus::Activating;
                activation.actor = actor.clone();
                activation.requested_at = now;
                activation.completed_at = None;
                activation.registered_commit = None;
                activation.detail = None;
                activation.attempts += 1;
                activation.transitions.push(OnboardingActivationTransition {
                    from: Some(previous),
                    to: OnboardingActivationStatus::Activating,
                    actor,
                    at: now,
                    detail: Some("retrying exact activation contract".into()),
                });
            }
            None => {
                proposal.activation = Some(OnboardingActivation {
                    operation_id,
                    status: OnboardingActivationStatus::Activating,
                    actor: actor.clone(),
                    requested_at: now,
                    expected_base_commit,
                    approved_commit,
                    approved_tree_revision,
                    target_digest,
                    attempts: 1,
                    completed_at: None,
                    registered_commit: None,
                    detail: None,
                    transitions: vec![OnboardingActivationTransition {
                        from: None,
                        to: OnboardingActivationStatus::Activating,
                        actor,
                        at: now,
                        detail: Some("activation intent persisted before landing".into()),
                    }],
                });
            }
        }
        let updated = proposal.clone();
        session.updated_at = now;
        self.persist()?;
        Ok((updated, true))
    }

    pub fn finish_activation(
        &mut self,
        session_id: &str,
        proposal_id: &str,
        digest: &str,
        operation_id: &str,
        registered_commit: String,
        detail: String,
    ) -> rk_core::Result<(OnboardingProposal, bool)> {
        let session = self.sessions.get_mut(session_id).ok_or_else(|| {
            rk_core::Error::other(format!("no such onboarding session: {session_id}"))
        })?;
        let proposal = proposal_mut(session, proposal_id, digest)?;
        let activation = proposal.activation.as_mut().ok_or_else(|| {
            rk_core::Error::other(format!("proposal {proposal_id} has no activation intent"))
        })?;
        if activation.operation_id != operation_id {
            return Err(rk_core::Error::other(format!(
                "proposal {proposal_id} activation operation drifted"
            )));
        }
        if activation.status == OnboardingActivationStatus::Activated {
            return Ok((proposal.clone(), false));
        }
        if activation.status != OnboardingActivationStatus::Activating {
            return Err(rk_core::Error::other(format!(
                "proposal {proposal_id} cannot finish activation from {}",
                activation.status
            )));
        }
        let now = Utc::now();
        activation.status = OnboardingActivationStatus::Activated;
        activation.completed_at = Some(now);
        activation.registered_commit = Some(registered_commit);
        activation.detail = nonempty(detail);
        activation.transitions.push(OnboardingActivationTransition {
            from: Some(OnboardingActivationStatus::Activating),
            to: OnboardingActivationStatus::Activated,
            actor: activation.actor.clone(),
            at: now,
            detail: activation.detail.clone(),
        });
        let updated = proposal.clone();
        session.updated_at = now;
        self.persist()?;
        Ok((updated, true))
    }

    pub fn fail_activation(
        &mut self,
        session_id: &str,
        proposal_id: &str,
        digest: &str,
        operation_id: &str,
        detail: String,
    ) -> rk_core::Result<OnboardingProposal> {
        let session = self.sessions.get_mut(session_id).ok_or_else(|| {
            rk_core::Error::other(format!("no such onboarding session: {session_id}"))
        })?;
        let proposal = proposal_mut(session, proposal_id, digest)?;
        let activation = proposal.activation.as_mut().ok_or_else(|| {
            rk_core::Error::other(format!("proposal {proposal_id} has no activation intent"))
        })?;
        if activation.operation_id != operation_id {
            return Err(rk_core::Error::other(format!(
                "proposal {proposal_id} activation operation drifted"
            )));
        }
        if activation.status == OnboardingActivationStatus::Activated {
            return Ok(proposal.clone());
        }
        let previous = activation.status;
        let now = Utc::now();
        activation.status = OnboardingActivationStatus::Failed;
        activation.completed_at = Some(now);
        activation.detail = Some(detail.clone());
        activation.transitions.push(OnboardingActivationTransition {
            from: Some(previous),
            to: OnboardingActivationStatus::Failed,
            actor: activation.actor.clone(),
            at: now,
            detail: Some(detail),
        });
        let updated = proposal.clone();
        session.updated_at = now;
        self.persist()?;
        Ok(updated)
    }

    /// Refuse activation after staging/validation without discarding the
    /// reviewed branch. The first activation decision wins.
    pub fn decline_activation(
        &mut self,
        session_id: &str,
        proposal_id: &str,
        digest: &str,
        actor: String,
        reason: Option<String>,
    ) -> rk_core::Result<(OnboardingProposal, bool)> {
        let actor = required_actor(actor)?;
        let session = self.sessions.get_mut(session_id).ok_or_else(|| {
            rk_core::Error::other(format!("no such onboarding session: {session_id}"))
        })?;
        let proposal = proposal_mut(session, proposal_id, digest)?;
        if proposal.status != OnboardingProposalStatus::Verified
            || proposal.automation_kind().is_none()
        {
            return Err(rk_core::Error::other(format!(
                "proposal {proposal_id} must be verified automation before activation can be declined"
            )));
        }
        if let Some(existing) = proposal.activation.as_ref() {
            match existing.status {
                OnboardingActivationStatus::Declined => {
                    return Ok((proposal.clone(), false));
                }
                OnboardingActivationStatus::Activated | OnboardingActivationStatus::Activating => {
                    return Err(rk_core::Error::other(format!(
                        "proposal {proposal_id} activation is already {}",
                        existing.status
                    )));
                }
                OnboardingActivationStatus::Failed => {}
            }
        }
        let now = Utc::now();
        let detail = reason.and_then(nonempty);
        let transition = OnboardingActivationTransition {
            from: proposal
                .activation
                .as_ref()
                .map(|activation| activation.status),
            to: OnboardingActivationStatus::Declined,
            actor: actor.clone(),
            at: now,
            detail: detail.clone(),
        };
        match proposal.activation.as_mut() {
            Some(activation) => {
                activation.status = OnboardingActivationStatus::Declined;
                activation.actor = actor;
                activation.completed_at = Some(now);
                activation.detail = detail;
                activation.transitions.push(transition);
            }
            None => {
                let application = proposal.application.as_ref().ok_or_else(|| {
                    rk_core::Error::other(format!(
                        "proposal {proposal_id} has no staged application"
                    ))
                })?;
                proposal.activation = Some(OnboardingActivation {
                    operation_id: activation_operation_id(
                        session_id,
                        proposal_id,
                        digest,
                        &application.commit,
                    ),
                    status: OnboardingActivationStatus::Declined,
                    actor,
                    requested_at: now,
                    expected_base_commit: String::new(),
                    approved_commit: application.commit.clone(),
                    approved_tree_revision: application.tree_revision.clone(),
                    target_digest: application.target_digest.clone(),
                    attempts: 0,
                    completed_at: Some(now),
                    registered_commit: None,
                    detail,
                    transitions: vec![transition],
                });
            }
        }
        let updated = proposal.clone();
        session.updated_at = now;
        self.persist()?;
        Ok((updated, true))
    }

    pub fn record_cleanup(
        &mut self,
        session_id: &str,
        cleanup: OnboardingCleanup,
    ) -> rk_core::Result<(OnboardingSession, bool)> {
        let session = self.sessions.get_mut(session_id).ok_or_else(|| {
            rk_core::Error::other(format!("no such onboarding session: {session_id}"))
        })?;
        if session.cleanup.is_some() {
            return Ok((session.clone(), false));
        }
        session.cleanup = Some(cleanup);
        session.updated_at = Utc::now();
        let updated = session.clone();
        self.persist()?;
        Ok((updated, true))
    }

    /// A daemon cannot know whether an in-flight launch crossed its last
    /// persistence boundary. Mark every nonterminal session orphaned on boot;
    /// reconciliation against the durable agent registry may immediately
    /// refine it to completed/failed, and `resume` is safe and idempotent.
    pub fn orphan_nonterminal(&mut self) -> rk_core::Result<()> {
        let now = Utc::now();
        let mut changed = false;
        for session in self.sessions.values_mut() {
            if matches!(
                session.state,
                OnboardingSessionState::Starting | OnboardingSessionState::Running
            ) {
                session.state = OnboardingSessionState::Orphaned;
                session.updated_at = now;
                changed = true;
            }
        }
        if changed {
            self.persist()?;
        }
        Ok(())
    }

    fn persist(&self) -> rk_core::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(&self.sessions)?)?;
        std::fs::rename(tmp, &self.path)?;
        Ok(())
    }
}

fn required_actor(actor: String) -> rk_core::Result<String> {
    nonempty(actor)
        .ok_or_else(|| rk_core::Error::other("proposal transition actor must not be empty"))
}

fn proposal_mut<'a>(
    session: &'a mut OnboardingSession,
    proposal_id: &str,
    digest: &str,
) -> rk_core::Result<&'a mut OnboardingProposal> {
    let proposal = session
        .proposals
        .iter_mut()
        .find(|proposal| proposal.id == proposal_id)
        .ok_or_else(|| {
            rk_core::Error::other(format!(
                "no such onboarding proposal in {}: {proposal_id}",
                session.id
            ))
        })?;
    proposal.validate_integrity()?;
    if proposal.digest != digest {
        return Err(rk_core::Error::other(format!(
            "stale proposal digest for {proposal_id}"
        )));
    }
    Ok(proposal)
}

pub fn activation_operation_id(
    session_id: &str,
    proposal_id: &str,
    digest: &str,
    approved_commit: &str,
) -> String {
    let mut hash = Sha256::new();
    hash.update(b"rat-kingdom-onboarding-activation-v1\0");
    for field in [session_id, proposal_id, digest, approved_commit] {
        hash.update((field.len() as u64).to_be_bytes());
        hash.update(field.as_bytes());
    }
    format!("onb-act-{}", &hex::encode(hash.finalize())[..24])
}

pub fn proposal_summary(proposals: &[OnboardingProposal]) -> OnboardingProposalSummary {
    let mut summary = OnboardingProposalSummary::default();
    for proposal in proposals {
        let destination = match proposal
            .activation
            .as_ref()
            .map(|activation| activation.status)
        {
            Some(OnboardingActivationStatus::Activated) => &mut summary.activated,
            Some(OnboardingActivationStatus::Declined) => &mut summary.declined,
            Some(OnboardingActivationStatus::Failed) => &mut summary.failed,
            Some(OnboardingActivationStatus::Activating) => &mut summary.unresolved,
            None => match proposal.status {
                OnboardingProposalStatus::Applied => &mut summary.staged,
                OnboardingProposalStatus::Verified => &mut summary.verified,
                OnboardingProposalStatus::Declined => &mut summary.declined,
                OnboardingProposalStatus::Failed => &mut summary.failed,
                OnboardingProposalStatus::Proposed | OnboardingProposalStatus::Approved => {
                    &mut summary.unresolved
                }
            },
        };
        destination.push(proposal.id.clone());
    }
    summary
}

fn nonempty(value: String) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::onboarding::RepositoryIdentity;
    use crate::onboarding_proposals::{
        OnboardingNamedCheck, OnboardingProposalAction, OnboardingProposalKind,
        OnboardingProposalRisk,
    };
    use rk_workflow::CheckEnvironmentPolicy;

    fn assessment(path: &Path) -> AssessmentReport {
        AssessmentReport {
            schema_version: 1,
            identity: RepositoryIdentity {
                target: path.display().to_string(),
                canonical_path: Some(path.display().to_string()),
                registered_name: Some("repo".into()),
                registered_aliases: vec!["repo".into()],
            },
            ready: true,
            findings: Vec::new(),
        }
    }

    fn proposal_draft(title: &str) -> OnboardingProposalDraft {
        OnboardingProposalDraft {
            kind: OnboardingProposalKind::RepoFile,
            title: title.into(),
            evidence: vec!["README documents `mise run verify`".into()],
            target_path: ".rk/checks.cue".into(),
            action: OnboardingProposalAction::WriteRepoFile,
            diff: "--- /dev/null\n+++ b/.rk/checks.cue\n+verify: {}\n".into(),
            risk: OnboardingProposalRisk::Low,
            verification: vec!["mise run verify".into()],
            named_check: Some(OnboardingNamedCheck {
                name: "verify".into(),
                command: "mise run verify".into(),
                cwd: ".".into(),
                expect_exit: 0,
                timeout: "20m".into(),
                environment_policy: CheckEnvironmentPolicy::StripRkSpawn,
                toolchain: "mise rust@1.95.0".into(),
            }),
        }
    }

    #[test]
    fn repository_identity_stabilizes_session_branch_and_worktree() {
        let root = PathBuf::from("/tmp/example");
        let worktrees = PathBuf::from("/tmp/rk-worktrees");
        let first = OnboardingSession::starting(
            root.display().to_string(),
            "repo".into(),
            root.clone(),
            "main".into(),
            "codex".into(),
            None,
            false,
            assessment(&root),
            &worktrees,
        );
        let second = OnboardingSession::starting(
            "repo".into(),
            "repo".into(),
            root,
            "main".into(),
            "codex".into(),
            None,
            true,
            first.assessment.clone(),
            &worktrees,
        );
        assert_eq!(first.id, second.id);
        assert_eq!(first.branch, second.branch);
        assert_eq!(first.worktree, second.worktree);
        assert!(first.branch.starts_with("onboarding/onb-"));
    }

    #[test]
    fn store_reuses_and_recovers_a_starting_session() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("repo");
        let path = dir.path().join("sessions.json");
        let session = OnboardingSession::starting(
            root.display().to_string(),
            "repo".into(),
            root.clone(),
            "main".into(),
            "codex".into(),
            None,
            false,
            assessment(&root),
            &dir.path().join("worktrees"),
        );
        let id = session.id.clone();
        {
            let mut store = OnboardingSessions::load(&path).unwrap();
            assert!(store.insert_if_absent(session.clone()).unwrap().1);
            assert!(!store.insert_if_absent(session).unwrap().1);
        }
        let mut store = OnboardingSessions::load(&path).unwrap();
        store.orphan_nonterminal().unwrap();
        assert_eq!(
            store.get(&id).unwrap().state,
            OnboardingSessionState::Orphaned
        );
    }

    #[test]
    fn proposal_decisions_and_application_transitions_are_durable_cas() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("repo");
        let path = dir.path().join("sessions.json");
        let session = OnboardingSession::starting(
            root.display().to_string(),
            "repo".into(),
            root,
            "main".into(),
            "codex".into(),
            None,
            false,
            assessment(dir.path()),
            &dir.path().join("worktrees"),
        );
        let session_id = session.id.clone();
        let mut store = OnboardingSessions::load(&path).unwrap();
        store.insert_if_absent(session).unwrap();
        let (proposal, created) = store
            .propose(
                &session_id,
                proposal_draft("Add checks"),
                "onboarder".into(),
                "tree-a".into(),
            )
            .unwrap();
        assert!(created);
        assert!(
            !store
                .propose(
                    &session_id,
                    proposal_draft("Add checks"),
                    "onboarder".into(),
                    "tree-a".into(),
                )
                .unwrap()
                .1
        );

        let (approved, changed) = store
            .decide(
                &session_id,
                &proposal.id,
                &proposal.digest,
                "tree-a",
                OnboardingDecision::Approve,
                "operator@castle".into(),
                Some("reviewed".into()),
            )
            .unwrap();
        assert!(changed);
        let decision_at = approved.decision_at;
        let duplicate = store
            .decide(
                &session_id,
                &proposal.id,
                &proposal.digest,
                "tree-a",
                OnboardingDecision::Approve,
                "operator@castle".into(),
                None,
            )
            .unwrap();
        assert!(!duplicate.1);
        assert_eq!(duplicate.0.decision_at, decision_at);
        assert!(store
            .decide(
                &session_id,
                &proposal.id,
                &proposal.digest,
                "tree-a",
                OnboardingDecision::Decline,
                "operator@castle".into(),
                None,
            )
            .is_err());

        let (applied, applied_now) = store
            .transition_proposal(
                &session_id,
                &proposal.id,
                &proposal.digest,
                OnboardingProposalStatus::Approved,
                OnboardingProposalStatus::Applied,
                "daemon".into(),
                Some("write journaled".into()),
            )
            .unwrap();
        assert!(applied_now);
        assert_eq!(applied.status, OnboardingProposalStatus::Applied);
        let duplicate_apply = store
            .transition_proposal(
                &session_id,
                &proposal.id,
                &proposal.digest,
                OnboardingProposalStatus::Approved,
                OnboardingProposalStatus::Applied,
                "daemon".into(),
                None,
            )
            .unwrap();
        assert!(!duplicate_apply.1, "replay must not double-apply");
        store
            .transition_proposal(
                &session_id,
                &proposal.id,
                &proposal.digest,
                OnboardingProposalStatus::Applied,
                OnboardingProposalStatus::Verified,
                "daemon".into(),
                Some("named check passed".into()),
            )
            .unwrap();

        drop(store);
        let reloaded = OnboardingSessions::load(&path).unwrap();
        let proposal = &reloaded.get(&session_id).unwrap().proposals[0];
        assert_eq!(proposal.status, OnboardingProposalStatus::Verified);
        assert_eq!(proposal.transitions.len(), 4);
        assert_eq!(proposal.decision_actor.as_deref(), Some("operator@castle"));
    }

    #[test]
    fn decision_rejects_stale_tree_digest_and_edited_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sessions.json");
        let session = OnboardingSession::starting(
            dir.path().display().to_string(),
            "repo".into(),
            dir.path().to_path_buf(),
            "main".into(),
            "codex".into(),
            None,
            false,
            assessment(dir.path()),
            &dir.path().join("worktrees"),
        );
        let session_id = session.id.clone();
        let mut store = OnboardingSessions::load(&path).unwrap();
        store.insert_if_absent(session).unwrap();
        let proposal = store
            .propose(
                &session_id,
                proposal_draft("Add checks"),
                "onboarder".into(),
                "tree-a".into(),
            )
            .unwrap()
            .0;
        assert!(store
            .decide(
                &session_id,
                &proposal.id,
                "not-the-reviewed-digest",
                "tree-a",
                OnboardingDecision::Approve,
                "operator@castle".into(),
                None,
            )
            .is_err());
        assert!(store
            .decide(
                &session_id,
                &proposal.id,
                &proposal.digest,
                "tree-b",
                OnboardingDecision::Approve,
                "operator@castle".into(),
                None,
            )
            .is_err());
        store.sessions.get_mut(&session_id).unwrap().proposals[0]
            .diff
            .push_str("+edited: true\n");
        assert!(store
            .decide(
                &session_id,
                &proposal.id,
                &proposal.digest,
                "tree-a",
                OnboardingDecision::Approve,
                "operator@castle".into(),
                None,
            )
            .is_err());
    }
}
