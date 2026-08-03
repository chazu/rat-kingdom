//! Content-bound proposals and human decisions for guided onboarding.
//!
//! Proposal content is immutable once journaled. The digest covers the stable
//! repository identity, onboarding tree, target, action, evidence, exact diff,
//! risk, and verification plan. Mutable lifecycle fields live outside that
//! digest and advance through compare-and-swap transitions in
//! [`crate::onboarding_sessions::OnboardingSessions`].

use chrono::{DateTime, Utc};
use rk_workflow::CheckEnvironmentPolicy;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Component, Path};
use std::process::Command;

pub const PROPOSAL_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingProposalKind {
    RepoFile,
    CastleConfig,
    Registration,
    WorkflowActivation,
    TriggerActivation,
    ScheduleActivation,
}

impl OnboardingProposalKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::RepoFile => "repo_file",
            Self::CastleConfig => "castle_config",
            Self::Registration => "registration",
            Self::WorkflowActivation => "workflow_activation",
            Self::TriggerActivation => "trigger_activation",
            Self::ScheduleActivation => "schedule_activation",
        }
    }
}

impl std::fmt::Display for OnboardingProposalKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingProposalAction {
    WriteRepoFile,
    ChangeCastleConfig,
    RegisterRepository,
    ActivateWorkflow,
    ActivateTrigger,
    ActivateSchedule,
}

impl OnboardingProposalAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::WriteRepoFile => "write_repo_file",
            Self::ChangeCastleConfig => "change_castle_config",
            Self::RegisterRepository => "register_repository",
            Self::ActivateWorkflow => "activate_workflow",
            Self::ActivateTrigger => "activate_trigger",
            Self::ActivateSchedule => "activate_schedule",
        }
    }
}

impl std::fmt::Display for OnboardingProposalAction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingProposalRisk {
    Low,
    Medium,
    High,
}

impl OnboardingProposalRisk {
    fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

impl std::fmt::Display for OnboardingProposalRisk {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingProposalStatus {
    Proposed,
    Approved,
    Declined,
    Applied,
    Verified,
    Failed,
}

impl OnboardingProposalStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Proposed => "proposed",
            Self::Approved => "approved",
            Self::Declined => "declined",
            Self::Applied => "applied",
            Self::Verified => "verified",
            Self::Failed => "failed",
        }
    }

    pub fn allows(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Proposed,
                Self::Approved | Self::Declined | Self::Failed
            ) | (Self::Approved, Self::Applied | Self::Failed)
                | (Self::Applied, Self::Verified | Self::Failed)
                | (Self::Failed, Self::Applied)
        )
    }
}

impl std::fmt::Display for OnboardingProposalStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnboardingDecision {
    Approve,
    Decline,
}

impl OnboardingDecision {
    pub fn status(self) -> OnboardingProposalStatus {
        match self {
            Self::Approve => OnboardingProposalStatus::Approved,
            Self::Decline => OnboardingProposalStatus::Declined,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OnboardingProposalDraft {
    pub kind: OnboardingProposalKind,
    pub title: String,
    pub evidence: Vec<String>,
    pub target_path: String,
    pub action: OnboardingProposalAction,
    pub diff: String,
    pub risk: OnboardingProposalRisk,
    pub verification: Vec<String>,
    /// Exact check metadata a human approves for `.rk/checks.cue`. The unified
    /// diff is still the file-level change; this contract makes the executable
    /// command and its environment independently visible and digest-bound.
    #[serde(default)]
    pub named_check: Option<OnboardingNamedCheck>,
}

impl OnboardingProposalDraft {
    fn canonicalized(mut self) -> rk_core::Result<Self> {
        self.title = required_line("title", &self.title)?;
        self.target_path = required_line("target_path", &self.target_path)?;
        self.diff = normalize_newlines(&self.diff);
        if self.diff.trim().is_empty() {
            return Err(rk_core::Error::other("proposal diff must not be empty"));
        }
        self.evidence = canonical_lines("evidence", self.evidence)?;
        self.verification = canonical_lines("verification", self.verification)?;
        validate_kind_action(self.kind, self.action)?;
        if matches!(
            self.kind,
            OnboardingProposalKind::RepoFile
                | OnboardingProposalKind::WorkflowActivation
                | OnboardingProposalKind::TriggerActivation
                | OnboardingProposalKind::ScheduleActivation
        ) {
            self.target_path = canonical_repo_target(&self.target_path)?;
        }
        match (
            self.kind,
            self.action,
            self.target_path.as_str(),
            self.named_check.take(),
        ) {
            (
                OnboardingProposalKind::RepoFile,
                OnboardingProposalAction::WriteRepoFile,
                ".rk/checks.cue",
                Some(check),
            ) => self.named_check = Some(check.canonicalized()?),
            (
                OnboardingProposalKind::RepoFile,
                OnboardingProposalAction::WriteRepoFile,
                ".rk/checks.cue",
                None,
            ) => {
                return Err(rk_core::Error::other(
                    "a .rk/checks.cue proposal must bind an exact named_check contract",
                ));
            }
            (_, _, _, Some(_)) => {
                return Err(rk_core::Error::other(
                    "named_check is only valid for a .rk/checks.cue repo-file proposal",
                ));
            }
            _ => {}
        }
        validate_automation_target(self.kind, self.action, &self.target_path)?;
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OnboardingNamedCheck {
    pub name: String,
    pub command: String,
    pub cwd: String,
    pub expect_exit: i64,
    pub timeout: String,
    pub environment_policy: CheckEnvironmentPolicy,
    pub toolchain: String,
}

impl OnboardingNamedCheck {
    fn canonicalized(mut self) -> rk_core::Result<Self> {
        self.name = required_line("named_check.name", &self.name)?;
        self.command = required_line("named_check.command", &self.command)?;
        self.cwd = canonical_repo_cwd(&self.cwd)?;
        self.timeout = required_line("named_check.timeout", &self.timeout)?;
        validate_duration(&self.timeout)?;
        self.toolchain = required_line("named_check.toolchain", &self.toolchain)?;
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingApplication {
    pub actor: String,
    pub at: DateTime<Utc>,
    pub commit: String,
    pub tree_revision: String,
    pub target_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingVerification {
    pub attempt: u32,
    pub actor: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub check_name: String,
    pub command: String,
    pub cwd: String,
    pub expected_exit: i64,
    pub timeout: String,
    pub environment_policy: CheckEnvironmentPolicy,
    pub toolchain: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_status: Option<i64>,
    pub timed_out: bool,
    pub passed: bool,
    pub output_summary: String,
    #[serde(default)]
    pub unresolved_risks: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingAutomationKind {
    RepositoryPolicy,
    Workflow,
    Trigger,
    Schedule,
}

impl std::fmt::Display for OnboardingAutomationKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::RepositoryPolicy => "repository_policy",
            Self::Workflow => "workflow",
            Self::Trigger => "trigger",
            Self::Schedule => "schedule",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingValidation {
    pub attempt: u32,
    pub actor: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub automation_kind: OnboardingAutomationKind,
    pub target_path: String,
    pub target_digest: String,
    pub validator: String,
    pub passed: bool,
    pub output_summary: String,
    #[serde(default)]
    pub unresolved_risks: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingActivationStatus {
    Activating,
    Activated,
    Declined,
    Failed,
}

impl std::fmt::Display for OnboardingActivationStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Activating => "activating",
            Self::Activated => "activated",
            Self::Declined => "declined",
            Self::Failed => "failed",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingActivationTransition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<OnboardingActivationStatus>,
    pub to: OnboardingActivationStatus,
    pub actor: String,
    pub at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingActivation {
    pub operation_id: String,
    pub status: OnboardingActivationStatus,
    pub actor: String,
    pub requested_at: DateTime<Utc>,
    pub expected_base_commit: String,
    pub approved_commit: String,
    pub approved_tree_revision: String,
    pub target_digest: String,
    pub attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registered_commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(default)]
    pub transitions: Vec<OnboardingActivationTransition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingProposalTransition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<OnboardingProposalStatus>,
    pub to: OnboardingProposalStatus,
    pub actor: String,
    pub at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingProposal {
    pub schema_version: u32,
    pub id: String,
    pub session_id: String,
    pub repository_identity: String,
    pub tree_revision: String,
    pub kind: OnboardingProposalKind,
    pub title: String,
    pub evidence: Vec<String>,
    pub target_path: String,
    pub action: OnboardingProposalAction,
    pub diff: String,
    pub risk: OnboardingProposalRisk,
    pub verification: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub named_check: Option<OnboardingNamedCheck>,
    pub digest: String,
    pub status: OnboardingProposalStatus,
    pub proposer: String,
    pub proposed_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_actor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application: Option<OnboardingApplication>,
    #[serde(default)]
    pub verification_results: Vec<OnboardingVerification>,
    #[serde(default)]
    pub validation_results: Vec<OnboardingValidation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation: Option<OnboardingActivation>,
    pub transitions: Vec<OnboardingProposalTransition>,
}

impl OnboardingProposal {
    pub fn new(
        session_id: String,
        repository_identity: String,
        tree_revision: String,
        draft: OnboardingProposalDraft,
        proposer: String,
    ) -> rk_core::Result<Self> {
        let draft = draft.canonicalized()?;
        let tree_revision = required_line("tree_revision", &tree_revision)?;
        let repository_identity = required_line("repository_identity", &repository_identity)?;
        let proposer = required_line("proposer", &proposer)?;
        let digest = proposal_digest(&session_id, &repository_identity, &tree_revision, &draft);
        let id = proposal_id(&digest);
        let now = Utc::now();
        Ok(Self {
            schema_version: PROPOSAL_SCHEMA_VERSION,
            id,
            session_id,
            repository_identity,
            tree_revision,
            kind: draft.kind,
            title: draft.title,
            evidence: draft.evidence,
            target_path: draft.target_path,
            action: draft.action,
            diff: draft.diff,
            risk: draft.risk,
            verification: draft.verification,
            named_check: draft.named_check,
            digest,
            status: OnboardingProposalStatus::Proposed,
            proposer: proposer.clone(),
            proposed_at: now,
            decision_actor: None,
            decision_at: None,
            decision_reason: None,
            failure: None,
            application: None,
            verification_results: Vec::new(),
            validation_results: Vec::new(),
            activation: None,
            transitions: vec![OnboardingProposalTransition {
                from: None,
                to: OnboardingProposalStatus::Proposed,
                actor: proposer,
                at: now,
                detail: None,
            }],
        })
    }

    pub fn draft(&self) -> OnboardingProposalDraft {
        OnboardingProposalDraft {
            kind: self.kind,
            title: self.title.clone(),
            evidence: self.evidence.clone(),
            target_path: self.target_path.clone(),
            action: self.action,
            diff: self.diff.clone(),
            risk: self.risk,
            verification: self.verification.clone(),
            named_check: self.named_check.clone(),
        }
    }

    /// Detect hand-edited or partially persisted immutable proposal content
    /// before a human decision is accepted.
    pub fn validate_integrity(&self) -> rk_core::Result<()> {
        let draft = self.draft().canonicalized()?;
        let expected = proposal_digest(
            &self.session_id,
            &self.repository_identity,
            &self.tree_revision,
            &draft,
        );
        if expected != self.digest || proposal_id(&expected) != self.id {
            return Err(rk_core::Error::other(format!(
                "proposal {} content no longer matches its canonical digest",
                self.id
            )));
        }
        Ok(())
    }

    pub fn automation_kind(&self) -> Option<OnboardingAutomationKind> {
        match (self.kind, self.target_path.as_str()) {
            (OnboardingProposalKind::RepoFile, ".rk/repo.cue") => {
                Some(OnboardingAutomationKind::RepositoryPolicy)
            }
            (OnboardingProposalKind::WorkflowActivation, _) => {
                Some(OnboardingAutomationKind::Workflow)
            }
            (OnboardingProposalKind::TriggerActivation, _) => {
                Some(OnboardingAutomationKind::Trigger)
            }
            (OnboardingProposalKind::ScheduleActivation, _) => {
                Some(OnboardingAutomationKind::Schedule)
            }
            (
                OnboardingProposalKind::RepoFile
                | OnboardingProposalKind::CastleConfig
                | OnboardingProposalKind::Registration,
                _,
            ) => None,
        }
    }
}

pub fn repository_identity(repo_path: &Path) -> String {
    let mut digest = Sha256::new();
    digest.update(b"rat-kingdom-onboarding-repository\0");
    digest.update(repo_path.as_os_str().as_encoded_bytes());
    format!("repo-{}", &hex::encode(digest.finalize())[..24])
}

/// The exact committed tree a human reviews. Approval re-reads this value so
/// branch movement after proposal creation makes the decision stale.
pub fn onboarding_tree_revision(worktree: &Path) -> rk_core::Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(["rev-parse", "HEAD^{tree}"])
        .env("LC_ALL", "C")
        .output()?;
    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(rk_core::Error::other(format!(
            "cannot resolve onboarding tree revision at {}: {detail}",
            worktree.display()
        )));
    }
    required_line(
        "tree_revision",
        String::from_utf8_lossy(&output.stdout).as_ref(),
    )
}

pub fn proposal_id(digest: &str) -> String {
    format!("onb-prop-{}", &digest[..digest.len().min(20)])
}

fn proposal_digest(
    session_id: &str,
    repository_identity: &str,
    tree_revision: &str,
    draft: &OnboardingProposalDraft,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"rat-kingdom-onboarding-proposal-v1\0");
    hash_field(&mut digest, session_id);
    hash_field(&mut digest, repository_identity);
    hash_field(&mut digest, tree_revision);
    hash_field(&mut digest, draft.kind.as_str());
    hash_field(&mut digest, &draft.title);
    hash_list(&mut digest, &draft.evidence);
    hash_field(&mut digest, &draft.target_path);
    hash_field(&mut digest, draft.action.as_str());
    hash_field(&mut digest, &draft.diff);
    hash_field(&mut digest, draft.risk.as_str());
    hash_list(&mut digest, &draft.verification);
    if let Some(check) = &draft.named_check {
        hash_field(&mut digest, "named_check");
        hash_field(&mut digest, &check.name);
        hash_field(&mut digest, &check.command);
        hash_field(&mut digest, &check.cwd);
        hash_field(&mut digest, &check.expect_exit.to_string());
        hash_field(&mut digest, &check.timeout);
        hash_field(&mut digest, &check.environment_policy.to_string());
        hash_field(&mut digest, &check.toolchain);
    }
    hex::encode(digest.finalize())
}

fn hash_list(digest: &mut Sha256, values: &[String]) {
    digest.update((values.len() as u64).to_be_bytes());
    for value in values {
        hash_field(digest, value);
    }
}

fn hash_field(digest: &mut Sha256, value: &str) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value.as_bytes());
}

fn required_line(name: &str, value: &str) -> rk_core::Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(rk_core::Error::other(format!(
            "proposal {name} must not be empty"
        )));
    }
    if value.contains('\0') {
        return Err(rk_core::Error::other(format!(
            "proposal {name} must not contain NUL"
        )));
    }
    Ok(value.to_string())
}

fn canonical_lines(name: &str, values: Vec<String>) -> rk_core::Result<Vec<String>> {
    if values.is_empty() {
        return Err(rk_core::Error::other(format!(
            "proposal {name} must not be empty"
        )));
    }
    values
        .into_iter()
        .map(|value| required_line(name, &value))
        .collect()
}

fn normalize_newlines(value: &str) -> String {
    value.replace("\r\n", "\n").replace('\r', "\n")
}

fn validate_kind_action(
    kind: OnboardingProposalKind,
    action: OnboardingProposalAction,
) -> rk_core::Result<()> {
    let valid = matches!(
        (kind, action),
        (
            OnboardingProposalKind::RepoFile,
            OnboardingProposalAction::WriteRepoFile
        ) | (
            OnboardingProposalKind::CastleConfig,
            OnboardingProposalAction::ChangeCastleConfig
        ) | (
            OnboardingProposalKind::Registration,
            OnboardingProposalAction::RegisterRepository
        ) | (
            OnboardingProposalKind::WorkflowActivation,
            OnboardingProposalAction::ActivateWorkflow
        ) | (
            OnboardingProposalKind::TriggerActivation,
            OnboardingProposalAction::ActivateTrigger
        ) | (
            OnboardingProposalKind::ScheduleActivation,
            OnboardingProposalAction::ActivateSchedule
        )
    );
    if !valid {
        return Err(rk_core::Error::other(format!(
            "proposal action {action} is not valid for kind {kind}"
        )));
    }
    Ok(())
}

fn validate_automation_target(
    kind: OnboardingProposalKind,
    action: OnboardingProposalAction,
    target: &str,
) -> rk_core::Result<()> {
    let valid = match (kind, action) {
        (
            OnboardingProposalKind::WorkflowActivation,
            OnboardingProposalAction::ActivateWorkflow,
        ) => {
            let path = Path::new(target);
            path.parent() == Some(Path::new(".rk/workflows"))
                && path.extension().is_some_and(|extension| extension == "cue")
                && path.file_stem().is_some_and(|stem| !stem.is_empty())
        }
        (OnboardingProposalKind::TriggerActivation, OnboardingProposalAction::ActivateTrigger) => {
            target == ".rk/triggers.cue"
        }
        (
            OnboardingProposalKind::ScheduleActivation,
            OnboardingProposalAction::ActivateSchedule,
        ) => target == ".rk/schedules.cue",
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(rk_core::Error::other(format!(
            "proposal kind {kind} requires its canonical repo-local automation target, not {target}"
        )))
    }
}

fn canonical_repo_target(target: &str) -> rk_core::Result<String> {
    let path = Path::new(target);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(rk_core::Error::other(format!(
            "repository proposal target must be a relative path without `..`: {target}"
        )));
    }
    let normalized = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_string_lossy()),
            Component::CurDir => None,
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/");
    if normalized.is_empty() {
        return Err(rk_core::Error::other(
            "repository proposal target must not be empty",
        ));
    }
    Ok(normalized)
}

fn canonical_repo_cwd(cwd: &str) -> rk_core::Result<String> {
    let cwd = cwd.trim();
    if cwd.is_empty() || cwd == "." {
        return Ok(".".into());
    }
    canonical_repo_target(cwd)
}

fn validate_duration(value: &str) -> rk_core::Result<()> {
    let value = value.trim();
    let (number, multiplier) = match value.chars().last() {
        Some('s') => (&value[..value.len() - 1], 1_u64),
        Some('m') => (&value[..value.len() - 1], 60),
        Some('h') => (&value[..value.len() - 1], 3600),
        _ => (value, 1),
    };
    let number = number
        .parse::<u64>()
        .map_err(|_| rk_core::Error::other(format!("invalid named_check.timeout: {value}")))?;
    number
        .checked_mul(multiplier)
        .filter(|seconds| *seconds > 0)
        .ok_or_else(|| rk_core::Error::other(format!("invalid named_check.timeout: {value}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn draft() -> OnboardingProposalDraft {
        OnboardingProposalDraft {
            kind: OnboardingProposalKind::RepoFile,
            title: "Add named checks".into(),
            evidence: vec!["README documents mise run verify".into()],
            target_path: "./.rk/checks.cue".into(),
            action: OnboardingProposalAction::WriteRepoFile,
            diff: "--- /dev/null\r\n+++ b/.rk/checks.cue\r\n".into(),
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
    fn digest_is_canonical_and_content_bound() {
        let first = OnboardingProposal::new(
            "onb-one".into(),
            "repo-one".into(),
            "tree-one".into(),
            draft(),
            "onboarder".into(),
        )
        .unwrap();
        let second = OnboardingProposal::new(
            "onb-one".into(),
            "repo-one".into(),
            "tree-one".into(),
            draft(),
            "other-proposer".into(),
        )
        .unwrap();
        assert_eq!(first.digest, second.digest);
        assert_eq!(first.target_path, ".rk/checks.cue");
        assert!(first.diff.contains('\n'));
        assert!(!first.diff.contains('\r'));

        let mut changed = draft();
        changed.named_check.as_mut().unwrap().command = "mise run test".into();
        let changed = OnboardingProposal::new(
            "onb-one".into(),
            "repo-one".into(),
            "tree-one".into(),
            changed,
            "onboarder".into(),
        )
        .unwrap();
        assert_ne!(first.digest, changed.digest);
        assert_ne!(first.id, changed.id);
    }

    #[test]
    fn checks_proposal_requires_exact_executable_contract() {
        let mut missing = draft();
        missing.named_check = None;
        let error = OnboardingProposal::new(
            "onb-one".into(),
            "repo-one".into(),
            "tree-one".into(),
            missing,
            "onboarder".into(),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("must bind an exact named_check contract"));
    }

    #[test]
    fn digest_binds_every_executable_contract_field() {
        let base = OnboardingProposal::new(
            "onb-one".into(),
            "repo-one".into(),
            "tree-one".into(),
            draft(),
            "onboarder".into(),
        )
        .unwrap();
        let mutations: [fn(&mut OnboardingNamedCheck); 6] = [
            |check| check.command = "mise run test".into(),
            |check| check.cwd = "crates/rk-cli".into(),
            |check| check.expect_exit = 2,
            |check| check.timeout = "30m".into(),
            |check| check.environment_policy = CheckEnvironmentPolicy::Inherit,
            |check| check.toolchain = "system cargo".into(),
        ];
        let variants = mutations.map(|mutate| {
            let mut changed = draft();
            mutate(changed.named_check.as_mut().unwrap());
            OnboardingProposal::new(
                "onb-one".into(),
                "repo-one".into(),
                "tree-one".into(),
                changed,
                "onboarder".into(),
            )
            .unwrap()
            .digest
        });
        assert!(variants.iter().all(|digest| digest != &base.digest));
    }

    #[test]
    fn integrity_check_detects_an_edited_diff() {
        let mut proposal = OnboardingProposal::new(
            "onb-one".into(),
            "repo-one".into(),
            "tree-one".into(),
            draft(),
            "onboarder".into(),
        )
        .unwrap();
        proposal.diff.push_str("+edited: true\n");
        assert!(proposal.validate_integrity().is_err());
    }

    #[test]
    fn repository_targets_cannot_escape_the_worktree() {
        let mut escaping = draft();
        escaping.target_path = "../config.toml".into();
        assert!(OnboardingProposal::new(
            "onb-one".into(),
            "repo-one".into(),
            "tree-one".into(),
            escaping,
            "onboarder".into(),
        )
        .is_err());
    }
}
