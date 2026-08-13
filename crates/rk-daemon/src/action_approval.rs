use std::{collections::BTreeMap, path::Path, sync::Mutex};

use chrono::{Duration, Utc};
use rk_core::action::{
    action_digest, ActionDigestPayload, ActionKind, ActionProposal, ActionRisk, ActionScope,
    ApprovalGrant, ApprovalStatus, FactoryAction, RepoScope, WorkflowRunAction,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Default)]
struct StoreData {
    proposals: BTreeMap<String, ActionProposal>,
    grants: BTreeMap<String, ApprovalGrant>,
}

pub struct ActionApprovalStore {
    path: std::path::PathBuf,
    data: Mutex<StoreData>,
}

#[derive(Debug, Clone)]
pub struct ResolvedWorkflowAction {
    pub proposal: ActionProposal,
    pub action: WorkflowRunAction,
}

impl ActionApprovalStore {
    pub fn load(path: impl AsRef<Path>) -> rk_core::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let data = if path.exists() {
            serde_json::from_str(&std::fs::read_to_string(&path)?)?
        } else {
            StoreData::default()
        };
        Ok(Self {
            path,
            data: Mutex::new(data),
        })
    }

    pub fn propose(
        &self,
        requester: &str,
        action: WorkflowRunAction,
        ttl_seconds: Option<i64>,
    ) -> rk_core::Result<ActionProposal> {
        let now = Utc::now();
        let scope = ActionScope {
            repo: RepoScope {
                identity: action.repo_identity.clone(),
                path: action.repo_path.clone(),
            },
        };
        let factory_action = FactoryAction::WorkflowRun(action);
        let mut proposal = ActionProposal {
            schema: 1,
            id: format!("act-{}", now.timestamp_nanos_opt().unwrap_or_default()),
            digest: String::new(),
            kind: ActionKind::WorkflowRun,
            risk: ActionRisk::Mutation,
            scope,
            requester: requester.to_string(),
            action: factory_action,
            nonce: proposal_nonce(&now, requester),
            created_at: now,
            expires_at: now + Duration::seconds(ttl_seconds.unwrap_or(3600).clamp(1, 86_400)),
            status: "proposed".into(),
        };
        let payload = ActionDigestPayload::from_proposal(&proposal);
        proposal.digest = action_digest(&payload)?;
        let mut data = self.lock()?;
        data.proposals.insert(proposal.id.clone(), proposal.clone());
        self.persist(&data)?;
        Ok(proposal)
    }

    pub fn approve(
        &self,
        proposal_id: &str,
        digest: &str,
        approved_by: &str,
    ) -> rk_core::Result<ApprovalGrant> {
        let mut data = self.lock()?;
        let proposal = data
            .proposals
            .get(proposal_id)
            .cloned()
            .ok_or_else(|| rk_core::Error::other("unknown proposal"))?;
        if proposal.digest != digest {
            return Err(rk_core::Error::other("approval digest mismatch"));
        }
        if proposal.expires_at <= Utc::now() {
            return Err(rk_core::Error::other("proposal expired"));
        }
        let grant = ApprovalGrant {
            schema: 1,
            proposal_id: proposal.id.clone(),
            digest: proposal.digest.clone(),
            kind: proposal.kind,
            scope: proposal.scope.clone(),
            requester: proposal.requester.clone(),
            approved_by: approved_by.to_string(),
            status: ApprovalStatus::Approved,
            approved_at: Utc::now(),
            expires_at: proposal.expires_at,
            execution_id: None,
            instance_id: None,
            failure: None,
            consumed_at: None,
        };
        data.grants.insert(proposal.id.clone(), grant.clone());
        self.persist(&data)?;
        Ok(grant)
    }

    pub fn begin_execute(
        &self,
        proposal_id: &str,
        digest: &str,
        caller: &str,
        action: &WorkflowRunAction,
    ) -> rk_core::Result<ApprovalGrant> {
        let mut data = self.lock()?;
        let proposal = data
            .proposals
            .get(proposal_id)
            .cloned()
            .ok_or_else(|| rk_core::Error::other("unknown proposal"))?;
        let mut recomputed = proposal.clone();
        recomputed.digest.clear();
        recomputed.action = FactoryAction::WorkflowRun(action.clone());
        recomputed.scope = ActionScope {
            repo: RepoScope {
                identity: action.repo_identity.clone(),
                path: action.repo_path.clone(),
            },
        };
        let payload = ActionDigestPayload::from_proposal(&recomputed);
        recomputed.digest = action_digest(&payload)?;
        if proposal.digest != digest || recomputed.digest != digest {
            return Err(rk_core::Error::other("action digest mismatch"));
        }
        if proposal.requester != caller {
            return Err(rk_core::Error::other("caller mismatch"));
        }
        if proposal.expires_at <= Utc::now() {
            return Err(rk_core::Error::other("proposal expired"));
        }
        let grant = data
            .grants
            .get_mut(proposal_id)
            .ok_or_else(|| rk_core::Error::other("proposal is not approved"))?;
        if grant.digest != digest || grant.scope != proposal.scope || grant.requester != caller {
            return Err(rk_core::Error::other("approval binding mismatch"));
        }
        match grant.status {
            ApprovalStatus::Approved | ApprovalStatus::Failed => {
                grant.status = ApprovalStatus::Executing;
                if grant.execution_id.is_none() {
                    grant.execution_id = Some(format!(
                        "exec-{}",
                        Utc::now().timestamp_nanos_opt().unwrap_or_default()
                    ));
                }
                grant.failure = None;
                let out = grant.clone();
                self.persist(&data)?;
                Ok(out)
            }
            ApprovalStatus::Executing | ApprovalStatus::Consumed if grant.instance_id.is_some() => {
                Ok(grant.clone())
            }
            ApprovalStatus::Executing => Ok(grant.clone()),
            ApprovalStatus::Consumed => Err(rk_core::Error::other("approval already consumed")),
        }
    }

    pub fn finish_success(
        &self,
        proposal_id: &str,
        instance_id: &str,
    ) -> rk_core::Result<ApprovalGrant> {
        let mut data = self.lock()?;
        let grant = data
            .grants
            .get_mut(proposal_id)
            .ok_or_else(|| rk_core::Error::other("unknown approval"))?;
        grant.status = ApprovalStatus::Consumed;
        grant.instance_id = Some(instance_id.to_string());
        grant.consumed_at = Some(Utc::now());
        let out = grant.clone();
        self.persist(&data)?;
        Ok(out)
    }

    pub fn finish_failed(
        &self,
        proposal_id: &str,
        failure: &str,
    ) -> rk_core::Result<ApprovalGrant> {
        let mut data = self.lock()?;
        let grant = data
            .grants
            .get_mut(proposal_id)
            .ok_or_else(|| rk_core::Error::other("unknown approval"))?;
        grant.status = ApprovalStatus::Failed;
        grant.failure = Some(failure.to_string());
        let out = grant.clone();
        self.persist(&data)?;
        Ok(out)
    }

    fn lock(&self) -> rk_core::Result<std::sync::MutexGuard<'_, StoreData>> {
        self.data
            .lock()
            .map_err(|_| rk_core::Error::other("factory approval store lock poisoned"))
    }

    fn persist(&self, data: &StoreData) -> rk_core::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(data)?)?;
        std::fs::rename(tmp, &self.path)?;
        Ok(())
    }
}

fn proposal_nonce(now: &chrono::DateTime<Utc>, requester: &str) -> String {
    format!(
        "{}:{requester}",
        now.timestamp_nanos_opt().unwrap_or_default()
    )
}
