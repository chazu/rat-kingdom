use std::{collections::BTreeMap, path::Path, sync::Mutex};

use chrono::{Duration, Utc};
use rk_core::action::{
    action_digest, ActionDigestPayload, ActionProposal, ActionScope, ApprovalGrant, ApprovalStatus,
    FactoryAction, RepoScope, WorkflowRunAction,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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

    pub fn propose_action(
        &self,
        requester: &str,
        factory_action: FactoryAction,
        ttl_seconds: Option<i64>,
    ) -> rk_core::Result<ActionProposal> {
        let now = Utc::now();
        let scope = ActionScope {
            repo: factory_action.repo_scope(),
        };
        let mut proposal = ActionProposal {
            schema: 1,
            id: format!("act-{}", now.timestamp_nanos_opt().unwrap_or_default()),
            digest: String::new(),
            kind: factory_action.kind(),
            risk: factory_action.risk(),
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
        proposal.id = proposal.digest.clone();
        let mut data = self.lock()?;
        data.proposals.insert(proposal.id.clone(), proposal.clone());
        self.persist(&data)?;
        Ok(proposal)
    }

    pub fn propose(
        &self,
        requester: &str,
        action: WorkflowRunAction,
        ttl_seconds: Option<i64>,
    ) -> rk_core::Result<ActionProposal> {
        self.propose_action(requester, FactoryAction::WorkflowRun(action), ttl_seconds)
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
        if let Some(existing) = data.grants.get(proposal_id) {
            if existing.digest != digest
                || existing.scope != proposal.scope
                || existing.requester != proposal.requester
            {
                return Err(rk_core::Error::other("approval binding mismatch"));
            }
            return match existing.status {
                ApprovalStatus::Approved => Err(rk_core::Error::other("approval already approved")),
                ApprovalStatus::Executing => {
                    Err(rk_core::Error::other("approval already executing"))
                }
                ApprovalStatus::Consumed => Err(rk_core::Error::other("approval already consumed")),
                ApprovalStatus::Failed => Err(rk_core::Error::other("approval already failed")),
            };
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
            instance_id: Some(bound_instance_id(&proposal.id, &proposal.digest)),
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
                if grant.instance_id.is_none() {
                    grant.instance_id = Some(bound_instance_id(&proposal.id, &proposal.digest));
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

    pub fn list(&self) -> rk_core::Result<Vec<ActionProposal>> {
        let data = self.lock()?;
        Ok(data.proposals.values().cloned().collect())
    }

    pub fn list_grants(&self) -> rk_core::Result<Vec<ApprovalGrant>> {
        let data = self.lock()?;
        Ok(data.grants.values().cloned().collect())
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

fn bound_instance_id(proposal_id: &str, digest: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"rk.workflow.approval.instance.v1\0");
    hasher.update(proposal_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(digest.as_bytes());
    let hex = hex::encode(hasher.finalize());
    format!("wf-{}", &hex[..32])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn action(repo_identity: &str, repo_path: &str) -> WorkflowRunAction {
        WorkflowRunAction {
            name: "factory-test".into(),
            repo: repo_identity.into(),
            repo_identity: repo_identity.into(),
            repo_path: repo_path.into(),
            params: [("taskId".into(), json!("one"))].into_iter().collect(),
            coordinator: Some("coord-a".into()),
        }
    }

    fn store() -> (tempfile::TempDir, ActionApprovalStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = ActionApprovalStore::load(dir.path().join("approvals.json")).unwrap();
        (dir, store)
    }

    #[test]
    fn begin_execute_rejects_caller_mismatch() {
        let (_dir, store) = store();
        let action = action("repo-a", "/repo/a");
        let proposal = store.propose("caller-a", action.clone(), None).unwrap();
        store
            .approve(&proposal.id, &proposal.digest, "operator")
            .unwrap();
        let err = store
            .begin_execute(&proposal.id, &proposal.digest, "caller-b", &action)
            .unwrap_err()
            .to_string();
        assert!(err.contains("caller mismatch"), "{err}");
    }

    #[test]
    fn begin_execute_rejects_scope_repo_mismatch() {
        let (_dir, store) = store();
        let original = action("repo-a", "/repo/a");
        let proposal = store.propose("caller-a", original.clone(), None).unwrap();
        store
            .approve(&proposal.id, &proposal.digest, "operator")
            .unwrap();
        let changed = action("repo-b", "/repo/b");
        let err = store
            .begin_execute(&proposal.id, &proposal.digest, "caller-a", &changed)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("digest mismatch") || err.contains("binding mismatch"),
            "{err}"
        );
    }

    #[test]
    fn begin_execute_rejects_expired_proposal() {
        let (_dir, store) = store();
        let action = action("repo-a", "/repo/a");
        let proposal = store.propose("caller-a", action.clone(), Some(1)).unwrap();
        store
            .approve(&proposal.id, &proposal.digest, "operator")
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let err = store
            .begin_execute(&proposal.id, &proposal.digest, "caller-a", &action)
            .unwrap_err()
            .to_string();
        assert!(err.contains("expired"), "{err}");
    }

    #[test]
    fn approval_lifecycle_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("approvals.json");
        let action = action("repo-a", "/repo/a");
        let (proposal_id, digest, instance_id) = {
            let store = ActionApprovalStore::load(&path).unwrap();
            let proposal = store.propose("caller-a", action.clone(), None).unwrap();
            store
                .approve(&proposal.id, &proposal.digest, "operator")
                .unwrap();
            let grant = store
                .begin_execute(&proposal.id, &proposal.digest, "caller-a", &action)
                .unwrap();
            let instance_id = grant.instance_id.expect("instance bound before launch");
            store.finish_success(&proposal.id, &instance_id).unwrap();
            (proposal.id, proposal.digest, instance_id)
        };
        let reloaded = ActionApprovalStore::load(&path).unwrap();
        let grant = reloaded
            .begin_execute(&proposal_id, &digest, "caller-a", &action)
            .unwrap();
        assert_eq!(grant.status, ApprovalStatus::Consumed);
        assert_eq!(grant.instance_id.as_deref(), Some(instance_id.as_str()));
    }

    #[test]
    fn approve_rejects_every_existing_grant_state() {
        let (_dir, store) = store();
        let action = action("repo-a", "/repo/a");
        let proposal = store.propose("caller-a", action.clone(), None).unwrap();
        let first = store
            .approve(&proposal.id, &proposal.digest, "operator")
            .unwrap();
        assert_eq!(first.status, ApprovalStatus::Approved);

        let err = store
            .approve(&proposal.id, &proposal.digest, "operator")
            .unwrap_err()
            .to_string();
        assert!(err.contains("already approved"), "{err}");

        let executing = store
            .begin_execute(&proposal.id, &proposal.digest, "caller-a", &action)
            .unwrap();
        assert_eq!(executing.status, ApprovalStatus::Executing);
        let err = store
            .approve(&proposal.id, &proposal.digest, "operator")
            .unwrap_err()
            .to_string();
        assert!(err.contains("already executing"), "{err}");

        store
            .finish_success(&proposal.id, executing.instance_id.as_deref().unwrap())
            .unwrap();
        let err = store
            .approve(&proposal.id, &proposal.digest, "operator")
            .unwrap_err()
            .to_string();
        assert!(err.contains("already consumed"), "{err}");

        let failed_proposal = store.propose("caller-a", action, None).unwrap();
        store
            .approve(&failed_proposal.id, &failed_proposal.digest, "operator")
            .unwrap();
        store.finish_failed(&failed_proposal.id, "boom").unwrap();
        let err = store
            .approve(&failed_proposal.id, &failed_proposal.digest, "operator")
            .unwrap_err()
            .to_string();
        assert!(err.contains("already failed"), "{err}");
    }

    #[test]
    fn approve_rejects_lifecycle_reset_for_executing_or_consumed() {
        let (_dir, store) = store();
        let action = action("repo-a", "/repo/a");
        let proposal = store.propose("caller-a", action.clone(), None).unwrap();
        store
            .approve(&proposal.id, &proposal.digest, "operator")
            .unwrap();
        let grant = store
            .begin_execute(&proposal.id, &proposal.digest, "caller-a", &action)
            .unwrap();
        let err = store
            .approve(&proposal.id, &proposal.digest, "operator")
            .unwrap_err()
            .to_string();
        assert!(err.contains("already executing"), "{err}");
        store
            .finish_success(&proposal.id, grant.instance_id.as_deref().unwrap())
            .unwrap();
        let err = store
            .approve(&proposal.id, &proposal.digest, "operator")
            .unwrap_err()
            .to_string();
        assert!(err.contains("already consumed"), "{err}");
    }

    #[test]
    fn concurrent_begin_execute_binds_single_instance() {
        let (_dir, store) = store();
        let action = action("repo-a", "/repo/a");
        let proposal = store.propose("caller-a", action.clone(), None).unwrap();
        store
            .approve(&proposal.id, &proposal.digest, "operator")
            .unwrap();
        let first = store
            .begin_execute(&proposal.id, &proposal.digest, "caller-a", &action)
            .unwrap();
        let second = store
            .begin_execute(&proposal.id, &proposal.digest, "caller-a", &action)
            .unwrap();
        assert_eq!(first.status, ApprovalStatus::Executing);
        assert_eq!(first.instance_id, second.instance_id);
        assert!(first.instance_id.is_some());
    }
}
