//! Durable undo of one exact delivered agent generation. Intent precedes Git;
//! the parked candidate precedes target advancement; completion follows every
//! required write. Retrying any interrupted phase settles the same operation.

use super::{blocking_io, Supervisor};
use crate::agents::AgentRecord;
use rk_core::id::{RecordId, SpawnId};
use rk_core::tuple::{Category, Lifecycle, Pattern, Tuple};
use rk_git::{AdvanceOutcome, PrepareOutcome, Repo};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

const IDENTITY: &str = "revert_operation";

#[derive(Clone, Serialize, Deserialize)]
struct Candidate {
    commit: String,
    base: String,
    candidate_ref: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum Phase {
    Intent,
    Prepared { candidate: Candidate },
    Advanced { candidate: Candidate },
    Complete { candidate: Candidate },
}

#[derive(Clone, Serialize, Deserialize)]
struct RevertOperation {
    id: RecordId,
    revision: u64,
    requested_at: chrono::DateTime<chrono::Utc>,
    agent: String,
    spawn: SpawnId,
    repo_root: std::path::PathBuf,
    repo: String,
    branch: Option<String>,
    target: String,
    task: Option<String>,
    merge_commit: String,
    block: bool,
    fact_id: RecordId,
    phase: Phase,
}

impl RevertOperation {
    fn status(&self) -> Option<&str> {
        self.task
            .as_deref()
            .filter(|task| task.starts_with(crate::tickets::ID_PREFIX))
            .map(|_| if self.block { "blocked" } else { "open" })
    }

    fn result(&self, commit: Option<&str>, detail: &str) -> Value {
        json!({"operation_id": self.id, "agent": self.agent, "spawn": self.spawn,
            "reverted": commit.is_some(), "merge_commit": self.merge_commit,
            "revert_commit": commit, "target": self.target, "task": self.task,
            "ticket_status": commit.and(self.status()), "detail": detail})
    }
}

impl Supervisor {
    /// Parked objects belonging to unfinished durable undo operations must
    /// survive the shared candidate-ref sweep at daemon startup.
    pub(crate) fn pending_revert_candidates(
        &self,
    ) -> rk_core::Result<Vec<(std::path::PathBuf, String)>> {
        Ok(self
            .revert_operations(None)?
            .into_iter()
            .filter_map(|operation| match operation.phase {
                Phase::Prepared { candidate } | Phase::Advanced { candidate } => {
                    Some((operation.repo_root, candidate.candidate_ref))
                }
                Phase::Intent | Phase::Complete { .. } => None,
            })
            .collect())
    }

    /// Retry is idempotent, including after registry anchoring has been cleared.
    /// A retry must retain the original `--block` decision. A newer generation
    /// or delivery is never selected by the old operation's finalization.
    pub async fn revert(&self, name: &str, block: bool) -> rk_core::Result<Value> {
        self.revert_exact(name, block, None).await
    }

    pub(crate) async fn revert_exact(
        &self,
        name: &str,
        block: bool,
        exact: Option<RecordId>,
    ) -> rk_core::Result<Value> {
        let exact_operation = exact
            .map(|id| {
                self.revert_operations(None)?
                    .into_iter()
                    .find(|op| op.id == id && op.agent == name)
                    .ok_or_else(|| rk_core::Error::other("no such revert operation for this agent"))
            })
            .transpose()?;
        let record = {
            let registry = self.lock_registry();
            if let Some(operation) = &exact_operation {
                registry
                    .records_of(name)
                    .into_iter()
                    .find(|record| record.spawn == Some(operation.spawn))
                    .cloned()
            } else {
                registry.get_any(name).cloned()
            }
        }
        .ok_or_else(|| rk_core::Error::other(format!("no such agent generation: {name}")))?;
        let repo_path = record.repo_root.clone();
        let repo = blocking_io("revert repo discovery", move || Repo::discover(&repo_path)).await?;
        let _guard = self
            .merge_queue
            .acquire(repo.root(), &record.target_branch)
            .await;
        // Re-read after waiting for the target lock: another revert or delivery
        // may have settled while this request was queued.
        let record = self
            .lock_registry()
            .records_of(name)
            .into_iter()
            .find(|current| {
                current.spawn == record.spawn
                    && current.repo_root == record.repo_root
                    && current.target_branch == record.target_branch
            })
            .cloned()
            .ok_or_else(|| rk_core::Error::other("agent changed while waiting to revert"))?;
        let mut operation = self.revert_operation(&record, block, exact)?;
        let result = self.run_revert(&repo, &mut operation).await;
        result.map_err(|error| {
            rk_core::Error::other(format!(
                "revert operation {} remains unsettled: {error}; retry rk revert {} --operation {}{}",
                operation.id,
                name,
                operation.id,
                if operation.block { " --block" } else { "" }
            ))
        })
    }

    fn revert_operations(&self, repo: Option<&str>) -> rk_core::Result<Vec<RevertOperation>> {
        let mut latest: BTreeMap<RecordId, RevertOperation> = BTreeMap::new();
        let mut pattern = Pattern::category(Category::Event).identity(IDENTITY);
        if let Some(repo) = repo {
            pattern = pattern.scope(repo);
        }
        for tuple in self.space.scan(&pattern)? {
            let operation: RevertOperation = serde_json::from_value(tuple.payload)?;
            if latest
                .get(&operation.id)
                .is_none_or(|prior| prior.revision < operation.revision)
            {
                latest.insert(operation.id, operation);
            }
        }
        Ok(latest.into_values().collect())
    }

    fn revert_operation(
        &self,
        record: &AgentRecord,
        block: bool,
        exact: Option<RecordId>,
    ) -> rk_core::Result<RevertOperation> {
        let spawn = record
            .spawn
            .ok_or_else(|| rk_core::Error::other("revert requires exact agent generation"))?;
        if let Some(operation) = self
            .revert_operations(Some(&record.repo_name))?
            .into_iter()
            .filter(|op| {
                op.agent == record.name
                    && op.spawn == spawn
                    && if let Some(id) = exact {
                        op.id == id
                    } else {
                        !matches!(op.phase, Phase::Complete { .. })
                            || record
                                .merge_commit
                                .as_ref()
                                .is_none_or(|commit| *commit == op.merge_commit)
                    }
            })
            .max_by_key(|op| op.requested_at)
        {
            if operation.block != block {
                return Err(rk_core::Error::other(
                    "retry must preserve the original revert --block decision",
                ));
            }
            if operation.repo_root != record.repo_root
                || operation.target != record.target_branch
                || operation.task != record.task
                || operation.branch != record.branch
            {
                return Err(rk_core::Error::other(
                    "revert operation no longer matches its recorded source",
                ));
            }
            return Ok(operation);
        }
        if exact.is_some() {
            return Err(rk_core::Error::other(
                "revert operation source is no longer available",
            ));
        }
        let commit = record
            .merge_commit
            .clone()
            .filter(|commit| !commit.is_empty())
            .ok_or_else(|| {
                rk_core::Error::other(format!(
                    "{} has no recorded merge commit to revert",
                    record.name
                ))
            })?;
        let operation = RevertOperation {
            id: RecordId::new(),
            revision: 0,
            requested_at: chrono::Utc::now(),
            agent: record.name.clone(),
            spawn,
            repo_root: record.repo_root.clone(),
            repo: record.repo_name.clone(),
            branch: record.branch.clone(),
            target: record.target_branch.clone(),
            task: record.task.clone(),
            merge_commit: commit,
            block,
            fact_id: RecordId::new(),
            phase: Phase::Intent,
        };
        self.persist_revert(&operation)?;
        Ok(operation)
    }

    fn persist_revert(&self, operation: &RevertOperation) -> rk_core::Result<()> {
        self.space.out(
            Tuple::new(
                Category::Event,
                &operation.repo,
                IDENTITY,
                operation.id.to_string(),
                serde_json::to_value(operation)?,
            )
            .with_lifecycle(Lifecycle::Furniture),
        )
    }

    fn advance_revert_phase(
        &self,
        operation: &mut RevertOperation,
        phase: Phase,
    ) -> rk_core::Result<()> {
        let mut next = operation.clone();
        next.revision += 1;
        next.phase = phase;
        self.persist_revert(&next)?;
        *operation = next;
        Ok(())
    }

    async fn run_revert(
        &self,
        repo: &Repo,
        operation: &mut RevertOperation,
    ) -> rk_core::Result<Value> {
        if let Phase::Complete { candidate } = &operation.phase {
            return Ok(operation.result(Some(&candidate.commit), "revert already completed"));
        }
        if matches!(operation.phase, Phase::Intent) {
            crate::fault::barrier(&self.layout, "revert-after-intent").await;
            let git = repo.clone();
            let commit = operation.merge_commit.clone();
            let target = operation.target.clone();
            let prepared = blocking_io("prepare revert", move || {
                git.prepare_revert(&commit, &target)
            })
            .await?;
            match prepared {
                PrepareOutcome::Conflict { detail } => return Ok(operation.result(None, &detail)),
                PrepareOutcome::Prepared(prepared) => {
                    self.advance_revert_phase(
                        operation,
                        Phase::Prepared {
                            candidate: Candidate {
                                commit: prepared.commit,
                                base: prepared.base,
                                candidate_ref: prepared.candidate_ref,
                            },
                        },
                    )?;
                }
            }
        }
        if let Phase::Prepared { candidate } = operation.phase.clone() {
            crate::fault::barrier(&self.layout, "revert-after-prepared").await;
            let git = repo.clone();
            let target = operation.target.clone();
            let prepared = candidate.clone();
            let outcome = blocking_io("advance revert", move || {
                let tip = git.rev_parse(&format!("refs/heads/{target}"))?;
                if git.is_ancestor(&prepared.commit, &tip) {
                    Ok(AdvanceOutcome::Advanced {
                        commit: prepared.commit,
                    })
                } else {
                    git.advance_target_to(&target, &prepared.commit, &prepared.base)
                }
            })
            .await?;
            match outcome {
                AdvanceOutcome::Advanced { .. } => {
                    crate::fault::barrier(&self.layout, "revert-after-git").await;
                    self.advance_revert_phase(operation, Phase::Advanced { candidate })?;
                }
                AdvanceOutcome::Stale { .. } => {
                    // The exact candidate was proved absent above. Re-preparing
                    // on retry is safe, but cannot be an unbounded retry loop.
                    self.advance_revert_phase(operation, Phase::Intent)?;
                    return Ok(
                        operation.result(None, "target moved; retry revert on the new target tip")
                    );
                }
                AdvanceOutcome::Blocked { path, detail, .. } => {
                    return Err(rk_core::Error::other(format!(
                        "refusing to advance {} at {}: {detail}",
                        operation.target,
                        path.display()
                    )))
                }
            }
        }
        let Phase::Advanced { candidate } = operation.phase.clone() else {
            unreachable!()
        };
        let git = repo.clone();
        let target = operation.target.clone();
        let commit = candidate.commit.clone();
        if !blocking_io("confirm reverted target", move || {
            let tip = git.rev_parse(&format!("refs/heads/{target}"))?;
            Ok(git.is_ancestor(&commit, &tip))
        })
        .await?
        {
            return Err(rk_core::Error::other(
                "recorded revert is no longer on its target; refusing to finalize",
            ));
        }
        self.lock_registry().settle_revert(
            &operation.agent,
            operation.spawn,
            &operation.merge_commit,
        )?;
        crate::fault::barrier(&self.layout, "revert-after-registry").await;
        if let (Some(task), Some(status)) = (operation.task.as_deref(), operation.status()) {
            self.tickets
                .revert_delivery(
                    task,
                    &operation.merge_commit,
                    &operation.target,
                    operation.id,
                    status,
                )
                .await?;
        }
        crate::fault::barrier(&self.layout, "revert-after-ticket").await;
        let result = operation.result(Some(&candidate.commit), "reverted recorded merge");
        let mut fact = Tuple::new(
            Category::Fact,
            &operation.repo,
            format!("merge-reverted-{}", operation.agent),
            self.castle.clone(),
            result.clone(),
        )
        .with_lifecycle(Lifecycle::Furniture);
        fact.id = operation.fact_id;
        fact.created_at = operation.requested_at;
        fact.payload["branch"] = json!(operation.branch);
        self.space.out_if_new(fact)?;
        crate::fault::barrier(&self.layout, "revert-after-evidence").await;
        let git = repo.clone();
        let candidate_ref = candidate.candidate_ref.clone();
        blocking_io("release revert candidate", move || {
            git.discard_candidate(&candidate_ref)
        })
        .await?;
        self.advance_revert_phase(operation, Phase::Complete { candidate })?;
        Ok(result)
    }
}
