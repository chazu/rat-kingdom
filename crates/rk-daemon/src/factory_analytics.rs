//! Read-only Phase 5 factory self-optimization adapter.
//!
//! Normalizes existing structured daemon records (`AgentRecord` and workflow
//! `Instance`) into `rk_core::factory` outcome facts, aggregates deterministic
//! scorecards, and evaluates advisory recommendations. Every function here is
//! pure over owned, read-only record clones: the adapter cannot mutate agents,
//! instances, tickets, policy, config, approvals, queues, or dispatch state,
//! and it never parses logs, transcripts, prose, or terminal output.
//!
//! Only the structured seams that RK actually exposes today populate metrics.
//! Source families without a structured RK store (Phase 3 contract/verified
//! delivery, structured reviewer rework, Phase 4 CI signals, structured revert,
//! human gate decisions, explicit recurrence keys, and pricing snapshots) are
//! reported as `unobserved` with `available=false`, never as zero failures.

use serde::Deserialize;
use serde_json::{json, Value};

use rk_core::factory::outcome_events::{FactoryMetricPayload, StructuredOutcomeInput};
use rk_core::factory::outcome_facts::{
    OutcomeEvidenceKind, OutcomeFact, OutcomeFactBuilder, OutcomeFactSource,
};
use rk_core::factory::recommendations::evaluate_recommendation_report;
use rk_core::factory::scorecards::{
    aggregate_scorecards, FactoryScorecard, ScorecardProjection, ScorecardQuery,
};

use crate::agents::{AgentRecord, AgentState};
use crate::workflow_exec::Instance;

/// Wire schema version of the read-only analytics envelopes.
pub const SCHEMA_VERSION: u32 = 1;

/// Source families that RK exposes as structured records today and can populate
/// with observed facts. Everything else is reported as `unobserved`.
const AVAILABLE_FAMILIES: &[OutcomeEvidenceKind] = &[
    OutcomeEvidenceKind::AgentRecord,
    OutcomeEvidenceKind::WorkflowInstance,
];

/// Source families with no structured RK store yet. Reported as unobserved with
/// availability/source counts so unavailable metrics never look healthy.
const UNOBSERVED_FAMILIES: &[OutcomeEvidenceKind] = &[
    OutcomeEvidenceKind::Phase3Contract,
    OutcomeEvidenceKind::Phase3VerifiedDelivery,
    OutcomeEvidenceKind::StructuredReviewerRework,
    OutcomeEvidenceKind::Phase4CiSignal,
    OutcomeEvidenceKind::StructuredRevert,
    OutcomeEvidenceKind::HumanGateDecision,
    OutcomeEvidenceKind::RecurrenceKey,
    OutcomeEvidenceKind::PricingSnapshot,
];

/// Read-only request shared by `factory.scorecards` and `factory.recommend`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct FactoryAnalyticsRequest {
    pub repo: Option<String>,
    pub group_by: Option<String>,
    pub include_archived: bool,
    pub since: Option<i64>,
    pub until: Option<i64>,
    pub min_sample: Option<u32>,
}

impl FactoryAnalyticsRequest {
    fn projection(&self) -> ScorecardProjection {
        match self.group_by.as_deref() {
            Some("task_class") => ScorecardProjection::TaskClass,
            Some("workflow") => ScorecardProjection::Workflow,
            Some("harness") => ScorecardProjection::Harness,
            Some("model") => ScorecardProjection::Model,
            Some("task_class_workflow") => ScorecardProjection::TaskClassWorkflow,
            Some("all") => ScorecardProjection::All,
            // Default and explicit "composite" both key on the primary composite.
            _ => ScorecardProjection::Composite,
        }
    }
}

/// Owned, read-only snapshot of the structured records the adapter reads.
pub struct AnalyticsInputs {
    pub repo: String,
    pub agents: Vec<AgentRecord>,
    pub instances: Vec<Instance>,
}

/// Convert decimal USD to integer micro-USD with round-half-away-from-zero.
/// Non-finite or negative costs yield `None` (cost unavailable for that run).
fn usd_to_micro(usd: f64) -> Option<u64> {
    if !usd.is_finite() || usd < 0.0 {
        return None;
    }
    let scaled = usd * 1_000_000.0;
    // round-half-away-from-zero; scaled is non-negative here.
    let rounded = (scaled + 0.5).floor();
    if !rounded.is_finite() || rounded < 0.0 || rounded > u64::MAX as f64 {
        return None;
    }
    Some(rounded as u64)
}

/// A settled run contributes outcome facts; live/orphaned generations are still
/// in flight and produce no terminal metrics.
fn is_settled(state: AgentState) -> bool {
    matches!(
        state,
        AgentState::Completed | AgentState::Failed | AgentState::Dismissed
    )
}

/// Normalize structured records into outcome facts. Pure over owned clones.
fn normalize_facts(inputs: &AnalyticsInputs) -> Vec<OutcomeFact> {
    let (structured, unavailable) = normalize_inputs(inputs);
    // Keep archived facts in the immutable fact set so source metadata always
    // exposes active/archived splits. `ScorecardQuery::include_archived` alone
    // controls whether archived facts enter metric numerators/denominators.
    OutcomeFactBuilder::from_structured_inputs(structured, unavailable)
        .include_archived(true)
        .build()
}

/// Build structured outcome inputs plus the unavailable source markers. Kept
/// separate so tests can assert exactly which families are observed.
fn normalize_inputs(
    inputs: &AnalyticsInputs,
) -> (Vec<StructuredOutcomeInput>, Vec<OutcomeFactSource>) {
    // Map workflow-instance id -> workflow name for grouping.
    let workflow_of = |instance_id: &Option<String>| -> Option<String> {
        let id = instance_id.as_deref()?;
        inputs
            .instances
            .iter()
            .find(|instance| instance.id == id)
            .map(|instance| instance.workflow.clone())
    };

    let mut structured = Vec::new();
    for agent in &inputs.agents {
        if !is_settled(agent.state) {
            continue;
        }
        let archived = agent.archived_at.is_some();
        let workflow = workflow_of(&agent.workflow_instance);
        let harness = Some(agent.harness.clone());
        let model = agent.model.clone();
        let observed_at_ms = agent.updated_at.timestamp_millis();
        // Stable per-generation run id: name + creation instant. Names are not
        // recycled, so this is unique across generations.
        let run_id = format!("{}:{}", agent.name, agent.created_at.timestamp_millis());

        let base = |source_id: String,
                    source_family: OutcomeEvidenceKind,
                    payload: FactoryMetricPayload|
         -> StructuredOutcomeInput {
            StructuredOutcomeInput {
                repo: inputs.repo.clone(),
                source_family,
                source_id,
                source_version: None,
                archived,
                archive_reason: None,
                observed_at_ms,
                // task_class requires an explicit Phase 3 contract/ticket/outcome
                // field, which RK does not attach to agent records today. Left
                // None so it normalizes to `unknown`, never inferred from prose.
                task_class: None,
                workflow: workflow.clone(),
                harness: harness.clone(),
                model: model.clone(),
                agent_id: Some(agent.name.clone()),
                workflow_instance_id: agent.workflow_instance.clone(),
                ticket_id: None,
                phase3_outcome_id: None,
                phase4_signal_id: None,
                recurrence_key: None,
                coalesce_key: None,
                payload,
                decoy_prose: String::new(),
            }
        };

        // One run per settled agent generation.
        structured.push(base(
            run_id.clone(),
            OutcomeEvidenceKind::AgentRecord,
            FactoryMetricPayload::Run { count: 1 },
        ));

        // Cost: harness-reported cost with an explicit pricing evidence id.
        if let Some(micro_usd) = usd_to_micro(agent.cost_usd).filter(|m| *m > 0) {
            structured.push(base(
                run_id.clone(),
                OutcomeEvidenceKind::AgentRecord,
                FactoryMetricPayload::Cost {
                    micro_usd,
                    pricing_evidence_id: Some(format!("harness-reported:{run_id}")),
                },
            ));
        }

        // Lead time from explicit lifecycle timestamps on the same run. The core
        // only accepts it when workflow_instance_id == run_id == completed_run_id,
        // so use the stable run id for all three.
        let started = agent.created_at.timestamp_millis();
        let completed = agent.updated_at.timestamp_millis();
        if completed >= started {
            let mut lead = base(
                run_id.clone(),
                OutcomeEvidenceKind::AgentRecord,
                FactoryMetricPayload::LeadTime {
                    started_at_ms: started,
                    completed_at_ms: completed,
                    run_id: run_id.clone(),
                    completed_run_id: run_id.clone(),
                },
            );
            lead.workflow_instance_id = Some(run_id.clone());
            structured.push(lead);
        }
    }

    let unavailable = UNOBSERVED_FAMILIES
        .iter()
        .map(|family| OutcomeFactSource::unavailable(*family))
        .collect();

    (structured, unavailable)
}

fn scorecards(inputs: &AnalyticsInputs, req: &FactoryAnalyticsRequest) -> Vec<FactoryScorecard> {
    let facts = normalize_facts(inputs);
    aggregate_scorecards(
        &facts,
        ScorecardQuery {
            include_archived: req.include_archived,
            projections: vec![req.projection()],
        },
    )
}

/// Source counts and availability metadata rolled up across observed families,
/// so unavailable metrics are visible and cannot look healthy.
fn availability_envelope(rows: &[FactoryScorecard]) -> (Value, Value, Vec<String>) {
    use rk_core::factory::outcome_facts::SourceCounts;
    use std::collections::BTreeMap;

    let mut counts: BTreeMap<OutcomeEvidenceKind, SourceCounts> = BTreeMap::new();
    let mut available: BTreeMap<OutcomeEvidenceKind, bool> = BTreeMap::new();
    // AgentRecord and WorkflowInstance are structured stores the daemon always
    // reads, so they are available regardless of row count. Every other family
    // has no structured RK store yet and stays unobserved until one exists.
    for family in AVAILABLE_FAMILIES {
        counts.entry(*family).or_default();
        available.insert(*family, true);
    }
    for family in UNOBSERVED_FAMILIES {
        counts.entry(*family).or_default();
        available.entry(*family).or_insert(false);
    }
    // Every aggregation request always includes the canonical composite rows.
    // Roll top-level metadata up from those rows only: projection rows repeat
    // the same facts for display and must not multiply source/event counts.
    for row in rows.iter().filter(|row| !row.projected) {
        for (family, sc) in &row.source_counts.by_family {
            let entry = counts.entry(*family).or_default();
            entry.active_source_count += sc.active_source_count;
            entry.archived_source_count += sc.archived_source_count;
            entry.event_count += sc.event_count;
        }
        for (family, avail) in &row.availability.by_family {
            let entry = available.entry(*family).or_insert(false);
            *entry = *entry || avail.available;
        }
    }

    let source_counts = json!(counts
        .iter()
        .map(|(family, sc)| {
            json!({
                "source_family": family,
                "active_source_count": sc.active_source_count,
                "archived_source_count": sc.archived_source_count,
                "event_count": sc.event_count,
            })
        })
        .collect::<Vec<_>>());
    let availability = json!(available
        .iter()
        .map(|(family, avail)| json!({"source_family": family, "available": avail}))
        .collect::<Vec<_>>());

    let warnings = available
        .iter()
        .filter(|(_, avail)| !**avail)
        .map(|(family, _)| {
            format!("source_family_unobserved: {family:?} has no structured RK store; metrics reported as unobserved, not zero")
        })
        .collect::<Vec<_>>();

    (source_counts, availability, warnings)
}

/// Build the read-only `factory.scorecards` response envelope.
pub fn scorecards_response(
    inputs: &AnalyticsInputs,
    req: &FactoryAnalyticsRequest,
    generated_at: chrono::DateTime<chrono::Utc>,
) -> Value {
    let rows = scorecards(inputs, req);
    let (source_counts, availability, warnings) = availability_envelope(&rows);
    json!({
        "schema_version": SCHEMA_VERSION,
        "repo": inputs.repo,
        "generated_at": generated_at,
        "group_by": req.projection(),
        "include_archived": req.include_archived,
        "source_counts": source_counts,
        "availability": availability,
        "scorecards": rows,
        "warnings": warnings,
    })
}

/// Build the read-only `factory.recommend` response envelope.
pub fn recommend_response(
    inputs: &AnalyticsInputs,
    req: &FactoryAnalyticsRequest,
    generated_at: chrono::DateTime<chrono::Utc>,
) -> Value {
    let rows = scorecards(inputs, req);
    let report = evaluate_recommendation_report(&rows);
    let (source_counts, availability, mut warnings) = availability_envelope(&rows);
    warnings.extend(report.warnings.iter().cloned());
    warnings.sort();
    warnings.dedup();
    json!({
        "schema_version": SCHEMA_VERSION,
        "repo": inputs.repo,
        "generated_at": generated_at,
        "group_by": req.projection(),
        "include_archived": req.include_archived,
        "nature": report.nature,
        "source_counts": source_counts,
        "availability": availability,
        "scorecards": rows,
        "recommendations": report.recommendations,
        "suppressions": report.suppressions,
        "warnings": warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agents::AgentState;
    use crate::workflow_exec::{Instance, InstanceStatus, WorkflowContext};
    use chrono::{TimeZone, Utc};
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn agent(
        name: &str,
        harness: &str,
        model: Option<&str>,
        instance: Option<&str>,
    ) -> AgentRecord {
        AgentRecord {
            name: name.into(),
            role: "rat".into(),
            coordination: None,
            harness: harness.into(),
            permission_mode: None,
            model: model.map(str::to_string),
            repo_root: PathBuf::from("/tmp/repo"),
            repo_name: "rat-kingdom".into(),
            task: Some("do work".into()),
            branch: None,
            worktree: None,
            target_branch: "main".into(),
            parent: None,
            workflow_instance: instance.map(str::to_string),
            coordinator: None,
            session_id: Some("sess".into()),
            attach_target: None,
            pid: None,
            merge_commit: None,
            state: AgentState::Completed,
            crashed: false,
            result: Some("did the work".into()),
            progress: None,
            usage: Default::default(),
            cost_usd: 0.0,
            created_at: Utc.timestamp_opt(1_000, 0).unwrap(),
            updated_at: Utc.timestamp_opt(1_030, 0).unwrap(),
            archived_at: None,
        }
    }

    fn instance(id: &str, workflow: &str) -> Instance {
        Instance {
            id: id.into(),
            workflow: workflow.into(),
            repo: "rat-kingdom".into(),
            coordinator: None,
            status: InstanceStatus::Completed,
            revision: 0,
            current_step: 1,
            total_steps: 1,
            context: WorkflowContext::default(),
            error: None,
            awaiting: None,
            instance_max_usd: None,
            definition: "implement-featureset".into(),
            definition_digest: String::new(),
            automated_landing_authorized: false,
            params: HashMap::new(),
            depth: 0,
            started_at: Utc.timestamp_opt(1_000, 0).unwrap(),
            completed_at: Some(Utc.timestamp_opt(1_030, 0).unwrap()),
            archived_at: None,
        }
    }

    fn inputs() -> AnalyticsInputs {
        let mut a = agent("rat-1", "claude", Some("sonnet"), Some("wf-1"));
        a.cost_usd = 0.25;
        let mut b = agent("rat-2", "claude", Some("sonnet"), Some("wf-1"));
        b.cost_usd = 0.10;
        AnalyticsInputs {
            repo: "rat-kingdom".into(),
            agents: vec![a, b],
            instances: vec![instance("wf-1", "implement-featureset")],
        }
    }

    #[test]
    fn normalizes_runs_from_agent_and_instance_without_reading_prose() {
        let (structured, _) = normalize_inputs(&inputs());
        let runs = structured
            .iter()
            .filter(|s| matches!(s.payload, FactoryMetricPayload::Run { .. }))
            .count();
        assert_eq!(runs, 2, "one run per settled agent generation");
        // Workflow grouping comes from the instance, harness/model from the agent.
        assert!(structured
            .iter()
            .all(|s| s.workflow.as_deref() == Some("implement-featureset")));
        assert!(structured
            .iter()
            .all(|s| s.harness.as_deref() == Some("claude")));
        // task_class is never inferred; stays None -> normalizes to unknown.
        assert!(structured.iter().all(|s| s.task_class.is_none()));
    }

    #[test]
    fn missing_source_families_are_unobserved_with_availability() {
        let req = FactoryAnalyticsRequest::default();
        let resp = scorecards_response(&inputs(), &req, Utc.timestamp_opt(2_000, 0).unwrap());
        let availability = resp["availability"].as_array().unwrap();
        let phase3 = availability
            .iter()
            .find(|a| a["source_family"] == json!("Phase3VerifiedDelivery"))
            .expect("phase3 family present");
        assert_eq!(phase3["available"], json!(false));
        let agent_family = availability
            .iter()
            .find(|a| a["source_family"] == json!("AgentRecord"))
            .expect("agent family present");
        assert_eq!(agent_family["available"], json!(true));
        assert!(resp["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w.as_str().unwrap().contains("Phase3VerifiedDelivery")));
    }

    #[test]
    fn scorecards_count_runs_and_cost() {
        let req = FactoryAnalyticsRequest::default();
        let resp = scorecards_response(&inputs(), &req, Utc.timestamp_opt(2_000, 0).unwrap());
        let cards = resp["scorecards"].as_array().unwrap();
        let observed: u64 = cards
            .iter()
            .map(|c| c["metrics"]["runs"].as_u64().unwrap())
            .sum();
        assert_eq!(observed, 2);
        let total_cost: u64 = cards
            .iter()
            .map(|c| c["metrics"]["total_cost_micro_usd"].as_u64().unwrap())
            .sum();
        // 0.25 + 0.10 USD = 350_000 micro-USD.
        assert_eq!(total_cost, 350_000);
    }

    #[test]
    fn deterministic_across_input_order() {
        let a = inputs();
        let mut b = inputs();
        b.agents.reverse();
        let req = FactoryAnalyticsRequest::default();
        let at = Utc.timestamp_opt(2_000, 0).unwrap();
        assert_eq!(
            scorecards_response(&a, &req, at),
            scorecards_response(&b, &req, at)
        );
    }

    #[test]
    fn recommend_is_advisory_and_read_only_shaped() {
        let req = FactoryAnalyticsRequest::default();
        let resp = recommend_response(&inputs(), &req, Utc.timestamp_opt(2_000, 0).unwrap());
        assert_eq!(resp["nature"], json!("advisory"));
        assert!(resp["recommendations"].is_array());
        // No mutation-shaped fields leak into the payload.
        let blob = resp.to_string();
        for banned in [
            "\"apply\"",
            "\"dispatch\"",
            "rewrite-policy",
            "update-workflow",
        ] {
            assert!(!blob.contains(banned), "payload must not contain {banned}");
        }
    }

    #[test]
    fn usd_to_micro_rounds_half_away_and_rejects_non_finite() {
        assert_eq!(usd_to_micro(0.0000005), Some(1));
        assert_eq!(usd_to_micro(1.0), Some(1_000_000));
        assert_eq!(usd_to_micro(-1.0), None);
        assert_eq!(usd_to_micro(f64::NAN), None);
        assert_eq!(usd_to_micro(f64::INFINITY), None);
    }

    fn source_count<'a>(response: &'a Value, family: &str) -> &'a Value {
        response["source_counts"]
            .as_array()
            .expect("source counts array")
            .iter()
            .find(|entry| entry["source_family"] == json!(family))
            .unwrap_or_else(|| panic!("missing source count for {family}"))
    }

    #[test]
    fn archived_history_is_reported_even_when_excluded_from_metrics() {
        let mut with_archived = inputs();
        let mut archived = agent("rat-3", "claude", Some("sonnet"), Some("wf-1"));
        archived.archived_at = Some(Utc.timestamp_opt(1_100, 0).unwrap());
        with_archived.agents.push(archived);
        let at = Utc.timestamp_opt(2_000, 0).unwrap();

        let excluded = scorecards_response(&with_archived, &FactoryAnalyticsRequest::default(), at);
        let excluded_runs: u64 = excluded["scorecards"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| !row["projected"].as_bool().unwrap_or(false))
            .map(|row| row["metrics"]["runs"].as_u64().unwrap())
            .sum();
        assert_eq!(excluded_runs, 2);
        let counts = source_count(&excluded, "AgentRecord");
        assert_eq!(counts["active_source_count"], json!(2));
        assert_eq!(counts["archived_source_count"], json!(1));

        let included = scorecards_response(
            &with_archived,
            &FactoryAnalyticsRequest {
                include_archived: true,
                ..Default::default()
            },
            at,
        );
        let included_runs: u64 = included["scorecards"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|row| !row["projected"].as_bool().unwrap_or(false))
            .map(|row| row["metrics"]["runs"].as_u64().unwrap())
            .sum();
        assert_eq!(included_runs, 3);
    }

    #[test]
    fn top_level_source_counts_do_not_multiply_across_projections() {
        let at = Utc.timestamp_opt(2_000, 0).unwrap();
        let composite = scorecards_response(&inputs(), &FactoryAnalyticsRequest::default(), at);
        let all = scorecards_response(
            &inputs(),
            &FactoryAnalyticsRequest {
                group_by: Some("all".into()),
                ..Default::default()
            },
            at,
        );
        assert_eq!(
            source_count(&composite, "AgentRecord"),
            source_count(&all, "AgentRecord")
        );
        assert_eq!(
            source_count(&all, "AgentRecord")["active_source_count"],
            json!(2)
        );
    }

    #[test]
    fn live_agents_produce_no_facts() {
        let mut only_live = inputs();
        for a in &mut only_live.agents {
            a.state = AgentState::Running;
        }
        let (structured, _) = normalize_inputs(&only_live);
        assert!(
            structured.is_empty(),
            "in-flight runs contribute no terminal facts"
        );
    }
}
