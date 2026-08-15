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
//! delivery and pricing snapshots) are reported as `unobserved` with
//! `available=false`, never as zero failures.

use serde::Deserialize;
use serde_json::{json, Value};

use rk_core::factory::outcome_events::{FactoryMetricPayload, StructuredOutcomeInput};
use rk_core::factory::outcome_facts::{
    OutcomeEvidenceKind, OutcomeFact, OutcomeFactBuilder, OutcomeFactSource,
};
use rk_core::action::ApprovalGrant;
use rk_core::factory::recommendations::{
    evaluate_recommendation_report, RecommendationSuppression, SuppressionReason,
};
use rk_core::factory::scorecards::{
    aggregate_scorecards, FactoryScorecard, ScorecardProjection, ScorecardQuery,
};

use crate::agents::{AgentRecord, AgentState};
use crate::workflow_exec::Instance;
use rk_core::tuple::Tuple;

/// Wire schema version of the read-only analytics envelopes.
pub const SCHEMA_VERSION: u32 = 1;

/// Source families that RK exposes as structured records today and can populate
/// with observed facts. Everything else is reported as `unobserved`.
const AVAILABLE_FAMILIES: &[OutcomeEvidenceKind] = &[
    OutcomeEvidenceKind::AgentRecord,
    OutcomeEvidenceKind::WorkflowInstance,
    OutcomeEvidenceKind::Phase4CiSignal,
    OutcomeEvidenceKind::StructuredReviewerRework,
    OutcomeEvidenceKind::StructuredRevert,
    OutcomeEvidenceKind::HumanGateDecision,
    OutcomeEvidenceKind::RecurrenceKey,
];

/// Source families with no structured RK store yet. Reported as unobserved with
/// availability/source counts so unavailable metrics never look healthy.
const UNOBSERVED_FAMILIES: &[OutcomeEvidenceKind] = &[
    OutcomeEvidenceKind::Phase3Contract,
    OutcomeEvidenceKind::Phase3VerifiedDelivery,
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
    pub fn validate(&self) -> Result<(), String> {
        match self.repo.as_deref() {
            Some(repo) if !repo.trim().is_empty() => {}
            _ => return Err("repo is required and must be non-empty".into()),
        }
        if let Some(group_by) = self.group_by.as_deref() {
            match group_by {
                "composite" | "task_class" | "workflow" | "harness" | "model"
                | "task_class_workflow" | "all" => {}
                other => return Err(format!("unsupported group_by {other:?}; expected composite, task_class, workflow, harness, model, task_class_workflow, or all")),
            }
        }
        if let (Some(since), Some(until)) = (self.since, self.until) {
            if since > until {
                return Err("since must be <= until".into());
            }
        }
        Ok(())
    }

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
    pub tickets: Vec<Tuple>,
    pub approval_grants: Vec<ApprovalGrant>,
    pub sdlc_ci_facts: Vec<Tuple>,
    pub revert_facts: Vec<Tuple>,
    pub reviewer_verdicts: Vec<Tuple>,
    pub runtime_unavailable: Vec<OutcomeEvidenceKind>,
    pub read_warnings: Vec<String>,
}

#[cfg(test)]
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
    let agent_of = |agent_id: Option<&str>| -> Option<&AgentRecord> {
        let agent_id = agent_id?.trim();
        (!agent_id.is_empty()).then(|| inputs.agents.iter().find(|agent| agent.name == agent_id))?
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

        // AgentRecord stores final cost but not the pricing snapshot id, so cost
        // remains unavailable rather than fabricating `pricing_evidence_id`.
        // Agent timestamps use an agent generation id, not the workflow instance
        // id, so workflow lead time is normalized only from WorkflowInstance.
    }

    for instance in &inputs.instances {
        let archived = instance.archived_at.is_some();
        let observed_at_ms = instance.completed_at.unwrap_or(instance.started_at).timestamp_millis();
        let payload = if let Some(completed_at) = instance.completed_at {
            FactoryMetricPayload::LeadTime {
                started_at_ms: instance.started_at.timestamp_millis(),
                completed_at_ms: completed_at.timestamp_millis(),
                run_id: instance.id.clone(),
                completed_run_id: instance.id.clone(),
            }
        } else {
            FactoryMetricPayload::Unknown
        };
        structured.push(StructuredOutcomeInput {
            repo: inputs.repo.clone(),
            source_family: OutcomeEvidenceKind::WorkflowInstance,
            source_id: instance.id.clone(),
            source_version: Some(instance.revision.to_string()),
            archived,
            archive_reason: None,
            observed_at_ms,
            task_class: None,
            workflow: Some(instance.workflow.clone()),
            harness: None,
            model: None,
            agent_id: None,
            workflow_instance_id: Some(instance.id.clone()),
            ticket_id: None,
            phase3_outcome_id: None,
            phase4_signal_id: None,
            recurrence_key: None,
            coalesce_key: None,
            payload,
            decoy_prose: String::new(),
        });
    }

    // Revert history is a durable Fact tuple emitted by supervisor.revert.
    // Read only the tuple's typed fields; detail/branch text is deliberately
    // ignored. A malformed matching fact remains an observed unknown event so
    // it cannot become a false successful revert or silently disappear.
    let mut revert_facts = inputs.revert_facts.iter().collect::<Vec<_>>();
    revert_facts.sort_by_key(|fact| (fact.id, fact.identity.clone()));
    for fact in revert_facts {
        if !is_structured_revert_fact(fact) {
            continue;
        }
        let agent_id = structured_string(&fact.payload, "agent")
            .or_else(|| fact.identity.strip_prefix("merge-reverted-").map(str::to_owned));
        let agent = agent_of(agent_id.as_deref());
        let workflow_instance_id = agent.and_then(|agent| agent.workflow_instance.clone());
        let merge_commit = structured_string(&fact.payload, "merge_commit");
        let revert_commit = structured_string(&fact.payload, "revert_commit");
        let payload = if merge_commit.is_some() && revert_commit.is_some() {
            FactoryMetricPayload::Reverted { reverted: true }
        } else {
            FactoryMetricPayload::Unknown
        };
        structured.push(StructuredOutcomeInput {
            repo: inputs.repo.clone(),
            source_family: OutcomeEvidenceKind::StructuredRevert,
            source_id: fact.id.to_string(),
            source_version: revert_commit,
            archived: false,
            archive_reason: None,
            observed_at_ms: fact.created_at.timestamp_millis(),
            task_class: None,
            workflow: workflow_of(&workflow_instance_id),
            harness: agent.map(|agent| agent.harness.clone()),
            model: agent.and_then(|agent| agent.model.clone()),
            agent_id,
            workflow_instance_id,
            ticket_id: ticket_id_from_payload(&fact.payload),
            phase3_outcome_id: None,
            phase4_signal_id: None,
            recurrence_key: None,
            coalesce_key: None,
            payload,
            decoy_prose: String::new(),
        });
    }

    // Reviewer verdicts are durable Artifact tuples. Only an explicit
    // recommendation of REWORK is a rework transition; notes and other
    // reviewer prose are not evidence. The tuple id is the durable source id,
    // keeping multiple verdicts distinct for denominators and source counts.
    let mut reviewer_verdicts = inputs.reviewer_verdicts.iter().collect::<Vec<_>>();
    reviewer_verdicts.sort_by_key(|artifact| (artifact.id, artifact.identity.clone()));
    for artifact in reviewer_verdicts {
        if !is_structured_rework_artifact(artifact) {
            continue;
        }
        let agent_id = structured_string(&artifact.payload, "agent");
        let agent = agent_of(agent_id.as_deref());
        let workflow_instance_id = structured_string(&artifact.payload, "workflow_instance_id")
            .or_else(|| structured_string(&artifact.payload, "run_id"))
            .or_else(|| agent.and_then(|agent| agent.workflow_instance.clone()));
        structured.push(StructuredOutcomeInput {
            repo: inputs.repo.clone(),
            source_family: OutcomeEvidenceKind::StructuredReviewerRework,
            source_id: artifact.id.to_string(),
            source_version: None,
            archived: false,
            archive_reason: None,
            observed_at_ms: artifact.created_at.timestamp_millis(),
            task_class: None,
            workflow: workflow_of(&workflow_instance_id),
            harness: agent.map(|agent| agent.harness.clone()),
            model: agent.and_then(|agent| agent.model.clone()),
            agent_id,
            workflow_instance_id,
            ticket_id: ticket_id_from_payload(&artifact.payload),
            phase3_outcome_id: None,
            phase4_signal_id: None,
            recurrence_key: None,
            coalesce_key: None,
            payload: FactoryMetricPayload::Reworked { requested: true },
            decoy_prose: String::new(),
        });
    }

    for ticket in &inputs.tickets {
        let Some(key) = ticket.payload.get("coalesce_key").and_then(Value::as_str) else { continue; };
        if key.trim().is_empty() { continue; }
        structured.push(StructuredOutcomeInput {
            repo: inputs.repo.clone(), source_family: OutcomeEvidenceKind::RecurrenceKey,
            source_id: ticket.identity.clone(), source_version: None, archived: false,
            archive_reason: None, observed_at_ms: ticket.created_at.timestamp_millis(),
            task_class: None, workflow: None, harness: None, model: None, agent_id: None,
            workflow_instance_id: None, ticket_id: Some(ticket.identity.clone()),
            phase3_outcome_id: None, phase4_signal_id: None,
            recurrence_key: Some(key.trim().to_owned()), coalesce_key: Some(key.trim().to_owned()),
            payload: FactoryMetricPayload::Recurrence, decoy_prose: String::new(),
        });
    }

    for grant in &inputs.approval_grants {
        structured.push(StructuredOutcomeInput {
            repo: inputs.repo.clone(), source_family: OutcomeEvidenceKind::HumanGateDecision,
            source_id: grant.proposal_id.clone(), source_version: Some(grant.digest.clone()), archived: false,
            archive_reason: None, observed_at_ms: grant.approved_at.timestamp_millis(),
            task_class: None, workflow: None, harness: None, model: None,
            agent_id: Some(grant.requester.clone()), workflow_instance_id: grant.instance_id.clone(),
            ticket_id: None, phase3_outcome_id: None, phase4_signal_id: None,
            recurrence_key: None, coalesce_key: None,
            payload: FactoryMetricPayload::HumanIntervention { count: 1 }, decoy_prose: String::new(),
        });
    }

    let mut prior_failed_by_subject_commit = std::collections::BTreeMap::<(String, String), String>::new();
    let mut ci_events = inputs.sdlc_ci_facts.iter().collect::<Vec<_>>();
    ci_events.sort_by_key(|fact| (structured_time_ms(fact, "observed_at"), fact.identity.clone()));
    for fact in ci_events {
        if !is_structured_sdlc_ci_event(fact) {
            continue;
        }
        let source_id = fact
            .payload
            .get("delivery_id")
            .and_then(Value::as_str)
            .filter(|id| !id.trim().is_empty())
            .unwrap_or(&fact.identity)
            .to_owned();
        let Some(kind) = fact
            .payload
            .get("kind")
            .and_then(Value::as_str)
            .map(|kind| kind.to_ascii_lowercase()) else { continue; };
        let failed = kind == "ci_failed";
        let recovered = kind == "ci_recovered";
        if !failed && !recovered {
            continue;
        }
        let subject = fact.payload.get("subject").and_then(Value::as_str).unwrap_or_default();
        let source_version = fact
            .payload
            .pointer("/correlation/commit_sha")
            .and_then(Value::as_str)
            .filter(|sha| !sha.trim().is_empty())
            .map(str::to_owned);
        let prior_failed = source_version.as_ref().and_then(|commit| {
            prior_failed_by_subject_commit.remove(&(subject.to_owned(), commit.clone()))
        });
        structured.push(StructuredOutcomeInput {
            repo: inputs.repo.clone(),
            source_family: OutcomeEvidenceKind::Phase4CiSignal,
            source_id: source_id.clone(),
            source_version: source_version.clone(),
            archived: false,
            archive_reason: None,
            observed_at_ms: structured_time_ms(fact, "observed_at"),
            task_class: None,
            workflow: fact
                .payload
                .get("subject")
                .and_then(Value::as_str)
                .and_then(|subject| subject.split(':').nth(2))
                .filter(|workflow| !workflow.trim().is_empty())
                .map(str::to_owned),
            harness: None,
            model: None,
            agent_id: None,
            workflow_instance_id: Some(subject.to_owned()).filter(|subject| !subject.trim().is_empty()),
            ticket_id: None,
            phase3_outcome_id: None,
            phase4_signal_id: if recovered { prior_failed } else { Some(source_id.clone()) },
            recurrence_key: None,
            coalesce_key: None,
            payload: FactoryMetricPayload::Ci { failed, recovered },
            decoy_prose: String::new(),
        });
        if failed {
            if let Some(commit) = source_version {
                prior_failed_by_subject_commit.insert((subject.to_owned(), commit), source_id);
            }
        }
    }

    let unavailable = UNOBSERVED_FAMILIES
        .iter()
        .chain(inputs.runtime_unavailable.iter())
        .map(|family| OutcomeFactSource::unavailable(*family))
        .collect();

    (structured, unavailable)
}

fn is_structured_sdlc_ci_event(tuple: &Tuple) -> bool {
    let source = tuple.payload.get("source").and_then(Value::as_str).unwrap_or_default();
    let delivery_id = tuple
        .payload
        .get("delivery_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    tuple.identity == format!("sdlc:event:{source}:{delivery_id}")
        && tuple.scope == "ci"
        && tuple.payload.get("family").and_then(Value::as_str) == Some("ci")
        && tuple.payload.get("kind").and_then(Value::as_str).is_some()
}

fn is_structured_revert_fact(tuple: &Tuple) -> bool {
    tuple.category == rk_core::tuple::Category::Fact
        && tuple.identity.starts_with("merge-reverted-")
        && !tuple.identity.trim_start_matches("merge-reverted-").is_empty()
}

fn is_structured_rework_artifact(tuple: &Tuple) -> bool {
    tuple.category == rk_core::tuple::Category::Artifact
        && tuple.identity == "review"
        && tuple
            .payload
            .get("recommendation")
            .and_then(Value::as_str)
            .is_some_and(|recommendation| recommendation.eq_ignore_ascii_case("REWORK"))
}

fn structured_string(payload: &Value, field: &str) -> Option<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn ticket_id_from_payload(payload: &Value) -> Option<String> {
    structured_string(payload, "ticket_id").or_else(|| {
        structured_string(payload, "task").filter(|task| task.starts_with("TKT-"))
    })
}

fn structured_time_ms(tuple: &Tuple, field: &str) -> i64 {
    tuple
        .payload
        .get(field)
        .and_then(Value::as_str)
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp_millis())
        .unwrap_or_else(|| tuple.created_at.timestamp_millis())
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
fn availability_envelope(
    rows: &[FactoryScorecard],
    runtime_unavailable: &[OutcomeEvidenceKind],
    read_warnings: &[String],
) -> (Value, Value, Vec<String>) {
    use rk_core::factory::outcome_facts::SourceCounts;
    use std::collections::BTreeMap;

    let mut counts: BTreeMap<OutcomeEvidenceKind, SourceCounts> = BTreeMap::new();
    let mut available: BTreeMap<OutcomeEvidenceKind, bool> = BTreeMap::new();
    // Structured stores the daemon can read are available regardless of row
    // count unless that read failed at runtime. Families still lacking a durable
    // store stay unobserved until one exists.
    for family in AVAILABLE_FAMILIES {
        counts.entry(*family).or_default();
        available.insert(*family, !runtime_unavailable.contains(family));
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
            if !runtime_unavailable.contains(family) {
                *entry = *entry || avail.available;
            }
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

    let mut warnings = available
        .iter()
        .filter(|(family, avail)| !**avail && !runtime_unavailable.contains(family))
        .map(|(family, _)| {
            format!("source_family_unobserved: {family:?} has no structured RK store; metrics reported as unobserved, not zero")
        })
        .collect::<Vec<_>>();
    warnings.extend(read_warnings.iter().cloned());

    (source_counts, availability, warnings)
}

/// Build the read-only `factory.scorecards` response envelope.
pub fn scorecards_response(
    inputs: &AnalyticsInputs,
    req: &FactoryAnalyticsRequest,
    generated_at: chrono::DateTime<chrono::Utc>,
) -> Value {
    let rows = scorecards(inputs, req);
    let (source_counts, availability, warnings) =
        availability_envelope(&rows, &inputs.runtime_unavailable, &inputs.read_warnings);
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
    let mut report = evaluate_recommendation_report(&rows);
    if let Some(min_sample) = req.min_sample {
        for recommendation in &mut report.recommendations {
            let metric_sample = recommendation
                .evidence
                .denominator
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or(recommendation.sample_size);
            if !recommendation.suppressed && metric_sample < min_sample {
                recommendation.advice = None;
                recommendation.suppressed = true;
                recommendation.suppression_reason = Some(SuppressionReason::LowSample);
                recommendation.thresholds.min_sample_size = min_sample;
                report.suppressions.push(RecommendationSuppression {
                    rule: recommendation.rule,
                    reason: SuppressionReason::LowSample,
                    subject_group_key: recommendation.subject_group_key.clone(),
                    source_family: recommendation.metric_availability.source_family,
                    source_counts: recommendation.source_counts.clone(),
                });
            } else if recommendation.thresholds.min_sample_size < min_sample {
                recommendation.thresholds.min_sample_size = min_sample;
            }
        }
        report.suppressions.sort_by(|l, r| {
            (&l.subject_group_key, &l.rule, &l.reason)
                .cmp(&(&r.subject_group_key, &r.rule, &r.reason))
        });
        report.suppressions.dedup_by(|l, r| {
            l.subject_group_key == r.subject_group_key && l.rule == r.rule && l.reason == r.reason
        });
    }
    let (source_counts, availability, mut warnings) =
        availability_envelope(&rows, &inputs.runtime_unavailable, &inputs.read_warnings);
    warnings.extend(report.warnings.iter().cloned());
    warnings.sort();
    warnings.dedup();
    json!({
        "schema_version": SCHEMA_VERSION,
        "repo": inputs.repo,
        "generated_at": generated_at,
        "group_by": req.projection(),
        "min_sample": req.min_sample,
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
            stderr_tail: None,
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
            schedule: None,
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
            trigger: None,
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
            tickets: Vec::new(),
            approval_grants: Vec::new(),
            sdlc_ci_facts: Vec::new(),
            revert_facts: Vec::new(),
            reviewer_verdicts: Vec::new(),
            runtime_unavailable: Vec::new(),
            read_warnings: Vec::new(),
        }
    }

    fn ci_event(delivery_id: &str, kind: &str, observed_at: i64, summary: &str) -> Tuple {
        let at = Utc.timestamp_opt(observed_at, 0).unwrap();
        let mut tuple = Tuple::new(
            rk_core::tuple::Category::Event,
            "ci",
            format!("sdlc:event:github:{delivery_id}"),
            "source:github",
            json!({
                "source": "github",
                "delivery_id": delivery_id,
                "family": "ci",
                "subject": "rat-kingdom:ci:build",
                "kind": kind,
                "summary": summary,
                "observed_at": at.to_rfc3339(),
                "occurred_at": at.to_rfc3339(),
                "correlation": {"repo": "rat-kingdom", "workflow": "build", "commit_sha": "abc123"},
                "payload": {"type": "ci", "status": "completed", "conclusion": "success"}
            }),
        );
        tuple.created_at = at;
        tuple
    }

    fn revert_fact(identity: &str, complete: bool) -> Tuple {
        let mut tuple = Tuple::new(
            rk_core::tuple::Category::Fact,
            "rat-kingdom",
            identity,
            "castle",
            if complete {
                json!({
                    "agent": "rat-1",
                    "task": "TKT-REVERT",
                    "merge_commit": "merge-abc",
                    "revert_commit": "revert-def",
                    "detail": "not an input"
                })
            } else {
                json!({"agent":"rat-1", "task":"TKT-REVERT", "detail":"missing commits"})
            },
        );
        tuple.created_at = Utc.timestamp_opt(1_040, 0).unwrap();
        tuple
    }

    fn rework_artifact() -> Tuple {
        let mut tuple = Tuple::new(
            rk_core::tuple::Category::Artifact,
            "rat-kingdom",
            "review",
            "castle",
            json!({
                "agent": "rat-1",
                "task": "TKT-REWORK",
                "recommendation": "REWORK",
                "notes": "not an input"
            }),
        );
        tuple.created_at = Utc.timestamp_opt(1_050, 0).unwrap();
        tuple
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
            .filter(|s| s.source_family == OutcomeEvidenceKind::AgentRecord)
            .all(|s| s.workflow.as_deref() == Some("implement-featureset")));
        assert!(structured
            .iter()
            .filter(|s| s.source_family == OutcomeEvidenceKind::AgentRecord)
            .all(|s| s.harness.as_deref() == Some("claude")));
        // task_class is never inferred; stays None -> normalizes to unknown.
        assert!(structured.iter().all(|s| s.task_class.is_none()));
    }

    #[test]
    fn normalizes_revert_facts_and_rework_artifacts_from_structured_fields() {
        let mut in_scope = inputs();
        in_scope.revert_facts = vec![revert_fact("merge-reverted-rat-1", true)];
        in_scope.reviewer_verdicts = vec![rework_artifact()];

        let (structured, unavailable) = normalize_inputs(&in_scope);
        assert!(!unavailable.iter().any(|source| {
            matches!(
                source.kind,
                OutcomeEvidenceKind::StructuredRevert
                    | OutcomeEvidenceKind::StructuredReviewerRework
            )
        }));
        let revert = structured
            .iter()
            .find(|event| event.source_family == OutcomeEvidenceKind::StructuredRevert)
            .unwrap();
        assert!(matches!(revert.payload, FactoryMetricPayload::Reverted { reverted: true }));
        assert_eq!(revert.agent_id.as_deref(), Some("rat-1"));
        assert_eq!(revert.ticket_id.as_deref(), Some("TKT-REVERT"));
        let rework = structured
            .iter()
            .find(|event| event.source_family == OutcomeEvidenceKind::StructuredReviewerRework)
            .unwrap();
        assert!(matches!(rework.payload, FactoryMetricPayload::Reworked { requested: true }));
        assert_eq!(rework.agent_id.as_deref(), Some("rat-1"));
        assert_eq!(rework.ticket_id.as_deref(), Some("TKT-REWORK"));

        let facts = normalize_facts(&in_scope);
        assert!(facts.iter().any(|fact| {
            fact.evidence_kind == OutcomeEvidenceKind::StructuredRevert
                && fact.status == rk_core::factory::outcome_facts::OutcomeStatus::Reverted
        }));
        assert!(facts.iter().any(|fact| {
            fact.evidence_kind == OutcomeEvidenceKind::StructuredReviewerRework
                && fact.status == rk_core::factory::outcome_facts::OutcomeStatus::Reworked
        }));
    }

    #[test]
    fn malformed_revert_is_unknown_and_input_order_does_not_change_output() {
        let mut first = inputs();
        first.revert_facts = vec![
            revert_fact("merge-reverted-rat-1", true),
            revert_fact("merge-reverted-rat-2", false),
        ];
        first.reviewer_verdicts = vec![rework_artifact()];
        let mut second = first.revert_facts.clone();
        second.reverse();
        let mut reordered = first.reviewer_verdicts.clone();
        reordered.reverse();
        let mut second_inputs = inputs();
        second_inputs.revert_facts = second;
        second_inputs.reviewer_verdicts = reordered;

        let facts = normalize_facts(&first);
        assert!(facts.iter().any(|fact| {
            fact.evidence_kind == OutcomeEvidenceKind::StructuredRevert
                && fact.status == rk_core::factory::outcome_facts::OutcomeStatus::Unknown
        }));
        let at = Utc.timestamp_opt(2_000, 0).unwrap();
        assert_eq!(
            scorecards_response(&first, &FactoryAnalyticsRequest::default(), at),
            scorecards_response(&second_inputs, &FactoryAnalyticsRequest::default(), at)
        );
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
    fn ci_history_uses_structured_sdlc_events_not_current_or_prose() {
        let mut in_scope = inputs();
        in_scope.sdlc_ci_facts = vec![
            ci_event("delivery-failed", "ci_failed", 1_040, "failed then later recovered"),
            ci_event("delivery-recovered", "ci_recovered", 1_050, "recovered from prior failure"),
        ];
        in_scope.sdlc_ci_facts.push({
            let mut tuple = ci_event("delivery-current-only", "deployment_succeeded", 1_060, "prose says ci_failed");
            tuple.identity = "sdlc:current:github:rat-kingdom:ci:build".into();
            tuple.payload["current"] = json!({"conclusion":"failure"});
            tuple
        });

        let req = FactoryAnalyticsRequest::default();
        let resp = scorecards_response(&in_scope, &req, Utc.timestamp_opt(2_000, 0).unwrap());
        let metrics = resp["scorecards"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| !row["projected"].as_bool().unwrap_or(false))
            .unwrap()["metrics"]
            .clone();

        assert_eq!(metrics["ci_failed"], json!(1));
        assert_eq!(metrics["ci_recovered"], json!(1));
    }

    #[test]
    fn runtime_read_degradation_marks_family_unavailable_with_warning() {
        let mut degraded = inputs();
        degraded.runtime_unavailable.push(OutcomeEvidenceKind::Phase4CiSignal);
        degraded
            .read_warnings
            .push("source_family_read_failed: Phase4CiSignal unavailable: boom".into());

        let resp = scorecards_response(
            &degraded,
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let ci = resp["availability"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["source_family"] == json!("Phase4CiSignal"))
            .unwrap();

        assert_eq!(ci["available"], json!(false));
        assert!(resp["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w.as_str().unwrap().contains("source_family_read_failed: Phase4CiSignal")));
    }

    #[test]
    fn min_sample_does_not_override_metric_unavailable_suppression() {
        let resp = recommend_response(
            &inputs(),
            &FactoryAnalyticsRequest {
                min_sample: Some(10_000),
                ..Default::default()
            },
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        assert!(resp["recommendations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["metric_availability"]["available"] == json!(false))
            .all(|r| r["suppression_reason"] == json!("metric_unavailable")));
        assert!(!resp["suppressions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["reason"] == json!("low_sample")
                && s["source_family"] == json!("PricingSnapshot")));
    }

    #[test]
    fn scorecards_count_runs_and_marks_cost_unobserved_without_pricing_snapshot() {
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
        // AgentRecord stores final cost but no pricing snapshot id. Do not invent
        // pricing_evidence_id, even for non-zero structured cost fields.
        assert_eq!(total_cost, 0);
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
            structured
                .iter()
                .filter(|s| s.source_family == OutcomeEvidenceKind::AgentRecord)
                .count()
                == 0,
            "in-flight agents contribute no terminal agent facts"
        );
    }
}
