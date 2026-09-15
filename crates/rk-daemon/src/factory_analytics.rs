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

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use serde_json::{json, Value};

use rk_core::action::ApprovalGrant;
use rk_core::factory::outcome_events::{FactoryMetricPayload, StructuredOutcomeInput};
use rk_core::factory::outcome_facts::{
    OutcomeEvidenceKind, OutcomeFact, OutcomeFactBuilder, OutcomeFactSource,
};
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

/// Wire schema version of the additive `native_delivery` section — versioned
/// independently of [`SCHEMA_VERSION`] because it reads a different producer
/// (`landing::mark_processed`'s `landing_processed` markers, not the
/// `OutcomeFact`/scorecard pipeline every other metric here goes through).
pub const NATIVE_DELIVERY_SCHEMA_VERSION: u32 = 1;

/// Wire schema version of the additive `native_recorded_cost` section —
/// versioned independently of [`SCHEMA_VERSION`] and
/// [`NATIVE_DELIVERY_SCHEMA_VERSION`] because it joins two more producers
/// neither of those read: `AgentRecord.cost_usd` (already loaded into
/// `AnalyticsInputs::agents`) and the rework/conflict resubmission markers
/// `landing.rs` writes when a landed correction requeues its original parent
/// (`landing::REWORK_RESUBMISSION_IDENTITY` /
/// `landing::CONFLICT_RESUBMISSION_IDENTITY`).
pub const NATIVE_RECORDED_COST_SCHEMA_VERSION: u32 = 1;

/// The exact `outcome` strings [`crate::landing::LandingPipeline::mark_processed`]
/// writes into a `landing_processed` marker's payload, other than `"landed"`
/// (handled separately as delivery). Any other value is malformed/unrecognized,
/// not a silently-ignored new outcome.
const NATIVE_NON_DELIVERY_OUTCOMES: &[&str] =
    &["gate-held", "no-gate", "rework-filed", "escalated", "empty"];

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
    pub native_delivery: NativeDeliveryInputs,
    pub native_correction_links: NativeCorrectionLinkInputs,
}

/// Bounded raw read of `landing_processed` markers plus the exact facts about
/// that read the `native_delivery` coverage report needs — a bare `Vec<Tuple>`
/// cannot say whether the storage query hit its cap or failed outright, and
/// guessing either would let the section look complete when it is not.
#[derive(Default)]
pub struct NativeDeliveryInputs {
    /// In-window markers, already capped at `limit` by the storage query
    /// itself (`Space::scan_newest_limited`) before this payload was built.
    pub events: Vec<Tuple>,
    /// Raw count returned by the bounded query before the since/until window
    /// filter was applied in Rust — i.e. `events.len()` plus anything the
    /// window excluded, capped at `limit`.
    pub scanned: usize,
    /// The configured bound passed to the storage query.
    pub limit: usize,
    /// `true` when the query returned strictly more than `limit` rows,
    /// meaning older `landing_processed` markers exist beyond this read and
    /// coverage is a strict subset, not the full history.
    pub truncated: bool,
    /// `false` only on a runtime read failure (the pattern above never
    /// reaches storage, or storage errors) — never on "zero markers found",
    /// which is a legitimate empty-but-observed result.
    pub available: bool,
    pub read_warning: Option<String>,
}

/// Bounded raw read of `landing_rework_resubmission` /
/// `landing_conflict_rework_resubmission` markers — the authoritative link
/// from a filed correction ticket back to the original task it corrected
/// (`landing::LandingPipeline`'s doc on those identities). Same truncation/
/// failure bookkeeping as [`NativeDeliveryInputs`] and for the same reason:
/// a bare `Vec<Tuple>` cannot say whether this bounded query is a complete
/// view of the linkage or a partial page.
#[derive(Default)]
pub struct NativeCorrectionLinkInputs {
    pub events: Vec<Tuple>,
    pub scanned: usize,
    pub limit: usize,
    pub truncated: bool,
    pub available: bool,
    pub read_warning: Option<String>,
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
        AgentState::Completed | AgentState::Failed | AgentState::Stopped | AgentState::Dismissed
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
        // Stable per-generation run id (docs/2026-08-17-tkt-c1-generation-identity.md,
        // consumer F4): keyed on the generation join key rather than the raw
        // instant. Keep `name` in the composite even though the id alone is
        // globally unique for a real (minted) spawn — a pre-migration record's
        // id is a *synthetic*, time-only fallback (`SpawnId::synthetic_for`,
        // zero random bits), so two legacy records created in the same
        // millisecond would otherwise collide and silently merge into one run.
        let run_id = format!("{}:{}", agent.name, agent.spawn_id());

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
        let observed_at_ms = instance
            .completed_at
            .unwrap_or(instance.started_at)
            .timestamp_millis();
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
        let agent_id = structured_string(&fact.payload, "agent").or_else(|| {
            fact.identity
                .strip_prefix("merge-reverted-")
                .map(str::to_owned)
        });
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
        let Some(key) = ticket.payload.get("coalesce_key").and_then(Value::as_str) else {
            continue;
        };
        if key.trim().is_empty() {
            continue;
        }
        structured.push(StructuredOutcomeInput {
            repo: inputs.repo.clone(),
            source_family: OutcomeEvidenceKind::RecurrenceKey,
            source_id: ticket.identity.clone(),
            source_version: None,
            archived: false,
            archive_reason: None,
            observed_at_ms: ticket.created_at.timestamp_millis(),
            task_class: None,
            workflow: None,
            harness: None,
            model: None,
            agent_id: None,
            workflow_instance_id: None,
            ticket_id: Some(ticket.identity.clone()),
            phase3_outcome_id: None,
            phase4_signal_id: None,
            recurrence_key: Some(key.trim().to_owned()),
            coalesce_key: Some(key.trim().to_owned()),
            payload: FactoryMetricPayload::Recurrence,
            decoy_prose: String::new(),
        });
    }

    for grant in &inputs.approval_grants {
        structured.push(StructuredOutcomeInput {
            repo: inputs.repo.clone(),
            source_family: OutcomeEvidenceKind::HumanGateDecision,
            source_id: grant.proposal_id.clone(),
            source_version: Some(grant.digest.clone()),
            archived: false,
            archive_reason: None,
            observed_at_ms: grant.approved_at.timestamp_millis(),
            task_class: None,
            workflow: None,
            harness: None,
            model: None,
            agent_id: Some(grant.requester.clone()),
            workflow_instance_id: grant.instance_id.clone(),
            ticket_id: None,
            phase3_outcome_id: None,
            phase4_signal_id: None,
            recurrence_key: None,
            coalesce_key: None,
            payload: FactoryMetricPayload::HumanIntervention { count: 1 },
            decoy_prose: String::new(),
        });
    }

    let mut prior_failed_by_subject_commit =
        std::collections::BTreeMap::<(String, String), String>::new();
    let mut ci_events = inputs.sdlc_ci_facts.iter().collect::<Vec<_>>();
    ci_events.sort_by_key(|fact| {
        (
            structured_time_ms(fact, "observed_at"),
            fact.identity.clone(),
        )
    });
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
            .map(|kind| kind.to_ascii_lowercase())
        else {
            continue;
        };
        let failed = kind == "ci_failed";
        let recovered = kind == "ci_recovered";
        if !failed && !recovered {
            continue;
        }
        let subject = fact
            .payload
            .get("subject")
            .and_then(Value::as_str)
            .unwrap_or_default();
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
            workflow_instance_id: Some(subject.to_owned())
                .filter(|subject| !subject.trim().is_empty()),
            ticket_id: None,
            phase3_outcome_id: None,
            phase4_signal_id: if recovered {
                prior_failed
            } else {
                Some(source_id.clone())
            },
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
    let source = tuple
        .payload
        .get("source")
        .and_then(Value::as_str)
        .unwrap_or_default();
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
        && !tuple
            .identity
            .trim_start_matches("merge-reverted-")
            .is_empty()
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
    structured_string(payload, "ticket_id")
        .or_else(|| structured_string(payload, "task").filter(|task| task.starts_with("TKT-")))
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

/// A `landing_processed` marker's typed fields, once its native provenance,
/// work-key identity and outcome are all known to be present and recognized.
/// Fields the reduction below needs are owned strings — the source set is
/// already bounded by [`NativeDeliveryInputs::limit`], so cloning here is not
/// an unbounded cost.
struct NativeDeliveryRecord {
    branch: String,
    head_sha: String,
    target: String,
    outcome: String,
    task: Option<String>,
    /// The target's tip captured at write time (`landing::LandingPipeline::
    /// mark_processed`), best-effort and `None` when unresolved — never
    /// itself a conflict signal by omission, only when two *present* values
    /// for one landed work key disagree (module doc on the producer: a
    /// non-landed marker's `target_head` can legitimately go stale and get
    /// superseded, but a landed marker's should not vary for the same key).
    target_head: Option<String>,
}

/// `tuple` really is a native daemon-authored `landing_processed` marker, not
/// merely a record whose payload happens to carry matching field names. The
/// storage-side query already filters on category/identity/scope; this is
/// the same defensive re-check this module applies to every other source
/// family (`is_structured_revert_fact`, `is_structured_rework_artifact`) so
/// the pure reducer never trusts an untyped `Vec<Tuple>` on faith alone.
/// `instance == "daemon"` is the one check the storage-side pattern cannot
/// express: only [`crate::landing::LandingPipeline::mark_processed`] ever
/// writes this identity, always under that fixed producer instance.
fn is_native_landing_processed_marker(tuple: &Tuple) -> bool {
    tuple.category == rk_core::tuple::Category::Event
        && tuple.identity == crate::landing::LANDING_PROCESSED_IDENTITY
        && tuple.instance == "daemon"
}

fn parse_native_delivery_record(tuple: &Tuple) -> Option<NativeDeliveryRecord> {
    if !is_native_landing_processed_marker(tuple) {
        return None;
    }
    let get = |field: &str| -> Option<String> {
        tuple
            .payload
            .get(field)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    };
    let branch = get("branch")?;
    let head_sha = get("head_sha")?;
    let target = get("target")?;
    let outcome = get("outcome")?;
    if outcome != "landed" && !NATIVE_NON_DELIVERY_OUTCOMES.contains(&outcome.as_str()) {
        return None;
    }
    Some(NativeDeliveryRecord {
        branch,
        head_sha,
        target,
        outcome,
        task: get("task"),
        target_head: get("target_head"),
    })
}

/// One distinct `(branch, head_sha, target)` work key's reduced disposition.
/// Kept as an enum (rather than folding straight into counters) so the
/// per-group decision is made once, in one place, and is exhaustively
/// testable independent of the JSON shape.
enum NativeDeliveryDisposition<'a> {
    /// A `"landed"` marker exists for this key and its task/target_head
    /// bindings agree (or are absent) — a genuine, cleanly attributed
    /// delivery edge. `task` is `None` for legitimate ad hoc delivery: a
    /// native land with no ticket attached is not an error.
    Delivered { task: Option<&'a str> },
    /// A `"landed"` marker exists, but its own task or target_head bindings
    /// disagree across records for this exact key — contradictory, not
    /// silently attributed to either value and not folded into the plain
    /// `Delivered` count.
    DeliveredConflicting { conflict: &'static str },
    /// No `"landed"` marker for this key inside the returned coverage: every
    /// record observed within `coverage.scanned`/`requested_window` agrees on
    /// the same single non-delivery outcome. This is a claim about the
    /// bounded/windowed read, NOT that the key was never delivered — a
    /// narrower `requested_window` or the storage-side row limit can exclude
    /// an actual later (or earlier) `"landed"` marker for this exact key
    /// (module doc on [`native_delivery_section`]'s `may_hide_delivery`).
    NoDeliveryObserved { outcome: &'a str },
    /// No `"landed"` marker observed, and the non-delivery outcomes recorded
    /// for this key disagree — order-independent by construction (module
    /// doc), so this is reported unknown rather than guessed via ULID or
    /// wall-clock order.
    ConflictingOutcome,
}

/// `(branch, head_sha, target)` — the exact identity fields
/// `landing::LandingPipeline::mark_processed` records, used as the dedup key.
type NativeDeliveryWorkKey = (String, String, String);

/// One parsed marker's `(outcome, task, target_head)`, grouped per
/// [`NativeDeliveryWorkKey`] for [`reduce_native_delivery_group`].
type NativeDeliveryOutcomeRecord = (String, Option<String>, Option<String>);

fn reduce_native_delivery_group(
    records: &[NativeDeliveryOutcomeRecord],
) -> NativeDeliveryDisposition<'_> {
    let outcomes: BTreeSet<&str> = records.iter().map(|(o, _, _)| o.as_str()).collect();
    if outcomes.contains("landed") {
        let landed = || records.iter().filter(|(outcome, _, _)| outcome == "landed");
        let tasks: BTreeSet<&str> = landed().filter_map(|(_, t, _)| t.as_deref()).collect();
        if tasks.len() > 1 {
            return NativeDeliveryDisposition::DeliveredConflicting { conflict: "task" };
        }
        let target_heads: BTreeSet<&str> = landed().filter_map(|(_, _, h)| h.as_deref()).collect();
        if target_heads.len() > 1 {
            return NativeDeliveryDisposition::DeliveredConflicting {
                conflict: "target_head",
            };
        }
        NativeDeliveryDisposition::Delivered {
            task: tasks.into_iter().next(),
        }
    } else if outcomes.len() == 1 {
        NativeDeliveryDisposition::NoDeliveryObserved {
            outcome: outcomes.into_iter().next().expect("len==1"),
        }
    } else {
        NativeDeliveryDisposition::ConflictingOutcome
    }
}

/// Reduce raw `landing_processed` markers into the additive `native_delivery`
/// section: bounded coverage, exact source ids, one delivery-edge count per
/// distinct `(branch, head_sha, target)` work key, a clearly-scoped
/// `no_delivery_observed` bucket, and a separate raw incident tally that a
/// later successful land cannot erase. See [`landing`](crate::landing) module
/// doc for the producer contract this reads.
///
/// Scope: this is the ONLY source family in [`AnalyticsInputs`] read through
/// a bounded, storage-capped query (`Server::factory_analytics_inputs`'s
/// `scan_newest_limited` call). Every other field on [`AnalyticsInputs`]
/// (agents, instances, tickets, revert facts, reviewer verdicts, CI facts) is
/// still an unbounded scan within its repo scope; this slice does not make
/// the rest of `factory.scorecards` bounded, only this new section.
///
/// Two independent accounting axes, deliberately not merged:
///   - Per-work-key summary (`delivered_edges`, `no_delivery_observed`, and
///     the `unknown` conflict buckets): one disposition per distinct key,
///     from [`reduce_native_delivery_group`].
///   - `observed_incidents`: a raw, non-deduplicated tally of every
///     non-`"landed"` marker actually read. A key that failed gates twice
///     before eventually landing still shows two recorded `gate_held`
///     incidents — the eventual delivery does not retroactively erase the
///     failure evidence.
///
/// Dedup within the per-key summary is a SET membership test on outcomes,
/// not a comparison by `Tuple::id` (ULID mint order) or `created_at` (wall
/// clock) — either would risk reordering a delayed writer's records. A
/// `"landed"` marker anywhere in a key's group makes that key delivered
/// regardless of how many other markers exist or in what order they were
/// read, matching this producer's own invariant that a landed outcome is
/// always current (`landing::LandingPipeline::admission_marker` doc). A key
/// whose non-delivery markers disagree without ever landing has no
/// order-independent way to prefer one verdict over another, so it is
/// reported `conflicting_outcome` rather than guessed.
///
/// `no_delivery_observed` is relative to `coverage`, not absolute: a
/// requested `since`/`until` window, or the storage-side row limit, can
/// exclude the one `"landed"` marker for a key while still including an
/// earlier `"gate-held"`/etc. marker for that SAME key — e.g. a candidate
/// gated-held inside the window, then landed after an operator fix, with
/// that later land's timestamp outside `until`. That key would show up here
/// as `no_delivery_observed.gate_held`, which is correct FOR THIS COVERAGE
/// but is not a claim the key was never delivered. `coverage.may_hide_delivery`
/// flags exactly this condition (a window was requested, or the read was
/// truncated) rather than silently resolving it — resolving it would require
/// reading unbounded history, which this bounded slice deliberately does not
/// do (deferred cursor/checkpoint work, ticket "Deferred, still required").
fn native_delivery_section(inputs: &AnalyticsInputs, req: &FactoryAnalyticsRequest) -> Value {
    let cov = &inputs.native_delivery;

    let mut source_ids: Vec<String> = Vec::with_capacity(cov.events.len());
    let mut malformed_ids: Vec<String> = Vec::new();
    let mut groups: BTreeMap<NativeDeliveryWorkKey, Vec<NativeDeliveryOutcomeRecord>> =
        BTreeMap::new();
    let mut observed_incidents: BTreeMap<&'static str, u64> = NATIVE_NON_DELIVERY_OUTCOMES
        .iter()
        .map(|outcome| (*outcome, 0u64))
        .collect();

    for tuple in &cov.events {
        source_ids.push(tuple.id.to_string());
        match parse_native_delivery_record(tuple) {
            Some(record) => {
                if record.outcome != "landed" {
                    // Raw incident tally: independent of which work key this
                    // belongs to and independent of that key's eventual
                    // outcome (doc above) — never gated on `available` since
                    // this loop only runs when the read itself succeeded.
                    *observed_incidents
                        .get_mut(record.outcome.as_str())
                        .expect("checked by parse_native_delivery_record") += 1;
                }
                groups
                    .entry((record.branch, record.head_sha, record.target))
                    .or_default()
                    .push((record.outcome, record.task, record.target_head));
            }
            None => malformed_ids.push(tuple.id.to_string()),
        }
    }
    source_ids.sort();
    malformed_ids.sort();

    let mut delivered_edges: u64 = 0;
    let mut delivered_edges_without_task: u64 = 0;
    let mut delivered_tasks: BTreeSet<String> = BTreeSet::new();
    let mut no_delivery_observed: BTreeMap<&'static str, u64> = NATIVE_NON_DELIVERY_OUTCOMES
        .iter()
        .map(|outcome| (*outcome, 0u64))
        .collect();
    let mut conflicting_task: u64 = 0;
    let mut conflicting_target_head: u64 = 0;
    let mut conflicting_outcome: u64 = 0;

    for records in groups.values() {
        match reduce_native_delivery_group(records) {
            NativeDeliveryDisposition::Delivered { task } => {
                delivered_edges += 1;
                match task {
                    Some(task) => {
                        delivered_tasks.insert(task.to_owned());
                    }
                    None => delivered_edges_without_task += 1,
                }
            }
            NativeDeliveryDisposition::DeliveredConflicting { conflict: "task" } => {
                conflicting_task += 1;
            }
            NativeDeliveryDisposition::DeliveredConflicting { .. } => {
                conflicting_target_head += 1;
            }
            NativeDeliveryDisposition::NoDeliveryObserved { outcome } => {
                *no_delivery_observed
                    .get_mut(outcome)
                    .expect("known outcome") += 1;
            }
            NativeDeliveryDisposition::ConflictingOutcome => {
                conflicting_outcome += 1;
            }
        }
    }

    // A requested window or a truncated read can each exclude the one
    // `"landed"` marker for a key while leaving an earlier/later non-delivery
    // marker for that same key inside coverage (function doc above) — flag
    // that condition explicitly rather than let `no_delivery_observed` read
    // as an absolute claim.
    let may_hide_delivery = req.since.is_some() || req.until.is_some() || cov.truncated;

    let mut warnings: Vec<String> = Vec::new();
    if let Some(warning) = &cov.read_warning {
        warnings.push(warning.clone());
    }
    if cov.truncated {
        warnings.push(format!(
            "native_delivery_coverage_truncated: read capped at {} landing_processed markers ordered by descending tuple id; older-by-id markers beyond this bound are not reflected",
            cov.limit
        ));
    }
    if may_hide_delivery {
        warnings.push(
            "native_delivery_no_delivery_observed_is_coverage_relative: no_delivery_observed \
             counts work keys with no landed marker inside coverage (bounded by the requested \
             since/until window and/or the row limit above) — a work key's landing outside this \
             coverage is not reflected and this is not a claim delivery never happened"
                .to_string(),
        );
    }

    // A failed read renders every derived count `null`, not `0` — a `0` here
    // would read as "observed and confirmed empty," which is exactly the
    // false-healthy-zero this module's own doc (top of file) exists to rule
    // out for every other source family.
    let count = |value: u64| -> Value {
        if cov.available {
            json!(value)
        } else {
            Value::Null
        }
    };
    let opt_count = |value: usize| -> Value {
        if cov.available {
            json!(value)
        } else {
            Value::Null
        }
    };

    json!({
        "schema_version": NATIVE_DELIVERY_SCHEMA_VERSION,
        "source": "landing_processed",
        "available": cov.available,
        "requested_window": {"since": req.since, "until": req.until},
        "coverage": {
            "scanned": opt_count(cov.scanned),
            "in_window": opt_count(cov.events.len()),
            "limit": cov.limit,
            "truncated": if cov.available { json!(cov.truncated) } else { Value::Null },
            // `Space::scan_newest_limited` orders by descending tuple id
            // (ULID mint order), not by persistence/commit sequence or wall
            // clock — naming it plainly rather than "newest_first" so a
            // reader does not assume temporal completeness a delayed writer
            // could violate. The reduction above never relies on this order.
            "order": "id_desc",
            // True when a `since`/`until` window was requested, or the read
            // hit its row limit — either can exclude a real `"landed"`
            // marker for a key while `no_delivery_observed` still counts it
            // (function doc). `false` means this coverage is a complete view
            // of every `landing_processed` marker for this repo.
            "may_hide_delivery": if cov.available { json!(may_hide_delivery) } else { Value::Null },
        },
        "source_ids": if cov.available { json!(source_ids) } else { json!([]) },
        "delivered_edges": count(delivered_edges),
        "delivered_edges_without_task": count(delivered_edges_without_task),
        "delivered_tasks": count(delivered_tasks.len() as u64),
        "no_delivery_observed": {
            "gate_held": count(no_delivery_observed["gate-held"]),
            "no_gate": count(no_delivery_observed["no-gate"]),
            "rework_filed": count(no_delivery_observed["rework-filed"]),
            "escalated": count(no_delivery_observed["escalated"]),
            "empty": count(no_delivery_observed["empty"]),
        },
        "observed_incidents": {
            "gate_held": count(observed_incidents["gate-held"]),
            "no_gate": count(observed_incidents["no-gate"]),
            "rework_filed": count(observed_incidents["rework-filed"]),
            "escalated": count(observed_incidents["escalated"]),
            "empty": count(observed_incidents["empty"]),
        },
        "unknown": {
            "malformed": opt_count(malformed_ids.len()),
            "malformed_source_ids": if cov.available { json!(malformed_ids) } else { json!([]) },
            "conflicting_task": count(conflicting_task),
            "conflicting_target_head": count(conflicting_target_head),
            "conflicting_outcome": count(conflicting_outcome),
        },
        "warnings": warnings,
    })
}

/// `tuple` really is a native rework/conflict resubmission marker written by
/// [`crate::landing::LandingPipeline`], not merely a record with matching
/// field names — mirrors [`is_native_landing_processed_marker`]'s defensive
/// re-check for the same reason. `instance == "daemon"` is, again, the one
/// check the storage-side pattern cannot express.
fn is_native_resubmission_marker(tuple: &Tuple) -> bool {
    tuple.category == rk_core::tuple::Category::Event
        && (tuple.identity == crate::landing::REWORK_RESUBMISSION_IDENTITY
            || tuple.identity == crate::landing::CONFLICT_RESUBMISSION_IDENTITY)
        && tuple.instance == "daemon"
}

/// One resubmission marker's link from a filed correction ticket back to the
/// original task it corrected. Deliberately task-only (not branch/head_sha
/// scoped): the marker's own `head_sha` is the original parent's
/// re-resolved current tip at resubmission time
/// (`landing.rs`'s `repo.rev_parse(original_branch)`), not necessarily the
/// exact head the correction's own delivery landed against, so joining on it
/// would be guessing an equivalence the producer never asserts.
struct NativeCorrectionLink {
    rework_ticket: String,
    original_task: String,
}

fn parse_native_correction_link(tuple: &Tuple) -> Option<NativeCorrectionLink> {
    if !is_native_resubmission_marker(tuple) {
        return None;
    }
    let get = |field: &str| -> Option<String> {
        tuple
            .payload
            .get(field)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    };
    Some(NativeCorrectionLink {
        rework_ticket: get("rework_ticket")?,
        original_task: get("task")?,
    })
}

/// Recomputes, from the same bounded `landing_processed` markers
/// [`native_delivery_section`] reads, the set of distinct delivered work
/// keys attributed to each task. Deliberately NOT extracted out of
/// [`native_delivery_section`] itself — that function's tested JSON shape
/// must stay unchanged — this reruns the identical pure reduction
/// ([`parse_native_delivery_record`], [`reduce_native_delivery_group`]) for
/// the recorded-cost join. A key whose disposition is not a clean
/// `Delivered { task: Some(_) }` (conflicting, ad hoc with no task, or no
/// delivery observed) contributes nothing here: cost is joined by task
/// identity, so an edge with no task or a contradictory one has nothing to
/// join it to.
fn delivered_task_work_keys(
    inputs: &AnalyticsInputs,
) -> BTreeMap<String, BTreeSet<NativeDeliveryWorkKey>> {
    let mut groups: BTreeMap<NativeDeliveryWorkKey, Vec<NativeDeliveryOutcomeRecord>> =
        BTreeMap::new();
    for tuple in &inputs.native_delivery.events {
        if let Some(record) = parse_native_delivery_record(tuple) {
            groups
                .entry((record.branch, record.head_sha, record.target))
                .or_default()
                .push((record.outcome, record.task, record.target_head));
        }
    }
    let mut by_task: BTreeMap<String, BTreeSet<NativeDeliveryWorkKey>> = BTreeMap::new();
    for (key, records) in &groups {
        if let NativeDeliveryDisposition::Delivered { task: Some(task) } =
            reduce_native_delivery_group(records)
        {
            by_task
                .entry(task.to_owned())
                .or_default()
                .insert(key.clone());
        }
    }
    by_task
}

/// One cost bucket (implementation, review, or correction) contributing to a
/// task's recorded cost. `cost_usd_micro` is `None` as soon as any
/// contributing generation's cost is malformed (nonfinite/negative) or the
/// running sum would overflow `u64` — never a partial or best-effort total.
struct NativeCostBucket {
    generation_count: u64,
    generation_ids: Vec<String>,
    cost_usd_micro: Option<u64>,
    malformed_cost_generation_ids: Vec<String>,
    excluded_archived_generations: u64,
}

impl NativeCostBucket {
    fn empty() -> Self {
        NativeCostBucket {
            generation_count: 0,
            generation_ids: Vec::new(),
            cost_usd_micro: Some(0),
            malformed_cost_generation_ids: Vec::new(),
            excluded_archived_generations: 0,
        }
    }

    /// `cost` is `None` for a malformed value; `archived_excluded` is `true`
    /// when this generation is archived and the request did not opt into
    /// `include_archived` — such a generation is counted (it is not hidden
    /// from `generation_count`/`generation_ids`) but contributes nothing to
    /// the cost sum, and its exclusion is surfaced via
    /// `excluded_archived_generations` rather than silently shrinking the
    /// total.
    fn add(&mut self, run_id: String, cost: Option<u64>, archived_excluded: bool) {
        self.generation_count += 1;
        self.generation_ids.push(run_id.clone());
        if archived_excluded {
            self.excluded_archived_generations += 1;
            return;
        }
        self.cost_usd_micro = match (self.cost_usd_micro, cost) {
            (Some(total), Some(c)) => total.checked_add(c),
            _ => None,
        };
        if cost.is_none() {
            self.malformed_cost_generation_ids.push(run_id);
        }
    }

    fn to_json(&self) -> Value {
        // Sorted at emit time, not accumulation time: `generation_ids`/
        // `malformed_cost_generation_ids` are built by iterating
        // `AnalyticsInputs::agents` in whatever order the caller supplied
        // it, and this section's whole point is that its output does not
        // depend on that order (module doc on `native_delivery_section`
        // applies here too).
        let mut generation_ids = self.generation_ids.clone();
        generation_ids.sort();
        let mut malformed_cost_generation_ids = self.malformed_cost_generation_ids.clone();
        malformed_cost_generation_ids.sort();
        json!({
            "generation_count": self.generation_count,
            "generation_ids": generation_ids,
            "cost_usd_micro": match self.cost_usd_micro {
                Some(v) => json!(v),
                None => Value::Null,
            },
            "malformed_cost_generation_ids": malformed_cost_generation_ids,
            "excluded_archived_generations": self.excluded_archived_generations,
        })
    }
}

/// Reduce settled agent generations into the additive `native_recorded_cost`
/// section: per delivered task, the recorded (not settled-bill) ledger cost
/// of its implementation, review, and any authoritatively linked correction
/// generations, plus an `unattributed` bucket so a settled generation's cost
/// never simply disappears because its task was not (yet, or ever) observed
/// delivered in this bounded coverage.
///
/// Reports what `AgentRecord.cost_usd` says NOW for generations linked to a
/// task the bounded `native_delivery` coverage shows delivered. This is not
/// a provider invoice, does not reconstruct a missing pricing snapshot (see
/// `normalize_inputs`'s doc on why cost is left out of the `OutcomeFact`
/// pipeline entirely), and a `Completed`/`Stopped` state is not treated as
/// proof a generation's usage has been finally reconciled. Every
/// contributing generation is counted exactly once, keyed on its stable
/// generation id (`AgentRecord::spawn_id`) — a same-generation respawn's
/// `cost_usd` is already the current cumulative value, never summed across
/// attempts or historical high-water samples.
///
/// Two joins, both authoritative (never title/body prose):
///   - implementation/review: `AgentRecord.task` (settled, non-reviewer
///     generations) and `AgentRecord.review.task` (reviewer generations —
///     `ReviewContext.task` is set from the reviewed candidate's own task at
///     dispatch, `landing.rs`'s `run_review_owned_with_id` call sites).
///   - correction: `landing::REWORK_RESUBMISSION_IDENTITY` /
///     `CONFLICT_RESUBMISSION_IDENTITY` markers linking a filed correction
///     ticket back to the original task it corrected
///     ([`parse_native_correction_link`]). A rework ticket id linked to more
///     than one distinct original task is ambiguous and excluded from every
///     task's correction bucket rather than guessed.
fn native_recorded_cost_section(inputs: &AnalyticsInputs, req: &FactoryAnalyticsRequest) -> Value {
    let available = inputs.native_delivery.available && inputs.native_correction_links.available;

    let by_task = if inputs.native_delivery.available {
        delivered_task_work_keys(inputs)
    } else {
        BTreeMap::new()
    };

    let mut rework_ticket_to_tasks: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut correction_links_by_task: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut malformed_link_ids: Vec<String> = Vec::new();
    if inputs.native_correction_links.available {
        for tuple in &inputs.native_correction_links.events {
            match parse_native_correction_link(tuple) {
                Some(link) => {
                    rework_ticket_to_tasks
                        .entry(link.rework_ticket.clone())
                        .or_default()
                        .insert(link.original_task.clone());
                    correction_links_by_task
                        .entry(link.original_task)
                        .or_default()
                        .insert(link.rework_ticket);
                }
                None => malformed_link_ids.push(tuple.id.to_string()),
            }
        }
    }
    malformed_link_ids.sort();
    // A rework ticket authoritatively linked to more than one distinct
    // original task cannot be attributed without guessing which one it
    // actually corrected — excluded from every task's correction bucket,
    // named here instead.
    let ambiguous_rework_tickets: BTreeSet<String> = rework_ticket_to_tasks
        .iter()
        .filter(|(_, tasks)| tasks.len() > 1)
        .map(|(ticket, _)| ticket.clone())
        .collect();

    let cost_of = |agent: &AgentRecord| -> Option<u64> { usd_to_micro(agent.cost_usd) };
    let run_id_of = |agent: &AgentRecord| -> String { format!("{}:{}", agent.name, agent.spawn_id()) };
    let archived_excluded = |agent: &AgentRecord| -> bool {
        agent.archived_at.is_some() && !req.include_archived
    };

    let mut tasks_json: Vec<Value> = Vec::new();
    let mut totals_generations: u64 = 0;
    let mut totals_cost: Option<u64> = Some(0);
    let mut linked_tasks: BTreeSet<&String> = BTreeSet::new();
    let mut linked_correction_tickets: BTreeSet<String> = BTreeSet::new();

    if available {
        for (task, work_keys) in &by_task {
            linked_tasks.insert(task);
            let confirmed_correction_tickets: BTreeSet<String> = correction_links_by_task
                .get(task)
                .into_iter()
                .flatten()
                .filter(|ticket| !ambiguous_rework_tickets.contains(*ticket))
                .cloned()
                .collect();
            linked_correction_tickets.extend(confirmed_correction_tickets.iter().cloned());

            let mut implementation = NativeCostBucket::empty();
            let mut review = NativeCostBucket::empty();
            let mut correction = NativeCostBucket::empty();

            for agent in &inputs.agents {
                if !is_settled(agent.state) {
                    continue;
                }
                let run_id = run_id_of(agent);
                let cost = cost_of(agent);
                let excluded = archived_excluded(agent);
                match &agent.review {
                    Some(rc) if rc.task == *task => review.add(run_id, cost, excluded),
                    Some(rc) if confirmed_correction_tickets.contains(&rc.task) => {
                        correction.add(run_id, cost, excluded)
                    }
                    Some(_) => {}
                    None => match &agent.task {
                        Some(t) if t == task => implementation.add(run_id, cost, excluded),
                        Some(t) if confirmed_correction_tickets.contains(t) => {
                            correction.add(run_id, cost, excluded)
                        }
                        _ => {}
                    },
                }
            }

            let total_cost = [&implementation, &review, &correction].iter().try_fold(
                0u64,
                |acc, bucket| match bucket.cost_usd_micro {
                    Some(c) => acc.checked_add(c),
                    None => None,
                },
            );
            let excluded_archived_total = implementation.excluded_archived_generations
                + review.excluded_archived_generations
                + correction.excluded_archived_generations;
            let malformed_total = implementation.malformed_cost_generation_ids.len()
                + review.malformed_cost_generation_ids.len()
                + correction.malformed_cost_generation_ids.len();
            let generation_count =
                implementation.generation_count + review.generation_count + correction.generation_count;

            let mut task_warnings: Vec<String> = Vec::new();
            if excluded_archived_total > 0 {
                task_warnings.push(format!(
                    "excluded_archived_generations: {excluded_archived_total} archived generation(s) excluded from recorded cost because include_archived=false; this total is not a complete lifetime cost"
                ));
            }
            if malformed_total > 0 {
                task_warnings.push(format!(
                    "malformed_cost_generations: {malformed_total} generation(s) had a nonfinite/negative recorded cost and were excluded from the sum"
                ));
            }

            totals_generations += generation_count;
            totals_cost = match (totals_cost, total_cost) {
                (Some(t), Some(c)) => t.checked_add(c),
                _ => None,
            };

            tasks_json.push(json!({
                "task": task,
                "delivered_work_keys": work_keys
                    .iter()
                    .map(|(branch, head_sha, target)| json!({
                        "branch": branch,
                        "head_sha": head_sha,
                        "target": target,
                    }))
                    .collect::<Vec<_>>(),
                "implementation": implementation.to_json(),
                "review": review.to_json(),
                "correction": correction.to_json(),
                "linked_correction_tickets": confirmed_correction_tickets,
                "recorded_cost_usd_micro": match total_cost {
                    Some(v) => json!(v),
                    None => Value::Null,
                },
                "coverage_complete": excluded_archived_total == 0 && malformed_total == 0,
                "warnings": task_warnings,
            }));
        }
    }

    // Every settled generation not linked to a delivered task above still
    // has a recorded cost; it must be visible here, not dropped because its
    // task was never (or not yet, within this bounded coverage) observed
    // delivered.
    let mut unattributed = NativeCostBucket::empty();
    if available {
        for agent in &inputs.agents {
            if !is_settled(agent.state) {
                continue;
            }
            let linked = match &agent.review {
                Some(rc) => linked_tasks.contains(&rc.task) || linked_correction_tickets.contains(&rc.task),
                None => agent.task.as_ref().is_some_and(|t| {
                    linked_tasks.contains(t) || linked_correction_tickets.contains(t)
                }),
            };
            if linked {
                continue;
            }
            unattributed.add(run_id_of(agent), cost_of(agent), archived_excluded(agent));
        }
    }

    let mut warnings: Vec<String> = Vec::new();
    if let Some(warning) = &inputs.native_delivery.read_warning {
        warnings.push(warning.clone());
    }
    if let Some(warning) = &inputs.native_correction_links.read_warning {
        warnings.push(warning.clone());
    }
    if inputs.native_delivery.truncated {
        warnings.push(format!(
            "native_recorded_cost_delivery_coverage_truncated: the underlying native_delivery read capped at {} landing_processed markers; a task delivered only by an older marker beyond this bound will not appear here",
            inputs.native_delivery.limit
        ));
    }
    if inputs.native_correction_links.truncated {
        warnings.push(format!(
            "native_recorded_cost_correction_link_coverage_truncated: read capped at {} resubmission markers; an older correction link beyond this bound will not be reflected in any task's correction bucket",
            inputs.native_correction_links.limit
        ));
    }
    if req.since.is_some() || req.until.is_some() {
        warnings.push(
            "native_recorded_cost_window_may_exclude_delivery: a requested since/until window can \
             exclude the landing_processed marker that attributes a task as delivered, so that \
             task's recorded cost would not appear here even though generations for it exist and \
             are counted in `unattributed`"
                .to_string(),
        );
    }
    if !malformed_link_ids.is_empty() {
        warnings.push(format!(
            "malformed_correction_link_markers: {} resubmission marker(s) were missing required fields and excluded from linkage",
            malformed_link_ids.len()
        ));
    }
    if !ambiguous_rework_tickets.is_empty() {
        warnings.push(format!(
            "ambiguous_correction_tickets: {} correction ticket id(s) authoritatively linked to more than one original task were excluded from every task's correction cost",
            ambiguous_rework_tickets.len()
        ));
    }

    json!({
        "schema_version": NATIVE_RECORDED_COST_SCHEMA_VERSION,
        "sources": [
            "agent_record.cost_usd",
            "landing_processed",
            "landing_rework_resubmission",
            "landing_conflict_rework_resubmission",
        ],
        "semantics": "Recorded ledger cost observed now (AgentRecord.cost_usd at read time) for \
            generations authoritatively linked to a task the bounded native_delivery coverage \
            shows delivered. Not a settled provider bill, not a reconstructed price, and a \
            Completed/Stopped generation state is not proof its usage has been finally \
            reconciled.",
        "unit": "usd_micro (1e-6 USD, round-half-away-from-zero; null means unavailable/malformed, never a false-healthy zero)",
        "available": available,
        "requested_window": {"since": req.since, "until": req.until},
        "include_archived": req.include_archived,
        "coverage": {
            "delivery": {
                "available": inputs.native_delivery.available,
                "scanned": inputs.native_delivery.scanned,
                "limit": inputs.native_delivery.limit,
                "truncated": inputs.native_delivery.truncated,
            },
            "correction_links": {
                "available": inputs.native_correction_links.available,
                "scanned": inputs.native_correction_links.scanned,
                "limit": inputs.native_correction_links.limit,
                "truncated": inputs.native_correction_links.truncated,
                "malformed_source_ids": malformed_link_ids,
            },
        },
        "ambiguous_correction_tickets": ambiguous_rework_tickets,
        "tasks": tasks_json,
        "unattributed": if available { unattributed.to_json() } else { Value::Null },
        "totals": {
            "tasks_with_recorded_cost": tasks_json.len(),
            "contributing_generations": if available { json!(totals_generations) } else { Value::Null },
            "recorded_cost_usd_micro": if available {
                match totals_cost {
                    Some(v) => json!(v),
                    None => Value::Null,
                }
            } else {
                Value::Null
            },
        },
        "warnings": warnings,
    })
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
        "native_delivery": native_delivery_section(inputs, req),
        "native_recorded_cost": native_recorded_cost_section(inputs, req),
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
            (&l.subject_group_key, &l.rule, &l.reason).cmp(&(
                &r.subject_group_key,
                &r.rule,
                &r.reason,
            ))
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
            spawn: Some(rk_core::id::SpawnId::new()),
            role: "rat".into(),
            coordination: None,
            harness: harness.into(),
            permission_mode: None,
            model: model.map(str::to_string),
            repo_root: PathBuf::from("/tmp/repo"),
            repo_name: "rat-kingdom".into(),
            task: Some("do work".into()),
            branch: None,
            fork_point: None,
            worktree: None,
            target_branch: "main".into(),
            parent: None,
            workflow_instance: instance.map(str::to_string),
            review: None,
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
            liveness: Default::default(),
            transport_outage: None,
            recovery: None,
            recovery_receipt: None,
            current_attempt: None,
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
            params: HashMap::new(),
            depth: 0,
            started_at: Utc.timestamp_opt(1_000, 0).unwrap(),
            completed_at: Some(Utc.timestamp_opt(1_030, 0).unwrap()),
            archived_at: None,
            trigger: None,
            stale_timeout_secs: None,
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
            native_delivery: NativeDeliveryInputs {
                available: true,
                ..Default::default()
            },
            native_correction_links: NativeCorrectionLinkInputs {
                available: true,
                ..Default::default()
            },
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
        assert!(matches!(
            revert.payload,
            FactoryMetricPayload::Reverted { reverted: true }
        ));
        assert_eq!(revert.agent_id.as_deref(), Some("rat-1"));
        assert_eq!(revert.ticket_id.as_deref(), Some("TKT-REVERT"));
        let rework = structured
            .iter()
            .find(|event| event.source_family == OutcomeEvidenceKind::StructuredReviewerRework)
            .unwrap();
        assert!(matches!(
            rework.payload,
            FactoryMetricPayload::Reworked { requested: true }
        ));
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
        // Same agent generations (and thus the same random `SpawnId`s the
        // native_recorded_cost join keys on) as `first` — this test varies
        // only the order of `revert_facts`/`reviewer_verdicts`, not agent
        // identity.
        second_inputs.agents = first.agents.clone();
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
            ci_event(
                "delivery-failed",
                "ci_failed",
                1_040,
                "failed then later recovered",
            ),
            ci_event(
                "delivery-recovered",
                "ci_recovered",
                1_050,
                "recovered from prior failure",
            ),
        ];
        in_scope.sdlc_ci_facts.push({
            let mut tuple = ci_event(
                "delivery-current-only",
                "deployment_succeeded",
                1_060,
                "prose says ci_failed",
            );
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
        degraded
            .runtime_unavailable
            .push(OutcomeEvidenceKind::Phase4CiSignal);
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
        assert!(resp["warnings"].as_array().unwrap().iter().any(|w| w
            .as_str()
            .unwrap()
            .contains("source_family_read_failed: Phase4CiSignal")));
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
        // Same agent generations as `a` (and thus the same random
        // `SpawnId`s), reversed — this test asserts the output is
        // insensitive to input order, not to agent identity itself.
        b.agents = a.agents.clone();
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
    fn run_id_stays_distinct_for_agents_sharing_a_creation_instant() {
        // Exact spawn ids, not creation timestamps, distinguish simultaneous
        // runs. `inputs()` gives rat-1 and rat-2 the same `created_at`.
        let at = Utc.timestamp_opt(2_000, 0).unwrap();
        let response = scorecards_response(&inputs(), &FactoryAnalyticsRequest::default(), at);
        assert_eq!(
            source_count(&response, "AgentRecord")["active_source_count"],
            json!(2),
            "two same-instant agents must not collapse into one run"
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

    // -- native_delivery (landing_processed) -------------------------------

    #[allow(clippy::too_many_arguments)]
    fn landing_processed_event(
        branch: &str,
        head_sha: &str,
        target: &str,
        task: &str,
        outcome: &str,
        target_head: Option<&str>,
        at_secs: i64,
    ) -> Tuple {
        let mut payload = json!({
            "branch": branch,
            "target": target,
            "head_sha": head_sha,
            "task": task,
            "outcome": outcome,
            "admission_hold": false,
            "admission_recovery": Value::Null,
        });
        payload["target_head"] = match target_head {
            Some(head) => json!(head),
            None => Value::Null,
        };
        let mut tuple = Tuple::new(
            rk_core::tuple::Category::Event,
            "rat-kingdom",
            crate::landing::LANDING_PROCESSED_IDENTITY,
            "daemon",
            payload,
        );
        tuple.created_at = Utc.timestamp_opt(at_secs, 0).unwrap();
        tuple
    }

    fn native_inputs(events: Vec<Tuple>, coverage: NativeDeliveryInputs) -> AnalyticsInputs {
        let mut base = inputs();
        base.native_delivery = NativeDeliveryInputs { events, ..coverage };
        base
    }

    fn observed(scanned: usize, limit: usize, truncated: bool) -> NativeDeliveryInputs {
        NativeDeliveryInputs {
            events: Vec::new(),
            scanned,
            limit,
            truncated,
            available: true,
            read_warning: None,
        }
    }

    #[test]
    fn native_delivery_counts_one_edge_per_work_key_and_attributes_its_task() {
        let events = vec![landing_processed_event(
            "feature",
            "abc123",
            "main",
            "TKT-1",
            "landed",
            Some("merge-abc"),
            1_000,
        )];
        let coverage = observed(1, 10_000, false);
        let resp = scorecards_response(
            &native_inputs(events, coverage),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(nd["available"], json!(true));
        assert_eq!(nd["delivered_edges"], json!(1));
        assert_eq!(nd["delivered_edges_without_task"], json!(0));
        assert_eq!(nd["delivered_tasks"], json!(1));
        assert_eq!(nd["coverage"]["order"], json!("id_desc"));
        assert_eq!(nd["coverage"]["truncated"], json!(false));
    }

    #[test]
    fn native_delivery_ad_hoc_land_with_no_task_is_legitimate_not_unknown() {
        let events = vec![landing_processed_event(
            "feature", "abc123", "main", "", "landed", None, 1_000,
        )];
        let resp = scorecards_response(
            &native_inputs(events, observed(1, 10_000, false)),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(nd["delivered_edges"], json!(1));
        assert_eq!(nd["delivered_edges_without_task"], json!(1));
        assert_eq!(nd["delivered_tasks"], json!(0));
        assert_eq!(nd["unknown"]["conflicting_task"], json!(0));
    }

    #[test]
    fn native_delivery_recovered_key_preserves_prior_gate_held_as_an_incident() {
        // Same work key: one prior gate-held marker, later a landed marker
        // (an operator resubmit after a fix). The key is delivered exactly
        // once, but the earlier gate-failure evidence must not disappear.
        let events = vec![
            landing_processed_event(
                "feature",
                "abc123",
                "main",
                "TKT-1",
                "gate-held",
                None,
                1_000,
            ),
            landing_processed_event(
                "feature",
                "abc123",
                "main",
                "TKT-1",
                "landed",
                Some("merge-abc"),
                2_000,
            ),
        ];
        let resp = scorecards_response(
            &native_inputs(events, observed(2, 10_000, false)),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(3_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(nd["delivered_edges"], json!(1));
        assert_eq!(nd["no_delivery_observed"]["gate_held"], json!(0));
        assert_eq!(
            nd["observed_incidents"]["gate_held"],
            json!(1),
            "the earlier gate-held attempt must remain visible even though this key later landed"
        );
    }

    #[test]
    fn native_delivery_never_landed_key_with_single_agreed_outcome_is_counted_once() {
        let events = vec![landing_processed_event(
            "feature",
            "abc123",
            "main",
            "TKT-1",
            "rework-filed",
            None,
            1_000,
        )];
        let resp = scorecards_response(
            &native_inputs(events, observed(1, 10_000, false)),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(nd["delivered_edges"], json!(0));
        assert_eq!(nd["no_delivery_observed"]["rework_filed"], json!(1));
        assert_eq!(nd["observed_incidents"]["rework_filed"], json!(1));
    }

    #[test]
    fn native_delivery_conflicting_task_on_a_landed_key_is_unknown_not_delivered() {
        // Two "landed" markers for the exact same work key disagreeing on
        // task is a data anomaly (the admission dedup is supposed to
        // prevent it) — must not be silently attributed to either ticket.
        let events = vec![
            landing_processed_event(
                "feature",
                "abc123",
                "main",
                "TKT-1",
                "landed",
                Some("merge-abc"),
                1_000,
            ),
            landing_processed_event(
                "feature",
                "abc123",
                "main",
                "TKT-2",
                "landed",
                Some("merge-abc"),
                1_100,
            ),
        ];
        let resp = scorecards_response(
            &native_inputs(events, observed(2, 10_000, false)),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(
            nd["delivered_edges"],
            json!(0),
            "a task conflict must not be folded into a clean delivered edge"
        );
        assert_eq!(nd["unknown"]["conflicting_task"], json!(1));
    }

    #[test]
    fn native_delivery_conflicting_target_head_on_a_landed_key_is_unknown() {
        let events = vec![
            landing_processed_event(
                "feature",
                "abc123",
                "main",
                "TKT-1",
                "landed",
                Some("merge-abc"),
                1_000,
            ),
            landing_processed_event(
                "feature",
                "abc123",
                "main",
                "TKT-1",
                "landed",
                Some("merge-def"),
                1_100,
            ),
        ];
        let resp = scorecards_response(
            &native_inputs(events, observed(2, 10_000, false)),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(nd["delivered_edges"], json!(0));
        assert_eq!(nd["unknown"]["conflicting_target_head"], json!(1));
    }

    #[test]
    fn native_delivery_never_landed_key_with_disagreeing_outcomes_is_unknown() {
        let events = vec![
            landing_processed_event(
                "feature",
                "abc123",
                "main",
                "TKT-1",
                "gate-held",
                None,
                1_000,
            ),
            landing_processed_event(
                "feature",
                "abc123",
                "main",
                "TKT-1",
                "escalated",
                None,
                1_100,
            ),
        ];
        let resp = scorecards_response(
            &native_inputs(events, observed(2, 10_000, false)),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(nd["delivered_edges"], json!(0));
        assert_eq!(nd["no_delivery_observed"]["gate_held"], json!(0));
        assert_eq!(nd["no_delivery_observed"]["escalated"], json!(0));
        assert_eq!(nd["unknown"]["conflicting_outcome"], json!(1));
    }

    #[test]
    fn native_delivery_rejects_a_record_not_authored_by_the_daemon_producer() {
        let mut forged = landing_processed_event(
            "feature",
            "abc123",
            "main",
            "TKT-1",
            "landed",
            Some("merge-abc"),
            1_000,
        );
        forged.instance = "some-rat".into();
        let resp = scorecards_response(
            &native_inputs(vec![forged], observed(1, 10_000, false)),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(nd["delivered_edges"], json!(0));
        assert_eq!(nd["unknown"]["malformed"], json!(1));
    }

    #[test]
    fn native_delivery_rejects_wrong_category_and_wrong_identity() {
        let mut wrong_category = landing_processed_event(
            "feature",
            "abc123",
            "main",
            "TKT-1",
            "landed",
            Some("merge-abc"),
            1_000,
        );
        wrong_category.category = rk_core::tuple::Category::Fact;
        let mut wrong_identity = landing_processed_event(
            "feature",
            "abc123",
            "main",
            "TKT-1",
            "landed",
            Some("merge-abc"),
            1_100,
        );
        wrong_identity.identity = "not_landing_processed".into();
        let resp = scorecards_response(
            &native_inputs(
                vec![wrong_category, wrong_identity],
                observed(2, 10_000, false),
            ),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(nd["delivered_edges"], json!(0));
        assert_eq!(nd["unknown"]["malformed"], json!(2));
    }

    #[test]
    fn native_delivery_missing_identity_fields_are_malformed_not_dropped_silently() {
        let mut missing_branch = landing_processed_event(
            "feature",
            "abc123",
            "main",
            "TKT-1",
            "landed",
            Some("merge-abc"),
            1_000,
        );
        missing_branch.payload["branch"] = Value::Null;
        let resp = scorecards_response(
            &native_inputs(vec![missing_branch], observed(1, 10_000, false)),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(nd["unknown"]["malformed"], json!(1));
        assert_eq!(
            nd["unknown"]["malformed_source_ids"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn native_delivery_empty_but_observed_dataset_reports_real_zeros_not_null() {
        let resp = scorecards_response(
            &native_inputs(Vec::new(), observed(0, 10_000, false)),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(nd["available"], json!(true));
        assert_eq!(nd["delivered_edges"], json!(0));
        assert_eq!(nd["coverage"]["scanned"], json!(0));
        assert_eq!(nd["coverage"]["in_window"], json!(0));
    }

    #[test]
    fn native_delivery_truncated_read_is_reported_explicitly() {
        let events = vec![landing_processed_event(
            "feature",
            "abc123",
            "main",
            "TKT-1",
            "landed",
            Some("merge-abc"),
            1_000,
        )];
        let resp = scorecards_response(
            &native_inputs(events, observed(10_000, 10_000, true)),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(nd["coverage"]["truncated"], json!(true));
        assert_eq!(
            nd["coverage"]["may_hide_delivery"],
            json!(true),
            "a truncated read can hide a real delivery beyond the row limit"
        );
        assert!(nd["warnings"].as_array().unwrap().iter().any(|w| w
            .as_str()
            .unwrap()
            .contains("native_delivery_coverage_truncated")));
    }

    #[test]
    fn native_delivery_window_excludes_a_later_landed_marker_reports_no_delivery_observed_not_never(
    ) {
        // Simulates the pre-filter `Server::factory_analytics_inputs` applies
        // before this pure function ever sees the data: a requested `until`
        // keeps this key's earlier gate-held marker in coverage but excludes
        // its later landed marker (an operator fix that landed after the
        // window's cutoff). Within THIS coverage the key never shows
        // "landed" — that must render as `no_delivery_observed`, explicitly
        // flagged as coverage-relative, never as an absolute "never landed"
        // claim, and without reading anything beyond this bounded window.
        let events = vec![landing_processed_event(
            "feature",
            "abc123",
            "main",
            "TKT-1",
            "gate-held",
            None,
            1_000,
        )];
        let req = FactoryAnalyticsRequest {
            repo: Some("rat-kingdom".into()),
            until: Some(1_500_000),
            ..Default::default()
        };
        let resp = scorecards_response(
            &native_inputs(events, observed(1, 10_000, false)),
            &req,
            Utc.timestamp_opt(6_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(nd["delivered_edges"], json!(0));
        assert_eq!(
            nd["no_delivery_observed"]["gate_held"],
            json!(1),
            "within this bounded window the key shows no landed marker"
        );
        assert_eq!(nd["requested_window"]["until"], json!(1_500_000));
        assert_eq!(
            nd["coverage"]["may_hide_delivery"],
            json!(true),
            "a requested window can exclude a real later delivery for this same key"
        );
        assert!(nd["warnings"].as_array().unwrap().iter().any(|w| w
            .as_str()
            .unwrap()
            .contains("no_delivery_observed_is_coverage_relative")));
    }

    #[test]
    fn native_delivery_complete_unwindowed_untruncated_read_does_not_hide_delivery() {
        let events = vec![landing_processed_event(
            "feature",
            "abc123",
            "main",
            "TKT-1",
            "landed",
            Some("merge-abc"),
            1_000,
        )];
        let resp = scorecards_response(
            &native_inputs(events, observed(1, 10_000, false)),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        assert_eq!(
            resp["native_delivery"]["coverage"]["may_hide_delivery"],
            json!(false),
            "no requested window and no truncation means this coverage is complete"
        );
    }

    #[test]
    fn native_delivery_failed_read_renders_null_counts_not_a_healthy_zero() {
        let failed = NativeDeliveryInputs {
            events: Vec::new(),
            scanned: 0,
            limit: 10_000,
            truncated: false,
            available: false,
            read_warning: Some(
                "source_family_read_failed: NativeLandingDelivery unavailable: boom".into(),
            ),
        };
        let resp = scorecards_response(
            &native_inputs(Vec::new(), failed),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nd = &resp["native_delivery"];
        assert_eq!(nd["available"], json!(false));
        assert!(nd["delivered_edges"].is_null());
        assert!(nd["coverage"]["scanned"].is_null());
        assert!(nd["no_delivery_observed"]["gate_held"].is_null());
        assert!(nd["observed_incidents"]["gate_held"].is_null());
        assert!(nd["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w.as_str().unwrap().contains("NativeLandingDelivery")));
    }

    #[test]
    fn native_delivery_deterministic_across_input_order() {
        let events = vec![
            landing_processed_event(
                "feature-a",
                "sha-a",
                "main",
                "TKT-1",
                "landed",
                Some("merge-a"),
                1_000,
            ),
            landing_processed_event(
                "feature-b",
                "sha-b",
                "main",
                "TKT-2",
                "gate-held",
                None,
                1_100,
            ),
        ];
        let mut reversed = events.clone();
        reversed.reverse();
        let at = Utc.timestamp_opt(2_000, 0).unwrap();
        let a = scorecards_response(
            &native_inputs(events, observed(2, 10_000, false)),
            &FactoryAnalyticsRequest::default(),
            at,
        );
        let b = scorecards_response(
            &native_inputs(reversed, observed(2, 10_000, false)),
            &FactoryAnalyticsRequest::default(),
            at,
        );
        assert_eq!(a["native_delivery"], b["native_delivery"]);
    }

    // -- native_recorded_cost (agent cost_usd joined to native_delivery) --

    fn implementer(name: &str, task: &str, cost: f64) -> AgentRecord {
        let mut a = agent(name, "claude", Some("sonnet"), None);
        a.task = Some(task.into());
        a.branch = Some("feature".into());
        a.cost_usd = cost;
        a
    }

    fn reviewer_agent(
        name: &str,
        task: &str,
        branch: &str,
        head_sha: &str,
        target: &str,
        cost: f64,
    ) -> AgentRecord {
        let mut a = agent(name, "claude", Some("sonnet"), None);
        a.review = Some(rk_core::review::ReviewContext {
            branch: branch.into(),
            head_sha: head_sha.into(),
            target: target.into(),
            task: task.into(),
            attempt: "attempt-1".into(),
        });
        a.cost_usd = cost;
        a
    }

    fn resubmission_event(rework_ticket: &str, original_task: &str, at_secs: i64) -> Tuple {
        let mut tuple = Tuple::new(
            rk_core::tuple::Category::Event,
            "rat-kingdom",
            crate::landing::REWORK_RESUBMISSION_IDENTITY,
            "daemon",
            json!({
                "dispatch_key": "dk-1",
                "rework_ticket": rework_ticket,
                "rework_branch": "rework-branch",
                "branch": "feature",
                "target": "main",
                "task": original_task,
                "head_sha": "resolved-sha",
                "seq": 1,
                "state": "queued",
            }),
        );
        tuple.created_at = Utc.timestamp_opt(at_secs, 0).unwrap();
        tuple
    }

    fn cost_inputs(
        agents: Vec<AgentRecord>,
        delivery_events: Vec<Tuple>,
        correction_events: Vec<Tuple>,
    ) -> AnalyticsInputs {
        let mut base = inputs();
        base.agents = agents;
        base.native_delivery = NativeDeliveryInputs {
            events: delivery_events,
            available: true,
            ..Default::default()
        };
        base.native_correction_links = NativeCorrectionLinkInputs {
            events: correction_events,
            available: true,
            ..Default::default()
        };
        base
    }

    #[test]
    fn native_recorded_cost_joins_implementation_and_review_by_task() {
        let delivery = vec![landing_processed_event(
            "feature",
            "sha1",
            "main",
            "TKT-1",
            "landed",
            Some("merge-1"),
            1_000,
        )];
        let agents = vec![
            implementer("rat-impl", "TKT-1", 0.10),
            reviewer_agent("rat-rev", "TKT-1", "feature", "sha1", "main", 0.05),
        ];
        let resp = scorecards_response(
            &cost_inputs(agents, delivery, Vec::new()),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nrc = &resp["native_recorded_cost"];
        assert_eq!(nrc["available"], json!(true));
        let tasks = nrc["tasks"].as_array().unwrap();
        assert_eq!(tasks.len(), 1);
        let task = &tasks[0];
        assert_eq!(task["task"], json!("TKT-1"));
        assert_eq!(task["implementation"]["cost_usd_micro"], json!(100_000));
        assert_eq!(task["review"]["cost_usd_micro"], json!(50_000));
        assert_eq!(task["recorded_cost_usd_micro"], json!(150_000));
        assert_eq!(task["coverage_complete"], json!(true));
    }

    #[test]
    fn native_recorded_cost_includes_an_authoritatively_linked_correction_generation() {
        let delivery = vec![landing_processed_event(
            "feature",
            "sha1",
            "main",
            "TKT-1",
            "landed",
            Some("merge-1"),
            1_000,
        )];
        let correction_links = vec![resubmission_event("TKT-2", "TKT-1", 1_100)];
        let agents = vec![
            implementer("rat-impl", "TKT-1", 0.10),
            implementer("rat-fix", "TKT-2", 0.20),
        ];
        let resp = scorecards_response(
            &cost_inputs(agents, delivery, correction_links),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let task = &resp["native_recorded_cost"]["tasks"][0];
        assert_eq!(task["correction"]["cost_usd_micro"], json!(200_000));
        assert_eq!(task["linked_correction_tickets"], json!(["TKT-2"]));
        assert_eq!(task["recorded_cost_usd_micro"], json!(300_000));
    }

    #[test]
    fn native_recorded_cost_excludes_archived_generation_unless_requested() {
        let delivery = vec![landing_processed_event(
            "feature",
            "sha1",
            "main",
            "TKT-1",
            "landed",
            Some("merge-1"),
            1_000,
        )];
        let mut archived_impl = implementer("rat-old", "TKT-1", 0.30);
        archived_impl.archived_at = Some(Utc.timestamp_opt(900, 0).unwrap());
        let agents = vec![implementer("rat-impl", "TKT-1", 0.10), archived_impl];

        let resp = scorecards_response(
            &cost_inputs(agents.clone(), delivery.clone(), Vec::new()),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let task = &resp["native_recorded_cost"]["tasks"][0];
        assert_eq!(task["implementation"]["cost_usd_micro"], json!(100_000));
        assert_eq!(
            task["implementation"]["excluded_archived_generations"],
            json!(1)
        );
        assert_eq!(task["coverage_complete"], json!(false));

        let req = FactoryAnalyticsRequest {
            include_archived: true,
            ..Default::default()
        };
        let resp2 = scorecards_response(
            &cost_inputs(agents, delivery, Vec::new()),
            &req,
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let task2 = &resp2["native_recorded_cost"]["tasks"][0];
        assert_eq!(task2["implementation"]["cost_usd_micro"], json!(400_000));
        assert_eq!(
            task2["implementation"]["excluded_archived_generations"],
            json!(0)
        );
    }

    #[test]
    fn native_recorded_cost_rejects_nonfinite_or_negative_cost_explicitly() {
        let delivery = vec![landing_processed_event(
            "feature",
            "sha1",
            "main",
            "TKT-1",
            "landed",
            Some("merge-1"),
            1_000,
        )];
        let agents = vec![implementer("rat-bad", "TKT-1", f64::NAN)];
        let resp = scorecards_response(
            &cost_inputs(agents, delivery, Vec::new()),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let task = &resp["native_recorded_cost"]["tasks"][0];
        assert!(task["implementation"]["cost_usd_micro"].is_null());
        assert!(task["recorded_cost_usd_micro"].is_null());
        assert_eq!(task["coverage_complete"], json!(false));
        assert_eq!(
            task["implementation"]["malformed_cost_generation_ids"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn native_recorded_cost_ambiguous_correction_ticket_excluded_from_every_task() {
        let delivery = vec![
            landing_processed_event(
                "feature-a",
                "sha-a",
                "main",
                "TKT-1",
                "landed",
                Some("merge-a"),
                1_000,
            ),
            landing_processed_event(
                "feature-b",
                "sha-b",
                "main",
                "TKT-2",
                "landed",
                Some("merge-b"),
                1_050,
            ),
        ];
        // The same rework ticket authoritatively linked to two different
        // originals: neither task may claim it without guessing.
        let correction_links = vec![
            resubmission_event("TKT-9", "TKT-1", 1_100),
            resubmission_event("TKT-9", "TKT-2", 1_150),
        ];
        let agents = vec![implementer("rat-fix", "TKT-9", 0.20)];
        let resp = scorecards_response(
            &cost_inputs(agents, delivery, correction_links),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nrc = &resp["native_recorded_cost"];
        assert_eq!(nrc["ambiguous_correction_tickets"], json!(["TKT-9"]));
        for task in nrc["tasks"].as_array().unwrap() {
            assert_eq!(task["correction"]["generation_count"], json!(0));
        }
        // Not silently dropped: the generation's cost still shows up,
        // just unattributed to either candidate task.
        assert_eq!(nrc["unattributed"]["generation_count"], json!(1));
        assert_eq!(nrc["unattributed"]["cost_usd_micro"], json!(200_000));
    }

    #[test]
    fn native_recorded_cost_unattributed_bucket_keeps_cost_for_a_task_never_observed_delivered() {
        let agents = vec![implementer("rat-orphan", "TKT-404", 0.15)];
        let resp = scorecards_response(
            &cost_inputs(agents, Vec::new(), Vec::new()),
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nrc = &resp["native_recorded_cost"];
        assert!(nrc["tasks"].as_array().unwrap().is_empty());
        assert_eq!(nrc["unattributed"]["generation_count"], json!(1));
        assert_eq!(nrc["unattributed"]["cost_usd_micro"], json!(150_000));
        assert_eq!(nrc["totals"]["recorded_cost_usd_micro"], json!(0));
    }

    #[test]
    fn native_recorded_cost_failed_delivery_read_is_unavailable_not_a_healthy_empty() {
        let mut base = inputs();
        base.native_delivery = NativeDeliveryInputs {
            available: false,
            read_warning: Some(
                "source_family_read_failed: NativeLandingDelivery unavailable: boom".into(),
            ),
            ..Default::default()
        };
        base.native_correction_links = NativeCorrectionLinkInputs {
            available: true,
            ..Default::default()
        };
        let resp = scorecards_response(
            &base,
            &FactoryAnalyticsRequest::default(),
            Utc.timestamp_opt(2_000, 0).unwrap(),
        );
        let nrc = &resp["native_recorded_cost"];
        assert_eq!(nrc["available"], json!(false));
        assert!(nrc["tasks"].as_array().unwrap().is_empty());
        assert!(nrc["unattributed"].is_null());
        assert!(nrc["totals"]["recorded_cost_usd_micro"].is_null());
        assert!(nrc["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w.as_str().unwrap().contains("NativeLandingDelivery")));
    }

    #[test]
    fn native_recorded_cost_deterministic_across_agent_and_link_order() {
        let delivery = vec![landing_processed_event(
            "feature",
            "sha1",
            "main",
            "TKT-1",
            "landed",
            Some("merge-1"),
            1_000,
        )];
        let correction_links = vec![resubmission_event("TKT-2", "TKT-1", 1_100)];
        let agents = vec![
            implementer("rat-impl", "TKT-1", 0.10),
            implementer("rat-fix", "TKT-2", 0.20),
            reviewer_agent("rat-rev", "TKT-1", "feature", "sha1", "main", 0.05),
        ];
        let mut reversed_agents = agents.clone();
        reversed_agents.reverse();
        let mut reversed_links = correction_links.clone();
        reversed_links.reverse();

        let at = Utc.timestamp_opt(2_000, 0).unwrap();
        let a = scorecards_response(
            &cost_inputs(agents, delivery.clone(), correction_links),
            &FactoryAnalyticsRequest::default(),
            at,
        );
        let b = scorecards_response(
            &cost_inputs(reversed_agents, delivery, reversed_links),
            &FactoryAnalyticsRequest::default(),
            at,
        );
        assert_eq!(a["native_recorded_cost"], b["native_recorded_cost"]);
    }
}
