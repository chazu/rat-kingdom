use crate::product_to_code::contracts::{
    GenericEvidence, InitiativeContract, TicketGraphNode, VerificationReport,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize)]
pub struct GateReport {
    pub schema: &'static str,
    pub valid: bool,
    pub gate: &'static str,
    pub errors: Vec<String>,
    pub evidence_ids: Vec<String>,
    pub mapped_criteria: BTreeMap<String, Vec<String>>,
}

pub fn validate_evidence_item(
    evidence: &GenericEvidence,
    initiative: &InitiativeContract,
) -> Vec<String> {
    let mut errors = contract_errors(evidence);
    if evidence.kind == "impact" {
        errors.extend(validate_impact_payload(evidence, None));
    }
    if evidence.kind == "browser_acceptance" {
        errors.extend(validate_browser_payload(evidence));
    }
    if let Some(ids) = payload_array(&evidence.payload, "acceptance_criterion_ids") {
        let known: BTreeSet<_> = initiative
            .acceptance_criteria
            .iter()
            .map(|c| c.id.as_str())
            .collect();
        for id in ids {
            if !known.contains(id.as_str()) {
                errors.push(format!(
                    "evidence {} references unknown acceptance criterion {}",
                    evidence.id, id
                ));
            }
        }
    }
    errors
}

pub fn dispatch_gate(ticket: &TicketGraphNode, evidence: &[GenericEvidence]) -> GateReport {
    let mut errors = Vec::new();
    let mut accepted = Vec::new();
    for item in evidence.iter().filter(|item| item.kind == "impact") {
        let mut item_errors = contract_errors(item);
        item_errors.extend(validate_impact_payload(item, Some(ticket)));
        if item_errors.is_empty() {
            accepted.push(item.id.clone());
        } else {
            errors.extend(item_errors);
        }
    }
    if accepted.is_empty() {
        errors.push(format!(
            "dispatch gate requires current impact evidence covering ticket {} or its feature set",
            ticket.id
        ));
    }
    GateReport {
        schema: "product_to_code.dispatch_gate.v1",
        valid: errors.is_empty(),
        gate: "dispatch-gate",
        errors,
        evidence_ids: accepted,
        mapped_criteria: BTreeMap::new(),
    }
}

pub fn delivery_gate(
    initiative: &InitiativeContract,
    ticket: &TicketGraphNode,
    report: &VerificationReport,
    evidence: &[GenericEvidence],
) -> GateReport {
    let known_evidence: Vec<String> = evidence.iter().map(|item| item.id.clone()).collect();
    let mut errors = Vec::new();
    if report.initiative_id != initiative.id {
        errors.push(format!(
            "verification report initiative_id {} must match initiative id {}",
            report.initiative_id, initiative.id
        ));
    }
    errors.extend(
        report
            .validate_against(&ticket.acceptance_criterion_ids, &known_evidence)
            .err()
            .map(|error| vec![error.to_string()])
            .unwrap_or_default(),
    );

    let mut applicable: BTreeSet<String> = BTreeSet::new();
    if initiative.browser_acceptance_applicable || ticket_browser_applicable(ticket) {
        applicable.extend(ticket.acceptance_criterion_ids.iter().cloned());
    }
    for id in criterion_browser_applicable(ticket) {
        applicable.insert(id);
    }

    let mut mapped = BTreeMap::<String, Vec<String>>::new();
    for item in evidence
        .iter()
        .filter(|item| item.kind == "browser_acceptance")
    {
        let item_errors = validate_browser_payload(item);
        if !item_errors.is_empty() {
            errors.extend(item_errors);
            continue;
        }
        for id in payload_array(&item.payload, "acceptance_criterion_ids")
            .into_iter()
            .flatten()
        {
            if applicable.contains(&id) {
                mapped.entry(id).or_default().push(item.id.clone());
            }
        }
    }
    for id in &applicable {
        if !mapped.contains_key(id) {
            errors.push(format!(
                "delivery gate requires browser_acceptance evidence for applicable criterion {id}"
            ));
        }
    }

    let accepted: Vec<String> = evidence
        .iter()
        .filter(|item| contract_errors(item).is_empty())
        .map(|item| item.id.clone())
        .collect();
    GateReport {
        schema: "product_to_code.delivery_gate.v1",
        valid: errors.is_empty(),
        gate: "delivery-gate",
        errors,
        evidence_ids: accepted,
        mapped_criteria: mapped,
    }
}

fn contract_errors(evidence: &GenericEvidence) -> Vec<String> {
    evidence
        .validate()
        .err()
        .map(|error| vec![error.to_string()])
        .unwrap_or_default()
}

fn validate_impact_payload(
    evidence: &GenericEvidence,
    ticket: Option<&TicketGraphNode>,
) -> Vec<String> {
    let mut errors = Vec::new();
    require_payload_string(&mut errors, evidence, "artifact_hash");
    require_payload_string(&mut errors, evidence, "current_artifact_hash");
    require_payload_string(&mut errors, evidence, "timestamp");
    require_payload_array(&mut errors, evidence, "covers", "ticket_ids");
    require_payload_array(&mut errors, evidence, "covers", "files_or_symbols");
    if let Some(ticket) = ticket {
        let tickets = nested_array(&evidence.payload, "covers", "ticket_ids");
        let feature_sets = nested_array(&evidence.payload, "covers", "feature_set_ids");
        let current_hash = evidence
            .payload
            .get("current_artifact_hash")
            .and_then(Value::as_str);
        let artifact_hash = evidence
            .payload
            .get("artifact_hash")
            .and_then(Value::as_str);
        if !tickets.iter().any(|id| id == &ticket.id)
            && !feature_sets
                .iter()
                .any(|id| ticket_feature_set_ids(ticket).contains(id))
        {
            errors.push(format!(
                "impact evidence {} does not cover ticket {} or its feature set",
                evidence.id, ticket.id
            ));
        }
        if current_hash != artifact_hash {
            errors.push(format!(
                "impact evidence {} artifact_hash is stale for ticket {}",
                evidence.id, ticket.id
            ));
        }
    }
    errors
}

fn validate_browser_payload(evidence: &GenericEvidence) -> Vec<String> {
    let mut errors = Vec::new();
    require_payload_string(&mut errors, evidence, "scenario");
    require_payload_array_top(&mut errors, evidence, "steps");
    require_payload_array_top(&mut errors, evidence, "observations");
    if evidence.artifact_paths.is_empty() {
        errors.push(format!(
            "browser evidence {} requires artifact paths",
            evidence.id
        ));
    }
    errors
}

fn require_payload_string(errors: &mut Vec<String>, evidence: &GenericEvidence, field: &str) {
    if evidence
        .payload
        .get(field)
        .and_then(Value::as_str)
        .is_none_or(|s| s.trim().is_empty())
    {
        errors.push(format!(
            "{} evidence {} payload.{field} must be nonblank",
            evidence.kind, evidence.id
        ));
    }
}

fn require_payload_array(
    errors: &mut Vec<String>,
    evidence: &GenericEvidence,
    parent: &str,
    field: &str,
) {
    if nested_array(&evidence.payload, parent, field).is_empty() {
        errors.push(format!(
            "{} evidence {} payload.{parent}.{field} must contain at least one item",
            evidence.kind, evidence.id
        ));
    }
}

fn require_payload_array_top(errors: &mut Vec<String>, evidence: &GenericEvidence, field: &str) {
    if payload_array(&evidence.payload, field)
        .unwrap_or_default()
        .is_empty()
    {
        errors.push(format!(
            "{} evidence {} payload.{field} must contain at least one item",
            evidence.kind, evidence.id
        ));
    }
}

fn nested_array(payload: &Value, parent: &str, field: &str) -> Vec<String> {
    payload
        .get(parent)
        .and_then(|v| v.get(field))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn payload_array(payload: &Value, field: &str) -> Option<Vec<String>> {
    payload.get(field).and_then(Value::as_array).map(|items| {
        items
            .iter()
            .filter_map(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
            .map(ToOwned::to_owned)
            .collect()
    })
}

fn ticket_feature_set_ids(ticket: &TicketGraphNode) -> Vec<String> {
    ticket_extra_array(ticket, "feature_set_ids")
}

fn ticket_browser_applicable(ticket: &TicketGraphNode) -> bool {
    ticket_extra_bool(ticket, "browser_acceptance_applicable")
}

fn criterion_browser_applicable(ticket: &TicketGraphNode) -> Vec<String> {
    ticket_extra_array(ticket, "browser_acceptance_criterion_ids")
}

fn ticket_extra_bool(ticket: &TicketGraphNode, field: &str) -> bool {
    serde_json::to_value(ticket)
        .ok()
        .and_then(|v| v.get(field).and_then(Value::as_bool))
        .unwrap_or(false)
}

fn ticket_extra_array(ticket: &TicketGraphNode, field: &str) -> Vec<String> {
    serde_json::to_value(ticket)
        .ok()
        .and_then(|v| payload_array(&v, field))
        .unwrap_or_default()
}
