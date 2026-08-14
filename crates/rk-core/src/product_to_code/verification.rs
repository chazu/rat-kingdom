use crate::product_to_code::contracts::{
    GenericEvidence, InitiativeContract, VerificationReport, verification_status,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerificationGapSummary {
    pub acceptance_criterion_id: String,
    pub status: String,
    pub gap: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerificationValidationReport {
    pub schema: &'static str,
    pub valid: bool,
    pub report_id: String,
    pub initiative_id: String,
    pub errors: Vec<String>,
    pub satisfied: Vec<String>,
    pub gaps: Vec<VerificationGapSummary>,
    pub recommendation: String,
}

pub fn validate_report(
    report: &VerificationReport,
    initiative: &InitiativeContract,
    evidence: &[GenericEvidence],
) -> VerificationValidationReport {
    let mut errors = Vec::new();
    if let Err(error) = initiative.validate() {
        errors.push(error.to_string());
    }
    if report.initiative_id != initiative.id {
        errors.push(format!(
            "verification report initiative_id {} must match initiative id {}",
            report.initiative_id, initiative.id
        ));
    }

    let criterion_ids = initiative
        .acceptance_criteria
        .iter()
        .map(|criterion| criterion.id.clone())
        .collect::<Vec<_>>();
    let evidence_ids = evidence
        .iter()
        .map(|item| item.id.clone())
        .collect::<Vec<_>>();
    if let Err(error) = report.validate_against(&criterion_ids, &evidence_ids) {
        errors.push(error.to_string());
    }

    let mut evidence_by_id = BTreeMap::new();
    for item in evidence {
        if evidence_by_id.insert(item.id.as_str(), item).is_some() {
            errors.push(format!("duplicate evidence id {}", item.id));
        }
        if let Err(error) = item.validate() {
            errors.push(format!("evidence {} is invalid: {error}", item.id));
        }
    }

    let browser_criteria = initiative
        .acceptance_criteria
        .iter()
        .filter(|criterion| {
            initiative.browser_acceptance_applicable || criterion.browser_acceptance_applicable
        })
        .map(|criterion| criterion.id.as_str())
        .collect::<BTreeSet<_>>();
    for entry in &report.entries {
        if !matches!(
            verification_status(&entry.status),
            Some("satisfied" | "partially_satisfied")
        ) || !browser_criteria.contains(entry.acceptance_criterion_id.as_str())
        {
            continue;
        }
        let has_browser_evidence = entry.evidence_ids.iter().any(|evidence_id| {
            evidence_by_id.get(evidence_id.as_str()).is_some_and(|item| {
                item.kind == "browser_acceptance"
                    && payload_ids(&item.payload, "acceptance_criterion_ids")
                        .contains(&entry.acceptance_criterion_id)
            })
        });
        if !has_browser_evidence {
            errors.push(format!(
                "browser_acceptance evidence is required for applicable criterion {}",
                entry.acceptance_criterion_id
            ));
        }
    }

    let mut satisfied = report
        .entries
        .iter()
        .filter(|entry| verification_status(&entry.status) == Some("satisfied"))
        .map(|entry| entry.acceptance_criterion_id.clone())
        .collect::<Vec<_>>();
    satisfied.sort();
    let mut gaps = report
        .entries
        .iter()
        .filter_map(|entry| {
            let status = verification_status(&entry.status)?;
            let explicit_gap = entry
                .gap
                .as_deref()
                .filter(|gap| !gap.trim().is_empty())
                .map(str::to_string);
            if explicit_gap.is_none()
                && !matches!(status, "partially_satisfied" | "not_satisfied")
            {
                return None;
            }
            Some(VerificationGapSummary {
                acceptance_criterion_id: entry.acceptance_criterion_id.clone(),
                status: status.to_string(),
                gap: explicit_gap
                    .or_else(|| entry.notes.clone())
                    .unwrap_or_else(|| "unspecified verification gap".to_string()),
            })
        })
        .collect::<Vec<_>>();
    gaps.sort_by(|left, right| {
        left.acceptance_criterion_id
            .cmp(&right.acceptance_criterion_id)
    });
    errors.sort();
    errors.dedup();
    let valid = errors.is_empty();
    let recommendation = report.recommendation.clone().unwrap_or_else(|| {
        if valid && gaps.is_empty() {
            "deliver".to_string()
        } else {
            "hold".to_string()
        }
    });

    VerificationValidationReport {
        schema: "product_to_code.verification_report.v1",
        valid,
        report_id: report.id.clone(),
        initiative_id: report.initiative_id.clone(),
        errors,
        satisfied,
        gaps,
        recommendation,
    }
}

pub fn render_markdown(report: &VerificationReport) -> String {
    let mut entries = report.entries.iter().collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.acceptance_criterion_id
            .cmp(&right.acceptance_criterion_id)
    });
    let mut output = format!("# Independent Verification {}\n\n", report.id);
    if let Some(verifier) = report.verifier.as_deref() {
        output.push_str(&format!("Verifier: {verifier}\n\n"));
    }
    if let Some(scope) = report.scope.as_deref() {
        output.push_str(&format!("Scope: {scope}\n\n"));
    }

    output.push_str("## Satisfied\n\n");
    for entry in entries.iter().filter(|entry| {
        verification_status(&entry.status) == Some("satisfied")
    }) {
        output.push_str(&format!(
            "- {}: evidence {}\n",
            entry.acceptance_criterion_id,
            if entry.evidence_ids.is_empty() {
                "none".to_string()
            } else {
                entry.evidence_ids.join(", ")
            }
        ));
    }

    output.push_str("\n## Gaps\n\n");
    for entry in entries.iter().filter(|entry| {
        matches!(
            verification_status(&entry.status),
            Some("partially_satisfied" | "not_satisfied")
        ) || entry.gap.is_some()
    }) {
        let gap = entry
            .gap
            .as_deref()
            .or(entry.notes.as_deref())
            .unwrap_or("unspecified verification gap");
        output.push_str(&format!("- {}: {gap}\n", entry.acceptance_criterion_id));
    }

    output.push_str("\n## Recommendation\n\n");
    output.push_str(report.recommendation.as_deref().unwrap_or("hold"));
    output.push('\n');
    output
}

fn payload_ids(payload: &Value, field: &str) -> BTreeSet<String> {
    payload
        .get(field)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}
