use crate::product_to_code::contracts::{ArchitectureResearchArtifact, InitiativeContract};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchValidationReport {
    pub valid: bool,
    pub artifact_id: String,
    pub initiative_id: String,
    pub errors: Vec<String>,
}

impl ArchitectureResearchArtifact {
    pub fn validate_for_initiative(
        &self,
        initiative: &InitiativeContract,
    ) -> ResearchValidationReport {
        let mut errors = research_validation_errors(self);
        if let Err(err) = initiative.validate() {
            errors.push(format!("initiative contract is invalid: {err}"));
        }
        if self.initiative_id != initiative.id {
            errors.push(format!(
                "artifact initiative_id {} does not match initiative id {}",
                self.initiative_id, initiative.id
            ));
        }
        ResearchValidationReport {
            valid: errors.is_empty(),
            artifact_id: self.id.clone(),
            initiative_id: initiative.id.clone(),
            errors,
        }
    }

    pub fn render_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("# Architecture Research: {}\n\n", self.id));
        out.push_str(&format!("Initiative: `{}`\n\n", self.initiative_id));
        push_section(&mut out, "Researched Files", &self.researched_files);
        push_section(&mut out, "Domain Terms", &self.domain_terms);
        push_section(&mut out, "Decisions", &self.architecture_decisions);
        push_section(&mut out, "Constraints", &self.constraints);
        push_section(&mut out, "Risks", &self.risks);
        if self.open_questions_exhausted && self.open_questions.is_empty() {
            out.push_str("## Open Questions\n\n- None. Open questions exhausted.\n\n");
        } else {
            push_section(&mut out, "Open Questions", &self.open_questions);
        }
        if let Some(path) = &self.recommended_ticket_graph_path {
            out.push_str("## Recommended Ticket Graph\n\n");
            out.push_str(&format!("- `{path}`\n\n"));
        }
        push_section(&mut out, "Evidence", &self.evidence_ids);
        out
    }
}

pub fn research_validation_errors(artifact: &ArchitectureResearchArtifact) -> Vec<String> {
    let mut errors = Vec::new();
    push_empty(&mut errors, "id", &artifact.id);
    push_empty(&mut errors, "initiative_id", &artifact.initiative_id);
    push_non_empty_string_vec(
        &mut errors,
        "researched_files must contain at least one repo file path",
        "researched_files",
        &artifact.researched_files,
    );
    for file in &artifact.researched_files {
        if !is_safe_repo_relative_path(file) {
            errors.push(format!(
                "researched_files must be repo-relative paths, got {file}"
            ));
        }
    }
    if artifact.architecture_decisions.is_empty()
        && artifact.constraints.is_empty()
        && artifact.risks.is_empty()
    {
        errors.push(
            "architecture_decisions, constraints, or risks must contain at least one architecture substance item"
                .to_string(),
        );
    }
    push_non_empty_items(&mut errors, "domain_terms", &artifact.domain_terms);
    push_non_empty_items(
        &mut errors,
        "architecture_decisions",
        &artifact.architecture_decisions,
    );
    push_non_empty_items(&mut errors, "constraints", &artifact.constraints);
    push_non_empty_items(&mut errors, "risks", &artifact.risks);
    if artifact.open_questions.is_empty() && !artifact.open_questions_exhausted {
        errors.push(
            "open_questions must contain at least one question unless open_questions_exhausted is true"
                .to_string(),
        );
    }
    push_non_empty_items(&mut errors, "open_questions", &artifact.open_questions);
    if let Some(path) = &artifact.recommended_ticket_graph_path {
        push_empty(&mut errors, "recommended_ticket_graph_path", path);
        if !path.trim().is_empty() && !is_safe_repo_relative_path(path) {
            errors.push(format!(
                "recommended_ticket_graph_path must be a safe repo-relative path, got {path}"
            ));
        }
    }
    push_non_empty_items(&mut errors, "evidence_ids", &artifact.evidence_ids);
    errors
}

fn is_safe_repo_relative_path(path: &str) -> bool {
    let path = path.trim();
    if path.is_empty() {
        return false;
    }
    let path = Path::new(path);
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_) | Component::CurDir))
}

pub fn validate_or_error(report: &ResearchValidationReport) -> Result<()> {
    if report.valid {
        Ok(())
    } else {
        Err(Error::other(report.errors.join("; ")))
    }
}

fn push_section(out: &mut String, title: &str, items: &[String]) {
    out.push_str(&format!("## {title}\n\n"));
    if items.is_empty() {
        out.push_str("- None\n\n");
    } else {
        for item in items {
            out.push_str(&format!("- {item}\n"));
        }
        out.push('\n');
    }
}

fn push_empty(errors: &mut Vec<String>, field: &str, value: &str) {
    if value.trim().is_empty() {
        errors.push(format!("{field} must not be empty"));
    }
}

fn push_non_empty_string_vec(
    errors: &mut Vec<String>,
    empty_msg: &str,
    field: &str,
    values: &[String],
) {
    if values.is_empty() {
        errors.push(empty_msg.to_string());
    }
    push_non_empty_items(errors, field, values);
}

fn push_non_empty_items(errors: &mut Vec<String>, field: &str, values: &[String]) {
    for value in values {
        if value.trim().is_empty() {
            errors.push(format!("{field} items must not be empty"));
        }
    }
}
