use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptanceCriterion {
    pub id: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitiativeContract {
    pub id: String,
    pub title: String,
    pub scope: String,
    pub acceptance_criteria: Vec<AcceptanceCriterion>,
    pub browser_acceptance_applicable: bool,
}

impl InitiativeContract {
    pub fn validate(&self) -> Result<()> {
        require_non_empty("id", &self.id)?;
        require_non_empty("title", &self.title)?;
        require_non_empty("scope", &self.scope)?;
        require_non_empty_vec("acceptance_criteria", &self.acceptance_criteria)?;
        ensure_unique(
            self.acceptance_criteria
                .iter()
                .map(|criterion| criterion.id.as_str()),
            "acceptance_criteria.id",
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchitectureResearchArtifact {
    pub id: String,
    pub initiative_id: String,
    pub researched_files: Vec<String>,
    #[serde(default)]
    pub domain_terms: Vec<String>,
    #[serde(default)]
    pub architecture_decisions: Vec<String>,
    #[serde(default)]
    pub constraints: Vec<String>,
    #[serde(default)]
    pub risks: Vec<String>,
    #[serde(default)]
    pub open_questions: Vec<String>,
    #[serde(default)]
    pub open_questions_exhausted: bool,
    pub recommended_ticket_graph_path: Option<String>,
    #[serde(default)]
    pub evidence_ids: Vec<String>,
}

impl ArchitectureResearchArtifact {
    pub fn validate(&self) -> Result<()> {
        let mut errors = Vec::new();
        push_empty(&mut errors, "id", &self.id);
        push_empty(&mut errors, "initiative_id", &self.initiative_id);
        if self.researched_files.is_empty() {
            errors.push("researched_files must contain at least one file".to_string());
        }
        if self.architecture_decisions.is_empty() {
            errors.push("architecture_decisions must contain at least one decision".to_string());
        }
        if self.open_questions.is_empty() && !self.open_questions_exhausted {
            errors.push("open_questions must contain at least one question unless open_questions_exhausted is true".to_string());
        }
        finish(errors)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProducerIdentity {
    pub kind: String,
    pub name: String,
    pub version: Option<String>,
    pub invocation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GenericEvidence {
    pub id: String,
    pub kind: String,
    pub producer: ProducerIdentity,
    pub summary: String,
    #[serde(default)]
    pub artifact_paths: Vec<String>,
    #[serde(default)]
    pub payload: Value,
}

impl GenericEvidence {
    pub fn validate(&self) -> Result<()> {
        let mut errors = Vec::new();
        push_empty(&mut errors, "id", &self.id);
        push_empty(&mut errors, "kind", &self.kind);
        let valid_kinds = [
            "impact",
            "browser_acceptance",
            "test_run",
            "code_review",
            "research_note",
            "workflow_result",
            "manual_observation",
        ];
        if !valid_kinds.contains(&self.kind.as_str()) {
            errors.push(format!("kind must be one of {}", valid_kinds.join(", ")));
        }
        push_empty(&mut errors, "producer.kind", &self.producer.kind);
        push_empty(&mut errors, "producer.name", &self.producer.name);
        push_empty(&mut errors, "summary", &self.summary);
        if self.kind == "browser_acceptance" {
            for field in ["url", "scenario", "steps", "observations"] {
                if self.payload.get(field).is_none() {
                    errors.push(format!(
                        "payload.{field} is required for browser_acceptance evidence"
                    ));
                }
            }
        }
        finish(errors)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketGraph {
    pub id: String,
    pub initiative_id: String,
    pub nodes: Vec<TicketGraphNode>,
    #[serde(default)]
    pub edges: Vec<TicketGraphEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketGraphNode {
    pub id: String,
    pub title: String,
    pub description: String,
    #[serde(default)]
    pub acceptance_criterion_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketGraphEdge {
    pub from: String,
    pub to: String,
    pub relationship: String,
}

impl TicketGraph {
    pub fn validate(&self, acceptance_criterion_ids: &[String]) -> Result<()> {
        let mut errors = Vec::new();
        push_empty(&mut errors, "id", &self.id);
        push_empty(&mut errors, "initiative_id", &self.initiative_id);
        if self.nodes.is_empty() {
            errors.push("nodes must contain at least one ticket".to_string());
        }
        let node_ids: HashSet<&str> = self.nodes.iter().map(|node| node.id.as_str()).collect();
        if node_ids.len() != self.nodes.len() {
            errors.push("nodes.id must be unique".to_string());
        }
        let criteria: HashSet<&str> = acceptance_criterion_ids
            .iter()
            .map(String::as_str)
            .collect();
        for node in &self.nodes {
            push_empty(&mut errors, "nodes.id", &node.id);
            push_empty(&mut errors, "nodes.title", &node.title);
            push_empty(&mut errors, "nodes.description", &node.description);
            for criterion_id in &node.acceptance_criterion_ids {
                if !criteria.contains(criterion_id.as_str()) {
                    errors.push(format!("unknown acceptance criterion {criterion_id}"));
                }
            }
        }
        for edge in &self.edges {
            if !node_ids.contains(edge.from.as_str()) {
                errors.push(format!("edge from references unknown node {}", edge.from));
            }
            if !node_ids.contains(edge.to.as_str()) {
                errors.push(format!("edge to references unknown node {}", edge.to));
            }
            push_empty(&mut errors, "edges.relationship", &edge.relationship);
        }
        finish(errors)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationReport {
    pub id: String,
    pub initiative_id: String,
    pub entries: Vec<AcceptanceCriterionVerification>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptanceCriterionVerification {
    pub acceptance_criterion_id: String,
    pub status: String,
    #[serde(default)]
    pub evidence_ids: Vec<String>,
    pub notes: Option<String>,
}

impl VerificationReport {
    pub fn validate(&self) -> Result<()> {
        let mut errors = Vec::new();
        push_empty(&mut errors, "id", &self.id);
        push_empty(&mut errors, "initiative_id", &self.initiative_id);
        if self.entries.is_empty() {
            errors.push("entries must map at least one acceptance criterion".to_string());
        }
        for entry in &self.entries {
            push_empty(
                &mut errors,
                "entries.acceptance_criterion_id",
                &entry.acceptance_criterion_id,
            );
            push_empty(&mut errors, "entries.status", &entry.status);
            if entry.evidence_ids.is_empty() {
                errors.push(format!(
                    "{} must reference at least one evidence id",
                    entry.acceptance_criterion_id
                ));
            }
        }
        finish(errors)
    }
}

fn require_non_empty(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(Error::other(format!("{field} must not be empty")))
    } else {
        Ok(())
    }
}

fn require_non_empty_vec<T>(field: &str, value: &[T]) -> Result<()> {
    if value.is_empty() {
        Err(Error::other(format!("{field} must not be empty")))
    } else {
        Ok(())
    }
}

fn ensure_unique<'a>(values: impl IntoIterator<Item = &'a str>, field: &str) -> Result<()> {
    let mut seen = HashSet::new();
    for value in values {
        if value.trim().is_empty() {
            return Err(Error::other(format!("{field} must not be empty")));
        }
        if !seen.insert(value) {
            return Err(Error::other(format!("{field} must be unique")));
        }
    }
    Ok(())
}

fn push_empty(errors: &mut Vec<String>, field: &str, value: &str) {
    if value.trim().is_empty() {
        errors.push(format!("{field} must not be empty"));
    }
}

fn finish(errors: Vec<String>) -> Result<()> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(Error::other(errors.join("; ")))
    }
}
