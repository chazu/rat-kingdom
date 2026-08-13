use crate::{Error, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceCriterion {
    pub id: String,
    pub text: String,
    #[serde(default)]
    pub browser_acceptance_applicable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
        )?;
        for criterion in &self.acceptance_criteria {
            require_non_empty("acceptance_criteria.text", &criterion.text)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
        let mut errors = crate::product_to_code::research::research_validation_errors(self);
        if self
            .recommended_ticket_graph_path
            .as_deref()
            .is_some_and(|path| path.trim_ascii().is_empty())
        {
            errors.push("recommended_ticket_graph_path must not be empty".to_string());
        }
        finish(errors)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProducerIdentity {
    pub kind: String,
    pub name: String,
    pub version: Option<String>,
    pub invocation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
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
        for artifact_path in &self.artifact_paths {
            push_empty(&mut errors, "artifact_paths", artifact_path);
        }
        if self.kind == "browser_acceptance" {
            if self.artifact_paths.is_empty() {
                errors.push(
                    "artifact_paths must contain at least one item for browser_acceptance evidence"
                        .to_string(),
                );
            }
            push_payload_string(&mut errors, &self.payload, "url");
            push_payload_string(&mut errors, &self.payload, "scenario");
            push_payload_non_empty_string_array(&mut errors, &self.payload, "steps");
            push_payload_non_empty_string_array(&mut errors, &self.payload, "observations");
        }
        finish(errors)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TicketGraph {
    pub id: String,
    pub initiative_id: String,
    pub nodes: Vec<TicketGraphNode>,
    #[serde(default)]
    pub edges: Vec<TicketGraphEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TicketGraphNode {
    pub id: String,
    pub title: String,
    pub description: String,
    #[serde(default)]
    pub acceptance_criterion_ids: Vec<String>,
    #[serde(default)]
    pub feature_set_ids: Vec<String>,
    #[serde(default)]
    pub browser_acceptance_applicable: bool,
    #[serde(default)]
    pub browser_acceptance_criterion_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TicketGraphEdge {
    pub from: String,
    pub to: String,
    pub relationship: String,
}

impl TicketGraph {
    pub fn validate(&self, acceptance_criterion_ids: &[String]) -> Result<()> {
        let report = self.validation_report(acceptance_criterion_ids);
        finish(report.errors)
    }

    pub fn validation_report(
        &self,
        acceptance_criterion_ids: &[String],
    ) -> TicketGraphValidationReport {
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
        let mut mapped_criteria = HashSet::new();
        for node in &self.nodes {
            push_empty(&mut errors, "nodes.id", &node.id);
            if node.id.starts_with("TKT-") {
                errors.push(format!(
                    "graph node id {} must not be shaped like minted ticket id TKT-*",
                    node.id
                ));
            }
            push_empty(&mut errors, "nodes.title", &node.title);
            push_empty(&mut errors, "nodes.description", &node.description);
            for criterion_id in &node.acceptance_criterion_ids {
                if criterion_id.trim().is_empty() {
                    errors.push("nodes.acceptance_criterion_ids must not be empty".to_string());
                } else if !criteria.contains(criterion_id.as_str()) {
                    errors.push(format!("unknown acceptance criterion {criterion_id}"));
                }
                if !mapped_criteria.insert(criterion_id.as_str()) {
                    errors.push(format!(
                        "duplicate acceptance criterion mapping {criterion_id}"
                    ));
                }
            }
        }
        for criterion_id in criteria {
            if !mapped_criteria.contains(criterion_id) {
                errors.push(format!(
                    "missing acceptance criterion mapping {criterion_id}"
                ));
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
        let (topological_order, cycle_path) = self.topological_order_and_cycle();
        if let Some(path) = &cycle_path {
            errors.push(format!("cycle path {}", path.join(" -> ")));
        }
        TicketGraphValidationReport {
            valid: errors.is_empty(),
            graph_id: self.id.clone(),
            initiative_id: self.initiative_id.clone(),
            errors,
            warnings: Vec::new(),
            topological_order,
            cycle_path,
        }
    }

    pub fn validation_report_for_initiative(
        &self,
        initiative: &InitiativeContract,
    ) -> TicketGraphValidationReport {
        let acceptance_criterion_ids = initiative
            .acceptance_criteria
            .iter()
            .map(|criterion| criterion.id.clone())
            .collect::<Vec<_>>();
        let mut report = self.validation_report(&acceptance_criterion_ids);
        if self.initiative_id != initiative.id {
            report.errors.push(format!(
                "graph initiative_id {} must match initiative id {}",
                self.initiative_id, initiative.id
            ));
            report.valid = false;
        }
        report
    }

    pub fn apply_plan(
        &self,
        repo: &str,
        acceptance_criterion_ids: &[String],
    ) -> Result<TicketGraphApplyPlan> {
        let report = self.validation_report(acceptance_criterion_ids);
        if !report.valid {
            return Err(Error::other(report.errors.join("; ")));
        }
        Ok(TicketGraphApplyPlan::from_graph(
            self,
            repo,
            report.topological_order,
        ))
    }

    pub fn apply_plan_for_initiative(
        &self,
        repo: &str,
        initiative: &InitiativeContract,
    ) -> Result<TicketGraphApplyPlan> {
        let report = self.validation_report_for_initiative(initiative);
        if !report.valid {
            return Err(Error::other(report.errors.join("; ")));
        }
        Ok(TicketGraphApplyPlan::from_graph(
            self,
            repo,
            report.topological_order,
        ))
    }

    fn topological_order_and_cycle(&self) -> (Vec<String>, Option<Vec<String>>) {
        let mut ids: BTreeSet<String> = self.nodes.iter().map(|node| node.id.clone()).collect();
        for edge in &self.edges {
            ids.insert(edge.from.clone());
            ids.insert(edge.to.clone());
        }
        let mut outgoing: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut indegree: BTreeMap<String, usize> = ids.iter().map(|id| (id.clone(), 0)).collect();
        for edge in &self.edges {
            if outgoing
                .entry(edge.from.clone())
                .or_default()
                .insert(edge.to.clone())
            {
                *indegree.entry(edge.to.clone()).or_default() += 1;
            }
        }
        let mut ready: BTreeSet<String> = indegree
            .iter()
            .filter(|(_, degree)| **degree == 0)
            .map(|(id, _)| id.clone())
            .collect();
        let mut order = Vec::new();
        while let Some(id) = ready.pop_first() {
            order.push(id.clone());
            if let Some(nexts) = outgoing.get(&id) {
                for next in nexts {
                    if let Some(degree) = indegree.get_mut(next) {
                        *degree -= 1;
                        if *degree == 0 {
                            ready.insert(next.clone());
                        }
                    }
                }
            }
        }
        if order.len() == ids.len() {
            (order, None)
        } else {
            let cycle_start = ids.iter().find(|id| !order.contains(id)).cloned();
            (order, cycle_start.and_then(|start| self.find_cycle(&start)))
        }
    }

    fn find_cycle(&self, start: &str) -> Option<Vec<String>> {
        let mut outgoing: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for edge in &self.edges {
            outgoing.entry(&edge.from).or_default().insert(&edge.to);
        }
        fn dfs<'a>(
            node: &'a str,
            outgoing: &BTreeMap<&'a str, BTreeSet<&'a str>>,
            stack: &mut Vec<&'a str>,
            seen: &mut HashSet<&'a str>,
        ) -> Option<Vec<String>> {
            if let Some(pos) = stack.iter().position(|existing| *existing == node) {
                let mut cycle = stack[pos..]
                    .iter()
                    .map(|id| (*id).to_string())
                    .collect::<Vec<_>>();
                cycle.push(node.to_string());
                return Some(cycle);
            }
            if !seen.insert(node) {
                return None;
            }
            stack.push(node);
            for next in outgoing.get(node).into_iter().flatten() {
                if let Some(cycle) = dfs(next, outgoing, stack, seen) {
                    return Some(cycle);
                }
            }
            stack.pop();
            None
        }
        dfs(start, &outgoing, &mut Vec::new(), &mut HashSet::new())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TicketGraphApplyPlan {
    pub topological_order: Vec<String>,
    pub creates: Vec<TicketCreateFact>,
    pub updates: Vec<Value>,
    pub dependencies: Vec<TicketDependencyFact>,
    pub dispatches: Vec<Value>,
    pub blocked: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TicketCreateFact {
    pub operation: String,
    pub stable_graph_node_id: String,
    pub repo: String,
    pub title: String,
    pub description: String,
    pub acceptance_criterion_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TicketDependencyFact {
    pub operation: String,
    pub blocked_graph_node_id: String,
    pub dependency_graph_node_id: String,
    pub relationship: String,
}

impl TicketGraphApplyPlan {
    fn from_graph(graph: &TicketGraph, repo: &str, topological_order: Vec<String>) -> Self {
        let creates = topological_order
            .iter()
            .filter_map(|id| graph.nodes.iter().find(|node| &node.id == id))
            .map(|node| TicketCreateFact {
                operation: "ticket.create".to_string(),
                stable_graph_node_id: node.id.clone(),
                repo: repo.to_string(),
                title: node.title.clone(),
                description: node.description.clone(),
                acceptance_criterion_ids: node.acceptance_criterion_ids.clone(),
            })
            .collect();
        let dependencies = graph
            .edges
            .iter()
            .map(|edge| TicketDependencyFact {
                operation: "ticket.dep.add".to_string(),
                blocked_graph_node_id: edge.to.clone(),
                dependency_graph_node_id: edge.from.clone(),
                relationship: edge.relationship.clone(),
            })
            .collect();
        Self {
            topological_order,
            creates,
            updates: Vec::new(),
            dependencies,
            dispatches: Vec::new(),
            blocked: Vec::new(),
        }
    }

    pub fn mutations(&self) -> Vec<Value> {
        self.creates
            .iter()
            .map(|create| serde_json::to_value(create).expect("ticket create fact serializes"))
            .chain(self.dependencies.iter().map(|dependency| {
                serde_json::to_value(dependency).expect("ticket dependency fact serializes")
            }))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TicketGraphValidationReport {
    pub valid: bool,
    pub graph_id: String,
    pub initiative_id: String,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub topological_order: Vec<String>,
    pub cycle_path: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationReport {
    pub id: String,
    pub initiative_id: String,
    pub entries: Vec<AcceptanceCriterionVerification>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceCriterionVerification {
    pub acceptance_criterion_id: String,
    pub status: String,
    #[serde(default)]
    pub evidence_ids: Vec<String>,
    pub notes: Option<String>,
}

impl VerificationReport {
    pub fn validate(&self) -> Result<()> {
        self.validate_inner(None, None)
    }

    pub fn validate_against(
        &self,
        acceptance_criterion_ids: &[String],
        evidence_ids: &[String],
    ) -> Result<()> {
        self.validate_inner(Some(acceptance_criterion_ids), Some(evidence_ids))
    }

    fn validate_inner(
        &self,
        acceptance_criterion_ids: Option<&[String]>,
        evidence_ids: Option<&[String]>,
    ) -> Result<()> {
        let mut errors = Vec::new();
        push_empty(&mut errors, "id", &self.id);
        push_empty(&mut errors, "initiative_id", &self.initiative_id);
        if self.entries.is_empty() {
            errors.push("entries must map at least one acceptance criterion".to_string());
        }
        let criteria = acceptance_criterion_ids
            .map(|ids| ids.iter().map(String::as_str).collect::<HashSet<_>>());
        let known_evidence =
            evidence_ids.map(|ids| ids.iter().map(String::as_str).collect::<HashSet<_>>());
        let mut verified_criteria = HashSet::new();
        for entry in &self.entries {
            push_empty(
                &mut errors,
                "entries.acceptance_criterion_id",
                &entry.acceptance_criterion_id,
            );
            push_empty(&mut errors, "entries.status", &entry.status);
            if let Some(criteria) = &criteria {
                if !criteria.contains(entry.acceptance_criterion_id.as_str()) {
                    errors.push(format!(
                        "unknown acceptance criterion verification {}",
                        entry.acceptance_criterion_id
                    ));
                }
                if !verified_criteria.insert(entry.acceptance_criterion_id.as_str()) {
                    errors.push(format!(
                        "duplicate acceptance criterion verification {}",
                        entry.acceptance_criterion_id
                    ));
                }
            }
            if entry.evidence_ids.is_empty() {
                errors.push(format!(
                    "{} must reference at least one evidence id",
                    entry.acceptance_criterion_id
                ));
            }
            for evidence_id in &entry.evidence_ids {
                push_empty(&mut errors, "entries.evidence_ids", evidence_id);
                if let Some(known_evidence) = &known_evidence {
                    if !known_evidence.contains(evidence_id.as_str()) {
                        errors.push(format!("unknown evidence id {evidence_id}"));
                    }
                }
            }
        }
        if let Some(criteria) = &criteria {
            for criterion_id in criteria {
                if !verified_criteria.contains(criterion_id) {
                    errors.push(format!(
                        "missing acceptance criterion verification {criterion_id}"
                    ));
                }
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

fn push_payload_string(errors: &mut Vec<String>, payload: &Value, field: &str) {
    match payload.get(field).and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => {}
        Some(_) => errors.push(format!("payload.{field} must not be empty")),
        None if payload.get(field).is_some() => {
            errors.push(format!("payload.{field} must be a string"));
        }
        None => errors.push(format!(
            "payload.{field} is required for browser_acceptance evidence"
        )),
    }
}

fn push_payload_non_empty_string_array(errors: &mut Vec<String>, payload: &Value, field: &str) {
    match payload.get(field).and_then(Value::as_array) {
        Some(values) if values.is_empty() => {
            errors.push(format!("payload.{field} must contain at least one item"));
        }
        Some(values) => {
            for value in values {
                match value.as_str() {
                    Some(text) if !text.trim().is_empty() => {}
                    Some(_) => errors.push(format!("payload.{field} items must not be empty")),
                    None => errors.push(format!("payload.{field} items must be strings")),
                }
            }
        }
        None if payload.get(field).is_some() => {
            errors.push(format!("payload.{field} must be an array of strings"));
        }
        None => errors.push(format!(
            "payload.{field} is required for browser_acceptance evidence"
        )),
    }
}

fn finish(errors: Vec<String>) -> Result<()> {
    if errors.is_empty() {
        Ok(())
    } else {
        Err(Error::other(errors.join("; ")))
    }
}
