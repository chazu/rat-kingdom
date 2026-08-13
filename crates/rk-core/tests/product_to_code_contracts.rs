use rk_core::product_to_code::contracts::{
    ArchitectureResearchArtifact, GenericEvidence, InitiativeContract, TicketGraph,
    VerificationReport,
};
use std::fs;
use std::path::Path;

fn fixture(name: &str) -> String {
    fs::read_to_string(format!(
        "{}/tests/fixtures/product_to_code/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("fixture exists")
}

#[test]
fn test_initiative_contract_deserializes_minimal_fixture() {
    let initiative: InitiativeContract =
        serde_json::from_str(&fixture("initiative_minimal.json")).unwrap();
    initiative.validate().unwrap();

    let encoded = serde_json::to_string(&initiative).unwrap();
    let decoded: InitiativeContract = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded.id, "INIT-product-to-code");
    assert_eq!(decoded.title, "Product to code contracts");
    assert_eq!(decoded.scope, "offline-contracts");
    assert_eq!(decoded.acceptance_criteria.len(), 2);
    assert!(decoded.browser_acceptance_applicable);
}

#[test]
fn test_architecture_research_artifact_requires_decisions_and_open_questions() {
    let artifact: ArchitectureResearchArtifact = serde_json::from_str(&fixture(
        "architecture_research_invalid_empty_decisions.json",
    ))
    .unwrap();

    let err = artifact.validate().unwrap_err().to_string();

    assert!(err.contains("architecture_decisions"));
    assert!(err.contains("open_questions"));
}

#[test]
fn test_architecture_research_artifact_rejects_whitespace_required_and_optional_lists() {
    let mut artifact: ArchitectureResearchArtifact = serde_json::from_str(&fixture(
        "architecture_research_invalid_empty_decisions.json",
    ))
    .unwrap();
    artifact.researched_files = vec!["   ".to_string()];
    artifact.domain_terms = vec!["   ".to_string()];
    artifact.architecture_decisions = vec!["   ".to_string()];
    artifact.constraints = vec!["   ".to_string()];
    artifact.risks = vec!["   ".to_string()];
    artifact.open_questions = vec!["   ".to_string()];
    artifact.open_questions_exhausted = false;
    artifact.evidence_ids = vec!["   ".to_string()];

    let err = artifact.validate().unwrap_err().to_string();

    assert!(err.contains("researched_files"));
    assert!(err.contains("domain_terms"));
    assert!(err.contains("architecture_decisions"));
    assert!(err.contains("constraints"));
    assert!(err.contains("risks"));
    assert!(err.contains("open_questions"));
    assert!(err.contains("evidence_ids"));
}

#[test]
fn test_architecture_research_artifact_rejects_whitespace_recommended_ticket_graph_path() {
    let mut artifact: ArchitectureResearchArtifact = serde_json::from_str(&fixture(
        "architecture_research_invalid_empty_decisions.json",
    ))
    .unwrap();
    artifact.researched_files = vec!["crates/rk-core/src/product_to_code/contracts.rs".to_string()];
    artifact.architecture_decisions = vec!["Keep contracts offline".to_string()];
    artifact.open_questions = vec!["Should graph ordering be exported?".to_string()];
    artifact.recommended_ticket_graph_path = Some("   ".to_string());

    let err = artifact.validate().unwrap_err().to_string();

    assert!(err.contains("recommended_ticket_graph_path"));
}

#[test]
fn test_generic_evidence_accepts_tool_agnostic_impact_payload() {
    let evidence: GenericEvidence = serde_json::from_str(&fixture("evidence_impact.json")).unwrap();
    evidence.validate().unwrap();

    assert_eq!(evidence.kind, "impact");
    assert_eq!(evidence.producer.kind, "external-tool");
    assert_eq!(
        evidence.payload["affected_files"][0],
        "crates/rk-core/src/lib.rs"
    );
}

#[test]
fn test_browser_acceptance_evidence_is_generic_and_offline() {
    let evidence: GenericEvidence =
        serde_json::from_str(&fixture("evidence_browser_acceptance.json")).unwrap();
    evidence.validate().unwrap();

    assert_eq!(evidence.kind, "browser_acceptance");
    assert_eq!(
        evidence.payload["url"],
        "http://localhost:3000/product-to-code"
    );
    assert_eq!(
        evidence.payload["scenario"],
        "review generated ticket graph"
    );
    assert_eq!(evidence.payload["steps"].as_array().unwrap().len(), 2);
    assert_eq!(
        evidence.payload["observations"].as_array().unwrap().len(),
        1
    );
    assert_eq!(
        evidence.artifact_paths,
        vec!["artifacts/browser/product-to-code.png"]
    );
}

#[test]
fn test_ticket_graph_fixture_preserves_nodes_edges_and_acceptance_links() {
    let graph: TicketGraph = serde_json::from_str(&fixture("ticket_graph_valid.json")).unwrap();
    graph
        .validate(&["AC-1".to_string(), "AC-2".to_string()])
        .unwrap();

    let encoded = serde_json::to_string(&graph).unwrap();
    let decoded: TicketGraph = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded.nodes.len(), 2);
    assert_eq!(decoded.edges.len(), 1);
    assert_eq!(decoded.nodes[0].acceptance_criterion_ids, vec!["AC-1"]);
    assert_eq!(decoded.edges[0].from, "TKT-contracts");
    assert_eq!(decoded.edges[0].to, "TKT-tests");
}

#[test]
fn test_verification_report_maps_each_acceptance_criterion_to_evidence() {
    let report: VerificationReport =
        serde_json::from_str(&fixture("verification_report_valid.json")).unwrap();
    let evidence_ids = [
        "evidence_test_run.json",
        "evidence_impact.json",
        "evidence_review.json",
    ]
    .map(|name| {
        let evidence: GenericEvidence = serde_json::from_str(&fixture(name)).unwrap();
        evidence.validate().unwrap();
        evidence.id
    });
    report
        .validate_against(&["AC-1".to_string(), "AC-2".to_string()], &evidence_ids)
        .unwrap();

    assert_eq!(report.entries.len(), 2);
    assert_eq!(report.entries[0].acceptance_criterion_id, "AC-1");
    assert_eq!(report.entries[0].evidence_ids, vec!["EVID-test-run"]);
    assert_eq!(report.entries[1].acceptance_criterion_id, "AC-2");
    assert_eq!(
        report.entries[1].evidence_ids,
        vec!["EVID-impact", "EVID-review"]
    );
}

#[test]
fn test_initiative_rejects_blank_acceptance_criterion_text() {
    let mut initiative: InitiativeContract =
        serde_json::from_str(&fixture("initiative_minimal.json")).unwrap();
    initiative.acceptance_criteria[0].text = "   ".to_string();

    let err = initiative.validate().unwrap_err().to_string();

    assert!(err.contains("acceptance_criteria.text"));
}

#[test]
fn test_browser_acceptance_evidence_requires_typed_non_empty_payload() {
    let mut evidence: GenericEvidence =
        serde_json::from_str(&fixture("evidence_browser_acceptance.json")).unwrap();
    evidence.artifact_paths.clear();
    evidence.payload = serde_json::json!({
        "url": "",
        "scenario": 42,
        "steps": [],
        "observations": [""]
    });

    let err = evidence.validate().unwrap_err().to_string();

    assert!(err.contains("artifact_paths"));
    assert!(err.contains("payload.url"));
    assert!(err.contains("payload.scenario"));
    assert!(err.contains("payload.steps"));
    assert!(err.contains("payload.observations"));
}

#[test]
fn test_browser_acceptance_evidence_rejects_whitespace_artifact_paths() {
    let mut evidence: GenericEvidence =
        serde_json::from_str(&fixture("evidence_browser_acceptance.json")).unwrap();
    evidence.artifact_paths = vec!["  ".to_string()];

    let err = evidence.validate().unwrap_err().to_string();

    assert!(err.contains("artifact_paths"));
}

#[test]
fn test_browser_acceptance_evidence_accepts_required_artifact_paths() {
    let evidence: GenericEvidence =
        serde_json::from_str(&fixture("evidence_browser_acceptance.json")).unwrap();

    evidence.validate().unwrap();
    assert!(!evidence.artifact_paths.is_empty());
}

#[test]
fn test_ticket_graph_rejects_duplicate_missing_and_unknown_criterion_mappings() {
    let mut graph: TicketGraph = serde_json::from_str(&fixture("ticket_graph_valid.json")).unwrap();
    graph.nodes[0].acceptance_criterion_ids = vec!["AC-1".to_string(), "AC-2".to_string()];
    graph.nodes[1].acceptance_criterion_ids = vec!["AC-2".to_string(), "AC-404".to_string()];

    let err = graph
        .validate(&["AC-1".to_string(), "AC-2".to_string(), "AC-3".to_string()])
        .unwrap_err()
        .to_string();

    assert!(err.contains("duplicate acceptance criterion mapping AC-2"));
    assert!(err.contains("missing acceptance criterion mapping AC-3"));
    assert!(err.contains("unknown acceptance criterion AC-404"));
}

#[test]
fn test_ticket_graph_rejects_whitespace_required_strings_and_list_ids() {
    let mut graph: TicketGraph = serde_json::from_str(&fixture("ticket_graph_valid.json")).unwrap();
    graph.id = "   ".to_string();
    graph.initiative_id = "   ".to_string();
    graph.nodes[0].id = "   ".to_string();
    graph.nodes[0].title = "   ".to_string();
    graph.nodes[0].description = "   ".to_string();
    graph.nodes[0].acceptance_criterion_ids = vec!["   ".to_string()];
    graph.edges[0].relationship = "   ".to_string();

    let err = graph
        .validate(&["AC-1".to_string()])
        .unwrap_err()
        .to_string();

    assert!(err.contains("id"));
    assert!(err.contains("initiative_id"));
    assert!(err.contains("nodes.id"));
    assert!(err.contains("nodes.title"));
    assert!(err.contains("nodes.description"));
    assert!(err.contains("nodes.acceptance_criterion_ids"));
    assert!(err.contains("edges.relationship"));
}

#[test]
fn test_verification_report_rejects_duplicate_missing_and_unknown_evidence_references() {
    let mut report: VerificationReport =
        serde_json::from_str(&fixture("verification_report_valid.json")).unwrap();
    report.entries[0].acceptance_criterion_id = "AC-2".to_string();
    report.entries[0].evidence_ids = vec!["EVID-missing".to_string()];

    let err = report
        .validate_against(
            &["AC-1".to_string(), "AC-2".to_string(), "AC-3".to_string()],
            &["EVID-impact".to_string(), "EVID-review".to_string()],
        )
        .unwrap_err()
        .to_string();

    assert!(err.contains("duplicate acceptance criterion verification AC-2"));
    assert!(err.contains("missing acceptance criterion verification AC-1"));
    assert!(err.contains("missing acceptance criterion verification AC-3"));
    assert!(err.contains("unknown evidence id EVID-missing"));
}

#[test]
fn test_verification_report_rejects_whitespace_required_strings_and_list_ids() {
    let mut report: VerificationReport =
        serde_json::from_str(&fixture("verification_report_valid.json")).unwrap();
    report.id = "   ".to_string();
    report.initiative_id = "   ".to_string();
    report.entries[0].acceptance_criterion_id = "   ".to_string();
    report.entries[0].status = "   ".to_string();
    report.entries[0].evidence_ids = vec!["   ".to_string()];

    let err = report.validate().unwrap_err().to_string();

    assert!(err.contains("id"));
    assert!(err.contains("initiative_id"));
    assert!(err.contains("entries.acceptance_criterion_id"));
    assert!(err.contains("entries.status"));
    assert!(err.contains("entries.evidence_ids"));
}

#[test]
fn test_architecture_research_rejects_whitespace_required_and_optional_strings() {
    let artifact = ArchitectureResearchArtifact {
        id: "ARCH-whitespace".to_string(),
        initiative_id: "INIT-product-to-code".to_string(),
        researched_files: vec!["   ".to_string()],
        domain_terms: vec!["   ".to_string()],
        architecture_decisions: vec!["   ".to_string()],
        constraints: vec!["   ".to_string()],
        risks: vec!["   ".to_string()],
        open_questions: vec!["   ".to_string()],
        open_questions_exhausted: false,
        recommended_ticket_graph_path: Some("   ".to_string()),
        evidence_ids: vec!["   ".to_string()],
    };

    let err = artifact.validate().unwrap_err().to_string();

    assert!(err.contains("researched_files"));
    assert!(err.contains("domain_terms"));
    assert!(err.contains("architecture_decisions"));
    assert!(err.contains("constraints"));
    assert!(err.contains("risks"));
    assert!(err.contains("open_questions"));
    assert!(err.contains("recommended_ticket_graph_path"));
    assert!(err.contains("evidence_ids"));
}

#[test]
fn test_contract_modules_do_not_reference_jcode_browser_or_gitnexus_crates() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = vec![root.join("Cargo.toml")];
    files.extend(
        fs::read_dir(root.join("crates"))
            .expect("crates directory is readable")
            .map(|entry| {
                entry
                    .expect("crate entry is readable")
                    .path()
                    .join("Cargo.toml")
            }),
    );
    files.extend([
        root.join("crates/rk-core/src/product_to_code/mod.rs"),
        root.join("crates/rk-core/src/product_to_code/contracts.rs"),
    ]);
    files.sort();
    files.dedup();

    let forbidden = ["jcode", "browser", "gitnexus", "playwright", "thirtyfour"];

    for file in files {
        let contents = fs::read_to_string(&file)
            .unwrap_or_else(|err| panic!("{} must be readable: {err}", file.display()))
            .to_lowercase();
        let lines: Box<dyn Iterator<Item = &str>> =
            if file.file_name().and_then(|name| name.to_str()) == Some("Cargo.toml") {
                Box::new(contents.lines())
            } else {
                Box::new(
                    contents
                        .lines()
                        .filter(|line| line.trim_start().starts_with("use ")),
                )
            };
        for line in lines {
            for word in forbidden {
                assert!(
                    !line.contains(word),
                    "{} must not reference forbidden runtime dependency/import {word}",
                    file.display()
                );
            }
        }
    }
}
