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
    report.validate().unwrap();

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
fn test_contract_modules_do_not_reference_jcode_browser_or_gitnexus_crates() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let files = [
        root.join("Cargo.toml"),
        root.join("crates/rk-core/Cargo.toml"),
        root.join("crates/rk-core/src/product_to_code/mod.rs"),
        root.join("crates/rk-core/src/product_to_code/contracts.rs"),
    ];
    let forbidden = ["jcode", "browser", "gitnexus", "playwright", "thirtyfour"];

    for file in files {
        if file.file_name().and_then(|name| name.to_str()) == Some("Cargo.toml") {
            let contents = fs::read_to_string(&file).unwrap_or_default().to_lowercase();
            for word in forbidden {
                assert!(
                    !contents.contains(word),
                    "{} must not reference forbidden runtime dependency/import {word}",
                    file.display()
                );
            }
        } else {
            let contents = fs::read_to_string(&file).unwrap_or_default().to_lowercase();
            for line in contents
                .lines()
                .filter(|line| line.trim_start().starts_with("use "))
            {
                for word in forbidden {
                    assert!(
                        !line.contains(word),
                        "{} must not import forbidden runtime dependency {word}",
                        file.display()
                    );
                }
            }
        }
    }
}
