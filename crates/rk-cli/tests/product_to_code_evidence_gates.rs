#[cfg(test)]
mod product_to_code_evidence_gates {
    use serde_json::Value;
    use std::{
        fs,
        process::{Command, Output},
    };

    // Resolved at runtime, not baked in via `env!("CARGO_MANIFEST_DIR")`: a
    // shared CARGO_TARGET_DIR can serve this binary unrecompiled to a
    // worktree other than the one it was compiled in
    // (TKT-01M0F0GHDPGA24X1TB24A0PZD0). Cargo sets a test binary's cwd to its
    // package's manifest directory on every run, so `current_dir()` is
    // always correct for the current process.
    fn crate_root() -> std::path::PathBuf {
        std::env::current_dir().expect("test process must have a current directory")
    }

    fn fixture(name: &str) -> String {
        format!(
            "{}/tests/fixtures/product_to_code/{name}",
            crate_root().display()
        )
    }

    fn run(args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_rk"))
            .args(args)
            .env_remove("RK_AGENT")
            .env_remove("RK_AUTH_TOKEN")
            .output()
            .unwrap()
    }

    fn json_success(output: Output) -> Value {
        assert!(
            output.status.success(),
            "stderr: {}\nstdout: {}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    fn json_failure(output: Output) -> Value {
        assert!(
            !output.status.success(),
            "stdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }

    #[test]
    fn test_dispatch_gate_accepts_generic_impact_evidence_from_jcode() {
        let value = json_success(run(&[
            "--json",
            "product-to-code",
            "dispatch-gate",
            "--ticket",
            &fixture("ticket_browser_applicable.json"),
            "--evidence-dir",
            &fixture("evidence_jcode"),
        ]));
        assert_eq!(value["valid"], true);
        assert_eq!(value["gate"], "dispatch-gate");
        assert_eq!(
            value["evidence_ids"],
            serde_json::json!(["EV-impact-jcode"])
        );
    }

    #[test]
    fn test_dispatch_gate_accepts_generic_impact_evidence_from_gitnexus_without_dependency() {
        let value = json_success(run(&[
            "--json",
            "product-to-code",
            "dispatch-gate",
            "--ticket",
            &fixture("ticket_browser_applicable.json"),
            "--evidence-dir",
            &fixture("evidence_gitnexus"),
        ]));
        assert_eq!(value["valid"], true);
        let cargo = fs::read_to_string(format!("{}/Cargo.toml", crate_root().display())).unwrap();
        assert!(!cargo.to_ascii_lowercase().contains("gitnexus"));
    }

    #[test]
    fn test_dispatch_gate_rejects_missing_impact_evidence() {
        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "dispatch-gate",
            "--ticket",
            &fixture("ticket_browser_applicable.json"),
            "--evidence-dir",
            &fixture("evidence_no_impact"),
        ]));
        assert_eq!(value["valid"], false);
        assert!(value["errors"].to_string().contains("dispatch gate"));
    }

    #[test]
    fn test_dispatch_gate_rejects_stale_or_wrong_ticket_coverage() {
        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "dispatch-gate",
            "--ticket",
            &fixture("ticket_browser_applicable.json"),
            "--evidence-dir",
            &fixture("evidence_stale_wrong"),
        ]));
        let errors = value["errors"].to_string();
        assert!(errors.contains("does not cover ticket"), "{errors}");
        assert!(errors.contains("stale"), "{errors}");
    }

    #[test]
    fn test_dispatch_gate_rejects_malformed_impact_contract() {
        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "dispatch-gate",
            "--ticket",
            &fixture("ticket_browser_applicable.json"),
            "--evidence-dir",
            &fixture("evidence_malformed_impact"),
        ]));
        let errors = value["errors"].to_string();
        assert!(errors.contains("summary must not be empty"), "{errors}");
    }

    #[test]
    fn test_dispatch_gate_rejects_impact_without_current_artifact_hash() {
        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "dispatch-gate",
            "--ticket",
            &fixture("ticket_browser_applicable.json"),
            "--evidence-dir",
            &fixture("evidence_missing_current_hash"),
        ]));
        let errors = value["errors"].to_string();
        assert!(errors.contains("current_artifact_hash"), "{errors}");
    }

    #[test]
    fn test_delivery_gate_requires_browser_acceptance_when_applicable() {
        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "delivery-gate",
            "--ticket",
            &fixture("ticket_browser_applicable.json"),
            "--verification-report",
            &fixture("verification_browser_missing.json"),
            "--evidence-dir",
            &fixture("evidence_test_review"),
        ]));
        assert!(value["errors"].to_string().contains("browser_acceptance"));
        assert!(value["errors"].to_string().contains("AC-1"));
    }

    #[test]
    fn test_delivery_gate_does_not_require_browser_when_not_applicable() {
        let value = json_success(run(&[
            "--json",
            "product-to-code",
            "delivery-gate",
            "--ticket",
            &fixture("ticket_non_browser.json"),
            "--verification-report",
            &fixture("verification_non_browser.json"),
            "--evidence-dir",
            &fixture("evidence_test_review"),
        ]));
        assert_eq!(value["valid"], true);
        assert_eq!(value["mapped_criteria"], serde_json::json!({}));
    }

    #[test]
    fn test_delivery_gate_rejects_verification_report_for_other_initiative() {
        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "delivery-gate",
            "--ticket",
            &fixture("ticket_non_browser.json"),
            "--verification-report",
            &fixture("verification_non_browser.json"),
            "--evidence-dir",
            &fixture("evidence_test_review"),
            "--initiative",
            &fixture("initiative_other.json"),
        ]));
        let errors = value["errors"].to_string();
        assert!(
            errors.contains("must match initiative id INIT-other"),
            "{errors}"
        );
    }

    #[test]
    fn test_delivery_gate_rejects_browser_evidence_without_observations() {
        let value = json_failure(run(&[
            "--json",
            "product-to-code",
            "delivery-gate",
            "--ticket",
            &fixture("ticket_browser_applicable.json"),
            "--verification-report",
            &fixture("verification_browser_bad.json"),
            "--evidence-dir",
            &fixture("evidence_browser_bad"),
        ]));
        let errors = value["errors"].to_string();
        assert!(errors.contains("scenario"), "{errors}");
        assert!(errors.contains("observations"), "{errors}");
        assert!(errors.contains("artifact paths"), "{errors}");
    }
}
