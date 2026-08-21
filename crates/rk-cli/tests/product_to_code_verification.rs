#[cfg(test)]
mod product_to_code_verification {
    use serde_json::Value;
    use std::process::Command;

    // Resolved at runtime, not baked in via `env!("CARGO_MANIFEST_DIR")`: a
    // shared CARGO_TARGET_DIR can serve this binary unrecompiled to a
    // worktree other than the one it was compiled in
    // (TKT-01M0F0GHDPGA24X1TB24A0PZD0). Cargo sets a test binary's cwd to its
    // package's manifest directory on every run, so `current_dir()` is
    // always correct for the current process.
    fn crate_root() -> std::path::PathBuf {
        std::env::current_dir().expect("test process must have a current directory")
    }

    fn workspace_root() -> std::path::PathBuf {
        crate_root()
            .ancestors()
            .find(|dir| dir.join("Cargo.toml").is_file() && dir.join("crates").is_dir())
            .map(std::path::Path::to_path_buf)
            .expect("could not find the rat-kingdom workspace above the test's runtime directory")
    }

    fn fixture(name: &str) -> String {
        format!(
            "{}/tests/fixtures/product_to_code/{name}",
            crate_root().display()
        )
    }

    fn run(args: &[&str]) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_rk"))
            .args(args)
            .env_remove("RK_AGENT")
            .env_remove("RK_AUTH_TOKEN")
            .output()
            .unwrap()
    }

    fn json_output(output: &std::process::Output) -> Value {
        serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
            panic!(
                "invalid JSON: {error}\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        })
    }

    fn errors(value: &Value) -> String {
        value["errors"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn validate(report: &str) -> std::process::Output {
        run(&[
            "--json",
            "product-to-code",
            "verify-report",
            "validate",
            "--report",
            &fixture(report),
            "--initiative",
            &fixture("initiative_verification.json"),
            "--evidence-dir",
            &fixture("evidence_verification"),
        ])
    }

    #[test]
    fn test_verify_report_accepts_complete_mapping_from_criteria_to_evidence() {
        let output = validate("verification_complete.json");
        assert!(
            output.status.success(),
            "stderr: {}\nstdout: {}",
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
        let value = json_output(&output);
        assert_eq!(value["valid"], true);
        assert_eq!(value["report_id"], "VR-complete");
        assert_eq!(value["satisfied"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_verify_report_rejects_missing_acceptance_criterion() {
        let output = validate("verification_missing_criterion.json");
        assert!(!output.status.success());
        let text = errors(&json_output(&output));
        assert!(text.contains("AC-2"), "{text}");
        assert!(text.contains("missing"), "{text}");
    }

    #[test]
    fn test_verify_report_rejects_unknown_evidence_id() {
        let output = validate("verification_unknown_evidence.json");
        assert!(!output.status.success());
        let text = errors(&json_output(&output));
        assert!(text.contains("EV-unknown"), "{text}");
    }

    #[test]
    fn test_verify_report_requires_gap_for_unsatisfied_without_evidence() {
        let output = validate("verification_unsatisfied_without_gap.json");
        assert!(!output.status.success());
        let text = errors(&json_output(&output));
        assert!(text.contains("AC-2"), "{text}");
        assert!(text.contains("gap") || text.contains("evidence"), "{text}");
    }

    #[test]
    fn test_verify_report_requires_browser_evidence_for_browser_applicable_criterion() {
        let output = validate("verification_browser_wrong_kind.json");
        assert!(!output.status.success());
        let text = errors(&json_output(&output));
        assert!(text.contains("browser_acceptance"), "{text}");
        assert!(text.contains("AC-1"), "{text}");
    }

    #[test]
    fn test_verify_report_render_markdown_groups_satisfied_gaps_and_recommendation() {
        let output = run(&[
            "product-to-code",
            "verify-report",
            "render",
            "--report",
            &fixture("verification_with_gap.json"),
            "--format",
            "markdown",
        ]);
        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let markdown = String::from_utf8(output.stdout).unwrap();
        assert!(markdown.contains("## Satisfied"));
        assert!(markdown.contains("## Gaps"));
        assert!(markdown.contains("## Recommendation"));
        assert!(markdown.contains("AC-1"));
        assert!(markdown.contains("AC-2"));
    }

    #[test]
    fn test_independent_verifier_workflow_declares_no_implementation_authority() {
        let workflow = std::fs::read_to_string(
            workspace_root().join("examples/workflows/independent-verifier.cue"),
        )
        .unwrap();
        assert!(workflow.contains("independent-verifier"));
        assert!(workflow.contains("verify-report validate"));
        assert!(workflow.contains("Do not modify implementation code"));
        assert!(workflow.contains("evidence and gaps"));
    }
}
