#[cfg(test)]
mod product_to_code_research {
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

    fn error_text(value: &Value) -> String {
        value["errors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|error| error.as_str().unwrap_or_default())
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn run(args: &[&str]) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_rk"))
            .args(args)
            .env_remove("RK_AGENT")
            .env_remove("RK_AUTH_TOKEN")
            .output()
            .unwrap()
    }

    fn write_temp_json(name: &str, value: Value) -> tempfile::TempPath {
        let file = tempfile::Builder::new()
            .prefix(name)
            .suffix(".json")
            .tempfile()
            .unwrap();
        serde_json::to_writer(file.as_file(), &value).unwrap();
        file.into_temp_path()
    }

    #[test]
    fn test_research_validate_accepts_complete_structured_artifact() {
        let output = run(&[
            "--json",
            "product-to-code",
            "research",
            "validate",
            "--artifact",
            &fixture("architecture_research_valid.json"),
            "--initiative",
            &fixture("initiative_minimal.json"),
        ]);

        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["valid"], true);
        assert_eq!(value["artifact_id"], "ARCH-product-to-code-research");
        assert_eq!(value["initiative_id"], "INIT-product-to-code");
        assert_eq!(value["errors"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_research_validate_rejects_artifact_for_wrong_initiative() {
        let output = run(&[
            "--json",
            "product-to-code",
            "research",
            "validate",
            "--artifact",
            &fixture("architecture_research_wrong_initiative.json"),
            "--initiative",
            &fixture("initiative_minimal.json"),
        ]);

        assert!(!output.status.success());
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        let errors = error_text(&value);
        assert!(errors.contains("INIT-other"), "{errors}");
        assert!(errors.contains("INIT-product-to-code"), "{errors}");
    }

    #[test]
    fn test_research_validate_rejects_empty_researched_files() {
        let output = run(&[
            "--json",
            "product-to-code",
            "research",
            "validate",
            "--artifact",
            &fixture("architecture_research_empty_files.json"),
            "--initiative",
            &fixture("initiative_minimal.json"),
        ]);

        assert!(!output.status.success());
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(error_text(&value).contains("researched_files"));
    }

    #[test]
    fn test_research_validate_rejects_no_decisions_constraints_or_risks() {
        let output = run(&[
            "--json",
            "product-to-code",
            "research",
            "validate",
            "--artifact",
            &fixture("architecture_research_no_substance.json"),
            "--initiative",
            &fixture("initiative_minimal.json"),
        ]);

        assert!(!output.status.success());
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        let errors = error_text(&value);
        assert!(errors.contains("architecture_decisions"), "{errors}");
        assert!(errors.contains("constraints"), "{errors}");
        assert!(errors.contains("risks"), "{errors}");
    }

    #[test]
    fn test_research_validate_rejects_invalid_initiative_semantics() {
        let mut initiative: Value = serde_json::from_str(
            &std::fs::read_to_string(fixture("initiative_minimal.json")).unwrap(),
        )
        .unwrap();
        initiative["acceptance_criteria"] = serde_json::json!([]);
        let initiative = write_temp_json("invalid-initiative", initiative);
        let output = run(&[
            "--json",
            "product-to-code",
            "research",
            "validate",
            "--artifact",
            &fixture("architecture_research_valid.json"),
            "--initiative",
            initiative.to_str().unwrap(),
        ]);

        assert!(!output.status.success());
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert!(error_text(&value).contains("acceptance_criteria"));
    }

    #[test]
    fn test_research_render_markdown_has_decisions_risks_and_open_questions() {
        let output = run(&[
            "product-to-code",
            "research",
            "render",
            "--artifact",
            &fixture("architecture_research_valid.json"),
            "--format",
            "markdown",
        ]);

        assert!(
            output.status.success(),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let markdown = String::from_utf8(output.stdout).unwrap();
        assert!(markdown.contains("## Decisions"));
        assert!(markdown.contains("## Risks"));
        assert!(markdown.contains("## Open Questions"));
    }

    #[test]
    fn test_architecture_research_workflow_declares_structured_artifact_output() {
        let workflow =
            std::fs::read_to_string(workspace_root().join("examples/workflows/research.cue"))
                .unwrap();

        assert!(workflow.contains("artifact"));
        assert!(workflow.contains("ArchitectureResearchArtifact"));
        assert!(workflow.contains("rk --json product-to-code research validate"));
    }

    #[test]
    fn test_architecture_research_workflow_loads_as_shared_cue() {
        let output = Command::new("cue")
            .arg("vet")
            .arg(workspace_root().join("examples/workflows/research.cue"))
            .arg(workspace_root().join("crates/rk-workflow/src/schema.cue"))
            .output();
        let Ok(output) = output else { return };

        assert!(
            output.status.success(),
            "stdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
