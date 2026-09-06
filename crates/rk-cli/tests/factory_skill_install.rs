//! Acceptance coverage for installing the bundled Factory Foreman skill globally.

use serde_json::Value;
use std::fs;
use std::process::Command;

fn run(home: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rk"))
        .args(args)
        .env("HOME", home)
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .env_remove("RK_TASK")
        .env_remove("RK_REPO")
        .env_remove("RK_ROLE")
        .output()
        .unwrap()
}

#[test]
fn install_skill_onboards_jcode_globally_and_is_idempotent() {
    let home = tempfile::tempdir().unwrap();
    let destination = home.path().join(".jcode/skills/factory-foreman");

    let first = run(home.path(), &["--json", "factory", "install-skill"]);
    assert!(
        first.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_json: Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(first_json["schema"], "factory.skill-install.v1");
    assert_eq!(first_json["skill"], "factory-foreman");
    assert_eq!(first_json["disposition"], "installed");
    assert_eq!(first_json["destination"], destination.display().to_string());
    assert!(destination.join("SKILL.md").is_file());

    let second = run(home.path(), &["--json", "factory", "install-skill"]);
    assert!(second.status.success());
    let second_json: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(second_json["disposition"], "already_installed");
}

#[test]
fn install_skill_preserves_customizations_unless_force_is_explicit() {
    let home = tempfile::tempdir().unwrap();
    let skill = home.path().join(".jcode/skills/factory-foreman/SKILL.md");
    assert!(run(home.path(), &["factory", "install-skill"])
        .status
        .success());
    fs::write(&skill, "customized\n").unwrap();

    let refused = run(home.path(), &["factory", "install-skill"]);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("--force"));
    assert_eq!(fs::read_to_string(&skill).unwrap(), "customized\n");

    let forced = run(
        home.path(),
        &["--json", "factory", "install-skill", "--force"],
    );
    assert!(forced.status.success());
    let forced_json: Value = serde_json::from_slice(&forced.stdout).unwrap();
    assert_eq!(forced_json["disposition"], "updated");
    assert!(fs::read_to_string(&skill)
        .unwrap()
        .contains("rk --json factory snapshot"));
}

#[test]
fn explicit_upgrade_removes_retired_python_payloads() {
    let home = tempfile::tempdir().unwrap();
    let destination = home.path().join(".jcode/skills/factory-foreman");
    fs::create_dir_all(destination.join("scripts")).unwrap();
    fs::create_dir_all(destination.join("dashboard/templates")).unwrap();
    for path in [
        "SKILL.md",
        "REFERENCE.md",
        "scripts/factory_foreman.py",
        "dashboard/render_factory_dashboard.py",
        "dashboard/templates/factory-dashboard.md",
    ] {
        fs::write(destination.join(path), "previous release\n").unwrap();
    }
    let refused = run(home.path(), &["factory", "install-skill"]);
    assert!(!refused.status.success());
    assert!(destination.join("scripts/factory_foreman.py").exists());

    let upgraded = run(home.path(), &["factory", "install-skill", "--force"]);
    assert!(
        upgraded.status.success(),
        "{}",
        String::from_utf8_lossy(&upgraded.stderr)
    );
    assert_eq!(fs::read_dir(&destination).unwrap().count(), 2);
    assert!(!destination.join("scripts").exists());
    assert!(!destination.join("dashboard").exists());
    let skill = fs::read_to_string(destination.join("SKILL.md")).unwrap();
    assert!(skill.contains("rk factory render"));
    assert!(!skill.contains("python3"));
    assert!(!home.path().join(".rat-kingdom").exists());
}
