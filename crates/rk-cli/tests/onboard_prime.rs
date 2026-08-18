use serde_json::Value;
use std::process::Command;

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rk"))
        .args(args)
        .env_remove("RK_AGENT")
        .env_remove("RK_AUTH_TOKEN")
        .env_remove("RK_TASK")
        .env_remove("RK_REPO")
        .env_remove("RK_ROLE")
        .env_remove("RK_BRANCH")
        .env_remove("RK_WORKTREE")
        .output()
        .unwrap()
}

#[test]
fn onboard_is_exact_sugar_for_the_onboarding_prime_role() {
    let sugar = run(&["onboard"]);
    assert!(
        sugar.status.success(),
        "rk onboard failed: {}",
        String::from_utf8_lossy(&sugar.stderr)
    );
    let explicit = run(&["prime", "--role", "onboarding"]);
    assert!(
        explicit.status.success(),
        "rk prime --role onboarding failed: {}",
        String::from_utf8_lossy(&explicit.stderr)
    );
    assert_eq!(sugar.stdout, explicit.stdout);
    let text = String::from_utf8(sugar.stdout).unwrap();
    assert!(text.contains("guided repository onboarding"));
    assert!(text.contains("Verification contract"));
}

#[test]
fn onboard_json_identifies_the_onboarding_role() {
    let output = run(&["--json", "onboard"]);
    assert!(
        output.status.success(),
        "rk --json onboard failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["role"], "onboarding");
    assert!(value["prime"]
        .as_str()
        .unwrap()
        .contains("Verification contract"));
}
