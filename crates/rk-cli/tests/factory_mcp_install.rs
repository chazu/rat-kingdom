//! Acceptance coverage for the explicit Jcode rk-mcp installer.

use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn prepare_rk(temp: &tempfile::TempDir) -> (PathBuf, PathBuf) {
    let bin = temp.path().join("bin");
    fs::create_dir_all(&bin).unwrap();
    let rk = bin.join("rk");
    fs::copy(env!("CARGO_BIN_EXE_rk"), &rk).unwrap();

    let source = temp.path().join("release-rk-mcp");
    fs::write(&source, b"release rk-mcp fixture\n").unwrap();
    make_executable(&source);
    (rk, source)
}

fn make_executable(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).unwrap();
    }
}

fn run(rk: &Path, home: &Path, source: &Path, args: &[&str]) -> Output {
    let mut last_busy = None;
    for _ in 0..50 {
        let result = Command::new(rk)
            .args(args)
            .env("HOME", home)
            .env("RK_MCP_SOURCE", source)
            .env_remove("RK_HOME")
            .env_remove("RK_AGENT")
            .env_remove("RK_AUTH_TOKEN")
            .env_remove("RK_TASK")
            .env_remove("RK_REPO")
            .env_remove("RK_ROLE")
            .output();
        match result {
            Ok(output) => return output,
            // Linux may briefly reject an executable immediately after this
            // fixture copies it. Retry only that platform-level condition.
            Err(error) if error.raw_os_error() == Some(26) => {
                last_busy = Some(error);
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(error) => panic!("failed to run {}: {error}", rk.display()),
        }
    }
    panic!(
        "{} remained busy after fixture copy: {}",
        rk.display(),
        last_busy.unwrap()
    )
}

fn json_output(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "stderr: {}\nstdout: {}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn install_mcp_copies_binary_preserves_config_and_is_idempotent() {
    let temp = tempfile::tempdir().unwrap();
    let (rk, source) = prepare_rk(&temp);
    let home = temp.path().join("home");
    let config = home.join(".jcode/mcp.json");
    fs::create_dir_all(config.parent().unwrap()).unwrap();
    fs::write(
        &config,
        serde_json::to_vec_pretty(&json!({
            "unknownRoot": {"keep": [1, 2, 3]},
            "servers": {
                "other": {"command": "other-server", "custom": {"keep": true}}
            }
        }))
        .unwrap(),
    )
    .unwrap();

    let first = json_output(&run(
        &rk,
        &home,
        &source,
        &["--json", "factory", "install-mcp"],
    ));
    let destination = temp.path().join("bin/rk-mcp");
    assert_eq!(first["schema"], "factory.mcp-install.v1");
    assert_eq!(first["server"], "rk");
    assert_eq!(first["disposition"], "installed");
    assert_eq!(first["binary"], destination.display().to_string());
    assert_eq!(fs::read(&destination).unwrap(), fs::read(&source).unwrap());

    let installed: Value = serde_json::from_slice(&fs::read(&config).unwrap()).unwrap();
    assert_eq!(installed["unknownRoot"], json!({"keep": [1, 2, 3]}));
    assert_eq!(
        installed["servers"]["other"],
        json!({
            "command": "other-server",
            "custom": {"keep": true}
        })
    );
    assert_eq!(
        installed["servers"]["rk"]["command"],
        destination.display().to_string()
    );
    assert_eq!(installed["servers"]["rk"]["args"], json!([]));
    assert_eq!(
        installed["servers"]["rk"]["x-rat-kingdom-managed"],
        "factory.mcp-install.v1"
    );
    assert!(installed.get("mcpServers").is_none());

    let second = json_output(&run(
        &rk,
        &home,
        &source,
        &["--json", "factory", "install-mcp"],
    ));
    assert_eq!(second["disposition"], "already_installed");
}

#[test]
fn install_mcp_refuses_foreign_entry_without_force_and_preserves_unknown_fields() {
    let temp = tempfile::tempdir().unwrap();
    let (rk, source) = prepare_rk(&temp);
    let home = temp.path().join("home");
    let config = home.join(".jcode/mcp.json");

    assert!(run(&rk, &home, &source, &["factory", "install-mcp"])
        .status
        .success());
    let mut foreign: Value = serde_json::from_slice(&fs::read(&config).unwrap()).unwrap();
    foreign["servers"]["rk"] = json!({
        "command": "human-owned",
        "args": ["--human"],
        "custom": {"preserve": [true, false]}
    });
    fs::write(&config, serde_json::to_vec_pretty(&foreign).unwrap()).unwrap();
    let before = fs::read(&config).unwrap();

    let refused = run(&rk, &home, &source, &["factory", "install-mcp"]);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("--force"));
    assert_eq!(fs::read(&config).unwrap(), before);

    let forced = json_output(&run(
        &rk,
        &home,
        &source,
        &["--json", "factory", "install-mcp", "--force"],
    ));
    assert_eq!(forced["disposition"], "updated");
    let replaced: Value = serde_json::from_slice(&fs::read(&config).unwrap()).unwrap();
    assert_eq!(
        replaced["servers"]["rk"]["custom"],
        json!({"preserve": [true, false]})
    );
    assert_eq!(
        replaced["servers"]["rk"]["command"],
        temp.path().join("bin/rk-mcp").display().to_string()
    );
    assert_eq!(
        replaced["servers"]["rk"]["x-rat-kingdom-managed"],
        "factory.mcp-install.v1"
    );
}
