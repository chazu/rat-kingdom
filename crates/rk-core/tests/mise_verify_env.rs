//! `mise.toml`'s `[tasks.test]`/`[tasks.verify]` `env -u ...` prefix is the
//! command a reviewer's own shell actually runs (as opposed to a
//! daemon-executed `strip_rk_spawn` check, which already derives from
//! `rk_core::review::STRIPPED_RK_SPAWN_ENV`). It must strip exactly that
//! canonical set — not a hand-typed subset — or a reviewer invoking
//! `mise run verify` directly leaks its own `RK_REVIEW_*` binding into any
//! nested process that exercises reviewer-role writes (e.g.
//! `reviewer_drives_rework.rs`'s synthetic reviewer subprocess).

use std::path::{Path, PathBuf};

/// Walks up from `start` to find the workspace root, identified by a
/// `mise.toml` alongside the workspace `Cargo.toml`.
///
/// Deliberately runtime-resolved rather than baked from
/// `env!("CARGO_MANIFEST_DIR")`: under a shared `CARGO_TARGET_DIR`, a
/// byte-identical test binary can be reused from a different (possibly
/// reaped) worktree, so a compile-time path can point at a directory that no
/// longer exists.
fn workspace_root_from(start: &Path) -> Option<PathBuf> {
    start
        .ancestors()
        .find(|dir| dir.join("Cargo.toml").is_file() && dir.join("mise.toml").is_file())
        .map(Path::to_path_buf)
}

fn workspace_root() -> PathBuf {
    let cwd = std::env::current_dir().expect("test process must have a current directory");
    workspace_root_from(&cwd).unwrap_or_else(|| {
        panic!(
            "could not find the rat-kingdom workspace above runtime directory {}",
            cwd.display()
        )
    })
}

fn task_run_command(mise_toml: &toml::Table, task: &str) -> String {
    mise_toml
        .get("tasks")
        .and_then(|tasks| tasks.get(task))
        .and_then(|entry| entry.get("run"))
        .and_then(|run| run.as_str())
        .unwrap_or_else(|| panic!("mise.toml has no [tasks.{task}].run string"))
        .to_string()
}

fn assert_strips_canonical_env(task: &str, command: &str) {
    for var in rk_core::review::STRIPPED_RK_SPAWN_ENV {
        let flag = format!("-u {var}");
        assert!(
            command.contains(&flag),
            "mise.toml's [tasks.{task}].run is missing `{flag}` in its `env -u` prefix — \
             it must strip the full rk_core::review::STRIPPED_RK_SPAWN_ENV set, not a \
             hand-typed subset, or a reviewer's own `mise run {task}` leaks RK_REVIEW_* \
             into nested reviewer-role subprocesses. Command was: {command}"
        );
    }
}

#[test]
fn mise_test_task_strips_the_full_strip_rk_spawn_environment() {
    let source =
        std::fs::read_to_string(workspace_root().join("mise.toml")).expect("read mise.toml");
    let parsed: toml::Table = source.parse().expect("parse mise.toml");
    assert_strips_canonical_env("test", &task_run_command(&parsed, "test"));
}

#[test]
fn mise_verify_task_strips_the_full_strip_rk_spawn_environment() {
    let source =
        std::fs::read_to_string(workspace_root().join("mise.toml")).expect("read mise.toml");
    let parsed: toml::Table = source.parse().expect("parse mise.toml");
    assert_strips_canonical_env("verify", &task_run_command(&parsed, "verify"));
}
