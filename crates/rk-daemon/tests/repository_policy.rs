//! Repository policies are content-bound operator configuration: registration
//! activates the exact `.rk/repo.cue`, spawn uses its naming templates, and
//! dismiss executes its delivery mode and remote-branch template.

mod fixture;

mod support;

use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::json;
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use support::connect;

static HARNESS_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const WORKING_FAKE: &str = r#"
read -r _prompt
echo "policy work" > policy-work.txt
git add policy-work.txt >/dev/null 2>&1
git -c user.email=r@x -c user.name=R commit -q -m "work: policy"
echo '{"type":"system","subtype":"init","session_id":"policy-fake"}'
rk_done "policy work complete"
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"policy-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#;

const POLICY: &str = r#"
repo: {
    work: {
        branch: "workers/{{role}}/{{task}}/{{agent}}"
        worktree: "custom/{{repo}}/{{task}}/{{agent}}"
    }
    delivery: {
        target: "agent-base"
        mode: "push-branch"
        remote: "origin"
        remoteBranch: "review/{{branch}}"
        deleteSource: true
    }
}
"#;

const MERGE_PUSH_POLICY: &str = r#"
repo: {
    delivery: {
        target: "agent-base"
        mode: "merge-push"
        remote: "origin"
        remoteBranch: "{{branch}}"
        deleteSource: true
    }
}
"#;

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

#[tokio::test]
async fn registered_repo_without_activated_cue_is_visible_but_cannot_dispatch() {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::write(repo.join("README.md"), "# inactive policy\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);

    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "inactive-policy".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    let name = repo.file_name().unwrap().to_string_lossy().to_string();

    let added = client
        .call(
            "repo.add",
            json!({"name": name, "path": repo.to_string_lossy()}),
        )
        .await
        .unwrap();
    assert!(
        added["repo"]["activated_policy"].is_null(),
        "registration must not invent a policy: {added}"
    );

    let listed = client.call("repo.list", json!({})).await.unwrap();
    assert!(
        listed["repos"]
            .as_array()
            .is_some_and(|repos| repos.iter().any(|entry| entry["name"] == name)),
        "inactive repo must remain inspectable: {listed}"
    );

    let error = client
        .call(
            "agent.spawn",
            json!({"repo": repo.to_string_lossy(), "task": "must-onboard", "harness": "fake"}),
        )
        .await
        .expect_err("dispatch must fail closed without activated repo CUE");
    let message = error.to_string();
    assert!(
        message.contains("no activated .rk/repo.cue policy"),
        "{message}"
    );
    assert!(message.contains("rk repo onboard"), "{message}");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn activated_policy_controls_names_target_and_remote_delivery() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let origin = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "--bare", "-b", "main"]);

    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path().join("policyrepo");
    std::fs::create_dir(&repo_path).unwrap();
    let repo = repo_path.as_path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    git(
        repo,
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    std::fs::create_dir_all(repo.join(".rk")).unwrap();
    std::fs::write(repo.join(".rk/repo.cue"), POLICY).unwrap();
    std::fs::write(repo.join("README.md"), "# policy test\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo);
    git(repo, &["push", "-u", "origin", "main"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "policy-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    let repo_name = repo.file_name().unwrap().to_string_lossy().to_string();

    let added = client
        .call(
            "repo.add",
            json!({"name": repo_name, "path": repo.to_string_lossy()}),
        )
        .await
        .unwrap();
    assert!(
        added["repo"]["activated_policy"]["digest"]
            .as_str()
            .is_some_and(|digest| digest.len() == 64),
        "registration must bind the exact policy: {added}"
    );

    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo.to_string_lossy(), "task": "Feature 42", "harness": "fake"}),
        )
        .await
        .unwrap();
    let agent = spawned["agent"]["name"].as_str().unwrap().to_string();
    let branch = spawned["agent"]["branch"].as_str().unwrap().to_string();
    assert_eq!(
        branch,
        format!("workers/rat/feature-42/{}", agent.to_ascii_lowercase())
    );
    assert_eq!(spawned["agent"]["target_branch"], "main");
    let expected_worktree = layout
        .worktrees_dir()
        .join("custom")
        .join(&repo_name)
        .join("Feature-42")
        .join(&agent);
    assert_eq!(
        spawned["agent"]["worktree"],
        expected_worktree.to_string_lossy().as_ref()
    );

    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let status = client
            .call("agent.status", json!({"name": agent}))
            .await
            .unwrap();
        if status["agent"]["state"] == "completed" {
            break;
        }
    }
    let events = client
        .call(
            "space.scan",
            json!({"category": "event", "identity": "harness_result"}),
        )
        .await
        .unwrap();
    let completion = events["tuples"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tuple| tuple["payload"]["agent"] == agent)
        .expect("agent completion event");
    assert_eq!(
        completion["payload"]["target"], "main",
        "completion must carry the daemon-authored agent base"
    );
    // C3 (docs/2026-08-17-tkt-c1-generation-identity.md): the producer side
    // of the spawn-keyed join — a real completion must carry the minted
    // generation id, not just the display name.
    assert!(
        completion["payload"]["spawn"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "harness_result must carry the completing generation's spawn id: {completion:?}"
    );
    let dismissed = client
        .call("agent.dismiss", json!({"name": agent}))
        .await
        .unwrap();
    assert_eq!(dismissed["delivered"], false);
    let delivered = client
        .call(
            "repo.land",
            json!({"repo": repo, "branch": branch, "target": "main"}),
        )
        .await
        .unwrap();
    assert_eq!(delivered["delivered"], true, "{delivered}");
    assert_eq!(delivered["pushed"], true, "{delivered}");
    assert_eq!(delivered["merged"], false, "{delivered}");
    assert_eq!(delivered["branch_deleted"], true, "{delivered}");
    let remote_branch = format!("review/{branch}");
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(origin.path())
            .args([
                "rev-parse",
                "--verify",
                &format!("refs/heads/{remote_branch}")
            ])
            .output()
            .unwrap()
            .status
            .success(),
        "configured remote branch must exist"
    );
    assert_eq!(
        git(repo, &["rev-parse", "main"]),
        git(origin.path(), &["rev-parse", "main"])
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn merge_push_delivers_the_agents_feature_base_to_the_remote() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let origin = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "--bare", "-b", "main"]);
    let repo_dir = tempfile::tempdir().unwrap();
    let repo_path = repo_dir.path().join("mergepushrepo");
    std::fs::create_dir(&repo_path).unwrap();
    let repo = repo_path.as_path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    git(
        repo,
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    std::fs::create_dir_all(repo.join(".rk")).unwrap();
    std::fs::write(repo.join(".rk/repo.cue"), MERGE_PUSH_POLICY).unwrap();
    std::fs::write(repo.join("README.md"), "# merge push\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo);
    git(repo, &["branch", "feature/integration"]);
    git(repo, &["push", "-u", "origin", "main"]);
    git(repo, &["push", "-u", "origin", "feature/integration"]);
    let main_before = git(repo, &["rev-parse", "main"]);
    let feature_before = git(repo, &["rev-parse", "feature/integration"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", fixture::with_rk_done(WORKING_FAKE));
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "merge-push".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call("repo.add", json!({"name": "friendly-alias", "path": repo}))
        .await
        .unwrap();
    let spawned = client
        .call(
            "agent.spawn",
            json!({
                "repo": repo,
                "task": "feature-policy",
                "harness": "fake",
                "base": "feature/integration",
            }),
        )
        .await
        .unwrap();
    let agent = spawned["agent"]["name"].as_str().unwrap().to_string();
    let branch = spawned["agent"]["branch"].as_str().unwrap().to_string();
    assert_eq!(spawned["agent"]["target_branch"], "feature/integration");
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if client
            .call("agent.status", json!({"name": agent}))
            .await
            .unwrap()["agent"]["state"]
            == "completed"
        {
            break;
        }
    }
    let dismissed = client
        .call("agent.dismiss", json!({"name": agent}))
        .await
        .unwrap();
    assert_eq!(dismissed["delivered"], false);
    let delivered = client
        .call(
            "repo.land",
            json!({"repo": repo, "branch": branch, "target": "feature/integration"}),
        )
        .await
        .unwrap();
    assert_eq!(delivered["delivered"], true, "{delivered}");
    assert_eq!(delivered["merged"], true, "{delivered}");
    assert_eq!(delivered["pushed"], true, "{delivered}");
    assert_eq!(delivered["target"], "feature/integration");
    assert_ne!(
        git(repo, &["rev-parse", "feature/integration"]),
        feature_before
    );
    assert_eq!(git(repo, &["rev-parse", "main"]), main_before);
    assert_eq!(
        git(repo, &["rev-parse", "feature/integration"]),
        git(origin.path(), &["rev-parse", "feature/integration"]),
        "merge-push must advance the configured remote feature branch"
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

// TKT-hisag-nubaf-kugon: worker verification handoff is a real spawn-path
// behavior, not just a pure-function unit test — these three prove the
// actual PrimeContext/prompt wiring end to end (enabled/default/unsupported
// route), using the same `$RK_FAKE_SYSTEM_PROMPT` capture mechanism
// `convention_priming.rs` uses to assert what a spawned rat is actually
// primed with.

const HANDOFF_POLICY: &str = r#"
repo: {
    delivery: {
        target: "agent-base"
        mode: "merge"
        remote: "origin"
        remoteBranch: "{{branch}}"
        deleteSource: true
    }
    landing: {
        verificationHandoff: true
    }
}
"#;

const HANDOFF_PUSH_BRANCH_POLICY: &str = r#"
repo: {
    delivery: {
        target: "agent-base"
        mode: "push-branch"
        remote: "origin"
        remoteBranch: "review/{{branch}}"
        deleteSource: true
    }
    landing: {
        verificationHandoff: true
    }
}
"#;

/// The daemon-native landing pipeline's real completion feed, repo-local —
/// same trigger `live_landing_burst.rs` installs to prove the reactor's
/// `action: "land"` dispatch actually routes THIS repo's completions onto
/// the queue, as opposed to a merely-wired (but inert) `LandingPipeline`
/// (TKT-hisag-nubaf-kugon REWORK finding #1).
const LANDING_TRIGGER: &str = r#"
triggers: [
	{
		name:   "landing-on-completion"
		action: "land"
		match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}
		maxFires: 20
	},
]
"#;

/// Write and commit a repo-local `.rk/triggers.cue` carrying [`LANDING_TRIGGER`]
/// — the reactor resolves it the same way it resolves the global trigger
/// directory (`Reactor::trigger_files`).
fn install_landing_trigger(repo: &Path) {
    std::fs::write(repo.join(".rk/triggers.cue"), LANDING_TRIGGER).unwrap();
    git(repo, &["add", ".rk/triggers.cue"]);
    git(repo, &["commit", "-m", "test: register landing trigger"]);
}

fn capture_prompt_fake() -> String {
    fixture::with_rk_done(
        r#"
read -r _prompt
printf '%s' "$RK_FAKE_SYSTEM_PROMPT" > primed.txt
git add primed.txt >/dev/null 2>&1
git -c user.email=rat@x -c user.name=Rat commit -q -m "capture prime"
echo '{"type":"system","subtype":"init","session_id":"handoff-fake"}'
rk_done "captured prime"
echo '{"type":"result","subtype":"success","is_error":false,"result":"captured prime","session_id":"handoff-fake","total_cost_usd":0.001,"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
"#,
    )
}

/// Spawn a "rat" against `repo`, wait for it to complete, dismiss it
/// (worktree-only, no merge needed), and return the system prompt it was
/// actually launched with, read back from the committed `primed.txt`.
async fn spawn_and_capture_prompt(client: &mut Client, repo: &Path, task: &str) -> String {
    let spawned = client
        .call(
            "agent.spawn",
            json!({"repo": repo.to_string_lossy(), "task": task, "harness": "fake"}),
        )
        .await
        .unwrap();
    let name = spawned["agent"]["name"].as_str().unwrap().to_string();
    let branch = spawned["agent"]["branch"].as_str().unwrap().to_string();
    let mut completed = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if client
            .call("agent.status", json!({"name": name}))
            .await
            .unwrap()["agent"]["state"]
            == "completed"
        {
            completed = true;
            break;
        }
    }
    assert!(completed, "agent never completed");
    client
        .call("agent.dismiss", json!({"name": name}))
        .await
        .unwrap();
    git(repo, &["show", &format!("{branch}:primed.txt")])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verification_handoff_enabled_merge_mode_swaps_step_3_in_spawned_prompt() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::create_dir_all(repo.join(".rk")).unwrap();
    std::fs::write(repo.join(".rk/repo.cue"), HANDOFF_POLICY).unwrap();
    std::fs::write(repo.join("README.md"), "# handoff enabled\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo);
    // Without a registered `action: "land"` trigger there is no live
    // automatic route, no matter how the policy flag is set (see the sibling
    // `_without_land_trigger_` test below) — this is what makes THIS test
    // actually prove a live route, not just a wired-but-inert pipeline.
    install_landing_trigger(repo);

    std::env::set_var("RK_FAKE_HARNESS_CMD", capture_prompt_fake());
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "handoff-enabled-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo).await;

    let primed = spawn_and_capture_prompt(&mut client, repo, "handoff-enabled").await;
    assert!(
        primed.contains("This repository has opted into verification handoff"),
        "opted-in merge-mode repo with a live automatic land route must \
         receive the handoff step 3:\n{primed}"
    );
    assert!(
        !primed.contains("Verify with the project's documented verification entrypoint"),
        "handoff step 3 must replace, not append to, the standard mandate:\n{primed}"
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verification_handoff_enabled_merge_mode_without_land_trigger_keeps_standard_step_3_in_spawned_prompt(
) {
    // TKT-hisag-nubaf-kugon REWORK finding #1: a `LandingPipeline` is wired
    // unconditionally at daemon startup (so manual `rk land` still works with
    // the reactor disabled), so its mere existence proves nothing about
    // whether THIS repo's completions ever reach an automatic gate. This is
    // otherwise byte-for-byte the same fixture as
    // `verification_handoff_enabled_merge_mode_swaps_step_3_in_spawned_prompt`
    // MINUS `install_landing_trigger` — same opted-in policy, same merge
    // delivery, same live reactor — proving the flag alone, or the pipeline
    // alone, is not sufficient: only a repo with a registered matching
    // "land" trigger gets the handoff.
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::create_dir_all(repo.join(".rk")).unwrap();
    std::fs::write(repo.join(".rk/repo.cue"), HANDOFF_POLICY).unwrap();
    std::fs::write(
        repo.join("README.md"),
        "# handoff enabled, no land trigger\n",
    )
    .unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
    support::install_passing_landing_checks(repo);

    std::env::set_var("RK_FAKE_HARNESS_CMD", capture_prompt_fake());
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "handoff-no-trigger-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo).await;

    let primed = spawn_and_capture_prompt(&mut client, repo, "handoff-no-trigger").await;
    assert!(
        primed.contains("Verify with the project's documented verification entrypoint"),
        "no registered land trigger means no live automatic route, so the \
         opted-in flag must not activate even with the reactor enabled and \
         a LandingPipeline wired:\n{primed}"
    );
    assert!(
        !primed.contains("This repository has opted into verification handoff"),
        "a merely-wired LandingPipeline must never be mistaken for a live \
         route:\n{primed}"
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verification_handoff_default_off_keeps_standard_step_3_in_spawned_prompt() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::write(repo.join("README.md"), "# handoff default\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);
    // Merge-mode delivery with no `landing.verificationHandoff` at all —
    // today's shipped default (`false`) on an otherwise fully-eligible repo.
    support::install_default_repository_policy(repo);

    std::env::set_var("RK_FAKE_HARNESS_CMD", capture_prompt_fake());
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "handoff-default-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo).await;

    let primed = spawn_and_capture_prompt(&mut client, repo, "handoff-default").await;
    assert!(
        primed.contains("Verify with the project's documented verification entrypoint"),
        "an unactivated flag must leave today's mandatory self-verify text \
         unchanged:\n{primed}"
    );
    assert!(
        !primed.contains("This repository has opted into verification handoff"),
        "the handoff text must never appear unless the repo opted in:\n{primed}"
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verification_handoff_enabled_push_branch_keeps_standard_step_3_in_spawned_prompt() {
    let _env_guard = HARNESS_ENV_LOCK.lock().await;
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    let repo = repo_dir.path();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "r@x"]);
    git(repo, &["config", "user.name", "R"]);
    std::fs::create_dir_all(repo.join(".rk")).unwrap();
    // Opted in, but push-branch has no automatic native gate to hand the
    // check to — a missing delivery route must retain the truthful protocol.
    std::fs::write(repo.join(".rk/repo.cue"), HANDOFF_PUSH_BRANCH_POLICY).unwrap();
    std::fs::write(repo.join("README.md"), "# handoff unsupported route\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "init"]);

    std::env::set_var("RK_FAKE_HARNESS_CMD", capture_prompt_fake());
    let layout = Layout::at(home.path());
    let daemon =
        Daemon::new_in_memory(layout.clone(), "handoff-push-branch-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    support::register_repo(&mut client, repo).await;

    let primed = spawn_and_capture_prompt(&mut client, repo, "handoff-push-branch").await;
    assert!(
        primed.contains("Verify with the project's documented verification entrypoint"),
        "push-branch delivery has no automatic gate to hand off to, so the \
         opted-in flag must not activate:\n{primed}"
    );
    assert!(
        !primed.contains("This repository has opted into verification handoff"),
        "push-branch delivery must never receive the handoff text:\n{primed}"
    );

    handle.abort();
    std::env::remove_var("RK_FAKE_HARNESS_CMD");
}
