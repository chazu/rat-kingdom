//! Staging and validation of approved repository onboarding proposals.
//!
//! All repository writes happen in the durable onboarding worktree. The exact
//! approved patch becomes one commit carrying proposal trailers; those trailers
//! are the restart seam when Git advanced but the JSON journal did not. The
//! named check or automation definition is then reloaded through
//! rk-workflow's existing CUE schemas. Automation remains inert here: only the
//! separate activation transition may advance it into the registered checkout.

use crate::onboarding_proposals::{
    onboarding_tree_revision, OnboardingApplication, OnboardingAutomationKind,
    OnboardingNamedCheck, OnboardingProposal, OnboardingValidation, OnboardingVerification,
};
use crate::onboarding_sessions::OnboardingSession;
use chrono::Utc;
use rk_workflow::{Check, CheckEnvironmentPolicy};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

const CHECKS_TARGET: &str = ".rk/checks.cue";
const OUTPUT_SUMMARY_LIMIT: usize = 8 * 1024;
const STRIPPED_RK_ENV: &[&str] = &[
    "RK_AGENT",
    "RK_TASK",
    "RK_REPO",
    "RK_ROLE",
    "RK_HOME",
    "RK_BRANCH",
    "RK_WORKTREE",
    "RK_AUTH_TOKEN",
];

/// Apply (or recover) the exact approved patch and return durable evidence for
/// its one onboarding-branch commit.
pub fn ensure_application(
    session: &OnboardingSession,
    proposal: &OnboardingProposal,
    actor: &str,
) -> rk_core::Result<OnboardingApplication> {
    proposal.validate_integrity()?;
    validate_supported_target(proposal)?;
    let worktree = &session.worktree;
    let target_path = proposal.target_path.as_str();
    let target = worktree.join(target_path);

    if let Some(application) = &proposal.application {
        require_clean(worktree)?;
        let head = git_text(worktree, &["rev-parse", "HEAD"])?;
        if head != application.commit {
            return Err(rk_core::Error::other(format!(
                "onboarding branch drift after apply: recorded {}, current {head}",
                application.commit
            )));
        }
        let digest = file_digest(&target)?;
        if digest != application.target_digest {
            return Err(rk_core::Error::other(format!(
                "{target_path} drift after apply: recorded {}, current {digest}",
                application.target_digest
            )));
        }
        validate_target(&target, proposal)?;
        return Ok(application.clone());
    }

    let expected_trailer = format!("Onboarding-Digest: {}", proposal.digest);
    let head_message = git_text(worktree, &["log", "-1", "--format=%B"])?;
    let committed_recovery = head_message
        .lines()
        .any(|line| line.trim() == expected_trailer);
    if committed_recovery {
        require_clean(worktree)?;
        let parent_tree = git_text(worktree, &["rev-parse", "HEAD^^{tree}"])?;
        if parent_tree != proposal.tree_revision {
            return Err(rk_core::Error::other(format!(
                "recovered onboarding commit parent drifted: proposed {}, current {parent_tree}",
                proposal.tree_revision
            )));
        }
        validate_target(&target, proposal)?;
        return application_evidence(worktree, &target, actor);
    }

    let current_tree = onboarding_tree_revision(worktree)?;
    if current_tree != proposal.tree_revision {
        return Err(rk_core::Error::other(format!(
            "stale onboarding tree before apply: proposed {}, current {current_tree}",
            proposal.tree_revision
        )));
    }

    let dirty_paths = status_paths(worktree)?;
    if dirty_paths.is_empty() {
        git_with_stdin(worktree, &["apply", "--check", "-"], &proposal.diff)?;
        git_with_stdin(worktree, &["apply", "-"], &proposal.diff)?;
    } else {
        require_only_target(&dirty_paths, target_path)?;
        git_with_stdin(
            worktree,
            &["apply", "--reverse", "--check", "-"],
            &proposal.diff,
        )
        .map_err(|_| {
            rk_core::Error::other(format!(
                "dirty onboarding worktree does not exactly match the approved patch: {}",
                dirty_paths.join(", ")
            ))
        })?;
    }

    let dirty_paths = status_paths(worktree)?;
    require_only_target(&dirty_paths, target_path)?;
    validate_target(&target, proposal)?;
    git_ok(worktree, &["add", "--", target_path])?;
    let subject = proposal
        .named_check
        .as_ref()
        .map(|check| check.name.as_str())
        .unwrap_or(&proposal.target_path);
    let message = format!(
        "onboarding: apply {}\n\nOnboarding-Proposal: {}\nOnboarding-Digest: {}",
        subject, proposal.id, proposal.digest
    );
    git_ok(
        worktree,
        &[
            "-c",
            "user.name=Rat Kingdom Onboarding",
            "-c",
            "user.email=onboarding@rat-kingdom.local",
            "commit",
            "-m",
            &message,
        ],
    )?;
    require_clean(worktree)?;
    application_evidence(worktree, &target, actor)
}

/// Revalidate one staged workflow, trigger, or schedule without activating it.
/// The resulting evidence is journaled independently of the later human
/// activation decision.
pub fn validate_automation(
    session: &OnboardingSession,
    proposal: &OnboardingProposal,
    actor: &str,
    attempt: u32,
) -> rk_core::Result<OnboardingValidation> {
    let kind = proposal.automation_kind().ok_or_else(|| {
        rk_core::Error::other(format!(
            "proposal {} is not an automation activation proposal",
            proposal.id
        ))
    })?;
    let target = session.worktree.join(&proposal.target_path);
    let started_at = Utc::now();
    let result = validate_automation_file(&target, kind);
    let finished_at = Utc::now();
    let target_digest = file_digest(&target)?;
    let (passed, output_summary, unresolved_risks) = match result {
        Ok(summary) => (true, summary, Vec::new()),
        Err(error) => (
            false,
            error.to_string(),
            vec!["automation definition is staged but invalid and cannot be activated".into()],
        ),
    };
    Ok(OnboardingValidation {
        attempt,
        actor: actor.to_string(),
        started_at,
        finished_at,
        automation_kind: kind,
        target_path: proposal.target_path.clone(),
        target_digest,
        validator: automation_validator(kind).into(),
        passed,
        output_summary,
        unresolved_risks,
    })
}

/// Execute the exact check contract from the applied CUE registry. A red
/// command is evidence, not an RPC transport error: the caller journals the
/// returned failed result before reporting failure to the operator.
pub async fn verify(
    session: &OnboardingSession,
    proposal: &OnboardingProposal,
    actor: &str,
    attempt: u32,
) -> rk_core::Result<OnboardingVerification> {
    let contract = proposal.named_check.as_ref().ok_or_else(|| {
        rk_core::Error::other("checks proposal has no digest-bound named_check contract")
    })?;
    let target = session.worktree.join(CHECKS_TARGET);
    validate_contract(&target, contract)?;
    let cwd = resolve_cwd(&session.worktree, &contract.cwd)?;
    let timeout = parse_duration(&contract.timeout)?;
    let started_at = Utc::now();

    let mut command = tokio::process::Command::new("sh");
    command
        .arg("-c")
        .arg(&contract.command)
        .current_dir(&cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if contract.environment_policy == CheckEnvironmentPolicy::StripRkSpawn {
        for name in STRIPPED_RK_ENV {
            command.env_remove(name);
        }
    }

    let outcome = match command.spawn() {
        Ok(child) => match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(Ok(output)) => ExecutionOutcome::Completed(output),
            Ok(Err(error)) => ExecutionOutcome::SpawnFailure(error.to_string()),
            Err(_) => ExecutionOutcome::TimedOut,
        },
        Err(error) => ExecutionOutcome::SpawnFailure(error.to_string()),
    };
    let finished_at = Utc::now();
    let (exit_status, timed_out, output_summary) = match outcome {
        ExecutionOutcome::Completed(output) => {
            let exit = output.status.code().map(i64::from);
            (exit, false, summarize_output(&output))
        }
        ExecutionOutcome::TimedOut => (
            None,
            true,
            format!(
                "command timed out after {} and was killed",
                contract.timeout
            ),
        ),
        ExecutionOutcome::SpawnFailure(detail) => {
            (None, false, format!("could not execute check: {detail}"))
        }
    };
    let passed = !timed_out && exit_status == Some(contract.expect_exit);
    let unresolved_risks = if passed {
        Vec::new()
    } else if timed_out {
        vec!["repository check did not finish within its approved timeout".into()]
    } else {
        vec!["repository check failed; onboarding branch is not ready to land".into()]
    };

    Ok(OnboardingVerification {
        attempt,
        actor: actor.to_string(),
        started_at,
        finished_at,
        check_name: contract.name.clone(),
        command: contract.command.clone(),
        cwd: contract.cwd.clone(),
        expected_exit: contract.expect_exit,
        timeout: contract.timeout.clone(),
        environment_policy: contract.environment_policy,
        toolchain: contract.toolchain.clone(),
        exit_status,
        timed_out,
        passed,
        output_summary,
        unresolved_risks,
    })
}

fn application_evidence(
    worktree: &Path,
    target: &Path,
    actor: &str,
) -> rk_core::Result<OnboardingApplication> {
    Ok(OnboardingApplication {
        actor: actor.to_string(),
        at: Utc::now(),
        commit: git_text(worktree, &["rev-parse", "HEAD"])?,
        tree_revision: onboarding_tree_revision(worktree)?,
        target_digest: file_digest(target)?,
    })
}

fn validate_supported_target(proposal: &OnboardingProposal) -> rk_core::Result<()> {
    if proposal.target_path == CHECKS_TARGET && proposal.named_check.is_some() {
        return Ok(());
    }
    if proposal.automation_kind().is_some() {
        return Ok(());
    }
    Err(rk_core::Error::other(format!(
        "repo onboarding apply does not support {} / {}",
        proposal.kind, proposal.target_path
    )))
}

fn validate_target(path: &Path, proposal: &OnboardingProposal) -> rk_core::Result<()> {
    if let Some(contract) = proposal.named_check.as_ref() {
        return validate_contract(path, contract).map(|_| ());
    }
    let kind = proposal.automation_kind().ok_or_else(|| {
        rk_core::Error::other(format!(
            "proposal {} has no supported validation contract",
            proposal.id
        ))
    })?;
    validate_automation_file(path, kind).map(|_| ())
}

fn validate_contract(path: &Path, contract: &OnboardingNamedCheck) -> rk_core::Result<Check> {
    let matches = rk_workflow::load_checks(path)?
        .into_iter()
        .filter(|check| check.name == contract.name)
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(rk_core::Error::other(format!(
            "{} must contain exactly one check named `{}`; found {}",
            path.display(),
            contract.name,
            matches.len()
        )));
    }
    let check = matches.into_iter().next().expect("one match");
    let observed_cwd = check.cwd.as_deref().unwrap_or(".");
    let observed_expect_exit = check.expect_exit.ok_or_else(|| {
        rk_core::Error::other(format!(
            "check `{}` must declare expectExit for onboarding",
            contract.name
        ))
    })?;
    let observed_timeout = check.timeout.as_deref().ok_or_else(|| {
        rk_core::Error::other(format!(
            "check `{}` must declare timeout for onboarding",
            contract.name
        ))
    })?;
    let observed_toolchain = check.toolchain.as_deref().ok_or_else(|| {
        rk_core::Error::other(format!(
            "check `{}` must declare toolchain for onboarding",
            contract.name
        ))
    })?;
    if check.command != contract.command
        || observed_cwd != contract.cwd
        || observed_expect_exit != contract.expect_exit
        || observed_timeout != contract.timeout
        || check.environment_policy != contract.environment_policy
        || observed_toolchain != contract.toolchain
    {
        return Err(rk_core::Error::other(format!(
            "check `{}` does not match its approved command/cwd/exit/timeout/environment/toolchain contract",
            contract.name
        )));
    }
    Ok(check)
}

pub(crate) fn validate_automation_file(
    path: &Path,
    kind: OnboardingAutomationKind,
) -> rk_core::Result<String> {
    let source = std::fs::read_to_string(path)
        .map_err(|error| rk_core::Error::other(format!("read {}: {error}", path.display())))?;
    crate::onboarding::reject_cue_imports(&source).map_err(rk_core::Error::other)?;
    match kind {
        OnboardingAutomationKind::Workflow => {
            rk_workflow::validate_workflow_str(&source)?;
            Ok(format!(
                "workflow schema validation passed for {}",
                path.display()
            ))
        }
        OnboardingAutomationKind::Trigger => {
            let triggers = rk_workflow::load_triggers_str(&source)?;
            if triggers.is_empty() {
                return Err(rk_core::Error::other(format!(
                    "{} contains no triggers",
                    path.display()
                )));
            }
            Ok(format!(
                "trigger schema validation passed for {} definition(s)",
                triggers.len()
            ))
        }
        OnboardingAutomationKind::Schedule => {
            let schedules = rk_workflow::load_schedules_str(&source)?;
            if schedules.is_empty() {
                return Err(rk_core::Error::other(format!(
                    "{} contains no schedules",
                    path.display()
                )));
            }
            for schedule in &schedules {
                crate::cron::Cron::parse(&schedule.cron).map_err(|error| {
                    rk_core::Error::other(format!(
                        "schedule `{}` has invalid cron: {error}",
                        schedule.name
                    ))
                })?;
            }
            Ok(format!(
                "schedule schema and cron validation passed for {} definition(s)",
                schedules.len()
            ))
        }
    }
}

fn automation_validator(kind: OnboardingAutomationKind) -> &'static str {
    match kind {
        OnboardingAutomationKind::Workflow => "rk_workflow::validate_workflow_str",
        OnboardingAutomationKind::Trigger => "rk_workflow::load_triggers_str",
        OnboardingAutomationKind::Schedule => {
            "rk_workflow::load_schedules_str + rk_daemon::cron::Cron::parse"
        }
    }
}

fn resolve_cwd(worktree: &Path, cwd: &str) -> rk_core::Result<PathBuf> {
    let relative = Path::new(cwd);
    if relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(rk_core::Error::other(format!(
            "named check cwd must stay inside the onboarding worktree: {cwd}"
        )));
    }
    let root = std::fs::canonicalize(worktree)?;
    let resolved = std::fs::canonicalize(worktree.join(relative))?;
    if !resolved.starts_with(&root) {
        return Err(rk_core::Error::other(format!(
            "named check cwd escapes the onboarding worktree: {cwd}"
        )));
    }
    Ok(resolved)
}

fn status_paths(worktree: &Path) -> rk_core::Result<Vec<String>> {
    let output = git_output(
        worktree,
        &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    )?;
    let mut paths = Vec::new();
    for record in output.stdout.split(|byte| *byte == 0) {
        if record.is_empty() {
            continue;
        }
        if record.len() < 4 || record[2] != b' ' {
            return Err(rk_core::Error::other(
                "could not parse onboarding worktree status",
            ));
        }
        paths.push(String::from_utf8_lossy(&record[3..]).into_owned());
    }
    Ok(paths)
}

fn require_clean(worktree: &Path) -> rk_core::Result<()> {
    let paths = status_paths(worktree)?;
    if paths.is_empty() {
        Ok(())
    } else {
        Err(rk_core::Error::other(format!(
            "dirty onboarding worktree: {}",
            paths.join(", ")
        )))
    }
}

fn require_only_target(paths: &[String], target: &str) -> rk_core::Result<()> {
    if paths.len() == 1 && paths[0] == target {
        Ok(())
    } else {
        Err(rk_core::Error::other(format!(
            "approved patch must change only {target}; observed {}",
            if paths.is_empty() {
                "no changed paths".into()
            } else {
                paths.join(", ")
            }
        )))
    }
}

fn git_output(worktree: &Path, args: &[&str]) -> rk_core::Result<Output> {
    let output = Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(args)
        .env("LC_ALL", "C")
        .output()?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(rk_core::Error::other(format!(
            "git {} failed: {}",
            args.join(" "),
            output_detail(&output)
        )))
    }
}

fn git_ok(worktree: &Path, args: &[&str]) -> rk_core::Result<()> {
    git_output(worktree, args).map(|_| ())
}

fn git_text(worktree: &Path, args: &[&str]) -> rk_core::Result<String> {
    let output = git_output(worktree, args)?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_with_stdin(worktree: &Path, args: &[&str], input: &str) -> rk_core::Result<()> {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args(args)
        .env("LC_ALL", "C")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .ok_or_else(|| rk_core::Error::other("git apply stdin was not piped"))?
        .write_all(input.as_bytes())?;
    let output = child.wait_with_output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(rk_core::Error::other(format!(
            "git {} failed: {}",
            args.join(" "),
            output_detail(&output)
        )))
    }
}

fn output_detail(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    } else {
        stderr
    }
}

fn file_digest(path: &Path) -> rk_core::Result<String> {
    let bytes = std::fs::read(path)
        .map_err(|error| rk_core::Error::other(format!("read {}: {error}", path.display())))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn parse_duration(value: &str) -> rk_core::Result<Duration> {
    let value = value.trim();
    let invalid = || rk_core::Error::other(format!("invalid duration: {value}"));
    let (number, multiplier) = match value.chars().last() {
        Some('s') => (&value[..value.len() - 1], 1_u64),
        Some('m') => (&value[..value.len() - 1], 60),
        Some('h') => (&value[..value.len() - 1], 3600),
        _ => (value, 1),
    };
    let seconds = number
        .parse::<u64>()
        .map_err(|_| invalid())?
        .checked_mul(multiplier)
        .filter(|seconds| *seconds > 0)
        .ok_or_else(invalid)?;
    Ok(Duration::from_secs(seconds))
}

fn summarize_output(output: &Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let text = match (stdout.trim().is_empty(), stderr.trim().is_empty()) {
        (true, true) => "(no output)".to_string(),
        (false, true) => format!("stdout:\n{}", stdout.trim()),
        (true, false) => format!("stderr:\n{}", stderr.trim()),
        (false, false) => format!("stdout:\n{}\nstderr:\n{}", stdout.trim(), stderr.trim()),
    };
    if text.len() <= OUTPUT_SUMMARY_LIMIT {
        text
    } else {
        let mut end = OUTPUT_SUMMARY_LIMIT;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}… [truncated]", &text[..end])
    }
}

enum ExecutionOutcome {
    Completed(Output),
    TimedOut,
    SpawnFailure(String),
}
