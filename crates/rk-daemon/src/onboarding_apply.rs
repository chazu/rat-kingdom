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
    OnboardingNamedCheck, OnboardingProposal, OnboardingProposalAction, OnboardingProposalKind,
    OnboardingValidation, OnboardingVerification,
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

/// Prove an immutable proposal is executable before it can be shown for human
/// approval. This is intentionally git-only: CUE and named-check validation
/// still run after application in the isolated onboarding worktree.
pub fn preflight_proposal(
    session: &OnboardingSession,
    proposal: &OnboardingProposal,
) -> rk_core::Result<()> {
    proposal.validate_integrity()?;
    validate_supported_target(proposal)?;
    require_clean(&session.worktree)?;
    let current_tree = onboarding_tree_revision(&session.worktree)?;
    if current_tree != proposal.tree_revision {
        return Err(rk_core::Error::other(format!(
            "stale onboarding tree before proposal: proposed {}, current {current_tree}",
            proposal.tree_revision
        )));
    }
    git_with_stdin(
        &session.worktree,
        &["apply", "--check", "-"],
        &proposal.diff,
    )?;
    let output = git_with_stdin_output(
        &session.worktree,
        &["apply", "--numstat", "-z", "-"],
        &proposal.diff,
    )?;
    let paths = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
        .filter_map(|record| record.splitn(3, |byte| *byte == b'\t').nth(2))
        .map(|path| String::from_utf8_lossy(path).into_owned())
        .collect::<Vec<_>>();
    require_only_target(&paths, &proposal.target_path)
}

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
        require_proposal_base(session, proposal, "HEAD^")?;
        validate_target(&target, proposal)?;
        return application_evidence(worktree, &target, actor);
    }

    require_proposal_base(session, proposal, "HEAD")?;

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
    let result = validate_automation_file(&target, proposal);
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

    let mut command = named_check_command(contract, &cwd);

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

/// Build the exact `sh -c` boundary `verify` spawns a named check through:
/// its own process group's worth of stdio (stdin null, stdout/stderr piped,
/// `kill_on_drop`), the check's declared environment policy, and — last,
/// right before the caller spawns it — `close_extra_fds`, guarding against
/// TKT-bikuz-kumuz-zutit's inherited-descriptor race. Factored out of
/// `verify` so a test can spawn this exact command directly rather than a
/// separately constructed stand-in, which would prove nothing about
/// `verify`'s own wiring.
fn named_check_command(contract: &OnboardingNamedCheck, cwd: &Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new("sh");
    command
        .arg("-c")
        .arg(&contract.command)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if contract.environment_policy == CheckEnvironmentPolicy::StripRkSpawn {
        for name in rk_workflow::STRIPPED_RK_SPAWN_ENV {
            command.env_remove(name);
        }
    }
    rk_core::exec::close_extra_fds(command.as_std_mut());
    command
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
    if proposal.kind == OnboardingProposalKind::RepoFile
        && proposal.action == OnboardingProposalAction::WriteRepoFile
    {
        return Ok(());
    }
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
    if proposal.kind == OnboardingProposalKind::RepoFile
        && proposal.action == OnboardingProposalAction::WriteRepoFile
    {
        let metadata = std::fs::symlink_metadata(path).map_err(|error| {
            rk_core::Error::other(format!("read staged repo file {}: {error}", path.display()))
        })?;
        if !metadata.file_type().is_file() {
            return Err(rk_core::Error::other(format!(
                "staged repo-file target must be a regular file: {}",
                path.display()
            )));
        }
        return Ok(());
    }
    validate_automation_file(path, proposal).map(|_| ())
}

/// Accept the proposal's exact reviewed tree, or a descendant made solely by
/// a previously journaled application in this same onboarding session. This
/// lets several proposals approved against one assessment apply in order while
/// refusing arbitrary branch movement.
fn require_proposal_base(
    session: &OnboardingSession,
    proposal: &OnboardingProposal,
    revision: &str,
) -> rk_core::Result<()> {
    let tree = git_text(
        &session.worktree,
        &["rev-parse", &format!("{revision}^{{tree}}")],
    )?;
    if tree == proposal.tree_revision {
        return Ok(());
    }
    let commit = git_text(&session.worktree, &["rev-parse", revision])?;
    let recorded_predecessor = session.proposals.iter().any(|candidate| {
        candidate.id != proposal.id
            && candidate
                .application
                .as_ref()
                .is_some_and(|application| application.commit == commit)
    });
    let reviewed_tree_is_ancestor = git_text(&session.worktree, &["log", "--format=%T", revision])?
        .lines()
        .any(|candidate| candidate == proposal.tree_revision);
    if recorded_predecessor && reviewed_tree_is_ancestor {
        Ok(())
    } else {
        Err(rk_core::Error::other(format!(
            "stale onboarding tree before apply: proposed {}, current {tree}",
            proposal.tree_revision
        )))
    }
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
    proposal: &OnboardingProposal,
) -> rk_core::Result<String> {
    let kind = proposal.automation_kind().ok_or_else(|| {
        rk_core::Error::other(format!(
            "proposal {} has no supported automation validation contract",
            proposal.id
        ))
    })?;
    let source = std::fs::read_to_string(path)
        .map_err(|error| rk_core::Error::other(format!("read {}: {error}", path.display())))?;
    if kind != OnboardingAutomationKind::CiWorkflow {
        crate::onboarding::reject_cue_imports(&source).map_err(rk_core::Error::other)?;
    }
    match kind {
        OnboardingAutomationKind::RepositoryPolicy => {
            rk_workflow::load_repository_policy_str(&source)?;
            Ok(format!(
                "repository policy schema and safety validation passed for {}",
                path.display()
            ))
        }
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
        OnboardingAutomationKind::Hook => {
            let hooks = rk_workflow::load_hooks_str(&source)?;
            if hooks.is_empty() {
                return Err(rk_core::Error::other(format!(
                    "{} contains no hooks",
                    path.display()
                )));
            }
            Ok(format!(
                "hook schema validation passed for {} definition(s)",
                hooks.len()
            ))
        }
        OnboardingAutomationKind::CheckRegistry => {
            let contract = proposal.named_check.as_ref().ok_or_else(|| {
                rk_core::Error::other(format!(
                    "proposal {} has no digest-bound named_check contract",
                    proposal.id
                ))
            })?;
            let check = validate_contract(path, contract)?;
            Ok(format!(
                "named check `{}` contract validated for {}",
                check.name,
                path.display()
            ))
        }
        OnboardingAutomationKind::CiWorkflow => validate_ci_workflow_str(&source),
    }
}

/// Bounded, dependency-free structural check for a GitHub Actions CI
/// workflow file. This is not a YAML parser and does not run the workflow;
/// it only proves the file has the shape a CI workflow needs (top-level
/// `on:`/`jobs:` keys, at least one job declaring `runs-on:` and `steps:`),
/// so a regular-file existence check cannot stand in for CI validation. The
/// actual maintained recipe invocation a job runs is reviewed separately.
/// Real YAML syntax validation (via `serde_yaml_ng`, an in-process,
/// statically-linked parser — no external tool-availability contract to fail
/// open on) plus bounded, per-job structural and value-shape checks. This
/// does not implement GitHub Actions semantics or run the workflow; it only
/// proves the parsed document has the shape the prepared CI companion uses:
/// a top-level `on:` trigger key with a nonempty event name/list/mapping, a
/// non-empty `jobs:` mapping, and every declared job (checked on its own,
/// not by scanning the whole `jobs:` section for the right substrings)
/// declaring both a nonempty `runs-on:` (a runner label or list of labels)
/// and a nonempty `steps:` list of `run:`/`uses:` steps. Any other shape is
/// refused explicitly rather than accepted as validated structure. The
/// actual maintained recipe invocation a job runs is reviewed separately.
fn validate_ci_workflow_str(source: &str) -> rk_core::Result<String> {
    if source.trim().is_empty() {
        return Err(rk_core::Error::other("CI workflow file is empty"));
    }
    let document: serde_yaml_ng::Value = serde_yaml_ng::from_str(source).map_err(|error| {
        rk_core::Error::other(format!("CI workflow file is not valid YAML: {error}"))
    })?;
    let root = document.as_mapping().ok_or_else(|| {
        rk_core::Error::other("CI workflow file must be a YAML mapping at its top level")
    })?;

    // `serde_yaml_ng`'s scalar resolution (see its `de::parse_bool`) only
    // recognizes `true`/`True`/`TRUE` and `false`/`False`/`FALSE` as
    // booleans, not the wider YAML 1.1 `on`/`off`/`yes`/`no` set some other
    // readers implement — so with this parser an unquoted `on:` really does
    // parse as the string key `"on"`. Require that real key rather than
    // inventing an alternate spelling that would accept a literal `true:`.
    let on_value = root.get("on").ok_or_else(|| {
        rk_core::Error::other("CI workflow file has no top-level `on:` trigger key")
    })?;
    validate_trigger_shape(on_value)?;

    let jobs_value = root
        .get("jobs")
        .ok_or_else(|| rk_core::Error::other("CI workflow file has no top-level `jobs:` key"))?;
    let jobs = jobs_value.as_mapping().ok_or_else(|| {
        rk_core::Error::other("CI workflow file's `jobs:` key must be a mapping of job id to job")
    })?;
    if jobs.is_empty() {
        return Err(rk_core::Error::other(
            "CI workflow file's `jobs:` key declares no jobs",
        ));
    }
    for (job_id, job) in jobs {
        let job_id = job_id_label(job_id);
        let job = job.as_mapping().ok_or_else(|| {
            rk_core::Error::other(format!("CI workflow job `{job_id}` must be a mapping"))
        })?;
        validate_runs_on_shape(&job_id, job.get("runs-on"))?;
        validate_steps_shape(&job_id, job.get("steps"))?;
    }
    Ok(format!(
        "CI workflow YAML parsed and structurally validated: on/jobs present, {} job(s) each declaring a supported runs-on/steps shape",
        jobs.len()
    ))
}

fn job_id_label(value: &serde_yaml_ng::Value) -> String {
    value.as_str().map(str::to_string).unwrap_or_else(|| {
        serde_yaml_ng::to_string(value)
            .unwrap_or_default()
            .trim()
            .to_string()
    })
}

fn nonempty_str(value: &serde_yaml_ng::Value) -> bool {
    matches!(value, serde_yaml_ng::Value::String(text) if !text.trim().is_empty())
}

/// `on:` must be a nonempty event name, a nonempty list of event names, or a
/// nonempty event mapping — the three shapes the prepared CI companion and
/// ordinary GitHub Actions workflows use. `null`, an empty collection, or a
/// non-string/non-collection scalar is refused rather than accepted.
fn validate_trigger_shape(value: &serde_yaml_ng::Value) -> rk_core::Result<()> {
    let supported = match value {
        serde_yaml_ng::Value::Sequence(items) => {
            !items.is_empty() && items.iter().all(nonempty_str)
        }
        serde_yaml_ng::Value::Mapping(map) => !map.is_empty(),
        other => nonempty_str(other),
    };
    if supported {
        Ok(())
    } else {
        Err(rk_core::Error::other(
            "CI workflow file's `on:` trigger must be a nonempty event name, list of event names, or event mapping",
        ))
    }
}

/// `runs-on:` must be a nonempty runner label (a string, matrix expressions
/// like `${{ matrix.os }}` included) or a nonempty list of labels. `null`,
/// a bare boolean/number, or an empty value is refused: presence of the key
/// alone is not a validated shape.
fn validate_runs_on_shape(
    job_id: &str,
    value: Option<&serde_yaml_ng::Value>,
) -> rk_core::Result<()> {
    let supported = match value {
        Some(serde_yaml_ng::Value::Sequence(items)) => {
            !items.is_empty() && items.iter().all(nonempty_str)
        }
        Some(other) => nonempty_str(other),
        None => false,
    };
    if supported {
        Ok(())
    } else {
        Err(rk_core::Error::other(format!(
            "CI workflow job `{job_id}` must declare `runs-on:` as a nonempty runner label or list of labels"
        )))
    }
}

/// `steps:` must be a nonempty sequence, and every step in it a mapping
/// declaring a nonempty `run:` or `uses:` — the two step shapes the
/// prepared CI companion and ordinary GitHub Actions jobs use. A step that
/// is not a mapping, or one with neither key nonempty, is refused.
fn validate_steps_shape(job_id: &str, value: Option<&serde_yaml_ng::Value>) -> rk_core::Result<()> {
    let Some(serde_yaml_ng::Value::Sequence(steps)) = value else {
        return Err(rk_core::Error::other(format!(
            "CI workflow job `{job_id}` must declare `steps:` as a nonempty list of steps"
        )));
    };
    if steps.is_empty() {
        return Err(rk_core::Error::other(format!(
            "CI workflow job `{job_id}` must declare `steps:` as a nonempty list of steps"
        )));
    }
    for (index, step) in steps.iter().enumerate() {
        let step = step.as_mapping().ok_or_else(|| {
            rk_core::Error::other(format!(
                "CI workflow job `{job_id}` step {index} must be a mapping"
            ))
        })?;
        let run_ok = step.get("run").is_some_and(nonempty_str);
        let uses_ok = step.get("uses").is_some_and(nonempty_str);
        if !run_ok && !uses_ok {
            return Err(rk_core::Error::other(format!(
                "CI workflow job `{job_id}` step {index} must declare a nonempty `run:` or `uses:`"
            )));
        }
    }
    Ok(())
}

fn automation_validator(kind: OnboardingAutomationKind) -> &'static str {
    match kind {
        OnboardingAutomationKind::RepositoryPolicy => "rk_workflow::load_repository_policy_str",
        OnboardingAutomationKind::Workflow => "rk_workflow::validate_workflow_str",
        OnboardingAutomationKind::Trigger => "rk_workflow::load_triggers_str",
        OnboardingAutomationKind::Schedule => {
            "rk_workflow::load_schedules_str + rk_daemon::cron::Cron::parse"
        }
        OnboardingAutomationKind::Hook => "rk_workflow::load_hooks_str",
        OnboardingAutomationKind::CheckRegistry => "onboarding_apply::validate_contract",
        OnboardingAutomationKind::CiWorkflow => "onboarding_apply::validate_ci_workflow_str",
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

/// Guard `cmd` against inheriting a descriptor left open by a concurrent,
/// unrelated pipe-creating spawn elsewhere in the daemon
/// (TKT-bikuz-kumuz-zutit — see `rk_core::exec::close_extra_fds`), then
/// spawn it and wait for its captured output. The one place every synchronous
/// `git` boundary in this file routes its spawn through, so a test exercising
/// this function exercises exactly what `git_output`/`git_with_stdin_output`
/// call in production.
fn guarded_spawn(cmd: &mut Command) -> std::io::Result<std::process::Child> {
    rk_core::exec::close_extra_fds(cmd);
    cmd.spawn()
}

fn git_output(worktree: &Path, args: &[&str]) -> rk_core::Result<Output> {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(worktree).args(args).env("LC_ALL", "C");
    rk_core::exec::close_extra_fds(&mut cmd);
    let output = cmd.output()?;
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
    git_with_stdin_output(worktree, args, input).map(|_| ())
}

fn git_with_stdin_output(worktree: &Path, args: &[&str], input: &str) -> rk_core::Result<Output> {
    let mut cmd = Command::new("git");
    cmd.arg("-C")
        .arg(worktree)
        .args(args)
        .env("LC_ALL", "C")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = guarded_spawn(&mut cmd)?;
    child
        .stdin
        .take()
        .ok_or_else(|| rk_core::Error::other("git apply stdin was not piped"))?
        .write_all(input.as_bytes())?;
    let output = child.wait_with_output()?;
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

#[cfg(test)]
mod ci_workflow_tests {
    use super::validate_ci_workflow_str;

    const VALID: &str = r#"
name: CI
on:
  push:
    branches: [main]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - run: mise run verify
"#;

    #[test]
    fn accepts_a_well_formed_workflow() {
        let summary = validate_ci_workflow_str(VALID).unwrap();
        assert!(summary.contains("1 job"), "{summary}");
    }

    #[test]
    fn rejects_an_empty_file() {
        let error = validate_ci_workflow_str("   \n").unwrap_err().to_string();
        assert!(error.contains("empty"), "{error}");
    }

    #[test]
    fn rejects_invalid_yaml_syntax() {
        // An unterminated flow sequence, followed by an otherwise ordinary
        // job: a text scanner sees the `on:`/`jobs:`/`runs-on:`/`steps:`
        // substrings at the right indentation and would wrongly accept
        // this; a real parser must reject it as a syntax error.
        let source = "on: [push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps: []\n";
        let error = validate_ci_workflow_str(source).unwrap_err().to_string();
        assert!(error.contains("not valid YAML"), "{error}");
    }

    #[test]
    fn rejects_a_missing_on_key() {
        let source = "jobs:\n  test:\n    runs-on: ubuntu-latest\n    steps: []\n";
        let error = validate_ci_workflow_str(source).unwrap_err().to_string();
        assert!(error.contains("`on:`"), "{error}");
    }

    #[test]
    fn accepts_the_real_unquoted_on_key() {
        // `serde_yaml_ng`'s bool resolution only covers true/false spellings
        // (see its `de::parse_bool`), so a real workflow's unquoted `on:`
        // parses as the string key `"on"` with this parser, not a boolean.
        let summary = validate_ci_workflow_str(VALID).unwrap();
        assert!(summary.contains("on/jobs present"), "{summary}");
    }

    #[test]
    fn rejects_a_literal_true_key_as_a_stand_in_for_on() {
        // A `true:` key is not a real GitHub Actions trigger key spelling;
        // it must not be accepted as an alternate way to write `on:`.
        let source = "true:\n  push: {}\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ok\n";
        let error = validate_ci_workflow_str(source).unwrap_err().to_string();
        assert!(error.contains("`on:`"), "{error}");
    }

    #[test]
    fn rejects_an_empty_on_trigger() {
        let source =
            "on:\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ok\n";
        let error = validate_ci_workflow_str(source).unwrap_err().to_string();
        assert!(error.contains("`on:` trigger"), "{error}");
    }

    #[test]
    fn rejects_a_null_runs_on() {
        let source =
            "on: push\njobs:\n  test:\n    runs-on: null\n    steps:\n      - run: echo ok\n";
        let error = validate_ci_workflow_str(source).unwrap_err().to_string();
        assert!(error.contains("runs-on"), "{error}");
    }

    #[test]
    fn rejects_a_boolean_steps_value() {
        let source = "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps: false\n";
        let error = validate_ci_workflow_str(source).unwrap_err().to_string();
        assert!(error.contains("steps"), "{error}");
    }

    #[test]
    fn rejects_a_step_with_neither_run_nor_uses() {
        let source =
            "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - name: noop\n";
        let error = validate_ci_workflow_str(source).unwrap_err().to_string();
        assert!(error.contains("run:") && error.contains("uses:"), "{error}");
    }

    #[test]
    fn accepts_a_matrix_runs_on_expression_and_a_uses_step() {
        // The prepared CI companion's ordinary shape: a matrix expression
        // string for `runs-on:` and a `uses:` step alongside `run:` steps.
        let source = "on:\n  push: {}\njobs:\n  test:\n    runs-on: \"${{ matrix.os }}\"\n    steps:\n      - uses: actions/checkout@v4\n      - run: mise run verify\n";
        let summary = validate_ci_workflow_str(source).unwrap();
        assert!(summary.contains("1 job"), "{summary}");
    }

    #[test]
    fn rejects_a_missing_jobs_key() {
        let source = "on:\n  push: {}\n";
        let error = validate_ci_workflow_str(source).unwrap_err().to_string();
        assert!(error.contains("`jobs:`"), "{error}");
    }

    #[test]
    fn rejects_a_job_missing_runs_on_or_steps() {
        let source = "on:\n  push: {}\njobs:\n  test:\n    steps: []\n";
        let error = validate_ci_workflow_str(source).unwrap_err().to_string();
        assert!(error.contains("runs-on"), "{error}");
    }

    #[test]
    fn rejects_runs_on_and_steps_split_across_different_jobs() {
        // Two jobs, each individually incomplete: one has only `runs-on:`,
        // the other only `steps:`. A global "does this string occur
        // somewhere in the jobs section" check wrongly accepts this; each
        // job must be checked on its own.
        let source = "on:\n  push: {}\njobs:\n  build:\n    runs-on: ubuntu-latest\n  test:\n    steps: []\n";
        let error = validate_ci_workflow_str(source).unwrap_err().to_string();
        assert!(
            error.contains("job `build`") || error.contains("job `test`"),
            "{error}"
        );
        assert!(
            error.contains("runs-on") || error.contains("steps"),
            "{error}"
        );
    }

    /// Exact adversarial input from the reviewed reproduction
    /// (`ci-invalid-yaml-example.yml`): an unterminated flow sequence
    /// followed by an otherwise ordinary job.
    #[test]
    fn rejects_the_reviewed_unterminated_flow_sequence_reproduction() {
        let source = "on: [unterminated\njobs:\n  check:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ok\n";
        let error = validate_ci_workflow_str(source).unwrap_err().to_string();
        assert!(error.contains("not valid YAML"), "{error}");
    }

    /// Exact adversarial input from the reviewed reproduction
    /// (`ci-split-invalid-jobs-example.yml`): `runs-on` on one job, `steps`
    /// on a different job, neither job complete on its own.
    #[test]
    fn rejects_the_reviewed_split_jobs_reproduction() {
        let source = "on: push\njobs:\n  first:\n    runs-on: ubuntu-latest\n  second:\n    steps:\n      - run: echo ok\n";
        let error = validate_ci_workflow_str(source).unwrap_err().to_string();
        assert!(
            error.contains("job `first`") || error.contains("job `second`"),
            "{error}"
        );
    }

    #[test]
    fn regular_file_existence_alone_is_not_ci_validation() {
        // A file that exists, is non-empty, and is valid YAML, but has none
        // of the required structure, must still be refused.
        let error = validate_ci_workflow_str("just some text\n")
            .unwrap_err()
            .to_string();
        assert!(error.contains("YAML mapping"), "{error}");
    }
}

#[cfg(all(test, unix))]
mod fd_guard_tests {
    use super::*;
    use std::time::Instant;

    /// How long a test child is given before it's treated as wedged and
    /// killed rather than left to hang the suite — these commands are
    /// trivial (`echo`/`cat`) and should return in milliseconds.
    const TEST_CHILD_BOUND: Duration = Duration::from_secs(5);

    /// Opens a real, deliberately non-close-on-exec pipe — the exact state a
    /// pipe is briefly in between `pipe()` and `fcntl(F_SETFD, FD_CLOEXEC)`
    /// on macOS (no `pipe2`), where TKT-bikuz-kumuz-zutit was diagnosed.
    /// Same methodology as `rk_core::exec`'s own `close_extra_fds` tests.
    fn leaky_pipe() -> (i32, i32) {
        let mut fds = [0i32; 2];
        assert_eq!(
            unsafe { libc::pipe(fds.as_mut_ptr()) },
            0,
            "pipe(2) failed: {}",
            std::io::Error::last_os_error()
        );
        (fds[0], fds[1])
    }

    /// Bounded wait for a real `std::process::Child`, killing and reaping it
    /// on overrun instead of leaving an unbounded `wait_with_output` in a
    /// test — `guarded_spawn` itself has no timeout of its own (that's
    /// `verify`'s `contract.timeout`/`tokio::time::timeout`, unused when a
    /// test calls the sync helper directly), so this test owns its own
    /// finite bound and cleans up the child rather than trusting it to exit.
    fn wait_bounded_or_kill(mut child: std::process::Child, timeout: Duration) -> Output {
        let deadline = Instant::now() + timeout;
        loop {
            if let Ok(Some(_)) = child.try_wait() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!(
                    "guarded_spawn test child exceeded its {}s bound and was killed",
                    timeout.as_secs()
                );
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        child.wait_with_output().unwrap()
    }

    /// TKT-nivab-fazoz-hajol: `named_check_command` — the exact function
    /// `verify` calls to build its `sh -c` named-check spawn — must not let
    /// a descriptor left open by a concurrent, unrelated pipe-creating spawn
    /// elsewhere in the daemon survive into its child (the
    /// TKT-bikuz-kumuz-zutit race `close_extra_fds` closes), while its own
    /// intended captured output still comes through untouched. This calls
    /// the real production helper — a contract whose `command` field probes
    /// the leaked fd, not a separately constructed stand-in that would prove
    /// nothing about `verify`'s own wiring.
    #[tokio::test]
    async fn named_check_command_hides_a_leaked_descriptor_and_still_captures_output() {
        let (leak_r, leak_w) = leaky_pipe();
        let cwd = std::env::temp_dir();
        let contract = OnboardingNamedCheck {
            name: "fd-probe".into(),
            command: format!("if [ -e /dev/fd/{leak_w} ]; then echo LEAKED; else echo SAFE; fi"),
            cwd: ".".into(),
            expect_exit: 0,
            timeout: "5s".into(),
            environment_policy: CheckEnvironmentPolicy::Inherit,
            toolchain: "none".into(),
        };

        let mut command = named_check_command(&contract, &cwd);
        let child = command.spawn().unwrap();
        // `named_check_command` sets `kill_on_drop(true)`, so a timeout here
        // drops (and thereby kills/reaps) the child rather than leaking it.
        let output = tokio::time::timeout(TEST_CHILD_BOUND, child.wait_with_output())
            .await
            .expect("named_check_command test child exceeded its bound and was killed")
            .unwrap();

        unsafe {
            libc::close(leak_r);
            libc::close(leak_w);
        }

        assert!(output.status.success());
        assert_eq!(
            String::from_utf8_lossy(&output.stdout).trim(),
            "SAFE",
            "a guarded onboarding named-check child observed a descriptor it was never given"
        );
    }

    /// Materially distinct boundary from the output-only named-check spawn
    /// above: `guarded_spawn` is the exact function `git_with_stdin_output`
    /// routes its spawn through, and that boundary writes to the child's
    /// stdin concurrently with capturing its output. `git` itself cannot
    /// self-report its own fd table, so this exercises `guarded_spawn`
    /// directly (the real helper, not a copy) with an `sh` child standing in
    /// for `git`, proving the same guarantee holds at a stdin-piping
    /// boundary: no leaked descriptor survives, and real data written to
    /// stdin still round-trips through the captured stdout correctly.
    #[test]
    fn guarded_spawn_hides_a_leaked_descriptor_and_still_pipes_stdin() {
        let (leak_r, leak_w) = leaky_pipe();

        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg(format!(
                "if [ -e /dev/fd/{leak_w} ]; then echo LEAKED; else cat; fi"
            ))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = guarded_spawn(&mut cmd).unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"round-trip-me")
            .unwrap();
        let output = wait_bounded_or_kill(child, TEST_CHILD_BOUND);

        unsafe {
            libc::close(leak_r);
            libc::close(leak_w);
        }

        assert!(output.status.success());
        assert_eq!(
            String::from_utf8_lossy(&output.stdout),
            "round-trip-me",
            "a guarded stdin-piping child either leaked a descriptor or lost its piped stdin"
        );
    }
}
