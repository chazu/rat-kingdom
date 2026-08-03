//! Repo subcommands: register repositories so the system knows where they live.

use anyhow::{bail, Result};
use rk_core::paths::Layout;
use rk_daemon::onboarding::AssessmentReport;
use rk_daemon::onboarding_proposals::OnboardingProposal;
use rk_daemon::onboarding_sessions::{OnboardingReport, OnboardingSessionStatus};
use rk_daemon::Client;
use serde_json::json;

pub async fn add(
    layout: &Layout,
    path: String,
    name: Option<String>,
    merge_mode: Option<String>,
    remote: Option<String>,
    as_json: bool,
) -> Result<()> {
    let canonical = std::fs::canonicalize(&path)
        .map_err(|e| anyhow::anyhow!("cannot resolve path {path}: {e}"))?;
    let name = match name {
        Some(n) => n,
        None => canonical
            .file_name()
            .and_then(|s| s.to_str())
            .map(str::to_string)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "cannot infer a name from {}; pass --name",
                    canonical.display()
                )
            })?,
    };
    let mut params = json!({ "name": name, "path": canonical.to_string_lossy() });
    if let Some(mode) = merge_mode {
        params["merge_mode"] = json!(mode);
    }
    if let Some(remote) = remote {
        params["remote"] = json!(remote);
    }
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("repo.add", params).await?;
    let repo = &result["repo"];
    if as_json {
        println!("{repo}");
    } else {
        println!(
            "registered {} → {} ({} mode)",
            repo["name"].as_str().unwrap_or("?"),
            repo["path"].as_str().unwrap_or("?"),
            repo["merge_mode"].as_str().unwrap_or("direct")
        );
    }
    Ok(())
}

pub async fn list(layout: &Layout, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("repo.list", json!({})).await?;
    if as_json {
        println!("{}", result["repos"]);
        return Ok(());
    }
    let repos = result["repos"].as_array().cloned().unwrap_or_default();
    if repos.is_empty() {
        println!("(no repos registered — rk repo add <path>)");
        return Ok(());
    }
    println!("{:<16} PATH", "NAME");
    for r in &repos {
        println!(
            "{:<16} {}",
            r["name"].as_str().unwrap_or("?"),
            r["path"].as_str().unwrap_or("?")
        );
    }
    Ok(())
}

pub async fn show(layout: &Layout, name: String, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client.call("repo.get", json!({ "name": name })).await?;
    if result["repo"].is_null() {
        if as_json {
            println!("{}", json!({ "repo": null }));
            return Ok(());
        }
        bail!("no such repo: {name} (rk repo add <path>)");
    }
    let repo = &result["repo"];
    if as_json {
        println!("{repo}");
        return Ok(());
    }
    println!("{}", repo["name"].as_str().unwrap_or("?"));
    println!("  path       {}", repo["path"].as_str().unwrap_or("?"));
    println!(
        "  registered {}",
        repo["created_at"].as_str().unwrap_or("?")
    );
    println!(
        "  merge      {}",
        repo["merge_mode"].as_str().unwrap_or("direct")
    );
    println!(
        "  remote     {}",
        repo["remote"].as_str().unwrap_or("origin")
    );
    if let Some(host) = repo["host"].as_str() {
        println!("  host       {host}");
    }
    // Show its open tickets as a convenience.
    let tickets = client
        .call("ticket.list", json!({ "scope": name, "status": "open" }))
        .await?;
    let tickets = tickets["tickets"].as_array().cloned().unwrap_or_default();
    if !tickets.is_empty() {
        println!("\nopen tickets:");
        for t in &tickets {
            println!(
                "  {:<8} {}",
                t["identity"].as_str().unwrap_or("?"),
                t["payload"]["title"].as_str().unwrap_or("")
            );
        }
    }
    Ok(())
}

pub async fn onboard_inspect(layout: &Layout, target: String, as_json: bool) -> Result<()> {
    // Inspection is a strict read-only path: unlike ordinary convenience
    // commands, it must not auto-start a daemon (which would write the castle
    // socket, pid, logs, and identity files).
    let mut client = Client::connect(layout).await?;
    let result = client
        .call("repo.onboard.inspect", json!({ "target": target }))
        .await?;
    let report: AssessmentReport = serde_json::from_value(result["report"].clone())?;

    if as_json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        print_assessment(&report);
    }
    if !report.ready {
        bail!("repository assessment failed closed; resolve the error findings above");
    }
    Ok(())
}

pub async fn onboard_start(
    layout: &Layout,
    target: String,
    harness: Option<String>,
    model: Option<String>,
    attach: bool,
    as_json: bool,
) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "repo.onboard.start",
            json!({
                "target": target,
                "harness": harness,
                "model": model,
                "attach": attach,
            }),
        )
        .await?;
    if as_json {
        println!("{result}");
    } else {
        let status: OnboardingSessionStatus = serde_json::from_value(result["session"].clone())?;
        print_onboarding_status(&status, result["reused"].as_bool().unwrap_or(false));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn onboard_propose(
    layout: &Layout,
    session: String,
    kind: String,
    title: String,
    evidence: Vec<String>,
    target_path: String,
    action: String,
    diff: String,
    risk: String,
    verification: Vec<String>,
    check_name: Option<String>,
    check_command: Option<String>,
    check_cwd: Option<String>,
    check_expect_exit: Option<i64>,
    check_timeout: Option<String>,
    check_environment_policy: Option<String>,
    check_toolchain: Option<String>,
    as_json: bool,
) -> Result<()> {
    let named_check = match check_name {
        Some(name) => Some(json!({
            "name": name,
            "command": check_command
                .ok_or_else(|| anyhow::anyhow!("--check-command is required with --check-name"))?,
            "cwd": check_cwd
                .ok_or_else(|| anyhow::anyhow!("--check-cwd is required with --check-name"))?,
            "expect_exit": check_expect_exit
                .ok_or_else(|| anyhow::anyhow!("--check-expect-exit is required with --check-name"))?,
            "timeout": check_timeout
                .ok_or_else(|| anyhow::anyhow!("--check-timeout is required with --check-name"))?,
            "environment_policy": check_environment_policy
                .ok_or_else(|| anyhow::anyhow!("--check-environment-policy is required with --check-name"))?,
            "toolchain": check_toolchain
                .ok_or_else(|| anyhow::anyhow!("--check-toolchain is required with --check-name"))?,
        })),
        None => {
            if check_command.is_some()
                || check_cwd.is_some()
                || check_expect_exit.is_some()
                || check_timeout.is_some()
                || check_environment_policy.is_some()
                || check_toolchain.is_some()
            {
                bail!("--check-name is required when supplying named-check fields");
            }
            None
        }
    };
    let mut client = Client::connect(layout).await?;
    let result = client
        .call(
            "repo.onboard.propose",
            json!({
                "session": session,
                "proposal": {
                    "kind": kind,
                    "title": title,
                    "evidence": evidence,
                    "target_path": target_path,
                    "action": action,
                    "diff": diff,
                    "risk": risk,
                    "verification": verification,
                    "named_check": named_check,
                },
            }),
        )
        .await?;
    let proposal: OnboardingProposal = serde_json::from_value(result["proposal"].clone())?;
    if as_json {
        println!("{result}");
    } else {
        print_onboarding_proposal(&proposal);
        println!(
            "  journal   {}",
            if result["created"].as_bool().unwrap_or(false) {
                "created"
            } else {
                "already present"
            }
        );
    }
    Ok(())
}

pub async fn onboard_apply(
    layout: &Layout,
    session: String,
    proposal: String,
    digest: String,
    as_json: bool,
) -> Result<()> {
    let mut client = Client::connect(layout).await?;
    let result = client
        .call(
            "repo.onboard.apply",
            json!({
                "session": session,
                "proposal": proposal,
                "digest": digest,
            }),
        )
        .await?;
    let proposal: OnboardingProposal = serde_json::from_value(result["proposal"].clone())?;
    if as_json {
        println!("{result}");
    } else {
        print_onboarding_proposal(&proposal);
        println!(
            "  apply     {}",
            if result["applied"].as_bool().unwrap_or(false) {
                "committed"
            } else {
                "already committed (idempotent)"
            }
        );
        println!(
            "  verify    {}",
            if result["verified"].as_bool().unwrap_or(false) {
                "passed"
            } else {
                "failed"
            }
        );
    }
    if !result["verified"].as_bool().unwrap_or(false)
        && proposal.status != rk_daemon::onboarding_proposals::OnboardingProposalStatus::Verified
    {
        bail!("onboarding named check did not pass; see the durable report");
    }
    Ok(())
}

pub async fn onboard_activate(
    layout: &Layout,
    session: String,
    proposal: String,
    digest: String,
    as_json: bool,
) -> Result<()> {
    let mut client = Client::connect(layout).await?;
    let result = client
        .call(
            "repo.onboard.activate",
            json!({
                "session": session,
                "proposal": proposal,
                "digest": digest,
            }),
        )
        .await?;
    let proposal: OnboardingProposal = serde_json::from_value(result["proposal"].clone())?;
    if as_json {
        println!("{result}");
    } else {
        print_onboarding_proposal(&proposal);
        println!(
            "  activation {}",
            if result["changed"].as_bool().unwrap_or(false) {
                "landed in registered checkout"
            } else {
                "already landed (idempotent replay)"
            }
        );
    }
    Ok(())
}

pub async fn onboard_decline_activation(
    layout: &Layout,
    session: String,
    proposal: String,
    digest: String,
    reason: Option<String>,
    as_json: bool,
) -> Result<()> {
    let mut client = Client::connect(layout).await?;
    let result = client
        .call(
            "repo.onboard.decline_activation",
            json!({
                "session": session,
                "proposal": proposal,
                "digest": digest,
                "reason": reason,
            }),
        )
        .await?;
    let proposal: OnboardingProposal = serde_json::from_value(result["proposal"].clone())?;
    if as_json {
        println!("{result}");
    } else {
        print_onboarding_proposal(&proposal);
        println!(
            "  activation {}",
            if result["changed"].as_bool().unwrap_or(false) {
                "declined"
            } else {
                "already declined (idempotent)"
            }
        );
    }
    Ok(())
}

pub async fn onboard_cleanup(layout: &Layout, session: String, as_json: bool) -> Result<()> {
    let mut client = Client::connect(layout).await?;
    let result = client
        .call("repo.onboard.cleanup", json!({"session": session}))
        .await?;
    if as_json {
        println!("{result}");
    } else {
        let status: OnboardingSessionStatus = serde_json::from_value(result["session"].clone())?;
        print_onboarding_status(&status, true);
        println!(
            "  cleanup   {}",
            if result["cleaned"].as_bool().unwrap_or(false) {
                "worktree removed; branch and report retained"
            } else {
                "already complete (idempotent)"
            }
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn onboard_decide(
    layout: &Layout,
    session: String,
    proposal: String,
    digest: String,
    reason: Option<String>,
    approve: bool,
    as_json: bool,
) -> Result<()> {
    let method = if approve {
        "repo.onboard.approve"
    } else {
        "repo.onboard.decline"
    };
    let mut client = Client::connect(layout).await?;
    let result = client
        .call(
            method,
            json!({
                "session": session,
                "proposal": proposal,
                "digest": digest,
                "reason": reason,
            }),
        )
        .await?;
    let proposal: OnboardingProposal = serde_json::from_value(result["proposal"].clone())?;
    if as_json {
        println!("{result}");
    } else {
        print_onboarding_proposal(&proposal);
        println!(
            "  decision  {}",
            if result["changed"].as_bool().unwrap_or(false) {
                "recorded"
            } else {
                "already recorded (idempotent)"
            }
        );
    }
    Ok(())
}

pub async fn onboard_resume(
    layout: &Layout,
    session: String,
    attach: bool,
    as_json: bool,
) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call(
            "repo.onboard.resume",
            json!({"session": session, "attach": attach}),
        )
        .await?;
    if as_json {
        println!("{result}");
    } else {
        let status: OnboardingSessionStatus = serde_json::from_value(result["session"].clone())?;
        print_onboarding_status(&status, result["reused"].as_bool().unwrap_or(false));
    }
    Ok(())
}

pub async fn onboard_status(layout: &Layout, session: String, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call("repo.onboard.status", json!({"session": session}))
        .await?;
    let status: OnboardingSessionStatus = serde_json::from_value(result["session"].clone())?;
    if as_json {
        println!("{}", serde_json::to_string(&status)?);
    } else {
        print_onboarding_status(&status, true);
    }
    Ok(())
}

pub async fn onboard_report(layout: &Layout, session: String, as_json: bool) -> Result<()> {
    let mut client = Client::connect_or_spawn(layout).await?;
    let result = client
        .call("repo.onboard.report", json!({"session": session}))
        .await?;
    let report: OnboardingReport = serde_json::from_value(result["report"].clone())?;
    if as_json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        print_onboarding_status(&report.session, true);
        println!();
        print_assessment(&report.assessment);
        println!("\nproposal summary:");
        println!("  staged     {}", report.summary.staged.join(", "));
        println!("  verified   {}", report.summary.verified.join(", "));
        println!("  activated  {}", report.summary.activated.join(", "));
        println!("  declined   {}", report.summary.declined.join(", "));
        println!("  failed     {}", report.summary.failed.join(", "));
        println!("  unresolved {}", report.summary.unresolved.join(", "));
        if let Some(cleanup) = report.cleanup {
            println!(
                "\ncleanup:\n  worktree removed at {} by {}; branch retained as {}",
                cleanup.at, cleanup.actor, cleanup.branch_retained
            );
        }
        if let Some(result) = report.agent_result {
            println!("\nonboarder result:\n  {result}");
        }
    }
    Ok(())
}

fn print_onboarding_status(status: &OnboardingSessionStatus, reused: bool) {
    println!("onboarding session: {}", status.id);
    println!("  state     {:?}", status.state);
    println!(
        "  repo      {} ({})",
        status.repo_name,
        status.repo_path.display()
    );
    println!("  branch    {}", status.branch);
    println!("  worktree  {}", status.worktree.display());
    println!("  harness   {}", status.harness);
    if let Some(model) = &status.model {
        println!("  model     {model}");
    }
    println!(
        "  mode      {}",
        if status.attached {
            "attached"
        } else {
            "headless"
        }
    );
    if let Some(agent) = &status.agent {
        println!("  agent     {agent}");
    }
    if let Some(target) = &status.attach_target {
        println!("  attach    rk attach {target}");
    }
    if reused {
        println!("  resume    rk repo onboard resume {}", status.id);
    }
    if status.proposals.is_empty() {
        println!("  proposals none");
    } else {
        println!("\nproposals:");
        for proposal in &status.proposals {
            print_onboarding_proposal(proposal);
        }
    }
}

fn print_onboarding_proposal(proposal: &OnboardingProposal) {
    println!("  {} [{}] {}", proposal.id, proposal.status, proposal.title);
    println!("    kind/action  {} / {}", proposal.kind, proposal.action);
    println!("    target       {}", proposal.target_path);
    println!("    risk         {}", proposal.risk);
    println!("    repository   {}", proposal.repository_identity);
    println!("    tree         {}", proposal.tree_revision);
    println!("    digest       {}", proposal.digest);
    println!(
        "    proposed by  {} at {}",
        proposal.proposer, proposal.proposed_at
    );
    println!("    evidence:");
    for evidence in &proposal.evidence {
        println!("      - {evidence}");
    }
    println!("    diff:");
    for line in proposal.diff.lines() {
        println!("      {line}");
    }
    println!("    verification:");
    for verification in &proposal.verification {
        println!("      - {verification}");
    }
    if let Some(check) = &proposal.named_check {
        println!("    named check:");
        println!("      name         {}", check.name);
        println!("      command      {}", check.command);
        println!("      cwd          {}", check.cwd);
        println!("      expect exit  {}", check.expect_exit);
        println!("      timeout      {}", check.timeout);
        println!("      environment  {}", check.environment_policy);
        println!("      toolchain    {}", check.toolchain);
    }
    if let (Some(actor), Some(at)) = (&proposal.decision_actor, proposal.decision_at) {
        println!("    decision     {actor} at {at}");
    }
    if let Some(reason) = &proposal.decision_reason {
        println!("    reason       {reason}");
    }
    if let Some(failure) = &proposal.failure {
        println!("    failure      {failure}");
    }
    if let Some(application) = &proposal.application {
        println!(
            "    application  commit {} tree {}",
            application.commit, application.tree_revision
        );
    }
    for verification in &proposal.verification_results {
        println!(
            "    check attempt {}  {} (exit {}, environment {}, toolchain {})",
            verification.attempt,
            if verification.passed {
                "passed"
            } else {
                "failed"
            },
            verification
                .exit_status
                .map(|exit| exit.to_string())
                .unwrap_or_else(|| "none".into()),
            verification.environment_policy,
            verification.toolchain
        );
        println!("      output       {}", verification.output_summary);
        for risk in &verification.unresolved_risks {
            println!("      unresolved   {risk}");
        }
    }
    for validation in &proposal.validation_results {
        println!(
            "    validation {} attempt {}  {} ({})",
            validation.automation_kind,
            validation.attempt,
            if validation.passed {
                "passed"
            } else {
                "failed"
            },
            validation.validator
        );
        println!("      target digest {}", validation.target_digest);
        println!("      output        {}", validation.output_summary);
        for risk in &validation.unresolved_risks {
            println!("      unresolved    {risk}");
        }
    }
    if let Some(activation) = &proposal.activation {
        println!(
            "    activation    {} (operation {}, attempts {})",
            activation.status, activation.operation_id, activation.attempts
        );
        println!("      approved      {}", activation.approved_commit);
        println!("      expected base {}", activation.expected_base_commit);
        if let Some(commit) = &activation.registered_commit {
            println!("      registered    {commit}");
        }
        if let Some(detail) = &activation.detail {
            println!("      detail        {detail}");
        }
    }
    if proposal.status == rk_daemon::onboarding_proposals::OnboardingProposalStatus::Proposed {
        println!(
            "    approve      rk repo onboard approve {} {} --digest {}",
            proposal.session_id, proposal.id, proposal.digest
        );
        println!(
            "    decline      rk repo onboard decline {} {} --digest {}",
            proposal.session_id, proposal.id, proposal.digest
        );
    }
    if proposal.status == rk_daemon::onboarding_proposals::OnboardingProposalStatus::Approved
        || proposal.status == rk_daemon::onboarding_proposals::OnboardingProposalStatus::Failed
    {
        println!(
            "    apply        rk repo onboard apply {} {} --digest {}",
            proposal.session_id, proposal.id, proposal.digest
        );
    }
    if proposal.status == rk_daemon::onboarding_proposals::OnboardingProposalStatus::Verified
        && proposal.automation_kind().is_some()
        && proposal.activation.as_ref().is_none_or(|activation| {
            activation.status
                == rk_daemon::onboarding_proposals::OnboardingActivationStatus::Failed
        })
    {
        println!(
            "    activate     rk repo onboard activate {} {} --digest {}",
            proposal.session_id, proposal.id, proposal.digest
        );
        println!(
            "    decline      rk repo onboard decline-activation {} {} --digest {}",
            proposal.session_id, proposal.id, proposal.digest
        );
    }
}

fn print_assessment(report: &AssessmentReport) {
    let identity = report
        .identity
        .registered_name
        .as_deref()
        .unwrap_or(&report.identity.target);
    let path = report
        .identity
        .canonical_path
        .as_deref()
        .unwrap_or("(unresolved)");
    println!("repository assessment: {identity}");
    println!("  path   {path}");
    println!(
        "  ready  {}",
        if report.ready {
            "yes"
        } else {
            "no (fail-closed)"
        }
    );
    println!("  schema {}", report.schema_version);
    println!("\nfindings:");
    for finding in &report.findings {
        println!(
            "  {:<7} {:<28} {}",
            finding.severity.to_string().to_uppercase(),
            finding.kind,
            finding.summary
        );
        for evidence in &finding.evidence {
            if let Some(command) = &evidence.command {
                println!(
                    "           evidence [{}] {}: {} — `{}`",
                    evidence.origin, evidence.source, evidence.detail, command
                );
            } else {
                println!(
                    "           evidence [{}] {}: {}",
                    evidence.origin, evidence.source, evidence.detail
                );
            }
        }
        if let Some(ambiguity) = &finding.unresolved_ambiguity {
            println!("           unresolved ambiguity: {ambiguity}");
        }
        if let Some(recommendation) = &finding.recommendation {
            if let Some(command) = &recommendation.command {
                println!(
                    "           recommendation [{}]: {} — `{}`",
                    recommendation.origin, recommendation.action, command
                );
            } else {
                println!(
                    "           recommendation [{}]: {}",
                    recommendation.origin, recommendation.action
                );
            }
        }
    }
}

/// Resolve a repo argument that may be either a filesystem path or a registered
/// repo name into an absolute path string. Used by `rk spawn`.
pub async fn resolve_path(client: &mut Client, repo: &str) -> Result<String> {
    if let Ok(path) = std::fs::canonicalize(repo) {
        return Ok(path.to_string_lossy().into_owned());
    }
    let result = client.call("repo.get", json!({ "name": repo })).await?;
    if let Some(path) = result["repo"]["path"].as_str() {
        return Ok(path.to_string());
    }
    bail!("'{repo}' is neither a path nor a registered repo (rk repo add <path>)");
}
