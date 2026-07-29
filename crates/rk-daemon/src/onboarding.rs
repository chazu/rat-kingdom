//! Deterministic, read-only repository onboarding assessment.
//!
//! This module deliberately stops at observation. It does not register a
//! repository, launch a harness, execute a project check, or edit either the
//! checkout or the castle. Git is invoked with optional locks disabled and CUE
//! definitions are validated through stdin.

use crate::cron::Cron;
use crate::repos::RepoRecord;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const REPORT_SCHEMA_VERSION: u32 = 1;
const MAX_DEFINITION_BYTES: u64 = 1024 * 1024;
const MAX_EVIDENCE_ROWS: usize = 64;

#[derive(Debug, Clone)]
pub struct InspectContext {
    pub default_harness: String,
    pub require_named_checks: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssessmentReport {
    pub schema_version: u32,
    pub identity: RepositoryIdentity,
    pub ready: bool,
    pub findings: Vec<Finding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryIdentity {
    pub target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registered_name: Option<String>,
    #[serde(default)]
    pub registered_aliases: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finding {
    pub kind: FindingKind,
    pub severity: Severity,
    pub summary: String,
    #[serde(default)]
    pub evidence: Vec<Evidence>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unresolved_ambiguity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommendation: Option<Recommendation>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Evidence {
    pub origin: EvidenceOrigin,
    pub source: String,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Recommendation {
    pub origin: EvidenceOrigin,
    pub action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceOrigin {
    Observed,
    Inferred,
}

impl fmt::Display for EvidenceOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Observed => f.write_str("observed"),
            Self::Inferred => f.write_str("inferred"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    Error,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Info => f.write_str("info"),
            Self::Warning => f.write_str("warning"),
            Self::Error => f.write_str("error"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingKind {
    IdentityResolved,
    IdentityUnregistered,
    IdentityUnresolved,
    IdentityAmbiguous,
    GitNotRepository,
    GitStateClean,
    GitStateDirty,
    GitStateUnborn,
    GitStateDetached,
    GitRemotePresent,
    GitRemoteMissing,
    GitBaseDetected,
    GitBaseAmbiguous,
    GitBaseMissing,
    GitSubmodulePresent,
    GitLfsPresent,
    InstructionsPresent,
    InstructionsMissing,
    ToolchainEntrypoint,
    ToolchainInferred,
    ToolchainMissing,
    ToolMissing,
    NamedChecksPresent,
    NamedChecksMissing,
    WorkflowDefinition,
    TriggerDefinition,
    ScheduleDefinition,
    CueMalformed,
    RkReady,
    HarnessReady,
    HarnessMissing,
}

impl FindingKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IdentityResolved => "identity_resolved",
            Self::IdentityUnregistered => "identity_unregistered",
            Self::IdentityUnresolved => "identity_unresolved",
            Self::IdentityAmbiguous => "identity_ambiguous",
            Self::GitNotRepository => "git_not_repository",
            Self::GitStateClean => "git_state_clean",
            Self::GitStateDirty => "git_state_dirty",
            Self::GitStateUnborn => "git_state_unborn",
            Self::GitStateDetached => "git_state_detached",
            Self::GitRemotePresent => "git_remote_present",
            Self::GitRemoteMissing => "git_remote_missing",
            Self::GitBaseDetected => "git_base_detected",
            Self::GitBaseAmbiguous => "git_base_ambiguous",
            Self::GitBaseMissing => "git_base_missing",
            Self::GitSubmodulePresent => "git_submodule_present",
            Self::GitLfsPresent => "git_lfs_present",
            Self::InstructionsPresent => "instructions_present",
            Self::InstructionsMissing => "instructions_missing",
            Self::ToolchainEntrypoint => "toolchain_entrypoint",
            Self::ToolchainInferred => "toolchain_inferred",
            Self::ToolchainMissing => "toolchain_missing",
            Self::ToolMissing => "tool_missing",
            Self::NamedChecksPresent => "named_checks_present",
            Self::NamedChecksMissing => "named_checks_missing",
            Self::WorkflowDefinition => "workflow_definition",
            Self::TriggerDefinition => "trigger_definition",
            Self::ScheduleDefinition => "schedule_definition",
            Self::CueMalformed => "cue_malformed",
            Self::RkReady => "rk_ready",
            Self::HarnessReady => "harness_ready",
            Self::HarnessMissing => "harness_missing",
        }
    }
}

impl fmt::Display for FindingKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Finding {
    fn new(kind: FindingKind, severity: Severity, summary: impl Into<String>) -> Self {
        Self {
            kind,
            severity,
            summary: summary.into(),
            evidence: Vec::new(),
            unresolved_ambiguity: None,
            recommendation: None,
        }
    }

    fn evidence(mut self, evidence: impl IntoIterator<Item = Evidence>) -> Self {
        self.evidence.extend(evidence);
        self
    }

    fn ambiguity(mut self, ambiguity: impl Into<String>) -> Self {
        self.unresolved_ambiguity = Some(ambiguity.into());
        self
    }

    fn recommend(mut self, action: impl Into<String>, command: Option<String>) -> Self {
        self.recommendation = Some(Recommendation {
            origin: EvidenceOrigin::Inferred,
            action: action.into(),
            command,
        });
        self
    }
}

fn observed(source: impl Into<String>, detail: impl Into<String>) -> Evidence {
    Evidence {
        origin: EvidenceOrigin::Observed,
        source: source.into(),
        detail: detail.into(),
        command: None,
    }
}

fn command_evidence(
    origin: EvidenceOrigin,
    source: impl Into<String>,
    detail: impl Into<String>,
    command: impl Into<String>,
) -> Evidence {
    Evidence {
        origin,
        source: source.into(),
        detail: detail.into(),
        command: Some(command.into()),
    }
}

/// Inspect `target`, which may be a registered repository name or a filesystem
/// path. All failures are represented as findings so JSON consumers receive a
/// stable report shape even when the target is not ready.
pub fn inspect(
    target: &str,
    registered: &[RepoRecord],
    context: &InspectContext,
) -> AssessmentReport {
    let mut report = AssessmentReport {
        schema_version: REPORT_SCHEMA_VERSION,
        identity: RepositoryIdentity {
            target: target.to_string(),
            canonical_path: None,
            registered_name: None,
            registered_aliases: Vec::new(),
        },
        ready: false,
        findings: Vec::new(),
    };

    add_rk_readiness(&mut report);
    add_harness_readiness(&mut report, &context.default_harness);

    let selected = registered.iter().find(|repo| repo.name == target);
    let input_path = selected
        .map(|repo| repo.path.clone())
        .unwrap_or_else(|| PathBuf::from(target));
    let canonical_input = match std::fs::canonicalize(&input_path) {
        Ok(path) => path,
        Err(error) => {
            report.findings.push(
                Finding::new(
                    FindingKind::IdentityUnresolved,
                    Severity::Error,
                    "repository target could not be resolved",
                )
                .evidence([observed(
                    "filesystem",
                    format!("{}: {error}", input_path.display()),
                )])
                .ambiguity("the target is neither a resolvable path nor a registered name")
                .recommend(
                    "Pass an existing repository path or inspect the castle registry with `rk repo list`.",
                    Some("rk repo list".into()),
                ),
            );
            return finish(report);
        }
    };

    if !command_exists("git", &canonical_input) {
        report.identity.canonical_path = Some(canonical_input.to_string_lossy().into_owned());
        report.findings.push(
            Finding::new(
                FindingKind::ToolMissing,
                Severity::Error,
                "required inspection tool `git` is unavailable",
            )
            .evidence([observed("PATH", "git was not found")])
            .recommend("Install git and rerun the assessment.", None),
        );
        return finish(report);
    }

    let top_level = match git_text(&canonical_input, &["rev-parse", "--show-toplevel"]) {
        Ok(path) => match std::fs::canonicalize(path.trim()) {
            Ok(path) => path,
            Err(error) => {
                report.identity.canonical_path =
                    Some(canonical_input.to_string_lossy().into_owned());
                report.findings.push(
                    Finding::new(
                        FindingKind::GitNotRepository,
                        Severity::Error,
                        "git reported a worktree root that could not be canonicalized",
                    )
                    .evidence([observed("git rev-parse --show-toplevel", error.to_string())])
                    .recommend("Repair the worktree path and rerun the assessment.", None),
                );
                return finish(report);
            }
        },
        Err(error) => {
            report.identity.canonical_path = Some(canonical_input.to_string_lossy().into_owned());
            report.findings.push(
                Finding::new(
                    FindingKind::GitNotRepository,
                    Severity::Error,
                    "target is not an inspectable git worktree",
                )
                .evidence([observed("git rev-parse --show-toplevel", error)])
                .recommend(
                    "Point the assessment at a git worktree; repository initialization is an explicit operator action.",
                    None,
                ),
            );
            return finish(report);
        }
    };

    report.identity.canonical_path = Some(top_level.to_string_lossy().into_owned());
    let mut aliases: Vec<String> = registered
        .iter()
        .filter_map(|repo| {
            std::fs::canonicalize(&repo.path)
                .ok()
                .filter(|path| path == &top_level)
                .map(|_| repo.name.clone())
        })
        .collect();
    aliases.sort();
    aliases.dedup();
    report.identity.registered_aliases = aliases.clone();
    report.identity.registered_name = selected
        .map(|repo| repo.name.clone())
        .or_else(|| aliases.first().cloned());

    report.findings.push(
        Finding::new(
            FindingKind::IdentityResolved,
            Severity::Info,
            "canonical repository identity resolved",
        )
        .evidence([
            observed("filesystem", top_level.to_string_lossy()),
            observed(
                "castle registry",
                if aliases.is_empty() {
                    "no registered names".to_string()
                } else {
                    format!("registered names: {}", aliases.join(", "))
                },
            ),
        ]),
    );
    if aliases.is_empty() {
        report.findings.push(
            Finding::new(
                FindingKind::IdentityUnregistered,
                Severity::Warning,
                "repository is not registered on this castle",
            )
            .evidence([observed("castle registry", "canonical path has no name")])
            .recommend(
                "Review the inferred identity before registering it; inspection never registers automatically.",
                Some(format!("rk repo add {}", shell_display(&top_level))),
            ),
        );
    } else if aliases.len() > 1 {
        report.findings.push(
            Finding::new(
                FindingKind::IdentityAmbiguous,
                Severity::Error,
                "multiple registered names point at the same canonical repository",
            )
            .evidence(aliases.iter().map(|name| observed("castle registry", name)))
            .ambiguity(format!(
                "canonical path is registered as {}",
                aliases.join(", ")
            ))
            .recommend(
                "Choose one canonical repository name before onboarding continues.",
                None,
            ),
        );
    }

    inspect_git(&top_level, &mut report);
    let instructions = inspect_instructions(&top_level, &mut report);
    let cue_available = command_exists("cue", &top_level);
    let checks = inspect_cue(&top_level, cue_available, context, &mut report);
    inspect_toolchain(&top_level, &instructions, &checks, &mut report);

    finish(report)
}

fn finish(mut report: AssessmentReport) -> AssessmentReport {
    for finding in &mut report.findings {
        finding.evidence.sort();
        finding.evidence.dedup();
        finding.evidence.truncate(MAX_EVIDENCE_ROWS);
    }
    report.findings.sort_by(|left, right| {
        left.kind
            .as_str()
            .cmp(right.kind.as_str())
            .then_with(|| left.summary.cmp(&right.summary))
    });
    report.ready = !report
        .findings
        .iter()
        .any(|finding| finding.severity == Severity::Error);
    report
}

fn add_rk_readiness(report: &mut AssessmentReport) {
    let executable = std::env::current_exe()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "running daemon executable".into());
    report.findings.push(
        Finding::new(
            FindingKind::RkReady,
            Severity::Info,
            "rk coordination path is reachable",
        )
        .evidence([observed(
            "repo.onboard.inspect RPC",
            format!("served by {executable}"),
        )]),
    );
}

fn add_harness_readiness(report: &mut AssessmentReport, harness: &str) {
    if harness == "fake" {
        report.findings.push(
            Finding::new(
                FindingKind::HarnessReady,
                Severity::Info,
                "configured harness `fake` is built in",
            )
            .evidence([observed("daemon configuration", "default harness: fake")]),
        );
    } else if command_exists(harness, Path::new(".")) {
        report.findings.push(
            Finding::new(
                FindingKind::HarnessReady,
                Severity::Info,
                format!("configured harness `{harness}` is available"),
            )
            .evidence([observed(
                "PATH",
                command_path(harness).unwrap_or_else(|| harness.into()),
            )]),
        );
    } else {
        report.findings.push(
            Finding::new(
                FindingKind::HarnessMissing,
                Severity::Error,
                format!("configured harness `{harness}` is unavailable"),
            )
            .evidence([observed("daemon configuration", format!("default harness: {harness}"))])
            .recommend(
                "Install the configured harness or explicitly select an available harness in castle configuration.",
                None,
            ),
        );
    }
}

fn inspect_git(root: &Path, report: &mut AssessmentReport) {
    inspect_git_state(root, report);
    let remotes = inspect_remotes(root, report);
    inspect_base(root, &remotes, report);
    inspect_submodules(root, report);
    inspect_lfs(root, report);
}

fn inspect_git_state(root: &Path, report: &mut AssessmentReport) {
    let status = match git_text(
        root,
        &[
            "status",
            "--porcelain=v2",
            "--branch",
            "--untracked-files=normal",
        ],
    ) {
        Ok(status) => status,
        Err(error) => {
            report.findings.push(
                Finding::new(
                    FindingKind::GitNotRepository,
                    Severity::Error,
                    "git state could not be read",
                )
                .evidence([observed("git status --porcelain=v2 --branch", error)])
                .recommend("Repair the worktree before onboarding continues.", None),
            );
            return;
        }
    };
    let mut branch = None;
    let mut unborn = false;
    let mut dirty = Vec::new();
    for line in status.lines() {
        if let Some(value) = line.strip_prefix("# branch.head ") {
            branch = Some(value.to_string());
        } else if line == "# branch.oid (initial)" {
            unborn = true;
        } else if !line.starts_with('#') && !line.trim().is_empty() {
            dirty.push(line.to_string());
        }
    }

    if unborn {
        report.findings.push(
            Finding::new(
                FindingKind::GitStateUnborn,
                Severity::Error,
                "repository has no commit yet",
            )
            .evidence([observed(
                "git status --porcelain=v2 --branch",
                "branch.oid is (initial)",
            )])
            .recommend(
                "Create and review an initial commit before onboarding continues.",
                None,
            ),
        );
    }
    if branch.as_deref() == Some("(detached)") {
        report.findings.push(
            Finding::new(
                FindingKind::GitStateDetached,
                Severity::Error,
                "repository is on a detached HEAD",
            )
            .evidence([observed(
                "git status --porcelain=v2 --branch",
                "branch.head is (detached)",
            )])
            .ambiguity("there is no current branch to relate to a base branch")
            .recommend(
                "Select the intended branch explicitly and rerun inspection.",
                None,
            ),
        );
    }
    if dirty.is_empty() {
        report.findings.push(
            Finding::new(
                FindingKind::GitStateClean,
                Severity::Info,
                "worktree and index are clean",
            )
            .evidence([observed(
                "git status --porcelain=v2 --branch",
                branch
                    .map(|name| format!("branch: {name}"))
                    .unwrap_or_else(|| "branch unavailable".into()),
            )]),
        );
    } else {
        report.findings.push(
            Finding::new(
                FindingKind::GitStateDirty,
                Severity::Error,
                "worktree or index contains uncommitted changes",
            )
            .evidence(
                dirty
                    .into_iter()
                    .take(MAX_EVIDENCE_ROWS)
                    .map(|line| observed("git status --porcelain=v2", line)),
            )
            .recommend(
                "Commit, discard, or otherwise account for every change before onboarding continues.",
                Some("git status --short".into()),
            ),
        );
    }
}

fn inspect_remotes(root: &Path, report: &mut AssessmentReport) -> Vec<String> {
    let output = git_text(root, &["remote", "-v"]).unwrap_or_default();
    let mut names = BTreeSet::new();
    let mut evidence = Vec::new();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        if let Some(name) = line.split_whitespace().next() {
            names.insert(name.to_string());
        }
        evidence.push(observed("git remote -v", line));
    }
    if names.is_empty() {
        report.findings.push(
            Finding::new(
                FindingKind::GitRemoteMissing,
                Severity::Error,
                "repository has no configured git remote",
            )
            .evidence([observed("git remote -v", "no remotes")])
            .recommend(
                "Choose and configure the intended remote explicitly; inspection never changes remotes.",
                None,
            ),
        );
    } else {
        report.findings.push(
            Finding::new(
                FindingKind::GitRemotePresent,
                Severity::Info,
                format!("{} git remote(s) observed", names.len()),
            )
            .evidence(evidence),
        );
    }
    names.into_iter().collect()
}

fn inspect_base(root: &Path, remotes: &[String], report: &mut AssessmentReport) {
    let mut candidates: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for remote in remotes {
        let reference = format!("refs/remotes/{remote}/HEAD");
        if let Ok(symbolic) = git_text(root, &["symbolic-ref", "--quiet", "--short", &reference]) {
            let symbolic = symbolic.trim();
            if let Some((_, branch)) = symbolic.split_once('/') {
                candidates
                    .entry(branch.to_string())
                    .or_default()
                    .insert(format!("{reference} -> {symbolic}"));
            }
        }
    }
    for branch in ["main", "master", "trunk", "develop"] {
        let local = format!("refs/heads/{branch}");
        if git_success(root, &["show-ref", "--verify", "--quiet", &local]) {
            candidates.entry(branch.into()).or_default().insert(local);
        }
        for remote in remotes {
            let remote_ref = format!("refs/remotes/{remote}/{branch}");
            if git_success(root, &["show-ref", "--verify", "--quiet", &remote_ref]) {
                candidates
                    .entry(branch.into())
                    .or_default()
                    .insert(remote_ref);
            }
        }
    }

    match candidates.len() {
        1 => {
            let (branch, sources) = candidates.into_iter().next().expect("one candidate");
            report.findings.push(
                Finding::new(
                    FindingKind::GitBaseDetected,
                    Severity::Info,
                    format!("base branch resolved as `{branch}`"),
                )
                .evidence(
                    sources
                        .into_iter()
                        .map(|source| observed("git refs", source)),
                ),
            );
        }
        0 => report.findings.push(
            Finding::new(
                FindingKind::GitBaseMissing,
                Severity::Error,
                "no base branch could be resolved",
            )
            .evidence([observed(
                "git refs",
                "no remote HEAD or conventional base ref (main/master/trunk/develop)",
            )])
            .ambiguity("the assessment cannot infer which branch future work should target")
            .recommend(
                "Set the remote default branch or document an explicit base branch.",
                None,
            ),
        ),
        _ => {
            let names = candidates.keys().cloned().collect::<Vec<_>>();
            report.findings.push(
                Finding::new(
                    FindingKind::GitBaseAmbiguous,
                    Severity::Error,
                    "multiple plausible base branches were observed",
                )
                .evidence(candidates.into_iter().flat_map(|(branch, sources)| {
                    sources
                        .into_iter()
                        .map(move |source| observed("git refs", format!("{branch}: {source}")))
                }))
                .ambiguity(format!("candidates: {}", names.join(", ")))
                .recommend(
                    "Choose and document one base branch before onboarding continues.",
                    None,
                ),
            );
        }
    }
}

fn inspect_submodules(root: &Path, report: &mut AssessmentReport) {
    let file = root.join(".gitmodules");
    if !file.is_file() {
        return;
    }
    let detail = read_bounded(&file)
        .map(|source| {
            let count = source
                .lines()
                .filter(|line| line.trim_start().starts_with("[submodule "))
                .count();
            format!(".gitmodules declares {count} submodule(s)")
        })
        .unwrap_or_else(|error| error);
    report.findings.push(
        Finding::new(
            FindingKind::GitSubmodulePresent,
            Severity::Error,
            "repository uses git submodules that require an explicit readiness decision",
        )
        .evidence([observed(".gitmodules", detail)])
        .recommend(
            "Verify submodule URLs, initialization state, credentials, and recursive check behavior before onboarding continues.",
            Some("git submodule status --recursive".into()),
        ),
    );
}

fn inspect_lfs(root: &Path, report: &mut AssessmentReport) {
    let mut attributes = Vec::new();
    find_named_files(root, ".gitattributes", 0, &mut attributes);
    let mut evidence = Vec::new();
    for file in attributes {
        if let Ok(source) = read_bounded(&file) {
            for line in source.lines() {
                if line.contains("filter=lfs") || line.contains("filter = lfs") {
                    evidence.push(observed(relative(root, &file), line.trim()));
                }
            }
        }
    }
    if evidence.is_empty() {
        return;
    }
    let lfs_available = command_exists("git-lfs", root);
    if !lfs_available {
        evidence.push(observed("PATH", "git-lfs was not found"));
    }
    report.findings.push(
        Finding::new(
            FindingKind::GitLfsPresent,
            Severity::Error,
            "repository uses Git LFS and object readiness is unresolved",
        )
        .evidence(evidence)
        .ambiguity(if lfs_available {
            "LFS filters are declared, but inspection does not fetch or rewrite objects"
        } else {
            "LFS filters are declared and git-lfs is unavailable"
        })
        .recommend(
            "Verify git-lfs availability and that required objects are materialized before onboarding continues.",
            Some("git lfs status".into()),
        ),
    );
}

fn inspect_instructions(root: &Path, report: &mut AssessmentReport) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let upper = name.to_ascii_uppercase();
            if path.is_file()
                && (upper == "AGENTS.MD"
                    || upper == "CLAUDE.MD"
                    || upper == "CODEX.MD"
                    || upper.starts_with("README")
                    || upper.starts_with("CONTRIBUTING"))
            {
                files.push(path);
            }
        }
    }
    let copilot = root.join(".github").join("copilot-instructions.md");
    if copilot.is_file() {
        files.push(copilot);
    }
    files.sort();
    files.dedup();

    if files.is_empty() {
        report.findings.push(
            Finding::new(
                FindingKind::InstructionsMissing,
                Severity::Warning,
                "no repository instruction file was found",
            )
            .evidence([observed(
                "repository root",
                "no README, CONTRIBUTING, AGENTS.md, CLAUDE.md, CODEX.md, or Copilot instructions",
            )])
            .recommend(
                "Document the repository's existing workflow before enabling unattended work.",
                None,
            ),
        );
    } else {
        report.findings.push(
            Finding::new(
                FindingKind::InstructionsPresent,
                Severity::Info,
                format!("{} repository instruction file(s) observed", files.len()),
            )
            .evidence(
                files
                    .iter()
                    .map(|path| observed(relative(root, path), "instruction source")),
            ),
        );
    }
    files
}

fn inspect_cue(
    root: &Path,
    cue_available: bool,
    context: &InspectContext,
    report: &mut AssessmentReport,
) -> Vec<rk_workflow::Check> {
    let rk_dir = root.join(".rk");
    let checks_file = rk_dir.join("checks.cue");
    let workflows = rk_workflow::definitions(&rk_dir.join("workflows"));
    let triggers_file = rk_dir.join("triggers.cue");
    let schedules_file = rk_dir.join("schedules.cue");
    let any_cue = checks_file.is_file()
        || !workflows.is_empty()
        || triggers_file.is_file()
        || schedules_file.is_file();

    if any_cue && !cue_available {
        let mut evidence = Vec::new();
        if checks_file.is_file() {
            evidence.push(observed(
                relative(root, &checks_file),
                "requires CUE validation",
            ));
        }
        evidence.extend(
            workflows
                .iter()
                .map(|path| observed(relative(root, path), "requires CUE validation")),
        );
        if triggers_file.is_file() {
            evidence.push(observed(
                relative(root, &triggers_file),
                "requires CUE validation",
            ));
        }
        if schedules_file.is_file() {
            evidence.push(observed(
                relative(root, &schedules_file),
                "requires CUE validation",
            ));
        }
        evidence.push(observed("PATH", "cue was not found"));
        report.findings.push(
            Finding::new(
                FindingKind::ToolMissing,
                Severity::Error,
                "CUE definitions exist but the `cue` validator is unavailable",
            )
            .evidence(evidence)
            .recommend("Install the CUE CLI and rerun inspection.", None),
        );
        return Vec::new();
    }

    let mut checks = Vec::new();
    if checks_file.is_file() {
        match read_cue(&checks_file).and_then(|source| {
            reject_cue_imports(&source)?;
            rk_workflow::load_checks_str(&source).map_err(|error| error.to_string())
        }) {
            Ok(mut loaded) => {
                loaded.sort_by(|left, right| left.name.cmp(&right.name));
                let evidence = loaded.iter().map(|check| {
                    command_evidence(
                        EvidenceOrigin::Observed,
                        relative(root, &checks_file),
                        format!(
                            "named check `{}` (cwd: {}, expected exit: {}, timeout: {}, environment: {}, toolchain: {})",
                            check.name,
                            check.cwd.as_deref().unwrap_or("."),
                            check
                                .expect_exit
                                .map(|exit| exit.to_string())
                                .unwrap_or_else(|| "workflow step default".into()),
                            check.timeout.as_deref().unwrap_or("workflow step default"),
                            check.environment_policy,
                            check.toolchain.as_deref().unwrap_or("not declared"),
                        ),
                        &check.command,
                    )
                });
                report.findings.push(
                    Finding::new(
                        FindingKind::NamedChecksPresent,
                        Severity::Info,
                        format!("{} valid named check(s) observed", loaded.len()),
                    )
                    .evidence(evidence),
                );
                checks = loaded;
            }
            Err(error) => push_cue_error(root, &checks_file, error, report),
        }
    } else {
        let severity = if context.require_named_checks {
            Severity::Error
        } else {
            Severity::Warning
        };
        report.findings.push(
            Finding::new(
                FindingKind::NamedChecksMissing,
                severity,
                "repository has no `.rk/checks.cue` registry",
            )
            .evidence([observed(
                "repository",
                format!(
                    "castle require_named_checks policy: {}",
                    context.require_named_checks
                ),
            )])
            .recommend(
                "Review documented project commands, then declare only approved commands as named checks.",
                None,
            ),
        );
    }

    if workflows.is_empty() {
        report.findings.push(
            Finding::new(
                FindingKind::WorkflowDefinition,
                Severity::Info,
                "no repo-local workflow definitions were observed",
            )
            .evidence([observed(".rk/workflows", "directory absent or empty")]),
        );
    }
    for workflow in workflows {
        match read_cue(&workflow).and_then(|source| {
            reject_cue_imports(&source)?;
            rk_workflow::validate_workflow_str(&source).map_err(|error| error.to_string())
        }) {
            Ok(()) => report.findings.push(
                Finding::new(
                    FindingKind::WorkflowDefinition,
                    Severity::Info,
                    format!("workflow `{}` is valid", file_stem(&workflow)),
                )
                .evidence([observed(
                    relative(root, &workflow),
                    "CUE schema validation passed",
                )]),
            ),
            Err(error) => push_cue_error(root, &workflow, error, report),
        }
    }

    if triggers_file.is_file() {
        match read_cue(&triggers_file).and_then(|source| {
            reject_cue_imports(&source)?;
            rk_workflow::load_triggers_str(&source).map_err(|error| error.to_string())
        }) {
            Ok(mut triggers) => {
                triggers.sort_by(|left, right| left.name.cmp(&right.name));
                report.findings.push(
                    Finding::new(
                        FindingKind::TriggerDefinition,
                        Severity::Info,
                        format!("{} valid trigger(s) observed", triggers.len()),
                    )
                    .evidence(triggers.into_iter().map(|trigger| {
                        observed(
                            relative(root, &triggers_file),
                            format!("{} -> workflow {}", trigger.name, trigger.run),
                        )
                    })),
                );
            }
            Err(error) => push_cue_error(root, &triggers_file, error, report),
        }
    } else {
        report.findings.push(
            Finding::new(
                FindingKind::TriggerDefinition,
                Severity::Info,
                "no repo-local trigger definition was observed",
            )
            .evidence([observed(".rk/triggers.cue", "file absent")]),
        );
    }

    if schedules_file.is_file() {
        match read_cue(&schedules_file).and_then(|source| {
            reject_cue_imports(&source)?;
            rk_workflow::load_schedules_str(&source).map_err(|error| error.to_string())
        }) {
            Ok(mut schedules) => {
                schedules.sort_by(|left, right| left.name.cmp(&right.name));
                let mut valid = Vec::new();
                for schedule in schedules {
                    match Cron::parse(&schedule.cron) {
                        Ok(_) => valid.push(observed(
                            relative(root, &schedules_file),
                            format!(
                                "{} ({}) -> workflow {}",
                                schedule.name, schedule.cron, schedule.run
                            ),
                        )),
                        Err(error) => push_cue_error(
                            root,
                            &schedules_file,
                            format!("schedule `{}` has invalid cron: {error}", schedule.name),
                            report,
                        ),
                    }
                }
                if !valid.is_empty() {
                    report.findings.push(
                        Finding::new(
                            FindingKind::ScheduleDefinition,
                            Severity::Info,
                            format!("{} valid schedule(s) observed", valid.len()),
                        )
                        .evidence(valid),
                    );
                }
            }
            Err(error) => push_cue_error(root, &schedules_file, error, report),
        }
    } else {
        report.findings.push(
            Finding::new(
                FindingKind::ScheduleDefinition,
                Severity::Info,
                "no repo-local schedule definition was observed",
            )
            .evidence([observed(".rk/schedules.cue", "file absent")]),
        );
    }

    checks
}

fn push_cue_error(root: &Path, file: &Path, error: String, report: &mut AssessmentReport) {
    report.findings.push(
        Finding::new(
            FindingKind::CueMalformed,
            Severity::Error,
            format!("{} is not a valid repository definition", relative(root, file)),
        )
        .evidence([observed(relative(root, file), single_line(&error))])
        .recommend(
            "Correct and review the CUE definition; malformed automation is never loaded or inferred.",
            None,
        ),
    );
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Entrypoint {
    source: String,
    command: String,
    origin: EvidenceOrigin,
}

fn inspect_toolchain(
    root: &Path,
    instructions: &[PathBuf],
    checks: &[rk_workflow::Check],
    report: &mut AssessmentReport,
) {
    let mut entrypoints = BTreeSet::new();
    for check in checks {
        entrypoints.insert(Entrypoint {
            source: ".rk/checks.cue".into(),
            command: check.command.clone(),
            origin: EvidenceOrigin::Observed,
        });
    }
    discover_mise(root, &mut entrypoints);
    discover_make(root, &mut entrypoints);
    discover_just(root, &mut entrypoints);
    discover_package_scripts(root, &mut entrypoints);
    discover_documented_commands(root, instructions, &mut entrypoints);

    if entrypoints.is_empty() {
        infer_entrypoints(root, &mut entrypoints);
    }

    let observed_count = entrypoints
        .iter()
        .filter(|entry| entry.origin == EvidenceOrigin::Observed)
        .count();
    let inferred_count = entrypoints.len() - observed_count;
    if observed_count > 0 {
        report.findings.push(
            Finding::new(
                FindingKind::ToolchainEntrypoint,
                Severity::Info,
                format!("{observed_count} documented project entrypoint(s) observed"),
            )
            .evidence(
                entrypoints
                    .iter()
                    .filter(|entry| entry.origin == EvidenceOrigin::Observed)
                    .map(|entry| {
                        command_evidence(
                            entry.origin,
                            &entry.source,
                            "repository-owned entrypoint",
                            &entry.command,
                        )
                    }),
            ),
        );
    }
    if inferred_count > 0 {
        report.findings.push(
            Finding::new(
                FindingKind::ToolchainInferred,
                Severity::Warning,
                format!("{inferred_count} verification command(s) were inferred from manifests"),
            )
            .evidence(
                entrypoints
                    .iter()
                    .filter(|entry| entry.origin == EvidenceOrigin::Inferred)
                    .map(|entry| {
                        command_evidence(
                            entry.origin,
                            &entry.source,
                            "manifest-based recommendation; not executed or documented",
                            &entry.command,
                        )
                    }),
            )
            .recommend(
                "Confirm an inferred command in repository documentation or `.rk/checks.cue` before treating it as authoritative.",
                None,
            ),
        );
    }
    if entrypoints.is_empty() {
        report.findings.push(
            Finding::new(
                FindingKind::ToolchainMissing,
                Severity::Error,
                "no documented or inferable project entrypoint was found",
            )
            .evidence([observed(
                "repository",
                "no named check, task runner entrypoint, documented command, or known manifest",
            )])
            .recommend(
                "Document the repository's build/test/lint entrypoint before onboarding continues.",
                None,
            ),
        );
    }

    let mut missing: BTreeMap<String, Vec<&Entrypoint>> = BTreeMap::new();
    for entry in &entrypoints {
        if let Some(tool) = command_tool(&entry.command) {
            if !command_exists(&tool, root) {
                missing.entry(tool).or_default().push(entry);
            }
        }
    }
    for (tool, entries) in missing {
        report.findings.push(
            Finding::new(
                FindingKind::ToolMissing,
                Severity::Error,
                format!("referenced project tool `{tool}` is unavailable"),
            )
            .evidence(entries.into_iter().map(|entry| {
                command_evidence(
                    entry.origin,
                    &entry.source,
                    format!("requires `{tool}`"),
                    &entry.command,
                )
            }))
            .recommend(
                "Install the documented tool or update the repository-owned entrypoint after review.",
                None,
            ),
        );
    }
}

fn discover_mise(root: &Path, entrypoints: &mut BTreeSet<Entrypoint>) {
    for name in ["mise.toml", ".mise.toml"] {
        let path = root.join(name);
        let Ok(source) = read_bounded(&path) else {
            continue;
        };
        for line in source.lines() {
            let line = line.trim();
            let Some(section) = line
                .strip_prefix("[tasks.")
                .and_then(|value| value.strip_suffix(']'))
            else {
                continue;
            };
            let task = section.trim_matches(['"', '\'']);
            if verification_name(task) {
                entrypoints.insert(Entrypoint {
                    source: name.into(),
                    command: format!("mise run {task}"),
                    origin: EvidenceOrigin::Observed,
                });
            }
        }
    }
}

fn discover_make(root: &Path, entrypoints: &mut BTreeSet<Entrypoint>) {
    for name in ["Makefile", "makefile", "GNUmakefile"] {
        let path = root.join(name);
        let Ok(source) = read_bounded(&path) else {
            continue;
        };
        for line in source.lines() {
            if line.starts_with(char::is_whitespace) || line.starts_with('.') {
                continue;
            }
            let Some((target, _)) = line.split_once(':') else {
                continue;
            };
            if verification_name(target) {
                entrypoints.insert(Entrypoint {
                    source: name.into(),
                    command: format!("make {target}"),
                    origin: EvidenceOrigin::Observed,
                });
            }
        }
    }
}

fn discover_just(root: &Path, entrypoints: &mut BTreeSet<Entrypoint>) {
    for name in ["justfile", "Justfile"] {
        let path = root.join(name);
        let Ok(source) = read_bounded(&path) else {
            continue;
        };
        for line in source.lines() {
            if line.starts_with(char::is_whitespace) || line.starts_with('#') {
                continue;
            }
            let Some((head, _)) = line.split_once(':') else {
                continue;
            };
            let target = head.split_whitespace().next().unwrap_or_default();
            if verification_name(target) {
                entrypoints.insert(Entrypoint {
                    source: name.into(),
                    command: format!("just {target}"),
                    origin: EvidenceOrigin::Observed,
                });
            }
        }
    }
}

fn discover_package_scripts(root: &Path, entrypoints: &mut BTreeSet<Entrypoint>) {
    let path = root.join("package.json");
    let Ok(source) = read_bounded(&path) else {
        return;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&source) else {
        return;
    };
    let Some(scripts) = value.get("scripts").and_then(|scripts| scripts.as_object()) else {
        return;
    };
    let mut names = scripts.keys().collect::<Vec<_>>();
    names.sort();
    for name in names {
        if verification_name(name) {
            entrypoints.insert(Entrypoint {
                source: "package.json".into(),
                command: format!("npm run {name}"),
                origin: EvidenceOrigin::Observed,
            });
        }
    }
}

fn discover_documented_commands(
    root: &Path,
    instructions: &[PathBuf],
    entrypoints: &mut BTreeSet<Entrypoint>,
) {
    const PREFIXES: [&str; 10] = [
        "mise run ",
        "mise exec ",
        "make ",
        "just ",
        "npm test",
        "npm run ",
        "cargo test",
        "cargo clippy",
        "go test",
        "zig build",
    ];
    for path in instructions {
        let Ok(source) = read_bounded(path) else {
            continue;
        };
        for line in source.lines() {
            let line = line.trim().strip_prefix("$ ").unwrap_or(line.trim());
            if PREFIXES.iter().any(|prefix| line.starts_with(prefix))
                && !line.contains(" # ")
                && line.len() <= 240
            {
                entrypoints.insert(Entrypoint {
                    source: relative(root, path),
                    command: line.to_string(),
                    origin: EvidenceOrigin::Observed,
                });
            }
        }
    }
}

fn infer_entrypoints(root: &Path, entrypoints: &mut BTreeSet<Entrypoint>) {
    for (manifest, command) in [
        ("Cargo.toml", "cargo test --workspace"),
        ("go.mod", "go test ./..."),
        ("build.zig", "zig build test"),
        ("pyproject.toml", "python -m pytest"),
        ("package.json", "npm test"),
    ] {
        if root.join(manifest).is_file() {
            entrypoints.insert(Entrypoint {
                source: manifest.into(),
                command: command.into(),
                origin: EvidenceOrigin::Inferred,
            });
        }
    }
}

fn verification_name(name: &str) -> bool {
    matches!(
        name.trim(),
        "verify" | "check" | "test" | "tests" | "lint" | "ci" | "build"
    )
}

fn command_tool(command: &str) -> Option<String> {
    let mut words = command.split_whitespace().peekable();
    let mut word = words.next()?.trim_matches(['\'', '"']).to_string();
    while word.contains('=') && !word.starts_with("./") && !word.starts_with('/') {
        word = words.next()?.trim_matches(['\'', '"']).to_string();
    }
    if word == "env" {
        loop {
            word = words.next()?.trim_matches(['\'', '"']).to_string();
            if word == "-u" || word == "--unset" {
                words.next()?;
                continue;
            }
            if word.starts_with("--unset=") || word.contains('=') {
                continue;
            }
            break;
        }
    }
    Some(word)
}

fn command_exists(command: &str, root: &Path) -> bool {
    if command.contains('/') {
        let path = Path::new(command);
        return if path.is_absolute() {
            path.is_file()
        } else {
            root.join(path).is_file()
        };
    }
    command_path(command).is_some()
}

fn command_path(command: &str) -> Option<String> {
    std::env::var_os("PATH").and_then(|path| {
        std::env::split_paths(&path)
            .map(|dir| dir.join(command))
            .find(|candidate| candidate.is_file())
            .map(|candidate| candidate.to_string_lossy().into_owned())
    })
}

fn read_cue(path: &Path) -> Result<String, String> {
    read_bounded(path)
}

fn read_bounded(path: &Path) -> Result<String, String> {
    let metadata =
        std::fs::metadata(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    if metadata.len() > MAX_DEFINITION_BYTES {
        return Err(format!(
            "{} is {} bytes; inspection limit is {} bytes",
            path.display(),
            metadata.len(),
            MAX_DEFINITION_BYTES
        ));
    }
    std::fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))
}

pub(crate) fn reject_cue_imports(source: &str) -> Result<(), String> {
    if source.lines().any(|line| {
        let line = line.trim_start();
        line == "import (" || line.starts_with("import ")
    }) {
        return Err(
            "CUE imports are not resolved during read-only assessment; dependency/cache state is ambiguous"
                .into(),
        );
    }
    Ok(())
}

fn find_named_files(root: &Path, name: &str, depth: usize, found: &mut Vec<PathBuf>) {
    if depth > 8 || found.len() >= MAX_EVIDENCE_ROWS {
        return;
    }
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    let mut paths = entries
        .flatten()
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    paths.sort();
    for path in paths {
        if found.len() >= MAX_EVIDENCE_ROWS {
            break;
        }
        if path.file_name().and_then(|value| value.to_str()) == Some(".git") {
            continue;
        }
        let Ok(file_type) = path.symlink_metadata().map(|metadata| metadata.file_type()) else {
            continue;
        };
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_file() && path.file_name().and_then(|value| value.to_str()) == Some(name) {
            found.push(path);
        } else if file_type.is_dir() {
            find_named_files(&path, name, depth + 1, found);
        }
    }
}

fn git_output(root: &Path, args: &[&str]) -> std::io::Result<Output> {
    Command::new("git")
        .arg("--no-optional-locks")
        .arg("-C")
        .arg(root)
        .args(args)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("LC_ALL", "C")
        .output()
}

fn git_text(root: &Path, args: &[&str]) -> Result<String, String> {
    let output = git_output(root, args).map_err(|error| error.to_string())?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(if stderr.trim().is_empty() {
            format!("git exited {}", output.status)
        } else {
            stderr.trim().to_string()
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn git_success(root: &Path, args: &[&str]) -> bool {
    git_output(root, args)
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

fn file_stem(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("?")
        .to_string()
}

fn shell_display(path: &Path) -> String {
    let value = path.to_string_lossy();
    if value.contains(char::is_whitespace) {
        format!("{value:?}")
    } else {
        value.into_owned()
    }
}

fn single_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rk_core::config::MergeMode;
    use std::fs;

    fn git(root: &Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .env("LC_ALL", "C")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn fixture() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        git(dir.path(), &["init", "-b", "main"]);
        git(dir.path(), &["config", "user.email", "test@example.com"]);
        git(dir.path(), &["config", "user.name", "Test"]);
        fs::write(
            dir.path().join("README.md"),
            "# Fixture\n\n```sh\nmise run verify\n```\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("mise.toml"),
            "[tasks.verify]\nrun = \"cargo test\"\n",
        )
        .unwrap();
        git(dir.path(), &["add", "."]);
        git(dir.path(), &["commit", "-m", "initial"]);
        git(
            dir.path(),
            &["remote", "add", "origin", "git@example.com:org/repo.git"],
        );
        dir
    }

    fn context() -> InspectContext {
        InspectContext {
            default_harness: "fake".into(),
            require_named_checks: false,
        }
    }

    fn record(name: &str, path: &Path) -> RepoRecord {
        RepoRecord {
            name: name.into(),
            path: path.into(),
            created_at: Utc::now(),
            merge_mode: MergeMode::Direct,
            remote: None,
            host: None,
        }
    }

    fn kinds(report: &AssessmentReport) -> BTreeSet<String> {
        report
            .findings
            .iter()
            .map(|finding| finding.kind.as_str().to_string())
            .collect()
    }

    #[test]
    fn clean_report_is_stable_and_distinguishes_observed_entrypoints() {
        let dir = fixture();
        let registered = vec![record("fixture", dir.path())];
        let first = inspect("fixture", &registered, &context());
        let second = inspect("fixture", &registered, &context());
        assert_eq!(first, second);
        assert!(first.ready, "{:#?}", first.findings);
        assert_eq!(first.schema_version, 1);
        assert_eq!(first.identity.registered_name.as_deref(), Some("fixture"));
        let entrypoint = first
            .findings
            .iter()
            .find(|finding| finding.kind == FindingKind::ToolchainEntrypoint)
            .unwrap();
        assert!(entrypoint
            .evidence
            .iter()
            .any(|evidence| evidence.origin == EvidenceOrigin::Observed
                && evidence.command.as_deref() == Some("mise run verify")));
    }

    #[test]
    fn valid_repo_automation_has_stable_report_kinds() {
        let dir = fixture();
        fs::create_dir_all(dir.path().join(".rk/workflows")).unwrap();
        fs::write(
            dir.path().join(".rk/checks.cue"),
            r#"checks: [{name: "verify", command: "mise run verify", expectExit: 0}]"#,
        )
        .unwrap();
        fs::write(
            dir.path().join(".rk/workflows/demo.cue"),
            r#"workflow: {
    name: "demo"
    params: {}
    agents: {}
    steps: [{type: "stop"}]
}"#,
        )
        .unwrap();
        fs::write(
            dir.path().join(".rk/triggers.cue"),
            r#"triggers: [{
    name: "on-event"
    match: {category: "event"}
    run: "demo"
    maxFires: 1
}]"#,
        )
        .unwrap();
        fs::write(
            dir.path().join(".rk/schedules.cue"),
            r#"schedules: [{name: "nightly", cron: "@daily", run: "demo"}]"#,
        )
        .unwrap();
        git(dir.path(), &["add", "."]);
        git(dir.path(), &["commit", "-m", "add automation"]);

        let report = inspect(dir.path().to_str().unwrap(), &[], &context());
        let kinds = kinds(&report);
        assert!(report.ready, "{:#?}", report.findings);
        for kind in [
            "named_checks_present",
            "workflow_definition",
            "trigger_definition",
            "schedule_definition",
        ] {
            assert!(
                kinds.contains(kind),
                "missing {kind}: {:#?}",
                report.findings
            );
        }
    }

    #[test]
    fn dirty_unborn_ambiguous_base_and_missing_remote_fail_closed() {
        let dirty = fixture();
        fs::write(dirty.path().join("dirty.txt"), "dirty").unwrap();
        let dirty_report = inspect(dirty.path().to_str().unwrap(), &[], &context());
        assert!(!dirty_report.ready);
        assert!(kinds(&dirty_report).contains("git_state_dirty"));

        let unborn = tempfile::tempdir().unwrap();
        git(unborn.path(), &["init", "-b", "main"]);
        let unborn_report = inspect(unborn.path().to_str().unwrap(), &[], &context());
        let unborn_kinds = kinds(&unborn_report);
        assert!(!unborn_report.ready);
        assert!(unborn_kinds.contains("git_state_unborn"));
        assert!(unborn_kinds.contains("git_remote_missing"));

        let no_remote = fixture();
        git(no_remote.path(), &["remote", "remove", "origin"]);
        let no_remote_report = inspect(no_remote.path().to_str().unwrap(), &[], &context());
        assert!(!no_remote_report.ready);
        assert!(kinds(&no_remote_report).contains("git_remote_missing"));

        let ambiguous = fixture();
        git(ambiguous.path(), &["branch", "master"]);
        let ambiguous_report = inspect(ambiguous.path().to_str().unwrap(), &[], &context());
        assert!(!ambiguous_report.ready);
        assert!(kinds(&ambiguous_report).contains("git_base_ambiguous"));
    }

    #[test]
    fn missing_tool_malformed_cue_submodule_and_lfs_fail_closed() {
        let dir = fixture();
        fs::create_dir_all(dir.path().join(".rk")).unwrap();
        fs::write(
            dir.path().join(".rk/checks.cue"),
            r#"checks: [{name: "verify", command: "rk-tool-that-does-not-exist verify"}]"#,
        )
        .unwrap();
        fs::write(
            dir.path().join(".rk/triggers.cue"),
            "triggers: [{name: \"broken\"",
        )
        .unwrap();
        fs::write(
            dir.path().join(".gitmodules"),
            "[submodule \"dep\"]\n\tpath = dep\n\turl = ../dep\n",
        )
        .unwrap();
        fs::write(
            dir.path().join(".gitattributes"),
            "*.bin filter=lfs diff=lfs\n",
        )
        .unwrap();
        git(dir.path(), &["add", "."]);
        git(dir.path(), &["commit", "-m", "add hazards"]);

        let report = inspect(dir.path().to_str().unwrap(), &[], &context());
        let kinds = kinds(&report);
        assert!(!report.ready);
        for kind in [
            "tool_missing",
            "cue_malformed",
            "git_submodule_present",
            "git_lfs_present",
        ] {
            assert!(
                kinds.contains(kind),
                "missing {kind}: {:#?}",
                report.findings
            );
        }
        assert!(report
            .findings
            .iter()
            .filter(|finding| finding.severity == Severity::Error)
            .all(|finding| finding.recommendation.is_some()));
    }
}
