//! The shipped example workflows must always load — they are the onboarding
//! surface, and a schema drift that breaks them should fail CI, not users.

use serde_json::json;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn workspace_root_from(start: &Path) -> Option<PathBuf> {
    start
        .ancestors()
        .find(|dir| dir.join("Cargo.toml").is_file() && dir.join("examples/workflows").is_dir())
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

fn examples_dir() -> PathBuf {
    workspace_root().join("examples/workflows")
}

#[test]
fn workspace_root_is_discovered_from_runtime_directory() {
    let root = tempfile::tempdir().unwrap();
    let nested = root.path().join("crates/rk-workflow");
    std::fs::create_dir_all(root.path().join("examples/workflows")).unwrap();
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(root.path().join("Cargo.toml"), "[workspace]\n").unwrap();

    assert_eq!(workspace_root_from(&nested).as_deref(), Some(root.path()));
}

#[test]
fn repository_verify_check_trusts_only_its_current_worktree() {
    let file = workspace_root().join(".rk/checks.cue");
    let checks = rk_workflow::load_checks(&file)
        .unwrap_or_else(|e| panic!("{} failed to load: {e}", file.display()));
    let verify = checks
        .iter()
        .find(|check| check.name == "verify")
        .expect("repository must declare a verify check");

    assert_eq!(
        verify.command, "MISE_TRUSTED_CONFIG_PATHS=\"$PWD\" mise run verify",
        "the named check runs in transient agent worktrees, so trust must be process-local"
    );
    assert_eq!(
        verify.environment_policy,
        rk_workflow::CheckEnvironmentPolicy::StripRkSpawn
    );
}

#[test]
fn repository_policy_preserves_agent_base_and_existing_names() {
    let file = workspace_root().join(".rk/repo.cue");
    let policy = rk_workflow::load_repository_policy(&file)
        .unwrap_or_else(|e| panic!("{} failed to load: {e}", file.display()));

    assert_eq!(policy.work.branch, "rat/{{agent}}/{{task}}");
    assert_eq!(policy.work.worktree, "{{repo}}/{{agent}}");
    assert_eq!(policy.delivery.target, "agent-base");
    assert_eq!(policy.delivery.mode, rk_workflow::DeliveryMode::Merge);
}

#[test]
fn all_shipped_examples_load() {
    let dir = examples_dir();
    let defs = rk_workflow::definitions(&dir);
    assert!(
        defs.len() >= 2,
        "expected shipped examples in {}",
        dir.display()
    );
    // Cover every required-without-default param across the shipped set so a new
    // example that adds one is caught here rather than by a user.
    let inputs = HashMap::from([
        ("taskId".to_string(), json!("example-task")),
        ("description".to_string(), json!("example description")),
        (
            "taskDescription".to_string(),
            json!("example feature-set description"),
        ),
        (
            "question".to_string(),
            json!("How does the tuplespace work?"),
        ),
        ("branch".to_string(), json!("rat/example-task/tkt-1")),
        ("reviewAttempt".to_string(), json!("landing-review-example")),
    ]);
    for def in defs {
        let workflow = rk_workflow::load(&def, &inputs)
            .unwrap_or_else(|e| panic!("{} failed to load: {e}", def.display()));
        assert!(!workflow.steps.is_empty(), "{}", def.display());
    }
}

#[test]
fn steward_review_carries_the_exact_review_binding_on_its_spawn() {
    let inputs = HashMap::from([
        ("taskId".to_string(), json!("TKT-1")),
        ("branch".to_string(), json!("rat/worker/tkt-1")),
        ("target".to_string(), json!("release")),
        ("headSha".to_string(), json!("abc123")),
        ("reviewAttempt".to_string(), json!("landing-review-1")),
    ]);
    let workflow = rk_workflow::load(&examples_dir().join("steward-review.cue"), &inputs)
        .expect("steward-review loads");
    let spawn = workflow
        .steps
        .iter()
        .find_map(|step| match step {
            rk_workflow::Step::Spawn(spawn) => Some(spawn),
            _ => None,
        })
        .expect("reviewer spawn");
    let review = spawn.review.as_ref().expect("typed review binding");
    assert_eq!(review.branch, "rat/worker/tkt-1");
    assert_eq!(review.head_sha, "abc123");
    assert_eq!(review.target, "release");
    assert_eq!(review.task, "TKT-1");
    assert_eq!(review.attempt, "landing-review-1");
}

#[test]
fn research_example_loads_and_runs_engine_validation_after_artifact_production() {
    use rk_workflow::Step;

    let inputs = HashMap::from([(
        "question".to_string(),
        json!("How does the tuplespace handle concurrent writers?"),
    )]);
    let workflow = rk_workflow::load(&examples_dir().join("research.cue"), &inputs).unwrap();

    let wait_at = workflow
        .steps
        .iter()
        .position(|step| matches!(step, Step::Wait(_)))
        .expect("research waits for artifact production");
    let validation_at = workflow
        .steps
        .iter()
        .position(|step| {
            matches!(
                step,
                Step::Run(run)
                    if run.command.as_deref().is_some_and(|cmd| cmd.contains("product-to-code research validate"))
                        && run.expect_exit == Some(0)
            )
        })
        .expect("research workflow has an engine-executed validation run");
    assert!(
        wait_at < validation_at,
        "validation must run after the producing agent finishes"
    );
}

#[test]
fn shipped_example_triggers_load() {
    let file = workspace_root().join("examples/triggers.cue");
    let triggers = rk_workflow::load_triggers(&file)
        .unwrap_or_else(|e| panic!("{} failed to load: {e}", file.display()));
    assert!(!triggers.is_empty(), "example triggers should not be empty");
    // Every example trigger names a workflow that ships in examples/workflows.
    let workflows: Vec<String> = rk_workflow::definitions(&examples_dir())
        .iter()
        .filter_map(|p| p.file_stem().map(|s| s.to_string_lossy().to_string()))
        .collect();
    for t in &triggers {
        assert!(
            workflows.contains(&t.run),
            "trigger '{}' runs unknown workflow '{}'",
            t.name,
            t.run
        );
    }
}

/// The daemon-native landing pipeline's completion feed (steward remediation
/// Phase 3-T4, post-cutover replacement for the retired `steward-on-completion`
/// workflow trigger): an `action: "land"` trigger needs no `run` (the schema
/// makes it optional for that action), and it must match every plain-rat
/// completion the old workflow trigger used to.
#[test]
fn landing_pipeline_trigger_loads_with_no_run_and_matches_rat_completions() {
    let file = workspace_root().join("examples/triggers-landing-pipeline.cue");
    let triggers = rk_workflow::load_triggers(&file)
        .unwrap_or_else(|e| panic!("{} failed to load: {e}", file.display()));
    let landing = triggers
        .iter()
        .find(|t| t.name == "steward-landing-on-completion")
        .expect("shipped landing-pipeline trigger");
    assert_eq!(landing.action, rk_workflow::TriggerAction::Land);
    assert_eq!(landing.run, "");
    assert_eq!(landing.matcher.category.as_deref(), Some("event"));
    assert_eq!(landing.matcher.identity.as_deref(), Some("harness_result"));
    assert_eq!(
        landing.matcher.search.as_deref(),
        Some("\"role\":\"rat\""),
        "must scope to plain-rat completions, the same re-entrancy break the \
         retired workflow trigger relied on"
    );
}

#[test]
fn nightly_self_improve_chains_groom_drain_refine() {
    use rk_workflow::Step;
    // All params default, so it loads with no inputs — exactly how the scheduler
    // fires it (static params only, everything else defaulted).
    let workflow = rk_workflow::load(
        &examples_dir().join("nightly-self-improve.cue"),
        &HashMap::new(),
    )
    .unwrap();

    // Phase 1 GROOM: a single spawn whose merge is a noMerge dismiss (it mutates
    // the ticket store), with NO abort gate — a groom hiccup must not stop the night.
    let Step::Spawn(groom) = &workflow.steps[0] else {
        panic!("phase 1 must open with a groom spawn");
    };
    assert_eq!(groom.task.title, "groom-backlog");
    assert!(
        matches!(&workflow.steps[2], Step::Dismiss(d) if d.no_merge),
        "groom dismisses with noMerge (ticket-store mutation, nothing to merge)"
    );

    // Phase 2 DRAIN: a fan-out over ready tickets, joined and batch-merge gated
    // on every rat finishing cleanly before dismiss_all.
    let Some(Step::ForEach(fe)) = workflow
        .steps
        .iter()
        .find(|s| matches!(s, Step::ForEach(_)))
    else {
        panic!("phase 2 must fan out over ready tickets");
    };
    assert_eq!(fe.query.status, "ready");
    let drain_gate = workflow
        .steps
        .iter()
        .any(|s| matches!(s, Step::Evaluate(e) if e.expect.get("all_ok").is_some()));
    assert!(drain_gate, "the drain batch merge must be gated on all_ok");
    assert!(
        workflow
            .steps
            .iter()
            .any(|s| matches!(s, Step::DismissAll(_))),
        "the drain phase must dismiss_all the fan-out"
    );

    // Phase 3 REFINE: the LAST spawn proposes prompt/convention edits and merges.
    let refine = workflow
        .steps
        .iter()
        .filter_map(|s| match s {
            Step::Spawn(sp) => Some(sp),
            _ => None,
        })
        .next_back()
        .expect("phase 3 refine spawn");
    assert_eq!(refine.task.title, "refine-prompts");

    // The chain is ordered groom-spawn ... for_each ... refine-spawn.
    let groom_at = 0;
    let foreach_at = workflow
        .steps
        .iter()
        .position(|s| matches!(s, Step::ForEach(_)))
        .unwrap();
    let refine_at = workflow
        .steps
        .iter()
        .rposition(|s| matches!(s, Step::Spawn(_)))
        .unwrap();
    assert!(
        groom_at < foreach_at && foreach_at < refine_at,
        "phases must run groom -> drain -> refine in order"
    );
}

#[test]
fn refine_prompts_task_requires_failure_boundary_classification() {
    use rk_workflow::Step;
    // Proposal 0013: an unclassified "recurring pain" reading lets an overnight
    // refine pass mistake infrastructure/model/workflow-policy/repository-gate
    // failures for a role-prompt defect and write a speculative prompt patch.
    // Both copies of the refine-prompts task description must fail closed:
    // require evidence the prompt caused the failure, and route everything
    // else to a ticket instead of a proposal.
    let standalone = rk_workflow::load(&examples_dir().join("prompt-refine.cue"), &HashMap::new())
        .unwrap_or_else(|e| panic!("prompt-refine.cue failed to load: {e}"));
    let Step::Spawn(standalone_refine) = &standalone.steps[0] else {
        panic!("prompt-refine.cue must open with the refine spawn");
    };
    assert_eq!(standalone_refine.task.title, "refine-prompts");

    let nightly = rk_workflow::load(
        &examples_dir().join("nightly-self-improve.cue"),
        &HashMap::new(),
    )
    .unwrap();
    let nightly_refine = nightly
        .steps
        .iter()
        .filter_map(|s| match s {
            Step::Spawn(sp) => Some(sp),
            _ => None,
        })
        .next_back()
        .expect("nightly-self-improve.cue must end with the refine spawn");
    assert_eq!(nightly_refine.task.title, "refine-prompts");

    for (name, description) in [
        ("prompt-refine.cue", &standalone_refine.task.description),
        ("nightly-self-improve.cue", &nightly_refine.task.description),
    ] {
        let description = description
            .as_deref()
            .unwrap_or_else(|| panic!("{name} refine task must have a description"));
        assert!(
            description.contains("Classify each failure before proposing a prompt change"),
            "{name} refine task must require classifying the failure boundary before \
             proposing a prompt change"
        );
        assert!(
            description.contains("do not write a speculative prompt proposal"),
            "{name} refine task must forbid speculative prompt proposals for \
             infrastructure/workflow/gate failures"
        );
    }
}

#[test]
fn fanout_and_nightly_examples_carry_a_budget_cap() {
    // Every unattended fan-out / overnight example must scope spend to ONE run
    // via a per-instance budget cap (#WorkflowBudget), not lean on the
    // fleet-wide caps. A silent drop of this field would let a single runaway
    // pass eat the whole fleet cap — regression-guard it here (TKT-45).
    for name in ["backlog-drain", "cost-tiered-drain", "nightly-self-improve"] {
        let workflow =
            rk_workflow::load(&examples_dir().join(format!("{name}.cue")), &HashMap::new())
                .unwrap_or_else(|e| panic!("{name} failed to load: {e}"));
        let budget = workflow
            .budget
            .unwrap_or_else(|| panic!("{name} must set a per-instance budget cap"));
        assert!(
            budget.max_usd > 0.0,
            "{name} budget.max_usd must be a positive dollar ceiling, got {}",
            budget.max_usd
        );
    }
}

/// TKT-01M08H9QQPJGFS9ET25Q26YSDM (A3): the payload `scripts/rk-action-approval-smoke.py`
/// proposes/approves/executes through the factory action-approval boundary
/// weekly. It must stay genuinely harmless (no merge, no branch mutation) —
/// pin its shape here so a hand-edit that adds real work to it is caught.
#[test]
fn action_approval_smoke_target_is_a_harmless_noop() {
    use rk_workflow::Step;

    let workflow = rk_workflow::load(
        &examples_dir().join("action-approval-smoke-target.cue"),
        &HashMap::new(),
    )
    .unwrap_or_else(|e| panic!("action-approval-smoke-target.cue failed to load: {e}"));

    assert_eq!(workflow.name, "action-approval-smoke-target");
    assert!(
        workflow.params.is_empty(),
        "the smoke target must need no params: the operator script never supplies any"
    );

    let Step::Spawn(spawn) = &workflow.steps[0] else {
        panic!("smoke target must open with a spawn");
    };
    assert_eq!(spawn.role, "rat");

    assert!(
        workflow
            .steps
            .iter()
            .any(|s| matches!(s, Step::Dismiss(d) if d.no_merge)),
        "the smoke target must dismiss with noMerge — it must never merge a branch"
    );
    assert!(
        !workflow.steps.iter().any(|s| matches!(
            s,
            Step::Land(_) | Step::OpenPr(_) | Step::ForEach(_) | Step::DismissAll(_)
        )),
        "the smoke target must stay a single harmless spawn, not gain real side effects"
    );
}

#[test]
fn shipped_example_schedules_load() {
    let file = workspace_root().join("examples/schedules.cue");
    let schedules = rk_workflow::load_schedules(&file)
        .unwrap_or_else(|e| panic!("{} failed to load: {e}", file.display()));
    assert!(
        !schedules.is_empty(),
        "example schedules should not be empty"
    );
    // Every example schedule names a workflow that ships in examples/workflows.
    let workflows: Vec<String> = rk_workflow::definitions(&examples_dir())
        .iter()
        .filter_map(|p| p.file_stem().map(|s| s.to_string_lossy().to_string()))
        .collect();
    for s in &schedules {
        assert!(
            workflows.contains(&s.run),
            "schedule '{}' runs unknown workflow '{}'",
            s.name,
            s.run
        );
    }
}

#[test]
fn repository_activates_the_daily_nightly_schedule() {
    let file = workspace_root().join(".rk/schedules.cue");
    let schedules = rk_workflow::load_schedules(&file)
        .unwrap_or_else(|e| panic!("{} failed to load: {e}", file.display()));
    assert_eq!(schedules.len(), 1, "one self-improvement cadence");
    let nightly = &schedules[0];
    assert_eq!(nightly.name, "nightly-self-improve");
    assert_eq!(nightly.cron, "0 3 * * *");
    assert_eq!(nightly.run, "nightly-self-improve");
    assert_eq!(nightly.repo, None, "repo-local discovery supplies the repo");
    assert_eq!(nightly.params.get("limit").map(String::as_str), Some("5"));
    assert_eq!(
        nightly.params.get("budgetUsd").map(String::as_str),
        Some("30")
    );
}

#[test]
fn shipped_example_checks_load() {
    let file = workspace_root().join("examples/checks.cue");
    let checks = rk_workflow::load_checks(&file)
        .unwrap_or_else(|e| panic!("{} failed to load: {e}", file.display()));
    assert!(!checks.is_empty(), "example checks should not be empty");
    // The named-check-merge example's default check must exist in the registry.
    assert!(
        checks.iter().any(|c| c.name == "test"),
        "example checks must register the 'test' check the workflow defaults to"
    );
}

#[test]
fn reviewer_drives_rework_loads_and_routes() {
    use rk_workflow::Step;
    let inputs = HashMap::from([
        ("taskId".to_string(), json!("fix-login")),
        ("maxRounds".to_string(), json!(4)),
    ]);
    let workflow =
        rk_workflow::load(&examples_dir().join("reviewer-drives-rework.cue"), &inputs).unwrap();

    // Top level: spawn, wait, evaluate, dismiss, repeat.
    let Step::Repeat(repeat) = workflow.steps.last().unwrap() else {
        panic!("last step should be a repeat loop");
    };
    // The `max` came from _input.maxRounds — a real, bounded cap.
    assert_eq!(repeat.max, 4);

    // The loop body ends in a `when` routing on the read verdict.
    let read = repeat
        .steps
        .iter()
        .find_map(|s| match s {
            Step::Read(r) => Some(r),
            _ => None,
        })
        .expect("a read step lifting the verdict");
    assert_eq!(read.into, "verdict");
    assert_eq!(read.field.as_deref(), Some("recommendation"));
    // TKT-161: bound to the reviewer this round spawned. Unbound, the read
    // takes the newest (artifact, <repo>, review) in the repo — a concurrent
    // instance's verdict, or this loop's PREVIOUS round if the current
    // reviewer recorded nothing — and the `when` below reworks or breaks on a
    // review of code it is not looking at.
    assert!(
        read.from_agent,
        "the verdict read must be bound to the reviewer that wrote it"
    );

    let Step::When(when) = repeat.steps.last().unwrap() else {
        panic!("loop body should end in a when");
    };
    assert_eq!(when.var, "verdict");
    // APPROVE merges then breaks; STOP aborts; REWORK loops back.
    assert!(matches!(when.cases["APPROVE"].last().unwrap(), Step::Break));
    assert!(when.cases["STOP"]
        .iter()
        .any(|s| matches!(s, Step::Stop(_))));
    assert!(when.cases["REWORK"]
        .iter()
        .any(|s| matches!(s, Step::Spawn(sp) if sp.role == "rat")));
}

#[test]
fn land_on_approve_gates_failed_delivery() {
    use rk_workflow::Step;

    let inputs = HashMap::from([
        ("taskId".to_string(), json!("risky-change")),
        ("description".to_string(), json!("rework retry logic")),
    ]);

    let approval = rk_workflow::load(&examples_dir().join("land-on-approve.cue"), &inputs).unwrap();
    let Step::When(approval_route) = approval.steps.last().unwrap() else {
        panic!("land-on-approve should end in approval routing");
    };
    let approval_gate = approval_route.cases["true"]
        .iter()
        .find_map(|step| match step {
            Step::Evaluate(e) if e.expect.get("merged").is_some() => Some(e),
            _ => None,
        })
        .expect("land-on-approve APPROVE must gate the land result");
    assert_eq!(approval_gate.expect.get("merged"), Some(&json!(true)));
    assert_eq!(approval_gate.any_of, vec![json!({"pr_opened": true})]);
}

/// TKT-174: EVERY shipped example that routes on a reviewer's verdict must bind
/// the read to the reviewer that wrote it — swept across the whole example set,
/// not asserted file by file.
///
/// TKT-161 fixed the two examples that carried the unbound read at the time, and
/// pinned each one in its own test. That is not a guarantee about the class: the
/// live fleet also ran a hand-copied `steward-grmpl` with no repo source, which
/// kept the unbound read for three more days and could auto-merge a branch on a
/// concurrent instance's verdict. A per-file assertion cannot fail for a file
/// nobody wrote a test for, so assert the property over the directory — the next
/// steward variant, or a copy-paste of an existing one, is covered the moment it
/// lands here.
///
/// This guards the SHIPPED set. It cannot reach a repo-scoped workflow living in
/// some other repo's `.rk/workflows/` (which is where `steward-grmpl` belongs —
/// TKT-178), so a per-repo steward still has to carry the binding on its own.
///
/// `(artifact, <repo>, review)` is the shared key: a steward is fired PER rat
/// completion, so instances run concurrently on one repo by design and all of
/// them read it. Unbound, "newest match wins" is whichever reviewer finished
/// last.
#[test]
fn every_shipped_verdict_read_is_bound_to_its_reviewer() {
    use rk_workflow::Step;

    /// Every read of a `review` artifact anywhere in a step tree, including
    /// inside `when` arms and `repeat` bodies — the binding has to hold wherever
    /// the read is nested, not just at the top level.
    fn verdict_reads<'a>(steps: &'a [Step], found: &mut Vec<&'a rk_workflow::ReadStep>) {
        for step in steps {
            match step {
                Step::Read(r) if r.identity == "review" => found.push(r),
                Step::When(w) => {
                    for branch in w.cases.values() {
                        verdict_reads(branch, found);
                    }
                    verdict_reads(&w.default, found);
                }
                Step::Repeat(r) => verdict_reads(&r.steps, found),
                _ => {}
            }
        }
    }

    let inputs = HashMap::from([
        ("taskId".to_string(), json!("example-task")),
        ("description".to_string(), json!("example description")),
        (
            "taskDescription".to_string(),
            json!("example feature-set description"),
        ),
        (
            "question".to_string(),
            json!("How does the tuplespace work?"),
        ),
        ("branch".to_string(), json!("rat/example-task/tkt-1")),
        ("reviewAttempt".to_string(), json!("landing-review-example")),
    ]);
    let mut total = 0;
    for def in rk_workflow::definitions(&examples_dir()) {
        let name = def.file_name().unwrap().to_string_lossy().to_string();
        let workflow = rk_workflow::load(&def, &inputs)
            .unwrap_or_else(|e| panic!("{name} failed to load: {e}"));
        let mut reads = Vec::new();
        verdict_reads(&workflow.steps, &mut reads);
        for read in &reads {
            assert!(
                read.from_agent || read.for_commit.is_some(),
                "{name}: the verdict read into `{}` must be bound to the reviewer that wrote \
                 it (`fromAgent: true`) or to the exact commit it verdicts on (`forCommit`), \
                 or a concurrent instance's or stale commit's verdict can route it",
                read.into
            );
            if read.for_commit.is_some() {
                assert!(
                    read.for_branch.as_deref().is_some_and(|b| !b.is_empty()),
                    "{name}: a `forCommit` read must also bind `forBranch`, or a verdict from \
                     one branch can satisfy a probe for another sharing the same tip commit"
                );
            }
        }
        total += reads.len();
    }
    // The premise: the shipped set really does contain a verdict-routed
    // workflow (reviewer-drives-rework). Without this the loop above passes
    // vacuously if it is renamed away.
    assert!(
        total >= 1,
        "expected the shipped examples to route on reviewer verdicts, found {total}"
    );
}

#[test]
fn pr_on_approve_opens_a_pr_on_approve_only() {
    use rk_workflow::Step;
    let inputs = HashMap::from([
        ("taskId".to_string(), json!("risky-change")),
        ("description".to_string(), json!("rework retry logic")),
    ]);
    let workflow = rk_workflow::load(&examples_dir().join("pr-on-approve.cue"), &inputs).unwrap();

    // Ends in a `when` routing on the human's approval decision.
    let Step::When(when) = workflow.steps.last().unwrap() else {
        panic!("pr-on-approve should end in a when routing on the approval decision");
    };
    assert_eq!(when.var, "approved");

    // The PR counterpart to land-on-approve: APPROVE opens a PR (never merges),
    // and it targets the workflow's `target` param (main by default).
    let open_pr = when.cases["true"]
        .iter()
        .find_map(|s| match s {
            Step::OpenPr(o) => Some(o),
            _ => None,
        })
        .expect("APPROVE must open a PR");
    assert_eq!(open_pr.branch, "{{ctx.activeBranch}}");
    assert_eq!(open_pr.target, "main");
    // APPROVE must never fall back to a direct land/merge.
    assert!(
        !when.cases["true"]
            .iter()
            .any(|s| matches!(s, Step::Land(_))),
        "pr-on-approve must hand off via a PR, never land directly"
    );
    // The PR result is gated: a failed push must fail closed, not complete clean.
    assert!(
        when.cases["true"]
            .iter()
            .any(|s| matches!(s, Step::Evaluate(e) if e.expect.get("pr_opened").is_some())),
        "APPROVE must gate on pr_opened so a failed hand-off surfaces"
    );

    // REJECT holds the branch: no PR, no land — just a no-merge dismiss.
    assert!(
        !when.cases["false"]
            .iter()
            .any(|s| matches!(s, Step::OpenPr(_) | Step::Land(_))),
        "REJECT must hold the branch, never open a PR or land"
    );
}

#[test]
fn code_review_resolves_reviewer_profile() {
    let inputs = HashMap::from([("taskId".to_string(), json!("t1"))]);
    let workflow = rk_workflow::load(&examples_dir().join("code-review.cue"), &inputs).unwrap();
    // The reviewer spawn resolves through the named profile to the cheap model.
    let reviewer = workflow
        .steps
        .iter()
        .find_map(|s| match s {
            rk_workflow::Step::Spawn(sp) if sp.role == "reviewer" => Some(sp),
            _ => None,
        })
        .expect("reviewer spawn step");
    let resolved =
        rk_workflow::resolve::resolve(reviewer, &workflow.agents, &HashMap::new(), "fake").unwrap();
    assert_eq!(resolved.harness, "claude");
    assert_eq!(resolved.model.as_deref(), Some("haiku"));
}

/// TKT-172: every example that parks at an approval gate must bind the `read`
/// that lifts the decision to its own instance.
///
/// The gate itself is already per-instance — it waits on `"instance":"<id>"` —
/// but `(event, <repo>, workflow_approval)` is shared by every gated instance on
/// the repo, so an unbound read behind it takes whichever decision landed last.
/// Approve one run and reject another and both route the same way: the rejected
/// branch merges on the approval, or the approved one is held on the rejection.
/// The fail-closed timeout widens it further, since a timing-out instance
/// synthesises an `{approved: false}` a live peer can then route on.
///
/// This is the static half of the guarantee — that the shipped examples carry
/// the binding, and that a hand-edit dropping it is caught here rather than in a
/// castle. The dynamic half (two instances, one approved and one rejected,
/// each routing on its own decision) is
/// `rk-daemon/tests/workflow_approval_binding.rs`.
#[test]
fn approval_gated_examples_bind_their_decision_read_to_the_instance() {
    use rk_workflow::Step;

    /// Every read of a `workflow_approval` event anywhere in a step tree,
    /// including inside `when` arms and `repeat` bodies — the binding has to
    /// hold wherever the read is nested, not just at the top level.
    fn approval_reads<'a>(steps: &'a [Step], found: &mut Vec<&'a rk_workflow::ReadStep>) {
        for step in steps {
            match step {
                Step::Read(r) if r.identity == "workflow_approval" => found.push(r),
                Step::When(w) => {
                    for branch in w.cases.values() {
                        approval_reads(branch, found);
                    }
                    approval_reads(&w.default, found);
                }
                Step::Repeat(r) => approval_reads(&r.steps, found),
                _ => {}
            }
        }
    }

    let inputs = HashMap::from([
        ("taskId".to_string(), json!("risky-change")),
        ("description".to_string(), json!("rework retry logic")),
    ]);
    for name in [
        "gated-merge.cue",
        "land-on-approve.cue",
        "pr-on-approve.cue",
    ] {
        let workflow = rk_workflow::load(&examples_dir().join(name), &inputs).unwrap();
        // The premise: these examples do park at an approval gate. If one stops
        // doing so the loop below would vacuously pass, so assert it outright.
        assert!(
            workflow
                .steps
                .iter()
                .any(|s| matches!(s, Step::Gate(g) if g.gate_type == "approval")),
            "{name} should park at an approval gate"
        );
        let mut reads = Vec::new();
        approval_reads(&workflow.steps, &mut reads);
        assert!(
            !reads.is_empty(),
            "{name} should lift the approval decision with a read"
        );
        for read in reads {
            assert!(
                read.from_instance,
                "{name}: the approval read into `{}` must be bound to this instance, or a \
                 concurrent instance's decision can route it",
                read.into
            );
        }
    }
}
