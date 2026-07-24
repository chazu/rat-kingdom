//! The shipped example workflows must always load — they are the onboarding
//! surface, and a schema drift that breaks them should fail CI, not users.

use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;

fn examples_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("examples")
        .join("workflows")
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
            "question".to_string(),
            json!("How does the tuplespace work?"),
        ),
    ]);
    for def in defs {
        let workflow = rk_workflow::load(&def, &inputs)
            .unwrap_or_else(|e| panic!("{} failed to load: {e}", def.display()));
        assert!(!workflow.steps.is_empty(), "{}", def.display());
    }
}

#[test]
fn shipped_example_triggers_load() {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("examples")
        .join("triggers.cue");
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

#[test]
fn shipped_example_schedules_load() {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("examples")
        .join("schedules.cue");
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
fn shipped_example_checks_load() {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("examples")
        .join("checks.cue");
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
fn steward_loads_and_routes() {
    use rk_workflow::Step;
    // Trigger passes {taskId, branch, repo}; the rest default. Loads with just
    // taskId, proving every other param is defaulted (the reactor supplies the
    // real branch/repo at fire time).
    let inputs = HashMap::from([("taskId".to_string(), json!("fix-login"))]);
    let workflow = rk_workflow::load(&examples_dir().join("steward.cue"), &inputs).unwrap();

    // The reviewer chains onto the completed branch (spawn.branch set), so the
    // gates below run against that work — not a fresh branch off HEAD.
    let Step::Spawn(spawn) = &workflow.steps[0] else {
        panic!("steward must start by spawning the reviewer");
    };
    assert_eq!(spawn.role, "reviewer");
    assert!(
        spawn.branch.is_some(),
        "reviewer must chain onto the branch param"
    );

    // Two fail-closed gates precede the merge decision: a protected-path POLICY
    // gate and the repo's real RUN gate, each a `run` + `evaluate {exit: 0}`.
    let runs: Vec<&rk_workflow::RunStep> = workflow
        .steps
        .iter()
        .filter_map(|s| match s {
            Step::Run(r) => Some(r),
            _ => None,
        })
        .collect();
    assert!(
        runs.iter().any(|r| r
            .command
            .as_deref()
            .is_some_and(|c| c.contains("git diff --name-only"))),
        "a protected-path policy gate must run before merge"
    );
    // A diff-scope gate bounds the SIZE of the branch's diff (#20): count files
    // and added+removed lines vs the target and exit non-zero when over budget.
    // The budget params interpolate to bare integers, so match the numstat probe.
    assert!(
        runs.iter().any(|r| r
            .command
            .as_deref()
            .is_some_and(|c| c.contains("git diff --numstat"))),
        "a diff-scope guardrail must run before merge"
    );
    let gate_evaluates = workflow
        .steps
        .iter()
        .filter(|s| matches!(s, Step::Evaluate(e) if e.expect.get("exit").is_some()))
        .count();
    assert!(
        gate_evaluates >= 3,
        "the policy, diff-scope, and run gates must each fail closed on non-zero exit"
    );

    // The verdict is lifted, then routed on.
    let read = workflow
        .steps
        .iter()
        .find_map(|s| match s {
            Step::Read(r) => Some(r),
            _ => None,
        })
        .expect("a read step lifting the verdict");
    assert_eq!(read.into, "verdict");
    assert_eq!(read.field.as_deref(), Some("recommendation"));

    let Step::When(when) = workflow.steps.last().unwrap() else {
        panic!("steward should end in a when routing on the verdict");
    };
    assert_eq!(when.var, "verdict");
    // APPROVE is the ONLY path that lands (auto-merge).
    assert!(
        when.cases["APPROVE"]
            .iter()
            .any(|s| matches!(s, Step::Land(_))),
        "APPROVE must land the branch"
    );
    // The land gate accepts EITHER a Direct-merge {merged:true} or a PR-mode
    // {pr_opened:true} — the mode-routed land can land or open a PR, and both
    // are a clean hand-off (TKT-67).
    let land_gate = when.cases["APPROVE"]
        .iter()
        .find_map(|s| match s {
            Step::Evaluate(e) if e.expect.get("merged").is_some() => Some(e),
            _ => None,
        })
        .expect("APPROVE must gate the land result on merged:true");
    assert_eq!(land_gate.expect.get("merged"), Some(&json!(true)));
    assert!(
        land_gate
            .any_of
            .iter()
            .any(|alt| alt.get("pr_opened") == Some(&json!(true))),
        "the land gate must also accept a PR-mode {{pr_opened:true}} outcome"
    );
    assert!(
        !when.cases["REWORK"]
            .iter()
            .any(|s| matches!(s, Step::Land(_))),
        "REWORK must never land"
    );
    // REWORK files a durable ticket rather than looping a rework rat here.
    assert!(
        when.cases["REWORK"]
            .iter()
            .any(|s| matches!(s, Step::Run(r) if r.command.as_deref().is_some_and(|c| c.contains("rk ticket new")))),
        "REWORK must file a follow-up ticket"
    );
    // STOP escalates to the operator via a need tuple and holds the branch.
    assert!(
        when.cases["STOP"]
            .iter()
            .any(|s| matches!(s, Step::Run(r) if r.command.as_deref().is_some_and(|c| c.contains("rk out need")))),
        "STOP must escalate via a need tuple"
    );
    // Unknown verdict: escalate and fail loudly.
    assert!(
        !when.default.is_empty(),
        "unknown verdicts must route to a default arm"
    );
    assert!(when.default.iter().any(|s| matches!(s, Step::Stop(_))));
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
        !when.cases["true"].iter().any(|s| matches!(s, Step::Land(_))),
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
