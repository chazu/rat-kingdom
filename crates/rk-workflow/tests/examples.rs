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
fn repository_verify_check_trusts_only_its_current_worktree() {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(".rk")
        .join("checks.cue");
    let checks = rk_workflow::load_checks(&file)
        .unwrap_or_else(|e| panic!("{} failed to load: {e}", file.display()));
    let verify = checks
        .iter()
        .find(|check| check.name == "verify")
        .expect("repository must declare a verify check");

    assert_eq!(
        verify.command,
        "MISE_TRUSTED_CONFIG_PATHS=\"$PWD\" mise run verify",
        "the named check runs in transient agent worktrees, so trust must be process-local"
    );
    assert_eq!(
        verify.environment_policy,
        rk_workflow::CheckEnvironmentPolicy::StripRkSpawn
    );
}

#[test]
fn repository_policy_preserves_agent_base_and_existing_names() {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(".rk")
        .join("repo.cue");
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
    ]);
    for def in defs {
        let workflow = rk_workflow::load(&def, &inputs)
            .unwrap_or_else(|e| panic!("{} failed to load: {e}", def.display()));
        assert!(!workflow.steps.is_empty(), "{}", def.display());
    }
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
    let steward = triggers
        .iter()
        .find(|trigger| trigger.name == "steward-on-completion")
        .expect("shipped completion trigger");
    assert_eq!(
        steward.params.get("target").map(String::as_str),
        Some("{{tuple.payload.target}}"),
        "steward must preserve the daemon-authored agent base"
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
fn repository_activates_the_daily_nightly_schedule() {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(".rk")
        .join("schedules.cue");
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
    let resolved =
        rk_workflow::resolve::resolve(spawn, &workflow.agents, &HashMap::new(), "fake").unwrap();
    assert_eq!(resolved.harness, "codex");
    assert_eq!(resolved.model.as_deref(), Some("gpt-5.6-luna"));
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
        runs.iter().any(|r| {
            r.check.as_deref() == Some("steward-protected-paths")
                && r.env.contains_key("RK_CHECK_TARGET")
                && r.env.contains_key("RK_CHECK_PROTECTED_PATHS")
        }),
        "a repository-authorized protected-path policy gate must run before merge"
    );
    // A diff-scope gate bounds the SIZE of the branch's diff (#20): count files
    // and added+removed lines vs the target and exit non-zero when over budget.
    // The budget params interpolate to bare integers, so match the numstat probe.
    assert!(
        runs.iter().any(|r| {
            r.check.as_deref() == Some("steward-diff-scope")
                && r.env.contains_key("RK_CHECK_MAX_DIFF_FILES")
                && r.env.contains_key("RK_CHECK_MAX_DIFF_LINES")
        }),
        "a repository-authorized diff-scope guardrail must run before merge"
    );
    assert!(
        runs.iter()
            .all(|r| r.command.is_none() && r.check.is_some()),
        "steward top-level run gates must use repo-owned named checks"
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
    // TKT-161: the steward is fired PER rat completion, so instances run
    // concurrently on one repo by design and every one of them reads
    // (artifact, <repo>, review). The binding is what stops the APPROVE arm
    // below — the only arm that lands — from acting on a peer's verdict.
    assert!(
        read.from_agent,
        "the verdict read must be bound to the reviewer that wrote it"
    );

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
    // Every repository delivery mode reports the same success truth. This
    // keeps the steward independent of merge/push/PR mechanics.
    let land_gate = when.cases["APPROVE"]
        .iter()
        .find_map(|s| match s {
            Step::Evaluate(e) if e.expect.get("delivered").is_some() => Some(e),
            _ => None,
        })
        .expect("APPROVE must gate the policy result on delivered:true");
    assert_eq!(land_gate.expect.get("delivered"), Some(&json!(true)));
    assert!(land_gate.any_of.is_empty());
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
            .any(|s| matches!(s, Step::Run(r) if r.check.as_deref() == Some("steward-file-rework-ticket"))),
        "REWORK must file a follow-up ticket"
    );
    // STOP escalates to the operator via a need tuple and holds the branch.
    assert!(
        when.cases["STOP"].iter().any(
            |s| matches!(s, Step::Run(r) if r.check.as_deref() == Some("steward-report-stop"))
        ),
        "STOP must escalate via a need tuple"
    );
    // Unknown verdict: escalate and fail loudly.
    assert!(
        !when.default.is_empty(),
        "unknown verdicts must route to a default arm"
    );
    assert!(when.default.iter().any(|s| matches!(s, Step::Stop(_))));
}

/// Review tiering (TKT-01M036N1RT74H6NPRH5FMM8A6T): a diffClass of
/// "doc-only" (or "trivial") takes the REDUCED tier — a gate-holder spawn
/// instead of a reviewer, no verdict read/routing, and an unconditional land
/// once the shared gates pass.
#[test]
fn steward_reduced_tier_skips_reviewer_and_lands_unconditionally() {
    use rk_workflow::Step;

    let inputs = HashMap::from([
        ("taskId".to_string(), json!("fix-typo")),
        ("diffClass".to_string(), json!("doc-only")),
    ]);
    let workflow = rk_workflow::load(&examples_dir().join("steward.cue"), &inputs).unwrap();

    let Step::Spawn(spawn) = &workflow.steps[0] else {
        panic!("reduced tier must still start with a spawn (only a spawned agent's worktree can host the gates)");
    };
    assert_eq!(
        spawn.agent.as_deref(),
        Some("gateholder"),
        "the reduced tier must spawn the gate-holder profile, not the reviewer"
    );
    assert!(
        spawn.branch.is_some(),
        "the gate-holder must chain onto the completed branch, same as the reviewer"
    );

    // The same deterministic gates still run — tiering only skips the LLM
    // review, never the gates.
    let runs: Vec<&rk_workflow::RunStep> = workflow
        .steps
        .iter()
        .filter_map(|s| match s {
            Step::Run(r) => Some(r),
            _ => None,
        })
        .collect();
    assert!(runs
        .iter()
        .any(|r| r.check.as_deref() == Some("steward-protected-paths")));
    assert!(runs
        .iter()
        .any(|r| r.check.as_deref() == Some("steward-diff-scope")));
    assert!(runs.iter().any(|r| r.check.as_deref() == Some("verify")));

    // No verdict to read or route on — nothing reviewed the diff.
    assert!(
        !workflow.steps.iter().any(|s| matches!(s, Step::Read(_))),
        "the reduced tier must not read a review verdict"
    );
    assert!(
        !workflow.steps.iter().any(|s| matches!(s, Step::When(w) if w.var == "verdict")),
        "the reduced tier must not route on a review verdict"
    );

    // Land is unconditional (not nested inside a verdict `when`) and still
    // gates the delivery result.
    let land = workflow
        .steps
        .iter()
        .rev()
        .find_map(|s| match s {
            Step::Land(l) => Some(l),
            _ => None,
        })
        .expect("reduced tier must land the branch");
    assert_eq!(land.target, "main");
    let evaluate_after_land = workflow
        .steps
        .iter()
        .rev()
        .find_map(|s| match s {
            Step::Evaluate(e) if e.expect.get("delivered").is_some() => Some(e),
            _ => None,
        })
        .expect("land must be gated by a delivered:true evaluate");
    assert_eq!(evaluate_after_land.expect.get("delivered"), Some(&json!(true)));
}

/// A completion payload with no diffClass at all (a legacy completion) must
/// default to "large" and take the unchanged review tier — fail-closed
/// toward review, not toward skipping it.
#[test]
fn steward_missing_diff_class_defaults_to_review_tier() {
    use rk_workflow::Step;

    let inputs = HashMap::from([("taskId".to_string(), json!("fix-login"))]);
    let workflow = rk_workflow::load(&examples_dir().join("steward.cue"), &inputs).unwrap();

    let Step::Spawn(spawn) = &workflow.steps[0] else {
        panic!("missing diffClass must still start with the reviewer spawn");
    };
    assert_eq!(spawn.role, "reviewer");
    assert_eq!(
        spawn.agent.as_deref(),
        Some("reviewer"),
        "a missing diffClass must default to \"large\" and take the review tier"
    );
    assert!(
        workflow.steps.iter().any(|s| matches!(s, Step::Read(_))),
        "the review tier must still read a verdict"
    );
}

/// Provenance for the commit-keyed verdict cache (Phase 2, and its rework):
/// the reviewer's task carries the branch-tip SHA, and it is instructed to
/// record BOTH head_sha and branch in its verdict artifact (the rework's fix:
/// a sha alone is not exclusive to one branch). A non-empty headSha+branch now
/// takes the cached-review arm, so the reviewer spawn is nested inside its
/// cache-miss (`""`) case rather than sitting at steps[0] — see
/// `steward_cached_verdict_probe_is_bound_to_the_exact_commit_and_gates_the_hit_path`
/// for the shape of the cache probe itself.
#[test]
fn steward_review_tier_threads_head_sha_into_reviewer_task_and_artifact() {
    use rk_workflow::Step;

    let inputs = HashMap::from([
        ("taskId".to_string(), json!("fix-login")),
        ("headSha".to_string(), json!("deadbeef123")),
        ("branch".to_string(), json!("rat/fix-login/tkt-1")),
    ]);
    let workflow = rk_workflow::load(&examples_dir().join("steward.cue"), &inputs).unwrap();

    let Step::When(cache_probe_route) = &workflow.steps[1] else {
        panic!("a non-empty headSha+branch must probe the verdict cache before reviewing");
    };
    let cache_miss = &cache_probe_route.cases[""];
    let Step::Spawn(spawn) = &cache_miss[0] else {
        panic!("a cache miss must fall through to spawning the reviewer");
    };
    let description = spawn
        .task
        .description
        .as_deref()
        .expect("reviewer task must carry a description");
    assert!(
        description.contains("deadbeef123"),
        "the reviewer's task must be told the branch-tip SHA it is reviewing"
    );
    assert!(
        description.contains("\"head_sha\": \"deadbeef123\""),
        "the reviewer must be instructed to record head_sha in its verdict artifact: {description}"
    );
    assert!(
        description.contains("\"branch\": \"rat/fix-login/tkt-1\""),
        "the reviewer must be instructed to record branch in its verdict artifact, or a \
         verdict from one branch can be reused on another sharing the same tip: {description}"
    );
}

/// The commit-keyed verdict cache probe (Phase 2) itself: bounded,
/// non-blocking, keyed on the exact commit, and — critically — its cache-HIT
/// path must never spawn the expensive `reviewer` agent, only the cheap
/// `gateholder` used elsewhere to host deterministic gates. A hit that spawned
/// a fresh reviewer anyway would defeat the entire point of the cache.
#[test]
fn steward_cached_verdict_probe_is_bound_to_the_exact_commit_and_gates_the_hit_path() {
    use rk_workflow::Step;

    let inputs = HashMap::from([
        ("taskId".to_string(), json!("fix-login")),
        ("headSha".to_string(), json!("deadbeef123")),
        ("branch".to_string(), json!("rat/fix-login/tkt-1")),
    ]);
    let workflow = rk_workflow::load(&examples_dir().join("steward.cue"), &inputs).unwrap();

    let Step::Read(probe) = &workflow.steps[0] else {
        panic!("steward with a headSha+branch must start with the cache probe read");
    };
    assert_eq!(probe.category, "artifact");
    assert_eq!(probe.identity, "review");
    assert_eq!(
        probe.for_commit.as_deref(),
        Some("deadbeef123"),
        "the cache probe must be keyed on this exact commit, not fromAgent/fromInstance"
    );
    assert_eq!(
        probe.for_branch.as_deref(),
        Some("rat/fix-login/tkt-1"),
        "the cache probe must also bind the branch, or a verdict from one branch could satisfy \
         a probe for another sharing the same tip commit"
    );
    assert!(!probe.from_agent, "forCommit and fromAgent share one predicate slot");
    assert!(!probe.from_instance, "forCommit and fromInstance share one predicate slot");
    assert_eq!(probe.field.as_deref(), Some("recommendation"));
    assert_eq!(probe.into, "cachedVerdict");
    assert_eq!(
        probe.on_timeout, "continue",
        "a cache miss must not fail the instance — it must fall through to a fresh review"
    );

    let Step::When(route) = &workflow.steps[1] else {
        panic!("the cache probe must be followed by a when on cachedVerdict");
    };
    assert_eq!(route.var, "cachedVerdict");

    // The HIT path (any non-empty cached recommendation) must spawn only the
    // cheap gate-holder — never a fresh `reviewer` — so a retry cannot shop
    // for a better opinion than the one already on record.
    let hit_spawn = route
        .default
        .iter()
        .find_map(|s| match s {
            Step::Spawn(sp) => Some(sp),
            _ => None,
        })
        .expect("the cache-hit path must still spawn a gate-holder to host the gates");
    assert_eq!(
        hit_spawn.agent.as_deref(),
        Some("gateholder"),
        "a cache hit must never spawn the expensive reviewer agent"
    );
    assert!(
        !route
            .default
            .iter()
            .any(|s| matches!(s, Step::Spawn(sp) if sp.agent.as_deref() == Some("reviewer"))),
        "a cache hit must not spawn a fresh reviewer anywhere in its routing"
    );

    // The MISS path (`""`, what a `null` cached value renders as) falls
    // through to the ordinary review arm, which does spawn the reviewer.
    assert!(
        route.cases[""]
            .iter()
            .any(|s| matches!(s, Step::Spawn(sp) if sp.agent.as_deref() == Some("reviewer"))),
        "a cache miss must still spawn the reviewer, unchanged from before the cache"
    );
}

/// An empty headSha (a legacy completion predating Phase 0) cannot key a
/// cache lookup at all — `forCommit: ""` would false-positive-match any other
/// legacy artifact missing the field. It must take the plain review arm with
/// no cache probe, exactly as it did before the cache existed.
#[test]
fn steward_empty_head_sha_skips_the_cache_probe_entirely() {
    use rk_workflow::Step;

    let inputs = HashMap::from([("taskId".to_string(), json!("fix-login"))]);
    let workflow = rk_workflow::load(&examples_dir().join("steward.cue"), &inputs).unwrap();

    assert!(
        !workflow
            .steps
            .iter()
            .any(|s| matches!(s, Step::Read(r) if r.for_commit.is_some())),
        "an empty headSha must never reach a forCommit read"
    );
    let Step::Spawn(spawn) = &workflow.steps[0] else {
        panic!("an empty headSha must start with the plain reviewer spawn, unchanged");
    };
    assert_eq!(spawn.agent.as_deref(), Some("reviewer"));
}

/// The rework's mirror case: a headSha WITHOUT a branch (a branchless attach
/// rat, per `branch`'s own doc — empty only there) is exactly as unsafe to key
/// a cache probe on as an empty sha, since the engine's `forBranch` guard
/// would reject an unbound probe outright. steward.cue must not even attempt
/// one — it takes the plain review arm, same as a missing headSha.
#[test]
fn steward_head_sha_without_branch_also_skips_the_cache_probe() {
    use rk_workflow::Step;

    let inputs = HashMap::from([
        ("taskId".to_string(), json!("fix-login")),
        ("headSha".to_string(), json!("deadbeef123")),
    ]);
    let workflow = rk_workflow::load(&examples_dir().join("steward.cue"), &inputs).unwrap();

    assert!(
        !workflow
            .steps
            .iter()
            .any(|s| matches!(s, Step::Read(r) if r.for_commit.is_some())),
        "a headSha with no branch must never reach a forCommit read"
    );
    let Step::Spawn(spawn) = &workflow.steps[0] else {
        panic!("a headSha with no branch must start with the plain reviewer spawn, unchanged");
    };
    assert_eq!(spawn.agent.as_deref(), Some("reviewer"));
}

#[test]
fn shipped_land_workflows_gate_failed_delivery() {
    use rk_workflow::Step;

    let inputs = HashMap::from([
        ("taskId".to_string(), json!("risky-change")),
        ("description".to_string(), json!("rework retry logic")),
    ]);

    let steward = rk_workflow::load(&examples_dir().join("steward.cue"), &inputs).unwrap();
    let Step::When(steward_route) = steward.steps.last().unwrap() else {
        panic!("steward should end in verdict routing");
    };
    let steward_gate = steward_route.cases["APPROVE"]
        .iter()
        .find_map(|step| match step {
            Step::Evaluate(e) if e.expect.get("delivered").is_some() => Some(e),
            _ => None,
        })
        .expect("steward APPROVE must gate the land delivery result");
    assert_eq!(steward_gate.expect.get("delivered"), Some(&json!(true)));
    assert!(steward_gate.any_of.is_empty());

    let approval =
        rk_workflow::load(&examples_dir().join("land-on-approve.cue"), &inputs).unwrap();
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
        // Set (both) so this sweep also exercises steward's commit-keyed cache
        // probe (Phase 2 and its rework) — a `headSha`-less load, or one
        // missing `branch`, only ever sees the plain, `fromAgent`-bound
        // review read.
        ("headSha".to_string(), json!("cafefeed00")),
        ("branch".to_string(), json!("rat/example-task/tkt-1")),
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
    // The premise: the shipped set really does contain verdict-routed workflows
    // (steward + reviewer-drives-rework). Without this the loop above passes
    // vacuously if they are all renamed away.
    assert!(
        total >= 2,
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
    for name in ["gated-merge.cue", "land-on-approve.cue", "pr-on-approve.cue"] {
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
