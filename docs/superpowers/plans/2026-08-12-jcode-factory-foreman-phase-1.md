# Jcode Factory Foreman Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a repository-local Jcode Factory Foreman skill that produces deterministic read-only Rat Kingdom snapshots and reliability triage from existing `rk --json` commands, then proposes workflow dispatch commands that Jcode may execute only after explicit user approval.

**Architecture:** Keep Rat Kingdom core unchanged. A Python standard-library helper under the repository-local skill runs existing `rk --json` observation commands through an injectable command runner, normalizes partial failures into one snapshot, classifies known reliability symptoms deterministically, and renders JSON or Markdown. `SKILL.md` defines the operator workflow and a strict propose-then-approve boundary for all mutations. Phase 1 deliberately does not add an MCP server, background daemon, external SDLC ingestion, or automatic dispatch.

**Tech Stack:** Jcode repository-local skills, Python 3 standard library, `unittest`, existing Rat Kingdom JSON CLI, Markdown.

## Global Constraints

- Do not modify Rat Kingdom daemon, workflow, ticket, repository-policy, or harness semantics in Phase 1.
- Observation commands are operationally read-only but most `rk` read commands call `Client::connect_or_spawn`. The helper must first run `rk --json daemon status`, which uses strict `Client::connect`; if no daemon is running, stop without running any observation command and instruct the user to start it separately. Phase 1 must never auto-start the daemon.
- Observation commands after that preflight: `rk --json list`, `inbox`, `workflow list`, `workflow defs --repo`, `cost --fleet`, `repo show`, and `ticket list`.
- A failed observation command must be represented in output and must not discard successful observations.
- The helper must never execute `workflow run`, `spawn`, `dismiss`, `approve`, `reject`, `revert`, ticket mutations, or tuple writes.
- The skill may execute a mutating `rk` command only after a later user message explicitly approves the exact rendered command. Initial requests to inspect, triage, fix, or improve the factory are not dispatch approval.
- Keep `SKILL.md` below 500 lines with valid YAML frontmatter and repository-local paths.
- Use only Python's standard library. Do not add runtime or development dependencies.
- Follow test-driven development. Each behavior gets a failing test before implementation.
- Commit each independently testable task.

---

### Task 1: Deterministic Factory Snapshot

**Files:**
- Create: `.jcode/skills/factory-foreman/scripts/factory_foreman.py`
- Create: `.jcode/skills/factory-foreman/tests/test_factory_foreman.py`
- Create: `.jcode/skills/factory-foreman/tests/fixtures/*.json`

**Interfaces:**
- Produces: `CommandResult`, `Observation`, `FactorySnapshot`, `SubprocessRunner`, `daemon_preflight(runner: Runner) -> Observation`, `collect_snapshot(repo: str, runner: Runner) -> FactorySnapshot`, and `FactorySnapshot.to_dict() -> dict`.
- Command names: `agents`, `inbox`, `workflows`, `definitions`, `cost`, `repository`, `tickets`.
- Snapshot schema: `{schema, generated_at, repo, healthy, observations, errors}`. Each observation is `{ok, command, data}` on success or `{ok, command, error}` on failure.

- [ ] **Step 1: Write failing snapshot tests**

Implement these exact test methods with fixture-backed inputs:

- `test_daemon_preflight_uses_strict_status_before_observations`: assert the first call is `("rk", "--json", "daemon", "status")`.
- `test_unavailable_daemon_stops_without_observation_autostart`: fail daemon status and assert no later argv is executed.
- `test_collect_snapshot_runs_only_read_only_json_commands`: assert later `runner.calls` equals the seven argv tuples listed in Step 3, in order.
- `test_collect_snapshot_preserves_successes_when_one_command_fails`: make `inbox` return exit 1, then assert `snapshot.observations["agents"].ok` is true and `snapshot.observations["inbox"].ok` is false.
- `test_collect_snapshot_rejects_non_json_stdout`: return exit 0 with `stdout="not-json"`, then assert the observation error contains `invalid JSON`.
- `test_snapshot_is_unhealthy_when_any_observation_fails`: fail one command and assert `snapshot.healthy` is false and `len(snapshot.errors) == 1`.

Use a fake runner that records argv and returns fixture-backed `CommandResult` values. Assert exact argv arrays rather than shell strings.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 -m unittest discover -s .jcode/skills/factory-foreman/tests -v
```

Expected: failure because `factory_foreman.py` and its public interfaces do not exist.

- [ ] **Step 3: Implement the minimal snapshot collector**

Implement command execution with `subprocess.run(argv, capture_output=True, text=True, timeout=30)`. Parse stdout with `json.loads`. Never use `shell=True`. Convert timeouts, non-zero exits, empty stdout, and malformed JSON into per-observation errors.

Use these exact argv shapes after a successful strict-connect preflight:

```python
("rk", "--json", "daemon", "status")
("rk", "--json", "list")
("rk", "--json", "inbox")
("rk", "--json", "workflow", "list")
("rk", "--json", "workflow", "defs", "--repo", repo)
("rk", "--json", "cost", "--fleet")
("rk", "--json", "repo", "show", repo)
("rk", "--json", "ticket", "list", "--repo", repo)
```

If daemon status fails, return a snapshot with only the failed preflight observation and do not invoke the seven commands that could auto-start it.

- [ ] **Step 4: Run tests and verify GREEN**

Run the unittest command above. Expected: all snapshot tests pass.

- [ ] **Step 5: Commit**

```bash
git add .jcode/skills/factory-foreman/scripts .jcode/skills/factory-foreman/tests
git commit -m "feat: add factory snapshot collector"
```

### Task 2: Reliability Triage Classifier

**Files:**
- Modify: `.jcode/skills/factory-foreman/scripts/factory_foreman.py`
- Modify: `.jcode/skills/factory-foreman/tests/test_factory_foreman.py`
- Add fixtures under: `.jcode/skills/factory-foreman/tests/fixtures/`

**Interfaces:**
- Produces: `Finding`, `TriageReport`, `classify_snapshot(snapshot: FactorySnapshot) -> TriageReport`.
- `Finding` fields: `category`, `severity`, `subject`, `summary`, `evidence`, `recommended_next_step`, `workflow_instance`, `agent`.
- Categories: `empty-harness-result`, `missing-rk-executable`, `named-check-failure`, `workflow-timeout`, `permission-or-authority`, `stale-or-moved-base`, `orphaned-agent`, `budget-pressure`, `unknown`.

- [ ] **Step 1: Write failing classification tests**

Create these focused tests with realistic fixture payloads captured from current `rk --json` output:

- `test_classifies_empty_undeclared_harness_result`: a failed workflow whose harness result has `declared_done:false`, empty `result`, zero usage, and no actionable error produces `empty-harness-result`.
- `test_classifies_missing_rk_command`: detail containing `rk: command not found` produces `missing-rk-executable`.
- `test_classifies_named_check_failure`: a run-step or detail naming a repository check with non-zero exit produces `named-check-failure`.
- `test_classifies_timeout_separately_from_red_check`: `timed_out:true`, exit 124, or `timed out` produces `workflow-timeout`, not `named-check-failure`.
- `test_classifies_orphaned_agent`: inbox kind `agent-orphaned` produces `orphaned-agent`.
- `test_reports_high_cost_instance_as_budget_pressure`: spend at or above 80 percent of that workflow's `instance_max_usd` produces `budget-pressure`.
- `test_unknown_failure_is_preserved_not_dropped`: an unmatched `workflow-failed` row produces `unknown` with the original detail preserved.
- `test_findings_are_deduplicated_and_severity_sorted`: duplicate rows collapse by `(category, subject, workflow_instance)` and critical/high findings precede medium/low findings.

- [ ] **Step 2: Run tests and verify RED**

Run the unittest command. Expected: classifier tests fail because the interfaces are missing.

- [ ] **Step 3: Implement deterministic classification**

Classify from structured fields first and bounded lowercase text matching second. Preserve source evidence without interpreting it as authority. Do not infer causality. Mark unknown failures explicitly.

Default budget-pressure threshold: instance spend greater than or equal to 80 percent of `instance_max_usd`, when both values exist. Do not compare historical instance spend to the current fleet remaining budget.

- [ ] **Step 4: Run tests and verify GREEN**

Run the unittest command. Expected: all classifier and snapshot tests pass.

- [ ] **Step 5: Commit**

```bash
git add .jcode/skills/factory-foreman/scripts .jcode/skills/factory-foreman/tests
git commit -m "feat: classify factory reliability failures"
```

### Task 3: CLI and Human-Readable Report

**Files:**
- Modify: `.jcode/skills/factory-foreman/scripts/factory_foreman.py`
- Modify: `.jcode/skills/factory-foreman/tests/test_factory_foreman.py`

**Interfaces:**
- Produces CLI subcommands:
  - `snapshot --repo NAME --format json|markdown`
  - `triage --repo NAME --format json|markdown`
  - `propose-workflow WORKFLOW --repo NAME [--param KEY=VALUE] [--coordinator ID]` where `--param` is repeatable
  - `validate-proposal --proposal-file PATH --approved-id SHA256`
- `propose-workflow` outputs `proposal_id`, an exact argv list, and a shell-escaped display command but never executes it. `proposal_id` is SHA-256 of canonical compact JSON for the argv list.
- `validate-proposal` reloads the saved proposal, recomputes its ID, compares it with `--approved-id`, and outputs the exact argv only on a match. It never executes the argv.

- [ ] **Step 1: Write failing CLI tests**

Implement these exact CLI tests:

- `test_triage_markdown_contains_health_and_findings_sections`: assert output contains `# Factory Triage`, `## Snapshot Health`, and `## Findings`.
- `test_partial_snapshot_warning_is_visible`: fail one observation and assert Markdown contains `DEGRADED` plus that observation name.
- `test_propose_workflow_renders_but_does_not_execute`: pass a runner that raises on invocation, assert the command returns successfully, and assert JSON contains `proposal_id`, `argv`, and `command`.
- `test_validate_proposal_returns_exact_argv_on_matching_id`: save proposal JSON and assert a matching approval ID returns the same argv byte-for-byte.
- `test_validate_proposal_rejects_exact_command_mismatch`: change one parameter or coordinator and assert validation fails without returning executable argv.
- `test_propose_workflow_rejects_invalid_param_without_equals`: pass `--param broken` and assert exit code 2 with a validation message.
- `test_propose_workflow_quotes_shell_display_without_changing_argv`: use a parameter containing spaces and assert `argv` retains one value while `command` is safely quoted.

- [ ] **Step 2: Run tests and verify RED**

Run the unittest command. Expected: CLI tests fail because parser and renderers do not exist.

- [ ] **Step 3: Implement CLI and renderers**

Use `argparse`, `hashlib.sha256`, `json.dumps`, and `shlex.join`. Return non-zero only when every observation fails, the daemon preflight fails, CLI input is invalid, or proposal validation fails. A partial snapshot remains renderable and visibly degraded.

The proposed workflow argv must be:

```python
argv = ["rk", "--json", "workflow", "run", workflow, "--repo", repo]
for value in params:
    argv.extend(["--param", value])
if coordinator is not None:
    argv.extend(["--coordinator", coordinator])
```

This matches `rk workflow run [OPTIONS] <NAME>`. Preserve the argv JSON as the authority identity. The shell display is for humans only and is never treated as an approval token.

- [ ] **Step 4: Run tests and verify GREEN**

Run the unittest command. Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add .jcode/skills/factory-foreman/scripts .jcode/skills/factory-foreman/tests
git commit -m "feat: render factory triage reports"
```

### Task 4: Repository-Local Jcode Skill

**Files:**
- Create: `.jcode/skills/factory-foreman/SKILL.md`
- Create: `.jcode/skills/factory-foreman/REFERENCE.md`
- Create: `.jcode/skills/factory-foreman/tests/test_skill_contract.py`

**Interfaces:**
- Skill name: `factory-foreman`.
- Trigger phrases include Rat Kingdom factory, fleet health, RK inbox, workflow failures, factory triage, dispatch work, and software factory.
- Default command: `python3 .jcode/skills/factory-foreman/scripts/factory_foreman.py triage --repo <repo> --format markdown`.

- [ ] **Step 1: Write failing skill-contract tests**

Tests must verify valid frontmatter, required trigger terms, SKILL.md under 500 lines, read-only default, exact approval language, absence of automatic mutation language, and links only one level deep.

The required approval statement is:

```text
Never execute a mutating Rat Kingdom command unless a later user message explicitly approves the exact command rendered in this conversation.
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 -m unittest discover -s .jcode/skills/factory-foreman/tests -v
```

Expected: skill-contract tests fail because `SKILL.md` does not exist.

- [ ] **Step 3: Write SKILL.md and REFERENCE.md**

The skill workflow must:

1. Resolve the repository name without guessing when ambiguous.
2. Run read-only triage first.
3. Report snapshot degradation before conclusions.
4. Separate observed evidence from hypotheses.
5. Deduplicate existing tickets before proposing new work.
6. Recommend an existing workflow definition where possible.
7. Render and save the exact dispatch proposal using `propose-workflow`, including its `proposal_id`.
8. Stop and request approval for that exact `proposal_id` and displayed command.
9. After a later user message explicitly approves that proposal ID or exact command, run `validate-proposal` and execute only the validated argv. A changed workflow, repo, parameter, coordinator, or argv order requires a new proposal and new approval.
10. Monitor the returned workflow ID with `rk --json workflow status <id>` or `rk --json workflow watch <id>` through completion, failure, or approval wait. `workflow watch --json` is NDJSON and must not be parsed as one JSON document.

REFERENCE.md documents categories, JSON schema, approval examples, and recovery behavior.

- [ ] **Step 4: Run tests and verify GREEN**

Run the unittest command. Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add .jcode/skills/factory-foreman
git commit -m "feat: add Jcode factory foreman skill"
```

### Task 5: Real Read-Only Acceptance and Documentation

**Files:**
- Create: `docs/factory-foreman.md`
- Modify: `README.md`

**Interfaces:**
- Documents installation/discovery, snapshot schema, triage categories, approval boundary, and known Phase 1 limitations.

- [ ] **Step 1: Run live read-only acceptance after Tasks 1-4**

Run the helper against the live local daemon and save output under `$JCODE_SCRATCH_DIR`, not the repository:

```bash
python3 .jcode/skills/factory-foreman/scripts/factory_foreman.py triage \
  --repo rat-kingdom --format json > "$JCODE_SCRATCH_DIR/factory-triage.json"
```

Validate with Python standard library only:

```bash
python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); assert d["schema"] == 1 and d["repo"] == "rat-kingdom" and isinstance(d["findings"], list)' "$JCODE_SCRATCH_DIR/factory-triage.json"
```

Expected: valid JSON when the daemon is already running. If the daemon is unavailable, the command must fail without starting it and print the separate operator action required.

- [ ] **Step 2: Add documentation**

Clearly label Phase 1 limitations:

- no typed MCP transport;
- no daemon subscription;
- no cryptographic or daemon-enforced approval token;
- no automatic dispatch;
- read commands are preceded by strict `rk --json daemon status`, but once connected they may still cause ordinary daemon logging or access-time state;
- approval identity is proposal-digest checked in the helper, while the fact that a human approved remains enforced by the Jcode skill rather than cryptographically attested by the daemon;
- no CI, deployment, or production signal ingestion;
- classifications are deterministic triage hints, not proven root causes.

- [ ] **Step 3: Run full verification**

```bash
python3 -m unittest discover -s .jcode/skills/factory-foreman/tests -v
MISE_TRUSTED_CONFIG_PATHS="$PWD" mise run verify
```

- [ ] **Step 4: Run live acceptance**

Run the triage command and Python standard-library assertion above. Confirm it identifies the current empty-result, missing-`rk`, orphaned-agent, named-check, and budget-pressure evidence when those rows remain present. Absence of a category is acceptable only when the corresponding live failure is no longer present.

- [ ] **Step 5: Review the diff**

Check:

```bash
git diff --check
git status --short
git diff --stat origin/main..HEAD
```

An independent reviewer must verify requirements, authority boundaries, exact proposal validation, daemon-no-autostart behavior, and that the helper contains no mutating subprocess path.

- [ ] **Step 6: Commit documentation**

```bash
git add README.md docs/factory-foreman.md
git commit -m "docs: document factory foreman prototype"
```
