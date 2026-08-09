# TKT-01KZFNJP0DCWTS99ZKB8HH125G: named-check test race

## Finding

The reported failure in
`crates/rk-daemon/tests/workflow_checks.rs::named_check_receives_namespaced_data_inputs_under_policy`
is a concurrency-sensitive test-environment failure, not a named-check
resolution failure. Each test in that integration binary writes the
process-global `RK_FAKE_HARNESS_CMD`; Tokio runs the tests concurrently, so a
sibling can remove or replace the variable while a workflow is spawning its
fake harness. The workflow may then complete without the fixture's commit, and
the postcondition `git ls-tree main` contains no `work-*` file.

## Evidence

- The focused test passed with the required stripped `RK_*` environment.
- All five tests in `workflow_checks.rs` passed when run as one isolated test
  binary.
- A canonical workspace run passed every `workflow_checks.rs` test, but failed
  the unrelated `workflow_run::approval_gate_blocks_until_approved_then_merges`
  test because its fake harness reported `declared_done: false`.
- The same environment-race signature is recorded for the adjacent named-check
  failure in the tuplespace.

## Existing fix handoff

Roquefort-3 owns the overlapping test path and has already committed the
narrow fix as `9e71d63` on
`rat/roquefort-3/tkt-01kzfpa123g0f68x371bzvytkc`: a Tokio mutex guards all five
tests while they mutate `RK_FAKE_HARNESS_CMD`. This branch deliberately does
not duplicate that edit because the path is actively claimed.

## Verification

```text
env -u RK_AGENT -u RK_TASK -u RK_REPO -u RK_ROLE -u RK_HOME -u RK_BRANCH -u RK_WORKTREE \
  MISE_TRUSTED_CONFIG_PATHS="$PWD" mise exec -- cargo test -p rk-daemon --test workflow_checks
```

Result: 5 passed. The full workspace run was otherwise green through the named
check binary and stopped on the unrelated approval-gate failure above.
