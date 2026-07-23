# Proposal 0001 — Strengthen the completion-protocol verification step

**Author:** rat-28 (task: refine-prompts)
**Target prompt:** `crates/rk-core/src/prime.rs` → `FRAGMENT_COMPLETION`
**Status:** proposed (do NOT apply live — see completion-protocol; an operator/steward lands this)

## The recurring pain

Cross-referencing `workflow_failed` events with the rats' `harness_result`
transcripts surfaces two failures that both trace to one weak sentence in the
shared completion protocol.

### Symptom A — non-compiling code reaches a commit

A `workflow_failed` event:

```
evaluate failed: expect {"exit":0} did not unify with {"exit":101,
  "stderr":"error[E0599]: no method named `contains` found for enum `Option` ...
  error: could not compile `rk-workflow` (test \"examples\") due to 3 previous errors"}
```

A rat committed test code that does not compile. It was caught only by a
downstream workflow `run` step, never by the rat itself. Several build rats in
the same batch reported verifying with **`cue vet` only** ("Sanity-checked with
`cue vet` — passes"), treating a schema check as sufficient verification while
the crate never built.

### Symptom B — duplicated rediscovery of a pre-existing red baseline

Three rats in one batch each independently hit the SAME pre-existing failing
test (`all_shipped_examples_load`, which never supplied `research.cue`'s
required `question` param):

- Splinter (TKT-2, $4.49): "I unbroke the pre-existing `all_shipped_examples_load` test."
- Remy (TKT-3, $6.70): "Also fixed a pre-existing red test (`research.cue` ...)."
- Rizzo (TKT-1, $11.15): instead **filed TKT-8** and reverted the unrelated churn.

Two rats fixed an out-of-scope failure inline (scope creep + merge-race on the
same file across three branches); one did the right thing. There was no shared
rule, so the fleet paid three times for the same discovery.

## Root cause

`FRAGMENT_COMPLETION` currently reads:

```
## Completion protocol (mandatory, in order)

1. Ensure the working tree is committed (no uncommitted changes).
2. Run the repo's tests/linters if present; fix what you broke.
3. `rk done "<summary>"` — this is how the orchestrator knows you finished.
```

Step 2 is too weak on two axes:

1. **"tests/linters if present"** invites a partial check (`cue vet`, one-file
   compile) to pass for "verification". It never says *the whole workspace must
   build with the project's own toolchain*.
2. It gives no rule for a **pre-existing** failure unrelated to the task, so
   rats each improvise — some fix inline (scope creep), some file a ticket.

## Proposed change (unified diff)

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@
 const FRAGMENT_COMPLETION: &str = "\
 ## Completion protocol (mandatory, in order)
 
 1. Ensure the working tree is committed (no uncommitted changes).
-2. Run the repo's tests/linters if present; fix what you broke.
-3. `rk done \"<summary>\"` — this is how the orchestrator knows you finished.
+2. Verify with the project's OWN build + test + lint, not a partial check.
+   Detect and run the real toolchain — for Rust: `cargo build`, `cargo test`,
+   and `cargo clippy`; for other stacks, the equivalent. A green `cue vet` or
+   a one-file compile is NOT verification: the whole workspace must build and
+   the suite must pass before you finish.
+3. Never `rk done` on a build you broke. Fix failures YOU introduced. If a
+   build/test was ALREADY red before you started and is unrelated to your
+   task, do NOT fix it inline — that is scope creep and races other rats on
+   the same files. Instead `rk ticket new` it and post a `fact` so peers do
+   not each rediscover it, then proceed once your OWN change is verified.
+4. `rk done \"<summary>\"` — this is how the orchestrator knows you finished.
 ";
```

## Why this is safe

- Purely additive guidance in the shared fragment; no per-role copy to drift
  (respects the existing single-source-of-truth design of `prime.rs`).
- The `rat_role_includes_all_fragments_once` test asserts on the substring
  `"Completion protocol"`, which is unchanged, so it stays green.
- Reinforces, rather than contradicts, the single-task banner ("Do not claim,
  start, or continue any other work") — a pre-existing failure is explicitly
  routed to a ticket + fact, exactly the escape hatch that banner already names.

## Related

- Convention proposal: `verify-with-real-toolchain`
- Convention proposal: `preexisting-failure-is-a-ticket-not-an-inline-fix`
- Prior art: TKT-8 (the ticket Rizzo correctly filed for symptom B).
