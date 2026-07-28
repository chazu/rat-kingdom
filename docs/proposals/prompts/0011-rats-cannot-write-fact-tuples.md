# Proposal 0011 — The prompt orders every rat to do something the daemon forbids

**Author:** Asiago-2 (task: refine-prompts)
**Target prompt:** `crates/rk-core/src/prime.rs` → `FRAGMENT_SINGLE_TASK` and
`FRAGMENT_COMPLETION` step 3
**Companion convention:** `hand-off-through-artifact-and-ticket-not-fact`
**Status:** implemented (daemon-side authorization)
**Confidence:** high — reproduced first-hand from inside a live rat, twice, in
two scopes

## The defect

The shared prompt instructs every rat to write a `fact` tuple in **two** places,
one of them the mandatory completion protocol:

`FRAGMENT_SINGLE_TASK`:
> Do not claim, start, or continue any other work, even if you notice claimable
> tasks or open needs — **post a `fact` or `need` tuple** instead and let the
> orchestrator route it.

`FRAGMENT_COMPLETION`, step 3:
> If you hit a pre-existing failure that is unrelated to your change, do NOT fix
> it inline (peers on other branches will race you) — file a ticket and **post a
> `fact` tuple** describing it, then finish your own task.

**A rat cannot do this.** Reproduced from inside this rat, at both scopes, with a
minimal payload:

```
$ rk out fact rat-kingdom probe-asiago2 --payload '{"t":"1"}'
Error: protocol: forbidden: agents cannot write furniture, fact, convention,
task, or available tuples (withdraw a ballot with `rk withdraw`, which checks
authorship)

$ rk out fact system probe-asiago2 --payload '{"t":"1"}'
Error: protocol: forbidden: …

$ rk need "probe: verifying which tuple categories a rat may write"
need recorded                                        # control: not a blanket denial
```

The guard is `crates/rk-daemon/src/server.rs:1994`, in `handle_out`. It arrived
with **`de689fe security: authenticate and scope daemon clients` (2026-07-26
22:54)** and is correct on its own terms: a `Fact` outranks every agent trail in
the hot-scan weighting (`tuple.rs`, `Category::ALL` runs `Fact` highest), so
letting any rat mint top-weight, non-consumable assertions is exactly the write
an authentication pass should close.

The prompt was simply never updated to match. This is the same class of drift the
`prime.rs` suite already has a named regression guard for — TKT-186, where a
sentence outlived the behaviour it described by nine days — except here the stale
sentence is not merely wrong, it is an **order that fails**.

## Resolution

The daemon-side alternative was implemented: an authenticated agent may write a
`fact` tuple when its `instance` is the agent caller. The existing instance check
still prevents impersonation, `Furniture` lifecycle writes remain denied, and the
other privileged categories remain operator/daemon-only. The prompt's fact-write
instructions are therefore valid again; the artifact routing below is retained as
the historical prompt-side alternative, not as a required substitute.

## Why it is worse than a wasted command

The two instructions sit on the fleet's only two hand-off paths:

1. **Step 3 is the pre-existing-failure route.** It is the rule that stops N rats
   racing each other to fix the same red test inline — the failure that produced
   proposal 0001 and convention `preexisting-failure-is-a-ticket-not-an-inline-fix`
   (TKT-43). Half of that rule now errors out.
2. **`FRAGMENT_SINGLE_TASK` is the scope fence.** "Post a fact instead of
   starting it" is what makes *not* doing the work feel like an action rather
   than a shrug.

A rat that follows either one gets `forbidden` at the moment it is winding down.
The likely responses are all bad: drop the observation entirely; burn turns
retrying and re-shaping the payload; or — worst — read `forbidden` as a symptom
of its own change and file a phantom ticket about it. (Compare 0008: a red suite
a rat did not cause is the exact input that manufactures phantom tickets.)

The corpus contains 18 rat reports ending "artifact + fact tuples posted" or
similar. **Every one predates `de689fe`.** Facts written since then carry the
`castle-…` operator identity, not an agent name — so the observable record shows
the rat-authored fact stream stopping dead on 2026-07-26, with no prompt change
and no rat ever told why.

## What a rat *can* write

Verified this lifetime: `artifact`, `claim`, `need`, `obstacle`, `suggestion`,
`endorsement`, and tickets all succeed. `artifact` is the natural substitute —
it is durable, scoped, carries an arbitrary JSON payload, and the reviewer arm
already teaches reading it (`rk scan artifact <repo>` to find the implementer's
sha).

## Proposed diff

```diff
--- a/crates/rk-core/src/prime.rs
+++ b/crates/rk-core/src/prime.rs
@@ const FRAGMENT_SINGLE_TASK: &str = "\
 You have exactly one task this lifetime: RK_TASK. When it is complete, run
 `rk done \"<one-line summary>\"` and STOP. Do not claim, start, or continue any
-other work, even if you notice claimable tasks or open needs — post a `fact`
-or `need` tuple instead and let the orchestrator route it.
+other work, even if you notice claimable tasks or open needs — file a ticket
+(`rk ticket new`) or post a `need` tuple instead and let the orchestrator route
+it. You cannot write a `fact` tuple: `fact` outranks every agent trail, so the
+daemon reserves it for the operator and `rk out fact` returns `forbidden`. Use
+`rk out artifact <repo> <name> --payload '{...}'` when you need to leave a
+durable, structured finding behind.
```

```diff
@@ const FRAGMENT_COMPLETION: &str = "\
 3. Never `rk done` on a build you broke. If you hit a pre-existing failure that
    is unrelated to your change, do NOT fix it inline (peers on other branches
-   will race you) — file a ticket and post a `fact` tuple describing it, then
-   finish your own task.
+   will race you) — file a ticket and record it as an artifact
+   (`rk out artifact <repo> preexisting-failure --payload '{...}'`), then
+   finish your own task. Do NOT reach for `rk out fact`: agents cannot write
+   `fact` tuples and it will return `forbidden`.
```

## Safety against the `prime.rs` tests

No test asserts on the removed phrases. Checked assertion by assertion:

| test | reads | after this diff |
|---|---|---|
| `templates_teach_area_claim_trails_not_work_claiming` | `rat.contains("Do not claim, start, or continue any")` | **preserved verbatim** — the diff edits only the clause *after* it |
| " | `rat.contains("only your task")` (heading) | untouched |
| `rat_role_includes_all_fragments_once` | `"only your task"`, `"Completion protocol"` counted once | no heading added or duplicated |
| `completion_protocol_puts_the_commit_ahead_of_verification` | `"Commit BEFORE you verify"` / `"Verify with the project's own build"` order, ``"`rk done` is NOT a\n   commit"``, `git status --porcelain`, `git log <base>..HEAD` | steps 1, 2, 4 untouched; step 3 is not read by any assertion |
| `reviewer_role_has_no_single_task_banner` | `!text.contains("only your task")` | reviewer arm still omits `FRAGMENT_SINGLE_TASK` |

**No test change required.** One worth adding, in the shape of the TKT-186 guard:

```rust
#[test]
fn templates_do_not_order_a_write_the_daemon_forbids() {
    // handle_out (server.rs:1994, de689fe) refuses fact/convention/task/
    // available/furniture from an agent caller — `fact` outranks every agent
    // trail, so it is the operator's to write. The prompt ordered it in two
    // places for two days after the guard landed. Re-adding it fails here
    // rather than at minute 25 of a rat's life.
    for role in ["rat", "reviewer"] {
        let text = render(role, &ctx());
        assert!(
            !text.contains("post a `fact` tuple"),
            "{role} template orders a write that returns forbidden"
        );
        assert!(text.contains("rk out artifact"));
    }
}
```

## The design call this exposes (a ticket, not this proposal)

There are two defensible fixes and only one of them is a prompt edit:

- **(a) Prompt-side** — route rats to `artifact` (this proposal). Cheap,
  reversible, correct today, and leaves the security property from `de689fe`
  intact.
- **(b) Daemon-side** — let an agent write a `fact` whose `instance` is itself,
  and rank an agent-authored fact below an operator-authored one, so the
  hot-scan weighting keeps its meaning.

(a) should land regardless — the prompt must not order a failing command while
the question is open. Whether (b) is also wanted is an operator call, filed as a
companion ticket. Note that a fleet-wide `convention` promoted by quorum is
written by the *daemon*, not an agent, so the stigmergy-norms loop (0006) is
unaffected either way.

## Companion convention proposal

```json
{
  "rule": "hand-off-through-artifact-and-ticket-not-fact: A rat cannot write fact, convention, task, available, or furniture tuples — the daemon refuses them from an agent caller (server.rs handle_out, de689fe). Hand work and findings off with `rk ticket new` plus `rk out artifact <repo> <name> --payload '{...}'`, and use need/obstacle for live signals. If you see `forbidden` on `rk out fact`, that is the rule, not a fault in your change: do not retry it and do not file a ticket about it.",
  "why": "The shared prompt orders a fact write in two places — FRAGMENT_SINGLE_TASK's scope fence and completion step 3's pre-existing-failure route — and both have returned `forbidden` for every rat since 2026-07-26. Reproduced first-hand in two scopes. The likely rat responses are dropping the observation, burning turns retrying, or mis-reading `forbidden` as its own breakage and filing a phantom ticket."
}
```
