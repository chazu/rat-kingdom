# TKT-180 — the clippy baseline is clean, and now stays that way

**Date:** 2026-07-27 · **Agent:** Marbles-2 · **Ticket:** TKT-180

## What the ticket asked for

TKT-180 (filed 2026-07-26 by Bramble-2) recorded that the committed tree was
warning-dirty under the mise-pinned Rust 1.95.0: three `unnecessary_sort_by` in
`rk-daemon/src/inbox.rs` (lines 310/395/534), a `useless_vec` and a
`manual_repeat_n` in `rk-space/src/store.rs`, plus their lib-test duplicates.
None was a behavioural bug. The stated cost was diagnostic, not functional: a
rat verifying its own change cannot tell its warnings from the baseline's —
exactly the confusion TKT-166 documents for `rustfmt`.

The ticket asked for one deliberate sweep by a single rat, or an explicit
decision to leave the warnings and say so.

## What was actually found

The sweep is unnecessary. Every site the ticket named was already fixed on
`main` by commit **69b06a1** (`fix: harden swarm lifecycle and resource
bounds`, 2026-07-26 23:46), which landed hours after the ticket was filed and
folded these lints in alongside its own changes:

| Site | Lint | Fix in 69b06a1 |
| --- | --- | --- |
| `inbox.rs` (PR ordering) | `unnecessary_sort_by` | `sort_by(\|a,b\| b.id.cmp(&a.id))` → `sort_by_key(\|b\| Reverse(b.id))` |
| `inbox.rs` (urgency ordering) | `unnecessary_sort_by` | same shape, on `b.urgency` |
| `inbox.rs` (`dropped_lands`) | `unnecessary_sort_by` | same shape, on `b.id` |
| `store.rs:412` | `manual_repeat_n` | `repeat("?").take(n)` → `repeat_n("?", n)` |
| `store.rs` (test) | `useless_vec` | `vec![weak, strong]` → `[weak, strong]` |

Measured in this worktree at base `e3f6292`:

```
mise exec -- cargo clippy --workspace --all-targets --all-features -- -D warnings
→ exit 0, zero warnings
```

That is a stronger command than the one that produced the ticket's census: it
adds `--all-features` (so `rk-daemon`'s `test-fixtures` feature is covered) and
promotes warnings to errors. `[workspace.lints.clippy] all = { level = "warn" }`
in the root `Cargo.toml` is the lint set in force; nothing is `allow`-ed to
manufacture the clean result.

## What this commit changes

Reporting "already fixed" and stopping would leave the ticket's actual problem
in place — nothing tells a rat the baseline is clean, and nothing keeps it
clean. So the deliverable is the durable half:

- **`mise.toml` gains `lint`, `test`, and `verify` tasks.** `mise run lint` is
  `cargo clippy --workspace --all-targets -- -D warnings`; `mise run verify` is
  build + test + lint, i.e. step 2 of the completion protocol as one command.
  All run through mise, so they get the pinned 1.95.0 rather than whatever
  `cargo` is on `PATH`.
- **README `## Development` rewritten.** It previously showed bare
  `cargo test --workspace` / `cargo clippy --workspace --all-targets`, which is
  actively wrong on a box where `PATH` cargo is 1.85 — that form fails the MSRV
  check before compiling anything, a trap recorded independently in several
  fleet facts. It now leads with the mise tasks, states the pin, and states that
  the baseline is clean so any clippy output belongs to the reader's change.

`-D warnings` is deliberately confined to these opt-in commands rather than set
in `[workspace.lints]`. Denying workspace-wide would mean the next toolchain
bump breaks `cargo build` for everyone the moment it adds a lint — the failure
mode this ticket exists to describe, escalated from confusing to blocking.

## Found while verifying: the test suite cannot pass inside a rat (TKT-182)

Running the new `verify` task from this worktree failed — not on clippy, on
`crates/rk-cli/tests/reviewer_drives_rework.rs`:

```
Protocol("forbidden: Marbles-2 is not authorized for workflow.run")
```

`rk_daemon::Client::connect` (`client.rs:26`) reads `RK_AGENT` from the process
environment and sends it as the RPC `caller`. `Server::authorized`
(`server.rs:756`) allows anything for caller `operator` or empty, but refuses an
*agent* caller for the operator-only method list — `workflow.run`,
`agent.spawn`, `repo.add`, `ticket.update`, and a dozen more. A rat's spawn env
sets `RK_AGENT`, test processes inherit it, and so every test that drives a test
daemon through one of those methods is rejected.

Proven by isolation, not inference — same commit, no code change, the only
variable is one environment variable:

| `cargo test --workspace --no-fail-fast` | Exit | Test binaries failed | Tests failed |
| --- | --- | --- | --- |
| as spawned (`RK_AGENT=Marbles-2`) | 101 | **39** | **79** |
| under `env -u RK_AGENT` | 0 | 0 | 0 (418 passed) |

All 79 failures have a single cause — every one panics on
`Protocol("forbidden: ...")`, nothing else. By method:

| Refused method | Failures |
| --- | --- |
| `workflow.run` | 36 |
| `agent.spawn` | 24 |
| `repo.add` | 6 |
| `stop` | 2 |
| `coordinator.watch` | 1 |

The consequence is the same failure class this ticket is about, one level up,
and far larger than the five clippy warnings that prompted it: the completion
protocol tells every rat to verify with `cargo test --workspace`, and that
command **cannot go green inside a rat** — only in an operator shell. A rat
running it sees 79 failures it did not cause, in 39 binaries, with no way to
tell them from a real regression. Any rat that has ever reported "pre-existing
failures, unrelated to my change" against this suite was looking at this.

So the `test` and `verify` tasks wrap the suite in `env -u RK_AGENT`. That is a
runner-level workaround, deliberately: the daemon's authorization is behaving
correctly, and the actual defect is that test helpers inherit ambient identity.
Fixing that properly means an explicit-caller constructor on `Client` rather
than env mutation (`set_var` in a test process is global and racy under cargo's
threaded runner). Filed as **TKT-182**, not started here — it touches test
helpers across crates and would race peers, the same reason TKT-180 asked for
one deliberate sweep.

`RK_HOME` is *not* affected: `supervisor.rs:2532` sets it per spawned agent from
the test's own layout and `rk-fixture-done` reads its own env, so an ambient
`RK_HOME` never points a test at the live fleet store.

## Relationship to the fmt baseline (TKT-166)

TKT-180 was filed as the clippy sibling of the `rustfmt` baseline drift in
TKT-166. They are no longer symmetric: **clippy is clean, fmt is not.** The
fmt baseline still disagrees with `rustfmt` 1.9.0 across dozens of untouched
files, so the "format only files you changed, revert fmt churn elsewhere" rule
in the rat prompt still applies in full. Only the clippy half of that caution
is now retired.

## Regression risk

A toolchain bump is the one thing that re-dirties this, and it does so for the
whole workspace at once. `mise run lint` makes that visible as a single
deliberate failure, and the sweep is then one commit — the shape 69b06a1 used,
just not buried inside an unrelated change next time.
