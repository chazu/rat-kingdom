# Backlog groom — 2026-08-16 (Manchego-7)

This pass reviewed the live inventory (`rk ticket list --status open` and
`--status in_progress`, all scopes) before any handoff was created. No
ticket was started or state-mutated: `ticket.update`/`ticket.dep` are
operator-only at the wire level (`crates/rk-daemon/src/server.rs:1193-1194`),
confirmed against source, not just prior tuplespace claims. All findings
below are handed off through this report plus a companion operator ticket,
per the `stale-ticket-findings-use-an-artifact-handoff` convention
(proposal 0014).

A same-dated, unlanded report also exists on `rat/sable-7/tkt-01m01q3b0hnpq77fbedzfk6qfp`
(commit f60d825, `docs/2026-08-16-backlog-groom.md`) covering three of the
34 stale rework clusters found here. This report is independent and more
complete; it does not overwrite that file since Sable-7's branch has not
landed. The operator should treat whichever lands first as canonical and
reconcile the other as a duplicate delivery.

## 1. Stale rework sweep (34 tickets)

Every open or in_progress ticket titled `rework: TKT-...` was checked by
reading the **live status of its referenced ticket** with `rk ticket show`.
All 34 point at a target ticket whose current status is **done** — the
rework was actually completed and reviewed under a later
`steward-review-<tkt>` branch that merged into main (spot-verified via
`git log main --grep`, e.g. `df21be4 merge rat/dart-5/steward-review-tkt-01m01nzbwerf4w9qarwg8p632p into main`).
Each of these rework tickets is stale and should be closed by the operator,
noting the done target as the resolution:

| Rework ticket | Target (status: done) |
| --- | --- |
| TKT-01M02SZWJDFDDQ1HFVQ47EVZ1M | TKT-01M01NZC49QVQHM7P4ZMAEKYMA |
| TKT-01M0399XRAHTJB3ZXM6PS71XJY, TKT-01M03D69MXP1MSQRCB5Q9VPMCC, TKT-01M03H0374G7VQB08K16JDJRA1 | TKT-01M036KH8898T0F5CSPTH07CMR |
| TKT-01M03C14MMKJDDC3E49C42YKE3 | TKT-01M036N1RT74H6NPRH5FMM8A6T |
| TKT-01M03G3RE30JB4A8FQ5APQC0EP | TKT-01M03DREFG15RKDCY87JSDW47B |
| TKT-01M03S27XE84VQ4A17H89HZAMQ, TKT-01M03S5XG11ZJ23S3ZX43855MF, TKT-01M03VXCWS7V7TSY5V28BVE5ME | TKT-01M036NWE1EW5B1PWSHK0MKX8E |
| TKT-01M03VWY4D0WV4YVR93MM5RAF0 | TKT-01M03SD5MDVXEH0W3H0JD994B9 |
| TKT-01M042VZE46M2GA9GY1S0TRZPX | TKT-01M036NWEG0H019BJ16G59RZVP |
| TKT-01M048KVYWT1B9PX3VVNJ3JY3M, TKT-01M04P6VEBY9ZTE5E4EZ61AT9P, TKT-01M04QKX2X0V44W0VWVRSDSZ8E, TKT-01M051533DFDXC0H2ZSGXNZ9Q4 | TKT-01M03ZXR2X84H9JZH505W6H97Z |
| TKT-01M04NZXJ2HDQV0QTQTB7YZB57, TKT-01M0514K8TPS6DYREQWTHR0EQT, TKT-01M0514QWEX6KE07PEYRA35YW6 | TKT-01M049DXCEH0YXJC38ERCNYHWN |
| TKT-01M04WFVYQHMZXFJ0YNNYH3GBY, TKT-01M04X0ZM6018YNPCYQZKPP0KA | TKT-01M04D394PQ8VS5N3V441D1MDD |
| TKT-01M052DMQ9BHF32RJ906F3CE7K, TKT-01M0562V6B0M7Y4329MF16W0XR | TKT-01M049DXEHEA2SH6WRB18S79GD |
| TKT-01M056WY699W12WQ3JB248VKHE | TKT-01M053NGBXY5KS5PQN0RK2ZYT2 |
| TKT-01M057KY8PAY0T3M8QA502Q5X3 | TKT-01M04N6W4X47KMXDA6MH0WPH8H |
| TKT-01M01FPPXKSV36W6EK88X4X7Q7 | TKT-01M00XHVBTEH7A4JEFM0TQZZ5N |
| TKT-01M02BHY3NKCD6APC8ZVGJANNP, TKT-01M02DH7XTTGRJ9CY4HP3AN87A | TKT-01M01DM0VXPD7VV09GX02YMEA1 |
| TKT-01M02C67WK5GTJBZRCZCJ14955 | TKT-01M01EYN0132N30BWP8BXHXDR6 |
| TKT-01M02FF4T0WW1NS81WJ6A6Z81W, TKT-01M02GXCCZB1JFTRVWJR430GZD | TKT-01M01NZBWERF4W9QARWG8P632P |
| TKT-01M02HKRMNZV4P8D6RZCJ6TEER | TKT-01M01NZC1QTGND4GGZ16RHKRAB |
| TKT-01M02QEM21DDSE38Y0A9DE7P0B | TKT-01M02AMKD24WZVVMARJPXKYKSW |

This supersedes/extends the narrower 3-cluster finding already recorded in
operator ticket TKT-01M04D77ZQ39Z92D7D8K3E5QB2 (which only covers the
TKT-01M01NZBWERF4W9QARWG8P632P / TKT-01M036KH8898T0F5CSPTH07CMR /
TKT-01M036NWE1EW5B1PWSHK0MKX8E clusters).

Separately, the two `rework: groom-backlog` tickets
(TKT-01M03KN2K8SQ10W4SE5ND34VWX, open; TKT-01M01Q3B0HNPQ77FBEDZFK6QFP,
in_progress) were filed against prior groom-backlog attempts that delivered
empty commits. This report is a non-empty, task-scoped, committed delivery
addressing the same task, so both should be closed as satisfied by this
commit once reviewed.

## 2. Stale pre-existing/flaky sweep (8 tickets)

Each named test was run once, individually, with the RK_* spawn env
stripped (`env -u RK_AGENT -u RK_TASK -u RK_REPO -u RK_ROLE -u RK_HOME
-u RK_BRANCH -u RK_WORKTREE mise exec -- cargo test ...`). All eight now
pass:

| Ticket | Test | Result |
| --- | --- | --- |
| TKT-01M038K6D2MT7MTAV90Q3CKKPA | `rk-cli::repo_onboard_sessions::onboarding_sessions_are_durable_resumable_and_capability_scoped` | pass |
| TKT-01M049H63SEKB31KTXGJK18Y24 | same test (different symptom, same file) | pass |
| TKT-01M03B27C8WPHY1KNRH1SFVE13 | `rk-daemon::harness_stderr::stderr_lines_are_tagged_in_the_agent_log` | pass |
| TKT-01M052DFCK2Y3939BX60NQ9BDK | `rk-daemon::instance_budget::instance_cap_refuses_later_dispatch_once_hit` | pass |
| TKT-01M05BKN0DQXDT1QXWJD8RBVJ6 | `rk-daemon::agent_archive::live_and_orphaned_records_are_never_archived` | pass |
| TKT-01KZFNWK87VEP6X96WR5Y1FJS7 | `rk-daemon::workflow_checks::*` (all 5) | pass |
| TKT-01M01F0BWFMKVKN674TK1EFMDF | `rk-daemon::workflow_run::*` (all 8) | pass |
| TKT-01KZ6BD8NJ6MG614R6M24QTFEW | `rk-cli::agent_cannot_self_elevate` + `rk-daemon::workflow_checks::*` | pass |

`git log --grep` confirms the workflow_run race was fixed at `6e6f814`
("serialize env-mutating tests in workflow_run.rs") and the workflow_checks
race at `9e71d63` ("isolate workflow harness environment"), both already on
main. TKT-01M01F0BWFMKVKN674TK1EFMDF is additionally a known duplicate of
the already-done TKT-01M00XHVBTEH7A4JEFM0TQZZ5N.

These were single-run verifications (the bar this task's instructions set),
not a full parallel-load `cargo test --workspace` reproduction — the
underlying races were specifically about contention under full-workspace
parallel execution, so a recurrence under heavy fleet load remains possible
even though today's isolated runs are clean. The operator should weigh that
before closing if any of these have reproduced very recently elsewhere.

## 3. Duplicate cluster: disk exhaustion (5 duplicates)

Six open tickets report the same symptom (cargo/rustc `No space left on
device` during a worktree's own `mise run verify`) from 2026-08-15/16:
TKT-01M04ABEE1HBGZH1MBTM1ZMPR2, TKT-01M04D1QDBNCF0T0D0EHRVNJV5,
TKT-01M04F17GB0RM7GMR7BEWYSQNW, TKT-01M04F1F7VJ0E9D2VAZ6CZMNES,
TKT-01M04GG0RHZE5CD5HJ8A5HVFJZ, TKT-01M04GRYCMYQ1D2AP2S780XG9Y. Recommend
**TKT-01M04D1QDBNCF0T0D0EHRVNJV5** as survivor — it is the only one with
root-cause analysis (per-worktree `target/` dirs never shared/pruned across
~60+ concurrent worktrees) and concrete fix directions (shared
`CARGO_TARGET_DIR`/sccache, a disk-usage watchdog, or a per-worktree quota).
The other five are symptom-only duplicates and should close with a note
pointing at the survivor. This is an infrastructure/capacity ticket, not a
code defect (per the `preexisting-failure-is-a-ticket-not-an-inline-fix`
convention, TKT-43) — it was not fixed inline. Root filesystem is currently
at 196Gi free / 6% used, so the symptom is not reproducing right now, but
the root cause (unbounded per-worktree target growth) is unaddressed.

## 4. Ordinary groom: decomposition

Two bundled, un-decomposed tickets were split into their natural
independently-gradable sub-items (each ticket enumerated three (1)/(2)/(3)
items in its own body):

- TKT-01M04X5T98M38ECH5WJ86PK6EB (daemon async hardening) →
  TKT-01M05GJ1TCPV5FX1YY3M8JZV4D (sync-call audit),
  TKT-01M05GJA5XSBKMEC1WDDR3B95T (slow-git shim regression),
  TKT-01M05GJA68WFD1YRWJQ8PM1HYS (schedulable shutdown signal).
- TKT-01M050VVGB7DVVTQ656EV20YPE (P3-T4 live-daemon e2e proofs) →
  TKT-01M05GJA6JZX6G8NHQQJ98KXPA (two-Daemon restart test),
  TKT-01M05GJA753JXH9PNEBKYEF0KP (completion-burst-under-load stress test),
  TKT-01M05GJA7NG3A74WV0TSWHEF8F (inbox row-shape assertion for
  STOP/REWORK escalation).

All other large umbrellas (TKT-85/86/87 and their P13b/P14/P15 children,
TKT-141, TKT-142, TKT-152, TKT-153, TKT-155, TKT-157, TKT-179, the
operator-config and steward-cutover parents) were already decomposed by
prior grooming passes and were left as-is; TKT-85 remains an intentional
load-bearing gate. No new duplicates were found beyond §1 and §3. Global
`grmpl` tickets outside this cluster were reviewed but not otherwise
changed.

## Scope

Snapshot taken 2026-08-16 against a live daemon; ticket counts will drift
as the fleet keeps working. `ticket.update`/`ticket.dep` remain
operator-only in the current source — no ticket status changes were made or
claimed by this pass.
