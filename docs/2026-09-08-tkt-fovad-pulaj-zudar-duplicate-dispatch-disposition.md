# TKT-fovad-pulaj-zudar: disposition of the pilot's recorded duplicate-dispatch finding

D3 proposed-work item 3 (`docs/2026-09-06-r1-qualification-deliverables.md`).
Reconstructs `duplicate_dispatches: 1` from the Glossolalia pilot repeat run
(`~/.rat-kingdom-observations/glossolalia-pilot-repeat-2026-09-02/`, run id
`01M1HS3MBHVNN87XKVKC71JZ5D`) from exact generations, roles, execution windows
and delivery records, and dispositions it.

## Disposition: CONFIRMED — a real duplicate implementation dispatch

Not a legacy-alias mismatch, not legitimate activity misclassified by the
observer, not insufficient evidence.

## Reconstruction

`samples.jsonl` shows `duplicate_dispatches` flip `0 -> 1` at sample 122
(`observed_at: 2026-09-02T20:22:58Z`) and back to `0` at sample 124
(`20:23:58Z`). At that window two `rat`-role agents are both `live` on the
identical `task`: `TKT-rusur-fihar-tubog`.

`agents-archive.json` records both generations in full:

| | Roquefort-13 | Parmesan-13 |
|---|---|---|
| spawn (generation) | `01M1HWK69ZDKE6MMZ24CEDEN20` | `01M1HWK7FT2H5Z5DZ01QHE6MVV` |
| task | `TKT-rusur-fihar-tubog` | `TKT-rusur-fihar-tubog` |
| fork_point | `b51cf0acc4ce3bc784aa454923d94409c0c38eaf` | `b51cf0acc4ce3bc784aa454923d94409c0c38eaf` |
| target_branch | `main` | `main` |
| created_at | `2026-09-02T20:22:31.487856Z` | `2026-09-02T20:22:32.698714Z` |
| updated_at (terminal) | `2026-09-02T20:23:29.284807Z` | `2026-09-02T20:30:13.933369Z` |
| state | `dismissed`, `is_error: true` | `completed`, `merge_commit: e4bd8bd7595390b84ca062da0a52e955a7135636` |
| cost | $0.6169749 (~695k tokens, 0 files landed) | $2.7875757 (landed) |

`crates/rk-daemon/src/server.rs` (`spawn routed to agent profile`) logs two
independent `agent.spawn` admissions for the same raw task string, 1.21s
apart:

```
2026-09-02T20:22:31.421297Z  spawn routed to agent profile task=TKT-rusur-fihar-tubog profile=sonnet-worker source="tier"
2026-09-02T20:22:32.631537Z  spawn routed to agent profile task=TKT-rusur-fihar-tubog profile=sonnet-worker source="tier"
```

Both generations forked from the same commit and targeted the same branch, so
this was a genuine race for the same unit of work, not two agents doing
different things that merely share a label. The two live-agent windows
overlap for ~57s (20:22:32 → 20:23:29). Roquefort-13 was dismissed with no
branch and no commit (`git branch -a` in the `glossolalia` checkout has no
`rat/roquefort-13/...` ref); Parmesan-13 committed `bff4e47`, passed
`mise run verify`, and its branch merged as `e4bd8bd7...` at `20:27:48Z`.
`TKT-rusur-fihar-tubog` is `closed`, assignee `Parmesan-13`. The prior
attempt on this ticket, `Provolone-13`, had already failed the
`steward-diff-scope` gate at `19:58:35Z` and was left as a stalled
escalation (`branch held unmerged`) — the ticket's dependency
(`TKT-fufav-sisuk-lavop`) then landed at `20:22:00Z`, its containing
landing-review workflow finished cleanup at `20:22:27Z`, and the ticket
re-entered the ready set immediately before both `tier`-routed spawns fired.

## Ruling out the legacy-alias comparison gap

`rk ticket show TKT-rusur-fihar-tubog` prints no separate canonical `id` line
(contrast `rk ticket show TKT-gusab-hihus-tijof`, which shows both its
proquint spelling and `id TKT-01M11QE25ARCM49DHSG0K904DJ`) — `TKT-rusur-fihar-tubog`
is a native proquint ticket with no legacy-ULID alias to diverge from. Both
`agent.spawn` calls above used the byte-identical string
`TKT-rusur-fihar-tubog`. `TKT-fabok-birib-rubun` and `TKT-gotup-lamur-pahub`
(the two tickets derived from `TKT-gusab-hihus-tijof`'s audit) are both
explicitly scoped to "only reachable when a caller/rat addresses a LEGACY
(TKT-<ULID>) ticket by its proquint alias" — `reconcile.rs
terminal_assignee_with_handoffs` and `server.rs ticket_reopen_sweep_at` are
not on the path a fresh `agent.spawn` admission takes at all. None of these
three tickets' mechanisms can produce this incident; the alias-comparison
gap does not explain it.

## Actual mechanism and required fix

`crates/rk-daemon/src/supervisor.rs::spawn` (~line 1524) admits every manual
or tier-routed spawn (`fleet_wip_cap: 0`) with **no check for an existing
live agent already working the same `(repo, task)`** — its own doc comment:
"`0` means this caller does not enforce one (manual/operator spawns,
sub-spawns), matching pre-admission-control behaviour." Only the drain
autoscaler and workflow `spawn` steps pass a nonzero `fleet_wip_cap` and
admit through `Registry::try_reserve_wip`, which caps concurrent *count*, not
identity of `task`. `crates/rk-daemon/src/server.rs::handle_spawn` (the RPC
entrypoint both `rk spawn --ticket` and an automated ticket-driven dispatcher
use) has the same gap: it resolves tier routing and calls `spawn_async`
without ever querying the registry for a live generation already on the same
task. The Glossolalia implementation WIP ceiling
(`policy.implementation_admission_limit_by_repo.glossolalia=4`) was not
activated until `2026-09-03T00:29:56Z`, over 4 hours after this incident, and
would not have prevented it in any case — a count ceiling of 4 does not
block two spawns onto the *same* ticket.

Filed `TKT-pumod-hubir-robik` for the fix: add a live-owner/in-flight check
keyed on `(repo, task)` to the manual/tier-routed spawn admission path,
mirroring the WIP-cap admission's atomicity so two near-simultaneous
`agent.spawn` calls for the identical task cannot both succeed.

## Observer accuracy

`crates/rk-cli/src/observation_cmds.rs` computes `duplicate_dispatches` by
grouping `spawning`/`running` agents by the raw `task` string and counting
groups with more than one member. For this incident both agents carried the
identical raw string, so the metric fired correctly — no observer change is
needed for this finding. Note for the record: this same raw-string grouping
is the blind spot the alias-comparison tickets describe from the other
direction — a duplicate dispatched under two *different* spellings of the
same canonical ticket would under-count here. That gap is already tracked by
the existing alias-audit tickets and is out of this ticket's scope.
