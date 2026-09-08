# TKT-rifaj-kopom-nojij: TKT-kifaj-lumop-borab close + stale-branch drop request

Dispatched ask (from Scritch-14's artifact `conflict-rework-obsolete`, 01M20ZAYTCW2Y1TWAM23YJ4R9K):
1. Close TKT-kifaj-lumop-borab as obsolete/superseded.
2. Delete/abandon stale branch `rat/rizzo-14/tkt-humih-nusok-lozus`.

Both were framed as outside an agent caller's authority; requesting an operator.

## Verified state

**(1) Already resolved.** `rk ticket show TKT-kifaj-lumop-borab` returns
`status: closed`, `by: daemon`, `updated_at: 2026-09-08T17:21:32Z` —
auto-closed by the normal delivery-closes-ticket flow when Scritch-14's
doc-only branch (`merge_commit 1ff50b3`) landed at 17:21:31Z. No further
operator action needed on this ticket.

**(2) Confirmed still outstanding and genuinely agent-inaccessible.**
`rat/rizzo-14/tkt-humih-nusok-lozus` still exists locally (HEAD `b8a5a1f`,
29 commits behind `main`). Directly tested `rk ticket update
TKT-kifaj-lumop-borab --status closed --reason stale-rework --evidence
TKT-humih-nusok-lozus` — result: `protocol: forbidden: Widget-14 is not
authorized for ticket.update`. There is also no `rk`-mediated
branch-deletion RPC of any kind (checked `rk --help`, `rk repo --help`) —
deleting another agent's branch is a plain destructive git operation, not
something any agent role is authorized to self-serve.

## Action taken

- Filed `TKT-dogav-gasud-vahab` (durable tracking, repo `rat-kingdom`) for
  an operator to run `git branch -D rat/rizzo-14/tkt-humih-nusok-lozus`.
- Posted a `need` pointing at that ticket with the same evidence.
- Recorded artifact `widget-14-conflict-branch-verified` with the full
  verification trail.
- No source change. No source files touched.
