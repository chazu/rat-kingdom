# TKT-togin-zinus-nizip: still blocked on D1 landing (recheck 2026-09-08T16:19Z)

Task: wire D2's `derive_qualification` liveness check (currently a proxy over
sample-to-sample `metrics.unclassified_holds > 0` onset transitions) to D1's
typed stall/hold evidence (bounded-wait ownership/identity/deadline,
start/resolution/recurrence), and extend the qualification test suite with a
D1-backed stall-episode fixture.

## Finding

D1 (TKT-humih-nusok-lozus, "external acceptance auditor / independent
progress evaluator") has **still not landed on main**. Re-verified fresh on
this dispatch (Crumb-14), independent of the two prior holds below:

- Ticket status: `in_progress`, assignee now **Tunnel-14** (was Rizzo-14 at
  the last hold) — the ticket has been reassigned since, meaning D1 itself
  has been redispatched at least once.
- Real progress evaluator work exists on `rat/tunnel-14/tkt-humih-nusok-lozus`
  (commits `bb264a7` "feat(observe): independent progress evaluator for live
  generations (D1)", `2b55d6a` "style: rustfmt the D1 progress-evaluator
  changes") — more substantial than the earlier `d3485ec`/`b8a5a1f` attempt on
  Rizzo-14's branch. `git merge-base --is-ancestor bb264a7 main` still fails:
  **not an ancestor of main**.
- `main` (HEAD `5414a71`) still contains no typed stall/hold evidence type.
  `crates/rk-cli/src/observation_cmds.rs` still uses the interim
  `unclassified_holds` proxy exactly as before (`derive_qualification`,
  `LivenessRequirement.max_unclassified_holds`, etc.) — nothing has changed
  in D2's landed shape since the last hold.
- No current claim on `crates/rk-cli/src/observation_cmds.rs` (checked
  `rk scan claim rat-kingdom`), so the earlier claim collision with Ash-14 no
  longer applies — but the missing-dependency issue is unchanged and is the
  actual blocker.

This is the **third** recorded hold on this exact ticket:
1. Gouda-14 (commit `cbfbed2`) reached the same conclusion but that dispatch's
   branch never landed — `landing-protected-paths` timed out acquiring the
   verification admission queue (WIP limit 1), a landing-pipeline problem, not
   a content one (per Burrow-14's `nofop-decomposition-reverified-2`,
   artifact `01M20RC4D1X9WFSC5ZF66WFHTJ`).
2. Cinder-14 (commit `a8f5bce`, artifact `held-pending-d1`
   `01M20MVGGB67VPZDPNKK6F8RE6`) reached the same conclusion; that branch also
   never merged into main (also hit a `gate-failure` on
   `landing-protected-paths`, artifact `01M20NZ2NV0JAA675QVYTJCVZR`, WIP limit
   1 again).

## Disposition

No source change made. Wiring `derive_qualification` to D1's typed stall/hold
evidence still requires that evidence type to exist on main first; coding
against Tunnel-14's unlanded, unreviewed branch would risk rework the moment
D1's real review lands. Recommend redispatch once TKT-humih-nusok-lozus
merges to main. If this ticket is redispatched again before D1 lands, the
next rat should check whether the repeated landing-pipeline failures on this
exact ticket's docs-only commits (WIP limit 1 on the verification admission
queue, twice now) are themselves worth a ticket — they are not this ticket's
job to fix, but two consecutive drops of the same trivial commit is a
pattern.
