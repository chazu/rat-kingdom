# E0 drain probe log

*Started 2026-08-18. 2-day supervised mini-drain per the Phase 2 epic's E0.
Configuration: `drain.enabled = true`, `max_wip = 2`, repo rat-kingdom,
sonnet tier default, all phase-1 recovery machinery live (respawn, done-kill,
burn detection, stale-instance timeout, ticket reopen, announce sinks).*

## Pre-probe grooming (operator, 2026-08-18)

- Closed 11 tickets: 4 duplicates of the already-fixed client_version seam,
  2 of the fixed freeze.rs clippy error, 5 duplicate agent_archive flake
  reports (canonical: TKT-…F214KDE7). All were self-improve-generated
  duplicates of resolved work — drain would have dispatched rats onto
  already-fixed bugs. **Grooming debt is itself a probe finding: the
  backlog accumulates duplicates faster than anything dedupes them.**
- Blocked 16 tickets: 3 operator-only, 2 decision tickets (+2 implementations
  gated on those decisions), 4 epic-wip (gate concurrency = Phase-2 E4
  territory), 4 building on the dead steward CUE path slated for removal,
  1 landing-policy (E2 territory).
- Filed 3 fresh probe tickets: verify/CI profile alignment (high),
  terminal-cost rollup fix (high), `rk ticket reopen` (normal).

## Success criteria (pre-registered)

- Tickets landed autonomously per day: **≥ 3** (yesterday's manual rate was
  ~15 with an operator driving; 2-WIP autonomous at ≥3 is the floor of
  useful).
- Interventions/day: recorded and **classified** below — the classification
  is the deliverable that orders the Phase 2 epic.
- Silent-stall check: at probe end, audit from the durable ledger that every
  ticket that left `open` either reached a terminal/landed state or has a
  live agent or an inbox row. Any ticket that stalled with none of those =
  criterion failure.

## Observations

- **O1** (T+0): drain's first two claims were normal-priority tickets
  (…8VMZANPD, …GA7JZ5JZ) while `high` tickets (…F214KDE7, both probe
  tickets) sat ready — claim order appears not to be priority-first,
  contradicting the drain config's own doc comment. Verify in drain.rs;
  candidate epic finding.
- **O2** (T+0): both drain-spawned rats came up on sonnet via the tier
  catch-all — tier routing confirmed working under drain dispatch (first
  time exercised).

## Scoreboard (T+8h)

- **First substantive autonomous landing**: Sooty-8's dead-steward-CUE
  deletion, full loop, zero operator touch (62fa8e0 / f744219).
- Landing outcomes so far: 3 landed (1 substantive, 2 empty/self-groom),
  1 rework-filed (steward auto-filed — the phase-1 hand-off working),
  1 gate-held (O10 rollover collateral), 1 escalated (dead reviewer,
  superseded by re-dispatch).
- Non-empty landings vs the ≥3/day bar: 1, with several branches still
  in review — pace determined by the serial gate, as the epic predicted.

- **O14** (T+16h): **C3's done-binding has no back-half under async
  landing — tickets can never reach `done` automatically.** A rat's
  `rk done` fires while its branch is still queued (unmerged), the
  delivery gate refuses the status flip, the ticket stays `in_progress`
  forever — and when the landing later succeeds, nothing retries. Every
  completed ticket then re-enters the reopen/re-dispatch cycle at the
  window boundary. 14 tickets in this state simultaneously observed.
  Epic fix (E1/E2): the landing pipeline marks the task's ticket done on
  successful land — delivery-bound done needs a delivery-driven writer.
- **O15** (T+16h): a rat filed a SECURITY ticket: the operator's Claude
  SessionEnd hook (aka/archive push), inherited into every rat harness,
  egresses rat transcripts and a pass-derived token to the operator's
  archive server on every rat session end. First-party evidence for the
  blocked rat-session-config curation program (KAF8KSBE tree) — that
  decision just became security-relevant rather than hygienic. Gruyere-8
  working the inventory/fix ticket now.

- **O16** (T+17h): the empty-branch-reads-merged flaw's third surfacing:
  Acorn-8 failed with an empty branch, and B3's merged-branch guard read
  empty (tip==fork==ancestor) as "already merged" and suppressed the
  respawn. Same predicate root as O7 (empty landings) and the done-gate
  refusals — one E1 fix (commit-count-aware delivery records) retires all
  three. Ticket recycles via the reopen sweep; no operator action.

## Day-1 summary (T+~15h)

**Volume:** 44 agents (39 rats, 5 reviewers), ~$144 recorded spend (true
spend somewhat higher — the cost-books bug, whose fix drain itself
delivered today, under-reports terminal costs). 32 commits reached main.
~12 substantive tickets delivered incl. all 3 probe tickets, the
startup-race flake retirement, gate-worktree retention, schedulable
shutdown, SpawnId migration slices, and 4 e2e test suites. Open backlog:
19 (mostly blocked/decision items).

**Pre-registered criteria:** ≥3 non-empty landings/day — **met** (~10,
though roughly half needed operator hand-landing due to two systemic
false-red windows). Silent-stall audit — **one true silent stall class
found** (disk-floor spawn refusal, O12): drain stopped with no signal.
Interventions — **~15, classified**; dominant classes: landing recovery
(false-reds: rollover-killed gates, lint-window, disk-floor — ~7),
duplicate-dispatch control (O8 — ~5), grooming/reconciliation (~3).

**The headline:** worker-loop + recovery machinery are production-solid
(zero unrecovered crashes; B3 respawn validated twice; runaway/burn never
fired falsely). The landing pipeline is the fragile organ — every
intervention class traces to it or to state ambiguity feeding it. This
inverts nothing in the epic but sharpens its priority order:
**E2 (merge queue, no-bypass, full-environment profile) > O6/O8 state
fixes > E1 delivery bars > E4 disk-aware arbitration > everything else.**

**Fleet self-organization observed:** stale-ticket self-grooming (~$1
each), a rat detecting duplicate program trees via claims/artifacts
before duplicating (needed operator judgment to resolve — rk-king
evidence), first organic ballot use (quorum unreachable at 2-WIP).

## Interventions

*(operator/steward actions that autonomous machinery should eventually own —
classified as: dispatch / recovery / review / landing / grooming / other)*

| When | Class | What | Should-be-owner |
|---|---|---|---|
| pre-probe | grooming | 27-ticket backlog groom (dupes of fixed bugs, decision tickets, epic-wip) | future: dedup-on-file + decision-ticket type |
| T+0.5h | landing/CI | pinned rust 1.95 via rust-toolchain.toml — CI's floating stable drifted to 1.97-only clippy lints, keeping CI red and flooding sdlc_ci_diagnostic needs (one per failed run backfilled) | canonical profile incl. toolchain pin (epic E2); diagnostic-need dedup by subject, not per-run |
| T+0.5h | grooming | consumed 4 stale gate-FAILED needs + acked 10 recovery announcements from pre-probe era | reconciler-class stale-escalation sweep (already in re-notify design) |
| T+6.5h | recovery | O8 went systemic (3rd re-dispatch: Dart-8 onto Sooty's landed-pending ticket) — interrupted the freshest duplicate, raised ticket_reopen_sweep.stale_after_secs 900→7200 (above landing latency), rollover. Duplicate spend incurred: ~$3-4 across Swipe/Filch/Dart | epic: reopen sweep must check landing-queue membership, not just agent liveness; reopen window must exceed observed landing p95 |
| T+8.5h | landing | closed the O8/O10 arc for the inventory ticket: original (Ash) paused → re-dispatched → duplicate (Swipe) completed EMPTY → original's gate rollover-killed (false red) → duplicate's gate hit the F214KDE7 flake → operator hand-landed the original (docs-only) and cleared both escalations. One ticket, five failure modes, ~$4 total | every link in this chain already has an epic owner (O6 state, O8 reopen, O10 rollover-gates, flake ticket, E2 queue) |
| T+9h | recovery | **O12: the probe day filled the disk** — 231 GB of worktree target/ dirs left 8.5 GB free, tripping the daemon's 10 GB spawn floor: gate verifies failed with disk-floor panics (read as test failures, holding Filch's branch), fresh spawn-refills silently stopped, and the earlier "disk exhaustion" ticket Tunnel-8 closed did not prevent it. Reclaimed 240 GB by deleting terminal rats' target/ dirs; rollover tests green after; hand-landed Filch (CUE-only). | worktree sweep must reap build artifacts of terminal agents (not just merged branches) — the F214KDE7 flake and "machine-load" flakes are plausibly this same disk/load pathology; epic E4's machine-signal must include disk |
| T+11h | landing | **O13/self-inflicted: operator hand-merge (Burrow) validated with cargo check but not clippy left a -D-warnings lint on main — every subsequent gate run failed on it** (two branches falsely held; one mis-attributed fix ticket filed then closed). The phase-2 review's "nobody reviews the operator" finding, demonstrated by the operator within 24h of reading it. Fixed lint, landed the falsely-held branches, full verify running | E2: operators go through the gate, no bypass — now with a first-party incident as evidence |
| T+3h | landing/CI | CI-red onion layer 3: ubuntu runner lacks `cue`; product_to_code e2e can never have passed there (masked by fmt, then clippy failures). Fixed via self-skip-when-tooling-absent (02a4b49) | canonical profile must pin the FULL environment (toolchain + external tools), not just commands — epic E2 |

- **O3** (T+0.5h): first drain cycle complete — Peppercorn-8 ($1.05) and
  Sable-8 ($1.39) completed; drain refilled both slots within one interval
  (Ash-8, Cinder-8). Claim→work→complete→refill loop confirmed autonomous.
- **O4** (T+0.5h): the CI-reaction filed one `sdlc_ci_diagnostic` need PER
  failed run during the historical backfill (9 needs for one red condition).
  Diagnostic needs should key on the CI *subject/state*, not per-run —
  epic-adjacent finding for the ingest reaction.
- **O5** (T+1h): a drain rat correctly used the parked ballots system for a
  convention-refresh ticket (sug-b291ync09z) — first organic use. But
  quorum=3 is structurally unreachable in a max_wip=2 fleet without
  operator participation: endorsed as operator (2/3), the third endorser
  can only come from a future rat noticing the open ballot. Confirms the
  strategic review's "social mechanism for a fleet size that doesn't
  exist" verdict — with the twist that the mechanism *works*, it's the
  quorum constant that doesn't scale down.

| T+1h | review/judgment | read + operator-endorsed convention ballot (quorum unreachable at 2-WIP) | quorum should scale with fleet size, or operator-endorsement should be the documented path at small WIP |

- **O6** (T+1.5h): **`completed` is ambiguous, and drain trusts it.** Ash-8
  went state=completed with NO `rk done` and no harness_result — its turn
  ended while it legitimately waits on a background verify, intending to
  resume and run the completion protocol. Consequences observed: (a) drain
  freed the slot and spawned a replacement — when Ash-8 resumes, actual
  concurrency exceeds max_wip untracked; (b) monitoring (mine included)
  cannot distinguish paused-mid-task from post-done-linger from runaway —
  I initially misclassified this as a B5 done-kill failure; (c) B5
  *correctly* held fire (it keys on the done declaration, not state).
  This is the TKT-160/173/175 "green but not done" lineage measured live:
  the strict declared-done rule (TKT-175, deliberately deferred) or a
  distinct `paused/awaiting-resume` state is the structural fix. Epic
  candidate, high priority — it undermines WIP accounting, the probe's
  own completion counts, and any future done-binding.
- **O7** (T+2h): **empty landings are legitimate, common, and invisible in
  the metrics.** Peppercorn's and Cinder's branches landed with zero
  commits — correctly: Peppercorn's ticket was a stale dispatch (work
  already on main from a prior rat; it verified and honestly declined to
  duplicate — drain self-groomed a stale ticket for ~$1), and Cinder's
  deliverable was a ballot (tuplespace, not git). Implications: (a) my
  pre-probe groom missed at least one more already-done ticket — backlog
  staleness detection is a real, recurring need, though drain's ~$1
  discovery cost is a tolerable interim mechanism; (b) the landed/day
  success criterion cannot distinguish empty from substantive landings —
  probe accounting switches to **non-empty landings** for the ≥3 bar,
  with empty landings tracked separately as self-grooming events; (c) the
  already-ancestor short-circuit lands empties without review — fine for
  stale dispatches, but it means an accidentally-empty delivery (the
  historical "EMPTY REVIEW BRANCH do NOT approve" class) also sails
  through unreviewed. Epic note for E1/E2: delivery records should carry
  commit counts.
- **O8** (T+6h): **O6's predicted failure arrived: paused rats lose their
  tickets and drain re-dispatches them.** Ash-8 (paused mid-task,
  state=completed, never declared done) held its ticket `in_progress`;
  after the 15-min window the reopen path reclaimed it (a completed-state
  owner is not "live", so B9's live-owner protection does not cover the
  paused case) and drain spawned Swipe-8 onto the same ticket. Sable's
  ticket was similarly re-dispatched (Filch-8) after its reviewer ended
  without a verdict. Consequences in flight: duplicate spend, and a
  landing race when the original work and the duplicate both arrive.
  **Deliberate non-intervention**: letting both run to observe
  convergence — this interaction (state ambiguity × reopen sweep ×
  drain) is the highest-value data the probe has produced. Fix shape for
  the epic: a completed-with-live-process or paused state must count as
  ownership; reopen must check landing-queue presence, not just agent
  liveness.
- **O10** (T+7h): **rollover kills in-flight landing gates and the corpse
  reads as a gate failure.** The O8-intervention rollover signal-killed the
  gate check running for Ash-8's branch ("sh exited with non-zero status:
  no exit status"); the pipeline recorded gate-FAILED, held the branch,
  and escalated — a false red with a dedup-blocked retry, caused by the
  recovery tooling itself. B11 gap: rollover must drain (or the pipeline
  must re-enqueue on startup) in-flight gate runs, mirroring the
  crash-safety the queue already has for its cursor. Resolution here:
  letting the queued duplicate (Swipe) land the same ticket, then clearing
  the stale escalation — the O8 duplicate turns out to be the recovery
  path, accidentally.
- **O11** (T+6.5h): operator `rk interrupt` leaves state=failed, which is
  respawn-sweep eligible — the machinery resurrects deliberately-stopped
  rats unless they are also dismissed. Deliberate stops need a distinct
  non-respawnable terminal state.
- **O9** (T+6h): Rummage-8 failed mid-turn at $7.39 (SessionEnd during
  cleanup, 2 commits on branch) — B3 auto-respawn should reclaim it;
  watching as a live respawn-path test. **RESOLVED: auto-respawn
  reclaimed it and the respawned session completed the ticket ($1.55
  post-respawn) — B3 validated end-to-end under drain, zero operator
  touch.** Separately Sable's reviewer
  (Scrounge-8) ended verdict-less at $0.00 — reviewer mortality under
  the still-unrestarted... no: current daemon HAS reviewMaxWait; a
  verdict-less reviewer *exit* (not timeout) is a different reviewer
  failure mode, escalated correctly by the pipeline.
