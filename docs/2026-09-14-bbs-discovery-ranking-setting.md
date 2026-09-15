# BBS discovery ranking: `bbs-discovery-ranking` setting

Status: implemented on this branch, pending review/acceptance and deployment
(P8/P11 first slice; TKT-fikih-zosom-sofar). Nothing below is live on any
installed daemon until this lands on main and that daemon is rebuilt and
restarted — see `docs/2026-09-13-continuous-validation-promotion.md` sections
1, 7.1, 9, 11 for the accepted plan this slice implements.

## What it does

`crates/rk-daemon/src/bbs.rs::brief` selects "task topic" (score 40) peer
posts by matching any title word (length >= 5, minus a fixed stoplist)
against a post's body. A retained pre-outcome observation (BBS artifact
`01M2ERKFJ8KCQ78TTBK42VVASP`; source
`/Users/chazu/.codex/artifacts/rk-stigmergy-20260913T022353Z/real-batch-01/discovery-observation-01.json`)
showed this baseline surfacing unrelated landing/review notices whenever a
task title happened to share a report-narration word — `source`, `report`,
`after`, `during`, `current`, `observer`, `budget`, `verification` — with an
unrelated post. Of those eight, `observer`, `budget` and `verification` are
load-bearing domain terms in rat-kingdom itself (the checks/verification
pipeline, ledger budgets, the observer component) and were dropped from the
adjustment after review — see Limitations.

Turning `bbs-discovery-ranking` **on** for a repo excludes the remaining
5-word list (`source`, `report`, `after`, `during`, `current`;
`OBSERVED_GENERIC_TITLE_WORDS` in `bbs.rs`) from the "task topic" match set,
for that repo only. It does not touch the `task or dependency` (100) or
`shared area` (80) tiers, expiry, accepted-Need suppression, category limits,
or any other selection rule.

## Default and fallback

**Default is `off`** for every repo. Off reproduces the original selection,
order, cursor and bounds exactly — this is also the permanent fallback if no
later P8/P10/P14 slice (shadow/cohort assignment, comparative fitness,
automatic promotion) ever ships. `off` never expires and is never retired.

## Enabling / disabling / inspecting

```
rk bbs discovery show    --repo <repo>
rk bbs discovery enable  --repo <repo>   # operator-only
rk bbs discovery disable --repo <repo>   # operator-only
```

Wire RPCs: `bbs.discovery.show` (agent- and operator-readable, like
`bbs.brief`), `bbs.discovery.set` (operator-only — absent from
`crate::capabilities::method_policy`, so a non-operator caller is refused
before the handler ever runs). `enable`/`disable` are both `bbs.discovery.set`
with `mode` `"on"`/`"off"`.

The setting is stored in `<RK_HOME>/bbs-discovery.json`
(`crate::bbs_discovery::DiscoveryRegistry`), a small JSON file mirroring
`crate::repos::RepoRegistry`, keyed by repo name. `set` rejects a repo name
that is not in the daemon's repo registry ("malformed or unauthorized
scope"), and rejects any mode other than `off`/`on` with a distinct message
for `shadow`/`cohort` (named in the design doc as later work, not silently
folded into `off`); both `ShowParams`/`SetParams` reject unknown wire fields
(`#[serde(deny_unknown_fields)]`). `DiscoveryRegistry::load` reads the file
directly rather than probing `Path::exists` first — a separate existence
check is racy and, on a permission error, `exists` itself returns `false`,
which would silently relabel a real read failure as "no config was ever
written"; only an actual `NotFound` is treated as absent. The `bbs.discovery.
set` handler holds `server.rs`'s existing repo-registry mutex for the whole
scope-check-then-write, which incidentally serializes concurrent `set` calls
against this registry too (see `server.rs`'s `"bbs.discovery.set"` dispatch
arm) — no additional lock was added for this slice.

**No restart or rollover is required.** The registry is read fresh from disk
on every `bbs.brief` call and every spawn/resume/recovery briefing
(`crate::bbs_discovery::resolve_for_brief`, called from both
`server.rs`'s `bbs.brief` handler and `supervisor.rs`'s `bbs_briefing`) — the
very next briefing for that repo observes a change. This is tested in
`crates/rk-daemon/tests/bbs_discovery_rpc.rs`.

## Evidence and honesty envelope

Every `Briefing` carries three identity fields:

- `ranking_variant`: `"baseline"` or `"observed-generic-word-filter"`.
- `ranking_config_revision`: `0` when no explicit per-repo record has ever
  been written; otherwise the exact revision of the record that produced
  `ranking_variant`, even when that record set the repo back to `off`
  (disable is a real, revision-bearing decision, not a reset to "unset").
- `ranking_config_status` (`rk_core::bbs::ConfigStatus`): `explicit` (a
  per-repo record was read), `default_absent` (the registry read fine and
  genuinely has no record for this repo), or `unreadable_fallback` (the
  registry itself could not be read — baseline was applied as a safe
  fallback, but whether this repo has an explicit setting is UNKNOWN, never
  reported as a confirmed absence). The two baseline-producing statuses are
  deliberately not collapsed: an unreadable registry must never masquerade as
  "operator confirmed this repo is unconfigured".

`crate::bbs::record_exposure` copies all three, plus the daemon's own
`rk_core::version::BUILD_VERSION` (`"build"`), onto the daemon-authored
exposure `Event` for every surface (`spawn`, `resume`, `recovery`, `brief`),
so a later report can join a briefing back to exactly which build/variant/
config revision produced it, without changing the exposure schema's existing
"prepared, not delivered/comprehended" semantics.

## Limitations

- This is a **ranking/filter adjustment only**: a fixed 5-word exclusion list
  (`source`, `report`, `after`, `during`, `current`), not a learned or general
  classifier, and not validated against a larger corpus. The observation this
  is derived from originally flagged 8 words, including `observer`, `budget`
  and `verification`; those three were deliberately REMOVED after review,
  because they are load-bearing domain terms in rat-kingdom itself (the
  checks/verification pipeline, ledger budgets, the observer component) —
  excluding them would drop genuinely relevant findings, not just noise. The
  counterexample was fixed by narrowing the list, not by telling an operator
  not to enable the feature. Presence of a word in two observed task titles
  is evidence it produced noise IN THAT DATA; it does not by itself prove a
  word is safe to exclude everywhere, which is why the three domain terms
  were pulled rather than kept with a caveat.
- No shadow execution, cohort assignment, comparative fitness scoring, or
  automatic promotion/retirement — those are later P8/P10 slices and require
  their own tickets.
- No `--expect-revision` optimistic-concurrency guard on `bbs.discovery.set`
  (unlike the design doc's proposed general `rk feature set --expect-revision
  N`). Concurrent writes ARE serialized (see above — no lost update or file
  corruption), but a caller has no way to assert "apply this only if the
  revision is still N"; a second caller's blind overwrite silently wins.
  `revision` is exposed so a future CAS layer can be added without a storage
  migration.
- No generic multi-feature `rk feature show/set/disable` namespace: this
  slice ships `bbs.discovery.*`/`rk bbs discovery *` specific to this one
  feature, per the "smallest deterministic adjustment" scope. A second
  feature reuses the same mechanics under its own module, not a shared
  dispatcher, until that generalization is actually needed.
- No measured productivity benefit is claimed. The observation this list is
  derived from is pre-outcome, unblinded, and not exhaustive; a benefit claim
  requires the bounded real-ticket trial in the ticket's required journey,
  which is a separate, later operator-run step.

## Owner, dependencies, retirement

- Owner: whichever rat/operator lands this slice; no standing owner role
  exists yet for feature policy (P8/P10 later work).
- Depends on: nothing beyond current main. Does not depend on release
  inventory, a controller, or P9 assessment reporting.
- Interacts with: `crates/rk-daemon/src/bbs.rs::brief` (the only ranking
  logic touched), `crate::repos::RepoRegistry` (read-only, for scope
  validation), the exposure envelope in `crate::bbs::record_exposure`.
- Retirement condition: once a later P8/P10 slice ships general
  shadow/cohort/comparative-fitness feature policy, this narrow
  `bbs-discovery-ranking`-specific registry and RPC surface should be folded
  into (or replaced by) that general mechanism, and this doc's CLI/RPC
  surface retired in favor of the general one. Until then this is the
  supported path and must keep working standalone.
