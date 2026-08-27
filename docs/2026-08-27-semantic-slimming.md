# Semantic slimming: exact state, one delivery path, CUE policy

Status: implementation candidate, measured against `2963e62` on 2026-08-27.

This slice removes operational guesses without removing repository-owned
automation. Rat Kingdom still uses activated per-repository CUE as the policy
engine for worktree/branch shape, delivery, landing limits, named checks,
triggers, schedules, and deterministic gates. Missing or inactive repository
policy now fails closed with an onboarding instruction.

## Persisted-state inventory

The operator state inspected before the change contained:

- 43 live registry rows, all with minted spawn ids;
- 1,305 archived rows, 844 written before the spawn-id field existed;
- 381 spawn-keyed, 527 timestamp-keyed, and 257 name-keyed transcript files;
- 183 completed and 11 failed workflow snapshots, with no running workflow;
- no live landing-queue entry;
- two repositories with activated CUE policy (`rat-kingdom`, `voxel`) and four
  visible but inactive registrations (`capsule`, `grip-and-sip-club`, `grmpl`,
  `infinite-prison`).

The startup migration refuses any live row without exact identity. Terminal
rows receive a deterministic synthetic spawn id. Before either ledger changes,
Rat Kingdom writes byte-exact rollback copies:

```text
agents.json.pre-spawn-id-v1.bak
agents-archive.json.pre-spawn-id-v1.bak
```

Old name/timestamp transcript entries are then split by the archived generation
windows and copied into exact `<agent>.<spawn>.jsonl` files. Source files remain
untouched for rollback. Target rewrites are atomic and content-deduplicated, so
restarting midway or applying the migration twice converges to the same bytes.
Operational log reads, hook paths, and deletion use only the exact file.

Rollback is therefore: stop the new daemon, restore the two backup ledgers over
their canonical files, and start the previous binary. The previous transcript
files were never moved or deleted; spawn-keyed copies are harmless to the old
reader.

The candidate was also run twice against a temporary copy of the operator's
current 43 live and 1,305 archived rows plus its transcript directory. Both
runs produced the same ledger bytes. The archive rollback copy matched the
pre-migration SHA-256 exactly, exact transcript files increased from 1,165 to
1,949, and the live operator ledgers remained byte-identical throughout.

## Removed operational compatibility

- lifecycle joins no longer synthesize identity from agent name and time;
- workflow waits, reads, completion checks, dismissal, and reconciliation bind
  exact spawn ids or fail closed;
- ticket closure comes only from the canonical delivery record written by the
  landing finalizer, never Git ancestry or a completion marker;
- repository policy comes only from activated `.rk/repo.cue`, never legacy
  registry fields, CLI translation, or a fleet-wide merge default;
- workflow names do not grant landing authority; workflow `land`/`open_pr`
  follows the uniform approval rule and activated target policy;
- the retired steward mega-workflow and unlinked-subworkflow recovery reader
  are gone; the daemon-native landing pipeline is the one shipped landing path.

Historical event and proposal documents may describe old releases, but no old
shape participates in live authorization.

## Preserved non-agentic automation

The following remain versioned, CUE-validated, digest-activated, and executable
without an LLM:

- `.rk/repo.cue`: branch/worktree templates, delivery, landing budgets and
  protected paths;
- `.rk/checks.cue`: named commands, timeouts, expected exits, environment and
  toolchain contracts;
- `.rk/triggers.cue`: deterministic event matching and daemon actions;
- `.rk/schedules.cue`: deterministic workflow cadence;
- landing protected-path, diff-scope, and repository verification gates.

An absent activated policy cannot dispatch. A missing named landing check,
failed check, protected path, oversized diff, or timeout holds the branch and
surfaces attention.

## Negative metrics

Measured in production Rust source from `2963e62` to this candidate:

| Compatibility concept | Before references | After references |
|---|---:|---:|
| `automated_landing_workflows` | 15 | 0 |
| `allowed_target_branches` | 12 | 0 |
| `default_merge_mode` | 8 | 0 |
| `ticket_undelivered_reason` | 7 | 0 |
| `for_agent_since` | 29 | 0 |
| `branch_verified_merged` | 8 | 0 |
| `fail_legacy_unlinked_subworkflows` | 2 | 0 |
| `latest_conflict_marker` | 4 | 0 |

Production Rust changes are +1,001/-1,835 lines, a net removal of 834 lines.
Repository delivery has one activated-policy authority,
workflow completion has one finalizer, and shipped landing has one pipeline.
