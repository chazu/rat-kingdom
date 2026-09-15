# Operator recovery: the 7323700 / 3bb1a9e stuck delivery (TKT-jonis-faror-zufuj)

Status: **proposed, not applied.** Root owns activation. This procedure is
reviewable before production application; it mutates nothing on its own.

The code fix on `rat/snuffle-16/tkt-jonis-faror-zufuj` (commit `80b912d`)
stops this from recurring. It does **not** retroactively settle the delivery
that is already stuck, because the stuck delivery's durable queue row is only
re-read by a running daemon — so the recovery below is what closes out the
existing incident.

## The stored receipts (read, not reconstructed)

Every fact below comes from a tuple in the live `rat-kingdom` space; no
timeline was inferred. Re-read them with `rk bbs show <id>` / `rk scan`.

| What | Where |
| --- | --- |
| Durable landing receipt, still `status: "landing"` | `event rat-kingdom landing_queue_entry 01M2JG0DH8QMCPTFWKBQPFP9QM` |
| Finalization failure | `event rat-kingdom landing_finalization_failed 01M2JAJNS69NKQYWXFX9GQC9XB` |
| Repeated merge-pointer conflict (the busy-retry) | `event rat-kingdom delivery_merge_pointer_conflict` — many, e.g. `01M2JAR1RDH6N632JKZ3V4NTBF`, `01M2JG0E6SABZM6GBHTR78H6MM` |

The receipt's own fields:

```
branch         rat/skitter-16/tkt-jodod-gubaz-jasad
head_sha       adc66d4efa511402124b796e18c1bf4b6e5cde98
candidate_base 3bb1a9e54de0b331a0c87d259d9279fdf25d2ab8
candidate_sha  732370066ca64cb5b87969cfe1a5757b70e60586
target         rat/burrow-16/tkt-zabok-huzab-vakot
task           TKT-jodod-gubaz-jasad
source_spawn   01M2J7HPVW0T7QZ25N4SVDWZ7A
seq            559
status         landing
enqueued_at    2026-09-15T10:24:35.040320Z
phase_entered  2026-09-15T10:42:35.271277Z
```

and the failure it hit:

```
agent Skitter-16 already carries a different merge commit
(3bb1a9e54de0b331a0c87d259d9279fdf25d2ab8) than this delivery's candidate
(732370066ca64cb5b87969cfe1a5757b70e60586); refusing to overwrite
```

## Target ancestry: why this is a successor, not a conflict

The receipt is self-proving. Its `candidate_base` **is** the recorded merge
commit, which is exactly the relationship
`lifecycle::classify_successor` classifies as `Advance`. Confirm it in the
repository rather than trusting this document:

```sh
cd /Users/chazu/dev/rust/rat-kingdom
# 1. the candidate descends from what the agent already carries
git merge-base --is-ancestor 3bb1a9e54de0b331a0c87d259d9279fdf25d2ab8 \
                            732370066ca64cb5b87969cfe1a5757b70e60586 && echo successor
# 2. the target already carries the candidate: the merge is DONE and durable
git rev-parse rat/burrow-16/tkt-zabok-huzab-vakot   # == 7323700...
git merge-base --is-ancestor 732370066ca64cb5b87969cfe1a5757b70e60586 \
                            rat/burrow-16/tkt-zabok-huzab-vakot && echo landed
```

Verified 2026-09-15: (1) prints `successor`, (2) prints the candidate sha and
`landed`. `7323700` is **not** in `main`, so nothing downstream has consumed
it yet.

Consequence: the already-landed code is intact and must be preserved. The
only thing missing is the projection — `AgentRecord.merge_commit` for
`Skitter-16` and the `TKT-jodod-gubaz-jasad` delivery record still name the
predecessor `3bb1a9e`.

## Recommended recovery: let the fixed daemon settle it

Preferred, because it performs no manual mutation at all and uses the exact
production path the fix repairs.

1. Land `rat/snuffle-16/tkt-jonis-faror-zufuj` onto `main` through the normal
   pipeline.
2. `mise run deploy` — the running daemon predates the fix, so the fix is not
   live until the binary is replaced and the daemon restarted (same
   redeploy caveat as TKT-146/147/161/167).
3. Do nothing else. The durable receipt above is still `status: "landing"`
   with its candidate already contained in the target, so the fresh daemon's
   `recover_completed_land` re-enters `finalize_landed` for it, and
   `finalize_delivery` — now `AdvanceOnDescendant` — classifies
   `3bb1a9e -> 7323700` as a proven successor, CAS-applies the pointer, and
   emits `delivery_merge_pointer_advanced`.

This is precisely the sequence
`crates/rk-cli/tests/resumed_generation_successor_landing.rs` executes
against a disposable repo: a real daemon is killed while parked between the
target advance and finalization, and the replacement daemon process recovers
the receipt and settles it exactly once.

### Confirming it settled

```sh
rk scan event rat-kingdom delivery_merge_pointer_advanced   # expect ONE, 3bb1a9e -> 7323700
rk --json status Skitter-16 | jq .merge_commit              # expect 7323700...
rk --json ticket show TKT-jodod-gubaz-jasad | jq .payload.delivery
rk scan event rat-kingdom landing_queue_entry               # seq 559 gone
git -C /Users/chazu/dev/rust/rat-kingdom rev-parse rat/burrow-16/tkt-zabok-huzab-vakot
#   MUST still be 7323700 — the target must not advance a second time
```

`delivery_merge_pointer_conflict` will stop accumulating; the historical ones
stay as evidence and must not be deleted.

## If the receipt is gone before the redeploy

Only if `rk scan event rat-kingdom landing_queue_entry` no longer lists
`seq 559` — e.g. an operator drained the queue — is any manual step needed.
In that case re-submit the same evidence through the ordinary operator path
rather than editing state:

```sh
rk land rat/skitter-16/tkt-jodod-gubaz-jasad \
  --repo /Users/chazu/dev/rust/rat-kingdom \
  --target rat/burrow-16/tkt-zabok-huzab-vakot \
  --task TKT-jodod-gubaz-jasad
```

The branch head is unchanged, so the enqueue is idempotent, the target
already contains the candidate, and the same descendant classification
applies. Named gates and review still apply.

## Explicitly out of bounds

Per the ticket, none of the following is part of this procedure, and none is
needed given the ancestry above:

- editing `agents.json` / the registry by hand,
- `rk land --force` (it keeps `SuccessorPolicy::FailClosed` deliberately: the
  ungated escape hatch carries no gate/review evidence, so it must not
  silently advance a pointer),
- clearing the landing queue, resetting caps, or landing from a worker,
- reverting or re-merging `7323700` — the code is already correctly on the
  target and must be preserved,
- deleting any conflict/failure receipt.
