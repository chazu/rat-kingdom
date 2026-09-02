# King control loop and idle context lifecycle

Status: implemented; explicit King registration is the opt-in boundary.

## Intent

The King is the operator's LLM delegate. Its bounded interventions count as
unattended operation; escalation to the human operator is reserved for an
explicit human authority gate or exhausted bounded recovery. The King is not a
normal rat and is not admitted through worker WIP lanes.

The transport is deliberately plain text injection into one dedicated Herdr
session. Reliability comes from keeping that transport stupid: RK persists an
opaque wake before injecting it, and the King pulls authoritative state from
RK after wake-up. No decision or repository-authored text is accepted from the
terminal prompt itself.

## Protocol

The ordinary operator lifecycle is `rk king spawn`, `rk king at` (or
`attach`), `rk king restart`, and `rk king dismiss`. Spawn creates a dedicated
Herdr workspace from `[king]` harness configuration, primes it as the operator
delegate, and performs the registration step below. Restart uses the same
checkpoint/restore boundary as hard hibernation. Dismiss closes the exact
registered generation and makes interrupted work replayable to a later spawn.

1. `rk king register <herdr-target> --holder <stable-id>` (normally performed
   by `rk king spawn`) resolves a name, pane,
   terminal, or agent-session id to a terminal, pane, and required Herdr pane
   revision, then persists that exact generation.
2. The daemon scans current reconciliation attention, operator inbox, ready
   tickets, and live agents. The generated timestamp is excluded from the
   state digest.
3. A changed actionable digest creates one durable `KWK-...` record in
   `pending`. One active record coalesces later changes.
4. Herdr receives only the opaque wake id and a fixed built-in RK pull
   instruction—never repository-authored text. Successful submission moves it
   to `injected`; an unclaimed record is retried after the configured TTL.
5. `rk king pull <wake> --holder <id>` fences the holder, moves the record to
   `claimed`, refreshes the authoritative snapshot, and acquires/resumes the
   orchestrator lease for each repository with attention.
6. The King acts through ordinary RK commands. Existing authority policy,
   allowlists, rate caps, approval gates, and lease generations remain the
   authority boundary.
7. `rk king resolve` or `rk king defer` settles the envelope. An unchanged
   settled digest is suppressed; a changed state creates a new wake.

Wake delivery is at least once. Claiming is idempotent for the registered
holder, and a terminal restarted in the same pane does not inherit the old
generation because the required Herdr pane revision is the registration fence.

## Context lifecycle

Durable states are:

`clean -> dirty -> compact_requested -> compacting -> compacted ->
hibernate_ready -> hibernating -> hibernated`

The first registered session is treated as compaction-eligible because it may
already contain a large pre-registration context and Herdr does not expose a
token counter. Later eligibility is based on claimed wake batches.

Compaction is allowed only when:

- the exact registered Herdr generation reports `idle` or a completed `done`
  turn;
- its pane is not focused (the conservative human-takeover signal);
- there is no pending, injected, or claimed wake; and
- idle time and work-batch thresholds are satisfied.

RK submits `/compact` with Herdr's atomic `agent prompt` operation. Codex
documents `/compact` as summarizing the visible conversation and freeing
context space: <https://learn.chatgpt.com/docs/developer-commands?surface=cli>.
New attention remains durable while compaction runs. A compaction that does not
settle before `compact_timeout_secs` transitions to hard hibernation; RK never
blindly sends Enter into an unknown confirmation state.

Hard hibernation writes a bounded `KCP-...` checkpoint containing the current
RK snapshot, active wake id, registration generation, and at most 4096
characters of optional King notes. It then exits the exact old agent generation
and starts a fresh configured harness in the same pane. The new process is not
given a resume id for the old model thread. It receives only the checkpoint id
and pulls both that checkpoint and current RK state through `rk king restore`.

This is the cost guarantee: after hibernation, a later wake cannot cold-load the
old large context. Prompt caching still helps during active bursts, but cache
retention is an optimization rather than a correctness dependency.

## Failure behavior

- No registered King: no action or terminal side effect.
- Registered generation missing or replaced: fail closed and require explicit
  registration of the replacement.
- Herdr injection failure: wake stays pending and is retried.
- King disconnect after injection: wake remains injected and is retried.
- King disconnect after claim: claim and snapshot survive restart; the same
  holder can pull again.
- Compaction failure/timeout: checkpoint-backed hibernation.
- Replacement failure: lifecycle remains `hibernate_ready` for diagnosis and a
  later retry; the checkpoint remains durable.

## Deliberate limits

- Herdr semantic state and generation ids are the observation boundary; RK
  does not scrape terminal contents.
- `focused = false` cannot prove a human is absent, but `focused = true` always
  blocks compaction.
- The initial wake snapshot is bounded and advisory. Every action must re-read
  the relevant RK resource and pass its own current policy/lease checks.
- The King is privileged local operator infrastructure. Registering its pane
  and enabling `[king]` are explicit human configuration actions.
