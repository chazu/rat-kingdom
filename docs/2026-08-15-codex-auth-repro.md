# Codex auth failure: reproduction attempt (TKT-01M01NZBWERF4W9QARWG8P632P)

Sub-ticket 1 of TKT-01M01EYN0132N30BWP8BXHXDR6 ("codex rats denied rk calls: auth env
lost inside sandbox"). Scope here is reproduce-and-capture only; instrumenting the
daemon's denial diagnostics permanently is TKT-01M01NZBZ336QYF9J404BPQHCD, and fixing
the harness adapter is TKT-01M01NZC1QTGND4GGZ16RHKRAB — neither is touched by this
ticket or this doc.

## Method

Built `rk` from this worktree (`cargo build -p rk-cli`, debug profile) and ran a fully
disposable daemon: fresh `RK_HOME=/tmp/rk-codex-repro-home`, a scratch git repo
registered via `rk repo add`, `RK_LOG=rk_daemon=debug`. Nothing here touched the live
castle or `~/.rat-kingdom`.

`authorized()` (`crates/rk-daemon/src/server.rs:930-1023`) returns a bare `bool` with
no internal logging, and every `false` result collapses to the same wire message at the
single call site (`server.rs:878-882`): `"{caller} is not authorized for {method}"`,
code `FORBIDDEN`. To tell arms apart for this repro, five `debug!` lines were added
temporarily (peer-origin resolution, plus one per denial arm: pid-not-observed,
supervised-agents ambiguous/mismatched, token mismatch, invalid role) and reverted
before committing — they are not part of this branch's diff. The exact snippet is
reproduced at the bottom of this doc for whoever picks up TKT-01M01NZBZ336QYF9J404BPQHCD.

Spawned disposable `codex`-harness rats (`--harness codex`, default permission mode,
i.e. `--dangerously-bypass-approvals-and-sandbox` per `crates/rk-harness/src/codex.rs`)
whose task was exactly the parent ticket's repro shape: run `env | grep ^RK_`, then
`rk scan fact system`, then `rk endorse <id>`, then stop. Ran it once solo, then three
at once (`Pip`, `Scurry`, `Templeton`) to stress the pid/pgid/cwd ancestry walk in
`supervised_agents_for_peer` (`crates/rk-daemon/src/supervisor.rs:3102-3145`) under
concurrency, since that function's own doc comment flags cross-worktree ambiguity as
the fail-closed case.

## Result: did not reproduce

Every run — solo and 3-way concurrent — completed cleanly. Credential env survived
into the codex sandbox intact:

```
RK_AGENT=Nibbles
RK_AUTH_TOKEN=<redacted: 64-hex derived token, present and well-formed; root secret lived only in the disposable /tmp repro home, now deleted>
RK_BASE=main
RK_BRANCH=rat/nibbles/repro-codex-auth-2
RK_HOME=/tmp/rk-codex-repro-home
RK_REPO=rk-codex-repro-repo
RK_ROLE=rat
RK_TASK=repro-codex-auth-2
RK_WORKTREE=/tmp/rk-codex-repro-home/worktrees/rk-codex-repro-repo/Nibbles
```

`rk scan fact system` and `rk endorse <id>` both exited 0 for every rat in every run.
The daemon's own origin resolution never produced anything but a clean singleton set,
including under 3-way concurrency:

```
peer_pid=Some(7251)  pid_observed=true  supervised_agents={"Scurry"}
peer_pid=Some(7194)  pid_observed=true  supervised_agents={"Scurry"}
peer_pid=Some(7286)  pid_observed=true  supervised_agents={"Templeton"}
peer_pid=Some(7221)  pid_observed=true  supervised_agents={"Templeton"}
peer_pid=Some(7658)  pid_observed=true  supervised_agents={"Pip"}
peer_pid=Some(7602)  pid_observed=true  supervised_agents={"Pip"}
```

None of the three denial arms that can plausibly fire for a normal agent caller ever
tripped:

- **B** (`server.rs:944-946`, pid not observed while claiming operator) — never hit;
  `pid_observed` was `true` on every connection (`peer_cred()` works fine through
  codex's own child processes).
- **C** (`server.rs:947-952`, supervised_agents non-empty but ambiguous or not
  containing the caller) — never hit; the set was always either empty (operator-side
  CLI calls made from this shell) or a correct singleton matching the caller.
- **D** (`server.rs:956-958`, token mismatch) — never hit, and structurally can't be
  what the original Django-4 report saw anyway: `authenticated()` (`server.rs:873`,
  `server.rs:1062-1074`) runs the identical token comparison *before* `authorized()` is
  ever called, and returns a different code/message (`UNAUTHORIZED` / `"invalid daemon
  token"`, not `FORBIDDEN`). The original report's error text was `forbidden: ... is not
  authorized for ...`, which only comes from `authorized()` — so whatever failed there,
  `RK_AUTH_TOKEN` itself was almost certainly matching fine. `Client::ambient_identity`
  (`crates/rk-daemon/src/client.rs:55-65`) also self-heals a *missing* `RK_AUTH_TOKEN`
  (it recomputes the correct token from the layout's on-disk root token via `token_for`)
  — only a `RK_AGENT` that survives paired with a *stale* `RK_AUTH_TOKEN` from a
  different identity would produce a real token mismatch, which is a narrower failure
  mode than "the sandbox strips the token."

Also confirmed by direct code read (not just this repro): `crates/rk-harness/src/codex.rs:72`
(`cmd.envs(&spec.env)`) is byte-for-byte identical to the claude and fake adapters —
there is no env filtering/rewriting anywhere in `rk-harness` or `rk-daemon` before the
top-level `codex exec` launch. `Supervisor::agent_env()` (`crates/rk-daemon/src/supervisor.rs:3493-3532`)
builds the same env map regardless of harness kind.

## What this narrows down

The failure is real (it happened once, to Django-4, per the parent ticket) but is not a
deterministic consequence of "codex harness spawn" as such — this repro exercised the
same code path (same daemon, same harness adapter, same default bypass permission mode,
including 3-way concurrent spawns to stress the ambiguity arm) and got a clean result
every time. Plausible remaining explanations, roughly ordered by how much this repro
weakens them:

1. **Something specific to the live castle's process topology** that a fresh disposable
   daemon doesn't have — e.g. a much larger registry (many still-`Dismissed`-pending or
   long-running agents for `supervised_agents_for_peer` to walk against), a different
   process launcher/session wrapper (tmux/herdr) sitting between the daemon and the
   codex child that this throwaway setup didn't reproduce, or simply far higher
   concurrency than 3 agents at once.
2. **A startup race**: the very first tool call of a freshly spawned agent, before its
   `AgentRecord` write is visible to `supervised_agents_for_peer`'s registry read. This
   repro's first call always landed after the record was already visible; an
   adversarial timing test (fire the harness's first `rk` call as early as possible,
   ideally racing the `agent.spawn` response) is untried.
3. **A specific codex CLI version/session-state interaction.** This repro used
   `codex-cli 0.147.0` with a stored, working auth session. Django-4 was recorded
   running `model gpt-5.6-luna`; if the fleet's live castle runs a different codex CLI
   build, this repro doesn't cover it.
4. **Genuinely transient** — a one-off OS-level scheduling/credential-passing hiccup
   that isn't reliably triggered by any input this repro controls.

## Recommendation

Given the failure didn't reproduce on demand, the highest-leverage next step is not
more blind repro attempts but **catching the live occurrence with per-arm diagnostics**
— which is exactly TKT-01M01NZBZ336QYF9J404BPQHCD's scope. The instrumentation used for
this doc (reproduced below) is a ready-made, tested pattern for that ticket to adapt
into a permanent (structured, not `eprintln`-style) denial reason surfaced on the wire
response or in a structured log field, rather than the current single generic string.

```rust
// server.rs, in the accept loop, right after `origin` is built:
debug!(?peer_pid, pid_observed = origin.pid_observed,
       supervised_agents = ?origin.supervised_agents, "peer origin resolved");

// server.rs, inside authorized(), one line added right before each `return false`:
// arm B (pid not observed, operator):
debug!(caller = %req.caller, method = %req.method, "denial arm B: pid not observed");
// arm C (supervised_agents ambiguous/mismatched):
debug!(caller = %req.caller, method = %req.method, supervised_agents = ?origin.supervised_agents,
       "denial arm C: supervised_agents ambiguous/mismatched");
// arm D (token mismatch):
debug!(caller = %req.caller, method = %req.method, "denial arm D: token mismatch");
// arm E (invalid role):
debug!(caller = %req.caller, method = %req.method, role = %record.role, "denial arm E: invalid role");
```

Run with `RK_LOG=rk_daemon=debug` (not `RUST_LOG` — the daemon's filter env var is
`RK_LOG`, see `crates/rk-cli/src/main.rs:653-656`).

## Coverage limits

Arm E (invalid role) is **unverified** by this reproduction. Exercising it needs a
supervised agent record carrying a role that `validate_role` rejects, and the
disposable castle offers no supported path to mint one: `rk spawn --role` validates
its input up front, and writing a corrupted record directly into the registry would
test the corruption tooling, not the denial arm. The arm E debug line above is
therefore asserted by the landed unit tests in `crates/rk-daemon/src/server.rs`
(diagnostics slice, TKT-01M01NZBZ336QYF9J404BPQHCD) rather than by a live denial
here. If a live arm E exercise becomes necessary, the supported route is a
test-fixture daemon with an injected invalid-role record, not a production castle.
