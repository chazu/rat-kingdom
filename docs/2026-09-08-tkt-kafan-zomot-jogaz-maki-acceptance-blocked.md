# Maki disposable authenticated acceptance: repair and repeat

Date: 2026-09-08
Ticket: TKT-kafan-zomot-jogaz (parent TKT-mosik-kulat-jaliz)
Result: **repair and repeat** — not qualified. No authenticated Maki worker
was started against either provider path.

## What this run was asked to prove

Per the [Maki harness research](2026-09-06-maki-agent-harness-research.md)
item 5, this ticket asked for a bounded external acceptance outside CI: in a
sanitized disposable home/repository, start one authenticated ChatGPT-OAuth
Maki worker and one API-key Maki worker, prove a Git write and `rk done`,
verify interrupt/resume, token/cost accounting, and the isolation properties
(no native subagents, no project Lua/custom commands, no global MCP
authority, no external memory writes), then commit reproducible evidence.

## Environment survey (zero-cost, no authenticated turn run)

- `maki --version` reports `maki 0.5.2` at `/usr/local/bin/maki`, matching the
  minimum version the research doc and
  [operator-reference.md](operator-reference.md#maki-configuration-and-precedence)
  already document.
- `maki auth status` on this host shows **no ChatGPT-OAuth session for any
  provider**. `openai` and `openrouter` show only the ambient-env-var form
  (`~` status, "via OPENAI_API_KEY" / "via OPENROUTER_API_KEY"); every OAuth
  provider, including `openai`'s coding-plan login, is unauthenticated (`✗`).
- The ambient `~/.config/maki` directory contains a live `init.lua`. That is
  exactly the ambient authority operator-reference.md warns a managed worker
  must not carry ("do not deploy managed Maki workers into a config root
  carrying credentials or MCP servers they should not exercise") — it
  confirms a genuinely sanitized disposable `$HOME` is a hard requirement
  here, not a formality.
- The control-metadata decision this ticket asked about is already resolved
  and shipped: `caps().steer` is `false` for Maki (operator-reference.md
  lines 284–291), so the "exercise steer only if enabled" branch does not
  apply. That part of the ticket needed no further action.

## Why the authenticated runs did not proceed

Both required provider paths hit a boundary an autonomous headless dispatch
cannot cross by itself:

1. **ChatGPT-OAuth.** `maki auth login [openai]` is an interactive
   human-authorization flow (`maki auth login --help`: "Authenticate with a
   provider (interactive if no provider specified)") — it opens a browser
   consent screen tied to a specific person's ChatGPT account. There is no
   device-code or non-interactive variant. A headless rat has no browser and
   no standing to authorize as the operator's personal ChatGPT identity; this
   is not a missing tool to route around, it is a human action this ticket's
   own scope depends on but cannot delegate to the worker doing the
   acceptance run.
2. **API key.** The only credential present in this environment is the
   operator's live, ambient `OPENAI_API_KEY` (plus an `OPENROUTER_API_KEY`),
   found in the shell environment rather than provisioned for this test.
   Using it would place real, metered charges against a personal production
   credential with no per-run budget cap of its own. Spending real money
   against a personal account is exactly the class of hard-to-reverse,
   externally-visible action this fleet's own operating guidance says an
   agent should not take without an operator's explicit, specific
   authorization for that run — and an autonomous dispatch has no
   synchronous channel to obtain that authorization mid-task.

Because the acceptance requires both provider paths side by side, and the
OAuth path is categorically blocked regardless of the credential question,
the run could not reach a qualifying result. Per this ticket's own
instruction, that is recorded here as **repair and repeat**, not fabricated
as a pass.

## What would unblock a repeat

1. An operator provisions a dedicated, budget-capped API key exclusively for
   Maki acceptance testing — not a reused personal/production key.
2. An operator interactively completes `maki auth login openai` once, from a
   `$HOME` they control that carries no `init.lua`, no project/global MCP
   configuration, and no unrelated credentials, producing a reusable OAuth
   credential store.
3. That disposable home (or just its `maki` auth store) is handed to the next
   acceptance dispatch, rather than asking a headless rat to author the OAuth
   grant itself. The dispatch can then build the sanitized disposable
   repository around it and run the full start / Git-write / `rk done` /
   interrupt-resume / accounting / isolation checklist for both providers.

## Evidence retained

- `maki --version` output: `maki 0.5.2`.
- `maki auth status` output at the time of this survey (system-scope only;
  not committed verbatim since it lists unrelated third-party provider
  catalog entries with no bearing on this ticket): both `openai` and
  `openrouter` show ambient-env-var auth (`~`), every OAuth-capable provider
  including `openai`'s coding-plan login shows `✗` (no session).
- `maki auth login --help` output confirming the login flow is interactive
  with no non-interactive/device-code path.
- No credentials, tokens, or private state from this host were copied into
  this repository.
