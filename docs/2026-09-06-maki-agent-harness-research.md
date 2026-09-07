# Maki agent harness research

Date: 2026-09-06
Scope: research only; no harness implementation or authenticated model turn was run.

## Conclusion

`maki` should mean the [Maki terminal coding agent](https://maki.sh/docs/), selected as the literal Rat Kingdom harness kind `maki`. The repository has no existing `maki` symbol or alias. The host currently has Maki 0.5.2 at `/usr/local/bin/maki`; its upstream v0.5.2 source describes the SDK transport as Claude Code-compatible.

The smallest complete first implementation is a managed, headless Maki adapter for ordinary mutable roles, using Maki's persistent SDK mode:

```text
maki --print --input-format stream-json \
  --no-plugins --no-commands \
  --permission-mode bypassPermissions \
  [--append-system-prompt <prime>] [--model <provider/model>] [--session <id>]
```

The initial task and later steers are JSONL `user` records on stdin; `system/init`, `assistant`, `result`, and retry records on stdout map to the existing normalized harness events. This is preferable to one-shot `maki --print --output-format stream-json`: it preserves stdin steering, system-prompt separation, and session resume without inventing a new runner.

Do **not** initially advertise restricted-role support. Maki's `plan` mode writes `plan.md`, and its project/global configuration can add tools through Lua and MCP. Until a tested allow-list profile exists, `onboarder`, `diagnostician`, and `groomer` should fail closed in `role_harness_profile`. Likewise, attached/TUI support should be a second slice: it needs deployment proof that Herdr's Maki detector is present on every castle and an explicit ambient-authority policy.

## Current Rat Kingdom seams

| Concern | Current evidence | Required Maki change |
|---|---|---|
| Adapter contract | [`rk-harness`](../crates/rk-harness/src/lib.rs) normalizes start, text, tool, usage, retry, completion, exit, control-delivery, and transport-failure events. `make_harness` is the runtime kind registry. | Add `maki.rs`, export it, and register `maki`. Reuse the shared runner and pre-work transport classifier. |
| Protocol precedent | [`claude.rs`](../crates/rk-harness/src/claude.rs) already drives a bidirectional Claude-compatible JSONL process and parses the same principal record types. [`jcode.rs`](../crates/rk-harness/src/jcode.rs) shows the more recent complete third-party-adapter pattern. | Share or deliberately duplicate the small Claude-shaped parser/message codec; keep Maki-specific argv, trust, cost, and tests isolated. Use `RK_MAKI_BIN` for deterministic fixtures. |
| Capability/lifecycle integration | `HarnessCaps` drives steering, interrupt/resume, and accounting. Supervisor launch, respawn, alternate recovery, and event accounting are adapter-generic once `make_harness` succeeds. | Proposed caps: `steer=true`, `interrupt=true`, `resume=true`, `native_budget=false`. Cost capability needs the rule below. Add transport-failure coverage for provider/auth failures before work. |
| Permissions and roles | [`supervisor.rs`](../crates/rk-daemon/src/supervisor.rs) chooses defaults and validates full-access modes; [`capabilities.rs`](../crates/rk-daemon/src/capabilities.rs) fails unknown harnesses for restricted roles. | Default ordinary Maki workers to `danger-full-access`, translate it to Maki `bypassPermissions`, reject narrower ordinary modes, and explicitly reject restricted roles in v1. |
| CLI/config validation | `rk spawn --harness` is a string; validation ultimately reaches role policy and `make_harness`. [`config.rs`](../crates/rk-core/src/config.rs) profiles are also strings. Workflow CUE has the only declarative enum. | Update error/help text and comments; add `maki` to [`schema.cue`](../crates/rk-workflow/src/schema.cue). Prove direct spawn, named/default profile, inline workflow override, respawn, and status preserve `maki` plus its model/mode. |
| Attached dispatch | [`rk-mux`](../crates/rk-mux/src/lib.rs) has an explicit interactive argv match for Claude, Codex, and Jcode. | Defer, or add `maki --no-plugins --no-commands --yolo [--model ...]` plus a first-prompt strategy and Herdr deployment gate. Headless support is complete without claiming `--attach`. |
| Readiness/onboarding | [`onboarding.rs`](../crates/rk-daemon/src/onboarding.rs) generically checks executable presence; Jcode adds harness-specific assessment behavior. | Require a supported Maki version and document `maki auth status`; do not perform a paid request as routine readiness. Restricted onboarding needs a later, separately proved tool boundary. |
| Documentation | [README](../README.md) and [operator reference](operator-reference.md) enumerate supported harnesses and explain provider-specific authority/configuration. | Document install/version, `maki auth login openai` for ChatGPT OAuth, provider-qualified model names, permission translation, disabled ambient features, role exclusions, resume/steer behavior, and accounting limits. |

The original Jcode integration commit (`e6e1d7f`) touched this same spine: adapter/registry, supervisor, onboarding, mux, workflow schema/tests, CLI/config copy, and operator docs. That is useful scope evidence, but Maki should not inherit Jcode's one-shot protocol or read-only claims.

## Compatibility and security findings

1. **The stream is compatible; trusted control metadata is not.** Maki SDK input deserializes only `message.content` from a `user` record. Extra top-level `metadata.rk_control` and `rk_control` fields emitted by RK's Claude adapter are ignored. RK can still authenticate and audit the daemon-to-adapter envelope, but Maki's model sees only its text. Before claiming the same trusted-control semantics as Claude, either Maki must preserve the side-band metadata upstream or RK must explicitly accept/document text-only delivery. The first implementation should test this and set `steer=false` if that trust decision is unresolved.

2. **Full access is necessary but not sufficient.** Headless rats cannot stop for prompts and must reach Git plus the daemon socket, so Maki needs `bypassPermissions`/`--yolo`. However, explicit deny rules still override that mode. A disposable acceptance run must prove both a Git write and `rk done`; executable discovery alone is not readiness.

3. **Ambient configuration expands authority.** `--no-plugins` suppresses user/project `init.lua`, and `--no-commands` suppresses custom commands, but Maki still loads permissions, environment files, built-in plugins, and global/project MCP configuration. Project configuration is repository-controlled; global MCP servers may carry the operator's external-account authority. Managed launch must at least disable the built-in `Task` (unregistered subagents) and `Memory` (writes outside the worktree) tools. Before production, decide between a daemon-owned sanitized Maki config root and a reviewed explicit deny-list for MCP/tools. Do not claim isolation from only `--no-plugins`.

4. **Read-only is not yet an enforcement mode.** Maki `plan` creates `plan.md`; broad deny lists must also account for built-in plugins and MCP tools, while removing Bash prevents `rk done`. A later restricted profile could use a strict read-only tool allow-list plus harness-terminal completion, as Jcode does, but it needs adversarial tests before enabling any restricted role. Groomer also needs an evidence-bearing ticket mutation, so it should remain rejected unless a narrowly scoped completion channel is designed.

5. **ChatGPT OAuth cost is not a dollar ledger.** Maki supports OpenAI via API key or `maki auth login openai`; OAuth uses the ChatGPT Coding Plan backend. For unpriced/OAuth models, its SDK result serializes unknown cost as `total_cost_usd: 0`. Passing that zero as authoritative would overwrite RK's incremental estimate and could defeat USD caps. The adapter should map zero/unpriced results to `None` (or initially set `reports_cost_usd=false`) while always preserving token usage. Token caps are the reliable subscription/OAuth guardrail.

6. **Version drift is material.** Maki calls itself new, and 0.5.2's SDK compatibility is an implementation contract rather than an RK-owned standard. Pin a minimum tested version, keep golden fake-binary tests, and add an opt-in real-CLI smoke test. Models should be documented as `provider/model`; OAuth-discovered OpenAI model availability can change independently of RK.

## Concrete implementation and test plan

One bounded implementation ticket should contain these vertical slices:

1. **Adapter and protocol:** add `crates/rk-harness/src/maki.rs`; register it in `lib.rs`. Unit-test exact argv/env, initial stdin record, system/model/session flags, permission translation/rejection, malformed/unknown records, assistant/tool/usage/retry/result mapping, nonzero exit, zero-cost handling, control delivery, resume, and pre-work auth/certificate/unavailable classification. Use a fake `RK_MAKI_BIN`; no network test in the normal suite.
2. **Policy and lifecycle:** update `crates/rk-daemon/src/supervisor.rs` and `capabilities.rs`. Test effective default/profile/direct/workflow precedence, recorded status, respawn preserving model/mode/session, interrupt/recovery, full-access validation, and fail-closed restricted roles. Assert native `Task`/`Memory` and ambient init/custom-command loading are disabled by argv/policy.
3. **Configuration/validation:** update `crates/rk-workflow/src/schema.cue`, its parser/resolve tests, CLI help/error text in `crates/rk-cli/src/agent_cmds.rs`, and stale harness documentation in `crates/rk-core/src/config.rs`. Add an RPC/CLI integration test showing `maki` reaches the adapter and an invalid kind fails before durable spawn side effects.
4. **Docs and readiness:** update `README.md` and `docs/operator-reference.md`. Add version/auth readiness tests in onboarding if the chosen minimum exceeds simple executable discovery. Defer `rk-mux` changes unless the ticket explicitly includes attached mode.
5. **Acceptance outside CI:** with a sanitized disposable home/repository and explicit model, run one authenticated ChatGPT-OAuth worker through start, tool call, commit, `rk done`, steering (only if enabled), interrupt/resume, accounting, and cleanup. Repeat once with an API-key provider. Inspect that no native subagent, project Lua, custom command, global MCP server, or external memory write was available.

## Decisions still needed

- Will RK require upstream preservation of `rk_control` metadata before enabling live steering, or accept Maki's text-only control turn?
- Will managed Maki use a daemon-owned sanitized config root, and how will it retain OAuth credentials without inheriting unrelated user/project MCP authority?
- Should v1 report cost only when strictly positive, or declare no self-reported cost and rely entirely on RK pricing plus token caps?
- Is headless ordinary-role support sufficient for v1? This report recommends yes; attached and restricted-role support should not block it.
- What minimum Maki version will castles deploy? The research is grounded in installed/tagged 0.5.2 and should not silently claim older releases.

## Upstream sources checked

- [Maki CLI reference](https://maki.sh/docs/cli/)
- [Headless/SDK mode](https://maki.sh/docs/headless/)
- [Permissions](https://maki.sh/docs/permissions/)
- [Providers and ChatGPT OAuth](https://maki.sh/docs/providers/)
- [MCP configuration](https://maki.sh/docs/mcp/)
- [Maki v0.5.2 source](https://github.com/tontinton/maki/tree/v0.5.2)
