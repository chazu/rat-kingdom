# Inventory: operator Claude Code configuration inherited by rat harnesses

TKT-01M048ASX3Q4PY9E9XAP8KXB2K (sub-ticket 1 of TKT-01M03RN6RWPNDZGR59KAF8KSBE).
Scope: inventory only — what the `claude` harness inherits today, and what the
rat spawn env already overrides. No evaluation, no policy decision, no
implementation. Evidence gathered 2026-08-18 by directly reading the operator
machine's `~/.claude` config and this repo's spawn code, plus first-hand
observation from inside a live rat session (this one).

## 0. The mechanism: why inheritance happens at all

`crates/rk-harness/src/claude.rs::ClaudeHarness::launch` builds the child
process with `tokio::process::Command::new("claude")` and `cmd.envs(&spec.env)`
(claude.rs:40-60). `Command::envs` **adds/overrides** entries on top of the
process's existing environment — it does not call `env_clear()`. No
`CLAUDE_CONFIG_DIR`, `HOME`, or `--settings` override is set anywhere in the
launch path. Verified two ways:
- `grep -rn "CLAUDE_CONFIG_DIR\|env_clear" crates/rk-harness crates/rk-daemon`
  finds no isolation of the harness launch itself (the only `env_clear`-style
  calls, `onboarding_apply.rs:215` and `workflow_exec.rs:3122`, strip `RK_*`
  vars from unrelated *named-check subprocesses*, not the harness process —
  see §5).
- Directly, from inside this rat session: `CLAUDE_CONFIG_DIR` is unset,
  `HOME=/Users/chazu`, and this session successfully read
  `~/.claude/settings.json`, `~/.claude/CLAUDE.md`, etc. — the operator's real
  personal config directory, not a curated copy.

So today: **a rat's `claude` process is the operator's `claude` process**,
plus `RK_*` vars layered on top (§5). Nothing is curated, filtered, or
isolated.

## 1. Hooks (`~/.claude/settings.json`, live as of this scan)

```json
"hooks": {
  "SessionEnd": [{"hooks": [{"type": "command",
    "command": "AKA_ARCHIVE_PUSH=1 AKA_ARCHIVE_URL=https://archive.tail7fd374.ts.net AKA_ARCHIVE_TOKEN=\"$(/opt/homebrew/bin/timeout 5 /opt/homebrew/bin/pass show loosh/platform/archive/workstation-token 2>/dev/null)\" /Users/chazu/go/bin/aka hook run claude-code"
  }]}],
  "SessionStart": [
    {"matcher": "startup|clear|resume", "hooks": [{"command":
      "test -f .agent/HANDOFF.md && echo '## Resuming from handoff' && cat .agent/HANDOFF.md"}]},
    {"matcher": "*", "hooks": [{"command":
      "bash '/Users/chazu/.claude/hooks/herdr-agent-state.sh' session", "timeout": 10}]},
    {"matcher": "startup|resume", "hooks": [{"command":
      "'/Users/chazu/.local/bin/jcode' setup-hotkey --notify-cli-launch claude", "timeout": 5}]}
  ]
}
```

Per-item, with live/no-op status confirmed by checking this rat's own env:

- **SessionEnd `aka hook run claude-code`**: shells out to `pass show
  loosh/platform/archive/workstation-token` (a local secret-store CLI) and, if
  it succeeds, pushes to `https://archive.tail7fd374.ts.net` via the `aka`
  binary. Runs unconditionally on every SessionEnd — no env guard. Confirmed
  by direct byte-identical read of the command string; not re-run to avoid
  triggering a real archive push. `pass` and the `aka` binary's on-disk
  reachability from a rat's PATH/HOME were not independently re-verified here
  beyond confirming the same `HOME` and hook file this session already
  inherited.
- **SessionStart `.agent/HANDOFF.md` cat**: repo-relative, harmless, purely
  local file read — matcher `startup|clear|resume`.
- **SessionStart `herdr-agent-state.sh session`**: guards on
  `HERDR_ENV=1 && HERDR_SOCKET_PATH set && HERDR_PANE_ID set` before doing
  anything (script body, lines 19-21). **This guard is satisfied in this rat
  session** — `env` shows `HERDR_ENV=1`,
  `HERDR_SOCKET_PATH=/Users/chazu/.config/herdr/herdr.sock`,
  `HERDR_PANE_ID=wS:p1`, all inherited from the daemon's process tree. So this
  hook is NOT a no-op for rats; it actively ran at this session's start (a
  `SessionStart:startup hook success: OK` line appears in this transcript).
  Payload egress (read from the script body): a JSON-RPC-shaped
  `pane.report_agent_session` message over a local Unix socket, carrying only
  `pane_id`, `agent_session_id` (the Claude session UUID), `agent_session_path`
  (the local transcript file *path*, not its contents), and a sequence
  number. No transcript content, no secrets. Socket is local
  (`~/.config/herdr/herdr.sock`), not network.
- **SessionStart `jcode setup-hotkey`**: matcher `startup|resume`; a local
  hotkey-registration helper. Not evaluated for rat relevance (out of scope:
  no decision made), just recorded as present.
- **No `PreToolUse` hook is present in the live `settings.json`.** See §3 —
  this contradicts RTK.md, which is unconditionally injected into every rat's
  system prompt.
- `~/.claude/settings.json.bak` (a stale backup, not live) still shows a
  `PreToolUse` → `rtk hook claude` hook on the `Bash` matcher, confirming this
  hook existed in the past and was later removed from the live config.

## 2. Permissions

- `~/.claude/settings.json`: `"skipDangerousModePermissionPrompt": true`,
  `"alwaysThinkingEnabled": true`, `"effortLevel": "high"`,
  `"agentPushNotifEnabled": true`, `"model": "opus[1m]"`. These are global
  defaults a rat's `claude` process would see unless a spawn arg overrides
  them (the harness does pass `--model` from `spec.model` when set —
  claude.rs:53-55 — which overrides the config default per-invocation).
- `~/.claude/settings.local.json`: a short, narrowly-scoped `allow` list (go
  tooling, `git clone`, one path-scoped `Read`, `cue vet`) with empty
  `deny`/`ask`. Not observed to matter in practice for rats — see next point.
- **Permission mode is moot for rats regardless of the above**:
  `crates/rk-daemon/src/supervisor.rs:113` — `default_permission_mode("claude")
  == "bypassPermissions"` — and `permission_args()` in claude.rs:14-22 turns
  that into `--dangerously-skip-permissions` on the CLI invocation. So a rat's
  `claude` process is launched with all permission prompts already bypassed
  at the CLI level; the operator's allow/deny/ask lists in
  `settings.local.json` are not consulted for rats in the normal case. (A
  read-only role can instead get `"plan"` mode —
  `crates/rk-daemon/src/read_only_roles.rs:41`.) Hooks, unlike permission
  prompts, are NOT gated by permission mode — they fire regardless (§1).
- No project-level `.claude/settings.json` or `.claude/settings.local.json`
  exists in this worktree or in the two registered project roots for this
  repo (`/Users/chazu/dev/go/rat-kingdom`, `/Users/chazu/dev/rust/rat-kingdom`
  — see `~/.claude.json` `"projects"` keys, §4). Project-level settings are
  not a current mitigation layer, just an unused one.

## 3. Global `CLAUDE.md`

`~/.claude/CLAUDE.md` is 8 bytes: literally `@RTK.md` — a Claude Code
`@import`. The imported `~/.claude/RTK.md` (964 bytes) is injected into every
session's context verbatim (confirmed: it appears in this very session's
system reminder as "Contents of /Users/chazu/.claude/RTK.md"). Its content
documents an `rtk` CLI proxy and states:

> All other commands are automatically rewritten by the Claude Code hook.
> Example: `git status` → `rtk git status` (transparent, 0 tokens overhead)

**This is stale relative to the live config.** Per §1, the live
`settings.json` has no `PreToolUse` hook at all; the rewrite hook only exists
in `settings.json.bak`. So every rat (and the operator) is told in its system
prompt that a token-saving rewrite is happening transparently, when — per the
currently-active hook config — it is not. A rat reading this instruction and
calling `rtk gain`/`rtk discover` per RTK.md's "Meta Commands" section would
still work (those are direct CLI invocations, not hook-dependent), but the
implied automatic rewrite of ordinary Bash calls is not currently live.

## 4. Skills, plugins, agents

- `~/.claude/settings.json` `enabledPlugins`: `caveman@caveman`,
  `compound-engineering@every-marketplace`, `frontend-design@claude-plugins-official`,
  `learning-opportunities@learning-opportunities`,
  `understand-anything@understand-anything` all `true`;
  `superpowers@claude-plugins-official` explicitly `false`.
- These plugins are the source of the large skill and subagent inventory
  visible in this very session (the "available skills" and "available agent
  types" system reminders list dozens of `compound-engineering:*` and
  `understand-anything:*` and `caveman:*` entries) — confirmed by cross-
  referencing plugin names against the injected listings rather than assumed.
- `~/.claude/skills/` (user-level, outside any plugin) has 45 entries on disk,
  separately contributing skills such as `dataviz`, `diagnose`, `tdd`,
  `git-guardrails-claude-code`, `procyon-park`, `pudl-core`, etc. — also
  present in this session's skill listing.
- No repo-local `.claude/skills/` or `.claude/agents/` directory exists in
  this worktree (checked: none found under the worktree root).
- None of this is gated by `RK_ROLE`/`RK_AGENT` — a rat gets the exact same
  plugin/skill surface as an interactive operator session on this machine.

## 5. Spawn environment overrides (what rk-daemon actually adds)

`crates/rk-daemon/src/supervisor.rs::agent_env` (lines 4366-4402) is the
**entire** set of environment vars rk-daemon injects for a spawned rat:

| Var | Value |
|---|---|
| `RK_HOME` | daemon's layout home dir |
| `RK_AGENT` | this rat's name |
| `RK_AUTH_TOKEN` | this rat's per-agent daemon auth token (if issued) |
| `RK_ROLE` | `rat` / `reviewer` / etc. |
| `RK_REPO` | repo name |
| `RK_TASK` | ticket id |
| `RK_BRANCH` | branch (if any) |
| `RK_BASE` | resolved base branch |
| `RK_WORKTREE` | worktree path |
| `RK_WORKFLOW_INSTANCE` | workflow instance id (if spawned by a workflow) |
| `PATH` | daemon's own binary dir prepended to the *inherited* `PATH` |

These are laid on top of (not a replacement for) the full ambient environment
the `claude` child process otherwise inherits from the daemon process tree —
confirmed live: this session's `env` shows both the `RK_*` block above **and**
unrelated ambient vars such as `HERDR_ENV`, `HERDR_SOCKET_PATH`,
`HERDR_PANE_ID`, `OBJC_DISABLE_INITIALIZE_FORK_SAFETY`, and a sourced-function
marker from the operator's shell profile
(`BASH_FUNC__mark_class_sourced%%`). None of these ambient vars are
RK-specific; they leaked in purely because nothing clears the environment
before launch (§0). `RK_AUTH_TOKEN` itself is therefore also visible to any
operator-inherited hook or tool running inside the rat's session, exactly as
symmetrically as the operator's secrets are visible to the rat.

**This is a distinct code path from the `STRIPPED_RK_ENV` mechanism** used by
named-check runs (`crates/rk-daemon/src/onboarding_apply.rs:215` and
`crates/rk-daemon/src/workflow_exec.rs:3100-3122`, `environment_policy:
StripRkSpawn` in `.rk/checks.cue`, e.g. this repo's own `verify` check). That
mechanism removes `RK_AGENT`/`RK_TASK`/`RK_REPO`/`RK_ROLE`/`RK_HOME`/
`RK_BRANCH`/`RK_WORKTREE`/`RK_AUTH_TOKEN` from a **verification subprocess**
spawned by a workflow run-step, so the check's `cargo test` isn't
misattributed to the calling rat's identity (see the fleet convention
`rk-env-poisons-cargo-test`). It does nothing for, and is entirely separate
from, the rat's own top-level `claude` process environment inventoried above
— that process is never stripped of anything, only ever added to.

## 6. MCP servers

Three distinct sources were found, all wholesale-inherited by a rat's
`claude` process (no rat-specific filtering observed anywhere):

- **`~/.claude/mcp.json` and `~/.claude/mcp_settings.json`** (byte-identical):
  `blender` (`uvx blender-mcp`) and `emacs` (local Unix-socket wrapper at
  `~/.emacs.d/emacs-mcp-server-chazu.sock`, `EMACS_MCP_TIMEOUT=10`).
- **`~/.claude.json` top-level `"mcpServers"`** (separate, global,
  account-level store): `emacs` (same, different absolute path variant via
  chezmoi) and `gitnexus` (`npx -y gitnexus@latest mcp`, no local
  socket/binary dependency — this is why `gitnexus` tools appeared as
  "still connecting" in this very session).
- **`~/.claude.json` per-project `"mcpServers"`** for both registered
  rat-kingdom project roots (`/Users/chazu/dev/go/rat-kingdom`,
  `/Users/chazu/dev/rust/rat-kingdom`): empty `{}` — no project-level
  addition or override; the global set above applies unmodified.
- **Account-linked OAuth connectors** (`claude.ai Gmail`, `claude.ai Google
  Calendar`, `claude.ai Google Drive`): present in this very session's MCP
  server list, but sourced from neither `mcp.json` nor `.claude.json`'s
  `mcpServers` keys — these are tied to the operator's authenticated claude.ai
  account rather than a project file. This session's own tool-listing
  instructions note "interactively-authenticated MCP servers (e.g. claude.ai)
  may be absent in headless/cron runs," i.e. Anthropic-side infrastructure
  already provides *some* mitigation for non-interactive spawns, but this
  rat's own session (an interactively-launched harness process, not a cron
  trigger) had them listed as connecting rather than absent — so that
  mitigation does not obviously apply to every rat spawn shape. Not
  independently confirmed whether they became reachable or errored, since
  exercising them was out of scope for a read-only inventory.

## Summary table

| Class | Curated for rats today? | Notes |
|---|---|---|
| Hooks (SessionEnd archive push) | No | Runs unconditionally; fetches a secret via `pass`, pushes to a remote archive URL |
| Hooks (SessionStart herdr) | No | Live in this session (env guard satisfied); local-socket-only, path/id metadata, no content |
| Hooks (SessionStart handoff/jcode) | No | Low-risk, local file reads / hotkey registration |
| Hooks (PreToolUse rtk rewrite) | N/A | Not live at all currently (removed from settings.json, only in .bak) — but still documented as live in the always-injected RTK.md |
| Permissions (allow/deny/ask) | Moot | Rats launch with `--dangerously-skip-permissions` regardless of settings.local.json |
| Global CLAUDE.md / RTK.md | No | Injected verbatim into every rat prompt, including a stale hook claim |
| Skills / plugins / agents | No | Full operator plugin+skill surface, identical to an interactive session |
| Spawn env (`RK_*`) | Yes, additive only | 11 vars added; nothing about the rest of the environment is cleared or filtered |
| MCP servers (local + account) | No | Local `mcp.json`/global `.claude.json` servers plus account-linked OAuth connectors all inherited; project-level entries are empty (no override) |

No evaluation of helps/harms and no policy recommendation is made here by
design — that is TKT-01M048ASX3ERRP7JRCQNGDH0AD's job, one level up in the
same parent (TKT-01M03RN6RWPNDZGR59KAF8KSBE).
