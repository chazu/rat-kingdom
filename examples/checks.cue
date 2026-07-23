// Per-repo named-check registry (TKT-30). Copy to <repo>/.rk/checks.cue.
//
// Each entry is a named command the repo owner trusts. A workflow `run` step
// invokes one by NAME (`{type: "run", check: "test"}`) instead of carrying a raw
// shell command. With `[policy] require_named_checks = true` in the daemon
// config, a raw `command` on a run step is refused fail-closed — so a compromised
// or untrusted workflow definition can only ever run the checks listed HERE,
// never arbitrary shell in an agent's worktree.
//
// The command still runs via `sh -c` in the active rat's worktree, but the text
// is fixed by this repo-owned file, which is the whole point of the allowlist.
checks: [
	{
		name:    "test"
		command: "cargo test --quiet"
		timeout: "20m"
	},
	{
		name:       "clippy"
		command:    "cargo clippy --all-targets -- -D warnings"
		expectExit: 0
		timeout:    "10m"
	},
	{
		name:    "fmt"
		command: "cargo fmt --check"
		timeout: "2m"
	},
]
