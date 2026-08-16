// Per-repo named-check registry (TKT-30). Copy to <repo>/.rk/checks.cue.
//
// Each entry is a named command the repo owner trusts. A workflow `run` step
// invokes one by NAME (`{type: "run", check: "test"}`) instead of carrying a raw
// shell command. With `[policy] require_named_checks = true` in the daemon
// config, a raw `command` on a run step is refused fail-closed — so a compromised
// or untrusted workflow definition can only ever run the checks listed HERE,
// never arbitrary shell in an agent's worktree.
//
// A valid registry is also rendered into spawned and resumed worker prompts as
// optional Repository verification checks guidance. The prompt shows the
// declared metadata, while workflow run steps remain the authoritative gate.
// A missing or invalid registry falls back to generic prompt guidance and does
// not make priming fail.
//
// The command still runs via `sh -c` in the active rat's worktree, but the text
// is fixed by this repo-owned file, which is the whole point of the allowlist.
checks: [
	{
		name:              "verify"
		command:           "cargo build --workspace && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings"
		timeout:           "60m"
		environmentPolicy: "strip_rk_spawn"
		toolchain:         "repository Rust toolchain"
	},
	{
		name:              "test"
		command:           "cargo test --quiet"
		timeout:           "20m"
		environmentPolicy: "strip_rk_spawn"
		toolchain:         "repository Rust toolchain"
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
	// The daemon-native landing pipeline (crates/rk-daemon/src/landing.rs)
	// resolves and runs these two by name for every landing candidate; their
	// tuning (protectedPaths/maxDiffFiles/maxDiffLines) lives in the repo's
	// `.rk/repo.cue` `landing` policy block, digest-activated like
	// `delivery`. Dynamic values arrive only through RK_CHECK_* variables;
	// command text remains owned by this registry.
	{
		name: "steward-protected-paths"
		command: "target=$RK_CHECK_TARGET; ! git diff --name-only \"$target\"...HEAD | grep -qE \"$RK_CHECK_PROTECTED_PATHS\""
		timeout: "2m"
	},
	{
		name: "steward-diff-scope"
		command: "target=$RK_CHECK_TARGET; files=$(git diff --name-only \"$target\"...HEAD | wc -l | tr -d ' '); lines=$(git diff --numstat \"$target\"...HEAD | awk '{a=$1;b=$2;if(a==\"-\")a=0;if(b==\"-\")b=0;s+=a+b} END{print s+0}'); echo \"diff-scope: $files files / $lines lines vs $target (budget ${RK_CHECK_MAX_DIFF_FILES}f/${RK_CHECK_MAX_DIFF_LINES}l, 0=off)\"; { [ \"$RK_CHECK_MAX_DIFF_FILES\" -eq 0 ] || [ \"$files\" -le \"$RK_CHECK_MAX_DIFF_FILES\" ]; } && { [ \"$RK_CHECK_MAX_DIFF_LINES\" -eq 0 ] || [ \"$lines\" -le \"$RK_CHECK_MAX_DIFF_LINES\" ]; }"
		timeout: "2m"
	},
	// steward-report-timeout/-gate-failure/-stop/-unknown-verdict and
	// steward-file-rework-ticket are REMOVED (Phase 4 of the steward
	// remediation): the daemon-native LandingPipeline now writes those
	// escalation/rework outcomes via direct Space::out/Tickets::create calls
	// instead of shelling out through a named check.
]
