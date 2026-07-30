// rat-kingdom #Check schema. A checks file defines a top-level `checks:` list;
// each entry is a NAMED, repo-owned command that a workflow `run` step may invoke
// by name (`check: "<name>"`) instead of carrying a raw shell command. The file
// lives at `<repo>/.rk/checks.cue` and belongs to the repo, not to the (possibly
// untrusted) workflow definition — so with `[policy] require_named_checks` on, a
// compromised workflow def can only ever run the checks listed here, never
// arbitrary shell (TKT-30). Files are evaluated as one CUE package with this
// schema, so violations are CUE unification errors — exactly like workflows.
package checks

checks: [...#Check]

#Check: {
	// Stable name a `run` step references via `check: "<name>"`. Unique per file
	// by convention.
	name: string & =~"^[a-z][a-z0-9-]*$"

	// Command line, executed verbatim via `sh -c` in the active agent's worktree.
	// The text is fixed by the repo owner, which is exactly the trust boundary
	// the named-check registry establishes.
	command: string

	// Working directory relative to the worktree root; the root if unset. A `run`
	// step referencing this check may override it with its own `cwd`.
	cwd?: string

	// Inline fail-closed exit gate: when set, a mismatched exit code fails the
	// instance directly (as on a raw `run` step's `expectExit`).
	expectExit?: int

	// Hard wall-clock bound; a suite still running when it elapses is killed and
	// the step fails closed. Unset falls back to the referencing run step's own
	// timeout (default 10m).
	timeout?: string

	// Exact environment contract for the command. "inherit" preserves the
	// daemon environment. "strip_rk_spawn" removes the supervised-agent RK_*
	// identity variables before execution, which is required by repositories
	// whose test clients otherwise inherit the caller rat's authorization.
	environmentPolicy?: "inherit" | "strip_rk_spawn"

	// Human-readable repository-owned toolchain description captured in
	// onboarding verification evidence (for example "mise rust@1.95.0").
	toolchain?: string
}
