// The daemon-native landing pipeline's completion feed (Phase 3-T4,
// docs/proposals/daemon-native-landing-pipeline.md §2.1 option (a)). This is
// the operator-adopted REPLACEMENT for `steward-on-completion` in
// examples/triggers.cue — see the cutover runbook in
// docs/proposals/daemon-native-landing-pipeline.md §6 before installing this
// file. Do NOT copy both `steward-on-completion` (examples/triggers.cue) and
// `steward-landing-on-completion` (this file) into the SAME triggers
// directory at once: both match the identical `harness_result` predicate, so
// every completion would be double-dispatched — once as a full workflow
// spawn, once onto the LandingQueue — racing each other for the same branch.
//
//   cp examples/triggers-landing-pipeline.cue ~/.rat-kingdom/triggers/steward.cue
//   rm ~/.rat-kingdom/triggers/<wherever steward-on-completion currently lives>
//
// (or the repo-local `.rk/triggers.cue` equivalents — see the runbook for the
// exact swap-over sequence and the parity check to run before removing the
// old trigger for good.)
triggers: [
	// THE LANDING PIPELINE (Phase 3, leverage #2 continued). Same match
	// predicate, same re-entrancy break, and the same admission reasoning as
	// `steward-on-completion` (examples/triggers.cue) — the difference is
	// entirely in `action`: instead of spawning the `steward` mega-workflow to
	// host three gates and a routing decision, this hands the completion
	// straight to the daemon-native `LandingPipeline`
	// (`crates/rk-daemon/src/landing.rs`), which runs the SAME gates in a
	// persistent daemon-owned worktree (no agent spawn for a doc-only/trivial
	// diff or a verdict-cache hit) and only spawns an agent — the shrunk
	// `steward-review` workflow — when an LLM judgment is genuinely needed.
	//
	// `run` is not read for `action: "land"` (see the schema); left unset here
	// rather than naming a placeholder workflow.
	{
		name:   "steward-landing-on-completion"
		action: "land"
		match: {category: "event", identity: "harness_result", search: "\"role\":\"rat\""}
		// A completion storm must not enqueue a storm's worth of work in one
		// reactor cycle beyond the rate cap — the SAME `maxFires` semantics
		// `steward-on-completion` uses today. There is no `maxInFlight` here:
		// admission beyond this point is the daemon-owned `LandingQueue`'s own
		// single-consumer-per-`(repo,target)` job (design doc §2.1), not the
		// reactor's.
		maxFires: 20
	},
]
