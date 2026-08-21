// Versioned repository lifecycle policy. `rk repo add` activates this exact
// content for a newly registered checkout; later edits remain inert until their
// digest is explicitly activated through `rk repo onboard activate`.
repo: {
	work: {
		branch:   "rat/{{agent}}/{{task}}"
		worktree: "{{repo}}/{{agent}}"
	}

	delivery: {
		// Preserve the branch an agent was actually based on. This lets work
		// land into feature/integration branches as well as main.
		target:       "agent-base"
		mode:         "merge"
		remote:       "origin"
		remoteBranch: "{{branch}}"
		deleteSource: true
	}

	landing: {
		maxDiffLines: 3000
	}

	// Regenerable build-artifact paths the daemon's worktree sweep reclaims.
	// The daemon default remains empty/stack-neutral; this Cargo workspace
	// declares its own `target/` as safe to delete.
	reap: {
		artifactPaths: ["target"]
	}
}
