// Rat Kingdom repository policy. This file is versioned as `.rk/repo.cue`,
// validated during onboarding, and copied into the operator-owned repository
// registry only after its exact content digest is activated.
package repository

repo: #RepositoryPolicy

#RepositoryPolicy: {
	work: #WorkPolicy | *{}
	delivery: #DeliveryPolicy | *{}
}

#WorkPolicy: {
	// Supported placeholders: {{agent}}, {{task}}, {{repo}}, and {{role}}.
	// Both templates must include {{agent}} so concurrent workers cannot collide.
	branch: string | *"rat/{{agent}}/{{task}}"
	worktree: string | *"{{repo}}/{{agent}}"
}

#DeliveryPolicy: {
	// `agent-base` carries the completed worker's actual base through steward;
	// any other value is a fixed branch name such as `main` or `develop`.
	target: string | *"agent-base"
	mode: "merge" | "merge-push" | "push-branch" | "pr" | *"merge"
	remote: string | *"origin"
	// Supported placeholders: {{branch}}, {{target}}, and {{repo}}.
	remoteBranch: string | *"{{branch}}"
	deleteSource: bool | *true
}
