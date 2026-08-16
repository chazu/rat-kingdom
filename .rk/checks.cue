package checks

// Repository-owned executable contract. Workflows may select these entries by
// name, but they cannot replace their command text. This keeps unattended
// automation compatible with the castle's fail-closed require_named_checks
// policy.
checks: [
	{
		name:              "verify"
		command:           "MISE_TRUSTED_CONFIG_PATHS=\"$PWD\" mise run verify"
		timeout:           "60m"
		environmentPolicy: "strip_rk_spawn"
		toolchain:         "mise rust@1.95.0"
	},
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
	// remediation, TKT-01M036PSF2WV7NHZE00G2EFCVK): the daemon-native
	// LandingPipeline (crates/rk-daemon/src/landing.rs) now writes those
	// escalation/rework outcomes via direct Space::out/Tickets::create calls
	// (`LandingPipeline::escalate`/`file_rework_ticket`) instead of shelling
	// out through a named check. steward-protected-paths/steward-diff-scope
	// above REMAIN: the pipeline still resolves and runs them by name
	// (`PROTECTED_PATHS_CHECK`/`DIFF_SCOPE_CHECK` in landing.rs) — only their
	// tuning (protectedPaths/maxDiffFiles/maxDiffLines) moved, into
	// `.rk/repo.cue`'s `landing` policy block.
]
