package checks

// Repository-owned executable contract. Workflows may select these entries by
// name, but they cannot replace their command text. This keeps unattended
// automation compatible with the castle's fail-closed require_named_checks
// policy.
checks: [
	{
		name:              "verify"
		command:           "mise run verify"
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
	{
		name: "steward-report-timeout"
		command: "text=\"steward: run gate for $RK_CHECK_TASK_ID did not finish within $RK_CHECK_GATE_TIMEOUT on $RK_CHECK_BRANCH — branch held unmerged; raise gateTimeout or select a narrower named check\"; payload=$(jq -nc --arg task \"$RK_CHECK_TASK_ID\" --arg text \"$text\" '{agent:\"steward\",task:$task,text:$text}'); rk out need \"$RK_CHECK_REPO\" steward --payload \"$payload\""
		expectExit: 0
		timeout: "2m"
	},
	{
		name: "steward-report-gate-failure"
		command: "text=\"steward: run gate FAILED for $RK_CHECK_TASK_ID on $RK_CHECK_BRANCH — branch held unmerged; read the suite output with rk workflow status\"; payload=$(jq -nc --arg task \"$RK_CHECK_TASK_ID\" --arg text \"$text\" '{agent:\"steward\",task:$task,text:$text}'); rk out need \"$RK_CHECK_REPO\" steward --payload \"$payload\""
		expectExit: 0
		timeout: "2m"
	},
	{
		name: "steward-file-rework-ticket"
		command: "rk ticket new \"rework: $RK_CHECK_TASK_ID\" --repo \"$RK_CHECK_REPO\" --body \"Steward routed REWORK on branch $RK_CHECK_BRANCH. Read the reviewer notes: rk scan artifact $RK_CHECK_REPO\""
		expectExit: 0
		timeout: "2m"
	},
	{
		name: "steward-report-stop"
		command: "text=\"steward: reviewer returned STOP for $RK_CHECK_TASK_ID on $RK_CHECK_BRANCH — needs a human merge decision; branch held unmerged\"; payload=$(jq -nc --arg task \"$RK_CHECK_TASK_ID\" --arg text \"$text\" '{agent:\"steward\",task:$task,text:$text}'); rk out need \"$RK_CHECK_REPO\" steward --payload \"$payload\""
		expectExit: 0
		timeout: "2m"
	},
	{
		name: "steward-report-unknown-verdict"
		command: "text=\"steward: unrecognized review verdict for $RK_CHECK_TASK_ID on $RK_CHECK_BRANCH — branch held unmerged, needs a human\"; payload=$(jq -nc --arg task \"$RK_CHECK_TASK_ID\" --arg text \"$text\" '{agent:\"steward\",task:$task,text:$text}'); rk out need \"$RK_CHECK_REPO\" steward --payload \"$payload\""
		expectExit: 0
		timeout: "2m"
	},
]
