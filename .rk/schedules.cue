package schedules

// Run the complete self-improvement chain once per day at 03:00 UTC. The
// scheduler keys single-flight protection on this name, so a slow prior night
// is skipped rather than overlapped.
schedules: [{
	name: "nightly-self-improve"
	cron: "0 3 * * *"
	run:  "nightly-self-improve"
	params: {
		limit:     "5"
		timeout:   "45m"
		budgetUsd: "30"
	}
}]
