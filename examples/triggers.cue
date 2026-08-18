// Example reactor triggers. Copy to `~/.rat-kingdom/triggers/` (global) or drop
// a `triggers.cue` in a repo's `.rk/` directory. Each entry fires a workflow
// when a matching tuple lands in the space — validated against
// crates/rk-workflow/src/triggers-schema.cue.
//
//   cp examples/triggers.cue ~/.rat-kingdom/triggers/
//
// Template params from the matched tuple:
//   {{tuple.category}} {{tuple.scope}} {{tuple.identity}} {{tuple.instance}}
//   {{tuple.id}}       {{tuple.payload.<field>}}
// A param whose whole value is one {{tuple.payload.<field>}} placeholder passes
// the raw JSON value (and type) through; otherwise it is string-interpolated.
//
// A {{tuple.payload.<field>}} value drawn from an ingest-sourced tuple (an
// SDLC alert/webhook event) is fenced/escaped/provenance-marked before it
// reaches a spawned prompt — see docs/reactor.md "Payload hygiene for
// ingest-sourced tuples". Do not template such a field where it must stay a
// bare short value.
triggers: [
	// When any rat records a blocking obstacle, kick off a triage workflow in
	// the repo the obstacle is scoped to (its scope resolves to a registered
	// repo), passing the obstacle text along.
	{
		name:  "triage-obstacle"
		match: {category: "obstacle"}
		run:   "solo-task"
		params: {
			taskId:      "triage-{{tuple.identity}}"
			description: "Investigate the reported obstacle: {{tuple.payload.text}}"
		}
		// Never react to the daemon's own bookkeeping obstacles.
		exclude: ["daemon"]
		// A pile-up of obstacles must not spawn a pile-up of triage rats.
		maxFires: 5
	},

	// DEPENDENCY-UNBLOCK AUTO-DISPATCH (leverage #6). When a ticket closes in
	// `rat-kingdom`, drain the now-ready backlog — a ticket's dependents can only
	// become ready when it (their last blocker) reaches done/closed, so this is
	// the moment to advance the DAG. `backlog-drain`'s `for_each` recomputes the
	// dependency-aware ready set (open tickets with every dep satisfied) and
	// atomically claims each, so a just-unblocked dependent is picked up the
	// instant its blocker closes instead of waiting for the next drain sweep or an
	// operator. The claim dedups against the continuous-drain, so both can run.
	//
	// `ticket_closed` is emitted by the ticket store on the non-terminal → terminal
	// edge (Tickets::edit), scoped to the ticket's repo — so `repo` here is
	// optional (it would default to the event's scope). Idempotent per close via
	// the reactor's durable marker; `maxFires` caps a close storm.
	{
		name:  "drain-on-unblock"
		match: {category: "event", identity: "ticket_closed", scope: "rat-kingdom"}
		run:   "backlog-drain"
		repo:  "rat-kingdom"
		maxFires: 3
	},

	// THE STEWARD (leverage #2). On every rat completion, reactively triage that
	// rat's branch: the daemon-native landing pipeline decides auto-merge /
	// rework-ticket / escalate — so the operator reviews exceptions, not every
	// branch. This used to be a `run: "steward"` workflow trigger fired from
	// here; that mega-workflow and its trigger entry are retired post-cutover
	// (steward remediation Phase 4, TKT-01M048ASYM00N37EBK1VM7FH5H) in favor of
	// `examples/triggers-landing-pipeline.cue`'s `steward-landing-on-completion`
	// (`action: "land"`), which hands the same completions straight to
	// `crates/rk-daemon/src/landing.rs` instead of spawning a CUE workflow.
]

// ── Reference: the convention-quorum loop (TKT-22) ──────────────────────────
//
// The flagship norm-formation loop — Suggestion → Endorsement → Convention →
// injected into the next rat's prompt — is deliberately NOT wired as a #Trigger
// here. All three of its hops are built-ins, so there is nothing to add to this
// file to enable it:
//
//   1. propose + endorse   `rk suggest '<norm>'`, then peers `rk endorse <id>`
//   2. promote at quorum    a built-in reactor reaction (`promote_conventions`),
//                           gated by `[reactor] quorum` in config.toml — no
//                           trigger, no workflow, no model, no operator.
//   3. inject at spawn      the supervisor composes active conventions into a
//                           spawned rat's system prompt (a "Standing conventions"
//                           section), so a promoted norm is binding, not advisory.
//
// GOTCHA — do not try to react to a promotion with a #Trigger. A promoted
// Convention is authored by the reserved instance "reactor", and the reactor
// skips its own output before matching any trigger (the re-entrancy break that
// stops the fleet from reacting to itself). So a trigger like
//   {name: "on-convention", match: {category: "convention"}, run: "..."}
// would type-check but SILENTLY NEVER FIRE. React to the rat-authored
// `suggestion`/`endorsement` tuples upstream if you need a hook — never to the
// reactor-authored `convention` downstream.
//
// See docs/reactor.md ("The composed convention-quorum loop"), the runnable
// demo `scripts/convention-quorum-demo.sh`, and the CI self-test
// `crates/rk-daemon/tests/convention_quorum.rs`.
