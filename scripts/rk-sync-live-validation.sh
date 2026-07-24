#!/usr/bin/env bash
# TKT-71 — rk-sync live multi-machine replication validation over a REAL remote.
#
# The in-process two-castle tests (crates/rk-daemon/src/sync.rs, crates/rk-sync)
# exercise the union merge, take-op replication (TKT-58), per-actor compaction
# (TKT-58) and Ed25519 identity (TKT-59) inside ONE process against an in-memory
# space. What they never do is drive the whole thing the way an operator would:
# two independent daemons, each in its own RK_HOME with its own castle.key, each
# with the real `rk` CLI, pushing and fetching a shared git remote out of
# process. This harness is that missing end-to-end proof.
#
# "Two machines" here is two isolated RK_HOMEs sharing a bare git remote on a
# `file://` URL — a real remote to git (real fetch/push, real fast-forward
# checks, real refs/notes/rk/<actor> per castle). This is exactly the "two
# containers with a shared bare remote" the ticket permits; nothing in rk-sync
# behaves differently over ssh:// or https:// (it is `git fetch`/`git push` of a
# notes refspec either way). Run it on one host, in CI, or across two boxes by
# pointing both configs at the same remote.
#
# It validates the five behaviours the ticket calls out:
#   1. durable tuples + claims converge on both castles
#   2. a consume on one castle drops the tuple on the other and never resurrects
#   3. partition (remote unreachable) then rejoin converges
#   4. compaction drains a consumed tuple's Out+Take on both, no resurrection
#   5. push stays fast-forward after compaction rewrites the notes ref
#
#   ./scripts/rk-sync-live-validation.sh
#
# Exit 0 = every scenario passed. Any FAIL exits non-zero with the daemon logs
# left under a printed temp dir for triage.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

command -v jq >/dev/null || { echo "FAIL: jq is required" >&2; exit 1; }

# --- build & locate a fresh rk ------------------------------------------------
echo "== building rk =="
cargo build -p rk-cli --manifest-path "$repo_root/Cargo.toml" >/dev/null
RK="$repo_root/target/debug/rk"

work="$(mktemp -d)"
A="$work/castle-a"
B="$work/castle-b"
remote="$work/remote.git"
mkdir -p "$A" "$B"

pid_a=""
pid_b=""
keep_logs=0
cleanup() {
	[ -n "$pid_a" ] && kill "$pid_a" >/dev/null 2>&1 || true
	[ -n "$pid_b" ] && kill "$pid_b" >/dev/null 2>&1 || true
	if [ "$keep_logs" = 1 ]; then
		echo "logs + state left in $work" >&2
	else
		rm -rf "$work"
	fi
}
trap cleanup EXIT

step() { printf '\n== %s ==\n' "$1"; }
pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail() { keep_logs=1; printf '  \033[31mFAIL\033[0m %s\n' "$1" >&2; exit 1; }

# --- a real shared remote (bare repo, file:// URL) ----------------------------
git init -q --bare "$remote"
url="file://$remote"

# --- two isolated castles pointed at the shared remote ------------------------
# interval_secs is clamped to >=5 by the daemon; we drive every cycle explicitly
# with `rk sync now` so the test is deterministic and never waits on a timer.
write_config() {
	local home="$1" name="$2"
	cat >"$home/config.toml" <<EOF
castle_name = "$name"

[sync]
enabled = true
remote_url = "$url"
interval_secs = 3600
EOF
}
write_config "$A" castle-a
write_config "$B" castle-b

start_daemon() {
	local home="$1" var="$2"
	RK_HOME="$home" "$RK" daemon run >"$home/daemon.log" 2>&1 &
	printf -v "$var" '%s' "$!"
	for _ in $(seq 1 80); do
		sleep 0.25
		RK_HOME="$home" "$RK" daemon status >/dev/null 2>&1 && return 0
	done
	fail "daemon for $home did not come up (see $home/daemon.log)"
}

step "starting two daemons, one per castle, sharing $url"
start_daemon "$A" pid_a
start_daemon "$B" pid_b
pass "castle-a (pid $pid_a) and castle-b (pid $pid_b) are up"

# --- helpers ------------------------------------------------------------------
a() { RK_HOME="$A" "$RK" "$@"; }
b() { RK_HOME="$B" "$RK" "$@"; }

# `rk sync now --json` -> CycleStats. Echoes the raw JSON for the caller to jq.
sync_a() { a --json sync now; }
sync_b() { b --json sync now; }

# A full pairwise exchange: A push -> B fetch/push -> A fetch. Two rounds so a
# tuple authored this cycle on either side is visible on both by the end.
converge() {
	sync_a >/dev/null; sync_b >/dev/null
	sync_a >/dev/null; sync_b >/dev/null
}

# Count durable tuples matching (category, scope, identity) in a castle's space.
count() { # count <a|b> <category> <scope> <identity>
	local who="$1"; shift
	"$who" --json scan "$1" "$2" "$3" | jq 'length'
}

# ============================================================================ #
step "SCENARIO 1 — durable tuples + claims converge"
# ============================================================================ #
# Each castle authors a durable fact and a conflicting durable claim on the same
# (claim, work, task-x). Claims are normally Ephemeral (they evaporate) and so
# never replicate; an explicit --lifecycle furniture makes a durable claim, the
# thing claim arbitration is actually about. Earliest-ULID-wins arbitration is a
# read-time function of the union, so the invariant we assert here is the one it
# depends on: both castles hold the identical union of claim records.
a out fact work fact-from-a --payload '{"note":"authored on a"}' >/dev/null
b out fact work fact-from-b --payload '{"note":"authored on b"}' >/dev/null
a out claim work task-x --lifecycle furniture --payload '{"who":"a"}' >/dev/null
b out claim work task-x --lifecycle furniture --payload '{"who":"b"}' >/dev/null

converge

[ "$(count a fact work fact-from-b)" = 1 ] || fail "castle-a never imported b's fact"
[ "$(count b fact work fact-from-a)" = 1 ] || fail "castle-b never imported a's fact"
pass "durable facts replicated both ways"

claims_a="$(count a claim work task-x)"
claims_b="$(count b claim work task-x)"
[ "$claims_a" = 2 ] && [ "$claims_b" = 2 ] \
	|| fail "conflicting durable claims did not converge (a=$claims_a b=$claims_b, want 2/2)"
pass "conflicting durable claims converged to the same 2-record union on both castles"

# Both castles must agree on the arbitrated winner (earliest ULID id). We derive
# it identically from each castle's local union and require the same answer.
winner_a="$(a --json scan claim work task-x | jq -r 'sort_by(.id)[0].payload.who')"
winner_b="$(b --json scan claim work task-x | jq -r 'sort_by(.id)[0].payload.who')"
[ "$winner_a" = "$winner_b" ] \
	|| fail "castles disagree on the arbitrated claim winner (a=$winner_a b=$winner_b)"
pass "deterministic claim arbitration agrees on both castles (winner=$winner_a)"

# Presence / peers: each castle sees the other as a distinct signed actor.
peers_a="$(a --json peers | jq 'length')"
[ "$peers_a" -ge 2 ] || fail "castle-a sees $peers_a peers, want >=2"
pass "castle-a sees $peers_a castles in the shared tuplespace"

# ============================================================================ #
step "SCENARIO 2 — a consume on one castle drops the tuple on the other, forever"
# ============================================================================ #
a out fact work consumable --payload '{"grab":"me"}' >/dev/null
converge
[ "$(count b fact work consumable)" = 1 ] || fail "castle-b never imported the consumable"
pass "castle-b imported the consumable authored on castle-a"

# castle-a consumes its own tuple destructively; the diff-based take detector
# turns the local disappearance into a replicated Take on the next cycle.
taken="$(a --json in fact work consumable --timeout 2s | jq -r '.payload.grab // empty')"
[ "$taken" = "me" ] || fail "castle-a could not consume its own tuple"
takes="$(sync_a | jq '.takes')"
[ "$takes" = 1 ] || fail "castle-a did not export the consumption as a Take (takes=$takes)"
pass "castle-a exported the consumption as a Take op"

removed="$(sync_b | jq '.removed')"
[ "$removed" = 1 ] || fail "castle-b did not drop the consumed tuple (removed=$removed)"
[ "$(count b fact work consumable)" = 0 ] || fail "consumed tuple still present on castle-b"
[ "$(count a fact work consumable)" = 0 ] || fail "consumed tuple still present on castle-a"
pass "the consumption propagated: gone on both castles"

# No resurrection window: many more cycles must never bring it back via out_if_new.
for _ in $(seq 1 6); do converge; done
[ "$(count a fact work consumable)" = 0 ] && [ "$(count b fact work consumable)" = 0 ] \
	|| fail "consumed tuple RESURRECTED after further sync cycles"
pass "no resurrection across 6 further convergence rounds"

# ============================================================================ #
step "SCENARIO 3 — partition, then rejoin, converges"
# ============================================================================ #
# Simulate a network partition by moving the shared remote out of the way: both
# castles' fetch/push now fail, so neither can reach the other. Each keeps
# writing locally. On rejoin the accumulated writes must exchange and converge.
mv "$remote" "$remote.parked"
pass "remote parked — castles are partitioned"

a out fact work made-during-partition-a --payload '{}' >/dev/null
b out fact work made-during-partition-b --payload '{}' >/dev/null

# A cycle during the partition must fail its push (surfaced, not silent) and
# must NOT crash the daemon or lose the local write.
pushed_a="$(sync_a | jq '.pushed')"
[ "$pushed_a" = false ] || fail "castle-a reported a successful push during a partition"
[ "$(count a fact work made-during-partition-a)" = 1 ] || fail "local write lost during partition"
[ "$(count a fact work made-during-partition-b)" = 0 ] || fail "castle-a saw b's write across a partition"
# The failure is coordination-visible as a system-scope obstacle (not a silent stall).
obst="$(a --json scan obstacle system sync_failure | jq 'length')"
[ "$obst" -ge 1 ] || fail "partition did not surface a sync_failure obstacle"
pass "partition kept local writes, failed the push, and raised a sync_failure obstacle"

# Rejoin.
sync_b >/dev/null 2>&1 || true   # let b also register the failure, best-effort
mv "$remote.parked" "$remote"
pass "remote restored — castles rejoined"

converge; converge
[ "$(count a fact work made-during-partition-b)" = 1 ] || fail "castle-a never converged b's partition-era write"
[ "$(count b fact work made-during-partition-a)" = 1 ] || fail "castle-b never converged a's partition-era write"
pass "partition-era writes from both sides converged after rejoin"

# The rejoin push must itself stay fast-forward (no force): a successful push.
[ "$(sync_a | jq '.pushed')" = true ] || fail "post-rejoin push did not succeed (non-ff?)"
pass "post-rejoin push stayed fast-forward"

# ============================================================================ #
step "SCENARIO 4 — compaction drains a consumed tuple's Out+Take on both castles"
# ============================================================================ #
# Compaction (NotesSync::compact) prunes each actor's OWN ref: drop Out(T) once
# T is taken anywhere; drop Take(T) once its Out is gone from the union. Two
# phases, author-of-Out first, author-of-Take second — no window where T is back.
# Self-contained: author a fresh tuple, replicate it, consume it, then watch the
# Out and its Take drain out of the log while the tuple never resurrects.
a out fact work drain-me --payload '{"grab":"drain-me"}' >/dev/null
converge
[ "$(count b fact work drain-me)" = 1 ] || fail "castle-b never imported drain-me"
a in fact work drain-me --timeout 2s >/dev/null || fail "castle-a could not consume drain-me"

# From the moment of consumption, drive cycles and total the pruned records. The
# drain is two-phase: the first cycle after the consume both appends the Take AND
# compacts the now-taken Out away (phase 1); a later cycle, once castle-a's own
# push has refreshed its remote-tracking copy so the Out is gone from the whole
# union, drops the Take (phase 2). Counting from here captures both phases.
compacted_total=0
for _ in $(seq 1 10); do
	ca="$(sync_a | jq '.compacted')"
	cb="$(sync_b | jq '.compacted')"
	compacted_total=$(( compacted_total + ca + cb ))
done
[ "$compacted_total" -ge 2 ] \
	|| fail "compaction did not drain the Out+Take (pruned=$compacted_total, want >=2)"
pass "compaction pruned $compacted_total record(s) — the Out and its Take both drained"

# The consumed tuple must still be gone on both castles after compaction — the
# "no resurrection window" guarantee across the two-phase drain.
[ "$(count a fact work drain-me)" = 0 ] && [ "$(count b fact work drain-me)" = 0 ] \
	|| fail "consumed tuple resurfaced during/after compaction"
pass "consumed tuple stayed gone through the full compaction drain"

# Directly confirm the records left the on-disk notes log (not just the space):
# no surviving record should mention the consumed identity on either ref.
grep_notes() { # grep_notes <home> -> emits every note blob's content across all rk refs
	local home="$1"
	git -C "$home/sync" for-each-ref --format='%(refname)' 'refs/notes/rk' | while read -r r; do
		git -C "$home/sync" notes --ref "$r" list 2>/dev/null | awk '{print $1}' | while read -r blob; do
			git -C "$home/sync" cat-file blob "$blob" 2>/dev/null
		done
	done
}
if grep_notes "$A" | grep -q '"grab":"drain-me"'; then
	fail "consumed tuple's Out record survived compaction in castle-a's notes log"
fi
pass "the consumed tuple's records are physically gone from the notes log"

# ============================================================================ #
step "SCENARIO 5 — push stays fast-forward after compaction rewrites the ref"
# ============================================================================ #
# git-notes edits advance the notes ref via new commits, so a compaction that
# rewrites note CONTENT still leaves the ref a fast-forward of the remote — the
# push in sync_with_remote carries no --force, so a non-ff would fail it and
# raise a sync_failure obstacle. Author fresh durable churn, consume half, and
# drive many cycles; every push must succeed and no new sync_failure may appear.
base_obstacles="$(a --json scan obstacle system sync_failure | jq 'length')"
for i in $(seq 1 5); do
	a out fact work "churn-a-$i" --payload "{\"i\":$i}" >/dev/null
	b out fact work "churn-b-$i" --payload "{\"i\":$i}" >/dev/null
done
converge
# consume some, forcing Out+Take churn and thus compaction rewrites
for i in 1 2 3; do
	a in fact work "churn-a-$i" --timeout 2s >/dev/null 2>&1 || true
	b in fact work "churn-b-$i" --timeout 2s >/dev/null 2>&1 || true
done

all_ff=1
for _ in $(seq 1 10); do
	[ "$(sync_a | jq '.pushed')" = true ] || all_ff=0
	[ "$(sync_b | jq '.pushed')" = true ] || all_ff=0
done
[ "$all_ff" = 1 ] || fail "a push was rejected after compaction (non-fast-forward)"
pass "every push stayed fast-forward across 10 post-compaction cycles"

now_obstacles="$(a --json scan obstacle system sync_failure | jq 'length')"
[ "$now_obstacles" = "$base_obstacles" ] \
	|| fail "new sync_failure obstacle(s) appeared after compaction ($base_obstacles -> $now_obstacles)"
pass "no new sync_failure obstacle raised by compaction pushes"

# Surviving churn converged consistently on both castles.
converge
for i in 4 5; do
	[ "$(count b fact work churn-a-$i)" = 1 ] || fail "castle-b missing surviving churn-a-$i"
	[ "$(count a fact work churn-b-$i)" = 1 ] || fail "castle-a missing surviving churn-b-$i"
done
pass "surviving post-compaction churn converged on both castles"

printf '\n\033[32m== ALL SCENARIOS PASSED ==\033[0m\n'
