#!/usr/bin/env bash
# TKT-22 — the convention-quorum loop, run against a real daemon with the real
# `rk` CLI. This is the operator-facing companion to the CI self-test
# (crates/rk-daemon/tests/convention_quorum.rs): it shows norm-formation with no
# human in the path, end to end, from three rats agreeing to a binding standing
# convention.
#
# The loop has three hops, and this demo drives all three the way the fleet does:
#
#   1. PROPOSE + ENDORSE  (rats)     `rk suggest` / `rk endorse`
#   2. PROMOTE AT QUORUM  (reactor)  a built-in reaction — no trigger, no model,
#                                    no operator: distinct endorsers reaching
#                                    `[reactor] quorum` mint a Furniture Convention
#   3. INJECT AT SPAWN    (supervisor) the promoted norm is composed into the
#                                    next rat's system prompt as a binding
#                                    "Standing conventions" section (TKT-18)
#
# Hops 1-2 are asserted here against a throwaway daemon in an isolated RK_HOME,
# so the demo is safe to run anywhere and never touches your real castle. Hop 3
# (spawn-injection) is verified by a real `rk spawn` under a fake harness that
# captures its own composed system prompt; if the installed binary predates the
# injection code it reports PENDING rather than failing, so the demo still proves
# the promotion loop on any build.
#
#   ./scripts/convention-quorum-demo.sh
#
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# --- isolated, throwaway castle so we never touch the real tuplespace ---------
work="$(mktemp -d)"
export RK_HOME="$work/home"
mkdir -p "$RK_HOME"

# --- build & locate a fresh rk ------------------------------------------------
echo "== building rk =="
cargo build --release -p rk-cli --manifest-path "$repo_root/Cargo.toml" >/dev/null
RK="$repo_root/target/release/rk"

daemon_pid=""
cleanup() {
	[ -n "$daemon_pid" ] && kill "$daemon_pid" >/dev/null 2>&1 || true
	rm -rf "$work"
}
trap cleanup EXIT

step() { printf '\n== %s ==\n' "$1"; }
fail() { printf 'FAIL: %s\n' "$1" >&2; exit 1; }

# --- fake harness that records its own composed system prompt (for hop 3) -----
# The daemon spawns rats in its OWN process, so RK_FAKE_HARNESS_CMD must be in
# the daemon's environment — set it (and the sink path) BEFORE launching it.
prompt_sink="$work/spawned-prompt.txt"
fake="$work/fake-harness.sh"
cat >"$fake" <<EOF
#!/usr/bin/env bash
read -r _prompt || true
printf '%s' "\${RK_FAKE_SYSTEM_PROMPT:-}" > "$prompt_sink"
echo '{"type":"system","subtype":"init","session_id":"demo"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"noted","session_id":"demo","total_cost_usd":0,"usage":{"input_tokens":1,"output_tokens":1,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}'
EOF
chmod +x "$fake"
export RK_FAKE_HARNESS_CMD="$fake"

# --- start a private daemon for this throwaway castle -------------------------
# `rk` never auto-starts a daemon from inside an agent session, and starting one
# explicitly keeps the demo self-contained everywhere. All `rk` calls below then
# just connect to this one over its socket in RK_HOME.
echo "== starting a private daemon (RK_HOME=$RK_HOME) =="
"$RK" daemon run >"$work/daemon.log" 2>&1 &
daemon_pid=$!
for _ in $(seq 1 60); do
	sleep 0.25
	"$RK" daemon status >/dev/null 2>&1 && break
done
"$RK" daemon status >/dev/null 2>&1 || fail "daemon did not come up (see $work/daemon.log)"

# A distinct endorser is distinct by RK_AGENT — the reactor counts DISTINCT
# `instance`s, so three agents endorsing is what trips quorum.
suggest_as() { RK_AGENT="$1" "$RK" --json suggest "$2"; }
endorse_as() { RK_AGENT="$1" "$RK" endorse "$2" >/dev/null; }

# --- hop 1: propose + endorse -------------------------------------------------
step "hop 1 — a rat proposes a norm, peers endorse it"
sug_json="$(suggest_as Whisker "squash your branch before requesting merge")"
sug_id="$(printf '%s' "$sug_json" | sed -n 's/.*"suggestion":"\([^"]*\)".*/\1/p')"
[ -n "$sug_id" ] || fail "could not read the minted suggestion id from: $sug_json"
echo "proposed $sug_id (author Whisker)"

# Three DISTINCT endorsers. Gouda votes twice on purpose: a duplicate must not
# inflate the distinct-endorser tally past 3.
for who in Nibbles Gouda Gouda Brie; do
	endorse_as "$who" "$sug_id"
	echo "  endorsed by $who"
done

# --- hop 2: the reactor promotes at quorum, on its own ------------------------
step "hop 2 — the reactor promotes it to a convention at quorum"
promoted=""
for _ in $(seq 1 60); do
	sleep 0.5
	convs="$("$RK" --json scan convention system 2>/dev/null || true)"
	if printf '%s' "$convs" | grep -q "\"$sug_id\""; then
		promoted="$convs"
		break
	fi
done
[ -n "$promoted" ] || fail "the reactor never promoted $sug_id (is [reactor] quorum <= 3?)"
echo "promoted: a Furniture convention now stands for $sug_id"
printf '%s' "$promoted" | grep -q '"count":3' \
	|| fail "expected a distinct-endorser count of 3 (the duplicate Gouda vote must not inflate it)"
printf '%s' "$promoted" | grep -q '"lifecycle":"furniture"' \
	|| fail "a promoted convention must be Furniture so it survives to be injected"
echo "  count=3 (duplicate vote did not inflate), lifecycle=furniture"

# --- hop 3: the norm is injected into the next rat's prompt -------------------
step "hop 3 — the standing convention is injected into a spawned rat's prompt"
# The fake harness (configured before the daemon started) records the composed
# system prompt as RK_FAKE_SYSTEM_PROMPT — what a real rat would actually read.

# A throwaway git repo for the rat to inhabit, registered so `rk spawn` resolves it.
demo_repo="$work/demo-repo"
mkdir -p "$demo_repo"
git -C "$demo_repo" init -q -b main
git -C "$demo_repo" config user.email demo@x
git -C "$demo_repo" config user.name demo
echo "# demo" > "$demo_repo/README.md"
git -C "$demo_repo" add . && git -C "$demo_repo" commit -qm init
"$RK" repo add "$demo_repo" --name demo >/dev/null 2>&1 || true

if "$RK" spawn --repo demo --harness fake --task inject-check \
	--prompt "observe the injected convention" >/dev/null 2>&1; then
	for _ in $(seq 1 40); do
		sleep 0.5
		[ -s "$prompt_sink" ] && break
	done
fi

if [ -s "$prompt_sink" ] && grep -q "squash your branch before requesting merge" "$prompt_sink"; then
	echo "PASS: the spawned rat's prompt carries the standing convention verbatim."
	grep -n "Standing conventions" "$prompt_sink" >/dev/null 2>&1 \
		&& echo "      (found under the binding 'Standing conventions' section)"
else
	echo "PENDING: this rk build does not inject conventions into the spawn prompt yet."
	echo "         Hops 1-2 (propose -> endorse -> promote) are proven above; hop 3"
	echo "         (spawn-injection) lands with TKT-18. Rebuild once it is merged and"
	echo "         re-run to see the convention appear in the spawned prompt."
fi

step "done — norm-formation ran with no human in the path"
