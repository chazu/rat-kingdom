#!/usr/bin/env bash
# Reap regenerable build artifacts (target/) from worktrees whose owning agent
# is not live. Interim guard against the 2026-08-18 O12 incident: a probe day
# accumulated 231 GB of terminal rats' target/ dirs, tripping the daemon's
# 10 GB spawn floor and silently stalling drain + failing gates. The durable
# fix (worktree sweep reaps artifacts of terminal agents regardless of merge
# state) is ticketed; this launchd job is the belt until it lands.
#
# Safe by construction: only deletes directories named target/ (pure cargo
# build cache), only in worktrees whose agent is absent from the live set,
# and never touches git state or source files.
set -euo pipefail

AGENTS_JSON="$HOME/.rat-kingdom/agents.json"
WORKTREES="$HOME/.rat-kingdom/worktrees"

live=$(python3 - "$AGENTS_JSON" <<'EOF'
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
agents = d.get('agents', d) if isinstance(d, dict) else d
if isinstance(agents, dict):
    agents = list(agents.values())
for a in agents:
    if a.get('state') in ('running', 'spawning'):
        print(a.get('name'))
EOF
)

reaped=0
for wt in "$WORKTREES"/*/*/; do
    name=$(basename "$wt")
    if printf '%s\n' "$live" | grep -qx "$name"; then
        continue  # live agent — leave everything alone
    fi
    if [ -d "$wt/target" ]; then
        rm -rf "$wt/target"
        reaped=$((reaped + 1))
    fi
done
echo "$(date -u +%FT%TZ) reaped target/ from $reaped non-live worktrees"
