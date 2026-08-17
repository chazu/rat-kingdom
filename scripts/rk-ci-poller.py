#!/usr/bin/env python3
"""TKT-01M08HB56NRQ72ZZ4W6JKQ760Y (C4) — minimal ingest bridge (R1).

A launchd shim that polls the GitHub Actions API for `ci.yml` run
conclusions on rat-kingdom's own repo and translates them into
`rk ingest event --kind ci_failed|ci_recovered` calls. It is a POLLER, not a
webhook listener: the laptop-hosted daemon has no public endpoint. This is
the only registered repo with a remote (docs/2026-08-17-rk-ticket-program.md,
ticket C4).

Dedup and transition semantics are NOT reimplemented here — `rk ingest
event` already derives `(source, delivery_id)` receipts and a
semantic-state-digest transition check server-side
(crates/rk-space/src/store.rs `accept_sdlc_signal`). This script's only
local state is a "seen delivery ids" cursor (`run_id-run_attempt`), kept
purely to avoid re-invoking `rk` for runs it has already reported;
correctness does not depend on it — resubmitting an already-seen delivery id
is a harmless no-op at the daemon. Keying on the full delivery id, not just
`run_id`, matters: GitHub's "re-run failed jobs" keeps the same `run_id` but
increments `run_attempt`, so a run_id-only cursor would silently swallow a
genuine new attempt.

Job-level granularity: the workflow-runs list API is run-level, not
job-level. `--job` is filled with the workflow's display name rather than a
real per-job identifier — good enough for a minimal bridge; a future
iteration could call the per-run jobs endpoint for finer correlation.

=========================== OPERATOR SETUP ================================

1. Mint a read-only GitHub fine-grained personal access token, scoped to:
     - Repository: chazu/rat-kingdom only (no other repos)
     - Permissions: "Actions" = Read-only (this also requires "Metadata" =
       Read-only, which GitHub sets automatically)
   No write scopes. This agent cannot mint the token — an operator must.

2. Store it somewhere only the operator's account can read, e.g.:
     install -m 600 /dev/stdin ~/.rat-kingdom/secrets/github-ci-token <<< "$TOKEN"
   Point the shim at it with --token-file, or export
   RK_CI_POLLER_GITHUB_TOKEN instead (env var name configurable via
   --token-env).

3. Register the ingest source in the daemon's config
   (~/.rat-kingdom/config.toml):
     [[ingest.sources]]
     name = "github-ci"
     allowed_kinds = ["ci_failed", "ci_recovered"]
   Config is read at daemon startup, so restart the daemon afterwards
   (`mise run deploy`, or `rk daemon stop` and let it respawn) for the new
   source to take effect.

4. Install the launchd job (~2min cadence via StartInterval, survives
   daemon and laptop restarts because launchd — not this script — owns the
   run loop):
     cp scripts/com.rat-kingdom.ci-poller.plist.example \
        ~/Library/LaunchAgents/com.rat-kingdom.ci-poller.plist
     # edit the paths inside (rk-kingdom checkout, python3, token file, rk bin dir)
     launchctl load ~/Library/LaunchAgents/com.rat-kingdom.ci-poller.plist

5. Verify without live credentials first:
     python3 scripts/rk-ci-poller.py --dry-run \
       --fixture scripts/fixtures/rk-ci-poller/run-failure.json \
       --state-file /tmp/ci-poller-test-state.json
   Run it a second time with the same fixture and state file — the AC
   "re-poll of the same run/attempt is a no-op" should show `emitted 0` on
   the second run.

Usage:
    rk-ci-poller.py [--dry-run --fixture PATH] [options]
"""

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

API_VERSION = "2022-11-28"
DEFAULT_GITHUB_REPO = "chazu/rat-kingdom"
DEFAULT_WORKFLOW = "ci.yml"
DEFAULT_BRANCH = "main"
DEFAULT_SOURCE = "github-ci"
DEFAULT_RK_REPO = "rat-kingdom"
DEFAULT_STATE_FILE = os.path.expanduser("~/.rat-kingdom/ci-poller-state.json")
DEFAULT_TOKEN_ENV = "RK_CI_POLLER_GITHUB_TOKEN"

# GitHub run `conclusion` values that represent a terminal, reportable
# outcome. Everything else (cancelled, skipped, neutral, stale,
# action_required) is left unreported — those aren't a CI health signal.
CONCLUSION_TO_KIND = {
    "success": "ci_recovered",
    "failure": "ci_failed",
    "timed_out": "ci_failed",
    "startup_failure": "ci_failed",
}


def load_state(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"seen_delivery_ids": []}


def save_state(path, state):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, path)


def read_token(args):
    if args.token_file:
        with open(args.token_file) as f:
            return f.read().strip()
    value = os.environ.get(args.token_env)
    if value:
        return value.strip()
    raise SystemExit(
        f"no GitHub token: pass --token-file or set {args.token_env} (see OPERATOR SETUP in this script's docstring)"
    )


def fetch_runs(github_repo, workflow, branch, token, per_page):
    query = urllib.parse.urlencode(
        {"branch": branch, "status": "completed", "per_page": per_page}
    )
    url = f"https://api.github.com/repos/{github_repo}/actions/workflows/{workflow}/runs?{query}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": API_VERSION,
            "User-Agent": "rk-ci-poller",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def load_fixture(path):
    with open(path) as f:
        return json.load(f)


def delivery_id_for(run):
    return f"{run['id']}-{run.get('run_attempt', 1)}"


def build_ingest_args(source, rk_repo, delivery_id, run):
    conclusion = run.get("conclusion") or ""
    kind = CONCLUSION_TO_KIND.get(conclusion)
    if kind is None:
        return None
    workflow_name = run.get("name") or DEFAULT_WORKFLOW
    branch = run.get("head_branch") or ""
    sha = run.get("head_sha") or ""
    summary = f"{workflow_name} {conclusion} on {branch} ({sha[:12]})"
    return [
        "ingest", "event",
        "--source", source,
        "--kind", kind,
        "--delivery-id", delivery_id,
        "--summary", summary,
        "--repo", rk_repo,
        "--branch", branch,
        "--workflow", workflow_name,
        "--job", workflow_name,
        "--commit-sha", sha,
        "--attr", f"run_id={run['id']}",
        "--attr", f"html_url={run.get('html_url', '')}",
    ]


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--github-repo", default=DEFAULT_GITHUB_REPO, help="owner/repo slug polled via the GitHub API")
    parser.add_argument("--workflow", default=DEFAULT_WORKFLOW, help="workflow file name, e.g. ci.yml")
    parser.add_argument("--branch", default=DEFAULT_BRANCH)
    parser.add_argument("--source", default=DEFAULT_SOURCE, help="rk ingest source name, must be configured in ingest.sources")
    parser.add_argument("--repo", default=DEFAULT_RK_REPO, help="the RK-registered repo name to stamp on Correlation.repo")
    parser.add_argument("--token-file")
    parser.add_argument("--token-env", default=DEFAULT_TOKEN_ENV)
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--rk-bin", default="rk")
    parser.add_argument("--per-page", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true", help="read --fixture instead of the live API, and print rk ingest commands instead of running them")
    parser.add_argument("--fixture", help="path to a recorded GitHub 'list workflow runs' API response; required with --dry-run")
    args = parser.parse_args(argv)
    if args.dry_run and not args.fixture:
        parser.error("--dry-run requires --fixture")
    return args


def main(argv=None):
    args = parse_args(argv)

    if args.dry_run:
        payload = load_fixture(args.fixture)
    else:
        token = read_token(args)
        try:
            payload = fetch_runs(args.github_repo, args.workflow, args.branch, token, args.per_page)
        except urllib.error.HTTPError as e:
            print(f"GitHub API error {e.code}: {e.read().decode('utf-8', 'replace')}", file=sys.stderr)
            return 1
        except urllib.error.URLError as e:
            print(f"GitHub API unreachable: {e.reason}", file=sys.stderr)
            return 1

    runs = payload.get("workflow_runs", [])
    runs.sort(key=lambda r: (r.get("id", 0), r.get("run_attempt", 1)))

    state = load_state(args.state_file)
    seen = set(state.get("seen_delivery_ids", []))

    emitted = 0
    for run in runs:
        if "id" not in run:
            continue
        delivery_id = delivery_id_for(run)
        if delivery_id in seen:
            continue
        ingest_args = build_ingest_args(args.source, args.repo, delivery_id, run)
        if ingest_args is None:
            seen.add(delivery_id)
            continue
        if args.dry_run:
            print("DRY-RUN would run:", args.rk_bin, " ".join(ingest_args))
        else:
            result = subprocess.run([args.rk_bin, *ingest_args], capture_output=True, text=True)
            if result.returncode != 0:
                print(f"rk ingest event failed for delivery {delivery_id}: {result.stderr.strip()}", file=sys.stderr)
                continue
            print(f"submitted {delivery_id}: {result.stdout.strip()}")
        seen.add(delivery_id)
        emitted += 1

    state["seen_delivery_ids"] = sorted(seen)[-500:]
    save_state(args.state_file, state)
    print(f"processed {len(runs)} run(s), emitted {emitted}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
