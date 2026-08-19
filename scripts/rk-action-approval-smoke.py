#!/usr/bin/env python3
"""TKT-01M08H9QQPJGFS9ET25Q26YSDM (A3) — action-approval boundary smoke check.

A launchd shim that drives the factory action-approval boundary
(`crates/rk-daemon/src/action_approval.rs`) through its full
propose -> digest -> approve -> daemon-execute (CAS) cycle, weekly, against
the harmless `action-approval-smoke-target` workflow
(examples/workflows/action-approval-smoke-target.cue /
.rk/workflows/action-approval-smoke-target.cue). This is the freeze's only
"keep exercised" obligation on the factory-foreman subsystem
(docs/2026-08-17-rk-ticket-program.md, A3) — an unexercised security gate
rots (TKT-171 pattern).

WHY THIS IS AN EXTERNAL SCRIPT, NOT AN `rk workflow` SCHEDULE ENTRY:
`factory.propose_action` / `factory.approve_action` / `factory.execute_action`
are operator-only RPCs (crates/rk-daemon/src/server.rs, the
`req.method.as_str()` allow-list gate). A supervised rat cannot call them even
after clearing its identity env — the daemon binds authorization to the
kernel-observed calling process, not just the bearer token
(crates/rk-cli/tests/agent_cannot_self_elevate.rs proves this). A cue
`workflow: {...}` fired by the scheduler always executes its steps as a
spawned rat, so it structurally cannot drive this boundary. Only a process
that is NOT a supervised agent — a human shell, or (here) a launchd/cron job
running as the operator's own user, same pattern as
scripts/rk-ci-poller.py (C4) — can act as operator.

RED PATH: on any failure (an RPC error, a non-"completed" terminal instance
status, or a poll timeout), this script files an escalation ticket
(`rk ticket new`) describing the failure, so it surfaces in `rk inbox` /
`rk ticket list` durably rather than only in a launchd log. Escalation is
throttled by a local state file (ESCALATION_QUIET_WINDOW) so a persistent
outage files one ticket, not one per weekly tick.

=========================== OPERATOR SETUP ================================

1. Verify without mutating anything:
     python3 scripts/rk-action-approval-smoke.py --dry-run

2. Run it for real once, by hand, before scheduling it:
     python3 scripts/rk-action-approval-smoke.py

3. Install the launchd job (weekly, survives daemon and laptop restarts
   because launchd — not this script — owns the run loop):
     cp scripts/com.rat-kingdom.action-approval-smoke.plist.example \\
        ~/Library/LaunchAgents/com.rat-kingdom.action-approval-smoke.plist
     # edit the paths inside (rat-kingdom checkout, python3, rk bin dir)
     launchctl load ~/Library/LaunchAgents/com.rat-kingdom.action-approval-smoke.plist

Usage:
    rk-action-approval-smoke.py [--dry-run] [options]
"""

import argparse
import json
import os
import subprocess
import sys
import time

DEFAULT_RK_REPO = "rat-kingdom"
DEFAULT_WORKFLOW = "action-approval-smoke-target"
DEFAULT_STATE_FILE = os.path.expanduser(
    "~/.rat-kingdom/action-approval-smoke-state.json"
)
DEFAULT_POLL_INTERVAL_SECS = 10
DEFAULT_POLL_TIMEOUT_SECS = 600
# Don't refile an escalation ticket more than once per this window — a
# persistent outage should produce one ticket, not one per weekly tick.
ESCALATION_QUIET_WINDOW_SECS = 6 * 24 * 3600

TICKET_LABEL = "action-approval-smoke"


class SmokeCheckError(Exception):
    """A step in the propose/approve/execute chain failed."""


def load_state(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(path, state):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, path)


def run_rk(rk_bin, args, dry_run, dry_run_result=None):
    """Run `rk --json <args>`, returning the parsed JSON stdout.

    Raises SmokeCheckError on a non-zero exit or unparsable stdout.
    """
    if dry_run:
        print("DRY-RUN would run:", rk_bin, "--json", " ".join(args))
        return dry_run_result if dry_run_result is not None else {}
    result = subprocess.run(
        [rk_bin, "--json", *args], capture_output=True, text=True
    )
    if result.returncode != 0:
        raise SmokeCheckError(
            f"`{rk_bin} --json {' '.join(args)}` exited {result.returncode}: "
            f"{result.stderr.strip()}"
        )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise SmokeCheckError(
            f"`{rk_bin} --json {' '.join(args)}` produced unparsable JSON: {e}"
        ) from e


def poll_instance_completed(rk_bin, instance_id, initial_status, interval, timeout, dry_run):
    """Poll `rk workflow status` until the instance reaches a terminal state.

    Returns the terminal status string ("completed" or "failed").
    """
    if dry_run:
        return "completed"
    status = initial_status
    deadline = time.monotonic() + timeout
    while status == "running":
        if time.monotonic() >= deadline:
            raise SmokeCheckError(
                f"instance {instance_id} still running after {timeout}s"
            )
        time.sleep(interval)
        result = run_rk(rk_bin, ["workflow", "status", instance_id], dry_run=False)
        status = result.get("status")
        if status is None:
            raise SmokeCheckError(
                f"workflow status for {instance_id} carried no 'status' field: {result}"
            )
    return status


def run_smoke_check(args):
    coordinator = f"action-approval-smoke-{int(time.time())}"

    proposal = run_rk(
        args.rk_bin,
        [
            "factory", "propose-workflow", args.workflow,
            "--repo", args.repo,
            "--coordinator", coordinator,
        ],
        args.dry_run,
        dry_run_result={
            "proposal_id": "dry-run-proposal",
            "digest": "0" * 64,
            "proposal": {"id": "dry-run-proposal"},
        },
    )
    proposal_id = proposal.get("proposal_id") or proposal.get("proposal", {}).get("id")
    digest = proposal.get("digest")
    if not proposal_id or not digest:
        raise SmokeCheckError(f"propose-workflow response missing proposal id/digest: {proposal}")

    approval = run_rk(
        args.rk_bin,
        ["factory", "approve", proposal_id, digest],
        args.dry_run,
        dry_run_result={"approval": {"status": "approved"}},
    )
    approval_status = approval.get("approval", {}).get("status")
    if approval_status != "approved":
        raise SmokeCheckError(f"approve did not return status=approved: {approval}")

    executed = run_rk(
        args.rk_bin,
        [
            "factory", "execute-workflow", proposal_id, digest,
            "--workflow", args.workflow,
            "--repo", args.repo,
            "--coordinator", coordinator,
        ],
        args.dry_run,
        dry_run_result={
            "instance": {"id": "dry-run-instance", "status": "completed"},
            "approval": {"status": "consumed"},
        },
    )
    execution_status = executed.get("approval", {}).get("status")
    if execution_status != "consumed":
        raise SmokeCheckError(f"execute-workflow did not consume the approval: {executed}")
    instance = executed.get("instance", {})
    instance_id = instance.get("id")
    if not instance_id:
        raise SmokeCheckError(f"execute-workflow response carried no instance id: {executed}")

    terminal_status = poll_instance_completed(
        args.rk_bin,
        instance_id,
        instance.get("status"),
        args.poll_interval,
        args.poll_timeout,
        args.dry_run,
    )
    if terminal_status != "completed":
        raise SmokeCheckError(
            f"instance {instance_id} finished with status={terminal_status}, expected completed"
        )
    return proposal_id, instance_id


def escalate(rk_bin, repo, reason, state, state_file, dry_run):
    now = time.time()
    last = state.get("last_escalated_at", 0)
    if now - last < ESCALATION_QUIET_WINDOW_SECS:
        print(
            f"action-approval smoke check FAILED (escalation suppressed, last "
            f"filed {int(now - last)}s ago): {reason}",
            file=sys.stderr,
        )
        return
    print(f"action-approval smoke check FAILED, escalating: {reason}", file=sys.stderr)
    title = "action-approval boundary smoke check failed"
    body = (
        "The weekly action-approval smoke check (TKT-01M08H9QQPJGFS9ET25Q26YSDM) "
        "could not drive propose -> approve -> execute against the no-op "
        f"{DEFAULT_WORKFLOW} action.\n\n"
        f"Reason: {reason}\n\n"
        "This is the only 'keep exercised' obligation on the frozen "
        "factory-foreman subsystem (docs/2026-08-17-rk-ticket-program.md, A3). "
        "Investigate crates/rk-daemon/src/action_approval.rs and the factory.* "
        "RPC handlers in crates/rk-daemon/src/server.rs before assuming this "
        "is transient."
    )
    if dry_run:
        print(f"DRY-RUN would file escalation ticket: {title}")
    else:
        result = subprocess.run(
            [
                rk_bin, "--json", "ticket", "new", title,
                "--body", body,
                "--repo", repo,
                "--label", TICKET_LABEL,
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            print(f"escalation ticket filing ALSO failed: {result.stderr.strip()}", file=sys.stderr)
        else:
            print(f"filed escalation ticket: {result.stdout.strip()}")
    state["last_escalated_at"] = now
    save_state(state_file, state)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--repo", default=DEFAULT_RK_REPO, help="registered rk repo the smoke action runs against")
    parser.add_argument("--workflow", default=DEFAULT_WORKFLOW, help="the no-op workflow.run action payload")
    parser.add_argument("--rk-bin", default="rk")
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--poll-interval", type=int, default=DEFAULT_POLL_INTERVAL_SECS)
    parser.add_argument("--poll-timeout", type=int, default=DEFAULT_POLL_TIMEOUT_SECS)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="exercise the script's control flow without calling a live daemon",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    state = load_state(args.state_file)

    try:
        proposal_id, instance_id = run_smoke_check(args)
    except SmokeCheckError as e:
        escalate(args.rk_bin, args.repo, str(e), state, args.state_file, args.dry_run)
        return 1

    print(f"action-approval smoke check OK: proposal={proposal_id} instance={instance_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
