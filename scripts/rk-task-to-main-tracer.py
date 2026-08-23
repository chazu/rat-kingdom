#!/usr/bin/env python3
"""TKT-01M0P974W01XT6NJG10YMJ0ZED — live Rat Kingdom task-to-main tracer.

Reads the public operator telemetry surfaces delivered by the sibling
substrate/rendering/policy tickets (parent TKT-01M0P2KNB92EAV2QG9256MY3QV) —
`rk --json status <TKT-id>` (crates/rk-cli/src/critical_path.rs) and
`rk --json digest --since <window>` (crates/rk-cli/src/observe.rs) — and
renders a task-to-main report per correlation identity (ticket id), plus
derived findings: duplicate full-suite verification, proof reuse, rework
amplification, and evaluation against the fleet's agreed temporary target
("ordinary clean changes must not accumulate duplicate full-suite or
duplicate semantic-review time", parent ticket acceptance criteria).

READ-ONLY: every code path here is `rk status`/`rk digest`, both read-only
RPCs. Nothing is written to the tuplespace and no historical tuple is
touched — re-running against the same production castle at a later time is
safe and will only ever show additional (newer) tickets, never a changed
past one, because spans are append-only and idempotent on (task, phase,
attempt).

Distinguishing "unavailable" from "zero": a JSON `null` in `rk status`
output means the substrate did not produce that value (e.g. a phase never
happened, or a producer exists but does not thread a timestamp) — this tool
renders that as `n/a`, never as `0` or an empty duration. A `--self-test`
mode parses bundled fixtures (captured, unmodified `rk --json status`
output from real production tickets) so parsing logic has automated
coverage that does not require a live daemon; the `--live` mode is the
explicit, separately-run production evidence path.

Usage:
    # Live, against the daemon this shell is connected to:
    scripts/rk-task-to-main-tracer.py --live \\
        clean=TKT-01M0QXS8WPTMJTW3ZR0QPD44XG \\
        baseline-duplicate=TKT-01M0P974FGSEFSX2KCS93QFPTF \\
        review-rework=TKT-01M0P974MQK5XE1MR9KQCWT654

    # Offline, parsing-only, against bundled fixtures (used by --self-test):
    scripts/rk-task-to-main-tracer.py --fixture-dir scripts/fixtures/rk-task-to-main-tracer \\
        clean=postchange-clean-proof-reuse.TKT-01M0QXS8WPTMJTW3ZR0QPD44XG.json \\
        baseline-duplicate=baseline-duplicate-full-suite.TKT-01M0P974FGSEFSX2KCS93QFPTF.json

    # Parsing self-check (exit 0/1, no daemon required):
    scripts/rk-task-to-main-tracer.py --self-test
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

FIXTURE_DIR_DEFAULT = Path(__file__).parent / "fixtures" / "rk-task-to-main-tracer"

# The "verify" lane is the actual full test-suite check (see .rk/checks.cue);
# steward-protected-paths / steward-diff-scope are cheap scope gates and are
# never candidates for "duplicate full-suite" — excluding them by lane name
# avoids over-counting.
FULL_SUITE_LANE = "verify"


def run_rk_json(args):
    proc = subprocess.run(
        ["rk", "--json", *args], capture_output=True, text=True, check=False
    )
    if proc.returncode != 0:
        raise RuntimeError(f"rk {' '.join(args)} exited {proc.returncode}: {proc.stderr.strip()}")
    return json.loads(proc.stdout)


def load_status(task_id, *, live, fixture_dir, fixture_name=None):
    """Return (command_str, raw_json) for one correlation identity."""
    if live:
        command = f"rk --json status {task_id}"
        return command, run_rk_json(["status", task_id])
    path = Path(fixture_dir) / (fixture_name or task_id)
    command = f"(fixture) {path}"
    return command, json.loads(path.read_text())


def na(value):
    """Render JSON null as the explicit 'n/a' (unavailable), never 0."""
    return "n/a" if value is None else value


def summarize_phases(cp):
    """Normalize the phases[] array into per-phase rows, preserving the
    unavailable/zero distinction on every timing field."""
    rows = []
    for p in cp.get("phases", []):
        rows.append(
            {
                "phase": p.get("phase"),
                "attempt": p.get("attempt"),
                "lane": p.get("lane"),
                "queue_wait_ms": na(p.get("queue_wait_ms")),
                "duration_ms": na(p.get("duration_ms")),
                "proof_kind": p.get("proof_kind"),
                "proof_reused": p.get("proof_reused"),
                "terminal_reason": p.get("terminal_reason"),
                "authority": p.get("authority"),
            }
        )
    return rows


def find_duplicate_full_suite(cp):
    """A duplicate full-suite run is >1 *distinct proof-kind stage* of
    full-suite verification that actually executed (carries a duration) for
    the same task — e.g. an "ad-hoc" (managed, mid-task) full run followed by
    a separate "full-final" (landing-gate) full run of the same content,
    instead of the second stage reusing the first's proof. Retries within one
    stage (e.g. two failed ad-hoc attempts before a passing one) are not
    counted as duplicates of each other — only the final span of each
    proof-kind stage represents that stage's real outcome. Returns a dict
    with the number of distinct stages that executed, total duplicate ms
    (all executed stages after the first), and whether any stage carried
    proof_reused=true while STILL recording a duration (which would mean the
    substrate thinks reuse happened but ran the suite anyway — should not
    happen; flagged separately)."""
    verify_spans = [
        p
        for p in cp.get("phases", [])
        if p.get("phase") == "verification" and p.get("lane") == FULL_SUITE_LANE
    ]
    last_per_proof_kind = {}
    for p in verify_spans:
        last_per_proof_kind[p.get("proof_kind")] = p  # phases[] is chronological; last write wins
    executed = [p for p in last_per_proof_kind.values() if p.get("duration_ms") is not None]
    reused_but_executed = [p for p in executed if p.get("proof_reused") is True]
    total_ms = sum(p["duration_ms"] for p in executed)
    duplicate_ms = total_ms - max((p["duration_ms"] for p in executed), default=0) if executed else 0
    return {
        "full_suite_runs": len(executed),
        "is_duplicate": len(executed) > 1,
        "total_full_suite_ms": total_ms if executed else None,
        "duplicate_ms": duplicate_ms if executed else None,
        "reused_but_still_executed": len(reused_but_executed),
    }


def find_proof_reuse_short_circuit(cp):
    """A short-circuited would-be duplicate: a full-suite-lane verification
    span with duration_ms == null AND proof_reused == true. This is what
    "no duplicate full-suite time" looks like on the wire post-fix."""
    return [
        p
        for p in cp.get("phases", [])
        if p.get("phase") == "verification"
        and p.get("lane") == FULL_SUITE_LANE
        and p.get("duration_ms") is None
        and p.get("proof_reused") is True
    ]


def review_rework_summary(cp):
    """Semantic-review load proxy. NOTE (observed limit): semantic_review
    phase spans do not carry duration_ms in the current substrate (verified
    against production — see design note), so "duplicate semantic-review
    time" can only be evaluated via round COUNTS, not elapsed review
    duration. Rendered explicitly, not papered over."""
    return {
        "review_rounds": cp.get("review_rounds"),
        "rework_rounds": cp.get("rework_rounds"),
        "rework_amplification": cp.get("rework_amplification"),
        "semantic_review_duration_ms_available": any(
            p.get("phase") == "semantic_review" and p.get("duration_ms") is not None
            for p in cp.get("phases", [])
        ),
    }


def evaluate_target(cp):
    """Evaluate the agreed temporary target: an ordinary CLEAN change must
    not accumulate (a) duplicate full-suite verification time, or (b)
    duplicate semantic-review time. Returns PASS/FAIL/NOT-APPLICABLE per
    sub-target with the evidence used."""
    dup = find_duplicate_full_suite(cp)
    rework = review_rework_summary(cp)
    full_suite_verdict = "FAIL" if dup["is_duplicate"] else "PASS"
    if rework["rework_rounds"]:
        review_verdict = "NOT-APPLICABLE (ticket underwent rework, not an ordinary clean change)"
    else:
        review_verdict = "PASS (0 rework rounds observed; duplicate-review-time proxy is 0 by construction)"
    return {
        "duplicate_full_suite": full_suite_verdict,
        "duplicate_semantic_review": review_verdict,
        "evidence": {"duplicate_full_suite": dup, "review_rework": rework},
    }


def build_report(entries):
    """entries: list of (label, task_id, command, raw_json)"""
    report = {"paths": []}
    for label, task_id, command, cp in entries:
        report["paths"].append(
            {
                "label": label,
                "task": task_id,
                "command": command,
                "target": cp.get("target"),
                "candidate": cp.get("candidate"),
                "in_flight": cp.get("in_flight"),
                "terminal_reason": cp.get("terminal_reason"),
                "task_to_main_ms": na(cp.get("task_to_main_ms")),
                "proof_reuse": cp.get("proof_reuse"),
                "authority_time_ms": cp.get("authority_time_ms"),
                "phases": summarize_phases(cp),
                "duplicate_full_suite": find_duplicate_full_suite(cp),
                "proof_reuse_short_circuit_spans": len(find_proof_reuse_short_circuit(cp)),
                "review_rework": review_rework_summary(cp),
                "target_evaluation": evaluate_target(cp),
            }
        )
    return report


def render_human(report):
    lines = []
    for path in report["paths"]:
        lines.append(f"== {path['label']}  ({path['task']}) ==")
        lines.append(f"  command: {path['command']}")
        lines.append(
            f"  target={path['target']} candidate={path['candidate']} "
            f"in_flight={path['in_flight']} terminal={path['terminal_reason']}"
        )
        lines.append(f"  task_to_main_ms: {path['task_to_main_ms']}")
        pr = path["proof_reuse"] or {}
        lines.append(f"  proof_reuse: {pr.get('reused', 'n/a')}/{pr.get('total', 'n/a')}")
        dup = path["duplicate_full_suite"]
        lines.append(
            f"  duplicate_full_suite: runs={dup['full_suite_runs']} "
            f"is_duplicate={dup['is_duplicate']} duplicate_ms={dup['duplicate_ms']}"
        )
        rr = path["review_rework"]
        lines.append(
            f"  review_rounds={rr['review_rounds']} rework_rounds={rr['rework_rounds']} "
            f"amplification={rr['rework_amplification']} "
            f"semantic_review_duration_available={rr['semantic_review_duration_ms_available']}"
        )
        te = path["target_evaluation"]
        lines.append(f"  target[duplicate_full_suite]: {te['duplicate_full_suite']}")
        lines.append(f"  target[duplicate_semantic_review]: {te['duplicate_semantic_review']}")
        lines.append("  phases:")
        for p in path["phases"]:
            lines.append(
                f"    {p['phase']:<16} attempt={p['attempt']:<6} lane={str(p['lane']):<24} "
                f"queue_wait_ms={p['queue_wait_ms']:<8} duration_ms={p['duration_ms']:<10} "
                f"proof_kind={p['proof_kind']} proof_reused={p['proof_reused']} "
                f"authority={p['authority']} terminal={p['terminal_reason']}"
            )
        lines.append("")
    return "\n".join(lines)


SELF_TEST_PATHS = [
    ("baseline-duplicate", "TKT-01M0P974FGSEFSX2KCS93QFPTF",
     "baseline-duplicate-full-suite.TKT-01M0P974FGSEFSX2KCS93QFPTF.json"),
    ("postchange-clean", "TKT-01M0QXS8WPTMJTW3ZR0QPD44XG",
     "postchange-clean-proof-reuse.TKT-01M0QXS8WPTMJTW3ZR0QPD44XG.json"),
    ("review-rework", "TKT-01M0P974MQK5XE1MR9KQCWT654",
     "review-rework.TKT-01M0P974MQK5XE1MR9KQCWT654.json"),
]


def self_test():
    """Parsing-only assertions against bundled, unmodified production
    fixtures. Exits 1 with a message on any assertion failure."""
    failures = []

    def check(cond, msg):
        if not cond:
            failures.append(msg)

    entries = []
    for label, task_id, fixture_name in SELF_TEST_PATHS:
        command, cp = load_status(task_id, live=False, fixture_dir=FIXTURE_DIR_DEFAULT, fixture_name=fixture_name)
        entries.append((label, task_id, command, cp))
    report = build_report(entries)
    by_label = {p["label"]: p for p in report["paths"]}

    baseline = by_label["baseline-duplicate"]
    check(baseline["duplicate_full_suite"]["is_duplicate"] is True,
          "baseline-duplicate: expected duplicate full-suite verification to be detected")
    check(baseline["duplicate_full_suite"]["full_suite_runs"] == 2,
          f"baseline-duplicate: expected 2 executed full-suite runs, got {baseline['duplicate_full_suite']['full_suite_runs']}")
    check(baseline["target_evaluation"]["duplicate_full_suite"] == "FAIL",
          "baseline-duplicate: expected temporary target to read FAIL")
    check(baseline["task_to_main_ms"] == "n/a",
          "baseline-duplicate: expected task_to_main_ms to render as the explicit 'n/a' (unavailable), not 0")

    postchange = by_label["postchange-clean"]
    check(postchange["duplicate_full_suite"]["is_duplicate"] is False,
          "postchange-clean: expected no duplicate full-suite verification")
    check(postchange["proof_reuse_short_circuit_spans"] == 1,
          f"postchange-clean: expected 1 proof-reuse short-circuit span, got {postchange['proof_reuse_short_circuit_spans']}")
    check(postchange["target_evaluation"]["duplicate_full_suite"] == "PASS",
          "postchange-clean: expected temporary target to read PASS")

    rework = by_label["review-rework"]
    check(rework["review_rework"]["review_rounds"] == 1,
          "review-rework: expected 1 review round")
    check(rework["review_rework"]["rework_rounds"] == 1,
          "review-rework: expected 1 rework round")
    check(rework["review_rework"]["rework_amplification"] == 1.0,
          "review-rework: expected rework_amplification == 1.0")
    check(rework["review_rework"]["semantic_review_duration_ms_available"] is False,
          "review-rework: expected semantic_review duration to be unavailable in the current substrate "
          "(this assertion documents a known gap — see design note; it should start failing, in a good way, "
          "the day a producer starts threading semantic_review duration_ms)")
    check(rework["target_evaluation"]["duplicate_semantic_review"].startswith("NOT-APPLICABLE"),
          "review-rework: reworked ticket should not be scored against the clean-path target")

    if failures:
        print(f"SELF-TEST FAILED ({len(failures)} assertion(s)):", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        return 1
    print(f"SELF-TEST PASSED ({len(SELF_TEST_PATHS)} fixture paths, parsing-only, no live daemon required)")
    return 0


def parse_path_args(raw):
    out = []
    for item in raw:
        if "=" not in item:
            raise SystemExit(f"path argument must be label=TKT-id, got: {item!r}")
        label, task_id = item.split("=", 1)
        out.append((label, task_id))
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--live", action="store_true", help="query the connected daemon via `rk --json status`")
    parser.add_argument("--fixture-dir", default=str(FIXTURE_DIR_DEFAULT),
                         help="directory of captured `rk --json status` output, used when --live is not set")
    parser.add_argument("--self-test", action="store_true",
                         help="run parsing assertions against bundled fixtures and exit (no daemon required)")
    parser.add_argument("--json", action="store_true", help="emit the full JSON report instead of the human table")
    parser.add_argument("paths", nargs="*",
                         help="label=TKT-id (--live) or label=fixture-filename (fixture mode)")
    args = parser.parse_args(argv)

    if args.self_test:
        return self_test()

    if not args.paths:
        parser.error("at least one label=TKT-id (or label=fixture-filename) is required unless --self-test is given")

    entries = []
    for label, ref in parse_path_args(args.paths):
        if args.live:
            command, cp = load_status(ref, live=True, fixture_dir=None)
            task_id = ref
        else:
            command, cp = load_status(ref, live=False, fixture_dir=args.fixture_dir, fixture_name=ref)
            task_id = cp.get("task", ref)
        entries.append((label, task_id, command, cp))

    report = build_report(entries)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(render_human(report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
