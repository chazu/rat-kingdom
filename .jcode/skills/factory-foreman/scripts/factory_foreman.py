"""Deterministic, read-only factory snapshot collection."""

from __future__ import annotations

import argparse
import hashlib
import json
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


SCHEMA = "factory-foreman.snapshot.v1"
GENERATED_AT = "1970-01-01T00:00:00Z"
PREFLIGHT_COMMAND = "daemon"
COMMANDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("agents", ("rk", "--json", "list")),
    ("inbox", ("rk", "--json", "inbox")),
    ("workflows", ("rk", "--json", "workflow", "list")),
    ("cost", ("rk", "--json", "cost", "--fleet")),
    ("repository", ("rk", "--json", "repo", "show", "{repo}")),
    ("definitions", ("rk", "--json", "workflow", "defs", "--repo", "{repo_path}")),
    ("tickets", ("rk", "--json", "ticket", "list", "--repo", "{repo}")),
)
OPTIONAL_FACTORY_DISPLAY_COMMANDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "factory_scorecards",
        (
            "rk",
            "--json",
            "factory",
            "scorecards",
            "--repo",
            "{repo}",
            "--group-by",
            "all",
            "--include-archived",
        ),
    ),
    (
        "factory_recommend",
        (
            "rk",
            "--json",
            "factory",
            "recommend",
            "--repo",
            "{repo}",
            "--group-by",
            "all",
            "--include-archived",
        ),
    ),
)
STATUS_ARGV = ("rk", "--json", "daemon", "status")
BUDGET_PRESSURE_RATIO = 0.8
LOW_SAMPLE_THRESHOLD = 3
SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


class Runner(Protocol):
    def run(self, argv: tuple[str, ...]) -> CommandResult:
        ...


class SubprocessRunner:
    def run(self, argv: tuple[str, ...]) -> CommandResult:
        try:
            completed = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                timeout=30,
            )
        except subprocess.TimeoutExpired as error:
            return CommandResult(124, error.stdout or "", "timeout")
        return CommandResult(completed.returncode, completed.stdout, completed.stderr)


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError(message)


@dataclass(frozen=True)
class Observation:
    ok: bool
    command: str
    data: Any | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        if self.ok:
            return {"ok": True, "command": self.command, "data": self.data}
        return {"ok": False, "command": self.command, "error": self.error}


@dataclass(frozen=True)
class FactorySnapshot:
    schema: str
    generated_at: str
    repo: str
    healthy: bool
    observations: dict[str, Observation]
    errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "generated_at": self.generated_at,
            "repo": self.repo,
            "healthy": self.healthy,
            "observations": {
                name: observation.to_dict()
                for name, observation in self.observations.items()
            },
            "errors": self.errors,
        }


@dataclass(frozen=True)
class Finding:
    category: str
    severity: str
    subject: str
    summary: str
    evidence: str
    recommended_next_step: str
    workflow_instance: str | None = None
    agent: str | None = None


@dataclass(frozen=True)
class TriageReport:
    findings: list[Finding]

    def to_dict(self) -> dict[str, Any]:
        return {
            "findings": [
                {
                    "category": finding.category,
                    "severity": finding.severity,
                    "subject": finding.subject,
                    "summary": finding.summary,
                    "evidence": finding.evidence,
                    "recommended_next_step": finding.recommended_next_step,
                    "workflow_instance": finding.workflow_instance,
                    "agent": finding.agent,
                }
                for finding in self.findings
            ]
        }


def daemon_preflight(runner: Runner) -> Observation:
    return _observe(PREFLIGHT_COMMAND, STATUS_ARGV, runner)


def collect_snapshot(
    repo: str, runner: Runner, *, include_factory_display: bool = False
) -> FactorySnapshot:
    observations: dict[str, Observation] = {}
    preflight = daemon_preflight(runner)

    if not preflight.ok:
        observations[PREFLIGHT_COMMAND] = preflight
        return _snapshot(repo, observations)

    repo_path = repo
    for command, argv_template in COMMANDS:
        if command == "definitions":
            repository = observations.get("repository")
            if repository and repository.ok and isinstance(repository.data, dict):
                path = repository.data.get("path")
                if isinstance(path, str) and path.strip():
                    repo_path = path
        argv = tuple(
            repo if part == "{repo}" else repo_path if part == "{repo_path}" else part
            for part in argv_template
        )
        observations[command] = _observe(command, argv, runner)

    if include_factory_display:
        for command, argv_template in OPTIONAL_FACTORY_DISPLAY_COMMANDS:
            argv = tuple(repo if part == "{repo}" else part for part in argv_template)
            observations[command] = _observe(command, argv, runner)

    return _snapshot(repo, observations)


def classify_snapshot(snapshot: FactorySnapshot) -> TriageReport:
    findings: list[Finding] = []

    for source, row in _snapshot_rows(snapshot):
        findings.extend(_classify_row(source, row))

    deduplicated: dict[tuple[str, str, str | None], Finding] = {}
    for finding in findings:
        key = (finding.category, finding.subject, finding.workflow_instance)
        deduplicated.setdefault(key, finding)

    return TriageReport(
        sorted(
            deduplicated.values(),
            key=lambda finding: (
                SEVERITY_ORDER.get(finding.severity, 99),
                finding.category,
                finding.subject,
                finding.workflow_instance or "",
            ),
        )
    )


def _snapshot(repo: str, observations: dict[str, Observation]) -> FactorySnapshot:
    errors = [
        f"{name}: {observation.error}"
        for name, observation in observations.items()
        if not observation.ok
    ]
    return FactorySnapshot(
        schema=SCHEMA,
        generated_at=GENERATED_AT,
        repo=repo,
        healthy=not errors,
        observations=observations,
        errors=errors,
    )


def _snapshot_rows(snapshot: FactorySnapshot) -> list[tuple[str, dict[str, Any]]]:
    rows: list[tuple[str, dict[str, Any]]] = []
    instance_spend = _instance_spend(snapshot.observations.get("cost"))
    for name, observation in snapshot.observations.items():
        if not observation.ok:
            rows.append((name, {"kind": "observation-failed", "detail": observation.error}))
            continue

        data = observation.data
        if isinstance(data, list) and name in {"agents", "inbox", "workflows"}:
            for row in data:
                if isinstance(row, dict):
                    rows.append((name, _enrich_live_row(name, row, instance_spend)))
            continue
        if isinstance(data, dict):
            for key in ("workflows", "messages", "inbox", "items", "agents"):
                values = data.get(key)
                if isinstance(values, list):
                    rows.extend(
                        (name, _enrich_live_row(name, row, instance_spend))
                        for row in values
                        if isinstance(row, dict)
                    )
                    break
    for error in snapshot.errors:
        rows.append(("snapshot", {"kind": "snapshot-error", "detail": error}))
    return rows


def _instance_spend(observation: Observation | None) -> dict[str, float]:
    if observation is None or not observation.ok or not isinstance(observation.data, dict):
        return {}
    instances = observation.data.get("instances")
    if not isinstance(instances, list):
        return {}
    spend: dict[str, float] = {}
    for instance in instances:
        if not isinstance(instance, dict):
            continue
        instance_id = _first_string(instance, "instance", "id", "workflow_instance")
        spent = _first_number(instance, "spent_usd")
        if instance_id and spent is not None:
            spend[instance_id] = spent
    return spend


def _enrich_live_row(
    source: str, row: dict[str, Any], instance_spend: dict[str, float]
) -> dict[str, Any]:
    enriched = dict(row)
    if source == "inbox" and enriched.get("kind") == "agent-orphaned":
        subject = _first_string(enriched, "subject")
        if subject and not _first_string(
            enriched, "agent", "agent_name", "agentName", "owner"
        ):
            enriched["agent"] = subject
    workflow_instance = _first_string(
        enriched,
        "workflow_instance",
        "instance_id",
        "workflowInstance",
        "instance",
        "id",
    )
    if (
        source == "workflows"
        and workflow_instance in instance_spend
        and "spent_usd" not in enriched
    ):
        enriched["spent_usd"] = instance_spend[workflow_instance]
    return enriched


def _classify_row(source: str, row: dict[str, Any]) -> list[Finding]:
    findings: list[Finding] = []
    detail = _evidence(row)
    lower = detail.lower()
    workflow_instance = _first_string(
        row,
        "workflow_instance",
        "instance_id",
        "workflowInstance",
        "instance",
        "id",
    )
    agent = _first_string(row, "agent", "agent_name", "agentName", "owner")

    if row.get("kind") == "agent-orphaned":
        findings.append(
            _finding(
                "orphaned-agent",
                "high",
                agent or workflow_instance or source,
                "Agent is no longer attached to an active workflow.",
                detail,
                "Reconcile the orphaned agent with the workflow registry before reassigning work.",
                workflow_instance,
                agent,
            )
        )

    budget_finding = _budget_pressure(row, detail, workflow_instance, agent, source)
    if budget_finding is not None:
        findings.append(budget_finding)

    if _is_timeout(row, lower):
        findings.append(
            _finding(
                "workflow-timeout",
                "high",
                workflow_instance or source,
                "Workflow exceeded its time limit.",
                detail,
                "Inspect the timed-out step and rerun with a narrower scope or longer timeout.",
                workflow_instance,
                agent,
            )
        )
        return findings

    if _is_empty_harness_result(row):
        findings.append(
            _finding(
                "empty-harness-result",
                "high",
                workflow_instance or source,
                "Failed workflow produced an undeclared empty harness result with zero usage.",
                detail,
                "Treat the worker result as non-actionable and rerun or inspect worker startup logs.",
                workflow_instance,
                agent,
            )
        )
        return findings

    if "rk: command not found" in lower:
        findings.append(
            _finding(
                "missing-rk-executable",
                "critical",
                workflow_instance or source,
                "The rk executable was not available in the worker environment.",
                detail,
                "Install rk or fix PATH before rerunning the workflow.",
                workflow_instance,
                agent,
            )
        )
        return findings

    check = _failed_named_check(row)
    if check is not None:
        findings.append(
            _finding(
                "named-check-failure",
                "medium",
                check,
                f"Repository check failed: {check}.",
                detail,
                "Open the named check output and fix the first failing assertion or command.",
                workflow_instance,
                agent,
            )
        )
        return findings

    text_category = _classify_text(lower) if _is_failure(row) else None
    if text_category is not None:
        category, severity, summary, next_step = text_category
        findings.append(
            _finding(
                category,
                severity,
                workflow_instance or source,
                summary,
                detail,
                next_step,
                workflow_instance,
                agent,
            )
        )
        return findings

    if _is_failure(row):
        findings.append(
            _finding(
                "unknown",
                "low",
                workflow_instance or source,
                "Workflow failed without matching a known deterministic classifier.",
                detail,
                "Preserve this evidence and add a classifier once the failure pattern is understood.",
                workflow_instance,
                agent,
            )
        )

    return findings


def _budget_pressure(
    row: dict[str, Any],
    detail: str,
    workflow_instance: str | None,
    agent: str | None,
    source: str,
) -> Finding | None:
    spend = _first_number(
        row, "spend_usd", "spent_usd", "cost_usd", "usage_usd", "current_spend_usd"
    )
    maximum = _first_number(
        row,
        "instance_max_usd",
        "workflow_instance_max_usd",
        "instance_budget_usd",
        "workflow_instance_budget_usd",
    )
    if spend is None or maximum is None or maximum <= 0:
        return None
    if spend / maximum < BUDGET_PRESSURE_RATIO:
        return None
    return _finding(
        "budget-pressure",
        "medium",
        workflow_instance or source,
        "Workflow instance has spent at least 80 percent of its own maximum budget.",
        detail,
        "Reduce scope, raise the instance budget, or stop the workflow before continuing.",
        workflow_instance,
        agent,
    )


def _classify_text(lower: str) -> tuple[str, str, str, str] | None:
    if any(token in lower for token in ("permission denied", "unauthorized", "forbidden")):
        return (
            "permission-or-authority",
            "high",
            "Worker lacks permission or authority for the requested action.",
            "Grant the required permission or change the task to an allowed action.",
        )
    if any(token in lower for token in ("stale base", "base moved", "branch moved", "non-fast-forward")):
        return (
            "stale-or-moved-base",
            "medium",
            "Workflow appears to be based on stale or moved repository state.",
            "Refresh from the current base and rerun the affected workflow.",
        )
    return None


def _is_timeout(row: dict[str, Any], lower: str) -> bool:
    if row.get("timed_out") is True:
        return True
    if _exit_code(row) == 124:
        return True
    for step in _steps(row):
        if step.get("timed_out") is True or _exit_code(step) == 124:
            return True
    return "timed out" in lower


def _is_empty_harness_result(row: dict[str, Any]) -> bool:
    harness = row.get("harness_result") or row.get("harnessResult")
    context = row.get("context")
    if not isinstance(harness, dict) and isinstance(context, dict):
        harness = context.get("previous_result")
    if not isinstance(harness, dict):
        return False
    usage = harness.get("usage") if isinstance(harness.get("usage"), dict) else {}
    cost = _first_number(harness, "cost_usd", "spent_usd", "usage_usd")
    return (
        _is_failure(row)
        and harness.get("declared_done") is False
        and not str(harness.get("result") or "").strip()
        and not str(harness.get("error") or "").strip()
        and all(_number(value) == 0 for value in usage.values())
        and (cost is None or cost == 0)
    )


def _failed_named_check(row: dict[str, Any]) -> str | None:
    for step in _steps(row):
        exit_code = _exit_code(step)
        name = _first_string(step, "name", "check", "command")
        if name and exit_code is not None and exit_code != 0:
            return name
    name = _first_string(row, "check", "check_name", "step", "command")
    exit_code = _exit_code(row)
    if name and exit_code is not None and exit_code != 0:
        return name
    return None


def _steps(row: dict[str, Any]) -> list[dict[str, Any]]:
    steps = row.get("run_steps") or row.get("steps") or row.get("checks") or []
    if not isinstance(steps, list):
        return []
    return [step for step in steps if isinstance(step, dict)]


def _is_failure(row: dict[str, Any]) -> bool:
    status = str(row.get("status") or "").lower()
    kind = str(row.get("kind") or "").lower()
    return (
        "failed" in status
        or "failure" in status
        or "failed" in kind
        or "failure" in kind
        or kind in {"observation-failed", "snapshot-error"}
    )


def _finding(
    category: str,
    severity: str,
    subject: str,
    summary: str,
    evidence: str,
    recommended_next_step: str,
    workflow_instance: str | None,
    agent: str | None,
) -> Finding:
    return Finding(
        category,
        severity,
        subject,
        summary,
        evidence,
        recommended_next_step,
        workflow_instance,
        agent,
    )


def _evidence(row: dict[str, Any]) -> str:
    try:
        return json.dumps(row, sort_keys=True, default=str)[:12_000]
    except TypeError:
        return str(row)[:12_000]


def _first_string(row: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return None


def _first_number(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _number(row.get(key))
        if value is not None:
            return value
    return None


def _exit_code(row: dict[str, Any]) -> int | None:
    value = _first_number(row, "exit_code", "returncode", "return_code", "status_code")
    if value is None:
        return None
    return int(value)


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _observe(command: str, argv: tuple[str, ...], runner: Runner) -> Observation:
    result = runner.run(argv)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
        return Observation(False, command, error=f"exit {result.returncode}: {detail}")

    if not result.stdout.strip():
        return Observation(False, command, error="empty stdout")

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        return Observation(False, command, error=f"invalid JSON: {error.msg}")

    return Observation(True, command, data=data)


def main(argv: list[str] | None = None, runner: Runner | None = None) -> CommandResult:
    runner = runner or SubprocessRunner()
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
        if args.command == "snapshot":
            snapshot = collect_snapshot(
                args.repo, runner, include_factory_display=args.include_factory_display
            )
            stdout = _render_snapshot(snapshot, args.format)
            return CommandResult(_snapshot_exit_code(snapshot), stdout, "")
        if args.command == "triage":
            snapshot = collect_snapshot(
                args.repo, runner, include_factory_display=args.include_factory_display
            )
            triage = classify_snapshot(snapshot)
            stdout = _render_triage(snapshot, triage, args.format)
            return CommandResult(_snapshot_exit_code(snapshot), stdout, "")
        if args.command == "propose-workflow":
            proposal = _proposal(args.workflow, args.repo, args.param or [], args.coordinator)
            return CommandResult(0, json.dumps(proposal, sort_keys=True) + "\n", "")
        if args.command == "validate-proposal":
            proposal = json.loads(Path(args.proposal_file).read_text(encoding="utf-8"))
            if not isinstance(proposal, dict):
                return CommandResult(1, "", "invalid proposal object\n")
            argv_value = proposal.get("argv")
            if not isinstance(argv_value, list) or not all(isinstance(part, str) for part in argv_value):
                return CommandResult(1, "", "invalid proposal argv\n")
            actual_id = proposal_id(argv_value)
            if actual_id != args.approved_id:
                return CommandResult(1, "", "proposal id mismatch\n")
            return CommandResult(0, json.dumps({"argv": argv_value}, sort_keys=True) + "\n", "")
        raise ValueError("missing command")
    except ValueError as error:
        return CommandResult(2, "", f"error: {error}\n")
    except (OSError, json.JSONDecodeError) as error:
        return CommandResult(1, "", f"error: {error}\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(prog="factory-foreman")
    subcommands = parser.add_subparsers(dest="command", required=True)

    for name in ("snapshot", "triage"):
        command = subcommands.add_parser(name)
        command.add_argument("--repo", required=True)
        command.add_argument("--format", choices=("json", "markdown"), required=True)
        command.add_argument("--include-factory-display", action="store_true")

    propose = subcommands.add_parser("propose-workflow")
    propose.add_argument("workflow")
    propose.add_argument("--repo", required=True)
    propose.add_argument("--param", action="append", type=_param)
    propose.add_argument("--coordinator")

    validate = subcommands.add_parser("validate-proposal")
    validate.add_argument("--proposal-file", required=True)
    validate.add_argument("--approved-id", required=True)
    return parser


def _param(value: str) -> str:
    if "=" not in value:
        raise argparse.ArgumentTypeError("--param must use KEY=VALUE")
    return value


def _snapshot_exit_code(snapshot: FactorySnapshot) -> int:
    if not snapshot.observations:
        return 1
    if (
        snapshot.observations.get(
            PREFLIGHT_COMMAND, Observation(False, "", error="")
        ).ok
        is False
        and len(snapshot.observations) == 1
    ):
        return 1
    if all(not observation.ok for observation in snapshot.observations.values()):
        return 1
    return 0


def _render_snapshot(snapshot: FactorySnapshot, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(snapshot.to_dict(), sort_keys=True) + "\n"
    health = "HEALTHY" if snapshot.healthy else "DEGRADED"
    lines = [
        "# Factory Snapshot",
        "",
        f"Repository: {snapshot.repo}",
        f"Status: {health}",
        "",
        "## Observations",
    ]
    for name, observation in snapshot.observations.items():
        status = "ok" if observation.ok else f"DEGRADED: {observation.error}"
        lines.append(f"- {name}: {status}")
    _append_factory_display(lines, snapshot)
    return "\n".join(lines) + "\n"


def _render_triage(snapshot: FactorySnapshot, triage: TriageReport, output_format: str) -> str:
    if output_format == "json":
        snapshot_payload = snapshot.to_dict()
        return (
            json.dumps(
                {
                    "schema": 1,
                    "repo": snapshot.repo,
                    "findings": triage.to_dict()["findings"],
                    "snapshot": snapshot_payload,
                    "snapshot_health": {
                        "healthy": snapshot.healthy,
                        "errors": snapshot.errors,
                    },
                    "observations": snapshot_payload["observations"],
                },
                sort_keys=True,
            )
            + "\n"
        )
    health = "HEALTHY" if snapshot.healthy else "DEGRADED"
    lines = ["# Factory Triage", "", "## Snapshot Health", "", f"Status: {health}"]
    for error in snapshot.errors:
        lines.append(f"- DEGRADED: {error}")
    lines.extend(["", "## Findings"])
    if not triage.findings:
        lines.append("- No findings.")
    else:
        for finding in triage.findings:
            lines.append(f"- {finding.severity.upper()} {finding.category}: {finding.summary}")
    _append_factory_display(lines, snapshot)
    return "\n".join(lines) + "\n"


def _append_factory_display(lines: list[str], snapshot: FactorySnapshot) -> None:
    scorecards = snapshot.observations.get("factory_scorecards")
    recommendations = snapshot.observations.get("factory_recommend")
    if scorecards is None and recommendations is None:
        return

    lines.extend(["", "## Factory Foreman Display", "", "Read-only advisory display."])
    if scorecards is None:
        lines.append("- Source availability: unavailable")
    elif not scorecards.ok:
        lines.append(f"- Source availability: unavailable ({scorecards.error})")
    else:
        lines.append(f"- Source availability: {_source_availability(scorecards.data)}")
        unobserved = _unobserved_metrics(scorecards.data)
        if unobserved:
            lines.append(f"- Unobserved metrics: {', '.join(unobserved)}")
        rows = _scorecard_rows(scorecards.data)
        visible_rows = [row for row in rows if _samples(row) >= LOW_SAMPLE_THRESHOLD]
        suppressed = len(rows) - len(visible_rows)
        lines.append("")
        lines.append("### Scorecards")
        if visible_rows:
            for row in visible_rows:
                metric_bits = _scorecard_metric_bits(row)
                suffix = f" ({', '.join(metric_bits)})" if metric_bits else ""
                lines.append(f"- {_row_label(row)}{suffix}")
        else:
            lines.append("- No sufficiently sampled rows.")
        lines.append(f"- Suppressed low-sample rows: {suppressed}")

    lines.extend(["", "### Recommendations"])
    if recommendations is None:
        lines.append("- ADVISORY only: recommendations source unavailable.")
    elif not recommendations.ok:
        lines.append(f"- ADVISORY only: recommendations unavailable ({recommendations.error}).")
    else:
        rendered = _recommendation_lines(recommendations.data)
        lines.extend(rendered or ["- ADVISORY only: no recommendations."])


def _source_availability(data: Any) -> str:
    if isinstance(data, dict):
        availability = data.get("availability")
        if isinstance(availability, list):
            if any(
                isinstance(item, dict) and item.get("available") is False
                for item in availability
            ):
                return "partially unavailable"
        source = data.get("source")
        if isinstance(source, dict) and source.get("available") is False:
            return "unavailable"
        available = data.get("available")
        if available is False:
            return "unavailable"
    return "available"


def _unobserved_metrics(data: Any) -> list[str]:
    metrics: list[str] = []
    if isinstance(data, dict):
        for item in data.get("availability") or []:
            if isinstance(item, dict) and item.get("available") is False:
                family = item.get("source_family")
                if isinstance(family, str) and family.strip():
                    metrics.append(family)
        for warning in data.get("warnings") or []:
            if isinstance(warning, str) and warning.strip():
                metrics.append(warning)
        for container in (data, data.get("source")):
            if isinstance(container, dict):
                value = container.get("unobserved_metrics") or container.get("unobserved")
                if isinstance(value, list):
                    metrics.extend(str(metric) for metric in value if str(metric).strip())
    for row in _scorecard_rows(data):
        value = row.get("unobserved_metrics") or row.get("unobserved")
        if isinstance(value, list):
            metrics.extend(str(metric) for metric in value if str(metric).strip())
    return sorted(set(metrics))


def _scorecard_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        for key in ("scorecards", "rows", "groups", "items"):
            value = data.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    return []


def _samples(row: dict[str, Any]) -> float:
    return _first_number(row, "sample_size", "samples", "sample_count", "n", "count") or 0


def _row_label(row: dict[str, Any]) -> str:
    group_key = row.get("group_key")
    if isinstance(group_key, dict):
        parts = [
            _first_string(group_key, key)
            for key in ("task_class", "workflow", "harness", "model")
        ]
        label = "/".join(part for part in parts if part)
        if label:
            return label
    return _first_string(row, "group", "label", "name", "agent", "workflow") or "scorecard row"


def _scorecard_metric_bits(row: dict[str, Any]) -> list[str]:
    bits: list[str] = []
    samples = _samples(row)
    if samples:
        bits.append(f"samples={int(samples) if samples.is_integer() else samples}")
    success_rate = _first_number(row, "success_rate", "successRate")
    metrics = row.get("metrics")
    if success_rate is None and isinstance(metrics, dict):
        accepted = _first_number(metrics, "accepted")
        runs = _first_number(metrics, "accepted_sample_size", "runs")
        if accepted is not None and runs:
            success_rate = accepted / runs
    if success_rate is not None:
        bits.append(f"success_rate={success_rate:g}")
    if isinstance(metrics, dict):
        for key in ("rework_sample_size", "ci_sample_size", "cost_sample_size", "lead_time_sample_size"):
            value = _first_number(metrics, key)
            if value == 0:
                bits.append(f"{key}=unobserved")
    return bits


def _recommendation_lines(data: Any) -> list[str]:
    if isinstance(data, dict):
        value = data.get("recommendations") or data.get("items") or data.get("rows")
    else:
        value = data
    if not isinstance(value, list):
        return []
    lines: list[str] = []
    for item in value:
        if isinstance(item, dict):
            if item.get("suppressed") is True:
                continue
            summary = _first_string(item, "summary", "recommendation", "title", "message")
            rationale = _first_string(item, "advice", "rationale", "reason")
            if summary:
                suffix = f" Rationale: {rationale}" if rationale else ""
                lines.append(f"- ADVISORY only: {summary}{suffix}")
        elif str(item).strip():
            lines.append(f"- ADVISORY only: {item}")
    return lines


def _proposal(workflow: str, repo: str, params: list[str], coordinator: str | None) -> dict[str, Any]:
    argv = ["rk", "--json", "workflow", "run", workflow, "--repo", repo]
    for value in params:
        argv.extend(["--param", value])
    if coordinator is not None:
        argv.extend(["--coordinator", coordinator])
    return {"proposal_id": proposal_id(argv), "argv": argv, "command": shlex.join(argv)}


def proposal_id(argv: list[str]) -> str:
    canonical = json.dumps(argv, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    result = main()
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    raise SystemExit(result.returncode)
