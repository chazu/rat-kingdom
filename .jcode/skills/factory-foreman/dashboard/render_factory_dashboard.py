#!/usr/bin/env python3
"""Render a deterministic Markdown dashboard from saved factory JSON files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


EMPTY = "_none_"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--events", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    snapshot = read_json(Path(args.snapshot))
    events = read_json(Path(args.events))
    rendered = render_dashboard(snapshot, events)
    Path(args.output).write_text(rendered, encoding="utf-8")
    return 0


def read_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def render_dashboard(snapshot: dict[str, Any], events: dict[str, Any]) -> str:
    lines: list[str] = []
    add = lines.append

    add("# Factory Dashboard")
    add("")
    add(f"- Repository: {code(snapshot.get('repo', 'unknown'))}")
    add(f"- Generated at: {code(snapshot.get('generated_at', 'unknown'))}")
    add(f"- Overall state: **{overall_state(snapshot)}**")
    add("")

    add("## Data Source")
    add(f"- Snapshot: {code(data_source(snapshot, 'live'))}")
    add(f"- Events: {code(data_source(events, 'replay'))}")
    add("")

    add("## Connection State")
    connection = snapshot.get("connection") or snapshot.get("connection_state") or {}
    mapping_section(lines, safe_mapping(connection, "connection"))

    add("## Resync State")
    resync = as_dict(snapshot.get("repo_resync"))
    status = str(resync.get("status", "unknown"))
    if status.lower() == "running":
        add("- State: **RESYNCING**")
    elif status.lower() in {"failed", "error"}:
        add("- State: **DEGRADED**")
    else:
        add(f"- State: {text(status)}")
    for key in ["last_started_at", "last_finished_at", "source", "error"]:
        if key in resync:
            add(f"- {label(key)}: {text(resync.get(key))}")
    add("")

    add("## Approvals")
    approvals = rows(snapshot, "approvals", "pending_approvals", "proposals")
    if approvals:
        add("| kind | proposal id | status | digest | requester | scope | expires at |")
        add("| --- | --- | --- | --- | --- | --- | --- |")
        for item in stable_rows(approvals, "proposal_id", "id"):
            status = str(pick(item, "status", default="pending"))
            kind = "approval" if status.lower() == "approved" else "proposal"
            add("| " + " | ".join([
                cell(kind),
                cell(pick(item, "proposal_id", "id")),
                cell(status),
                cell(pick(item, "digest", "canonical_digest")),
                cell(pick(item, "requester", "requested_by")),
                cell(pick(item, "scope", "action", "command")),
                cell(pick(item, "expires_at", "expiry")),
            ]) + " |")
    else:
        add(EMPTY)
    add("")

    table(lines, "## Workflow Runs", rows(snapshot, "workflow_runs", "workflows"), ["id", "workflow", "state", "repo", "updated_at"])
    table(lines, "## Agents", rows(snapshot, "agents"), ["id", "state", "task", "repo", "updated_at"])
    table(lines, "## Tickets", rows(snapshot, "tickets"), ["id", "status", "title", "repo", "updated_at"])
    table(lines, "## Inbox", rows(snapshot, "inbox", "messages"), ["id", "state", "subject", "from", "updated_at"])

    add("## Budget")
    budget = snapshot.get("budget") or snapshot.get("cost") or {}
    mapping_section(lines, as_dict(budget))

    add("## Recent Events")
    add(f"- From cursor: {code(events.get('from_cursor', ''))}")
    add(f"- Cursor: {code(events.get('cursor', ''))}")
    if events.get("truncated"):
        boundary = events.get("boundary") or events.get("boundary_cursor") or events.get("oldest_cursor") or events.get("from_cursor")
        add(f"- WARNING: replay truncated, boundary cursor: {code(boundary)}")
    event_rows = rows(events, "events")
    if event_rows:
        add("| cursor | kind | summary |")
        add("| --- | --- | --- |")
        for item in stable_rows(event_rows, "cursor", "kind", "type"):
            add("| " + " | ".join([cell(item.get("cursor")), cell(pick(item, "kind", "type")), cell(pick(item, "summary", "message", "subject"))]) + " |")
    else:
        add(EMPTY)
    add("")

    add("## Degraded Data")
    degraded = degraded_sources(snapshot)
    if degraded:
        add("- State: **DEGRADED**")
        for item in degraded:
            add(f"- {text(item)}")
    else:
        add(EMPTY)
    add("")

    return "\n".join(lines)


def overall_state(snapshot: dict[str, Any]) -> str:
    resync = as_dict(snapshot.get("repo_resync"))
    if str(resync.get("status", "")).lower() == "running":
        return "RESYNCING"
    if degraded_sources(snapshot):
        return "DEGRADED"
    return "OK"


def degraded_sources(snapshot: dict[str, Any]) -> list[str]:
    found: list[str] = []
    if snapshot.get("stale") or snapshot.get("is_stale"):
        found.append("stale snapshot")
    for item in rows(snapshot, "degraded_sources", "degraded", "errors"):
        found.append(flatten(item))
    observations = as_dict(snapshot.get("observations"))
    for name in sorted(observations):
        observation = as_dict(observations.get(name))
        if observation and observation.get("ok") is False:
            source = observation.get("source") or observation.get("command") or name
            error = observation.get("error") or observation.get("message") or "failed"
            found.append(f"{source}: {error}")
    resync = as_dict(snapshot.get("repo_resync"))
    if str(resync.get("status", "")).lower() in {"failed", "error"}:
        found.append(f"{resync.get('source', 'repo_resync')}: {resync.get('error', 'failed')}")
    return found


def data_source(mapping: dict[str, Any], default: str) -> Any:
    return pick(mapping, "source", "mode", "data_source", default=default)


def safe_mapping(value: Any, name: str) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    return {f"Malformed {name}": value}


def mapping_section(lines: list[str], mapping: dict[str, Any]) -> None:
    if not mapping:
        lines.append(EMPTY)
        lines.append("")
        return
    for key in sorted(mapping):
        lines.append(f"- {label(key)}: {text(mapping[key])}")
    lines.append("")


def table(lines: list[str], heading: str, data: list[dict[str, Any]], columns: list[str]) -> None:
    lines.append(heading)
    if not data:
        lines.append(EMPTY)
        lines.append("")
        return
    lines.append("| " + " | ".join(label(column) for column in columns) + " |")
    lines.append("| " + " | ".join("---" for _ in columns) + " |")
    for item in stable_rows(data, *columns):
        lines.append("| " + " | ".join(cell(item.get(column)) for column in columns) + " |")
    lines.append("")


def rows(mapping: dict[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        value = mapping.get(key)
        if isinstance(value, list):
            return [item if isinstance(item, dict) else {"value": item} for item in value]
        if isinstance(value, dict):
            for nested in ("items", "rows", "messages", "workflows", "agents", "tickets", "approvals", "events"):
                nested_value = value.get(nested)
                if isinstance(nested_value, list):
                    return [item if isinstance(item, dict) else {"value": item} for item in nested_value]
    return []


def stable_rows(data: list[dict[str, Any]], *keys: str) -> list[dict[str, Any]]:
    return sorted(data, key=lambda item: tuple(str(item.get(key, "")) for key in keys) + (json.dumps(item, sort_keys=True),))


def pick(mapping: dict[str, Any], *keys: str, default: Any = "") -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return default


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def flatten(value: Any) -> str:
    if isinstance(value, dict):
        return ", ".join(f"{label(key)}={text(value[key])}" for key in sorted(value))
    return text(value)


def label(value: str) -> str:
    return str(value).replace("_", " ")


def code(value: Any) -> str:
    return f"`{text(value)}`" if value not in (None, "") else "`unknown`"


def text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return str(value).replace("\n", " ")


def cell(value: Any) -> str:
    return text(value).replace("|", "\\|")


if __name__ == "__main__":
    raise SystemExit(main())
