"""Deterministic, read-only factory snapshot collection."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from typing import Any, Protocol


SCHEMA = "factory-foreman.snapshot.v1"
GENERATED_AT = "1970-01-01T00:00:00Z"
PREFLIGHT_COMMAND = "daemon"
COMMANDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("agents", ("rk", "--json", "list")),
    ("inbox", ("rk", "--json", "inbox")),
    ("workflows", ("rk", "--json", "workflow", "list")),
    ("definitions", ("rk", "--json", "workflow", "defs", "--repo", "{repo}")),
    ("cost", ("rk", "--json", "cost", "--fleet")),
    ("repository", ("rk", "--json", "repo", "show", "{repo}")),
    ("tickets", ("rk", "--json", "ticket", "list", "--repo", "{repo}")),
)
STATUS_ARGV = ("rk", "--json", "daemon", "status")


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


def daemon_preflight(runner: Runner) -> Observation:
    return _observe(PREFLIGHT_COMMAND, STATUS_ARGV, runner)


def collect_snapshot(repo: str, runner: Runner) -> FactorySnapshot:
    observations: dict[str, Observation] = {}
    preflight = daemon_preflight(runner)

    if not preflight.ok:
        observations[PREFLIGHT_COMMAND] = preflight
        return _snapshot(repo, observations)

    for command, argv_template in COMMANDS:
        argv = tuple(repo if part == "{repo}" else part for part in argv_template)
        observations[command] = _observe(command, argv, runner)

    return _snapshot(repo, observations)


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
