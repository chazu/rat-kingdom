import copy
import json
import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import factory_foreman
from factory_foreman import CommandResult, FactorySnapshot, Observation, collect_snapshot, daemon_preflight


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
REPO = "demo"

STATUS_ARGV = ("rk", "--json", "daemon", "status")
SNAPSHOT_ARGVS = [
    ("rk", "--json", "list"),
    ("rk", "--json", "inbox"),
    ("rk", "--json", "workflow", "list"),
    ("rk", "--json", "workflow", "defs", "--repo", REPO),
    ("rk", "--json", "cost", "--fleet"),
    ("rk", "--json", "repo", "show", REPO),
    ("rk", "--json", "ticket", "list", "--repo", REPO),
]


def fixture(name):
    return (FIXTURES_DIR / f"{name}.json").read_text()


def fixture_json(name):
    return json.loads(fixture(name))


def observation_from_dict(payload):
    if payload["ok"]:
        return Observation(True, payload["command"], data=payload.get("data"))
    return Observation(False, payload["command"], error=payload.get("error"))


def snapshot_from_fixture(name="classification_base_snapshot"):
    payload = fixture_json(name)
    return FactorySnapshot(
        payload["schema"],
        payload["generated_at"],
        payload["repo"],
        payload["healthy"],
        {
            key: observation_from_dict(value)
            for key, value in payload["observations"].items()
        },
        payload["errors"],
    )


def classified_snapshot(*, workflows=None, inbox=None, errors=None):
    snapshot = snapshot_from_fixture()
    observations = dict(snapshot.observations)
    if workflows is not None:
        observations["workflows"] = Observation(
            True, "workflows", data={"workflows": workflows}
        )
    if inbox is not None:
        observations["inbox"] = Observation(True, "inbox", data={"messages": inbox})
    return FactorySnapshot(
        snapshot.schema,
        snapshot.generated_at,
        snapshot.repo,
        False,
        observations,
        errors or [],
    )


def classification_case(case):
    payload = fixture_json("classification_cases_snapshot")
    for observation_name, observation in payload["observations"].items():
        data = observation.get("data") or {}
        for key in ("workflows", "messages"):
            for row in data.get(key, []):
                if row.get("case") == case:
                    return observation_name, row
    raise AssertionError(f"missing classification fixture case: {case}")


def classified_case_snapshot(case):
    observation_name, row = classification_case(case)
    if observation_name == "workflows":
        return classified_snapshot(workflows=[copy.deepcopy(row)])
    if observation_name == "inbox":
        return classified_snapshot(inbox=[copy.deepcopy(row)])
    raise AssertionError(f"unsupported classification source: {observation_name}")


class FakeRunner:
    def __init__(self, results=None):
        self.calls = []
        self.results = results or {}

    def run(self, argv):
        argv = tuple(argv)
        self.calls.append(argv)
        result = self.results.get(argv)
        if result is None:
            raise AssertionError(f"unexpected argv: {argv!r}")
        return result


class FactoryForemanSnapshotTests(unittest.TestCase):
    def default_results(self):
        return {
            STATUS_ARGV: CommandResult(0, fixture("status"), ""),
            SNAPSHOT_ARGVS[0]: CommandResult(0, fixture("agents"), ""),
            SNAPSHOT_ARGVS[1]: CommandResult(0, fixture("inbox"), ""),
            SNAPSHOT_ARGVS[2]: CommandResult(0, fixture("workflows"), ""),
            SNAPSHOT_ARGVS[3]: CommandResult(0, fixture("definitions"), ""),
            SNAPSHOT_ARGVS[4]: CommandResult(0, fixture("cost"), ""),
            SNAPSHOT_ARGVS[5]: CommandResult(0, fixture("repository"), ""),
            SNAPSHOT_ARGVS[6]: CommandResult(0, fixture("tickets"), ""),
        }

    def test_daemon_preflight_uses_strict_status_before_observations(self):
        runner = FakeRunner(self.default_results())

        daemon_preflight(runner)

        self.assertEqual(runner.calls[0], STATUS_ARGV)

    def test_unavailable_daemon_stops_without_observation_autostart(self):
        runner = FakeRunner({STATUS_ARGV: CommandResult(1, "", "daemon down")})

        collect_snapshot(REPO, runner)

        self.assertEqual(runner.calls, [STATUS_ARGV])

    def test_collect_snapshot_runs_only_read_only_json_commands(self):
        runner = FakeRunner(self.default_results())

        collect_snapshot(REPO, runner)

        self.assertEqual(runner.calls[1:], SNAPSHOT_ARGVS)

    def test_collect_snapshot_preserves_successes_when_one_command_fails(self):
        results = self.default_results()
        results[SNAPSHOT_ARGVS[1]] = CommandResult(1, "", "inbox failed")
        runner = FakeRunner(results)

        snapshot = collect_snapshot(REPO, runner)

        self.assertTrue(snapshot.observations["agents"].ok)
        self.assertFalse(snapshot.observations["inbox"].ok)

    def test_collect_snapshot_rejects_non_json_stdout(self):
        results = self.default_results()
        results[SNAPSHOT_ARGVS[0]] = CommandResult(0, "not-json", "")
        runner = FakeRunner(results)

        snapshot = collect_snapshot(REPO, runner)

        self.assertIn("invalid JSON", snapshot.observations["agents"].error)

    def test_snapshot_is_unhealthy_when_any_observation_fails(self):
        results = self.default_results()
        results[SNAPSHOT_ARGVS[6]] = CommandResult(1, "", "ticket failed")
        runner = FakeRunner(results)

        snapshot = collect_snapshot(REPO, runner)

        self.assertFalse(snapshot.healthy)
        self.assertEqual(len(snapshot.errors), 1)


class FactoryForemanClassificationTests(unittest.TestCase):
    def classify(self, snapshot):
        return factory_foreman.classify_snapshot(snapshot)

    def assert_single_category(self, report, category):
        self.assertEqual([finding.category for finding in report.findings], [category])
        return report.findings[0]

    def test_classifies_empty_undeclared_harness_result(self):
        snapshot = classified_case_snapshot("empty-undeclared-harness-result")

        finding = self.assert_single_category(
            self.classify(snapshot), "empty-harness-result"
        )

        self.assertEqual(finding.workflow_instance, "inst-empty")
        self.assertEqual(finding.agent, "agent-empty")

    def test_classifies_missing_rk_command(self):
        snapshot = classified_case_snapshot("missing-rk-command")

        finding = self.assert_single_category(
            self.classify(snapshot), "missing-rk-executable"
        )

        self.assertIn("rk: command not found", finding.evidence)

    def test_classifies_named_check_failure(self):
        snapshot = classified_case_snapshot("named-check-failure")

        finding = self.assert_single_category(
            self.classify(snapshot), "named-check-failure"
        )

        self.assertEqual(finding.subject, "cargo test")
        self.assertIn("exit_code", finding.evidence)

    def test_classifies_timeout_separately_from_red_check(self):
        snapshot = classified_case_snapshot("timeout")

        self.assert_single_category(self.classify(snapshot), "workflow-timeout")

    def test_classifies_orphaned_agent(self):
        snapshot = classified_case_snapshot("orphaned-agent")

        finding = self.assert_single_category(self.classify(snapshot), "orphaned-agent")

        self.assertEqual(finding.subject, "agent-orphan")

    def test_reports_high_cost_instance_as_budget_pressure(self):
        snapshot = classified_case_snapshot("budget-pressure")

        finding = self.assert_single_category(self.classify(snapshot), "budget-pressure")

        self.assertIn("8", finding.evidence)
        self.assertIn("10", finding.evidence)

    def test_unknown_failure_is_preserved_not_dropped(self):
        snapshot = classified_case_snapshot("unknown-failure")

        finding = self.assert_single_category(self.classify(snapshot), "unknown")

        self.assertIn("frobnicator returned a new opaque status", finding.evidence)

    def test_findings_are_deduplicated_and_severity_sorted(self):
        _, duplicate = classification_case("missing-rk-command")
        snapshot = classified_snapshot(
            inbox=[copy.deepcopy(duplicate), copy.deepcopy(duplicate)],
            workflows=[
                {
                    "kind": "workflow-running",
                    "workflow_instance": "inst-budget",
                    "agent": "agent-9",
                    "spend_usd": 4.0,
                    "instance_max_usd": 5.0,
                }
            ],
        )

        report = self.classify(snapshot)

        self.assertEqual(
            [finding.category for finding in report.findings],
            ["missing-rk-executable", "budget-pressure"],
        )
        self.assertEqual(
            [finding.severity for finding in report.findings], ["critical", "medium"]
        )

    def test_named_check_beats_generic_permission_or_stale_text(self):
        snapshot = classified_case_snapshot("named-check-with-permission-text")

        finding = self.assert_single_category(
            self.classify(snapshot), "named-check-failure"
        )

        self.assertEqual(finding.subject, "cargo clippy")

    def test_timeout_token_in_check_name_is_not_workflow_timeout(self):
        snapshot = classified_case_snapshot("timeout-token-not-timeout")

        finding = self.assert_single_category(
            self.classify(snapshot), "named-check-failure"
        )

        self.assertEqual(finding.subject, "timeout_tests")

    def test_budget_pressure_ignores_fleet_budget_aliases(self):
        snapshot = classified_case_snapshot("fleet-budget-alias-not-instance")

        report = self.classify(snapshot)

        self.assertEqual(report.findings, [])


if __name__ == "__main__":
    unittest.main()
