import copy
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import factory_foreman
from factory_foreman import CommandResult, FactorySnapshot, Observation, collect_snapshot, daemon_preflight


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
REPO = "demo"
REPO_PATH = "/tmp/demo-repo"

STATUS_ARGV = ("rk", "--json", "daemon", "status")
SNAPSHOT_ARGVS = [
    ("rk", "--json", "list"),
    ("rk", "--json", "inbox"),
    ("rk", "--json", "workflow", "list"),
    ("rk", "--json", "cost", "--fleet"),
    ("rk", "--json", "repo", "show", REPO),
    ("rk", "--json", "workflow", "defs", "--repo", REPO_PATH),
    ("rk", "--json", "ticket", "list", "--repo", REPO),
]
FACTORY_SCORECARDS_ARGV = (
    "rk",
    "--json",
    "factory",
    "scorecards",
    "--repo",
    REPO,
    "--group-by",
    "all",
    "--include-archived",
)
FACTORY_RECOMMEND_ARGV = ("rk", "--json", "factory", "recommend", "--repo", REPO)


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
            SNAPSHOT_ARGVS[3]: CommandResult(0, fixture("cost"), ""),
            SNAPSHOT_ARGVS[4]: CommandResult(0, fixture("repository"), ""),
            SNAPSHOT_ARGVS[5]: CommandResult(0, fixture("definitions"), ""),
            SNAPSHOT_ARGVS[6]: CommandResult(0, fixture("tickets"), ""),
        }

    def default_results_with_factory_display(self):
        results = self.default_results()
        results[FACTORY_SCORECARDS_ARGV] = CommandResult(
            0,
            json.dumps(
                {
                    "source": {"available": True, "unobserved_metrics": ["review_latency"]},
                    "rows": [
                        {
                            "group": "agent:alpha",
                            "samples": 8,
                            "success_rate": 0.75,
                            "unobserved_metrics": ["cost_usd"],
                        },
                        {"group": "agent:beta", "samples": 1, "success_rate": 1.0},
                    ],
                }
            ),
            "",
        )
        results[FACTORY_RECOMMEND_ARGV] = CommandResult(
            0,
            json.dumps(
                {
                    "recommendations": [
                        {
                            "summary": "Prefer alpha for repair workflows.",
                            "rationale": "Higher observed success rate.",
                        }
                    ]
                }
            ),
            "",
        )
        return results

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

    def test_collect_snapshot_optionally_collects_factory_scorecards_and_recommendations_after_preflight(self):
        runner = FakeRunner(self.default_results_with_factory_display())

        snapshot = collect_snapshot(REPO, runner, include_factory_display=True)

        self.assertEqual(runner.calls[0], STATUS_ARGV)
        self.assertEqual(runner.calls[-2:], [FACTORY_SCORECARDS_ARGV, FACTORY_RECOMMEND_ARGV])
        self.assertTrue(snapshot.observations["factory_scorecards"].ok)
        self.assertTrue(snapshot.observations["factory_recommend"].ok)

    def test_collect_snapshot_skips_factory_display_when_strict_preflight_fails(self):
        runner = FakeRunner({STATUS_ARGV: CommandResult(1, "", "daemon down")})

        snapshot = collect_snapshot(REPO, runner, include_factory_display=True)

        self.assertEqual(runner.calls, [STATUS_ARGV])
        self.assertNotIn("factory_scorecards", snapshot.observations)
        self.assertNotIn("factory_recommend", snapshot.observations)

    def test_collect_snapshot_degrades_when_optional_factory_display_commands_are_unavailable(self):
        results = self.default_results()
        results[FACTORY_SCORECARDS_ARGV] = CommandResult(2, "", "unknown command scorecards")
        results[FACTORY_RECOMMEND_ARGV] = CommandResult(2, "", "unknown command recommend")
        runner = FakeRunner(results)

        snapshot = collect_snapshot(REPO, runner, include_factory_display=True)

        self.assertFalse(snapshot.observations["factory_scorecards"].ok)
        self.assertFalse(snapshot.observations["factory_recommend"].ok)
        self.assertIn("factory_scorecards", snapshot.errors[0])

    def test_collect_snapshot_resolves_defs_repo_from_observed_repository_path(self):
        runner = FakeRunner(self.default_results())

        snapshot = collect_snapshot(REPO, runner)

        self.assertTrue(snapshot.observations["repository"].ok)
        self.assertEqual(snapshot.observations["repository"].data["path"], REPO_PATH)
        self.assertIn(("rk", "--json", "workflow", "defs", "--repo", REPO_PATH), runner.calls)
        self.assertNotIn(("rk", "--json", "workflow", "defs", "--repo", REPO), runner.calls)

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

    def test_live_direct_list_shapes_are_consumed_without_ticket_history_noise(self):
        live = fixture_json("live_direct_shapes")
        snapshot = snapshot_from_fixture()
        observations = dict(snapshot.observations)
        for name in ("agents", "inbox", "workflows", "cost", "tickets"):
            observations[name] = Observation(True, name, data=copy.deepcopy(live[name]))
        snapshot = FactorySnapshot(
            snapshot.schema,
            snapshot.generated_at,
            snapshot.repo,
            True,
            observations,
            [],
        )

        report = self.classify(snapshot)

        categories = [finding.category for finding in report.findings]
        self.assertEqual(
            set(categories),
            {"orphaned-agent", "empty-harness-result", "budget-pressure"},
        )
        by_category = {finding.category: finding for finding in report.findings}
        self.assertEqual(by_category["orphaned-agent"].subject, "agent-live-orphan")
        self.assertEqual(by_category["orphaned-agent"].agent, "agent-live-orphan")
        self.assertEqual(by_category["empty-harness-result"].workflow_instance, "inst-empty-live")
        self.assertEqual(by_category["budget-pressure"].workflow_instance, "inst-budget-live")
        self.assertIn('"spent_usd": 8.5', by_category["budget-pressure"].evidence)
        self.assertNotIn("permission-or-authority", categories)
        self.assertNotIn("stale-or-moved-base", categories)


class ExplodingRunner:
    def run(self, argv):
        raise AssertionError(f"runner must not execute: {argv!r}")


class FactoryForemanCliTests(unittest.TestCase):
    def default_runner(self):
        return FakeRunner(FactoryForemanSnapshotTests().default_results())

    def run_cli(self, args, runner=None):
        return factory_foreman.main(args, runner=runner or self.default_runner())

    def test_triage_markdown_contains_health_and_findings_sections(self):
        result = self.run_cli(["triage", "--repo", REPO, "--format", "markdown"])

        self.assertEqual(result.returncode, 0)
        self.assertIn("# Factory Triage", result.stdout)
        self.assertIn("## Snapshot Health", result.stdout)
        self.assertIn("## Findings", result.stdout)

    def test_triage_json_exposes_live_contract_top_level_fields(self):
        result = self.run_cli(["triage", "--repo", REPO, "--format", "json"])

        payload = json.loads(result.stdout)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(payload["schema"], 1)
        self.assertEqual(payload["repo"], REPO)
        self.assertIsInstance(payload["findings"], list)

    def test_triage_markdown_renders_optional_factory_display_as_advisory_without_controls(self):
        result = self.run_cli(
            ["triage", "--repo", REPO, "--format", "markdown", "--include-factory-display"],
            runner=FakeRunner(FactoryForemanSnapshotTests().default_results_with_factory_display()),
        )

        self.assertEqual(result.returncode, 0)
        self.assertIn("## Factory Foreman Display", result.stdout)
        self.assertIn("Source availability: available", result.stdout)
        self.assertIn("Unobserved metrics:", result.stdout)
        self.assertIn("review_latency", result.stdout)
        self.assertIn("cost_usd", result.stdout)
        self.assertIn("agent:alpha", result.stdout)
        self.assertIn("Suppressed low-sample rows: 1", result.stdout)
        self.assertIn("ADVISORY only: Prefer alpha for repair workflows.", result.stdout)
        forbidden = ["apply", "dispatch", "policy", "config", "workflow run", "ticket create", "approve", "gate"]
        for token in forbidden:
            self.assertNotIn(token, result.stdout.lower())

    def test_partial_snapshot_warning_is_visible(self):
        results = FactoryForemanSnapshotTests().default_results()
        results[SNAPSHOT_ARGVS[1]] = CommandResult(1, "", "inbox failed")

        result = self.run_cli(
            ["snapshot", "--repo", REPO, "--format", "markdown"],
            runner=FakeRunner(results),
        )

        self.assertEqual(result.returncode, 0)
        self.assertIn("DEGRADED", result.stdout)
        self.assertIn("inbox", result.stdout)

    def test_propose_workflow_renders_but_does_not_execute(self):
        result = self.run_cli(
            ["propose-workflow", "repair", "--repo", REPO],
            runner=ExplodingRunner(),
        )

        payload = json.loads(result.stdout)
        self.assertEqual(result.returncode, 0)
        self.assertIn("proposal_id", payload)
        self.assertIn("argv", payload)
        self.assertIn("command", payload)

    def test_validate_proposal_returns_exact_argv_on_matching_id(self):
        proposal = json.loads(
            self.run_cli(
                [
                    "propose-workflow",
                    "repair",
                    "--repo",
                    REPO,
                    "--param",
                    "ticket=123",
                    "--coordinator",
                    "coord-1",
                ],
                runner=ExplodingRunner(),
            ).stdout
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "proposal.json"
            path.write_text(json.dumps(proposal), encoding="utf-8")
            result = self.run_cli(
                [
                    "validate-proposal",
                    "--proposal-file",
                    str(path),
                    "--approved-id",
                    proposal["proposal_id"],
                ],
                runner=ExplodingRunner(),
            )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(json.loads(result.stdout)["argv"], proposal["argv"])

    def test_validate_proposal_rejects_exact_command_mismatch(self):
        proposal = json.loads(
            self.run_cli(
                ["propose-workflow", "repair", "--repo", REPO, "--coordinator", "coord-1"],
                runner=ExplodingRunner(),
            ).stdout
        )
        proposal["argv"][-1] = "coord-2"

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "proposal.json"
            path.write_text(json.dumps(proposal), encoding="utf-8")
            result = self.run_cli(
                [
                    "validate-proposal",
                    "--proposal-file",
                    str(path),
                    "--approved-id",
                    proposal["proposal_id"],
                ],
                runner=ExplodingRunner(),
            )

        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("argv", result.stdout)

    def test_validate_proposal_rejects_non_object_json_without_executing(self):
        for payload in ([], 42, "not object"):
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "proposal.json"
                path.write_text(json.dumps(payload), encoding="utf-8")
                result = self.run_cli(
                    [
                        "validate-proposal",
                        "--proposal-file",
                        str(path),
                        "--approved-id",
                        "approved-id",
                    ],
                    runner=ExplodingRunner(),
                )

                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(result.stdout, "")
                self.assertIn("proposal", result.stderr)
                self.assertIn("object", result.stderr)

    def test_validate_proposal_subprocess_routes_non_object_json_error_to_stderr(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "proposal.json"
            path.write_text("[]", encoding="utf-8")

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPTS_DIR / "factory_foreman.py"),
                    "validate-proposal",
                    "--proposal-file",
                    str(path),
                    "--approved-id",
                    "approved-id",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertIn("invalid proposal object", result.stderr)
        self.assertNotIn("argv", result.stderr)

    def test_propose_workflow_rejects_invalid_param_without_equals(self):
        result = self.run_cli(
            ["propose-workflow", "repair", "--repo", REPO, "--param", "broken"],
            runner=ExplodingRunner(),
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn("KEY=VALUE", result.stderr)

    def test_propose_workflow_quotes_shell_display_without_changing_argv(self):
        result = self.run_cli(
            [
                "propose-workflow",
                "repair",
                "--repo",
                REPO,
                "--param",
                "summary=hello world",
            ],
            runner=ExplodingRunner(),
        )

        payload = json.loads(result.stdout)
        self.assertIn("summary=hello world", payload["argv"])
        self.assertIn("'summary=hello world'", payload["command"])


if __name__ == "__main__":
    unittest.main()
