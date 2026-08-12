import sys
import unittest
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

from factory_foreman import CommandResult, collect_snapshot, daemon_preflight


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


if __name__ == "__main__":
    unittest.main()
