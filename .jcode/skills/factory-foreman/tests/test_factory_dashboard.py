import json
import sys
import tempfile
import unittest
from pathlib import Path

DASHBOARD_DIR = Path(__file__).resolve().parents[1] / "dashboard"
sys.path.insert(0, str(DASHBOARD_DIR))

import render_factory_dashboard


class FactoryDashboardTests(unittest.TestCase):
    def render(self, snapshot, events):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            snapshot_path = tmp / "snapshot.json"
            events_path = tmp / "events.json"
            output_path = tmp / "dashboard.md"
            snapshot_path.write_text(json.dumps(snapshot, sort_keys=True), encoding="utf-8")
            events_path.write_text(json.dumps(events, sort_keys=True), encoding="utf-8")
            render_factory_dashboard.main([
                "--snapshot", str(snapshot_path),
                "--events", str(events_path),
                "--output", str(output_path),
            ])
            return output_path.read_text(encoding="utf-8")

    def base_snapshot(self):
        return {
            "generated_at": "2026-08-13T10:00:00Z",
            "repo": "rat-kingdom",
            "connection": {"status": "connected", "socket": "/tmp/rk.sock"},
            "repo_resync": {"status": "idle", "source": "snapshot"},
            "approvals": [
                {
                    "proposal_id": "prop-1",
                    "status": "pending",
                    "digest": "sha256:abc123",
                    "requester": "jcode",
                    "scope": "workflow.run repair rat-kingdom",
                    "expires_at": "2026-08-13T11:00:00Z",
                    "command": "rk --json workflow run repair --repo rat-kingdom",
                }
            ],
            "workflow_runs": [{"id": "wf-1", "state": "running", "workflow": "repair"}],
            "agents": [{"id": "agent-1", "state": "busy", "task": "repair"}],
            "tickets": [{"id": "T-1", "status": "open", "title": "Fix tests"}],
            "inbox": [{"id": "msg-1", "subject": "Need approval", "state": "unread"}],
            "budget": {"spent_usd": 1.25, "limit_usd": 5.0},
            "degraded_sources": [],
        }

    def base_events(self):
        return {
            "cursor": "cursor-10",
            "from_cursor": "cursor-1",
            "truncated": False,
            "events": [
                {"cursor": "cursor-9", "type": "workflow.started", "summary": "repair started"},
                {"cursor": "cursor-10", "type": "approval.requested", "summary": "approval requested"},
            ],
        }


    def fixture(self, name):
        path = Path(__file__).resolve().parent / "fixtures" / "dashboard" / name
        return json.loads(path.read_text(encoding="utf-8"))

    def test_dashboard_consumes_typed_replay_boundary_kind_and_source(self):
        output = self.render(self.fixture("typed_snapshot.json"), self.fixture("typed_replay.json"))
        self.assertIn("## Data Source", output)
        self.assertIn("Snapshot: `daemon.live`", output)
        self.assertIn("Events: `daemon.replay`", output)
        self.assertIn("boundary cursor: `evt-0000`", output)
        self.assertIn("approval.requested", output)
        self.assertIn("workflow.started", output)
        self.assertNotIn("unknown | digest approval requested", output)

    def test_dashboard_tolerates_legacy_replay_aliases(self):
        events = self.base_events()
        events.update({"source": "legacy.replay", "truncated": True, "boundary_cursor": "cursor-legacy"})
        output = self.render(self.base_snapshot(), events)
        self.assertIn("Events: `legacy.replay`", output)
        self.assertIn("boundary cursor: `cursor-legacy`", output)
        self.assertIn("approval.requested", output)

    def test_dashboard_safely_renders_malformed_connection_values(self):
        snapshot = self.base_snapshot()
        snapshot["connection"] = [
            {"status": "connected"},
            {"endpoint": "unix:///tmp/rk.sock"},
        ]
        output = self.render(snapshot, self.base_events())
        self.assertIn("## Connection State", output)
        self.assertIn("Malformed connection", output)
        self.assertIn("status", output)

    def test_dashboard_renders_snapshot_and_events_sections(self):
        output = self.render(self.base_snapshot(), self.base_events())
        for heading in [
            "# Factory Dashboard",
            "## Connection State",
            "## Resync State",
            "## Approvals",
            "## Workflow Runs",
            "## Agents",
            "## Tickets",
            "## Inbox",
            "## Budget",
            "## Recent Events",
            "## Degraded Data",
        ]:
            self.assertIn(heading, output)

    def test_dashboard_exposes_replay_truncation_and_boundary(self):
        events = self.base_events()
        events.update({"truncated": True, "boundary_cursor": "cursor-4"})
        output = self.render(self.base_snapshot(), events)
        self.assertIn("WARNING: replay truncated", output)
        self.assertIn("boundary cursor: `cursor-4`", output)

    def test_dashboard_marks_resyncing_state(self):
        snapshot = self.base_snapshot()
        snapshot["repo_resync"] = {"status": "running", "last_started_at": "2026-08-13T10:01:00Z", "source": "repo.watch"}
        output = self.render(snapshot, self.base_events())
        self.assertIn("RESYNCING", output)
        self.assertIn("repo.watch", output)

    def test_dashboard_marks_stale_or_degraded_state(self):
        snapshot = self.base_snapshot()
        snapshot["stale"] = True
        snapshot["observations"] = {"tickets": {"ok": False, "source": "factory.snapshot.tickets", "error": "timeout"}}
        output = self.render(snapshot, self.base_events())
        self.assertIn("DEGRADED", output)
        self.assertIn("factory.snapshot.tickets", output)
        self.assertIn("timeout", output)

    def test_dashboard_lists_pending_approvals_with_digest(self):
        output = self.render(self.base_snapshot(), self.base_events())
        for text in ["prop-1", "sha256:abc123", "jcode", "workflow.run repair rat-kingdom", "2026-08-13T11:00:00Z"]:
            self.assertIn(text, output)

    def test_dashboard_never_renders_execute_command_as_approved(self):
        output = self.render(self.base_snapshot(), self.base_events())
        self.assertIn("proposal", output)
        self.assertNotIn("approved | prop-1", output)

    def test_renderer_has_no_control_or_execution_behavior(self):
        source = (DASHBOARD_DIR / "render_factory_dashboard.py").read_text(encoding="utf-8")
        forbidden = ["subprocess", "socket", "urllib", "requests", "http.client", "rk-mcp", "workflow run", "os.system", "Popen"]
        for token in forbidden:
            self.assertNotIn(token, source)


if __name__ == "__main__":
    unittest.main()
