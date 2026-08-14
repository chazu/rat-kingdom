# Factory Dashboard Template

This file documents the section order rendered by `render_factory_dashboard.py`.
The renderer keeps Markdown generation in Python so values can be escaped and rows
can be sorted deterministically.

1. Factory Dashboard
2. Data Source
3. Connection State
4. Resync State
5. Approvals
6. Workflow Runs
7. Agents
8. Tickets
9. Inbox
10. Budget
11. Recent Events
12. Degraded Data

The Recent Events section consumes typed replay `boundary` metadata and event
`kind` fields. Legacy `boundary_cursor` and `type` aliases are tolerated for old
artifacts only. The dashboard is display-only: typed execution still requires
daemon-verifiable approval of the exact canonical digest, while the Phase 1
helper remains a fallback for legacy/manual proposal validation.
