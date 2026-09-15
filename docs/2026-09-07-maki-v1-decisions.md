# Maki v1 decisions

Date: 2026-09-07
Decision ticket: `TKT-dolih-navan-jagop`
Parent initiative: `TKT-mosik-kulat-jaliz`

The operator approved the following boundaries for the first Maki integration:

- **Steering:** disabled in v1. Maki's SDK currently drops Rat Kingdom's `rk_control` side-band metadata, so RK must not claim trusted steering semantics until an authenticated preservation path is proved.
- **Configuration and authority:** use a daemon-owned sanitized Maki configuration root. Disable plugins and custom commands, disable native `Task` and `Memory`, and do not inherit unreviewed global or project MCP authority. Attached/TUI and restricted-role support remain out of scope.
- **Accounting:** treat ChatGPT OAuth and other unpriced Maki results as unknown cost (`None`), never as authoritative zero dollars. Preserve token usage and RK's own estimates/caps.

These are the accepted v1 constraints; implementation and acceptance tickets must test them explicitly.
