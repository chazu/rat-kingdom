# Backlog groom — 2026-08-15

This pass reviewed the live repo-scoped inventory from
`rk ticket list --repo rat-kingdom --status open`. It did not start any
ticket. Four oversized open tickets were split into independently grabbable
children; existing child work under the rk-mcp installer umbrella was kept.

Counts: **14 decomposed**, **0 deduped by state mutation**, **4 stale flagged**.

## Decomposition

| Parent | Children | Shape |
| --- | --- | --- |
| TKT-01M03RN6RWPNDZGR59KAF8KSBE | TKT-01M048ASX3Q4PY9E9XAP8KXB2K, TKT-01M048ASX3ERRP7JRCQNGDH0AD, TKT-01M048ASX7MY9J9RKXMW4TQDKA, TKT-01M048ASYKD2HNKHZ1K6MVXFF5 | inventory; policy decision; implementation; verification/rollout |
| TKT-01M036PSF2WV7NHZE00G2EFCVK | TKT-01M048ASYM00N37EBK1VM7FH5H, TKT-01M048ASY8MDB5DVV5VG3WRM47, TKT-01M048ASYPEM94FZY73TT33QE2, TKT-01M048ASYWQPVGXBV4BZYXQRZ8 | CUE removal; policy relocation; installed-copy cleanup; cutover proof/docs |
| TKT-01M0382WS1NNR0W5RA59PPDDYP | TKT-01M048ASYZSP4ZN9KEAFN8S0AR, TKT-01M048ASZ68WXJRAZ1J270BZHN, TKT-01M048ASZGGNA5J6AXBJ8YYAE0 | trigger wiring; CUE tiering; regression/deployment reconciliation |
| TKT-01M02NAF4DP4BAGTWTXBM8XMFV | TKT-01M048ASZPDJTRRNKTKRPG76GB, TKT-01M048AT004HEZ711EMQNCFKG0, TKT-01M048AT0CQBR7P3BBRKYGJA1S | concurrency design; implementation; load proof/diagnostics |

TKT-01M00XJ6BCQNW48FVE235YAMSK already has three children covering the
installer binary, schema-aware registration, and CLI/docs wiring, so it was
not split again. The focused pre-existing, auth-reproduction, and rework
tickets were not decomposed.

## Duplicate and stale decisions

The evidence-backed duplicate groups are:

- Keep TKT-01M00SS4WEFY0BBT9TZH3MF9GJ as the survivor for macOS temporary
  path normalization. TKT-01M00SWEJVPVYJHQGQAZDK34TM is already closed and
  TKT-01M00T53TZSTRJZTEMGKWZ96XH is the open duplicate.
- Keep TKT-01M00XHVBTEH7A4JEFM0TQZZ5N as the survivor for the workflow-run
  fake-harness race. TKT-01M01F0BWFMKVKN674TK1EFMDF is the open duplicate.

TKT-01M01FPPXKSV36W6EK88X4X7Q7 is also stale: it is a rework handoff for the
already-done survivor TKT-01M00XHVBTEH7A4JEFM0TQZZ5N, not independent work.
These three open records are the four stale findings when the already-closed
duplicate is included in the duplicate group count. The intentional grmpl
anchors TKT-125, TKT-139, TKT-144, and TKT-145 were not touched.

Agent callers cannot perform `rk ticket update`; the daemon rejects that
state mutation. Therefore no closure was falsely claimed. Operator follow-up
is recorded by TKT-01M01P111JQA0F4KQRG25S95W7, with the survivor mapping and
original evidence preserved in the ticket bodies.

Global `grmpl` tickets were outside this rat-kingdom pass and were not
changed.
