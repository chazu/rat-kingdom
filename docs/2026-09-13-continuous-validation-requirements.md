# Human requirements for continuous factory delivery

This is the review baseline for `2026-09-13-continuous-validation-promotion.md`.
It records the operator's requested outcomes, independently of plan choices.

1. Validation, integration and promotion are major bottlenecks. Improve throughput
   for RK itself and other projects on one machine while continuing to ship fixes
   and features.
2. Inspect and reuse existing repository rules and RK features carefully.
3. Make promotion/deployment and rollback explicit. RK's first deployment target
   is local; support the general external pre-production/production problem.
4. Validation must not require quieting the entire factory for lengthy trials.
   Keep progress possible while appropriately scoped evidence accumulates.
5. Objective/fitness functions should be defined ahead of time and metrics feed
   back continuously or as continuously as practical.
6. Use feature flags or configuration to enable/disable behavior and isolate
   failures and bugs.
7. Integrate stigmergy where appropriate, especially agent discovery and useful
   evidence reuse through the existing tuplespace/BBS.
8. Produce a thorough implementation plan, stabilize it through adversarial
   review, and execute it. Do not stop at discussion or require another general
   implementation approval.
9. Respect existing role and authority boundaries: King is the operator delegate,
   native workers implement, and routine work proceeds under existing policy.
10. Preserve completed work, failed evidence, live state and user-owned changes.
    Report implementation, delivery, installed behavior, and demonstrated benefit
    separately and honestly.

These requirements authorize routine scope decomposition, tracker choices,
implementation, validation and previously established delivery/deployment work.
They do not authorize fabricated evidence, destructive production-state rewinds,
arbitrary external credential use, or workers widening their own authority.
