# Typed capability registry

*Status: stabilized after adversarial review. 2026-09-01. Review dispositions:
`docs/2026-09-01-three-enhancements-adversarial-review.md` C1-C6.*

## Decision

Introduce one typed, default-deny daemon registry for callable-operation policy
and supervised role/harness compatibility. Server authorization, read-only
grants, foreman exceptions, resolved harness permission, terminal completion,
and role-specific completion guidance derive from it. Harness adapters stay
thin and consume the resolved mode plus the existing core-owned narrow Jcode
tool set; no harness-to-daemon dependency is introduced.

Operator identity and peer-origin/token validation remain separate prior
gates. The registry answers what an already-authenticated principal may do; it
does not authenticate principals.

## Problem

Capability meaning is currently split across:

- an operator-only negative list, which makes every unlisted operation
  ordinary-agent callable;
- a separate read-only role allowlist;
- groomer parameter-shape checks;
- foreman child-management exceptions;
- harness permission modes and Jcode tool lists;
- prompt text and terminal-completion exceptions.

This creates fail-open extension risk and allows internally inconsistent
role/harness pairings. In particular, Jcode read-only mode exposes no shell or
`rk` tool, diagnostician guidance requires `rk done`, and harness terminal
completion is trusted only for Jcode onboarders. Jcode groomers likewise cannot
perform their required evidence-bearing ticket closure.

## Registry model

Each method intentionally callable by a non-operator has a typed policy
describing its base audience:

- operator only;
- ordinary authenticated agent;
- read-only role;
- ingest principal;
- conditional grant requiring a second predicate.

Conditional grants remain narrow code predicates for payload- or
relationship-sensitive checks, including self `task_done`, groomer ticket
closure, and foreman child ownership. The registry names the predicate; it
does not replace it with a broad boolean.

An unknown or unclassified method is denied to every non-operator principal.
Operator dispatch remains independently extensible after the stronger
observed-origin authentication gate.

Role/harness policy describes:

- filesystem permission mode;
- completion channel (`rk done` or trusted harness terminal result);
- required tool capabilities;
- whether the pairing is supported.

## Stabilized initial pairings

- Jcode onboarder: read-only tools plus trusted terminal completion.
- Jcode diagnostician: read-only tools plus trusted terminal completion; its
  generated guidance must not require an unavailable `rk done` command.
- Jcode groomer: rejected at spawn until the adapter exposes a narrow,
  evidence-bearing ticket-close tool. Terminal completion alone cannot perform
  the groomer's required mutation.
- Codex/Claude/fake read-only roles retain their current enforced permission
  modes and `rk done` completion path.

Rejecting an unsupported pairing is preferable to spawning an agent whose
documented deliverable cannot be produced.

## Migration

1. Add the registry with policies for every currently dispatched method.
2. Change ordinary-agent authorization from “not operator-only” to an explicit
   registry grant.
3. Route read-only, foreman, and groomer decisions through registry policy plus
   their existing conditional predicates.
4. Centralize role/harness support and completion channel selection.
5. Generate the completion paragraph appended to role guidance and the resume
   instruction from the selected pairing. Attach refusal text names the actual
   restricted role rather than hard-coding onboarding.
6. Remove superseded policy lists only after parity tests pass.

The first release should preserve existing intended grants. Any accidental
grant discovered by parity tests is treated as a security defect, not
compatibility to preserve.

## Invariants

1. Unknown methods fail closed for non-operators.
2. Operator authentication and kernel-observed origin checks remain unchanged.
3. Ingest principals can call only ingest methods.
4. Read-only roles cannot gain a mutation merely because a method is added.
5. Conditional grants still validate caller, target relationship, and payload
   shape at dispatch time.
6. Every spawnable role/harness pairing has a usable completion channel and
   every required mutation/tool.
7. Prime guidance names only operations available to the selected pairing.
8. An explicit authenticated agent caller without a current registry record
   receives only the ordinary default-deny method profile. A live record may
   narrow that profile by role; absence can never grant operator authority.

## Verification

- Exhaustive registry uniqueness and intended non-operator-method tests.
- A parity test covering every currently intended non-operator grant.
- Unknown-method denial tests for rat, foreman, groomer, diagnostician, and
  ingest principals.
- Existing authorization tests rerun unchanged, then converted to table-driven
  registry cases.
- Role/harness matrix tests for permission mode, completion channel, and
  unsupported pairings.
- Jcode diagnostician test proving terminal completion is accepted without
  `rk done`.
- Jcode groomer spawn rejection test with a direct explanation.
- Explicit recordless-agent test proving an ordinary registered method works
  while operator-only and unknown methods remain denied.

## Implementation status

Implemented in `crates/rk-daemon/src/capabilities.rs` and consumed by server
authorization, read-only-role policy, and supervisor role/harness resolution.
Unknown non-operator methods fail closed; conditional foreman and groomer
checks remain live predicates. Jcode onboarders and diagnosticians use trusted
terminal completion, while Jcode groomers are rejected before supervised spawn
effects. Authenticated recordless callers retain only the explicit ordinary
profile.

Registry, authorization, recordless-caller, role/harness, Jcode completion, and
unsupported-pairing regressions pass. On 2026-09-01, `mise run verify` passed
all 1,755 affected tests and `mise run verify-full` passed the protected
workspace suite and doc tests.

## Non-goals

- Making the local daemon safe for mutually untrusted operating-system users.
- Replacing peer credential checks or bearer-token derivation.
- Giving Jcode a general shell tool in read-only mode.
- Broadening foreman or groomer authority.
- Encoding repository-specific policy in the capability registry.

## Rollback

The registry changes no durable data. Rollback restores prior authorization,
but also restores fail-open method extension and the Jcode pairing mismatch;
those risks must be explicit in any rollback decision.
