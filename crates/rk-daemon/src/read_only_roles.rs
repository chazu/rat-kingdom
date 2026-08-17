//! Forced read-only worker roles.
//!
//! Some roles are assessment-only: they read a repository and report, and must
//! not be able to mutate anything even if their prompt is subverted. That
//! guarantee is enforced by two layers, both of which live here so every
//! read-only role shares one implementation rather than growing a parallel one:
//!
//! 1. **Harness layer** — [`permission_mode`] forces a non-writing harness mode
//!    that callers cannot override, so the agent cannot edit files, commit, or
//!    push. It fails closed: a harness with no known read-only mode is an
//!    error, not a silent full-access spawn.
//! 2. **Daemon layer** — [`method_allowed`] is an allowlist over RPC methods, so
//!    the agent cannot reach a state-changing `rk` command even though it holds
//!    a valid agent token.
//!
//! [`ONBOARDER_ROLE`] was the first role to need this; [`DIAGNOSTICIAN_ROLE`]
//! is the general-purpose read-only worker that workflow dispatch can spawn for
//! diagnosis. The onboarder additionally reaches the onboarding-session methods
//! that own its report; that is the only per-role difference.

use serde_json::Value;

use crate::onboarding_sessions::ONBOARDER_ROLE;
use crate::proto::Request;

/// A read-only worker spawnable by ordinary dispatch (`rk spawn --role
/// diagnostician`, or a workflow `spawn` step). It gets an ordinary branch and
/// worktree like any rat, but cannot write to either.
pub const DIAGNOSTICIAN_ROLE: &str = "diagnostician";

/// Roles whose capability is forced narrower than an ordinary rat's.
pub fn is_read_only_role(role: &str) -> bool {
    matches!(role, ONBOARDER_ROLE | DIAGNOSTICIAN_ROLE)
}

/// Harness mode for a read-only role. Fails closed on an unknown harness: a
/// harness we cannot prove confines to reading must not receive one of these
/// agents at all.
pub fn permission_mode(harness: &str) -> rk_core::Result<String> {
    match harness {
        "claude" => Ok("plan".into()),
        "codex" | "fake" | "jcode" => Ok("read-only".into()),
        other => Err(rk_core::Error::other(format!(
            "harness {other:?} has no enforced read-only mode"
        ))),
    }
}

/// Daemon-side allowlist. Everything not named here is refused, so a new
/// mutating RPC is denied to read-only roles by default rather than by
/// remembering to deny it.
pub fn method_allowed(role: &str, req: &Request) -> bool {
    match req.method.as_str() {
        "ping"
        | "status"
        | "space.scan"
        | "space.rd"
        | "repo.list"
        | "repo.get"
        | "agent.status"
        | "agent.log"
        | "agent.progress" => true,
        // The one permitted write: declaring yourself finished. Narrowed to a
        // task_done event naming the caller, so it cannot carry a finding for
        // another instance or masquerade as another tuple category.
        "space.out" => {
            req.params.get("category").and_then(Value::as_str) == Some("event")
                && req.params.get("identity").and_then(Value::as_str) == Some("task_done")
                && req
                    .params
                    .get("instance")
                    .and_then(Value::as_str)
                    .is_none_or(|instance| instance == req.caller)
                && req
                    .params
                    .get("payload")
                    .and_then(|payload| payload.get("agent"))
                    .and_then(Value::as_str)
                    == Some(req.caller.as_str())
        }
        // Onboarding sessions own the assessment report the onboarder is
        // spawned to produce. Neither method mutates the repository: inspect
        // reads, and propose journals advice that still needs approval.
        "repo.onboard.inspect" | "repo.onboard.propose" => role == ONBOARDER_ROLE,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(caller: &str, method: &str, params: Value) -> Request {
        Request {
            id: "1".into(),
            method: method.into(),
            params,
            caller: caller.into(),
            auth: String::new(),
        }
    }

    #[test]
    fn both_read_only_roles_share_the_enforcement() {
        assert!(is_read_only_role(ONBOARDER_ROLE));
        assert!(is_read_only_role(DIAGNOSTICIAN_ROLE));
        for role in ["rat", "reviewer", "foreman", "verifier"] {
            assert!(!is_read_only_role(role), "{role} must keep full capability");
        }
    }

    #[test]
    fn permission_mode_is_forced_and_fails_closed() {
        assert_eq!(permission_mode("claude").unwrap(), "plan");
        assert_eq!(permission_mode("codex").unwrap(), "read-only");
        assert_eq!(permission_mode("jcode").unwrap(), "read-only");
        assert_eq!(permission_mode("fake").unwrap(), "read-only");
        assert!(
            permission_mode("axe").is_err(),
            "an unproven harness must not receive a read-only-role agent"
        );
    }

    #[test]
    fn diagnostician_cannot_reach_state_changing_methods() {
        for method in [
            "agent.spawn",
            "agent.dismiss",
            "agent.steer",
            "repo.add",
            "repo.remove",
            "workflow.run",
            "ticket.new",
            "ticket.update",
            "space.take",
            "repo.onboard.approve",
            "repo.onboard.apply",
        ] {
            assert!(
                !method_allowed(DIAGNOSTICIAN_ROLE, &req("rat-1", method, json!({}))),
                "{method} must be refused to a diagnostician"
            );
        }
    }

    #[test]
    fn diagnostician_can_read_and_declare_done() {
        for method in ["status", "space.scan", "space.rd", "repo.get", "agent.log"] {
            assert!(method_allowed(
                DIAGNOSTICIAN_ROLE,
                &req("rat-1", method, json!({}))
            ));
        }
        assert!(method_allowed(
            DIAGNOSTICIAN_ROLE,
            &req(
                "rat-1",
                "space.out",
                json!({"category": "event", "identity": "task_done",
                       "payload": {"agent": "rat-1"}}),
            )
        ));
    }

    #[test]
    fn done_write_cannot_be_widened_into_a_general_tuple_write() {
        // Another category, another instance, or another agent in the payload
        // are each independently disqualifying.
        for params in [
            json!({"category": "artifact", "identity": "task_done",
                   "payload": {"agent": "rat-1"}}),
            json!({"category": "event", "identity": "finding",
                   "payload": {"agent": "rat-1"}}),
            json!({"category": "event", "identity": "task_done", "instance": "rat-2",
                   "payload": {"agent": "rat-1"}}),
            json!({"category": "event", "identity": "task_done",
                   "payload": {"agent": "rat-2"}}),
        ] {
            assert!(
                !method_allowed(DIAGNOSTICIAN_ROLE, &req("rat-1", "space.out", params)),
                "widened done-write must be refused"
            );
        }
    }

    #[test]
    fn onboarding_methods_stay_with_the_onboarder() {
        for method in ["repo.onboard.inspect", "repo.onboard.propose"] {
            assert!(method_allowed(
                ONBOARDER_ROLE,
                &req("rat-1", method, json!({}))
            ));
            assert!(
                !method_allowed(DIAGNOSTICIAN_ROLE, &req("rat-1", method, json!({}))),
                "{method} belongs to onboarding sessions, not diagnosis"
            );
        }
    }
}
