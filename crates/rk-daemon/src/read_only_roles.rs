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
//!
//! [`GROOMER_ROLE`] is a different shape entirely and is deliberately NOT one
//! of the [`is_read_only_role`] roles: it keeps the ordinary rat's daemon
//! surface (it can file tickets, write artifacts/obstacles/needs, and scan the
//! tuplespace to gather evidence) but has its harness forced into the same
//! non-writing mode as the roles above — a backlog groom mutates the ticket
//! store, not a worktree, so it never needs to edit or commit. On top of that
//! ordinary surface it gets exactly one additional grant a rat does not have:
//! [`groomer_can_close_ticket`] narrowly allows `ticket.update` to close a
//! ticket when the request carries recorded evidence. See `server.rs`
//! `authorize_reasoned` for where that grant is wired in (alongside the
//! foreman's own narrow grant), and `handle_ticket_update` for the audit event
//! every groomer closure writes.

use serde_json::Value;

use crate::onboarding_sessions::ONBOARDER_ROLE;
use crate::proto::Request;

/// A read-only worker spawnable by ordinary dispatch (`rk spawn --role
/// diagnostician`, or a workflow `spawn` step). It gets an ordinary branch and
/// worktree like any rat, but cannot write to either.
pub const DIAGNOSTICIAN_ROLE: &str = "diagnostician";

/// Backlog groomer: ordinary rat daemon surface, harness forced read-only, plus
/// one narrow extra grant to close a ticket with recorded evidence.
pub const GROOMER_ROLE: &str = "groomer";

/// Roles whose *daemon* capability is forced narrower than an ordinary rat's
/// (a strict RPC allowlist, not just a harness sandbox). [`GROOMER_ROLE`] is
/// harness-sandboxed the same way but is NOT here — it keeps the ordinary
/// surface plus one extra grant, so it must not collapse into this allowlist.
pub fn is_read_only_role(role: &str) -> bool {
    matches!(role, ONBOARDER_ROLE | DIAGNOSTICIAN_ROLE)
}

/// Roles whose harness is forced into a non-writing mode, regardless of what
/// their daemon RPC surface looks like. A superset of [`is_read_only_role`].
pub fn forces_read_only_harness(role: &str) -> bool {
    is_read_only_role(role) || role == GROOMER_ROLE
}

/// Harness mode for a read-only role. Fails closed on an unknown harness: a
/// harness we cannot prove confines to reading must not receive one of these
/// agents at all.
pub fn permission_mode(harness: &str) -> rk_core::Result<String> {
    crate::capabilities::role_harness_profile(DIAGNOSTICIAN_ROLE, harness).map(|profile| {
        profile
            .forced_permission_mode
            .expect("diagnostician always forces a permission mode")
            .to_string()
    })
}

/// Daemon-side allowlist. Everything not named here is refused, so a new
/// mutating RPC is denied to read-only roles by default rather than by
/// remembering to deny it.
pub fn method_allowed(role: &str, req: &Request) -> bool {
    let Some(policy) = crate::capabilities::method_policy(&req.method) else {
        return false;
    };
    match policy.read_only {
        crate::capabilities::ReadOnlyGrant::Always => true,
        // The one permitted write: declaring yourself finished. Narrowed to a
        // task_done event naming the caller, so it cannot carry a finding for
        // another instance or masquerade as another tuple category.
        crate::capabilities::ReadOnlyGrant::SelfTaskDone => {
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
        crate::capabilities::ReadOnlyGrant::Onboarder => role == ONBOARDER_ROLE,
        crate::capabilities::ReadOnlyGrant::None => false,
    }
}

/// A groomer's `ticket.update` must be exactly `{id, status: "closed",
/// reason: {reason, evidence}}` — no other field present (labels included:
/// `add_labels`/`remove_labels` are tolerated only when empty, since the
/// client always sends them). This is deliberately an allowlist over the
/// wire shape, not a denylist, so a new `TicketChanges` field is refused by
/// default rather than by remembering to add it here. Called from
/// `server.rs` `authorize_reasoned`, not from [`method_allowed`] above —
/// [`GROOMER_ROLE`] is not a read-only-allowlist role, see the module doc.
pub fn groomer_can_close_ticket(params: &Value) -> bool {
    let Some(obj) = params.as_object() else {
        return false;
    };
    let shape_ok = obj.iter().all(|(key, value)| match key.as_str() {
        "id" | "status" | "reason" => true,
        "add_labels" | "remove_labels" => value.as_array().is_none_or(|arr| arr.is_empty()),
        _ => false,
    });
    if !shape_ok {
        return false;
    }
    if obj.get("status").and_then(Value::as_str) != Some("closed") {
        return false;
    }
    let non_empty_str = |v: Option<&Value>| {
        v.and_then(Value::as_str)
            .is_some_and(|s| !s.trim().is_empty())
    };
    let reason = obj.get("reason");
    non_empty_str(reason.and_then(|r| r.get("reason")))
        && non_empty_str(reason.and_then(|r| r.get("evidence")))
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
            client_version: None,
        }
    }

    #[test]
    fn both_read_only_roles_share_the_enforcement() {
        assert!(is_read_only_role(ONBOARDER_ROLE));
        assert!(is_read_only_role(DIAGNOSTICIAN_ROLE));
        for role in ["rat", "reviewer", "foreman", "verifier", GROOMER_ROLE] {
            assert!(!is_read_only_role(role), "{role} must keep full capability");
        }
    }

    #[test]
    fn groomer_harness_is_sandboxed_but_daemon_surface_is_not_the_strict_allowlist() {
        assert!(forces_read_only_harness(GROOMER_ROLE));
        assert!(forces_read_only_harness(ONBOARDER_ROLE));
        assert!(forces_read_only_harness(DIAGNOSTICIAN_ROLE));
        assert!(!forces_read_only_harness("rat"));
        // method_allowed is the strict read-only-role allowlist; a groomer
        // must not be routed through it, or it would lose ticket.new /
        // space.out for its own hand-off fallback.
        for method in ["ticket.new", "ticket.list", "ticket.get"] {
            assert!(
                !method_allowed(GROOMER_ROLE, &req("rat-1", method, json!({}))),
                "{method} must not be granted via the read-only allowlist for groomer"
            );
        }
    }

    #[test]
    fn groomer_can_close_ticket_requires_exact_shape_and_evidence() {
        let good = json!({"id": "TKT-1", "status": "closed",
            "reason": {"reason": "stale-rework", "evidence": "TKT-2 done"}});
        assert!(groomer_can_close_ticket(&good));

        let good_with_empty_labels = json!({"id": "TKT-1", "status": "closed",
            "reason": {"reason": "stale-rework", "evidence": "TKT-2 done"},
            "add_labels": [], "remove_labels": []});
        assert!(groomer_can_close_ticket(&good_with_empty_labels));

        for bad in [
            json!({"id": "TKT-1", "status": "done",
                "reason": {"reason": "x", "evidence": "y"}}),
            json!({"id": "TKT-1", "status": "open",
                "reason": {"reason": "x", "evidence": "y"}}),
            json!({"id": "TKT-1", "status": "closed"}),
            json!({"id": "TKT-1", "status": "closed", "reason": {"reason": "x"}}),
            json!({"id": "TKT-1", "status": "closed", "reason": {"evidence": "y"}}),
            json!({"id": "TKT-1", "status": "closed",
                "reason": {"reason": "", "evidence": "y"}}),
            json!({"id": "TKT-1", "status": "closed",
                "reason": {"reason": "x", "evidence": "  "}}),
            json!({"id": "TKT-1", "status": "closed",
                "reason": {"reason": "x", "evidence": "y"}, "title": "new title"}),
            json!({"id": "TKT-1", "status": "closed",
                "reason": {"reason": "x", "evidence": "y"}, "add_labels": ["frozen"]}),
            json!({"id": "TKT-1", "status": "closed",
                "reason": {"reason": "x", "evidence": "y"}, "parent": "TKT-9"}),
        ] {
            assert!(!groomer_can_close_ticket(&bad), "must be refused: {bad}");
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
