//! Typed callable-operation and role/harness capability policy.
//!
//! Authentication (peer credentials, supervised origin, and derived tokens)
//! happens before this module. This registry answers the narrower question:
//! what may an already-authenticated non-operator call, and can a supervised
//! role actually complete work through a selected harness?

use crate::onboarding_sessions::ONBOARDER_ROLE;
use crate::read_only_roles::{DIAGNOSTICIAN_ROLE, GROOMER_ROLE};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadOnlyGrant {
    None,
    Always,
    SelfTaskDone,
    Onboarder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MethodPolicy {
    pub(crate) ordinary: bool,
    pub(crate) read_only: ReadOnlyGrant,
    pub(crate) foreman_child: bool,
    pub(crate) groomer_close: bool,
}

const ORDINARY: MethodPolicy = MethodPolicy {
    ordinary: true,
    read_only: ReadOnlyGrant::None,
    foreman_child: false,
    groomer_close: false,
};
const ORDINARY_READ_ONLY: MethodPolicy = MethodPolicy {
    ordinary: true,
    read_only: ReadOnlyGrant::Always,
    ..ORDINARY
};
const ORDINARY_SELF_DONE: MethodPolicy = MethodPolicy {
    ordinary: true,
    read_only: ReadOnlyGrant::SelfTaskDone,
    ..ORDINARY
};
const ORDINARY_ONBOARDER: MethodPolicy = MethodPolicy {
    ordinary: true,
    read_only: ReadOnlyGrant::Onboarder,
    ..ORDINARY
};
const ONBOARDER_ONLY: MethodPolicy = MethodPolicy {
    ordinary: false,
    read_only: ReadOnlyGrant::Onboarder,
    foreman_child: false,
    groomer_close: false,
};
const FOREMAN_CHILD: MethodPolicy = MethodPolicy {
    ordinary: false,
    read_only: ReadOnlyGrant::None,
    foreman_child: true,
    groomer_close: false,
};
const GROOMER_CLOSE: MethodPolicy = MethodPolicy {
    ordinary: false,
    read_only: ReadOnlyGrant::None,
    foreman_child: false,
    groomer_close: true,
};

/// Explicit non-operator grants. Anything absent fails closed for every
/// non-operator, even if a dispatch arm is added elsewhere.
pub(crate) fn method_policy(method: &str) -> Option<MethodPolicy> {
    Some(match method {
        // `bbs.export` is a bounded READ of the caller's own repository; the
        // handler refuses a foreign scope. It authors nothing.
        // `control.verify` is a bounded, self-scoped lookup + narrow
        // evidentiary write (it never touches the caller's task, git, or
        // ticket state) — every role, including the read-only ones, needs it
        // to check a claimed steer, exactly like the read-only surfaces
        // beside it.
        "bbs.brief" | "bbs.show" | "bbs.export" | "bbs.discovery.show"
        | "bbs.retirement.show" | "bbs.assessment.show" | "bbs.assessment.status"
        | "bbs.assessment.latest" | "ping" | "status"
        | "space.scan" | "space.rd"
        | "repo.list"
        | "repo.get" | "agent.status" | "agent.log" | "agent.progress" | "release.list"
        | "release.show" | "release.status" | "control.verify" => ORDINARY_READ_ONLY,
        "space.out" => ORDINARY_SELF_DONE,
        "repo.onboard.inspect" => ORDINARY_ONBOARDER,
        "repo.onboard.propose" => ONBOARDER_ONLY,
        "agent.spawn" | "agent.respawn" | "agent.dismiss" | "agent.interrupt" | "agent.steer" => {
            FOREMAN_CHILD
        }
        "ticket.update" => GROOMER_CLOSE,
        "bbs.ask"
        | "bbs.answer"
        | "bbs.accept"
        | "bbs.publish"
        | "bbs.reuse"
        // "bbs.assess" is deliberately absent: it is operator-only. Absence
        // here makes `authorize_reasoned` reject any non-operator caller with
        // `operator_only_method` before `crate::bbs::write` is ever reached;
        // see the matching in-handler check there for defense in depth.
        | "space.withdraw"
        | "fact.vote"
        | "space.take"
        | "space.watch"
        | "agent.list"
        | "budget.rollup"
        | "work.current"
        | "inbox.list"
        | "inbox.ack"
        | "reconcile.report"
        | "lease.acquire"
        | "lease.renew"
        | "attention.next"
        | "attention.decide"
        | "verify.run"
        | "factory.snapshot"
        | "factory.events.replay"
        | "factory.events.watch"
        | "factory.scorecards"
        | "factory.recommend"
        | "workflow.list"
        | "workflow.status"
        | "workflow.timeline"
        | "workflow.definitions"
        | "ticket.new"
        | "ticket.list"
        | "ticket.get"
        | "ticket.ready" => ORDINARY,
        _ => return None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionChannel {
    RkDone,
    HarnessTerminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoleHarnessProfile {
    pub(crate) forced_permission_mode: Option<&'static str>,
    pub(crate) completion: CompletionChannel,
}

/// Validate a role/harness pairing before any durable spawn side effect and
/// return the enforcement/completion profile used by launch and prime text.
pub(crate) fn role_harness_profile(
    role: &str,
    harness: &str,
) -> rk_core::Result<RoleHarnessProfile> {
    if role == GROOMER_ROLE && harness == "jcode" {
        return Err(rk_core::Error::other(
            "unsupported role/harness pairing: jcode groomer cannot perform the required evidence-bearing ticket closure with its read-only tool set",
        ));
    }
    let forced_permission_mode =
        if matches!(role, ONBOARDER_ROLE | DIAGNOSTICIAN_ROLE | GROOMER_ROLE) {
            Some(match harness {
                "claude" => "plan",
                "codex" | "fake" | "jcode" => "read-only",
                other => {
                    return Err(rk_core::Error::other(format!(
                        "harness {other:?} has no enforced read-only mode"
                    )))
                }
            })
        } else {
            None
        };
    let completion = if harness == "jcode" && matches!(role, ONBOARDER_ROLE | DIAGNOSTICIAN_ROLE) {
        CompletionChannel::HarnessTerminal
    } else {
        CompletionChannel::RkDone
    };
    Ok(RoleHarnessProfile {
        forced_permission_mode,
        completion,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_and_operator_methods_are_not_non_operator_grants() {
        for method in [
            "future.dangerous",
            "stop",
            "repo.land",
            "ticket.deliver",
            "bbs.assess",
            "bbs.discovery.set",
            "bbs.retirement.set",
            "bbs.assessment.configure",
            "bbs.assessment.activate",
            "bbs.assessment.disable",
            "bbs.assessment.tick",
        ] {
            assert!(method_policy(method).is_none(), "{method}");
        }
    }

    #[test]
    fn intended_ordinary_surface_is_explicit() {
        for method in [
            "ping",
            "status",
            "space.out",
            "space.withdraw",
            "fact.vote",
            "space.scan",
            "space.take",
            "space.rd",
            "space.watch",
            "agent.progress",
            "agent.list",
            "budget.rollup",
            "work.current",
            "inbox.list",
            "inbox.ack",
            "reconcile.report",
            "lease.acquire",
            "lease.renew",
            "attention.next",
            "attention.decide",
            "agent.status",
            "agent.log",
            "verify.run",
            "factory.snapshot",
            "factory.events.replay",
            "factory.events.watch",
            "factory.scorecards",
            "factory.recommend",
            "workflow.list",
            "workflow.status",
            "workflow.timeline",
            "workflow.definitions",
            "repo.list",
            "repo.get",
            "repo.onboard.inspect",
            "ticket.new",
            "ticket.list",
            "ticket.get",
            "ticket.ready",
            "bbs.publish",
            "bbs.reuse",
            "control.verify",
            "bbs.discovery.show",
            "bbs.retirement.show",
            "bbs.assessment.show",
            "bbs.assessment.status",
            "bbs.assessment.latest",
            "release.list",
            "release.show",
            "release.status",
        ] {
            assert!(
                method_policy(method).is_some_and(|policy| policy.ordinary),
                "ordinary grant missing for {method}"
            );
        }
        for method in ["ingest.event", "ingest.state"] {
            assert!(method_policy(method).is_none(), "{method} is ingest-only");
        }
    }

    #[test]
    fn jcode_restricted_pairings_have_usable_completion_or_are_rejected() {
        for role in [ONBOARDER_ROLE, DIAGNOSTICIAN_ROLE] {
            let profile = role_harness_profile(role, "jcode").unwrap();
            assert_eq!(profile.forced_permission_mode, Some("read-only"));
            assert_eq!(profile.completion, CompletionChannel::HarnessTerminal);
        }
        assert!(role_harness_profile(GROOMER_ROLE, "jcode").is_err());
    }

    /// Maki v1 has no tested allow-list/enforced read-only profile, so every
    /// restricted role must fail closed by construction: no arm in the
    /// `match harness` for "maki" means the fallback `other` case always
    /// returns an error, before any durable spawn side effect. This locks
    /// that omission in as a regression guard rather than relying on it
    /// staying accidentally true.
    #[test]
    fn restricted_roles_fail_closed_for_maki_pending_a_tested_read_only_profile() {
        for role in [ONBOARDER_ROLE, DIAGNOSTICIAN_ROLE, GROOMER_ROLE] {
            let error = role_harness_profile(role, "maki")
                .expect_err("maki has no enforced read-only mode yet");
            assert!(error.to_string().contains("no enforced read-only mode"));
        }
    }
}
