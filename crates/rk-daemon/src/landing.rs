//! Daemon-native landing pipeline (Phase 3).
//!
//! T2 (`LandingQueue` + gate runner) grows here — see
//! docs/proposals/daemon-native-landing-pipeline.md. T1's contribution to
//! this module is its *interface*: a daemon-native consumer in this crate can
//! run a fully-resolved named check in an arbitrary directory (a persistent
//! gate worktree) through [`crate::workflow_exec::WorkflowEngine::run_check_in`]
//! without any agent worktree or workflow context. The test below is that
//! interface's compile-and-behavior proof from outside `workflow_exec`.

#[cfg(test)]
mod tests {
    use crate::supervisor::Supervisor;
    use crate::tickets::Tickets;
    use crate::workflow_exec::{OnTimeout, ResolvedRun, WorkflowEngine};
    use rk_core::paths::Layout;
    use rk_space::Space;
    use rk_workflow::TierRouting;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    /// The T1->T2 interface, consumed from the module T2 will live in: build
    /// the resolved-run input shape by hand, point it at a bare directory (a
    /// stand-in for the persistent gate worktree), and run the extracted
    /// check runner with no agent and no workflow context.
    #[tokio::test]
    async fn landing_consumer_runs_a_check_in_a_bare_gate_dir() {
        let home = tempfile::tempdir().unwrap();
        let layout = Layout::at(home.path());
        let space = Space::open_in_memory().unwrap();
        let tickets = Arc::new(Tickets::new(space.clone(), "castle".into()));
        let supervisor = Arc::new(
            Supervisor::new(
                layout.clone(),
                "castle".into(),
                "fake".into(),
                rk_ledger::Budget::default(),
                rk_ledger::FleetBudget::default(),
                space.clone(),
                tickets.clone(),
            )
            .unwrap(),
        );
        let engine = WorkflowEngine::new(
            layout,
            supervisor,
            space,
            tickets,
            HashMap::new(),
            TierRouting::default(),
            "fake".into(),
            false,
            true,
            false,
            Vec::new(),
            Vec::new(),
            0,
        );

        let gate_dir = tempfile::tempdir().unwrap();
        std::fs::write(gate_dir.path().join("candidate.txt"), "tip\n").unwrap();
        let resolved = ResolvedRun {
            command: "cat candidate.txt".into(),
            cwd: None,
            expect_exit: Some(0),
            timeout: "5s".into(),
            on_timeout: OnTimeout::Fail,
            environment_policy: rk_workflow::CheckEnvironmentPolicy::StripRkSpawn,
            retry_on_fail: 0,
        };
        let result = engine
            .run_check_in(
                "landing-t1-interface",
                "/repo",
                "daemon",
                gate_dir.path(),
                &resolved.command,
                &resolved,
                &[],
                Duration::from_secs(5),
                None,
            )
            .await
            .unwrap();
        assert_eq!(result["verdict"], "pass");
        assert_eq!(result["exit"], 0);
        assert!(result["stdout"].as_str().unwrap().contains("tip"));
    }
}
