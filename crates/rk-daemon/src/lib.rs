//! rat-kingdom daemon: NDJSON-over-UDS server hosting the tuplespace, plus the
//! client used by `rk`.

pub mod action_approval;
pub mod agent_log;
pub mod agents;
pub mod attention;
pub mod authority;
pub mod client;
pub mod coordinator;
pub mod cron;
pub mod drain;
pub mod factory_analytics;
pub mod factory_events;
pub mod inbox;
pub mod ingest_auth;
mod landing;
mod landing_conflict;
mod landing_review_retry;
mod landing_rework;
pub mod machine;
pub mod onboarding;
pub mod onboarding_activation;
pub mod onboarding_apply;
pub mod onboarding_proposals;
pub mod onboarding_sessions;
pub mod orchestrator_lease;
pub mod proto;
pub mod reactor;
pub mod read_only_roles;
pub mod reconcile;
pub mod reconcile_repair;
pub mod recovery;
pub mod repos;
pub mod scheduler;
pub mod server;
pub mod steer;
pub mod supervisor;
pub mod sync;
pub mod tickets;
pub mod workflow_exec;

pub use client::{Client, ClientRpcError, WatchStream};
pub use server::Daemon;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{Request, Response};
    use rk_core::paths::Layout;
    use serde_json::json;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;

    async fn start_daemon() -> (
        tempfile::TempDir,
        Layout,
        tokio::task::JoinHandle<rk_core::Result<()>>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        let handle = tokio::spawn(daemon.run());
        (dir, layout, handle)
    }

    async fn start_daemon_with_config(
        config: rk_core::config::Config,
    ) -> (
        tempfile::TempDir,
        Layout,
        tokio::task::JoinHandle<rk_core::Result<()>>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let daemon = Daemon::new(layout.clone(), &config).unwrap();
        let handle = tokio::spawn(daemon.run());
        (dir, layout, handle)
    }

    async fn raw_request(layout: &Layout, req: Request) -> Response {
        let stream = loop {
            match UnixStream::connect(layout.socket_path()).await {
                Ok(stream) => break stream,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        };
        let mut stream = BufReader::new(stream);
        let mut line = serde_json::to_vec(&req).unwrap();
        line.push(b'\n');
        stream.get_mut().write_all(&line).await.unwrap();
        let mut response = String::new();
        stream.read_line(&mut response).await.unwrap();
        serde_json::from_str(&response).unwrap()
    }

    /// Always an explicit operator connection: an ambient `RK_AGENT` (every
    /// test run inside a supervised rat has one) would otherwise make these
    /// tests speak as that rat and be refused the operator-only methods.
    async fn connect(layout: &Layout) -> Client {
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if let Ok(c) = Client::connect_as_operator(layout).await {
                return c;
            }
        }
        panic!("daemon did not come up");
    }

    mod sdlc_ingest_auth {
        use super::*;

        fn source(name: &str) -> rk_core::config::IngestSourceConfig {
            rk_core::config::IngestSourceConfig {
                name: name.into(),
                allowed_kinds: vec!["ci_failed".into()],
                ..Default::default()
            }
        }

        fn envelope(source: &str) -> serde_json::Value {
            json!({
                "kind": "ci_failed",
                "source": source,
                "delivery_id": "delivery-1",
                "occurred_at": "2026-08-13T00:00:00Z",
                "observed_at": "2026-08-13T00:00:01Z",
                "correlation": {
                    "repo": "repo",
                    "branch": "main",
                    "workflow": "ci",
                    "job": "test",
                    "commit_sha": "abc123"
                },
                "summary": "ci failed",
                "refs": [],
                "attributes": {},
                "payload": {"type": "ci", "status": "failed", "conclusion": "failure"}
            })
        }

        fn config_with_source(
            source: rk_core::config::IngestSourceConfig,
        ) -> rk_core::config::Config {
            let mut config = rk_core::config::Config::default();
            config.ingest.sources = vec![source];
            config
        }

        #[test]
        fn test_default_config_has_no_public_ingest_listener() {
            let config = rk_core::config::Config::default();
            assert!(config.ingest.sources.is_empty());
            assert_eq!(config.ingest.max_state_limit, 1_000);
        }

        #[test]
        fn test_ingest_source_must_be_enabled_to_resolve() {
            let mut src = source("probe");
            src.enabled = false;
            let config = config_with_source(src);
            let root = "root";
            assert!(crate::ingest_auth::SourcePrincipal::from_request(
                &config.ingest,
                root,
                &crate::ingest_auth::source_caller("probe"),
                &crate::ingest_auth::derive_source_token(root, "probe"),
            )
            .is_none());
        }

        #[test]
        fn test_requested_source_name_resolves_to_configured_source_principal() {
            let config = config_with_source(source("probe"));
            let root = "root";
            let principal = crate::ingest_auth::SourcePrincipal::from_request(
                &config.ingest,
                root,
                &crate::ingest_auth::source_caller("probe"),
                &crate::ingest_auth::derive_source_token(root, "probe"),
            )
            .unwrap();
            assert_eq!(principal.name, "probe");
        }

        #[test]
        fn test_source_token_required_for_non_operator_source_mode() {
            let config = config_with_source(source("probe"));
            assert!(crate::ingest_auth::SourcePrincipal::from_request(
                &config.ingest,
                "root",
                &crate::ingest_auth::source_caller("probe"),
                "root",
            )
            .is_none());
        }

        #[test]
        fn test_inline_principal_is_rejected() {
            let config = config_with_source(source("probe"));
            let root = "root";
            let principal = crate::ingest_auth::SourcePrincipal::from_request(
                &config.ingest,
                root,
                &crate::ingest_auth::source_caller("probe"),
                &crate::ingest_auth::derive_source_token(root, "probe"),
            )
            .unwrap();
            let params =
                serde_json::from_value(json!({"source":"other", "envelope": envelope("probe")}))
                    .unwrap();
            assert!(crate::ingest_auth::validate_event(&principal, &params).is_err());
        }

        #[tokio::test]
        async fn test_server_allowlist_accepts_only_ingest_methods_for_source_tokens() {
            let (_dir, layout, handle) =
                start_daemon_with_config(config_with_source(source("probe"))).await;
            let token =
                crate::ingest_auth::derive_source_token(&layout.auth_token().unwrap(), "probe");
            let caller = crate::ingest_auth::source_caller("probe");
            let ok = raw_request(
                &layout,
                Request {
                    id: "ok".into(),
                    method: "ingest.event".into(),
                    auth: token.clone(),
                    caller: caller.clone(),
                    client_version: None,
                    params: json!({"source":"probe", "envelope": envelope("probe")}),
                },
            )
            .await;
            assert!(ok.error.is_none(), "source ingest should validate: {ok:?}");
            assert_eq!(ok.result.as_ref().unwrap()["accepted"], true);
            let scoped = raw_request(
                &layout,
                Request {
                    id: "scope".into(),
                    method: "ingest.event".into(),
                    auth: token.clone(),
                    caller: caller.clone(),
                    client_version: None,
                    params: json!({"source":"probe", "envelope": envelope("probe"), "scope":"repo"}),
                },
            )
            .await;
            assert_eq!(scoped.error.unwrap().code, crate::proto::codes::BAD_PARAMS);
            let denied = raw_request(
                &layout,
                Request {
                    id: "deny".into(),
                    method: "space.out".into(),
                    auth: token,
                    caller,
                    client_version: None,
                    params: json!({"category":"event", "identity":"bad"}),
                },
            )
            .await;
            assert_eq!(denied.error.unwrap().code, crate::proto::codes::FORBIDDEN);
            let mut operator = connect(&layout).await;
            operator.call("stop", json!({})).await.unwrap();
            handle.await.unwrap().unwrap();
        }

        #[tokio::test]
        async fn test_ingest_event_requires_local_authenticated_client() {
            let (_dir, layout, handle) =
                start_daemon_with_config(config_with_source(source("probe"))).await;
            let response = raw_request(
                &layout,
                Request {
                    id: "bad-auth".into(),
                    method: "ingest.event".into(),
                    auth: layout.auth_token().unwrap(),
                    caller: crate::ingest_auth::source_caller("probe"),
                    client_version: None,
                    params: json!({"source":"probe", "envelope": envelope("probe")}),
                },
            )
            .await;
            assert_eq!(
                response.error.unwrap().code,
                crate::proto::codes::UNAUTHORIZED
            );
            let mut operator = connect(&layout).await;
            operator.call("stop", json!({})).await.unwrap();
            handle.await.unwrap().unwrap();
        }

        #[tokio::test]
        async fn test_ingest_event_rejects_unknown_source() {
            let (_dir, layout, handle) =
                start_daemon_with_config(config_with_source(source("probe"))).await;
            let response = raw_request(
                &layout,
                Request {
                    id: "unknown".into(),
                    method: "ingest.event".into(),
                    auth: crate::ingest_auth::derive_source_token(
                        &layout.auth_token().unwrap(),
                        "unknown",
                    ),
                    caller: crate::ingest_auth::source_caller("unknown"),
                    client_version: None,
                    params: json!({"source":"unknown", "envelope": envelope("unknown")}),
                },
            )
            .await;
            assert_eq!(
                response.error.unwrap().code,
                crate::proto::codes::UNAUTHORIZED
            );
            let mut operator = connect(&layout).await;
            operator.call("stop", json!({})).await.unwrap();
            handle.await.unwrap().unwrap();
        }

        #[tokio::test]
        async fn test_ingest_event_validates_before_persisting() {
            let (_dir, layout, handle) =
                start_daemon_with_config(config_with_source(source("probe"))).await;
            let token =
                crate::ingest_auth::derive_source_token(&layout.auth_token().unwrap(), "probe");
            let mut bad = envelope("probe");
            bad["summary"] = json!("");
            let response = raw_request(
                &layout,
                Request {
                    id: "invalid".into(),
                    method: "ingest.event".into(),
                    auth: token,
                    caller: crate::ingest_auth::source_caller("probe"),
                    client_version: None,
                    params: json!({"source":"probe", "envelope": bad}),
                },
            )
            .await;
            assert_eq!(response.error.unwrap().code, crate::proto::codes::FORBIDDEN);
            let mut operator = connect(&layout).await;
            let scan = operator
                .call("space.scan", json!({"category":"event"}))
                .await
                .unwrap();
            assert_eq!(scan["tuples"].as_array().unwrap().len(), 0);
            operator.call("stop", json!({})).await.unwrap();
            handle.await.unwrap().unwrap();
        }

        #[tokio::test]
        async fn test_ingest_event_returns_receipt_with_semantic_state_digest() {
            let (_dir, layout, handle) =
                start_daemon_with_config(config_with_source(source("probe"))).await;
            let result = ingest_probe(&layout).await;
            assert_eq!(result["accepted"], true);
            assert!(result["receipt"]["semantic_state_digest"]
                .as_str()
                .unwrap()
                .starts_with("sha256:"));
            let mut operator = connect(&layout).await;
            operator.call("stop", json!({})).await.unwrap();
            handle.await.unwrap().unwrap();
        }

        #[tokio::test]
        async fn test_ingest_event_projects_event_tuple() {
            let (_dir, layout, handle) =
                start_daemon_with_config(config_with_source(source("probe"))).await;
            let result = ingest_probe(&layout).await;
            let mut operator = connect(&layout).await;
            let scan = operator
                .call("space.scan", json!({"category":"event", "scope":"ci"}))
                .await
                .unwrap();
            let tuples = scan["tuples"].as_array().unwrap();
            assert!(tuples
                .iter()
                .any(|tuple| tuple["id"] == result["receipt"]["projected_event_id"]));
            operator.call("stop", json!({})).await.unwrap();
            handle.await.unwrap().unwrap();
        }

        #[tokio::test]
        async fn test_ingest_event_projects_current_fact_tuple() {
            let (_dir, layout, handle) =
                start_daemon_with_config(config_with_source(source("probe"))).await;
            let result = ingest_probe(&layout).await;
            let fact_id = result["receipt"]["projected_fact_ids"][0].clone();
            let mut source_client =
                Client::connect_as(&layout, &crate::ingest_auth::source_caller("probe"))
                    .await
                    .unwrap();
            let state = source_client
                .call("ingest.state", json!({"repo":"repo"}))
                .await
                .unwrap();
            assert!(state["facts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|fact| fact["id"] == fact_id));
            let mut operator = connect(&layout).await;
            operator.call("stop", json!({})).await.unwrap();
            handle.await.unwrap().unwrap();
        }

        #[tokio::test]
        async fn test_ingest_state_returns_current_facts_without_raw_payload() {
            let (_dir, layout, handle) =
                start_daemon_with_config(config_with_source(source("probe"))).await;
            ingest_probe(&layout).await;
            let mut source_client =
                Client::connect_as(&layout, &crate::ingest_auth::source_caller("probe"))
                    .await
                    .unwrap();
            let state = source_client
                .call("ingest.state", json!({"repo":"repo"}))
                .await
                .unwrap();
            let fact = &state["facts"].as_array().unwrap()[0];
            assert!(fact["payload"].get("current").is_some());
            assert!(fact["payload"].get("payload").is_none());
            let mut operator = connect(&layout).await;
            operator.call("stop", json!({})).await.unwrap();
            handle.await.unwrap().unwrap();
        }

        async fn ingest_probe(layout: &Layout) -> serde_json::Value {
            let response = raw_request(
                layout,
                Request {
                    id: "ingest-probe".into(),
                    method: "ingest.event".into(),
                    auth: crate::ingest_auth::derive_source_token(
                        &layout.auth_token().unwrap(),
                        "probe",
                    ),
                    caller: crate::ingest_auth::source_caller("probe"),
                    client_version: None,
                    params: json!({"source":"probe", "envelope": envelope("probe")}),
                },
            )
            .await;
            response.result.unwrap()
        }

        #[test]
        fn test_allowed_kinds_are_enforced_by_source() {
            let config = config_with_source(source("probe"));
            let root = "root";
            let principal = crate::ingest_auth::SourcePrincipal::from_request(
                &config.ingest,
                root,
                &crate::ingest_auth::source_caller("probe"),
                &crate::ingest_auth::derive_source_token(root, "probe"),
            )
            .unwrap();
            let params = serde_json::from_value(json!({"source":"probe", "envelope": {"kind": "ci_recovered", "source": "probe", "delivery_id": "delivery-2", "occurred_at": "2026-08-13T00:00:00Z", "observed_at": "2026-08-13T00:00:01Z", "correlation": {"repo": "repo", "branch": "main", "workflow": "ci", "job": "test", "commit_sha": "abc123"}, "summary": "ci recovered", "refs": [], "attributes": {}, "payload": {"type": "ci", "status": "passed", "conclusion": null}}})).unwrap();
            assert!(crate::ingest_auth::validate_event(&principal, &params).is_err());
        }

        #[test]
        fn test_source_limits_cannot_exceed_daemon_maximums() {
            let mut config = config_with_source(rk_core::config::IngestSourceConfig {
                name: "probe".into(),
                allowed_kinds: vec!["status".into()],
                max_state_limit: 99,
                max_summary_len: 999,
                max_refs: 999,
                max_attributes: 999,
                ..Default::default()
            });
            config.ingest.max_state_limit = 5;
            config.ingest.max_summary_len = 7;
            config.ingest.max_refs = 3;
            config.ingest.max_attributes = 2;
            let root = "root";
            let principal = crate::ingest_auth::SourcePrincipal::from_request(
                &config.ingest,
                root,
                &crate::ingest_auth::source_caller("probe"),
                &crate::ingest_auth::derive_source_token(root, "probe"),
            )
            .unwrap();
            assert_eq!(principal.max_state_limit(), 5);
            assert_eq!(principal.limits().max_summary_len, 7);
            assert_eq!(principal.limits().max_refs, 3);
            assert_eq!(principal.limits().max_attributes, 2);
        }
    }

    #[tokio::test]
    async fn ping_status_stop_round_trip() {
        let (_dir, layout, handle) = start_daemon().await;
        let mut client = connect(&layout).await;

        let pong = client.call("ping", json!({})).await.unwrap();
        assert_eq!(pong, json!("pong"));

        let status = client.call("status", json!({})).await.unwrap();
        assert_eq!(status["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(status["castle"], "test-castle");

        assert!(client.call("nope", json!({})).await.is_err());

        client.call("stop", json!({})).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("daemon did not stop")
            .unwrap();
        assert!(result.is_ok());
        assert!(!layout.socket_path().exists());
    }

    mod agent_list_frame_limits {
        //! TKT-01M036PSJCD31YBV7KZTKKQG19: `agent.list` with `include_archived`
        //! serializes the union of two views that are each individually kept
        //! under the frame cap by construction — nothing capped the *combined*
        //! reply, so a fleet large enough to blow past it made
        //! `write_json_line` return a bare `io::Error` that `serve_conn`
        //! propagated straight out, closing the socket with no wire response at
        //! all (`rk list --all` surfaced as `protocol: daemon closed
        //! connection`, unusable and undiagnosable). Reproduced here at a much
        //! smaller scale than the real castle (580 records) by padding each
        //! record's `stderr_tail`.
        use super::*;
        use crate::agents::{AgentRecord, AgentState, Registry};
        use rk_harness::TokenUsage;

        fn padded(name: &str, state: AgentState, tail_bytes: usize, stale: bool) -> AgentRecord {
            let now = chrono::Utc::now();
            AgentRecord {
                name: name.into(),
                spawn: None,
                role: "rat".into(),
                coordination: None,
                harness: "fake".into(),
                permission_mode: None,
                model: None,
                repo_root: "/tmp/repo".into(),
                repo_name: "repo".into(),
                task: Some("tkt-1".into()),
                branch: Some(format!("rat/{name}/tkt-1")),
                fork_point: None,
                worktree: Some(format!("/tmp/wt/{name}").into()),
                target_branch: "main".into(),
                parent: None,
                workflow_instance: None,
                review: None,
                coordinator: None,
                session_id: Some("sess".into()),
                attach_target: None,
                pid: None,
                merge_commit: None,
                state,
                crashed: false,
                stderr_tail: Some("x".repeat(tail_bytes)),
                result: Some("done".into()),
                progress: None,
                usage: TokenUsage::default(),
                cost_usd: 0.01,
                created_at: now,
                // Old enough for `Registry::archive` to sweep it up.
                updated_at: if stale {
                    now - chrono::TimeDelta::try_hours(2).unwrap()
                } else {
                    now
                },
                archived_at: None,
            }
        }

        /// Each of `rk list` and `rk list --archived` fits under the old 1 MiB
        /// frame cap on its own; their union does not. Before the fix this was
        /// exactly the reported repro: `agent.list {include_archived: true}`
        /// closed the connection instead of replying.
        #[tokio::test]
        async fn combined_reply_over_the_old_1mib_cap_still_succeeds() {
            let dir = tempfile::tempdir().unwrap();
            let layout = Layout::at(dir.path());
            let live_count = 20;
            let archived_count = 20;
            {
                let mut reg = Registry::load(&layout.home().join("agents.json")).unwrap();
                for i in 0..live_count {
                    reg.insert(padded(
                        &format!("live-{i}"),
                        AgentState::Running,
                        35_000,
                        false,
                    ))
                    .unwrap();
                }
                for i in 0..archived_count {
                    reg.insert(padded(
                        &format!("done-{i}"),
                        AgentState::Completed,
                        35_000,
                        true,
                    ))
                    .unwrap();
                }
                reg.archive(chrono::Utc::now() - chrono::TimeDelta::try_hours(1).unwrap())
                    .unwrap();
            }

            let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
            let _handle = tokio::spawn(daemon.run());
            let mut client = connect(&layout).await;

            let live = client.call("agent.list", json!({})).await.unwrap();
            assert_eq!(live["agents"].as_array().unwrap().len(), live_count);

            let archived = client
                .call("agent.list", json!({"archived_only": true}))
                .await
                .unwrap();
            assert_eq!(archived["agents"].as_array().unwrap().len(), archived_count);

            let all = client
                .call("agent.list", json!({"include_archived": true}))
                .await
                .expect("agent.list --all must not close the connection");
            assert_eq!(
                all["agents"].as_array().unwrap().len(),
                live_count + archived_count
            );
        }

        /// Even beyond the raised response cap, an oversized reply must come
        /// back as a clean `frame_too_large` error the client can report, not
        /// a dropped connection.
        #[tokio::test]
        async fn reply_over_the_new_cap_errors_cleanly_instead_of_closing() {
            let dir = tempfile::tempdir().unwrap();
            let layout = Layout::at(dir.path());
            {
                let mut reg = Registry::load(&layout.home().join("agents.json")).unwrap();
                // 6 * 4 MiB well clears the 16 MiB response cap.
                for i in 0..6 {
                    reg.insert(padded(
                        &format!("live-{i}"),
                        AgentState::Running,
                        4 * 1024 * 1024,
                        false,
                    ))
                    .unwrap();
                }
            }

            let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
            let _handle = tokio::spawn(daemon.run());
            let mut client = connect(&layout).await;

            let resp = client
                .call_raw("agent.list", json!({}))
                .await
                .expect("daemon must not close the connection on an oversized reply");
            let err = resp
                .error
                .expect("an oversized reply must come back as an error, not succeed");
            assert_eq!(err.code, crate::proto::codes::FRAME_TOO_LARGE);
        }
    }

    #[tokio::test]
    async fn agent_rpc_is_authenticated_and_instance_scoped() {
        let (_dir, layout, handle) = start_daemon().await;
        let agent_token = layout.agent_auth_token("Whisker").unwrap();
        let mut operator = connect(&layout).await;

        let mut stream = BufReader::new(UnixStream::connect(layout.socket_path()).await.unwrap());
        let forbidden = Request {
            id: "1".into(),
            method: "space.out".into(),
            auth: agent_token.clone(),
            caller: "Whisker".into(),
            client_version: None,
            params: json!({
                "category": "need",
                "scope": "repo",
                "identity": "help",
                "instance": "another-agent"
            }),
        };
        let mut line = serde_json::to_vec(&forbidden).unwrap();
        line.push(b'\n');
        stream.get_mut().write_all(&line).await.unwrap();
        let mut response = String::new();
        stream.read_line(&mut response).await.unwrap();
        let decoded: Response = serde_json::from_str(&response).unwrap();
        assert_eq!(decoded.error.unwrap().code, crate::proto::codes::FORBIDDEN);

        let denied_task = Request {
            id: "1-task".into(),
            method: "space.out".into(),
            auth: agent_token.clone(),
            caller: "Whisker".into(),
            client_version: None,
            params: json!({
                "category": "task",
                "scope": "repo",
                "identity": "operator-work"
            }),
        };
        let mut line = serde_json::to_vec(&denied_task).unwrap();
        line.push(b'\n');
        stream.get_mut().write_all(&line).await.unwrap();
        response.clear();
        stream.read_line(&mut response).await.unwrap();
        let decoded: Response = serde_json::from_str(&response).unwrap();
        assert_eq!(decoded.error.unwrap().code, crate::proto::codes::FORBIDDEN);

        let denied_event = Request {
            id: "1-event".into(),
            method: "space.out".into(),
            auth: agent_token.clone(),
            caller: "Whisker".into(),
            client_version: None,
            params: json!({
                "category": "event",
                "scope": "repo",
                "identity": "workflow_approval",
                "payload": {"instance": "wf-other", "approved": true}
            }),
        };
        let mut line = serde_json::to_vec(&denied_event).unwrap();
        line.push(b'\n');
        stream.get_mut().write_all(&line).await.unwrap();
        response.clear();
        stream.read_line(&mut response).await.unwrap();
        let decoded: Response = serde_json::from_str(&response).unwrap();
        assert_eq!(decoded.error.unwrap().code, crate::proto::codes::FORBIDDEN);

        for (request_id, method, params) in [
            (
                "1-update",
                "ticket.update",
                json!({"id": "TKT-01ARZ3NDEKTSV4RRFFQ69G5FAV", "status": "done"}),
            ),
            (
                "1-dep",
                "ticket.dep",
                json!({"id": "TKT-01ARZ3NDEKTSV4RRFFQ69G5FAV", "dep": "TKT-01ARZ3NDEKTSV4RRFFQ69G5FAV"}),
            ),
        ] {
            let denied = Request {
                id: request_id.into(),
                method: method.into(),
                auth: agent_token.clone(),
                caller: "Whisker".into(),
                client_version: None,
                params,
            };
            let mut line = serde_json::to_vec(&denied).unwrap();
            line.push(b'\n');
            stream.get_mut().write_all(&line).await.unwrap();
            response.clear();
            stream.read_line(&mut response).await.unwrap();
            let decoded: Response = serde_json::from_str(&response).unwrap();
            assert_eq!(decoded.error.unwrap().code, crate::proto::codes::FORBIDDEN);
        }

        let allowed = Request {
            id: "2".into(),
            method: "space.out".into(),
            auth: agent_token.clone(),
            caller: "Whisker".into(),
            client_version: None,
            params: json!({"category": "need", "scope": "repo", "identity": "help"}),
        };
        let mut line = serde_json::to_vec(&allowed).unwrap();
        line.push(b'\n');
        stream.get_mut().write_all(&line).await.unwrap();
        response.clear();
        stream.read_line(&mut response).await.unwrap();
        let decoded: Response = serde_json::from_str(&response).unwrap();
        assert!(decoded.error.is_none());

        assert_eq!(
            operator.call("ping", json!({})).await.unwrap(),
            json!("pong")
        );
        operator.call("stop", json!({})).await.unwrap();
        handle.await.unwrap().unwrap();
    }

    /// TKT-182: an operator-only method must succeed on a connection whose
    /// caller is named by the code, whatever `RK_AGENT` says about the process
    /// running the test — and must still be refused for a real agent caller.
    ///
    /// This is the end-to-end shape of the bug: every test above reaches the
    /// daemon through `connect`, which used to resolve identity from the
    /// environment. Inside a rat that env names the rat, so the whole
    /// daemon-backed suite failed with `forbidden: <Agent> is not authorized`
    /// — and `cargo test --workspace`, the command the completion protocol
    /// tells every rat to verify with, could only pass in an operator shell.
    #[tokio::test]
    async fn explicit_callers_decide_authority_not_the_environment() {
        let (_dir, layout, _handle) = start_daemon().await;
        let snapshot_of = json!({"repo": "myrepo", "instance": "wf-1"});

        // `coordinator.snapshot` is on Server::authorized's operator-only list.
        let mut operator = connect(&layout).await;
        let snapshot = operator
            .call("coordinator.snapshot", snapshot_of.clone())
            .await
            .unwrap();
        assert!(snapshot["snapshot"]["workflows"].is_array());

        // Production semantics are untouched: naming an agent caller still
        // authenticates it and still refuses it the operator-only method.
        let mut agent = Client::connect_as(&layout, "Whisker").await.unwrap();
        let err = agent
            .call("coordinator.snapshot", snapshot_of)
            .await
            .expect_err("an agent must not read the coordinator snapshot");
        assert!(
            err.to_string().contains("forbidden"),
            "expected a forbidden error, got: {err}"
        );
        // ...while a method agents *are* allowed still works on that same
        // connection, so the refusal above is about authority, not a broken
        // token.
        assert_eq!(agent.call("ping", json!({})).await.unwrap(), json!("pong"));
    }

    #[tokio::test]
    async fn configured_source_auth_and_ingest_allowlist() {
        fn envelope(source: &str, kind: &str) -> serde_json::Value {
            json!({
                "kind": kind,
                "source": source,
                "delivery_id": format!("delivery-{kind}"),
                "occurred_at": "2026-08-13T00:00:00Z",
                "observed_at": "2026-08-13T00:00:01Z",
                "correlation": {
                    "repo": "repo",
                    "branch": "main",
                    "workflow": "ci",
                    "job": "test",
                    "commit_sha": "abc123"
                },
                "summary": format!("{kind} from configured source"),
                "refs": [],
                "attributes": {},
                "payload": {"type": "ci", "status": "failed", "conclusion": "failure"}
            })
        }

        let mut config = rk_core::config::Config::default();
        config.ingest.max_state_limit = 5;
        config.ingest.sources = vec![
            rk_core::config::IngestSourceConfig {
                name: "probe".into(),
                enabled: true,
                allowed_kinds: vec!["ci_failed".into()],
                max_state_limit: 99,
                ..Default::default()
            },
            rk_core::config::IngestSourceConfig {
                name: "disabled".into(),
                enabled: false,
                allowed_kinds: vec!["ci_failed".into()],
                max_state_limit: 5,
                ..Default::default()
            },
        ];
        let (_dir, layout, handle) = start_daemon_with_config(config).await;
        let token = crate::ingest_auth::derive_source_token(&layout.auth_token().unwrap(), "probe");
        let caller = crate::ingest_auth::source_caller("probe");

        let ok = raw_request(
            &layout,
            Request {
                id: "ingest-ok".into(),
                method: "ingest.event".into(),
                auth: token.clone(),
                caller: caller.clone(),
                client_version: None,
                params: json!({"source":"probe", "envelope": envelope("probe", "ci_failed")}),
            },
        )
        .await;
        assert!(ok.error.is_none(), "ingest.event should validate: {ok:?}");
        let result = ok.result.unwrap();
        assert_eq!(result["accepted"], true);
        assert!(result["receipt"]["semantic_state_digest"]
            .as_str()
            .unwrap()
            .starts_with("sha256:"));

        let mut operator_for_scan = connect(&layout).await;
        let scan = operator_for_scan
            .call("space.scan", json!({"category": "event"}))
            .await
            .unwrap();
        let projected_tuple_count = scan["tuples"].as_array().unwrap().len();
        assert!(
            !scan["tuples"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tuple| tuple["payload"] == json!({"ok": true})),
            "ingest.event must not persist raw source payloads as generic tuples"
        );

        let state = raw_request(
            &layout,
            Request {
                id: "state".into(),
                method: "ingest.state".into(),
                auth: token.clone(),
                caller: caller.clone(),
                client_version: None,
                params: json!({"repo": "repo", "limit": 5}),
            },
        )
        .await;
        assert!(
            state.error.is_none(),
            "ingest.state should succeed: {state:?}"
        );
        let state_result = state.result.unwrap();
        assert_eq!(state_result["facts"].as_array().unwrap().len(), 1);

        let unknown_state_field = raw_request(
            &layout,
            Request {
                id: "state-unknown-field".into(),
                method: "ingest.state".into(),
                auth: token.clone(),
                caller: caller.clone(),
                client_version: None,
                params: json!({"repo": "repo", "limit": 5, "principal": "operator"}),
            },
        )
        .await;
        assert_eq!(
            unknown_state_field.error.unwrap().code,
            crate::proto::codes::BAD_PARAMS,
            "ingest.state must reject unexpected fields before any state/tuple writes"
        );
        let after_rejected_state = operator_for_scan
            .call("space.scan", json!({"category": "event"}))
            .await
            .unwrap();
        assert_eq!(
            after_rejected_state["tuples"].as_array().unwrap().len(),
            projected_tuple_count,
            "rejected ingest.state requests must not persist tuple/state writes"
        );

        let cases = [
            (
                "spoofed-token",
                "ingest.event",
                caller.clone(),
                layout.auth_token().unwrap(),
                json!({"source":"probe", "envelope": envelope("probe", "ci_failed")}),
                crate::proto::codes::UNAUTHORIZED,
            ),
            (
                "inline-principal",
                "ingest.event",
                caller.clone(),
                token.clone(),
                json!({"source":"probe", "envelope": envelope("probe", "ci_failed"), "principal": "operator"}),
                crate::proto::codes::BAD_PARAMS,
            ),
            (
                "disabled-source",
                "ingest.event",
                crate::ingest_auth::source_caller("disabled"),
                crate::ingest_auth::derive_source_token(&layout.auth_token().unwrap(), "disabled"),
                json!({"source":"disabled", "envelope": envelope("disabled", "ci_failed")}),
                crate::proto::codes::UNAUTHORIZED,
            ),
            (
                "unknown-source",
                "ingest.event",
                crate::ingest_auth::source_caller("unknown"),
                crate::ingest_auth::derive_source_token(&layout.auth_token().unwrap(), "unknown"),
                json!({"source":"unknown", "envelope": envelope("unknown", "ci_failed")}),
                crate::proto::codes::UNAUTHORIZED,
            ),
            (
                "disallowed-kind",
                "ingest.event",
                caller.clone(),
                token.clone(),
                json!({"source":"probe", "envelope": envelope("probe", "ci_recovered")}),
                crate::proto::codes::FORBIDDEN,
            ),
            (
                "excessive-limit",
                "ingest.state",
                caller.clone(),
                token.clone(),
                json!({"repo": "repo", "limit": 6}),
                crate::proto::codes::BAD_PARAMS,
            ),
            (
                "source-mutates-workflow",
                "workflow.run",
                caller.clone(),
                token.clone(),
                json!({}),
                crate::proto::codes::FORBIDDEN,
            ),
            (
                "source-mutates-space",
                "space.out",
                caller.clone(),
                token.clone(),
                json!({"category":"event", "identity":"bad"}),
                crate::proto::codes::FORBIDDEN,
            ),
            (
                "normal-rat-no-source",
                "ingest.event",
                "Whisker".into(),
                layout.agent_auth_token("Whisker").unwrap(),
                json!({"source":"probe", "envelope": envelope("probe", "ci_failed")}),
                crate::proto::codes::FORBIDDEN,
            ),
        ];
        for (id, method, caller, auth, params, code) in cases {
            let response = raw_request(
                &layout,
                Request {
                    id: id.into(),
                    method: method.into(),
                    auth,
                    caller,
                    client_version: None,
                    params,
                },
            )
            .await;
            assert_eq!(response.error.unwrap().code, code, "case {id}");
        }

        let mut operator = connect(&layout).await;
        assert_eq!(
            operator.call("ping", json!({})).await.unwrap(),
            json!("pong")
        );
        operator.call("stop", json!({})).await.unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn space_out_scan_take_over_the_wire() {
        let (_dir, layout, _handle) = start_daemon().await;
        let mut client = connect(&layout).await;

        let written = client
            .call(
                "space.out",
                json!({
                    "category": "event",
                    "scope": "myrepo",
                    "identity": "task_done",
                    "payload": {"agent": "Whisker"},
                }),
            )
            .await
            .unwrap();
        assert_eq!(written["written"], true);

        let scanned = client
            .call(
                "space.scan",
                json!({"category": "event", "scope": "myrepo"}),
            )
            .await
            .unwrap();
        assert_eq!(scanned["tuples"].as_array().unwrap().len(), 1);
        // Instance defaulted to the castle name.
        assert_eq!(scanned["tuples"][0]["instance"], "test-castle");

        let taken = client
            .call(
                "space.take",
                json!({"category": "event", "identity": "task_done", "timeout_ms": 500}),
            )
            .await
            .unwrap();
        assert_eq!(taken["tuple"]["payload"]["agent"], "Whisker");

        let empty = client
            .call("space.scan", json!({"category": "event"}))
            .await
            .unwrap();
        assert_eq!(empty["tuples"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn blocked_take_over_the_wire_wakes_on_out() {
        let (_dir, layout, _handle) = start_daemon().await;
        let mut taker = connect(&layout).await;
        let mut writer = connect(&layout).await;

        let take_task = tokio::spawn(async move {
            taker
                .call(
                    "space.take",
                    json!({"category": "need", "identity": "help", "timeout_ms": 5000}),
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        writer
            .call(
                "space.out",
                json!({"category": "need", "scope": "myrepo", "identity": "help"}),
            )
            .await
            .unwrap();
        let taken = take_task.await.unwrap().unwrap();
        assert_eq!(taken["tuple"]["identity"], "help");
    }

    #[tokio::test]
    async fn watch_streams_matching_tuples() {
        let (_dir, layout, _handle) = start_daemon().await;
        let watcher = connect(&layout).await;
        let mut writer = connect(&layout).await;

        let mut stream = watcher.watch(json!({"scope": "watched"})).await.unwrap();

        writer
            .call(
                "space.out",
                json!({"category": "event", "scope": "ignored", "identity": "x"}),
            )
            .await
            .unwrap();
        writer
            .call(
                "space.out",
                json!({"category": "event", "scope": "watched", "identity": "y"}),
            )
            .await
            .unwrap();

        let note = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("watch timed out")
            .unwrap()
            .expect("stream closed");
        assert_eq!(note["method"], "tuple");
        assert_eq!(note["params"]["scope"], "watched");
        assert_eq!(note["params"]["identity"], "y");
    }

    #[tokio::test]
    async fn inbox_aggregates_and_ranks_over_the_wire() {
        let (_dir, layout, _handle) = start_daemon().await;
        let mut client = connect(&layout).await;

        // A budget-exceeded obstacle and a plain need, written straight to the
        // space, are the two attention items with no agents/workflows around.
        client
            .call(
                "space.out",
                json!({
                    "category": "obstacle",
                    "scope": "myrepo",
                    "identity": "Nibbles",
                    "payload": {"type": "budget_exceeded", "cost_usd": 3.5, "tokens": 800000},
                }),
            )
            .await
            .unwrap();
        client
            .call(
                "space.out",
                json!({
                    "category": "need",
                    "scope": "myrepo",
                    "identity": "Scamper",
                    "payload": {"text": "need a reviewer"},
                }),
            )
            .await
            .unwrap();

        let inbox = client.call("inbox.list", json!({})).await.unwrap();
        let items = inbox["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        // Budget obstacle outranks the need and carries its resolving command.
        assert_eq!(items[0]["kind"], "obstacle");
        assert_eq!(items[0]["action"], "rk status Nibbles");
        assert_eq!(items[1]["kind"], "need");
    }

    #[tokio::test]
    async fn inbox_repo_filter_applies_before_response_truncation() {
        let (_dir, layout, _handle) = start_daemon().await;
        let mut client = connect(&layout).await;

        for index in 0..2_049 {
            client
                .call(
                    "space.out",
                    json!({
                        "category": "obstacle",
                        "scope": "other-repo",
                        "identity": format!("other-{index}"),
                        "payload": {"text": "higher-priority unrelated row"},
                    }),
                )
                .await
                .unwrap();
        }
        client
            .call(
                "space.out",
                json!({
                    "category": "need",
                    "scope": "target-repo",
                    "identity": "target-human-review",
                    "payload": {"text": "target repository row"},
                }),
            )
            .await
            .unwrap();

        let inbox = client
            .call("inbox.list", json!({"repo": "target-repo"}))
            .await
            .unwrap();
        let items = inbox["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{items:?}");
        assert_eq!(items[0]["subject"], "target-human-review");
        assert!(!inbox["truncated"].as_bool().unwrap());
    }

    /// TKT-167: an open ballot reaches the operator over the wire, tallied
    /// against the reactor's own configured quorum. Without this the whole norms
    /// program is invisible — the only endorser who is always reachable never
    /// learns a vote was open, and nothing else in the fleet announces one.
    ///
    /// The ballot below is written Ephemeral (`ttl_secs`) on purpose even though
    /// TKT-168 made that the non-default: it is the legacy shape still sitting in
    /// live spaces, and the row has to keep rendering for it. The durable shape
    /// is pinned separately by `a_ballot_written_without_a_window_is_durable`.
    #[tokio::test]
    async fn open_suggestion_surfaces_in_the_inbox_over_the_wire() {
        let (_dir, layout, _handle) = start_daemon().await;
        let mut client = connect(&layout).await;

        // A system-scope Suggestion authored by the proposing agent, carrying the
        // voting window `rk suggest` used to apply by default (TKT-168 dropped
        // it; `--ttl` and legacy tuples still produce this shape).
        client
            .call(
                "space.out",
                json!({
                    "category": "suggestion",
                    "scope": "system",
                    "identity": "sug-8nsqa4132x",
                    "instance": "rat-28",
                    "payload": {"agent": "rat-28", "text": "a pre-existing failure is a ticket"},
                    "ttl_secs": 86400,
                }),
            )
            .await
            .unwrap();
        // ... and what `rk endorse` writes: one vote from a distinct agent.
        client
            .call(
                "space.out",
                json!({
                    "category": "endorsement",
                    "scope": "system",
                    "identity": "sug-8nsqa4132x",
                    "instance": "rat-36",
                    "payload": {"agent": "rat-36", "suggestion": "sug-8nsqa4132x"},
                    "ttl_secs": 86400,
                }),
            )
            .await
            .unwrap();

        let inbox = client.call("inbox.list", json!({})).await.unwrap();
        let items = inbox["items"].as_array().unwrap();
        assert_eq!(items.len(), 1, "{items:?}");
        assert_eq!(items[0]["kind"], "open-suggestion");
        assert_eq!(items[0]["subject"], "sug-8nsqa4132x");
        assert_eq!(items[0]["action"], "rk endorse sug-8nsqa4132x");
        // Tallied against the configured quorum, not a number invented here.
        let detail = items[0]["detail"].as_str().unwrap();
        assert!(
            detail.starts_with("1/3 endorsers (23h"),
            "unexpected detail: {detail}"
        );
        assert!(detail.contains("rat-28 proposes: a pre-existing failure"));
    }

    #[tokio::test]
    async fn ttl_writes_become_ephemeral() {
        let (_dir, layout, _handle) = start_daemon().await;
        let mut client = connect(&layout).await;
        client
            .call(
                "space.out",
                json!({
                    "category": "claim",
                    "scope": "myrepo",
                    "identity": "task-1",
                    "ttl_secs": 3600,
                }),
            )
            .await
            .unwrap();
        let scanned = client
            .call("space.scan", json!({"category": "claim"}))
            .await
            .unwrap();
        assert_eq!(scanned["tuples"][0]["lifecycle"], "ephemeral");
        assert!(scanned["tuples"][0]["expires_at"].is_string());
    }

    #[tokio::test]
    async fn coordinator_watch_replays_from_cursor_and_delivers_live_events() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        let space = daemon.space_handle();
        let _handle = tokio::spawn(daemon.run());

        let first_cursor = space
            .out_coordinator(
                rk_core::tuple::Tuple::new(
                    rk_core::tuple::Category::Event,
                    "myrepo",
                    "workflow_state_changed",
                    "daemon",
                    json!({"instance": "wf-1", "revision": 1, "status": "running"}),
                )
                .with_lifecycle(rk_core::tuple::Lifecycle::Furniture),
            )
            .unwrap();

        space
            .out_coordinator(
                rk_core::tuple::Tuple::new(
                    rk_core::tuple::Category::Event,
                    "myrepo",
                    "workflow_state_changed",
                    "daemon",
                    json!({"instance": "wf-1", "revision": 2, "status": "completed"}),
                )
                .with_lifecycle(rk_core::tuple::Lifecycle::Furniture),
            )
            .unwrap();

        let watcher = connect(&layout).await;
        let (initial, mut stream) = watcher
            .call_then_stream(
                "coordinator.watch",
                json!({"repo": "myrepo", "instance": "wf-1", "after": first_cursor}),
            )
            .await
            .unwrap();
        assert_eq!(initial["events"].as_array().unwrap().len(), 1);
        assert_eq!(initial["events"][0]["event"]["payload"]["revision"], 2);
        assert_eq!(
            initial["snapshot"]["workflows"].as_array().unwrap().len(),
            0
        );
        assert_eq!(initial["resync_required"], false);

        space
            .out_coordinator(
                rk_core::tuple::Tuple::new(
                    rk_core::tuple::Category::Event,
                    "myrepo",
                    "workflow_state_changed",
                    "daemon",
                    json!({"instance": "wf-1", "revision": 3, "status": "failed"}),
                )
                .with_lifecycle(rk_core::tuple::Lifecycle::Furniture),
            )
            .unwrap();
        let note = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("coordinator event did not arrive")
            .unwrap()
            .unwrap();
        assert_eq!(note["method"], "coordinator.event");
        assert_eq!(note["params"]["event"]["payload"]["revision"], 3);
    }

    #[tokio::test]
    async fn coordinator_pending_replays_until_acknowledged() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
        let space = daemon.space_handle();
        let _handle = tokio::spawn(daemon.run());
        let mut client = connect(&layout).await;
        client
            .call("coordinator.register", json!({"coordinator": "session-1"}))
            .await
            .unwrap();
        let cursor = space
            .out_coordinator(rk_core::tuple::Tuple::new(
                rk_core::tuple::Category::Event,
                "myrepo",
                "coordination_attention",
                "daemon",
                json!({
                    "coordinator": "session-1",
                    "workflow_instance": "wf-1",
                    "route": "escalate",
                    "summary": "middle-rat needs a decision"
                }),
            ))
            .unwrap();
        let pending = client
            .call(
                "coordinator.pending",
                json!({"coordinator": "session-1", "scope": "owned"}),
            )
            .await
            .unwrap();
        assert_eq!(pending["events"].as_array().unwrap().len(), 1);
        assert_eq!(pending["events"][0]["cursor"], cursor);
        client
            .call(
                "coordinator.ack",
                json!({"coordinator": "session-1", "cursor": cursor}),
            )
            .await
            .unwrap();
        let pending = client
            .call(
                "coordinator.pending",
                json!({"coordinator": "session-1", "scope": "owned"}),
            )
            .await
            .unwrap();
        assert_eq!(pending["events"].as_array().unwrap().len(), 0);
    }
}
