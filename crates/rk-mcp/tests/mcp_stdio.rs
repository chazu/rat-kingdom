use rk_core::paths::Layout;
use rk_daemon::{Client, Daemon};
use serde_json::{json, Value};
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::Duration;

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

const WORKFLOW: &str = r#"
workflow: {
    name: "factory-test"
    params: { taskId: {type: "string", required: true} }
    agents: { default: {harness: "fake", model: "sonnet"} }
    steps: [
        {type: "spawn", role: "rat", task: {title: _input.taskId, description: "do " + _input.taskId}},
        {type: "wait", timeout: "30s"},
        {type: "evaluate", expect: {is_error: false}},
        {type: "dismiss"},
    ]
}
"#;

async fn setup() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    Layout,
    tokio::task::JoinHandle<rk_core::Result<()>>,
) {
    let home = tempfile::tempdir().unwrap();
    let repo_dir = tempfile::tempdir().unwrap();
    git(repo_dir.path(), &["init", "-b", "main"]);
    git(repo_dir.path(), &["config", "user.email", "r@x"]);
    git(repo_dir.path(), &["config", "user.name", "R"]);
    std::fs::write(repo_dir.path().join("README.md"), "# x\n").unwrap();
    let wf_dir = repo_dir.path().join(".rk/workflows");
    std::fs::create_dir_all(&wf_dir).unwrap();
    std::fs::write(wf_dir.join("factory-test.cue"), WORKFLOW).unwrap();
    git(repo_dir.path(), &["add", "."]);
    git(repo_dir.path(), &["commit", "-m", "init"]);
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut client = connect(&layout).await;
    client
        .call(
            "repo.add",
            json!({"name":"repo-a", "path": repo_dir.path()}),
        )
        .await
        .unwrap();
    (home, repo_dir, layout, handle)
}

async fn mcp_call(layout: &Layout, id: u64, name: &str, arguments: Value) -> Value {
    let req = rk_mcp::JsonRpcRequest {
        jsonrpc: Some("2.0".into()),
        id: json!(id),
        method: "tools/call".into(),
        params: json!({"name": name, "arguments": arguments}),
    };
    let mut connect = || async { Client::connect_as_operator(layout).await };
    serde_json::to_value(rk_mcp::handle_request(req, &mut connect).await).unwrap()
}

fn run_rk_mcp_stdio(input: &[u8]) -> Output {
    let home = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_rk-mcp"))
        .env("RK_HOME", home.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.as_mut().unwrap().write_all(input).unwrap();
    drop(child.stdin.take());
    child.wait_with_output().unwrap()
}

fn stdout_json_lines(output: &Output) -> Vec<Value> {
    String::from_utf8(output.stdout.clone())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn initialize_and_tools_list_expose_exact_tool_set() {
    let init_req = rk_mcp::JsonRpcRequest {
        jsonrpc: Some("2.0".into()),
        id: json!(0),
        method: "initialize".into(),
        params: json!({}),
    };
    let mut init_connect =
        || async { Err::<Client, _>(rk_core::Error::other("initialize connected")) };
    let init_response =
        serde_json::to_value(rk_mcp::handle_request(init_req, &mut init_connect).await).unwrap();
    assert_eq!(init_response["jsonrpc"], "2.0");
    assert_eq!(init_response["result"]["protocolVersion"], "2024-11-05");
    assert_eq!(init_response["result"]["serverInfo"]["name"], "rk-mcp");
    assert_eq!(
        init_response["result"]["capabilities"],
        json!({"tools": {}})
    );

    let req = rk_mcp::JsonRpcRequest {
        jsonrpc: Some("2.0".into()),
        id: json!(1),
        method: "tools/list".into(),
        params: json!({}),
    };
    let mut connect = || async { Err::<Client, _>(rk_core::Error::other("tools/list connected")) };
    let response = serde_json::to_value(rk_mcp::handle_request(req, &mut connect).await).unwrap();
    let names: Vec<_> = response["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec![
            "factory_snapshot",
            "factory_events_replay",
            "propose_workflow_run",
            "approve_action",
            "execute_approved_workflow_run"
        ]
    );

    let schemas: Vec<_> = response["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| (tool["name"].as_str().unwrap(), tool["inputSchema"].clone()))
        .collect();
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/mcp_tool_input_schemas.json")).unwrap();
    assert_eq!(json!(schemas), expected);
}

#[tokio::test]
async fn rejects_jsonrpc_versions_other_than_2_0_before_dispatch() {
    for (id, jsonrpc) in [(10, json!("1.0")), (11, Value::Null)] {
        let req = rk_mcp::JsonRpcRequest {
            jsonrpc: serde_json::from_value(jsonrpc).ok(),
            id: json!(id),
            method: "tools/list".into(),
            params: json!({}),
        };
        let mut connect = || async { Err::<Client, _>(rk_core::Error::other("connected")) };
        let response =
            serde_json::to_value(rk_mcp::handle_request(req, &mut connect).await).unwrap();
        assert_eq!(response["error"]["code"], -32600);
        assert!(response["result"].is_null());
    }
}

#[tokio::test]
async fn raw_stdio_rejects_invalid_and_missing_jsonrpc_without_dispatch_or_noise() {
    let input = br#"{"jsonrpc":"1.0","id":12,"method":"tools/list","params":{}}
{"id":13,"method":"tools/list","params":{}}
"#;
    let mut output = Vec::new();
    let mut connect = || async { Err::<Client, _>(rk_core::Error::other("raw stdio connected")) };
    rk_mcp::serve(&input[..], &mut output, &mut connect)
        .await
        .unwrap();
    let text = String::from_utf8(output).unwrap();
    let lines: Vec<_> = text.lines().collect();
    assert_eq!(
        lines.len(),
        2,
        "stdout must contain one JSON response per request only: {text:?}"
    );
    for (line, id) in lines.into_iter().zip([12, 13]) {
        let response: Value = serde_json::from_str(line).unwrap();
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], id);
        assert_eq!(response["error"]["code"], -32600);
        assert!(response.get("result").is_none());
    }
}

#[tokio::test]
async fn initialized_notification_is_accepted_without_response_or_daemon_dispatch() {
    let input = br#"{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}
"#;
    let mut output = Vec::new();
    let mut connect =
        || async { Err::<Client, _>(rk_core::Error::other("notification connected")) };
    rk_mcp::serve(&input[..], &mut output, &mut connect)
        .await
        .unwrap();
    assert!(
        output.is_empty(),
        "notifications must not produce stdout responses"
    );
}

#[test]
fn subprocess_initialize_notification_list_and_call_keep_stdout_protocol_only() {
    let input = br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"factory_snapshot","arguments":{"schema":1,"repo":"repo-a"}}}
"#;
    let output = run_rk_mcp_stdio(input);
    assert!(output.status.success());
    let responses = stdout_json_lines(&output);
    assert_eq!(responses.len(), 3, "notification must emit no response");
    assert_eq!(responses[0]["id"], 1);
    assert_eq!(responses[0]["result"]["serverInfo"]["name"], "rk-mcp");
    assert_eq!(responses[1]["id"], 2);
    assert!(responses[1]["result"]["tools"].as_array().unwrap().len() >= 5);
    assert_eq!(responses[2]["id"], 3);
    assert_eq!(responses[2]["error"]["code"], -32000);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("daemon connection failed"),
        "stderr: {stderr}"
    );
}

#[tokio::test]
async fn raw_stdio_rejects_unknown_top_level_fields_and_non_object_params() {
    let input = br#"{"jsonrpc":"2.0","id":14,"method":"tools/list","params":{},"extra":true}
{"jsonrpc":"2.0","id":15,"method":"tools/list","params":[]}
{"jsonrpc":"2.0","id":16,"method":"tools/call","params":{"name":"factory_snapshot","arguments":{"schema":1,"repo":"repo-a"},"extra":true}}
"#;
    let mut output = Vec::new();
    let mut connect =
        || async { Err::<Client, _>(rk_core::Error::other("strict params connected")) };
    rk_mcp::serve(&input[..], &mut output, &mut connect)
        .await
        .unwrap();
    let text = String::from_utf8(output).unwrap();
    let responses: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0]["id"], 14);
    assert_eq!(responses[0]["error"]["code"], -32600);
    assert_eq!(responses[1]["id"], 15);
    assert_eq!(responses[1]["error"]["code"], -32602);
    assert!(responses[1]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("params must be an object"));
    assert_eq!(responses[2]["id"], 16);
    assert_eq!(responses[2]["error"]["code"], -32602);
    assert!(responses[2]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("unknown field"));
}

#[tokio::test]
async fn raw_stdio_invalid_utf8_returns_controlled_parse_error_and_continues() {
    let mut input = b"{\"jsonrpc\":\"2.0\",\"id\":".to_vec();
    input.push(0xff);
    input.extend_from_slice(
        b"}\n{\"jsonrpc\":\"2.0\",\"id\":17,\"method\":\"tools/list\",\"params\":{}}\n",
    );
    let mut output = Vec::new();
    let mut connect = || async { Err::<Client, _>(rk_core::Error::other("utf8 connected")) };
    rk_mcp::serve(&input[..], &mut output, &mut connect)
        .await
        .unwrap();
    let text = String::from_utf8(output).unwrap();
    let responses: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        responses.len(),
        2,
        "invalid UTF-8 must not terminate stdio loop"
    );
    assert_eq!(responses[0]["id"], Value::Null);
    assert_eq!(responses[0]["error"]["code"], -32700);
    assert!(responses[0]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("invalid UTF-8"));
    assert_eq!(responses[1]["id"], 17);
    assert!(responses[1].get("error").is_none(), "{responses:?}");
}

#[test]
fn subprocess_rejects_unknown_fields_params_array_and_invalid_utf8_with_stderr_diagnostics() {
    let mut input = br#"{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"factory_snapshot","arguments":{"schema":1,"repo":"repo-a","extra":true}}}
{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"factory_snapshot","arguments":[]}}
"#
    .to_vec();
    input.extend_from_slice(b"{\"jsonrpc\":\"2.0\",\"id\":");
    input.push(0xff);
    input.extend_from_slice(b"}\n");

    let output = run_rk_mcp_stdio(&input);
    assert!(output.status.success());
    let responses = stdout_json_lines(&output);
    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0]["id"], 10);
    assert_eq!(responses[0]["error"]["code"], -32602);
    assert!(responses[0]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("unknown field"));
    assert_eq!(responses[1]["id"], 11);
    assert_eq!(responses[1]["error"]["code"], -32602);
    assert!(responses[1]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("arguments must be an object"));
    assert_eq!(responses[2]["id"], Value::Null);
    assert_eq!(responses[2]["error"]["code"], -32700);
    assert!(responses[2]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("invalid UTF-8"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid UTF-8 request"), "stderr: {stderr}");
}

#[tokio::test]
async fn read_snapshot_tool_calls_production_client_path_and_returns_canonical_snapshot_wrapper() {
    let (_home, _repo, layout, handle) = setup().await;
    let response = mcp_call(
        &layout,
        1,
        "factory_snapshot",
        json!({"schema":1, "repo":"repo-a"}),
    )
    .await;
    let text = response["result"]["content"][0]["text"].as_str().unwrap();
    let wrapped: Value = serde_json::from_str(text).unwrap();
    assert_eq!(wrapped["schema"], 1);
    let keys: Vec<_> = wrapped["daemon"]["snapshot"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        vec![
            "agents",
            "approvals",
            "budget",
            "inbox",
            "repo_resync",
            "tickets",
            "workflows"
        ]
    );
    let mut client = connect(&layout).await;
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn events_replay_is_bounded_and_finite() {
    let (_home, _repo, layout, handle) = setup().await;
    let mut client = connect(&layout).await;
    for task in ["one", "two", "three"] {
        client.call("factory.propose_action", json!({"kind":"workflow.run","action":{"name":"factory-test","repo":"repo-a", "params":{"taskId":task}}})).await.unwrap();
    }
    let response = mcp_call(
        &layout,
        2,
        "factory_events_replay",
        json!({"schema":1, "repo":"repo-a", "limit":1}),
    )
    .await;
    let text = response["result"]["content"][0]["text"].as_str().unwrap();
    let wrapped: Value = serde_json::from_str(text).unwrap();
    assert_eq!(wrapped["daemon"]["events"].as_array().unwrap().len(), 1);
    assert_eq!(wrapped["daemon"]["truncated"], true);
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn propose_workflow_run_never_executes_the_workflow() {
    let (_home, _repo, layout, handle) = setup().await;
    let response = mcp_call(
        &layout,
        3,
        "propose_workflow_run",
        json!({"schema":1, "workflow":"factory-test", "repo":"repo-a", "params":{"taskId":"one"}, "coordinator":"coord-a", "ttl":60}),
    )
    .await;
    assert!(response.get("error").is_none(), "{response}");
    let snapshot_response = mcp_call(
        &layout,
        30,
        "factory_snapshot",
        json!({"schema":1, "repo":"repo-a"}),
    )
    .await;
    let text = snapshot_response["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    let snapshot: Value = serde_json::from_str(text).unwrap();
    assert_eq!(
        snapshot["daemon"]["snapshot"]["approvals"]["proposals"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(snapshot["daemon"]["snapshot"]["workflows"]
        .as_array()
        .unwrap()
        .is_empty());
    let mut client = connect(&layout).await;
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn missing_digest_rejects_before_connecting_to_daemon() {
    let req = rk_mcp::JsonRpcRequest {
        jsonrpc: Some("2.0".into()),
        id: json!(4),
        method: "tools/call".into(),
        params: json!({"name":"execute_approved_workflow_run", "arguments":{"schema":1, "proposal_id":"p", "digest":"", "action":{"schema":1, "workflow":"factory-test", "repo":"repo-a", "params":{}}}}),
    };
    let mut connect =
        || async { Err::<Client, _>(rk_core::Error::other("digest validation connected")) };
    let response = serde_json::to_value(rk_mcp::handle_request(req, &mut connect).await).unwrap();
    assert_eq!(response["error"]["code"], -32602);
    assert!(response["error"]["message"]
        .as_str()
        .unwrap()
        .contains("digest"));
}

#[tokio::test]
async fn daemon_rpc_error_codes_survive_mcp_mapping() {
    let (_home, _repo, layout, handle) = setup().await;
    let response = mcp_call(
        &layout,
        5,
        "approve_action",
        json!({"schema":1, "proposal_id":"missing", "digest":"sha256:nope"}),
    )
    .await;
    assert_eq!(response["error"]["code"], -32000);
    assert_eq!(response["error"]["data"]["daemon_code"], "bad_params");
    let mut client = connect(&layout).await;
    client.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}
