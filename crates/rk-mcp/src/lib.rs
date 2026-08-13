use rk_core::paths::Layout;
use rk_daemon::proto::{Response as DaemonResponse, RpcError};
use rk_daemon::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::future::Future;
use std::pin::Pin;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

pub const SCHEMA: u8 = 1;

const TOOLS: &[(&str, &str)] = &[
    ("factory_snapshot", "Read a finite factory snapshot."),
    (
        "factory_events_replay",
        "Replay a bounded finite factory event page.",
    ),
    (
        "propose_workflow_run",
        "Propose, but do not execute, a workflow.run action.",
    ),
    (
        "approve_action",
        "Approve an existing action proposal by digest.",
    ),
    (
        "execute_approved_workflow_run",
        "Execute an approved workflow.run action by digest.",
    ),
];

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    #[serde(default)]
    pub jsonrpc: Option<String>,
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FactorySnapshotRequest {
    pub schema: u8,
    pub repo: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FactoryEventsReplayRequest {
    pub schema: u8,
    pub repo: String,
    #[serde(default)]
    pub kinds: Vec<String>,
    pub limit: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WorkflowRunRequest {
    pub schema: u8,
    pub name: String,
    pub repo: String,
    #[serde(default)]
    pub params: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordinator: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ApproveActionRequest {
    pub schema: u8,
    pub proposal_id: String,
    pub digest: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExecuteApprovedWorkflowRunRequest {
    pub schema: u8,
    pub proposal_id: String,
    pub digest: String,
    pub action: WorkflowRunRequest,
}

pub trait DaemonCaller {
    fn call_raw<'a>(
        &'a mut self,
        method: &'a str,
        params: Value,
    ) -> Pin<Box<dyn Future<Output = rk_core::Result<DaemonResponse>> + Send + 'a>>;
}

impl DaemonCaller for Client {
    fn call_raw<'a>(
        &'a mut self,
        method: &'a str,
        params: Value,
    ) -> Pin<Box<dyn Future<Output = rk_core::Result<DaemonResponse>> + Send + 'a>> {
        Box::pin(async move { Client::call_raw(self, method, params).await })
    }
}

pub async fn serve_stdio() -> rk_core::Result<()> {
    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let stdout = tokio::io::stdout();
    serve(stdin, stdout, || async {
        let layout = Layout::discover()?;
        Client::connect(&layout).await
    })
    .await
}

pub async fn serve<R, W, F, Fut, C>(
    mut input: R,
    mut output: W,
    mut connect: F,
) -> rk_core::Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
    F: FnMut() -> Fut,
    Fut: Future<Output = rk_core::Result<C>>,
    C: DaemonCaller,
{
    let mut line = String::new();
    loop {
        line.clear();
        if input.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        let response = match serde_json::from_str::<JsonRpcRequest>(&line) {
            Ok(req) => handle_request(req, &mut connect).await,
            Err(err) => JsonRpcResponse::err(Value::Null, -32700, format!("parse error: {err}")),
        };
        let mut bytes = serde_json::to_vec(&response)?;
        bytes.push(b'\n');
        output.write_all(&bytes).await?;
        output.flush().await?;
    }
}

pub async fn handle_request<F, Fut, C>(req: JsonRpcRequest, connect: &mut F) -> JsonRpcResponse
where
    F: FnMut() -> Fut,
    Fut: Future<Output = rk_core::Result<C>>,
    C: DaemonCaller,
{
    match req.method.as_str() {
        "initialize" => JsonRpcResponse::ok(
            req.id,
            json!({
                "protocolVersion": "2024-11-05",
                "serverInfo": {"name": "rk-mcp", "version": env!("CARGO_PKG_VERSION")},
                "capabilities": {"tools": {}}
            }),
        ),
        "tools/list" => JsonRpcResponse::ok(req.id, json!({"tools": tools()})),
        "tools/call" => handle_tool_call(req.id, req.params, connect).await,
        _ => JsonRpcResponse::err(req.id, -32601, "method not found"),
    }
}

async fn handle_tool_call<F, Fut, C>(id: Value, params: Value, connect: &mut F) -> JsonRpcResponse
where
    F: FnMut() -> Fut,
    Fut: Future<Output = rk_core::Result<C>>,
    C: DaemonCaller,
{
    let name = match params.get("name").and_then(Value::as_str) {
        Some(name) => name,
        None => return JsonRpcResponse::err(id, -32602, "missing tool name"),
    };
    let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);
    let prepared = match prepare_tool(name, arguments) {
        Ok(prepared) => prepared,
        Err(err) => return JsonRpcResponse::err(id, -32602, err),
    };
    let mut client = match connect().await {
        Ok(client) => client,
        Err(err) => return JsonRpcResponse::err(id, -32000, err.to_string()),
    };
    match client.call_raw(&prepared.method, prepared.params).await {
        Ok(resp) => daemon_to_mcp(id, resp),
        Err(err) => JsonRpcResponse::err(id, -32000, err.to_string()),
    }
}

struct PreparedCall {
    method: String,
    params: Value,
}

fn prepare_tool(name: &str, arguments: Value) -> Result<PreparedCall, String> {
    match name {
        "factory_snapshot" => {
            let args: FactorySnapshotRequest = parse_args(arguments)?;
            ensure_schema(args.schema)?;
            Ok(PreparedCall {
                method: "factory.snapshot".into(),
                params: json!({"repo": args.repo}),
            })
        }
        "factory_events_replay" => {
            let args: FactoryEventsReplayRequest = parse_args(arguments)?;
            ensure_schema(args.schema)?;
            if args.limit == 0 || args.limit > 200 {
                return Err("limit must be between 1 and 200".into());
            }
            let mut params = json!({"repo": args.repo, "limit": args.limit});
            if !args.kinds.is_empty() {
                params["kinds"] = json!(args.kinds);
            }
            if let Some(after) = args.after {
                params["after"] = json!(after);
            }
            Ok(PreparedCall {
                method: "factory.events.replay".into(),
                params,
            })
        }
        "propose_workflow_run" => {
            let args: WorkflowRunRequest = parse_args(arguments)?;
            ensure_schema(args.schema)?;
            Ok(PreparedCall {
                method: "factory.propose_action".into(),
                params: json!({"kind":"workflow.run", "action": workflow_action(args)}),
            })
        }
        "approve_action" => {
            let args: ApproveActionRequest = parse_args(arguments)?;
            ensure_schema(args.schema)?;
            require_digest(&args.digest)?;
            Ok(PreparedCall {
                method: "factory.approve_action".into(),
                params: json!({"proposal_id": args.proposal_id, "digest": args.digest}),
            })
        }
        "execute_approved_workflow_run" => {
            let args: ExecuteApprovedWorkflowRunRequest = parse_args(arguments)?;
            ensure_schema(args.schema)?;
            require_digest(&args.digest)?;
            ensure_schema(args.action.schema)?;
            Ok(PreparedCall {
                method: "factory.execute_action".into(),
                params: json!({"proposal_id": args.proposal_id, "digest": args.digest, "action": workflow_action(args.action)}),
            })
        }
        _ => Err("unknown tool".into()),
    }
}

fn workflow_action(args: WorkflowRunRequest) -> Value {
    let mut action = json!({"name": args.name, "repo": args.repo, "params": args.params});
    if let Some(coordinator) = args.coordinator {
        action["coordinator"] = json!(coordinator);
    }
    action
}

fn parse_args<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T, String> {
    serde_json::from_value(value).map_err(|err| format!("invalid arguments: {err}"))
}

fn ensure_schema(schema: u8) -> Result<(), String> {
    if schema == SCHEMA {
        Ok(())
    } else {
        Err("unsupported schema".into())
    }
}

fn require_digest(digest: &str) -> Result<(), String> {
    if digest.trim().is_empty() {
        Err("digest is required".into())
    } else {
        Ok(())
    }
}

fn daemon_to_mcp(id: Value, resp: DaemonResponse) -> JsonRpcResponse {
    if let Some(error) = resp.error {
        return daemon_error(id, error);
    }
    let text = serde_json::to_string(
        &json!({"schema": SCHEMA, "daemon": resp.result.unwrap_or(Value::Null)}),
    )
    .unwrap();
    JsonRpcResponse::ok(id, json!({"content": [{"type": "text", "text": text}]}))
}

fn daemon_error(id: Value, error: RpcError) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: None,
        error: Some(JsonRpcError {
            code: -32000,
            message: error.message,
            data: Some(json!({"daemon_code": error.code})),
        }),
    }
}

fn tools() -> Vec<Value> {
    TOOLS.iter().map(|(name, description)| json!({
        "name": name,
        "description": description,
        "inputSchema": {"type":"object", "properties": {"schema": {"const": SCHEMA}}, "required": ["schema"]}
    })).collect()
}

impl JsonRpcResponse {
    fn ok(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    fn err(id: Value, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data: None,
            }),
        }
    }
}
