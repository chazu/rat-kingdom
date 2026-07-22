//! Wire protocol: newline-delimited JSON request/response envelopes over a Unix
//! domain socket. Same shape as herdr's socket API, which agents already know.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: String,
    pub message: String,
}

impl Response {
    pub fn ok(id: impl Into<String>, result: Value) -> Self {
        Self {
            id: id.into(),
            result: Some(result),
            error: None,
        }
    }

    pub fn err(id: impl Into<String>, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            result: None,
            error: Some(RpcError {
                code: code.into(),
                message: message.into(),
            }),
        }
    }
}

/// Error codes used by the daemon.
pub mod codes {
    pub const UNKNOWN_METHOD: &str = "unknown_method";
    pub const BAD_PARAMS: &str = "bad_params";
    pub const INTERNAL: &str = "internal";
    pub const SHUTTING_DOWN: &str = "shutting_down";
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_round_trip() {
        let line = r#"{"id":"1","method":"ping","params":{}}"#;
        let req: Request = serde_json::from_str(line).unwrap();
        assert_eq!(req.method, "ping");
    }

    #[test]
    fn params_default_to_null_when_missing() {
        let req: Request = serde_json::from_str(r#"{"id":"1","method":"ping"}"#).unwrap();
        assert!(req.params.is_null());
    }

    #[test]
    fn response_omits_empty_fields() {
        let ok = serde_json::to_value(Response::ok("1", json!("pong"))).unwrap();
        assert!(ok.get("error").is_none());
        let err = serde_json::to_value(Response::err("1", "internal", "boom")).unwrap();
        assert!(err.get("result").is_none());
    }
}
