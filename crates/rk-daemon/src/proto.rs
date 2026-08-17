//! Wire protocol: newline-delimited JSON request/response envelopes over a Unix
//! domain socket. Same shape as herdr's socket API, which agents already know.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: String,
    pub method: String,
    /// Per-layout bearer token. Empty/missing tokens are rejected by the
    /// daemon; the default exists only so old wire fixtures deserialize cleanly.
    #[serde(default)]
    pub auth: String,
    /// Operator or the agent name supplied by the supervised process. This is
    /// authorization context, not a tuple payload and never a sync identity.
    #[serde(default = "default_caller")]
    pub caller: String,
    /// Build the caller was compiled from (`rk_core::version::BUILD_VERSION`).
    ///
    /// Half of the version handshake: the daemon cannot warn the operator's
    /// terminal, but it can record in `daemon.log` that it is being driven by
    /// a binary it does not match. Optional so a client older than this field
    /// still parses; absent means "did not say", not "matches".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,
    #[serde(default)]
    pub params: Value,
}

fn default_caller() -> String {
    "operator".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
    /// Build the daemon was compiled from, stamped on *every* reply.
    ///
    /// Carried on the response rather than exchanged in a dedicated
    /// connect-time round trip so the check costs nothing: `rk` is invoked
    /// constantly by rats and an extra RPC per invocation would be a real tax.
    /// The client compares it against its own [`rk_core::version::BUILD_VERSION`]
    /// and warns once per process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: String,
    pub message: String,
}

impl Response {
    /// Both constructors stamp `server_version`, which is why no call site in
    /// the daemon has to remember to: `Response` is only ever *built* by the
    /// server (the client merely deserializes it), so stamping here covers
    /// every reply, including the pre-dispatch auth and frame-size refusals a
    /// connection-level handshake would have happened too late to catch.
    pub fn ok(id: impl Into<String>, result: Value) -> Self {
        Self {
            id: id.into(),
            result: Some(result),
            error: None,
            server_version: Some(rk_core::version::BUILD_VERSION.into()),
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
            server_version: Some(rk_core::version::BUILD_VERSION.into()),
        }
    }
}

/// Error codes used by the daemon.
pub mod codes {
    pub const UNKNOWN_METHOD: &str = "unknown_method";
    pub const BAD_PARAMS: &str = "bad_params";
    pub const INTERNAL: &str = "internal";
    pub const SHUTTING_DOWN: &str = "shutting_down";
    pub const UNAUTHORIZED: &str = "unauthorized";
    pub const FORBIDDEN: &str = "forbidden";
    pub const FRAME_TOO_LARGE: &str = "frame_too_large";
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

    #[test]
    fn every_reply_is_stamped_with_the_daemon_build() {
        for response in [
            Response::ok("1", json!("pong")),
            Response::err("1", "internal", "boom"),
        ] {
            let wire = serde_json::to_value(&response).unwrap();
            assert_eq!(
                wire["server_version"],
                json!(rk_core::version::BUILD_VERSION),
                "a reply the client cannot date is a reply that cannot warn"
            );
        }
    }

    #[test]
    fn a_pre_handshake_peer_still_parses() {
        // Both fields are additive: an old CLI's request and an old daemon's
        // reply must keep deserializing, reporting "did not say" rather than
        // failing the call outright.
        let req: Request =
            serde_json::from_str(r#"{"id":"1","method":"ping","caller":"operator"}"#).unwrap();
        assert!(req.client_version.is_none());
        let resp: Response = serde_json::from_str(r#"{"id":"1","result":"pong"}"#).unwrap();
        assert!(resp.server_version.is_none());
    }

    #[test]
    fn a_request_carries_the_callers_build() {
        let sent = serde_json::to_value(Request {
            id: "1".into(),
            method: "ping".into(),
            auth: "t".into(),
            caller: "operator".into(),
            client_version: Some(rk_core::version::BUILD_VERSION.into()),
            params: json!({}),
        })
        .unwrap();
        assert_eq!(
            sent["client_version"],
            json!(rk_core::version::BUILD_VERSION)
        );
    }
}
