//! rat-kingdom daemon: NDJSON-over-UDS server plus the client used by `rk`.

pub mod client;
pub mod proto;
pub mod server;

pub use client::Client;
pub use server::Daemon;

#[cfg(test)]
mod tests {
    use super::*;
    use rk_core::paths::Layout;
    use serde_json::json;

    /// End-to-end: run a daemon on a temp socket, ping it, stop it.
    #[tokio::test]
    async fn ping_status_stop_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::at(dir.path());
        let daemon = Daemon::new(layout.clone());
        let handle = tokio::spawn(daemon.run());

        // Wait for the socket to come up.
        let mut client = None;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            if let Ok(c) = Client::connect(&layout).await {
                client = Some(c);
                break;
            }
        }
        let mut client = client.expect("daemon did not come up");

        let pong = client.call("ping", json!({})).await.unwrap();
        assert_eq!(pong, json!("pong"));

        let status = client.call("status", json!({})).await.unwrap();
        assert_eq!(status["version"], env!("CARGO_PKG_VERSION"));

        let unknown = client.call("nope", json!({})).await;
        assert!(unknown.is_err());

        client.call("stop", json!({})).await.unwrap();
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("daemon did not stop")
            .unwrap();
        assert!(result.is_ok());
        assert!(!layout.socket_path().exists());
    }
}
