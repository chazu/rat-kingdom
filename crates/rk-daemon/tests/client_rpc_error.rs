use rk_core::paths::Layout;
use rk_daemon::{Client, ClientRpcError, Daemon};
use serde_json::{json, Value};
use std::time::Duration;

async fn connect(layout: &Layout) -> Client {
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = Client::connect_as_operator(layout).await {
            return c;
        }
    }
    panic!("daemon did not come up");
}

#[tokio::test]
async fn test_client_raw_and_typed_preserve_rpc_error_code() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    let daemon = Daemon::new_in_memory(layout.clone(), "test-castle".into()).unwrap();
    let handle = tokio::spawn(daemon.run());
    let mut raw_client = connect(&layout).await;
    let raw = raw_client
        .call_raw("no.such.method", json!({}))
        .await
        .unwrap();
    assert_eq!(raw.error.unwrap().code, "unknown_method");

    let mut typed_client = connect(&layout).await;
    let err = typed_client
        .call_typed::<Value>("repo.add", json!({"name":"x","path":"/definitely/missing"}))
        .await
        .unwrap_err();
    match err {
        ClientRpcError::Rpc(rpc) => assert_eq!(rpc.code, "bad_params"),
        other => panic!("expected rpc error, got {other:?}"),
    }

    let mut stop = connect(&layout).await;
    stop.call("stop", json!({})).await.unwrap();
    handle.await.unwrap().unwrap();
}
