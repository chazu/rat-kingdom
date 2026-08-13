#[tokio::main]
async fn main() -> rk_core::Result<()> {
    rk_mcp::serve_stdio().await
}
