#[tokio::main]
async fn main() -> rk_core::Result<()> {
    // Must run before anything below reads `rk_core::version::build_version()`
    // / `build_sha()` — `rk-mcp` is its own executable (not a subcommand of
    // `rk`), so it needs this call independently. `RK_BUILD_SHA` is this
    // crate's own compile-time env var, stamped by `crates/rk-mcp/build.rs`.
    rk_core::version::init_build_sha(env!("RK_BUILD_SHA"));

    rk_mcp::serve_stdio().await
}
