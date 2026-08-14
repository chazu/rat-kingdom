#!/usr/bin/env bash
# Build an optimized (release, non-debug) rk and install it.
# Override the install location with RK_INSTALL_DIR (default: ~/.local/bin).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
install_dir="${RK_INSTALL_DIR:-$HOME/.local/bin}"

cd "$repo_root"
cargo build --release -p rk-cli -p rk-mcp

mkdir -p "$install_dir"
install -m 755 target/release/rk "$install_dir/rk"
install -m 755 target/release/rk-mcp "$install_dir/rk-mcp"

echo "Installed $("$install_dir/rk" --version 2>/dev/null || echo rk) to $install_dir/rk and $install_dir/rk-mcp"
case ":$PATH:" in
  *":$install_dir:"*) ;;
  *) echo "warning: $install_dir is not on your PATH" ;;
esac
