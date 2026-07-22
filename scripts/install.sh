#!/usr/bin/env bash
# Build an optimized (release, non-debug) rk and install it.
# Override the install location with RK_INSTALL_DIR (default: ~/.local/bin).
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
install_dir="${RK_INSTALL_DIR:-$HOME/.local/bin}"

cd "$repo_root"
cargo build --release -p rk-cli

mkdir -p "$install_dir"
install -m 755 target/release/rk "$install_dir/rk"

echo "Installed $("$install_dir/rk" --version 2>/dev/null || echo rk) to $install_dir/rk"
case ":$PATH:" in
  *":$install_dir:"*) ;;
  *) echo "warning: $install_dir is not on your PATH" ;;
esac
