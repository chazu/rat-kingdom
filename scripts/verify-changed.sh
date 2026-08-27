#!/usr/bin/env bash
set -euo pipefail

# Fast development verification for this Cargo workspace. Rust files select
# their owning package plus every workspace reverse-dependent. Changes to
# workspace-wide build/test configuration fail safe to verify-full.

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/rk-verify-changed.XXXXXX")
trap 'rm -rf "$tmp_dir"' EXIT
changed_file="$tmp_dir/changed"
packages_file="$tmp_dir/packages"
affected_file="$tmp_dir/affected"

base_ref=${RK_VERIFY_BASE:-${1:-}}
if [[ -z "$base_ref" ]]; then
	if git rev-parse --verify --quiet origin/main >/dev/null; then
		base_ref=$(git merge-base HEAD origin/main)
	elif upstream=$(git rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' 2>/dev/null); then
		base_ref=$(git merge-base HEAD "$upstream")
	else
		base_ref=HEAD
	fi
elif ! base_ref=$(git merge-base HEAD "$base_ref"); then
	echo "verify-changed: cannot resolve base '$base_ref'" >&2
	exit 2
fi

{
	git diff --name-only "$base_ref"...HEAD
	git diff --name-only HEAD
	git ls-files --others --exclude-standard
} | sed '/^$/d' | sort -u >"$changed_file"

echo "verify-changed: diff base $base_ref"

# Formatting is cheap and workspace-wide. It also catches edits that are not
# tied to one package before we decide whether Cargo work is necessary.
cargo fmt --all --check

full_required=false
: >"$packages_file"
while IFS= read -r path; do
	case "$path" in
		crates/*/*)
			crate_dir=${path#crates/}
			crate_dir=${crate_dir%%/*}
			manifest="crates/$crate_dir/Cargo.toml"
			if [[ ! -f "$manifest" ]]; then
				full_required=true
				continue
			fi
			package=$(awk -F '"' '/^[[:space:]]*name[[:space:]]*=/ { print $2; exit }' "$manifest")
			if [[ -z "$package" ]]; then
				full_required=true
			else
				printf '%s\n' "$package" >>"$packages_file"
			fi
			;;
		Cargo.toml|Cargo.lock|rust-toolchain|rust-toolchain.toml|build.rs|mise.toml|.cargo/*|scripts/*|.rk/checks.cue|.github/*)
			full_required=true
			;;
		README.md|LICENSE*|docs/*|.gitignore|.git-issue/*)
			# These do not feed Rust compilation or runtime tests.
			;;
		*)
			# Unknown repository-level inputs may be consumed by build scripts or
			# integration tests. Be conservative instead of silently under-testing.
			full_required=true
			;;
	esac
done <"$changed_file"

if [[ "$full_required" == true ]]; then
	echo "verify-changed: workspace-wide input changed; running verify-full"
	MISE_TRUSTED_CONFIG_PATHS="$repo_root" mise run verify-full
	exit
fi

sort -u "$packages_file" -o "$packages_file"
if [[ ! -s "$packages_file" ]]; then
	echo "verify-changed: no Rust package changes; formatting is sufficient"
	exit
fi

: >"$affected_file"
while IFS= read -r package; do
	cargo tree --workspace --invert "$package" --depth workspace --prefix none --format '{p}' |
		awk '{ print $1 }' >>"$affected_file"
done <"$packages_file"
sort -u "$affected_file" -o "$affected_file"

echo "verify-changed: affected packages"
sed 's/^/  - /' "$affected_file"

# Package names are Cargo identifiers and therefore contain no shell spaces.
# shellcheck disable=SC2046
cargo clippy $(awk '{ printf "-p %s ", $0 }' "$affected_file") --all-targets -- -D warnings
# shellcheck disable=SC2046
env -u RK_AGENT -u RK_TASK -u RK_REPO -u RK_ROLE -u RK_HOME -u RK_BRANCH -u RK_WORKTREE -u RK_AUTH_TOKEN -u RK_REVIEW_BRANCH -u RK_REVIEW_HEAD -u RK_REVIEW_TARGET -u RK_REVIEW_TASK -u RK_REVIEW_ATTEMPT cargo nextest run $(awk '{ printf "-p %s ", $0 }' "$affected_file") --no-fail-fast
