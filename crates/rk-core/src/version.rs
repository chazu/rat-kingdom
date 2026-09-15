//! The build identity a binary carries on the wire, and the one comparison
//! that identity exists to support.
//!
//! `rk` is a single binary that is both the CLI and — as `rk daemon run` — the
//! daemon. Installing a new build therefore replaces the CLI immediately while
//! the daemon keeps serving the *old* code until someone restarts it, so every
//! feature merged since that daemon started is silently absent. Nothing used to
//! notice: the daemon reported `CARGO_PKG_VERSION`, which has been `0.1.0`
//! since the first commit, and nothing compared it to anything.
//!
//! [`build_version`] fixes both halves. It appends the commit the executable
//! was built from, so two builds of different code carry different strings,
//! and [`mismatch_warning`] turns a difference into something an operator
//! sees.
//!
//! The commit is *not* a compile-time constant of this crate. `rk-core` is a
//! dependency of every crate in the workspace, so embedding a per-commit
//! value here via its own `build.rs` would invalidate rk-core's compiled
//! artifact — and so force a relink of the entire dependent workspace — on
//! every commit, including for crates with no interest in build identity.
//! Instead each executable that actually ships — `rk-cli` (the `rk` binary)
//! and `rk-mcp` (the standalone MCP stdio bridge), each with its own
//! git-sha-stamping `build.rs` — calls [`init_build_sha`] once at the top of
//! its own `main`, and every runtime consumer in that process — including
//! code in `rk-daemon`, which `rk` runs in-process as `rk daemon run` and
//! which both `rk-cli` and `rk-mcp` drive as an RPC client — reads it back
//! through [`build_sha`] / [`build_version`].

use std::sync::OnceLock;

/// The workspace semver. Shared by every crate; bumps rarely.
pub const SEMVER: &str = env!("CARGO_PKG_VERSION");

struct BuildIdentity {
    sha: String,
    version: String,
}

static BUILD: OnceLock<BuildIdentity> = OnceLock::new();

/// Record the commit the running executable was built from. Call once, as
/// early as possible in the executable's `main` — before any code below is
/// read — with the value the executable's own `build.rs` stamped into its
/// `RK_BUILD_SHA` compile-time env var.
///
/// A second call (or a call after [`build_sha`]/[`build_version`] has already
/// been read once and lazily defaulted) is ignored: a process has exactly one
/// build identity, so the first value recorded wins.
pub fn init_build_sha(sha: &str) {
    let sha = sha.to_string();
    let version = format!("{SEMVER}+{sha}");
    let _ = BUILD.set(BuildIdentity { sha, version });
}

fn build() -> &'static BuildIdentity {
    BUILD.get_or_init(|| BuildIdentity {
        sha: "unknown".to_string(),
        version: format!("{SEMVER}+unknown"),
    })
}

/// Short commit the running executable was built from, or `unknown` if
/// [`init_build_sha`] was never called (outside a git checkout, or in a
/// context — such as this crate's own unit tests — that has no single
/// executable's build.rs to draw the value from).
pub fn build_sha() -> &'static str {
    &build().sha
}

/// What one build calls itself on the wire: `<semver>+<short sha>`.
///
/// Every binary linked against rk-core in a single `cargo build` gets the same
/// value, which is what keeps the check quiet in tests — an in-process test
/// daemon and the `rk` child it is driving are one build and agree.
pub fn build_version() -> &'static str {
    &build().version
}

/// The warning to show when `local` is talking to a peer built as `remote`,
/// or `None` when the two are the same build and there is nothing to say.
///
/// The comparison is a plain string equality on purpose. An `unknown` sha
/// facing a real one is *not* treated as a match: we cannot show that those
/// two agree, and a false silence here is the exact failure this function was
/// added to end — a mismatch that nobody sees costs a debugging session,
/// whereas a warning that turns out to be pessimistic costs three lines of
/// stderr.
pub fn mismatch_warning(local: &str, remote: &str) -> Option<String> {
    if local == remote {
        return None;
    }
    Some(format!(
        "!! rk build mismatch: this CLI is {local}, the running daemon is {remote}.\n\
         !! The daemon is serving different code — anything merged or built since it\n\
         !! started does not exist yet, and commands relying on it will fail oddly.\n\
         !! Roll it onto this build without losing fleet state:  rk daemon rollover"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matched_builds_say_nothing() {
        assert!(mismatch_warning("0.1.0+abc123", "0.1.0+abc123").is_none());
        assert!(mismatch_warning(build_version(), build_version()).is_none());
    }

    #[test]
    fn mismatch_names_both_builds() {
        let warning = mismatch_warning("0.1.0+aaaaaaaaaaaa", "0.1.0+bbbbbbbbbbbb")
            .expect("differing builds must warn");
        assert!(warning.contains("0.1.0+aaaaaaaaaaaa"), "{warning}");
        assert!(warning.contains("0.1.0+bbbbbbbbbbbb"), "{warning}");
        // The AC is "warns loudly": it has to be more than a version dump, and
        // it has to tell the operator what to do about it.
        assert!(warning.contains("rk daemon rollover"), "{warning}");
    }

    #[test]
    fn an_unknown_provenance_is_not_a_match() {
        assert!(mismatch_warning("0.1.0+unknown", "0.1.0+abc123").is_some());
        // ...but two provenance-less builds have nothing to disagree about.
        assert!(mismatch_warning("0.1.0+unknown", "0.1.0+unknown").is_none());
    }

    #[test]
    fn build_version_carries_more_than_the_semver() {
        let version = build_version();
        assert!(
            version.starts_with(SEMVER),
            "build version must stay readable as a semver: {version}"
        );
        assert_eq!(version, format!("{SEMVER}+{}", build_sha()));
        // Guards the whole point of stamping: were this to collapse back to
        // the bare semver, every comparison above would pass forever.
        assert_ne!(version, SEMVER);
    }
}
