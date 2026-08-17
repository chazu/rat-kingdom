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
//! [`BUILD_VERSION`] fixes both halves. It appends the commit stamped in by
//! `build.rs`, so two builds of different code carry different strings, and
//! [`mismatch_warning`] turns a difference into something an operator sees.

/// The workspace semver. Shared by every crate; bumps rarely.
pub const SEMVER: &str = env!("CARGO_PKG_VERSION");

/// Short commit this binary was built from, or `unknown` outside a git
/// checkout. See `crates/rk-core/build.rs` for how it is resolved.
pub const BUILD_SHA: &str = env!("RK_BUILD_SHA");

/// What one build calls itself on the wire: `<semver>+<short sha>`.
///
/// Every binary linked against rk-core in a single `cargo build` gets the same
/// value, which is what keeps the check quiet in tests — an in-process test
/// daemon and the `rk` child it is driving are one build and agree.
pub const BUILD_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "+", env!("RK_BUILD_SHA"));

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
         !! Restart it onto this build:  rk daemon stop && rk ping"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matched_builds_say_nothing() {
        assert!(mismatch_warning("0.1.0+abc123", "0.1.0+abc123").is_none());
        assert!(mismatch_warning(BUILD_VERSION, BUILD_VERSION).is_none());
    }

    #[test]
    fn mismatch_names_both_builds() {
        let warning = mismatch_warning("0.1.0+aaaaaaaaaaaa", "0.1.0+bbbbbbbbbbbb")
            .expect("differing builds must warn");
        assert!(warning.contains("0.1.0+aaaaaaaaaaaa"), "{warning}");
        assert!(warning.contains("0.1.0+bbbbbbbbbbbb"), "{warning}");
        // The AC is "warns loudly": it has to be more than a version dump, and
        // it has to tell the operator what to do about it.
        assert!(warning.contains("rk daemon stop"), "{warning}");
    }

    #[test]
    fn an_unknown_provenance_is_not_a_match() {
        assert!(mismatch_warning("0.1.0+unknown", "0.1.0+abc123").is_some());
        // ...but two provenance-less builds have nothing to disagree about.
        assert!(mismatch_warning("0.1.0+unknown", "0.1.0+unknown").is_none());
    }

    #[test]
    fn build_version_carries_more_than_the_semver() {
        assert!(
            BUILD_VERSION.starts_with(SEMVER),
            "build version must stay readable as a semver: {BUILD_VERSION}"
        );
        assert_eq!(BUILD_VERSION, format!("{SEMVER}+{BUILD_SHA}"));
        // Guards the whole point of build.rs: were this to collapse back to the
        // bare semver, every comparison below would pass forever.
        assert_ne!(BUILD_VERSION, SEMVER);
    }
}
