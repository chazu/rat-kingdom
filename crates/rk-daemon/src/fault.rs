//! Deterministic, test-only fault barriers for crash-recovery proofs.
//!
//! A restart proof is only worth the bytes it costs if the crash lands at a
//! *known* point. Sleeping for N milliseconds and hoping the daemon is
//! mid-transition proves nothing: on a loaded runner the sleep expires
//! before the transition starts (the test then proves ordinary restart, not
//! crash recovery) or long after it finished (the test proves nothing at
//! all), and either way it stays green. Both readings are indistinguishable
//! from the outside, so such a test silently degrades into a tautology.
//!
//! A barrier removes the timing entirely. [`barrier`] parks the daemon
//! *inside* a chosen transition, forever, and announces that it has done so
//! by creating a file. A test waits for that file — not for a duration — and
//! only then kills the process. The crash is therefore guaranteed to land
//! between the two statements the barrier sits between, every run, on any
//! machine, under any load.
//!
//! # Arming
//!
//! Deliberately armed through the daemon's **on-disk home** rather than an
//! environment variable: the daemon under test is a separate OS process that
//! a `Client::connect_or_spawn` may have started, so a test cannot rely on
//! its own env reaching it, and a *second* daemon started over the same home
//! after the kill must come up **disarmed** — which a test controls here by
//! simply deleting one file. Writing [`ARM_FILE`] with a barrier's name arms
//! exactly that barrier; nothing else in the daemon reads or writes it.
//!
//! # Safety
//!
//! The whole module compiles to nothing outside `debug_assertions`, so a
//! release daemon cannot park even if [`ARM_FILE`] is somehow present in a
//! real `~/.rat-kingdom` — the barrier is a test instrument, not a feature
//! flag, and must never be reachable in a shipped binary.

/// Name of the file, relative to the daemon's home, whose contents name the
/// single armed barrier. Absent (the normal case) means every barrier is a
/// no-op.
#[allow(dead_code)]
pub(crate) const ARM_FILE: &str = "fault-barrier";

/// Name of the file [`barrier`] creates, relative to the daemon's home, to
/// announce that the armed barrier has been reached and the daemon is now
/// parked inside the transition. Its contents are the barrier's name.
#[allow(dead_code)]
pub(crate) const REACHED_FILE: &str = "fault-barrier.reached";

/// Park forever if the barrier called `name` is armed for `layout`'s home;
/// return immediately otherwise.
///
/// "Forever" is literal and intended: the only exit is the test killing the
/// process. That is what makes the crash site deterministic — an
/// unsynchronised call site could always be raced past, whereas a call site
/// that never returns cannot be.
#[cfg(debug_assertions)]
pub(crate) async fn barrier(layout: &rk_core::paths::Layout, name: &str) {
    let armed = std::fs::read_to_string(layout.home().join(ARM_FILE))
        .map(|armed| armed.trim() == name)
        .unwrap_or(false);
    if !armed {
        return;
    }
    // Announce *before* parking, never after: the test's next step is to
    // kill this process, so a marker written afterward would never exist.
    let _ = std::fs::write(layout.home().join(REACHED_FILE), name);
    tracing::warn!(
        barrier = name,
        "TEST FAULT BARRIER reached; parking this transition until killed"
    );
    loop {
        // An async park, not `thread::sleep`: the runtime stays live so the
        // test can keep reading daemon state (`rk daemon status`) right up
        // to the kill, and so this cannot wedge an unrelated worker thread.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[cfg(not(debug_assertions))]
pub(crate) async fn barrier(_layout: &rk_core::paths::Layout, _name: &str) {}
