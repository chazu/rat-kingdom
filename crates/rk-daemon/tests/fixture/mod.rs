//! Shared support for the fake-harness scripts these tests model rats with.
//!
//! Lives in a subdirectory so cargo does not compile it as a test target of its
//! own; each test binary picks it up with `mod fixture;`.

/// Make `rk_done` callable inside a fake-harness script.
///
/// Since TKT-175 a generation that reaches the exit-flush without a `task_done`
/// publishes as a FAILURE, because reaching that flush is itself the proof that
/// the generation never declared itself finished. So a fake that means to model
/// a rat which *completed* has to declare done first, exactly as a real primed
/// rat does — and one that means to model a rat cut off mid-task simply does
/// not call it. Which of the two a fixture is saying used to be invisible;
/// now it is one line of the script.
///
/// The declaration has to come from inside the fake rather than from the test
/// process: exact matching requires a spawn id that does not exist until after
/// dispatch, and a `for_each` fan-out names
/// its rats itself, so the test often never learns who to write it for. See
/// `src/bin/rk-fixture-done.rs`.
///
/// Call it BEFORE the script's `{"type":"result"}` line: the supervisor decides
/// whether to withhold a turn at the moment that line is parsed, and a `rk_done`
/// after it is a rat declaring done too late to be believed.
pub fn with_rk_done(script: &str) -> String {
    // Single-quoted: a cargo target path is not going to contain a quote, but
    // it can easily contain `$` or a space.
    format!(
        "rk_done() {{ '{done}' \"$@\"; }}\n{script}",
        done = env!("CARGO_BIN_EXE_rk-fixture-done"),
    )
}
