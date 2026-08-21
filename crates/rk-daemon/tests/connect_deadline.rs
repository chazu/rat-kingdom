//! TKT-01M0HJV6ZCREQTYSXGETEENY2F: `support::connect` polls against a
//! monotonic 30s deadline instead of a fixed iteration count, so it survives
//! full-workspace `cargo test` process contention that can stretch a fixed
//! ~4s/200-iteration budget past a daemon that would have come up given more
//! time. These tests exercise the underlying `support::poll_until` deadline
//! and backoff logic directly against a fake, near-instant attempt closure —
//! not a real daemon socket — so delayed-availability and
//! deadline-exhaustion behavior is covered without a 30-second wall-clock
//! test.

mod support;

use std::time::{Duration, Instant};

#[tokio::test]
async fn poll_until_succeeds_once_the_attempt_starts_returning_some() {
    let mut attempts = 0;
    let result = support::poll_until(
        Duration::from_millis(500),
        Duration::from_millis(5),
        move || {
            attempts += 1;
            let this_attempt = attempts;
            async move {
                // Simulate a daemon that is not yet connectable for the
                // first couple of polls, then comes up.
                (this_attempt >= 3).then_some(this_attempt)
            }
        },
    )
    .await;

    assert_eq!(result, Ok(3));
}

#[tokio::test]
async fn poll_until_fails_finitely_with_elapsed_and_deadline_context() {
    let deadline = Duration::from_millis(50);
    let wall_clock_start = Instant::now();

    let result: Result<(), Duration> =
        support::poll_until(deadline, Duration::from_millis(5), || async { None }).await;

    // Fails, rather than hanging or succeeding spuriously.
    let elapsed = result.expect_err("attempt never returns Some, so this must exhaust");
    // The reported elapsed time is enough context to distinguish "exhausted
    // the deadline" from any other failure mode when this surfaces in a panic.
    assert!(elapsed >= deadline);
    // Proves this test itself runs fast — it validates deadline-exhaustion
    // behavior without ever waiting out a real, production-sized deadline.
    assert!(
        wall_clock_start.elapsed() < Duration::from_secs(5),
        "deadline exhaustion should resolve close to the configured deadline, not hang"
    );
}
