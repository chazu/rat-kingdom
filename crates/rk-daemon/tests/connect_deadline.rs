//! TKT-01M0HJV6ZCREQTYSXGETEENY2F: `support::connect` polls against a
//! monotonic 30s deadline instead of a fixed iteration count, so it survives
//! full-workspace `cargo test` process contention that can stretch a fixed
//! ~4s/200-iteration budget past a daemon that would have come up given more
//! time. These tests exercise the underlying `support::poll_until` deadline
//! and backoff logic directly against a fake, near-instant attempt closure —
//! not a real daemon socket — so delayed-availability and
//! deadline-exhaustion behavior is covered without a 30-second wall-clock
//! test.
//!
//! TKT-mukos-pogim-lopis: `support::try_connect_or_report` (built on the
//! generic `support::race_attempt_or_report`) additionally races the WHOLE
//! retrying connect attempt against the spawned daemon's own `JoinHandle`
//! and one absolute deadline via `tokio::select!`, so IF a daemon's `run()`
//! has already returned (lost the singleton-lock race, failed its bind, any
//! other startup error), that real result is reported instead of reading as
//! a slow daemon until the deadline expires. Individual attempts are never
//! capped short by the retry `poll_interval` — an earlier version that did
//! cap each attempt at `poll_interval` would cancel a real connect/auth
//! attempt that is merely slow under load before it ever got to succeed,
//! which is exactly the failure mode this instrumentation exists to avoid;
//! see `race_attempt_or_report_lets_a_slow_but_healthy_attempt_finish_
//! instead_of_cancelling_it` below. This is diagnostic instrumentation for
//! the intermittent `restart_mid_queue_replays_fifo_order_*` failure, not a
//! confirmed fix — whether an early-finished `run()` is the actual cause of
//! that failure is still unproven pending an instrumented failing run.
//! Exercised here against fake near-instant/slow-but-successful tasks, same
//! reasoning as the `poll_until` tests above.

mod support;

use rk_core::paths::Layout;
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

#[tokio::test]
async fn try_connect_or_report_surfaces_a_finished_daemons_own_error_before_the_deadline() {
    // Nothing ever listens on this layout's socket — the only way this
    // resolves at all is by the loop noticing the handle finished.
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    let mut handle: tokio::task::JoinHandle<rk_core::Result<()>> = tokio::spawn(async {
        rk_core::Result::Err(rk_core::Error::other("simulated bind failure"))
    });

    // A generous deadline that the pre-fix behavior (`support::connect`,
    // which never inspects the handle) would have to wait out in full,
    // producing only "daemon did not come up" with no cause. The fix must
    // instead resolve almost immediately once the handle is observed
    // finished, well before this deadline.
    let deadline = Duration::from_secs(30);
    let wall_clock_start = Instant::now();

    let result =
        support::try_connect_or_report(&layout, &mut handle, deadline, Duration::from_millis(5))
            .await;
    let failure = match result {
        Ok(_) => panic!("the daemon task already resolved with an error, so this must not connect"),
        Err(failure) => failure,
    };

    assert!(
        matches!(failure, support::StartupFailure::DaemonExited(Err(_))),
        "expected the daemon's own startup error, got: {failure:?}"
    );
    let message = failure.to_string();
    assert!(
        message.contains("simulated bind failure") && message.contains("stopped daemon"),
        "panic message must name the real cause, not just time out: {message}"
    );
    assert!(
        wall_clock_start.elapsed() < Duration::from_secs(2),
        "a finished handle must be reported immediately, not after waiting out the deadline"
    );
}

#[tokio::test]
async fn try_connect_or_report_still_times_out_when_the_daemon_neither_connects_nor_exits() {
    let home = tempfile::tempdir().unwrap();
    let layout = Layout::at(home.path());
    // Simulates a daemon that is genuinely just slow (still running, never
    // finished) rather than stopped — the deadline path must still fire so
    // this doesn't regress into hanging forever.
    let mut handle: tokio::task::JoinHandle<rk_core::Result<()>> =
        tokio::spawn(async { std::future::pending().await });

    let deadline = Duration::from_millis(50);
    let wall_clock_start = Instant::now();

    let result =
        support::try_connect_or_report(&layout, &mut handle, deadline, Duration::from_millis(5))
            .await;
    let failure = match result {
        Ok(_) => {
            panic!("nothing ever connects and the handle never finishes, so this must time out")
        }
        Err(failure) => failure,
    };

    assert!(
        matches!(failure, support::StartupFailure::TimedOut(elapsed) if elapsed >= deadline),
        "expected a deadline timeout, got: {failure:?}"
    );
    assert!(
        wall_clock_start.elapsed() < Duration::from_secs(5),
        "deadline exhaustion should resolve close to the configured deadline, not hang"
    );
    handle.abort();
}

#[tokio::test]
async fn race_attempt_or_report_lets_a_slow_but_healthy_attempt_finish_instead_of_cancelling_it() {
    // Regression for a real defect caught in review: an earlier version
    // wrapped each individual attempt in its own `poll_interval`-sized
    // `tokio::time::timeout`, which would cancel a real connect attempt that
    // is merely pending/slow under load before it ever got a chance to
    // succeed. `Client::connect_as_operator` itself does no server
    // handshake (it opens the socket and reads local identity), so this
    // fake attempt is a simulated pending attempt, not a reproduced
    // production round trip — it only ever returns `Some` on its FIRST
    // call, and only after sleeping well past `poll_interval`. Under the
    // buggy per-attempt-timeout design, that first attempt would have been
    // cancelled and every subsequent call would return `None` immediately,
    // spinning until `deadline` and reporting `TimedOut` instead of success.
    // The existing five-test journey in verification_saturation_regression.rs
    // already exercises the real `Client`/Unix-socket connect path via
    // `connect_or_report`, so this is deliberately not duplicated here.
    let mut handle: tokio::task::JoinHandle<rk_core::Result<()>> =
        tokio::spawn(async { std::future::pending().await });

    let poll_interval = Duration::from_millis(20);
    let attempt_delay = poll_interval * 3;
    let deadline = Duration::from_secs(5);
    let wall_clock_start = Instant::now();

    let mut calls = 0;
    let result = support::race_attempt_or_report(&mut handle, deadline, poll_interval, move || {
        calls += 1;
        let this_call = calls;
        async move {
            if this_call == 1 {
                tokio::time::sleep(attempt_delay).await;
                Some(())
            } else {
                // Only the slow first attempt ever succeeds — proves the
                // fix didn't just get lucky on a later, fast retry.
                None
            }
        }
    })
    .await;

    assert!(
        matches!(result, Ok(())),
        "a single slow-but-successful attempt within the deadline must succeed, got: {result:?}"
    );
    let elapsed = wall_clock_start.elapsed();
    assert!(
        elapsed >= attempt_delay,
        "the attempt must have been allowed to run to completion, not cut short: {elapsed:?}"
    );
    assert!(
        elapsed < deadline,
        "must resolve well before the overall deadline, not by exhausting it: {elapsed:?}"
    );
    handle.abort();
}
