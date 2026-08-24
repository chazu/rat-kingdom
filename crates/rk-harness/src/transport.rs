//! Shared provider-transport-failure classification.
//!
//! An adapter that watches its child exit before the harness ever reaches
//! its `Started` handshake hands the buffered stderr here instead of
//! letting the supervisor guess from vendor prose. Only a known, narrow set
//! of transport-shaped signals classify at all — a CLI usage error, a bad
//! flag, or any failure once real work has started falls through as `None`
//! and is left to ordinary failure handling, unchanged.

use crate::{TransportClass, TransportOutcome};

const CERTIFICATE_MARKERS: &[&str] = &[
    "certificate",
    "self signed",
    "self-signed",
    "x509",
    "tls",
    "ssl",
    "cert_",
];
const AUTHENTICATION_MARKERS: &[&str] = &[
    "unauthorized",
    "authentication failed",
    "not authenticated",
    "invalid api key",
    "401",
    "403",
    "forbidden",
    "please run",
    "auth error",
];
const UNAVAILABLE_MARKERS: &[&str] = &[
    "service unavailable",
    "503",
    "overloaded",
    "econnrefused",
    "connection refused",
    "temporarily unavailable",
];
const GENERIC_MARKERS: &[&str] = &[
    "econnreset",
    "etimedout",
    "enotfound",
    "network error",
    "connection reset",
    "broken pipe",
    "socket hang up",
    "transport error",
    "getaddrinfo",
];

/// Classify buffered stderr lines from a pre-`Started` process exit. Returns
/// `None` when nothing in `lines` matches a known transport signal — the
/// caller must then treat the exit as an ordinary launch/task failure.
///
/// Checked in this fixed priority order (certificate, then authentication,
/// then unavailable, then generic): a line can carry more than one signal
/// (e.g. a TLS error whose message also contains "unauthorized"), and
/// certificate/auth problems are the more actionable diagnosis when both are
/// present.
pub fn classify(provider: &str, lines: &[String]) -> Option<TransportOutcome> {
    let haystack = lines.join("\n").to_lowercase();
    let (class, marker) = [
        (TransportClass::Certificate, CERTIFICATE_MARKERS),
        (TransportClass::Authentication, AUTHENTICATION_MARKERS),
        (TransportClass::Unavailable, UNAVAILABLE_MARKERS),
        (TransportClass::Generic, GENERIC_MARKERS),
    ]
    .into_iter()
    .find_map(|(class, markers)| {
        markers
            .iter()
            .find(|m| haystack.contains(*m))
            .map(|m| (class, *m))
    })?;

    let evidence_line = lines
        .iter()
        .find(|l| l.to_lowercase().contains(marker))
        .cloned()
        .unwrap_or_default();

    Some(TransportOutcome {
        provider: provider.to_string(),
        // A rejected credential does not heal by reconnecting: retrying it
        // would just burn the bounded retry ceiling on a certain repeat, so
        // authentication escalates after this one attempt instead.
        retryable: !matches!(class, TransportClass::Authentication),
        class,
        generation: None,
        evidence: redact(&evidence_line),
    })
}

/// Best-effort secret scrub: an authentication/certificate error line can
/// carry a token, key fragment, or session id inline. Any run of 20+
/// token-shaped characters is replaced outright rather than attempting to
/// recognize a specific provider's secret format. Capped to a short single
/// line so a stray multi-KB dump never lands whole in a persisted record.
fn redact(line: &str) -> String {
    let mut out = String::new();
    let mut run = String::new();
    for c in line.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '+' | '/') {
            run.push(c);
        } else {
            flush_run(&mut run, &mut out);
            out.push(c);
        }
    }
    flush_run(&mut run, &mut out);
    out.chars().take(240).collect()
}

fn flush_run(run: &mut String, out: &mut String) {
    if run.len() >= 20 {
        out.push_str("[redacted]");
    } else {
        out.push_str(run);
    }
    run.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(s: &[&str]) -> Vec<String> {
        s.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn classifies_certificate_failure() {
        let outcome = classify(
            "claude",
            &lines(&["error: unable to get local issuer certificate"]),
        )
        .expect("must classify");
        assert_eq!(outcome.provider, "claude");
        assert_eq!(outcome.class, TransportClass::Certificate);
        assert!(outcome.retryable);
        assert_eq!(outcome.generation, None);
        assert!(outcome.evidence.contains("certificate"));
    }

    #[test]
    fn classifies_authentication_failure_as_not_retryable() {
        let outcome = classify("codex", &lines(&["401 Unauthorized: invalid api key"]))
            .expect("must classify");
        assert_eq!(outcome.class, TransportClass::Authentication);
        assert!(
            !outcome.retryable,
            "a rejected credential does not heal by reconnecting"
        );
    }

    #[test]
    fn classifies_unavailable_failure() {
        let outcome = classify("codex", &lines(&["503 Service Unavailable, overloaded"]))
            .expect("must classify");
        assert_eq!(outcome.class, TransportClass::Unavailable);
        assert!(outcome.retryable);
    }

    #[test]
    fn classifies_generic_transport_failure() {
        let outcome = classify("claude", &lines(&["connect ECONNRESET 1.2.3.4:443"]))
            .expect("must classify");
        assert_eq!(outcome.class, TransportClass::Generic);
        assert!(outcome.retryable);
    }

    #[test]
    fn ordinary_stderr_does_not_classify() {
        assert!(classify("claude", &lines(&["deprecation warning: flag --foo"])).is_none());
        assert!(classify("claude", &lines(&[])).is_none());
    }

    #[test]
    fn certificate_takes_priority_over_overlapping_authentication_signal() {
        let outcome = classify(
            "codex",
            &lines(&["TLS handshake failed: unauthorized client certificate"]),
        )
        .expect("must classify");
        assert_eq!(outcome.class, TransportClass::Certificate);
    }

    #[test]
    fn evidence_is_redacted_and_bounded() {
        let secret = "sk-ant-abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOP";
        let line = format!("401 unauthorized: invalid api key {secret}");
        let outcome = classify("claude", &lines(&[&line])).expect("must classify");
        assert!(!outcome.evidence.contains(secret));
        assert!(outcome.evidence.contains("[redacted]"));
        assert!(outcome.evidence.len() <= 240);
    }
}
