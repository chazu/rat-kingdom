//! A small, self-contained cron expression evaluator for the daemon scheduler.
//!
//! Standard 5-field syntax (`minute hour day-of-month month day-of-week`) plus
//! the common `@macro` shorthands, evaluated in UTC at minute granularity. Each
//! field supports `*`, comma lists (`a,b`), ranges (`a-b`), and steps (`*/n`,
//! `a-b/n`). Names (`MON`, `JAN`) are intentionally unsupported — numeric only —
//! to keep the parser tiny and unambiguous.
//!
//! # The day-of-month / day-of-week rule
//!
//! Vixie cron's quirk is preserved: when BOTH the day-of-month and day-of-week
//! fields are restricted (not a bare `*`), a day matches if EITHER field matches
//! (a logical OR). When only one is restricted, it alone decides; when neither
//! is, every day passes. A field counts as restricted iff its literal text is
//! not exactly `*` — so `*/2` is restricted, matching Vixie semantics.

use chrono::{DateTime, Datelike, Timelike, Utc};

/// A parsed cron expression: one allow-set per field, precomputed at parse time.
#[derive(Debug, Clone, PartialEq)]
pub struct Cron {
    minutes: [bool; 60],
    hours: [bool; 24],
    /// Indexed by day-of-month value directly; index 0 is unused.
    doms: [bool; 32],
    /// Indexed by month value directly; index 0 is unused.
    months: [bool; 13],
    /// Indexed by day-of-week 0..=6 with 0 = Sunday.
    dows: [bool; 7],
    dom_restricted: bool,
    dow_restricted: bool,
}

impl Cron {
    /// Parse a 5-field cron expression or a supported `@macro`.
    pub fn parse(expr: &str) -> Result<Cron, String> {
        let expr = expr.trim();
        let normalized = expand_macro(expr).unwrap_or_else(|| expr.to_string());
        let fields: Vec<&str> = normalized.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(format!(
                "expected 5 cron fields (or an @macro), got {}: {expr:?}",
                fields.len()
            ));
        }

        let (minutes, _) = parse_field(fields[0], 0, 59)?;
        let (hours, _) = parse_field(fields[1], 0, 23)?;
        let (doms_vec, dom_restricted) = parse_field(fields[2], 1, 31)?;
        let (months_vec, _) = parse_field(fields[3], 1, 12)?;
        // Day-of-week accepts 0..=7 on input; 7 is an alias for Sunday (0).
        let (dow_vec, dow_restricted) = parse_field(fields[4], 0, 7)?;

        let mut minutes_arr = [false; 60];
        minutes_arr.copy_from_slice(&minutes);
        let mut hours_arr = [false; 24];
        hours_arr.copy_from_slice(&hours);
        let mut doms = [false; 32];
        doms.copy_from_slice(&doms_vec);
        let mut months = [false; 13];
        months.copy_from_slice(&months_vec);

        let mut dows = [false; 7];
        for (i, on) in dow_vec.iter().enumerate() {
            if *on {
                dows[i % 7] = true; // fold 7 -> 0 (Sunday)
            }
        }

        Ok(Cron {
            minutes: minutes_arr,
            hours: hours_arr,
            doms,
            months,
            dows,
            dom_restricted,
            dow_restricted,
        })
    }

    /// Whether the given instant's minute satisfies this expression (UTC).
    pub fn matches(&self, dt: DateTime<Utc>) -> bool {
        if !self.minutes[dt.minute() as usize]
            || !self.hours[dt.hour() as usize]
            || !self.months[dt.month() as usize]
        {
            return false;
        }
        let dom_ok = self.doms[dt.day() as usize];
        let dow_ok = self.dows[dt.weekday().num_days_from_sunday() as usize];
        match (self.dom_restricted, self.dow_restricted) {
            (true, true) => dom_ok || dow_ok, // Vixie OR when both restricted
            (true, false) => dom_ok,
            (false, true) => dow_ok,
            (false, false) => true,
        }
    }
}

/// Expand a `@macro` to its 5-field equivalent, or `None` if it is not a macro.
fn expand_macro(expr: &str) -> Option<String> {
    let field = match expr {
        "@yearly" | "@annually" => "0 0 1 1 *",
        "@monthly" => "0 0 1 * *",
        "@weekly" => "0 0 * * 0",
        "@daily" | "@midnight" => "0 0 * * *",
        "@hourly" => "0 * * * *",
        _ => return None,
    };
    Some(field.to_string())
}

/// Parse one cron field into an allow-set of size `max + 1` (values indexed
/// directly) and whether it is "restricted" (literal text is not `*`).
fn parse_field(spec: &str, min: u32, max: u32) -> Result<(Vec<bool>, bool), String> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err("empty cron field".into());
    }
    let restricted = spec != "*";
    let mut set = vec![false; (max + 1) as usize];

    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err(format!("empty item in cron field {spec:?}"));
        }
        // Optional step: `range/step`.
        let (range_part, step) = match part.split_once('/') {
            Some((r, s)) => {
                let step: u32 = s
                    .parse()
                    .map_err(|_| format!("bad step {s:?} in cron field {spec:?}"))?;
                if step == 0 {
                    return Err(format!("zero step in cron field {spec:?}"));
                }
                (r, step)
            }
            None => (part, 1),
        };
        // Range bounds. `*` spans the full [min, max]; `a-b` is explicit; a lone
        // value `a` is the point a..=a (unless a step widens it to a..=max, the
        // conventional `a/step` reading).
        let (lo, hi) = if range_part == "*" {
            (min, max)
        } else if let Some((a, b)) = range_part.split_once('-') {
            (parse_num(a, spec)?, parse_num(b, spec)?)
        } else {
            let v = parse_num(range_part, spec)?;
            // `a/step` means "from a to the field max, every step".
            if step > 1 {
                (v, max)
            } else {
                (v, v)
            }
        };
        if lo < min || hi > max || lo > hi {
            return Err(format!(
                "cron field {spec:?} value {lo}-{hi} out of range {min}-{max}"
            ));
        }
        let mut i = lo;
        while i <= hi {
            set[i as usize] = true;
            i += step;
        }
    }
    Ok((set, restricted))
}

fn parse_num(s: &str, field: &str) -> Result<u32, String> {
    s.trim()
        .parse()
        .map_err(|_| format!("bad number {s:?} in cron field {field:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    #[test]
    fn every_minute_matches_always() {
        let c = Cron::parse("* * * * *").unwrap();
        assert!(c.matches(at(2026, 7, 23, 0, 0)));
        assert!(c.matches(at(2026, 12, 31, 23, 59)));
    }

    #[test]
    fn fixed_time_of_day() {
        let c = Cron::parse("30 9 * * *").unwrap();
        assert!(c.matches(at(2026, 7, 23, 9, 30)));
        assert!(!c.matches(at(2026, 7, 23, 9, 31)));
        assert!(!c.matches(at(2026, 7, 23, 10, 30)));
    }

    #[test]
    fn macros_expand() {
        assert!(Cron::parse("@hourly")
            .unwrap()
            .matches(at(2026, 7, 23, 14, 0)));
        assert!(!Cron::parse("@hourly")
            .unwrap()
            .matches(at(2026, 7, 23, 14, 1)));
        // @daily fires at midnight only.
        let d = Cron::parse("@daily").unwrap();
        assert!(d.matches(at(2026, 7, 23, 0, 0)));
        assert!(!d.matches(at(2026, 7, 23, 1, 0)));
        // @weekly is Sunday 00:00. 2026-07-26 is a Sunday.
        let w = Cron::parse("@weekly").unwrap();
        assert!(w.matches(at(2026, 7, 26, 0, 0)));
        assert!(!w.matches(at(2026, 7, 23, 0, 0)));
    }

    #[test]
    fn step_and_range_and_list() {
        // Every 15 minutes.
        let c = Cron::parse("*/15 * * * *").unwrap();
        for m in [0, 15, 30, 45] {
            assert!(c.matches(at(2026, 7, 23, 8, m)), "minute {m}");
        }
        assert!(!c.matches(at(2026, 7, 23, 8, 7)));

        // Business hours range 9-17, on the hour.
        let biz = Cron::parse("0 9-17 * * *").unwrap();
        assert!(biz.matches(at(2026, 7, 23, 9, 0)));
        assert!(biz.matches(at(2026, 7, 23, 17, 0)));
        assert!(!biz.matches(at(2026, 7, 23, 18, 0)));

        // Comma list of minutes.
        let list = Cron::parse("0,20,40 * * * *").unwrap();
        assert!(list.matches(at(2026, 7, 23, 8, 20)));
        assert!(!list.matches(at(2026, 7, 23, 8, 21)));

        // Stepped range: minutes 10..=30 every 10 -> 10,20,30.
        let sr = Cron::parse("10-30/10 * * * *").unwrap();
        assert!(sr.matches(at(2026, 7, 23, 8, 20)));
        assert!(!sr.matches(at(2026, 7, 23, 8, 25)));
        assert!(!sr.matches(at(2026, 7, 23, 8, 40)));
    }

    #[test]
    fn day_of_week_sunday_is_zero_and_seven() {
        // 2026-07-26 is a Sunday.
        let z = Cron::parse("0 0 * * 0").unwrap();
        let s = Cron::parse("0 0 * * 7").unwrap();
        assert!(z.matches(at(2026, 7, 26, 0, 0)));
        assert!(s.matches(at(2026, 7, 26, 0, 0)), "7 aliases Sunday");
        assert!(!z.matches(at(2026, 7, 27, 0, 0)));
    }

    #[test]
    fn dom_and_dow_both_restricted_is_or() {
        // "0 0 13 * 5" — midnight on the 13th OR any Friday.
        // 2026-07-13 is a Monday (the 13th), 2026-07-17 is a Friday.
        let c = Cron::parse("0 0 13 * 5").unwrap();
        assert!(c.matches(at(2026, 7, 13, 0, 0)), "the 13th matches via dom");
        assert!(c.matches(at(2026, 7, 17, 0, 0)), "a Friday matches via dow");
        assert!(!c.matches(at(2026, 7, 14, 0, 0)), "neither -> no match");
    }

    #[test]
    fn dom_restricted_only_is_and() {
        // Only day-of-month restricted: dow is `*`, so weekday is ignored.
        let c = Cron::parse("0 0 15 * *").unwrap();
        assert!(c.matches(at(2026, 7, 15, 0, 0)));
        assert!(!c.matches(at(2026, 7, 16, 0, 0)));
    }

    #[test]
    fn month_field() {
        let c = Cron::parse("0 0 1 1 *").unwrap(); // Jan 1st midnight
        assert!(c.matches(at(2026, 1, 1, 0, 0)));
        assert!(!c.matches(at(2026, 2, 1, 0, 0)));
    }

    #[test]
    fn rejects_malformed() {
        assert!(Cron::parse("").is_err());
        assert!(Cron::parse("* * * *").is_err(), "too few fields");
        assert!(Cron::parse("* * * * * *").is_err(), "too many fields");
        assert!(Cron::parse("60 * * * *").is_err(), "minute out of range");
        assert!(Cron::parse("* 24 * * *").is_err(), "hour out of range");
        assert!(Cron::parse("* * 0 * *").is_err(), "dom below range");
        assert!(Cron::parse("* * * 13 *").is_err(), "month out of range");
        assert!(Cron::parse("* * * * 8").is_err(), "dow above range");
        assert!(Cron::parse("*/0 * * * *").is_err(), "zero step");
        assert!(Cron::parse("5-2 * * * *").is_err(), "inverted range");
        assert!(Cron::parse("abc * * * *").is_err(), "not a number");
    }
}
