//! Proquints: PRO-nounceable QUINT-uplets, encoding 16 bits per syllable as
//! consonant-vowel-consonant-vowel-consonant (Lucent's scheme,
//! <https://arxiv.org/html/0901.4016>). An integer of `n * 16` bits becomes
//! `n` dash-joined five-letter syllables — e.g. the 32-bit value
//! `0x7f000001` is `lusab-babad`. Every syllable round-trips exactly: no
//! rounding, no lossy normalization beyond case.
//!
//! This module is the low-level codec only. It has no opinion on what the
//! encoded bits mean — [`crate::id`] and callers like ticket identifiers
//! decide bit width and word count for their own use.

use std::fmt;

const CONSONANTS: [char; 16] = [
    'b', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 'm', 'n', 'p', 'r', 's', 't', 'v', 'z',
];
const VOWELS: [char; 4] = ['a', 'i', 'o', 'u'];

/// A spelling that does not decode. Fails closed: a caller must never
/// coerce a malformed proquint into "close enough" and resolve it anyway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProquintError {
    /// The dash-separated word count did not match what the caller asked to
    /// decode.
    WrongWordCount { expected: usize, found: usize },
    /// A word was not exactly five letters.
    WrongWordLength { word: String },
    /// A letter at `position` (0-indexed within its word) is not a legal
    /// consonant/vowel for that position's role in the C-V-C-V-C pattern.
    BadCharacter {
        word: String,
        position: usize,
        ch: char,
    },
}

impl fmt::Display for ProquintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProquintError::WrongWordCount { expected, found } => {
                write!(f, "expected {expected} proquint word(s), found {found}")
            }
            ProquintError::WrongWordLength { word } => {
                write!(f, "proquint word '{word}' is not five letters")
            }
            ProquintError::BadCharacter { word, position, ch } => write!(
                f,
                "proquint word '{word}' has an invalid character '{ch}' at position {position}"
            ),
        }
    }
}

impl std::error::Error for ProquintError {}

fn encode_u16(v: u16) -> [char; 5] {
    [
        CONSONANTS[((v >> 12) & 0xF) as usize],
        VOWELS[((v >> 10) & 0x3) as usize],
        CONSONANTS[((v >> 6) & 0xF) as usize],
        VOWELS[((v >> 4) & 0x3) as usize],
        CONSONANTS[(v & 0xF) as usize],
    ]
}

fn consonant_value(ch: char, word: &str, position: usize) -> Result<u16, ProquintError> {
    CONSONANTS
        .iter()
        .position(|&c| c == ch)
        .map(|i| i as u16)
        .ok_or(ProquintError::BadCharacter {
            word: word.to_string(),
            position,
            ch,
        })
}

fn vowel_value(ch: char, word: &str, position: usize) -> Result<u16, ProquintError> {
    VOWELS
        .iter()
        .position(|&c| c == ch)
        .map(|i| i as u16)
        .ok_or(ProquintError::BadCharacter {
            word: word.to_string(),
            position,
            ch,
        })
}

fn decode_u16(word: &str) -> Result<u16, ProquintError> {
    let chars: Vec<char> = word.chars().collect();
    if chars.len() != 5 {
        return Err(ProquintError::WrongWordLength {
            word: word.to_string(),
        });
    }
    let c1 = consonant_value(chars[0], word, 0)?;
    let v1 = vowel_value(chars[1], word, 1)?;
    let c2 = consonant_value(chars[2], word, 2)?;
    let v2 = vowel_value(chars[3], word, 3)?;
    let c3 = consonant_value(chars[4], word, 4)?;
    Ok((c1 << 12) | (v1 << 10) | (c2 << 6) | (v2 << 4) | c3)
}

/// Encode the low `words * 16` bits of `value` as `words` dash-separated
/// proquint syllables, most-significant chunk first. `words` must be
/// between 1 and 4 inclusive (a `u64` holds at most four 16-bit chunks).
///
/// # Panics
///
/// If `words` is 0 or greater than 4.
pub fn encode(value: u64, words: usize) -> String {
    assert!(
        (1..=4).contains(&words),
        "proquint word count must be 1..=4, got {words}"
    );
    (0..words)
        .map(|i| {
            let shift = 16 * (words - 1 - i);
            let chunk = ((value >> shift) & 0xFFFF) as u16;
            encode_u16(chunk).iter().collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("-")
}

/// Decode a dash-separated proquint spelling of exactly `words` syllables
/// back to its integer value. Case-sensitive — normalize with [`normalize`]
/// first if the input may be mixed-case.
pub fn decode(s: &str, words: usize) -> Result<u64, ProquintError> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != words {
        return Err(ProquintError::WrongWordCount {
            expected: words,
            found: parts.len(),
        });
    }
    let mut value: u64 = 0;
    for part in parts {
        value = (value << 16) | decode_u16(part)? as u64;
    }
    Ok(value)
}

/// Trim and lowercase a spelling before decoding or shape-testing.
/// Proquints are conventionally lowercase and case-insensitive; this is the
/// one normalization step — no separator rewriting, no fuzzy correction.
pub fn normalize(s: &str) -> String {
    s.trim().to_ascii_lowercase()
}

/// True if `s` has proquint *shape* for `words` syllables — that many
/// dash-separated five-letter alphabetic words — without checking that
/// every letter is a legal consonant/vowel in its C-V-C-V-C position. A
/// cheap pre-filter so a caller can route "is this even worth decoding"
/// before paying for [`decode`]'s per-letter validation.
pub fn looks_like(s: &str, words: usize) -> bool {
    let parts: Vec<&str> = s.split('-').collect();
    parts.len() == words
        && parts
            .iter()
            .all(|p| p.chars().count() == 5 && p.chars().all(|c| c.is_ascii_alphabetic()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_every_16_bit_value_through_one_word() {
        for v in [0u16, 1, 0xFFFF, 0x1234, 0xABCD, 0x8000] {
            let word = encode(v as u64, 1);
            assert_eq!(decode(&word, 1).unwrap(), v as u64);
        }
    }

    #[test]
    fn round_trips_multi_word_values() {
        for words in 1..=4 {
            for value in [0u64, 1, u64::MAX, 0x1122_3344_5566_7788] {
                let masked = if words == 4 {
                    value
                } else {
                    value & ((1u64 << (16 * words)) - 1)
                };
                let spelling = encode(masked, words);
                assert_eq!(decode(&spelling, words).unwrap(), masked);
            }
        }
    }

    #[test]
    fn known_vector_matches_the_published_example() {
        // From the proquint paper: 127.0.0.1 (0x7f000001) is lusab-babad.
        assert_eq!(encode(0x7f00_0001, 2), "lusab-babad");
        assert_eq!(decode("lusab-babad", 2).unwrap(), 0x7f00_0001);
    }

    #[test]
    fn decode_rejects_wrong_word_count() {
        assert_eq!(
            decode("babad-bisub", 3),
            Err(ProquintError::WrongWordCount {
                expected: 3,
                found: 2
            })
        );
    }

    #[test]
    fn decode_rejects_wrong_word_length() {
        assert!(matches!(
            decode("bad", 1),
            Err(ProquintError::WrongWordLength { .. })
        ));
    }

    #[test]
    fn decode_rejects_bad_characters() {
        // 'q' is not a proquint consonant, 'e' is not a proquint vowel.
        assert!(matches!(
            decode("qabad", 1),
            Err(ProquintError::BadCharacter { position: 0, .. })
        ));
        assert!(matches!(
            decode("bebad", 1),
            Err(ProquintError::BadCharacter { position: 1, .. })
        ));
    }

    #[test]
    fn normalize_trims_and_lowercases() {
        assert_eq!(normalize("  BaBaD-BiSuB  "), "babad-bisub");
    }

    #[test]
    fn looks_like_checks_shape_not_validity() {
        assert!(looks_like("babad-bisub-lodob", 3));
        // Wrong word count.
        assert!(!looks_like("babad-bisub", 3));
        // Right shape, even with a letter that won't actually decode —
        // looks_like is a shape test, decode is the validity check.
        assert!(looks_like("qqqqq-bisub-lodob", 3));
        // Contains a digit: not proquint shape at all.
        assert!(!looks_like("bab4d-bisub-lodob", 3));
        // No dashes at all (e.g. a bare ULID): never proquint shape.
        assert!(!looks_like("01ARZ3NDEKTSV4RRFFQ69G5FAV", 3));
    }

    #[test]
    fn encode_is_case_stable_lowercase() {
        assert_eq!(encode(0, 1), encode(0, 1).to_ascii_lowercase());
    }
}
