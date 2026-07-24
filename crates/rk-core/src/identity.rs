//! Castle identity: a per-machine Ed25519 keypair that authenticates every
//! replicated op. The public key — not the hostname — is the castle's stable,
//! forgery-resistant actor id.
//!
//! Why not the hostname (the pre-TKT-59 basis)? Hostnames collide across
//! machines, change under the operator's feet, and carry no proof of origin.
//! `arbitrate_claims` (earliest-actor-wins) and cross-castle trust both need an
//! identity that is (a) stable for the life of a `~/.rat-kingdom` home and (b)
//! bound to something only that castle holds. An Ed25519 secret key is exactly
//! that: the actor id is derived from the public key, and every op is signed so
//! a peer can verify it really came from the actor whose ref it rode in on.
//!
//! Key storage lives under the RK home (`<home>/castle.key`), written 0600.
//! The signing layer (op canonicalization) lives in `rk-sync`, which composes
//! the low-level [`CastleIdentity::sign`] / [`verify_sig`] here with its own
//! record shape — this module knows nothing of `SyncRecord`.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use std::path::Path;

/// Number of public-key hex chars that form the actor id. 16 hex = 64 bits of
/// the key: short enough for a git ref / log line, wide enough that two honest
/// castles never collide. The FULL key travels in every signed record, so this
/// prefix is a label — authentication rests on the signature, not its length.
const ACTOR_HEX_LEN: usize = 16;

/// A castle's signing identity: its Ed25519 secret key plus the actor id
/// derived from the matching public key.
pub struct CastleIdentity {
    signing: SigningKey,
    actor: String,
}

impl CastleIdentity {
    /// Load the keypair persisted at `path`, or generate a fresh one and persist
    /// it (0600) if the file is absent. Idempotent: two calls on the same path
    /// yield the same identity, so the daemon and syncer can each load it.
    pub fn load_or_create(path: &Path) -> crate::Result<Self> {
        if let Some(id) = Self::try_load(path)? {
            return Ok(id);
        }
        let id = Self::generate();
        id.persist(path)?;
        Ok(id)
    }

    /// A fresh random identity. Used by tests and as the first-run keypair.
    pub fn generate() -> Self {
        let signing = SigningKey::generate(&mut rand::rngs::OsRng);
        Self::from_signing_key(signing)
    }

    fn from_signing_key(signing: SigningKey) -> Self {
        let actor = actor_from_pubkey(&signing.verifying_key());
        Self { signing, actor }
    }

    fn try_load(path: &Path) -> crate::Result<Option<Self>> {
        let raw = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let bytes = hex::decode(raw.trim())
            .map_err(|e| crate::Error::Config(format!("castle key not valid hex: {e}")))?;
        let seed: [u8; 32] = bytes
            .try_into()
            .map_err(|_| crate::Error::Config("castle key must be 32 bytes".into()))?;
        Ok(Some(Self::from_signing_key(SigningKey::from_bytes(&seed))))
    }

    fn persist(&self, path: &Path) -> crate::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, hex::encode(self.signing.to_bytes()))?;
        // Secret material: owner read/write only. Best-effort on unix.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
        }
        Ok(())
    }

    /// This castle's actor id (derived from its public key).
    pub fn actor(&self) -> &str {
        &self.actor
    }

    /// The public key as lowercase hex — embedded in every signed record so any
    /// peer can verify without an out-of-band key exchange.
    pub fn public_key_hex(&self) -> String {
        hex::encode(self.signing.verifying_key().to_bytes())
    }

    /// Sign `msg`, returning the detached signature as lowercase hex.
    pub fn sign(&self, msg: &[u8]) -> String {
        hex::encode(self.signing.sign(msg).to_bytes())
    }

    /// Read the actor id from a persisted key WITHOUT minting one. Returns `None`
    /// if no key exists yet (e.g. the daemon has never run) or it is unreadable.
    /// Presentation-only callers (a [`CastleDisplay`] resolver in a read-only CLI
    /// render path) use this so merely printing a friendly name never has the
    /// side effect of creating signing material.
    pub fn actor_at(path: &Path) -> Option<String> {
        Self::try_load(path).ok().flatten().map(|id| id.actor)
    }
}

/// Presentation-only mapping from a wire actor id to an operator-facing display
/// string (TKT-124). An operator may set a friendly `castle_name` alias (e.g.
/// "Nikaido"); this resolver rewrites THIS castle's own actor id to that alias at
/// render time — and nothing else.
///
/// The alias is never signed, replicated, written to a git ref, or consulted in
/// arbitration/trust: those always key on [`CastleIdentity::actor`]. Two castles
/// that pick the same alias stay unambiguous on the wire because the wire never
/// sees the alias. Absent an alias every id renders as itself, so unset behaviour
/// is unchanged. Aliases for REMOTE castles are out of scope: any author that is
/// not this castle's own actor passes through verbatim.
#[derive(Debug, Clone)]
pub struct CastleDisplay {
    actor: String,
    alias: Option<String>,
}

impl CastleDisplay {
    /// Build a resolver for the local castle from its `actor` id and the
    /// operator's optional `castle_name`. A blank/whitespace alias is treated as
    /// unset so a stray `castle_name = ""` cannot erase the id.
    pub fn new(actor: impl Into<String>, alias: Option<String>) -> Self {
        let alias = alias.filter(|a| !a.trim().is_empty());
        Self {
            actor: actor.into(),
            alias,
        }
    }

    /// This castle's own display string: the alias if set, else its actor id.
    pub fn own(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.actor)
    }

    /// Render an author id for an operator: this castle's own actor id becomes its
    /// alias; every other author (a remote `castle-<hex>`, a rat name, "daemon")
    /// is returned unchanged.
    pub fn resolve<'a>(&'a self, author: &'a str) -> &'a str {
        match &self.alias {
            Some(alias) if author == self.actor => alias,
            _ => author,
        }
    }
}

/// Derive the actor id from a public key: `castle-<first 16 hex of key>`.
pub fn actor_from_pubkey(key: &VerifyingKey) -> String {
    format!("castle-{}", &hex::encode(key.to_bytes())[..ACTOR_HEX_LEN])
}

/// Derive the actor id a hex public key must map to, or `None` if the hex is not
/// a well-formed Ed25519 public key. Peers use this to reject a record whose
/// claimed actor does not match the key that signed it (impersonation).
pub fn actor_from_pubkey_hex(pubkey_hex: &str) -> Option<String> {
    let key = verifying_key(pubkey_hex)?;
    Some(actor_from_pubkey(&key))
}

/// Verify `sig_hex` over `msg` under the public key `pubkey_hex`. Any malformed
/// input (bad hex, wrong length, bad signature) is a plain `false` — never an
/// error — because verification sits on the best-effort record-union read path.
pub fn verify_sig(pubkey_hex: &str, sig_hex: &str, msg: &[u8]) -> bool {
    let Some(key) = verifying_key(pubkey_hex) else {
        return false;
    };
    let Ok(sig_bytes) = hex::decode(sig_hex) else {
        return false;
    };
    let Ok(sig_bytes) = <[u8; 64]>::try_from(sig_bytes.as_slice()) else {
        return false;
    };
    let sig = Signature::from_bytes(&sig_bytes);
    key.verify(msg, &sig).is_ok()
}

fn verifying_key(pubkey_hex: &str) -> Option<VerifyingKey> {
    let bytes = hex::decode(pubkey_hex).ok()?;
    let bytes: [u8; 32] = bytes.try_into().ok()?;
    VerifyingKey::from_bytes(&bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_is_derived_from_the_public_key_and_is_stable() {
        let id = CastleIdentity::generate();
        assert!(id.actor().starts_with("castle-"));
        // Derivation is a pure function of the public key.
        assert_eq!(
            actor_from_pubkey_hex(&id.public_key_hex()).as_deref(),
            Some(id.actor())
        );
    }

    #[test]
    fn sign_then_verify_round_trips_and_rejects_tampering() {
        let id = CastleIdentity::generate();
        let msg = b"replicated-op-bytes";
        let sig = id.sign(msg);
        assert!(verify_sig(&id.public_key_hex(), &sig, msg));
        // A different message under the same signature fails.
        assert!(!verify_sig(&id.public_key_hex(), &sig, b"other"));
        // A different key fails.
        let other = CastleIdentity::generate();
        assert!(!verify_sig(&other.public_key_hex(), &sig, msg));
    }

    #[test]
    fn malformed_inputs_verify_false_never_panic() {
        assert!(!verify_sig("not-hex", "also-not-hex", b"x"));
        assert!(!verify_sig("abcd", "ef01", b"x"));
        assert!(actor_from_pubkey_hex("zzzz").is_none());
    }

    #[test]
    fn display_resolves_own_actor_to_alias_and_leaves_others_verbatim() {
        let id = CastleIdentity::generate();
        let actor = id.actor().to_string();
        let display = CastleDisplay::new(actor.clone(), Some("Nikaido".into()));
        // Own name and the own actor id both render as the alias.
        assert_eq!(display.own(), "Nikaido");
        assert_eq!(display.resolve(&actor), "Nikaido");
        // A remote castle and a rat name pass through unchanged (remote aliases
        // are out of scope).
        assert_eq!(
            display.resolve("castle-deadbeefdeadbeef"),
            "castle-deadbeefdeadbeef"
        );
        assert_eq!(display.resolve("Martin"), "Martin");
        // The alias is purely presentational — the crypto actor id is untouched.
        assert_eq!(id.actor(), actor);
    }

    #[test]
    fn display_without_alias_falls_back_to_the_actor_id() {
        let id = CastleIdentity::generate();
        let actor = id.actor().to_string();
        // Unset, and a blank alias, both fall back to the actor id (no behaviour
        // change when `castle_name` is absent or empty).
        for alias in [None, Some(String::new()), Some("  ".into())] {
            let display = CastleDisplay::new(actor.clone(), alias);
            assert_eq!(display.own(), actor);
            assert_eq!(display.resolve(&actor), actor);
        }
    }

    #[test]
    fn actor_at_reads_without_minting_a_key() {
        let dir = std::env::temp_dir().join(format!("rk-id-at-{}", std::process::id()));
        let path = dir.join("castle.key");
        let _ = std::fs::remove_file(&path);
        // No key yet: a read-only display path must not create one.
        assert!(CastleIdentity::actor_at(&path).is_none());
        assert!(!path.exists());
        // Once the daemon mints one, the same actor id is readable side-effect-free.
        let id = CastleIdentity::load_or_create(&path).unwrap();
        assert_eq!(CastleIdentity::actor_at(&path).as_deref(), Some(id.actor()));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn load_or_create_persists_and_reloads_the_same_identity() {
        let dir = std::env::temp_dir().join(format!("rk-id-{}", std::process::id()));
        let path = dir.join("castle.key");
        let _ = std::fs::remove_file(&path);
        let first = CastleIdentity::load_or_create(&path).unwrap();
        let second = CastleIdentity::load_or_create(&path).unwrap();
        assert_eq!(first.actor(), second.actor());
        assert_eq!(first.public_key_hex(), second.public_key_hex());
        // Persisted file is the 32-byte seed as hex.
        let raw = std::fs::read_to_string(&path).unwrap();
        assert_eq!(raw.trim().len(), 64);
        let _ = std::fs::remove_file(&path);
    }
}
