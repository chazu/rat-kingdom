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
