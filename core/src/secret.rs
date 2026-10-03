//! Secrets sealed before they go into Felix. Felix is not a secret store, so
//! a value read from the `config` cache without the relay's key is useless.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use serde::{Deserialize, Serialize};

const NONCE_BYTES: usize = 24;

/// The relay's sealing key, `RELAY_SECRET_KEY`: 32 bytes as base64 or hex.
#[derive(Clone)]
pub struct SecretKey(XChaCha20Poly1305);

/// A secret as stored: base64 of a random nonce followed by the ciphertext.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sealed(String);

/// A key or a sealed value that cannot be used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretError(&'static str);

impl std::fmt::Display for SecretError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for SecretError {}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretKey(..)")
    }
}

impl SecretKey {
    /// # Errors
    /// When `encoded` is not 32 bytes in base64 or hex.
    pub fn parse(encoded: &str) -> Result<Self, SecretError> {
        let encoded = encoded.trim();
        let bytes = hex::decode(encoded)
            .ok()
            .or_else(|| BASE64.decode(encoded).ok())
            .filter(|bytes| bytes.len() == 32)
            .ok_or(SecretError(
                "RELAY_SECRET_KEY must be 32 bytes, base64 or hex",
            ))?;
        Ok(Self(
            XChaCha20Poly1305::new_from_slice(&bytes).expect("32 bytes"),
        ))
    }

    /// Seal `secret` for the entry named `name`. The name is bound in as
    /// associated data, so a sealed value copied to another entry will not open.
    pub fn seal(&self, name: &str, secret: &str) -> Sealed {
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let payload = Payload {
            msg: secret.as_bytes(),
            aad: name.as_bytes(),
        };
        let ciphertext = self
            .0
            .encrypt(&nonce, payload)
            .expect("sealing cannot fail");
        let mut bytes = nonce.to_vec();
        bytes.extend(ciphertext);
        Sealed(BASE64.encode(bytes))
    }

    /// # Errors
    /// When `sealed` was not sealed by this key for `name`.
    pub fn open(&self, name: &str, sealed: &Sealed) -> Result<String, SecretError> {
        let wrong = SecretError("a sealed secret does not open with RELAY_SECRET_KEY");
        let bytes = BASE64.decode(&sealed.0).map_err(|_| wrong.clone())?;
        if bytes.len() < NONCE_BYTES {
            return Err(wrong);
        }
        let (nonce, ciphertext) = bytes.split_at(NONCE_BYTES);
        let payload = Payload {
            msg: ciphertext,
            aad: name.as_bytes(),
        };
        let plain = self
            .0
            .decrypt(XNonce::from_slice(nonce), payload)
            .map_err(|_| wrong.clone())?;
        String::from_utf8(plain).map_err(|_| wrong)
    }
}

/// 32 random bytes, for new secrets and tokens.
pub fn random_bytes() -> [u8; 32] {
    XChaCha20Poly1305::generate_key(&mut OsRng).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    #[test]
    fn seals_and_opens() {
        let key = SecretKey::parse(KEY).unwrap();
        let sealed = key.seal("endpoint/a", "whsec_abc");
        assert_eq!(key.open("endpoint/a", &sealed).unwrap(), "whsec_abc");
        assert!(!sealed.0.contains("whsec_abc"));
    }

    #[test]
    fn a_sealed_value_is_bound_to_its_entry_and_key() {
        let key = SecretKey::parse(KEY).unwrap();
        let sealed = key.seal("endpoint/a", "whsec_abc");
        assert!(key.open("endpoint/b", &sealed).is_err());
        let other = SecretKey::parse(&BASE64.encode([9u8; 32])).unwrap();
        assert!(other.open("endpoint/a", &sealed).is_err());
        assert!(key.open("endpoint/a", &Sealed("AAAA".to_string())).is_err());
    }

    #[test]
    fn every_seal_uses_a_new_nonce() {
        let key = SecretKey::parse(KEY).unwrap();
        assert_ne!(key.seal("s", "x"), key.seal("s", "x"));
    }

    #[test]
    fn keys_are_32_bytes_in_hex_or_base64() {
        assert!(SecretKey::parse(KEY).is_ok());
        assert!(SecretKey::parse(&BASE64.encode([1u8; 32])).is_ok());
        assert!(SecretKey::parse(&BASE64.encode([1u8; 16])).is_err());
        assert!(SecretKey::parse("not a key").is_err());
    }
}
