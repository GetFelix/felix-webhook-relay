//! Checking what senders sign, and signing what the relay delivers.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// How far a signed timestamp may be from the relay's clock, either way.
pub const TOLERANCE_SECS: u64 = 300;

/// How a source proves that a webhook came from its sender.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Scheme {
    /// [Standard Webhooks](https://www.standardwebhooks.com/): `webhook-id`,
    /// `webhook-timestamp` and `webhook-signature`, under a `whsec_` secret.
    StandardWebhooks,
    /// GitHub's `X-Hub-Signature-256: sha256=<hex>` over the body.
    Github,
    /// Stripe's `Stripe-Signature: t=<secs>,v1=<hex>` over `<t>.<body>`.
    Stripe,
    /// HMAC-SHA256 of the body in a header the source names. An optional
    /// `sha256=` prefix on the value is ignored.
    Hmac {
        header: String,
        #[serde(default)]
        encoding: Encoding,
    },
    /// No signature. The secret is a long random token in the intake URL,
    /// which is weaker: anyone who sees the URL can send.
    Token,
}

/// How a generic HMAC header writes the digest.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Encoding {
    #[default]
    Hex,
    Base64,
}

/// Why intake refused a webhook. Every case is a `401`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    Missing(String),
    BadSignature,
    Stale,
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing(what) => write!(f, "missing {what}"),
            Self::BadSignature => f.write_str("signature does not match"),
            Self::Stale => write!(f, "timestamp is more than {TOLERANCE_SECS} s from now"),
        }
    }
}

/// A Standard Webhooks secret that is not `whsec_` and base64.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadSecret;

impl std::fmt::Display for BadSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a Standard Webhooks secret is whsec_ followed by base64")
    }
}

impl std::error::Error for BadSecret {}

impl Scheme {
    /// Whether `secret` can be used with this scheme.
    ///
    /// # Errors
    /// When the scheme is Standard Webhooks and the secret is not `whsec_` and base64.
    pub fn check_secret(&self, secret: &str) -> Result<(), BadSecret> {
        match self {
            Self::StandardWebhooks => standard_key(secret).map(drop),
            _ if secret.is_empty() => Err(BadSecret),
            _ => Ok(()),
        }
    }

    /// Check one request. `header` looks a header up by its lowercase name,
    /// `token` is the token from the intake URL, and `now` is Unix seconds.
    ///
    /// # Errors
    /// The reason to answer `401`.
    pub fn verify<'a>(
        &self,
        secret: &str,
        header: impl Fn(&str) -> Option<&'a str>,
        token: Option<&str>,
        body: &[u8],
        now: u64,
    ) -> Result<(), Refusal> {
        let required = |name: &str| header(name).ok_or_else(|| Refusal::Missing(name.to_string()));
        match self {
            Self::StandardWebhooks => {
                let id = required("webhook-id")?;
                let timestamp = timestamp(required("webhook-timestamp")?, now)?;
                let expected = standard_signature(secret, id, timestamp, body)
                    .map_err(|_| Refusal::BadSignature)?;
                let matches = required("webhook-signature")?
                    .split_whitespace()
                    .any(|candidate| bool::from(candidate.as_bytes().ct_eq(expected.as_bytes())));
                matches.then_some(()).ok_or(Refusal::BadSignature)
            }
            Self::Github => {
                let value = required("x-hub-signature-256")?;
                let digest = value.strip_prefix("sha256=").ok_or(Refusal::BadSignature)?;
                check_hmac(secret.as_bytes(), &[body], &decode(digest, Encoding::Hex)?)
            }
            Self::Stripe => {
                let value = required("stripe-signature")?;
                let mut signed_at = None;
                let mut signatures = Vec::new();
                for part in value.split(',') {
                    match part.trim().split_once('=') {
                        Some(("t", t)) => signed_at = Some(t),
                        Some(("v1", signature)) => signatures.push(signature),
                        _ => {}
                    }
                }
                let signed_at = signed_at.ok_or_else(|| Refusal::Missing("t".to_string()))?;
                timestamp(signed_at, now)?;
                let signed = [signed_at.as_bytes(), b".", body];
                let matches = signatures.into_iter().any(|signature| {
                    decode(signature, Encoding::Hex)
                        .and_then(|digest| check_hmac(secret.as_bytes(), &signed, &digest))
                        .is_ok()
                });
                matches.then_some(()).ok_or(Refusal::BadSignature)
            }
            Self::Hmac {
                header: name,
                encoding,
            } => {
                let value = required(name)?;
                let digest = value.strip_prefix("sha256=").unwrap_or(value);
                check_hmac(secret.as_bytes(), &[body], &decode(digest, *encoding)?)
            }
            Self::Token => {
                let token = token.ok_or_else(|| Refusal::Missing("token".to_string()))?;
                bool::from(token.as_bytes().ct_eq(secret.as_bytes()))
                    .then_some(())
                    .ok_or(Refusal::BadSignature)
            }
        }
    }
}

/// The Standard Webhooks signature, `v1,<base64>`, of one delivery.
///
/// # Errors
/// When `secret` is not `whsec_` and base64.
pub fn standard_signature(
    secret: &str,
    id: &str,
    timestamp: u64,
    body: &[u8],
) -> Result<String, BadSecret> {
    let mut mac = HmacSha256::new_from_slice(&standard_key(secret)?).expect("any key length");
    mac.update(format!("{id}.{timestamp}.").as_bytes());
    mac.update(body);
    Ok(format!("v1,{}", BASE64.encode(mac.finalize().into_bytes())))
}

/// A new Standard Webhooks secret from 32 random bytes.
pub fn new_standard_secret(random: [u8; 32]) -> String {
    format!("whsec_{}", BASE64.encode(random))
}

fn standard_key(secret: &str) -> Result<Vec<u8>, BadSecret> {
    let encoded = secret.strip_prefix("whsec_").ok_or(BadSecret)?;
    match BASE64.decode(encoded) {
        Ok(key) if !key.is_empty() => Ok(key),
        _ => Err(BadSecret),
    }
}

fn timestamp(value: &str, now: u64) -> Result<u64, Refusal> {
    let at: u64 = value.trim().parse().map_err(|_| Refusal::Stale)?;
    (at.abs_diff(now) <= TOLERANCE_SECS)
        .then_some(at)
        .ok_or(Refusal::Stale)
}

fn decode(digest: &str, encoding: Encoding) -> Result<Vec<u8>, Refusal> {
    let digest = digest.trim();
    let decoded = match encoding {
        Encoding::Hex => hex::decode(digest).ok(),
        Encoding::Base64 => BASE64.decode(digest).ok(),
    };
    decoded.ok_or(Refusal::BadSignature)
}

/// Compares in constant time, as `verify_slice` does.
fn check_hmac(key: &[u8], parts: &[&[u8]], digest: &[u8]) -> Result<(), Refusal> {
    let mut mac = HmacSha256::new_from_slice(key).expect("any key length");
    for part in parts {
        mac.update(part);
    }
    mac.verify_slice(digest).map_err(|_| Refusal::BadSignature)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<&'a str> {
        move |name| pairs.iter().find(|(key, _)| *key == name).map(|(_, v)| *v)
    }

    // From the Standard Webhooks reference libraries' tests.
    const SW_SECRET: &str = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
    const SW_ID: &str = "msg_p5jXN8AQM9LWM0D4loKWxJek";
    const SW_AT: u64 = 1_614_265_330;
    const SW_BODY: &[u8] = br#"{"test": 2432232314}"#;
    const SW_SIGNATURE: &str = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=";

    fn standard(signature: &str, at: &str, now: u64) -> Result<(), Refusal> {
        let pairs = [
            ("webhook-id", SW_ID),
            ("webhook-timestamp", at),
            ("webhook-signature", signature),
        ];
        Scheme::StandardWebhooks.verify(SW_SECRET, headers(&pairs), None, SW_BODY, now)
    }

    #[test]
    fn standard_webhooks_signs_the_reference_vector() {
        assert_eq!(
            standard_signature(SW_SECRET, SW_ID, SW_AT, SW_BODY).unwrap(),
            SW_SIGNATURE
        );
    }

    #[test]
    fn standard_webhooks_verifies() {
        let at = SW_AT.to_string();
        assert_eq!(standard(SW_SIGNATURE, &at, SW_AT + 10), Ok(()));
        let several = format!("v1,bm90IGl0 {SW_SIGNATURE}");
        assert_eq!(standard(&several, &at, SW_AT), Ok(()));
        assert_eq!(
            standard("v1,bm90IGl0", &at, SW_AT),
            Err(Refusal::BadSignature)
        );
        assert_eq!(
            standard(SW_SIGNATURE, &at, SW_AT + TOLERANCE_SECS + 1),
            Err(Refusal::Stale)
        );
        assert_eq!(
            standard(SW_SIGNATURE, &at, SW_AT - TOLERANCE_SECS - 1),
            Err(Refusal::Stale)
        );
        let missing = Scheme::StandardWebhooks.verify(SW_SECRET, |_| None, None, SW_BODY, SW_AT);
        assert_eq!(missing, Err(Refusal::Missing("webhook-id".to_string())));
    }

    // From GitHub's "Validating webhook deliveries".
    #[test]
    fn github_verifies() {
        let secret = "It's a Secret to Everybody";
        let good = [(
            "x-hub-signature-256",
            "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17",
        )];
        assert_eq!(
            Scheme::Github.verify(secret, headers(&good), None, b"Hello, World!", 0),
            Ok(())
        );
        assert_eq!(
            Scheme::Github.verify(secret, headers(&good), None, b"Hello, World?", 0),
            Err(Refusal::BadSignature)
        );
        let unprefixed = [(
            "x-hub-signature-256",
            "757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17",
        )];
        assert_eq!(
            Scheme::Github.verify(secret, headers(&unprefixed), None, b"Hello, World!", 0),
            Err(Refusal::BadSignature)
        );
    }

    // Stripe's docs give the construction, HMAC-SHA256 of `t.body` as hex,
    // but no vector with its secret, so this one was computed outside Rust.
    const STRIPE_SECRET: &str = "whsec_test_secret";
    const STRIPE_BODY: &[u8] = br#"{"id":"evt_1","object":"event"}"#;
    const STRIPE_V1: &str = "654b43d3508ecadb3f8cefc4bc7d3d2bdc5ba9e03bf2c89b2d0657a8e24e4543";

    fn stripe(value: &str, now: u64) -> Result<(), Refusal> {
        let pairs = [("stripe-signature", value)];
        Scheme::Stripe.verify(STRIPE_SECRET, headers(&pairs), None, STRIPE_BODY, now)
    }

    #[test]
    fn stripe_verifies() {
        let at = 1_492_774_577;
        assert_eq!(stripe(&format!("t={at},v1={STRIPE_V1}"), at), Ok(()));
        let rolled = format!("t={at},v1=00ff,v1={STRIPE_V1},v0=6ffbb59b2300aae63f2");
        assert_eq!(stripe(&rolled, at + 60), Ok(()));
        assert_eq!(
            stripe(&format!("t={at},v1=00ff"), at),
            Err(Refusal::BadSignature)
        );
        assert_eq!(
            stripe(&format!("t={},v1={STRIPE_V1}", at + 1), at),
            Err(Refusal::BadSignature)
        );
        assert_eq!(
            stripe(&format!("t={at},v1={STRIPE_V1}"), at + TOLERANCE_SECS + 1),
            Err(Refusal::Stale)
        );
        assert_eq!(
            stripe(&format!("v1={STRIPE_V1}"), at),
            Err(Refusal::Missing("t".to_string()))
        );
    }

    #[test]
    fn generic_hmac_verifies_hex_and_base64() {
        let body = br#"{"ok":true}"#;
        let hex_digest = "bbb3b3c4cbaa75cef706d50357234102354ada79214380626d98fbff276909d2";
        let hex_scheme = Scheme::Hmac {
            header: "x-signature".to_string(),
            encoding: Encoding::Hex,
        };
        for value in [hex_digest.to_string(), format!("sha256={hex_digest}")] {
            let pairs = [("x-signature", value.as_str())];
            assert_eq!(
                hex_scheme.verify("generic-secret", headers(&pairs), None, body, 0),
                Ok(())
            );
        }
        let base64_scheme = Scheme::Hmac {
            header: "x-signature".to_string(),
            encoding: Encoding::Base64,
        };
        let pairs = [(
            "x-signature",
            "u7OzxMuqdc73BtUDVyNBAjVK2nkhQ4BibZj7/ydpCdI=",
        )];
        assert_eq!(
            base64_scheme.verify("generic-secret", headers(&pairs), None, body, 0),
            Ok(())
        );
        assert_eq!(
            base64_scheme.verify("other-secret", headers(&pairs), None, body, 0),
            Err(Refusal::BadSignature)
        );
    }

    #[test]
    fn token_compares_the_path_token() {
        let secret = "tok_8d1c0e5b";
        assert_eq!(
            Scheme::Token.verify(secret, |_| None, Some(secret), b"", 0),
            Ok(())
        );
        assert_eq!(
            Scheme::Token.verify(secret, |_| None, Some("tok_8d1c0e5c"), b"", 0),
            Err(Refusal::BadSignature)
        );
        assert!(
            Scheme::Token
                .verify(secret, |_| None, None, b"", 0)
                .is_err()
        );
    }

    #[test]
    fn secrets_are_checked_per_scheme() {
        assert!(Scheme::StandardWebhooks.check_secret(SW_SECRET).is_ok());
        assert!(Scheme::StandardWebhooks.check_secret("hunter2").is_err());
        assert!(Scheme::StandardWebhooks.check_secret("whsec_").is_err());
        assert!(Scheme::Github.check_secret("hunter2").is_ok());
        assert!(Scheme::Token.check_secret("").is_err());
        let fresh = new_standard_secret([7; 32]);
        assert!(Scheme::StandardWebhooks.check_secret(&fresh).is_ok());
    }

    #[test]
    fn schemes_read_from_json() {
        let scheme: Scheme =
            serde_json::from_str(r#"{"type":"hmac","header":"x-sig","encoding":"base64"}"#)
                .unwrap();
        assert_eq!(
            scheme,
            Scheme::Hmac {
                header: "x-sig".to_string(),
                encoding: Encoding::Base64
            }
        );
        let scheme: Scheme = serde_json::from_str(r#"{"type":"standard-webhooks"}"#).unwrap();
        assert_eq!(scheme, Scheme::StandardWebhooks);
    }
}
