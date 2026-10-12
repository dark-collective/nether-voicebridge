//! Companion key tokens: `nvbck_<key-id>.<secret>`. The id is public (it names
//! the key in DM commands and audit lines); only SHA-256 of the secret is stored.

use base64::Engine;
use rand::Rng;
use sha2::{Digest, Sha256};

pub const TOKEN_PREFIX: &str = "nvbck_";

const ID_ALPHABET: &[u8] = b"abcdefghijkmnpqrstuvwxyz23456789";
const ID_LEN: usize = 10;

/// A freshly minted key: its public id, the full token (shown once), and the
/// hash to store.
pub struct Minted {
    pub id: String,
    pub token: String,
    pub secret_sha256: String,
}

pub fn mint() -> Minted {
    let mut rng = rand::thread_rng();
    let id: String = (0..ID_LEN).map(|_| ID_ALPHABET[rng.gen_range(0..ID_ALPHABET.len())] as char).collect();
    let mut secret = [0u8; 32];
    rng.fill(&mut secret);
    let secret = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
    Minted { token: format!("{TOKEN_PREFIX}{id}.{secret}"), secret_sha256: sha256_hex(&secret), id }
}

/// Random lease id (hex, 128 bits).
pub fn lease_id() -> String {
    let mut b = [0u8; 16];
    rand::thread_rng().fill(&mut b);
    hex(&b)
}

/// Split a presented token into (key id, secret). `None` for anything not
/// shaped like a token.
pub fn parse(token: &str) -> Option<(&str, &str)> {
    let rest = token.strip_prefix(TOKEN_PREFIX)?;
    let (id, secret) = rest.split_once('.')?;
    if id.is_empty() || id.len() > 64 || secret.is_empty() || secret.len() > 256 {
        return None;
    }
    Some((id, secret))
}

/// Does `secret` hash to `stored_hex`? Compares the full digest in constant time.
pub fn secret_matches(secret: &str, stored_hex: &str) -> bool {
    let presented = Sha256::digest(secret.as_bytes());
    let Some(stored) = unhex(stored_hex) else { return false };
    if stored.len() != presented.len() {
        return false;
    }
    stored.iter().zip(presented.iter()).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

pub fn sha256_hex(s: &str) -> String {
    hex(&Sha256::digest(s.as_bytes()))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minted_tokens_verify_and_others_do_not() {
        let m = mint();
        let (id, secret) = parse(&m.token).unwrap();
        assert_eq!(id, m.id);
        assert_eq!(id.len(), ID_LEN);
        assert!(secret_matches(secret, &m.secret_sha256));
        assert!(!secret_matches("wrong", &m.secret_sha256));
        assert!(!secret_matches(secret, "zz"));
        assert!(!secret_matches(secret, ""));
        let other = mint();
        assert_ne!(other.id, m.id);
        assert!(!secret_matches(parse(&other.token).unwrap().1, &m.secret_sha256));
    }

    #[test]
    fn parse_rejects_malformed_tokens() {
        assert!(parse("nvbck_abc").is_none());
        assert!(parse("Bearer nvbck_a.b").is_none());
        assert!(parse("nvbck_.b").is_none());
        assert!(parse("nvbck_a.").is_none());
        assert_eq!(parse("nvbck_a.b"), Some(("a", "b")));
    }
}
