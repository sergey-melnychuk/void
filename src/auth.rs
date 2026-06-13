//! Identity & secrets: room IDs, admin secrets, session tokens, the admin
//! token derivation, and password hashing.
//!
//! The admin token is never stored. It is derived client-side as
//! `HMAC-SHA256(key = secret, msg = room_id)` and recomputed server-side from
//! the room's secret on every privileged action, then compared in constant
//! time (see [`admin_token`] / [`ct_eq`]).

use hmac::{Hmac, Mac};
use rand::distributions::Alphanumeric;
use rand::{Rng, RngCore};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// 3 random bytes rendered as 6 lowercase hex chars, e.g. `a3f9c2`.
/// 16M possible rooms.
pub fn new_room_id() -> String {
    let mut b = [0u8; 3];
    rand::thread_rng().fill_bytes(&mut b);
    hex::encode(b)
}

/// 16-char URL-safe alphanumeric secret, e.g. `8f3kQpXmN2vLzR9w`.
/// Whoever holds it is admin.
pub fn new_secret() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(16)
        .map(char::from)
        .collect()
}

/// 16 random bytes as hex — an opaque per-user session token.
pub fn new_session_token() -> String {
    let mut b = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut b);
    hex::encode(b)
}

/// Derive the admin token: `HMAC-SHA256(secret, room_id)` as lowercase hex.
pub fn admin_token(secret: &str, room_id: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts keys of any size");
    mac.update(room_id.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Constant-time string comparison — no early-out, so no timing side channel
/// on admin-token validation.
pub fn ct_eq(a: &str, b: &str) -> bool {
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

pub fn hash_password(password: &str, cost: u32) -> Result<String, bcrypt::BcryptError> {
    bcrypt::hash(password, cost)
}

pub fn verify_password(password: &str, hash: &str) -> bool {
    bcrypt::verify(password, hash).unwrap_or(false)
}
