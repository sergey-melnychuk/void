//! Runtime configuration, sourced entirely from environment variables
//! (12-factor; see the Deployment section of VOID.md). All values have
//! sensible defaults so the binary runs with zero configuration.

use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub base_url: String,
    pub bcrypt_cost: u32,

    pub default_ttl_seconds: u64,
    pub max_ttl_seconds: u64,

    pub default_max_messages: usize,
    pub max_messages_per_room: usize,

    pub default_max_participants: usize,
    pub max_participants_per_room: usize,

    pub default_rate_limit_seconds: u64,
    pub max_message_length: usize,

    pub ttl_sweep_interval_seconds: u64,
}

fn var<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

impl Config {
    pub fn from_env() -> Self {
        Config {
            host: env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into()),
            port: var("PORT", 8080),
            base_url: env::var("BASE_URL").unwrap_or_else(|_| "http://localhost:8080".into()),
            bcrypt_cost: var("BCRYPT_COST", bcrypt::DEFAULT_COST),

            default_ttl_seconds: var("DEFAULT_TTL_SECONDS", 7200),
            max_ttl_seconds: var("MAX_TTL_SECONDS", 86400),

            default_max_messages: var("DEFAULT_MAX_MESSAGES", 200),
            max_messages_per_room: var("MAX_MESSAGES_PER_ROOM", 1000),

            default_max_participants: var("DEFAULT_MAX_PARTICIPANTS", 100),
            max_participants_per_room: var("MAX_PARTICIPANTS_PER_ROOM", 500),

            default_rate_limit_seconds: var("DEFAULT_RATE_LIMIT_SECONDS", 3),
            max_message_length: var("MAX_MESSAGE_LENGTH", 500),

            ttl_sweep_interval_seconds: var("TTL_SWEEP_INTERVAL_SECONDS", 60),
        }
    }

    /// Clamp a requested TTL (seconds) into `[1, max_ttl_seconds]`, defaulting
    /// when not provided.
    pub fn clamp_ttl(&self, requested: Option<u64>) -> u64 {
        requested.unwrap_or(self.default_ttl_seconds).clamp(1, self.max_ttl_seconds)
    }

    pub fn clamp_max_messages(&self, requested: Option<usize>) -> usize {
        requested.unwrap_or(self.default_max_messages).clamp(1, self.max_messages_per_room)
    }

    pub fn clamp_max_participants(&self, requested: Option<usize>) -> usize {
        requested
            .unwrap_or(self.default_max_participants)
            .clamp(1, self.max_participants_per_room)
    }

    /// Clamp the per-message rate limit (seconds). Upper-bounded so the
    /// downstream `* 1000` conversion to milliseconds can never overflow.
    pub fn clamp_rate_limit(&self, requested: Option<u64>) -> u64 {
        requested.unwrap_or(self.default_rate_limit_seconds).min(3600)
    }
}
