//! Liveness signal for the background Kafka consumer.
//!
//! The HTTP `/health/ready` probe reads this so a wedged or disconnected
//! consumer is reported **not ready**, instead of the process staying falsely
//! green while it silently drops events. (See the 2026-06 stall: the consumer
//! was evicted for ~13h while `/health` kept returning 200, so nothing
//! restarted it and no verification emails went out.)
//!
//! Two independent signals gate readiness:
//! - `connected` — flipped true on every successful poll, false on a fatal
//!   broker error (`AllBrokersDown`, `PollExceeded`). This is the primary
//!   signal: librdkafka surfaces eviction/disconnect as a recv error.
//! - `last_progress_secs` — a heartbeat refreshed on every poll *and* on each
//!   idle tick, so a consumer that is "connected" but no longer looping is
//!   still caught as stale.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Current wall-clock time as Unix seconds.
///
/// Used only for the consumer liveness heartbeat / staleness comparison, where
/// a coarse, locally-read clock is the right tool (the SNTP-backed
/// `gridtokenx_telemetry::time::now()` would add a network round-trip to a hot
/// loop and to the readiness probe). Saturates rather than panicking on a clock
/// before the epoch.
#[must_use]
pub fn unix_now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// Shared health state for the background Kafka consumer.
///
/// Wrapped in an `Arc` and handed to both the consumer loop (writer) and the
/// `/health/ready` handler (reader).
#[derive(Debug)]
pub struct KafkaConsumerHealth {
    /// Whether the consumer currently believes it is connected and subscribed.
    connected: AtomicBool,
    /// Set when the consumer is intentionally not running (no Kafka brokers
    /// configured). Readiness is reported OK in that mode — there is no
    /// consumer whose liveness we could be lying about.
    disabled: AtomicBool,
    /// Unix seconds of the last consumer-loop progress (poll returned or idle
    /// tick fired).
    last_progress_secs: AtomicI64,
}

impl KafkaConsumerHealth {
    /// Create a new health state. Starts **not** connected until the consumer
    /// subscribes and polls successfully.
    #[must_use]
    pub fn new() -> Self {
        Self {
            connected: AtomicBool::new(false),
            disabled: AtomicBool::new(false),
            last_progress_secs: AtomicI64::new(0),
        }
    }

    /// Record that the consumer is connected and made progress at `now_secs`.
    pub fn mark_progress(&self, now_secs: i64) {
        self.connected.store(true, Ordering::Relaxed);
        self.last_progress_secs.store(now_secs, Ordering::Relaxed);
    }

    /// Record that the consumer lost its broker connection or was evicted.
    pub fn mark_down(&self) {
        self.connected.store(false, Ordering::Relaxed);
    }

    /// Mark the consumer intentionally disabled (no brokers configured). In
    /// this mode `is_ready` always returns `true`.
    pub fn mark_disabled(&self) {
        self.disabled.store(true, Ordering::Relaxed);
    }

    /// Unix seconds of the last recorded progress.
    #[must_use]
    pub fn last_progress_secs(&self) -> i64 {
        self.last_progress_secs.load(Ordering::Relaxed)
    }

    /// Ready iff the consumer is disabled, or it is currently connected **and**
    /// made progress within `stale_after_secs` of `now_secs`.
    #[must_use]
    pub fn is_ready(&self, now_secs: i64, stale_after_secs: i64) -> bool {
        if self.disabled.load(Ordering::Relaxed) {
            return true;
        }
        if !self.connected.load(Ordering::Relaxed) {
            return false;
        }
        let last = self.last_progress_secs.load(Ordering::Relaxed);
        now_secs.saturating_sub(last) <= stale_after_secs
    }
}

impl Default for KafkaConsumerHealth {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_state_is_not_ready() {
        let h = KafkaConsumerHealth::new();
        // Never polled → not connected → not ready.
        assert!(!h.is_ready(100, 90));
    }

    #[test]
    fn progress_within_window_is_ready() {
        let h = KafkaConsumerHealth::new();
        h.mark_progress(100);
        assert!(h.is_ready(150, 90), "50s since progress, threshold 90");
    }

    #[test]
    fn stale_progress_is_not_ready() {
        let h = KafkaConsumerHealth::new();
        h.mark_progress(100);
        // Connected, but last poll was 200s ago > 90s threshold → stale.
        assert!(!h.is_ready(300, 90));
    }

    #[test]
    fn mark_down_drops_readiness() {
        let h = KafkaConsumerHealth::new();
        h.mark_progress(100);
        assert!(h.is_ready(120, 90));
        h.mark_down();
        assert!(!h.is_ready(120, 90), "down consumer must not be ready");
    }

    #[test]
    fn disabled_is_always_ready() {
        let h = KafkaConsumerHealth::new();
        h.mark_disabled();
        // Disabled overrides both the connected flag and staleness.
        assert!(h.is_ready(10_000, 90));
    }

    #[test]
    fn last_progress_is_tracked() {
        let h = KafkaConsumerHealth::new();
        assert_eq!(h.last_progress_secs(), 0);
        h.mark_progress(4242);
        assert_eq!(h.last_progress_secs(), 4242);
    }
}
