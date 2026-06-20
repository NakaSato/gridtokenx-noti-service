//! Environment-parsing tests for `Config::from_env` (closes gap #9).
//!
//! `from_env` is the only constructor the binary uses, yet nothing exercised
//! it: the env→field mapping (the `config` crate's `__`-separator scheme), the
//! `#[serde(default)]` fallbacks, the optional fields (`grpc_port`, the `smtp_*`
//! set), and the "a required field is missing → error" path were all untested.
//! A typo in a `serde(default)` fn, a renamed field, or a wrong default would
//! ship silently. This drives the real function over a controlled process
//! environment.
//!
//! `from_env` reads the *process* environment, so all cases run inside a single
//! `#[test]` (one integration-test binary = one process; one test fn = no
//! intra-process parallelism) which fully owns and resets the env between
//! cases. `RUN_MODE` is pinned to a name with no `config/<mode>` file so no
//! on-disk source perturbs the result.

#![allow(unsafe_code, clippy::expect_used, clippy::unwrap_used)]

use noti_core::config::Config;

/// Every env key that maps onto a `Config` field — cleared before each case so
/// stray ambient vars (a CI `PORT`, a shell `DATABASE_URL`) can't leak in.
const MAPPED_KEYS: &[&str] = &[
    "PORT",
    "DATABASE_URL",
    "KAFKA_BROKERS",
    "RABBITMQ_URL",
    "REDIS_URL",
    "LOG_LEVEL",
    "DATABASE_MAX_CONNECTIONS",
    "DATABASE_MIN_CONNECTIONS",
    "DATABASE_ACQUIRE_TIMEOUT_SECS",
    "DATABASE_IDLE_TIMEOUT_SECS",
    "TWILIO_ACCOUNT_SID",
    "TWILIO_AUTH_TOKEN",
    "FCM_PROJECT_ID",
    "SMTP_HOST",
    "SMTP_PORT",
    "SMTP_USER",
    "SMTP_PASS",
    "SMTP_FROM",
    "SMTP_TLS_MODE",
    "JWT_SECRET",
    "FRONTEND_URL",
    "GRPC_PORT",
    "KAFKA_TOPIC_USER_EVENTS",
    "KAFKA_TOPIC_AUDIT_EVENTS",
    "KAFKA_TOPIC_TRADING_TRIGGERS",
];

/// Wipe every mapped key, then set only `pairs`. `set_var`/`remove_var` are
/// `unsafe` in edition 2024; safe here because the whole suite is single
/// threaded within this one process.
fn set_env(pairs: &[(&str, &str)]) {
    unsafe {
        for k in MAPPED_KEYS {
            std::env::remove_var(k);
        }
        // No `config/<mode>` file exists for this mode → only env contributes.
        std::env::set_var("RUN_MODE", "test-isolated-no-such-file");
        for (k, v) in pairs {
            std::env::set_var(k, v);
        }
    }
}

/// The five fields with no default and no `Option` — the minimal valid set.
fn required() -> Vec<(&'static str, &'static str)> {
    vec![
        ("DATABASE_URL", "postgres://u:p@localhost:5432/db"),
        ("KAFKA_BROKERS", "localhost:9092"),
        ("RABBITMQ_URL", "amqp://guest:guest@localhost:5672"),
        ("REDIS_URL", "redis://localhost:6379"),
        ("JWT_SECRET", "a-secret"),
    ]
}

#[test]
fn config_from_env_parsing() {
    // ---- Case A: minimal required set → defaults applied ------------------
    set_env(&required());
    let c = Config::from_env().expect("minimal required env must parse");

    assert_eq!(c.database_url, "postgres://u:p@localhost:5432/db");
    assert_eq!(c.jwt_secret, "a-secret");
    // serde defaults:
    assert_eq!(c.port, 8080, "default port");
    assert_eq!(c.log_level, "info", "default log level");
    assert_eq!(c.database_max_connections, 20);
    assert_eq!(c.database_min_connections, 5);
    assert_eq!(c.database_acquire_timeout_secs, 30);
    assert_eq!(c.database_idle_timeout_secs, 600);
    assert_eq!(c.kafka_topic_user_events, "iam.user.events");
    assert_eq!(c.kafka_topic_audit_events, "iam.audit.events");
    assert_eq!(c.kafka_topic_trading_triggers, "trading.triggers");
    // optionals default to None:
    assert_eq!(c.grpc_port, None, "grpc_port unset → None (run() derives +10)");
    assert_eq!(c.smtp_host, None);
    assert_eq!(c.smtp_tls_mode, None);
    assert_eq!(c.frontend_url, None);

    // ---- Case B: overrides parse into typed fields -----------------------
    let mut env = required();
    env.push(("PORT", "9100"));
    env.push(("GRPC_PORT", "9555"));
    env.push(("DATABASE_MAX_CONNECTIONS", "50"));
    env.push(("LOG_LEVEL", "debug"));
    env.push(("KAFKA_TOPIC_USER_EVENTS", "custom.user.events"));
    env.push(("FRONTEND_URL", "https://app.example.test"));
    set_env(&env);
    let c = Config::from_env().expect("override env must parse");

    assert_eq!(c.port, 9100, "PORT parsed as u16");
    assert_eq!(c.grpc_port, Some(9555), "GRPC_PORT parsed into Some");
    assert_eq!(c.database_max_connections, 50);
    assert_eq!(c.log_level, "debug");
    assert_eq!(c.kafka_topic_user_events, "custom.user.events", "default overridden");
    assert_eq!(c.frontend_url.as_deref(), Some("https://app.example.test"));

    // ---- Case C: SMTP_TLS_MODE variants carried through verbatim ----------
    // Config does no validation; SmtpProvider interprets the string. Assert the
    // value is faithfully carried for each documented mode.
    for mode in ["starttls", "tls", "none"] {
        let mut env = required();
        env.push(("SMTP_HOST", "smtp.example.test"));
        env.push(("SMTP_PORT", "2525"));
        env.push(("SMTP_TLS_MODE", mode));
        set_env(&env);
        let c = Config::from_env().expect("smtp env must parse");
        assert_eq!(c.smtp_host.as_deref(), Some("smtp.example.test"));
        assert_eq!(c.smtp_port, Some(2525), "SMTP_PORT parsed as u16");
        assert_eq!(c.smtp_tls_mode.as_deref(), Some(mode), "tls mode {mode} carried");
    }

    // ---- Case D: a missing required field → error ------------------------
    let without_db: Vec<_> = required()
        .into_iter()
        .filter(|(k, _)| *k != "DATABASE_URL")
        .collect();
    set_env(&without_db);
    let err = Config::from_env();
    assert!(
        err.is_err(),
        "missing DATABASE_URL must fail to deserialize, got {err:?}"
    );

    // ---- Cleanup ----------------------------------------------------------
    set_env(&[]);
}
