-- Push device-token registry for the Push (FCM) channel.
-- Migration: 20260620000000_device_tokens.sql
--
-- A user's Push notifications fan out to every active row here. Tokens reported
-- invalid/unregistered by FCM are soft-revoked (revoked_at) so the registry
-- self-heals without losing audit history.

CREATE TABLE IF NOT EXISTS device_tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    token TEXT NOT NULL UNIQUE,
    platform TEXT NOT NULL CHECK (platform IN ('android', 'ios', 'web')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ
);

-- Fan-out lookup: active tokens for a user.
CREATE INDEX IF NOT EXISTS idx_device_tokens_user_active
ON device_tokens(user_id)
WHERE revoked_at IS NULL;
