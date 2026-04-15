-- Initial schema for gridtokenx-noti-service
-- Migration: 20260415000000_initial_schema.sql

-- 1. Create Enums
DO $$ BEGIN
    CREATE TYPE notification_channel AS ENUM ('email', 'sms', 'push', 'webhook', 'websocket');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
    CREATE TYPE notification_status AS ENUM ('pending', 'processing', 'sent', 'delivered', 'failed', 'permanent_failure');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- 2. Create Notifications Table
CREATE TABLE IF NOT EXISTS notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID, -- Optional
    channel notification_channel NOT NULL,
    status notification_status NOT NULL DEFAULT 'pending',
    recipient TEXT NOT NULL, -- Email address, phone number, or webhook URL
    template_id TEXT NOT NULL,
    variables JSONB DEFAULT '{}',
    provider_id TEXT, -- e.g., 'twilio', 'sendgrid'
    provider_ref TEXT, -- ID from the external provider
    retry_count INT DEFAULT 0,
    next_retry_at TIMESTAMPTZ DEFAULT NOW(),
    error_message TEXT,
    idempotency_key TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ
);

-- 3. Create Indexes
CREATE INDEX IF NOT EXISTS idx_notifications_status_retry ON notifications(status, next_retry_at) 
WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id);

-- 4. Audit/Trigger for updated_at (If using a common trigger, otherwise manually handle)
-- For now, we'll manually handle updated_at in code or add a generic trigger if common in the project.
-- Based on other services, they usually handle it in Rust or use a standard trigger.
