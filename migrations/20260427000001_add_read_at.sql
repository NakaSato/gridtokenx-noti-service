-- Add read_at column to notifications
-- Migration: 20260427000001_add_read_at.sql

ALTER TABLE notifications ADD COLUMN IF NOT EXISTS read_at TIMESTAMPTZ;

-- Add index for unread count performance
CREATE INDEX IF NOT EXISTS idx_notifications_user_unread ON notifications(user_id) 
WHERE read_at IS NULL;
