-- Existing events remain one-off events with no reminder.
ALTER TABLE calendar_events
    ADD COLUMN IF NOT EXISTS recurrence JSON NULL,
    ADD COLUMN IF NOT EXISTS reminder_minutes INT NULL,
    ADD COLUMN IF NOT EXISTS last_reminder_occurs_at DATETIME NULL;
