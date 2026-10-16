-- Fixed calendars; existing events retain their identity and become local.
ALTER TABLE calendar_events
    ADD COLUMN IF NOT EXISTS calendar VARCHAR(6) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'local' CHECK (calendar IN ('local','shared')),
    ADD INDEX IF NOT EXISTS calendar_time (calendar,occurs_at,id);
