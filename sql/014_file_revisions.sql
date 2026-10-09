-- Empty revision keeps existing UUID.blob files compatible.
ALTER TABLE files ADD COLUMN IF NOT EXISTS content_revision VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '';
