-- Separate administrator requesters from devices; preserve existing device history.
ALTER TABLE commands
    MODIFY COLUMN source_device VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    ADD COLUMN IF NOT EXISTS source_admin VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NULL,
    ADD COLUMN IF NOT EXISTS requester_key VARCHAR(140) CHARACTER SET ascii COLLATE ascii_bin
        GENERATED ALWAYS AS (IF(source_admin IS NULL,CONCAT('device:',source_device),CONCAT('admin:',source_admin))) STORED,
    DROP INDEX IF EXISTS request_key,
    ADD UNIQUE INDEX IF NOT EXISTS request_identity (requester_key,idempotency_key);
