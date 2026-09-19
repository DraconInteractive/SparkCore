CREATE TABLE IF NOT EXISTS timers (
    id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    label VARCHAR(200) CHARACTER SET utf8mb4 NOT NULL DEFAULT '',
    duration_seconds INT UNSIGNED NOT NULL,
    status ENUM('running','expired','confirmed','stopped') NOT NULL DEFAULT 'running',
    source_device VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    ends_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX active_deadlines (status, ends_at, id)
) ENGINE=InnoDB;

-- Generic durable domain events. Written in the same transaction as each change.
-- No foreign keys: resource/device deletion must not erase event history.
-- Event IDs identify events, but concurrent transactions can commit out of ID order.
-- A future notification dispatcher must track delivery rather than assume ID order.
CREATE TABLE IF NOT EXISTS events (
    event_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    type VARCHAR(80) CHARACTER SET ascii NOT NULL,
    resource VARCHAR(255) CHARACTER SET ascii NOT NULL,
    source_device VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    payload JSON NOT NULL,
    recorded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX resource_events (resource, event_id)
) ENGINE=InnoDB;
