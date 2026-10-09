-- Serializes queue transitions and action replacement for this PoC.
CREATE TABLE IF NOT EXISTS command_queue_lock (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
INSERT IGNORE INTO command_queue_lock VALUES (1);
CREATE TABLE IF NOT EXISTS device_actions (
    device_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    action VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY(device_id, action), INDEX action_devices(action, device_id),
    FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE CASCADE
) ENGINE=InnoDB;
-- No device FKs: routing intent and execution history survive device deletion.
CREATE TABLE IF NOT EXISTS commands (
    id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    source_device VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    target_device VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    executing_device VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    action VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    input JSON NOT NULL,
    idempotency_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    expires_in_seconds INT UNSIGNED NOT NULL,
    status ENUM('queued','running','succeeded','failed','expired','cancelled','outcome_unknown') NOT NULL DEFAULT 'queued',
    claim_token CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    result JSON NULL, error JSON NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    expires_at DATETIME(6) NOT NULL,
    lease_expires_at DATETIME(6) NULL,
    UNIQUE KEY request_key(source_device,idempotency_key),
    INDEX pending(status,created_at,id), INDEX expiry(status,expires_at,lease_expires_at),
    INDEX source_history(source_device,created_at,id)
) ENGINE=InnoDB;
