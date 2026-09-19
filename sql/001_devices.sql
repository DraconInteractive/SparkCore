-- Run once when provisioning a database, or re-run safely to create missing tables.
-- Times are UTC because SPARK sets its MariaDB session time zone to +00:00.
CREATE TABLE IF NOT EXISTS devices (
    id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    local_ip VARCHAR(45) CHARACTER SET ascii NOT NULL,
    external_ip VARCHAR(45) CHARACTER SET ascii NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

-- Intentionally no foreign key: history survives deletion and re-registration.
CREATE TABLE IF NOT EXISTS device_events (
    event_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    device_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    action ENUM('created', 'updated', 'deleted') NOT NULL,
    local_ip VARCHAR(45) CHARACTER SET ascii NOT NULL,
    external_ip VARCHAR(45) CHARACTER SET ascii NULL,
    recorded_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX device_history (device_id, event_id)
) ENGINE=InnoDB;
