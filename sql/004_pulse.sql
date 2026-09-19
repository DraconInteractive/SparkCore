-- One shared recurring pulse. Re-running setup preserves the current settings.
CREATE TABLE IF NOT EXISTS pulse (
    id INT NOT NULL PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    interval_seconds INT UNSIGNED NOT NULL DEFAULT 3600,
    next_pulse_at DATETIME(6) NULL,
    last_pulse_at DATETIME(6) NULL,
    pulse_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
INSERT IGNORE INTO pulse (id) VALUES (1);
