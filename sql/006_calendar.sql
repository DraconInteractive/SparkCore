-- User-facing dated events, separate from the internal domain event/outbox log.
CREATE TABLE IF NOT EXISTS calendar_events (
    id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    name VARCHAR(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    description TEXT CHARACTER SET utf8mb4 NOT NULL,
    occurs_at DATETIME NOT NULL,
    place VARCHAR(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX chronological (occurs_at,id),
    INDEX place_time (place,occurs_at,id)
) ENGINE=InnoDB;
