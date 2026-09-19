-- Serializes dispatch sequencing across multiple SPARK processes. Held only while
-- copying committed event IDs, never during network writes.
CREATE TABLE IF NOT EXISTS notification_dispatch_lock (
    id INT NOT NULL PRIMARY KEY
) ENGINE=InnoDB;
INSERT IGNORE INTO notification_dispatch_lock (id) VALUES (1);

-- Dispatch order, not event-ID order, is safe to use as a stream cursor.
-- Rows mean sequenced for broadcast, not acknowledged by clients.
CREATE TABLE IF NOT EXISTS notification_log (
    sequence BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    event_id BIGINT UNSIGNED NOT NULL UNIQUE,
    sequenced_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    FOREIGN KEY (event_id) REFERENCES events(event_id)
) ENGINE=InnoDB;
