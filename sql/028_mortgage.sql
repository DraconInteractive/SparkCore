-- Singleton snapshot. Keep the row/revision when clearing to prevent stale writes.
CREATE TABLE IF NOT EXISTS budget_mortgage (
 id INT NOT NULL PRIMARY KEY,
 data JSON NULL,
 revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
 updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
INSERT IGNORE INTO budget_mortgage(id) VALUES (1);
