-- Generic action envelope. Each implemented kind supplies transaction-bound
-- mutation and reversal logic; unimplemented kinds cannot be executed or undone.
CREATE TABLE IF NOT EXISTS action_history (
 id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 owner VARCHAR(160) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
 fingerprint CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 kind VARCHAR(80) CHARACTER SET ascii NOT NULL,
 resource VARCHAR(255) CHARACTER SET ascii NOT NULL DEFAULT '',
 before_state JSON NULL,
 after_state JSON NOT NULL,
 undo_state JSON NULL,
 status ENUM('completed','undone') NOT NULL DEFAULT 'completed',
 created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 undo_until DATETIME(6) NOT NULL,
 undone_at DATETIME(6) NULL,
 INDEX owner_history(owner,created_at)
) ENGINE=InnoDB;
