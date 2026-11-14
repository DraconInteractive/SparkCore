-- Pending previews and decision receipts are durable and caller scoped.
ALTER TABLE action_history ADD COLUMN IF NOT EXISTS metadata JSON NULL;
CREATE TABLE IF NOT EXISTS assistant_confirmations (
 id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 owner VARCHAR(160) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
 fingerprint CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 conversation_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 kind ENUM('calendar.update','calendar.delete') NOT NULL,
 before_state JSON NOT NULL,
 after_state JSON NOT NULL,
 source_device VARCHAR(64) CHARACTER SET ascii NULL,
 status ENUM('pending','confirmed','cancelled','superseded','expired','conflict') NOT NULL DEFAULT 'pending',
 created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 expires_at DATETIME(6) NOT NULL,
 INDEX conversation(owner,conversation_id,created_at)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS assistant_confirmation_decisions (
 id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 owner VARCHAR(160) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
 fingerprint CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 confirmation_id CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 decision ENUM('confirm','cancel') NOT NULL
) ENGINE=InnoDB;
