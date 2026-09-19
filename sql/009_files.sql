CREATE TABLE IF NOT EXISTS files (
    id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    name VARCHAR(255) CHARACTER SET utf8mb4 NOT NULL,
    size_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
    state ENUM('uploading','ready','deleting') NOT NULL DEFAULT 'uploading',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX listing (state,created_at,id)
) ENGINE=InnoDB;
