CREATE TABLE IF NOT EXISTS configs (
    id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    name VARCHAR(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    description TEXT CHARACTER SET utf8mb4 NOT NULL,
    tags JSON NOT NULL,
    data JSON NOT NULL,
    revision BIGINT UNSIGNED NOT NULL DEFAULT 1,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX config_name (name),
    INDEX config_created (created_at,id)
) ENGINE=InnoDB;
