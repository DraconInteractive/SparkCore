CREATE TABLE IF NOT EXISTS daily_tasks (
    id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    name VARCHAR(200) CHARACTER SET utf8mb4 NOT NULL,
    description TEXT CHARACTER SET utf8mb4 NOT NULL,
    checked BOOLEAN NOT NULL DEFAULT FALSE,
    checked_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX list_order (created_at,id)
) ENGINE=InnoDB;
-- Serializes day rollover and task changes so a new-day check cannot be erased
-- by a concurrent reset. Setup never overwrites an existing day.
CREATE TABLE IF NOT EXISTS daily_task_day (
    id INT NOT NULL PRIMARY KEY,
    local_day DATE NOT NULL
) ENGINE=InnoDB;
INSERT IGNORE INTO daily_task_day(id,local_day) VALUES (1,DATE(UTC_TIMESTAMP()+INTERVAL 8 HOUR));
