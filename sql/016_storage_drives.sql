CREATE TABLE IF NOT EXISTS storage_registry_lock (id INT NOT NULL PRIMARY KEY) ENGINE=InnoDB;
INSERT IGNORE INTO storage_registry_lock VALUES(1);
CREATE TABLE IF NOT EXISTS storage_drives (
    id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY
) ENGINE=InnoDB;
-- NULL identifies legacy files until checked and assigned at first startup.
ALTER TABLE files ADD COLUMN IF NOT EXISTS drive_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL;
