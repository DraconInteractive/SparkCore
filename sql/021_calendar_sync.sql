-- Sync state is transactional with calendar data and included in database backups.
CREATE TABLE IF NOT EXISTS calendar_sync_identity (
    singleton TINYINT PRIMARY KEY,
    instance_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    sequence BIGINT UNSIGNED NOT NULL DEFAULT 0
) ENGINE=InnoDB;
INSERT IGNORE INTO calendar_sync_identity(singleton,instance_id) VALUES (1,UUID());
CREATE TABLE IF NOT EXISTS calendar_sync_items (
    sync_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    event_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    revision CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    sequence BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    data JSON NULL,
    UNIQUE KEY sequence_number(sequence),
    UNIQUE KEY local_event(event_id)
) ENGINE=InnoDB;
-- Only seed shared rows not already tracked; repeated migrations retain tombstones.
INSERT INTO calendar_sync_items(sync_id,event_id,revision,data)
SELECT c.id,c.id,UUID(),JSON_OBJECT('name',c.name,'description',c.description,
    'occurs_at',DATE_FORMAT(c.occurs_at,'%Y-%m-%dT%H:%i:%sZ'),'place',c.place,
    'recurrence',JSON_EXTRACT(c.recurrence,'$'),'reminder_minutes',c.reminder_minutes)
FROM calendar_events c WHERE c.calendar='shared'
AND NOT EXISTS (SELECT 1 FROM calendar_sync_items s WHERE s.event_id=c.id)
AND NOT EXISTS (SELECT 1 FROM calendar_sync_items s WHERE s.sync_id=c.id);
UPDATE calendar_sync_identity SET sequence=GREATEST(sequence,
    (SELECT COALESCE(MAX(sequence),0) FROM calendar_sync_items)) WHERE singleton=1;
CREATE TABLE IF NOT EXISTS calendar_sync_receipts (
    coordinator_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    operation_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request JSON NOT NULL,
    response JSON NOT NULL,
    PRIMARY KEY(coordinator_id,operation_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS calendar_sync_connections (
    connection_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    state JSON NOT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS calendar_sync_baselines (
    connection_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    sync_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    data JSON NOT NULL,
    PRIMARY KEY(connection_id,sync_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS calendar_sync_conflicts (
    sequence BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    connection_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    operation_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    data JSON NOT NULL,
    UNIQUE KEY operation(connection_id,operation_id)
) ENGINE=InnoDB;
