-- Empty drive_id marks legacy global directories until persistent storage is known.
ALTER TABLE file_directories
    ADD COLUMN IF NOT EXISTS drive_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    DROP PRIMARY KEY, ADD PRIMARY KEY (drive_id,path);
ALTER TABLE projects
    ADD COLUMN IF NOT EXISTS drive_id CHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL,
    DROP INDEX IF EXISTS directory,
    DROP INDEX IF EXISTS project_sibling_name,
    ADD UNIQUE INDEX IF NOT EXISTS project_drive_directory (drive_id,directory),
    ADD UNIQUE INDEX project_sibling_name (drive_id,parent_scope,name);
-- Safe to repeat: only unassigned legacy rows are migrated, never existing drive data.
INSERT IGNORE INTO file_directories(drive_id,path)
    SELECT s.id,d.path FROM file_directories d JOIN storage_drives s ON s.persistent=TRUE WHERE d.drive_id='';
DELETE FROM file_directories WHERE drive_id='' AND EXISTS(SELECT 1 FROM storage_drives WHERE persistent=TRUE);
UPDATE projects SET drive_id=(SELECT id FROM storage_drives WHERE persistent=TRUE LIMIT 1) WHERE drive_id IS NULL;

INSERT IGNORE INTO file_directories(drive_id,path) SELECT id,'/' FROM storage_drives;
