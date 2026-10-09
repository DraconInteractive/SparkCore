-- Existing projects remain roots. Root and sibling names are unique separately.
ALTER TABLE projects
    ADD COLUMN IF NOT EXISTS parent_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NULL,
    ADD COLUMN IF NOT EXISTS parent_scope VARBINARY(36)
        GENERATED ALWAYS AS (IFNULL(RTRIM(parent_id),'')) PERSISTENT,
    ADD INDEX IF NOT EXISTS project_parent (parent_id),
    ADD UNIQUE INDEX IF NOT EXISTS project_sibling_name (parent_scope,name),
    DROP INDEX IF EXISTS name;
-- The API keeps parent_id immutable and validates parent existence while locked.
