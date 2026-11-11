-- Existing tasks and older clients default to Normal. Labels are case-sensitive.
ALTER TABLE project_tasks ADD COLUMN IF NOT EXISTS severity
    ENUM('Very Low','Low','Normal','High','Very High','Urgent')
    CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'Normal';
