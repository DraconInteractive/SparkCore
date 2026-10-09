CREATE TABLE IF NOT EXISTS project_boards (
    id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    project_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    name VARBINARY(200) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY board_name (project_id,name),
    CONSTRAINT board_project FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS project_columns (
    id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    board_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    name VARBINARY(200) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY column_name (board_id,name),
    UNIQUE KEY column_board (id,board_id),
    CONSTRAINT column_board_owner FOREIGN KEY (board_id) REFERENCES project_boards(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS project_tasks (
    id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    board_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    column_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    name VARCHAR(200) CHARACTER SET utf8mb4 NOT NULL,
    description TEXT CHARACTER SET utf8mb4 NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    KEY task_board (board_id),
    CONSTRAINT task_column FOREIGN KEY (column_id,board_id) REFERENCES project_columns(id,board_id) ON DELETE CASCADE
) ENGINE=InnoDB;
