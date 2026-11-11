#!/usr/bin/env python3
"""Verify severity migration preserves legacy tasks and is safe to rerun."""
import os, subprocess
from pathlib import Path
assert os.environ.get('SPARK_DB_NAME','').startswith('spark_test_')
command=['mariadb','--no-defaults','--socket='+os.environ['SPARK_DB_SOCKET'],'--user=root','--batch','--skip-column-names']
db='spark_test_severity_'+str(os.getpid())
def sql(query,database=True):
    return subprocess.check_output(command+([db] if database else []),input=query,text=True).strip()
try:
    sql('CREATE DATABASE '+db,False)
    sql(Path('sql/011_projects.sql').read_text()+Path('sql/013_project_boards.sql').read_text())
    sql("INSERT INTO projects(id,name,description,directory) VALUES ('project','Legacy','','/projects/legacy'); INSERT INTO project_boards(id,project_id,name) VALUES ('board','project','Development'); INSERT INTO project_columns(id,board_id,name) VALUES ('column','board','Todo'); INSERT INTO project_tasks(id,board_id,column_id,name,description) VALUES ('task','board','column','Original','Keep me')")
    query="SELECT CONCAT_WS('|',id,board_id,column_id,name,description,created_at,updated_at) FROM project_tasks"
    before=sql(query)
    migration=Path('sql/024_board_task_severity.sql').read_text();sql(migration)
    assert sql('SELECT severity FROM project_tasks')=='Normal'
    assert sql(query)==before
    sql("UPDATE project_tasks SET severity='Urgent'");sql(migration)
    assert sql('SELECT severity FROM project_tasks')=='Urgent'
    assert sql(query)==before
    print('Board severity migration preserves existing task data, defaults to Normal and safely reruns.')
finally:
    sql('DROP DATABASE IF EXISTS '+db,False)
