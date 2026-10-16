#!/usr/bin/env python3
"""Exercise calendar separation against a legacy schema in the private test database."""
import os,subprocess
from pathlib import Path
assert os.environ.get('SPARK_DB_NAME','').startswith('spark_test_')
command=['mariadb','--no-defaults','--socket='+os.environ['SPARK_DB_SOCKET'],'--user=root','--batch','--skip-column-names']
db='spark_test_calendar_migration_'+str(os.getpid())
def sql(query,database=True):
    return subprocess.check_output(command+([db] if database else []),input=query,text=True).strip()
try:
    sql('CREATE DATABASE '+db,False)
    sql(Path('sql/006_calendar.sql').read_text()+Path('sql/008_calendar_recurrence.sql').read_text())
    sql("INSERT INTO calendar_events(id,name,description,occurs_at,reminder_minutes,last_reminder_occurs_at) VALUES ('legacy','Original','Keep me','2026-10-16 01:00:00',5,'2026-10-16 01:00:00')")
    before=sql("SELECT CONCAT_WS('|',id,name,description,occurs_at,reminder_minutes,last_reminder_occurs_at,created_at,updated_at) FROM calendar_events")
    migration=Path('sql/020_event_calendars.sql').read_text();sql(migration)
    assert sql('SELECT calendar FROM calendar_events')=='local'
    assert sql("SELECT CONCAT_WS('|',id,name,description,occurs_at,reminder_minutes,last_reminder_occurs_at,created_at,updated_at) FROM calendar_events")==before
    sql("UPDATE calendar_events SET calendar='shared' WHERE id='legacy'");sql(migration)
    assert sql('SELECT calendar FROM calendar_events')=='shared'
    invalid=subprocess.run(command+[db],input="UPDATE calendar_events SET calendar='other'",text=True,capture_output=True)
    assert invalid.returncode!=0,'database accepted invalid calendar'
    print('Calendar migration preserves legacy data and reminder state; repeated migration preserves shared entries.')
finally:
    sql('DROP DATABASE IF EXISTS '+db,False)
