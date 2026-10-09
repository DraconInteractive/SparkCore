#!/bin/sh
# Each run gets an isolated database. Requires local MariaDB administrative access.
set -eu
cd "$(dirname "$0")/.."
db_user=$(id -un)
case "$db_user" in ''|*[!a-zA-Z0-9_]*) echo 'Unsupported OS username' >&2; exit 1;; esac
SPARK_DB_NAME="spark_test_$(date +%s)_$$"
export SPARK_DB_NAME
SPARK_FILE_DIR=$(mktemp -d /tmp/spark-files-test.XXXXXXXX)
export SPARK_FILE_DIR
export SPARK_TEST_AUDIT_FAILURE=1
export SPARK_DB_HOST=localhost SPARK_DB_USER="$db_user" SPARK_DB_PASSWORD=
unset SPARK_DB_SOCKET SPARK_DB_PORT
cleanup() { rm -rf -- "$SPARK_FILE_DIR"; printf 'DROP DATABASE IF EXISTS `%s`;\n' "$SPARK_DB_NAME" | sudo mariadb; }
printf 'CREATE DATABASE `%s`;\n' "$SPARK_DB_NAME" | sudo mariadb
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
{
    printf 'USE `%s`;\n' "$SPARK_DB_NAME"
    cat sql/001_devices.sql
    cat sql/002_timers.sql
    cat sql/003_notifications.sql
    cat sql/004_pulse.sql
    cat sql/005_pulse_subscriptions.sql
    cat sql/006_calendar.sql
    cat sql/007_daily_tasks.sql sql/008_calendar_recurrence.sql sql/009_files.sql sql/010_file_directories.sql sql/011_projects.sql sql/012_project_hierarchy.sql sql/013_project_boards.sql sql/014_file_revisions.sql sql/015_commands.sql sql/016_storage_drives.sql sql/017_runtime_drives.sql sql/018_drive_directories.sql sql/019_command_administrators.sql
    cat <<'SQL'
DELIMITER //
CREATE TRIGGER reject_test_event BEFORE INSERT ON device_events FOR EACH ROW
BEGIN
    IF NEW.device_id = 'rollback-probe' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Intentional audit failure for rollback test';
    END IF;
END//
DELIMITER ;
SQL
    printf "CREATE USER IF NOT EXISTS '%s'@'localhost' IDENTIFIED VIA unix_socket;\n" "$db_user"
    printf "GRANT SELECT, INSERT, UPDATE, DELETE, TRIGGER ON %s.* TO '%s'@'localhost';\n" "$SPARK_DB_NAME" "$db_user"
} | sudo mariadb
ctest --test-dir build --output-on-failure
# Verify that DELETE preserved history and that timestamps were recorded.
count=$(printf "SELECT COUNT(*) FROM device_events WHERE device_id='watch-1' AND action='deleted' AND recorded_at IS NOT NULL;" | mariadb --batch --skip-column-names "$SPARK_DB_NAME")
[ "$count" -ge 1 ] || { echo 'Deletion history missing' >&2; exit 1; }
printf 'Persistent device history verified.\n'
count=$(printf "SELECT COUNT(*) FROM devices WHERE id='rollback-probe';" | mariadb --batch --skip-column-names "$SPARK_DB_NAME")
[ "$count" -eq 0 ] || { echo 'Failed audit left an untracked device' >&2; exit 1; }
printf 'Atomic device/history rollback verified.\n'
