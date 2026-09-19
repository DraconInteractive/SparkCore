#!/bin/sh
# Run as the OS user that will run SPARK. Only database provisioning uses sudo.
set -eu
cd "$(dirname "$0")/.."
db_user=$(id -un)
case "$db_user" in ''|*[!a-zA-Z0-9_]*) echo 'OS username must contain only letters, digits, or underscores' >&2; exit 1;; esac
if [ "$db_user" = root ]; then echo 'Run this script as the SPARK service user, not root.' >&2; exit 1; fi
# Authenticate the local OS user using MariaDB unix_socket: no app password file.
{
    printf 'CREATE DATABASE IF NOT EXISTS spark;\n'
    printf "CREATE USER IF NOT EXISTS '%s'@'localhost' IDENTIFIED VIA unix_socket;\n" "$db_user"
    printf 'USE spark;\n'
    cat sql/001_devices.sql
    cat sql/002_timers.sql
    cat sql/003_notifications.sql
    cat sql/004_pulse.sql
    cat sql/005_pulse_subscriptions.sql
    cat sql/006_calendar.sql
    cat sql/007_daily_tasks.sql sql/008_calendar_recurrence.sql sql/009_files.sql sql/010_file_directories.sql
    printf "GRANT SELECT, INSERT, UPDATE, DELETE ON spark.devices TO '%s'@'localhost';\n" "$db_user"
    printf "GRANT SELECT, INSERT ON spark.device_events TO '%s'@'localhost';\n" "$db_user"
    printf "GRANT SELECT, INSERT, UPDATE ON spark.timers TO '%s'@'localhost';\n" "$db_user"
    printf "GRANT SELECT, INSERT ON spark.events TO '%s'@'localhost';\n" "$db_user"
    printf "GRANT SELECT, INSERT ON spark.notification_log TO '%s'@'localhost';\n" "$db_user"
    printf "GRANT SELECT, UPDATE ON spark.notification_dispatch_lock TO '%s'@'localhost';\n" "$db_user"
    printf "GRANT SELECT, UPDATE ON spark.pulse TO '%s'@'localhost';\n" "$db_user"
    printf "GRANT SELECT, INSERT, DELETE ON spark.pulse_subscriptions TO '%s'@'localhost';\n" "$db_user"
    printf "GRANT SELECT, INSERT, UPDATE, DELETE ON spark.calendar_events TO '%s'@'localhost';\n" "$db_user"
    printf "GRANT SELECT, INSERT, UPDATE, DELETE ON spark.daily_tasks TO '%s'@'localhost';\n" "$db_user"
    printf "GRANT SELECT, UPDATE ON spark.daily_task_day TO '%s'@'localhost';\n" "$db_user"
    printf "GRANT SELECT, INSERT, UPDATE, DELETE ON spark.files TO '%s'@'localhost';\n" "$db_user"
    printf "GRANT SELECT, INSERT ON spark.file_directories TO '%s'@'localhost';\n" "$db_user"
} | sudo mariadb
printf 'Provisioned spark database for local OS user %s.\n' "$db_user"
