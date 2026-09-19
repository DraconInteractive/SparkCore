#!/bin/sh
# Private MariaDB instance: no sudo, no TCP listener, no changes to the live database.
set -eu
cd "$(dirname "$0")/.."
test_dir=$(mktemp -d /tmp/spark-db-test.XXXXXXXX)
database_pid=
cleanup() {
    if [ -n "$database_pid" ]; then
        mariadb-admin --no-defaults --socket="$test_dir/mysql.sock" --user=root shutdown >/dev/null 2>&1 || kill "$database_pid" 2>/dev/null || true
        wait "$database_pid" 2>/dev/null || true
    fi
    rm -rf -- "$test_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mariadb-install-db --no-defaults --datadir="$test_dir/data" --auth-root-authentication-method=normal --skip-test-db >"$test_dir/install.log" 2>&1 || { cat "$test_dir/install.log"; exit 1; }
start_database() {
    /usr/sbin/mariadbd --no-defaults --datadir="$test_dir/data" --socket="$test_dir/mysql.sock" --pid-file="$test_dir/mysql.pid" --skip-networking --log-error="$test_dir/error.log" --innodb-buffer-pool-size=64M &
    database_pid=$!
    attempts=0
    until mariadb-admin --no-defaults --socket="$test_dir/mysql.sock" --user=root ping >/dev/null 2>&1; do
        attempts=$((attempts + 1))
        if [ "$attempts" -ge 50 ] || ! kill -0 "$database_pid" 2>/dev/null; then cat "$test_dir/error.log"; exit 1; fi
        sleep 0.1
    done
}
start_database
export SPARK_DB_HOST=localhost SPARK_DB_USER=root SPARK_DB_PASSWORD= SPARK_DB_NAME=spark_test_local SPARK_DB_PORT=3306
export SPARK_DB_SOCKET="$test_dir/mysql.sock"
export SPARK_FILE_DIR="$test_dir/files"
unset SPARK_TEST_AUDIT_FAILURE
{
    printf 'CREATE DATABASE spark_test_local; USE spark_test_local;\n'
    cat sql/001_devices.sql sql/002_timers.sql sql/003_notifications.sql sql/004_pulse.sql sql/005_pulse_subscriptions.sql sql/006_calendar.sql sql/007_daily_tasks.sql sql/008_calendar_recurrence.sql sql/009_files.sql sql/010_file_directories.sql
} | mariadb --no-defaults --socket="$test_dir/mysql.sock" --user=root
ctest --test-dir build --output-on-failure --verbose || {
    mariadb --no-defaults --socket="$test_dir/mysql.sock" --user=root spark_test_local -e "SET time_zone='+00:00'; SELECT CURRENT_TIMESTAMP(6),pulse.* FROM pulse; SELECT event_id,type,resource FROM events ORDER BY event_id DESC LIMIT 10;"
    exit 1
}
# Restart MariaDB itself, then verify durable timer records and committed events.
mariadb-admin --no-defaults --socket="$test_dir/mysql.sock" --user=root shutdown
wait "$database_pid"
database_pid=
start_database
count=$(mariadb --no-defaults --socket="$test_dir/mysql.sock" --user=root --batch --skip-column-names spark_test_local -e "SELECT COUNT(*) FROM timers WHERE status='confirmed';")
[ "$count" -ge 1 ] || { echo 'Timer did not survive database restart' >&2; exit 1; }
count=$(mariadb --no-defaults --socket="$test_dir/mysql.sock" --user=root --batch --skip-column-names spark_test_local -e "SELECT COUNT(*) FROM events WHERE type='timer.confirmed';")
[ "$count" -ge 1 ] || { echo 'Event did not survive database restart' >&2; exit 1; }
echo 'Timer and event persistence verified across MariaDB restart.'
count=$(mariadb --no-defaults --socket="$test_dir/mysql.sock" --user=root --batch --skip-column-names spark_test_local -e "SELECT pulse_count FROM pulse WHERE id=1;")
[ "$count" -ge 1 ] || { echo 'Pulse state did not survive database restart' >&2; exit 1; }
echo 'Pulse persistence verified across MariaDB restart.'
count=$(mariadb --no-defaults --socket="$test_dir/mysql.sock" --user=root --batch --skip-column-names spark_test_local -e "SELECT COUNT(*) FROM pulse_subscriptions WHERE device_id='watch-1';")
[ "$count" -eq 1 ] || { echo 'Pulse subscription did not survive database restart' >&2; exit 1; }
echo 'Pulse subscription persistence verified across MariaDB restart.'
count=$(mariadb --no-defaults --socket="$test_dir/mysql.sock" --user=root --batch --skip-column-names spark_test_local -e "SELECT COUNT(*) FROM calendar_events WHERE name='Lunch';")
[ "$count" -eq 1 ] || { echo 'Calendar event did not survive database restart' >&2; exit 1; }
echo 'Calendar persistence verified across MariaDB restart.'
count=$(mariadb --no-defaults --socket="$test_dir/mysql.sock" --user=root --batch --skip-column-names spark_test_local -e "SELECT COUNT(*) FROM daily_tasks WHERE name='Persistent daily task';")
[ "$count" -eq 1 ] || { echo 'Daily task did not survive database restart' >&2; exit 1; }
echo 'Daily task persistence verified across MariaDB restart.'

count=$(mariadb --no-defaults --socket="$test_dir/mysql.sock" --user=root --batch --skip-column-names spark_test_local -e "SELECT COUNT(*) FROM calendar_events WHERE name='Reminder' AND last_reminder_occurs_at IS NOT NULL;")
[ "$count" -eq 1 ] || { echo 'Reminder marker did not survive database restart' >&2; exit 1; }
echo 'Calendar reminder state verified across MariaDB restart.'

./build/file_tests --verify-restart
echo "File metadata and downloads verified across MariaDB restart."
