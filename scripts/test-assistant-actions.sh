#!/bin/sh
# A disposable MariaDB, never the configured application database.
set -eu
cd "$(dirname "$0")/.."
test_dir=$(mktemp -d /tmp/spark-actions.XXXXXXXX)
database_pid=
cleanup(){
 if [ -n "$database_pid" ]; then
  mariadb-admin --no-defaults --socket="$test_dir/mysql.sock" --user=root shutdown >/dev/null 2>&1 || kill "$database_pid" 2>/dev/null || true
  wait "$database_pid" || true
 fi
 rm -rf -- "$test_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mariadb-install-db --no-defaults --datadir="$test_dir/data" --auth-root-authentication-method=normal --skip-test-db > "$test_dir/install.log" 2>&1
/usr/sbin/mariadbd --no-defaults --datadir="$test_dir/data" --socket="$test_dir/mysql.sock" --pid-file="$test_dir/mysql.pid" --skip-networking --log-error="$test_dir/error.log" --innodb-buffer-pool-size=64M &
database_pid=$!
attempt=0
until mariadb-admin --no-defaults --socket="$test_dir/mysql.sock" --user=root ping >/dev/null 2>&1; do
 attempt=$((attempt+1)); if [ "$attempt" -ge 100 ]; then cat "$test_dir/error.log";exit 1;fi
 sleep .1
done
{ printf 'CREATE DATABASE spark_actions_test; USE spark_actions_test;\n'; cat sql/011_projects.sql sql/012_project_hierarchy.sql sql/013_project_boards.sql sql/024_board_task_severity.sql sql/002_timers.sql sql/006_calendar.sql sql/008_calendar_recurrence.sql sql/020_event_calendars.sql sql/021_calendar_sync.sql sql/025_action_history.sql sql/026_assistant_confirmations.sql; } | mariadb --no-defaults --socket="$test_dir/mysql.sock" --user=root
# Migration is safe to repeat.
mariadb --no-defaults --socket="$test_dir/mysql.sock" --user=root spark_actions_test < sql/026_assistant_confirmations.sql
export SPARK_DB_HOST=localhost SPARK_DB_USER=root SPARK_DB_PASSWORD= SPARK_DB_NAME=spark_actions_test SPARK_DB_SOCKET="$test_dir/mysql.sock"
./build/action_tests
{ printf 'CREATE DATABASE spark_confirmations_test; USE spark_confirmations_test;\n'; cat sql/011_projects.sql sql/012_project_hierarchy.sql sql/013_project_boards.sql sql/024_board_task_severity.sql sql/002_timers.sql sql/006_calendar.sql sql/008_calendar_recurrence.sql sql/020_event_calendars.sql sql/021_calendar_sync.sql sql/025_action_history.sql sql/026_assistant_confirmations.sql; } | mariadb --no-defaults --socket="$test_dir/mysql.sock" --user=root
export SPARK_DB_NAME=spark_confirmations_test
./build/confirmation_tests
