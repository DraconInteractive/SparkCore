#!/bin/sh
# A disposable MariaDB, never the configured application database.
set -eu
cd "$(dirname "$0")/.."
test_dir=$(mktemp -d /tmp/spark-db-test.XXXXXXXX)
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
{ printf 'CREATE DATABASE spark_test_budget; USE spark_test_budget;\n'; cat sql/*.sql; } | mariadb --no-defaults --socket="$test_dir/mysql.sock" --user=root
export SPARK_DB_HOST=localhost SPARK_DB_USER=root SPARK_DB_PASSWORD= SPARK_DB_NAME=spark_test_budget SPARK_DB_SOCKET="$test_dir/mysql.sock" SPARK_FILE_DIR="$test_dir/files"
./build/budget_tests
./build/authorization_tests
./build/spark_tests

python3 scripts/test-db-provisioning.py
