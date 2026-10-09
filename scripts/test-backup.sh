#!/usr/bin/env bash
# Requires the isolated database and fixtures from test-local-db.sh.
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repo_dir"
[[ ${SPARK_DB_NAME:-} == spark_test_* && -n ${SPARK_FILE_DIR:-} && -n ${SPARK_DB_SOCKET:-} ]] || exit 77
mountpoint -q /dev/shm || { echo 'Backup tests need the /dev/shm tmpfs mount.' >&2; exit 1; }
backup_test_dir=$(mktemp -d /dev/shm/spark-backup-test.XXXXXXXX)
restore_test_dir=$(mktemp -d /tmp/spark-restore-test.XXXXXXXX)
restore_db="spark_test_restore_$$"
export SPARK_BACKUP_HISTORY="$restore_test_dir/locations.tsv"
mysql=(mariadb --no-defaults --socket="$SPARK_DB_SOCKET" --user=root)
cleanup() {
    "${mysql[@]}" -e "DROP DATABASE IF EXISTS $restore_db" >/dev/null
    rm -rf -- "$backup_test_dir" "$restore_test_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# An ordinary directory cannot stand in for an unmounted external drive.
if ./scripts/backup.sh --mount "$backup_test_dir" >/dev/null 2>&1; then echo 'Unmounted destination accepted' >&2; exit 1; fi
# The same flock held by the server must prevent a backup from starting.
(
    exec 9<>"$SPARK_FILE_DIR/.spark.lock"
    flock -x 9
    if ./scripts/backup.sh --mount /dev/shm --destination "$backup_test_dir/locked" >"$restore_test_dir/locked.log" 2>&1; then exit 1; fi
    grep -q 'Stop SPARK' "$restore_test_dir/locked.log"
)
# Enter a new destination through the same selector used interactively.
./scripts/backup.sh --choose <<ANSWERS
0
/dev/shm
$backup_test_dir/backups with spaces
ANSWERS
shopt -s nullglob
completed=("$backup_test_dir"/"backups with spaces"/spark-*)
[[ ${#completed[@]} == 1 ]]
backup_path=${completed[0]}
[[ -s "$SPARK_BACKUP_HISTORY" ]]
# Reuse a remembered destination; successes are deduplicated in history.
printf '1\n' | ./scripts/backup.sh --choose
[[ $(wc -l < "$SPARK_BACKUP_HISTORY") == 1 ]]
completed=("$backup_test_dir"/"backups with spaces"/spark-*)
[[ ${#completed[@]} == 2 ]]
./scripts/verify-backup.sh "$backup_path"
[[ ! -e "$backup_path/.spark.lock" ]]
# Exercise reassembly of many small chunks without allocating gigabytes in tests.
mkdir "$backup_test_dir/rechunked"
cp "$backup_path/manifest.txt" "$backup_test_dir/rechunked/manifest.txt"
cat "$backup_path"/database.sql.gz.part-* | split --bytes=4096 --numeric-suffixes=0 --suffix-length=6 - "$backup_test_dir/rechunked/database.sql.gz.part-"
cat "$backup_path"/files.tar.gz.part-* | split --bytes=4096 --numeric-suffixes=0 --suffix-length=6 - "$backup_test_dir/rechunked/files.tar.gz.part-"
backup_path="$backup_test_dir/rechunked"
(cd "$backup_path" && sha256sum manifest.txt database.sql.gz.part-* files.tar.gz.part-* > SHA256SUMS)
./scripts/verify-backup.sh "$backup_path"
"${mysql[@]}" -e "CREATE DATABASE $restore_db"
cat "$backup_path"/database.sql.gz.part-* | gzip -dc | "${mysql[@]}" "$restore_db"
mkdir "$restore_test_dir/files"
cat "$backup_path"/files.tar.gz.part-* | gzip -dc | tar --extract --sparse --file=- --directory="$restore_test_dir/files" --no-same-owner --no-same-permissions
[[ ! -e "$restore_test_dir/files/.spark.lock" ]]
# The actual server API verifies restored bytes, edited revisions and project hierarchy.
SPARK_DB_NAME="$restore_db" SPARK_FILE_DIR="$restore_test_dir/files" ./build/file_tests --verify-restart
# Corruption and missing chunks must be rejected.
printf 'damage' >> "$backup_path/files.tar.gz.part-000000"
if ./scripts/verify-backup.sh "$backup_path" >/dev/null 2>&1; then echo 'Corruption accepted' >&2; exit 1; fi
rm "$backup_path/database.sql.gz.part-000000"
if ./scripts/verify-backup.sh "$backup_path" >/dev/null 2>&1; then echo 'Missing chunk accepted' >&2; exit 1; fi
# A failed database dump must never publish a completed backup.
if SPARK_DB_NAME=spark_test_missing_backup_db ./scripts/backup.sh --mount /dev/shm --destination "$backup_test_dir/failure" >/dev/null 2>&1; then echo 'Failed database dump accepted' >&2; exit 1; fi
failed=("$backup_test_dir"/failure/spark-*)
[[ ${#failed[@]} == 0 ]]
[[ $(wc -l < "$SPARK_BACKUP_HISTORY") == 1 ]]
./scripts/verify-backup.sh "${completed[0]}"
echo 'Backup lock/mount guards, compression, chunk verification, failure handling and full database/file restore passed.'
