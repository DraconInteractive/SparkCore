#!/usr/bin/env bash
# Offline, streaming SPARK backup. Run with the same environment as the server.
set -euo pipefail
umask 077
export LC_ALL=C
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
backup_mount=${SPARK_BACKUP_MOUNT:-}
backup_dir=${SPARK_BACKUP_DIR:-}
file_dir=${SPARK_FILE_DIR:-$repo_dir/storage/files}
history_file=${SPARK_BACKUP_HISTORY:-$repo_dir/storage/backup-locations.tsv}
file_dirs=()
choose=0
usage() {
    cat <<'HELP'
Usage: scripts/backup.sh [--choose | --mount DRIVE_MOUNT] [--destination DIRECTORY] [--file-dir DIRECTORY ...]
Without a mount in an interactive terminal, choose a previous location or enter a new one.
--choose always opens the selector, overriding mount/destination environment defaults.
Stop SPARK first; leave MariaDB running. Uses SPARK_DB_* environment settings.
The drive must already be mounted. Destination defaults to DRIVE_MOUNT/spark-backups.
Repeat --file-dir for every configured storage drive. All registered drives are required.
Environment alternatives: SPARK_BACKUP_MOUNT, SPARK_BACKUP_DIR, SPARK_FILE_DIR.
Creates a timestamped directory of compressed, checksummed 1 GiB chunks.
Existing backups are never overwritten or automatically deleted.
HELP
}
fail() { printf 'Backup failed: %s\n' "$*" >&2; exit 1; }
while (($#)); do
    case "$1" in
        --mount|--destination|--file-dir)
            (($#>=2)) || fail "Missing value for $1"
            case "$1" in --mount) backup_mount=$2;; --destination) backup_dir=$2;; --file-dir) file_dirs+=("$2");; esac
            shift 2;;
        --choose) choose=1; shift;;
        --help|-h) usage; exit 0;;
        *) fail "Unknown option: $1";;
    esac
done
for program in mariadb mariadb-dump tar gzip split sha256sum flock mountpoint realpath sync mktemp stat; do
    command -v "$program" >/dev/null || fail "Missing command: $program"
done
history_file=$(realpath -m -- "$history_file")
choose_location() {
    local mount destination extra selection index=0 state
    local -a mounts=() destinations=()
    printf 'Backup locations (mount -> destination):\n' >&2
    if [[ -f "$history_file" ]]; then
        while IFS=$'\t' read -r mount destination extra; do
            [[ -n "$mount" && -n "$destination" && -z "$extra" ]] || continue
            mounts+=("$mount"); destinations+=("$destination"); index=$((index+1))
            state='not mounted'; if mountpoint -q -- "$mount"; then state='mounted'; fi
            printf '  %s) %s -> %s [%s]\n' "$index" "$mount" "$destination" "$state" >&2
            ((index<20)) || break
        done < "$history_file"
    fi
    printf '  0) Enter a new location\n' >&2
    while :; do
        printf 'Choose a number (or q to cancel): ' >&2
        IFS= read -r selection || fail 'Location selection cancelled.'
        [[ "$selection" != q ]] || fail 'Location selection cancelled.'
        if [[ "$selection" =~ ^(0|[1-9][0-9]?)$ ]] && ((selection<=index)); then break; fi
        printf 'Select one of the listed numbers.\n' >&2
    done
    if ((selection)); then
        backup_mount=${mounts[selection-1]}; backup_dir=${destinations[selection-1]}
    else
        printf 'External drive mount path: ' >&2
        IFS= read -r backup_mount || fail 'Location selection cancelled.'
        printf 'Backup directory (Enter for <mount>/spark-backups): ' >&2
        IFS= read -r backup_dir || fail 'Location selection cancelled.'
    fi
}
if ((choose)) || { [[ -z "$backup_mount" ]] && [[ -t 0 ]]; }; then choose_location; fi
[[ -n "$backup_mount" ]] || fail 'Specify --mount, SPARK_BACKUP_MOUNT, or run interactively to choose a location.'
if ((${#file_dirs[@]}==0)); then
    [[ -z "${SPARK_FILE_DRIVES:-}" ]] || fail 'Pass every runtime drive explicitly with repeated --file-dir options.'
    file_dirs=("$file_dir")
fi
for value in "$backup_mount" "$backup_dir" "${file_dirs[@]}"; do
    [[ "$value" != *$'\t'* && "$value" != *$'\n'* && "$value" != *$'\r'* ]] || fail 'Paths cannot contain tabs or newlines.'
done
backup_mount=$(realpath -e -- "$backup_mount") || fail 'Backup mount does not exist.'
[[ "$backup_mount" != / ]] || fail 'The root filesystem is not a backup drive.'
mountpoint -q -- "$backup_mount" || fail 'Backup drive is not mounted; refusing to write to the internal disk.'
backup_dir=$(realpath -m -- "${backup_dir:-$backup_mount/spark-backups}")
[[ "$backup_dir" == "$backup_mount/"* ]] || fail 'Destination must be inside the specified mounted drive.'
# Keep every source directory and lock open until the dump and all archives finish.
declare -A drive_paths=()
drive_ids=() source_fds=() source_locks=() normalized_dirs=()
for file_dir in "${file_dirs[@]}"; do
    [[ -d "$file_dir" ]] || fail 'File storage does not exist; check --file-dir.'
    file_dir=$(realpath -e -- "$file_dir")
    [[ "$(stat -c %d -- "$file_dir")" != "$(stat -c %d -- "$backup_mount")" ]] || fail 'Backup drive must be on a different filesystem from every storage drive.'
    [[ "$backup_dir" != "$file_dir" && "$backup_dir" != "$file_dir/"* && "$file_dir" != "$backup_dir/"* ]] || fail 'Backup destination and file storage must be separate.'
    for previous in "${normalized_dirs[@]}"; do
        [[ "$file_dir" != "$previous" && "$file_dir" != "$previous/"* && "$previous" != "$file_dir/"* ]] || fail 'Storage drives overlap.'
    done
    [[ -f "$file_dir/.spark.lock" && ! -L "$file_dir/.spark.lock" ]] || fail 'Storage lock missing or invalid; use directories initialized by SPARK.'
    exec {source_fd}<"$file_dir"
    pinned="/proc/$$/fd/$source_fd"
    exec {storage_lock}<>"$pinned/.spark.lock"
    flock -n -x "$storage_lock" || fail 'SPARK is using this storage. Stop SPARK before backing up.'
    [[ -f "$pinned/.spark-drive" && ! -L "$pinned/.spark-drive" ]] || fail 'Drive identity missing; initialize storage with SPARK first.'
    drive_id=$(cat -- "$pinned/.spark-drive")
    [[ "$drive_id" =~ ^[0-9a-f]{32}$ && "$(stat -c %s -- "$pinned/.spark-drive")" == 33 ]] || fail 'Invalid drive identity.'
    [[ -z "${drive_paths[$drive_id]+present}" ]] || fail 'Duplicate drive identity.'
    drive_paths[$drive_id]=$file_dir
    drive_ids+=("$drive_id"); source_fds+=("$source_fd"); source_locks+=("$storage_lock"); normalized_dirs+=("$file_dir")
done
file_dir=${normalized_dirs[0]}
# Hold a directory reference on the mounted drive during all writes.
mkdir -p -- "$backup_dir"
cd -- "$backup_dir"
[[ "$(pwd -P)" == "$backup_dir" ]] || fail 'Destination changed during setup.'
mountpoint -q -- "$backup_mount" || fail 'Backup drive was disconnected.'
exec {backup_lock}>.backup.lock
flock -n -x "$backup_lock" || fail 'Another backup is writing to this destination.'
credentials=
incomplete=
cleanup() {
    [[ -z "$credentials" ]] || rm -f -- "$credentials"
    if [[ -n "$incomplete" ]]; then printf 'Incomplete backup retained at %s/%s; it is not a recovery point.\n' "$backup_dir" "$incomplete" >&2; fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
credentials=$(mktemp /tmp/spark-backup-client.XXXXXXXX.cnf)
# Escape option-file values; never put credentials in argv or the archive.
option() {
    local value=$2
    value=${value//\\/\\\\}; value=${value//\"/\\\"}
    value=${value//$'\n'/\\n}; value=${value//$'\r'/\\r}; value=${value//$'\t'/\\t}
    printf '%s="%s"\n' "$1" "$value"
}
{
    printf '[client]\n'
    option host "${SPARK_DB_HOST:-localhost}"
    option port "${SPARK_DB_PORT:-3306}"
    option user "${SPARK_DB_USER:-${USER:-spark}}"
    option password "${SPARK_DB_PASSWORD:-}"
    [[ -z "${SPARK_DB_SOCKET:-}" ]] || option socket "$SPARK_DB_SOCKET"
} > "$credentials"
db_name=${SPARK_DB_NAME:-spark}
[[ "$db_name" =~ ^[A-Za-z0-9_]+$ ]] || fail 'Unsupported database name; use letters, digits or underscores.'
# A successful backup must contain the whole registered storage set, even drives
# with no currently ready files. Refuse a partial source list before dumping.
registered=$(mariadb --defaults-file="$credentials" --batch --skip-column-names "$db_name" -e 'SELECT id FROM storage_drives ORDER BY id')
[[ -n "$registered" ]] || fail 'Storage registry is empty; start SPARK once after migration.'
registered_count=0
while IFS= read -r drive_id; do
    [[ "$drive_id" =~ ^[0-9a-f]{32}$ && -n "${drive_paths[$drive_id]+present}" ]] || fail 'A registered storage drive is missing from this backup.'
    registered_count=$((registered_count+1))
done <<< "$registered"
[[ "$registered_count" == "${#drive_ids[@]}" ]] || fail 'A supplied drive is not registered in this database.'
unmapped=$(mariadb --defaults-file="$credentials" --batch --skip-column-names "$db_name" -e 'SELECT COUNT(*) FROM files f LEFT JOIN storage_drives d ON d.id=f.drive_id WHERE d.id IS NULL')
[[ "$unmapped" == 0 ]] || fail 'File drive mappings are incomplete; start SPARK to migrate them.'
backup_format=spark-backup-v1
if ((${#drive_ids[@]}>1)); then backup_format=spark-backup-v2; fi
incomplete=$(mktemp -d ".incomplete-spark-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXXXX")
{
    printf 'format=%s\ncreated_at_utc=%s\n' "$backup_format" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'database=%s\nfile_directory=%s\n' "$db_name" "$file_dir"
    printf 'chunk_bytes=1073741824\ncompression=gzip\nconsistency=offline-storage-lock\n'
    printf 'scope=SPARK database tables and file storage; excludes DB accounts, custom triggers/routines/events, server configuration and binaries\n'
} > "$incomplete/manifest.txt"
printf 'Backing up MariaDB tables…\n'
mariadb-dump --defaults-file="$credentials" --single-transaction --quick --hex-blob \
    --no-tablespaces --skip-lock-tables --skip-triggers --default-character-set=utf8mb4 "$db_name" \
    | gzip -c | split --bytes=1073741824 --numeric-suffixes=0 --suffix-length=6 - "$incomplete/database.sql.gz.part-"
printf 'Backing up file contents…\n'
archive_parts=()
for index in "${!drive_ids[@]}"; do
    pinned="/proc/$$/fd/${source_fds[index]}"
    [[ "$(stat -Lc '%d:%i' -- "${normalized_dirs[index]}")" == "$(stat -Lc '%d:%i' -- "$pinned")" ]] || fail 'Source drive changed or disconnected.'
    prefix=files
    if [[ "$backup_format" == spark-backup-v2 ]]; then
        prefix="drive-${drive_ids[index]}"
        printf '%s\t%s\n' "${drive_ids[index]}" "${normalized_dirs[index]}" >> "$incomplete/drives.tsv"
    fi
    tar --create --sparse --file=- --directory="$pinned" --exclude='./.spark.lock' . \
        | gzip -c | split --bytes=1073741824 --numeric-suffixes=0 --suffix-length=6 - "$incomplete/$prefix.tar.gz.part-"
done
(
    cd -- "$incomplete"
    if [[ "$backup_format" == spark-backup-v2 ]]; then
        LC_ALL=C sort -o drives.tsv drives.tsv
        sha256sum -- manifest.txt drives.tsv database.sql.gz.part-* drive-*.tar.gz.part-* > SHA256SUMS
    else
        sha256sum -- manifest.txt database.sql.gz.part-* files.tar.gz.part-* > SHA256SUMS
    fi
)
"$repo_dir/scripts/verify-backup.sh" "$backup_dir/$incomplete"
# Flush the mounted filesystem before publishing the completed directory name.
sync -f .
complete=${incomplete#.incomplete-}
[[ ! -e "$complete" ]] || fail 'Backup name collision.'
mv -T -- "$incomplete" "$complete"
incomplete=
sync -f .
# Remember only verified, completed destinations. Failure here must not turn a
# successful backup into a failure or remove the existing location history.
remember_location() (
    local history_parent temporary mount destination extra count=1
    history_parent=$(dirname -- "$history_file")
    mkdir -p -- "$history_parent" || return 1
    exec {history_lock}>"$history_file.lock" || return 1
    flock -x "$history_lock" || return 1
    temporary=$(mktemp "$history_parent/.backup-locations.XXXXXXXX") || return 1
    trap 'rm -f -- "$temporary"' EXIT
    printf '%s\t%s\n' "$backup_mount" "$backup_dir" > "$temporary" || return 1
    if [[ -f "$history_file" ]]; then
        while IFS=$'\t' read -r mount destination extra; do
            [[ -n "$mount" && -n "$destination" && -z "$extra" ]] || continue
            [[ "$mount" != "$backup_mount" || "$destination" != "$backup_dir" ]] || continue
            printf '%s\t%s\n' "$mount" "$destination" >> "$temporary" || return 1
            count=$((count+1)); ((count<20)) || break
        done < "$history_file"
    fi
    mv -T -- "$temporary" "$history_file" || return 1
)
if ! remember_location; then printf 'Backup succeeded, but the location history could not be saved.\n' >&2; fi
printf 'Backup complete: %s/%s\n'  "$backup_dir" "$complete"
