#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C
fail() { printf 'Backup verification failed: %s\n' "$*" >&2; exit 1; }
[[ $# == 1 ]] || fail 'Usage: scripts/verify-backup.sh BACKUP_DIRECTORY'
cd -- "$1"
[[ -f manifest.txt && ! -L manifest.txt && -f SHA256SUMS && ! -L SHA256SUMS ]] || fail 'Missing manifest or checksum file.'
shopt -s nullglob
format=$(head -n 1 manifest.txt)
database_parts=(database.sql.gz.part-*)
((${#database_parts[@]})) || fail 'Missing database chunks.'
if [[ "$format" == format=spark-backup-v1 ]]; then
    file_parts=(files.tar.gz.part-*)
    ((${#file_parts[@]})) || fail 'Missing file chunks.'
    expected=(manifest.txt "${database_parts[@]}" "${file_parts[@]}")
    prefixes=(files)
elif [[ "$format" == format=spark-backup-v2 ]]; then
    [[ -f drives.tsv && ! -L drives.tsv ]] || fail 'Missing drive manifest.'
    file_parts=(drive-*.tar.gz.part-*)
    expected=(manifest.txt drives.tsv "${database_parts[@]}" "${file_parts[@]}")
    prefixes=(); previous=
    while IFS=$'\t' read -r id directory extra; do
        [[ "$id" =~ ^[0-9a-f]{32}$ && "$directory" == /* && -z "$extra" ]] || fail 'Invalid drive manifest.'
        [[ -z "$previous" || "$id" > "$previous" ]] || fail 'Duplicate or unsorted drives.'
        previous=$id;prefixes+=("drive-$id")
    done < drives.tsv
    ((${#prefixes[@]}>=2 && ${#prefixes[@]}<=64)) || fail 'Invalid drive count.'
    enumerated=()
    for prefix in "${prefixes[@]}"; do
        parts=("$prefix".tar.gz.part-*)
        ((${#parts[@]})) || fail 'Missing drive archive.'
        enumerated+=("${parts[@]}")
    done
    [[ "${enumerated[*]}" == "${file_parts[*]}" ]] || fail 'Unlisted drive archive.'
else fail 'Unsupported backup format.'; fi
index=0
while IFS= read -r line; do
    [[ "$line" =~ ^[0-9a-f]{64}\ \ (manifest\.txt|drives\.tsv|database\.sql\.gz\.part-[0-9]{6}|files\.tar\.gz\.part-[0-9]{6}|drive-[0-9a-f]{32}\.tar\.gz\.part-[0-9]{6})$ ]] || fail 'Invalid checksum entry.'
    name=${line:66}
    [[ $index -lt ${#expected[@]} && "$name" == "${expected[$index]}" && -f "$name" && ! -L "$name" ]] || fail 'Missing, extra, reordered or unsafe member.'
    index=$((index+1))
done < SHA256SUMS
[[ $index == ${#expected[@]} ]] || fail 'Not all archive members are checksummed.'
[[ ${SPARK_BACKUP_PROGRESS:-0} != 1 ]] || printf 'SPARK_STAGE checksums_verify\n'
sha256sum --check --strict SHA256SUMS
[[ ${SPARK_BACKUP_PROGRESS:-0} != 1 ]] || printf 'SPARK_STAGE database_verify\n'
cat -- "${database_parts[@]}" | gzip -t
for prefix in "${prefixes[@]}"; do
    [[ ${SPARK_BACKUP_PROGRESS:-0} != 1 ]] || printf 'SPARK_STAGE archive_verify %s\n' "$prefix"
    parts=("$prefix".tar.gz.part-*)
    cat -- "${parts[@]}" | gzip -dc | tar --list --file=- >/dev/null
    if [[ "$format" == format=spark-backup-v2 ]]; then
        marker=$(cat -- "${parts[@]}" | gzip -dc | tar --extract --to-stdout --file=- ./.spark-drive)
        [[ "$marker" == "${prefix#drive-}" ]] || fail 'Drive archive identity mismatch.'
    fi
done
printf 'Backup checksums and compressed archives verified.\n'
