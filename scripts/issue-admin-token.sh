#!/usr/bin/env bash
# Issue an additional administrator credential with every currently known grant.
set -euo pipefail

if [[ ${1:-} == --help || ${1:-} == -h ]]; then
    cat <<'HELP'
Usage: sudo ./scripts/issue-admin-token.sh [CREDENTIALS_FILE [SUBJECT]]
       ./scripts/issue-admin-token.sh [CREDENTIALS_FILE [SUBJECT]]

Defaults: invoking user's ~/.config/spark/credentials.json and username.
Uses build/spark_credentials from this checkout. The file must already exist.
Prints JSON containing the new credential ID and token. Existing tokens remain valid.
When invoked through sudo, issuance runs as the original user, not root.
HELP
    exit 0
fi
if (( $# > 2 )); then
    echo 'Too many arguments. Use --help for usage.' >&2
    exit 2
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
if (( EUID == 0 )); then
    if [[ -z ${SUDO_USER:-} || ${SUDO_USER} == root || -z ${SUDO_UID:-} ]]; then
        echo 'Run this helper as the SPARK user, or through sudo from that user; direct root invocation is not supported.' >&2
        exit 2
    fi
    if [[ $(id -u -- "$SUDO_USER") != "$SUDO_UID" ]]; then
        echo 'Cannot verify the invoking sudo user.' >&2
        exit 2
    fi
    # Drop privileges before reading credentials or running the project binary.
    exec runuser -u "$SUDO_USER" -- "$script_dir/issue-admin-token.sh" "$@"
fi

spark_user=$(id -un)
spark_account=$(getent passwd "$spark_user")
IFS=: read -r _ _ _ _ _ spark_user_home _ <<< "$spark_account"
credentials_file=${1:-"$spark_user_home/.config/spark/credentials.json"}
subject=${2:-"$spark_user"}
binary="$script_dir/../build/spark_credentials"
if [[ ! -x "$binary" ]]; then
    echo 'Build the credential tool first: cmake --build build --target spark_credentials -j2' >&2
    exit 1
fi
if [[ ! -f "$credentials_file" ]]; then
    echo 'Credentials file not found. Pass the credentials_file path used by your SPARK configuration.' >&2
    exit 1
fi

# pipefail and command substitution propagate enumeration/JSON errors. A failed
# permission query must never silently issue a token with an empty grant list.
permission_lines=$("$binary" permissions | python3 -c '
import json, re, sys
permissions = json.load(sys.stdin)
if (not isinstance(permissions, list) or not permissions
        or any(not isinstance(p, str) or not re.fullmatch(r"[a-z]+(?:\.[a-z]+)+", p) for p in permissions)):
    raise SystemExit("Invalid permission list from spark_credentials")
print("\n".join(permissions))
')
mapfile -t spark_permissions <<< "$permission_lines"
exec "$binary" issue "$credentials_file" administrator "$subject" "${spark_permissions[@]}"
