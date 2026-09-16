#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
ENV_FILE=${ENV_FILE:-$PROJECT_ROOT/.env}
LAKEHOUSE_DATA_ROOT=${LAKEHOUSE_DATA_ROOT:-$PROJECT_ROOT/dataset/lakehouse}

umask 077
mkdir -p "$(dirname -- "$ENV_FILE")"

if [ ! -f "$ENV_FILE" ]; then
    cp "$PROJECT_ROOT/.env.example" "$ENV_FILE"
fi

random_urlsafe() {
    openssl rand -base64 "$1" | tr '+/' '-_' | tr -d '=\n'
}

fernet_key() {
    openssl rand -base64 32 | tr '+/' '-_' | tr -d '\n'
}

last_value() {
    awk -v target="$1" '
        index($0, target "=") == 1 { value = substr($0, length(target) + 2) }
        END { print value }
    ' "$ENV_FILE"
}

set_if_blank() {
    key=$1
    value=$2
    current=$(last_value "$key")
    if [ -n "$current" ]; then
        return
    fi

    temporary=$(mktemp "${ENV_FILE}.tmp.XXXXXX")
    awk -v target="$key" -v replacement="$value" '
        BEGIN { written = 0 }
        index($0, target "=") == 1 {
            if (!written) {
                print target "=" replacement
                written = 1
            }
            next
        }
        { print }
        END {
            if (!written) print target "=" replacement
        }
    ' "$ENV_FILE" > "$temporary"
    chmod 600 "$temporary"
    mv "$temporary" "$ENV_FILE"
}

set_if_blank LAKEHOUSE_POSTGRES_PASSWORD "$(random_urlsafe 24)"
set_if_blank AIRFLOW_DB_PASSWORD "$(random_urlsafe 24)"
set_if_blank POLARIS_DB_PASSWORD "$(random_urlsafe 24)"
set_if_blank MINIO_ROOT_USER lakehouse_admin
set_if_blank MINIO_ROOT_PASSWORD "$(random_urlsafe 24)"
set_if_blank POLARIS_CLIENT_ID polaris_root
set_if_blank POLARIS_CLIENT_SECRET "$(random_urlsafe 24)"
set_if_blank AIRFLOW_FERNET_KEY "$(fernet_key)"
set_if_blank AIRFLOW_API_SECRET_KEY "$(random_urlsafe 32)"
set_if_blank AIRFLOW_ADMIN_USERNAME admin
set_if_blank AIRFLOW_ADMIN_PASSWORD "$(random_urlsafe 24)"
set_if_blank AIRFLOW_UID "$(id -u)"

chmod 600 "$ENV_FILE"
mkdir -p \
    "$LAKEHOUSE_DATA_ROOT/airflow/logs" \
    "$LAKEHOUSE_DATA_ROOT/minio" \
    "$LAKEHOUSE_DATA_ROOT/postgres"

printf '%s\n' 'Lakehouse local environment is ready.'
