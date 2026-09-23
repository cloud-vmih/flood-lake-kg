#!/bin/sh
set -eu

: "${POLARIS_URI:?POLARIS_URI is required}"
: "${POLARIS_CATALOG:?POLARIS_CATALOG is required}"
: "${POLARIS_CLIENT_ID:?POLARIS_CLIENT_ID is required}"
: "${POLARIS_CLIENT_SECRET:?POLARIS_CLIENT_SECRET is required}"
: "${MINIO_ENDPOINT:?MINIO_ENDPOINT is required}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"

catalog_dir=${TRINO_CATALOG_DIR:-/etc/trino/catalog}
umask 077
mkdir -p "$catalog_dir"
catalog_file=$catalog_dir/lakehouse.properties

printf '%s\n' \
    'connector.name=iceberg' \
    'iceberg.catalog.type=rest' \
    'iceberg.security=READ_ONLY' \
    "iceberg.rest-catalog.uri=${POLARIS_URI%/}" \
    "iceberg.rest-catalog.warehouse=$POLARIS_CATALOG" \
    'iceberg.rest-catalog.security=OAUTH2' \
    "iceberg.rest-catalog.oauth2.server-uri=${POLARIS_URI%/}/v1/oauth/tokens" \
    "iceberg.rest-catalog.oauth2.credential=$POLARIS_CLIENT_ID:$POLARIS_CLIENT_SECRET" \
    'iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL' \
    'iceberg.rest-catalog.http-headers=Polaris-Realm: POLARIS' \
    'fs.s3.enabled=true' \
    "s3.endpoint=$MINIO_ENDPOINT" \
    's3.region=us-east-1' \
    's3.path-style-access=true' \
    "s3.aws-access-key=$MINIO_ROOT_USER" \
    "s3.aws-secret-key=$MINIO_ROOT_PASSWORD" \
    > "$catalog_file"

chmod 600 "$catalog_file"
