#!/bin/sh
set -eu

POLARIS_URL=${POLARIS_URL:-http://polaris:8181}
CATALOG_NAME=flood_lakehouse

token_response=$(curl --fail-with-body --silent --show-error \
    --request POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
    --user "$POLARIS_CLIENT_ID:$POLARIS_CLIENT_SECRET" \
    --data-urlencode grant_type=client_credentials \
    --data-urlencode scope=PRINCIPAL_ROLE:ALL)
access_token=$(printf '%s' "$token_response" | jq -er '.access_token')

catalogs=$(curl --fail-with-body --silent --show-error \
    --request GET "$POLARIS_URL/api/management/v1/catalogs" \
    --header "Authorization: Bearer $access_token")

if ! printf '%s' "$catalogs" | jq -e --arg name "$CATALOG_NAME" \
    '.catalogs[]? | select(.name == $name)' >/dev/null; then
    response_file=$(mktemp)
    trap 'rm -f "$response_file"' EXIT HUP INT TERM

    if status_code=$(curl --fail-with-body --silent --show-error \
        --output "$response_file" \
        --write-out '%{http_code}' \
        --request POST "$POLARIS_URL/api/management/v1/catalogs" \
        --header "Authorization: Bearer $access_token" \
        --header 'Content-Type: application/json' \
        --data-binary @- <<'JSON'
{
  "catalog": {
    "name": "flood_lakehouse",
    "type": "INTERNAL",
    "readOnly": false,
    "properties": {"default-base-location": "s3://warehouse/"},
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://warehouse/"],
      "endpoint": "http://minio:9000",
      "endpointInternal": "http://minio:9000",
      "pathStyleAccess": true,
      "region": "us-east-1"
    }
  }
}
JSON
    ); then
        jq -e '.catalog.name == "flood_lakehouse" or .name == "flood_lakehouse"' \
            "$response_file" >/dev/null
    elif [ "$status_code" != 409 ]; then
        printf '%s\n' "Unable to create Polaris catalog (HTTP $status_code)." >&2
        exit 1
    fi
fi

grants_url="$POLARIS_URL/api/management/v1/catalogs/$CATALOG_NAME/catalog-roles/catalog_admin/grants"
grants=$(curl --fail-with-body --silent --show-error \
    --request GET "$grants_url" \
    --header "Authorization: Bearer $access_token")

if ! printf '%s' "$grants" | jq -e \
    '.grants[]? | select(.type == "catalog" and .privilege == "CATALOG_MANAGE_CONTENT")' \
    >/dev/null; then
    curl --fail-with-body --silent --show-error \
        --request PUT "$grants_url" \
        --header "Authorization: Bearer $access_token" \
        --header 'Content-Type: application/json' \
        --data-binary '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}' \
        >/dev/null
fi

printf '%s\n' "Polaris catalog $CATALOG_NAME is ready."
