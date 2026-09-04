#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)
cd "$PROJECT_ROOT"

docker compose exec -T postgres pg_isready -U lakehouse -d lakehouse >/dev/null
database_count=$(docker compose exec -T postgres psql -U lakehouse -d lakehouse -Atc \
    "SELECT count(*) FROM pg_database WHERE datname IN ('airflow', 'polaris');")
[ "$database_count" = 2 ]
printf '%s\n' 'PostgreSQL: sẵn sàng (airflow, polaris).'

docker compose run --no-deps --rm minio-bootstrap >/dev/null
docker compose run --no-deps --rm --entrypoint /bin/sh minio-bootstrap -c '
    mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
    mc stat local/raw >/dev/null
    mc stat local/warehouse >/dev/null
' >/dev/null
printf '%s\n' 'MinIO: sẵn sàng (raw, warehouse).'

docker compose run --no-deps --rm polaris-bootstrap >/dev/null
printf '%s\n' 'Polaris: sẵn sàng (flood_lakehouse).'

docker compose exec -T airflow-api-server \
    curl --fail --silent http://localhost:8080/api/v2/monitor/health >/dev/null
printf '%s\n' 'Airflow API: sẵn sàng.'

docker compose exec -T airflow-scheduler \
    airflow jobs check --job-type SchedulerJob >/dev/null
printf '%s\n' 'Airflow SchedulerJob: sẵn sàng.'

docker compose exec -T airflow-dag-processor \
    airflow jobs check --job-type DagProcessorJob >/dev/null
printf '%s\n' 'Airflow DagProcessorJob: sẵn sàng.'

dag_list=$(docker compose exec -T airflow-api-server airflow dags list --output json)
if printf '%s' "$dag_list" | grep -q '"dag_id"[[:space:]]*:[[:space:]]*"example_'; then
    printf '%s\n' 'Airflow đang nạp example DAG ngoài dự kiến.' >&2
    exit 1
fi
printf '%s\n' 'Airflow DAG: không có example DAG.'
