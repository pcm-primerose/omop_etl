#!/usr/bin/env bash
set -euo pipefail

# todo: mount inputs and pass paths

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "${SCRIPT_DIR}/../../.." && pwd)"

PLATFORM="${PLATFORM:-}" # linux/arm64 or linux/amd64
BUILD_IMAGES="${BUILD_IMAGES:-1}" # 1=yes
KEEP_DB="${KEEP_DB:-1}" # 1=yes

NET="${NET:-omop-net}"
DB_NAME="${DB_NAME:-omop-db}"
DB_IMAGE="${DB_IMAGE:-omop-postgres:local}"
ETL_IMAGE="${ETL_IMAGE:-omop-etl:local}"

POSTGRES_DB="${POSTGRES_DB:-omop}"
POSTGRES_USER="${POSTGRES_USER:-omop}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-omop}"
HOST_PORT="${HOST_PORT:-5433}"


DATA_IN="/Users/gabriebs/projects/omop_etl/.data/synthetic/nonv600_cohorts"
DATA_OUT="/Users/gabriebs/projects/omop_etl/.data/full_run_test"

mkdir -p "$DATA_OUT"

platform_args=()
if [[ -n "${PLATFORM}" ]]; then
  platform_args=(--platform "${PLATFORM}")
fi

cleanup() {
  if [[ "${KEEP_DB}" != "1" ]]; then
    podman rm -f "$DB_NAME" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ "${BUILD_IMAGES}" == "1" ]]; then
  podman build "${platform_args[@]}" -t "$DB_IMAGE" -f "${SCRIPT_DIR}/postgres.Dockerfile" "${SCRIPT_DIR}"
  podman build "${platform_args[@]}" -t "$ETL_IMAGE" -f "${SCRIPT_DIR}/etl.Dockerfile" "${ROOT_DIR}"
fi

podman network exists "$NET" || podman network create "$NET" >/dev/null

podman rm -f "$DB_NAME" >/dev/null 2>&1 || true
podman run -d --rm \
  --name "$DB_NAME" \
  --network "$NET" \
  -e POSTGRES_DB="$POSTGRES_DB" \
  -e POSTGRES_USER="$POSTGRES_USER" \
  -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  -p "${HOST_PORT}:5432" \
  "$DB_IMAGE" >/dev/null

echo "Waiting for Postgres..."
until podman exec "$DB_NAME" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
  sleep 0.2
done

DATABASE_URL="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${DB_NAME}:5432/${POSTGRES_DB}"

ETL_NAME="${ETL_NAME:-omop-etl-run}"

podman rm -f "$ETL_NAME" >/dev/null 2>&1 || true

podman run --rm \
  --network "$NET" \
  -v "${DATA_IN}:/input:Z" \
  -v "${DATA_OUT}:/output:Z" \
  -e PYTHONUNBUFFERED=1 \
  -e DATABASE_URL="$DATABASE_URL" \
  "$ETL_IMAGE" \
  etl load \
    --input /input \
    --outdir /output \
    --trial IMPRESS \
    --static-mapping /app/src/omop_etl/resources/static_mapped/static_mapping.csv \
    --structural-mapping /app/src/omop_etl/resources/static_mapped/structural_mapping.csv \
    --truncate \
    --log-level DEBUG

echo "Postgres exposed on localhost:${HOST_PORT}"
echo "psql 'postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${HOST_PORT}/${POSTGRES_DB}'"
