#!/usr/bin/env bash
#
# Single podman entry-point: starts the OMOP DB (if not already running)
# and runs the ETL against it.
#
#   scripts/run/podman_run.sh --input REL_PATH [--trial NAME]
#
# REL_PATH is relative to DATA_ROOT e.g. if some eCRF data is
# in /data/durable/some_path, pass --input some_path.
#
# DATA_ROOT is bind-mounted once and everything under it (athena,
# mappings, output, input) are subpaths inside the container. 
# athena/, mappings/ and run/ below match dist/'s dirnames so
# the whole dist/ dir unzips straight into DATA_ROOT/omop/ with no renaming.

set -euo pipefail

DATA_ROOT="${DATA_ROOT:-/data/durable}"
OMOP_SUBDIR="omop"
ATHENA_SUBDIR="${OMOP_SUBDIR}/athena"
MAPPINGS_SUBDIR="${OMOP_SUBDIR}/mappings"
OUTPUT_SUBDIR="${OMOP_SUBDIR}/output"

NET="omop-net"
DB_NAME="omop-db"
DB_VOLUME="omop-pgdata"
DB_IMAGE="${DB_IMAGE:-omop-db:amd64-latest}"
ETL_IMAGE="${ETL_IMAGE:-omop-etl:amd64-latest}"
POSTGRES_DB="omop"
POSTGRES_USER="omop"
POSTGRES_PASSWORD="omop"

INPUT_SUBDIR="" TRIAL="IMPRESS" TARGET_BIOMARKER=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --input) INPUT_SUBDIR="$2"; shift 2 ;;
        --trial) TRIAL="$2"; shift 2 ;;
        --target-biomarker) TARGET_BIOMARKER="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done
: "${INPUT_SUBDIR:?--input required, a path relative to ${DATA_ROOT}}"

[[ -d "${DATA_ROOT}/${INPUT_SUBDIR}" ]] || { echo "error: missing ${INPUT_SUBDIR}/ under ${DATA_ROOT}" >&2; exit 1; }
[[ -d "${DATA_ROOT}/${ATHENA_SUBDIR}" ]] || { echo "error: missing ${ATHENA_SUBDIR}/ under ${DATA_ROOT}" >&2; exit 1; }
[[ -d "${DATA_ROOT}/${MAPPINGS_SUBDIR}" ]] || { echo "error: missing ${MAPPINGS_SUBDIR}/ under ${DATA_ROOT}" >&2; exit 1; }
mkdir -p "${DATA_ROOT}/${OUTPUT_SUBDIR}"

echo "==> ensuring network and volume"
podman network exists "${NET}" || podman network create "${NET}"
podman volume exists "${DB_VOLUME}" || podman volume create "${DB_VOLUME}"

if podman container exists "${DB_NAME}"; then
    echo "==> ${DB_NAME} already exists, starting if needed"
    podman start "${DB_NAME}" >/dev/null 2>&1 || true
else
    echo "==> starting ${DB_NAME}"
    podman run \
        -d --name "${DB_NAME}" \
        --network "${NET}" \
        --restart=always \
        -v "${DB_VOLUME}:/var/lib/postgresql/data" \
        -e POSTGRES_DB="${POSTGRES_DB}" \
        -e POSTGRES_USER="${POSTGRES_USER}" \
        -e POSTGRES_PASSWORD="${POSTGRES_PASSWORD}" \
        "${DB_IMAGE}"
fi

echo "==> waiting for postgres"
until podman exec "${DB_NAME}" pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; do
    sleep 0.5
done

DATABASE_URL="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${DB_NAME}:5432/${POSTGRES_DB}"

etl_args=(
    load
    --input "/data/${INPUT_SUBDIR}"
    --outdir "/data/${OUTPUT_SUBDIR}"
    --athena-dir "/data/${ATHENA_SUBDIR}"
    --mapping-dir "/data/${MAPPINGS_SUBDIR}"
    --trial "${TRIAL}"
    --log-level INFO
)
[[ -n "${TARGET_BIOMARKER}" ]] && etl_args+=(--target-biomarker "${TARGET_BIOMARKER}")

echo "==> running ETL"
podman run --rm \
    --network "${NET}" \
    -v "${DATA_ROOT}:/data:Z" \
    -e PYTHONUNBUFFERED=1 \
    -e DATABASE_URL="${DATABASE_URL}" \
    "${ETL_IMAGE}" \
    "${etl_args[@]}"