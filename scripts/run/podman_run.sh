#!/usr/bin/env bash
#
# Single podman entry-point: starts the OMOP DB (if not already running)
# and runs the ETL against it.
#
#   scripts/run/podman_run.sh [--trial NAME]
#
# DATA_ROOT is bind-mounted once and everything under it (athena,
# mappings, output, input) are subpaths inside the container. athena/,
# mappings/ and run/ below match dist/'s dirnames so the whole dist/ dir
# unzips straight into DATA_ROOT/omop/ with no renaming.

set -euo pipefail

DATA_ROOT="${DATA_ROOT:-/tsd/p2828/data/durable}"
CLUSTER_ROOT="${CLUSTER_ROOT:-/tsd/p2828/cluster}"

OMOP_SUBDIR="${OMOP_SUBDIR:-omop}"
IMAGES_DIR="${IMAGES_DIR:-${OMOP_SUBDIR}/dist/images}"
ATHENA_SUBDIR="${ATHENA_SUBDIR:-${OMOP_SUBDIR}/dist/athena}"
MAPPINGS_SUBDIR="${MAPPINGS_SUBDIR:-${OMOP_SUBDIR}/dist/mappings}"
OUTPUT_SUBDIR="${OUTPUT_SUBDIR:-${OMOP_SUBDIR}/output}"
INPUT_SUBDIR="${INPUT_SUBDIR:-impress_ecrf/OUS_20260619_091637.xlsx}"

NET="omop-net"
DB_NAME="omop-db"
DB_VOLUME="omop-pgdata"
DB_IMAGE="${DB_IMAGE:-omop-db:amd64-latest}"
ETL_IMAGE="${ETL_IMAGE:-omop-etl:amd64-latest}"
POSTGRES_DB="omop"
POSTGRES_USER="omop"
POSTGRES_PASSWORD="omop"

TRIAL="IMPRESS" TARGET_BIOMARKER=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --trial) TRIAL="$2"; shift 2 ;;
        --target-biomarker) TARGET_BIOMARKER="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

[[ -e "${DATA_ROOT}/${INPUT_SUBDIR}" ]] || { echo "error: missing ${INPUT_SUBDIR} under ${DATA_ROOT} (dir of CSVs or a single .xlsx)" >&2; exit 1; }
[[ -d "${DATA_ROOT}/${ATHENA_SUBDIR}" ]] || { echo "error: missing ${ATHENA_SUBDIR}/ under ${DATA_ROOT}" >&2; exit 1; }
[[ -d "${DATA_ROOT}/${MAPPINGS_SUBDIR}" ]] || { echo "error: missing ${MAPPINGS_SUBDIR}/ under ${DATA_ROOT}" >&2; exit 1; }
mkdir -p "${DATA_ROOT}/${OUTPUT_SUBDIR}"

echo "==> loading images"
podman load -i "${DATA_ROOT}/${IMAGES_DIR}/omop-db-amd64-latest.tar" >/dev/null
podman load -i "${DATA_ROOT}/${IMAGES_DIR}/omop-etl-amd64-latest.tar" >/dev/null

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
    etl
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
    --security-opt label=disable \
    -v "${DATA_ROOT}:/data" \
    -e PYTHONUNBUFFERED=1 \
    -e DATABASE_URL="${DATABASE_URL}" \
    "${ETL_IMAGE}" \
    "${etl_args[@]}"