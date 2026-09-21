#!/usr/bin/env bash
#
# Single apptainer entry-point: starts the OMOP DB instance (if not
# already running) and runs the ETL against it. Mirrors podman_run.sh.
#
#   scripts/run/apptainer_run.sh [--trial NAME]
#
# apptainer instances share the host network by default, so the ETL just
# connects to localhost.
#
# DATA_ROOT is the only bind-mount, reused for the DB and ETL - TSD's
# network shares can only be bind mounted at their top level so a subdir
# can't be mounted directly. Postgres's data dir is relocated to a
# subpath of that same mount via PGDATA instead of a second bind.
#
# athena/, mappings/ and run/ below match dist/'s dirnames so the whole
# dist/ dir unzips straight into DATA_ROOT/omop/ with no renaming.

set -euo pipefail

DATA_ROOT="${DATA_ROOT:-/tsd/p2828/data/durable}"
OMOP_SUBDIR="omop"
IMAGES_DIR="${OMOP_SUBDIR}/dist/images"
ATHENA_SUBDIR="${OMOP_SUBDIR}/dist/athena"
MAPPINGS_SUBDIR="${OMOP_SUBDIR}/dist/mappings"
OUTPUT_SUBDIR="${OMOP_SUBDIR}/output"
PGDATA_SUBDIR="${OMOP_SUBDIR}/pgdata"
INPUT_SUBDIR="impress_ecrf/OUS_20260619_091637.xlsx"

DB_SIF="${DB_SIF:-${DATA_ROOT}/${IMAGES_DIR}/omop-db-amd64-latest.sif}"
ETL_SIF="${ETL_SIF:-${DATA_ROOT}/${IMAGES_DIR}/omop-etl-amd64-latest.sif}"
INSTANCE="omop-db"
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
mkdir -p "${DATA_ROOT}/${OUTPUT_SUBDIR}" "${DATA_ROOT}/${PGDATA_SUBDIR}"

if apptainer instance list | grep -q "^${INSTANCE} "; then
    echo "==> ${INSTANCE} already running"
else
    echo "==> starting ${INSTANCE}"
    apptainer instance start \
        --bind "${DATA_ROOT}:/data" \
        --env "PGDATA=/data/${PGDATA_SUBDIR}" \
        --env "POSTGRES_DB=${POSTGRES_DB}" \
        --env "POSTGRES_USER=${POSTGRES_USER}" \
        --env "POSTGRES_PASSWORD=${POSTGRES_PASSWORD}" \
        "${DB_SIF}" "${INSTANCE}"
fi

echo "==> waiting for postgres"
until apptainer exec "instance://${INSTANCE}" pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" >/dev/null 2>&1; do
    sleep 0.5
done

DATABASE_URL="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:5432/${POSTGRES_DB}"

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
apptainer exec \
    --bind "${DATA_ROOT}:/data" \
    --env "PYTHONUNBUFFERED=1" \
    --env "DATABASE_URL=${DATABASE_URL}" \
    "${ETL_SIF}" \
    "${etl_args[@]}"