#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PYTHON_BIN="/usr/bin/python3"
SCHEDULE="35 3 * * *"
DB_PATH="logs/quant_api.db"
AUDIT_TTL_DAYS="180"
RUNTIME_LOG_TTL_DAYS="30"
BACKTEST_TTL_DAYS="90"
BACKTEST_FINAL_STATUSES="finished,failed,stopped,cancelled"
LOG_FILE="logs/sqlite_retention_cleanup_cron.log"
MARKER="quant_sqlite_retention_cleanup_job"
DRY_RUN="false"

usage() {
  cat <<'EOF'
Install or update cron entry for sqlite_retention_cleanup.py.

Options:
  --python <path>                     Python executable (default: /usr/bin/python3)
  --project-dir <path>                Project root (default: parent of this script)
  --schedule "<cron expr>"            Cron schedule (default: 35 3 * * *)
  --db-path <path>                    Source DB path
  --audit-ttl-days <days>             Audit log TTL in days
  --runtime-log-ttl-days <days>       Runtime log TTL in days
  --backtest-ttl-days <days>          Backtest metadata TTL in days
  --backtest-final-statuses <csv>     Statuses eligible for cleanup
  --log-file <path>                   Cron log output path
  --dry-run                           Print final crontab without installing
  -h, --help                          Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python) PYTHON_BIN="$2"; shift 2 ;;
    --project-dir) PROJECT_DIR="$2"; shift 2 ;;
    --schedule) SCHEDULE="$2"; shift 2 ;;
    --db-path) DB_PATH="$2"; shift 2 ;;
    --audit-ttl-days) AUDIT_TTL_DAYS="$2"; shift 2 ;;
    --runtime-log-ttl-days) RUNTIME_LOG_TTL_DAYS="$2"; shift 2 ;;
    --backtest-ttl-days) BACKTEST_TTL_DAYS="$2"; shift 2 ;;
    --backtest-final-statuses) BACKTEST_FINAL_STATUSES="$2"; shift 2 ;;
    --log-file) LOG_FILE="$2"; shift 2 ;;
    --dry-run) DRY_RUN="true"; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

PROJECT_DIR="$(cd "${PROJECT_DIR}" && pwd)"
if [[ ! -d "${PROJECT_DIR}" ]]; then
  echo "project dir not found: ${PROJECT_DIR}" >&2
  exit 1
fi

ENTRY="${SCHEDULE} cd ${PROJECT_DIR} && ${PYTHON_BIN} tools/sqlite_retention_cleanup.py --db-path ${DB_PATH} --audit-ttl-days ${AUDIT_TTL_DAYS} --runtime-log-ttl-days ${RUNTIME_LOG_TTL_DAYS} --backtest-ttl-days ${BACKTEST_TTL_DAYS} --backtest-final-statuses ${BACKTEST_FINAL_STATUSES} >> ${LOG_FILE} 2>&1 # ${MARKER}"

CURRENT_CRON="$(crontab -l 2>/dev/null || true)"
FILTERED_CRON="$(printf '%s\n' "${CURRENT_CRON}" | sed "/${MARKER}/d")"

if [[ -n "${FILTERED_CRON//$'\n'/}" ]]; then
  NEW_CRON="${FILTERED_CRON}"$'\n'"${ENTRY}"
else
  NEW_CRON="${ENTRY}"
fi

if [[ "${DRY_RUN}" == "true" ]]; then
  printf '%s\n' "${NEW_CRON}"
  exit 0
fi

printf '%s\n' "${NEW_CRON}" | crontab -
echo "installed cron entry:"
echo "${ENTRY}"
