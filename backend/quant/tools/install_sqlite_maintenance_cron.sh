#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PYTHON_BIN="/usr/bin/python3"
SCHEDULE="*/15 * * * *"
DB_PATH="logs/quant_api.db"
STATE_PATH="logs/sqlite_maintenance_state.json"
REPORT_PATH="logs/sqlite_maintenance_latest.json"
LOG_FILE="logs/sqlite_maintenance_cron.log"
CHECKPOINT="PASSIVE"
ANALYZE_HOURS="24"
VACUUM_FRAG="20"
VACUUM_FREE="268435456"
ALERT_FRAG="35"
ALERT_FREE="536870912"
MARKER="quant_sqlite_maintenance_job"
DRY_RUN="false"

usage() {
  cat <<'EOF'
Install or update cron entry for sqlite_maintenance_job.py.

Options:
  --python <path>                 Python executable (default: /usr/bin/python3)
  --project-dir <path>            Project root (default: parent of this script)
  --schedule "<cron expr>"        Cron schedule (default: */15 * * * *)
  --db-path <path>                DB path passed to job script
  --state-path <path>             State file path
  --report-path <path>            Report file path
  --log-file <path>               Cron log output path
  --checkpoint <mode>             PASSIVE|FULL|RESTART|TRUNCATE
  --analyze-every-hours <float>   Analyze interval hours
  --vacuum-fragmentation-threshold <float>
  --vacuum-free-bytes-threshold <int>
  --alert-fragmentation-threshold <float>
  --alert-free-bytes-threshold <int>
  --dry-run                       Print final cron entry without installing
  -h, --help                      Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python)
      PYTHON_BIN="$2"
      shift 2
      ;;
    --project-dir)
      PROJECT_DIR="$2"
      shift 2
      ;;
    --schedule)
      SCHEDULE="$2"
      shift 2
      ;;
    --db-path)
      DB_PATH="$2"
      shift 2
      ;;
    --state-path)
      STATE_PATH="$2"
      shift 2
      ;;
    --report-path)
      REPORT_PATH="$2"
      shift 2
      ;;
    --log-file)
      LOG_FILE="$2"
      shift 2
      ;;
    --checkpoint)
      CHECKPOINT="$2"
      shift 2
      ;;
    --analyze-every-hours)
      ANALYZE_HOURS="$2"
      shift 2
      ;;
    --vacuum-fragmentation-threshold)
      VACUUM_FRAG="$2"
      shift 2
      ;;
    --vacuum-free-bytes-threshold)
      VACUUM_FREE="$2"
      shift 2
      ;;
    --alert-fragmentation-threshold)
      ALERT_FRAG="$2"
      shift 2
      ;;
    --alert-free-bytes-threshold)
      ALERT_FREE="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN="true"
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

PROJECT_DIR="$(cd "${PROJECT_DIR}" && pwd)"
if [[ ! -d "${PROJECT_DIR}" ]]; then
  echo "project dir not found: ${PROJECT_DIR}" >&2
  exit 1
fi

ENTRY="${SCHEDULE} cd ${PROJECT_DIR} && ${PYTHON_BIN} tools/sqlite_maintenance_job.py --db-path ${DB_PATH} --state-path ${STATE_PATH} --report-path ${REPORT_PATH} --checkpoint ${CHECKPOINT} --analyze-every-hours ${ANALYZE_HOURS} --vacuum-fragmentation-threshold ${VACUUM_FRAG} --vacuum-free-bytes-threshold ${VACUUM_FREE} --alert-fragmentation-threshold ${ALERT_FRAG} --alert-free-bytes-threshold ${ALERT_FREE} >> ${LOG_FILE} 2>&1 # ${MARKER}"

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
