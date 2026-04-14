#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PYTHON_BIN="/usr/bin/python3"
SCHEDULE="10 3 * * *"
DB_PATH="logs/quant_api.db"
BACKUP_DIR="logs/db_backups"
PREFIX="quant_api"
RETAIN="14"
LOG_FILE="logs/sqlite_backup_cron.log"
MARKER="quant_sqlite_backup_job"
DRY_RUN="false"

usage() {
  cat <<'EOF'
Install or update cron entry for sqlite_backup.py.

Options:
  --python <path>          Python executable (default: /usr/bin/python3)
  --project-dir <path>     Project root (default: parent of this script)
  --schedule "<cron expr>" Cron schedule (default: 10 3 * * *)
  --db-path <path>         Source DB path
  --backup-dir <path>      Backup directory
  --prefix <name>          Backup filename prefix
  --retain <count>         Retained backup files count
  --log-file <path>        Cron log output path
  --dry-run                Print final crontab without installing
  -h, --help               Show help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python) PYTHON_BIN="$2"; shift 2 ;;
    --project-dir) PROJECT_DIR="$2"; shift 2 ;;
    --schedule) SCHEDULE="$2"; shift 2 ;;
    --db-path) DB_PATH="$2"; shift 2 ;;
    --backup-dir) BACKUP_DIR="$2"; shift 2 ;;
    --prefix) PREFIX="$2"; shift 2 ;;
    --retain) RETAIN="$2"; shift 2 ;;
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

ENTRY="${SCHEDULE} cd ${PROJECT_DIR} && ${PYTHON_BIN} tools/sqlite_backup.py --db-path ${DB_PATH} --backup-dir ${BACKUP_DIR} --prefix ${PREFIX} --retain ${RETAIN} --verify >> ${LOG_FILE} 2>&1 # ${MARKER}"

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
