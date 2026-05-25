#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROLLBACK_SQL="$REPO_ROOT/supabase/manual/20260512_partner_booking_requests_rollback.sql"
POSTCHECK_SQL="$REPO_ROOT/supabase/manual/20260512_partner_booking_requests_postcheck.sql"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
REPORT_DIR="$REPO_ROOT/reports/db-rollout"
LOG_FILE="$REPORT_DIR/partner-booking-rollback-$TIMESTAMP.log"

mkdir -p "$REPORT_DIR"

if ! command -v psql >/dev/null 2>&1; then
  echo "psql non trovato. Installa PostgreSQL client oppure esegui i file SQL dal pannello SQL di Supabase." >&2
  exit 1
fi

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL mancante. Esporta la connection string prima di eseguire lo script." >&2
  exit 1
fi

run_sql_file() {
  local label="$1"
  local file_path="$2"
  {
    echo
    echo "===== $label ====="
    echo "FILE: $file_path"
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$file_path"
  } | tee -a "$LOG_FILE"
}

echo "Log rollback: $LOG_FILE"
echo "Eseguo rollback e verifica finale..." | tee -a "$LOG_FILE"

run_sql_file "ROLLBACK" "$ROLLBACK_SQL"
run_sql_file "POSTCHECK" "$POSTCHECK_SQL"

echo
echo "Rollback partner_booking_requests completato. Verifica il log: $LOG_FILE"
