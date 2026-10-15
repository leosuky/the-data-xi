#!/usr/bin/env bash
# =============================================================================
# backup_warehouse.sh
# =============================================================================
# Nightly backup of the The Data XI warehouse (Postgres, all schemas) on the
# server. Run from cron:
#
#     30 3 * * * bash $HOME/the-data-xi/include/scripts/backup_warehouse.sh >> $HOME/backups/backup.log 2>&1
#
# What it does:
#   1. pg_dump (custom format, compressed) from the warehouse container into
#      $BACKUP_DIR/dataxi_<timestamp>.dump — written to a .tmp file first, so a
#      failed dump never looks like a good one.
#   2. Verifies the dump is readable (pg_restore --list) before keeping it.
#   3. Points $BACKUP_DIR/latest.dump at the new file (for easy off-box pulls).
#   4. Deletes dumps older than KEEP_DAYS.
#
# Off-box copy (from Windows/Mac, uses the `dataxi` ssh alias):
#     scp dataxi:~/backups/latest.dump <local folder>
#
# Restore (into an empty/new warehouse):
#     docker exec -i the_data_xi_postgres pg_restore -U the_data_xi -d the_data_xi \
#         --clean --if-exists --no-owner < dataxi_<timestamp>.dump
# =============================================================================

set -euo pipefail

CONTAINER="the_data_xi_postgres"
DB_USER="the_data_xi"
DB_NAME="the_data_xi"
BACKUP_DIR="${BACKUP_DIR:-$HOME/backups}"
KEEP_DAYS="${KEEP_DAYS:-7}"

mkdir -p "$BACKUP_DIR"
stamp="$(date +%Y%m%d_%H%M)"
final="$BACKUP_DIR/dataxi_${stamp}.dump"
tmp="${final}.tmp"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
trap 'rm -f "$tmp"' EXIT

log "Dumping ${DB_NAME} from ${CONTAINER} ..."
docker exec "$CONTAINER" pg_dump -U "$DB_USER" -d "$DB_NAME" -Fc > "$tmp"

# A dump that pg_restore can't list is corrupt/truncated — fail loudly.
docker exec -i "$CONTAINER" pg_restore --list > /dev/null < "$tmp"

mv "$tmp" "$final"
ln -sfn "$(basename "$final")" "$BACKUP_DIR/latest.dump"
log "OK: $final ($(du -h "$final" | cut -f1))"

deleted="$(find "$BACKUP_DIR" -maxdepth 1 -name 'dataxi_*.dump' -mtime +"$KEEP_DAYS" -print -delete | wc -l)"
log "Retention: removed ${deleted} dump(s) older than ${KEEP_DAYS} days"
