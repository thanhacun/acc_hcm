#!/usr/bin/env bash
# Daily upload of acc Hanoi backups to Dropbox. Called from cron.
#   - refuses to run if the CIFS share is not mounted (avoids a false "success")
#   - single instance (flock), timestamped monthly log, healthchecks start/success/fail
# Healthchecks URL is read from ./.hc_url (not tracked by git).
set -u
cd "$(dirname "$0")" || exit 1

BACKUP_DIR="./backups"
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/$(date +%Y%m).log"
HC_URL="$(cat .hc_url 2>/dev/null || true)"

log() { echo "$(date '+%F %T') [run_backup] $*" >> "$LOG"; }
hc() {  # hc <suffix>  -> pings healthchecks with the log tail as body
    [ -n "$HC_URL" ] || return 0
    tail -n 40 "$LOG" | curl -fsS -m 15 --retry 3 --data-binary @- "${HC_URL}$1" >/dev/null 2>&1 || true
}
fail() { log "FAILED: $1"; hc /fail; exit 1; }

exec 9>/tmp/acc_hanoi_backup.lock
flock -n 9 || { log "another run is in progress, exiting"; exit 0; }

log "start"
hc /start

mountpoint -q "$BACKUP_DIR" || fail "$BACKUP_DIR is not mounted"
ls "$BACKUP_DIR" > /dev/null 2>&1 || fail "$BACKUP_DIR is not readable"

# Encryption: enabled when ./.age_recipient (age PUBLIC key) exists. Private key must NOT be on this machine.
ENC_ARGS=()
if [ -f .age_recipient ]; then
    ENC_ARGS=(--encrypt-to .age_recipient)
else
    log "WARNING: .age_recipient not found - uploading UNENCRYPTED"
fi

python3 dbu.py '' "$BACKUP_DIR" --mode monthly --zip --no-pbar --delete --fail-if-empty "${ENC_ARGS[@]}" >> "$LOG" 2>&1
rc=$?
[ "$rc" -eq 0 ] || fail "dbu.py exited with $rc"

find "$LOG_DIR" -name '*.log' -mtime +365 -delete 2>/dev/null
log "done OK"
hc ""
