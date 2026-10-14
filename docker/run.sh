#!/usr/bin/env bash
# Один проход rw-cleaner на сервере (из каталога с docker-compose.yml и .env).
# Аргументы передаются в main.py как есть. Без --execute — dry-run: БД только
# на чтение, в панели только чтение.
#
#   ./run.sh --full-scan                         # dry-run, режим expired
#   ./run.sh --mode free_inactive --full-scan    # dry-run, бесплатный тариф
#
# --execute (реальные удаления) заблокирован, пока явно не разрешён:
#   RW_CLEANER_ALLOW_EXECUTE=yes ./run.sh --execute --max-deletions 5
set -uo pipefail
cd "$(dirname "$0")"

for arg in "$@"; do
  if [[ "$arg" == "--execute" && "${RW_CLEANER_ALLOW_EXECUTE:-}" != "yes" ]]; then
    echo "run.sh: --execute is blocked (dry-run rollout stage)." >&2
    echo "run.sh: to really delete, run: RW_CLEANER_ALLOW_EXECUTE=yes ./run.sh $*" >&2
    exit 2
  fi
done

if [[ ! -f .env ]]; then
  echo "run.sh: .env not found in $(pwd) (copy .env.example and fill it in)" >&2
  exit 2
fi

mkdir -p log report

docker compose --project-directory . -f docker-compose.yml \
  run --rm -T rw-cleaner "$@"
rc=$?

case "$rc" in
  0) meaning="ok (including dry-run / nothing to do)" ;;
  1) meaning="some candidates not processed / open_archive_mismatch / report or state write failed" ;;
  2) meaning="settings error" ;;
  3) meaning="another run is in progress" ;;
  4) meaning="AUTO-STOP: orphan / candidate ratio or failure streak" ;;
  5) meaning="panel scan incomplete, nothing deleted" ;;
  6) meaning="NEEDS A HUMAN: uncertain/pending records (see README)" ;;
  *) meaning="unexpected (docker / compose error?)" ;;
esac
echo "rw-cleaner exit code: ${rc} — ${meaning}" >&2
exit "$rc"
