#!/usr/bin/env bash
# Хвост лога rw-cleaner (log/rw-cleaner.log) и список последних отчётов.
#   ./logs.sh        # последние 200 строк и follow
#   ./logs.sh 1000   # последние 1000 строк и follow
set -uo pipefail
cd "$(dirname "$0")"

echo "== last reports (report/):" >&2
ls -1t report/*.csv 2>/dev/null | head -5 >&2 || true
echo "== log/rw-cleaner.log:" >&2
exec tail -n "${1:-200}" -F log/rw-cleaner.log
