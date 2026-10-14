#!/usr/bin/env bash
# Выкатка rw-cleaner на сервер (запускать локально, из любого каталога):
#   1. собирает образ (linux/amd64) в tar — пропустить: --skip-build;
#   2. кладёт на сервер docker-compose.yml, run.sh, logs.sh, .env.example
#      (.env не трогает никогда);
#   3. докачиваемо заливает tar (rsync --partial, повторы при обрывах) во
#      временный *.new и сверяет sha256;
#   4. на сервере: старый tar → .bak (один, без накопления), *.new → tar,
#      docker load.
# Ничего не запускает: rw-cleaner — одноразовый запуск через ./run.sh.
set -euo pipefail
cd "$(dirname "$0")"

SSH_HOST="${SSH_HOST:-mi.fornex.app}"
REMOTE_DIR="${REMOTE_DIR:-/srv/monkey-island/rw-cleaner}"
IMAGE_TAR_NAME="monkey-island-rw-cleaner-amd64.tar"
UPLOAD_ATTEMPTS="${UPLOAD_ATTEMPTS:-30}"

skip_build=false
for arg in "$@"; do
  case "$arg" in
    --skip-build) skip_build=true ;;
    *) echo "usage: $0 [--skip-build]" >&2; exit 2 ;;
  esac
done

if [[ "$skip_build" == false ]]; then
  echo "==> build image"
  ./build-image-amd64.sh
fi
[[ -f "$IMAGE_TAR_NAME" ]] || { echo "no $IMAGE_TAR_NAME, build first" >&2; exit 1; }

echo "==> prepare ${SSH_HOST}:${REMOTE_DIR}"
ssh "$SSH_HOST" "mkdir -p '${REMOTE_DIR}/log' '${REMOTE_DIR}/report'"
rsync -t docker-compose.yml run.sh logs.sh .env.example "${SSH_HOST}:${REMOTE_DIR}/"
ssh "$SSH_HOST" "chmod +x '${REMOTE_DIR}/run.sh' '${REMOTE_DIR}/logs.sh'"

echo "==> upload ${IMAGE_TAR_NAME} (resumable)"
attempt=1
while true; do
  rc=0
  rsync --partial --append-verify --timeout=60 \
    "$IMAGE_TAR_NAME" "${SSH_HOST}:${REMOTE_DIR}/${IMAGE_TAR_NAME}.new" || rc=$?
  [[ "$rc" -eq 0 ]] && break
  if [[ "$attempt" -ge "$UPLOAD_ATTEMPTS" ]]; then
    echo "upload failed after ${attempt} attempts (rsync rc=${rc})" >&2
    exit 1
  fi
  echo "rsync rc=${rc}, retry ${attempt}/${UPLOAD_ATTEMPTS} in 5s" >&2
  attempt=$((attempt + 1))
  sleep 5
done

local_sum="$(shasum -a 256 "$IMAGE_TAR_NAME" | awk '{print $1}')"
remote_sum="$(ssh "$SSH_HOST" "sha256sum '${REMOTE_DIR}/${IMAGE_TAR_NAME}.new'" | awk '{print $1}')"
echo "local  ${local_sum}"
echo "remote ${remote_sum}"
if [[ "$local_sum" != "$remote_sum" ]]; then
  echo "sha256 mismatch, removing the remote .new file" >&2
  ssh "$SSH_HOST" "rm -f '${REMOTE_DIR}/${IMAGE_TAR_NAME}.new'"
  exit 1
fi

echo "==> swap tar and docker load"
ssh "$SSH_HOST" "set -e; cd '${REMOTE_DIR}'; \
  if [ -f '${IMAGE_TAR_NAME}' ]; then mv -f '${IMAGE_TAR_NAME}' '${IMAGE_TAR_NAME}.bak'; fi; \
  mv '${IMAGE_TAR_NAME}.new' '${IMAGE_TAR_NAME}'; \
  docker load -i '${IMAGE_TAR_NAME}'; \
  if [ ! -f .env ]; then echo 'NOTE: .env is missing — cp .env.example .env and fill it in'; fi"

echo "==> done. On the server:"
echo "    cd ${REMOTE_DIR} && ./run.sh --full-scan     # dry-run"
echo "    cd ${REMOTE_DIR} && ./logs.sh                # follow the log"
