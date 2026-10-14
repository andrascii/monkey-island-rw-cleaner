#!/usr/bin/env bash
# Сборка образа под сервер (linux/amd64) в tar рядом со скриптом.
set -euo pipefail
cd "$(dirname "$0")"

docker buildx build \
  --platform linux/amd64 \
  -t monkey-island-rw-cleaner:v0.1 \
  -f Dockerfile \
  --output type=docker,dest=monkey-island-rw-cleaner-amd64.tar ..
