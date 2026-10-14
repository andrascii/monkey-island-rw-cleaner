# syntax=docker/dockerfile:1.7
ARG PYTHON_VERSION=3.12
FROM python:$PYTHON_VERSION-slim AS builder

ENV container="docker"
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

COPY requirements.txt ./

RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install --upgrade pip setuptools && \
    pip install -r requirements.txt

COPY main.py ./
COPY rw_cleaner ./rw_cleaner
COPY common ./common
COPY proto ./proto

FROM python:$PYTHON_VERSION-slim

ENV container="docker"
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PYTHON_LIB_PATH=/usr/local/lib/python${PYTHON_VERSION%.*}/site-packages
WORKDIR /app

COPY --from=builder $PYTHON_LIB_PATH $PYTHON_LIB_PATH
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /app /app

# Один запуск = один проход (не демон). Аргументы `docker compose run`
# передаются в main.py как есть; без --execute — dry-run.
# log/ и report/ (CSV-отчёты и файл состояния STATE_PATH) — тома, см.
# docker-compose.yml.
ENTRYPOINT ["python3", "/app/main.py"]
