#!/usr/bin/env bash
set -e

cd "$(dirname "$0")"

docker build --pull --no-cache -t envoy:latest -f ../Dockerfile.server ../

HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose down -v

docker volume rm demo_postgres_data || true

echo ""
echo "Demo has been reset, run the following to start:"
echo 'HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up --build'
