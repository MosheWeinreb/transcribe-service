#!/bin/sh
set -eu
cd "$(dirname "$0")/.."

fail() {
    printf '%s\n' "$1" >&2
    exit 1
}

command -v docker >/dev/null 2>&1 || fail "Install Docker with Docker Compose, then run this script again."
docker info >/dev/null 2>&1 || fail "Start Docker (Linux containers), then run this script again."
docker compose version
if [ ! -e .env ]; then
    cp .env.example .env
    printf '%s\n' 'Created .env from .env.example.'
fi
docker compose build api
docker compose up -d --wait
api_port=$(docker compose exec -T api python -c 'from src.settings.config import get_settings; print(get_settings().server_port)')
address=$(docker compose port api "$api_port")
address=$(printf '%s\n' "$address" | head -n 1)
printf 'App: http://%s\nAPI docs: http://%s/docs\n' "$address" "$address"
printf '%s\n' 'The first transcription downloads and loads the model.'
