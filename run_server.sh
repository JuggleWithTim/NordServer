#!/usr/bin/env bash
set -euo pipefail

SERVER_ROOT="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SERVER_ROOT/.." && pwd)"
DB_PATH="${NORD_DB_PATH:-$SERVER_ROOT/nord-data.sqlite}"

if [ ! -f "$SERVER_ROOT/NordServer.class" ] || [ "$SERVER_ROOT/NordServer.java" -nt "$SERVER_ROOT/NordServer.class" ] || [ "$SERVER_ROOT/NordDatabase.java" -nt "$SERVER_ROOT/NordDatabase.class" ]; then
  COMPILE_CP="$(printf '%s:' "$PROJECT_ROOT"/*.jar)$SERVER_ROOT:$PROJECT_ROOT"
  javac -cp "$COMPILE_CP" "$SERVER_ROOT/NordDatabase.java" "$SERVER_ROOT/NordServer.java"
fi

: >"$SERVER_ROOT/web.log"
: >"$SERVER_ROOT/server.log"

NORD_HTTP_BIND="${NORD_HTTP_BIND:-127.0.0.1}" \
NORD_HTTP_PORT="${NORD_HTTP_PORT:-8080}" \
stdbuf -oL -eL python3 "$SERVER_ROOT/nord_server.py" >"$SERVER_ROOT/web.log" 2>&1 &
WEB_PID=$!
trap 'kill $WEB_PID >/dev/null 2>&1 || true' EXIT

CP="$(printf '%s:' "$PROJECT_ROOT"/*.jar)$SERVER_ROOT:$PROJECT_ROOT"
SERVER_ARGS=("--tcp-only")
if [ "${NORD_ENABLE_UDP:-0}" = "1" ]; then
  SERVER_ARGS=("--udp")
fi
stdbuf -oL -eL java \
  -Dnord.db.path="$DB_PATH" \
  -Dnord.building.costs.path="$SERVER_ROOT/building-costs.properties" \
  -cp "$CP" \
  NordServer "${SERVER_ARGS[@]}" | tee -a "$SERVER_ROOT/server.log"
