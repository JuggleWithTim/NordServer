#!/usr/bin/env bash
set -euo pipefail

SERVER_ROOT="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SERVER_ROOT/.." && pwd)"
DB_PATH="${NORD_DB_PATH:-$SERVER_ROOT/nord-data.sqlite}"

EVENT_COMPAT_SRC="$PROJECT_ROOT/com/slx/nord/jgnpersistentobjectdetails/Event.java"
EVENT_COMPAT_CLASS="$PROJECT_ROOT/com/slx/nord/jgnpersistentobjectdetails/Event.class"
if [ -f "$EVENT_COMPAT_SRC" ] && { [ ! -f "$EVENT_COMPAT_CLASS" ] || [ "$EVENT_COMPAT_SRC" -nt "$EVENT_COMPAT_CLASS" ]; }; then
  javac --release 8 -cp "$PROJECT_ROOT" "$EVENT_COMPAT_SRC"
fi

if [ ! -f "$SERVER_ROOT/NordServer.class" ] || [ "$SERVER_ROOT/NordServer.java" -nt "$SERVER_ROOT/NordServer.class" ] || [ "$SERVER_ROOT/NordDatabase.java" -nt "$SERVER_ROOT/NordDatabase.class" ]; then
  COMPILE_CP="$SERVER_ROOT:$PROJECT_ROOT:$(printf '%s:' "$PROJECT_ROOT"/*.jar)"
  javac -cp "$COMPILE_CP" "$SERVER_ROOT/NordDatabase.java" "$SERVER_ROOT/NordServer.java"
fi

: >"$SERVER_ROOT/web.log"
: >"$SERVER_ROOT/server.log"

NORD_HTTP_BIND="${NORD_HTTP_BIND:-0.0.0.0}" \
NORD_HTTP_PORT="${NORD_HTTP_PORT:-8080}" \
NORD_DB_PATH="$DB_PATH" \
stdbuf -oL -eL python3 "$SERVER_ROOT/nord_server.py" >"$SERVER_ROOT/web.log" 2>&1 &
WEB_PID=$!
trap 'kill $WEB_PID >/dev/null 2>&1 || true' EXIT

CP="$SERVER_ROOT:$PROJECT_ROOT:$(printf '%s:' "$PROJECT_ROOT"/*.jar)"
SERVER_ARGS=("--tcp-only")
if [ "${NORD_ENABLE_UDP:-0}" = "1" ]; then
  SERVER_ARGS=("--udp")
fi
stdbuf -oL -eL java \
  -Dnord.db.path="$DB_PATH" \
  -Dnord.randomseed.path="$PROJECT_ROOT/randomseed.dat" \
  -Dnord.levelup.reward="${NORD_LEVELUP_REWARD:-}" \
  -Dnord.levelup.reward.base="${NORD_LEVELUP_REWARD_BASE:-}" \
  -Dnord.levelup.reward.step="${NORD_LEVELUP_REWARD_STEP:-}" \
  -Dnord.buildings.checksum.enabled="${NORD_BUILDINGS_CHECKSUM_ENABLED:-0}" \
  -Dnord.buycredits.require.providercallback="${NORD_BUY_CREDITS_REQUIRE_PROVIDER_CALLBACK:-}" \
  -cp "$CP" \
  NordServer "${SERVER_ARGS[@]}" | tee -a "$SERVER_ROOT/server.log"
