#!/usr/bin/env bash
set -euo pipefail

SERVER_ROOT="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SERVER_ROOT/.." && pwd)"

HOST="${NORD_SMOKE_HOST:-127.0.0.1}"
TCP_PORT="${NORD_SMOKE_TCP_PORT:-41210}"
UDP_PORT="${NORD_SMOKE_UDP_PORT:-41211}"
CONNECT_TIMEOUT_MS="${NORD_SMOKE_CONNECT_TIMEOUT_MS:-10000}"
MESSAGE_TIMEOUT_MS="${NORD_SMOKE_MESSAGE_TIMEOUT_MS:-10000}"
USE_UDP="${NORD_SMOKE_USE_UDP:-0}"
START_SERVER="${NORD_SMOKE_START_SERVER:-0}"

if [ ! -f "$SERVER_ROOT/LiveProtocolSmoke.class" ] || [ "$SERVER_ROOT/LiveProtocolSmoke.java" -nt "$SERVER_ROOT/LiveProtocolSmoke.class" ]; then
  CP="$SERVER_ROOT:$PROJECT_ROOT:$(printf '%s:' "$PROJECT_ROOT"/*.jar)"
  javac -cp "$CP" "$SERVER_ROOT/LiveProtocolSmoke.java"
fi

SERVER_PID=""
cleanup() {
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [ "$START_SERVER" = "1" ]; then
  if (echo >/dev/tcp/"$HOST"/"$TCP_PORT") >/dev/null 2>&1; then
    echo "Cannot auto-start smoke server: TCP $HOST:$TCP_PORT is already in use. Stop the existing server or set NORD_SMOKE_START_SERVER=0." >&2
    exit 1
  fi

  DB_PATH="${NORD_DB_PATH:-/tmp/nord-live-smoke.sqlite}"
  NORD_DB_PATH="$DB_PATH" NORD_ENABLE_UDP="$USE_UDP" "$SERVER_ROOT/run_server.sh" >/tmp/nord-live-smoke-server.log 2>&1 &
  SERVER_PID=$!

  deadline=$((SECONDS + 25))
  while ! (echo >/dev/tcp/"$HOST"/"$TCP_PORT") >/dev/null 2>&1; do
    if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      echo "Auto-started server exited before opening TCP $HOST:$TCP_PORT. See /tmp/nord-live-smoke-server.log" >&2
      exit 1
    fi
    if [ $SECONDS -ge "$deadline" ]; then
      echo "Server did not open TCP $HOST:$TCP_PORT in time. See /tmp/nord-live-smoke-server.log" >&2
      exit 1
    fi
    sleep 1
  done
fi

CP="$SERVER_ROOT:$PROJECT_ROOT:$(printf '%s:' "$PROJECT_ROOT"/*.jar)"
ARGS=(
  "--host=$HOST"
  "--tcp-port=$TCP_PORT"
  "--udp-port=$UDP_PORT"
  "--connect-timeout-ms=$CONNECT_TIMEOUT_MS"
  "--message-timeout-ms=$MESSAGE_TIMEOUT_MS"
)
if [ "$USE_UDP" = "1" ]; then
  ARGS+=("--use-udp")
fi

java -cp "$CP" LiveProtocolSmoke "${ARGS[@]}"
