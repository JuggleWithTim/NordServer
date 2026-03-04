#!/usr/bin/env bash
set -euo pipefail

SERVER_ROOT="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SERVER_ROOT/.." && pwd)"

HOST="${NORD_SMOKE_HOST:-127.0.0.1}"
TCP_PORT="${NORD_SMOKE_TCP_PORT:-41210}"
USE_UDP="${NORD_SMOKE_USE_UDP:-0}"
RUNS="${NORD_CHECKSUM_SOAK_RUNS:-20}"
START_SERVER="${NORD_CHECKSUM_SOAK_START_SERVER:-1}"
DB_PATH="${NORD_DB_PATH:-/tmp/nord-checksum-soak.sqlite}"

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [ "$RUNS" -le 0 ]; then
  echo "NORD_CHECKSUM_SOAK_RUNS must be a positive integer, got: $RUNS" >&2
  exit 1
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
    echo "Cannot auto-start checksum soak server: TCP $HOST:$TCP_PORT is already in use." >&2
    echo "Stop the existing server or set NORD_CHECKSUM_SOAK_START_SERVER=0." >&2
    exit 1
  fi

  NORD_DB_PATH="$DB_PATH" NORD_ENABLE_UDP="$USE_UDP" "$SERVER_ROOT/run_server.sh" >/tmp/nord-checksum-soak-server.log 2>&1 &
  SERVER_PID=$!

  deadline=$((SECONDS + 25))
  while ! (echo >/dev/tcp/"$HOST"/"$TCP_PORT") >/dev/null 2>&1; do
    if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      echo "Checksum soak server exited before opening TCP $HOST:$TCP_PORT. See /tmp/nord-checksum-soak-server.log" >&2
      exit 1
    fi
    if [ $SECONDS -ge "$deadline" ]; then
      echo "Checksum soak server did not open TCP $HOST:$TCP_PORT in time. See /tmp/nord-checksum-soak-server.log" >&2
      exit 1
    fi
    sleep 1
  done
fi

pass_count=0
for run_index in $(seq 1 "$RUNS"); do
  echo "CHECKSUM_SOAK_RUN $run_index/$RUNS"
  NORD_SMOKE_START_SERVER=0 \
  NORD_SMOKE_USE_UDP="$USE_UDP" \
  NORD_SMOKE_HOST="$HOST" \
  NORD_SMOKE_TCP_PORT="${NORD_SMOKE_TCP_PORT:-41210}" \
  NORD_SMOKE_UDP_PORT="${NORD_SMOKE_UDP_PORT:-41211}" \
  NORD_SMOKE_CONNECT_TIMEOUT_MS="${NORD_SMOKE_CONNECT_TIMEOUT_MS:-10000}" \
  NORD_SMOKE_MESSAGE_TIMEOUT_MS="${NORD_SMOKE_MESSAGE_TIMEOUT_MS:-10000}" \
  "$SERVER_ROOT/run_live_smoke.sh"
  pass_count=$run_index
done

echo "CHECKSUM_SOAK_PASS runs=$pass_count udp=$USE_UDP db=$DB_PATH"
