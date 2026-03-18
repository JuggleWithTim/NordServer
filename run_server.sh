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

HTTP_BIND="${NORD_HTTP_BIND:-0.0.0.0}"
HTTP_PORT="${NORD_HTTP_PORT:-8080}"
HTTP_WATCHDOG_INTERVAL_SECONDS="${NORD_HTTP_WATCHDOG_INTERVAL_SECONDS:-5}"
HTTP_WATCHDOG_MAX_FAILS="${NORD_HTTP_WATCHDOG_MAX_FAILS:-3}"
HTTP_WATCHDOG_HEALTHCHECK="${NORD_HTTP_WATCHDOG_HEALTHCHECK:-1}"
HTTP_WATCHDOG_HOST="${NORD_HTTP_WATCHDOG_HOST:-}"
HTTP_PID_FILE="$SERVER_ROOT/http_sidecar.pid"

if [ -z "$HTTP_WATCHDOG_HOST" ]; then
  case "$HTTP_BIND" in
    0.0.0.0|::|"")
      HTTP_WATCHDOG_HOST="127.0.0.1"
      ;;
    *)
      HTTP_WATCHDOG_HOST="$HTTP_BIND"
      ;;
  esac
fi

WEB_PID=0
WATCHDOG_PID=0

start_http_sidecar() {
  NORD_HTTP_BIND="$HTTP_BIND" \
  NORD_HTTP_PORT="$HTTP_PORT" \
  NORD_DB_PATH="$DB_PATH" \
  stdbuf -oL -eL python3 "$SERVER_ROOT/nord_server.py" >>"$SERVER_ROOT/web.log" 2>&1 &
  WEB_PID=$!
  printf '%s\n' "$WEB_PID" >"$HTTP_PID_FILE"
  echo "[launcher] HTTP sidecar started (pid=$WEB_PID bind=$HTTP_BIND:$HTTP_PORT)"
}

http_healthcheck_url() {
  if [[ "$HTTP_WATCHDOG_HOST" == *:* ]]; then
    printf "http://[%s]:%s/ServerTime.jsp" "$HTTP_WATCHDOG_HOST" "$HTTP_PORT"
  else
    printf "http://%s:%s/ServerTime.jsp" "$HTTP_WATCHDOG_HOST" "$HTTP_PORT"
  fi
}

start_http_watchdog() {
  (
    local health_url failures
    health_url="$(http_healthcheck_url)"
    failures=0
    while true; do
      sleep "$HTTP_WATCHDOG_INTERVAL_SECONDS"

      if ! kill -0 "$WEB_PID" >/dev/null 2>&1; then
        echo "[launcher] HTTP sidecar exited; restarting"
        start_http_sidecar
        failures=0
        continue
      fi

      if [ "$HTTP_WATCHDOG_HEALTHCHECK" != "1" ]; then
        continue
      fi

      if ! command -v curl >/dev/null 2>&1; then
        continue
      fi

      if curl -fsS --max-time 3 "$health_url" >/dev/null 2>&1; then
        failures=0
        continue
      fi

      failures=$((failures + 1))
      if [ "$failures" -lt "$HTTP_WATCHDOG_MAX_FAILS" ]; then
        continue
      fi

      echo "[launcher] HTTP sidecar healthcheck failed ${failures} times; restarting"
      kill "$WEB_PID" >/dev/null 2>&1 || true
      wait "$WEB_PID" >/dev/null 2>&1 || true
      start_http_sidecar
      failures=0
    done
  ) &
  WATCHDOG_PID=$!
}

cleanup() {
  if [ "$WATCHDOG_PID" -gt 1 ] 2>/dev/null; then
    pkill -P "$WATCHDOG_PID" >/dev/null 2>&1 || true
    kill "$WATCHDOG_PID" >/dev/null 2>&1 || true
  fi
  if [ -f "$HTTP_PID_FILE" ]; then
    HTTP_PID="$(cat "$HTTP_PID_FILE" 2>/dev/null || true)"
    if [ -n "$HTTP_PID" ] && [ "$HTTP_PID" -gt 1 ] 2>/dev/null; then
      kill "$HTTP_PID" >/dev/null 2>&1 || true
    fi
    rm -f "$HTTP_PID_FILE"
  fi
  if [ "$WEB_PID" -gt 1 ] 2>/dev/null; then
    kill "$WEB_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

start_http_sidecar
start_http_watchdog

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
