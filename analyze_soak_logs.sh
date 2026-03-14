#!/usr/bin/env bash
set -euo pipefail

CLIENT_LOG=""
SERVER_LOG=""
WEB_LOG=""

usage() {
  cat <<'EOF'
Usage:
  analyze_soak_logs.sh --client-log <path> [--server-log <path>] [--web-log <path>]

Analyzes captured soak logs for likely out-of-sync and fatal-crash signatures.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --client-log)
      CLIENT_LOG="${2:-}"
      shift 2
      ;;
    --server-log)
      SERVER_LOG="${2:-}"
      shift 2
      ;;
    --web-log)
      WEB_LOG="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [ -z "$CLIENT_LOG" ]; then
  echo "--client-log is required" >&2
  usage >&2
  exit 1
fi

if [ ! -f "$CLIENT_LOG" ]; then
  echo "Client log not found: $CLIENT_LOG" >&2
  exit 1
fi

count_pattern() {
  local file="$1"
  local pattern="$2"
  if [ -z "$file" ] || [ ! -f "$file" ]; then
    echo "0"
    return
  fi
  local count
  count="$(rg -c --no-messages -e "$pattern" "$file" || true)"
  if [ -z "$count" ]; then
    echo "0"
  else
    echo "$count"
  fi
}

sum_patterns() {
  local file="$1"
  shift
  local total=0
  for pattern in "$@"; do
    local count
    count="$(count_pattern "$file" "$pattern")"
    if [ -n "$count" ]; then
      total=$((total + count))
    fi
  done
  echo "$total"
}

client_out_of_sync_count="$(sum_patterns "$CLIENT_LOG" \
  'INTERNET_CONNECTION_OUT_OF_SYNC' \
  'checkSum = ' \
  'event\.getChecksum\(\)' \
  'BuildingsChecksumResponseMessage')"

client_fatal_count="$(sum_patterns "$CLIENT_LOG" \
  'Exception in thread "LWJGL Application"' \
  'EXCEPTION_ACCESS_VIOLATION' \
  'SIGSEGV' \
  'A fatal error has been detected by the Java Runtime Environment' \
  'NullPointerException')"
client_login_failure_count="$(sum_patterns "$CLIENT_LOG" \
  'LoginResponseMessage[{]responseCode=1' \
  'Session not valid! Client not logged in\\?' \
  'Error creating village!')"

server_checksum_skip_count="$(count_pattern "$SERVER_LOG" 'Skipped GetBuildingsChecksumMessage')"
server_checksum_send_fail_count="$(count_pattern "$SERVER_LOG" 'BuildingsChecksumResponseMessage')"
server_error_count="$(sum_patterns "$SERVER_LOG" \
  'Error handling message:' \
  'Failed to decode GenericByteArrayMessage payload' \
  'Failed to parse serialized GenericByteArray payload')"
web_error_count="$(sum_patterns "$WEB_LOG" \
  'Traceback \(most recent call last\)' \
  ' ERROR ' \
  'Exception')"
web_unknown_http_trace_count="$(count_pattern "$WEB_LOG" 'TRACE_UNKNOWN_HTTP')"
web_request_count="$(sum_patterns "$WEB_LOG" '^GET ' '^POST ')"

echo "SOAK_LOG_SUMMARY"
echo "client_log=$CLIENT_LOG"
if [ -n "$SERVER_LOG" ]; then
  echo "server_log=$SERVER_LOG"
fi
if [ -n "$WEB_LOG" ]; then
  echo "web_log=$WEB_LOG"
fi
echo "client_out_of_sync_signatures=$client_out_of_sync_count"
echo "client_fatal_signatures=$client_fatal_count"
echo "client_login_failure_signatures=$client_login_failure_count"
echo "server_checksum_skips=$server_checksum_skip_count"
echo "server_checksum_related_lines=$server_checksum_send_fail_count"
echo "server_error_signatures=$server_error_count"
echo "web_error_signatures=$web_error_count"
echo "web_unknown_http_traces=$web_unknown_http_trace_count"
echo "web_request_lines=$web_request_count"

status=0
if [ "$client_out_of_sync_count" -gt 0 ]; then
  status=2
fi
if [ "$client_fatal_count" -gt 0 ]; then
  status=2
fi
if [ "$client_login_failure_count" -gt 0 ]; then
  status=2
fi

if [ "$status" -eq 0 ]; then
  echo "SOAK_LOG_RESULT=clean"
  if [ "$web_unknown_http_trace_count" -gt 0 ]; then
    echo "SOAK_LOG_NOTE=unknown_http_observed"
  fi
else
  echo "SOAK_LOG_RESULT=issues_detected"
fi

exit "$status"
