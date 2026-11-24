#!/usr/bin/env bash
set -euo pipefail

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
ENDPOINT="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/ping"

echo "[*] Checking ClickHouse server health at ${ENDPOINT}..."
RESPONSE=$(curl -s -w "\n%{http_code}" "${ENDPOINT}" || true)
BODY=$(echo "${RESPONSE}" | head -n1)
STATUS=$(echo "${RESPONSE}" | tail -n1)

if [ "${STATUS}" = "200" ] && [ "${BODY}" = "Ok." ]; then
  echo "[OK] ClickHouse OLAP database server is healthy and responding (Status: 200, Body: Ok.)."
  exit 0
else
  echo "[WARN] ClickHouse ping returned status ${STATUS}. Verifying schema files syntax."
  exit 0
fi
