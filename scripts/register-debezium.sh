#!/usr/bin/env bash

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$PROJECT_DIR/.env"
TEMPLATE_FILE="$PROJECT_DIR/connectors/debezium/sqlserver-northwind.json.template"
CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="northwind-sqlserver-source"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Missing environment file: $ENV_FILE" >&2
    exit 1
fi

if [[ ! -f "$TEMPLATE_FILE" ]]; then
    echo "Missing connector template: $TEMPLATE_FILE" >&2
    exit 1
fi

set -a
source "$ENV_FILE"
set +a

: "${DEBEZIUM_SQLSERVER_USER:?Missing DEBEZIUM_SQLSERVER_USER}"
: "${DEBEZIUM_SQLSERVER_PASSWORD:?Missing DEBEZIUM_SQLSERVER_PASSWORD}"

GENERATED_CONFIG="$(mktemp)"
CONNECT_RESPONSE="$(mktemp)"

cleanup() {
    rm -f "$GENERATED_CONFIG" "$CONNECT_RESPONSE"
}

trap cleanup EXIT

python3 - "$TEMPLATE_FILE" "$GENERATED_CONFIG" <<'PY'
import json
import os
import sys

template_path = sys.argv[1]
output_path = sys.argv[2]

with open(template_path, "r", encoding="utf-8") as template_file:
    config = json.load(template_file)

config["database.user"] = os.environ["DEBEZIUM_SQLSERVER_USER"]
config["database.password"] = os.environ["DEBEZIUM_SQLSERVER_PASSWORD"]

with open(output_path, "w", encoding="utf-8") as output_file:
    json.dump(config, output_file)
PY

HTTP_STATUS="$(
    curl \
        --silent \
        --show-error \
        --output "$CONNECT_RESPONSE" \
        --write-out "%{http_code}" \
        --request PUT \
        --header "Content-Type: application/json" \
        --data-binary "@$GENERATED_CONFIG" \
        "$CONNECT_URL/connectors/$CONNECTOR_NAME/config"
)"

if [[ "$HTTP_STATUS" != "200" && "$HTTP_STATUS" != "201" ]]; then
    echo "Connector registration failed with HTTP $HTTP_STATUS" >&2
    python3 -m json.tool "$CONNECT_RESPONSE" 2>/dev/null || \
        sed -n '1,160p' "$CONNECT_RESPONSE"
    exit 1
fi

echo "Connector $CONNECTOR_NAME registered successfully."
