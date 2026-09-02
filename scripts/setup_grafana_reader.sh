#!/usr/bin/env bash

set -Eeuo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.." &&
  pwd
)"

cd "$PROJECT_ROOT"

if [[ ! -f .env ]]; then
  echo "ERROR: .env does not exist."
  exit 1
fi

set -a
source .env
set +a

required_variables=(
  CLICKHOUSE_USER
  CLICKHOUSE_PASSWORD
  GRAFANA_CLICKHOUSE_USER
  GRAFANA_CLICKHOUSE_PASSWORD
)

for variable in "${required_variables[@]}"; do
  if [[ -z "${!variable:-}" ]]; then
    echo "ERROR: ${variable} is not configured."
    exit 1
  fi
done

if [[ ! "$GRAFANA_CLICKHOUSE_USER" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
  echo "ERROR: Invalid Grafana ClickHouse username."
  exit 1
fi

if [[ ! "$GRAFANA_CLICKHOUSE_PASSWORD" =~ ^[A-Za-z0-9_-]+$ ]]; then
  echo "ERROR: Grafana ClickHouse password must be URL-safe."
  exit 1
fi

docker compose exec -T clickhouse \
  clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --multiquery <<SQL
CREATE USER IF NOT EXISTS ${GRAFANA_CLICKHOUSE_USER}
IDENTIFIED WITH sha256_password
BY '${GRAFANA_CLICKHOUSE_PASSWORD}';

ALTER USER ${GRAFANA_CLICKHOUSE_USER}
IDENTIFIED WITH sha256_password
BY '${GRAFANA_CLICKHOUSE_PASSWORD}';

GRANT SELECT ON northwind_dw.* TO ${GRAFANA_CLICKHOUSE_USER};

CREATE SETTINGS PROFILE IF NOT EXISTS grafana_reader_profile
TO ${GRAFANA_CLICKHOUSE_USER};

ALTER SETTINGS PROFILE grafana_reader_profile
SETTINGS readonly = 1,
SETTINGS max_execution_time CHANGEABLE_IN_READONLY
TO ${GRAFANA_CLICKHOUSE_USER};
SQL

docker compose exec -T clickhouse \
  clickhouse-client \
  --user "$GRAFANA_CLICKHOUSE_USER" \
  --password "$GRAFANA_CLICKHOUSE_PASSWORD" \
  --query "
SELECT
    currentUser() AS current_user,
    count() AS active_order_lines
FROM northwind_dw.fact_orders FINAL
WHERE is_deleted = 0;
"

echo "Grafana ClickHouse reader configured successfully."
