#!/usr/bin/env bash

set -Eeuo pipefail

PROJECT_ROOT="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.." &&
  pwd
)"

cd "$PROJECT_ROOT"

if [[ ! -f .env ]]; then
  echo "FAIL .env is missing"
  exit 1
fi

set -a
source .env
set +a

failures=0

pass() {
  printf 'PASS %s\n' "$1"
}

fail() {
  printf 'FAIL %s\n' "$1" >&2
  failures=$((failures + 1))
}

check_command() {
  if command -v "$1" >/dev/null 2>&1; then
    pass "command=$1"
  else
    fail "missing command=$1"
  fi
}

check_command docker
check_command curl
check_command python3

if docker compose config --quiet; then
  pass "compose configuration"
else
  fail "compose configuration"
fi

expected_services=(
  sqlserver
  postgres
  clickhouse
  kafka
  connect
  spark
  airflow-webserver
  airflow-scheduler
  grafana
)

running_services="$(docker compose ps --status running --services)"

for service in "${expected_services[@]}"; do
  if grep -qx "$service" <<<"$running_services"; then
    pass "service=$service running"
  else
    fail "service=$service not running"
  fi
done

if docker compose exec -T sqlserver \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost \
  -U sa \
  -P "$MSSQL_SA_PASSWORD" \
  -C -b -Q "SET NOCOUNT ON; SELECT DB_NAME();" \
  >/dev/null; then
  pass "SQL Server query"
else
  fail "SQL Server query"
fi

if docker compose exec -T postgres \
  psql -v ON_ERROR_STOP=1 \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -tAc "SELECT COUNT(*) FROM staging.cdc_events;" \
  >/dev/null; then
  pass "PostgreSQL staging query"
else
  fail "PostgreSQL staging query"
fi

pending_events="$(
  docker compose exec -T postgres \
    psql -v ON_ERROR_STOP=1 \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -tAc "SELECT pending_events FROM staging.v_incremental_pipeline_status;" \
    | tr -d '[:space:]'
)"

if [[ "$pending_events" == "0" ]]; then
  pass "incremental pending_events=0"
else
  fail "incremental pending_events=${pending_events:-unknown}"
fi

clickhouse_validation="$(
  docker compose exec -T clickhouse \
    clickhouse-client --multiquery \
    < sql/clickhouse/03_validate_incremental_dw.sql
)"

if grep -q "INCREMENTAL DW VALIDATION PASSED" \
  <<<"$clickhouse_validation"; then
  pass "ClickHouse incremental validation"
else
  fail "ClickHouse incremental validation"
fi

if docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list >/dev/null; then
  pass "Kafka broker query"
else
  fail "Kafka broker query"
fi

connect_url="http://localhost:${KAFKA_CONNECT_PORT}"

if connectors_json="$(curl -fsS "${connect_url}/connectors")"; then
  pass "Kafka Connect API"
else
  connectors_json="[]"
  fail "Kafka Connect API"
fi

if CONNECT_URL="$connect_url" CONNECTORS_JSON="$connectors_json" \
  python3 - <<'PY'
import json
import os
import sys
import urllib.request

base_url = os.environ["CONNECT_URL"]
connectors = json.loads(os.environ["CONNECTORS_JSON"])

if not connectors:
    print("No connectors registered", file=sys.stderr)
    raise SystemExit(1)

for name in connectors:
    with urllib.request.urlopen(f"{base_url}/connectors/{name}/status") as response:
        status = json.load(response)

    if status["connector"]["state"] != "RUNNING":
        raise SystemExit(f"Connector {name} is not RUNNING")

    for task in status.get("tasks", []):
        if task["state"] != "RUNNING":
            raise SystemExit(f"Connector {name} task {task['id']} is not RUNNING")

    print(f"connector={name} state=RUNNING")
PY
then
  pass "Debezium connector state"
else
  fail "Debezium connector state"
fi

if docker compose exec -T spark python3 - <<'PYTHON'
from pathlib import Path

paths = [
    Path("/opt/northwind/jobs/kafka_to_postgres.py"),
    Path("/opt/northwind/jobs/initial_load_clickhouse.py"),
    Path("/opt/northwind/jobs/incremental_load_clickhouse.py"),
]

for path in paths:
    source = path.read_text(encoding="utf-8")
    compile(source, str(path), "exec")
    print(f"syntax_valid={path.name}")
PYTHON
then
  pass "Spark job syntax"
else
  fail "Spark job syntax"
fi

airflow_health_url="http://localhost:${AIRFLOW_PORT}/health"

if airflow_health="$(curl -fsS "$airflow_health_url")" && \
  AIRFLOW_HEALTH="$airflow_health" python3 - <<'PY'
import json
import os

health = json.loads(os.environ["AIRFLOW_HEALTH"])
assert health["metadatabase"]["status"] == "healthy"
assert health["scheduler"]["status"] == "healthy"
PY
then
  pass "Airflow health"
else
  fail "Airflow health"
fi

if docker compose exec -T airflow-scheduler airflow dags list \
  | grep -q "northwind_incremental_pipeline"; then
  pass "Airflow Northwind DAG"
else
  fail "Airflow Northwind DAG"
fi

if grafana_health="$(curl -fsS "http://localhost:${GRAFANA_PORT}/api/health")" && \
  GRAFANA_HEALTH="$grafana_health" python3 - <<'PY'
import json
import os

health = json.loads(os.environ["GRAFANA_HEALTH"])
assert health["database"] == "ok"
PY
then
  pass "Grafana health"
else
  fail "Grafana health"
fi

if datasource_health="$(
  curl -fsS \
    -u "${GRAFANA_ADMIN_USER}:${GRAFANA_ADMIN_PASSWORD}" \
    "http://localhost:${GRAFANA_PORT}/api/datasources/uid/northwind-clickhouse/health"
)" && DATASOURCE_HEALTH="$datasource_health" python3 - <<'PY'
import json
import os

health = json.loads(os.environ["DATASOURCE_HEALTH"])
assert health["status"] == "OK"
PY
then
  pass "Grafana ClickHouse datasource"
else
  fail "Grafana ClickHouse datasource"
fi

if dashboard_json="$(
  curl -fsS \
    -u "${GRAFANA_ADMIN_USER}:${GRAFANA_ADMIN_PASSWORD}" \
    "http://localhost:${GRAFANA_PORT}/api/dashboards/uid/northwind-realtime-overview"
)" && DASHBOARD_JSON="$dashboard_json" python3 - <<'PY'
import json
import os

payload = json.loads(os.environ["DASHBOARD_JSON"])
dashboard = payload["dashboard"]
assert dashboard["uid"] == "northwind-realtime-overview"
panels = dashboard["panels"]
panel_titles = {
    panel.get("title")
    for panel in panels
}

required_titles = {
    "Net Revenue",
    "Distinct Orders",
    "Average Order Value",
    "Units Sold",
    "Average Discount",
    "Monthly Revenue Trend",
    "Top Markets by Revenue",
    "Revenue Mix by Category",
    "Monthly Order Volume",
    "Top Products",
    "Top Customers",
    "Recent Orders",
    "Warehouse Row Health",
}

missing_titles = required_titles - panel_titles

variables = {
    item["name"]
    for item in dashboard["templating"]["list"]
}

required_variables = {
    "year",
    "country",
    "category",
}

assert dashboard["title"] == (
    "Northwind Real-Time Commerce Intelligence"
)
assert not missing_titles, (
    f"Missing dashboard panels: {sorted(missing_titles)}"
)
assert required_variables <= variables, (
    f"Missing variables: "
    f"{sorted(required_variables - variables)}"
)

print(
    f"dashboard_panels={len(panels)} "
    f"dashboard_variables={len(variables)}"
)
PY
then
  pass "Grafana dashboard structure"
else
  fail "Grafana dashboard"
fi

if (( failures > 0 )); then
  printf '\nPLATFORM VALIDATION FAILED failures=%d\n' "$failures" >&2
  exit 1
fi

printf '\nPLATFORM VALIDATION PASSED\n'
