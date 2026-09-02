# Operations Runbook

## 1. Load configuration

Run commands from the repository root:

```bash
set -a
source .env
set +a
```

Validate the merged Compose configuration:

```bash
docker compose config --quiet
docker compose config --services
```

## 2. Start and inspect services

```bash
docker compose up -d
docker compose ps
```

Follow logs for a service:

```bash
docker compose logs --tail=200 --follow SERVICE_NAME
```

## 3. SQL Server initialization

SQL scripts are ordered by filename under `sql/sqlserver/`.

Run a script inside SQL Server:

```bash
docker compose exec sqlserver \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost \
  -U sa \
  -P "$MSSQL_SA_PASSWORD" \
  -C \
  -b \
  -i /var/opt/mssql/scripts/SCRIPT_NAME.sql
```

The normal initialization order is:

1. Restore the reference DW.
2. Create the operational Northwind database.
3. Install Northwind objects.
4. Enable CDC.
5. Create the Debezium login.
6. Run the CDC smoke test.

## 4. Register Debezium

```bash
./scripts/register-debezium.sh
```

Inspect connector status:

```bash
curl -fsS http://localhost:${KAFKA_CONNECT_PORT}/connectors
curl -fsS http://localhost:${KAFKA_CONNECT_PORT}/connectors/northwind-sqlserver-source/status
```

The connector and all tasks must report `RUNNING`.

## 5. PostgreSQL staging

Apply or reapply a staging script:

```bash
docker compose exec postgres \
  psql \
  -v ON_ERROR_STOP=1 \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -f /docker-entrypoint-initdb.d/SCRIPT_NAME.sql
```

Inspect the incremental watermark:

```bash
docker compose exec postgres \
  psql -P pager=off \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -c "SELECT * FROM staging.v_incremental_pipeline_status;"
```

## 6. ClickHouse warehouse

Apply schema SQL:

```bash
docker compose exec -T clickhouse \
  clickhouse-client --multiquery \
  < sql/clickhouse/01_create_dw.sql
```

Validate the initial warehouse:

```bash
docker compose exec -T clickhouse \
  clickhouse-client --multiquery \
  < sql/clickhouse/02_validate_initial_load.sql
```

Validate incremental state:

```bash
docker compose exec -T clickhouse \
  clickhouse-client --multiquery \
  < sql/clickhouse/03_validate_incremental_dw.sql
```

## 7. Spark jobs

Check Python syntax:

```bash
docker compose exec -T spark python3 -m py_compile \
  /opt/northwind/jobs/kafka_to_postgres.py \
  /opt/northwind/jobs/initial_load_clickhouse.py \
  /opt/northwind/jobs/incremental_load_clickhouse.py
```

Run the incremental loader:

```bash
docker compose exec -T spark \
  python3 /opt/northwind/jobs/incremental_load_clickhouse.py
```

`NO PENDING EVENTS` is a successful idempotent result.

## 8. Airflow

Initialize metadata and the administrator:

```bash
docker compose up \
  --no-deps \
  --force-recreate \
  --exit-code-from airflow-init \
  airflow-init
```

Start Airflow:

```bash
docker compose up -d airflow-webserver airflow-scheduler
```

Check health and DAG imports:

```bash
curl -fsS http://localhost:${AIRFLOW_PORT}/health | python3 -m json.tool

docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Trigger an incremental run:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger northwind_incremental_pipeline
```

## 9. Grafana

Configure the read-only account:

```bash
./scripts/setup_grafana_reader.sh
```

Build and start Grafana:

```bash
docker compose build grafana
docker compose up -d grafana
```

Check the datasource:

```bash
curl -fsS \
  -u "${GRAFANA_ADMIN_USER}:${GRAFANA_ADMIN_PASSWORD}" \
  "http://localhost:${GRAFANA_PORT}/api/datasources/uid/northwind-clickhouse/health"
```

## 10. Platform validation

```bash
./scripts/validate_platform.sh
```

The script exits non-zero when a required service, connector, DAG, database check, or dashboard check fails.

## Recovery guidance

### CDC events are pending

1. Confirm Kafka Connect and Spark are running.
2. Inspect `staging.v_incremental_pipeline_status`.
3. Run the incremental loader manually.
4. Re-run incremental ClickHouse validation.

### Airflow cannot access Docker

Confirm `DOCKER_GID` matches the socket group:

```bash
stat -c '%g' /var/run/docker.sock
```

Recreate Airflow after changing `.env`:

```bash
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

### Grafana datasource fails

1. Run `./scripts/setup_grafana_reader.sh`.
2. Confirm the ClickHouse plugin exists inside the Grafana container.
3. Check `docker compose logs --tail=200 grafana`.
4. Test the datasource health endpoint.

### Safe shutdown

Stop containers while preserving named volumes:

```bash
docker compose down
```

Do not use `docker compose down -v` unless permanent database removal is explicitly intended.
