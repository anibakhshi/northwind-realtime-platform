import os

import docker
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


SPARK_CONTAINER = os.environ["SPARK_CONTAINER_NAME"]
CLICKHOUSE_CONTAINER = os.environ["CLICKHOUSE_CONTAINER_NAME"]


def run_in_container(container_name, command):
    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
        result = container.exec_run(
            command,
            stdout=True,
            stderr=True,
            demux=True,
        )
        stdout, stderr = result.output
        if stdout:
            print(stdout.decode("utf-8", errors="replace"))
        if stderr:
            print(stderr.decode("utf-8", errors="replace"))
        if result.exit_code != 0:
            raise RuntimeError(
                f"Container command failed with exit code {result.exit_code}"
            )
    finally:
        client.close()


def run_incremental_load():
    run_in_container(
        SPARK_CONTAINER,
        [
            "python3",
            "/opt/northwind/jobs/incremental_load_clickhouse.py",
        ],
    )


def run_incremental_validation():
    run_in_container(
        CLICKHOUSE_CONTAINER,
        [
            "bash",
            "-lc",
            "clickhouse-client --multiquery "
            "< /docker-entrypoint-initdb.d/03_validate_incremental_dw.sql",
        ],
    )


def run_full_refresh():
    run_in_container(
        SPARK_CONTAINER,
        [
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[2]",
            "--packages",
            "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11",
            "--conf",
            "spark.jars.ivy=/tmp/.ivy2",
            "--conf",
            "spark.sql.session.timeZone=UTC",
            "/opt/northwind/jobs/initial_load_clickhouse.py",
        ],
    )


def run_initial_validation():
    run_in_container(
        CLICKHOUSE_CONTAINER,
        [
            "bash",
            "-lc",
            "clickhouse-client --multiquery "
            "< /docker-entrypoint-initdb.d/02_validate_initial_load.sql",
        ],
    )


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="northwind_incremental_pipeline",
    description="Incrementally load Northwind CDC data into ClickHouse",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["northwind", "cdc", "clickhouse"],
) as incremental_dag:
    incremental_load = PythonOperator(
        task_id="incremental_load",
        python_callable=run_incremental_load,
    )

    validate_incremental_dw = PythonOperator(
        task_id="validate_incremental_dw",
        python_callable=run_incremental_validation,
    )

    incremental_load >> validate_incremental_dw


with DAG(
    dag_id="northwind_full_refresh",
    description="Manually rebuild the Northwind ClickHouse warehouse",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["northwind", "full-refresh", "clickhouse"],
) as full_refresh_dag:
    full_refresh = PythonOperator(
        task_id="full_refresh",
        python_callable=run_full_refresh,
    )

    validate_initial_load = PythonOperator(
        task_id="validate_initial_load",
        python_callable=run_initial_validation,
    )

    full_refresh >> validate_initial_load
