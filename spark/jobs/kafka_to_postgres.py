import json
import os
from datetime import datetime, timezone
from typing import Iterator

import psycopg2
from psycopg2.extras import Json, execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


PIPELINE_NAME = "kafka_to_postgres_staging"

KAFKA_TOPICS = [
    "northwind.Northwind.dbo.Customers",
    "northwind.Northwind.dbo.Products",
    "northwind.Northwind.dbo.Orders",
    "northwind.Northwind.dbo.Order_Details",
]


def postgres_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def optional_json(raw_value):
    if raw_value is None:
        return None

    try:
        return json.loads(raw_value)
    except json.JSONDecodeError:
        return {"raw": raw_value}


def source_datetime(source_ts_ms):
    if source_ts_ms is None:
        return None

    return datetime.fromtimestamp(
        source_ts_ms / 1000,
        tz=timezone.utc,
    )


def create_pipeline_run() -> int:
    connection = postgres_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO staging.pipeline_runs
                (
                    pipeline_name,
                    status
                )
                VALUES
                (
                    %s,
                    'RUNNING'
                )
                RETURNING run_id
                """,
                (PIPELINE_NAME,),
            )

            run_id = cursor.fetchone()[0]

        connection.commit()
        return run_id
    finally:
        connection.close()


def finish_pipeline_run(
    run_id: int,
    status: str,
    records_read: int,
    records_written: int,
    error_message=None,
):
    connection = postgres_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE staging.pipeline_runs
                SET
                    finished_at = CURRENT_TIMESTAMP,
                    status = %s,
                    records_read = %s,
                    records_written = %s,
                    records_failed = %s,
                    error_message = %s
                WHERE run_id = %s
                """,
                (
                    status,
                    records_read,
                    records_written,
                    records_read if status == "FAILED" else 0,
                    error_message,
                    run_id,
                ),
            )

        connection.commit()
    finally:
        connection.close()


def write_partition(rows: Iterator):
    connection = postgres_connection()
    inserted_count = 0

    insert_sql = """
        INSERT INTO staging.cdc_events
        (
            kafka_topic,
            kafka_partition,
            kafka_offset,
            kafka_timestamp,
            event_key,
            before_data,
            after_data,
            operation,
            source_database,
            source_schema,
            source_table,
            source_change_lsn,
            source_commit_lsn,
            source_timestamp,
            is_snapshot
        )
        VALUES %s
        ON CONFLICT
        (
            kafka_topic,
            kafka_partition,
            kafka_offset
        )
        DO NOTHING
        RETURNING event_id
    """

    try:
        records = []

        for row in rows:
            records.append(
                (
                    row.kafka_topic,
                    row.kafka_partition,
                    row.kafka_offset,
                    row.kafka_timestamp,
                    Json(optional_json(row.event_key_raw))
                    if row.event_key_raw is not None
                    else None,
                    Json(optional_json(row.before_raw))
                    if row.before_raw is not None
                    else None,
                    Json(optional_json(row.after_raw))
                    if row.after_raw is not None
                    else None,
                    row.operation,
                    row.source_database,
                    row.source_schema,
                    row.source_table,
                    row.source_change_lsn,
                    row.source_commit_lsn,
                    source_datetime(row.source_ts_ms),
                    row.is_snapshot,
                )
            )

            if len(records) >= 500:
                with connection.cursor() as cursor:
                    inserted_rows = execute_values(
                        cursor,
                        insert_sql,
                        records,
                        page_size=500,
                        fetch=True,
                    )

                inserted_count += len(inserted_rows)
                records.clear()

        if records:
            with connection.cursor() as cursor:
                inserted_rows = execute_values(
                    cursor,
                    insert_sql,
                    records,
                    page_size=500,
                    fetch=True,
                )

            inserted_count += len(inserted_rows)

        connection.commit()
        yield inserted_count

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def process_batch(batch_df: DataFrame, batch_id: int):
    cached_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    records_read = 0
    records_written = 0
    run_id = None

    try:
        records_read = cached_df.count()

        if records_read == 0:
            return

        run_id = create_pipeline_run()

        partition_results = (
            cached_df.rdd
            .mapPartitions(write_partition)
            .collect()
        )

        records_written = sum(partition_results)

        finish_pipeline_run(
            run_id=run_id,
            status="SUCCESS",
            records_read=records_read,
            records_written=records_written,
        )

        print(
            f"Batch {batch_id}: "
            f"read={records_read}, "
            f"written={records_written}",
            flush=True,
        )

    except Exception as error:
        if run_id is not None:
            finish_pipeline_run(
                run_id=run_id,
                status="FAILED",
                records_read=records_read,
                records_written=records_written,
                error_message=str(error)[:4000],
            )

        raise

    finally:
        cached_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("NorthwindKafkaToPostgres")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS",
                "kafka:9092",
            ),
        )
        .option("subscribe", ",".join(KAFKA_TOPICS))
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "10000")
        .load()
    )

    raw_events = kafka_stream.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("event_key_raw"),
        F.col("value").cast("string").alias("event_value_raw"),
    )

    payload_json = F.coalesce(
        F.get_json_object(
            F.col("event_value_raw"),
            "$.payload",
        ),
        F.col("event_value_raw"),
    )

    snapshot_value = F.lower(
        F.get_json_object(
            payload_json,
            "$.source.snapshot",
        )
    )

    parsed_events = raw_events.select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "event_key_raw",
        F.get_json_object(
            payload_json,
            "$.before",
        ).alias("before_raw"),
        F.get_json_object(
            payload_json,
            "$.after",
        ).alias("after_raw"),
        F.get_json_object(
            payload_json,
            "$.op",
        ).alias("operation"),
        F.get_json_object(
            payload_json,
            "$.source.db",
        ).alias("source_database"),
        F.get_json_object(
            payload_json,
            "$.source.schema",
        ).alias("source_schema"),
        F.get_json_object(
            payload_json,
            "$.source.table",
        ).alias("source_table"),
        F.get_json_object(
            payload_json,
            "$.source.change_lsn",
        ).alias("source_change_lsn"),
        F.get_json_object(
            payload_json,
            "$.source.commit_lsn",
        ).alias("source_commit_lsn"),
        F.get_json_object(
            payload_json,
            "$.source.ts_ms",
        ).cast("long").alias("source_ts_ms"),
        snapshot_value.isin(
            "true",
            "last",
        ).alias("is_snapshot"),
    ).filter(
        F.col("operation").isin(
            "r",
            "c",
            "u",
            "d",
            "t",
        )
    )

    query = (
        parsed_events.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option(
            "checkpointLocation",
            "/opt/spark/checkpoints/kafka-to-postgres",
        )
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
