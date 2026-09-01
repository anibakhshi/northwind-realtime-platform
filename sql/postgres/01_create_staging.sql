CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.cdc_events
(
    event_id             BIGSERIAL PRIMARY KEY,
    kafka_topic          TEXT NOT NULL,
    kafka_partition      INTEGER NOT NULL,
    kafka_offset         BIGINT NOT NULL,
    kafka_timestamp      TIMESTAMPTZ,
    event_key            JSONB,
    before_data          JSONB,
    after_data           JSONB,
    operation            VARCHAR(1),
    source_database      TEXT,
    source_schema        TEXT,
    source_table         TEXT,
    source_change_lsn    TEXT,
    source_commit_lsn    TEXT,
    source_timestamp     TIMESTAMPTZ,
    is_snapshot          BOOLEAN NOT NULL DEFAULT FALSE,
    processed            BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at         TIMESTAMPTZ,
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_cdc_kafka_position
        UNIQUE (kafka_topic, kafka_partition, kafka_offset),

    CONSTRAINT chk_cdc_operation
        CHECK (operation IN ('r', 'c', 'u', 'd', 't') OR operation IS NULL)
);

CREATE INDEX IF NOT EXISTS idx_cdc_events_unprocessed
    ON staging.cdc_events (processed, event_id)
    WHERE processed = FALSE;

CREATE INDEX IF NOT EXISTS idx_cdc_events_source
    ON staging.cdc_events
    (
        source_database,
        source_schema,
        source_table,
        event_id
    );

CREATE INDEX IF NOT EXISTS idx_cdc_events_ingested_at
    ON staging.cdc_events (ingested_at);

CREATE TABLE IF NOT EXISTS staging.pipeline_runs
(
    run_id              BIGSERIAL PRIMARY KEY,
    pipeline_name       TEXT NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at         TIMESTAMPTZ,
    status              TEXT NOT NULL DEFAULT 'RUNNING',
    records_read        BIGINT NOT NULL DEFAULT 0,
    records_written     BIGINT NOT NULL DEFAULT 0,
    records_failed      BIGINT NOT NULL DEFAULT 0,
    error_message       TEXT,

    CONSTRAINT chk_pipeline_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED'))
);

CREATE TABLE IF NOT EXISTS staging.pipeline_errors
(
    error_id            BIGSERIAL PRIMARY KEY,
    pipeline_name       TEXT NOT NULL,
    kafka_topic         TEXT,
    kafka_partition     INTEGER,
    kafka_offset        BIGINT,
    error_type          TEXT,
    error_message       TEXT NOT NULL,
    raw_event           JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW staging.v_cdc_pending_summary AS
SELECT
    source_table,
    operation,
    COUNT(*) AS event_count,
    MIN(ingested_at) AS oldest_event_at,
    MAX(ingested_at) AS newest_event_at
FROM staging.cdc_events
WHERE processed = FALSE
GROUP BY source_table, operation;
