BEGIN;

CREATE TABLE IF NOT EXISTS staging.pipeline_watermarks
(
    pipeline_name text PRIMARY KEY,
    last_event_id bigint NOT NULL,
    initialized_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_pipeline_watermark_nonnegative
        CHECK (last_event_id >= 0)
);

INSERT INTO staging.pipeline_watermarks
(
    pipeline_name,
    last_event_id
)
SELECT
    'postgres_to_clickhouse_incremental',
    COALESCE(MAX(event_id), 0)
FROM staging.cdc_events
ON CONFLICT (pipeline_name) DO NOTHING;

UPDATE staging.cdc_events AS events
SET
    processed = true,
    processed_at = CURRENT_TIMESTAMP
FROM staging.pipeline_watermarks AS watermark
WHERE watermark.pipeline_name = 'postgres_to_clickhouse_incremental'
  AND events.event_id <= watermark.last_event_id
  AND events.processed = false;

CREATE OR REPLACE VIEW staging.v_incremental_pipeline_status AS
SELECT
    watermark.pipeline_name,
    watermark.last_event_id,
    COALESCE(
        (
            SELECT MAX(events.event_id)
            FROM staging.cdc_events AS events
        ),
        0
    ) AS latest_event_id,
    (
        SELECT COUNT(*)
        FROM staging.cdc_events AS events
        WHERE events.event_id > watermark.last_event_id
          AND events.processed = false
    ) AS pending_events,
    watermark.initialized_at,
    watermark.updated_at
FROM staging.pipeline_watermarks AS watermark;

COMMIT;
