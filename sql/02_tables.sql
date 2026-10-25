-- =============================================================================
-- Tables. Timestamps are TIMESTAMP WITHOUT TIME ZONE on purpose: TLC publishes
-- NYC local wall-clock times without an offset. The pipeline never converts
-- them (see docs/data_quality_rules.md, "Timestamp policy").
-- =============================================================================

-- ---------------------------------------------------------------- serving ---
CREATE TABLE serving.taxi_zones (
    location_id  INTEGER PRIMARY KEY,
    borough      TEXT,
    zone         TEXT,
    service_zone TEXT
);

CREATE TABLE serving.hourly_zone_metrics (
    pickup_hour         TIMESTAMP        NOT NULL,   -- hour bucket, NYC wall-clock
    pu_location_id      INTEGER          NOT NULL,   -- TLC taxi zone of pickup
    trip_count          BIGINT           NOT NULL CHECK (trip_count > 0),
    avg_fare            DOUBLE PRECISION,            -- mean of fare_amount (metered fare only)
    p50_fare            DOUBLE PRECISION,            -- approximate median (percentile_approx)
    p90_fare            DOUBLE PRECISION,            -- approximate 90th percentile
    sum_fare_amount     DOUBLE PRECISION,            -- sum of fare_amount (excl. tips/tolls/surcharges)
    sum_total_amount    DOUBLE PRECISION,            -- sum of total_amount (all charges, excl. cash tips)
    avg_trip_distance   DOUBLE PRECISION,            -- miles, taximeter-reported
    avg_duration_min    DOUBLE PRECISION,            -- dropoff - pickup, minutes
    avg_passenger_count DOUBLE PRECISION,            -- driver-entered; nulls ignored
    batch_id            TEXT             NOT NULL,   -- lineage: batch that produced the row
    loaded_at           TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (pickup_hour, pu_location_id)
);
-- Per-zone time-series access (the dominant ML access pattern); the PK already
-- serves "all zones for an hour".
CREATE INDEX idx_hzm_zone_hour ON serving.hourly_zone_metrics (pu_location_id, pickup_hour);

CREATE TABLE serving.forecast_features (
    pickup_hour                  TIMESTAMP        NOT NULL,
    pu_location_id               INTEGER          NOT NULL,
    -- targets (never to be used as inputs for the same row)
    target_trip_count            BIGINT           NOT NULL,
    target_avg_fare              DOUBLE PRECISION,           -- NULL when no trips that hour
    -- calendar features (known in advance)
    hour_of_day                  INTEGER          NOT NULL,  -- 0-23
    day_of_week                  INTEGER          NOT NULL,  -- ISO: 1=Mon ... 7=Sun
    month                        INTEGER          NOT NULL,  -- 1-12
    is_weekend                   BOOLEAN          NOT NULL,
    -- history-only features (strictly earlier hours => no target leakage)
    lag_1h_trip_count            BIGINT,
    lag_24h_trip_count           BIGINT,
    lag_168h_trip_count          BIGINT,
    rolling_mean_24h_trip_count  DOUBLE PRECISION,           -- mean of the 24 PRECEDING hours
    lag_1h_avg_fare              DOUBLE PRECISION,
    batch_id                     TEXT             NOT NULL,
    loaded_at                    TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (pickup_hour, pu_location_id)
);
CREATE INDEX idx_ff_zone_hour ON serving.forecast_features (pu_location_id, pickup_hour);

CREATE VIEW serving.v_hourly_zone_metrics_named AS
SELECT m.*, z.borough, z.zone, z.service_zone
FROM serving.hourly_zone_metrics m
LEFT JOIN serving.taxi_zones z ON z.location_id = m.pu_location_id;

-- -------------------------------------------------------------------- ops ---
CREATE TABLE ops.batch_runs (
    batch_id             TEXT PRIMARY KEY,
    months               TEXT[]           NOT NULL,
    window_start         TIMESTAMP        NOT NULL,
    window_end           TIMESTAMP        NOT NULL,          -- exclusive
    source_rows          BIGINT           NOT NULL,
    accepted_rows        BIGINT           NOT NULL,
    rejected_rows        BIGINT           NOT NULL,
    reject_breakdown     JSONB            NOT NULL,
    hourly_rows          BIGINT           NOT NULL,
    feature_rows         BIGINT           NOT NULL,
    input_bytes          BIGINT,
    output_bytes         BIGINT,
    processing_s         DOUBLE PRECISION,
    publish_s            DOUBLE PRECISION,
    throughput_rows_per_s DOUBLE PRECISION,
    config               JSONB,
    status               TEXT             NOT NULL,
    finished_at          TIMESTAMPTZ      NOT NULL DEFAULT now(),
    CHECK (source_rows = accepted_rows + rejected_rows)
);

-- ---------------------------------------------------------------- staging ---
-- Truncated and refilled by Spark on every run; published into `serving` inside
-- one transaction. Column names/order mirror the Spark output.
CREATE TABLE staging.taxi_zones (
    location_id  INTEGER,
    borough      TEXT,
    zone         TEXT,
    service_zone TEXT
);

CREATE TABLE staging.hourly_zone_metrics (
    pickup_hour         TIMESTAMP,
    pu_location_id      INTEGER,
    trip_count          BIGINT,
    avg_fare            DOUBLE PRECISION,
    p50_fare            DOUBLE PRECISION,
    p90_fare            DOUBLE PRECISION,
    sum_fare_amount     DOUBLE PRECISION,
    sum_total_amount    DOUBLE PRECISION,
    avg_trip_distance   DOUBLE PRECISION,
    avg_duration_min    DOUBLE PRECISION,
    avg_passenger_count DOUBLE PRECISION
);

CREATE TABLE staging.forecast_features (
    pickup_hour                  TIMESTAMP,
    pu_location_id               INTEGER,
    target_trip_count            BIGINT,
    target_avg_fare              DOUBLE PRECISION,
    hour_of_day                  INTEGER,
    day_of_week                  INTEGER,
    month                        INTEGER,
    is_weekend                   BOOLEAN,
    lag_1h_trip_count            BIGINT,
    lag_24h_trip_count           BIGINT,
    lag_168h_trip_count          BIGINT,
    rolling_mean_24h_trip_count  DOUBLE PRECISION,
    lag_1h_avg_fare              DOUBLE PRECISION
);
