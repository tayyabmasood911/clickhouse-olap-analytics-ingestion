-- =====================================================================
-- ClickHouse Enterprise OLAP Telemetry Schema
-- Storage Engines: MergeTree, SummingMergeTree & Materialized Views
-- =====================================================================

CREATE DATABASE IF NOT EXISTS analytics;

-- 1. Base High-Volume Telemetry Table (MergeTree)
CREATE TABLE IF NOT EXISTS analytics.telemetry_events (
    event_time DateTime64(3, 'UTC') DEFAULT now64(3),
    event_date Date DEFAULT toDate(event_time),
    device_id UUID,
    tenant_id LowCardinality(String),
    event_type LowCardinality(String),
    environment LowCardinality(String) DEFAULT 'production',
    latency_ms Float32,
    status_code UInt16,
    payload String CODEC(ZSTD(3))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
PRIMARY KEY (tenant_id, event_type)
ORDER BY (tenant_id, event_type, toStartOfHour(event_time), device_id, event_time)
SETTINGS index_granularity = 8192,
         ttl_only_drop_parts = 1;

-- 2. Hourly Pre-Aggregated Rollup Table (SummingMergeTree)
CREATE TABLE IF NOT EXISTS analytics.telemetry_hourly_rollup (
    hourly_window DateTime,
    tenant_id LowCardinality(String),
    event_type LowCardinality(String),
    total_events UInt64,
    total_latency_ms Float64,
    error_count UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hourly_window)
PRIMARY KEY (tenant_id, event_type)
ORDER BY (tenant_id, event_type, hourly_window);

-- 3. Materialized View: Real-Time Stream Aggregation
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_telemetry_hourly_rollup
TO analytics.telemetry_hourly_rollup AS
SELECT
    toStartOfHour(event_time) AS hourly_window,
    tenant_id,
    event_type,
    count() AS total_events,
    sum(latency_ms) AS total_latency_ms,
    countIf(status_code >= 400) AS error_count
FROM analytics.telemetry_events
GROUP BY hourly_window, tenant_id, event_type;
