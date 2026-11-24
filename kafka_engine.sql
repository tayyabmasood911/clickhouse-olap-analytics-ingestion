-- =====================================================================
-- ClickHouse Kafka Engine & Ingestion Pipe DDL
-- Consumes JSONEachRow stream and materializes directly into MergeTree
-- =====================================================================

CREATE DATABASE IF NOT EXISTS analytics;

-- 1. Kafka Consumer Queue Engine Table
CREATE TABLE IF NOT EXISTS analytics.kafka_telemetry_queue (
    event_time String,
    device_id UUID,
    tenant_id String,
    event_type String,
    environment String,
    latency_ms Float32,
    status_code UInt16,
    payload String
) ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'telemetry_stream',
         kafka_group_name = 'clickhouse_analytics_consumer',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 2,
         kafka_max_block_size = 65536,
         kafka_skip_broken_messages = 10;

-- 2. Materialized View Pipeline: Kafka Queue -> Base MergeTree Table
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_kafka_to_telemetry
TO analytics.telemetry_events AS
SELECT
    parseDateTime64BestEffortOrNull(event_time, 3, 'UTC') AS event_time,
    toDate(event_time) AS event_date,
    device_id,
    tenant_id,
    event_type,
    environment,
    latency_ms,
    status_code,
    payload
FROM analytics.kafka_telemetry_queue
WHERE event_time IS NOT NULL;
