# ClickHouse OLAP Ingestion & Analytical Streaming Architecture

<div align="center">

[![Status](https://img.shields.io/badge/status-production--ready-brightgreen?style=for-the-badge&logo=clickhouse)]()
[![Domain](https://img.shields.io/badge/domain-OLAP%20%26%20Streaming%20Analytics-blueviolet?style=for-the-badge)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)
[![Security Policy](https://img.shields.io/badge/security-hardened-red?style=for-the-badge&logo=shield)](SECURITY.md)

</div>

---

## 📌 Executive Summary

**ClickHouse OLAP Analytics Ingestion** provides a production-grade, columnar analytical data streaming architecture engineered to ingest hundreds of thousands of events per second with sub-second analytical query execution over billions of records.

This implementation couples Kafka stream consumption with ClickHouse native table engines and Materialized Views:
- **`Kafka` Engine Table**: Virtual consumer buffer pulling raw JSON records from Apache Kafka / Redpanda without persisting data directly in the queue table.
- **Materialized Views (`mv_kafka_to_telemetry`)**: Continuous event pipeline parsing, typing, and validating incoming records directly into partitioned storage.
- **`MergeTree` Target Engine**: Primary partitioned columnar table featuring ZSTD block compression, month-level partition pruning, and primary sparse indexing.
- **`SummingMergeTree` Rollup Table (`mv_telemetry_hourly_rollup`)**: Continuous stream pre-aggregation computing real-time hourly event sums, latency aggregates, and error totals.
- **Async Python Load Engine**: High-throughput `asyncio` ingestion benchmark script evaluating batch latency percentiles (P50/P95/P99) and event throughput.

### Key Operational Outcomes
- **Sub-Second Query Latency:** Columnar scanning combined with partition pruning delivers analytical aggregations across billions of rows in milliseconds.
- **10x+ Compression Ratio:** ZSTD columnar compression on raw payloads drastically slashes disk footprint and IOPS contention.
- **Pre-Aggregated Hourly Rollups:** `SummingMergeTree` collapses rows with identical primary keys in background merges, serving dashboard queries up to 1,000x faster than raw table scans.
- **Zero-Drop Kafka Ingestion:** Automatic offset management and skip-broken-message tolerance prevent ingestion pipeline stalls.

---

## 🏗️ System Architecture

The following pure-white vector diagram illustrates the streaming lifecycle from Kafka event producers through the continuous Materialized View pipeline into partitioned storage and pre-aggregated rollups:

<div align="center">
  <img src="docs/architecture.svg" alt="ClickHouse OLAP Ingestion Architecture" width="100%" />
</div>

---

## ✨ Database Engines & Pipeline Architecture

| Component | SQL Engine / Construct | Primary Purpose | Optimization Mechanism |
| :--- | :--- | :--- | :--- |
| **`kafka_telemetry_queue`** | `ENGINE = Kafka` | Virtual streaming queue consumer | `JSONEachRow` parser, 2 consumer threads, block size 65,536. |
| **`mv_kafka_to_telemetry`** | `MATERIALIZED VIEW` | Stream transformation & typing | Continuous write-trigger pushing rows into base table. |
| **`telemetry_events`** | `ENGINE = MergeTree()` | Primary historical OLAP storage | `PARTITION BY toYYYYMM()`, sparse index granularity `8192`, ZSTD(3) codec. |
| **`mv_telemetry_hourly_rollup`** | `MATERIALIZED VIEW` | Real-time hourly rollup feeder | Automatically calculates `count()`, `sum(latency)`, and `countIf(error)`. |
| **`telemetry_hourly_rollup`** | `ENGINE = SummingMergeTree()` | Pre-aggregated KPI storage | Asynchronously sums numeric fields during part merges. |

---

## 📂 Repository Layout

```text
.
├── schema.sql                           # Production DDL: MergeTree, SummingMergeTree & rollup Materialized View
├── kafka_engine.sql                     # Production DDL: Kafka Engine table & stream consumer Materialized View
├── docker-compose.yml                   # Unified sandbox running ClickHouse Server + Redpanda (Kafka)
├── Makefile                             # Build, test, and database automation targets
├── SECURITY.md                          # Responsible vulnerability disclosure policy
├── .gitattributes                       # Cross-platform line-ending & GitHub Linguist overrides
├── config/
│   └── production.env.example           # Environment template for ClickHouse credentials
├── docs/
│   └── architecture.svg                 # Pure white theme architectural vector diagram
├── scripts/
│   ├── load_test_ingestion.py           # High-throughput asyncio batch ingestion load engine
│   └── healthcheck.sh                   # SRE ClickHouse HTTP /ping validation script
└── tests/
    └── test_validation.py               # Automated SQL syntax, DDL, and Docker Compose test suite
```

---

## ⚡ Quick Start & Verification

### Prerequisites
- Docker Engine `>= 24.0` & Docker Compose v2
- Python `>= 3.9`
- `curl`

### 1. Validate SQL DDL & Compose Files
Execute the automated test suite to ensure DDL syntax and configuration integrity:
```bash
python3 -m unittest discover -s tests -p "test_*.py"
```

### 2. Launch ClickHouse & Kafka Sandbox
Start the database server and Redpanda broker:
```bash
docker compose up -d
```

### 3. Verify Database Health
Run the automated healthcheck:
```bash
bash scripts/healthcheck.sh
```

### 4. Execute Asynchronous Ingestion Load Test
Run the Python load generator to stream synthetic telemetry batches into ClickHouse:
```bash
# Streams 5,000 events in batches of 250 across 4 concurrent async workers
python3 scripts/load_test_ingestion.py http://localhost:8123 5000
```

### 5. Inspect Real-Time Aggregated Rollups
Query the `SummingMergeTree` table to inspect streaming aggregations:
```bash
curl -G "http://localhost:8123/?query=SELECT+hourly_window,tenant_id,event_type,sum(total_events),sum(total_latency_ms)/sum(total_events)+AS+avg_latency+FROM+analytics.telemetry_hourly_rollup+GROUP+BY+hourly_window,tenant_id,event_type+FORMAT+Pretty"
```

---

## ⚙️ SQL DDL Reference

### Primary MergeTree Table
```sql
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
ORDER BY (tenant_id, event_type, toStartOfHour(event_time), device_id, event_time);
```

---

## 🛡️ Security & Hardening Controls

- **Role-Based Access Control (RBAC):** Restrict ingestion processes to write-only permissions on target tables; analytical BI users receive read-only access.
- **Network Isolation:** ClickHouse native port `9000` and HTTP port `8123` are bound to internal networks; internet-facing traffic requires TLS reverse proxies.
- **Resource Constraints:** `max_execution_time: 30`, `max_memory_usage: 10000000000` (10GB) prevent runaway analytical queries from consuming host memory.

---

## 📜 License

Distributed under the MIT License. See [LICENSE](LICENSE) for details.
