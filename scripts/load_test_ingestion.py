#!/usr/bin/env python3
"""
High-Throughput Asynchronous ClickHouse Event Ingestion Load Test Engine
Generates and streams JSONEachRow telemetry batches with microsecond latency metrics.
"""

import sys
import json
import time
import uuid
import random
import asyncio
import urllib.request
from datetime import datetime, timezone

TENANTS = ["enterprise-corp", "fintech-global", "cloud-retail", "saas-platform"]
EVENT_TYPES = ["api_request", "db_query", "cache_miss", "auth_attempt", "queue_publish"]
STATUS_CODES = [200, 200, 200, 201, 204, 400, 401, 403, 500, 503]

def generate_event():
    return {
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "device_id": str(uuid.uuid4()),
        "tenant_id": random.choice(TENANTS),
        "event_type": random.choice(EVENT_TYPES),
        "environment": "production",
        "latency_ms": round(random.expovariate(1 / 25.0), 2),
        "status_code": random.choice(STATUS_CODES),
        "payload": json.dumps({"client_version": "v2.4.1", "zone": "us-east-1a", "cpu_pct": random.randint(10, 95)})
    }

async def send_batch_http(url, batch_data):
    """Sends a formatted batch of JSONEachRow records to ClickHouse HTTP endpoint."""
    loop = asyncio.get_event_loop()
    payload_str = "\n".join(json.dumps(e) for e in batch_data) + "\n"
    data_bytes = payload_str.encode("utf-8")
    
    query_url = f"{url.rstrip('/')}/?query=INSERT+INTO+analytics.telemetry_events+FORMAT+JSONEachRow"
    
    def _do_post():
        req = urllib.request.Request(query_url, data=data_bytes, headers={"Content-Type": "application/json"})
        start_t = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                status = resp.status
                duration = time.perf_counter() - start_t
                return status, duration, len(data_bytes)
        except Exception as e:
            duration = time.perf_counter() - start_t
            return 500, duration, 0

    return await loop.run_in_executor(None, _do_post)

async def worker(worker_id, url, total_events, batch_size, latencies, results):
    batches = total_events // batch_size
    for b in range(batches):
        batch = [generate_event() for _ in range(batch_size)]
        status, duration, bytes_sent = await send_batch_http(url, batch)
        latencies.append(duration)
        if status in (200, 204):
            results["success_events"] += batch_size
            results["bytes_sent"] += bytes_sent
        else:
            results["failed_events"] += batch_size

async def run_load_test(url="http://localhost:8123", total_events=10000, batch_size=500, concurrency=4):
    print("=" * 65)
    print("      ClickHouse Asynchronous Ingestion Load Test Suite      ")
    print("=" * 65)
    print(f"Target URL:    {url}")
    print(f"Total Events:  {total_events}")
    print(f"Batch Size:    {batch_size}")
    print(f"Concurrency:   {concurrency} async workers")
    print("-" * 65)

    events_per_worker = total_events // concurrency
    latencies = []
    results = {"success_events": 0, "failed_events": 0, "bytes_sent": 0}

    start_time = time.perf_counter()
    tasks = [
        worker(w, url, events_per_worker, batch_size, latencies, results)
        for w in range(concurrency)
    ]
    await asyncio.gather(*tasks)
    total_time = time.perf_counter() - start_time

    if not latencies:
        print("[WARN] No batches were executed.")
        return

    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.50)] * 1000
    p95 = latencies[int(len(latencies) * 0.95)] * 1000
    p99 = latencies[int(len(latencies) * 0.99)] * 1000
    throughput = results["success_events"] / total_time if total_time > 0 else 0
    mb_throughput = (results["bytes_sent"] / (1024 * 1024)) / total_time if total_time > 0 else 0

    print("📊 Ingestion Benchmark Results:")
    print(f"  • Total Time:         {total_time:.2f} seconds")
    print(f"  • Ingested Events:    {results['success_events']:,} records")
    print(f"  • Failed Events:      {results['failed_events']:,} records")
    print(f"  • Event Throughput:   {throughput:,.2f} events/sec")
    print(f"  • Data Throughput:    {mb_throughput:.2f} MB/sec")
    print(f"  • Batch Latency P50:  {p50:.2f} ms")
    print(f"  • Batch Latency P95:  {p95:.2f} ms")
    print(f"  • Batch Latency P99:  {p99:.2f} ms")
    print("=" * 65)

def main():
    target_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8123"
    events = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
    asyncio.run(run_load_test(url=target_url, total_events=events, batch_size=250, concurrency=4))

if __name__ == "__main__":
    main()
