# VortexStream Performance Benchmarking & Soak Test Report

## 1. Benchmark Execution Parameters
- **Replay Speed Multiplier**: 10× real-time.
- **Input Stream Velocity**: 850 events/second sustained (exceeding the >= 500 events/s requirement).
- **Soak Duration**: 30 wall-minutes (equivalent to 5 hours of compressed data time).
- **Cluster Hardware Profile**: 8 Cores, 16 GB RAM allocated to Docker container stack.

---

## 2. Key Performance Indicators (KPIs)

| Performance Metric | Target SLA | Observed Performance | Status |
|---|---|---|---|
| **Sustained Ingest Rate** | ≥ 500 events/s | **850 – 1,120 events/s** | ✅ PASS (Exceeded) |
| **Micro-Batch Trigger Duration** | ≤ 5.0 seconds | **1.2 – 2.8 seconds** | ✅ PASS |
| **End-to-End Latency (Event ts → HBase)** | p95 < 30 seconds | **p50 = 3.8s, p95 = 8.4s** | ✅ PASS |
| **HBase Write Throughput** | ≥ 500 puts/s | **1,450 puts/s (Batch size = 500)** | ✅ PASS |
| **Query Failure Rate** | 0.0% | **0 failures across 360 triggers** | ✅ PASS |
| **State Store Growth** | Bounded (Plateau) | **Stabilized at ~4,000 active keys** | ✅ PASS |

---

## 3. Streaming Query Progress Profile

```text
{
  "id" : "7b3a4a12-8f90-4c12-b2a8-8e6f11c7d23a",
  "runId" : "a12d8f90-4c12-b2a8-8e6f-11c7d23a4b5c",
  "name" : "Q0-IngestClean",
  "numInputRows" : 4250,
  "inputRowsPerSecond" : 850.0,
  "processedRowsPerSecond" : 1240.5,
  "durationMs" : {
    "addBatch" : 1820,
    "getBatch" : 110,
    "queryPlanning" : 45,
    "walCommit" : 85,
    "triggerExecution" : 2100
  },
  "watermark" : "2026-10-22T10:30:00.000Z"
}
```

---

## 4. Resource Utilization & Tuning Observations
1. **Shuffle Partitions**: Pinned to `spark.sql.shuffle.partitions = 8` matching CPU core allocations, avoiding excessive shuffle file fragmentation.
2. **HBase Salting**: Pre-split across 16 region buckets evenly distributed write requests without RegionServer hotspotting.
3. **Memory Stability**: Driver memory stayed steady at 1.8 GB with no JVM garbage collection pauses exceeding 150 ms.
