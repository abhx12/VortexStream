# VortexStream Automated Verification Report (V1-V8)

**Overall Status**: `PASS`

| Check | Description | Status | Details |
|---|---|---|---|
| **V1** | Kafka Bronze Offsets vs Manifest Emitted | ✅ PASS | Emitted: 1000, Verified offset match: True |
| **V2** | Record Conservation Equation | ✅ PASS | 1000 emitted == 1000 silver + 0 quarantine + 0 dups + 0 late_dropped (Sum: 1000) |
| **V3** | Silver Uniqueness and Schema Validity | ✅ PASS | Duplicate IDs: 0, Rule Violations: 0 |
| **V4** | Session Facts Lifecycle Completeness | ✅ PASS | Terminal facts identified: 2 |
| **V5** | Streaming vs Batch Aggregates Equivalence | ✅ PASS | Additive columns exact (0.0% variance); distinct session HLL within tolerance (1.8% <= 5%) |
| **V6** | HBase Hot-Store vs Warehouse Reconciliation | ✅ PASS | Row keys match exactly across HBase qoe_metrics and DuckDB gold_qoe_5m |
| **V7** | Alert Delivery Consistency | ✅ PASS | Equal alert_id sets across HBase alerts, Kafka topic, and warehouse Parquet |
| **V8** | Standardization Coverage | ✅ PASS | Unknown rate: 0.000% (<= 0.1% target for clean telemetry) |
