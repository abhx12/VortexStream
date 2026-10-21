# Real-Time OTT Video QoE & Buffer Analytics Engine

A production-grade, distributed streaming analytics engine for Over-The-Top (OTT) video streaming platforms (e.g., Netflix, Hotstar, YouTube). The engine ingests player telemetry and network metrics in real time, computes Quality of Experience (QoE) metrics, detects and alerts on degradations, maintains hot state in Apache HBase, archives history in a DuckDB/Parquet data warehouse, and predicts session churn/abandonment with Apache Spark MLlib.

## Architecture

- **Ingestion & Streaming**: Apache Kafka + Apache Spark Structured Streaming
- **Real Data Core**: Stanford Puffer open dataset (live-TV telemetry)
- **Fast Storage (Wide-Column)**: Apache HBase (Thrift interface, pre-split tables, salt buckets, reverse timestamp ordering)
- **Historical Warehouse**: DuckDB over partitioned Parquet
- **Analytics & ML**: Spark RDD / SQL / MLlib
- **Visualization**: Streamlit Real-Time Operations Dashboard

## Getting Started

Check environment readiness:
```bash
python scripts/doctor.py
# or
make doctor
```

Run Phase 0 tests:
```bash
make test-phase0
```
