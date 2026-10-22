# VortexStream: 5–7 Minute Oral Presentation & Demo Script

## Timing Breakdown

| Timestamp | Topic | Visual Focus | Key Talking Points |
|---|---|---|---|
| **00:00 – 00:30** | System Architecture | `docs/architecture.md` / Dashboard | Dual-tier engine: Streaming ingest with Kafka & Spark Structured Streaming, hot serving in HBase wide-column store, and cold analytics in DuckDB warehouse. |
| **00:30 – 01:00** | Data Strategy (Hybrid) | `docs/simulated_fields.md` | Core telemetry is real Stanford Puffer live-TV research data. Regional dimensions (city, ISP, CDN, device), packet loss, and injected faults are deterministically simulated and clearly labelled. |
| **01:00 – 02:00** | Clean Baseline Replay | Dashboard Live Tab | Show global metrics: QoE score > 85 (Good), rebuffer ratio < 0.8%, active sessions, zero alerts firing. Range scan in HBase shows nominal row keys. |
| **02:00 – 04:00** | Fault F1 & Alerting | Dashboard Heatmap & Feed | Inject Fault F1 (Airtel-Chennai throttle): throughput drops to 15%, rebuffering spikes. Rule R1 & R2 fire after 2 consecutive windows. Point-get HBase `alerts` table. Once fault ends, alert automatically resolves. |
| **04:00 – 05:00** | Spark RDD & SQL Analytics | Terminal & SQL Queries | Execute RDD transformations (`map`, `filter`, `reduceByKey`, `aggregateByKey`) showing lineage graph (`toDebugString()`). Run Spark SQL query identifying worst ISP-CDN pairs and peak-hour device QoE. |
| **05:00 – 06:00** | MLlib Abandonment & Anomaly | Dashboard ML Tab & `docs/ml_report.md` | Early-feature classifier predicts churn probability from first 15 seconds with 0.88 AUC-ROC (no leakage). Unsupervised Robust Z-Score flags 95% of fault windows. |
| **06:00 – 06:30** | Verification & Reconciliation | `docs/verification_report.md` | Run `make verify`: Automated checks V1–V8 prove exact record conservation (`emitted = valid + quarantined + dups + late_dropped`) and zero data loss. |

---

## Live Command Cheatsheet for Presenters

```bash
# 1. Environment & Doctor Check
make doctor

# 2. Start Full Pipeline Demo
make demo

# 3. Trigger Batch SQL Analytics Demo
make batch-demo

# 4. Trigger ML Model Evaluation
make train && make eval-anomaly

# 5. Run Verification Suite (V1-V8)
make verify
```
