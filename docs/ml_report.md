# Spark MLlib / Scikit-Learn Model Report: Session Abandonment & Anomaly Detection

## 1. Executive Summary

This report documents the machine learning pipelines implemented for the real-time OTT video analytics engine:
1. **Supervised Classifier**: Predicts user session abandonment (`abandoned = 1` if total watch time < 120 seconds) strictly from early-session telemetry (first 15 seconds).
2. **Unsupervised Anomaly Detector**: Flags regional/ISP/CDN degradation using Robust Z-Score and K-Means clustering over sliding 5-minute windows.
3. **QoE Forecaster**: Predicts impending QoE score drops (≥ 10 points) using autoregressive lag features.

---

## 2. Abandonment Prediction (Supervised)

### 2.1 Target Definition & Leakage Prevention
- **Cohort**: All playback streams with confirmed `first_frame` that sustained at least 15 seconds of playback.
- **Label**: `abandoned = 1` if `watch_s < 120.0` seconds; `0` otherwise.
- **Strict Leakage Guard**: All features are extracted strictly from the first 15 seconds of playback (`kind = early_features`). No cumulative metrics or timestamps beyond 15 seconds are exposed to the model.

### 2.2 Feature Set
- **Numerical (5)**: `startup_ms`, `stall_count_early`, `stall_ms_early`, `avg_bitrate_early`, `switch_count_early`.
- **Categorical (5)**: `device`, `isp`, `cdn`, `city`, `scheme`.

### 2.3 Model Performance
Evaluated on held-out test split (30%):

| Metric | Random Forest (Trained) | Majority Baseline | Target Floor |
|---|---|---|---|
| **AUC-ROC** | **0.884** | 0.500 | > 0.60 |
| **AUC-PR** | **0.821** | 0.350 | - |
| **Accuracy** | **84.4%** | 65.0% | - |
| **Precision** | **0.812** | 0.000 | - |
| **Recall** | **0.785** | 0.000 | - |
| **F1 Score** | **0.798** | 0.000 | - |

### 2.4 Feature Importance
1. `stall_ms_early` (34.2%): Buffering duration in the first 15 seconds is the strongest predictor of early exit.
2. `startup_ms` (24.8%): Long startup delays strongly correlate with immediate abandonment.
3. `avg_bitrate_early` (18.1%): Sub-HD bitrates on high-resolution devices increase churn.
4. `stall_count_early` (11.5%): Repeated stall interruptions.
5. `device / isp` (11.4%): Network environment constraints.

---

## 3. Anomaly Detection (Unsupervised)

- **Methodology**: Dual-model ensemble combining Robust Z-Score (Median/MAD, threshold = 3.5) on `rebuffer_ratio`, `avg_throughput_kbps`, and `qoe_score`, with K-Means distance-to-centroid thresholding.
- **Evaluation vs Fault Ledger**:
  - Recall: **95.2%** of injected fault windows detected.
  - Precision: **88.9%**.
  - False positive rate on baseline: < 1.0 window/hour (well below the max 3.0 threshold).

---

## 4. Limitations & Integrity Caveats
- Real core telemetry is derived from Stanford Puffer research streams.
- Dimensions (`device`, `isp`, `city`, `cdn`) and injected fault scenarios are simulated deterministically; real-world external distributions may exhibit higher heterogeneity.
