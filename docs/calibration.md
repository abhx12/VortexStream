# Baseline Alert Threshold Calibration Report

## 1. Executive Summary
Calibrated from clean no-fault Puffer replay telemetry over sliding 5-minute windows.

## 2. Proposed Thresholds

| Rule | Metric | Threshold | Method |
|---|---|---|---|
| R1 | Rebuffer Ratio | 0.03 | Baseline p99 + 0.01 margin |
| R2 | QoE Score Drop | 15.0 points | 5-min baseline lag delta |
| R3 | Video Start Failure Rate | 0.15 | Baseline p99 + 0.05 margin |
| R4 | Error Rate | 0.05 | Baseline p99 + 0.02 margin |

## 3. Evidence & Distribution Analysis
- Real Puffer baseline stall ratio is empirically low (< 0.01 in undisturbed conditions).
- Hysteresis requires 2 consecutive breaching windows before firing and 2 healthy windows to resolve.
