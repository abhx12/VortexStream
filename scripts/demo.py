"""End-to-end demo execution orchestrator for VortexStream."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

from qoe.common.config import get_config
from qoe.data.ref_data import generate_reference_data
from qoe.data.sim_dims import generate_simulated_dimensions
from qoe.ml.churn_model import train_abandonment_model
from qoe.verify.lineage import generate_lineage_docs
from qoe.verify.reconcile import run_reconciliation


def run_demo(speed: int = 10, duration_minutes: int = 15):
    """Run interactive demonstration of VortexStream pipeline."""
    print("=" * 65)
    print(" [VORTEXSTREAM] REAL-TIME OTT VIDEO QoE & BUFFER ANALYTICS ENGINE")
    print("=" * 65)

    print("\n[1/6] Generating Deterministic Reference Data & Dimensions...")
    generate_reference_data()
    generate_simulated_dimensions()
    print("      Reference catalogs and dimensional distributions initialized.")

    print("\n[2/6] Training & Loading MLlib Abandonment PipelineModel...")
    metrics = train_abandonment_model()
    print(f"      Abandonment Model Ready -- AUC-ROC: {metrics['auc_roc']}, Accuracy: {metrics['accuracy']}")

    print("\n[3/6] Generating System Lineage & Architecture Docs...")
    generate_lineage_docs()
    print("      Mermaid lineage updated in docs/architecture.md.")

    print("\n[4/6] Executing Automated Verification Suite (V1-V8)...")
    rec = run_reconciliation()
    print(f"      Reconciliation Gate: {rec['overall']}")

    print("\n[5/6] System Access Endpoints:")
    print("      * Operations Dashboard : http://localhost:8501")
    print("      * HBase Master UI      : http://localhost:16010")
    print("      * Kafka Cluster UI     : http://localhost:8081")

    print("\n[6/6] Simulated Fault Demonstration Timeline:")
    print("      * T = 00:00 - Baseline Replay (Clean playback, QoE > 85, no alerts)")
    print("      * T = 02:30 - Fault F1 (Airtel-Chennai throttle: QoE falls, Alert fires)")
    print("      * T = 06:00 - Fault F2 (Akamai CDN edge slowdown: 5xx errors & rebuffering)")
    print("      * T = 09:00 - Fault F3 (Android v2.1 DRM decoder crash bug)")
    print("      * T = 12:00 - Automatic alert resolution & recovery verification")

    print("\n" + "=" * 65)
    print(" Demo orchestration complete. Ready for live presentation.")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VortexStream Demo Orchestrator")
    parser.add_argument("--speed", type=int, default=10, help="Replay speed multiplier")
    parser.add_argument("--duration", type=int, default=15, help="Demo slice duration in minutes")
    args = parser.parse_args()
    run_demo(speed=args.speed, duration_minutes=args.duration)
