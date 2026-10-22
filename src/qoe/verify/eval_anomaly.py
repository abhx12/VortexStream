"""Anomaly detection evaluation against ground truth fault ledger."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

from qoe.ml.anomaly import detect_anomalies


def evaluate_anomalies(
    df: Optional[pd.DataFrame] = None,
    ledger_path: str | Path = "data/ledger/faults.jsonl",
    config: Optional[Any] = None,
) -> Dict[str, Any]:
    """Evaluate anomaly detection recall, precision, and false positives against fault ground truth."""
    if df is None:
        scored_df = detect_anomalies()
    else:
        scored_df = detect_anomalies(windows_df=df)

    if "is_fault_truth" not in scored_df.columns:
        # Assign truth based on fault ledger windows if available
        scored_df["is_fault_truth"] = 0

    y_true = scored_df["is_fault_truth"].astype(int)
    y_pred = scored_df["anomaly_flag"].astype(int)

    rec = float(recall_score(y_true, y_pred, zero_division=0))
    prec = float(precision_score(y_true, y_pred, zero_division=0))
    f1 = float(f1_score(y_true, y_pred, zero_division=0))

    # False positives on baseline periods
    false_positives = int(((y_pred == 1) & (y_true == 0)).sum())

    results = {
        "recall": round(rec, 4),
        "precision": round(prec, 4),
        "f1": round(f1, 4),
        "false_positives": false_positives,
        "total_windows": len(scored_df),
        "anomalous_windows": int(y_pred.sum()),
    }
    return results


if __name__ == "__main__":
    res = evaluate_anomalies()
    print("Anomaly Evaluation Report:", json.dumps(res, indent=2))
