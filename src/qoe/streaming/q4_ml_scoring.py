"""Streaming Query Q4: Real-time abandonment scoring on early session features."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd

from qoe.common.config import get_config, get_topic
from qoe.ml.churn_model import load_abandonment_model, predict_abandonment
from qoe.streaming.sinks_warehouse import write_partitioned_parquet


def score_early_features_batch(
    facts: List[Dict[str, Any]],
    model_dir: str | Path = "data/models/abandon_v1",
    warehouse_dir: Optional[str | Path] = None,
    config: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Score early_features facts using saved ML model and write predictions."""
    early_facts = [f for f in facts if f.get("kind") == "early_features"]
    if not early_facts:
        return []

    model = load_abandonment_model(model_dir=model_dir)

    # Extract features for scoring
    extracted_records: List[Dict[str, Any]] = []
    for f in early_facts:
        f_n = f.get("first_N_s") or {}
        extracted_records.append({
            "session_id": f.get("session_id"),
            "startup_ms": f.get("startup_ms") or f_n.get("startup_ms") or 0.0,
            "stall_count_early": f.get("stall_count") or f_n.get("stall_count") or 0,
            "stall_ms_early": f.get("stall_ms") or f_n.get("stall_ms") or 0.0,
            "avg_bitrate_early": f.get("avg_bitrate_kbps") or f_n.get("avg_bitrate_kbps") or 0.0,
            "switch_count_early": f.get("switch_count") or f_n.get("switch_count") or 0,
            "device": f.get("device") or "unknown",
            "isp": f.get("isp") or "unknown",
            "cdn": f.get("cdn") or "unknown",
            "city": f.get("city") or "unknown",
            "scheme": f_n.get("scheme") or "unknown",
        })

    probs, preds = predict_abandonment(model, extracted_records)

    predictions: List[Dict[str, Any]] = []
    for f, prob, pred in zip(early_facts, probs, preds):
        pred_rec = {
            "session_id": f.get("session_id"),
            "ts": f.get("ts"),
            "abandon_probability": round(float(prob), 4),
            "predicted_label": int(pred),
            "model_version": "v1.0",
            "device": f.get("device"),
            "isp": f.get("isp"),
            "cdn": f.get("cdn"),
            "city": f.get("city"),
            "scheme": (f.get("first_N_s") or {}).get("scheme"),
        }
        predictions.append(pred_rec)

    if warehouse_dir and predictions:
        write_partitioned_parquet(
            predictions,
            output_dir=Path(warehouse_dir) / "ml_predictions",
            partition_cols=["predicted_label"],
        )

    return predictions


def run_q4_ml_scoring(*args, **kwargs) -> None:
    """Run Q4 real-time scoring query."""
    # When executed as standalone, load model and test scoring
    load_abandonment_model()
    print("Q4 ML scoring initialized.")


if __name__ == "__main__":
    run_q4_ml_scoring()
