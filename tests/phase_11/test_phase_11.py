"""Unit tests for Phase 11: Machine learning models, anomaly detection, and streaming scoring."""

from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd
import pytest

from qoe.ml.anomaly import detect_anomalies
from qoe.ml.churn_model import (
    load_abandonment_model,
    predict_abandonment,
    train_abandonment_model,
)
from qoe.ml.dataset import generate_synthetic_ml_dataset, prepare_ml_dataset
from qoe.ml.forecast import forecast_metric
from qoe.streaming.q4_ml_scoring import score_early_features_batch
from qoe.verify.eval_anomaly import evaluate_anomalies


def test_no_leakage():
    """Verify features are strictly from early window and label-derived columns are excluded."""
    df = prepare_ml_dataset()
    assert "watch_s" in df.columns
    assert "abandoned" in df.columns

    # Verify early feature columns exist
    for col in ["startup_ms", "stall_count_early", "stall_ms_early", "avg_bitrate_early"]:
        assert col in df.columns

    # In training, features X must not include watch_s or label
    from qoe.ml.churn_model import NUMERICAL_COLS, CATEGORICAL_COLS
    assert "watch_s" not in NUMERICAL_COLS
    assert "abandoned" not in NUMERICAL_COLS
    assert "watch_s" not in CATEGORICAL_COLS
    assert "abandoned" not in CATEGORICAL_COLS


def test_pipeline_fits_and_transforms(tmp_path):
    """Verify ML pipeline trains and outputs valid probabilities in [0, 1]."""
    df = generate_synthetic_ml_dataset(n_samples=200, seed=42)
    m_dir = tmp_path / "models" / "abandon_test"

    metrics = train_abandonment_model(df=df, model_dir=m_dir)
    assert metrics["auc_roc"] >= 0.60
    assert 0.0 <= metrics["accuracy"] <= 1.0

    model = load_abandonment_model(model_dir=m_dir)
    test_rows = df.iloc[:20].to_dict(orient="records")
    probs, preds = predict_abandonment(model, test_rows)

    assert len(probs) == 20
    assert len(preds) == 20
    assert (probs >= 0.0).all() and (probs <= 1.0).all()
    assert set(preds).issubset({0, 1})


def test_metrics_floor(tmp_path):
    """Verify AUC-ROC exceeds 0.60 baseline requirement."""
    df = generate_synthetic_ml_dataset(n_samples=400, seed=123)
    metrics = train_abandonment_model(df=df, model_dir=tmp_path / "models" / "abandon_v1")
    assert metrics["auc_roc"] > 0.60, f"Expected AUC-ROC > 0.60, got {metrics['auc_roc']}"


def test_model_roundtrip(tmp_path):
    """Verify model save and reload produces identical predictions."""
    df = generate_synthetic_ml_dataset(n_samples=200, seed=99)
    m_dir = tmp_path / "models" / "roundtrip"
    train_abandonment_model(df=df, model_dir=m_dir)

    model1 = load_abandonment_model(model_dir=m_dir)
    model2 = load_abandonment_model(model_dir=m_dir)

    sample_rows = df.iloc[:50].to_dict(orient="records")
    probs1, preds1 = predict_abandonment(model1, sample_rows)
    probs2, preds2 = predict_abandonment(model2, sample_rows)

    np.testing.assert_allclose(probs1, probs2, rtol=1e-5)
    np.testing.assert_array_equal(preds1, preds2)


def test_anomaly_vs_ledger():
    """Verify anomaly detection flags fault windows with recall >= 0.8."""
    report = evaluate_anomalies()
    assert report["recall"] >= 0.80, f"Expected anomaly recall >= 0.80, got {report['recall']}"
    assert report["anomalous_windows"] > 0


def test_forecast_metric():
    """Verify metric forecasting computes drop risk probabilities."""
    fc_df = forecast_metric()
    assert "drop_risk_prob" in fc_df.columns
    assert (fc_df["drop_risk_prob"] >= 0.0).all()


def test_streaming_scoring_batch(tmp_path):
    """Verify Q4 scoring transforms early_features facts into prediction records."""
    m_dir = tmp_path / "models" / "scoring"
    train_abandonment_model(model_dir=m_dir)

    facts = [
        {
            "schema_version": 1,
            "fact_id": "f1",
            "session_id": "sess-sc-1-0",
            "ts": "2026-10-22T10:00:15.000Z",
            "kind": "early_features",
            "device": "android",
            "isp": "Jio",
            "cdn": "Cloudflare",
            "city": "Mumbai",
            "startup_ms": 1500.0,
            "stall_count": 0,
            "stall_ms": 0.0,
            "avg_bitrate_kbps": 3000.0,
            "switch_count": 0,
            "first_N_s": {"scheme": "bba"},
        },
        {
            "schema_version": 1,
            "fact_id": "f2",
            "session_id": "sess-sc-2-0",
            "ts": "2026-10-22T10:00:15.000Z",
            "kind": "startup",  # Not early_features, should be ignored
        },
    ]

    preds = score_early_features_batch(
        facts=facts,
        model_dir=m_dir,
        warehouse_dir=tmp_path / "warehouse",
    )

    assert len(preds) == 1
    p0 = preds[0]
    assert p0["session_id"] == "sess-sc-1-0"
    assert 0.0 <= p0["abandon_probability"] <= 1.0
    assert p0["predicted_label"] in (0, 1)
    assert p0["model_version"] == "v1.0"
