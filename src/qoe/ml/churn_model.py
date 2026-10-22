"""Session abandonment classifier training, evaluation, and serialization."""

from __future__ import annotations

import json
from pathlib import Path
import pickle
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score, average_precision_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from qoe.common.config import get_config
from qoe.ml.dataset import prepare_ml_dataset

NUMERICAL_COLS = ["startup_ms", "stall_count_early", "stall_ms_early", "avg_bitrate_early", "switch_count_early"]
CATEGORICAL_COLS = ["device", "isp", "cdn", "city", "scheme"]


def build_pipeline() -> Pipeline:
    """Build preprocessing and classification pipeline."""
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), NUMERICAL_COLS),
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CATEGORICAL_COLS),
        ]
    )
    clf = RandomForestClassifier(n_estimators=50, max_depth=6, random_state=42)
    return Pipeline([("preprocessor", preprocessor), ("classifier", clf)])


def train_abandonment_model(
    df: Optional[pd.DataFrame] = None,
    model_dir: str | Path = "data/models/abandon_v1",
    config: Optional[Any] = None,
) -> Dict[str, Any]:
    """Train abandonment classification model, evaluate metrics, and save artifacts."""
    if df is None:
        df = prepare_ml_dataset()

    # Fill missing columns if any
    for col in NUMERICAL_COLS:
        if col not in df.columns:
            df[col] = 0.0
        else:
            df[col] = df[col].fillna(0.0)

    for col in CATEGORICAL_COLS:
        if col not in df.columns:
            df[col] = "unknown"
        else:
            df[col] = df[col].fillna("unknown")

    X = df[NUMERICAL_COLS + CATEGORICAL_COLS]
    y = df["abandoned"].astype(int)

    # Train / test split (70 / 30)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y if len(np.unique(y)) > 1 else None
    )

    pipeline = build_pipeline()
    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    y_prob = pipeline.predict_proba(X_test)[:, 1]

    auc_roc = float(roc_auc_score(y_test, y_prob)) if len(np.unique(y_test)) > 1 else 1.0
    auc_pr = float(average_precision_score(y_test, y_prob)) if len(np.unique(y_test)) > 1 else 1.0
    acc = float(accuracy_score(y_test, y_pred))
    prec = float(precision_score(y_test, y_pred, zero_division=0))
    rec = float(recall_score(y_test, y_pred, zero_division=0))
    f1 = float(f1_score(y_test, y_pred, zero_division=0))

    metrics = {
        "auc_roc": round(auc_roc, 4),
        "auc_pr": round(auc_pr, 4),
        "accuracy": round(acc, 4),
        "precision": round(prec, 4),
        "recall": round(rec, 4),
        "f1": round(f1, 4),
        "train_samples": len(X_train),
        "test_samples": len(X_test),
    }

    # Save model and metrics
    out_dir = Path(model_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "model.pkl", "wb") as f:
        pickle.dump(pipeline, f)

    with open(out_dir / "metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    return metrics


def load_abandonment_model(model_dir: str | Path = "data/models/abandon_v1") -> Pipeline:
    """Load trained pipeline from disk."""
    m_path = Path(model_dir) / "model.pkl"
    if not m_path.is_file():
        # Auto-train and save if not exists
        train_abandonment_model(model_dir=model_dir)
    with open(m_path, "rb") as f:
        return pickle.load(f)


def predict_abandonment(
    model: Pipeline,
    records: List[Dict[str, Any]],
) -> Tuple[np.ndarray, np.ndarray]:
    """Predict abandonment probability and binary label for incoming feature dicts."""
    df = pd.DataFrame(records)
    for col in NUMERICAL_COLS:
        if col not in df.columns:
            df[col] = 0.0
        else:
            df[col] = df[col].fillna(0.0)

    for col in CATEGORICAL_COLS:
        if col not in df.columns:
            df[col] = "unknown"
        else:
            df[col] = df[col].fillna("unknown")

    X = df[NUMERICAL_COLS + CATEGORICAL_COLS]
    probs = model.predict_proba(X)[:, 1]
    preds = model.predict(X)
    return probs, preds


if __name__ == "__main__":
    m = train_abandonment_model()
    print("Abandonment model trained:", json.dumps(m, indent=2))
