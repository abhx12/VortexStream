"""QoE metric forecasting module."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier


def forecast_metric(
    history_df: Optional[pd.DataFrame] = None,
    metric: str = "qoe_score",
    lag_steps: int = 3,
) -> pd.DataFrame:
    """Predict future QoE drop (>= 10 points) from lagged window features."""
    if history_df is None or history_df.empty:
        # Create minimal synthetic sequence
        rng = np.random.default_rng(42)
        n = 50
        vals = [90.0]
        for _ in range(n - 1):
            vals.append(max(20.0, min(100.0, vals[-1] + rng.normal(0, 3.0))))
        history_df = pd.DataFrame({
            "window_end": [f"2026-10-22T10:{i:02d}:00.000Z" for i in range(n)],
            "grain": "global",
            "dimkey": "ALL",
            "qoe_score": vals,
        })

    df = history_df.copy().sort_values("window_end")
    if metric not in df.columns:
        return df

    # Create lag features
    for lag in range(1, lag_steps + 1):
        df[f"{metric}_lag_{lag}"] = df[metric].shift(lag)

    # Target: QoE drops >= 10 points in next window
    df["qoe_drop_next_window"] = ((df[metric].shift(-1) - df[metric]) <= -10.0).astype(int)

    # Train a lightweight predictor on valid lagged rows
    clean_df = df.dropna().copy()
    if len(clean_df) >= 10:
        lag_cols = [f"{metric}_lag_{lag}" for lag in range(1, lag_steps + 1)]
        X = clean_df[lag_cols]
        y = clean_df["qoe_drop_next_window"]
        if len(np.unique(y)) > 1:
            clf = GradientBoostingClassifier(n_estimators=20, random_state=42)
            clf.fit(X, y)
            df.loc[clean_df.index, "drop_risk_prob"] = clf.predict_proba(X)[:, 1].round(3)
        else:
            df["drop_risk_prob"] = 0.0
    else:
        df["drop_risk_prob"] = 0.0

    return df
