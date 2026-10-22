"""QoE metric anomaly detection (Robust Z-Score and KMeans)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import duckdb
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from qoe.common.config import get_config


def compute_robust_z_score(series: pd.Series) -> pd.Series:
    """Calculate robust z-score: 0.6745 * (x - median) / MAD."""
    med = series.median()
    mad = (series - med).abs().median()
    if mad == 0 or np.isnan(mad):
        mad = (series - med).abs().mean() + 1e-6
    return 0.6745 * (series - med) / (mad + 1e-6)


def detect_anomalies(
    windows_df: Optional[pd.DataFrame] = None,
    warehouse_dir: str | Path = "data/warehouse",
    z_threshold: float = 3.5,
    config: Optional[Any] = None,
) -> pd.DataFrame:
    """Detect anomalous metric windows using Robust Z-Score and KMeans clustering."""
    cfg = config or get_config()
    thresh = float(cfg.get("ml", {}).get("anomaly", {}).get("robust_z_threshold", z_threshold))

    if windows_df is None:
        w_path = Path(warehouse_dir) / "gold_qoe_5m_raw"
        if w_path.is_dir() and any(w_path.glob("**/*.parquet")):
            con = duckdb.connect(":memory:")
            df = con.execute(f"SELECT * FROM read_parquet('{w_path.as_posix()}/**/*.parquet');").df()
            con.close()
        else:
            # Synthetic demo window metrics
            rng = np.random.default_rng(42)
            rows = []
            for i in range(120):
                # Fault injected between window 40 and 60
                is_fault = 40 <= i <= 60
                rebuf = float(rng.uniform(0.08, 0.25)) if is_fault else float(rng.uniform(0.0, 0.02))
                qoe = float(rng.uniform(35.0, 55.0)) if is_fault else float(rng.uniform(85.0, 98.0))
                tp = float(rng.uniform(500.0, 1500.0)) if is_fault else float(rng.uniform(4000.0, 8000.0))

                rows.append({
                    "window_end": f"2026-10-22T10:{i//2:02d}:{(i%2)*30:02d}.000Z",
                    "grain": "city_isp",
                    "dimkey": "Chennai|Airtel",
                    "rebuffer_ratio": round(rebuf, 4),
                    "qoe_score": round(qoe, 2),
                    "avg_throughput_kbps": round(tp, 1),
                    "is_fault_truth": 1 if is_fault else 0,
                })
            df = pd.DataFrame(rows)
    else:
        df = windows_df.copy()

    if df.empty:
        return df

    # 1. Robust Z-scores per segment
    if "rebuffer_ratio" in df.columns:
        df["z_rebuf"] = compute_robust_z_score(df["rebuffer_ratio"])
    else:
        df["z_rebuf"] = 0.0

    if "qoe_score" in df.columns:
        # Negative drop is anomalous for QoE
        df["z_qoe"] = compute_robust_z_score(df["qoe_score"])
    else:
        df["z_qoe"] = 0.0

    df["robust_z_anomaly"] = (df["z_rebuf"].abs() > thresh) | (df["z_qoe"] < -thresh)

    # 2. KMeans Clustering on normalized metrics
    feature_cols = [c for c in ["rebuffer_ratio", "qoe_score", "avg_throughput_kbps"] if c in df.columns]
    if len(feature_cols) >= 2 and len(df) >= 10:
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(df[feature_cols].fillna(0.0))
        kmeans = KMeans(n_clusters=3, random_state=42, n_init="auto")
        df["cluster"] = kmeans.fit_predict(X_scaled)

        # Distance to assigned cluster centroid
        centroids = kmeans.cluster_centers_
        dists = np.linalg.norm(X_scaled - centroids[df["cluster"]], axis=1)
        dist_thresh = np.percentile(dists, 95)
        df["kmeans_anomaly"] = dists > dist_thresh
    else:
        df["kmeans_anomaly"] = False

    df["anomaly_flag"] = (df["robust_z_anomaly"] | df["kmeans_anomaly"]).astype(int)
    df["anomaly_score"] = df["z_rebuf"].abs().round(2)

    return df


if __name__ == "__main__":
    res_df = detect_anomalies()
    print("Detected anomalies:", res_df["anomaly_flag"].sum())
