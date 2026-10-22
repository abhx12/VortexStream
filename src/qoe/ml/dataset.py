"""ML training and evaluation dataset preparation from early session features."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import duckdb
import numpy as np
import pandas as pd

from qoe.common.config import get_config


def generate_synthetic_ml_dataset(n_samples: int = 600, seed: int = 42) -> pd.DataFrame:
    """Generate realistic early session feature dataset with ground truth abandonment labels."""
    rng = np.random.default_rng(seed)

    devices = ["android", "ios", "smarttv", "web"]
    isps = ["Jio", "Airtel", "Vi", "ACT", "BSNL"]
    cdns = ["Cloudflare", "Akamai", "Fastly"]
    cities = ["Mumbai", "Delhi", "Bengaluru", "Chennai", "Hyderabad"]
    schemes = ["bba", "linear", "elastic"]

    rows: List[Dict[str, Any]] = []
    base_ts = 1792670000.0

    for i in range(n_samples):
        sid = f"sess-ml-{i}-0"
        dev = rng.choice(devices)
        isp = rng.choice(isps)
        cdn = rng.choice(cdns)
        city = rng.choice(cities)
        sch = rng.choice(schemes)

        # Early metrics (first 15 seconds)
        startup_ms = float(rng.exponential(scale=1800.0) + 400.0)
        has_stall = rng.random() < 0.25
        stall_count = int(rng.poisson(lam=1.5)) if has_stall else 0
        stall_ms = float(rng.uniform(1000.0, 5000.0) * stall_count) if stall_count > 0 else 0.0
        avg_bitrate = float(rng.choice([1200, 2400, 3600, 5000, 8000]))
        switch_count = int(rng.poisson(lam=0.8))

        # Abandonment rule: high stalls, high startup delay, or low bitrate increase abandon probability
        abandon_prob = 0.10
        if startup_ms > 4000.0:
            abandon_prob += 0.35
        if stall_ms > 2000.0:
            abandon_prob += 0.40
        if avg_bitrate < 2000.0:
            abandon_prob += 0.14


        abandon_prob = min(0.95, max(0.05, abandon_prob))
        abandoned = int(rng.random() < abandon_prob)

        # Total watch time: if abandoned, watch_s < 120s, else >= 120s
        watch_s = float(rng.uniform(15.0, 110.0)) if abandoned else float(rng.uniform(180.0, 1800.0))

        rows.append({
            "session_id": sid,
            "ts": f"2026-10-22T10:{i//60:02d}:{i%60:02d}.000Z",
            "startup_ms": round(startup_ms, 1),
            "stall_count_early": stall_count,
            "stall_ms_early": round(stall_ms, 1),
            "avg_bitrate_early": round(avg_bitrate, 1),
            "switch_count_early": switch_count,
            "device": dev,
            "isp": isp,
            "cdn": cdn,
            "city": city,
            "scheme": sch,
            "watch_s": round(watch_s, 1),
            "abandoned": abandoned,
        })

    return pd.DataFrame(rows)


def prepare_ml_dataset(
    warehouse_dir: str | Path = "data/warehouse",
    min_samples: int = 100,
    config: Optional[Any] = None,
) -> pd.DataFrame:
    """Build feature dataset for abandonment prediction strictly from first 15s features."""
    w_dir = Path(warehouse_dir)
    facts_path = w_dir / "session_facts"

    if facts_path.is_dir() and any(facts_path.glob("**/*.parquet")):
        con = duckdb.connect(":memory:")
        con.execute(f"""
            CREATE VIEW facts_view AS 
            SELECT * FROM read_parquet('{facts_path.as_posix()}/**/*.parquet');
        """)

        # Join early_features with session_end to extract label (total watch_s < 120)
        df = con.execute("""
            WITH early AS (
                SELECT session_id, ts, startup_ms, stall_count as stall_count_early,
                       stall_ms as stall_ms_early, avg_bitrate_kbps as avg_bitrate_early,
                       switch_count as switch_count_early, device, isp, cdn, city
                FROM facts_view
                WHERE kind = 'early_features'
            ),
            terminal AS (
                SELECT session_id, max(watch_ms) / 1000.0 as watch_s
                FROM facts_view
                WHERE kind IN ('session_end', 'vsf')
                GROUP BY session_id
            )
            SELECT e.*, t.watch_s,
                   CASE WHEN t.watch_s < 120.0 THEN 1 ELSE 0 END as abandoned
            FROM early e
            JOIN terminal t ON e.session_id = t.session_id
        """).df()

        if len(df) >= min_samples:
            return df

    # Fallback to simulated clean dataset
    return generate_synthetic_ml_dataset(n_samples=500)
