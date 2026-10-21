"""Stream-static enrichment joins against content, users, and subscriptions."""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import duckdb
import pandas as pd


def enrich_with_reference_data(
    df: pd.DataFrame,
    ref_dir: str | Path = "data/ref",
) -> pd.DataFrame:
    """Left join telemetry DataFrame with content and users reference data without dropping rows."""
    if df.empty:
        return df.copy()

    ref_path = Path(ref_dir)
    content_file = ref_path / "content.parquet"
    users_file = ref_path / "users.parquet"
    subs_file = ref_path / "subscriptions.parquet"

    out_df = df.copy()
    if "video_id" not in out_df.columns:
        out_df["video_id"] = None
    if "user_id" not in out_df.columns:
        out_df["user_id"] = None

    # If reference files don't exist yet, return unchanged
    if not content_file.is_file() and not users_file.is_file():
        return out_df

    con = duckdb.connect(":memory:")
    con.register("stream_df", out_df)

    join_sql = "SELECT s.*"
    joins = []

    if content_file.is_file():
        con.execute(f"CREATE VIEW ref_content AS SELECT video_id, title, genre, language FROM read_parquet('{content_file.as_posix()}');")
        join_sql += ", c.title AS content_title, c.genre AS content_genre, c.language AS content_language"
        joins.append("LEFT JOIN ref_content c ON s.video_id = c.video_id")

    if users_file.is_file():
        con.execute(f"CREATE VIEW ref_users AS SELECT user_id, plan_tier, signup_date FROM read_parquet('{users_file.as_posix()}');")
        join_sql += ", u.plan_tier, u.signup_date"
        joins.append("LEFT JOIN ref_users u ON s.user_id = u.user_id")

    final_sql = f"{join_sql} FROM stream_df s {' '.join(joins)};"
    enriched = con.execute(final_sql).df()

    assert len(enriched) == len(df), f"Enrichment must preserve all {len(df)} rows, got {len(enriched)}"
    return enriched
