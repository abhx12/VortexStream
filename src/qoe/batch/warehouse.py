"""DuckDB data warehouse manager and analytical views."""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import duckdb

from qoe.common.config import get_config


def init_warehouse(
    warehouse_dir: str | Path = "data/warehouse",
    db_path: Optional[str | Path] = None,
) -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB warehouse database and create analytical views over Parquet."""
    w_dir = Path(warehouse_dir)
    w_dir.mkdir(parents=True, exist_ok=True)

    target_db = Path(db_path) if db_path else w_dir / "qoe.duckdb"
    con = duckdb.connect(str(target_db))

    # Helper to safely create view over parquet directory if it has files
    def safe_create_view(view_name: str, sub_dir: str, dedup: bool = False):
        folder = w_dir / sub_dir
        if folder.is_dir() and any(folder.glob("**/*.parquet")):
            posix_path = folder.as_posix()
            if dedup:
                query = f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    WITH ranked AS (
                        SELECT *,
                               row_number() OVER (
                                   PARTITION BY grain, dimkey, window_end 
                                   ORDER BY updated_at DESC
                               ) as _rn
                        FROM read_parquet('{posix_path}/**/*.parquet')
                    )
                    SELECT * EXCLUDE (_rn)
                    FROM ranked
                    WHERE _rn = 1;
                """
            else:
                query = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{posix_path}/**/*.parquet');"
            con.execute(query)

    safe_create_view("gold_qoe_5m", "gold_qoe_5m_raw", dedup=True)
    safe_create_view("gold_network_5m", "gold_network_5m_raw", dedup=True)
    safe_create_view("silver_playback", "silver_playback_events")
    safe_create_view("silver_network", "silver_network_metrics")
    safe_create_view("session_facts", "session_facts")
    safe_create_view("alerts", "alerts")
    safe_create_view("ml_predictions", "ml_predictions")

    return con


def get_warehouse_connection(
    warehouse_dir: str | Path = "data/warehouse",
    db_path: Optional[str | Path] = None,
) -> duckdb.DuckDBPyConnection:
    """Return an active connection to the DuckDB warehouse."""
    return init_warehouse(warehouse_dir=warehouse_dir, db_path=db_path)


if __name__ == "__main__":
    con = init_warehouse()
    print("DuckDB warehouse initialized with active views.")
    tables = con.execute("SHOW TABLES;").df()
    print(tables)
    con.close()
