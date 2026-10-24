"""Data access layer for dashboard reading from HBase and DuckDB."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import duckdb
import pandas as pd

from qoe.common.config import get_config
from qoe.common.hbase_keys import parse_qoe_key, qoe_key


def _parse_ts(ts_val: Any) -> float:
    if not ts_val:
        return 0.0
    ts_str = str(ts_val).strip()
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(ts_str).timestamp()
    except Exception:
        return 0.0


class DashboardDataAccess:
    """Provides querying methods for live HBase metrics and historical DuckDB views."""

    def __init__(
        self,
        hbase_host: Optional[str] = None,
        hbase_port: Optional[int] = None,
        warehouse_dir: str | Path = "data/warehouse",
        config: Optional[Any] = None,
    ):
        cfg = config or get_config()
        h_cfg = cfg.get("hbase", {})
        self.hbase_host = hbase_host or str(h_cfg.get("host", "hbase"))
        self.hbase_port = int(hbase_port or h_cfg.get("port", 9090))
        self.warehouse_dir = Path(warehouse_dir)
        self.config = cfg

    def _query_duckdb(self, query: str) -> pd.DataFrame:
        """Run DuckDB query safely over warehouse files."""
        try:
            con = duckdb.connect(":memory:")
            # Register views if directories exist
            raw_qoe = self.warehouse_dir / "gold_qoe_5m_raw"
            if raw_qoe.is_dir() and any(raw_qoe.glob("**/*.parquet")):
                con.execute(f"CREATE VIEW IF NOT EXISTS gold_qoe_5m_raw AS SELECT * FROM read_parquet('{raw_qoe.as_posix()}/**/*.parquet');")
                con.execute("""
                    CREATE VIEW IF NOT EXISTS gold_qoe_5m AS
                    WITH ranked AS (
                        SELECT *, row_number() OVER (PARTITION BY grain, dimkey, window_end ORDER BY updated_at DESC) as rn
                        FROM gold_qoe_5m_raw
                    )
                    SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1;
                """)

            raw_hourly = self.warehouse_dir / "gold_qoe_hourly"
            if raw_hourly.is_dir() and any(raw_hourly.glob("**/*.parquet")):
                con.execute(f"CREATE VIEW IF NOT EXISTS gold_qoe_hourly AS SELECT * FROM read_parquet('{raw_hourly.as_posix()}/**/*.parquet');")

            raw_alerts = self.warehouse_dir / "alerts"
            if raw_alerts.is_dir() and any(raw_alerts.glob("**/*.parquet")):
                con.execute(f"CREATE VIEW IF NOT EXISTS alerts AS SELECT * FROM read_parquet('{raw_alerts.as_posix()}/**/*.parquet');")

            raw_preds = self.warehouse_dir / "ml_predictions"
            if raw_preds.is_dir() and any(raw_preds.glob("**/*.parquet")):
                con.execute(f"CREATE VIEW IF NOT EXISTS ml_predictions AS SELECT * FROM read_parquet('{raw_preds.as_posix()}/**/*.parquet');")

            raw_silver = self.warehouse_dir / "silver_playback_events"
            if raw_silver.is_dir() and any(raw_silver.glob("**/*.parquet")):
                con.execute(f"CREATE VIEW IF NOT EXISTS silver_playback AS SELECT * FROM read_parquet('{raw_silver.as_posix()}/**/*.parquet');")

            df = con.execute(query).df()
            con.close()
            return df
        except Exception:
            return pd.DataFrame()

    def latest_segment(self, grain: str, dimkey: str) -> Optional[Dict[str, Any]]:
        """Retrieve latest window row for a specific (grain, dimkey)."""
        # Try HBase first
        try:
            import happybase
            conn = happybase.Connection(host=self.hbase_host, port=self.hbase_port, timeout=2000)
            table = conn.table("qoe_metrics")
            prefix = f"{grain}#{dimkey}#".encode("utf-8")
            rows = list(table.scan(row_prefix=prefix, limit=1))
            conn.close()
            if rows:
                row_key, data = rows[0]
                res: Dict[str, Any] = {"row_key": row_key.decode("utf-8"), "grain": grain, "dimkey": dimkey}
                for k, v in data.items():
                    col_name = k.decode("utf-8").split(":", 1)[-1]
                    res[col_name] = v.decode("utf-8")
                return res
        except Exception:
            pass

        # DuckDB fallback
        query = f"""
            SELECT * FROM gold_qoe_5m 
            WHERE grain = '{grain}' AND dimkey = '{dimkey}' 
            ORDER BY window_end DESC LIMIT 1;
        """
        df = self._query_duckdb(query)
        if not df.empty:
            return df.iloc[0].to_dict()
        return None

    def segment_history(self, grain: str, dimkey: str, minutes: int = 60) -> List[Dict[str, Any]]:
        """Retrieve recent windows for a segment, returned chronologically (oldest to newest)."""
        limit_count = max(1, minutes // 5)
        # Try HBase first for live telemetry
        try:
            import happybase
            conn = happybase.Connection(host=self.hbase_host, port=self.hbase_port, timeout=2000)
            table = conn.table("qoe_metrics")
            prefix = f"{grain}#{dimkey}#".encode("utf-8")
            rows = list(table.scan(row_prefix=prefix, limit=limit_count))
            conn.close()
            if rows:
                hbase_hist = []
                for row_key, data in rows:
                    rec = {"row_key": row_key.decode("utf-8"), "grain": grain, "dimkey": dimkey}
                    for k, v in data.items():
                        col_name = k.decode("utf-8").split(":", 1)[-1]
                        val_str = v.decode("utf-8")
                        try:
                            rec[col_name] = float(val_str)
                        except ValueError:
                            rec[col_name] = val_str
                    hbase_hist.append(rec)
                # HBase reverse timestamps order newest first; reverse so oldest is first for plotting
                return list(reversed(hbase_hist))
        except Exception:
            pass

        # DuckDB fallback
        query = f"""
            SELECT * FROM gold_qoe_5m 
            WHERE grain = '{grain}' AND dimkey = '{dimkey}' 
            ORDER BY window_end DESC LIMIT {limit_count};
        """
        df = self._query_duckdb(query)
        if not df.empty:
            rows = df.to_dict(orient="records")
            return list(reversed(rows))
        return []

    def latest_all(self, grain: str) -> List[Dict[str, Any]]:
        """Retrieve latest window row for all dimkeys in a grain."""
        # Try HBase first
        try:
            import happybase
            conn = happybase.Connection(host=self.hbase_host, port=self.hbase_port, timeout=2000)
            table = conn.table("qoe_metrics")
            prefix = f"{grain}#".encode("utf-8")
            seen_dimkeys = set()
            hbase_all = []
            for row_key, data in table.scan(row_prefix=prefix, limit=100):
                rk = row_key.decode("utf-8")
                parts = rk.split("#")
                if len(parts) >= 3:
                    dk = parts[1]
                    if dk not in seen_dimkeys:
                        seen_dimkeys.add(dk)
                        rec = {"row_key": rk, "grain": grain, "dimkey": dk}
                        for k, v in data.items():
                            col_name = k.decode("utf-8").split(":", 1)[-1]
                            val_str = v.decode("utf-8")
                            try:
                                rec[col_name] = float(val_str)
                            except ValueError:
                                rec[col_name] = val_str
                        hbase_all.append(rec)
            conn.close()
            if hbase_all:
                return hbase_all
        except Exception:
            pass

        query = f"""
            WITH ranked AS (
                SELECT *, row_number() OVER (PARTITION BY dimkey ORDER BY window_end DESC) as rn
                FROM gold_qoe_5m
                WHERE grain = '{grain}'
            )
            SELECT * EXCLUDE (rn) FROM ranked WHERE rn = 1;
        """
        df = self._query_duckdb(query)
        return df.to_dict(orient="records") if not df.empty else []

    def recent_alerts(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Retrieve recent alert records sorted newest first from HBase."""
        # Query HBase alerts table directly
        try:
            import happybase
            conn = happybase.Connection(host=self.hbase_host, port=self.hbase_port, timeout=2000)
            table = conn.table("alerts")
            hbase_alerts = []
            for row_key, data in table.scan(limit=limit):
                a = {"row_key": row_key.decode("utf-8")}
                for k, v in data.items():
                    col_name = k.decode("utf-8").split(":", 1)[-1]
                    val_str = v.decode("utf-8")
                    if col_name in ("metric_value", "value", "threshold"):
                        try:
                            a[col_name] = float(val_str)
                        except ValueError:
                            a[col_name] = val_str
                    else:
                        a[col_name] = val_str
                hbase_alerts.append(a)
            conn.close()
            return hbase_alerts
        except Exception:
            pass

        # DuckDB fallback only if HBase fails
        query = f"""
            SELECT * FROM alerts 
            ORDER BY window_end DESC, opened_at DESC 
            LIMIT {limit};
        """
        df = self._query_duckdb(query)
        return df.to_dict(orient="records") if not df.empty else []

    def active_sessions_count(self) -> int:
        """Estimate current active session count from latest global window or state."""
        g = self.latest_segment("global", "ALL")
        if g and "sessions" in g:
            try:
                return int(g["sessions"])
            except ValueError:
                return 0
        return 0

    def history_hourly_qoe(self) -> pd.DataFrame:
        """Return hourly aggregated QoE trends."""
        query = """
            SELECT hour_window, grain, dimkey, qoe_score, rebuffer_ratio, avg_bitrate_kbps, sessions
            FROM gold_qoe_hourly
            WHERE grain = 'global'
            ORDER BY hour_window ASC;
        """
        return self._query_duckdb(query)

    def history_worst_segments(self, limit: int = 10) -> pd.DataFrame:
        """Return worst performing segments historically."""
        query = f"""
            SELECT grain, dimkey, avg(qoe_score) as avg_qoe,
                   sum(stall_ms_sum) / nullif(sum(watch_ms_sum + stall_ms_sum), 0) as rebuffer_ratio,
                   sum(stall_count) as total_stalls
            FROM gold_qoe_5m
            WHERE grain IN ('city_isp', 'cdn', 'device')
            GROUP BY grain, dimkey
            ORDER BY avg_qoe ASC
            LIMIT {limit};
        """
        return self._query_duckdb(query)

    def model_risk_distribution(self) -> pd.DataFrame:
        """Return distribution of predicted abandonment risk."""
        query = """
            SELECT predicted_label,
                   avg(abandon_probability) as mean_risk,
                   count(*) as count_sessions
            FROM ml_predictions
            GROUP BY predicted_label;
        """
        return self._query_duckdb(query)

    def get_data_freshness_seconds(self) -> float:
        """Return elapsed seconds since the latest processed window_end."""
        g = self.latest_segment("global", "ALL")
        if g and "window_end" in g:
            w_ts = _parse_ts(g["window_end"])
            if w_ts > 0:
                now_ts = datetime.now(timezone.utc).timestamp()
                return max(0.0, now_ts - w_ts)
        return 0.0
