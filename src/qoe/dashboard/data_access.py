"""Data access layer for dashboard reading from HBase and DuckDB."""

from __future__ import annotations


class DashboardDataAccess:
    """Provides querying methods for live HBase metrics and historical DuckDB views."""

    def __init__(self, hbase_host: str, hbase_port: int, warehouse_dir: str):
        self.hbase_host = hbase_host
        self.hbase_port = hbase_port
        self.warehouse_dir = warehouse_dir
