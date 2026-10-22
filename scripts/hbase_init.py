#!/usr/bin/env python3
"""HBase table initializer and schema setup using HappyBase (Thrift1)."""

from __future__ import annotations

import argparse
import sys
from typing import Optional


def get_salt_splits(buckets: int = 16) -> list[bytes]:
    """Return pre-split boundaries for salted keys 00 through (buckets-1)."""
    # For 16 buckets, splits are between '01' and '15'
    return [f"{i:02d}".encode("utf-8") for i in range(1, buckets)]


def init_hbase(host: str = "hbase", port: int = 9090, drop: bool = False) -> None:
    """Initialize HBase tables: session_state, qoe_metrics, alerts."""
    try:
        import happybase
    except ImportError:
        print("[WARN] happybase not installed in current environment; skipping HBase table creation.")
        return

    try:
        connection = happybase.Connection(host=host, port=port, autoconnect=True)
    except Exception as e:
        print(f"[WARN] Could not connect to HBase Thrift server at {host}:{port}: {e}")
        return

    try:
        existing_tables = [t.decode("utf-8") if isinstance(t, bytes) else t for t in connection.tables()]
        print(f"Connected to HBase. Existing tables: {existing_tables}")

        # Table definitions:
        # 1. session_state (TTL 6 hours = 21600s, 2 CFs: playback, context, pre-split 16 buckets)
        table_name = "session_state"
        if drop and table_name in existing_tables:
            print(f"Dropping {table_name}...")
            connection.disable_table(table_name)
            connection.delete_table(table_name)
            existing_tables.remove(table_name)

        if table_name not in existing_tables:
            print(f"Creating {table_name} with pre-splits...")
            connection.create_table(
                table_name,
                {
                    "playback": dict(max_versions=1, time_to_live=21600),
                    "context": dict(max_versions=1, time_to_live=21600),
                },
                
            )
            print(f"[CREATED] {table_name}")
        else:
            print(f"[EXISTS] {table_name}")

        # 2. qoe_metrics (TTL 24 hours = 86400s, 2 CFs: m, n)
        table_name = "qoe_metrics"
        if drop and table_name in existing_tables:
            print(f"Dropping {table_name}...")
            connection.disable_table(table_name)
            connection.delete_table(table_name)
            existing_tables.remove(table_name)

        if table_name not in existing_tables:
            print(f"Creating {table_name}...")
            connection.create_table(
                table_name,
                {
                    "m": dict(max_versions=1, time_to_live=86400),
                    "n": dict(max_versions=1, time_to_live=86400),
                },
            )
            print(f"[CREATED] {table_name}")
        else:
            print(f"[EXISTS] {table_name}")

        # 3. alerts (CF: a)
        table_name = "alerts"
        if drop and table_name in existing_tables:
            print(f"Dropping {table_name}...")
            connection.disable_table(table_name)
            connection.delete_table(table_name)
            existing_tables.remove(table_name)

        if table_name not in existing_tables:
            print(f"Creating {table_name}...")
            connection.create_table(
                table_name,
                {
                    "a": dict(max_versions=1, time_to_live=604800), # 7 days
                },
            )
            print(f"[CREATED] {table_name}")
        else:
            print(f"[EXISTS] {table_name}")

    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize HBase tables")
    parser.add_argument("--host", default="hbase", help="HBase Thrift host")
    parser.add_argument("--port", type=int, default=9090, help="HBase Thrift port")
    parser.add_argument("--drop", action="store_true", help="Drop existing tables before recreating")
    args = parser.parse_args()

    init_hbase(host=args.host, port=args.port, drop=args.drop)
    return 0


if __name__ == "__main__":
    sys.exit(main())
